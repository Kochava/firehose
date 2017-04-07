// Copyright 2017 Kochava
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kochava/firehose/cmd/internal/influxlogger"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

// Kafka object that provides nice helper functions
type Kafka struct {
	// Config
	Conf Config

	Brokers []string

	// Kafka stuff
	Producer sarama.AsyncProducer
	Consumer *consumergroup.ConsumerGroup

	ProducerChan chan *sarama.ProducerMessage
	TransferChan chan *sarama.ProducerMessage

	errors int32

	// internal kafka stuff
	kzClient        *kazoo.Kazoo
	kzConsumerGroup *kazoo.Consumergroup
	kafkaClient     sarama.Client
	kafkaConfig     *consumergroup.Config

	// other stuff
	WaitGroup *sync.WaitGroup
	Shutdown  chan struct{}
	influx    influxlogger.InfluxD
}

// Config is a convenient wrapper for all the config values needed for InitKafka
type Config struct {
	Topic                string
	Zookeepers           []string
	ConsumerGroupName    string
	ConsumerBuffer       int
	MaxErrors            int
	MaxRetry             int
	BatchSize            int
	FlushInterval        int
	ConsumerTransactions *uint64
	ProducerTransactions *uint64
}

// InitKafka initializes the Kafka object creating some helper clients
func InitKafka(conf Config, influxAccessor influxlogger.InfluxD, signalChan chan struct{}, wg *sync.WaitGroup) (*Kafka, error) {
	var err error
	kafka := &Kafka{
		Conf:      conf,
		Shutdown:  signalChan,
		WaitGroup: wg,
	}

	kafka.kafkaConfig = consumergroup.NewConfig()

	kafka.kafkaConfig.Config.Producer.Retry.Max = conf.MaxRetry
	kafka.kafkaConfig.Config.Producer.RequiredAcks = sarama.WaitForLocal
	kafka.kafkaConfig.Config.Producer.Compression = sarama.CompressionGZIP
	kafka.kafkaConfig.Config.Producer.Flush.Messages = conf.BatchSize
	kafka.kafkaConfig.Config.Producer.Flush.Frequency = time.Millisecond * time.Duration(conf.FlushInterval)
	kafka.kafkaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	kafka.kafkaConfig.Config.Producer.Return.Successes = true
	kafka.kafkaConfig.Config.Producer.Return.Errors = true
	kafka.kafkaConfig.Config.ClientID = "firehose_realtime"

	log.Printf("InitKafka - client id %v\n", kafka.kafkaConfig.Config.ClientID)

	kafka.kzClient, err = kazoo.NewKazoo(conf.Zookeepers, kafka.kafkaConfig.Zookeeper)
	if err != nil {
		log.Printf("InitKafka - Unable to initialize kazoo client: %v", err)
		return nil, err
	}

	kafka.Brokers, err = kafka.kzClient.BrokerList()
	if err != nil {
		log.Printf("InitKafka - Unable to get list of brokers: %v", err)
		return nil, err
	}

	log.Printf("InitKafka - Brokers connecting to %v", kafka.Brokers)

	kafka.kafkaClient, err = sarama.NewClient(kafka.Brokers, nil) // this will just be used for grabbing config so no reason to give conf
	if err != nil {
		log.Printf("InitKafka - Unable to create kafka client: %v", err)
		return nil, err
	}

	kafka.kzConsumerGroup = kafka.kzClient.Consumergroup(conf.ConsumerGroupName)

	atomic.StoreUint64(kafka.Conf.ConsumerTransactions, 0)
	atomic.StoreUint64(kafka.Conf.ProducerTransactions, 0)

	kafka.influx = influxAccessor

	return kafka, err
}

// GetTransferChan returns a transfer channel that can be used to pass messages around
func GetTransferChan(consumerBuffer int) chan *sarama.ProducerMessage {
	return make(chan *sarama.ProducerMessage, consumerBuffer)
}

// InitConsumer sets up the consumer for kafka
func (k *Kafka) InitConsumer(transferChan chan *sarama.ProducerMessage, reset bool) error {
	var err error
	groupConfig := consumergroup.NewConfig()
	groupConfig.Offsets.Initial = sarama.OffsetNewest
	groupConfig.Offsets.ProcessingTimeout = 10 * time.Second
	groupConfig.Offsets.ResetOffsets = reset

	groupConfig.Zookeeper.Chroot = ""

	topicSlice := []string{k.Conf.Topic}

	k.Consumer, err = consumergroup.JoinConsumerGroup(k.Conf.ConsumerGroupName, topicSlice, k.Conf.Zookeepers, groupConfig)
	if err != nil {
		log.Printf("InitConsumer - Failed to join consumer group: %v\n", err)
		return err
	}

	k.TransferChan = transferChan

	// Return err (should be nil if there was no err)
	return err
}

// GetConsumerErrors logs any consumer errors
func (k *Kafka) GetConsumerErrors() {
	for err := range k.Consumer.Errors() {
		log.Printf("GetConsumerErrors - %v", err)
		atomic.AddInt32(&k.errors, 1)
		if int(k.errors) > k.Conf.MaxErrors {
			close(k.Shutdown)
			log.Println("GetConsumerErrors - shutting down")
			return
		}
	}
}

// InitProducerFromConsumer sets up the producer for kafka using a consumer channel as input
func (k *Kafka) InitProducerFromConsumer(transferChan chan *sarama.ProducerMessage) error {
	var err error

	k.TransferChan = transferChan

	k.Producer, err = sarama.NewAsyncProducer(k.Brokers, k.kafkaConfig.Config)
	if err != nil {
		log.Printf("InitConsumer - Failed to create producer: %v\n", err)
		return err
	}

	return err
}

// Pull pulls messages from the topic partition
func (k *Kafka) Pull() {
	defer k.WaitGroup.Done()
	defer log.Println("Pull - done")

	for {
		select {
		case <-k.Shutdown:
			log.Println("Pull - shutting down")
			return
		case msg := <-k.Consumer.Messages():
			if msg != nil {
				producerMsg := &sarama.ProducerMessage{
					Topic: msg.Topic,
					Key:   sarama.StringEncoder(msg.Key),
					Value: sarama.StringEncoder(msg.Value),
				}
				k.TransferChan <- producerMsg

				atomic.AddUint64(k.Conf.ConsumerTransactions, 1)

				k.Consumer.CommitUpto(msg)
			}
		}
	}

}

// Push pushes messages to topic
func (k *Kafka) Push() {
	defer k.WaitGroup.Done()
	defer log.Println("Push - done")

	for {
		select {
		case <-k.Shutdown:
			if len(k.TransferChan) == 0 || k.errors > 0 {
				log.Println("Push - shutting down")
				return
			}
		case err := <-k.Producer.Errors():
			log.Println("Failed to produce message to kafka cluster. ", err)
			atomic.AddInt32(&k.errors, 1)
			if int(k.errors) > k.Conf.MaxErrors {
				close(k.Shutdown)
				log.Println("Push - shutting down")
				return
			}
		case msg := <-k.TransferChan:
			k.Producer.Input() <- msg
		}
	}
}

// RPSTicker simply pulls from Successes and ticks the rps counter
func (k *Kafka) RPSTicker() {
	for range k.Producer.Successes() {
		atomic.AddUint64(k.Conf.ProducerTransactions, 1)
	}
}

// Close closes all of the associated connections
func (k *Kafka) Close() {
	if k.Producer != nil {
		log.Println("Closing producer client")
		if err := k.Producer.Close(); err != nil {
			// Should not reach here
			log.Println(err)
		}
		log.Println("Producer client closed")
	}

	if k.Consumer != nil {
		log.Println("Closing consumer client")
		if err := k.Consumer.Close(); err != nil {
			// Should not reach here
			log.Println(err)
		}
		log.Println("Consumer client closed")
	}
}

// Monitor monitors the transfer channel
func (k *Kafka) Monitor() {
	var partitionDiff int64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-k.Shutdown:
			log.Println("Monitor - shutting down")
			return
		case <-ticker.C:
			if k.Consumer != nil {
				partitions, err := k.getNewestOffsets()

				for p, offset := range partitions {
					zkOffset, _ := k.kzConsumerGroup.FetchOffset(k.Conf.Topic, p)
					if err != nil {
						log.Printf("MonitorChan - %v", err)
					}
					partitionDiff += (offset - (zkOffset))
					k.influx.CreateKafkaOffsetPoint(k.Conf.ConsumerGroupName, k.Conf.Topic, p, zkOffset, offset)
				}
				partitionDiff = 0
			}
		}
	}
}

func (k *Kafka) getNewestOffsets() (map[int32]int64, error) {
	offsets := make(map[int32]int64)
	var err error

	partitions, err := k.kafkaClient.Partitions(k.Conf.Topic)
	if err != nil {
		log.Printf("getCurrentOffsets - %v", err)
		return offsets, err
	}

	for _, p := range partitions {
		lastoffset, _ := k.kafkaClient.GetOffset(k.Conf.Topic, p, sarama.OffsetNewest)
		offsets[p] = lastoffset
	}
	return offsets, nil
}

// LogRPS aggregates the transaction count for this process
func LogRPS(tag, topic string, influxAccessor influxlogger.InfluxD, rps []*uint64) {
	for {
		time.Sleep(time.Second)
		var tr uint64
		for _, r := range rps {
			tr += atomic.LoadUint64(r)
			atomic.StoreUint64(r, 0)
		}
		log.Printf("LogRPS - %s - RPS %v", tag, int64(tr))
		influxAccessor.CreateRPSPoint(topic, tag, int64(tr))
	}
}
