// Copyright 2016 Kochava
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
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

// Kafka object that provides nice helper functions
type Kafka struct {
	// Config
	Topic             string
	Zookeepers        []string
	Brokers           []string
	ConsumerGroupName string

	// Kafka stuff
	Producer sarama.AsyncProducer
	Consumer *consumergroup.ConsumerGroup

	ProducerChan chan *sarama.ProducerMessage
	TransferChan chan *sarama.ProducerMessage

	// internal kafka stuff
	kzClient                  *kazoo.Kazoo
	kzConsumerGroup           *kazoo.Consumergroup
	kafkaClient               sarama.Client
	kafkaConfig               *consumergroup.Config
	kafkaConsumerTransactions uint64
	kafkaProducerTransactions uint64
	consumerBuffer            int

	// other stuff
	WaitGroup  *sync.WaitGroup
	SignalChan chan os.Signal
}

// InitKafka initializes the Kafka object creating some helper clients
func InitKafka(topic string, zookeepers []string, consumerBuffer int, signalChan chan os.Signal, wg *sync.WaitGroup) (*Kafka, error) {
	var err error
	kafka := &Kafka{
		Topic:             topic,
		Zookeepers:        zookeepers,
		ConsumerGroupName: fmt.Sprintf("%s_firehose", topic),
		consumerBuffer:    consumerBuffer,
		SignalChan:        signalChan,
		WaitGroup:         wg,
	}

	kafka.kafkaConfig = consumergroup.NewConfig()

	kafka.kafkaConfig.Config.Producer.Retry.Max = 1
	kafka.kafkaConfig.Config.Producer.RequiredAcks = sarama.WaitForLocal
	kafka.kafkaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	kafka.kafkaConfig.Config.Producer.Return.Successes = true
	kafka.kafkaConfig.Config.Producer.Return.Errors = true
	kafka.kafkaConfig.Config.ClientID = "firehose_realtime"

	log.Printf("GetKafkaProducer - client id %v\n", kafka.kafkaConfig.Config.ClientID)

	kafka.kzClient, err = kazoo.NewKazoo(zookeepers, kafka.kafkaConfig.Zookeeper)
	if err != nil {
		log.Printf("InitKafka - Unable to initialize kazoo client: %v", err)
		return nil, err
	}

	kafka.Brokers, err = kafka.kzClient.BrokerList()
	if err != nil {
		log.Printf("InitKafka - Unable to get list of brokers: %v", err)
		return nil, err
	}

	kafka.kafkaClient, err = sarama.NewClient(kafka.Brokers, nil) // this will just be used for grabbing config so no reason to give conf
	if err != nil {
		log.Printf("InitKafka - Unable to create kafka client: %v", err)
		return nil, err
	}

	kafka.kzConsumerGroup = kafka.kzClient.Consumergroup(kafka.ConsumerGroupName)

	atomic.StoreUint64(&kafka.kafkaConsumerTransactions, 0)
	atomic.StoreUint64(&kafka.kafkaProducerTransactions, 0)

	return kafka, err
}

// GetTransferChan returns a transfer channel that can be used to pass messages around
func GetTransferChan(consumerBuffer int) chan *sarama.ProducerMessage {
	return make(chan *sarama.ProducerMessage, consumerBuffer)
}

// InitConsumer sets up the consumer for kafka
func (k *Kafka) InitConsumer(transferChan chan *sarama.ProducerMessage) error {
	var err error
	groupConfig := consumergroup.NewConfig()
	groupConfig.Offsets.Initial = sarama.OffsetNewest
	groupConfig.Offsets.ProcessingTimeout = 10 * time.Second

	groupConfig.Zookeeper.Chroot = ""

	topicSlice := []string{k.Topic}

	k.Consumer, err = consumergroup.JoinConsumerGroup(k.ConsumerGroupName, topicSlice, k.Zookeepers, groupConfig)
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
		log.Printf("GetConsumerErrors - %v\n", err)
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

	for {
		select {
		case sig := <-k.SignalChan:
			k.SignalChan <- sig
			return
		case msg := <-k.Consumer.Messages():
			producerMsg := &sarama.ProducerMessage{
				Topic: msg.Topic,
				Key:   sarama.StringEncoder(msg.Key),
				Value: sarama.StringEncoder(msg.Value),
			}
			k.TransferChan <- producerMsg

			atomic.AddUint64(&k.kafkaConsumerTransactions, 1)

			k.Consumer.CommitUpto(msg)
		}
	}

}

// Push pushes messages to topic
func (k *Kafka) Push() {
	defer k.WaitGroup.Done()

	for {
		select {
		case sig := <-k.SignalChan:
			k.SignalChan <- sig
			if len(k.TransferChan) == 0 {
				return
			}
		case err := <-k.Producer.Errors():
			log.Println("Failed to produce message to kafka cluster. ", err)
			return
		case <-k.Producer.Successes():
			atomic.AddUint64(&k.kafkaProducerTransactions, 1)
		case msg := <-k.TransferChan:
			select {
			case k.Producer.Input() <- msg:
			default:
				k.TransferChan <- msg
			}
		}
	}
}

// Monitor monitors the transfer channel
func (k *Kafka) Monitor() {
	var partitionDiff int64
	for {
		if k.Consumer != nil {
			partitions, err := k.kafkaClient.Partitions(k.Topic)
			if err != nil {
				log.Printf("MonitorChan - %v", err)
			}

			for _, p := range partitions {
				lastoffset, err := k.kafkaClient.GetOffset(k.Topic, p, sarama.OffsetNewest)
				zkOffset, _ := k.kzConsumerGroup.FetchOffset(k.Topic, p)
				if err != nil {
					log.Printf("MonitorChan - %v", err)
				}
				// log.Printf("MonitorChan - Consumer - Partition %v Kafka Offset %v Zookeeper Offset %v", p, lastoffset, zkOffset)

				partitionDiff += (lastoffset - (zkOffset))
			}

			log.Printf("MonitorChan - Consumer - Avg partition diff %v", (partitionDiff / int64(len(partitions))))
			log.Printf("MonitorChan - Consumer - RPS %v", atomic.LoadUint64(&k.kafkaConsumerTransactions))
			partitionDiff = 0
		}
		if k.Producer != nil {
			log.Printf("MonitorChan - Producer RPS %v", atomic.LoadUint64(&k.kafkaProducerTransactions))
		}

		log.Printf("MonitorChan - Transfer Channel length %v", len(k.TransferChan))

		atomic.StoreUint64(&k.kafkaConsumerTransactions, 0)
		atomic.StoreUint64(&k.kafkaProducerTransactions, 0)
		time.Sleep(1 * time.Second)
	}
}
