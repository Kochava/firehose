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

package main

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

// GetKafkaConsumer returns a new consumer
func GetKafkaConsumer(custConfig Config, file *os.File) (*consumergroup.ConsumerGroup, error) {
	config := consumergroup.NewConfig()
	config.Consumer.Return.Errors = true

	appMetricRegistry := metrics.NewRegistry()

	consumerMetricRegistry := metrics.NewPrefixedChildRegistry(appMetricRegistry, "consumer.")

	config.MetricRegistry = consumerMetricRegistry

	go metrics.Log(consumerMetricRegistry, 30*time.Second, log.New(file, "consumer: ", log.Lmicroseconds))

	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	var zookeeperNodes []string
	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(custConfig.zookeepers)

	consumer, consumerErr := consumergroup.JoinConsumerGroup("firehose_realtime", []string{custConfig.topic}, zookeeperNodes, config)
	if consumerErr != nil {
		return nil, consumerErr
	}

	// Create new consumer
	return consumer, nil

}

// GetKafkaProducer returns a new consumer
func GetKafkaProducer(custConfig Config, file *os.File) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Errors = false
	config.Producer.Return.Successes = false
	// config.Producer.Partitioner = sarama.NewManualPartitioner
	config.ClientID = "firehose"
	if custConfig.historical {
		config.ClientID = "firehose-historical"
	}

	log.Println("GetKafkaProducer - client id ", config.ClientID)

	appMetricRegistry := metrics.NewRegistry()

	producerMetricRegistry := metrics.NewPrefixedChildRegistry(appMetricRegistry, "producer.")

	config.MetricRegistry = producerMetricRegistry

	go metrics.Log(producerMetricRegistry, 30*time.Second, log.New(file, "producer: ", log.Lmicroseconds))

	// Create new consumer
	return sarama.NewAsyncProducer(custConfig.dstBrokers, config)

}

// PullFromTopic pulls messages from the topic partition
func PullFromTopic(consumer *consumergroup.ConsumerGroup,
	producer chan<- sarama.ProducerMessage,
	signals chan os.Signal,
	finalOffset int64,
	syncChan chan int64,
	wg *sync.WaitGroup) {

	defer wg.Done()

	for {
		if len(signals) > 0 {
			log.Println("Consumer - Interrupt is detected - exiting")
			return
		}
		select {
		case err := <-consumer.Errors():
			log.Println(err)
			return
		case consumerMsg := <-consumer.Messages():
			producerMsg := sarama.ProducerMessage{
				Topic:     consumerMsg.Topic,
				Partition: consumerMsg.Partition,
				Key:       sarama.StringEncoder(consumerMsg.Key),
				Value:     sarama.StringEncoder(consumerMsg.Value),
			}
			producer <- producerMsg

			if finalOffset > 0 && consumerMsg.Offset >= finalOffset {
				syncChan <- consumerMsg.Offset
				log.Println("Consumer - partition ", consumerMsg.Partition, " reached final offset, shutting down partition")
				return
			}
		}
	}
}

// PushToTopic pushes messages to topic
func PushToTopic(producer sarama.AsyncProducer,
	consumer <-chan sarama.ProducerMessage,
	signals chan os.Signal,
	syncChan chan int64,
	wg *sync.WaitGroup) {

	defer wg.Done()

	for {
		if len(signals) > 0 {
			log.Println("Producer - Interrupt is detected - exiting")
			return
		}
		select {
		case consumerMsg := <-consumer:
			producer.Input() <- &consumerMsg
		}
		if len(consumer) <= 0 && len(syncChan) > 0 {
			parNum := <-syncChan
			log.Println("Producer - partition ", parNum, " finished, closing partition")
			return
		}
	}
}

// MonitorChan monitors the transfer channel
func MonitorChan(transferChan chan sarama.ProducerMessage, signals chan os.Signal, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if len(signals) > 0 {
			log.Println("Monitor - Interrupt is detected - exiting")
			return
		}
		log.Println("Transfer channel length: ", len(transferChan))
		time.Sleep(10 * time.Second)
	}
}

// CloseProducer Closes the producer
func CloseProducer(producer *sarama.AsyncProducer) {
	log.Println("Closing producer client")
	if err := (*producer).Close(); err != nil {
		// Should not reach here
		log.Println(err)
	}
}

// CloseConsumer closes the consumer
func CloseConsumer(consumer *consumergroup.ConsumerGroup) {
	log.Println("Closing consumer client")
	if err := (*consumer).Close(); err != nil {
		// Should not reach here
		log.Println(err)
	}
}
