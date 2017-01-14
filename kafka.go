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
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

// GetKafkaConsumer returns a new consumer
func GetKafkaConsumer(custConfig *Config) (*consumergroup.ConsumerGroup, error) {
	groupConfig := consumergroup.NewConfig()
	groupConfig.Offsets.Initial = sarama.OffsetNewest
	groupConfig.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes := strings.Split(custConfig.SourceZookeepers, ",")
	groupConfig.Zookeeper.Chroot = ""

	topic := []string{custConfig.Topic}

	consumer, err := consumergroup.JoinConsumerGroup("firehose", topic, zookeeperNodes, groupConfig)
	if err != nil {
		log.Printf("GetKafkaConsumer - Failed to join consumer group: %v\n", err)
		return nil, err
	}

	// Create new consumer
	return consumer, nil

}

// GetConsumerErrors logs any consumer errors
func GetConsumerErrors(consumer *consumergroup.ConsumerGroup) {
	for err := range consumer.Errors() {
		log.Printf("GetConsumerErrors - %v\n", err)
	}
}

// GetKafkaProducer returns a new consumer
func GetKafkaProducer(custConfig *Config) (sarama.AsyncProducer, error) {
	config := consumergroup.NewConfig()

	config.Config.Producer.Retry.Max = 1
	config.Config.Producer.RequiredAcks = sarama.WaitForAll
	config.Config.Producer.Return.Errors = true
	config.Config.Producer.Return.Successes = true
	// config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Config.ClientID = "firehose_realtime"

	log.Printf("GetKafkaProducer - client id %v\n", config.Config.ClientID)

	zookeepers := strings.Split(custConfig.DestinationZookeepers, ",")
	kz, err := kazoo.NewKazoo(zookeepers, config.Zookeeper)
	if err != nil {
		log.Printf("GetKafkaProducer - Unable to create zookeeper object: %v\n", err)
		return nil, err
	}

	// Create new consumer
	brokerList, bErr := kz.BrokerList()
	if bErr != nil {
		log.Printf("GetKafkaProducer - Unable to get broker list: %v\n", err)
		return nil, err
	}

	return sarama.NewAsyncProducer(brokerList, config.Config)
}

// PullFromTopic pulls messages from the topic partition
func PullFromTopic(consumer *consumergroup.ConsumerGroup, producer chan<- sarama.ProducerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	for consumerMsg := range consumer.Messages() {
		producerMsg := sarama.ProducerMessage{
			Topic: consumerMsg.Topic,
			Key:   sarama.StringEncoder(consumerMsg.Key),
			Value: sarama.StringEncoder(consumerMsg.Value),
		}
		producer <- producerMsg
		consumer.CommitUpto(consumerMsg)
	}
	// 	// for {
	// 	// 	if len(signals) > 0 {
	// 	// 		log.Println("Consumer - Interrupt is detected - exiting")
	// 	// 		return
	// 	// 	}
	// 	// 	select {
	// 	// 	case err := <-consumer.Errors():
	// 	// 		log.Println(err)
	// 	// 		return
	// 	// 	case consumerMsg := <-consumer.Messages():
	// 	// 		producerMsg := sarama.ProducerMessage{
	// 	// 			Topic:     consumerMsg.Topic,
	// 	// 			Partition: consumerMsg.Partition,
	// 	// 			Key:       sarama.StringEncoder(consumerMsg.Key),
	// 	// 			Value:     sarama.StringEncoder(consumerMsg.Value),
	// 	// 		}
	// 	//
	// 	//
	// 	// 		if finalOffset > 0 && consumerMsg.Offset >= finalOffset {
	// 	// 			syncChan <- consumerMsg.Offset
	// 	// 			log.Println("Consumer - partition ", consumerMsg.Partition, " reached final offset, shutting down partition")
	// 	// 			return
	// 	// 		}
	// 	// 	}
	// 	// }
}

// PushToTopic pushes messages to topic
func PushToTopic(producer sarama.AsyncProducer, consumer <-chan sarama.ProducerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	success := 1

	for {
		select {
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
		case <-producer.Successes():
			success++
		case consumerMsg := <-consumer:
			producer.Input() <- &consumerMsg
		}
	}
}

// MonitorChan monitors the transfer channel
func MonitorChan(transferChan chan sarama.ProducerMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		log.Println("Transfer channel length: ", len(transferChan))
		time.Sleep(10 * time.Second)
	}
}
