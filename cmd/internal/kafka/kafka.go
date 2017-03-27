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
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

// GetKafkaConsumer returns a new consumer
func GetKafkaConsumer(srcZookeepers []string, topic string) (*consumergroup.ConsumerGroup, error) {
	groupConfig := consumergroup.NewConfig()
	groupConfig.Offsets.Initial = sarama.OffsetNewest
	groupConfig.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes := srcZookeepers
	groupConfig.Zookeeper.Chroot = ""

	topicSlice := []string{topic}

	consumer, err := consumergroup.JoinConsumerGroup("firehose", topicSlice, zookeeperNodes, groupConfig)
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
func GetKafkaProducer(dstZookeepers []string) (sarama.SyncProducer, error) {
	config := consumergroup.NewConfig()

	config.Config.Producer.Retry.Max = 1
	config.Config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Config.Producer.Return.Successes = true
	config.Config.Producer.Return.Errors = true
	// config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Config.ClientID = "firehose_realtime"

	log.Printf("GetKafkaProducer - client id %v\n", config.Config.ClientID)

	zookeepers := dstZookeepers
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

	return sarama.NewSyncProducer(brokerList, config.Config)
}

// PullFromTopic pulls messages from the topic partition
func PullFromTopic(consumer *consumergroup.ConsumerGroup, producer chan<- sarama.ProducerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	eventCount := 0
	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {
		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount++
		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
			log.Printf("PullFromTopic - Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		producerMsg := sarama.ProducerMessage{
			Topic: message.Topic,
			Key:   sarama.StringEncoder(message.Key),
			Value: sarama.StringEncoder(message.Value),
		}
		producer <- producerMsg

		log.Printf("PullFromTopic - got offset %v on partition %v", message.Offset, message.Partition)

		offsets[message.Topic][message.Partition] = message.Offset
		consumer.CommitUpto(message)
	}

	// for consumerMsg := range consumer.Messages() {
	// 	log.Println("got message")
	// 	producerMsg := sarama.ProducerMessage{
	// 		Topic: consumerMsg.Topic,
	// 		Key:   sarama.StringEncoder(consumerMsg.Key),
	// 		Value: sarama.StringEncoder(consumerMsg.Value),
	// 	}
	// 	producer <- producerMsg
	// 	consumer.CommitUpto(consumerMsg)
	// }
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
func PushToTopic(producer sarama.SyncProducer, consumer <-chan sarama.ProducerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		// case err := <-producer.Errors():
		// 	log.Println("Failed to produce message", err)
		// case <-producer.Successes():
		// 	success++
		case consumerMsg := <-consumer:
			_, _, err := producer.SendMessage(&consumerMsg)
			if err != nil {
				log.Printf("PushToTopic - FAILED to send message: %s\n", err)
			}
		}
	}
}

// MonitorChan monitors the transfer channel
func MonitorChan(transferChan chan sarama.ProducerMessage, brokers []string, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	client, err := sarama.NewClient(brokers, nil) // I am not giving any configuration
	if err != nil {
		log.Printf("MonitorChan - %v", err)
		return
	}

	for {
		partitions, err := client.Partitions(topic)
		if err != nil {
			log.Printf("MonitorChan - %v", err)
		}

		log.Println("==============================")
		log.Println("=         Monitoring         =")
		log.Println("==============================")
		for _, p := range partitions {
			lastoffset, err := client.GetOffset(topic, p, sarama.OffsetNewest)
			if err != nil {
				log.Printf("MonitorChan - %v", err)
			}
			log.Printf("Partition %v Last Commited Offset %v", p, lastoffset)
		}
		time.Sleep(10 * time.Second)
	}
}
