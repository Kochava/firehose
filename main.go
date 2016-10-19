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
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

func main() {

	config := InitConfig()

	config.GetConfig()

	logFile, err := os.OpenFile(config.firehoseLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	metricsFile, err := os.OpenFile(config.metricsLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}
	defer metricsFile.Close()

	log.Println("Getting the Kafka consumer")
	consumer, err := GetKafkaConsumer(config, metricsFile)
	if err != nil {
		log.Fatalln("Unable to create consumer", err)
	}

	log.Println("Getting the Kafka producer")
	producer, err := GetKafkaProducer(config, metricsFile)
	if err != nil {
		log.Fatalln("Unable to create producer", err)
	}

	defer CloseConsumer(&consumer)
	defer CloseProducer(&producer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	var wg sync.WaitGroup

	configClient := NewClient(config)
	defer configClient.Close()

	transferChan := make(chan sarama.ProducerMessage, 100000)

	log.Println(configClient.GetNumPartitions())
	for partition := 0; partition < configClient.GetNumPartitions(); partition++ {

		syncChan := make(chan int64, 1)
		offset := sarama.OffsetNewest
		finalOffset := int64(-1)

		if config.historical {
			c := NewClient(config)
			offset, finalOffset = c.GetCustomOffset(lastFourHours)
			err := c.Close()
			if err != nil {
				log.Fatalln("Unable to close client. ", err)
			}

		}

		log.Println("Using offset ", offset, " for partition ", partition)
		partitionConsumer, err := consumer.ConsumePartition(config.topic, int32(partition), offset)
		if err != nil {
			log.Println("Unable to create partition consumer", err)
			c := NewClient(config)
			offset, finalOffset = c.GetCustomOffset(lastFourHours)
			err := c.Close()
			if err != nil {
				log.Fatalln("Unable to close client. ", err)
			}
			partitionConsumer, err = consumer.ConsumePartition(config.topic, int32(partition), offset)
			if err != nil {
				log.Println("Unable to create partition consumer", err)
				continue
			}
		}

		wg.Add(1)

		go PullFromTopic(partitionConsumer, transferChan, signals, finalOffset, syncChan, &wg)
		log.Println("Started consumer for partition ", partition)

		wg.Add(2)
		go PushToTopic(producer, transferChan, signals, syncChan, &wg)
		log.Println("Started producer")
		go PushToTopic(producer, transferChan, signals, syncChan, &wg)
		log.Println("Started producer")
	}

	go MonitorChan(transferChan, signals, &wg)

	wg.Wait()

	log.Println("All threads done, closing clients and ending")
}
