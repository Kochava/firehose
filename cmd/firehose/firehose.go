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
	"sync"

	"git.dev.kochava.com/jbury/firehose/cmd/internal/kafka"
	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
)

// Essentially main
func startFirehose(c *cli.Context, conf *Config) error {

	var wg sync.WaitGroup
	transferChan := make(chan sarama.ProducerMessage, 100000)

	for i := 0; i < conf.ConsumerConcurrency; i++ {
		log.Println("Getting the Kafka consumer")
		consumer, cErr := kafka.GetKafkaConsumer(conf.SourceZookeepers, conf.Topic)
		if cErr != nil {
			log.Println("startFirehose - Unable to create the consumer")
			return cErr
		}

		log.Println("Starting error consumer")
		go kafka.GetConsumerErrors(consumer)
		defer consumer.Close()

		wg.Add(1)
		go kafka.PullFromTopic(consumer, transferChan, &wg)
	}

	for i := 0; i < conf.ProducerConcurrency; i++ {
		log.Println("Getting the Kafka producer")
		producer, err := kafka.GetKafkaProducer(conf.DestinationZookeepers)
		if err != nil {
			log.Printf("startFirehose - Unable to create the producer: %v\n", err)
			return err
		}
		defer producer.Close()

		wg.Add(1)
		go kafka.PushToTopic(producer, transferChan, &wg)
	}

	wg.Add(1)
	go kafka.MonitorChan(transferChan, []string{conf.SourceKafkaBroker}, conf.SourceZookeepers, conf.Topic, &wg)

	wg.Wait()

	return nil
}
