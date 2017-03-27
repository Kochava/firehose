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

	"github.com/Kochava/firehose/cmd/internal/kafka"
	"github.com/urfave/cli"
)

// Essentially main
func startFirehose(c *cli.Context, conf *Config) error {

	signals := make(chan os.Signal, 10)
	transferChan := kafka.GetTransferChan(100000)
	var wg sync.WaitGroup

	for i := 0; i < conf.ConsumerConcurrency; i++ {
		var err error
		kafkaClient, err := kafka.InitKafka(conf.Topic, conf.SourceZookeepers, 100000, signals, &wg)
		if err != nil {
			log.Println("startFirehose - Unable to create the kafka consumer client")
			return err
		}

		log.Println("Initializing the Kafka consumer")
		err = kafkaClient.InitConsumer(transferChan)
		if err != nil {
			log.Println("startFirehose - Unable to create the consumer")
			return err
		}

		log.Println("Starting error consumer")
		go kafkaClient.GetConsumerErrors()
		defer kafkaClient.Consumer.Close()

		log.Println("Starting consumer")
		kafkaClient.WaitGroup.Add(1)
		go kafkaClient.Pull()

		log.Println("Starting consumer monitor thread")
		go kafkaClient.Monitor()
	}

	for i := 0; i < conf.ProducerConcurrency; i++ {
		var err error
		kafkaClient, err := kafka.InitKafka(conf.Topic, conf.DestinationZookeepers, 100000, signals, &wg)
		if err != nil {
			log.Println("startFirehose - Unable to create the kafka producer client")
			return err
		}

		log.Println("Initializing the Kafka producer")
		err = kafkaClient.InitProducerFromConsumer(transferChan)
		if err != nil {
			log.Printf("startFirehose - Unable to create the producer: %v\n", err)
			return err
		}
		defer kafkaClient.Producer.Close()

		log.Println("Starting Producer")
		kafkaClient.WaitGroup.Add(1)
		go kafkaClient.Push()

		log.Println("Starting producer monitor thread")
		go kafkaClient.Monitor()
	}

	wg.Wait()

	return nil
}
