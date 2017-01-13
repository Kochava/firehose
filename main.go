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
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
)

//NOTE :: Still using globals - blegh

//Conf is the Global config object
var Conf Config //Access as global for easy flag config

func init() {
	//Ensure config object has been created with empty value
	Conf = Config{}
}

func initLogging(c *cli.Context, conf *Config) {
	if conf.STDOutLogging {
		return // Essentially a noop to skip setting up the log file
	}

	logFile, err := os.Create(conf.LogFile)
	if err != nil {
		log.Printf("Unable to open log file: %s\n", err)
		log.Println("Reverting to stdout logging")
		return // log already goes to stdout so just early exit without calling log.SetOutput
	}

	log.SetOutput(logFile)
}

// Essentially main
func startFirehose(c *cli.Context, conf *Config) error {

	var wg sync.WaitGroup
	transferChan := make(chan sarama.ProducerMessage, 100000)

	consumerConcurrency, err := strconv.Atoi(conf.ConsumerConcurrency)
	if err != nil {
		log.Printf("startFirehose - Error converting consumer concurrency %v\n", err)
		consumerConcurrency = 4
	}

	for i := 0; i < consumerConcurrency; i++ {
		log.Println("Getting the Kafka consumer")
		consumer, cErr := GetKafkaConsumer(conf)
		if cErr != nil {
			log.Println("startFirehose - Unable to create the consumer")
			return cErr
		}

		log.Println("Starting error consumer")
		go GetConsumerErrors(consumer)
		defer consumer.Close()

		wg.Add(1)
		go PullFromTopic(consumer, transferChan, &wg)
	}

	producerConcurrency, err := strconv.Atoi(conf.ProducerConcurrency)
	if err != nil {
		log.Printf("startFirehose - Error converting producer concurrency %v\n", err)
		producerConcurrency = 4
	}

	for i := 0; i < producerConcurrency; i++ {
		log.Println("Getting the Kafka producer")
		producer, err := GetKafkaProducer(conf)
		if err != nil {
			log.Println("startFirehose - Unable to create the producer")
			return err
		}
		defer producer.Close()

		wg.Add(1)
		go PushToTopic(producer, transferChan, &wg)
	}

	wg.Add(1)
	go MonitorChan(transferChan, &wg)

	wg.Wait()

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "Kochava Kafka Transfer Agent"
	app.Usage = "An agent which consumes a topic from one set of brokers and publishes to another set of brokers"
	app.Flags = AppConfigFlags //defined in flags.go
	//Major, minor, patch version
	app.Version = "0.1.0"
	app.Action = func(c *cli.Context) error {
		initLogging(c, &Conf)

		if err := startFirehose(c, &Conf); err != nil {
			log.Fatal(err)
		}
		return nil
	}
	app.Run(os.Args)
}
