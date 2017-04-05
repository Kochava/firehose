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

package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Kochava/firehose/cmd/internal/influxlogger"
	"github.com/Kochava/firehose/cmd/internal/kafka"
	"github.com/urfave/cli"
)

// Essentially main
func startFirehose(c *cli.Context, conf *Config) error {

	signals := make(chan os.Signal, 1)
	shutdown := make(chan struct{})                        // used to broadcast the intent to shutdown processing
	transferChan := kafka.GetTransferChan(conf.BufferSize) // used to pass messages between the consumer threads and the producer threads
	var wg sync.WaitGroup

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM) // notify on sig int and sig term

	// Connect to influx
	influxClient, err := influxlogger.ConnectToInflux(conf.InfluxAddr, conf.InfluxUser, conf.InfluxPass)
	if err != nil {
		return err
	}
	influxAccessor, err := influxlogger.NewInfluxD(influxClient, conf.InfluxDB, "s")
	if err != nil {
		return err
	}

	logSystemStats := true // this will log relevant process stats to influx
	// Function that starts writing batch points on a tick interval
	go influxAccessor.PointTickWriter(logSystemStats) //(continuous)
	log.Println("startFirehose - Started Influx")

	for i := 0; i < conf.ConsumerConcurrency; i++ {
		var err error
		kafkaClient, err := kafka.InitKafka(conf.Topic, conf.SourceZookeepers, conf.BufferSize, conf.MaxErrors, influxAccessor, shutdown, &wg)
		if err != nil {
			log.Println("startFirehose - Unable to create the kafka consumer client")
			return err
		}

		log.Println("Initializing the Kafka consumer")
		err = kafkaClient.InitConsumer(transferChan, conf.ResetOffset)
		if err != nil {
			log.Println("startFirehose - Unable to create the consumer")
			return err
		}

		log.Println("Starting error consumer")
		go kafkaClient.GetConsumerErrors()
		defer kafkaClient.Close()

		log.Println("Starting consumer")
		kafkaClient.WaitGroup.Add(1)
		go kafkaClient.Pull()

		log.Println("Starting consumer monitor thread")
		go kafkaClient.Monitor()
	}

	for i := 0; i < conf.ProducerConcurrency; i++ {
		var err error
		kafkaClient, err := kafka.InitKafka(conf.Topic, conf.DestinationZookeepers, conf.BufferSize, conf.MaxErrors, influxAccessor, shutdown, &wg)
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
		defer kafkaClient.Close()

		log.Println("Starting Producer")
		kafkaClient.WaitGroup.Add(1)
		go kafkaClient.Push()

		// A dedicated thread for consuming successes
		// this is needed because input to the producer and pulling from successes happens at different rates
		// there's no easy way to unblock the input to the async producer if pulling from successes falls behind
		// and they're done in the same thread.
		// This allows RPSTicker to unblock Push from a different thread
		go kafkaClient.RPSTicker()

		log.Println("Starting producer monitor thread")
		go kafkaClient.Monitor()
	}

	defer func() {
		log.Println("Waiting for all threads to exit.")
		wg.Wait()
	}()

	for {
		select {
		case <-shutdown:
			return nil
		case <-signals:
			close(shutdown) // signal to all threads to shutdown
			log.Println("startFirehose - received signal, shutting down.")
			return nil
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}
