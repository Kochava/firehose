package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {

	brokers := []string{"kafka-broker-01.ana.kochava.com:9092", "kafka-broker-02.ana.kochava.com:9092", "kafka-broker-03.ana.kochava.com:9092"}

	fmt.Println("Getting the Kafka consumer")
	consumer, err := GetKafkaConsumer(brokers)
	if err != nil {
		fmt.Println("Unable to create consumer", err)
	}

	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup

	topic := "events"

	for partition := 0; partition < 24; partition++ {
		producerChan := make(chan string)

		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}

		wg.Add(1)

		go PullFromTopic(partitionConsumer, producerChan, signals, &wg)
		fmt.Println("Started consumer for partition ", partition)

	}

	wg.Wait()
}
