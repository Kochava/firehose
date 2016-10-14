package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {

	consumerBrokers := []string{"kafka-broker-01.ana.kochava.com:9092", "kafka-broker-02.ana.kochava.com:9092", "kafka-broker-03.ana.kochava.com:9092"}
	producerBrokers := []string{"104.196.140.55:9092", "104.196.0.83:9092", "104.196.35.215:9092"}

	fmt.Println("Getting the Kafka consumer")
	consumer, err := GetKafkaConsumer(consumerBrokers)
	if err != nil {
		fmt.Println("Unable to create consumer", err)
	}

	fmt.Println("Getting the Kafka producer")
	producer, err := GetKafkaProducer(producerBrokers)
	if err != nil {
		fmt.Println("Unable to create producer", err)
	}

	defer CloseConsumer(consumer)
	defer CloseProducer(producer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup

	topic := "events"

	for partition := 0; partition < 24; partition++ {
		transferChan := make(chan *sarama.ProducerMessage, 100000)

		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}

		wg.Add(2)

		go PullFromTopic(partitionConsumer, transferChan, signals, &wg)
		fmt.Println("Started consumer for partition ", partition)

		go PushToTopic(producer, transferChan, signals, &wg)
		fmt.Println("Started producer for partition ", partition)
	}

	wg.Wait()
}
