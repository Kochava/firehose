package main

import (
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {

	consumerBrokers := []string{"kafka-broker-01.ana.kochava.com:9092", "kafka-broker-02.ana.kochava.com:9092", "kafka-broker-03.ana.kochava.com:9092"}
	producerBrokers := []string{"analytics-kafka-v000-9lzw:9092", "analytics-kafka-v000-u40u:9092", "analytics-kafka-v000-wrdq:9092"}

	logFile, err := os.OpenFile("/var/log/firehose/firehose.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	metricsFile, err := os.OpenFile("/var/log/firehose/metrics.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}
	defer metricsFile.Close()

	log.Println("Getting the Kafka consumer")
	consumer, err := GetKafkaConsumer(consumerBrokers, metricsFile)
	if err != nil {
		log.Println("Unable to create consumer", err)
	}

	log.Println("Getting the Kafka producer")
	producer, err := GetKafkaProducer(producerBrokers, metricsFile)
	if err != nil {
		log.Println("Unable to create producer", err)
	}

	defer CloseConsumer(consumer)
	defer CloseProducer(producer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup

	topic := "events"

	for partition := 0; partition < 24; partition++ {
		transferChan := make(chan sarama.ProducerMessage, 10000000)

		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}

		wg.Add(2)

		go PullFromTopic(partitionConsumer, transferChan, signals, &wg)
		log.Println("Started consumer for partition ", partition)

		go PushToTopic(producer, transferChan, signals, &wg)
		log.Println("Started producer for partition ", partition)
	}

	wg.Wait()
}
