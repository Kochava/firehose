package main

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

func main() {

	stagingBrokers := os.Getenv("STAGING_BROKERS")
	if stagingBrokers == "" {
		log.Fatalln("No staging brokers supplied. Please set STAGING_BROKERS")
	}

	consumerBrokers := []string{"kafka-broker-01.ana.kochava.com:9092", "kafka-broker-02.ana.kochava.com:9092", "kafka-broker-03.ana.kochava.com:9092"}
	producerBrokers := strings.Split(stagingBrokers, ",")

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
		log.Fatalln("Unable to create consumer", err)
	}

	log.Println("Getting the Kafka producer")
	producer, err := GetKafkaProducer(producerBrokers, metricsFile)
	if err != nil {
		log.Fatalln("Unable to create producer", err)
	}

	defer CloseConsumer(&consumer)
	defer CloseProducer(&producer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	var wg sync.WaitGroup

	topic := "events"

	transferChan := make(chan sarama.ProducerMessage, 100000)
	for partition := 0; partition < 24; partition++ {

		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			log.Fatalln("Unable to create partition consumer", err)
		}

		wg.Add(1)

		go PullFromTopic(partitionConsumer, transferChan, signals, &wg)
		log.Println("Started consumer for partition ", partition)

		wg.Add(2)
		go PushToTopic(producer, transferChan, signals, &wg)
		log.Println("Started producer")
		go PushToTopic(producer, transferChan, signals, &wg)
		log.Println("Started producer")
	}

	go MonitorChan(transferChan, signals, &wg)

	wg.Wait()

	log.Println("All threads done, closing clients and ending")
}
