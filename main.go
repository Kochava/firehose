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

	consumerBrokers := config.srcBrokers
	producerBrokers := config.dstBrokers

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

	configClient := NewClient(config)

	transferChan := make(chan sarama.ProducerMessage, 100000)
	for partition := 0; partition < configClient.GetNumPartitions(); partition++ {

		partitionConsumer, err := consumer.ConsumePartition(config.topic, int32(partition), sarama.OffsetNewest)
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
