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
				return
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
