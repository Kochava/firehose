package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

// GetKafkaConsumer returns a new consumer
func GetKafkaConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	appMetricRegistry := metrics.NewRegistry()

	consumerMetricRegistry := metrics.NewPrefixedChildRegistry(appMetricRegistry, "consumer.")

	config.MetricRegistry = consumerMetricRegistry

	file, err := os.OpenFile("consumer.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}

	go metrics.Log(consumerMetricRegistry, 30*time.Second, log.New(file, "consumer: ", log.Lmicroseconds))

	// Create new consumer
	return sarama.NewConsumer(brokers, config)

}

// GetKafkaProducer returns a new consumer
func GetKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.ClientID = "firehose"

	appMetricRegistry := metrics.NewRegistry()

	producerMetricRegistry := metrics.NewPrefixedChildRegistry(appMetricRegistry, "producer.")

	config.MetricRegistry = producerMetricRegistry

	file, err := os.OpenFile("producer.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}

	go metrics.Log(producerMetricRegistry, 30*time.Second, log.New(file, "producer: ", log.Lmicroseconds))

	// Create new consumer
	return sarama.NewSyncProducer(brokers, config)

}

// PullFromTopic pulls messages from the topic partition
func PullFromTopic(consumer sarama.PartitionConsumer, producer chan<- *sarama.ProducerMessage, signals chan os.Signal, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		if len(signals) > 0 {
			fmt.Println("Consumer - Interrupt is detected - exiting")
			return
		}
		select {
		case err := <-consumer.Errors():
			fmt.Println(err)
			return
		case consumerMsg := <-consumer.Messages():
			producerMsg := &sarama.ProducerMessage{
				Topic:     consumerMsg.Topic,
				Partition: consumerMsg.Partition,
				Key:       sarama.StringEncoder(consumerMsg.Key),
				Value:     sarama.StringEncoder(consumerMsg.Value),
			}
			producer <- producerMsg
		}
	}
}

// PushToTopic pushes messages to topic
func PushToTopic(producer sarama.SyncProducer, consumer <-chan *sarama.ProducerMessage, signals chan os.Signal, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		if len(signals) > 0 {
			fmt.Println("Producer - Interrupt is detected - exiting")
			return
		}
		select {
		case consumerMsg := <-consumer:
			_, _, err := producer.SendMessage(consumerMsg)
			if err != nil {
				log.Fatalln("Failed to produce message to kafka cluster.")
				return
			}

			//fmt.Printf("Produced message to partition %d with offset %d\n", partition, offset)
		}
	}
}

// TestConsumer testing
func TestConsumer(partition int, producer chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	producer <- partition
}

// TestProducer testing
func TestProducer(partition int, consumer <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	cPartition := <-consumer
	fmt.Println("Producer ", partition, " Consumer ", cPartition)
}

// CloseProducer Closes the producer
func CloseProducer(producer sarama.SyncProducer) {
	fmt.Println("Trying to close Producer")
	if err := producer.Close(); err != nil {
		// Should not reach here
		panic(err)
	}
}

// CloseConsumer closes the consumer
func CloseConsumer(consumer sarama.Consumer) {
	fmt.Println("Trying to close consumer")
	if err := consumer.Close(); err != nil {
		// Should not reach here
		panic(err)
	}
}
