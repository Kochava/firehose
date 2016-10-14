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

	config.MetricRegistry = appMetricRegistry

	go metrics.Log(consumerMetricRegistry, 30*time.Second, log.New(os.Stdout, "consumer: ", log.Lmicroseconds))

	// Create new consumer
	return sarama.NewConsumer(brokers, config)

}

// GetKafkaProducer returns a new consumer
func GetKafkaProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5

	config.Producer.RequiredAcks = sarama.WaitForLocal

	appMetricRegistry := metrics.NewRegistry()

	consumerMetricRegistry := metrics.NewPrefixedChildRegistry(appMetricRegistry, "consumer.")

	config.MetricRegistry = appMetricRegistry

	go metrics.Log(consumerMetricRegistry, 30*time.Second, log.New(os.Stdout, "consumer: ", log.Lmicroseconds))

	// Create new consumer
	return sarama.NewConsumer(brokers, config)

}

// PullFromTopic pulls messages from the topic partition
func PullFromTopic(consumer sarama.PartitionConsumer, producer chan<- string, signals chan os.Signal, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		if len(signals) > 0 {
			fmt.Println("Interrupt is detected")
			return
		}
		select {
		case err := <-consumer.Errors():
			fmt.Println(err)
			return
		case <-consumer.Messages():
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
