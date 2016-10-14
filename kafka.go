package main

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

// GetKafkaConsumer returns a new consumer
func GetKafkaConsumer(brokers []string, file *os.File) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	appMetricRegistry := metrics.NewRegistry()

	consumerMetricRegistry := metrics.NewPrefixedChildRegistry(appMetricRegistry, "consumer.")

	config.MetricRegistry = consumerMetricRegistry

	go metrics.Log(consumerMetricRegistry, 30*time.Second, log.New(file, "consumer: ", log.Lmicroseconds))

	// Create new consumer
	return sarama.NewConsumer(brokers, config)

}

// GetKafkaProducer returns a new consumer
func GetKafkaProducer(brokers []string, file *os.File) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.ClientID = "firehose"

	appMetricRegistry := metrics.NewRegistry()

	producerMetricRegistry := metrics.NewPrefixedChildRegistry(appMetricRegistry, "producer.")

	config.MetricRegistry = producerMetricRegistry

	go metrics.Log(producerMetricRegistry, 30*time.Second, log.New(file, "producer: ", log.Lmicroseconds))

	// Create new consumer
	return sarama.NewSyncProducer(brokers, config)

}

// PullFromTopic pulls messages from the topic partition
func PullFromTopic(consumer sarama.PartitionConsumer, producer chan<- *sarama.ProducerMessage, signals chan os.Signal, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		if len(signals) > 0 {
			log.Println("Consumer - Interrupt is detected - exiting")
			return
		}
		select {
		case err := <-consumer.Errors():
			log.Println(err)
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
			log.Println("Producer - Interrupt is detected - exiting")
			return
		}
		select {
		case consumerMsg := <-consumer:
			_, _, err := producer.SendMessage(consumerMsg)
			if err != nil {
				log.Fatalln("Failed to produce message to kafka cluster.")
				return
			}

			//log.Printf("Produced message to partition %d with offset %d\n", partition, offset)
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
	log.Println("Producer ", partition, " Consumer ", cPartition)
}

// CloseProducer Closes the producer
func CloseProducer(producer sarama.SyncProducer) {
	log.Println("Trying to close Producer")
	if err := producer.Close(); err != nil {
		// Should not reach here
		panic(err)
	}
}

// CloseConsumer closes the consumer
func CloseConsumer(consumer sarama.Consumer) {
	log.Println("Trying to close consumer")
	if err := consumer.Close(); err != nil {
		// Should not reach here
		panic(err)
	}
}
