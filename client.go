package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

// custom offset predefined
const (
	lastOneHours    float64 = 0.01042
	lastFourHours   float64 = 0.041647
	lastTwelveHours float64 = 0.125
	lastOneDays     float64 = 0.25
	lastTwoDays     float64 = 0.5
	lastThreeDays   float64 = 0.75
	lastFourDays    float64 = 1.0
)

// CustomClient Adds an additional custom functionality surrounding offsets to the sarama client
type CustomClient struct {
	sarama.Client

	topic     string
	partition int32
}

// NewClient creates a new custom client
func (client CustomClient) NewClient(srcBrokers []string, topic string, partition int32) CustomClient {
	c, err := sarama.NewClient(srcBrokers, nil)
	if err != nil {
		log.Println("ERROR:", err)
	}

	return CustomClient{c, topic, partition}
}

// GetNumPartitions gets the number of partitions for the topic
func (client CustomClient) GetNumPartitions() int {
	var list []int32
	list, err := client.Partitions(client.topic)
	if err != nil {
		log.Fatalln("Unable to get number of partitions: ", err)
	}
	return len(list)
}

// GetCustomOffset takes a fraction of the total data stored in kafka and gets a relative offset
func (client CustomClient) GetCustomOffset(fraction float64) int {

	newestOffset, err := client.GetOffset(client.topic, client.partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Println("ERROR:", err)
	}

	oldestOffset, err := client.GetOffset(client.topic, client.partition, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("ERROR:", err)
	}

	diff := newestOffset - oldestOffset

	fractionalOffset := float64(diff) * fraction

	return int(fractionalOffset)
}
