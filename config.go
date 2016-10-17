package main

import (
	"flag"
	"fmt"
)

// Config Contains all the values needed for firehose
type Config struct {
	srcBrokers    []string
	dstBrokers    []string
	historical    bool
	numPartitions int
	topic         string
	metricsLog    string
	firehoseLog   string
}

// Init initalize all the configuration data for firehose
func Init() {
	historical := flag.Bool("historical", false, "Enable historical transfer")

	if *historical {
		fmt.Println("historical transfer enabled")
	}
}
