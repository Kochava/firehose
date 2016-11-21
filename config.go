// Copyright 2016 Kochava
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"log"
	"os"
	"strings"
)

const (
	defaultMetricsLog  string = "/var/log/firehose/metrics.log"
	defaultFirehoseLog string = "/var/log/firehose/firehose.log"
)

// Config Contains all the values needed for firehose
type Config struct {
	srcBrokers  []string
	dstBrokers  []string
	topic       string
	metricsLog  string
	firehoseLog string
	historical  bool
}

// InitConfig initialize the config object
func InitConfig() Config {
	return Config{}
}

// GetConfig initialize all the configuration data for firehose
func (config *Config) GetConfig() {

	historical := flag.Bool("historical", false, "Enable historical transfer")
	flag.Parse()

	if *historical {
		log.Println("historical transfer enabled")
	}
	config.historical = *historical

	sBrokers := os.Getenv("SRC_BROKERS")
	if sBrokers == "" {
		log.Fatalln("No source brokers supplied. Please set env var SRC_BROKERS")
	}
	config.srcBrokers = strings.Split(sBrokers, ",")

	dBrokers := os.Getenv("DST_BROKERS")
	if dBrokers == "" {
		log.Fatalln("No destination brokers supplied. Please set env var DST_BROKERS")
	}
	config.dstBrokers = strings.Split(dBrokers, ",")

	topic := os.Getenv("TOPIC")
	if topic == "" {
		log.Fatalln("No topic supplied. Please set env var TOPIC")
	}
	config.topic = topic

	metricsLog := os.Getenv("METRICS_LOG")
	if metricsLog == "" {
		log.Println("No metrics log location supplied. Use env METRICS_LOG to set. Using default ", defaultMetricsLog)
		config.metricsLog = defaultMetricsLog
	} else {
		config.metricsLog = metricsLog
	}

	firehoseLog := os.Getenv("FIREHOSE_LOG")
	if firehoseLog == "" {
		log.Println("No firehoseLog log location supplied. Use env FIREHOSE_LOG to set. Using default ", defaultFirehoseLog)
		config.firehoseLog = defaultFirehoseLog
	} else {
		config.firehoseLog = firehoseLog
	}
}
