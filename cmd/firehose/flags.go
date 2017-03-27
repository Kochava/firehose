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

import "github.com/urfave/cli"

//AppConfigFlags defines all of the Application Config Flags
var AppConfigFlags = []cli.Flag{
	cli.StringSliceFlag{
		Name:   "src-zookeepers",
		Value:  &cli.StringSlice{}, //Default value
		Usage:  "Comma delimited list of zookeeper nodes to connect to for the source brokers",
		EnvVar: "SRC_ZOOKEEPERS",
	},
	cli.StringSliceFlag{
		Name:   "dst-zookeepers",
		Value:  &cli.StringSlice{}, //Default value
		Usage:  "Comma delimited list of zookeeper nodes to connect to for the destination brokers",
		EnvVar: "DST_ZOOKEEPERS",
	},
	cli.StringFlag{
		Name:        "src-kafka-broker",
		Value:       "localhost:9092", //Default value
		Usage:       "One of the source brokers to connect to",
		EnvVar:      "SRC_KAFKA_BROKER",
		Destination: &Conf.SourceKafkaBroker,
	},
	cli.StringFlag{
		Name:        "topic",
		Value:       "firehose", //Default value
		Usage:       "Topic to transfer",
		EnvVar:      "TOPIC",
		Destination: &Conf.Topic,
	},
	cli.StringFlag{
		Name:        "log-file",
		Value:       "/var/log/firehose/firehose.log", //Default value
		Usage:       "Main log file to save to",
		EnvVar:      "FIREHOSE_LOG_FILE",
		Destination: &Conf.LogFile,
	},
	cli.IntFlag{
		Name:        "consumer-concurrency",
		Value:       4, //Default value
		Usage:       "Number of consumer threads to run",
		EnvVar:      "CONSUMER_CONCURRENCY",
		Destination: &Conf.ConsumerConcurrency,
	},
	cli.IntFlag{
		Name:        "producer-concurrency",
		Value:       4, //Default value
		Usage:       "Number of producer threads to run",
		EnvVar:      "PRODUCER_CONCURRENCY",
		Destination: &Conf.ProducerConcurrency,
	},
	cli.BoolFlag{
		Name:        "stdout-logging",
		Usage:       "Override logging settings to log to STDOUT",
		EnvVar:      "PROXCON_STDOUT_LOGGING",
		Destination: &Conf.STDOutLogging,
	},
}
