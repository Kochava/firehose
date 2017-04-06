// Copyright 2017 Kochava
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
		EnvVar: "FIREHOSE_SRC_ZOOKEEPERS",
	},
	cli.StringSliceFlag{
		Name:   "dst-zookeepers",
		Value:  &cli.StringSlice{}, //Default value
		Usage:  "Comma delimited list of zookeeper nodes to connect to for the destination brokers",
		EnvVar: "FIREHOSE_DST_ZOOKEEPERS",
	},
	cli.StringFlag{
		Name:        "topic",
		Value:       "firehose", //Default value
		Usage:       "Topic to transfer",
		EnvVar:      "FIREHOSE_TOPIC",
		Destination: &Conf.Topic,
	},
	cli.IntFlag{
		Name:        "buffer-size",
		Value:       10000, //Default value
		Usage:       "The number of messages to hold in memory at once",
		EnvVar:      "FIREHOSE_BUFFER_SIZE",
		Destination: &Conf.BufferSize,
	},
	cli.IntFlag{
		Name:        "max-errors",
		Value:       10, //Default value
		Usage:       "The maximum number of errors to allow the kafka to experience before quitting",
		EnvVar:      "FIREHOSE_MAX_ERROR",
		Destination: &Conf.MaxErrors,
	},
	cli.IntFlag{
		Name:        "max-retry",
		Value:       5, //Default value
		Usage:       "The maximum number of times to retry sending a message to the destination cluster",
		EnvVar:      "FIREHOSE_MAX_RETRY",
		Destination: &Conf.MaxRetry,
	},
	cli.IntFlag{
		Name:        "batch-size",
		Value:       500, //Default value
		Usage:       "The number of messages to batch together when sending to the destination cluster",
		EnvVar:      "FIREHOSE_BATCH_SIZE",
		Destination: &Conf.BatchSize,
	},
	cli.IntFlag{
		Name:        "flush-interval",
		Value:       10000, //Default value
		Usage:       "The interval (in ms) to flush messages that haven't been sent in a batch yet",
		EnvVar:      "FIREHOSE_FLUSH_INTERVAL",
		Destination: &Conf.FlushInterval,
	},
	cli.BoolFlag{
		Name:        "reset-offset",
		Usage:       "Resets the offset in the consumer group to real-time before starting",
		EnvVar:      "FIREHOSE_RESET_OFFSET",
		Destination: &Conf.ResetOffset,
	},
	cli.StringFlag{
		Name:        "influx-address",
		Value:       "http://localhost:8086", //Default value,
		Usage:       "Influx address",
		EnvVar:      "FIREHOSE_INFLUX_ADDRESS",
		Destination: &Conf.InfluxAddr,
	},
	cli.StringFlag{
		Name:        "influx-user",
		Value:       "firehose", //Default value,
		Usage:       "Influx user name",
		EnvVar:      "FIREHOSE_INFLUX_USER",
		Destination: &Conf.InfluxUser,
	},
	cli.StringFlag{
		Name:        "influx-pass",
		Value:       "firehose", //Default value,
		Usage:       "Influx password",
		EnvVar:      "FIREHOSE_INFLUX_PASS",
		Destination: &Conf.InfluxPass,
	},
	cli.StringFlag{
		Name:        "influx-db",
		Value:       "firehose", //Default value,
		Usage:       "Influx database name",
		EnvVar:      "FIREHOSE_INFLUX_DB",
		Destination: &Conf.InfluxDB,
	},
	cli.IntFlag{
		Name:        "consumer-concurrency",
		Value:       4, //Default value
		Usage:       "Number of consumer threads to run",
		EnvVar:      "FIREHOSE_CONSUMER_CONCURRENCY",
		Destination: &Conf.ConsumerConcurrency,
	},
	cli.IntFlag{
		Name:        "producer-concurrency",
		Value:       4, //Default value
		Usage:       "Number of producer threads to run",
		EnvVar:      "FIREHOSE_PRODUCER_CONCURRENCY",
		Destination: &Conf.ProducerConcurrency,
	},
	cli.StringFlag{
		Name:        "log-file",
		Value:       "/var/log/firehose/firehose.log", //Default value
		Usage:       "Main log file to save to",
		EnvVar:      "FIREHOSE_LOG_FILE",
		Destination: &Conf.LogFile,
	},
	cli.BoolFlag{
		Name:        "stdout-logging",
		Usage:       "Override logging settings to log to STDOUT",
		EnvVar:      "FIREHOSE_STDOUT_LOGGING",
		Destination: &Conf.STDOutLogging,
	},
}
