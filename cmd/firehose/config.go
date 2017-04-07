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

// Config Contains all the values needed for firehose
type Config struct {
	// Kafka stuff
	SourceZookeepers      []string // Zookeepers for the source broker
	DestinationZookeepers []string // Zookeepers for the destination broker
	Topic                 string   // the topic to transfer
	CGNameSuffix          string   // the suffix to use to build the consumer group name
	ConsumerConcurrency   int      // number of consumer threads
	ProducerConcurrency   int      // number of producer threads
	BufferSize            int      // the number of messages to hold in memory at once
	MaxErrors             int      // max number of errors to allow the producer
	MaxRetry              int      // the maximum number of times to retry sending a message to the dst cluster
	BatchSize             int      // the batch size to use for the producer
	FlushInterval         int      // the interval (in ms) to flush messages which haven't been batched
	ResetOffset           bool     // reset the consumer group offset

	// Influx stuff
	InfluxAddr string
	InfluxDB   string
	InfluxUser string
	InfluxPass string

	LogFile       string
	STDOutLogging bool
	Version       string
}
