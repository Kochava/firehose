Firehose [![Build Status](https://travis-ci.org/Kochava/firehose.svg?branch=master)](https://travis-ci.org/Kochava/firehose) [![Coverage Status](https://coveralls.io/repos/github/Kochava/firehose/badge.svg?branch=master)](https://coveralls.io/github/Kochava/firehose?branch=master)
======

Firehose is a Kafka transfer agent which can do real-time of a Topic from one set of Brokers to another. This is useful when you want the two clusters to remain independent and not use the built in replication process of Kafka.

### Requirements
* Docker (for local testing)

## Install

Clone this repository then:

```
$ git clone --recursive https://github.com/Kochava/firehose.git
$ cd firehose
$ go build ./cmd/firehose
```

#### Update Dependancies
```
$ git submodule update --recursive --remote
```

## Usage

```
NAME:
   Kochava Kafka Transfer Agent - An agent which consumes a topic from one set of brokers and publishes to another set of brokers

USAGE:
   firehose [global options] command [command options] [arguments...]

VERSION:
   0.3.3

GLOBAL OPTIONS:
   --src-zookeepers value        Comma delimited list of zookeeper nodes to connect to for the source brokers [$FIREHOSE_SRC_ZOOKEEPERS]
   --dst-zookeepers value        Comma delimited list of zookeeper nodes to connect to for the destination brokers [$FIREHOSE_DST_ZOOKEEPERS]
   --topic value                 Topic to transfer (default: "firehose") [$FIREHOSE_TOPIC]
   --cg-name-suffix value        Suffix to use for the consumer group name in the format <topic>_<suffix> (default: "firehose") [$FIREHOSE_CG_NAME_SUFFIX]
   --buffer-size value           The number of messages to hold in memory at once (default: 10000) [$FIREHOSE_BUFFER_SIZE]
   --max-errors value            The maximum number of errors to allow the kafka to experience before quitting (default: 10) [$FIREHOSE_MAX_ERROR]
   --max-retry value             The maximum number of times to retry sending a message to the destination cluster (default: 5) [$FIREHOSE_MAX_RETRY]
   --batch-size value            The number of messages to batch together when sending to the destination cluster (default: 500) [$FIREHOSE_BATCH_SIZE]
   --flush-interval value        The interval (in ms) to flush messages that haven't been sent in a batch yet (default: 10000) [$FIREHOSE_FLUSH_INTERVAL]
   --reset-offset                Resets the offset in the consumer group to real-time before starting [$FIREHOSE_RESET_OFFSET]
   --influx-address value        Influx address (default: "http://localhost:8086") [$FIREHOSE_INFLUX_ADDRESS]
   --influx-user value           Influx user name (default: "firehose") [$FIREHOSE_INFLUX_USER]
   --influx-pass value           Influx password (default: "firehose") [$FIREHOSE_INFLUX_PASS]
   --influx-db value             Influx database name (default: "firehose") [$FIREHOSE_INFLUX_DB]
   --consumer-concurrency value  Number of consumer threads to run (default: 4) [$FIREHOSE_CONSUMER_CONCURRENCY]
   --producer-concurrency value  Number of producer threads to run (default: 4) [$FIREHOSE_PRODUCER_CONCURRENCY]
   --log-file value              Main log file to save to (default: "/var/log/firehose/firehose.log") [$FIREHOSE_LOG_FILE]
   --stdout-logging              Override logging settings to log to STDOUT [$FIREHOSE_STDOUT_LOGGING]
   --help, -h                    show help
   --version, -v                 print the version
```

### Getting Started

Once you have the repo downloaded the following is all that's needed to get started testing locally. First grab your docker machine IP address, then in `Docker/test-compose.yml` update the `KAFKA_ADVERTISED_HOST_NAME` EnvVar to use this.

```
$ go build ./cmd/firehose
$ docker-compose -f Docker/test-compose.yml up -d
$ ./firehose --src-zookeepers="<docker-machine-ip>:2181" --dst-zookeepers="<docker-machine-ip>:2182" --stdout-logging
```

If you want to scale up the kafka clusters you can do so
```
$ docker-compose -f Docker/test-compose.yml scale kafka_src=3
```

Once up and running you can access Grafana at http://localhost:3000 with the default username and password. Next just add a new datasource, choosing `InfluxDB` and method `proxy`. For the location use `http://172.18.0.2:8086` with the default Influx db of `firehose` and user:pass being `firehose:firehose`. Once that's setup you can import the simple dashboard preprepared under `dist/dashboard.json`.

#### Cloud Building

You can leverage Google Cloud Building in order to build the binary and generate a docker image.

```
$ PROJECT_ID=your-project-id BUILD_VERSION=v1 ./cloudbuild.sh
```


## Contributing

### Grab the source and make a branch

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Make your changes
4. Add some tests
5. Commit your changes (`git commit -am 'Add some feature'`)
6. Push to the branch (`git push origin my-new-feature`)
7. Create new Pull Request

### TODO

With the new refactor a lot of work was put into performance, because of that the historical transfer aspect was essentially scrapped in this release. It will be added back in with the next release.

#### Github
The Github version of this repo is a mirror of Master from our internal repo. This means that feature branches are not available here.
