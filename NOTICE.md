Firehose transfers a single Topic from one cluster of Brokers to another
Copyright (C) 2017 Kochava

Firehose makes use of several third-party OSS libraries


# Shopify/sarama

Sarama is used as the main Framework for interacting with Kafka. Firehose uses both its low level API and high level API

https://github.com/Shopify/sarama


# urfave/cli

Urfave/cli is used to easily handle all of the configuration management and argument parsing

https://github.com/urfave/cli


# wvanbergen/kafka

wvanbergen/kafka is used as a wrapper for sarama which introduces the ability to use zookeeper based consumergroups

https://github.com/wvanbergen/kafka


# wvanbergen/kazoo-go

wvanbergen/kazoo-go is actually a dependency of wvanbergen/kafka but is also used in tandem to manually grab current offsets of the consumergroup

https://github.com/wvanbergen/kazoo-go


# influxdata/influxdb

influxdata/influxdb is used to report runtime metrics for firehose

https://github.com/influxdata/influxdb
