Firehose
======

Firehose is a Kafka transfer agent which can do real-time or historical transfers of a Topic from one set of Brokers to another. This is useful when you want the two clusters to remain independent and not use the built in replication process of Kafka.

### Requirements
* sarama
* go-metrics

## Install

Clone this repository then:

    $ git clone https://github.com/Kochava/firehose.git
    $ cd firehose
    $ glide install
    $ go install

#### Update Dependancies
    $ glide update

## Usage

firehose \[options\]

### Options

* ```--historical``` - Enable historical transfer rather than real-time

### Environment Variables

* ```SRC_BROKERS``` - A comma separated list of Source Brokers to pull from
* ```DST_BROKERS``` - A comma separated list of Destination Brokers to push to
* ```TOPIC``` - The Topic that will be transferred
* ```METRICS_LOG``` - (Optional) The path to place the metrics log file
  * Defaults to `/var/log/firehose/metrics.log`
* ```FIREHOSE_LOG``` - (Optional) The path to place the main firehose log file
  * Defaults to `/var/log/firehose/firehose.log`


## Contributing

### Grab the source and make a branch

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Make your changes
4. Add some tests
5. Commit your changes (`git commit -am 'Add some feature'`)
6. Push to the branch (`git push origin my-new-feature`)
7. Create new Pull Request


### Todo

* Code level testing
* Add a monitor that will reset real-time transfers if they get too far behind the newest offset
* Add a diff log for current offset vs newest to track drift for real-time transfer
* Add a flag for specifying the historical transfer window


#### Github
The Github version of this repo is a mirror of Master from our internal repo. This means that feature branches are not available here.
