Firehose
======

Firehose is a Kafka transfer agent which can do real-time or historical transfers of a Topic from one set of Brokers to another. This is useful when you want the two clusters to remain independent and not use the built in replication process of Kafka.

### Requirements
* sarama
* go-metrics


Installing deps with glide  
`glide install`

Updating deps with glide  
`glide update`

### Todo

* Add more documentation
* Add test files
* Add a monitor that will reset real-time transfers if they get too far behind the newest offset

#### Github
The Github version of this repo is a mirror of Master from our internal repo. This means that feature branches are not available here.
