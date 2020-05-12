# go-kafka


This library provides an opinionated set of functions for reading Kafka records
 using [segmentio/kafka-go](https://github.com/segmentio/kafka-go).

We use Segment's `Reader` abstraction to form a consumer group, 
which ensures that only one Kubernetes pod per service consumes a message.