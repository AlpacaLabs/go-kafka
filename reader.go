package kafka

import (
	"github.com/segmentio/kafka-go"
)

type getReaderInput struct {
	brokers []string
	groupID string
	topic   string
}

func getReader(in getReaderInput) (reader *kafka.Reader, closer func() error) {
	brokers := in.brokers
	groupID := in.groupID
	topic := in.topic

	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,

		// If GroupID is specified, then Partition should NOT be specified
		GroupID: groupID,

		Topic: topic,

		// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
		// polling the brokers and rebalancing if any partition changes happen to the topic.
		WatchPartitionChanges: true,
	})
	return reader, reader.Close
}
