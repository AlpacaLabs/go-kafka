package kafka

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

type GetReaderInput struct {
	Host    string
	Port    int
	GroupID string
	Topic   string
}

func GetReader(in GetReaderInput) (*kafka.Reader, func() error) {
	host := in.Host
	port := in.Port
	groupID := in.GroupID
	topic := in.Topic

	broker := fmt.Sprintf("%s:%d", host, port)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},

		// If GroupID is specified, then Partition should NOT be specified
		GroupID: groupID,

		Topic: topic,

		// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
		// polling the brokers and rebalancing if any partition changes happen to the topic.
		WatchPartitionChanges: true,
	})
	return r, r.Close
}
