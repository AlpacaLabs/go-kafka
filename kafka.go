package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type ProcessKafkaMessagesInput struct {
	Brokers        []string
	GroupID        string
	Topic          string
	CommitInterval time.Duration
}

func ProcessKafkaMessages(ctx context.Context, in ProcessKafkaMessagesInput, fn func(context.Context, Message) error) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: in.Brokers,

		// If GroupID is specified, then Partition should NOT be specified
		GroupID: in.GroupID,

		Topic: in.Topic,

		// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
		// polling the brokers and rebalancing if any partition changes happen to the topic.
		WatchPartitionChanges: true,

		// CommitInterval indicates the interval at which offsets are committed to
		// the broker.
		CommitInterval: in.CommitInterval,
	})
	defer reader.Close()

	for {
		// FetchMessage does not commit offsets automatically when using consumer groups.
		// The method returns io.EOF to indicate that the reader has been closed.
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to read kafka message: %w", err)
		}

		if err := fn(ctx, fromMessage(m)); err != nil {
			return fmt.Errorf("failed to process kafka message: %w", err)
		}

		if in.CommitInterval == 0 {
			// A commit represents the instruction of publishing an update of the last
			// offset read by a program for a topic and partition.
			if err := reader.CommitMessages(ctx, m); err != nil {
				return fmt.Errorf("failed to commit kafka offset: %w", err)
			}
		}
	}
}
