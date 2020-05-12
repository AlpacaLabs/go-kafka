package kafka

import (
	"context"
	"fmt"
)

type ProcessKafkaMessagesInput struct {
	Brokers []string
	GroupID string
	Topic   string
}

func ProcessKafkaMessages(ctx context.Context, in ProcessKafkaMessagesInput, fn func(context.Context, Message) error) error {
	reader, closer := getReader(getReaderInput{
		brokers: in.Brokers,
		groupID: in.GroupID,
		topic:   in.Topic,
	})
	defer closer()

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

		if err := reader.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("failed to commit kafka offset: %w", err)
		}
	}
}
