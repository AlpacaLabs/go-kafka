package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func ProcessKafkaMessages(ctx context.Context, reader *kafka.Reader, fn func(context.Context, kafka.Message) error) error {
	for {
		// FetchMessage does not commit offsets automatically when using consumer groups.
		// The method returns io.EOF to indicate that the reader has been closed.
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to read kafka message: %w", err)
		}

		if err := fn(ctx, m); err != nil {
			return fmt.Errorf("failed to process kafka message: %w", err)
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("failed to commit kafka offset: %w", err)
		}
	}
}
