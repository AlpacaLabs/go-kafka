package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	ErrNilProcessFunc = errors.New("must provide a function to process Kafka messages")
	ErrNilBrokers     = errors.New("must specify Kafka brokers to connect to")
	ErrEmptyGroupID   = errors.New("must specify Kafka group ID; this is often the service's name")
	ErrEmptyTopic     = errors.New("must specify Kafka topic to connect to")
)

const (
	defaultCommitInterval = time.Second * 10
)

type ProcessKafkaMessagesInput struct {
	ProcessFunc    ProcessFunc
	Brokers        []string
	GroupID        string
	Topic          string
	CommitInterval time.Duration
}

type ProcessFunc func(context.Context, Message)

func ProcessKafkaMessages(ctx context.Context, in ProcessKafkaMessagesInput) error {
	// Perform validation of all fields
	if in.ProcessFunc == nil {
		return ErrNilProcessFunc
	}

	if in.Brokers == nil {
		return ErrNilBrokers
	}

	if in.GroupID == "" {
		return ErrEmptyGroupID
	}

	if in.Topic == "" {
		return ErrEmptyTopic
	}

	fn := in.ProcessFunc

	// For performance, we won't be calling reader.CommitMessages.
	//
	// The downside is that a message might get processed / replayed
	// in the event the app crashes before it commits the fact that it
	// read that particular offset for that topic/partition.
	//
	// The upside is that the app won't have to block until the offset
	// is committed and can freely process the next message.
	//
	// This might be considered a premature optimization, but if the only
	// downside is messages can get replayed, that seems tolerable; message
	// processing is supposed to be idempotent anyway.
	commitInterval := in.CommitInterval
	if commitInterval == 0 {
		commitInterval = defaultCommitInterval
	}

	// Create reader
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
		//
		// A commit represents the instruction of publishing an update of the last
		// offset read by a program for a topic and partition.
		CommitInterval: commitInterval,
	})
	defer reader.Close()

	for {
		// FetchMessage does not commit offsets automatically when using consumer groups.
		// The method returns io.EOF to indicate that the reader has been closed.
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to read kafka message: %w", err)
		}

		// Process message
		go fn(ctx, fromMessage(m))
	}
}
