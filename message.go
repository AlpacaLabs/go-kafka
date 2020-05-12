package kafka

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/segmentio/kafka-go"
)

type Message struct {
	Headers map[string][]byte
	Payload []byte
}

func fromMessage(in kafka.Message) Message {
	headers := make(map[string][]byte)
	for _, h := range in.Headers {
		headers[h.Key] = h.Value
	}

	return Message{
		Headers: headers,
		Payload: in.Value,
	}
}

func (m Message) GetString(key string) (string, error) {
	if v, ok := m.Headers[key]; ok {
		return string(v), nil
	}
	return "", fmt.Errorf("no message header found for key: %s", key)
}

func (m Message) Unmarshal(pb proto.Message) error {
	return proto.Unmarshal(m.Payload, pb)
}
