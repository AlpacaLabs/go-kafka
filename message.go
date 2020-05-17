package kafka

import (
	"fmt"

	eventV1 "github.com/AlpacaLabs/protorepo-event-go/alpacalabs/event/v1"

	"github.com/golang/protobuf/proto"
	"github.com/segmentio/kafka-go"
)

const (
	HeaderForTraceID = "trace_id"
	HeaderForEventID = "event_id"
)

type Message struct {
	Headers map[string][]byte
	Payload []byte
}

func NewMessage(traceInfo eventV1.TraceInfo, eventInfo eventV1.EventInfo, pb proto.Message) (Message, error) {
	m := Message{
		Headers: make(map[string][]byte),
	}
	b, err := proto.Marshal(pb)
	if err != nil {
		return Message{}, err
	}
	m.Payload = b

	m.Headers[HeaderForTraceID] = []byte(traceInfo.TraceId)
	m.Headers[HeaderForEventID] = []byte(eventInfo.EventId)

	return m, nil
}

func (m Message) ToKafkaMessage() kafka.Message {
	var headers []kafka.Header
	for k, v := range m.Headers {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: v,
		})
	}
	return kafka.Message{
		Headers: headers,
		Value:   m.Payload,
	}
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
