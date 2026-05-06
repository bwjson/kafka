package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Config struct {
	Brokers  []string
	ClientID string
}

type Producer struct {
	cl *kafka.Producer
}

func NewProducer(cfg Config) (*Producer, error) {
	cl, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),
		"client.id":         cfg.ClientID,
		"acks":              "all",
	})
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

func (p *Producer) Close() { p.cl.Close() }

func (p *Producer) Send(_ context.Context, topic, key string, value any) (int32, int64, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal: %w", err)
	}

	deliveryChan := make(chan kafka.Event, 1)
	err = p.cl.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          data,
	}, deliveryChan)
	if err != nil {
		return 0, 0, fmt.Errorf("produce: %w", err)
	}

	e := <-deliveryChan
	msg := e.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		return 0, 0, fmt.Errorf("delivery: %w", msg.TopicPartition.Error)
	}

	return int32(msg.TopicPartition.Partition), int64(msg.TopicPartition.Offset), nil
}
