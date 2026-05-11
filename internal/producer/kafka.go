package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

type KafkaConfig struct {
	Brokers  []string
	ClientID string
}

type KafkaProducer struct {
	cl *kafka.Producer
}

func NewKafkaProducer(cfg KafkaConfig) (Producer, error) {
	cl, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),
		"client.id":         cfg.ClientID,
		"acks":              "all",
	})
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{cl: cl}, nil
}

func (p *KafkaProducer) Close() {
	p.cl.Close()
}

func (p *KafkaProducer) Send(ctx context.Context, topic, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	deliveryChan := make(chan kafka.Event, 1)
	err = p.cl.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          data,
	}, deliveryChan)
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	var msg *kafka.Message
	select {
	case <-ctx.Done():
		return ctx.Err()
	case msg = <-deliveryChan:
		if msg.TopicPartition.Error != nil {
			return fmt.Errorf("delivery: %w", msg.TopicPartition.Error)
		}

		return nil
	}
}
