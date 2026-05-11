package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bwjson/kafka/internal/producer"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
)

const (
	dlqTopicName = "users.events.dlq"
)

type KafkaConsumer struct {
	cl      *kafka.Consumer
	dlq     producer.Producer
	handler Handler
}

type KafkaConfig struct {
	Brokers []string
	Topics  []string
	GroupID string
}

func NewKafkaConsumer(cfg KafkaConfig, dlq producer.Producer, handler Handler) (Consumer, error) {
	cl, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(cfg.Brokers, ","),
		"group.id":           cfg.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err := cl.SubscribeTopics(cfg.Topics, nil); err != nil {
		cl.Close()
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	return &KafkaConsumer{cl: cl, dlq: dlq, handler: handler}, nil
}

func (c *KafkaConsumer) Close() {
	c.cl.Close()
	c.dlq.Close()
}

func (c *KafkaConsumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg := c.cl.Poll(100)
		if msg == nil {
			continue
		}

		switch e := msg.(type) {
		case *kafka.Message:
			err := c.handler.Handle(ctx, e)
			if err != nil {
				log.Printf("handle p=%d o=%d: %v", e.TopicPartition.Partition, e.TopicPartition.Offset, err)
				err := c.dlq.Send(ctx, dlqTopicName, string(e.Key), json.RawMessage(e.Value))
				if err != nil {
					return err
				}
				if _, err := c.cl.CommitMessage(e); err != nil {
					log.Printf("commit: %v", err)
				}
				continue
			}
			if _, err := c.cl.CommitMessage(e); err != nil {
				log.Printf("commit: %v", err)
			}
		case kafka.Error:
			log.Printf("fetch: %v", e)
		}
	}
}
