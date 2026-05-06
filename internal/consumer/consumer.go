package consumer

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Handler interface {
	Handle(ctx context.Context, msg *kafka.Message) error
}

type Consumer struct {
	cl      *kafka.Consumer
	handler Handler
}

type Config struct {
	Brokers []string
	Topics  []string
	GroupID string
}

func NewConsumer(cfg Config, handler Handler) (*Consumer, error) {
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

	return &Consumer{cl: cl, handler: handler}, nil
}

func (c *Consumer) Close() {
	c.cl.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		event := c.cl.Poll(100)
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			if err := c.handler.Handle(ctx, e); err != nil {
				log.Printf("handle p=%d o=%d: %v", e.TopicPartition.Partition, e.TopicPartition.Offset, err)
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
