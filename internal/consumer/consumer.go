package consumer

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Handler interface {
	Handle(ctx context.Context, msg *kafka.Message) error
}

type Consumer interface {
	Run(ctx context.Context) error
	Close()
}
