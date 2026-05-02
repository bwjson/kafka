package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
)

type Handler interface {
	Handle(ctx context.Context, msg *kgo.Record) error
}

type Consumer struct {
	cl      *kgo.Client
	handler Handler
}

type Config struct {
	Brokers []string
	Topics  []string
	GroupID string
}

func NewConsumer(cfg Config, handler Handler) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Consumer{cl: cl, handler: handler}, err
}

func (c *Consumer) Close() {
	c.cl.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		if err := fetches.Err(); err != nil {
			log.Printf("fetch: %v", err)
			continue
		}

		var failed bool
		fetches.EachRecord(func(r *kgo.Record) {
			if err := c.handler.Handle(ctx, r); err != nil {
				log.Printf("handle p=%d o=%d: %v", r.Partition, r.Offset, err)
				failed = true
			}
		})

		if failed {
			continue
		}
		if err := c.cl.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit: %v", err)
		}
	}
}
