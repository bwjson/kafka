package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers  []string
	ClientID string
}

type Producer struct {
	cl *kgo.Client
}

func NewProducer(cfg Config) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

func (p *Producer) Close() { p.cl.Close() }

func (p *Producer) SendJSON(ctx context.Context, topic, key string, value any) (int32, int64, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal: %w", err)
	}

	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: data}
	res := p.cl.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return 0, 0, fmt.Errorf("produce: %w", err)
	}
	r := res[0].Record

	return r.Partition, r.Offset, nil
}
