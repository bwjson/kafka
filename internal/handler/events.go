package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LoginEvent struct {
	UserID string `json:"user_id"`
	Event  string `json:"event"`
	Price  int    `json:"price"`
}

type Login struct{}

func (Login) Handle(_ context.Context, r *kafka.Message) error {
	var evt LoginEvent
	if err := json.Unmarshal(r.Value, &evt); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	log.Printf("login: p=%d o=%d user=%s event=%s",
		r.TopicPartition.Partition, r.TopicPartition.Offset, evt.UserID, evt.Event)

	return nil
}
