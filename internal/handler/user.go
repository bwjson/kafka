package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bwjson/kafka/internal/domain"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Login struct{}

func (Login) Handle(_ context.Context, r *kafka.Message) error {
	var evt domain.LoginEvent
	if err := json.Unmarshal(r.Value, &evt); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	log.Printf("login: p=%d o=%d user=%s event=%s",
		r.TopicPartition.Partition, r.TopicPartition.Offset, evt.UserID, evt.Event)

	return errors.New("test dlq")
}
