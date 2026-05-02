package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

type LoginEvent struct {
	UserID int    `json:"user_id"`
	Event  string `json:"event"`
}

type Login struct{}

func (Login) Handle(_ context.Context, r *kgo.Record) error {
	var evt LoginEvent
	if err := json.Unmarshal(r.Value, &evt); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	log.Printf("login: p=%d o=%d user=%d event=%s",
		r.Partition, r.Offset, evt.UserID, evt.Event)

	return nil
}
