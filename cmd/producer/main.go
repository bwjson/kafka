package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bwjson/kafka/internal/handler"
	"github.com/bwjson/kafka/internal/producer"
)

func main() {
	ctx := context.Background()

	p, err := producer.NewProducer(producer.Config{
		Brokers:  []string{"localhost:9092"},
		ClientID: "playground-producer",
	})
	if err != nil {
		log.Fatalf("new: %v", err)
	}
	defer p.Close()

	for i := range 5 {
		evt := handler.LoginEvent{UserID: i, Event: "login"}
		part, off, err := p.SendJSON(ctx, "playground.events", fmt.Sprintf("user-%d", i), evt)
		if err != nil {
			log.Fatalf("send: %v", err)
		}
		log.Printf("sent: p=%d o=%d", part, off)
	}
}
