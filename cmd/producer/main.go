package main

import (
	"context"
	"fmt"
	"github.com/bwjson/kafka/internal/handler"
	"github.com/bwjson/kafka/internal/producer"
	"log"
)

const (
	topicName = "users.events"
)

var (
	events = []string{"registration", "login", "subscription"}
)

func main() {
	ctx := context.Background()

	p, err := producer.NewProducer(producer.Config{
		Brokers:  []string{"localhost:9092"},
		ClientID: "users-producer",
	})
	if err != nil {
		log.Fatalf("new: %v", err)
	}
	defer p.Close()

	num := 4432

	for range 100 {
		userID := fmt.Sprintf("%04d", num)
		evt := handler.LoginEvent{UserID: userID, Event: events[num%3], Price: num / 2}
		part, off, err := p.Send(ctx, topicName, userID, evt)
		if err != nil {
			log.Fatalf("send: %v", err)
		}
		log.Printf("sent: p=%d o=%d", part, off)
	}
}
