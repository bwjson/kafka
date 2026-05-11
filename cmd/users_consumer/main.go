package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/bwjson/kafka/internal/consumer"
	"github.com/bwjson/kafka/internal/handler"
)

const (
	topicName = "users.events"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := consumer.NewKafkaConsumer(
		consumer.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			GroupID: "users.consumer",
			Topics:  []string{topicName},
		},
		handler.Login{},
	)
	if err != nil {
		log.Fatalf("new: %v", err)
	}
	defer c.Close()

	if err := c.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}
