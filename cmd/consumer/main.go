package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/bwjson/kafka/internal/consumer"
	"github.com/bwjson/kafka/internal/handler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := consumer.NewConsumer(
		consumer.Config{
			Brokers: []string{"localhost:9092"},
			GroupID: "playground.consumer",
			Topics:  []string{"playground.events"},
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
