package main

import (
	"context"
	"fmt"
	"github.com/bwjson/kafka/internal/domain"
	"github.com/bwjson/kafka/internal/producer"
	"log/slog"
	"math/rand"
	"os"
)

const (
	topicName    = "users.events"
	eventsNumber = 100
)

var (
	events = []string{"registration", "login", "subscription"}
)

func main() {
	ctx := context.Background()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	prd, err := producer.NewKafkaProducer(
		producer.KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "users-producer",
		},
	)
	if err != nil {
		log.Error("failed to create producer", slog.Any("err", err))
	}
	defer prd.Close()

	// simulating the process of sending from the app
	for i := 0; i < eventsNumber; i++ {
		num := rand.Intn(100)
		userID := fmt.Sprintf("%03d", num)

		evt := domain.LoginEvent{
			UserID: userID,
			Event:  events[num%3],
			Price:  num / 2,
		}

		err = prd.Send(ctx, topicName, userID, evt)
		if err != nil {
			log.Error("send failed", slog.Any("err", err))
		}
	}
}
