package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/willdot/messagebroker"
	"github.com/willdot/messagebroker/pubsub"
	"github.com/willdot/messagebroker/server"
)

func main() {
	server, err := server.New(context.Background(), ":3000")
	if err != nil {
		panic(err)
	}
	defer server.Shutdown()

	go sendMessages()

	sub, err := pubsub.NewSubscriber(":3000")
	if err != nil {
		panic(err)
	}
	defer sub.Close()

	sub.SubscribeToTopics([]string{"topic a"})

	consumer := sub.Consume(context.Background())
	if consumer.Err != nil {
		panic(err)
	}

	for msg := range consumer.Messages() {
		slog.Info("received message", "message", string(msg.Data))
	}

}

func sendMessages() {
	publisher, err := pubsub.NewPublisher("localhost:3000")
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	// send some messages
	i := 0
	for {
		i++
		msg := messagebroker.Message{
			Topic: "topic a",
			Data:  []byte(fmt.Sprintf("message %d", i)),
		}

		err = publisher.PublishMessage(msg)
		if err != nil {
			slog.Error("failed to publish message", "error", err)
			continue
		}
	}
}
