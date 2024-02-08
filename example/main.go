package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"time"

	"github.com/willdot/messagebroker/client"
	"github.com/willdot/messagebroker/internal/server"
)

var consumeOnly *bool
var consumeFrom *int

func main() {
	consumeOnly = flag.Bool("consume-only", false, "just consumes (doesn't start server and doesn't publish)")
	consumeFrom = flag.Int("consume-from", -1, "index of message to start consuming from. If not set it will consume from the most recent")
	flag.Parse()

	if !*consumeOnly {
		go sendMessages()
	}

	sub, err := client.NewSubscriber(":3000")
	if err != nil {
		panic(err)
	}

	defer func() {
		_ = sub.Close()
	}()
	startAt := 0
	startAtType := server.Current
	if *consumeFrom > -1 {
		startAtType = server.From
		startAt = *consumeFrom
	}

	err = sub.SubscribeToTopics([]string{"topic a"}, startAtType, startAt)
	if err != nil {
		panic(err)
	}

	consumer := sub.Consume(context.Background())
	if consumer.Err != nil {
		panic(err)
	}

	for msg := range consumer.Messages() {
		slog.Info("received message", "message", string(msg.Data))
		msg.Ack(true)
	}

}

func sendMessages() {
	publisher, err := client.NewPublisher("localhost:3000")
	if err != nil {
		panic(err)
	}

	defer func() {
		_ = publisher.Close()
	}()

	// send some messages
	i := 0
	for {
		i++
		msg := client.NewMessage("topic a", []byte(fmt.Sprintf("message %d", i)))

		err = publisher.PublishMessage(msg)
		if err != nil {
			slog.Error("failed to publish message", "error", err)
			continue
		}

		time.Sleep(time.Millisecond * 500)
	}
}
