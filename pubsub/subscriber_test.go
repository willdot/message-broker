package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/messagebroker"

	"github.com/willdot/messagebroker/server"
)

const (
	serverAddr = ":3000"
)

func createServer(t *testing.T) {
	server, err := server.New(context.Background(), serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		server.Shutdown()
	})
}

func TestNewSubscriber(t *testing.T) {
	createServer(t)

	sub, err := NewSubscriber(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})
}

func TestNewSubscriberInvalidServerAddr(t *testing.T) {
	createServer(t)

	_, err := NewSubscriber(":123456")
	require.Error(t, err)
}

func TestNewPublisher(t *testing.T) {
	createServer(t)

	sub, err := NewPublisher(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})
}

func TestNewPublisherInvalidServerAddr(t *testing.T) {
	createServer(t)

	_, err := NewPublisher(":123456")
	require.Error(t, err)
}

func TestSubscribeToTopics(t *testing.T) {
	createServer(t)

	sub, err := NewSubscriber(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})

	topics := []string{"topic a", "topic b"}

	err = sub.SubscribeToTopics(topics)
	require.NoError(t, err)
}

func TestUnsubscribesFromTopic(t *testing.T) {
	createServer(t)

	sub, err := NewSubscriber(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})

	topics := []string{"topic a", "topic b"}

	err = sub.SubscribeToTopics(topics)
	require.NoError(t, err)

	err = sub.UnsubscribeToTopics([]string{"topic a"})
	require.NoError(t, err)

	// TODO: is there a way to check? Maybe start consuming and publish to the topic unsubscribed from??
}

func TestPublishAndSubscribe(t *testing.T) {
	createServer(t)

	sub, err := NewSubscriber(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})

	topics := []string{"topic a", "topic b"}

	err = sub.SubscribeToTopics(topics)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	consumer := sub.Consume(ctx)
	require.NoError(t, err)

	var receivedMessages []messagebroker.Message

	consumerFinCh := make(chan struct{})
	go func() {
		for msg := range consumer.Msgs {
			receivedMessages = append(receivedMessages, msg)
		}

		require.NoError(t, err)
		consumerFinCh <- struct{}{}
	}()

	publisher, err := NewPublisher("localhost:3000")
	require.NoError(t, err)
	t.Cleanup(func() {
		publisher.Close()
	})

	// send some messages
	sentMessages := make([]messagebroker.Message, 0, 10)
	for i := 0; i < 10; i++ {
		msg := messagebroker.Message{
			Topic: "topic a",
			Data:  []byte(fmt.Sprintf("message %d", i)),
		}

		sentMessages = append(sentMessages, msg)

		err = publisher.PublishMessage(msg)
		require.NoError(t, err)
	}

	// give the consumer some time to read the messages -- TODO: make better!
	time.Sleep(time.Millisecond * 500)
	cancel()

	select {
	case <-consumerFinCh:
		break
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for consumer to read messages")
	}

	assert.ElementsMatch(t, receivedMessages, sentMessages)
}
