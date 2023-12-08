package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/willdot/messagebroker/server"
)

const (
	serverAddr = ":9999"
	topicA     = "topic a"
	topicB     = "topic b"
)

func createServer(t *testing.T) {
	server, err := server.New(serverAddr)
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

	topics := []string{topicA, topicB}

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

	topics := []string{topicA, topicB}

	err = sub.SubscribeToTopics(topics)
	require.NoError(t, err)

	err = sub.UnsubscribeToTopics([]string{topicA})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	consumer := sub.Consume(ctx)
	require.NoError(t, err)

	var receivedMessages []Message
	consumerFinCh := make(chan struct{})
	go func() {
		for msg := range consumer.Messages() {
			receivedMessages = append(receivedMessages, msg)
		}

		require.NoError(t, err)
		consumerFinCh <- struct{}{}
	}()

	// publish a message to both topics and check the subscriber only gets the message from the 1 topic
	// and not the unsubscribed topic

	publisher, err := NewPublisher("localhost:9999")
	require.NoError(t, err)
	t.Cleanup(func() {
		publisher.Close()
	})

	msg := Message{
		Topic: topicA,
		Data:  []byte("hello world"),
	}

	err = publisher.PublishMessage(msg)
	require.NoError(t, err)

	msg.Topic = topicB
	err = publisher.PublishMessage(msg)
	require.NoError(t, err)

	cancel()

	select {
	case <-consumerFinCh:
		break
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for consumer to read messages")
	}

	assert.Len(t, receivedMessages, 1)
	assert.Equal(t, topicB, receivedMessages[0].Topic)
}

func TestPublishAndSubscribe(t *testing.T) {
	createServer(t)

	sub, err := NewSubscriber(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})

	topics := []string{topicA, topicB}

	err = sub.SubscribeToTopics(topics)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	consumer := sub.Consume(ctx)
	require.NoError(t, err)

	var receivedMessages []Message

	consumerFinCh := make(chan struct{})
	go func() {
		for msg := range consumer.Messages() {
			receivedMessages = append(receivedMessages, msg)
		}

		require.NoError(t, err)
		consumerFinCh <- struct{}{}
	}()

	publisher, err := NewPublisher("localhost:9999")
	require.NoError(t, err)
	t.Cleanup(func() {
		publisher.Close()
	})

	// send some messages
	sentMessages := make([]Message, 0, 10)
	for i := 0; i < 10; i++ {
		msg := Message{
			Topic: topicA,
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
