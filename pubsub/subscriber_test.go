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

type fakeStore struct {
}

func (f *fakeStore) Write(msg server.MessageToSend) error {
	return nil
}
func (f *fakeStore) ReadFrom(offset int, handleFunc func(msgs []server.MessageToSend)) error {
	return nil
}

func createServer(t *testing.T) {
	fs := &fakeStore{}
	server, err := server.New(serverAddr, time.Millisecond*100, time.Millisecond*100, fs)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = server.Shutdown()
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

	err = sub.SubscribeToTopics(topics, server.Current, 0)
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

	err = sub.SubscribeToTopics(topics, server.Current, 0)
	require.NoError(t, err)

	err = sub.UnsubscribeToTopics([]string{topicA})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	consumer := sub.Consume(ctx)
	require.NoError(t, err)

	var receivedMessages []*Message
	consumerFinCh := make(chan struct{})
	go func() {
		for msg := range consumer.Messages() {
			msg.Ack(true)
			receivedMessages = append(receivedMessages, msg)
		}

		consumerFinCh <- struct{}{}
	}()

	// publish a message to both topics and check the subscriber only gets the message from the 1 topic
	// and not the unsubscribed topic

	publisher, err := NewPublisher("localhost:9999")
	require.NoError(t, err)
	t.Cleanup(func() {
		publisher.Close()
	})

	msg := NewMessage(topicA, []byte("hello world"))

	err = publisher.PublishMessage(msg)
	require.NoError(t, err)

	msg.Topic = topicB
	err = publisher.PublishMessage(msg)
	require.NoError(t, err)

	time.Sleep(time.Second)
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
	consumer, cancel := setupConsumer(t)

	var receivedMessages []*Message

	consumerFinCh := make(chan struct{})
	go func() {
		for msg := range consumer.Messages() {
			msg.Ack(true)
			receivedMessages = append(receivedMessages, msg)
		}

		consumerFinCh <- struct{}{}
	}()

	publisher, err := NewPublisher("localhost:9999")
	require.NoError(t, err)
	t.Cleanup(func() {
		publisher.Close()
	})

	// send some messages
	sentMessages := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		msg := NewMessage(topicA, []byte(fmt.Sprintf("message %d", i)))

		sentMessages = append(sentMessages, msg)

		err = publisher.PublishMessage(msg)
		require.NoError(t, err)
	}

	// give the consumer some time to read the messages -- TODO: make better!
	time.Sleep(time.Second)
	cancel()

	select {
	case <-consumerFinCh:
		break
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for consumer to read messages")
	}

	// THIS IS SO HACKY
	for _, msg := range receivedMessages {
		msg.ack = nil
	}

	for _, msg := range sentMessages {
		msg.ack = nil
	}

	assert.ElementsMatch(t, receivedMessages, sentMessages)
}

func TestPublishAndSubscribeNackMessage(t *testing.T) {
	consumer, cancel := setupConsumer(t)

	var receivedMessages []*Message

	consumerFinCh := make(chan struct{})
	timesMsgWasReceived := 0
	go func() {
		for msg := range consumer.Messages() {
			msg.Ack(false)
			timesMsgWasReceived++
		}

		consumerFinCh <- struct{}{}
	}()

	publisher, err := NewPublisher("localhost:9999")
	require.NoError(t, err)
	t.Cleanup(func() {
		publisher.Close()
	})

	// send a message
	msg := NewMessage(topicA, []byte("hello world"))

	err = publisher.PublishMessage(msg)
	require.NoError(t, err)

	// give the consumer some time to read the messages -- TODO: make better!
	time.Sleep(time.Second)
	cancel()

	select {
	case <-consumerFinCh:
		break
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for consumer to read messages")
	}

	assert.Empty(t, receivedMessages)
	assert.Equal(t, 5, timesMsgWasReceived)
}

func setupConsumer(t *testing.T) (*Consumer, context.CancelFunc) {
	createServer(t)

	sub, err := NewSubscriber(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})

	topics := []string{topicA, topicB}

	err = sub.SubscribeToTopics(topics, server.Current, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	consumer := sub.Consume(ctx)
	require.NoError(t, err)

	return consumer, cancel
}
