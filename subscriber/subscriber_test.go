package subscriber_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/messagebroker"
	"github.com/willdot/messagebroker/server"
	"github.com/willdot/messagebroker/subscriber"
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

func TestNew(t *testing.T) {
	createServer(t)

	sub, err := subscriber.New(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})
}

func TestNewInvalidServerAddr(t *testing.T) {
	createServer(t)

	_, err := subscriber.New(":123456")
	require.Error(t, err)
}

func TestSubscribeToTopics(t *testing.T) {
	createServer(t)

	sub, err := subscriber.New(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		sub.Close()
	})

	topics := []string{"topic a", "topic b"}

	err = sub.SubscribeToTopics(topics)
	require.NoError(t, err)
}

func TestSubscribeConsumeFromSubscription(t *testing.T) {
	createServer(t)

	sub, err := subscriber.New(serverAddr)
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

	publisherConn, err := net.Dial("tcp", "localhost:3000")
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, server.Publish)
	require.NoError(t, err)

	// send some messages
	sentMessages := make([]messagebroker.Message, 0, 10)
	for i := 0; i < 10; i++ {
		msg := messagebroker.Message{
			Topic: "topic a",
			Data:  []byte(fmt.Sprintf("message %d", i)),
		}

		sentMessages = append(sentMessages, msg)

		b, err := json.Marshal(msg)
		require.NoError(t, err)

		err = binary.Write(publisherConn, binary.BigEndian, uint32(len(b)))
		require.NoError(t, err)
		n, err := publisherConn.Write(b)
		require.NoError(t, err)
		require.Equal(t, len(b), n)
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
