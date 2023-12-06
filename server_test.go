package messagebroker

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
)

func createServer(t *testing.T) *Server {
	srv, err := NewServer(context.Background(), ":3000")
	require.NoError(t, err)

	t.Cleanup(func() {
		srv.Shutdown()
	})

	return srv
}

func createServerWithExistingTopic(t *testing.T, topicName string) *Server {
	srv := createServer(t)
	srv.topics[topicName] = topic{
		name:          topicName,
		subscriptions: make(map[net.Addr]Subscriber),
	}

	return srv
}

func createConnectionAndSubscribe(t *testing.T, topics []string) net.Conn {
	conn, err := net.Dial("tcp", "localhost:3000")
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, Subscribe)
	require.NoError(t, err)

	rawTopics, err := json.Marshal(topics)
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, uint32(len(rawTopics)))
	require.NoError(t, err)

	_, err = conn.Write(rawTopics)
	require.NoError(t, err)

	expectedRes := "subscribed"

	buf := make([]byte, len(expectedRes))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(expectedRes), n)

	assert.Equal(t, expectedRes, string(buf))

	return conn
}

func TestSubscribeToTopics(t *testing.T) {
	// create a server with an existing topic so we can test subscribing to a new and
	// existing topic
	srv := createServerWithExistingTopic(t, "topic a")

	_ = createConnectionAndSubscribe(t, []string{"topic a", "topic b"})

	assert.Len(t, srv.topics, 2)
	assert.Len(t, srv.topics["topic a"].subscriptions, 1)
	assert.Len(t, srv.topics["topic b"].subscriptions, 1)
}

func TestUnsubscribesFromTopic(t *testing.T) {
	srv := createServerWithExistingTopic(t, "topic a")

	conn := createConnectionAndSubscribe(t, []string{"topic a", "topic b", "topic c"})

	assert.Len(t, srv.topics, 3)
	assert.Len(t, srv.topics["topic a"].subscriptions, 1)
	assert.Len(t, srv.topics["topic b"].subscriptions, 1)
	assert.Len(t, srv.topics["topic c"].subscriptions, 1)

	err := binary.Write(conn, binary.BigEndian, Unsubscribe)
	require.NoError(t, err)

	topics := []string{"topic a", "topic b"}
	rawTopics, err := json.Marshal(topics)
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, uint32(len(rawTopics)))
	require.NoError(t, err)

	_, err = conn.Write(rawTopics)
	require.NoError(t, err)

	expectedRes := "unsubscribed"

	buf := make([]byte, len(expectedRes))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(expectedRes), n)

	assert.Equal(t, expectedRes, string(buf))

	assert.Len(t, srv.topics, 3)
	assert.Len(t, srv.topics["topic a"].subscriptions, 0)
	assert.Len(t, srv.topics["topic b"].subscriptions, 0)
	assert.Len(t, srv.topics["topic c"].subscriptions, 1)
}

func TestSubscriberClosesWithoutUnsubscribing(t *testing.T) {
	srv := createServer(t)

	conn := createConnectionAndSubscribe(t, []string{"topic a", "topic b"})

	assert.Len(t, srv.topics, 2)
	assert.Len(t, srv.topics["topic a"].subscriptions, 1)
	assert.Len(t, srv.topics["topic b"].subscriptions, 1)

	// close the conn
	err := conn.Close()
	require.NoError(t, err)

	publisherConn, err := net.Dial("tcp", "localhost:3000")
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	data := []byte("hello world")
	// send data length first
	err = binary.Write(publisherConn, binary.BigEndian, uint32(len(data)))
	require.NoError(t, err)
	n, err := publisherConn.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	assert.Len(t, srv.topics, 2)
	assert.Len(t, srv.topics["topic a"].subscriptions, 0)
	assert.Len(t, srv.topics["topic b"].subscriptions, 0)
}

func TestInvalidAction(t *testing.T) {
	_ = createServer(t)

	conn, err := net.Dial("tcp", "localhost:3000")
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, uint8(99))
	require.NoError(t, err)

	expectedRes := "unknown action"

	buf := make([]byte, len(expectedRes))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(expectedRes), n)

	assert.Equal(t, expectedRes, string(buf))
}

func TestInvalidMessagePublished(t *testing.T) {
	_ = createServer(t)

	publisherConn, err := net.Dial("tcp", "localhost:3000")
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	// send some data
	data := []byte("this isn't wrapped in a message type")

	// send data length first
	err = binary.Write(publisherConn, binary.BigEndian, uint32(len(data)))
	require.NoError(t, err)
	n, err := publisherConn.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	buf := make([]byte, 15)
	_, err = publisherConn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "invalid message", string(buf))
}

func TestSendsDataToTopicSubscribers(t *testing.T) {
	_ = createServer(t)

	subscribers := make([]net.Conn, 0, 5)
	for i := 0; i < 5; i++ {
		subscriberConn := createConnectionAndSubscribe(t, []string{"topic a", "topic b"})

		subscribers = append(subscribers, subscriberConn)
	}

	publisherConn, err := net.Dial("tcp", "localhost:3000")
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	// send a message
	msg := Message{
		Topic: "topic a",
		Data:  []byte("hello world"),
	}

	rawMsg, err := json.Marshal(msg)
	require.NoError(t, err)

	// send data length first
	err = binary.Write(publisherConn, binary.BigEndian, uint32(len(rawMsg)))
	require.NoError(t, err)
	n, err := publisherConn.Write(rawMsg)
	require.NoError(t, err)
	require.Equal(t, len(rawMsg), n)

	// check the subsribers got the data
	for _, conn := range subscribers {

		var dataLen uint64
		err = binary.Read(conn, binary.BigEndian, &dataLen)
		require.NoError(t, err)

		buf := make([]byte, dataLen)
		n, err := conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, int(dataLen), n)

		assert.Equal(t, rawMsg, buf)
	}
}

func TestPublishMultipleTimes(t *testing.T) {
	_ = createServer(t)

	publisherConn, err := net.Dial("tcp", "localhost:3000")
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	messages := make([][]byte, 0, 10)
	for i := 0; i < 10; i++ {
		msg := Message{
			Topic: "topic a",
			Data:  []byte(fmt.Sprintf("message %d", i)),
		}

		rawMsg, err := json.Marshal(msg)
		require.NoError(t, err)

		messages = append(messages, rawMsg)
	}

	subscribeFinCh := make(chan struct{})
	// create a subscriber that will read messages
	subscriberConn := createConnectionAndSubscribe(t, []string{"topic a", "topic b"})
	go func() {
		// check subscriber got all messages
		for _, msg := range messages {
			var dataLen uint64
			err = binary.Read(subscriberConn, binary.BigEndian, &dataLen)
			require.NoError(t, err)

			buf := make([]byte, dataLen)
			n, err := subscriberConn.Read(buf)
			require.NoError(t, err)
			require.Equal(t, int(dataLen), n)

			assert.Equal(t, msg, buf)
		}

		subscribeFinCh <- struct{}{}
	}()

	// send multiple messages
	for _, msg := range messages {
		// send data length first
		err = binary.Write(publisherConn, binary.BigEndian, uint32(len(msg)))
		require.NoError(t, err)
		n, err := publisherConn.Write(msg)
		require.NoError(t, err)
		require.Equal(t, len(msg), n)
	}

	select {
	case <-subscribeFinCh:
		break
	case <-time.After(time.Second):
		t.Fatal(fmt.Errorf("timed out waiting for subscriber to read messages"))
	}
}
