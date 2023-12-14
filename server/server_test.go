package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	topicA = "topic a"
	topicB = "topic b"
	topicC = "topic c"

	serverAddr = ":6666"
)

func createServer(t *testing.T) *Server {
	srv, err := New(serverAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = srv.Shutdown()
	})

	return srv
}

func createServerWithExistingTopic(t *testing.T, topicName string) *Server {
	srv := createServer(t)
	srv.topics[topicName] = &topic{
		name:          topicName,
		subscriptions: make(map[net.Addr]subscriber),
	}

	return srv
}

func createConnectionAndSubscribe(t *testing.T, topics []string) net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, Subscribe)
	require.NoError(t, err)

	rawTopics, err := json.Marshal(topics)
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, uint32(len(rawTopics)))
	require.NoError(t, err)

	_, err = conn.Write(rawTopics)
	require.NoError(t, err)

	expectedRes := Subscribed

	var resp Status
	err = binary.Read(conn, binary.BigEndian, &resp)
	require.NoError(t, err)

	assert.Equal(t, expectedRes, int(resp))

	return conn
}

func TestSubscribeToTopics(t *testing.T) {
	// create a server with an existing topic so we can test subscribing to a new and
	// existing topic
	srv := createServerWithExistingTopic(t, topicA)

	_ = createConnectionAndSubscribe(t, []string{topicA, topicB})

	assert.Len(t, srv.topics, 2)
	assert.Len(t, srv.topics[topicA].subscriptions, 1)
	assert.Len(t, srv.topics[topicB].subscriptions, 1)
}

func TestUnsubscribesFromTopic(t *testing.T) {
	srv := createServerWithExistingTopic(t, topicA)

	conn := createConnectionAndSubscribe(t, []string{topicA, topicB, topicC})

	assert.Len(t, srv.topics, 3)
	assert.Len(t, srv.topics[topicA].subscriptions, 1)
	assert.Len(t, srv.topics[topicB].subscriptions, 1)
	assert.Len(t, srv.topics[topicC].subscriptions, 1)

	err := binary.Write(conn, binary.BigEndian, Unsubscribe)
	require.NoError(t, err)

	topics := []string{topicA, topicB}
	rawTopics, err := json.Marshal(topics)
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, uint32(len(rawTopics)))
	require.NoError(t, err)

	_, err = conn.Write(rawTopics)
	require.NoError(t, err)

	expectedRes := Unsubscribed

	var resp Status
	err = binary.Read(conn, binary.BigEndian, &resp)
	require.NoError(t, err)

	assert.Equal(t, expectedRes, int(resp))

	assert.Len(t, srv.topics, 3)
	assert.Len(t, srv.topics[topicA].subscriptions, 0)
	assert.Len(t, srv.topics[topicB].subscriptions, 0)
	assert.Len(t, srv.topics[topicC].subscriptions, 1)
}

func TestSubscriberClosesWithoutUnsubscribing(t *testing.T) {
	srv := createServer(t)

	conn := createConnectionAndSubscribe(t, []string{topicA, topicB})

	assert.Len(t, srv.topics, 2)
	assert.Len(t, srv.topics[topicA].subscriptions, 1)
	assert.Len(t, srv.topics[topicB].subscriptions, 1)

	// close the conn
	err := conn.Close()
	require.NoError(t, err)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
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
	assert.Len(t, srv.topics[topicA].subscriptions, 0)
	assert.Len(t, srv.topics[topicB].subscriptions, 0)
}

func TestInvalidAction(t *testing.T) {
	_ = createServer(t)

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, uint8(99))
	require.NoError(t, err)

	expectedRes := Error

	var resp Status
	err = binary.Read(conn, binary.BigEndian, &resp)
	require.NoError(t, err)

	assert.Equal(t, expectedRes, int(resp))

	expectedMessage := "unknown action"

	var dataLen uint32
	err = binary.Read(conn, binary.BigEndian, &dataLen)
	require.NoError(t, err)
	assert.Equal(t, len(expectedMessage), int(dataLen))

	buf := make([]byte, dataLen)
	_, err = conn.Read(buf)
	require.NoError(t, err)

	assert.Equal(t, expectedMessage, string(buf))
}

func TestInvalidTopicDataPublished(t *testing.T) {
	_ = createServer(t)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	// send topic
	topic := topicA
	err = binary.Write(publisherConn, binary.BigEndian, uint32(len(topic)))
	require.NoError(t, err)
	_, err = publisherConn.Write([]byte(topic))
	require.NoError(t, err)

	expectedRes := Error

	var resp Status
	err = binary.Read(publisherConn, binary.BigEndian, &resp)
	require.NoError(t, err)

	assert.Equal(t, expectedRes, int(resp))

	expectedMessage := "topic data does not contain 'topic:' prefix"

	var dataLen uint32
	err = binary.Read(publisherConn, binary.BigEndian, &dataLen)
	require.NoError(t, err)
	assert.Equal(t, len(expectedMessage), int(dataLen))

	buf := make([]byte, dataLen)
	_, err = publisherConn.Read(buf)
	require.NoError(t, err)

	assert.Equal(t, expectedMessage, string(buf))
}

func TestSendsDataToTopicSubscribers(t *testing.T) {
	_ = createServer(t)

	subscribers := make([]net.Conn, 0, 10)
	for i := 0; i < 10; i++ {
		subscriberConn := createConnectionAndSubscribe(t, []string{topicA, topicB})

		subscribers = append(subscribers, subscriberConn)
	}

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	topic := fmt.Sprintf("topic:%s", topicA)
	messageData := "hello world"

	// send topic first
	err = binary.Write(publisherConn, binary.BigEndian, uint32(len(topic)))
	require.NoError(t, err)
	_, err = publisherConn.Write([]byte(topic))
	require.NoError(t, err)

	// now send the data
	err = binary.Write(publisherConn, binary.BigEndian, uint32(len(messageData)))
	require.NoError(t, err)
	n, err := publisherConn.Write([]byte(messageData))
	require.NoError(t, err)
	require.Equal(t, len(messageData), n)

	// check the subsribers got the data
	for _, conn := range subscribers {
		var topicLen uint64
		err = binary.Read(conn, binary.BigEndian, &topicLen)
		require.NoError(t, err)

		topicBuf := make([]byte, topicLen)
		_, err = conn.Read(topicBuf)
		require.NoError(t, err)
		assert.Equal(t, topicA, string(topicBuf))

		var dataLen uint64
		err = binary.Read(conn, binary.BigEndian, &dataLen)
		require.NoError(t, err)

		buf := make([]byte, dataLen)
		n, err := conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, int(dataLen), n)

		assert.Equal(t, messageData, string(buf))
	}
}

func TestPublishMultipleTimes(t *testing.T) {
	_ = createServer(t)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	messages := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		messages = append(messages, fmt.Sprintf("message %d", i))
	}

	subscribeFinCh := make(chan struct{})
	// create a subscriber that will read messages
	subscriberConn := createConnectionAndSubscribe(t, []string{topicA, topicB})
	go func() {
		// check subscriber got all messages
		results := make([]string, 0, len(messages))
		for i := 0; i < len(messages); i++ {
			var topicLen uint64
			err = binary.Read(subscriberConn, binary.BigEndian, &topicLen)
			require.NoError(t, err)

			topicBuf := make([]byte, topicLen)
			_, err = subscriberConn.Read(topicBuf)
			require.NoError(t, err)
			assert.Equal(t, topicA, string(topicBuf))

			var dataLen uint64
			err = binary.Read(subscriberConn, binary.BigEndian, &dataLen)
			require.NoError(t, err)

			buf := make([]byte, dataLen)
			n, err := subscriberConn.Read(buf)
			require.NoError(t, err)
			require.Equal(t, int(dataLen), n)

			results = append(results, string(buf))
		}

		assert.ElementsMatch(t, results, messages)

		subscribeFinCh <- struct{}{}
	}()

	topic := fmt.Sprintf("topic:%s", topicA)

	// send multiple messages
	for _, msg := range messages {
		// send topic first
		err = binary.Write(publisherConn, binary.BigEndian, uint32(len(topic)))
		require.NoError(t, err)
		_, err = publisherConn.Write([]byte(topic))
		require.NoError(t, err)

		// now send the data
		err = binary.Write(publisherConn, binary.BigEndian, uint32(len(msg)))
		require.NoError(t, err)
		n, err := publisherConn.Write([]byte(msg))
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
