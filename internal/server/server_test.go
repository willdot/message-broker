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
	"github.com/willdot/messagebroker/internal/messagestore"
)

const (
	topicA = "topic a"
	topicB = "topic b"
	topicC = "topic c"

	serverAddr = ":6666"

	ackDelay   = time.Millisecond * 100
	ackTimeout = time.Millisecond * 100
)

func createServer(t *testing.T) *Server {
	srv, err := New(serverAddr, ackDelay, ackTimeout)
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
		subscriptions: make(map[net.Addr]*subscriber),
		messageStore:  messagestore.NewMemoryStore(),
	}

	return srv
}

func createConnectionAndSubscribe(t *testing.T, topics []string, startAtType StartAtType, startAtIndex int) net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	subscribeToTopics(t, conn, topics, startAtType, startAtIndex)

	expectedRes := Subscribed

	var resp Status
	err = binary.Read(conn, binary.BigEndian, &resp)
	require.NoError(t, err)

	assert.Equal(t, expectedRes, resp)

	return conn
}

func sendMessage(t *testing.T, conn net.Conn, topic string, message []byte) {
	topicLenB := make([]byte, 4)
	binary.BigEndian.PutUint32(topicLenB, uint32(len(topic)))

	headers := topicLenB
	headers = append(headers, []byte(topic)...)

	messageLenB := make([]byte, 4)
	binary.BigEndian.PutUint32(messageLenB, uint32(len(message)))
	headers = append(headers, messageLenB...)

	_, err := conn.Write(append(headers, message...))
	require.NoError(t, err)
}

func subscribeToTopics(t *testing.T, conn net.Conn, topics []string, startAtType StartAtType, startAtIndex int) {
	actionB := make([]byte, 2)
	binary.BigEndian.PutUint16(actionB, uint16(Subscribe))
	headers := actionB

	b, err := json.Marshal(topics)
	require.NoError(t, err)

	topicNamesB := make([]byte, 4)
	binary.BigEndian.PutUint32(topicNamesB, uint32(len(b)))
	headers = append(headers, topicNamesB...)
	headers = append(headers, b...)

	startAtTypeB := make([]byte, 2)
	binary.BigEndian.PutUint16(startAtTypeB, uint16(startAtType))
	headers = append(headers, startAtTypeB...)

	if startAtType == From {
		fromB := make([]byte, 2)
		binary.BigEndian.PutUint16(fromB, uint16(startAtIndex))
		headers = append(headers, fromB...)
	}

	_, err = conn.Write(headers)
	require.NoError(t, err)
}

func unsubscribetoTopics(t *testing.T, conn net.Conn, topics []string) {
	actionB := make([]byte, 2)
	binary.BigEndian.PutUint16(actionB, uint16(Unsubscribe))
	headers := actionB

	b, err := json.Marshal(topics)
	require.NoError(t, err)

	topicNamesB := make([]byte, 4)
	binary.BigEndian.PutUint32(topicNamesB, uint32(len(b)))
	headers = append(headers, topicNamesB...)

	_, err = conn.Write(append(headers, b...))
	require.NoError(t, err)
}

func TestSubscribeToTopics(t *testing.T) {
	// create a server with an existing topic so we can test subscribing to a new and
	// existing topic
	srv := createServerWithExistingTopic(t, topicA)

	_ = createConnectionAndSubscribe(t, []string{topicA, topicB}, Current, 0)

	assert.Len(t, srv.topics, 2)
	assert.Len(t, srv.topics[topicA].subscriptions, 1)
	assert.Len(t, srv.topics[topicB].subscriptions, 1)
}

func TestUnsubscribesFromTopic(t *testing.T) {
	srv := createServerWithExistingTopic(t, topicA)

	conn := createConnectionAndSubscribe(t, []string{topicA, topicB, topicC}, Current, 0)

	assert.Len(t, srv.topics, 3)
	assert.Len(t, srv.topics[topicA].subscriptions, 1)
	assert.Len(t, srv.topics[topicB].subscriptions, 1)
	assert.Len(t, srv.topics[topicC].subscriptions, 1)

	topics := []string{topicA, topicB}

	unsubscribetoTopics(t, conn, topics)

	expectedRes := Unsubscribed

	var resp Status
	err := binary.Read(conn, binary.BigEndian, &resp)
	require.NoError(t, err)

	assert.Equal(t, expectedRes, resp)

	assert.Len(t, srv.topics, 3)
	assert.Len(t, srv.topics[topicA].subscriptions, 0)
	assert.Len(t, srv.topics[topicB].subscriptions, 0)
	assert.Len(t, srv.topics[topicC].subscriptions, 1)
}

func TestSubscriberClosesWithoutUnsubscribing(t *testing.T) {
	srv := createServer(t)

	conn := createConnectionAndSubscribe(t, []string{topicA, topicB}, Current, 0)

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

	sendMessage(t, publisherConn, topicA, data)

	// the timeout for a connection is 100 milliseconds, so we should wait at least this long before checking the unsubscribe
	// TODO: see if theres a better way, but without this, the test is flakey
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, srv.topics, 2)
	assert.Len(t, srv.topics[topicA].subscriptions, 0)
	assert.Len(t, srv.topics[topicB].subscriptions, 0)
}

func TestInvalidAction(t *testing.T) {
	_ = createServer(t)

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(conn, binary.BigEndian, uint16(99))
	require.NoError(t, err)

	expectedRes := Error

	var resp Status
	err = binary.Read(conn, binary.BigEndian, &resp)
	require.NoError(t, err)

	assert.Equal(t, expectedRes, resp)

	expectedMessage := "unknown action"

	var dataLen uint16
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

	assert.Equal(t, expectedRes, resp)

	expectedMessage := "topic data does not contain 'topic:' prefix"

	var dataLen uint16
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
		subscriberConn := createConnectionAndSubscribe(t, []string{topicA, topicB}, Current, 0)

		subscribers = append(subscribers, subscriberConn)
	}

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	topic := fmt.Sprintf("topic:%s", topicA)
	messageData := "hello world"

	sendMessage(t, publisherConn, topic, []byte(messageData))

	// check the subsribers got the data
	for _, conn := range subscribers {
		msg := readMessage(t, conn)
		assert.Equal(t, messageData, string(msg))
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
	subscriberConn := createConnectionAndSubscribe(t, []string{topicA, topicB}, Current, 0)
	go func() {
		// check subscriber got all messages
		results := make([]string, 0, len(messages))
		for i := 0; i < len(messages); i++ {
			msg := readMessage(t, subscriberConn)
			results = append(results, string(msg))
		}

		assert.ElementsMatch(t, results, messages)

		subscribeFinCh <- struct{}{}
	}()

	topic := fmt.Sprintf("topic:%s", topicA)

	// send multiple messages
	for _, msg := range messages {
		sendMessage(t, publisherConn, topic, []byte(msg))
	}

	select {
	case <-subscribeFinCh:
		break
	case <-time.After(time.Second):
		t.Fatal(fmt.Errorf("timed out waiting for subscriber to read messages"))
	}
}

func TestSendsDataToTopicSubscriberNacksThenAcks(t *testing.T) {
	_ = createServer(t)

	subscriberConn := createConnectionAndSubscribe(t, []string{topicA, topicB}, Current, 0)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	topic := fmt.Sprintf("topic:%s", topicA)
	messageData := "hello world"

	sendMessage(t, publisherConn, topic, []byte(messageData))

	// check the subsribers got the data
	readMessage := func(conn net.Conn, ack Action) {
		var topicLen uint16
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

		err = binary.Write(conn, binary.BigEndian, ack)
		require.NoError(t, err)
	}

	// NACK the  message and then ack it
	readMessage(subscriberConn, Nack)
	readMessage(subscriberConn, Ack)
	// reading for another message should now timeout but give enough time for the ack delay to kick in
	// should the second read of the message not have been ack'd properly
	var topicLen uint16
	_ = subscriberConn.SetReadDeadline(time.Now().Add(ackDelay + time.Millisecond*100))
	err = binary.Read(subscriberConn, binary.BigEndian, &topicLen)
	require.Error(t, err)
}

func TestSendsDataToTopicSubscriberDoesntAckMessage(t *testing.T) {
	_ = createServer(t)

	subscriberConn := createConnectionAndSubscribe(t, []string{topicA, topicB}, Current, 0)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	topic := fmt.Sprintf("topic:%s", topicA)
	messageData := "hello world"

	sendMessage(t, publisherConn, topic, []byte(messageData))

	// check the subsribers got the data
	readMessage := func(conn net.Conn, ack bool) {
		var topicLen uint16
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

		if ack {
			err = binary.Write(conn, binary.BigEndian, Ack)
			require.NoError(t, err)
			return
		}
	}

	// don't send ack or nack and then ack on the second attempt
	readMessage(subscriberConn, false)
	readMessage(subscriberConn, true)

	// reading for another message should now timeout but give enough time for the ack delay to kick in
	// should the second read of the message not have been ack'd properly
	var topicLen uint16
	_ = subscriberConn.SetReadDeadline(time.Now().Add(ackDelay + time.Millisecond*100))
	err = binary.Read(subscriberConn, binary.BigEndian, &topicLen)
	require.Error(t, err)
}

func TestSendsDataToTopicSubscriberDeliveryCountTooHighWithNoAck(t *testing.T) {
	_ = createServer(t)

	subscriberConn := createConnectionAndSubscribe(t, []string{topicA, topicB}, Current, 0)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	topic := fmt.Sprintf("topic:%s", topicA)
	messageData := "hello world"

	sendMessage(t, publisherConn, topic, []byte(messageData))

	// check the subsribers got the data
	readMessage := func(conn net.Conn, ack bool) {
		var topicLen uint16
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

		if ack {
			err = binary.Write(conn, binary.BigEndian, Ack)
			require.NoError(t, err)
			return
		}
	}

	// nack the message 5 times
	readMessage(subscriberConn, false)
	readMessage(subscriberConn, false)
	readMessage(subscriberConn, false)
	readMessage(subscriberConn, false)
	readMessage(subscriberConn, false)

	// reading for the message should now timeout as we have nack'd the message too many times
	var topicLen uint16
	_ = subscriberConn.SetReadDeadline(time.Now().Add(ackDelay + time.Millisecond*100))
	err = binary.Read(subscriberConn, binary.BigEndian, &topicLen)
	require.Error(t, err)
}

func TestSubscribeAndReplaysFromStart(t *testing.T) {
	_ = createServer(t)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	messages := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		messages = append(messages, fmt.Sprintf("message %d", i))
	}

	topic := fmt.Sprintf("topic:%s", topicA)

	for _, msg := range messages {
		sendMessage(t, publisherConn, topic, []byte(msg))
	}

	// send some messages for topic B as well
	sendMessage(t, publisherConn, fmt.Sprintf("topic:%s", topicB), []byte("topic b message 1"))
	sendMessage(t, publisherConn, fmt.Sprintf("topic:%s", topicB), []byte("topic b message 2"))
	sendMessage(t, publisherConn, fmt.Sprintf("topic:%s", topicB), []byte("topic b message 3"))

	subscriberConn := createConnectionAndSubscribe(t, []string{topicA}, From, 0)
	results := make([]string, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		msg := readMessage(t, subscriberConn)
		results = append(results, string(msg))
	}
	assert.ElementsMatch(t, results, messages)
}

func TestSubscribeAndReplaysFromIndex(t *testing.T) {
	_ = createServer(t)

	publisherConn, err := net.Dial("tcp", fmt.Sprintf("localhost%s", serverAddr))
	require.NoError(t, err)

	err = binary.Write(publisherConn, binary.BigEndian, Publish)
	require.NoError(t, err)

	messages := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		messages = append(messages, fmt.Sprintf("message %d", i))
	}

	topic := fmt.Sprintf("topic:%s", topicA)

	// send multiple messages
	for _, msg := range messages {
		sendMessage(t, publisherConn, topic, []byte(msg))
	}

	// send some messages for topic B as well
	sendMessage(t, publisherConn, fmt.Sprintf("topic:%s", topicB), []byte("topic b message 1"))
	sendMessage(t, publisherConn, fmt.Sprintf("topic:%s", topicB), []byte("topic b message 2"))
	sendMessage(t, publisherConn, fmt.Sprintf("topic:%s", topicB), []byte("topic b message 3"))

	subscriberConn := createConnectionAndSubscribe(t, []string{topicA}, From, 3)

	// now that the subscriber has subecribed send another message that should arrive after all the other messages were consumed
	sendMessage(t, publisherConn, topic, []byte("hello there"))

	results := make([]string, 0, len(messages))
	for i := 0; i < len(messages)-3; i++ {
		msg := readMessage(t, subscriberConn)
		results = append(results, string(msg))
	}
	require.Len(t, results, 7)
	expMessages := make([]string, 0, 7)
	for i, msg := range messages {
		if i < 3 {
			continue
		}
		expMessages = append(expMessages, msg)
	}
	assert.Equal(t, expMessages, results)

	// now check we can get the message that was sent after the subscription was created
	msg := readMessage(t, subscriberConn)
	assert.Equal(t, "hello there", string(msg))
}

func readMessage(t *testing.T, subscriberConn net.Conn) []byte {
	var topicLen uint16
	err := binary.Read(subscriberConn, binary.BigEndian, &topicLen)
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

	err = binary.Write(subscriberConn, binary.BigEndian, Ack)
	require.NoError(t, err)

	return buf
}
