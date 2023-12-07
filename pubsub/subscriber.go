package pubsub

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/willdot/messagebroker"
	"github.com/willdot/messagebroker/server"
)

// Subscriber allows subscriptions to a server and the consumption of messages
type Subscriber struct {
	conn net.Conn
}

// NewSubscriber will connect to the server at the given address
func NewSubscriber(addr string) (*Subscriber, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &Subscriber{
		conn: conn,
	}, nil
}

// Close cleanly shuts down the subscriber
func (s *Subscriber) Close() error {
	return s.conn.Close()
}

// SubscribeToTopics will subscribe to the provided topics
func (s *Subscriber) SubscribeToTopics(topicNames []string) error {
	err := binary.Write(s.conn, binary.BigEndian, server.Subscribe)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	b, err := json.Marshal(topicNames)
	if err != nil {
		return fmt.Errorf("failed to marshal topic names: %w", err)
	}

	err = binary.Write(s.conn, binary.BigEndian, uint32(len(b)))
	if err != nil {
		return fmt.Errorf("failed to write topic data length: %w", err)
	}

	_, err = s.conn.Write(b)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	var resp server.Status
	err = binary.Read(s.conn, binary.BigEndian, &resp)
	if err != nil {
		return fmt.Errorf("failed to read confirmation of subscription: %w", err)
	}

	if resp == server.Subscribed {
		return nil
	}

	var dataLen uint32
	err = binary.Read(s.conn, binary.BigEndian, &dataLen)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	buf := make([]byte, dataLen)
	_, err = s.conn.Read(buf)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	return fmt.Errorf("received status %s - %s", resp, buf)
}

// UnsubscribeToTopics will unsubscribe to the provided topics
func (s *Subscriber) UnsubscribeToTopics(topicNames []string) error {
	err := binary.Write(s.conn, binary.BigEndian, server.Unsubscribe)
	if err != nil {
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	b, err := json.Marshal(topicNames)
	if err != nil {
		return fmt.Errorf("failed to marshal topic names: %w", err)
	}

	err = binary.Write(s.conn, binary.BigEndian, uint32(len(b)))
	if err != nil {
		return fmt.Errorf("failed to write topic data length: %w", err)
	}

	_, err = s.conn.Write(b)
	if err != nil {
		return fmt.Errorf("failed to unsubscribe to topics: %w", err)
	}

	var resp server.Status
	err = binary.Read(s.conn, binary.BigEndian, &resp)
	if err != nil {
		return fmt.Errorf("failed to read confirmation of unsubscription: %w", err)
	}

	if resp == server.Unsubscribed {
		return nil
	}

	var dataLen uint32
	err = binary.Read(s.conn, binary.BigEndian, &dataLen)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	buf := make([]byte, dataLen)
	_, err = s.conn.Read(buf)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	return fmt.Errorf("received status %s - %s", resp, buf)
}

// Consumer allows the consumption of messages. It is thread safe to range over the Msgs channel to consume. If during the consumer
// receiving messages from the server an error occurs, it will be stored in Err
type Consumer struct {
	Msgs chan messagebroker.Message
	// TODO: better error handling? Maybe a channel of errors?
	Err error
}

// Consume will create a consumer and start it running in a go routine. You can then use the Msgs channel of the consumer
// to read the messages
func (s *Subscriber) Consume(ctx context.Context) *Consumer {
	consumer := &Consumer{
		Msgs: make(chan messagebroker.Message),
	}

	go s.consume(ctx, consumer)

	return consumer
}

func (s *Subscriber) consume(ctx context.Context, consumer *Consumer) {
	defer close(consumer.Msgs)
	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := s.readMessage()
		if err != nil {
			consumer.Err = err
			return
		}

		if msg != nil {
			consumer.Msgs <- *msg
		}
	}
}

func (s *Subscriber) readMessage() (*messagebroker.Message, error) {
	err := s.conn.SetReadDeadline(time.Now().Add(time.Second))
	if err != nil {
		return nil, err
	}

	var dataLen uint64
	err = binary.Read(s.conn, binary.BigEndian, &dataLen)
	if err != nil {
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			return nil, nil
		}
		return nil, err
	}

	if dataLen <= 0 {
		return nil, nil
	}

	buf := make([]byte, dataLen)
	_, err = s.conn.Read(buf)
	if err != nil {
		return nil, err
	}

	var msg messagebroker.Message
	err = json.Unmarshal(buf, &msg)
	if err != nil {
		slog.Error("failed to unmarshal message", "error", err)
		return nil, nil
	}

	return &msg, nil
}
