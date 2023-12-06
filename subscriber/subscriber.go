package subscriber

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

type Subscriber struct {
	conn net.Conn
}

func New(addr string) (*Subscriber, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &Subscriber{
		conn: conn,
	}, nil
}

func (s *Subscriber) Close() error {
	return s.conn.Close()
}

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
	buf := make([]byte, 512)
	_, err = s.conn.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read confirmation of subscription: %w", err)
	}

	// TODO: this is soooo hacky - need to have some sort of response code
	if string(buf[:10]) != "subscribed" {
		return fmt.Errorf("failed to subscribe: '%s'", string(buf))
	}

	return nil
}

type Consumer struct {
	Msgs chan messagebroker.Message
	Err  error
}

// TODO: maybe buffer the message channel up?
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
