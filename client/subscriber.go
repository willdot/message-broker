package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/willdot/messagebroker/internal/server"
)

type connOpp func(conn net.Conn) error

// Subscriber allows subscriptions to a server and the consumption of messages
type Subscriber struct {
	conn             net.Conn
	connMu           sync.Mutex
	subscribedTopics []string
	addr             string
}

// NewSubscriber will connect to the server at the given address
func NewSubscriber(addr string) (*Subscriber, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &Subscriber{
		conn: conn,
		addr: addr,
	}, nil
}

func (s *Subscriber) reconnect() error {
	conn, err := net.Dial("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	s.conn = conn
	return nil
}

// Close cleanly shuts down the subscriber
func (s *Subscriber) Close() error {
	return s.conn.Close()
}

// SubscribeToTopics will subscribe to the provided topics
func (s *Subscriber) SubscribeToTopics(topicNames []string, startAtType server.StartAtType, startAtIndex int) error {
	op := func(conn net.Conn) error {
		return subscribeToTopics(conn, topicNames, startAtType, startAtIndex)
	}

	err := s.connOperation(op)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	s.addToSubscribedTopics(topicNames)

	return nil
}

func (s *Subscriber) addToSubscribedTopics(topics []string) {
	existingSubs := make(map[string]struct{})
	for _, topic := range s.subscribedTopics {
		existingSubs[topic] = struct{}{}
	}

	for _, topic := range topics {
		existingSubs[topic] = struct{}{}
	}

	subs := make([]string, 0, len(existingSubs))
	for topic := range existingSubs {
		subs = append(subs, topic)
	}

	s.subscribedTopics = subs
}

func (s *Subscriber) removeTopicsFromSubscription(topics []string) {
	existingSubs := make(map[string]struct{})
	for _, topic := range s.subscribedTopics {
		existingSubs[topic] = struct{}{}
	}

	for _, topic := range topics {
		delete(existingSubs, topic)
	}

	subs := make([]string, 0, len(existingSubs))
	for topic := range existingSubs {
		subs = append(subs, topic)
	}

	s.subscribedTopics = subs
}

// UnsubscribeToTopics will unsubscribe to the provided topics
func (s *Subscriber) UnsubscribeToTopics(topicNames []string) error {
	op := func(conn net.Conn) error {
		return unsubscribeToTopics(conn, topicNames)
	}

	err := s.connOperation(op)
	if err != nil {
		return fmt.Errorf("failed to unsubscribe to topics: %w", err)
	}

	s.removeTopicsFromSubscription(topicNames)

	return nil
}

func subscribeToTopics(conn net.Conn, topicNames []string, startAtType server.StartAtType, startAtIndex int) error {
	actionB := make([]byte, 2)
	binary.BigEndian.PutUint16(actionB, uint16(server.Subscribe))
	headers := actionB

	b, err := json.Marshal(topicNames)
	if err != nil {
		return fmt.Errorf("failed to marshal topic names: %w", err)
	}

	topicNamesB := make([]byte, 4)
	binary.BigEndian.PutUint32(topicNamesB, uint32(len(b)))
	headers = append(headers, topicNamesB...)
	headers = append(headers, b...)

	startAtTypeB := make([]byte, 2)
	binary.BigEndian.PutUint16(startAtTypeB, uint16(startAtType))
	headers = append(headers, startAtTypeB...)

	if startAtType == server.From {
		fromB := make([]byte, 2)
		binary.BigEndian.PutUint16(fromB, uint16(startAtIndex))
		headers = append(headers, fromB...)
	}

	_, err = conn.Write(headers)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	var resp server.Status
	err = binary.Read(conn, binary.BigEndian, &resp)
	if err != nil {
		return fmt.Errorf("failed to read confirmation of subscribe: %w", err)
	}

	if resp == server.Subscribed {
		return nil
	}

	var dataLen uint16
	err = binary.Read(conn, binary.BigEndian, &dataLen)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	buf := make([]byte, dataLen)
	_, err = conn.Read(buf)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	return fmt.Errorf("received status %s - %s", resp, buf)
}

func unsubscribeToTopics(conn net.Conn, topicNames []string) error {
	actionB := make([]byte, 2)
	binary.BigEndian.PutUint16(actionB, uint16(server.Unsubscribe))
	headers := actionB

	b, err := json.Marshal(topicNames)
	if err != nil {
		return fmt.Errorf("failed to marshal topic names: %w", err)
	}

	topicNamesB := make([]byte, 4)
	binary.BigEndian.PutUint32(topicNamesB, uint32(len(b)))
	headers = append(headers, topicNamesB...)

	_, err = conn.Write(append(headers, b...))
	if err != nil {
		return fmt.Errorf("failed to unsubscribe to topics: %w", err)
	}

	var resp server.Status
	err = binary.Read(conn, binary.BigEndian, &resp)
	if err != nil {
		return fmt.Errorf("failed to read confirmation of unsubscribe: %w", err)
	}

	if resp == server.Unsubscribed {
		return nil
	}

	var dataLen uint16
	err = binary.Read(conn, binary.BigEndian, &dataLen)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	buf := make([]byte, dataLen)
	_, err = conn.Read(buf)
	if err != nil {
		return fmt.Errorf("received status %s:", resp)
	}

	return fmt.Errorf("received status %s - %s", resp, buf)
}

// Consumer allows the consumption of messages. If during the consumer receiving messages from the
// server an error occurs, it will be stored in Err
type Consumer struct {
	msgs chan *Message
	// TODO: better error handling? Maybe a channel of errors?
	Err error
}

// Messages returns a channel in which this consumer will put messages onto. It is safe to range over the channel since it will be closed once
// the consumer has finished either due to an error or from being cancelled.
func (c *Consumer) Messages() <-chan *Message {
	return c.msgs
}

// Consume will create a consumer and start it running in a go routine. You can then use the Msgs channel of the consumer
// to read the messages
func (s *Subscriber) Consume(ctx context.Context) *Consumer {
	consumer := &Consumer{
		msgs: make(chan *Message),
	}

	go s.consume(ctx, consumer)

	return consumer
}

func (s *Subscriber) consume(ctx context.Context, consumer *Consumer) {
	defer close(consumer.msgs)
	for {
		if ctx.Err() != nil {
			return
		}

		err := s.readMessage(ctx, consumer.msgs)
		if err == nil {
			continue
		}

		// if we couldn't connect to the server, attempt to reconnect
		if !errors.Is(err, syscall.EPIPE) && !errors.Is(err, io.EOF) {
			slog.Error("failed to read message", "error", err)
			consumer.Err = err
			return
		}

		slog.Info("attempting to reconnect")

		for i := 0; i < 5; i++ {
			time.Sleep(time.Millisecond * 500)
			err = s.reconnect()
			if err == nil {
				break
			}

			slog.Error("Failed to reconnect", "error", err, "attempt", i)
		}

		slog.Info("attempting to resubscribe")

		err = s.SubscribeToTopics(s.subscribedTopics, server.Current, 0)
		if err != nil {
			consumer.Err = fmt.Errorf("failed to subscribe to topics after reconnecting: %w", err)
			return
		}

	}
}

func (s *Subscriber) readMessage(ctx context.Context, msgChan chan *Message) error {
	op := func(conn net.Conn) error {
		err := s.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 300))
		if err != nil {
			return err
		}

		var topicLen uint16
		err = binary.Read(s.conn, binary.BigEndian, &topicLen)
		if err != nil {
			// TODO: check if this is needed elsewhere. I'm not sure where the read deadline resets....
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				return nil
			}
			return err
		}

		topicBuf := make([]byte, topicLen)
		_, err = s.conn.Read(topicBuf)
		if err != nil {
			return err
		}

		var dataLen uint64
		err = binary.Read(s.conn, binary.BigEndian, &dataLen)
		if err != nil {
			return err
		}

		if dataLen <= 0 {
			return nil
		}

		dataBuf := make([]byte, dataLen)
		_, err = s.conn.Read(dataBuf)
		if err != nil {
			return err
		}

		msg := NewMessage(string(topicBuf), dataBuf)

		msgChan <- msg

		var ack bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ack = <-msg.ack:
		}
		ackMessage := server.Nack
		if ack {
			ackMessage = server.Ack
		}

		err = binary.Write(s.conn, binary.BigEndian, ackMessage)
		if err != nil {
			return fmt.Errorf("failed to ack/nack message: %w", err)
		}

		return nil
	}

	err := s.connOperation(op)
	if err != nil {
		var neterr net.Error
		if errors.As(err, &neterr) && neterr.Timeout() {
			return nil
		}
		return err
	}

	return err
}

func (s *Subscriber) connOperation(op connOpp) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	return op(s.conn)
}
