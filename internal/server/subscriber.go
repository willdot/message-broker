package server

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/willdot/messagebroker/internal"
)

type subscriber struct {
	peer          *Peer
	topic         string
	messages      chan internal.Message
	unsubscribeCh chan struct{}

	ackDelay   time.Duration
	ackTimeout time.Duration
}

func newSubscriber(peer *Peer, topic *topic, ackDelay, ackTimeout time.Duration, startAt int) *subscriber {
	s := &subscriber{
		peer:          peer,
		topic:         topic.name,
		messages:      make(chan internal.Message),
		ackDelay:      ackDelay,
		ackTimeout:    ackTimeout,
		unsubscribeCh: make(chan struct{}, 1),
	}

	go s.sendMessages()

	go func() {
		topic.messageStore.ReadFrom(startAt, func(msg internal.Message) {
			select {
			case s.messages <- msg:
				return
			case <-s.unsubscribeCh:
				return
			}
		})
	}()

	return s
}

func (s *subscriber) sendMessages() {
	for {
		select {
		case <-s.unsubscribeCh:
			return
		case msg := <-s.messages:
			ack, err := s.sendMessage(s.topic, msg)
			if err != nil {
				slog.Error("failed to send to message", "error", err, "peer", s.peer.Addr())
			}

			if ack {
				continue
			}

			if msg.DeliveryCount >= 5 {
				slog.Error("max delivery count for message. Dropping", "peer", s.peer.Addr())
				continue
			}

			msg.DeliveryCount++
			s.addMessage(msg, s.ackDelay)
		}
	}
}

func (s *subscriber) addMessage(msg internal.Message, delay time.Duration) {
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-s.unsubscribeCh:
			return
		case <-timer.C:
			s.messages <- msg
		}
	}()
}

func (s *subscriber) sendMessage(topic string, msg internal.Message) (bool, error) {
	var ack bool
	op := func(conn net.Conn) error {
		topicB := make([]byte, 2)
		binary.BigEndian.PutUint16(topicB, uint16(len(topic)))

		headers := topicB
		headers = append(headers, []byte(topic)...)

		// TODO: if message is empty, return error?
		dataLenB := make([]byte, 8)
		binary.BigEndian.PutUint64(dataLenB, uint64(len(msg.Data)))
		headers = append(headers, dataLenB...)

		_, err := conn.Write(append(headers, msg.Data...))
		if err != nil {
			return fmt.Errorf("failed to write to peer: %w", err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(s.ackTimeout)); err != nil {
			slog.Error("failed to set connection read deadline", "error", err, "peer", s.peer.Addr())
		}
		defer func() {
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				slog.Error("failed to reset connection read deadline", "error", err, "peer", s.peer.Addr())
			}
		}()
		var ackRes Action
		err = binary.Read(conn, binary.BigEndian, &ackRes)
		if err != nil {
			return fmt.Errorf("failed to read ack from peer: %w", err)
		}

		if ackRes == Ack {
			ack = true
		}

		return nil
	}

	err := s.peer.RunConnOperation(op)

	return ack, err
}

func (s *subscriber) unsubscribe() {
	close(s.unsubscribeCh)
}
