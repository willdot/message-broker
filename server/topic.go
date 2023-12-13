package server

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/willdot/messagebroker/server/peer"
)

type topic struct {
	name          string
	subscriptions map[net.Addr]subscriber
	mu            sync.Mutex
}

type subscriber struct {
	peer          *peer.Peer
	currentOffset int
}

func newTopic(name string) topic {
	return topic{
		name:          name,
		subscriptions: make(map[net.Addr]subscriber),
	}
}

func (t *topic) removeSubscriber(addr net.Addr) {
	t.mu.Lock()
	defer t.mu.Unlock()

	slog.Info("removing subscriber", "peer", addr)
	delete(t.subscriptions, addr)
}

func (t *topic) sendMessageToSubscribers(msgData []byte) {
	t.mu.Lock()
	subscribers := t.subscriptions
	t.mu.Unlock()

	for addr, subscriber := range subscribers {
		err := subscriber.peer.ConnOperation(sendMessageOp(t.name, msgData))
		if err != nil {
			slog.Error("failed to send to message", "error", err, "peer", addr)
			return
		}
	}
}

func sendMessageOp(topic string, data []byte) peer.ConnOpp {
	return func(conn net.Conn) error {
		topicLen := uint64(len(topic))
		err := binary.Write(conn, binary.BigEndian, topicLen)
		if err != nil {
			return fmt.Errorf("failed to send topic length: %w", err)
		}
		_, err = conn.Write([]byte(topic))
		if err != nil {
			return fmt.Errorf("failed to send topic: %w", err)
		}

		dataLen := uint64(len(data))

		err = binary.Write(conn, binary.BigEndian, dataLen)
		if err != nil {
			return fmt.Errorf("failed to send data length: %w", err)
		}

		_, err = conn.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write to peer: %w", err)
		}
		return nil
	}
}
