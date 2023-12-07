package server

import (
	"encoding/json"
	"log/slog"
	"net"
	"sync"

	"github.com/willdot/messagebroker"
)

type topic struct {
	name          string
	subscriptions map[net.Addr]subscriber
	mu            sync.Mutex
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

func (t *topic) sendMessageToSubscribers(msg messagebroker.Message) {
	t.mu.Lock()
	subscribers := t.subscriptions
	t.mu.Unlock()

	msgData, err := json.Marshal(msg)
	if err != nil {
		slog.Error("failed to marshal message for subscribers", "error", err)
	}

	for addr, subscriber := range subscribers {
		err := subscriber.sendMessage(msgData)
		if err != nil {
			slog.Error("failed to send to message", "error", err, "peer", addr)
			continue
		}
	}
}
