package messagebroker

import (
	"encoding/json"
	"log/slog"
	"net"
	"sync"
)

type topic struct {
	name          string
	subscriptions map[net.Addr]Subscriber
	mu            sync.Mutex
}

func newTopic(name string) topic {
	return topic{
		name:          name,
		subscriptions: make(map[net.Addr]Subscriber),
	}
}

func (t *topic) removeSubscriber(addr net.Addr) {
	t.mu.Lock()
	defer t.mu.Unlock()

	slog.Info("removing subscriber", "peer", addr)
	delete(t.subscriptions, addr)
}

func (t *topic) sendMessageToSubscribers(msg Message) {
	t.mu.Lock()
	subscribers := t.subscriptions
	t.mu.Unlock()

	msgData, err := json.Marshal(msg)
	if err != nil {
		slog.Error("failed to marshal message for subscribers", "error", err)
	}

	for addr, subscriber := range subscribers {
		err := subscriber.SendMessage(msgData)
		if err != nil {
			slog.Error("failed to send to message", "error", err, "peer", addr)
			continue
		}
	}
}
