package server

import (
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

	for _, subscriber := range subscribers {
		// TODO: this should be done in a differnt go routine so that all subscribers get the message at the same time.
		// However we will need to find a way to cancel the go routine
		subscriber.peer.sendMessage(msg)
	}
}
