package server

import (
	"net"
	"sync"
)

type topic struct {
	name          string
	subscriptions map[net.Addr]*subscriber
	mu            sync.Mutex
}

func newTopic(name string) *topic {
	return &topic{
		name:          name,
		subscriptions: make(map[net.Addr]*subscriber),
	}
}

func (t *topic) sendMessageToSubscribers(msgData []byte) {
	t.mu.Lock()
	subscribers := t.subscriptions
	t.mu.Unlock()

	for _, subscriber := range subscribers {
		subscriber.addMessage(newMessage(msgData), 0)
	}
}
