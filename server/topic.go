package server

import (
	"fmt"
	"net"
	"sync"
)

type topic struct {
	name          string
	subscriptions map[net.Addr]*subscriber
	mu            sync.Mutex
	messageStore  Store
}

func newTopic(name string, messageStore Store) *topic {
	return &topic{
		name:          name,
		subscriptions: make(map[net.Addr]*subscriber),
		messageStore:  messageStore,
	}
}

func (t *topic) sendMessageToSubscribers(msg MessageToSend) error {
	err := t.messageStore.Write(msg)
	if err != nil {
		return fmt.Errorf("failed to write message to store: %w", err)
	}

	t.mu.Lock()
	subscribers := t.subscriptions
	t.mu.Unlock()

	for _, subscriber := range subscribers {
		subscriber.addMessage(newMessage(msg.data), 0)
	}

	return nil
}
