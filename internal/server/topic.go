package server

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/willdot/messagebroker/internal"
	"github.com/willdot/messagebroker/internal/messagestore"
)

type Store interface {
	Write(msg internal.Message) error
	ReadFrom(offset int, handleFunc func(msg internal.Message))
}

type topic struct {
	name          string
	subscriptions map[net.Addr]*subscriber
	mu            sync.Mutex
	messageStore  Store
}

func newTopic(name string, db *bolt.DB) *topic {
	// messageStore := messagestore.NewMemoryStore()
	store, err := messagestore.NewFileStore(name, db)
	if err != nil {
		slog.Error("failed to create filestore, reverting to in memory store", "error", err)
		store := messagestore.NewMemoryStore()
		return &topic{
			name:          name,
			subscriptions: make(map[net.Addr]*subscriber),
			messageStore:  store,
		}
	}

	return &topic{
		name:          name,
		subscriptions: make(map[net.Addr]*subscriber),
		messageStore:  store,
	}
}

func (t *topic) sendMessageToSubscribers(msg internal.Message) error {
	err := t.messageStore.Write(msg)
	if err != nil {
		return fmt.Errorf("failed to write message to store: %w", err)
	}

	t.mu.Lock()
	subscribers := t.subscriptions
	t.mu.Unlock()

	for _, subscriber := range subscribers {
		subscriber.addMessage(msg, 0)
	}

	return nil
}

func (t *topic) findSubscription(addr net.Addr) *subscriber {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.subscriptions[addr]
}

func (t *topic) removeSubscription(addr net.Addr) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.subscriptions, addr)
}
