package main

import (
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

func (t *topic) addSubscriber(conn net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()

	slog.Info("adding subscriber", "conn", conn.LocalAddr())
	t.subscriptions[conn.LocalAddr()] = Subscriber{conn: conn}
}

func (t *topic) removeSubscriber(addr net.Addr) {
	t.mu.Lock()
	defer t.mu.Unlock()

	slog.Info("removing subscriber", "conn", addr)
	delete(t.subscriptions, addr)
}

func (t *topic) sendMessageToSubscribers(msg message) {
	t.mu.Lock()
	subscribers := t.subscriptions
	t.mu.Unlock()

	for addr, subscriber := range subscribers {
		err := subscriber.SendMessage(msg.Data)
		if err != nil {
			slog.Error("failed to send to message", "error", err, "conn", addr)
			continue
		}
	}
}
