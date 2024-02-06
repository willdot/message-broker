package server

import (
	"fmt"
	"sync"
)

type MemoryStore struct {
	mu     sync.Mutex
	msgs   map[int]MessageToSend
	offset int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		msgs: make(map[int]MessageToSend),
	}
}

func (m *MemoryStore) Write(msg MessageToSend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.msgs[m.offset] = msg

	m.offset++

	return nil
}

func (m *MemoryStore) ReadFrom(offset int, handleFunc func(msgs []MessageToSend)) error {
	if offset < 0 || offset > m.offset {
		return fmt.Errorf("invalid offset provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	msgs := make([]MessageToSend, 0, len(m.msgs))
	for i := offset; i < len(m.msgs); i++ {
		msgs = append(msgs, m.msgs[i])
	}

	handleFunc(msgs)

	return nil
}