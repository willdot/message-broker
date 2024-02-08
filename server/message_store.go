package server

import (
	"sync"
)

// Memory store allows messages to be stored in memory
type MemoryStore struct {
	mu         sync.Mutex
	msgs       map[int]message
	nextOffset int
}

// New memory store initializes a new in memory store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		msgs: make(map[int]message),
	}
}

// Write will write the provided message to the in memory store
func (m *MemoryStore) Write(msg message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.msgs[m.nextOffset] = msg

	m.nextOffset++

	return nil
}

// ReadFrom will read messages from (and including) the provided offset and pass them to the provided handler
func (m *MemoryStore) ReadFrom(offset int, handleFunc func(msg message)) {
	if offset < 0 || offset >= m.nextOffset {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i := offset; i < len(m.msgs); i++ {
		handleFunc(m.msgs[i])
	}
}
