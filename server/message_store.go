package server

import (
	"fmt"
	"sync"
)

// Memory store allows messages to be stored in memory
type MemoryStore struct {
	mu     sync.Mutex
	msgs   map[int]message
	offset int
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

	m.msgs[m.offset] = msg

	m.offset++

	return nil
}

// ReadFrom will read messages from (and including) the provided offset and pass them to the provided handler
func (m *MemoryStore) ReadFrom(offset int, handleFunc func(msg message)) error {
	if offset < 0 || offset > m.offset {
		return fmt.Errorf("invalid offset provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i := offset; i < len(m.msgs); i++ {
		handleFunc(m.msgs[i])
	}

	return nil
}
