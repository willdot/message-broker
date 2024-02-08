package messagestore

import (
	"sync"

	"github.com/willdot/messagebroker/internal"
)

// MemoryStore allows messages to be stored in memory
type MemoryStore struct {
	mu         sync.Mutex
	msgs       map[int]internal.Message
	nextOffset int
}

// NewMemoryStore initializes a new in memory store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		msgs: make(map[int]internal.Message),
	}
}

// Write will write the provided message to the in memory store
func (m *MemoryStore) Write(msg internal.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.msgs[m.nextOffset] = msg

	m.nextOffset++

	return nil
}

// ReadFrom will read messages from (and including) the provided offset and pass them to the provided handler
func (m *MemoryStore) ReadFrom(offset int, handleFunc func(msg internal.Message)) {
	if offset < 0 || offset >= m.nextOffset {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i := offset; i < len(m.msgs); i++ {
		handleFunc(m.msgs[i])
	}
}
