package messagestore

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/willdot/messagebroker/internal"
)

// FileStore allows messages to be stored to file
type FileStore struct {
	db *bolt.DB

	topicName string
	mu        sync.Mutex
	offset    int
}

// NewFileStore initializes a new file store
func NewFileStore(topicName string, db *bolt.DB) (*FileStore, error) {
	var offset int
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(topicName))
		if err != nil {
			return err
		}

		stats := bucket.Stats()

		offset = stats.KeyN - 1
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create topic bucket: %w", err)
	}

	slog.Info(fmt.Sprintf("current offset: %d", offset))

	return &FileStore{
		db:        db,
		topicName: topicName,
		offset:    offset,
	}, nil
}

// Write will write the provided message to the file store
func (m *FileStore) Write(msg internal.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(m.topicName))

		err := bucket.Put([]byte(fmt.Sprintf("%d", m.offset)), msg.Data)
		if err != nil {
			return err
		}

		m.offset++

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to put message in bucket: %w", err)
	}

	return nil
}

// ReadFrom will read messages from (and including) the provided offset and pass them to the provided handler
func (m *FileStore) ReadFrom(offset int, handleFunc func(msg internal.Message)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_ = m.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(m.topicName))

		for i := offset; i < m.offset; i++ {
			data := bucket.Get([]byte(fmt.Sprintf("%d", i)))
			handleFunc(internal.NewMessage(data))
		}

		return nil
	})
}
