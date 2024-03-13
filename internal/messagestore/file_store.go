package messagestore

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/willdot/messagebroker/internal"
)

// FileStore allows messages to be stored to file
type FileStore struct {
	db *bolt.DB

	topicName string
}

// NewFileStore initializes a new file store
func NewFileStore(topicName string, db *bolt.DB) (*FileStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(topicName))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create topic bucket: %w", err)
	}

	return &FileStore{
		db:        db,
		topicName: topicName,
	}, nil
}

// Write will write the provided message to the file store
func (m *FileStore) Write(msg internal.Message) error {
	err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(m.topicName))

		offset, err := bucket.NextSequence()
		if err != nil {
			return fmt.Errorf("failed to get the next sequence in bucket")
		}

		err = bucket.Put([]byte(fmt.Sprintf("%d", offset)), msg.Data)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to put message in bucket: %w", err)
	}

	return nil
}

// ReadFrom will read messages from (and including) the provided offset and pass them to the provided handler
func (m *FileStore) ReadFrom(offset int, handleFunc func(msg internal.Message)) {
	_ = m.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(m.topicName))

		stats := bucket.Stats()

		for i := offset; i < stats.KeyN-1; i++ {
			data := bucket.Get([]byte(fmt.Sprintf("%d", i)))
			handleFunc(internal.NewMessage(data))
		}

		return nil
	})
}
