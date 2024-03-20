package messagestore

import (
	"fmt"

	"github.com/boltdb/bolt"
)

type Store struct {
	db *bolt.DB
}

func NewStore(file bool) (*Store, error) {
	if !file {
		// memStore := NewMemoryStore()

		return &Store{}, nil
	}

	db, err := bolt.Open("store.db", 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database store: %w", err)
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Close() {
	if s.db != nil {
		_ = s.db.Close()
	}
}
