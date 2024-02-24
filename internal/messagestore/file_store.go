package messagestore

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/willdot/messagebroker/internal"
)

type indexEntry struct {
	start   int
	dataLen int
}

// FileStore allows messages to be stored to file
type FileStore struct {
	index map[int]indexEntry
	mu    sync.Mutex

	topicName  string
	nextOffset int

	dataFile  *os.File
	indexFile *os.File
}

// NewFileStore initializes a new file store
func NewFileStore(topicName string) (*FileStore, error) {
	dataFile, err := os.OpenFile(fmt.Sprintf("%s.data", topicName), os.O_CREATE|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return nil, fmt.Errorf("failed to open / create data file: %w", err)
	}
	indexFile, err := os.OpenFile(fmt.Sprintf("%s.index", topicName), os.O_CREATE|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return nil, fmt.Errorf("failed to open / create index file: %w", err)
	}

	// f, err := os.Create(topicName)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create file store file: %w", err)
	// }
	// defer f.Close()

	return &FileStore{
		topicName: topicName,
		index:     make(map[int]indexEntry),
		dataFile:  dataFile,
		indexFile: indexFile,
	}, nil
}

// Write will write the provided message to the file store
func (m *FileStore) Write(msg internal.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// first calculate where to store the data and store it in the index
	lastEntry := m.index[m.nextOffset-1]
	m.index[m.nextOffset] = indexEntry{
		start:   lastEntry.start + lastEntry.dataLen,
		dataLen: len(msg.Data),
	}

	// then write the data
	f, err := os.OpenFile(m.topicName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		delete(m.index, m.nextOffset)
		return fmt.Errorf("failed to write message to file: %w", err)
	}
	defer f.Close()

	_, err = f.Write(msg.Data)
	if err != nil {
		delete(m.index, m.nextOffset)
		return fmt.Errorf("failed to write message to file: %w", err)
	}

	m.nextOffset++

	return nil
}

// ReadFrom will read messages from (and including) the provided offset and pass them to the provided handler
func (m *FileStore) ReadFrom(offset int, handleFunc func(msg internal.Message)) {
	if offset < 0 || offset >= m.nextOffset {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := os.Open(m.topicName)
	if err != nil {
		slog.Error("failed to read topic file", "error", err)
		return
	}
	defer file.Close()

	for i := offset; i < m.nextOffset; i++ {
		idx, ok := m.index[i]
		if !ok {
			continue
		}

		data := make([]byte, idx.dataLen)
		_, err = file.ReadAt(data, int64(idx.start))
		if err != nil && err != io.EOF {
			slog.Error("failed to read at", "error", err)
			return
		}

		handleFunc(internal.NewMessage(data))

		if err == io.EOF {
			break
		}
	}

}
