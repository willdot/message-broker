package messagestore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/messagebroker/internal"
)

func TestFileStoreWritesMesssages(t *testing.T) {
	fs, err := NewFileStore("testfile")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll("testfile")
	})

	err = fs.Write(internal.Message{Data: []byte("message 1")})
	require.NoError(t, err)
	err = fs.Write(internal.Message{Data: []byte("message 2")})
	require.NoError(t, err)
	err = fs.Write(internal.Message{Data: []byte("message 3")})
	require.NoError(t, err)

	assert.Len(t, fs.index, 3)

	assert.Equal(t, 0, fs.index[0].start)
	assert.Equal(t, 9, fs.index[0].dataLen)

	assert.Equal(t, 9, fs.index[1].start)
	assert.Equal(t, 9, fs.index[1].dataLen)

	assert.Equal(t, 18, fs.index[2].start)
	assert.Equal(t, 9, fs.index[2].dataLen)

	res, err := os.ReadFile("testfile")
	require.NoError(t, err)
	assert.Equal(t, "message 1message 2message 3", string(res))
}

func TestFileStoreReadsFrom(t *testing.T) {
	fs, err := NewFileStore("testfile")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll("testfile")
	})

	err = os.WriteFile("testfile", []byte("message 1message 2message 3"), os.ModeAppend)
	require.NoError(t, err)

	fs.index[0] = indexEntry{start: 0, dataLen: 9}
	fs.index[1] = indexEntry{start: 9, dataLen: 9}
	fs.index[2] = indexEntry{start: 18, dataLen: 9}
	fs.nextOffset = 3

	results := make([]internal.Message, 0, 2)
	fs.ReadFrom(1, func(msg internal.Message) {
		results = append(results, msg)
	})

	expected := []internal.Message{
		internal.NewMessage([]byte("message 2")),
		internal.NewMessage([]byte("message 3")),
	}

	assert.EqualValues(t, expected, results)
}
