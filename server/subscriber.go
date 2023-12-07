package server

import (
	"encoding/binary"
	"fmt"
)

type subscriber struct {
	peer          peer
	currentOffset int
}

func (s *subscriber) sendMessage(msg []byte) error {
	dataLen := uint64(len(msg))

	err := binary.Write(&s.peer, binary.BigEndian, dataLen)
	if err != nil {
		return fmt.Errorf("failed to send data length: %w", err)
	}

	_, err = s.peer.Write(msg)
	if err != nil {
		return fmt.Errorf("failed to write to peer: %w", err)
	}
	return nil
}
