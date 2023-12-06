package messagebroker

import (
	"encoding/binary"
	"fmt"
)

type Subscriber struct {
	peer          peer
	currentOffset int
}

func (s *Subscriber) SendMessage(msg []byte) error {
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
