package main

import (
	"fmt"
	"net"
)

type Subscriber struct {
	conn          net.Conn
	currentOffset int
}

func (s *Subscriber) SendMessage(data []byte) error {
	_, err := s.conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to connection: %w", err)
	}
	return nil
}
