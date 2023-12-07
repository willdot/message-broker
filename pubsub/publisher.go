package pubsub

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"

	"github.com/willdot/messagebroker"
	"github.com/willdot/messagebroker/server"
)

// Publisher allows messages to be published to a server
type Publisher struct {
	conn net.Conn
}

// NewPublisher connects to the server at the given address and registers as a publisher
func NewPublisher(addr string) (*Publisher, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	err = binary.Write(conn, binary.BigEndian, server.Publish)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to register publish to server: %w", err)
	}

	return &Publisher{
		conn: conn,
	}, nil
}

// Close cleanly shuts down the publisher
func (p *Publisher) Close() error {
	return p.conn.Close()
}

// Publish will publish the given message to the server
func (p *Publisher) PublishMessage(message messagebroker.Message) error {
	b, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	err = binary.Write(p.conn, binary.BigEndian, uint32(len(b)))
	if err != nil {
		return fmt.Errorf("failed to write message size to server")
	}

	_, err = p.conn.Write(b)
	if err != nil {
		return fmt.Errorf("failed to publish data to server")
	}

	return nil
}
