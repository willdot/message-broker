package client

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/willdot/messagebroker/internal/server"
)

// Publisher allows messages to be published to a server
type Publisher struct {
	conn   net.Conn
	connMu sync.Mutex
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
func (p *Publisher) PublishMessage(message *Message) error {
	op := func(conn net.Conn) error {
		// send topic first
		topic := fmt.Sprintf("topic:%s", message.Topic)

		topicLenB := make([]byte, 2)
		binary.BigEndian.PutUint16(topicLenB, uint16(len(topic)))

		headers := append(topicLenB, []byte(topic)...)

		messageLenB := make([]byte, 4)
		binary.BigEndian.PutUint32(messageLenB, uint32(len(message.Data)))
		headers = append(headers, messageLenB...)

		_, err := conn.Write(append(headers, message.Data...))
		if err != nil {
			return fmt.Errorf("failed to publish data to server")
		}
		return nil
	}

	return p.connOperation(op)
}

func (p *Publisher) connOperation(op connOpp) error {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	return op(p.conn)
}
