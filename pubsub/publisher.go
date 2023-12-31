package pubsub

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/willdot/messagebroker/server"
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
		err := binary.Write(p.conn, binary.BigEndian, uint32(len(topic)))
		if err != nil {
			return fmt.Errorf("failed to write topic size to server")
		}

		_, err = p.conn.Write([]byte(topic))
		if err != nil {
			return fmt.Errorf("failed to write topic to server")
		}

		err = binary.Write(p.conn, binary.BigEndian, uint32(len(message.Data)))
		if err != nil {
			return fmt.Errorf("failed to write message size to server")
		}

		_, err = p.conn.Write(message.Data)
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
