package server

import (
	"encoding/binary"
	"fmt"
	"net"
)

type peer struct {
	conn net.Conn
}

func newPeer(conn net.Conn) peer {
	return peer{
		conn: conn,
	}
}

// Read wraps the peers underlying connections Read function to satisfy io.Reader
func (p *peer) Read(b []byte) (n int, err error) {
	return p.conn.Read(b)
}

// Write wraps the peers underlying connections Write function to satisfy io.Writer
func (p *peer) Write(b []byte) (n int, err error) {
	return p.conn.Write(b)
}

func (p *peer) addr() net.Addr {
	return p.conn.LocalAddr()
}

func (p *peer) readAction() (Action, error) {
	var action Action
	err := binary.Read(p.conn, binary.BigEndian, &action)
	if err != nil {
		return 0, fmt.Errorf("failed to read action from peer: %w", err)
	}

	return action, nil
}

func (p *peer) readDataLength() (uint32, error) {
	var dataLen uint32
	err := binary.Read(p.conn, binary.BigEndian, &dataLen)
	if err != nil {
		return 0, fmt.Errorf("failed to read data length from peer: %w", err)
	}

	return dataLen, nil
}
