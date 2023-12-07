package server

import (
	"encoding/binary"
	"fmt"
	"log/slog"
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

// Status represents the status of a request
type Status uint8

const (
	Subscribed   = 1
	Unsubscribed = 2
	Error        = 3
)

func (s Status) String() string {
	switch s {
	case Subscribed:
		return "subsribed"
	case Unsubscribed:
		return "unsubscribed"
	case Error:
		return "error"
	}

	return ""
}

func (p *peer) writeStatus(status Status, message string) {
	err := binary.Write(p.conn, binary.BigEndian, status)
	if err != nil {
		slog.Error("failed to write status to peers connection", "error", err, "peer", p.addr())
		return
	}

	if message == "" {
		return
	}

	msgBytes := []byte(message)
	err = binary.Write(p.conn, binary.BigEndian, uint32(len(msgBytes)))
	if err != nil {
		slog.Error("failed to write message length to peers connection", "error", err, "peer", p.addr())
		return
	}

	_, err = p.conn.Write(msgBytes)
	if err != nil {
		slog.Error("failed to write message to peers connection", "error", err, "peer", p.addr())
		return
	}
}
