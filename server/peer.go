package server

import (
	"log/slog"
	"net"
	"sync"

	"github.com/google/uuid"
)

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

type peer struct {
	conn   net.Conn
	connMu sync.Mutex
	name   string
}

func newPeer(conn net.Conn) peer {
	return peer{
		conn: conn,
		name: uuid.New().String(),
	}
}

func (p *peer) addr() net.Addr {
	return p.conn.RemoteAddr()
}

type connOpp func(conn net.Conn) error

func (p *peer) connOperation(op connOpp, from string) error {
	slog.Info("operation running", "from", from, "peer", p.conn.RemoteAddr(), "name", p.name, "mu addr", &p.connMu)

	p.connMu.Lock()
	err := op(p.conn)
	p.connMu.Unlock()

	slog.Info("operation finished", "from", from, "peer", p.conn.RemoteAddr(), "name", p.name, "mu addr", &p.connMu)

	return err
}
