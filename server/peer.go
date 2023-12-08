package server

import (
	"net"
	"sync"
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
}

func newPeer(conn net.Conn) peer {
	return peer{
		conn: conn,
	}
}

func (p *peer) addr() net.Addr {
	return p.conn.RemoteAddr()
}

type connOpp func(conn net.Conn) error

func (p *peer) connOperation(op connOpp, from string) error {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	return op(p.conn)
}
