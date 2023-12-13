package peer

import (
	"net"
	"sync"
)

type Peer struct {
	conn   net.Conn
	connMu sync.Mutex
}

func New(conn net.Conn) *Peer {
	return &Peer{
		conn: conn,
	}
}

func (p *Peer) Addr() net.Addr {
	return p.conn.RemoteAddr()
}

type ConnOpp func(conn net.Conn) error

func (p *Peer) ConnOperation(op ConnOpp) error {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	return op(p.conn)
}
