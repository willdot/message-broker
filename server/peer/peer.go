package peer

import (
	"net"
	"sync"
)

// Peer represents a remote connection to the server such as a publisher or subscriber
type Peer struct {
	conn   net.Conn
	connMu sync.Mutex
}

// New returns a new peer
func New(conn net.Conn) *Peer {
	return &Peer{
		conn: conn,
	}
}

// Addr returns the peers connections address
func (p *Peer) Addr() net.Addr {
	return p.conn.RemoteAddr()
}

// ConnOpp represents a set of actions on a connection that can be used synchrnously
type ConnOpp func(conn net.Conn) error

// RunConnOperation will run the provided operation. It ensures that it is the only operation that is being
// run on the connection to ensure any other operations don't get mixed up.
func (p *Peer) RunConnOperation(op ConnOpp) error {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	return op(p.conn)
}
