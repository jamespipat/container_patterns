package main

import (
	"bufio"
	"bytes"
	"net"
	"time"
)

// pool.go holds a tiny per-shard connection pool. Each backend command borrows
// an idle connection (or dials a new one), does a single request/reply
// roundtrip while holding it exclusively, then returns it. Because a
// connection is held for the whole roundtrip, it is safe to share the pool
// across concurrent client goroutines.

const (
	dialTimeout = 2 * time.Second
	opTimeout   = 5 * time.Second // per request/reply roundtrip
	maxIdle     = 16              // idle conns kept per shard
)

type dialFunc func(addr string) (net.Conn, error)

func tcpDial(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, dialTimeout)
}

// conn is a backend connection plus its buffered reader (which may hold bytes
// read past a reply boundary, so it must travel with the connection).
type conn struct {
	nc net.Conn
	br *bufio.Reader
}

func (c *conn) close() { _ = c.nc.Close() }

// roundtrip writes one command and reads back exactly one reply, returning the
// reply's raw bytes for verbatim relay.
func (c *conn) roundtrip(cmd []byte) ([]byte, error) {
	if err := c.nc.SetDeadline(time.Now().Add(opTimeout)); err != nil {
		return nil, err
	}
	if _, err := c.nc.Write(cmd); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := readReply(c.br, &buf); err != nil {
		return nil, err
	}
	if err := c.nc.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type pool struct {
	addr string
	dial dialFunc
	idle chan *conn
}

func newPool(addr string, dial dialFunc) *pool {
	return &pool{addr: addr, dial: dial, idle: make(chan *conn, maxIdle)}
}

// fresh dials a brand-new connection.
func (p *pool) fresh() (*conn, error) {
	nc, err := p.dial(p.addr)
	if err != nil {
		return nil, err
	}
	return &conn{nc: nc, br: bufio.NewReader(nc)}, nil
}

func (p *pool) put(c *conn) {
	select {
	case p.idle <- c:
	default:
		c.close() // pool full; drop it
	}
}

// do runs one command against the shard. A pooled connection may have been
// closed by the server since it was last used, so a reused connection gets one
// retry on a fresh connection before the error is surfaced.
func (p *pool) do(cmd []byte) ([]byte, error) {
	select {
	case c := <-p.idle:
		reply, err := c.roundtrip(cmd)
		if err == nil {
			p.put(c)
			return reply, nil
		}
		c.close() // stale/broken idle conn; fall through to a fresh dial
	default:
	}

	c, err := p.fresh()
	if err != nil {
		return nil, err
	}
	reply, err := c.roundtrip(cmd)
	if err != nil {
		c.close()
		return nil, err
	}
	p.put(c)
	return reply, nil
}

// closeAll drains and closes idle connections (best-effort, called on shutdown).
func (p *pool) closeAll() {
	for {
		select {
		case c := <-p.idle:
			c.close()
		default:
			return
		}
	}
}
