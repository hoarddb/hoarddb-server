package transport

import (
	"bufio"
	"crypto/ed25519"
	"crypto/tls"
	"net"
	"sync"
	"time"
)

// PeerMux splits the connections of one internode listener by the channel byte of the
// HDPX handshake (conventions section 6.2), so a node keeps one port for all four
// channels. It reads no more than the handshake's first six bytes; the connection is then
// handed on with those bytes still unread, so each channel's server reads its own
// handshake as before.
//
// Cost: one accept goroutine for the mux, one short-lived goroutine per connection until
// its channel byte arrives, and no state per peer, database or bucket.
type PeerMux struct {
	base   net.Listener
	routes [HandshakeChannelIdentity + 1]*muxListener
	peek   time.Duration
	// pending holds one token per connection in the routing step.
	pending chan struct{}

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// Defaults of the mux's bounds; config.DefaultClusterConfig carries the same numbers.
const (
	// muxPeekTimeout bounds how long a new connection may take to send its channel byte,
	// and how long a routed one waits for its channel's server to accept it.
	muxPeekTimeout = 5 * time.Second
	// muxMaxPending is the most connections the mux holds before they are routed.
	muxMaxPending = 128
)

// PeerMuxLimits bound what the mux holds for connections that have not yet been routed,
// which is everything an unauthenticated caller can make it hold. A zero field takes its
// default.
type PeerMuxLimits struct {
	// MaxPending is the most connections in the routing step at once (reading the
	// channel byte, or waiting for the channel's server); one past it is closed at accept.
	MaxPending int
	// PeekTimeout bounds the routing step of one connection.
	PeekTimeout time.Duration
}

// NewPeerMux starts routing base with the default limits. Close the mux, not base.
func NewPeerMux(base net.Listener) *PeerMux { return NewPeerMuxWithLimits(base, PeerMuxLimits{}) }

// NewPeerMuxWithLimits is NewPeerMux with explicit limits.
func NewPeerMuxWithLimits(base net.Listener, limits PeerMuxLimits) *PeerMux {
	if limits.MaxPending <= 0 {
		limits.MaxPending = muxMaxPending
	}
	if limits.PeekTimeout <= 0 {
		limits.PeekTimeout = muxPeekTimeout
	}
	mux := &PeerMux{base: base, peek: limits.PeekTimeout, pending: make(chan struct{}, limits.MaxPending)}
	for channel := HandshakeChannelData; channel <= HandshakeChannelIdentity; channel++ {
		mux.routes[channel] = &muxListener{addr: base.Addr(), conns: make(chan net.Conn), done: make(chan struct{}), wait: limits.PeekTimeout}
	}
	mux.wg.Add(1)
	go mux.acceptLoop()
	return mux
}

// Listener returns the listener that yields the connections whose handshake names ch.
func (m *PeerMux) Listener(ch Channel) net.Listener { return m.routes[byte(ch)] }

// Addr is the shared address.
func (m *PeerMux) Addr() net.Addr { return m.base.Addr() }

// Close stops accepting, closes the per-channel listeners and waits for the goroutines.
func (m *PeerMux) Close() error {
	m.closeOnce.Do(func() {
		m.closeErr = m.base.Close()
		for _, route := range m.routes {
			if route != nil {
				route.shut()
			}
		}
		m.wg.Wait()
	})
	return m.closeErr
}

func (m *PeerMux) acceptLoop() {
	defer m.wg.Done()
	for {
		raw, err := m.base.Accept()
		if err != nil {
			return
		}
		select {
		case m.pending <- struct{}{}:
		default:
			_ = raw.Close() // best-effort: the routing step is full; the peer redials after its backoff
			continue
		}
		m.wg.Add(1)
		go m.route(raw)
	}
}

// route reads the channel byte and hands the connection to that channel's listener.
func (m *PeerMux) route(raw net.Conn) {
	defer m.wg.Done()
	defer func() { <-m.pending }()
	if err := raw.SetReadDeadline(time.Now().Add(m.peek)); err != nil {
		_ = raw.Close() // best-effort: the connection is unusable
		return
	}
	reader := bufio.NewReaderSize(raw, HandshakeBytes+maxHandshakeNodeID)
	head, err := reader.Peek(6)
	if err != nil || head[5] < HandshakeChannelData || head[5] > HandshakeChannelIdentity {
		_ = raw.Close() // best-effort: not a hoardDB node, or silent
		return
	}
	if err := raw.SetReadDeadline(time.Time{}); err != nil {
		_ = raw.Close() // best-effort: the connection is unusable
		return
	}
	conn := &peekedConn{Conn: raw, reader: reader}
	if !m.routes[head[5]].offer(conn) {
		_ = raw.Close() // best-effort: the channel's listener is closed
	}
}

// peekedConn replays the bytes the mux buffered before the connection's owner reads.
type peekedConn struct {
	net.Conn
	reader *bufio.Reader
}

// Read implements net.Conn over the buffered reader.
func (c *peekedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

// muxListener is the net.Listener of one channel.
type muxListener struct {
	addr      net.Addr
	conns     chan net.Conn
	done      chan struct{}
	wait      time.Duration
	closeOnce sync.Once
}

func (l *muxListener) offer(conn net.Conn) bool {
	wait := time.NewTimer(l.wait)
	defer wait.Stop()
	select {
	case l.conns <- conn:
		return true
	case <-l.done:
		return false
	case <-wait.C:
		return false // nobody serves this channel on this node
	}
}

func (l *muxListener) shut() { l.closeOnce.Do(func() { close(l.done) }) }

// Accept returns the next connection routed to this channel.
func (l *muxListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close ends this channel's listener; the mux keeps serving the others.
func (l *muxListener) Close() error {
	l.shut()
	return nil
}

// Addr is the shared address.
func (l *muxListener) Addr() net.Addr { return l.addr }

// Unwrap returns the connection the buffered reader sits on.
func (c *peekedConn) Unwrap() net.Conn { return c.Conn }

// ConnFingerprint is the fingerprint of the ed25519 certificate the TLS peer of conn
// presented, looking through the mux's and the stream layer's wrappers. It is empty when
// no TLS connection is underneath, or its peer presented no ed25519 certificate.
func ConnFingerprint(conn net.Conn) string {
	for conn != nil {
		if secured, ok := conn.(*tls.Conn); ok {
			certs := secured.ConnectionState().PeerCertificates
			if len(certs) == 0 {
				return ""
			}
			pub, ok := certs[0].PublicKey.(ed25519.PublicKey)
			if !ok {
				return ""
			}
			return Fingerprint(pub)
		}
		inner, ok := conn.(interface{ Unwrap() net.Conn })
		if !ok {
			return ""
		}
		conn = inner.Unwrap()
	}
	return ""
}
