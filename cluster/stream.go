package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	"github.com/hoarddb/hoarddb-server/transport"
)

// handshakeTimeout bounds how long an inbound connection may take to present
// its handshake before it is dropped.
const handshakeTimeout = 5 * time.Second

// Defaults of the Raft channel's bounds; config.DefaultClusterConfig carries the same
// numbers and a test keeps the two equal (this package cannot import config).
const (
	defaultRaftMaxConns = 64
	defaultRaftIdle     = 2 * time.Minute
)

// RaftLimits bound what the Raft channel holds for peers that have connected. A zero field
// takes its default.
type RaftLimits struct {
	// MaxConns is the most inbound connections held at once, handshaking or admitted; one
	// past it is closed at accept. Each connection carries one RPC at a time, so this
	// also bounds the Raft frames in flight.
	MaxConns int
	// Idle is how long an admitted connection may stay silent before it is closed. It is
	// refreshed by every read, so a leader's heartbeats keep a healthy connection open; a
	// peer that went quiet loses it, and the library redials on its next RPC.
	Idle time.Duration
}

func (l RaftLimits) withDefaults() RaftLimits {
	if l.MaxConns <= 0 {
		l.MaxConns = defaultRaftMaxConns
	}
	if l.Idle <= 0 {
		l.Idle = defaultRaftIdle
	}
	return l
}

// localIdentity is what this node says about itself in a handshake.
type localIdentity struct {
	NodeID          string
	Incarnation     uint64
	ClusterID       [16]byte
	Epoch, StateSeq uint64
	// DataAddr is the address this node tells its peers to dial; the cluster-mismatch refusal names it
	// as the seed address the other node should be joined through.
	DataAddr string
	// Range is the state-level range this node declares in every handshake.
	Range StateRange
}

// streamHooks lets the stream layer read the committed state without owning it.
type streamHooks struct {
	local func() localIdentity
	// committedIncarnation reports the incarnation the cluster committed for a
	// node id, and whether the id is a member at all.
	committedIncarnation func(id string) (uint64, bool)
	// admit, when set, is the stricter admission of a node that binds keys to members; it
	// runs after the handshake rules, with the peer's authenticated fingerprint filled in.
	admit func(transport.Handshake) error
}

// streamLayer is the Raft channel: a listener whose connections each begin
// with the 60-byte handshake (conventions section 6.2). The handshake is read
// here, so the library's stream starts after it.
type streamLayer struct {
	listener net.Listener
	hooks    streamHooks
	limits   RaftLimits
	log      hclog.Logger
	// dial opens an outbound connection; nil dials plain TCP.
	dial func(addr string, timeout time.Duration) (net.Conn, error)

	accepted chan net.Conn
	done     chan struct{}
	wg       sync.WaitGroup
	// handlers counts the connections handed to the library that it has not closed yet. Its
	// transport serves each on a goroutine Raft shutdown does not wait for, and that goroutine
	// answers a heartbeat by writing the term to the stable store, so Close waits for them before
	// the caller closes the store.
	handlers  sync.WaitGroup
	mu        sync.Mutex
	live      map[*trackedConn]struct{}
	inbound   int
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func newStreamLayer(listener net.Listener, hooks streamHooks, limits RaftLimits, log hclog.Logger) *streamLayer {
	layer := &streamLayer{
		listener: listener, hooks: hooks, limits: limits.withDefaults(), log: log,
		accepted: make(chan net.Conn), done: make(chan struct{}), live: make(map[*trackedConn]struct{}),
	}
	return layer
}

// start begins accepting. It is separate from newStreamLayer so a caller can finish wiring what the
// admit hook reads (Raft.stream, the dial) before the first connection reaches it.
func (s *streamLayer) start() {
	s.wg.Add(1)
	go s.acceptLoop()
}

func (s *streamLayer) acceptLoop() {
	defer s.wg.Done()
	for {
		raw, err := s.listener.Accept()
		if err != nil {
			return
		}
		conn, err := s.track(raw, true)
		if errors.Is(err, errTooManyRaftConns) {
			continue // the peer redials after its backoff
		}
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.admit(conn)
	}
}

// admit reads and checks one handshake, then hands the connection on or
// closes it.
func (s *streamLayer) admit(conn net.Conn) {
	defer s.wg.Done()
	admitted := false
	defer func() {
		if !admitted {
			_ = conn.Close() // best-effort: the peer was refused or is gone
		}
	}()
	if err := conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return
	}
	hello, err := transport.ReadHandshake(conn)
	if err == nil {
		hello.PeerFingerprint = transport.ConnFingerprint(conn)
		err = s.checkInbound(hello)
		if err == nil && s.hooks.admit != nil {
			err = s.hooks.admit(hello)
		}
	}
	var ranged *rangeRefusal
	if errors.As(err, &ranged) {
		// checkStateRange already logged it, once a minute per member.
		s.log.Debug("refused raft connection", "remote", conn.RemoteAddr().String(), "reason", err.Error())
		return
	}
	if err != nil {
		s.log.Warn("refused raft connection", "remote", conn.RemoteAddr().String(), "reason", err.Error())
		return
	}
	// Every read from here on refreshes the idle deadline. The handlers count is taken while this
	// goroutine is still counted in s.wg, so Close's handlers.Wait comes after every Add.
	s.handlers.Add(1)
	select {
	case s.accepted <- &idleConn{Conn: conn, idle: s.limits.Idle, release: s.handlers.Done}:
		admitted = true
	case <-s.done:
		s.handlers.Done()
	}
}

// checkInbound applies the handshake rules. A cluster id that differs is
// refused only when both sides know theirs: a joiner learns its id from the
// log it has not received yet, so a zero id is accepted from either side. An incarnation older than the committed one is
// a stopped process speaking for a replaced identity.
func (s *streamLayer) checkInbound(hello transport.Handshake) error {
	if hello.Channel != transport.HandshakeChannelRaft {
		return fmt.Errorf("channel %d is not the raft channel", hello.Channel)
	}
	return s.checkIdentity(hello)
}

// checkIdentity is the part of the handshake rules that holds on every channel.
func (s *streamLayer) checkIdentity(hello transport.Handshake) error {
	local := s.hooks.local()
	zero := [16]byte{}
	if local.ClusterID != zero && hello.ClusterID != zero && !bytes.Equal(local.ClusterID[:], hello.ClusterID[:]) {
		return &Error{Code: CodeClusterMismatch, Message: CodeClusterMismatch.Text(CondClusterMismatch, map[string]string{
			"node": hello.NodeID, "node_cluster": fmt.Sprintf("%x", hello.ClusterID[:4]), "this_cluster": fmt.Sprintf("%x", local.ClusterID[:4]),
			"seed_addr": local.DataAddr,
		})}
	}
	if committed, known := s.hooks.committedIncarnation(hello.NodeID); known {
		return hello.CheckIncarnation(committed)
	}
	return nil
}

// handlerDrainTimeout bounds how long Close waits for the library to finish with the connections
// it was handed. A handler returns within one store write once its connection is closed, so the
// bound is only reached by a handler that is stuck.
const handlerDrainTimeout = 5 * time.Second

// errTooManyRaftConns is the internal refusal of a connection past RaftLimits.MaxConns.
var errTooManyRaftConns = errors.New("raft channel is at its connection limit")

// trackedConn removes itself from the layer when closed.
type trackedConn struct {
	net.Conn
	layer   *streamLayer
	inbound bool
	once    sync.Once
}

// idleConn closes a connection that stays silent: every read arms the idle deadline first. It is
// the connection the library holds; its Close is the library's handler finishing with it.
type idleConn struct {
	net.Conn
	idle    time.Duration
	release func()
	once    sync.Once
}

// Close closes the connection and, the first time, tells the layer the library is done with it.
func (c *idleConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// Read arms the idle deadline, then reads.
func (c *idleConn) Read(p []byte) (int, error) {
	if err := c.SetReadDeadline(time.Now().Add(c.idle)); err != nil {
		return 0, fmt.Errorf("arm raft idle deadline: %w", err)
	}
	return c.Conn.Read(p)
}

// Unwrap returns the connection underneath, so the peer's TLS identity can be read.
func (c *idleConn) Unwrap() net.Conn { return c.Conn }

// Unwrap returns the connection underneath, so the peer's TLS identity can be read.
func (c *trackedConn) Unwrap() net.Conn { return c.Conn }

// Close closes the connection once and forgets it.
func (c *trackedConn) Close() error {
	var err error
	c.once.Do(func() {
		c.layer.mu.Lock()
		delete(c.layer.live, c)
		if c.inbound {
			c.layer.inbound--
		}
		c.layer.mu.Unlock()
		err = c.Conn.Close()
	})
	return err
}

// track registers a connection so Close can end it. The library keeps
// pooled connections open after its transport shuts down, and a peer's
// reader goroutine lives as long as the connection does. It closes raw and
// returns net.ErrClosed once the layer is closed, and errTooManyRaftConns for an
// inbound connection past the limit.
func (s *streamLayer) track(raw net.Conn, inbound bool) (*trackedConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = raw.Close() // best-effort: the layer is shutting down
		return nil, net.ErrClosed
	}
	if inbound && s.inbound >= s.limits.MaxConns {
		_ = raw.Close() // best-effort: over the cap
		return nil, errTooManyRaftConns
	}
	conn := &trackedConn{Conn: raw, layer: s, inbound: inbound}
	if inbound {
		s.inbound++
	}
	s.live[conn] = struct{}{}
	return conn, nil
}

// Accept returns the next connection that passed its handshake.
func (s *streamLayer) Accept() (net.Conn, error) {
	select {
	case conn := <-s.accepted:
		return conn, nil
	case <-s.done:
		return nil, net.ErrClosed
	}
}

// Dial opens a connection and sends this node's handshake first.
func (s *streamLayer) Dial(address hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dial := s.dial
	if dial == nil {
		dial = func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("tcp", addr, timeout)
		}
	}
	dialed, err := dial(string(address), timeout)
	if err != nil {
		return nil, fmt.Errorf("dial raft peer %s: %w", address, err)
	}
	conn, err := s.track(dialed, false)
	if err != nil {
		return nil, fmt.Errorf("dial raft peer %s: %w", address, err)
	}
	local := s.hooks.local()
	raw, err := local.Range.Stamp(transport.Handshake{
		Channel: transport.HandshakeChannelRaft, ClusterID: local.ClusterID, Epoch: local.Epoch,
		StateSeq: local.StateSeq, SentAtNs: time.Now().UnixNano(), Incarnation: local.Incarnation, NodeID: local.NodeID,
	}).Encode()
	if err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return nil, errors.Join(fmt.Errorf("set handshake deadline: %w", err), conn.Close())
	}
	if _, err := conn.Write(raw); err != nil {
		return nil, errors.Join(fmt.Errorf("send raft handshake to %s: %w", address, err), conn.Close())
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		return nil, errors.Join(fmt.Errorf("clear handshake deadline: %w", err), conn.Close())
	}
	return conn, nil
}

// Close stops accepting, drops connections still in their handshake and waits
// for every goroutine of the layer and for the library's handler of every connection it
// accepted, so the caller can close the stable store after it.
func (s *streamLayer) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		s.closeErr = s.listener.Close()
		s.mu.Lock()
		s.closed = true
		open := make([]*trackedConn, 0, len(s.live))
		for conn := range s.live {
			open = append(open, conn)
		}
		s.mu.Unlock()
		for _, conn := range open {
			_ = conn.Close() // best-effort: ends the peer's reader and our handshake read
		}
		s.wg.Wait()
		// The closes above fail every handler's next read or write, so each returns and closes
		// its connection; one answering a heartbeat finishes its store write first. The wait is
		// bounded so a handler that never returns cannot hold a node's shutdown forever.
		drained := make(chan struct{})
		go func() { // exits when the last handler returns
			s.handlers.Wait()
			close(drained)
		}()
		select {
		case <-drained:
		case <-time.After(handlerDrainTimeout):
			s.log.Warn("raft connection handlers still running after the stream layer closed", "waited", handlerDrainTimeout.String())
		}
	})
	return s.closeErr
}

// Addr is the listener's address.
func (s *streamLayer) Addr() net.Addr { return s.listener.Addr() }
