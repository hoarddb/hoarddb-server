package transport

import "sync"

// ConnLimiter enforces the server-wide connection caps (spec 5.6) with
// split budgets: client connections draw on MaxConns and peer (node-to-node)
// connections draw on the reserved MaxPeerConns budget. The split is what
// keeps a driver flood on the client port from starving replication — a
// peer connection is never refused because the client budget is full.
//
// The same limiter instance is shared by both TCP listeners (driver and
// internode) so they cannot disagree about the ceiling they are enforcing.
type ConnLimiter struct {
	clientSlots chan struct{} // capacity: MaxConns
	peerSlots   chan struct{} // capacity: MaxPeerConns
}

// NewConnLimiter creates a limiter with the given client and peer budgets.
func NewConnLimiter(maxClient, maxPeer int) *ConnLimiter {
	if maxClient < 1 {
		maxClient = 1
	}
	if maxPeer < 1 {
		maxPeer = 1
	}
	return &ConnLimiter{
		clientSlots: make(chan struct{}, maxClient),
		peerSlots:   make(chan struct{}, maxPeer),
	}
}

// AcquireClient reserves one client-budget slot for the connection's
// lifetime. The release func must be deferred immediately — including on
// error paths — so a crash mid-accept cannot leak capacity. It reports
// ok=false when the client budget is full.
func (l *ConnLimiter) AcquireClient() (release func(), ok bool) {
	select {
	case l.clientSlots <- struct{}{}:
		return l.releaser(l.clientSlots), true
	default:
		return nil, false
	}
}

// AcquirePeer reserves one peer-budget slot. It reports ok=false only when
// the peer budget itself is full — never because the client budget is,
// which is the whole point of the reservation: a client flood cannot block
// replication.
func (l *ConnLimiter) AcquirePeer() (release func(), ok bool) {
	select {
	case l.peerSlots <- struct{}{}:
		return l.releaser(l.peerSlots), true
	default:
		return nil, false
	}
}

// releaser returns a release closure wrapped in a sync.Once so a double
// release (e.g. a deferred close racing a manual one on an error path)
// cannot return a slot twice — that would silently let the cap grow.
func (l *ConnLimiter) releaser(slots chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { <-slots }) }
}

// Counts returns the number of client and peer slots currently held.
func (l *ConnLimiter) Counts() (clients, peers int) {
	return len(l.clientSlots), len(l.peerSlots)
}
