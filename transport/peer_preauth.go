package transport

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// The internode port takes a peer slot at accept, before anyone has
// authenticated, because taking it later would move the unauthenticated cost
// into unbounded goroutines and sockets. Two bounds keep that slot from being
// held by someone who never authenticates: a deadline by which a
// connection must have authenticated, and a per-address share of the budget
// for connections that have not yet done so.

// peerPreAuthTimeout is how long an internode connection may stay
// unauthenticated after its TLS handshake. It is the member's own AUTH budget
// (peerAuthTimeout), so the listener never closes a connection whose member is
// still waiting for its AUTH answer: the member's clock starts before the
// listener's, and it gives up first. A shorter idle timeout wins, so a
// connection is never kept unauthenticated longer than it may stay silent.
func peerPreAuthTimeout(idleTimeout time.Duration) time.Duration {
	return min(peerAuthTimeout, idleTimeout)
}

// unauthenticatedPeerShare is the number of unauthenticated internode
// connections one source address may hold at once: a quarter of the peer
// budget, never less than one. A member authenticates as soon as it connects,
// so an honest address holds one unauthenticated socket per pooled socket it
// is dialling. That is InternodePoolSize per member process, and the sockets
// are dialled one at a time except when a pool redials. At the default
// budget of 64 the share is 16: four member processes on one address could
// redial every socket at once, which is a five-node cluster on one loopback.
// The other three quarters stay with every other address.
func unauthenticatedPeerShare(peerBudget int) int {
	return max(1, peerBudget/4)
}

// unauthenticatedPeers counts, per source address, the internode connections
// that hold a peer slot and have not authenticated.
type unauthenticatedPeers struct {
	mu      sync.Mutex
	share   int
	pending map[string]int
}

func newUnauthenticatedPeers(peerBudget int) *unauthenticatedPeers {
	return &unauthenticatedPeers{share: unauthenticatedPeerShare(peerBudget), pending: make(map[string]int)}
}

// admit counts one more unauthenticated connection from host, or reports
// false when host already holds its share. The release func is idempotent,
// because a connection releases its count either when it authenticates or
// when it ends, whichever comes first.
func (u *unauthenticatedPeers) admit(host string) (release func(), ok bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.pending[host] >= u.share {
		return nil, false
	}
	u.pending[host]++
	var once sync.Once
	return func() {
		once.Do(func() {
			u.mu.Lock()
			defer u.mu.Unlock()
			u.pending[host]--
			if u.pending[host] == 0 {
				delete(u.pending, host)
			}
		})
	}, true
}

// remoteHost is the source address without its port, the unit the per-address
// limits count. An address that does not split is used whole, as serveConn's
// per-IP cap does.
func remoteHost(addr net.Addr) string {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// refusal explains to the refused address what happened and what to check.
func (u *unauthenticatedPeers) refusal(host string, peerBudget int) string {
	return fmt.Sprintf("too many unauthenticated connections from %s on the internode port (%d, a quarter of the %d-connection "+
		"peer budget HOARDDB_MAX_PEER_CONNS); a cluster member authenticates as soon as it connects, so retry shortly, and "+
		"if %s is a member check that it holds this cluster's token", host, u.share, peerBudget, host)
}
