// Package transport — the username-to-session registry.
//
// A user who is dropped or disabled must stop being served immediately, not
// at their next AUTH. Review finding 4 corrected an earlier claim that this
// needed only a lookup: an index from username to session does not exist, and
// the layer that owns the connections has to add one (spec 8.8).
//
// The registry is per node: closing here affects only the connections this
// process serves, and nothing is shared across nodes. A drop on node A closes
// A's sessions at once; node B closes its own when it applies the pushed
// config. A session that authenticated in the moment before the change and was
// not yet registered is closed by the listener as it registers: the change
// revokes the session's LoginStanding before it closes the registered
// sessions, and the listener checks that standing after Add, so no ordering
// of the two leaves the connection open. The frame gate's check of the same
// standing stays as the bound on what a revoked login can be served: nothing.
package transport

import (
	"net"
	"sync"
	"sync/atomic"
)

// SessionRegistry maps an authenticated username to that user's live
// sessions on one listener. It is owned by a TCPListener, where conns and the
// write serialisation already live, and is safe for concurrent use.
type SessionRegistry struct {
	mu     sync.Mutex
	byUser map[string]map[*Session]net.Conn

	// beforeAdd is a test seam, nil in production: it runs at the start of
	// every Add, before the session is recorded, so a test can complete an
	// account change after everything the caller did ahead of Add and before
	// the registration itself (TCPListener.SetBeforeRegistrationTestHook).
	beforeAdd atomic.Pointer[func()]
}

// NewSessionRegistry returns an empty registry.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{byUser: make(map[string]map[*Session]net.Conn)}
}

// Add records a session under a username. The same session is replaced, not
// duplicated, so a token refresh cannot leave a stale binding behind.
func (r *SessionRegistry) Add(username string, sess *Session, conn net.Conn) {
	if r == nil || username == "" || sess == nil || conn == nil {
		return
	}
	if hook := r.beforeAdd.Load(); hook != nil {
		(*hook)()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	conns := r.byUser[username]
	if conns == nil {
		conns = make(map[*Session]net.Conn)
		r.byUser[username] = conns
	}
	conns[sess] = conn
}

// Remove drops a session; the connection goroutine calls it on close. It
// reads the session's identity to find the entry, so it is called only after
// AUTH set one (Add is what creates the entry).
func (r *SessionRegistry) Remove(sess *Session) {
	if r == nil || sess == nil {
		return
	}
	identity, ok := sess.Identity()
	if !ok {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	conns := r.byUser[identity.Username]
	if conns == nil {
		return
	}
	delete(conns, sess)
	if len(conns) == 0 {
		delete(r.byUser, identity.Username)
	}
}

// CloseSessionsFor closes every session for username and returns how many it
// closed. It collects the connections under the registry lock, releases it,
// then closes them: a close blocks on the client's socket, and holding a lock
// across it would stall every other caller behind one slow peer. The server
// calls it for a committed drop, disable or password change, and on every
// member applying one, never for grant/revoke (spec 8.8).
func (r *SessionRegistry) CloseSessionsFor(username string) int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	conns := r.byUser[username]
	delete(r.byUser, username)
	r.mu.Unlock()

	for _, conn := range conns {
		// Best effort: a connection already closing is not an error, and the
		// count is what the caller asserts.
		_ = conn.Close()
	}
	return len(conns)
}
