package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The user-table follower of a cluster member (ring user store spec 6.3): one goroutine per node,
// woken through a coalescing channel after each applied command, that installs the cluster's
// user table into this node's AuthManager when the table changed and ends the logins the change
// revokes. Its watermark is the state_seq of the state it last looked at, which the binding waits
// on before it answers (F5), and which hoarddb_auth_config_version reports in cluster mode.
//
// Rule 11: one goroutine, one channel and one watermark per node; nothing per bucket or store.
// A wake that finds the same table (copy-on-write: a changed table is a new slice) costs one
// identity check and one lock; an install is O(users) and happens only when a user command
// committed or a snapshot was installed.

// userTableSource is where the follower reads the applied table: the ring node.
type userTableSource interface {
	Users() cluster.UserView
	UserTableChanged() <-chan struct{}
}

// userFollower follows one node's user table.
type userFollower struct {
	am            *AuthManager
	source        userTableSource
	closeSessions func(username string) int
	logger        *slog.Logger

	// mu guards the watermark and its waiters' channel, closed and replaced whenever the
	// watermark moves.
	mu        sync.Mutex
	installed int64
	moved     chan struct{}
	// last is the table this follower installed last; only the follower goroutine touches it.
	last    []cluster.UserRow
	started bool
	// installs counts installs, for the test that a commit leaving the table alone installs nothing.
	installs atomic.Int64
	done     chan struct{}
}

func newUserFollower(am *AuthManager, source userTableSource, closeSessions func(string) int, logger *slog.Logger) *userFollower {
	return &userFollower{am: am, source: source, closeSessions: closeSessions, logger: logger, moved: make(chan struct{}), done: make(chan struct{})}
}

// start installs the table as applied now, then follows it until ctx ends.
func (f *userFollower) start(ctx context.Context) {
	go func() {
		defer close(f.done)
		f.step()
		for {
			select {
			case <-ctx.Done():
				return
			case <-f.source.UserTableChanged():
				f.step()
			}
		}
	}()
}

// wait returns once the follower goroutine has exited (its context ended).
func (f *userFollower) wait() { <-f.done }

// step reads the applied table and installs it when it is not the one installed last.
func (f *userFollower) step() {
	view := f.source.Users()
	var revoked []string
	if !f.started || !sameUserTable(view.Users, f.last) {
		revoked = f.am.installClusterUsers(view)
		f.last = view.Users
		f.started = true
		f.installs.Add(1)
	} else {
		f.am.noteClusterStateSeq(view.StateSeq)
	}
	f.advance(view.StateSeq)
	for _, name := range revoked {
		if closed := f.closeSessions(name); closed > 0 {
			f.logger.Info("closed sessions for a user whose access the cluster revoked", "username", name, "closed", closed)
		}
	}
}

// advance moves the watermark to seq and wakes every waiter.
func (f *userFollower) advance(seq int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if seq <= f.installed {
		return
	}
	f.installed = seq
	close(f.moved)
	f.moved = make(chan struct{})
}

// awaitInstalled returns once the follower has installed a state at or after seq, or ctx's error.
func (f *userFollower) awaitInstalled(ctx context.Context, seq int64) error {
	for {
		f.mu.Lock()
		installed, moved := f.installed, f.moved
		f.mu.Unlock()
		if installed >= seq {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("this node has installed the users up to state_seq %d, not %d: %w", installed, seq, ctx.Err())
		case <-moved:
		}
	}
}

// sameUserTable reports whether a and b are the same committed slice: copy-on-write gives every
// changed table a new backing array, and the follower keeps the last one alive, so its address
// cannot be reused by another table.
func sameUserTable(a, b []cluster.UserRow) bool {
	if len(a) != len(b) {
		return false
	}
	return len(a) == 0 || &a[0] == &b[0]
}

// clusterUserRecord is a row of the cluster's table as the login path reads it. The install never
// refuses a row (a check here would make this node's view differ from another's).
func clusterUserRecord(row cluster.UserRow) *UserRecord {
	roles := make([]Grant, len(row.Roles))
	for i, grant := range row.Roles {
		roles[i] = Grant{Role: grant.Role, DB: grant.DB}
	}
	return &UserRecord{
		Username: row.Name,
		Roles:    roles,
		Credentials: CredentialMaterial{
			Algo: row.Algo, Salt: base64.StdEncoding.EncodeToString(row.Salt), Hash: base64.StdEncoding.EncodeToString(row.Hash),
			Params: Argon2Params{Memory: uint32(row.Memory), Iterations: uint32(row.Iterations), Parallelism: uint8(row.Parallelism),
				SaltLen: len(row.Salt), HashLen: len(row.Hash)},
			PepperKID: row.PepperKID,
		},
		Enabled:           row.Enabled,
		Epoch:             uint32(row.Epoch),
		CreatedAt:         time.UnixMilli(row.CreatedMs).UTC(),
		PasswordChangedAt: time.UnixMilli(row.PasswordChangedMs).UTC(),
		LoginGeneration:   uint32(row.LoginGeneration),
		createdSeq:        row.CreatedSeq,
		changedSeq:        row.ChangedSeq,
	}
}

// installClusterUsers replaces a cluster member's records with the table in view and returns the
// accounts whose logins it ended: dropped, disabled, re-created (another created_seq) or given a
// new login generation. The store's version is the view's state_seq and its drop fence the view's
// user_drop_seq, so both bindings report one meaning (ring user store spec 4.3, R3).
func (am *AuthManager) installClusterUsers(view cluster.UserView) []string {
	incoming := make(map[string]*UserRecord, len(view.Users))
	for _, row := range view.Users {
		incoming[row.Name] = clusterUserRecord(row)
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	var revoked []string
	for name, current := range am.users {
		next, kept := incoming[name]
		if !kept || !next.Enabled || !sameLogin(current, next) {
			am.revokeLoginsLocked(name)
			revoked = append(revoked, name)
		}
	}
	am.users = incoming
	am.version = uint64(view.StateSeq)
	am.dropSeq = view.DropSeq
	am.loaded = true
	am.lastSync = time.Now()
	am.updateMetricsLocked()
	return revoked
}

// noteClusterStateSeq records a commit that left the user table alone: the version, and so the
// gauge, follow the state_seq the follower looked at.
func (am *AuthManager) noteClusterStateSeq(seq int64) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if uint64(seq) > am.version {
		am.version = uint64(seq)
		am.updateMetricsLocked()
	}
}
