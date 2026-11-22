package wire

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	jitter "math/rand/v2"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The cluster pepper on a ring node (ring user store spec 5): the node that proposes USER_BOOTSTRAP
// generates it; every other data member fetches it from a member that holds it, over the pinned
// control channel, and keeps it in its own credential file. Witnesses never ask for it and are
// refused it. It is never proposed, logged or replicated.
//
// Rule 11: one 32-byte file, one atomic pointer and at most one goroutine per node (the fetcher,
// which exits once the node holds the state's key and never runs on a witness); nothing per
// bucket, store or user. A joiner fetches the key once: one request and one reply, O(1) in
// buckets and users.

// PepperFile is where a node keeps the cluster pepper: auth.cluster.pepper in its data directory,
// sealed under the node master when the node seals its credential files (the server's
// implementation), or the 0600 plaintext form (clusterpepper.File).
type PepperFile interface {
	// Load reads the pepper; a missing file is an error matching fs.ErrNotExist.
	Load() (clusterpepper.Pepper, error)
	// Store writes the pepper durably: it is on disk when Store returns.
	Store(clusterpepper.Pepper) error
	// MoveAside keeps a wrong pepper as auth.cluster.pepper.wrong-<kid> and returns that path; it
	// never deletes it.
	MoveAside(kid string) (string, error)
}

// The values of a node's password_key status (spec 5.2): describe_node and health report one.
const (
	// PasswordKeyHeld: the node holds the key the state names; it serves logins.
	PasswordKeyHeld = "held"
	// PasswordKeyFetching: the state names a key this data node does not hold yet.
	PasswordKeyFetching = "fetching"
	// PasswordKeyNoneYet: the cluster has no users, so no key exists yet.
	PasswordKeyNoneYet = "none_yet"
	// PasswordKeyWitness: a witness holds no key and serves no logins.
	PasswordKeyWitness = "witness"
)

const (
	// pepperFirstBackoff and pepperMaxBackoff bound the fetcher's wait between rounds; a change of
	// the state's kid wakes it at once (B2).
	pepperFirstBackoff = time.Second
	pepperMaxBackoff   = 30 * time.Second
	// pepperAskTimeout bounds one PEPPER_REQUEST.
	pepperAskTimeout = 5 * time.Second
)

// ErrNoPasswordKey is what every PasswordKeyError matches: this node cannot check or make a
// cluster password hash now.
var ErrNoPasswordKey = errors.New("no cluster password key on this node")

// PasswordKeyError is why a node cannot check a password now, with the rule 3 text for the case
// (spec 6.2): no users yet, fetching, or witness.
type PasswordKeyError struct {
	// Status is one of PasswordKeyNoneYet, PasswordKeyFetching or PasswordKeyWitness.
	Status string
	text   string
}

func (e *PasswordKeyError) Error() string { return e.text }

// Unwrap lets errors.Is match ErrNoPasswordKey.
func (e *PasswordKeyError) Unwrap() error { return ErrNoPasswordKey }

// The no-key texts of spec 6.2. Their facts are fixed by the spec; 'nodes' parses in the ring CLI.
const (
	noUsersYetText = "this cluster has no users yet, so no login can succeed. Set HOARDDB_ROOT_PASSWORD on one data node (not a witness) and start it; that node creates the first administrator."
	fetchingText   = "password logins are not available on this node yet: it has not received the cluster's password key (auth.cluster.pepper) from another member. " +
		"It asks again whenever the cluster changes and at least every 30 seconds; log in on another node meanwhile. " +
		"If this lasts, run 'nodes' on another node to check that the others are up, and look for \"pepper\" in this node's log. " +
		noHolderFix
	// noHolderFix is the one way out when no member can hand the key over (spec 6.2, review F2).
	noHolderFix = "If no data node holds the key any more, restore auth.cluster.pepper from a backup of this node's own data directory and restart it: " +
		"with encryption at rest on, only this node's own backup works, because a sealed copy opens only on the node that sealed it."
	witnessNoKeyText = "this node is a witness: it holds no password key and serves no logins or statements. Connect to a data node instead; 'nodes' on any data node lists them."
)

// pepperKeeper is a node's cluster pepper: the file, the key it holds (nil until loaded, generated
// or fetched), and the fetcher's wake. Readers load the pointer without a lock; a wrong key is
// cleared before its file is moved, so a reader sees nil or one whole key, never a wrong one after
// the finding (spec 7.1).
type pepperKeeper struct {
	file PepperFile
	held atomic.Pointer[clusterpepper.Pepper]
	// mu orders generation, the wrong-key heal and the fetcher's store, so two of them never write
	// the file at once.
	mu sync.Mutex
	// wake is signalled, without blocking, when the state's kid changes.
	wake chan struct{}
	// seenKID is the state kid the apply hook last saw; only the FSM goroutine touches it.
	seenKID string
	// ask sends one PEPPER_REQUEST; startCluster sets it to the control channel's, a test replaces it.
	ask func(ctx context.Context, member string, req transport.PepperRequest) (transport.PepperReply, error)
	// log writes an operator line; startCluster points it at the node's logger.
	log func(line string)
	// stuckKID names a wrong key whose file could not be moved aside; under mu. While it is set the
	// node stores no fetched key, so the wrong file is never renamed over (review F6).
	stuckKID string
	// derived caches the key and kid of the held pepper for the cluster id it was derived for; it
	// is used only while its pepper is still the one held, so a cleared key is never used.
	derived atomic.Pointer[derivedKey]
}

// derivedKey is the key a held pepper gives for one cluster id, and that pepper's kid.
type derivedKey struct {
	from      *clusterpepper.Pepper
	kid       string
	clusterID [16]byte
	key       clusterpepper.Key
}

// heldDerived returns the held pepper's kid and its key for clusterID, deriving them once per
// held pepper; ok is false when no pepper is held.
func (k *pepperKeeper) heldDerived(clusterID [16]byte) (string, clusterpepper.Key, bool) {
	held := k.heldKey()
	if held == nil {
		return "", clusterpepper.Key{}, false
	}
	if cached := k.derived.Load(); cached != nil && cached.from == held && cached.clusterID == clusterID {
		return cached.kid, cached.key, true
	}
	fresh := &derivedKey{from: held, kid: held.KID(), clusterID: clusterID, key: held.Key(clusterID)}
	k.derived.Store(fresh)
	return fresh.kid, fresh.key, true
}

// newPepperKeeper loads the pepper from file. A missing file is no error: the node fetches the key
// once the state names one. An unreadable file (wrong node master, damaged) is the credential
// refusal, with its restore text.
func newPepperKeeper(file PepperFile) (*pepperKeeper, error) {
	k := &pepperKeeper{file: file, wake: make(chan struct{}, 1), log: func(string) {}}
	p, err := file.Load()
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, fmt.Errorf("reading the cluster's password key: %w", err)
	default:
		k.held.Store(&p)
	}
	return k, nil
}

func (k *pepperKeeper) signal() {
	select {
	case k.wake <- struct{}{}:
	default:
	}
}

// heldKey is the key this keeper holds, or nil.
func (k *pepperKeeper) heldKey() *clusterpepper.Pepper {
	if k == nil {
		return nil
	}
	return k.held.Load()
}

// notePepperKID is called by the apply hook on the FSM goroutine: it wakes the fetcher when the
// state's kid changed (spec 5.3, B2). It never blocks.
func (c *clusterParts) notePepperKID(kid string) {
	k := c.pepper
	if k == nil || kid == k.seenKID {
		return
	}
	k.seenKID = kid
	k.signal()
}

// PasswordKeyStatus is this node's password_key: held, fetching, none_yet or witness; empty on a
// node that is not a cluster member (zero-config keeps users.json and needs no cluster key).
func (n *Node) PasswordKeyStatus() string {
	if n.cl == nil || (n.cl.pepper == nil && !n.cfg.Witness) {
		return ""
	}
	if n.cfg.Witness {
		return PasswordKeyWitness
	}
	clusterID, kid := n.raft.PepperIdentity()
	if kid == "" {
		return PasswordKeyNoneYet
	}
	if heldKID, _, ok := n.cl.pepper.heldDerived(clusterID); ok && heldKID == kid {
		return PasswordKeyHeld
	}
	return PasswordKeyFetching
}

// PasswordKey returns the key this node checks and makes cluster password hashes with, and its
// kid, which is always the state's. A node that cannot (no users yet, the key not fetched yet, or a
// witness) gets a *PasswordKeyError with the case's text, and must refuse before it runs Argon2
// (spec 5.5).
func (n *Node) PasswordKey() (clusterpepper.Key, string, error) {
	switch status := n.PasswordKeyStatus(); status {
	case PasswordKeyHeld:
	case PasswordKeyWitness:
		return clusterpepper.Key{}, "", &PasswordKeyError{Status: status, text: witnessNoKeyText}
	case PasswordKeyNoneYet:
		return clusterpepper.Key{}, "", &PasswordKeyError{Status: status, text: noUsersYetText}
	case "":
		return clusterpepper.Key{}, "", &PasswordKeyError{Status: PasswordKeyFetching, text: "this node keeps no cluster password key: it is not a cluster member, so it checks passwords against its own users.json"}
	default:
		return clusterpepper.Key{}, "", &PasswordKeyError{Status: PasswordKeyFetching, text: fetchingText}
	}
	clusterID, stateKID := n.raft.PepperIdentity()
	// The state's kid is fixed once set, and the pointer is only ever set to the state's key after
	// it is set; a key read between the status and here that no longer matches is refused rather
	// than used. The cache is keyed on the held pointer, so a cleared key is never returned.
	heldKID, key, ok := n.cl.pepper.heldDerived(clusterID)
	if !ok || heldKID != stateKID {
		return clusterpepper.Key{}, "", &PasswordKeyError{Status: PasswordKeyFetching, text: fetchingText}
	}
	return key, stateKID, nil
}

// FirstAdministratorReady reports whether this node may make a pepper and propose the cluster's
// first administrator now (ring user store spec 6.5), and the cluster id the root hash is peppered
// for: it knows a leader, and its own row is a committed data member that is joining, active or
// down (as servePepper serves; a leaving or dead member creates nothing). A joiner
// that has not been admitted, or has not applied the state that admits it, sees an empty table
// that is not the cluster's, so it waits; a pepper made then would be a wrong one.
func (n *Node) FirstAdministratorReady() ([16]byte, bool) {
	if n.cfg.Witness || n.cl == nil || n.raft.Leader() == "" {
		return [16]byte{}, false
	}
	view := n.raft.PepperView()
	self, member := view.Member(n.cfg.NodeID)
	live := self.State == cluster.MemberJoining || self.State == cluster.MemberActive || self.State == cluster.MemberDown
	return view.ClusterID, member && !self.Witness && live
}

// BootstrapPepper is the pepper this node peppers its USER_BOOTSTRAP root hash with (spec 5.2):
// the one it holds while the state names no kid (a crash before the commit keeps it), or a new one
// it generates and writes durably first. It refuses on a witness, on a node that keeps no pepper
// file, and once the state names a kid, because then the cluster's key exists and this node
// fetches it instead.
func (n *Node) BootstrapPepper() (clusterpepper.Pepper, error) {
	if n.cfg.Witness {
		return clusterpepper.Pepper{}, errors.New("this node is a witness: it holds no password key, so it never creates the cluster's first administrator. Set HOARDDB_ROOT_PASSWORD on a data node instead")
	}
	if n.cl == nil || n.cl.pepper == nil {
		return clusterpepper.Pepper{}, errors.New("this node keeps no cluster password key: it is not a cluster member")
	}
	k := n.cl.pepper
	k.mu.Lock()
	defer k.mu.Unlock()
	if kid := n.raft.Users().PepperKID; kid != "" {
		return clusterpepper.Pepper{}, fmt.Errorf("this cluster already has a password key (key %s), so this node does not create another; it fetches the cluster's from a member", kid)
	}
	if held := k.held.Load(); held != nil {
		return *held, nil
	}
	p, err := clusterpepper.Generate()
	if err != nil {
		return clusterpepper.Pepper{}, err
	}
	if err := k.file.Store(p); err != nil {
		return clusterpepper.Pepper{}, fmt.Errorf("writing the cluster's new password key before using it: %w", err)
	}
	k.held.Store(&p)
	k.log(fmt.Sprintf("created the cluster's password key (key %s) for the first administrator; the cluster's password key has one copy until another data node fetches it", p.KID()))
	return p, nil
}

// servePepper answers a PEPPER_REQUEST (spec 5.3). The control channel admitted the asker as a
// pinned, committed member of this cluster when the connection opened; a pooled connection can
// outlive a REMOVE_MEMBER, so the asker's committed row is read again here for every request (F12).
func (n *Node) servePepper(_ context.Context, from transport.Handshake, req transport.PepperRequest) transport.PepperReply {
	reply := transport.PepperReply{Nonce: req.Nonce}
	refuse := func(text string) transport.PepperReply {
		reply.Status, reply.Message = transport.PepperRefused, text
		if n.cl != nil && n.cl.pepper != nil {
			n.cl.pepper.log("refused the cluster's password key: " + text)
		}
		return reply
	}
	view := n.raft.PepperView()
	asker, found := view.Member(from.NodeID)
	switch {
	case !found:
		return refuse(fmt.Sprintf("%s is not a member of this cluster in this node's state, so it is not given the cluster's password key. Run 'nodes' to see the members", from.NodeID))
	case asker.Witness:
		return refuse(fmt.Sprintf("%s is a witness: it serves no client logins and is not given the cluster's password key.", from.NodeID))
	case asker.State != cluster.MemberJoining && asker.State != cluster.MemberActive && asker.State != cluster.MemberDown:
		return refuse(fmt.Sprintf("%s is %s: a member that is leaving or dead is not given the cluster's password key", from.NodeID, asker.State))
	}
	var held *clusterpepper.Pepper
	if n.cl != nil {
		held = n.cl.pepper.heldKey()
	}
	if held == nil || view.PepperKID == "" || held.KID() != view.PepperKID {
		reply.Status = transport.PepperNotHeld
		return reply
	}
	reply.Status, reply.KID = transport.PepperOK, held.KID()
	reply.Pepper = transport.NewPepperKey(held.Bytes())
	n.cl.pepper.log(fmt.Sprintf("handed the cluster's password key (key %s) to %s", reply.KID, from.NodeID))
	return reply
}

// startsPepperFetcher reports whether this node runs the fetcher: a cluster member that keeps a
// pepper file and is not configured as a witness (F16).
func (n *Node) startsPepperFetcher() bool {
	return n.cl != nil && n.cl.pepper != nil && !n.cfg.Witness
}

// runPepperFetcher is the node's one pepper goroutine (spec 5.3): it heals a wrong key and fetches
// the state's, woken by a change of the state's kid and, while a kid exists and no member gave the
// key, at least every 30 seconds. While the state names no kid there is nothing to fetch, so it
// waits for the kid alone. It exits once the node holds the state's key or the node closes.
func (n *Node) runPepperFetcher(ctx context.Context) {
	k := n.cl.pepper
	backoff := pepperFirstBackoff
	for {
		if n.pepperStep(ctx) {
			return
		}
		if n.raft.Users().PepperKID == "" {
			select {
			case <-ctx.Done():
				return
			case <-k.wake:
			}
			continue
		}
		timer := time.NewTimer(spreadWait(backoff))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-k.wake:
			timer.Stop()
		case <-timer.C:
			backoff = min(2*backoff, pepperMaxBackoff)
		}
	}
}

// spreadWait spreads d over [d/2, d), so data members that missed one round do not ask in step.
func spreadWait(d time.Duration) time.Duration {
	return d/2 + jitter.N(d/2)
}

// pepperStep is one round of the fetcher. It reports true when the node holds the state's key (or
// never will hold one, as a witness), so the goroutine can exit.
func (n *Node) pepperStep(ctx context.Context) bool {
	if n.cfg.Witness || n.cl == nil || n.cl.pepper == nil {
		return true
	}
	k := n.cl.pepper
	view := n.raft.PepperView()
	if view.PepperKID == "" {
		// No user exists yet: there is no key to fetch, and a key this node holds is a bootstrap
		// candidate, not a wrong one.
		return false
	}
	if held := k.held.Load(); held != nil {
		if held.KID() == view.PepperKID {
			return true
		}
		if err := n.healWrongPepper(*held, view.PepperKID); err != nil {
			k.log(err.Error())
			return false
		}
	}
	if err := n.retryStuckMove(view.PepperKID); err != nil {
		k.log(err.Error())
		return false
	}
	if _, member := view.Member(n.cfg.NodeID); !member {
		return false
	}
	return n.fetchPepper(ctx, view)
}

// healWrongPepper stops using a key the state does not name and keeps its file aside (spec 5.4).
// No committed hash can use a key whose kid is not the state's, so the file is useless here.
func (n *Node) healWrongPepper(wrong clusterpepper.Pepper, stateKID string) error {
	k := n.cl.pepper
	k.mu.Lock()
	defer k.mu.Unlock()
	if held := k.held.Load(); held == nil || !held.Equal(wrong) {
		return nil
	}
	k.held.Store(nil)
	k.derived.Store(nil)
	k.stuckKID = wrong.KID()
	return k.moveStuckLocked(stateKID)
}

// retryStuckMove moves aside a wrong file an earlier round could not move.
func (n *Node) retryStuckMove(stateKID string) error {
	k := n.cl.pepper
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.stuckKID == "" {
		return nil
	}
	return k.moveStuckLocked(stateKID)
}

// moveStuckLocked moves the wrong file named by stuckKID aside; k.mu is held. Until it succeeds,
// stuckKID stays set and no fetched key is stored over the file.
func (k *pepperKeeper) moveStuckLocked(stateKID string) error {
	wrongKID := k.stuckKID
	aside, err := k.file.MoveAside(wrongKID)
	switch {
	case errors.Is(err, clusterpepper.ErrSourceGone):
		// Moved by someone else (the operator, as the text below asks): the file is no longer where
		// a fetched key would be written, so nothing can be overwritten. Not retried: a rename of a
		// file that is gone fails for ever.
		k.stuckKID = ""
		k.log(fmt.Sprintf("this node's auth.cluster.pepper (key %s) is no longer at its path (moved by hand); the cluster's key (key %s) is being fetched from another member. Logins on this node resume when it arrives.",
			wrongKID, stateKID))
		return nil
	case errors.Is(err, clusterpepper.ErrMovedNotSynced):
		// The rename happened; only the directory sync failed. The fetched key's own write syncs
		// the same directory, so the move is not retried and no second name is picked.
		k.log(fmt.Sprintf("moved this node's wrong auth.cluster.pepper (key %s) to %s, but the directory sync after it failed (%v); the next write of the key syncs it",
			wrongKID, filepath.Base(aside), err))
	case err != nil:
		return fmt.Errorf("this node's auth.cluster.pepper (key %s) is not the cluster's (key %s) and could not be moved aside: %w. "+
			"The node uses no key and stores none over it until it can; it tries again at its next round. "+
			"Move the file out of the data directory or to any other name by hand; the node notices at its next round, no restart needed",
			wrongKID, stateKID, err)
	}
	k.stuckKID = ""
	k.log(fmt.Sprintf("this node's auth.cluster.pepper (key %s) is not the cluster's (key %s); it was moved to %s and the cluster's key is being fetched from another member. Logins on this node resume when it arrives.",
		wrongKID, stateKID, filepath.Base(aside)))
	return nil
}

// fetchPepper asks the members, the leader first, until one hands over the state's key, which it
// checks, writes durably and only then uses.
func (n *Node) fetchPepper(ctx context.Context, view cluster.PepperView) bool {
	k := n.cl.pepper
	order := make([]string, 0, len(view.Members))
	leader := n.raft.Leader()
	if leader != "" {
		order = append(order, leader)
	}
	for _, m := range view.Members {
		if m.ID != leader {
			order = append(order, m.ID)
		}
	}
	var misses []string
	for _, id := range order {
		m, ok := view.Member(id)
		if !ok || id == n.cfg.NodeID || m.Witness {
			continue
		}
		p, why := n.askPepper(ctx, id, view.PepperKID)
		if why != "" {
			misses = append(misses, id+": "+why)
			continue
		}
		k.mu.Lock()
		var err error
		if k.stuckKID != "" {
			err = fmt.Errorf("the wrong key %s is still in auth.cluster.pepper and is never overwritten", k.stuckKID)
		} else if err = k.file.Store(p); err == nil {
			k.held.Store(&p)
		}
		k.mu.Unlock()
		if err != nil {
			k.log(fmt.Sprintf("received the cluster's password key (key %s) from %s but could not write it: %v; this node asks again", p.KID(), id, err))
			return false
		}
		k.log(fmt.Sprintf("received the cluster's password key (key %s) from %s; password logins on this node can start", p.KID(), id))
		return true
	}
	if len(misses) > 0 {
		k.log("this node holds no cluster password key (pepper) yet and no member gave it one: " + strings.Join(misses, "; ") + ". It asks again when the cluster changes and at least every 30 seconds. " + noHolderFix)
	}
	return false
}

// askPepper sends one PEPPER_REQUEST to member and returns the key, or why it got none.
func (n *Node) askPepper(ctx context.Context, member, stateKID string) (clusterpepper.Pepper, string) {
	k := n.cl.pepper
	var req transport.PepperRequest
	if _, err := rand.Read(req.Nonce[:]); err != nil {
		return clusterpepper.Pepper{}, fmt.Sprintf("drawing a nonce: %v", err)
	}
	if hold := n.cl.opts.BeforePepperFetch; hold != nil {
		if err := hold(ctx, member); err != nil {
			return clusterpepper.Pepper{}, err.Error()
		}
	}
	askCtx, cancel := context.WithTimeout(ctx, pepperAskTimeout)
	defer cancel()
	reply, err := k.ask(askCtx, member, req)
	switch {
	case err != nil:
		return clusterpepper.Pepper{}, err.Error()
	case reply.Nonce != req.Nonce:
		return clusterpepper.Pepper{}, "the reply echoes another request's nonce; discarded"
	case reply.Status == transport.PepperNotHeld:
		return clusterpepper.Pepper{}, "it does not hold the key either"
	case reply.Status == transport.PepperRefused:
		return clusterpepper.Pepper{}, "refused: " + reply.Message
	case reply.KID != stateKID:
		return clusterpepper.Pepper{}, fmt.Sprintf("it sent key %s and the cluster's is %s; discarded", reply.KID, stateKID)
	}
	p, err := clusterpepper.FromBytes(reply.Pepper.Bytes())
	if err != nil {
		return clusterpepper.Pepper{}, err.Error()
	}
	if got := p.KID(); got != reply.KID {
		return clusterpepper.Pepper{}, fmt.Sprintf("the key it sent is key %s, not the %s it named; discarded", got, reply.KID)
	}
	return p, ""
}

// ProposeUserCommand commits one of the six user commands (ring user store spec 4.2) wherever the
// Raft leader is, as propose does for every cluster command. The replicated user binding and the
// root bootstrap task send their commands through it; any other kind is refused before anything is
// sent. Its body must already carry the peppered hash: the pepper itself never enters a command.
func (n *Node) ProposeUserCommand(ctx context.Context, kind byte, body []byte) (cluster.Result, error) {
	if !cluster.UserCommandKind(kind) {
		return cluster.Result{}, fmt.Errorf("kind 0x%02x is not a user command; ProposeUserCommand sends only kinds 0x%02x to 0x%02x", kind, cluster.CommandUserBootstrap, cluster.CommandUserRevoke)
	}
	result, undecided, err := n.proposeTracked(ctx, kind, body)
	if err != nil && undecided {
		return result, &UndecidedError{Err: err}
	}
	return result, err
}

// UndecidedError is the error of a user command after an attempt whose outcome this node never
// learned (a relay cut after the send, a leader that could not commit in time, a commit not yet
// applied here) and whose effect the committed state does not show: the command may or may not
// be applied, so its caller must not answer "nothing changed" (ring user store spec 6.1 step 6,
// 6.2 B3, B5). Err is the last ending, a fence refusal included.
type UndecidedError struct {
	Err error
}

func (e *UndecidedError) Error() string { return "the outcome is unknown: " + e.Err.Error() }

// Unwrap returns the last ending.
func (e *UndecidedError) Unwrap() error { return e.Err }
