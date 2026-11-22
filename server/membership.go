package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
)

// Membership is cluster state that placement depends on: a monotonic epoch and
// the member set that epoch describes. Every node persists its own copy; the
// epoch makes agreement checkable across nodes and allows a node that fell
// behind to resync instead of staying permanently poisoned (which is what a
// permanent membership mismatch used to do — see §6.5 of the rebalance-design record).
//
// The first epoch is derived from configuration (SeedNodes is the bootstrap)
// and persisted. After that, membership advances only by explicit join/remove
// proposals (the rebalance-design record §6.2) and by resyncing to a peer that is
// on a newer epoch.
const (
	// membershipFileName is the per-node durable membership record.
	membershipFileName = config.MembershipFileName
	// InitialEpoch is the epoch a freshly bootstrapped cluster starts at.
	InitialEpoch uint64 = 1
)

// membershipFile is the serialized form of a node's membership state.
type membershipFile struct {
	Epoch   uint64      `json:"epoch"`
	Members []ring.Node `json:"members"`
	// SealedPeers names the members that have said their store is encrypted
	// (sealed_peers.go), so a restarted sealed node does not refuse its sealed
	// peers until it has gossiped with each again. Additive: a file written
	// before the field existed has none, and every peer is then asked afresh;
	// a binary from before it ignores the key and drops it on its next save.
	SealedPeers []string `json:"sealed_peers,omitempty"`
	// JoinedAt is the epoch at which this node learnt each current member
	// joined; a founding member has none (spec §8.3 of join-freeze).
	JoinedAt map[string]uint64 `json:"joined_at,omitempty"`
	// SettledOf is the best Settled epoch this node has for each current
	// member (join-freeze spec §8.2).
	SettledOf map[string]uint64 `json:"settled_of,omitempty"`
	// Bootstrapped marks a member list taken from seed settings that no member
	// has confirmed yet: a node started for joining (join-freeze spec §5.4).
	// Absent means confirmed, which is what a file from before the field says.
	Bootstrapped bool `json:"bootstrapped,omitempty"`
	// Pending is this node's promise and acceptance for the next epoch, if
	// it holds one. Saved before the node answers a prepare or an accept, so
	// a restart cannot promise below a promise it made. Absent means none,
	// which is what a file from before the field says.
	Pending *pendingChange `json:"pending,omitempty"`
}

// membershipState is everything membership.json records, gathered from memory
// by the one writer (writeMembershipRecord) or by NewCluster's first write.
type membershipState struct {
	epoch        uint64
	members      []ring.Node
	sealedPeers  []string
	joinedAt     map[string]uint64
	settledOf    map[string]uint64
	bootstrapped bool
	pending      *pendingChange
}

// membershipRecordOf assembles the whole membership record. It is the one
// place the record is built: a writer that built membershipFile{...} by hand
// would drop the fields it did not name, and after a restart a joiner whose
// joined_at was dropped looks like a founder (join-freeze build doc, trap 3).
func membershipRecordOf(state membershipState) membershipFile {
	return membershipFile{
		Epoch:        state.epoch,
		Members:      state.members,
		SealedPeers:  state.sealedPeers,
		JoinedAt:     state.joinedAt,
		SettledOf:    state.settledOf,
		Bootstrapped: state.bootstrapped,
		Pending:      state.pending,
	}
}

// Peer is a cluster member as it travels on the node-to-node wire: the stable
// id and the dial address placement needs.
type Peer struct {
	ID   string `bson:"id"`
	Addr string `bson:"addr,omitempty"`
	// Store is set by a node on its own record, and only there: whether its
	// store is encrypted at rest, "sealed" or "plaintext". A node of this
	// version always says one of the two, so an absent value means only that
	// the node does not say — a version from before the field, or a record one
	// node relays about another, which never carries it (sealed_peers.go). A
	// sealed node gives such a peer nothing, and never calls it plaintext.
	Store storeClaim `bson:"store,omitempty"`
	// Driver is where a client reaches this node (its tcp.listen), set by a
	// node on its own record only, like Store (peer_drivers.go). Absent means
	// the node does not say: a version from before the field, or a record one
	// node relays about another.
	Driver string `bson:"driver,omitempty"`
}

// membershipPath returns the location of this node's membership record.
func (c *Cluster) membershipPath() string {
	return membershipPathFor(c.s.config.Storage.DataDir)
}

// membershipPathFor locates the membership record under a data directory,
// for the paths that run before a Cluster exists.
func membershipPathFor(dataDir string) string {
	return filepath.Join(dataDir, membershipFileName)
}

// loadMembership restores a node's persisted membership. A missing file is the
// normal first start and returns found=false.
func loadMembership(path string) (epoch uint64, members []ring.Node, found bool, err error) {
	record, found, err := readMembershipRecord(path)
	if err != nil || !found {
		return 0, nil, false, err
	}
	return record.Epoch, record.Members, true, nil
}

// readMembershipRecord reads the whole persisted record, the sealed peers
// included. A missing file returns found=false.
func readMembershipRecord(path string) (record membershipFile, found bool, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return membershipFile{}, false, nil
		}
		return membershipFile{}, false, fmt.Errorf("read membership %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return membershipFile{}, false, fmt.Errorf("parse membership %s: %w", path, err)
	}
	if record.Epoch < InitialEpoch {
		return membershipFile{}, false, fmt.Errorf("membership %s has epoch %d, below the initial %d", path, record.Epoch, InitialEpoch)
	}
	if len(record.Members) == 0 {
		return membershipFile{}, false, fmt.Errorf("membership %s has an empty member set", path)
	}
	return record, true, nil
}

// syncMembershipFile and syncMembershipDirectory are the two fsyncs
// saveMembershipRecord makes. They are variables so a test can record that
// both happen, and in which order: a power cut is not available to a test.
var (
	syncMembershipFile      = func(file *os.File) error { return file.Sync() }
	syncMembershipDirectory = syncDirectory
)

// saveMembershipRecord durably records the membership record. Written to a
// temporary file and renamed, so a crash mid-write cannot leave a half-written
// record. Only writeMembershipRecordLocked and NewCluster's first write call it.
func saveMembershipRecord(path string, record membershipFile) error {
	if record.Epoch < InitialEpoch {
		return fmt.Errorf("refusing to persist membership epoch %d", record.Epoch)
	}
	if len(record.Members) == 0 {
		return fmt.Errorf("refusing to persist an empty member set")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal membership: %w", err)
	}
	payload = append(payload, '\n')
	// Synced before the rename, or the rename can reach the disk before the
	// data and a power loss leaves an empty record; the directory after it, or
	// the rename itself can be lost.
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := file.Write(payload); err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", tmp, err), file.Close())
	}
	if err := syncMembershipFile(file); err != nil {
		return errors.Join(fmt.Errorf("sync %s: %w", tmp, err), file.Close())
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace membership: %w", err)
	}
	if err := syncMembershipDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("making the replacement of %s durable: %w", path, err)
	}
	return nil
}

// epochMembers returns the node's current authoritative (epoch, members).
func (c *Cluster) epochMembers() (uint64, []ring.Node) {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return c.epoch, append([]ring.Node(nil), c.members...)
}

// Epoch returns this node's current membership epoch.
func (c *Cluster) Epoch() uint64 {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return c.epoch
}

// memberIDSet returns the current member ids as a set.
func (c *Cluster) memberIDSet() map[string]struct{} {
	_, members := c.epochMembers()
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[m.ID] = struct{}{}
	}
	return set
}

// memberNodes returns the current members in wire form. This node's own
// record says whether its store is sealed; the others' say nothing, because a
// node speaks only for itself (sealed_peers.go).
func (c *Cluster) memberNodes() []Peer {
	_, members := c.epochMembers()
	peers := nodesToPeers(members)
	for i := range peers {
		if peers[i].ID == c.self.ID {
			peers[i].Store = c.s.storeClaim()
			peers[i].Driver = c.s.ownDriverAddress()
		}
	}
	return peers
}

// persistSealedPeers rewrites the membership record with the members now
// known sealed.
func (c *Cluster) persistSealedPeers() error {
	return c.writeMembershipRecord()
}

// writeMembershipRecord rewrites membership.json, the whole record, from
// memory. membershipFileMu orders it against adoptMembership, so it can never
// write an epoch older than the one on disk. settledOf is copied before the
// file lock is taken: settleMu is a leaf and is never taken under
// membershipFileMu (join-freeze spec §12).
func (c *Cluster) writeMembershipRecord() error {
	// A removed node follows the members' view in memory and keeps, on disk,
	// the membership it was last a member of: its removal record says the
	// rest (removed_node.go), and a record of a set without it would not load.
	if c.removed() {
		return nil
	}
	settledOf := c.settledOfSnapshot()
	c.membershipFileMu.Lock()
	defer c.membershipFileMu.Unlock()
	epoch, members := c.epochMembers()
	return c.writeMembershipRecordLocked(membershipWrite{
		epoch: epoch, members: members, settledOf: settledOf, bootstrapped: c.bootstrapped.Load(), pending: c.pending,
	})
}

// membershipWrite is what a writer of the record supplies; the rest of the
// record (the sealed peers, joined_at) is derived from memory by
// writeMembershipRecordLocked, so no writer can drop it.
type membershipWrite struct {
	epoch        uint64
	members      []ring.Node
	settledOf    map[string]uint64
	bootstrapped bool
	pending      *pendingChange
}

// writeMembershipRecordLocked writes the record for (epoch, members), which
// adoptMembership passes before it has swapped them into memory, with the
// bootstrapped mark and the pending change it will have then. The caller holds
// membershipFileMu, and took settledOf before it.
func (c *Cluster) writeMembershipRecordLocked(write membershipWrite) error {
	return saveMembershipRecord(c.membershipPath(), membershipRecordOf(membershipState{
		epoch:        write.epoch,
		members:      write.members,
		sealedPeers:  c.sealedPeers.sealedIDs(write.members),
		joinedAt:     copyEpochMap(c.joinedAtSnapshot()),
		settledOf:    settledOfMembers(write.settledOf, write.members),
		bootstrapped: write.bootstrapped,
		pending:      write.pending,
	}))
}

// confirmMembershipHook, when set, runs before a bootstrapped node weighs an
// agreement and may block: a test uses it to hold confirmation back past the
// moment every node knows every other, which in a real cluster are one gossip
// exchange apart. Nil in production.
var confirmMembershipHook atomic.Pointer[func(nodeID string)]

// noteViewAgreement records that peer, a member of this node's view, agrees
// with it on epoch and member set, and confirms the view, provided no member
// of the view has disputed it since boot, by one of two routes (join-freeze
// spec §5.4 as the coordinator ruled it, §16 F-A):
//
//   - a strict majority of the view, counting this node, are confirmed peers
//     that agree: a bootstrapped peer's agreement counts for nothing here, so
//     newcomers started from one seed list cannot confirm each other however
//     their messages are ordered;
//   - every other member of the view has answered and agreed, bootstrapped or
//     not: the founders of a new cluster, all bootstrapped, confirm this way,
//     so every founder must be up before any serves.
func (c *Cluster) noteViewAgreement(peerID string, peerBootstrapped bool, viewSize int) {
	if !c.bootstrapped.Load() {
		return
	}
	if hook := confirmMembershipHook.Load(); hook != nil {
		(*hook)(c.self.ID)
	}
	c.viewConfirmMu.Lock()
	if !c.bootstrapped.Load() {
		c.viewConfirmMu.Unlock()
		return
	}
	if c.viewAgreed == nil {
		c.viewAgreed = make(map[string]struct{})
		c.viewAgreedConfirmed = make(map[string]struct{})
	}
	c.viewAgreed[peerID] = struct{}{}
	if !peerBootstrapped {
		c.viewAgreedConfirmed[peerID] = struct{}{}
	}
	var route string
	switch {
	case c.viewDisputed.Load():
	case 2*(len(c.viewAgreedConfirmed)+1) > viewSize:
		route = fmt.Sprintf("by agreement of %d confirmed members of %d", len(c.viewAgreedConfirmed), viewSize)
	case len(c.viewAgreed) == viewSize-1:
		route = fmt.Sprintf("every one of the %d members has answered and agrees", viewSize)
	}
	if route != "" {
		c.clearBootstrappedLocked()
	}
	c.viewConfirmMu.Unlock()
	if route == "" {
		return
	}
	if err := c.writeMembershipRecord(); err != nil {
		c.logger.Warn("could not persist that this node's membership is confirmed; after a restart it asks its members again",
			"error", err)
	}
	c.logger.Info("this node's membership is confirmed (" + route + ")")
	c.settleAtFounding(c.Epoch())
	c.recomputeJoinFreeze()
}

// noteViewDispute records that a member of this node's view answered with
// something other than agreement: another member set at its epoch, or a newer
// epoch. A bootstrapped node is then confirmed only by adopting the join.
func (c *Cluster) noteViewDispute() {
	if !c.bootstrapped.Load() {
		return
	}
	c.viewConfirmMu.Lock()
	if c.bootstrapped.Load() {
		c.viewDisputed.Store(true)
	}
	c.viewConfirmMu.Unlock()
}

// clearBootstrapped clears the mark when this node adopts, from a peer, a
// membership that lists it — the join itself — and reports whether it was set.
func (c *Cluster) clearBootstrapped() bool {
	c.viewConfirmMu.Lock()
	defer c.viewConfirmMu.Unlock()
	if !c.bootstrapped.Load() {
		return false
	}
	c.clearBootstrappedLocked()
	return true
}

// clearBootstrappedLocked clears the mark and forgets who agreed and whether
// anyone disputed. The caller holds viewConfirmMu.
func (c *Cluster) clearBootstrappedLocked() {
	c.bootstrapped.Store(false)
	c.viewAgreed = nil
	c.viewAgreedConfirmed = nil
	c.membershipRefusals = nil
	c.viewDisputed.Store(false)
}

// noteMembershipRefusal records, while this node is bootstrapped, that it will
// not exchange membership with member peerID, and why.
func (c *Cluster) noteMembershipRefusal(peerID string, refusal error) {
	c.viewConfirmMu.Lock()
	defer c.viewConfirmMu.Unlock()
	if !c.bootstrapped.Load() {
		return
	}
	if c.membershipRefusals == nil {
		c.membershipRefusals = make(map[string]error)
	}
	c.membershipRefusals[peerID] = refusal
}

// forgetMembershipRefusal drops a refusal once membership with peerID is
// exchanged after all.
func (c *Cluster) forgetMembershipRefusal(peerID string) {
	c.viewConfirmMu.Lock()
	defer c.viewConfirmMu.Unlock()
	delete(c.membershipRefusals, peerID)
}

// membershipRefusal returns one recorded refusal, the first by member id so
// the answer is stable, or nil.
func (c *Cluster) membershipRefusal() error {
	c.viewConfirmMu.Lock()
	defer c.viewConfirmMu.Unlock()
	var firstID string
	for id := range c.membershipRefusals {
		if firstID == "" || id < firstID {
			firstID = id
		}
	}
	return c.membershipRefusals[firstID]
}

// foundersNotAnswered tells a founder waiting for its fellow founders from a
// newcomer: while no member of the view has disputed it, it returns the members
// of the view that have not answered since boot, and true. A disputed view is a
// newcomer's (its members list a set without it), and returns false.
func (c *Cluster) foundersNotAnswered() ([]string, bool) {
	_, members := c.epochMembers()
	c.viewConfirmMu.Lock()
	defer c.viewConfirmMu.Unlock()
	if c.viewDisputed.Load() {
		return nil, false
	}
	var silent []string
	for _, member := range members {
		if _, answered := c.viewAgreed[member.ID]; member.ID != c.self.ID && !answered {
			silent = append(silent, member.ID)
		}
	}
	return silent, len(silent) > 0
}

// nonMemberViewLogEvery is how often this node notes, per peer, that a node
// outside its membership announces a member list of its own.
const nonMemberViewLogEvery = time.Minute

// noteNonMemberView logs, at most once per peer per nonMemberViewLogEvery,
// that a node which is not a member announced a different member set at this
// node's epoch: a newcomer waiting for 'join' does exactly that.
func (c *Cluster) noteNonMemberView(peerID string, epoch uint64) {
	now := time.Now()
	c.viewConfirmMu.Lock()
	if last, logged := c.nonMemberViewLoggedAt[peerID]; logged && now.Sub(last) < nonMemberViewLogEvery {
		c.viewConfirmMu.Unlock()
		return
	}
	for id, last := range c.nonMemberViewLoggedAt {
		if now.Sub(last) >= nonMemberViewLogEvery {
			delete(c.nonMemberViewLoggedAt, id)
		}
	}
	if c.nonMemberViewLoggedAt == nil {
		c.nonMemberViewLoggedAt = make(map[string]time.Time)
	}
	c.nonMemberViewLoggedAt[peerID] = now
	c.viewConfirmMu.Unlock()
	c.logger.Debug("a node that is not a member yet announces a member list that includes this cluster; run 'join' to add it",
		"node_id", peerID, "epoch", epoch)
}

// settledOfSnapshot copies the Settled epochs this node holds for its members.
func (c *Cluster) settledOfSnapshot() map[string]uint64 {
	c.settleMu.Lock()
	defer c.settleMu.Unlock()
	return copyEpochMap(c.settledOf)
}

// copyEpochMap copies a member-to-epoch map; an empty one copies as nil, so
// the record omits the key.
func copyEpochMap(epochs map[string]uint64) map[string]uint64 {
	if len(epochs) == 0 {
		return nil
	}
	copied := make(map[string]uint64, len(epochs))
	for id, epoch := range epochs {
		copied[id] = epoch
	}
	return copied
}

// adoptMembershipHook, when set, runs before this node adopts a membership
// epoch and can refuse it. A test uses it to hold one chosen member on the old
// epoch while the others move on, which is what a member that has not yet
// learned a join looks like -- for about fifteen seconds in every real cluster
// run -- and which the ring, left to itself, only produces for some buckets.
// Nil in production.
var adoptMembershipHook atomic.Pointer[func(nodeID string, epoch uint64) error]

// viewAdoptedHook, when set, runs after this node's view has moved to an
// adopted epoch and before that epoch's placement pass starts. A test holds a
// node there to show what another adoption sees in between. Nil in
// production.
var viewAdoptedHook atomic.Pointer[func(nodeID string, epoch uint64)]

// errAdoptionNotAhead is an adoption of an epoch this node has already reached
// or passed: a proposer whose vote round outlasted a catch-up, or a voter that
// a catch-up overtook while it waited for its placement pass.
var errAdoptionNotAhead = errors.New("this node is already at or past that membership epoch")

// adoptMembership atomically moves this node onto (epoch, members): persist,
// rebuild the placement ring, replace the in-memory view, and clear any
// divergence flag. The ring is the placement authority, so placement changes
// the moment this returns — see memberSetsEqual for why a caller must have
// validated the proposal before calling.
//
// It is the one gate every adoption passes, under adoptMu, and it only moves
// forward: an epoch at or below this node's is refused with
// errAdoptionNotAhead. A vote round waits for its slowest voter, and a
// catch-up can take the proposer past the epoch it proposed in the meantime;
// adopting it then moved the view back and recorded the marks of the older
// epoch over the newer one's.
func (c *Cluster) adoptMembership(epoch uint64, members []ring.Node) error {
	c.adoptMu.Lock()
	defer c.adoptMu.Unlock()
	return c.adoptMembershipLocked(epoch, members, false, nil)
}

// adoptMembershipLocked is adoptMembership for a caller that holds adoptMu: a
// catch-up adopting the epochs it missed in turn (adoptInOrder). reentering
// says the catch-up took this node through an epoch that did not list it: it
// is joining again, and takes every member as joined now (deriveJoinedAt).
// carried is the cluster's record of this node's provisional epoch, from the
// answer it adopts from, or nil.
func (c *Cluster) adoptMembershipLocked(epoch uint64, members []ring.Node, reentering bool, carried []ring.Node) error {
	if current := c.Epoch(); epoch <= current {
		return fmt.Errorf("adopting membership epoch %d at epoch %d: %w", epoch, current, errAdoptionNotAhead)
	}
	// Before the file lock: a test hook may hold this node on the old epoch for
	// seconds, and a refused adoption must not prune the sealed-peer table.
	if hook := adoptMembershipHook.Load(); hook != nil {
		if err := (*hook)(c.self.ID, epoch); err != nil {
			return err
		}
	}
	// Copied before the file lock: settleMu is never taken under it.
	settledOf := c.settledOfSnapshot()
	// Held from the save to the in-memory swap: writeMembershipRecord writes
	// the epoch it reads from memory, and between the two that is the old one.
	c.membershipFileMu.Lock()
	// The history holds the epoch before its ring replaces the current one:
	// an entry logged under it is judged against it from then on (step 6.5a).
	if err := c.recordMembership(epoch, members); err != nil {
		c.membershipFileMu.Unlock()
		return err
	}
	// A bootstrapped node's first adoption is its join, and the view it
	// started on was provisional, so the history never keeps it (owner ruling
	// 52, R8-D (b)). When the answer it adopts from carried the cluster's
	// record of that epoch, the record is written over the view, so the
	// joiner knows its buckets' previous owners (on-demand pull spec §6.7);
	// otherwise the view is forgotten. A founder is normally confirmed by
	// agreement; one that adopts a later epoch before it confirmed (it was
	// slow to hear every founder) is handed the cluster's record the same
	// way. Without one it fails safe: an epoch missing from the history makes
	// R8-D (b) refuse with retry, never pass over. The write is before
	// membership.json moves: a crash between leaves the node provisional with
	// the cluster's record, which loadMembershipHistory keeps.
	if c.bootstrapped.Load() {
		replace := func() error { return c.forgetMembership(c.Epoch()) }
		if carried != nil {
			replace = func() error { return c.overwriteMembership(c.Epoch(), carried) }
		}
		if err := replace(); err != nil {
			c.membershipFileMu.Unlock()
			return err
		}
	}
	c.sealedPeers.retain(members)
	c.peerDrivers.retain(members)
	// Derived before the record is built, while the mark still says whether
	// this node is bootstrapped: a newcomer's first adoption is its join
	// (join-freeze spec §8.3, review F-1).
	_, before := c.epochMembers()
	joined := deriveJoinedAt(joinedAtChange{
		held:     c.joinedAtSnapshot(),
		before:   before,
		after:    members,
		epoch:    epoch,
		everyone: c.bootstrapped.Load() || reentering,
		floor:    settledFloor(c.self.ID, c.settled.Load(), settledOf, members),
	})
	c.joinedAt.Store(&joined)
	// Adopting a membership from a peer that lists this node — the set always
	// does, or placementRing refuses it below — is the join that confirms a
	// bootstrapped node: the record says so, and the mark clears after the swap.
	// An acceptance for this epoch or below is done with once the epoch is
	// adopted, and is dropped in the same write that moves the epoch: no
	// instant has the view at the next epoch and its acceptance still
	// pending. The acceptor refuses every later phase for an epoch it has
	// adopted, so nothing it forgets here can be contradicted.
	pending := c.pending
	if pending != nil && epoch >= pending.Epoch {
		pending = nil
	}
	if err := c.writeMembershipRecordLocked(membershipWrite{
		epoch: epoch, members: members, settledOf: settledOf, bootstrapped: false, pending: pending,
	}); err != nil {
		c.membershipFileMu.Unlock()
		return err
	}
	placement, err := c.placementRing(members, false)
	if err != nil {
		c.membershipFileMu.Unlock()
		return err
	}
	// The ring and the epoch change under one hold, so no reader sees one
	// without the other (step 9.4).
	c.viewMu.Lock()
	c.r = placement
	c.epoch = epoch
	c.members = members
	c.viewMu.Unlock()
	c.noteViewChanged()
	// What reads in the window recorded was about the epoch it left
	// (on-demand pull spec §4.2): membershipFileMu → takeOver.mu.
	c.clearReadOutcomes()
	c.pending = pending
	// Cleared under the file lock, so no concurrent writer of the record puts
	// the mark back between the save above and this.
	confirmed := c.clearBootstrapped()
	c.membershipFileMu.Unlock()
	c.pruneSettled(members)
	c.pruneRemovedSizeRefusals(members)
	if confirmed {
		c.logger.Info(fmt.Sprintf("this node's membership is confirmed (by adopting epoch %d)", epoch))
	}
	// After both locks are released: the report reads the view and the
	// settled values under locks of its own (join-freeze spec §12).
	c.recomputeJoinFreeze()
	// Every stream starts again from the log under the new ring: an entry
	// queued under the old one may be addressed to a peer the new ring does not
	// name, or be of a bucket this node no longer owns.
	c.resetStreams(fmt.Sprintf("membership epoch %d adopted", epoch))
	if hook := viewAdoptedHook.Load(); hook != nil {
		(*hook)(c.self.ID, epoch)
	}
	// It runs whether or not auto-rebalance is on: it moves nothing.
	c.startPlacementPass(epoch)
	c.noteMembershipAdopted()

	// Keep the peer table current so a newly adopted member is reachable for
	// gossip and catch-up. Existing peers are untouched.
	for _, m := range members {
		if m.ID == c.self.ID {
			continue
		}
		c.peerFor(m)
		// A member adopted after boot also needs its sender and verifier
		// goroutines — Start() only started those for boot-time members.
		// This covers all three adoption paths: the proposer committing,
		// voters adopting the proposal, and gossip reconciliation.
		c.ensurePeerRunning(m)
	}
	// A node started alone ran none of the member workers at Start; this is
	// the adoption that gives it a member (BUG-0498). Before Start, Start
	// itself sees the members and starts them.
	if c.startCalled.Load() && c.Enabled() {
		c.startMemberWorkers()
	}

	c.mismatch.Store(false)
	return nil
}

// rebuildRing rebuilds the placement ring from a member set. The ring is a
// pure function of (epoch, bucket), so replacing it is how a membership change
// takes effect. It fails if self is not a member: a node that is not on the
// ring cannot own anything and must not run in a state where every write would
// be forwarded (that was a silent correctness lie before).
func (c *Cluster) rebuildRing(members []ring.Node) error {
	nr, err := c.placementRing(members, false)
	if err != nil {
		return err
	}
	c.viewMu.Lock()
	c.r = nr
	c.viewMu.Unlock()
	c.noteViewChanged()
	return nil
}

// placementRing builds the placement ring of a member set. A set that lacks
// this node is refused, unless the node was removed (step 9.3): a removed node
// places buckets by the members' ring and owns nothing.
func (c *Cluster) placementRing(members []ring.Node, removed bool) (*ring.Ring, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("cluster: refusing to build a placement ring from an empty member set")
	}
	nr := ring.New(c.rf)
	for _, m := range members {
		if err := nr.AddNode(m); err != nil {
			return nil, fmt.Errorf("cluster: add member %s: %w", m.ID, err)
		}
	}
	if !removed && !nr.Has(c.self.ID) {
		return nil, fmt.Errorf("cluster: node identity %q is not a member of %v; it could not take ownership of anything",
			c.self.ID, memberIDsOf(members))
	}
	return nr, nil
}

// reconcileMembership compares a peer's membership view with ours and either
// confirms agreement, resyncs to a newer epoch, or flags a genuine divergence.
//
// This replaces the old "disagreement is permanent until a node is killed"
// behaviour: a node that is behind a committed membership change fast-forwards
// instead of poisoning every DDL and write.
// The peer must be a member of our current epoch to resync us: only an
// existing member is in a position to have propagated a change it invited, so
// we never adopt a stranger's membership.
// A peer speaking the old wire protocol (no epoch, no node addresses) is
// treated as same-epoch and only set-compared, preserving the old safety
// without adoption.
func (c *Cluster) reconcileMembership(peerID string, peerNodes []Peer, peerIDs []string, peerEpoch uint64) error {
	return c.reconcileMembershipWith(peerID, peerNodes, peerIDs, peerEpoch, peerGossip{})
}

// peerGossip is what a peer's gossip carried besides its view: the membership
// history — answered is set on a gossip answer, which carries every epoch
// above the asker's the peer remembers, and not on a request, which carries
// none — and whether the peer is itself bootstrapped.
type peerGossip struct {
	records      []MembershipRecord
	answered     bool
	bootstrapped bool
}

// gossipReconcileHook, when set, runs at the top of both gossip
// reconciliations: side "responder" on a node handling another's gossip
// request, side "initiator" on a node reading the answer to its own. true
// skips the reconciliation; the exchange itself still completes. It returns at
// once and never blocks, because a gossip handler runs on the control lane,
// which also carries the membership change's messages. Nil in production.
var gossipReconcileHook atomic.Pointer[func(nodeID, peerID, side string) bool]

// gossipReconcileSkipped reports whether a test's gossipReconcileHook skips
// this node's reconciliation with peerID.
func (c *Cluster) gossipReconcileSkipped(peerID, side string) bool {
	hook := gossipReconcileHook.Load()
	return hook != nil && (*hook)(c.self.ID, peerID, side)
}

// reconcileRequest reconciles with the view a gossip request carries.
func (c *Cluster) reconcileRequest(req GossipRequest) error {
	if c.gossipReconcileSkipped(req.NodeID, "responder") {
		return nil
	}
	return c.reconcileMembershipWith(req.NodeID, req.Nodes, req.Members, req.Epoch, peerGossip{bootstrapped: req.Bootstrapped})
}

// reconcileAnswer reconciles with the view a gossip answer carries, and the
// history with it.
func (c *Cluster) reconcileAnswer(peerID string, resp GossipResponse) error {
	if c.gossipReconcileSkipped(peerID, "initiator") {
		return nil
	}
	return c.reconcileMembershipWith(peerID, resp.Nodes, resp.Members, resp.Epoch, peerGossip{records: resp.History, answered: true, bootstrapped: resp.Bootstrapped})
}

// reconcileMembershipWith is reconcileMembership with the peer's history, from
// which a node that is behind by more than one epoch adopts each in turn.
func (c *Cluster) reconcileMembershipWith(peerID string, peerNodes []Peer, peerIDs []string, peerEpoch uint64, history peerGossip) error {
	// Old-wire peer: fall back to the set comparison that predates epochs.
	if peerEpoch == 0 && len(peerNodes) == 0 {
		return c.reconcileLegacyPeer(peerID, peerIDs)
	}

	their := make([]ring.Node, 0, len(peerNodes))
	theirIDs := make(map[string]struct{}, len(peerNodes))
	for _, p := range peerNodes {
		n := ring.Node{ID: p.ID, Addr: p.Addr}
		their = append(their, n)
		if p.ID != "" {
			theirIDs[p.ID] = struct{}{}
		}
	}

	myEpoch, _ := c.epochMembers()
	myIDs := c.memberIDSet()

	switch {
	case peerEpoch < myEpoch:
		// The peer is behind and will resync from us on its own exchange with
		// us. Agreeing here would regress our membership, and setting the
		// divergence flag would fail writes for no reason.
		c.logger.Debug("peer is on an older membership epoch", "node_id", peerID, "peer_epoch", peerEpoch, "our_epoch", myEpoch)
		return nil

	case peerEpoch == myEpoch:
		_, peerIsMember := myIDs[peerID]
		if memberSetsEqual(theirIDs, myIDs) {
			// Equal ids are not agreement when one id is at two addresses:
			// nodes given different seed lists would each run a node as it
			// (BUG-0598).
			if disagreement, found := c.addrDisagreement(their); found {
				return c.refuseAddrDisagreement(peerID, disagreement, history.bootstrapped)
			}
			// A peer under this node's own id is this node, reached through a
			// seed that lists it as id@address (cmd/server asks every seed but
			// the literal listen address): its agreement confirms nothing, and
			// counted, it confirmed a node no other member had answered.
			if peerIsMember && peerID != c.self.ID {
				c.noteViewAgreement(peerID, history.bootstrapped, len(myIDs))
			}
			if c.mismatch.Swap(false) {
				c.logger.Info("cluster membership agrees again", "node_id", peerID, "epoch", myEpoch)
			}
			return nil
		}
		// A node outside this epoch cannot change this node's placement, and a
		// newcomer waiting for 'join' is exactly such a node: its seed view
		// lists the members and itself. Only a member's
		// disagreement is a divergence.
		if !peerIsMember {
			c.noteNonMemberView(peerID, myEpoch)
			return nil
		}
		c.noteViewDispute()
		// A node no member has confirmed expects exactly this answer: the
		// members do not list it yet. It is no divergence and no failed
		// exchange, so it neither sets mismatch nor errors — an error would
		// have verifyPeer record a healthy member as down (build report F-E).
		if c.bootstrapped.Load() {
			return nil
		}
		err := fmt.Errorf("%w (epoch %d, peer %s)", errMembershipDiverged, myEpoch, peerID)
		c.mismatch.Store(true)
		c.logger.Error("cluster membership divergence at the same epoch", "node_id", peerID, "epoch", myEpoch, "error", err)
		return err

	default: // peerEpoch > myEpoch: we are behind — resync, if the peer is a member of our epoch
		if _, ok := myIDs[peerID]; !ok {
			// A node no member has confirmed (started to join, or a founder
			// not yet agreed with) serves nothing and has no placement to
			// protect, and a node the cluster removed keeps gossiping later
			// epochs to it. The flag would only refuse the requests forwarded
			// to it once it is added (BUG-0704). Keying on the removed history
			// instead would miss a joiner, which has none yet.
			if c.bootstrapped.Load() {
				c.noteNonMemberView(peerID, myEpoch)
				return nil
			}
			// A confirmed member has a history, and a node in it is one this
			// member has itself seen removed: it holds no placement and
			// adopts nothing from it, so the flag would only refuse direct
			// writes until the member resyncs from a real member (BUG-0707).
			// A node removed and added again is a member once the add is
			// adopted, so it is not in removedInHistory then. A stranger in
			// neither keeps the flag.
			if slices.ContainsFunc(c.removedInHistory(), func(n ring.Node) bool { return n.ID == peerID }) {
				c.noteNonMemberView(peerID, myEpoch)
				return nil
			}
			err := fmt.Errorf("%w: peer %s reports newer membership epoch %d but is not a member of our epoch %d; refusing to adopt an uninvited membership",
				ErrMemberMismatch, peerID, peerEpoch, myEpoch)
			c.mismatch.Store(true)
			c.logger.Error("refusing to adopt membership from a non-member", "node_id", peerID, "peer_epoch", peerEpoch, "our_epoch", myEpoch, "error", err)
			return err
		}
		c.noteViewDispute()
		return c.adoptInOrder(peerID, myEpoch, peerEpoch, their, history)
	}
}

// adoptInOrder moves this node from epoch from to epoch to, one epoch at a
// time, waiting for the placement pass of each before it adopts the next
// (step 9.4): the marks of a change are those of its own two rings, and a node
// that jumped would mark against a ring it never had. A membership without
// this node is its removal, recorded and followed as a non-member (steps
// 6.5b, 9.3). A request carries no history: a node behind by more than one
// epoch adopts nothing from it and asks for the history itself. An answer that
// does not reach back to from + 1 means the peer forgot what this node missed.
// One catch-up runs at a time, and starts from the epoch the one before it
// left the node at.
func (c *Cluster) adoptInOrder(peerID string, from, to uint64, their []ring.Node, history peerGossip) error {
	c.adoptMu.Lock()
	defer c.adoptMu.Unlock()
	from = max(from, c.Epoch())
	if from >= to {
		return nil
	}
	// A node no member has confirmed was never in any epoch of this history:
	// the first epoch that omits it is not its removal, and a removal recorded
	// now would outlive the 'join' that adds it. It adopts nothing
	// until the membership it is shown lists it.
	if c.bootstrapped.Load() && !nodeListed(their, c.self.ID) {
		c.logger.Debug("a member is on a membership that does not list this node yet; waiting for 'join' to add it",
			"node_id", peerID, "peer_epoch", to, "epoch", from)
		return nil
	}
	steps := make(map[uint64][]ring.Node, len(history.records)+1)
	for _, record := range history.records {
		steps[record.Epoch] = peersToNodes(record.Members)
	}
	steps[to] = their
	for epoch := from + 1; epoch <= to; epoch++ {
		if _, known := steps[epoch]; known {
			continue
		}
		if !history.answered {
			c.wakeGossip()
			return nil
		}
		return c.missedTooMany(from, to)
	}
	// A node the cluster removed and added again — an operator emptied it in
	// between — is a member of the membership it ends on: the epochs it was
	// out of are recorded in its history, for the rings they placed by, and
	// not taken as its removal.
	endsAMember := nodeListed(their, c.self.ID)
	reentering := false
	// The cluster's record of this node's provisional epoch, when the answer
	// carries it: the first adoption writes it over the provisional view
	// (spec §6.7). Later adoptions are not bootstrapped and ignore it.
	carried, carriedFound := steps[from]
	if !c.bootstrapped.Load() || !carriedFound {
		carried = nil
	}
	for epoch := from + 1; epoch <= to; epoch++ {
		if err := c.waitForPlacementPass(c.ctx); err != nil {
			return fmt.Errorf("adopting membership epoch %d from %s: %w", epoch, peerID, err)
		}
		// The wait returns at once when no pass runs, and before Start none
		// does, even for an epoch whose pass a crash left unfinished: Start
		// resumes it, and the internode listener opens first (BUG-0511). The
		// node adopts nothing until that pass has finished, and a later
		// exchange catches it up; nil, as for a request that carries too little
		// history, so the asker does not take this node for down.
		if !c.placementPassFinished() {
			c.logger.Debug("this node's placement pass for its membership epoch has not finished; it adopts the next one once it has",
				"node_id", peerID, "epoch", c.Epoch(), "peer_epoch", to)
			return nil
		}
		members := steps[epoch]
		if !nodeListed(members, c.self.ID) && endsAMember {
			reentering = true
			if err := c.recordMembership(epoch, members); err != nil {
				return fmt.Errorf("recording membership epoch %d from %s: %w", epoch, peerID, err)
			}
			continue
		}
		if !nodeListed(members, c.self.ID) {
			if err := c.markRemoved(epoch, members); err != nil {
				return err
			}
			continue
		}
		if err := c.adoptMembershipLocked(epoch, members, reentering, carried); err != nil {
			return fmt.Errorf("adopting membership epoch %d from %s: %w", epoch, peerID, err)
		}
		c.logger.Info("resynced to a newer membership epoch", "node_id", peerID, "epoch", epoch, "members", len(members))
	}
	c.maybeSweep(to)
	return nil
}

// missedMembershipWarnEvery is how often a node that missed more changes than
// the cluster remembers says so.
const missedMembershipWarnEvery = 5 * time.Minute

// missedTooMany refuses a membership this node cannot reach one epoch at a
// time: it adopts nothing, stops taking writes, and says, at most once per
// missedMembershipWarnEvery, how to bring the node back.
func (c *Cluster) missedTooMany(from, to uint64) error {
	err := fmt.Errorf("%w: this node is at membership epoch %d and the cluster is at %d; it missed more membership changes than the cluster remembers (%d). "+
		"Remove it ('remove-node %s'), empty its data directory and add it again",
		ErrMemberMismatch, from, to, gossipHistoryLimit.Load(), c.self.ID)
	c.mismatch.Store(true)
	now := time.Now().UnixNano()
	if last := c.missedLoggedAt.Load(); last == 0 || time.Duration(now-last) >= missedMembershipWarnEvery {
		if c.missedLoggedAt.CompareAndSwap(last, now) {
			c.logger.Error(err.Error(), "epoch", from, "cluster_epoch", to)
		}
	}
	return err
}

// epochMembers0 is a nil-safe helper for the clause above.
func (c *Cluster) reconcileLegacyPeer(peerID string, theirIDs []string) error {
	theirs := make(map[string]struct{}, len(theirIDs))
	for _, id := range theirIDs {
		theirs[id] = struct{}{}
	}
	if memberSetsEqual(theirs, c.memberIDSet()) {
		if c.mismatch.Swap(false) {
			c.logger.Info("cluster membership agrees with peer again", "node_id", peerID)
		}
		return nil
	}
	err := fmt.Errorf("%w with peer %s: membership differs; every node must agree on the member set or placement will diverge",
		ErrMemberMismatch, peerID)
	c.mismatch.Store(true)
	c.logger.Error("cluster membership mismatch with a pre-epoch peer", "node_id", peerID, "error", err)
	return err
}

// memberAddrDisagreement is a node id that this node's member list and a
// peer's place at different addresses.
type memberAddrDisagreement struct {
	id, mine, theirs string
}

// addrDisagreement returns the node id, first in id order, that their member
// list places at another address than this node's does.
func (c *Cluster) addrDisagreement(their []ring.Node) (memberAddrDisagreement, bool) {
	_, members := c.epochMembers()
	addrOf := make(map[string]string, len(members))
	for _, m := range members {
		addrOf[m.ID] = m.Addr
	}
	var first memberAddrDisagreement
	found := false
	for _, n := range their {
		mine, listed := addrOf[n.ID]
		if !listed || n.Addr == "" || n.Addr == mine {
			continue
		}
		if !found || n.ID < first.id {
			first = memberAddrDisagreement{id: n.ID, mine: mine, theirs: n.Addr}
			found = true
		}
	}
	return first, found
}

// refuseAddrDisagreement refuses a member list that places a node id at
// another address than this node's: one of the two nodes running as that id is
// not the node the other member list means. It returns the refusal, which a
// gossip answer carries back to the peer. When the id in question is the
// peer's own, the peer is the node this node does not know as that id, and
// this node's list stays undisputed: a node started with the wrong list must
// not keep the members it contradicts from being confirmed. Otherwise this
// node is never confirmed on this list. Only a disagreement between two
// confirmed nodes is a divergence that stops writes.
func (c *Cluster) refuseAddrDisagreement(peerID string, disagreement memberAddrDisagreement, peerBootstrapped bool) error {
	err := fmt.Errorf("%w: node id %q is at %s in this node's member list and at %s in member %s's; "+
		"two nodes under one id would both own its buckets and both act as the auth master. "+
		"Give every node the same HOARDDB_SEED_NODES, each node listed once as <node-id>@<address> under its own HOARDDB_NODE_ID, "+
		"and restart the node whose list differs",
		ErrMemberMismatch, disagreement.id, disagreement.mine, disagreement.theirs, peerID)
	if disagreement.id != peerID {
		c.noteViewDispute()
	}
	c.noteMembershipRefusal(peerID, err)
	if !c.bootstrapped.Load() && !peerBootstrapped {
		c.mismatch.Store(true)
	}
	c.logger.Error("a member places a node id at another address", "node_id", peerID,
		"disputed_id", disagreement.id, "addr", disagreement.mine, "peer_addr", disagreement.theirs, "error", err)
	return err
}

// reconcileRejectedView is the asker's side of a gossip answer that refused
// its view: it returns the refusal of the answer's member list when that list
// places a node id at another address, so the asker records the same
// disagreement the peer refused it for. Any other refusal returns nil.
func (c *Cluster) reconcileRejectedView(peerID string, resp GossipResponse) error {
	if len(resp.Nodes) == 0 || resp.Epoch != c.Epoch() {
		return nil
	}
	their := peersToNodes(resp.Nodes)
	theirIDs := memberIDSetOf(their)
	if !memberSetsEqual(theirIDs, c.memberIDSet()) {
		return nil
	}
	disagreement, found := c.addrDisagreement(their)
	if !found {
		return nil
	}
	return c.refuseAddrDisagreement(peerID, disagreement, resp.Bootstrapped)
}

// locateSelf finds this node's own record in a persisted member set. The
// identity a node uses is the one it is listed under (its address), not the
// config-derived one, so adoption of a member set must re-resolve self the
// same way memberList does for the config path.
//
// The id fallback serves a node added by 'join <id>@<address>' under another
// name for its listen address, and a node on a wildcard listener (BUG-0318),
// whose address never equals the listed one. On a concrete listener that is
// another endpoint, the record is another process's: a copy of its data
// directory started elsewhere would run as it (BUG-0600).
func locateSelf(members []ring.Node, cfgSelf ring.Node, listen, membershipPath string) (ring.Node, error) {
	for _, m := range members {
		if m.Addr == cfgSelf.Addr {
			return m, nil
		}
	}
	for _, m := range members {
		if m.ID != cfgSelf.ID {
			continue
		}
		if config.ListensOnWildcard(listen) || sameEndpoint(m.Addr, cfgSelf.Addr) {
			return m, nil
		}
		wildcard := "0.0.0.0"
		if _, port, splitErr := net.SplitHostPort(m.Addr); splitErr == nil {
			wildcard = net.JoinHostPort("0.0.0.0", port)
		}
		return ring.Node{}, fmt.Errorf("cluster: this node listens on %s, but the member set in %s lists its node id %q at %s, "+
			"another address: this looks like a copy of that node's data directory (a cloned disk or a restored backup), and two "+
			"processes under one id would both own its buckets and both act as the auth master. If this is a copy, wipe its data "+
			"directory, start it with its own HOARDDB_NODE_ID and run 'join <node-id>@%s' on a member; if this is node %q itself, "+
			"set HOARDDB_NODE_ID=%q and HOARDDB_LISTEN=%s, which is the fix when its address changed (a moved VM) or when %s "+
			"resolves on this host to another address (for example a 127.0.1.1 hosts entry); if %s does reach this node, set "+
			"HOARDDB_LISTEN=%s instead",
			cfgSelf.Addr, membershipPath, m.ID, m.Addr, cfgSelf.Addr, m.ID, m.ID, wildcard, m.Addr, m.Addr, m.Addr)
	}
	return ring.Node{}, fmt.Errorf("cluster: node at %q is not a member of the persisted member set %v; "+
		"remove membership.json to re-bootstrap from config, or list this node in the set",
		cfgSelf.Addr, memberIDsOf(members))
}

// endpointLookupWait bounds the name lookups of sameEndpoint, which run once at
// start and only on locateSelf's id fallback.
const endpointLookupWait = 2 * time.Second

// sameEndpoint reports whether two host:port addresses can be one listener:
// the same port, and hosts that are equal or resolve to a shared address. A
// lookup that fails cannot tell them apart, and counts as the same: refusing a
// node's start on a resolver outage would be worse than the duplicate
// detection that still runs (node_instance.go).
func sameEndpoint(listed, own string) bool {
	listedHost, listedPort, err := net.SplitHostPort(listed)
	if err != nil {
		return true
	}
	ownHost, ownPort, err := net.SplitHostPort(own)
	if err != nil {
		return true
	}
	if listedPort != ownPort {
		return false
	}
	if strings.EqualFold(listedHost, ownHost) {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), endpointLookupWait)
	defer cancel()
	listedIPs, err := net.DefaultResolver.LookupIP(ctx, "ip", listedHost)
	if err != nil {
		return true
	}
	ownIPs, err := net.DefaultResolver.LookupIP(ctx, "ip", ownHost)
	if err != nil {
		return true
	}
	for _, listedIP := range listedIPs {
		for _, ownIP := range ownIPs {
			if listedIP.Equal(ownIP) {
				return true
			}
		}
	}
	return false
}

// memberSetsEqual reports whether two member-id sets describe the same set.
func memberSetsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}

// memberIDsOf returns the member ids in the given order.
func memberIDsOf(members []ring.Node) []string {
	ids := make([]string, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.ID)
	}
	return ids
}

func nodesToPeers(nodes []ring.Node) []Peer {
	if len(nodes) == 0 {
		return nil
	}
	ps := make([]Peer, 0, len(nodes))
	for _, n := range nodes {
		ps = append(ps, Peer{ID: n.ID, Addr: n.Addr})
	}
	return ps
}
