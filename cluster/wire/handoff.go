package wire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The kind bytes of conventions 6.1 that the barrier handlers check. HANDOFF_STOPPED is
// transport.KindHandoffStopped; the others have no constant there.
const (
	kindHandoffReq     byte = 0x05
	kindHandoffReady   byte = 0x06
	kindHandoffStopped byte = transport.KindHandoffStopped
	kindPositionQuery  byte = 0x12
	kindPositionReply  byte = 0x13
)

// defaultHandoffRetry is replication.handoff_retry_ms.
const defaultHandoffRetry = 200 * time.Millisecond

// HandoffLink sends the barrier messages and the position query on the data channel.
// One call is one message: the Handoff has already grouped the slots of one database
// for one peer into the message's bitmap.
type HandoffLink interface {
	// SendHandoffReq sends one HANDOFF_REQ for database db to peer.
	SendHandoffReq(ctx context.Context, peer string, db uint64, msg HandoffReq) error
	// SendHandoffStopped sends one HANDOFF_STOPPED for database db to peer.
	SendHandoffStopped(ctx context.Context, peer string, db uint64, msg HandoffStopped) error
	// SendHandoffReady sends one HANDOFF_READY for database db to peer.
	SendHandoffReady(ctx context.Context, peer string, db uint64, msg HandoffReady) error
	// QueryPosition asks peer for its own record of one pair (POSITION_QUERY) and
	// waits for the POSITION_REPLY. It returns an error when the peer does not answer.
	QueryPosition(ctx context.Context, peer string, query PositionQuery) (PositionReply, error)
}

// HandoffCatchUp is the replicator's catch-up half. *Replicator implements it.
type HandoffCatchUp interface {
	// CatchUp brings this node's copy of the pair to the source's tail.
	CatchUp(ctx context.Context, slot uint16, db uint64, from cluster.Position, cursor uint64) error
	// IsCurrent reports whether the last completed catch-up marked the pair current.
	IsCurrent(slot uint16, db uint64) bool
}

// HandoffDrain latches the drain a HANDOFF_READY covers. *Replicator implements it.
type HandoffDrain interface {
	// NoteDrained records that every position up to upTo is drained.
	NoteDrained(slot uint16, db uint64, upTo cluster.Position)
}

// HandoffConfig is the collaborators of a Handoff.
type HandoffConfig struct {
	// Self is this node's id.
	Self string
	// Pairs is the pair state the write path shares: the Handoff sets and clears its
	// sending, awaiting and stopProved bits, always under the pair's lock.
	Pairs *cluster.PairSet
	// Link sends the barrier messages.
	Link HandoffLink
	// CatchUp is the replicator's catch-up.
	CatchUp HandoffCatchUp
	// Drain receives the drain latch of a HANDOFF_READY.
	Drain HandoffDrain
	// Clock is the monotonic clock the lease bound is measured on.
	Clock machine.Clock
	// Ready reports whether this node's own copy of the pair is ready (conv section 12);
	// nil means always. It decides whether this node's own record counts as an answer to
	// its own position query.
	Ready func(slot uint16, db uint64) bool
	// Retry is replication.handoff_retry_ms; 0 takes 200 ms. A caller whose deadline is
	// not longer than this does not wait for an unreachable old owner.
	Retry time.Duration
	// Opened, when set, is called once for a pair that became this node's to order by a move
	// (a restart hold does not count), after the pair's lock is released and with the Handoff's
	// lock held, so it must not wait for I/O. The node uses it to queue the pair's tail for the
	// copies (BUG-0746).
	Opened func(slot uint16, db uint64)
	// Restarted says a previous process of this node had joined a cluster (BUG-0743). A
	// barrier its replayed epochs open inside the stop bound after boot is described as the
	// restart hold it is, not as a handoff someone could still be running.
	Restarted bool
	// Deferred reports whether the boot replay of this node's own pair has not finished
	// (BUG-0760): its log holds acknowledged records above its applied position, so that
	// position is neither counted as an answer nor sent in a HANDOFF_READY. Nil means never.
	Deferred func(slot uint16, db uint64) bool
}

// HeldRequest is one operation a hold parks (write path section 8, conventions section 8
// check 2). It carries no position.
type HeldRequest struct {
	Slot uint16
	DB   uint64
	// RequestID is kept exactly as the client sent it: the new owner dedupes a retry by it.
	RequestID [16]byte
	// Body is the operation, kept for the forwarding path.
	Body []byte
	// Read says the operation is a read.
	Read bool
	// PeerReachable says the other node of the barrier answers; it only matters to a read
	// at the awaiting side.
	PeerReachable bool
	// Deadline is the caller's remaining budget; 0 means none.
	Deadline time.Duration
}

// HoldRelease says where a parked operation goes when its hold ends. The parked caller
// is the only one who acts on it: the Handoff never forwards or answers a held operation
// itself, so a held read cannot be answered twice.
type HoldRelease struct {
	// Owner is the node that owns the pair now, empty when no member does.
	Owner string
	// Local says the owner is this node: the caller re-enters the ordinary path here.
	// Otherwise the caller forwards to Owner with the same request id.
	Local bool
}

// holdWaiter is one parked caller. rel is written before done is closed.
type holdWaiter struct {
	done chan struct{}
	rel  HoldRelease
}

// handoffPair is the barrier wiring state of one (slot, database) pair, kept only while
// a barrier is open for it or a READY it sent can still be asked for again. It is one
// value per moved pair: nothing in it is per bucket.
type handoffPair struct {
	barrier machine.BarrierPair
	rf      int
	// fromEpoch is the epoch before the commit that opened the barrier.
	fromEpoch uint64
	// gen counts the epochs fed to the pair, so a slow catch-up or position query that
	// finishes after a further epoch change is dropped.
	gen uint64
	// oldCopies are the members of the copy set at fromEpoch that were not committed down, and
	// oldDown the ones that were: the copy-set rule (conv section 10 rule 2) covers both, so
	// neither is asked again as a holder outside the copy set.
	oldCopies   []string
	oldDown     []string
	copiesKnown bool
	// target is the highest position the old copy set reported (conv section 10 rule 2).
	target      cluster.Position
	targetKnown bool
	// caughtUp says a catch-up finished since the old owner was proved stopped.
	caughtUp bool
	// restartWindow says the barrier opened inside the stop bound after this process booted.
	restartWindow bool
	// waiters are the parked callers, one per entry of barrier.Held, in the same order.
	waiters []*holdWaiter
	// opened says Opened was called for this pair.
	opened bool
}

// Handoff wires the primary-handoff barrier (write path section 8) to the pair state, the
// replicator and the data channel. It holds one machine.BarrierPair per pair that is in a
// barrier, steers it per (peer, database), and parks operations the hold refuses.
//
// Cost: one entry per pair this node moves in or out of, freed when the barrier ends and
// the next epoch arrives; a bucket adds nothing, and there is no goroutine, no timer and
// no file descriptor: the caller drives Tick every handoff_retry_ms. A membership change
// is O(slots x databases) to scan, never O(buckets).
//
// Lock order: the Handoff's lock, then a pair's lock, then targetMu. The write path takes
// a pair lock and may call the Handoff only through CachedTarget, which takes targetMu
// alone; every other method takes the Handoff's lock and must not be called under a pair
// lock (BUG-0744).
type Handoff struct {
	cfg HandoffConfig

	mu    sync.Mutex
	epoch uint64
	// applied is the last epoch OnEpoch finished feeding, 0 before the first. The gates read
	// it under the pair lock, where taking mu would invert the order (mu, then pair lock).
	applied atomic.Uint64
	table   *cluster.SlotTable
	rf      map[uint64]int
	// before is the table of the epoch before the current one, kept so a catch-up from a
	// copy that has not yet applied the change is still served (CATCHUP_REQ role check).
	before *cluster.SlotTable
	states map[string]cluster.MemberState
	// epochBound is step_down_ms + heartbeat_ms of the first state fed at the current epoch:
	// what the leases of the owners before that commit were granted under. stateBound is the
	// same sum in the latest state fed at the current epoch, which a settings change commits
	// without moving the epoch (BUG-0813). stopBound is the larger of the two.
	epochBound, stateBound time.Duration
	// fedSeq is the state_seq of the latest state fed (SawState).
	fedSeq int64
	// bootedAt is this process's start on the boot clock: the origin of the stop bound a
	// restarted owner waits (RestartHold).
	bootedAt time.Duration
	// advanceAfter is the last pair a Tick advanced; the next Tick starts after it.
	advanceAfter   pairKey
	advanceStarted bool
	// degradedNoted is the epoch a database last logged a degraded target at.
	degradedNoted map[uint64]uint64
	// seeded is the committed SEEDED rows of the latest state fed. A RETIRE clears a member's
	// bit at a state_seq that moves no epoch, so it follows every state, not only an epoch
	// move. It is shared with the fed state and never written. deadAfter is that state's
	// dead_ms: how long a down member is still expected back with its data.
	seeded    []cluster.SeedBitmap
	deadAfter time.Duration
	// holderNoted is the epoch a database last logged a hold on, or an opening without, a
	// holder outside the copy set (BUG-0834).
	holderNoted map[holderNote]uint64
	pairs       map[pairKey]*handoffPair
	// targetMu guards targets, the targets CachedTarget serves. It is a leaf lock: the write
	// path asks for a target while it holds a pair's lock, so the answer cannot need h.mu
	// (BUG-0744). Writers hold h.mu and then targetMu, never the other way round.
	targetMu sync.Mutex
	targets  map[pairKey]publishedTarget
}

// NewHandoff returns a Handoff over cfg.
func NewHandoff(cfg HandoffConfig) *Handoff {
	if cfg.Retry <= 0 {
		cfg.Retry = defaultHandoffRetry
	}
	return &Handoff{
		cfg: cfg, bootedAt: cfg.Clock.BootNow(),
		pairs: make(map[pairKey]*handoffPair), targets: make(map[pairKey]publishedTarget),
	}
}

// outKey names one message: the peer, the database and the epochs it carries.
type outKey struct {
	peer     string
	db       uint64
	from, to uint64
}

type readyOut struct {
	slots   []uint16
	applied cluster.Position
}

// handoffOut collects the messages one step produces so they leave as one per
// (peer, database), and are sent after the lock is released.
type handoffOut struct {
	reqs    map[outKey][]uint16
	stops   map[outKey][]uint16
	readies map[outKey]*readyOut
}

func (o *handoffOut) addSlot(group *map[outKey][]uint16, key outKey, slot uint16) {
	if *group == nil {
		*group = make(map[outKey][]uint16)
	}
	(*group)[key] = append((*group)[key], slot)
}

func (o *handoffOut) addReady(key outKey, slot uint16, applied cluster.Position) {
	if o.readies == nil {
		o.readies = make(map[outKey]*readyOut)
	}
	entry := o.readies[key]
	if entry == nil {
		entry = &readyOut{applied: applied}
		o.readies[key] = entry
	}
	entry.slots = append(entry.slots, slot)
	// One appliedPos covers the message: the least over its pairs (build SPEC-GAP G2).
	if applied.Less(entry.applied) {
		entry.applied = applied
	}
}

func sortedKeys[V any](group map[outKey]V) []outKey {
	keys := make([]outKey, 0, len(group))
	for key := range group {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.peer != b.peer {
			return a.peer < b.peer
		}
		if a.db != b.db {
			return a.db < b.db
		}
		if a.to != b.to {
			return a.to < b.to
		}
		return a.from < b.from
	})
	return keys
}

// flush sends the collected messages, one per (peer, database), and reports every
// failure: a lost message is retransmitted by the next Tick, but the caller still sees it.
func (h *Handoff) flush(ctx context.Context, out *handoffOut) error {
	var errs []error
	for _, key := range sortedKeys(out.reqs) {
		msg := HandoffReq{FromEpoch: key.from, ToEpoch: key.to, Bitmap: BitmapOfSlots(out.reqs[key])}
		if err := h.cfg.Link.SendHandoffReq(ctx, key.peer, key.db, msg); err != nil {
			errs = append(errs, fmt.Errorf("sending HANDOFF_REQ for database %d to %s: %w", key.db, key.peer, err))
		}
	}
	for _, key := range sortedKeys(out.stops) {
		msg := HandoffStopped{ToEpoch: key.to, Bitmap: BitmapOfSlots(out.stops[key])}
		if err := h.cfg.Link.SendHandoffStopped(ctx, key.peer, key.db, msg); err != nil {
			errs = append(errs, fmt.Errorf("sending HANDOFF_STOPPED for database %d to %s: %w", key.db, key.peer, err))
		}
	}
	for _, key := range sortedKeys(out.readies) {
		ready := out.readies[key]
		msg := HandoffReady{ToEpoch: key.to, Bitmap: BitmapOfSlots(ready.slots), AppliedPos: ready.applied}
		if err := h.cfg.Link.SendHandoffReady(ctx, key.peer, key.db, msg); err != nil {
			errs = append(errs, fmt.Errorf("sending HANDOFF_READY for database %d to %s: %w", key.db, key.peer, err))
		}
	}
	return errors.Join(errs...)
}

// syncBarrierFlags mirrors the machine's hold into the pair's flag bits the write path
// reads. It runs under the pair's lock, in the same step as the machine's decision, so
// the write path never sees a half-applied one. stopProved is raised only together with
// the end of the awaiting hold (READY), together with replayed, and dropped whenever a
// hold is open.
func syncBarrierFlags(p *cluster.Pair, barrier *machine.BarrierPair, restarted bool) {
	p.Flags &^= flagSending | flagAwaiting | flagRestarted
	switch barrier.Hold {
	case machine.HoldSending:
		p.Flags |= flagSending
		p.DrainTo, p.Peer = barrier.DrainTo, barrier.Peer
	case machine.HoldAwaiting:
		p.Flags |= flagAwaiting
		p.Peer = barrier.Peer
		if restarted {
			p.Flags |= flagRestarted
		}
	}
	if barrier.Hold != machine.HoldNone {
		p.Flags &^= flagStopProved
	} else if barrier.ReadySent {
		// READY is the proof of both: the old owner stopped, and this node holds every
		// position it was told of. A pair that became owner by handoff never goes through
		// Acquire, so nothing else would mark its log tail replayed.
		p.Flags |= flagStopProved | flagReplayed
	}
}

// apply feeds one event to the pair's machine and mirrors the result into the pair flags
// under the pair's lock. When the machine hands back held operations it wakes their
// parked callers: they are the only ones who act on them. Callers hold h.mu.
func (h *Handoff) apply(key pairKey, hp *handoffPair, event machine.BarrierEvent) machine.BarrierAction {
	var act machine.BarrierAction
	h.cfg.Pairs.With(key.slot, key.db, func(p *cluster.Pair) {
		act = hp.barrier.Apply(event)
		syncBarrierFlags(p, &hp.barrier, hp.restartWindow)
	})
	// fromEpoch below the current epoch is a move; a restart hold has fromEpoch equal to it
	// and is shipped by the node's own sweep.
	// Lock order: Opened runs under h.mu and takes the pair lock and the replicator's lock. No
	// I/O may be added to it.
	if !hp.opened && hp.fromEpoch < h.epoch && hp.barrier.Hold == machine.HoldNone && hp.barrier.ReadySent {
		hp.opened = true
		if h.cfg.Opened != nil {
			h.cfg.Opened(key.slot, key.db)
		}
	}
	if len(act.ForwardHeld) > 0 {
		h.release(key, hp)
	}
	return act
}

// release wakes every parked caller of the pair with where its operation goes now. The
// machine has already emptied its Held list for these operations, so nothing else holds
// them. Callers hold h.mu.
func (h *Handoff) release(key pairKey, hp *handoffPair) {
	owner := h.ownerID(key.slot, hp.rf)
	release := HoldRelease{Owner: owner, Local: owner == h.cfg.Self}
	for _, waiter := range hp.waiters {
		waiter.rel = release
		close(waiter.done)
	}
	hp.waiters = nil
}

func (h *Handoff) ownerID(slot uint16, rf int) string {
	if h.table == nil {
		return ""
	}
	owner, ok := h.table.Owner(slot, rf)
	if !ok {
		return ""
	}
	return owner.ID
}

// queue turns the machine's decision into messages. A message goes to the barrier's
// peer; with no peer (a further epoch left this node rank 1 with nobody to tell) there
// is nobody to send to. Callers hold h.mu.
func (h *Handoff) queue(key pairKey, hp *handoffPair, act machine.BarrierAction, out *handoffOut) {
	peer := hp.barrier.Peer
	if peer == "" {
		return
	}
	to := hp.barrier.ToEpoch
	if act.SendHandoffReq {
		out.addSlot(&out.reqs, outKey{peer: peer, db: key.db, from: hp.fromEpoch, to: to}, key.slot)
	}
	if act.SendStopped {
		out.addSlot(&out.stops, outKey{peer: peer, db: key.db, to: to}, key.slot)
	}
	// A READY carries the pair's applied position and the old owner latches its drain there, so
	// none is sent for a pair whose boot replay has not finished. The machine has recorded the
	// READY as sent and answers the old owner's next request, which its Tick repeats while it has
	// no READY, with another: that one is sent, with the replayed position, after the release.
	if act.SendReady && !h.deferred(key.slot, key.db) {
		applied := h.cfg.Pairs.Snapshot(key.slot, key.db).AppliedPos
		out.addReady(outKey{peer: peer, db: key.db, to: to}, key.slot, applied)
	}
}

// finish frees a pair's entry once nothing about it can still matter: no hold, no READY
// that a repeated REQ could ask for again, no parked caller. Callers hold h.mu.
func (h *Handoff) finish(key pairKey, hp *handoffPair) {
	if hp.barrier.Hold == machine.HoldNone && !hp.barrier.ReadySent && len(hp.waiters) == 0 {
		delete(h.pairs, key)
		h.publishTarget(key, publishedTarget{}, false)
	}
}

// handoffPrev is the view of the epoch before the one being applied.
type handoffPrev struct {
	table *cluster.SlotTable
	epoch uint64
	rf    map[uint64]int
}

// TableBefore is the slot table of the epoch before the one this handoff last saw, nil until
// two epochs have been fed. It serves the CATCHUP_REQ role check, which must still answer a
// copy that has not applied an epoch change.
func (h *Handoff) TableBefore() *cluster.SlotTable {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.before
}

// Applied reports whether the Handoff has been fed the committed epoch. Before its first
// state there is no earlier owner to prove stopped, so it answers true.
func (h *Handoff) Applied(committed uint64) bool {
	applied := h.applied.Load()
	return applied == 0 || applied >= committed
}

// OnEpoch feeds a committed cluster state to every pair it can change. Call it for every
// committed epoch, in order, including the ones that move nothing: a pair in a barrier
// needs them to tell a further epoch change from the change that opened it. Call it too
// for a state that moved no epoch: it follows the databases and the stop bound of that
// state (BUG-0747, BUG-0813). The first state only becomes the baseline. It sends nothing:
// REQ and STOPPED go out on the next Tick, which sends them for every pair still in
// sending. The cost is O(slots x databases) table lookups and one machine step per pair
// that moved or is in a barrier.
func (h *Handoff) OnEpoch(state cluster.ClusterState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	epoch := uint64(state.Epoch)
	bound := settingsBound(state.Settings)
	if h.table != nil && epoch <= h.epoch {
		if epoch == h.epoch {
			h.refreshDatabases(state)
			// h.states is not refreshed here on purpose: a member's state moves the epoch whenever
			// it changes who is ranked (down, dead, joining to active). h.states is read for the
			// "alone" test in RestartHold and by holdersOf, for every member holding a SEEDED bit,
			// ranked or not. The states that can change inside an epoch (active to leaving, an
			// unranked member added or removed) leave those reads as they were or err toward
			// holding longer; a joiner added inside the epoch reads as the zero state, which
			// holdersOf ignores, and that is safe because a joiner never counted for a write.
			// A barrier already open keeps the bound it was opened with; only the barriers
			// opened from here on read the new one.
			if state.StateSeq >= h.fedSeq {
				h.seeded, h.deadAfter = state.SeedBitmaps, time.Duration(state.Settings.DeadMs)*time.Millisecond
			}
			h.stateBound, h.fedSeq = bound, max(h.fedSeq, state.StateSeq)
		}
		return
	}
	// Stored after every pair was fed, so a gate that sees the epoch sees its barriers.
	defer h.applied.Store(epoch)
	prev := handoffPrev{table: h.table, epoch: h.epoch, rf: h.rf}
	h.before = h.table
	h.table, h.epoch = cluster.BuildTable(state), epoch
	h.rf = databaseRF(state)
	h.states = make(map[string]cluster.MemberState, len(state.Members))
	for _, member := range state.Members {
		h.states[member.ID] = member.State
	}
	h.epochBound, h.stateBound, h.fedSeq = bound, bound, max(h.fedSeq, state.StateSeq)
	h.seeded, h.deadAfter = state.SeedBitmaps, time.Duration(state.Settings.DeadMs)*time.Millisecond
	if prev.table == nil {
		return
	}
	now := h.cfg.Clock.BootNow()
	for _, key := range h.affectedKeys(prev) {
		h.feedEpoch(key, prev, now)
	}
}

// settingsBound is step_down_ms + heartbeat_ms: how long a lease granted under settings can
// still run after its grant (conventions 15).
func settingsBound(settings cluster.Settings) time.Duration {
	return time.Duration(settings.StepDownMs+settings.HeartbeatMs) * time.Millisecond
}

// stopBound is the bound a barrier opened now waits. A step_down_ms raised after the epoch's
// commit lengthens it, so a hold follows the committed value; one lowered after it does not
// shorten it, because the owner before the commit took its last grant under the epoch's
// value (BUG-0813). Callers hold h.mu.
func (h *Handoff) stopBound() time.Duration {
	return max(h.epochBound, h.stateBound)
}

// SawState reports whether the handoff has been fed the state at seq or a later one, so a
// barrier it opens now reads the stop bound of that state (BUG-0813).
func (h *Handoff) SawState(seq int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.table != nil && h.fedSeq >= seq
}

// refreshDatabases follows a state at the current epoch that created, altered or dropped a
// database (BUG-0747): the next epoch move must find a new database in rf, or its pairs open
// with no barrier, and a dropped database must not keep a barrier that waits for an epoch
// move that may never come. A state that changed no database costs a comparison and no
// allocation, because it runs on every supervisor step that sees a new state.
func (h *Handoff) refreshDatabases(state cluster.ClusterState) {
	if len(h.rf) == len(state.Databases) {
		unchanged := true
		for _, db := range state.Databases {
			if rf, listed := h.rf[uint64(db.ID)]; !listed || rf != int(db.RF) {
				unchanged = false
				break
			}
		}
		if unchanged {
			return
		}
	}
	h.rf = databaseRF(state)
	for key, hp := range h.pairs {
		if _, listed := h.rf[key.db]; !listed {
			h.release(key, hp)
			delete(h.pairs, key)
			h.publishTarget(key, publishedTarget{}, false)
		}
	}
}

// databaseRF maps each database of a committed state to its replication factor.
func databaseRF(state cluster.ClusterState) map[uint64]int {
	rfs := make(map[uint64]int, len(state.Databases))
	for _, db := range state.Databases {
		rfs[uint64(db.ID)] = int(db.RF)
	}
	return rfs
}

// affectedKeys is every pair with an entry plus every pair whose owner changed to or
// from this node. A database missing from the previous rf is skipped: the supervisor feeds
// the handoff every state that changed the database list (queueState, refreshDatabases), so
// one created before this epoch moved is in rf unless the state was never fed (BUG-0747).
func (h *Handoff) affectedKeys(prev handoffPrev) []pairKey {
	seen := make(map[pairKey]struct{}, len(h.pairs))
	for key := range h.pairs {
		seen[key] = struct{}{}
	}
	for db, rf := range h.rf {
		prevRF, existed := prev.rf[db]
		if !existed {
			continue
		}
		for slot := range uint16(barrierSlots) {
			before, _ := prev.table.Owner(slot, prevRF)
			after, _ := h.table.Owner(slot, rf)
			if before.ID != after.ID && (before.ID == h.cfg.Self || after.ID == h.cfg.Self) {
				seen[pairKey{slot, db}] = struct{}{}
			}
		}
	}
	keys := make([]pairKey, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].db != keys[j].db {
			return keys[i].db < keys[j].db
		}
		return keys[i].slot < keys[j].slot
	})
	return keys
}

// feedEpoch gives one pair the committed epoch. Callers hold h.mu.
func (h *Handoff) feedEpoch(key pairKey, prev handoffPrev, now time.Duration) {
	hp := h.pairs[key]
	rf, known := h.rf[key.db]
	if !known {
		// The database is gone: whatever was parked on it goes back to the ordinary path.
		if hp != nil {
			h.release(key, hp)
			delete(h.pairs, key)
			h.publishTarget(key, publishedTarget{}, false)
		}
		return
	}
	before, _ := prev.table.Owner(key.slot, prev.rf[key.db])
	after, _ := h.table.Owner(key.slot, rf)
	if hp == nil {
		hp = &handoffPair{}
		hp.barrier.Epoch = prev.epoch
		h.pairs[key] = hp
	}
	hp.rf, hp.fromEpoch = rf, prev.epoch
	hp.restartWindow = h.cfg.Restarted && now-h.bootedAt < h.stopBound()
	hp.gen++
	hp.opened = false // a further move opens the pair again
	hp.target, hp.targetKnown, hp.caughtUp = cluster.Position{}, false, false
	h.publishTarget(key, publishedTarget{}, false)
	hp.oldCopies, hp.oldDown, hp.copiesKnown = nil, nil, false
	event := machine.EpochCommitted{
		At: now, Epoch: h.epoch, SelfID: h.cfg.Self, OldRank1ID: before.ID, Rank1ID: after.ID, Moved: before.ID != after.ID,
		Appended: appendedAt(h.cfg.Pairs.Snapshot(key.slot, key.db), prev.epoch), Bound: h.stopBound(),
	}
	h.apply(key, hp, event)
	if hp.barrier.Hold == machine.HoldAwaiting {
		hp.oldCopies, hp.oldDown = h.splitDown(prev.table.Copies(key.slot, prev.rf[key.db]))
		hp.copiesKnown = true
	}
	h.finish(key, hp)
}

// Saw reports whether the handoff has been fed the committed epoch, so what it knows of the
// ring is the settled state and not an older one.
func (h *Handoff) Saw(epoch uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.table != nil && h.epoch == epoch
}

// RestartHold opens the lazy restart barrier for a pair this node may own by a move it
// never saw (BUG-0743, SPEC-GAP U57-19). The caller is the first touch of the pair after
// boot, so nothing is enumerated at boot and nothing depends on the epoch replay. rf and
// epoch come from the committed cluster state, never from h.rf, which may not list a
// database created at the current epoch yet and would let feedEpoch cancel the hold as
// "database gone".
// The owner and the copies come from the table of that same epoch, the bound from stopBound.
//
// held is true when a barrier is open for the pair (now or already): the pair serves only
// once the stop bound counted from boot has passed and the pair caught up to its copies.
// held is false when the pair is not this node's to order, so the ordinary rule applies.
// decided is false while the handoff has not seen the epoch yet; the caller asks again.
// A single-member cluster has no other owner, so its bound is zero. It must not be called
// under a pair lock.
func (h *Handoff) RestartHold(slot uint16, db uint64, rf int, epoch uint64) (held, decided bool) {
	key := pairKey{slot, db}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.table == nil || h.epoch != epoch {
		return false, false
	}
	if existing := h.pairs[key]; existing != nil {
		// A barrier since boot already owns the pair, or finished with READY: a second
		// touch must neither reset its timing and target nor reopen it.
		return existing.barrier.Hold != machine.HoldNone, true
	}
	if owner, ok := h.table.Owner(slot, rf); !ok || owner.ID != h.cfg.Self {
		return false, true
	}
	bound := h.stopBound()
	if len(h.states) <= 1 {
		bound = 0
	}
	hp := &handoffPair{}
	h.pairs[key] = hp
	hp.rf, hp.fromEpoch = rf, epoch
	hp.gen++
	hp.target, hp.targetKnown, hp.caughtUp = cluster.Position{}, false, false
	hp.oldCopies, hp.oldDown = h.splitDown(h.table.Copies(slot, rf))
	hp.copiesKnown = true
	h.apply(key, hp, machine.RestartHold{At: h.bootedAt, Bound: bound, Epoch: epoch})
	// After apply, so the order is the documented one: h.mu, the pair's lock, targetMu.
	h.publishTarget(key, publishedTarget{}, false)
	held = hp.barrier.Hold != machine.HoldNone
	h.finish(key, hp)
	return held, true
}

// splitDown parts a copy set by the members' states now: live are the ones not committed
// down or gone, self included, whose answers a new owner (the copy set at the previous epoch,
// conv section 10 rule 2) or a restarted owner (the current one) needs; down are the ones
// committed down or dead, whose loss the copy-set rule already accounts for. Callers hold h.mu.
func (h *Handoff) splitDown(copies []cluster.Member) (live, down []string) {
	for _, member := range copies {
		if state, present := h.states[member.ID]; present && state != cluster.MemberDown && state != cluster.MemberDead {
			live = append(live, member.ID)
		} else {
			down = append(down, member.ID)
		}
	}
	return live, down
}

// pairHolders are the members outside a pair's copy-set rule whose committed SEEDED bit for
// the pair is still set (BUG-0834). The bit is cleared by the member's RETIRE, which commits
// only after its deletion gate passed (seeding section 8), so a member that left the copy set
// keeps it exactly while it may hold acknowledged writes that no copy has: an activation adds
// a copy before that copy holds the data.
type pairHolders struct {
	// live may answer for their position: active or leaving.
	live []string
	// down are committed down: what only they hold may come back with them, until they are
	// declared dead.
	down []string
	// dead were declared dead: a member that returns after that is a new identity holding
	// nothing, so what only they held is gone.
	dead []string
}

// holdersOf lists the pair's holders other than self and the members in skip. A joining
// member's pulled pairs carry its bit too, but a joiner is in no copy set, never counted for a
// write concern, and so holds no acknowledged write the copy set did not; waiting for one
// would hold every promotion of the pairs it pulled. The cost is one bit read per committed
// SEEDED row (members x databases). Callers hold h.mu.
func (h *Handoff) holdersOf(slot uint16, db uint64, skip []string) pairHolders {
	var found pairHolders
	for _, row := range h.seeded {
		if uint64(row.DBID) != db || row.Node == h.cfg.Self || slices.Contains(skip, row.Node) {
			continue
		}
		if set, err := machine.Bit(row.Bits, slot); err != nil || !set {
			continue
		}
		switch h.states[row.Node] {
		case cluster.MemberActive, cluster.MemberLeaving:
			found.live = append(found.live, row.Node)
		case cluster.MemberDown:
			found.down = append(found.down, row.Node)
		case cluster.MemberDead:
			found.dead = append(found.dead, row.Node)
		}
	}
	return found
}

// Holders lists the members other than this node that hold the pair's committed SEEDED bit
// and may answer: active or leaving. A catch-up asks them besides the copy set, so a copy that
// is behind can read from a holder that left the copy set and still has every acknowledged
// write (BUG-0834).
func (h *Handoff) Holders(slot uint16, db uint64) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.holdersOf(slot, db, nil).live
}

// handoffAdvancePerTick caps how many awaiting pairs one Tick advances, and
// handoffAdvanceBudget caps the time it spends doing so. A restart makes every pair a node
// owns provable on the same Tick (they share the boot instant), and each advance is up to
// rf-1 position queries and a catch-up; Tick is the supervisor's last step, so a long Tick
// delays the failure detector and every later step (since lease-renewer-cadence S2 it no
// longer delays the lease renewal, which runs on its own goroutine).
// The pairs left over are advanced by the following Ticks, in rotation.
const (
	handoffAdvancePerTick = 256
	handoffAdvanceBudget  = 250 * time.Millisecond
)

// Tick is the retransmission clock. Call it once after each OnEpoch and then every
// replication.handoff_retry_ms. It resends HANDOFF_REQ and HANDOFF_STOPPED for every
// pair still in sending (one message per peer and database), lets the lease bound prove
// an unreachable old owner stopped, and drives each awaiting pair whose old owner is
// proved stopped through its target, its catch-up and READY. It returns every failure; a
// failed step is simply tried again on the next Tick.
func (h *Handoff) Tick(ctx context.Context) error {
	var out handoffOut
	now := h.cfg.Clock.BootNow()
	var awaiting []pairKey
	h.mu.Lock()
	keys := make([]pairKey, 0, len(h.pairs))
	for key := range h.pairs {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].db != keys[j].db {
			return keys[i].db < keys[j].db
		}
		return keys[i].slot < keys[j].slot
	})
	for _, key := range keys {
		hp := h.pairs[key]
		act := h.apply(key, hp, machine.RetryTick{At: now})
		h.queue(key, hp, act, &out)
		if hp.barrier.Hold == machine.HoldAwaiting && hp.barrier.StopProved {
			awaiting = append(awaiting, key)
		}
		h.finish(key, hp)
	}
	awaiting = h.rotate(awaiting)
	h.mu.Unlock()
	var errs []error
	budget, cancel := context.WithTimeout(ctx, handoffAdvanceBudget)
	defer cancel()
	advanced := 0
	for _, key := range awaiting {
		if advanced == handoffAdvancePerTick || budget.Err() != nil {
			break
		}
		err := h.advance(budget, key, &out)
		advanced++
		h.mu.Lock()
		h.advanceAfter, h.advanceStarted = key, true
		h.mu.Unlock()
		if err != nil && (budget.Err() == nil || ctx.Err() != nil) {
			errs = append(errs, err)
		}
	}
	errs = append(errs, h.flush(ctx, &out))
	return errors.Join(errs...)
}

// rotate starts the awaiting pairs after the last one the previous Tick advanced, so a pair
// that cannot open (a silent copy) does not keep the pairs behind it from being asked.
// awaiting is sorted. Callers hold h.mu.
func (h *Handoff) rotate(awaiting []pairKey) []pairKey {
	if !h.advanceStarted || len(awaiting) == 0 {
		return awaiting
	}
	start := sort.Search(len(awaiting), func(i int) bool {
		a, b := awaiting[i], h.advanceAfter
		return a.db > b.db || (a.db == b.db && a.slot > b.slot)
	})
	return append(slices.Clone(awaiting[start:]), awaiting[:start]...)
}

// MaybeSendReady takes one awaiting pair as far toward HANDOFF_READY as its facts allow:
// the old owner must be proved stopped, then the old copy set is asked for the target,
// then the pair catches up, and READY is sent only when the pair is current and its
// applied position (fsynced in its own log, then applied) has reached the target. It is
// a no-op for a pair that is not awaiting or whose old owner is not proved stopped.
func (h *Handoff) MaybeSendReady(ctx context.Context, slot uint16, db uint64) error {
	var out handoffOut
	err := h.advance(ctx, pairKey{slot, db}, &out)
	return errors.Join(err, h.flush(ctx, &out))
}

// advance is the awaiting side's step for one pair. The I/O (position queries, catch-up)
// runs outside h.mu; a result that arrives after a further epoch change is dropped.
func (h *Handoff) advance(ctx context.Context, key pairKey, out *handoffOut) error {
	h.mu.Lock()
	hp := h.pairs[key]
	if hp == nil || hp.barrier.Hold != machine.HoldAwaiting || !hp.barrier.StopProved || !hp.copiesKnown {
		h.mu.Unlock()
		return nil
	}
	gen, needTarget, needCatchUp, from := hp.gen, !hp.targetKnown, !hp.caughtUp, hp.fromEpoch
	h.mu.Unlock()

	if needTarget {
		if _, err := h.Target(ctx, key.slot, key.db, from); err != nil {
			return err
		}
	}
	if needCatchUp {
		applied := h.cfg.Pairs.Snapshot(key.slot, key.db).AppliedPos
		if err := h.cfg.CatchUp.CatchUp(ctx, key.slot, key.db, applied, 0); err != nil {
			return fmt.Errorf("catching up slot %d of database %d before HANDOFF_READY: %w", key.slot, key.db, err)
		}
	}
	current := h.cfg.CatchUp.IsCurrent(key.slot, key.db)

	h.mu.Lock()
	defer h.mu.Unlock()
	hp = h.pairs[key]
	if hp == nil || hp.gen != gen || hp.barrier.Hold != machine.HoldAwaiting || !hp.targetKnown {
		return nil
	}
	// The applied position is the pair's own record: it moves only after the record is
	// fsynced in this node's log and applied, so it, and not the last chunk received, is
	// what READY may claim.
	applied := h.cfg.Pairs.Snapshot(key.slot, key.db).AppliedPos
	act := h.apply(key, hp, machine.CatchUpDone{At: h.cfg.Clock.BootNow(), AppliedPos: applied, Target: hp.target, Current: current})
	h.queue(key, hp, act, out)
	// Not ready yet: the next step catches up again rather than trusting this one.
	hp.caughtUp = hp.barrier.Hold != machine.HoldAwaiting
	h.finish(key, hp)
	return nil
}

// Target asks the pair's old copy set for its positions and returns the highest, the
// position this node must apply through before its first (conv section 10 rule 2). It
// needs answers from at least RF - majority + 1 of the copies at fromEpoch that are not
// committed down, counting only copies that report themselves ready; fewer is an error
// and a hold while a quorum could still form. When fewer such copies exist than the quorum
// needs and every one of them is accounted for (it answered, or said it is still seeding),
// the target is the highest position that answered and the pair is marked degraded; an
// unanswered live copy keeps the hold.
//
// Every holder outside the copy-set rule (holdersOf: a member that left the copy set and has
// not retired, BUG-0834) is asked too, and its position, ready or retired, raises the target;
// it never counts toward the quorum. A live holder that does not answer, or answers with an
// error (a pair deferred behind its log replay answers so), keeps the hold: its silence is no
// evidence that it holds less. A holder committed down keeps the hold until it is declared
// dead, the committed dead_ms (Settings.DeadMs, the failure detector's value) after it went
// down; a dead holder's data is out of reach, so the pair then
// opens on what the others hold and is marked degraded. The result is kept for CachedTarget.
func (h *Handoff) Target(ctx context.Context, slot uint16, db uint64, fromEpoch uint64) (cluster.Position, error) {
	key := pairKey{slot, db}
	h.mu.Lock()
	hp := h.pairs[key]
	if hp == nil || hp.barrier.Hold != machine.HoldAwaiting || !hp.copiesKnown {
		h.mu.Unlock()
		return cluster.Position{}, fmt.Errorf("slot %d of database %d is not waiting on a handoff, so it has no catch-up target to ask for", slot, db)
	}
	if hp.fromEpoch != fromEpoch {
		current := hp.fromEpoch
		h.mu.Unlock()
		return cluster.Position{}, fmt.Errorf("the target of slot %d of database %d was asked for epoch %d and the open barrier moves it from epoch %d", slot, db, fromEpoch, current)
	}
	copies, rf, gen := slices.Clone(hp.oldCopies), hp.rf, hp.gen
	holders := h.holdersOf(slot, db, slices.Concat(hp.oldCopies, hp.oldDown))
	deadAfter := h.deadAfter
	h.mu.Unlock()

	need := rf - (rf/2 + 1) + 1
	var best cluster.Position
	answers, notReady := 0, 0
	var missing []error
	for _, name := range copies {
		reply, counts, err := h.positionOf(ctx, name, slot, db)
		if err != nil {
			if errors.Is(err, errCopyNotReady) {
				notReady++
			}
			missing = append(missing, err)
			continue
		}
		if best.Less(reply) {
			best = reply
		}
		if !counts {
			// A copy that answered retired has left the copy set at the new epoch (the old owner
			// at RF 1, or a copy the move dropped) or is back in it with its old record: either
			// way its position is evidence of what is held, but it is not a ready copy for the
			// quorum.
			notReady++
			continue
		}
		answers++
	}
	// A shortfall is transient while an answer could still arrive: at least need copies
	// exist that are not committed down. With fewer, a majority of the old copy set is
	// gone, no message can close the gap and a down member never returns (a member that returns is committed active again only after it asks (BUG-0754), and is excluded here while it is down), so the
	// highest position the survivors report is the best any survivor holds. Waiting
	// would not recover a write only the lost copies acknowledged (U57-15 puts that
	// inside the contract); it would turn the loss into an outage. That holds only when
	// every copy still alive is accounted for: one that did not answer may hold a write
	// the lost copies acknowledged with it, so it keeps the hold (a copy that answered
	// "seeding" holds no acknowledged write and is excluded).
	degraded := len(copies) < need && answers+notReady == len(copies)
	if answers < need && !degraded {
		return cluster.Position{}, shortfallError(slot, db, shortfall{need: need, rf: rf, answers: answers, live: len(copies), fromEpoch: fromEpoch, missing: missing})
	}
	if len(holders.down) > 0 {
		err := downHolderError(slot, db, holders.down, deadAfter)
		h.mu.Lock()
		h.noteHolder(db, holderWaiting, err.Error())
		h.mu.Unlock()
		return cluster.Position{}, err
	}
	held, err := h.holdersBest(ctx, slot, db, holders.live)
	if err != nil {
		return cluster.Position{}, err
	}
	if best.Less(held) {
		best = held
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if current := h.pairs[key]; current == hp && hp.gen == gen && hp.barrier.Hold == machine.HoldAwaiting {
		hp.target, hp.targetKnown = best, true
		h.publishTarget(key, publishedTarget{position: best, degraded: degraded || len(holders.dead) > 0}, true)
		if degraded {
			h.noteDegraded(slot, db, shortfall{need: need, rf: rf, answers: answers, live: len(copies), fromEpoch: fromEpoch})
		}
		if len(holders.dead) > 0 {
			h.noteHolder(db, holderDead, fmt.Sprintf(
				"slot %d of database %d opens without %s, which left the pair's copy set holding a copy of it that was not yet deleted, and was declared dead: acknowledged writes that only it held, if any, are gone. Run 'nodes' to see the members",
				slot, db, strings.Join(holders.dead, ", ")))
		}
	}
	return best, nil
}

// holdersBest asks each live holder outside the copy set for its position and returns the
// highest that is evidence (ready or retired). A holder that is seeding again or deleting
// answers no evidence and holds nothing only it has. Any other failure, no answer or an error
// answer, is an error: the promotion waits for it.
func (h *Handoff) holdersBest(ctx context.Context, slot uint16, db uint64, holders []string) (cluster.Position, error) {
	var best cluster.Position
	var missing []error
	for _, name := range holders {
		reply, _, err := h.positionOf(ctx, name, slot, db)
		switch {
		case err == nil:
			if best.Less(reply) {
				best = reply
			}
		case !errors.Is(err, errCopyNotReady):
			missing = append(missing, err)
		}
	}
	if len(missing) > 0 {
		return cluster.Position{}, fmt.Errorf("slot %d of database %d waits for a member that left its copy set and has not deleted its copy, which may hold acknowledged writes no copy has: nothing is ordered on the pair until it gives its position. Run 'nodes' to see whether it is up: %w",
			slot, db, errors.Join(missing...))
	}
	return best, nil
}

// downHolderError is the hold on a pair whose holder outside the copy set is committed down.
func downHolderError(slot uint16, db uint64, down []string, deadAfter time.Duration) error {
	names := strings.Join(down, ", ")
	return fmt.Errorf("slot %d of database %d waits for %s: it left the pair's copy set without deleting its copy, which may hold acknowledged writes that no live member has, and it is down. Nothing is ordered on the pair until it answers. Start %s again; if it cannot come back, it is declared dead %s after it went down (the cluster's committed dead_ms setting, which no command changes yet: BUG-0838) and the pair then opens without what only it held. Run 'nodes' to see its state",
		slot, db, names, names, deadAfter)
}

// holderNote names one kind of holder log line for one database.
type holderNote struct {
	db   uint64
	kind uint8
}

const (
	holderWaiting uint8 = iota + 1
	holderDead
)

// noteHolder logs a hold on, or an opening without, a holder outside the copy set once per
// database, kind and epoch, as noteDegraded does: a failover moves thousands of slots at once.
// Callers hold h.mu.
func (h *Handoff) noteHolder(db uint64, kind uint8, text string) {
	note := holderNote{db: db, kind: kind}
	if noted, seen := h.holderNoted[note]; seen && noted == h.epoch {
		return
	}
	if h.holderNoted == nil {
		h.holderNoted = map[holderNote]uint64{}
	}
	h.holderNoted[note] = h.epoch
	slog.Warn(text)
}

// noteDegraded logs a degraded target once per database and epoch, naming the first slot:
// a failover moves thousands of slots at once and each would repeat the same line. Callers
// hold h.mu.
func (h *Handoff) noteDegraded(slot uint16, db uint64, s shortfall) {
	if noted, seen := h.degradedNoted[db]; seen && noted == h.epoch {
		return
	}
	if h.degradedNoted == nil {
		h.degradedNoted = map[uint64]uint64{}
	}
	h.degradedNoted[db] = h.epoch
	slog.Warn(fmt.Sprintf("slot %d of database %d opens on the %d of its %d copies that answered: a majority of the copies it had at epoch %d are down, so the %d answers needed cannot exist. Writes acknowledged only by the lost copies are gone; run 'nodes' to see which copies are down",
		slot, db, s.answers, s.rf, s.fromEpoch, s.need))
}

// Degraded reports whether the pair's catch-up target was taken from fewer copies than the
// quorum needs (conv section 10 rule 2, last sentence). It stays readable after READY until
// the next epoch frees the pair, and, like CachedTarget, it takes only targetMu, so the
// write path may ask it under a pair lock.
func (h *Handoff) Degraded(slot uint16, db uint64) bool {
	h.targetMu.Lock()
	defer h.targetMu.Unlock()
	return h.targets[pairKey{slot, db}].degraded
}

// errCopyNotReady marks a position answer from a copy that replied but is not ready (still
// seeding): it holds no acknowledged write, so it is accounted for without counting.
var errCopyNotReady = errors.New("the copy is not ready")

// deferred reports whether the boot replay of this node's own pair has not finished.
func (h *Handoff) deferred(slot uint16, db uint64) bool {
	return h.cfg.Deferred != nil && h.cfg.Deferred(slot, db)
}

// positionOf returns one member's own applied position for the pair and whether it is a
// ready copy, or an error when it did not answer, answered for another pair, or holds no
// evidence: a seeding record holds no acknowledged write, and a retiring one has started
// deleting. A retired record (out of the copy set, nothing deleted yet) is evidence of what
// is held, not a ready copy (BUG-0834). A member whose boot replay has not finished answers
// an error too, here for this node and in Node.servePosition for a peer.
func (h *Handoff) positionOf(ctx context.Context, name string, slot uint16, db uint64) (cluster.Position, bool, error) {
	if name == h.cfg.Self {
		if h.deferred(slot, db) {
			// Before the ready check, and not errCopyNotReady: Target accounts for a copy that
			// is not ready and leaves it out, and this copy holds acknowledged records in its
			// log. As a plain error it is a copy that has not answered, so the hold stays.
			return cluster.Position{}, false, pairReplayingError(h.cfg.Self, slot, db)
		}
		if h.cfg.Ready != nil && !h.cfg.Ready(slot, db) {
			return cluster.Position{}, false, fmt.Errorf("this node's own copy of slot %d of database %d is not ready: %w", slot, db, errCopyNotReady)
		}
		return h.cfg.Pairs.Snapshot(slot, db).AppliedPos, true, nil
	}
	reply, err := h.cfg.Link.QueryPosition(ctx, name, PositionQuery{Slot: slot, DB: db})
	if err != nil {
		return cluster.Position{}, false, fmt.Errorf("asking %s for its position of slot %d of database %d: %w", name, slot, db, err)
	}
	if reply.Slot != slot || reply.DB != db {
		return cluster.Position{}, false, fmt.Errorf("%s answered for slot %d of database %d when asked about slot %d of database %d", name, reply.Slot, reply.DB, slot, db)
	}
	switch reply.State {
	case PositionStateReady:
		return reply.AppliedPos, true, nil
	case PositionStateRetired:
		return reply.AppliedPos, false, nil
	}
	return cluster.Position{}, false, fmt.Errorf("%s holds slot %d of database %d in state %d, neither ready nor retired, so its position is not evidence: %w", name, slot, db, reply.State, errCopyNotReady)
}

// publishedTarget is what the write path may read under a pair lock: the target and whether
// it was taken from fewer copies than the quorum needs.
type publishedTarget struct {
	position cluster.Position
	degraded bool
}

// publishTarget makes the pair's target visible to CachedTarget, or withdraws it. Callers
// hold h.mu.
func (h *Handoff) publishTarget(key pairKey, target publishedTarget, known bool) {
	h.targetMu.Lock()
	defer h.targetMu.Unlock()
	if known {
		h.targets[key] = target
		return
	}
	delete(h.targets, key)
}

// CachedTarget is the last target Target computed for the pair, false when there is none
// (never asked, asked and short of answers, or reset by a further epoch). The write path's
// Topology.Target reads it.
func (h *Handoff) CachedTarget(slot uint16, db uint64) (cluster.Position, bool) {
	h.targetMu.Lock()
	defer h.targetMu.Unlock()
	target, known := h.targets[pairKey{slot, db}]
	return target.position, known
}

// checkEnvelope refuses a message whose envelope names another kind, and a database or
// state this node cannot place yet. Callers hold h.mu.
func (h *Handoff) checkEnvelope(env transport.Envelope, kind byte, name string) error {
	if env.Kind != kind {
		return fmt.Errorf("a %s arrived in an envelope of kind 0x%02x, want 0x%02x. The message was dropped", name, env.Kind, kind)
	}
	if h.table == nil {
		return fmt.Errorf("a %s for database %d arrived before this node applied any cluster state. The message was dropped; the sender retransmits it", name, env.DBID)
	}
	if _, known := h.rf[env.DBID]; !known {
		return fmt.Errorf("a %s names database %d, which this node's committed state does not hold. The message was dropped; the sender retransmits it", name, env.DBID)
	}
	return nil
}

// OnHandoffReq handles HANDOFF_REQ from node from. It is exempt from the frame-epoch
// rule (conv section 7): it is judged by its own two epochs and the ranks, so a request
// ahead of this node's commit opens the awaiting hold and one for an epoch already passed
// is dropped. A repeated REQ after READY is answered with READY again. The sender is the
// authenticated peer, not a field of the message, so it is a parameter.
func (h *Handoff) OnHandoffReq(from string, env transport.Envelope, msg HandoffReq) error {
	var out handoffOut
	h.mu.Lock()
	if err := h.checkEnvelope(env, kindHandoffReq, "HANDOFF_REQ"); err != nil {
		h.mu.Unlock()
		return err
	}
	ahead := msg.ToEpoch > h.epoch
	for _, slot := range SlotsOfBitmap(msg.Bitmap) {
		key := pairKey{slot, env.DBID}
		hp := h.pairs[key]
		if hp == nil {
			if !ahead {
				continue
			}
			hp = &handoffPair{rf: h.rf[env.DBID], fromEpoch: msg.FromEpoch}
			hp.barrier.Epoch = h.epoch
			h.pairs[key] = hp
		}
		event := machine.HandoffReqArrived{
			From: from, To: h.cfg.Self, FromEpoch: msg.FromEpoch, ToEpoch: msg.ToEpoch, Slots: []uint16{slot},
			Valid: h.validReq(from, key, msg),
		}
		act := h.apply(key, hp, event)
		h.queue(key, hp, act, &out)
		h.finish(key, hp)
	}
	h.mu.Unlock()
	return h.flush(context.Background(), &out)
}

// validReq is the rank check of a HANDOFF_REQ the machine cannot make: this node is rank 1
// at ToEpoch. When ToEpoch is ahead of the committed epoch the ranks cannot be read yet,
// and the commit that arrives re-checks the peer (machine E5). Callers hold h.mu.
func (h *Handoff) validReq(from string, key pairKey, msg HandoffReq) bool {
	switch {
	case from == h.cfg.Self || msg.FromEpoch >= msg.ToEpoch:
		return false
	case msg.ToEpoch > h.epoch:
		return true
	case msg.ToEpoch == h.epoch:
		return h.ownerID(key.slot, h.rf[key.db]) == h.cfg.Self
	}
	return false
}

// OnHandoffStopped handles HANDOFF_STOPPED from node from. It proves the old owner
// stopped only for a pair awaiting that owner at the message's epoch (the machine checks
// the sender is the barrier's peer); it never orders and never creates a pair. READY
// follows on the next Tick or MaybeSendReady, after the pair has caught up to a target
// taken once the old owner had stopped.
func (h *Handoff) OnHandoffStopped(from string, env transport.Envelope, msg HandoffStopped) error {
	var out handoffOut
	h.mu.Lock()
	if err := h.checkEnvelope(env, kindHandoffStopped, "HANDOFF_STOPPED"); err != nil {
		h.mu.Unlock()
		return err
	}
	for _, slot := range SlotsOfBitmap(msg.Bitmap) {
		key := pairKey{slot, env.DBID}
		hp := h.pairs[key]
		if hp == nil {
			continue
		}
		act := h.apply(key, hp, machine.HandoffStoppedArrived{From: from, To: h.cfg.Self, ToEpoch: msg.ToEpoch, Slots: []uint16{slot}, Valid: true})
		h.queue(key, hp, act, &out)
		h.finish(key, hp)
	}
	h.mu.Unlock()
	return h.flush(context.Background(), &out)
}

// OnHandoffReady handles HANDOFF_READY from node from on the old owner. A pair whose
// drainTo is above the message's applied position stays in sending (build SPEC-GAP G2);
// every other pair ends its hold, latches the drain at the applied position and wakes its
// parked callers, who go to the new owner.
func (h *Handoff) OnHandoffReady(from string, env transport.Envelope, msg HandoffReady) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.checkEnvelope(env, kindHandoffReady, "HANDOFF_READY"); err != nil {
		return err
	}
	for _, slot := range SlotsOfBitmap(msg.Bitmap) {
		key := pairKey{slot, env.DBID}
		hp := h.pairs[key]
		if hp == nil {
			continue
		}
		act := h.apply(key, hp, machine.HandoffReadyArrived{From: from, To: h.cfg.Self, ToEpoch: msg.ToEpoch, Slots: []uint16{slot}, AppliedPos: msg.AppliedPos})
		if act.ClearHold && hp.barrier.ReadyRecv {
			h.cfg.Drain.NoteDrained(slot, env.DBID, msg.AppliedPos)
		}
		h.finish(key, hp)
	}
	return nil
}

// Hold parks one operation a hold refuses, until the hold ends, and says where it goes.
// It is the one place a held write or read waits, and the only place that acts on it:
//
//   - A write that the write path answered Held is parked here with its request id and body.
//   - A read at either side of a barrier is parked here too. It is never answered by the
//     Handoff: the old owner may be superseded, the new owner may not be current yet.
//   - When the hold ends the parked caller returns a HoldRelease and acts on it exactly
//     once: Local means re-enter the ordinary path on this node, otherwise forward to Owner
//     with the same request id. The machine's list no longer holds the operation, and the
//     Handoff forwards nothing itself, so a held read is never answered twice. A reader
//     that also re-admits itself after waiting must not use Hold's list as well.
//   - The caller's budget (HeldRequest.Deadline, and ctx) bounds the wait: past it the
//     operation leaves the list and the caller is refused with a retryable code naming
//     the owner, never parked longer.
//
// A hold that already ended returns at once with the current owner.
func (h *Handoff) Hold(ctx context.Context, req HeldRequest) (HoldRelease, error) {
	key := pairKey{req.Slot, req.DB}
	if req.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Deadline)
		defer cancel()
	}
	h.mu.Lock()
	hp := h.pairs[key]
	if hp == nil || hp.barrier.Hold == machine.HoldNone {
		defer h.mu.Unlock()
		return h.settled(key)
	}
	var event machine.BarrierEvent = machine.WriteArrived{RequestID: req.RequestID, Body: req.Body}
	if req.Read {
		event = machine.ReadArrived{RequestID: req.RequestID, PeerReachable: req.PeerReachable, CallerWillWait: req.Deadline == 0 || req.Deadline > h.cfg.Retry}
	}
	act := h.apply(key, hp, event)
	if act.Refuse != 0 {
		peer := hp.barrier.Peer
		h.mu.Unlock()
		return HoldRelease{}, &cluster.Error{Code: act.Refuse, Message: fmt.Sprintf(
			"slot %d of database %d is changing owner and %s, the node it is changing from, does not answer, so this read cannot wait for it. Nothing was returned. Retry in a few seconds, or run 'nodes' to see whether %s is up",
			req.Slot, req.DB, peer, peer)}
	}
	waiter := &holdWaiter{done: make(chan struct{})}
	hp.waiters = append(hp.waiters, waiter)
	h.mu.Unlock()

	select {
	case <-waiter.done:
		return h.released(key, waiter.rel)
	case <-ctx.Done():
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	select {
	case <-waiter.done:
		return h.released(key, waiter.rel)
	default:
	}
	h.unpark(hp, waiter)
	return HoldRelease{}, h.overdue(key, hp, req)
}

// released is what a parked caller is handed when its hold ends. A hold that ended
// with no owner is refused like a settled pair with none: there is nobody to forward to.
func (h *Handoff) released(key pairKey, rel HoldRelease) (HoldRelease, error) {
	if rel.Owner == "" {
		return HoldRelease{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"every copy of slot %d of database %d is down, so no node owns it. Nothing was done. Run 'nodes' to see which members are down", key.slot, key.db)}
	}
	return rel, nil
}

// settled is the answer for an operation whose hold is not open: the pair's owner now.
// Callers hold h.mu.
func (h *Handoff) settled(key pairKey) (HoldRelease, error) {
	rf, known := h.rf[key.db]
	if !known || h.table == nil {
		return HoldRelease{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"database %d is not in this node's committed state, so slot %d has no owner to send the operation to. Nothing was done. Retry in a few seconds, or run 'nodes' to see whether this node has joined", key.db, key.slot)}
	}
	owner := h.ownerID(key.slot, rf)
	if owner == "" {
		return HoldRelease{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"every copy of slot %d of database %d is down, so no node owns it. Nothing was done. Run 'nodes' to see which members are down", key.slot, key.db)}
	}
	return HoldRelease{Owner: owner, Local: owner == h.cfg.Self}, nil
}

// unpark removes a waiter and its operation from the machine's list; the two lists move
// together, so they share an index. Callers hold h.mu.
func (h *Handoff) unpark(hp *handoffPair, waiter *holdWaiter) {
	index := slices.Index(hp.waiters, waiter)
	if index < 0 {
		return
	}
	hp.waiters = slices.Delete(hp.waiters, index, index+1)
	hp.barrier.Held = slices.Delete(hp.barrier.Held, index, index+1)
}

// overdue is the refusal for an operation whose budget ran out while parked. Callers hold h.mu.
func (h *Handoff) overdue(key pairKey, hp *handoffPair, req HeldRequest) error {
	nothing := "Nothing was written."
	if req.Read {
		nothing = "Nothing was returned."
	}
	if owner := h.ownerID(key.slot, hp.rf); owner != "" && owner != h.cfg.Self {
		return &cluster.Error{Code: cluster.CodeNotOwner, Message: fmt.Sprintf(
			"slot %d of database %d moved to %s and the handoff had not finished when this request's deadline passed. %s Retry at %s, or at any node and let it forward",
			key.slot, key.db, owner, nothing, owner)}
	}
	if hp.barrier.Hold == machine.HoldAwaiting && (hp.barrier.Peer == "" || hp.restartWindow) {
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"slot %d of database %d is held: %s. That had not finished when this request's deadline passed. %s Retry in a few seconds",
			key.slot, key.db, restartHoldCause, nothing)}
	}
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"slot %d of database %d is waiting for the previous owner to stop and for this node to catch up, and that had not finished when this request's deadline passed. %s Retry in a few seconds; 'nodes' shows whether the previous owner is up",
		key.slot, key.db, nothing)}
}

// shortfall is what a Target that could not meet its quorum knew.
type shortfall struct {
	need, rf, answers, live int
	fromEpoch               uint64
	missing                 []error
}

// shortfallError names the copies that did not answer. It never wraps a nil.
func shortfallError(slot uint16, db uint64, s shortfall) error {
	return fmt.Errorf("slot %d of database %d needs ready answers from %d of the copies it had at epoch %d (replication factor %d) and has %d, so no write is ordered yet. Run 'nodes' to see which copy is not answering: %w",
		slot, db, s.need, s.fromEpoch, s.rf, s.answers, errors.Join(s.missing...))
}
