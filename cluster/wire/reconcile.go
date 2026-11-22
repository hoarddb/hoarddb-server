package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// reconcileBatchOps bounds how many record writes one batch holds, so a join
// that queues thousands of slots commits in a few batches, not one oversized one.
var reconcileBatchLimit = 256

// reconcileFenceKind is the kind byte of the 's' reclaim record (conv §12).
const reconcileFenceKind byte = 's'

// Store is the part of storage the reconciler needs. storage.KV satisfies it.
type Store interface {
	NewBatch() storage.KVBatch
	NewSnapshot() storage.KVSnapshot
}

// Clock reads wall time; the reconciler uses it only to mint seed ids.
type Clock interface {
	Now() time.Time
}

// StateSource returns the latest committed ClusterState. OnBoot reads it
// because the first Reconciled after a restart must come from the latest
// committed state (slot.go contract 1).
type StateSource interface {
	State() cluster.ClusterState
}

// stateSeqSource is the cheap read of the committed state_seq that a StateSource may
// offer. State copies the whole state, bitmaps included; the pass asks for the sequence
// once per pair it is about to retire, so it takes this when there is one.
type stateSeqSource interface {
	StateSeq() int64
}

// ActionSink receives every action the table asks for that needs more than the
// record write the reconciler already did: queue or resume a seed (U45), the
// gate and the range delete (U46), a catch-up, a bit report, a RETIRE proposal.
type ActionSink interface {
	Do(Action) error
}

// Action is one row of the table firing for one pair.
type Action struct {
	Slot  uint16
	DBID  uint64
	Epoch uint64
	machine.SlotAction
}

// Config wires a Reconciler. Every field is required.
type Config struct {
	NodeID string
	Store  Store
	Clock  Clock
	State  StateSource
	Sink   ActionSink
	// Pairs receives each ready record's applied position and ordinal at boot,
	// so a restarted pair continues where its data batch left it.
	Pairs *cluster.PairSet
	// Current says a completed catch-up at the committed epoch is on record for
	// the pair (SlotView.Current).
	Current func(slot uint16, dbID uint64) bool
	// Deferred says a pair's boot replay has not finished (BUG-0760): its log holds records its
	// record and applied position do not count. Nil means none is. The reconciler decides nothing
	// for such a pair.
	Deferred func(slot uint16, dbID uint64) bool
	// WindowBytes is the seed window offered when a resumed attempt registers
	// again on its cut.
	WindowBytes int64
	// Diverged says this node's cluster state is known to disagree with the cluster's. A
	// reconcile pass then decides nothing: the state it would judge every pair against is
	// frozen, and a pair of a database that state lacks would be retired (BUG-0781 section 4).
	// Nil means the state is trusted.
	Diverged func() bool
	// Floor is the node's log-floor tracker (store-vouched-cut-spec.md §3.6): a step to a pair
	// with no log cursor registers a hold in it before it appends. Nil registers none.
	Floor *logFloor
}

// reconcilePair is one held (slot, database): the lifecycle machine, the
// record as it lies on disk, and the resumed seed machine if there is one.
type reconcilePair struct {
	slot    machine.SlotRecord
	disk    SeedRecord
	present bool // the record key exists on disk
	seed    *machine.SeedRecord
	// seen is the last view the slot machine was given, to notice a target set
	// that changed without the epoch moving.
	seen    reconcileSeen
	hasSeen bool
	// gen changes with every install into r.pairs. A pass stages from a copy and
	// installs only if the live gen is still the one it copied.
	gen uint64
}

// reconcileSeen identifies a view for the same-epoch change check.
type reconcileSeen struct {
	epoch    uint64
	inTarget bool
	copies   string
}

type reconcileKey struct {
	slot uint16
	db   uint64
}

// Reconciler runs seeding §3's table against the record on disk, at boot and
// on every commit that changes what this node should hold. It runs every
// decision through machine.SlotRecord.Apply and persists the record the
// machine leaves. Memory is one reconcilePair per held (slot, database), never
// per bucket (Rule 11); membership changes cost O(slots x databases).
type Reconciler struct {
	cfg Config

	// Lock order is always: pass, then a pair lock (Config.Pairs), then mu. mu
	// guards the maps and the records; a pair lock serialises everything that
	// changes or deletes one pair's data and record, including the caller's
	// data batch (see ApplyAdvance).
	pass sync.Mutex // one OnBoot/OnStateSeq/OnJoiningCommit pass at a time

	mu     sync.Mutex
	pairs  map[reconcileKey]*reconcilePair
	loaded bool
	// busy holds the pairs whose staged change is in a batch that has not
	// committed. Their memory still equals the disk, and Feed, FeedSeed,
	// ApplyAdvance and ApplySeedChunk refuse them until the change lands or fails.
	busy    map[reconcileKey]struct{}
	genSeq  uint64
	drops   []reconcileKey // pair-set entries to forget once no lock is held
	seedSeq uint64
	// keepSeeding makes a forced reset skip a record that is already seeding, so a joiner that
	// restarts mid-seed resumes from its cursor instead of starting over (SPEC-GAP T3-2). A
	// ready record on a joining node is still reset. Only ResumeJoining sets it, for one pass.
	keepSeeding bool
}

// NewReconciler returns a Reconciler that has read nothing yet; call OnBoot.
func NewReconciler(cfg Config) *Reconciler {
	return &Reconciler{cfg: cfg, pairs: make(map[reconcileKey]*reconcilePair), busy: make(map[reconcileKey]struct{})}
}

// OnBoot loads every record from disk, restores the ready ones into the pair
// set, and reconciles against the latest committed state. When that state
// says this node is joining, it resets its records exactly as OnJoiningCommit
// does, so a crash between the joining commit and that call cannot leave a
// ready record on a joining node; a record earned during this join is seeded
// again, which costs time and not data. A node does not call it: Node.open
// loads the records through LoadRecords and the driver's first pass is
// OnStateSeq (ResumeJoining or OnJoiningCommit on a joiner), because load
// refuses to run twice; OnBoot is the test rigs' one-call boot.
func (r *Reconciler) OnBoot(ctx context.Context) error {
	return r.pass1(func() ([]Action, error) {
		if err := r.load(); err != nil {
			return nil, err
		}
		s := r.cfg.State.State()
		member, _ := reconcileMember(s, r.cfg.NodeID)
		return r.reconcile(ctx, s, member.State == cluster.MemberJoining)
	})
}

// pass1 runs one reconcile pass under the pass lock and hands the actions to
// the sink after releasing it, so a sink that starts another pass does not
// deadlock. Actions of two passes may therefore reach the sink in either order;
// each pass recomputes from installed state, so a late one is harmless.
func (r *Reconciler) pass1(run func() ([]Action, error)) error {
	r.pass.Lock()
	actions, err := run()
	r.pass.Unlock()
	if err != nil {
		return err
	}
	return r.dispatch(actions)
}

// OnStateSeq reconciles after a commit changed what this node should hold
// (member state, a database's RF, a database added or removed), and is the
// first pass of a member that is not joining. It refuses to run before the
// records are loaded. ctx is the driver's: the pass checks it between
// databases, so a closing node does not finish an 8,192-slot sweep first.
func (r *Reconciler) OnStateSeq(ctx context.Context, s cluster.ClusterState) error {
	return r.pass1(func() ([]Action, error) {
		if err := r.needLoaded("OnStateSeq"); err != nil {
			return nil, err
		}
		return r.reconcile(ctx, s, false)
	})
}

// OnJoiningCommit runs when this node's own joining commit lands (a join, a
// return after stage 2, a replace). It rewrites every record the node holds for
// its placing set to seeding from phase reset, because a ready record cannot
// arrive with data older than the log (conv §14), and then reconciles.
func (r *Reconciler) OnJoiningCommit(s cluster.ClusterState) error {
	return r.pass1(func() ([]Action, error) {
		if err := r.needLoaded("OnJoiningCommit"); err != nil {
			return nil, err
		}
		if member, ok := reconcileMember(s, r.cfg.NodeID); !ok || member.State != cluster.MemberJoining {
			return nil, fmt.Errorf("joining commit: node %q is not joining in state_seq %d. Run 'nodes' to see its state", r.cfg.NodeID, s.StateSeq)
		}
		return r.reconcile(context.Background(), s, true)
	})
}

func (r *Reconciler) needLoaded(what string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.loaded {
		return fmt.Errorf("%s: the seed records are not loaded. Call LoadRecords (or OnBoot in a test) first; before it every pair looks absent and a ready record would be rewritten to seeding", what)
	}
	return nil
}

// Feed runs one lifecycle event (SeedFinished, RetireStarted, LogBelowHorizon,
// RetireCommitted, PositionReplied, ...) on a pair, persists the record the
// machine leaves, and returns the action. It does not hand the action to the
// sink: the caller acts on it, because RetireStarted must be answered before
// the first delete batch is written. Feed takes the pair lock itself, so the
// caller must not hold it. The new state is staged on a copy and installed only
// after the commit succeeds; a failed commit leaves the pair as it was.
func (r *Reconciler) Feed(slot uint16, dbID uint64, ev machine.SlotEvent) (machine.SlotAction, error) {
	if err := r.needLoaded("Feed"); err != nil {
		return machine.SlotAction{}, err
	}
	key := reconcileKey{slot, dbID}
	var act machine.SlotAction
	var err error
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err = r.checkMutable(key); err != nil {
			return
		}
		sp := r.stageFrom(key, shared)
		w := r.newWriter()
		w.inline = shared
		defer w.abort()
		staged := sp.p.slot.Apply(ev)
		if err = r.settle(w, key, sp, staged); err != nil {
			err = fmt.Errorf("slot %d db %d: feed %T: %w", slot, dbID, ev, err)
			return
		}
		w.stage(key, sp)
		if err = w.commit(); err != nil {
			err = fmt.Errorf("slot %d db %d: feed %T: %w", slot, dbID, ev, err)
			return
		}
		act = staged
	})
	r.flushDrops()
	return act, err
}

// ForgetOutcome says what removeRecordIf did.
type ForgetOutcome uint8

// The outcomes of removeRecordIf.
const (
	// ForgetRemoved: the record was in the wanted state and is gone, durably.
	ForgetRemoved ForgetOutcome = iota + 1
	// ForgetNoRecord: the pair had no record.
	ForgetNoRecord
	// ForgetHeld: the record is in another state and was left alone.
	ForgetHeld
	// ForgetOnlyCopy: the record is retired, no other member has confirmed a copy covering the
	// fence and the caller did not say anyway, so it was left alone (BUG-0795).
	ForgetOnlyCopy
)

// ForgetResult carries the outcome and the state the record was in (the zero
// state for ForgetNoRecord).
type ForgetResult struct {
	Outcome ForgetOutcome
	State   machine.SlotState
}

// removeRecordIf removes the pair's record, durably (one synced commit), when it
// is in the state want, and says what it found. It re-reads the record under the
// pair lock, so a record that changed since the caller looked is ForgetHeld and
// is never removed on a stale picture. It never touches the pair's range, its 's'
// fence keys or its pair-set position. Like Feed it takes the pair lock itself,
// so the caller must not hold it.
//
// With guardOnlyCopy set, a record in the wanted state that no other member of its copy
// set has confirmed a copy of is ForgetOnlyCopy and stays. The check is made here, under
// the pair lock and r.mu, on the same record the removal acts on: a check made by the
// caller before the lock could see the record retiring and the removal see it retired.
func (r *Reconciler) removeRecordIf(slot uint16, dbID uint64, want machine.SlotState, guardOnlyCopy bool) (ForgetResult, error) {
	if err := r.needLoaded("removeRecordIf"); err != nil {
		return ForgetResult{}, err
	}
	key := reconcileKey{slot, dbID}
	var out ForgetResult
	var err error
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err = r.checkMutable(key); err != nil {
			return
		}
		sp := r.stageFrom(key, shared)
		if !sp.p.present {
			out = ForgetResult{Outcome: ForgetNoRecord}
			return
		}
		if held := machine.SlotState(sp.p.disk.State); held != want {
			out = ForgetResult{Outcome: ForgetHeld, State: held}
			return
		}
		if guardOnlyCopy && sp.p.slot.AttestedCopies() == 0 {
			out = ForgetResult{Outcome: ForgetOnlyCopy, State: want}
			return
		}
		w := r.newWriter()
		w.inline = shared
		defer w.abort()
		sp.p.slot = machine.SlotRecord{}
		if err = r.persist(w, key, sp); err != nil {
			err = fmt.Errorf("slot %d db %d: remove the record: %w", slot, dbID, err)
			return
		}
		w.stage(key, sp)
		if err = w.commit(); err != nil {
			err = fmt.Errorf("slot %d db %d: remove the record: %w", slot, dbID, err)
			return
		}
		out = ForgetResult{Outcome: ForgetRemoved, State: want}
	})
	r.flushDrops()
	return out, err
}

// Forget is the engine side of SeedingView.Forget: it removes the record of a retired pair, durably, and keeps its range and its 's' fences. A record in any other state is ForgetHeld with that state, no record is ForgetNoRecord, and a retired record with no confirmed copy elsewhere is ForgetOnlyCopy unless anyway is set.
//
// The kept range is why a pair with no record is not taken for an empty one unseen (BUG-0819):
// while the node is in the pair's copy set, a step that would create the record is refused over
// a range that holds a key (admitAdvance), and the pass queues a seed for it instead of writing
// it ready empty (SlotView.RangeHoldsKeys), so the seed's range reset deletes what was kept.
func (r *Reconciler) Forget(slot uint16, dbID uint64, anyway bool) (ForgetResult, error) {
	return r.removeRecordIf(slot, dbID, machine.SlotRetired, !anyway)
}

// Seed returns a copy of the seed machine of an attempt that resumed from disk
// and has registered again on its cut (conv §12 contract 4), or of one a
// FeedSeed or ApplySeedChunk event has created. Drive it through those two.
func (r *Reconciler) Seed(slot uint16, dbID uint64) (machine.SeedRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pairs[reconcileKey{slot, dbID}]
	if p == nil || p.seed == nil {
		return machine.SeedRecord{}, false
	}
	return *p.seed, true
}

// ErrPairNotSeeding is returned by ApplySeedChunk for a pair that has no
// seeding attempt.
var ErrPairNotSeeding = errors.New("the pair is not seeding")

// ErrPairBusy is returned when a pair has a staged change whose commit has not
// finished; retry.
var ErrPairBusy = errors.New("the pair is being changed")

// ErrPairDeferred is a change refused to a pair whose boot replay has not finished (BUG-0760).
var ErrPairDeferred = errors.New("the pair is replaying its log tail after a restart")

// checkMutable is checkNotBusy for a change to the pair's record: a pair whose boot replay has
// not finished holds records the record and the applied position do not count, so nothing may
// retire it, fence it, reset it or remove its record until the replay is done. Callers hold mu.
func (r *Reconciler) checkMutable(key reconcileKey) error {
	if r.cfg.Deferred != nil && r.cfg.Deferred(key.slot, key.db) {
		return fmt.Errorf("slot %d db %d: %w. Nothing was changed; it is retried on its own once the replay finishes, and 'nodes' shows whether the nodes it waits for are up", key.slot, key.db, ErrPairDeferred)
	}
	return r.checkNotBusy(key)
}

func (r *Reconciler) checkNotBusy(key reconcileKey) error {
	if _, busy := r.busy[key]; busy {
		return fmt.Errorf("slot %d db %d: %w. Retry in a moment", key.slot, key.db, ErrPairBusy)
	}
	return nil
}

// FeedSeedRequest is one seed event that carries no data.
type FeedSeedRequest struct {
	Event machine.SeedEvent
	Cfg   machine.SeedConfig
	// DataDurable says every chunk, buffered write and derived entry of the attempt is
	// written, and durable once the commit of this event returns: a SyncDone that marks the copy
	// ready commits its record with a sync, which makes everything committed before it durable
	// with it (vouched cut I-4), and the record is installed only after that. A SyncDone without
	// it is refused, because that event is what marks a copy ready.
	DataDurable bool
	// Ordinal, when not zero, is the pair's dedupe ordinal and is written with the record:
	// the SyncDone that makes a copy ready carries it, so the pair never serves with an
	// ordinal its range has outgrown (T3 review F1).
	Ordinal uint64
	// Attempt, when set, is the seed id the event was produced for: the event is applied
	// only while the record still holds that attempt, and is refused with
	// ErrSeedAttemptReplaced otherwise. An event that answers a call made outside the pair
	// lock sets it, because the record can be replaced while the call is in flight
	// (BUG-0818).
	Attempt *[16]byte
}

// ErrSeedAttemptReplaced is a seed event refused because the pair's record no longer holds
// the attempt the event was produced for (or the pair no longer holds a seeding record): a pass, a restart of the seed or `seeding retry`
// wrote a new attempt while the event was on its way. Nothing was changed; the new attempt starts from its own
// range reset and its own registration.
var ErrSeedAttemptReplaced = errors.New("the pair's seeding attempt was replaced")

// FeedSeed runs one data-free event of a pair's seeding attempt (registration,
// a refusal, a buffered live write, the drain, SyncDone) on its seed machine
// and carries the result into the slot machine, which is how a seed reaches
// ready:
//
//   - the seed machine's phase, cut and applied position are copied into the
//     slot record;
//   - when the outcome is MarkReady, the slot machine receives SeedFinished at
//     the seed's applied position, the record becomes ready (committed with
//     sync, after the data it describes), and the pair set takes the position;
//   - a Restart outcome writes a fresh attempt from reset, and the old attempt's
//     later events find a new seed machine. None of them takes the new attempt
//     out of the reset phase except RegisterAccepted, which therefore carries
//     FeedSeedRequest.Attempt and is refused for another attempt (BUG-0818).
//
// A chunk or a buffered-write apply carries data and goes through
// ApplySeedChunk, so the record can never lead its data; FeedSeed refuses them.
// It takes the pair lock itself, so the caller must not hold it, and installs
// nothing unless the commit succeeds.
func (r *Reconciler) FeedSeed(slot uint16, dbID uint64, req FeedSeedRequest) (machine.SeedOutcome, error) {
	if err := r.needLoaded("FeedSeed"); err != nil {
		return machine.SeedOutcome{}, err
	}
	switch ev := req.Event.(type) {
	case machine.ChunkStored, machine.BufferApplyStep:
		return machine.SeedOutcome{}, fmt.Errorf("slot %d db %d: %T carries data. Apply it with ApplySeedChunk so the cursor moves in the data's own batch", slot, dbID, ev)
	case machine.RetireOrdered, machine.DeleteStarted, machine.RetireDeleted:
		return machine.SeedOutcome{}, fmt.Errorf("slot %d db %d: %T is a retire event. Send retire events through Feed", slot, dbID, ev)
	case machine.SyncDone:
		if ev.Err == nil && !req.DataDurable {
			return machine.SeedOutcome{}, fmt.Errorf("slot %d db %d: SyncDone would mark the copy ready, and the caller has not said its data is durable. Sync the store first, then set DataDurable", slot, dbID)
		}
	}
	key := reconcileKey{slot, dbID}
	var out machine.SeedOutcome
	var err error
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err = r.checkMutable(key); err != nil {
			return
		}
		out, err = r.feedSeedLocked(key, shared, req)
		if err != nil {
			err = fmt.Errorf("slot %d db %d: seed event %T: %w", slot, dbID, req.Event, err)
		}
	})
	r.flushDrops()
	return out, err
}

// feedSeedLocked runs with the pair lock and mu held.
func (r *Reconciler) feedSeedLocked(key reconcileKey, shared *cluster.Pair, req FeedSeedRequest) (machine.SeedOutcome, error) {
	orig := r.pairs[key]
	if orig == nil || orig.slot.State != machine.SlotSeeding {
		if req.Attempt != nil {
			// An event that names its attempt answers a call made outside the pair lock, and a
			// pair that holds no seeding record now is the replaced case, not the stale one
			// (BUG-0830): the caller must not go on as if its attempt were registered.
			return machine.SeedOutcome{}, fmt.Errorf("%w: the event is for attempt %x and the pair holds no seeding record", ErrSeedAttemptReplaced, *req.Attempt)
		}
		return machine.SeedOutcome{}, nil // no attempt is running: the event is stale
	}
	if req.Attempt != nil && orig.slot.SeedID != *req.Attempt {
		// The check and the apply are one critical section under the pair lock, so no attempt
		// can be written between them. Without it an accept sent for the old attempt moves
		// the new one out of the reset phase, and the range reset it owes never runs.
		return machine.SeedOutcome{}, fmt.Errorf("%w: the event is for attempt %x and the record holds attempt %x", ErrSeedAttemptReplaced, *req.Attempt, orig.slot.SeedID)
	}
	sp := r.stageFrom(key, shared)
	seed := reconcileSeedOf(&sp.p)
	out := seed.Apply(req.Event, req.Cfg)
	if req.Ordinal != 0 && out.MarkReady {
		sp.p.disk.Ordinal = req.Ordinal
	}
	r.carrySeed(sp, seed, out)
	w := r.newWriter()
	w.inline = shared
	w.unsynced = seedStepCommitsUnsynced(req.Event, out)
	defer w.abort()
	if err := r.persist(w, key, sp); err != nil {
		return machine.SeedOutcome{}, err
	}
	w.stage(key, sp)
	if err := w.commit(); err != nil {
		return machine.SeedOutcome{}, err
	}
	return out, nil
}

// seedStepCommitsUnsynced says whether the record change of one seed event may commit without a
// store sync: the accepted registration and the end of the drain, when neither restarts the
// attempt. Neither can mark the pair ready: only SyncDone does (machine seed.go, sync/ready),
// and it is not in the list. Nothing outside this node acts on either step: the pair is
// still seeding, so it serves nothing, counts as no copy and reports no bit, and the source's
// pin does not depend on it. A power cut that loses the step reopens the record at an earlier
// step of the same attempt (vouched cut I-1: the store opens at a prefix of its commits), from
// which the pull goes on: it registers again or resumes on its cut, as after a cut between two
// chunks. The ready record is committed with a sync and installed only after it, so it vouches
// every earlier step before anything acts on the pair (I-4). A range reset that deleted keys
// still syncs before its registration (Puller.resetRange). Every other event, a restart's fresh
// attempt, a redirect and the ready record among them, keeps its synced commit
// (TestASeedRestartAndARedirectAreCommittedWithASync, seeding-speed-note.md). Retire events do
// not come here: they go through Reconciler.Feed, whose commits are all synced.
func seedStepCommitsUnsynced(event machine.SeedEvent, out machine.SeedOutcome) bool {
	if out.Restart != 0 {
		return false
	}
	switch event.(type) {
	case machine.RegisterAccepted, machine.BufferDrained:
		return true
	}
	return false
}

// reconcileSeedOf returns the pair's seed machine as a value: a copy of the
// resumed or running one, or one built from the record.
func reconcileSeedOf(p *reconcilePair) machine.SeedRecord {
	if p.seed != nil {
		return *p.seed
	}
	return reconcileSeedFrom(p)
}

// carrySeed copies the result of a seed event into the staged pair.
func (r *Reconciler) carrySeed(sp *stagedPair, seed machine.SeedRecord, out machine.SeedOutcome) {
	p := &sp.p
	switch {
	case out.Restart != 0:
		r.freshAttempt(sp)
	case out.MarkReady:
		// The seed machine is ready; the slot machine learns it as SeedFinished,
		// which needs the phase it last saw: the drain is done.
		p.slot.Phase = machine.PhaseNone
		p.slot.Applied = seed.Applied
		p.slot.Apply(machine.SeedFinished{Pos: seed.Applied})
		// The cut has no record in this node's log, so a seeded pair has no tail cursor until it is
		// written (ShipTail sends nothing for it).
		sp.shared.AppliedPos, sp.shared.Ordinal, sp.shared.TailCursor, sp.setShared = p.slot.Applied, p.disk.Ordinal, 0, true
		p.seed = nil
	default:
		p.slot.Phase, p.slot.Applied, p.slot.Cut = seed.Phase, seed.Applied, seed.Cut
		p.slot.Cursor, p.slot.Source, p.slot.SeedID = seed.Cursor, seed.Source, seed.SeedID
		p.seed = &seed
	}
}

// SeedChunkRequest is one chunk (or one buffered write) of a seeding attempt.
type SeedChunkRequest struct {
	Cfg machine.SeedConfig
	// Stage writes the chunk's data into batch and returns the seed event that
	// describes it (ChunkStored or BufferApplyStep). It receives a copy of the
	// seed machine to read.
	Stage func(batch storage.KVBatch, seed machine.SeedRecord) (machine.SeedEvent, error)
	// Commit commits batch; sync says the machine wants the store synced now.
	// The reconciler has already added the record to the batch.
	Commit func(batch storage.KVBatch, sync bool) error
}

// ApplySeedChunk applies one data-carrying seed event as one step under the
// pair lock: it refuses a pair that is not seeding (ErrPairNotSeeding), calls
// Stage to put the data in a new batch, runs the event on a copy of the seed
// machine, and, if the machine wants the chunk applied, adds the new record
// (cursor, phase, applied position) to the SAME batch, calls Commit, and only
// then installs the machine and the record. The record therefore advances only
// in the data's own batch: a failed commit leaves record, data and machine as
// they were, and a crash after the commit leaves both. A chunk the machine
// does not want (a duplicate, a late one) is not committed, and a restart
// outcome writes a fresh attempt instead. Neither callback may call a
// Reconciler method that takes the pair lock. The caller must not hold it.
func (r *Reconciler) ApplySeedChunk(slot uint16, dbID uint64, req SeedChunkRequest) (machine.SeedOutcome, error) {
	if err := r.needLoaded("ApplySeedChunk"); err != nil {
		return machine.SeedOutcome{}, err
	}
	key := reconcileKey{slot, dbID}
	var out machine.SeedOutcome
	var err error
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		out, err = r.seedChunkLocked(key, shared, req)
	})
	r.flushDrops()
	if err != nil {
		return out, fmt.Errorf("slot %d db %d: seed chunk: %w", slot, dbID, err)
	}
	return out, nil
}

func (r *Reconciler) seedChunkLocked(key reconcileKey, shared *cluster.Pair, req SeedChunkRequest) (machine.SeedOutcome, error) {
	r.mu.Lock()
	if err := r.checkMutable(key); err != nil {
		r.mu.Unlock()
		return machine.SeedOutcome{}, err
	}
	orig := r.pairs[key]
	if orig == nil || orig.slot.State != machine.SlotSeeding {
		r.mu.Unlock()
		return machine.SeedOutcome{}, fmt.Errorf("%w: a chunk has no attempt to join", ErrPairNotSeeding)
	}
	sp := r.stageFrom(key, shared)
	seed := reconcileSeedOf(&sp.p)
	r.mu.Unlock()

	batch := r.cfg.Store.NewBatch()
	defer batch.Discard()
	ev, err := req.Stage(batch, seed)
	if err != nil {
		return machine.SeedOutcome{}, fmt.Errorf("stage the chunk: %w", err)
	}
	switch ev.(type) {
	case machine.ChunkStored, machine.BufferApplyStep:
	default:
		return machine.SeedOutcome{}, fmt.Errorf("%T carries no data. Send it through FeedSeed", ev)
	}
	out := seed.Apply(ev, req.Cfg)

	r.mu.Lock()
	defer r.mu.Unlock()
	if out.Restart != 0 {
		// The chunk is not applied; a fresh attempt makes the data untrusted
		// and is a record change on its own.
		batch.Discard()
		r.carrySeed(sp, seed, out)
		w := r.newWriter()
		w.inline = shared
		defer w.abort()
		if err := r.persist(w, key, sp); err != nil {
			return machine.SeedOutcome{}, err
		}
		w.stage(key, sp)
		return out, w.commit()
	}
	if !out.ApplyChunk && len(out.ApplyBuffered) == 0 {
		return out, nil // not wanted: nothing is committed, the data is discarded
	}
	r.carrySeed(sp, seed, out)
	if err := r.persistTo(sp, key, batch.Set, batch.Delete); err != nil {
		return machine.SeedOutcome{}, err
	}
	r.mu.Unlock()
	err = req.Commit(batch, out.Sync)
	r.mu.Lock()
	if err != nil {
		return machine.SeedOutcome{}, fmt.Errorf("commit the chunk: %w", err)
	}
	r.installLocked(key, sp, shared)
	return out, nil
}

// Record returns the record as it lies on disk, and false when there is none.
func (r *Reconciler) Record(slot uint16, dbID uint64) (SeedRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pairs[reconcileKey{slot, dbID}]
	if p == nil || !p.present {
		return SeedRecord{}, false
	}
	return p.disk, true
}

// Serves says a copy on this node may answer a read of the pair, by the lifecycle machine's
// own posture rather than the record on disk: false for a pair that is not held, not ready,
// asked to rebuild, not yet reconciled in this process or outside the target set.
func (r *Reconciler) Serves(slot uint16, dbID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pairs[reconcileKey{slot, dbID}]
	return p != nil && p.present && p.slot.Serves()
}

// RetiredIntact says the pair's record is retired with its range whole: it left the copy set
// from ready in this process and its delete has not started (machine.SlotRecord.RetiredIntact,
// false after a restart). The gate's RetireStarted flips it under the pair lock and r.mu
// before the first delete batch, so a caller that reads true holds a range no delete has
// touched yet.
func (r *Reconciler) RetiredIntact(slot uint16, dbID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pairs[reconcileKey{slot, dbID}]
	return p != nil && p.present && p.slot.RetiredIntact()
}

// ErrPairNotReady is returned by ApplyAdvance for a pair whose record is not
// ready: a write to it would put a ready record over a range that is being
// seeded, retired or deleted.
var ErrPairNotReady = errors.New("the pair is not ready")

// Advance is the position a write moved a ready pair to.
type Advance struct {
	Applied   cluster.Position
	WALCursor uint64
	Ordinal   uint64
}

// AdvanceRequest is one write's part of the record advance. Both functions run
// under the pair lock of the pair being advanced, so neither may call a
// Reconciler method that takes it (Feed, ApplyAdvance, OnBoot, OnStateSeq,
// OnJoiningCommit); Record and Seed are safe.
type AdvanceRequest struct {
	// Stage writes the write's data into batch and returns the position it
	// reaches. It receives the pair state, which it may read and not change.
	Stage func(batch storage.KVBatch, pair *cluster.Pair) (Advance, error)
	// Commit commits batch, with sync, and returns its error. The reconciler
	// has already added the record to the batch.
	Commit func(batch storage.KVBatch, sync bool) error
	// Replay says the step is the boot replay of a log record (AdvanceStep.Replay),
	// which never creates a record for a pair that has none.
	Replay bool
	// Pos is the replayed record's position (AdvanceStep.Pos).
	Pos cluster.Position
	// Admitted says an earlier critical section of the same write already passed the target-set test
	// and appended the record (AdvanceStep.Admitted): a record-less pair is not judged against the copy
	// set again.
	Admitted bool
	// Appended says whether the step appended a record (AdvanceStep.Appended).
	Appended func() bool
}

// ApplyAdvance moves a ready pair's record in the same batch as the data it
// describes, as one step under the pair lock: it refuses a pair that is not
// ready (ErrPairNotReady, nothing staged or written), calls Stage, adds the
// record computed from the pair's current state to the batch, calls Commit, and
// only if Commit succeeds installs the record and the pair-set position and
// ordinal. Every Reconciler entry that retires, deletes or restarts a pair
// takes the same lock, so none can run between the record being encoded as
// ready and the batch landing, and a crash leaves either both or neither.
// The caller must not hold the pair lock.
//
// A pair with no record is caller-dependent (seeding-completion plan §0.2,
// decisions §16 items 55 and 61): a live step creates a ready record at the
// step's position in the step's own batch when this node is in the pair's
// copy set at the committed state, and is refused ErrPairNotReady when it is
// not; a replay (req.Replay) stages nothing and answers errNothingStaged
// whatever the state says, because the state a boot replay sees is not the
// applied-committed one and a record created from it would give a forgotten
// pair its range back at every such boot. The first reconcile pass writes the
// interim record for an in-target pair the replay skipped. One replay creates
// (BUG-0809): recoverPair's, of the record at the pair's last appended position
// after that record's own apply failed (flagFirstApplyFailed, set at runtime only,
// so no boot replay sees it), judged against the copy set as a live step is.
func (r *Reconciler) ApplyAdvance(slot uint16, dbID uint64, req AdvanceRequest) error {
	key := reconcileKey{slot, dbID}
	var err error
	r.cfg.Pairs.With(slot, dbID, func(shared *cluster.Pair) {
		err = r.advanceLocked(key, shared, req)
	})
	if err != nil {
		return fmt.Errorf("slot %d db %d: advance: %w", slot, dbID, err)
	}
	return nil
}

// ApplyAdvanceLocked is ApplyAdvance for a caller that holds the pair's lock and passes the
// pair it was given (a copy's apply, BUG-0752). It takes no lock and does not wait for a
// busy pair: the reconcile pass that holds it busy commits under this very lock, so a
// wait here could not end.
func (r *Reconciler) ApplyAdvanceLocked(slot uint16, dbID uint64, shared *cluster.Pair, req AdvanceRequest) error {
	if err := r.advanceLocked(reconcileKey{slot, dbID}, shared, req); err != nil {
		return fmt.Errorf("slot %d db %d: advance: %w", slot, dbID, err)
	}
	return nil
}

// PrepareAdvanceLocked is the checks of advanceLocked before its stage, for the first section of an
// owner write that holds the pair's lock: ErrPairBusy for a pair a reconcile pass has staged,
// ErrPairNotReady for one that is not ready or, with no record, not in the copy set. It changes nothing.
// It reports whether the pair has no record yet.
func (r *Reconciler) PrepareAdvanceLocked(slot uint16, dbID uint64) (recordless bool, err error) {
	p, err := r.admitAdvance(reconcileKey{slot, dbID}, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("slot %d db %d: advance: %w", slot, dbID, err)
	}
	return p == nil || !p.present, nil
}

// Recordless implements RecordlessReporter: the pair has no record on this node.
func (r *Reconciler) Recordless(slot uint16, dbID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pairs[reconcileKey{slot, dbID}]
	return p == nil || !p.present
}

// admitAdvance is what a step must pass before it stages: the pair is not busy and is ready, or has
// no record and may get one. It returns the pair's reconcile state, nil when it has none. The pair
// lock is held. An admitted step (the second section of a pipelined write) skips the target-set test
// and the look at the range: its first section made both, its record is in the log, and no range
// gains a key under a pair that has no record between the two sections.
// owed, when set, is the position of the record a row 9 left a record-less pair owing
// (flagFirstApplyFailed, at the pair's last appended position) and the step replays it: the one
// replay that may create the record, judged against the copy set as a live step is.
func (r *Reconciler) admitAdvance(key reconcileKey, replay, admitted bool, owed *cluster.Position) (*reconcilePair, error) {
	r.mu.Lock()
	if err := r.checkNotBusy(key); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	p := r.pairs[key]
	r.mu.Unlock()
	if p == nil || !p.present {
		switch {
		case replay && owed == nil:
			return nil, errNothingStaged
		case owed != nil:
			if !r.inTargetSet(key) {
				return nil, fmt.Errorf("%w: %w: the write at %s is saved in this node's log, but this node holds no record of the pair and is no longer in its copy set, so it cannot apply the write here (BUG-0809). The pair stays fenced and orders no write until the copy set names this node again; run 'status' to see which pairs this node holds", ErrPairNotReady, errOwedNotHeld, *owed)
			}
		case !admitted && !r.inTargetSet(key):
			// A live step: the owner's write today; after the BUG-0752 fix the copy's apply
			// reaches here through the same advancer, with Replay false, and takes the same
			// two answers. The target-set test builds one ring table per created record.
			// An admitted step was judged by its first section, and the committed state has moved since
			// without the pair's lock: its record is durable, so it applies.
			return nil, fmt.Errorf("%w: this node holds no record of the pair and is not in its copy set, so it applies no write to it; 'status' shows which pairs this node holds", ErrPairNotReady)
		}
		if !admitted {
			// The step is about to create the pair's record, ready, on the rule that a pair
			// with no record was never written here. A range `seeding forget` kept breaks
			// that rule (BUG-0819), so the range is looked at once, here, per created record.
			held, err := r.rangeHoldsKeys(key)
			if err != nil {
				return nil, err
			}
			if held {
				return nil, fmt.Errorf("%w: this node holds no record of the pair, but its key range holds data, which is what forgetting a retired copy (seeding forget) leaves, so no write is applied over it. The node's next reconcile pass queues the pair for a seed, and the data is deleted once another copy accepts that pull or this node leaves the pair's copy set; run 'status seeding' to watch it", ErrPairNotReady)
			}
		}
	} else if p.disk.State != machine.SeedReady || p.slot.State != machine.SlotReady {
		return nil, fmt.Errorf("%w: a pair applies writes only after it is ready; 'status' shows which pairs are still seeding or retiring", ErrPairNotReady)
	}
	return p, nil
}

func (r *Reconciler) advanceLocked(key reconcileKey, shared *cluster.Pair, req AdvanceRequest) error {
	var owed *cluster.Position
	if req.Replay && shared.Flags&flagFirstApplyFailed != 0 && req.Pos == (cluster.Position{Epoch: shared.AppendedEpoch, Seq: shared.AppendedSeq}) {
		owed = &req.Pos
	}
	// The log-floor hold of a pair with no cursor, before the stage appends (§3.6). A replay
	// registers nothing: the boot hold covers it.
	floorKey := pairKey(key)
	added := false
	if !req.Replay && shared.TailCursor == 0 {
		added = r.cfg.Floor.hold(floorKey)
	}
	p, err := r.admitAdvance(key, req.Replay, req.Admitted, owed)
	if err != nil {
		if added {
			r.cfg.Floor.release(floorKey) // refused before the stage: nothing was appended
		}
		return err
	}

	batch := r.cfg.Store.NewBatch()
	defer batch.Discard()
	adv, err := req.Stage(batch, shared)
	if err != nil {
		r.cfg.Floor.releaseUnappended(floorKey, added, req.Appended)
		return fmt.Errorf("stage the write: %w", err)
	}
	// The pair lock is held, so p cannot have changed since the check above. A pair with
	// no record gets a ready one with a zero SeedID and no source: the signature of a pair
	// written here and never seeded (plan §0.3).
	var next SeedRecord
	if p != nil {
		r.mu.Lock()
		next = p.disk
		r.mu.Unlock()
	} else {
		next.State = machine.SeedReady
	}
	next.Applied, next.WALCursor, next.Ordinal = adv.Applied, adv.WALCursor, adv.Ordinal
	raw, err := EncodeSeedRecord(next)
	if err != nil {
		return err
	}
	if err := batch.Set(SeedRecordKey(key.slot, key.db), raw); err != nil {
		return fmt.Errorf("stage the record: %w", err)
	}
	// The batch that creates the pair's record is synced (BUG-0809): every later record of the
	// pair commits unsynced and is answered on the log's fsync alone, which holds only while a
	// boot replay finds the record; a pair with no record is skipped by every replay.
	created := p == nil || !p.present
	if beforeAdvanceCommit != nil {
		beforeAdvanceCommit(key.slot, key.db)
	}
	commitErr := req.Commit(batch, created)
	if commitErr != nil && !errors.Is(commitErr, storage.ErrCommittedNotSynced) {
		return fmt.Errorf("commit the write: %w", commitErr)
	}
	// A batch whose sync alone failed is committed: the store holds its keys and the record, so
	// memory is installed from it as from any commit. Left out, the pair would read as record-less
	// at applied 0 while the store holds the write, and a replay would apply the write a second time
	// (BUG-0809). The store refuses every later write (storage.StoreSyncFailedError), so nothing
	// acknowledged after this batch can depend on it reaching the disk.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.genSeq++
	if p == nil {
		p = &reconcilePair{slot: reconcileSlotFrom(next), present: true}
		r.pairs[key] = p
	}
	p.disk = next
	p.slot.Applied = next.Applied
	p.gen = r.genSeq
	shared.AppliedPos, shared.Ordinal, shared.TailCursor = next.Applied, next.Ordinal, next.WALCursor
	// The pair has a record now, so no replay of it is owed a creation any more.
	shared.Flags &^= flagFirstApplyFailed
	// The commit returned (a sync that alone failed still committed) and the pair has a cursor:
	// its record no longer needs the floor, and a replay releases a failed first write's hold.
	if shared.TailCursor != 0 {
		r.cfg.Floor.release(floorKey)
	}
	if commitErr != nil {
		return fmt.Errorf("commit the write: %w", commitErr)
	}
	return nil
}

// rangeHoldsKeys says whether the pair's key range holds a key now, over the bounds a seed's
// range reset deletes (Puller.resetRange). Where a decision rests on it the pair lock is held,
// so no write, chunk or delete of the pair runs beside the look: a step that would create the
// record, and (after rangeHeldIn said yes) a pass that visits an absent pair in the copy set.
// The puller also asks it, without the lock, for the status row of an attempt that waits for
// a source (Puller.noteKept), where nothing is decided on it. One snapshot and one seek each
// time; nothing per bucket (Rule 11).
func (r *Reconciler) rangeHoldsKeys(key reconcileKey) (bool, error) {
	snap := r.cfg.Store.NewSnapshot()
	defer snap.Discard()
	return rangeHeldIn(snap, key)
}

// rangeHeldIn says whether the pair's key range holds a key in snap: one seek.
func rangeHeldIn(snap storage.KVSnapshot, key reconcileKey) (bool, error) {
	prefix := storage.PairPrefix(key.slot, key.db)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer it.Close()
	it.Seek(prefix)
	held := it.Valid()
	if err := it.Err(); err != nil {
		return false, fmt.Errorf("look for keys in the pair's range: %w", err) // the callers name the pair
	}
	return held, nil
}

// inTargetSet says whether this node is in the pair's copy set at the committed state,
// the test a live step to a record-less pair is judged by. A live step reaches the
// advancer only after the Raft log is applied (mayOrder needs the committed epoch and a
// lease), so the state read here is the applied-committed one.
func (r *Reconciler) inTargetSet(key reconcileKey) bool {
	state := r.cfg.State.State()
	if state.StateSeq == 0 {
		return false
	}
	table := reconcilePlacingTable(state, r.cfg.NodeID)
	for _, database := range state.Databases {
		if uint64(database.ID) != key.db {
			continue
		}
		for _, member := range table.Copies(key.slot, int(database.RF)) {
			if member.ID == r.cfg.NodeID {
				return true
			}
		}
	}
	return false
}

// load reads every record under the seed-state namespace into pairs. It holds
// no lock while it reads; each pair is installed under its own lock.
func (r *Reconciler) load() error {
	r.mu.Lock()
	already := r.loaded
	r.mu.Unlock()
	if already {
		return errors.New("OnBoot already ran. A second boot would install older disk state over newer memory; restart the process to reload")
	}
	type loaded struct {
		key reconcileKey
		rec SeedRecord
	}
	var found []loaded
	lower := storage.NodeKey(storage.NSSeedState)
	snap := r.cfg.Store.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: reconcilePrefixEnd(lower)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) != len(lower)+10 {
			return fmt.Errorf("load seed records: key %x is %d bytes, want %d. The seed-state namespace holds only 21-byte keys; this store is damaged", key, len(key), len(lower)+10)
		}
		slot := binary.BigEndian.Uint16(key[len(lower):])
		dbID := binary.BigEndian.Uint64(key[len(lower)+2:])
		raw, err := it.Value()
		if err != nil {
			return fmt.Errorf("load seed record slot %d database %d: %w", slot, dbID, err)
		}
		if len(raw) == localPairRecordLen {
			continue // the position record of a pair that was never seeded (SPEC-GAP T3-1)
		}
		rec, err := DecodeSeedRecord(raw)
		if err != nil {
			return fmt.Errorf("load seed record slot %d database %d: %w", slot, dbID, err)
		}
		found = append(found, loaded{reconcileKey{slot, dbID}, rec})
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("load seed records: %w", err)
	}
	for _, f := range found {
		r.cfg.Pairs.With(f.key.slot, f.key.db, func(pair *cluster.Pair) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.genSeq++
			r.pairs[f.key] = &reconcilePair{disk: f.rec, present: true, slot: reconcileSlotFrom(f.rec), gen: r.genSeq}
			if f.rec.State == machine.SeedReady {
				pair.AppliedPos, pair.Ordinal, pair.TailCursor = f.rec.Applied, f.rec.Ordinal, f.rec.WALCursor
			}
		})
	}
	r.mu.Lock()
	r.loaded = true
	r.mu.Unlock()
	return nil
}

// reconcile runs the table for every pair the node should hold or does hold.
func (r *Reconciler) reconcile(ctx context.Context, s cluster.ClusterState, forceReset bool) ([]Action, error) {
	if r.cfg.Diverged != nil && r.cfg.Diverged() {
		return nil, nil // the guard of last resort: whatever drove this pass, a frozen state retires nothing
	}
	if s.StateSeq == 0 || len(s.Members) == 0 {
		return nil, fmt.Errorf("reconcile: the cluster state is empty (state_seq %d, %d members). Nothing is retired or dropped from a state that is not loaded; wait for the committed state, or run 'join <node>@<host:port>' if this node is new", s.StateSeq, len(s.Members))
	}
	if _, ok := reconcileMember(s, r.cfg.NodeID); !ok {
		return nil, fmt.Errorf("reconcile: node %q is not a member in state_seq %d. A state that does not list this node says nothing about what it holds; run 'nodes' to see the members", r.cfg.NodeID, s.StateSeq)
	}
	for _, m := range s.Members {
		if len(m.ID) > seedRecordMaxSource {
			return nil, fmt.Errorf("member id %q is %d bytes; a record names its source in %d. Rename the node", m.ID[:16]+"...", len(m.ID), seedRecordMaxSource)
		}
	}
	table := reconcilePlacingTable(s, r.cfg.NodeID)
	joining := false
	if member, ok := reconcileMember(s, r.cfg.NodeID); ok {
		joining = member.State == cluster.MemberJoining
	}
	r.mu.Lock()
	passGen := r.genSeq
	r.mu.Unlock()
	snap := r.cfg.Store.NewSnapshot()
	defer snap.Discard()
	w := r.newWriter()
	defer w.abort()
	var actions []Action
	known := make(map[uint64]bool, len(s.Databases))
	visit := func(key reconcileKey, view reconcileView) error {
		r.mu.Lock()
		held := r.pairs[key]
		var heldGen uint64
		var heldState machine.SlotState
		if held != nil {
			heldGen, heldState = held.gen, held.slot.State
		}
		r.mu.Unlock()
		if held == nil && !view.inTarget {
			return nil // nothing to decide, and no pair-set entry to create
		}
		if held != nil && !view.inTarget && (heldState == machine.SlotReady || heldState == machine.SlotSeeding) &&
			(!forceReset || heldGen > passGen) && r.stateMovedPast(s) {
			// s is no longer the committed state, and a live step (a write's record, a seed)
			// judges a pair against the committed one: a database created after s was read is
			// not in s, so every pair of it looks out of the set, and retiring or deleting it
			// here would take a copy away from the node the committed table keeps it on
			// (BUG-0757). The caller read s before this pass read anything of its own, so no
			// mark taken inside the pass tells such a record from an older one (BUG-0893):
			// a pass over a state that was moved past retires nothing that is ready or
			// seeding. A retire that s is right about is done by the next pass, which the
			// driver runs because the state moved.
			//
			// A pass that resets (a joiner's first) keeps the narrower test, the pair changed
			// since the pass began: the pass that follows it resets nothing, so a record
			// loaded from disk that is out of the set in s and back in it one commit later
			// would stay ready with whatever it held before the join.
			return nil
		}
		var visitErr error
		r.cfg.Pairs.With(key.slot, key.db, func(shared *cluster.Pair) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if err := r.checkNotBusy(key); err != nil {
				visitErr = err
				return
			}
			orig := r.pairs[key]
			sp := r.stageFrom(key, shared)
			act, err := r.reconcilePair(pairRequest{Snap: snap, Writer: w, Key: key, Staged: sp, View: view, Epoch: uint64(s.Epoch), ForceReset: forceReset})
			if err != nil {
				visitErr = fmt.Errorf("slot %d db %d: reconcile: %w", key.slot, key.db, err)
				return
			}
			if reconcileChanged(orig, sp) {
				w.stage(key, sp)
			}
			if act.Do != machine.SlotDoNothing || act.ProposeRetire {
				actions = append(actions, Action{Slot: key.slot, DBID: key.db, Epoch: uint64(s.Epoch), SlotAction: act})
			}
		})
		if visitErr != nil {
			return visitErr
		}
		if w.full() {
			if err := w.commit(); err != nil {
				return fmt.Errorf("reconcile epoch %d: %w", s.Epoch, err)
			}
			r.flushDrops()
		}
		return nil
	}
	for _, db := range s.Databases {
		known[uint64(db.ID)] = true
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("reconcile database %q: %w", db.Name, err)
		}
		for slot := 0; slot < storage.SlotCount; slot++ {
			copies := table.Copies(uint16(slot), int(db.RF))
			view := reconcileViewOf(reconcileViewRequest{
				State: s, Self: r.cfg.NodeID, Key: reconcileKey{uint16(slot), uint64(db.ID)},
				Copies: copies, Joining: joining, Table: table, RF: int(db.RF),
			})
			if err := visit(reconcileKey{uint16(slot), uint64(db.ID)}, view); err != nil {
				return nil, err
			}
		}
	}
	r.mu.Lock()
	var gone []reconcileKey
	for key := range r.pairs {
		if !known[key.db] {
			gone = append(gone, key)
		}
	}
	r.mu.Unlock()
	for _, key := range gone {
		// A database that is gone from the state: nothing is in its target set.
		if err := visit(key, reconcileView{}); err != nil {
			return nil, err
		}
	}
	if err := w.commit(); err != nil {
		return nil, fmt.Errorf("reconcile epoch %d: %w", s.Epoch, err)
	}
	r.flushDrops()
	// A pair that changed under the pass kept its newer state; the action the
	// pass computed from the older picture is dropped and the next pass redoes it.
	return slices.DeleteFunc(actions, func(a Action) bool { _, lost := w.lost[reconcileKey{a.Slot, a.DBID}]; return lost }), nil
}

// stateMovedPast says the committed state has moved on from s, so a pass over s is looking
// at an older picture than the live steps that run beside it.
func (r *Reconciler) stateMovedPast(s cluster.ClusterState) bool {
	if src, ok := r.cfg.State.(stateSeqSource); ok {
		return src.StateSeq() > s.StateSeq
	}
	return r.cfg.State.State().StateSeq > s.StateSeq
}

// dispatch hands each action to the sink, in slot order, after every record
// change it depends on is durable. The pass leaves a seeding pair that left the
// target seeding (SlotDeleteRangeAndForget); the sink voids that record (a fresh
// attempt in phase reset, synced), the range is deleted under it, and the record
// is removed last (dropLeftSeed). A crash at any point leaves a record that
// admits nothing and that a boot starts from a range reset or drops again, and
// never a resumable cut over a deleted range. Removing the record first left a
// pair with no record over a range that still held keys, which an active member
// admits writes to (BUG-0817); deleting first would leave a seeding record whose
// cursor points into missing data, and a pair that came back into the target
// would resume it.
func (r *Reconciler) dispatch(actions []Action) error {
	slices.SortFunc(actions, func(a, b Action) int {
		if a.DBID != b.DBID {
			return cmpUint(a.DBID, b.DBID)
		}
		return cmpUint(uint64(a.Slot), uint64(b.Slot))
	})
	for _, a := range actions {
		if err := r.cfg.Sink.Do(a); err != nil {
			return fmt.Errorf("slot %d database %d: %w", a.Slot, a.DBID, err)
		}
	}
	r.flushDrops()
	return nil
}

// flushDrops forgets the pair-set entries of pairs that no longer have a
// record. It runs with no lock held, because PairSet.Drop waits for the pair
// lock.
func (r *Reconciler) flushDrops() {
	r.mu.Lock()
	drops := r.drops
	r.drops = nil
	r.mu.Unlock()
	for _, key := range drops {
		r.mu.Lock()
		_, stillHeld := r.pairs[key]
		r.mu.Unlock()
		if !stillHeld {
			r.cfg.Pairs.Drop(key.slot, key.db)
		}
	}
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// pairRequest is one Reconciled event for one pair.
type pairRequest struct {
	Snap       storage.KVSnapshot
	Writer     *reconcileWriter
	Key        reconcileKey
	Staged     *stagedPair
	View       reconcileView
	Epoch      uint64
	ForceReset bool
}

// seedRestartRangeReset is SlotAction.Restart for a record whose attempt has not registered
// (phase reset): the range may hold anything, so the attempt starts from a reset.
const seedRestartRangeReset = 4

// reconcilePair runs one Reconciled event on one pair.
func (r *Reconciler) reconcilePair(req pairRequest) (machine.SlotAction, error) {
	snap, w, key, sp, view, epoch := req.Snap, req.Writer, req.Key, req.Staged, req.View, req.Epoch
	p := &sp.p
	if r.cfg.Deferred != nil && r.cfg.Deferred(key.slot, key.db) {
		// The pair's record says ready and its applied position lags its log (BUG-0760). A retire
		// would write a fence from that position and a restart would forget the deferral, so the
		// retire gate could delete a record only this node's log holds. Decide nothing: the
		// record stays ready, a restart defers the pair again, and the next pass after the
		// replay judges it on its real position.
		return machine.SlotAction{}, nil
	}
	if req.ForceReset && view.inTarget && p.slot.State != machine.SlotAbsent && (!r.keepSeeding || p.slot.State != machine.SlotSeeding) {
		r.freshAttempt(sp)
		if err := r.persist(w, key, sp); err != nil {
			return machine.SlotAction{}, fmt.Errorf("reset the record: %w", err)
		}
	}
	seen := reconcileSeen{epoch: epoch, inTarget: view.inTarget, copies: strings.Join(view.others, "\x00")}
	repeatedView := p.hasSeen && p.seen == seen
	if p.hasSeen && p.seen.epoch == epoch && p.seen != seen {
		// An RF change moves state_seq and not the epoch, and the slot machine
		// treats a same-epoch view with another copy set as an old event and
		// ignores it. The machine takes the first view it is given after a
		// restart, so give it the persisted record afresh. What it forgets is
		// evidence gathered for the old member set, which was void anyway.
		p.slot = reconcileSlotFrom(p.disk)
		if !p.present {
			p.slot = machine.SlotRecord{}
		}
	}
	p.seen, p.hasSeen = seen, true
	if p.slot.State == machine.SlotReady {
		// The write path moves the pair's position after its batch commits; the
		// fence a retire writes must not be older than the writes already applied.
		if applied := sp.shared.AppliedPos; p.slot.Applied.Less(applied) {
			p.slot.Applied = applied
		}
	}
	slotView := machine.SlotView{
		InTargetSet: view.inTarget, OwnBit: view.ownBit, Rank1: view.rank1,
		OthersHoldBit: view.othersHoldBit, Joining: view.joining,
		Current: r.cfg.Current(key.slot, key.db), Copies: view.others,
	}
	if p.slot.State == machine.SlotRetired || p.slot.State == machine.SlotRetiring {
		fence, ok, err := reconcileReadFence(snap, key)
		if err != nil {
			return machine.SlotAction{}, err // reconcileReadFence names the pair
		}
		slotView.Fence, slotView.HasFence = fence, ok
	}
	if p.slot.State == machine.SlotAbsent && view.inTarget {
		// The table takes an absent in-target pair for a new one unless its range says
		// otherwise: `seeding forget` removes a retired record and keeps the range (BUG-0819).
		// The pass's snapshot answers for nearly every pair with one seek: a range that was
		// empty when the pass began is empty now, because a range gains keys only under a
		// record, and a record that came after the snapshot is not gone again before the pass
		// that would retire it. A key in it is looked for once more on a snapshot of now,
		// because a left seed's delete may have emptied the range since the pass began.
		held, err := rangeHeldIn(snap, key)
		if err == nil && held {
			held, err = r.rangeHoldsKeys(key)
		}
		if err != nil {
			return machine.SlotAction{}, err
		}
		slotView.RangeHoldsKeys = held
	}
	act := p.slot.Apply(machine.Reconciled{View: slotView, Epoch: epoch})
	if act.WriteFence && !slotView.HasFence {
		// settle writes this fence below. The machine learns a fence only from a view that
		// carries it, and the next view comes with the next state_seq, which an idle cluster
		// never commits: without this the other copies' answers cover the fence and the gate
		// still says "write the fence", so the departed copy is never deleted.
		slotView.Fence, slotView.HasFence = act.Fence, true
		p.slot.Apply(machine.Reconciled{View: slotView, Epoch: epoch})
	}
	if act.Do == machine.SlotWriteReadyEmpty {
		// The table's reconciled/absent-in-target row decided that no other member holds the
		// pair's data, this node is not joining and the range holds no key, so the pair is ready empty (seeding-completion
		// plan §0.3, the owner's Q5 ruling, decisions §16 items 21, 33 and 37). The machine
		// cannot write the record's position, ordinal and cursor, which belong to the pair, so
		// the write is done here, after the table ran on the event: the machine's world is
		// known and the pair answers the horizon row like any other ready pair.
		r.interimReady(sp)
		// The pair is ready and its bit is not set, and the reporter looks for that only when
		// state_seq moves, which may be before this pass wrote the record: ask for the report.
		act = machine.SlotAction{Do: machine.SlotReportBit}
	}
	settled := act
	if act.Do == machine.SlotRestartSeed && act.Restart == seedRestartEpochChanged && repeatedView && p.seed != nil {
		// The table calls the record unresumable because its cut is zero, which is what an
		// attempt on an empty range has after it registered at 0:0. This process registered
		// it (its seed machine is in memory) and the view is the one the last pass saw, so
		// the epoch did not change and the attempt is running: restarting it would reset
		// the partial range and orphan the source's pin, once per commit that moves
		// state_seq (a seed bit report moves it too, so one join passes about pairs/64 times).
		settled, act = machine.SlotAction{}, machine.SlotAction{}
	}
	if act.Do == machine.SlotRestartSeed && act.Restart == seedRestartRangeReset && !act.RemoveRecord && repeatedView &&
		p.slot.State == machine.SlotSeeding && p.slot.Phase == machine.PhaseReset {
		// The table asks for a fresh attempt because the record has not registered yet, and
		// this one is already a fresh attempt that an earlier pass wrote for this very view.
		// Writing another mints a new seed id for every queued pair on every commit and
		// voids the registration a puller has in flight on the old id. The action stays a
		// queue request, which is idempotent and keeps the pair known to the puller.
		settled, act = machine.SlotAction{}, machine.SlotAction{Do: machine.SlotQueueSeed}
	}
	if err := r.settle(w, key, sp, settled); err != nil {
		return machine.SlotAction{}, fmt.Errorf("persist the table's decision: %w", err)
	}
	if act.Do == machine.SlotResumeSeed {
		r.resume(p, epoch)
	}
	return act, nil
}

// settle makes the record match what the machine decided: a fresh attempt for
// a queued or restarted seed, the fence beside a retire, and the record write.
func (r *Reconciler) settle(w *reconcileWriter, key reconcileKey, sp *stagedPair, act machine.SlotAction) error {
	if act.Do == machine.SlotQueueSeed || act.Do == machine.SlotRestartSeed {
		r.freshAttempt(sp)
	}
	if act.WriteFence {
		if err := w.put(key, reconcileFenceKey(key, act.Fence), []byte{}); err != nil {
			return fmt.Errorf("slot %d db %d: write the fence: %w", key.slot, key.db, err)
		}
	}
	// A record that makes data untrusted (a removed seeding record, a retired or
	// retiring one, a reset) is committed before the destructive step the
	// action asks for: dispatch and the caller run after the commit.
	return r.persist(w, key, sp)
}

// freshAttempt writes a new seeding attempt from phase reset through the seed
// machine, so the record, the machine and the disk agree. The range may hold
// anything, so it always owes the reset.
func (r *Reconciler) freshAttempt(sp *stagedPair) {
	p := &sp.p
	seed := reconcileSeedFrom(p)
	seed.Apply(machine.AttemptWritten{SeedID: r.mintSeedID(), LocalNonEmpty: true}, machine.SeedConfig{})
	p.slot.State, p.slot.Phase = machine.SlotState(seed.State), seed.Phase
	p.slot.Applied, p.slot.Cut, p.slot.Cursor = seed.Applied, seed.Cut, seed.Cursor
	p.slot.Source, p.slot.SeedID = seed.Source, seed.SeedID
	p.disk.WALCursor, p.disk.Ordinal = 0, 0
	p.seed = nil
	// The range is about to be reset, so the pair claims no position and no
	// ordinal; the zero value is installed with the record.
	sp.shared, sp.setShared = cluster.Pair{}, true
}

// interimReady makes the staged absent pair ready with the interim's signature: a zero cut,
// no source and a zero SeedID. The table decides when to write it (SlotWriteReadyEmpty: no
// other member holds the pair's bit); this method only writes it. Nothing reseeds a ready
// pair against another member's bit afterwards (the owner dropped that row on 2026-11-05,
// because it made every idle copy of a fresh database seed once and its first write degraded). The position, ordinal and cursor are the pair's own:
// a copy that applied replicated writes before the first pass (admitted through
// recordSlots.Admits) has all three in cluster.Pair, and reconcilePair's raise-to-AppliedPos
// block runs only for a record that is already ready while persistTo writes Applied alone
// and keeps the ordinal and cursor from the zero disk image, so all three are set here
// explicitly (pi's fourth review, SF-1). An interim at ordinal 0 for such a pair would make
// the next restart regress its ordinal and the next write take 1 against dedupe entries
// already at N. A pair that never applied gets 0:0, ordinal 0, cursor 0: the signature.
func (r *Reconciler) interimReady(sp *stagedPair) {
	p := &sp.p
	p.slot.State, p.slot.Phase = machine.SlotReady, machine.PhaseNone
	p.slot.Applied, p.slot.Cut, p.slot.Cursor = sp.shared.AppliedPos, cluster.Position{}, nil
	p.slot.Source, p.slot.SeedID = "", [16]byte{}
	p.disk.Ordinal, p.disk.WALCursor = sp.shared.Ordinal, sp.shared.TailCursor
	p.seed = nil
}

// resume registers a seeding record again on its cut, which is what makes the
// attempt's events current after a restart (conv §12 contract 4).
func (r *Reconciler) resume(p *reconcilePair, epoch uint64) {
	seed := reconcileSeedFrom(p)
	// The slot machine already judged this cut resumable (slotResumable), so the
	// outcome carries no restart.
	seed.Apply(machine.RegisterAccepted{Cut: p.slot.Cut, WindowBytes: r.cfg.WindowBytes}, machine.SeedConfig{Epoch: epoch})
	p.seed = &seed
}

// persist stages the record the machine left into the writer's batch.
func (r *Reconciler) persist(w *reconcileWriter, key reconcileKey, sp *stagedPair) error {
	return r.persistTo(sp, key,
		func(k, v []byte) error { return w.put(key, k, v) },
		func(k []byte) error { return w.erase(key, k) })
}

// persistTo stages the record the machine left through put and del, on the
// staged copy only. A record that did not change from the staged copy's disk
// image is not rewritten; the image is the last confirmed one, so memory can
// never look unchanged while the disk is behind. While a pair is ready its
// position, cursor and ordinal belong to the data batches (ApplyAdvance), so
// they are kept.
func (r *Reconciler) persistTo(sp *stagedPair, key reconcileKey, put func(k, v []byte) error, del func(k []byte) error) error {
	p := &sp.p
	recordKey := SeedRecordKey(key.slot, key.db)
	if p.slot.State == machine.SlotAbsent {
		if p.present {
			if err := del(recordKey); err != nil {
				return fmt.Errorf("slot %d db %d: remove the record: %w", key.slot, key.db, err)
			}
		}
		p.present, p.disk = false, SeedRecord{}
		return nil
	}
	next := p.disk
	next.State, next.Phase = uint8(p.slot.State), p.slot.Phase
	next.Cut, next.Cursor, next.Source, next.SeedID = p.slot.Cut, p.slot.Cursor, p.slot.Source, p.slot.SeedID
	if !p.present || p.disk.State != machine.SeedReady || next.State != machine.SeedReady {
		next.Applied = p.slot.Applied
	}
	raw, err := EncodeSeedRecord(next)
	if err != nil {
		return fmt.Errorf("slot %d database %d: %w", key.slot, key.db, err)
	}
	if p.present {
		// p.disk was decoded or encoded before, so it encodes again.
		prev, err := EncodeSeedRecord(p.disk)
		if err != nil {
			return fmt.Errorf("slot %d database %d: %w", key.slot, key.db, err)
		}
		if bytes.Equal(raw, prev) {
			return nil
		}
	}
	if err := put(recordKey, raw); err != nil {
		return fmt.Errorf("slot %d database %d: %w", key.slot, key.db, err)
	}
	p.disk, p.present = next, true
	return nil
}

// stagedPair is a pair's next state, built on copies. It reaches r.pairs and
// the pair set only through installLocked, after its batch has committed.
type stagedPair struct {
	base      uint64 // live gen when copied (0: no live state)
	p         reconcilePair
	shared    cluster.Pair
	setShared bool // shared replaces the pair-set state at install
}

// stageFrom copies the pair's confirmed state. mu and the pair lock are held.
func (r *Reconciler) stageFrom(key reconcileKey, shared *cluster.Pair) *stagedPair {
	sp := &stagedPair{shared: *shared}
	if p := r.pairs[key]; p != nil {
		sp.p = *p
		sp.base = p.gen
	}
	return sp
}

// movedLocked reports whether the live pair changed since sp copied it. mu held.
func (r *Reconciler) movedLocked(key reconcileKey, sp *stagedPair) bool {
	var live uint64
	if p := r.pairs[key]; p != nil {
		live = p.gen
	}
	return live != sp.base
}

// reconcileChanged says whether a pass has anything to install for the pair: a
// different image of its state, or a pair-set update.
func reconcileChanged(orig *reconcilePair, sp *stagedPair) bool {
	if sp.setShared {
		return true
	}
	if orig == nil {
		return sp.p.slot.State != machine.SlotAbsent || sp.p.present
	}
	staged := sp.p
	staged.gen = orig.gen
	return !reflect.DeepEqual(*orig, staged)
}

// installLocked makes a committed staged pair the pair's state. mu and the pair
// lock are held. A pair with no record and no machine state leaves the map.
func (r *Reconciler) installLocked(key reconcileKey, sp *stagedPair, shared *cluster.Pair) {
	delete(r.busy, key)
	if sp.p.slot.State == machine.SlotAbsent && !sp.p.present {
		delete(r.pairs, key)
		r.drops = append(r.drops, key)
	} else {
		r.genSeq++
		installed := sp.p
		installed.gen = r.genSeq
		r.pairs[key] = &installed
	}
	if sp.setShared {
		*shared = sp.shared
	}
}

func (r *Reconciler) mintSeedID() [16]byte {
	r.seedSeq++
	var id [16]byte
	binary.BigEndian.PutUint64(id[0:8], uint64(r.cfg.Clock.Now().UnixNano()))
	binary.BigEndian.PutUint64(id[8:16], r.seedSeq)
	return id
}

func reconcileSlotFrom(rec SeedRecord) machine.SlotRecord {
	return machine.SlotRecord{
		State: machine.SlotState(rec.State), Phase: rec.Phase, Applied: rec.Applied, Cut: rec.Cut,
		Cursor: rec.Cursor, Source: rec.Source, SeedID: rec.SeedID,
	}
}

func reconcileSeedFrom(p *reconcilePair) machine.SeedRecord {
	return machine.SeedRecord{
		State: uint8(p.slot.State), Phase: p.slot.Phase, Applied: p.slot.Applied, Cut: p.slot.Cut,
		SeedID: p.slot.SeedID, Source: p.slot.Source, Cursor: p.slot.Cursor,
	}
}

// reconcileView is what committed state says about one pair for this node.
type reconcileView struct {
	inTarget, ownBit, rank1 bool
	// othersHoldBit says another member of the copy set holds the committed SEEDED bit, so
	// an empty pair here is not a new one; joining says this node is joining.
	othersHoldBit, joining bool
	others                 []string
}

func reconcileMember(s cluster.ClusterState, id string) (cluster.Member, bool) {
	for _, m := range s.Members {
		if m.ID == id {
			return m, true
		}
	}
	return cluster.Member{}, false
}

// reconcilePlacingTable ranks the state. A joining node is promoted to ranked
// for its own table only, which is the placing set copies(R(e) + self) of conv
// §14; every other joining identity stays unranked, so two joiners never see
// each other.
func reconcilePlacingTable(s cluster.ClusterState, self string) *cluster.SlotTable {
	member, ok := reconcileMember(s, self)
	if !ok || member.State != cluster.MemberJoining || member.Witness {
		return cluster.BuildTable(s)
	}
	placing := s
	placing.Members = slices.Clone(s.Members)
	for i := range placing.Members {
		if placing.Members[i].ID == self {
			placing.Members[i].State = cluster.MemberActive
		}
	}
	return cluster.BuildTable(placing)
}

// reconcileViewRequest is what committed state says about one pair.
type reconcileViewRequest struct {
	State   cluster.ClusterState
	Self    string
	Key     reconcileKey
	Copies  []cluster.Member
	Joining bool
	Table   *cluster.SlotTable
	RF      int
}

func reconcileViewOf(req reconcileViewRequest) reconcileView {
	self, slot, dbID, copies := req.Self, req.Key.slot, req.Key.db, req.Copies
	var view reconcileView
	for _, m := range copies {
		if m.ID == self {
			view.inTarget = true
			continue
		}
		view.others = append(view.others, m.ID)
	}
	if owner, ok := req.Table.Owner(slot, req.RF); ok && owner.ID == self && !req.Joining {
		view.rank1 = true
	}
	view.joining = req.Joining
	for _, bm := range req.State.SeedBitmaps {
		if bm.DBID != int64(dbID) || len(bm.Bits) <= int(slot)/8 || bm.Bits[slot/8]>>(slot%8)&1 == 0 {
			continue
		}
		if bm.Node == self {
			view.ownBit = true
		} else if slices.Contains(view.others, bm.Node) {
			view.othersHoldBit = true
		}
	}
	return view
}

// reconcileFenceKey is the 's' reclaim record:
// [node prefix][NSReclaim]['s'][slot:2][db:8][epoch:8][seq:8], empty value.
func reconcileFenceKey(key reconcileKey, fence cluster.Position) []byte {
	return storage.NodeKey(storage.NSReclaim, []byte{reconcileFenceKind}, reconcilePairBytes(key), cluster.AppendPosition(nil, fence))
}

func reconcilePairBytes(key reconcileKey) []byte {
	out := binary.BigEndian.AppendUint16(nil, key.slot)
	return binary.BigEndian.AppendUint64(out, key.db)
}

// reconcileReadFence returns the highest 's' record of the pair.
func reconcileReadFence(snap storage.KVSnapshot, key reconcileKey) (cluster.Position, bool, error) {
	lower := storage.NodeKey(storage.NSReclaim, []byte{reconcileFenceKind}, reconcilePairBytes(key))
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: reconcilePrefixEnd(lower)})
	defer it.Close()
	var fence cluster.Position
	found := false
	for it.Seek(nil); it.Valid(); it.Next() {
		pos, err := cluster.DecodePosition(it.Key()[len(lower):])
		if err != nil {
			return cluster.Position{}, false, fmt.Errorf("read fence slot %d database %d: %w", key.slot, key.db, err)
		}
		fence, found = pos, true
	}
	if err := it.Err(); err != nil {
		return cluster.Position{}, false, fmt.Errorf("read fence slot %d database %d: %w", key.slot, key.db, err)
	}
	return fence, found, nil
}

// reconcilePrefixEnd is the smallest key above every key that starts with prefix.
func reconcilePrefixEnd(prefix []byte) []byte {
	end := slices.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	// Every prefix here holds a namespace byte below 0xFF, so the loop has
	// returned; the zeroed copy is only the compiler's way out.
	return end
}

// reconcileWriter stages record writes into one batch, remembers the staged
// pairs those writes belong to, and installs them only after the batch commits.
type reconcileWriter struct {
	r       *Reconciler
	batch   storage.KVBatch
	ops     int
	pending []pendingPair
	marked  []reconcileKey            // pairs this writer marked busy
	lost    map[reconcileKey]struct{} // staged pairs whose install was skipped
	// inline is the pair-set state of the one pair whose lock the caller holds
	// together with mu (Feed, FeedSeed); nil when commit runs with no lock held.
	inline *cluster.Pair
	// unsynced commits without a store sync; only FeedSeed sets it, for the steps of an
	// attempt the ready record's synced commit covers (seedStepCommitsUnsynced).
	unsynced bool
}

type pendingPair struct {
	key reconcileKey
	sp  *stagedPair
}

func (r *Reconciler) newWriter() *reconcileWriter {
	return &reconcileWriter{r: r}
}

func (w *reconcileWriter) open() {
	if w.batch == nil {
		w.batch = w.r.cfg.Store.NewBatch()
	}
}

// put stages a write for the pair key. mu is held.
func (w *reconcileWriter) put(key reconcileKey, k, v []byte) error {
	w.open()
	w.markBusy(key)
	if err := w.batch.Set(k, v); err != nil {
		return fmt.Errorf("stage record write: %w", err)
	}
	w.ops++
	return nil
}

// erase stages a delete for the pair key. mu is held.
func (w *reconcileWriter) erase(key reconcileKey, k []byte) error {
	w.open()
	w.markBusy(key)
	if err := w.batch.Delete(k); err != nil {
		return fmt.Errorf("stage record delete: %w", err)
	}
	w.ops++
	return nil
}

// stage registers the pair's next state for install at the next commit. A pair
// staged twice keeps its latest state.
func (w *reconcileWriter) stage(key reconcileKey, sp *stagedPair) {
	for i := range w.pending {
		if w.pending[i].key == key {
			w.pending[i].sp = sp
			return
		}
	}
	w.pending = append(w.pending, pendingPair{key, sp})
}

// markBusy refuses other entries on the pair between this staging and its
// install. A writer that holds the pair lock through its install needs no mark.
func (w *reconcileWriter) markBusy(key reconcileKey) {
	if w.inline != nil {
		return
	}
	w.r.busy[key] = struct{}{}
	w.marked = append(w.marked, key)
}

func (w *reconcileWriter) full() bool { return w.ops >= reconcileBatchLimit }

// commit makes everything staged durable, then installs the staged pairs. On a
// failure nothing is installed and every staged pair keeps its old state.
func (w *reconcileWriter) commit() error {
	pending := w.pending
	w.pending = nil
	if w.batch != nil {
		batch := w.batch
		w.batch, w.ops = nil, 0
		err := batch.Commit(!w.unsynced)
		batch.Discard()
		if err != nil {
			w.release()
			if len(pending) > 0 {
				first := pending[0].key
				return fmt.Errorf("slot %d db %d (and %d more pairs in the batch): commit seed records: %w", first.slot, first.db, len(pending)-1, err)
			}
			return fmt.Errorf("commit seed records: %w", err)
		}
	}
	w.install(pending)
	w.release()
	return nil
}

// abort drops whatever is staged and not committed. It is safe after commit.
func (w *reconcileWriter) abort() {
	if w.batch != nil {
		w.batch.Discard()
		w.batch, w.ops = nil, 0
	}
	w.pending = nil
	w.release()
}

// release clears the busy marks this writer set.
func (w *reconcileWriter) release() {
	if len(w.marked) == 0 {
		return
	}
	// A non-inline writer runs with no lock held.
	w.r.mu.Lock()
	for _, key := range w.marked {
		delete(w.r.busy, key)
	}
	w.r.mu.Unlock()
	w.marked = nil
}

func (w *reconcileWriter) install(pending []pendingPair) {
	if w.inline != nil {
		for _, pp := range pending {
			w.r.installLocked(pp.key, pp.sp, w.inline)
		}
		return
	}
	for _, pp := range pending {
		w.r.cfg.Pairs.With(pp.key.slot, pp.key.db, func(shared *cluster.Pair) {
			w.r.mu.Lock()
			defer w.r.mu.Unlock()
			if w.r.movedLocked(pp.key, pp.sp) {
				// Another entry changed the pair after the pass copied it. Its
				// state is newer than this picture; the pass wrote no record for
				// the pair (a written pair is busy and cannot move), so dropping
				// the picture loses nothing the disk holds.
				delete(w.r.busy, pp.key)
				if w.lost == nil {
					w.lost = make(map[reconcileKey]struct{})
				}
				w.lost[pp.key] = struct{}{}
				return
			}
			w.r.installLocked(pp.key, pp.sp, shared)
		})
	}
}
