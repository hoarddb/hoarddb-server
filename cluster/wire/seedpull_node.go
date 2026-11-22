package wire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The receiver side of seeding, composed into a networked Node (T3).
//
// # What a node holds, and when a pair serves
//
// A pair's state is its seed record: seeding, ready, retired or retiring. recordSlots reads
// it for the read gate, the seed source, the replicator and POSITION_QUERY, so all four
// agree. A pair that is seeding serves no read, is not a source, answers POSITION_QUERY
// "seeding", and the replicator acks a pushed write for it AckNotApplied, which the owner
// does not count (replicate_apply.go). A seeding copy therefore never acknowledges a write.
//
// # One record shape, every data member reconciles (seeding S0; SPEC-GAP T3-1 closed)
//
// Every pair of a cluster node has a seed record: the write path moves it through
// Reconciler.ApplyAdvance, the boot loader rewrites the 32-byte position records of earlier
// builds, and a pair with no record is absent on every node (recordSlots.State), admitted
// for a write or a pushed record on an active node through recordSlots.Admits, and given
// the interim record by the first reconcile pass. The driver runs for every data member:
// a joiner reconciles its placing set, an active member its copy set (an absent in-target
// pair is written ready empty until S3b's bits say otherwise), a witness nothing
// (seeding-completion plan §0.2 and §0.3).
//
// # Cost
//
// recordSlots.State and Admits are one map lookup under the Reconciler's mutex. The driver
// is one goroutine; the puller has seed_slots_in_flight goroutines. Nothing is per bucket.
// Each pass is O(slots x databases), one at a time and one per observed state_seq change:
// every applied command signals the driver, the one-slot wake coalesces the wakes that
// arrive while a pass runs, and the 250 ms ticker does nothing while the state_seq is
// unchanged, so the rate is bounded by the pass duration, not the tick.

// seedParts is the receiver-side seeding state of a networked node.
type seedParts struct {
	recon  *Reconciler
	puller *Puller
	wake   chan struct{}
	// hooks are the reseed's seams (BeforeReset is set by startCluster).
	hooks ReseedHooks
	// dropQueue empties a pair's queued copy writes; report takes the errors that have no caller.
	dropQueue func(slot uint16, db uint64)
	report    func(error)
	// joining is whether this node's member row says joining, as of the last look at the
	// cluster state; observed says there has been one. Until then a pair with no record is
	// absent, not ready: a restarted joiner must not answer POSITION_QUERY ready, ack a write
	// applied, or pass the activation gate for pairs it never pulled (T3 review F3).
	joining  atomic.Bool
	observed atomic.Bool
	// passes is how many reconcile passes the driver has finished; SeedPasses reads it.
	passes atomic.Int64

	mu      sync.Mutex
	lastSeq uint64
	started bool
	// repassAsked makes the driver reconcile once more although the state_seq has not moved
	// (repass). It is a flag beside lastSeq, not a zeroed lastSeq, because a pass that is
	// running when it is set ends by writing lastSeq.
	repassAsked bool
	// latch is whether the committed state is known to be applied (cluster.LoadLatch, the rule
	// the server's replay gate shares through Raft.StateLoaded).
	latch cluster.LoadLatch
}

// stateLoaded says whether a pass may run against the committed state; the rule is
// cluster.LoadLatch's, stepped once per driver tick.
func (s *seedParts) stateLoaded(loaded, diverged bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latch.Step(loaded, diverged)
}

// loadedLatched reports whether the latch already closed, so the driver stops asking the node
// (LoadedNow reads the log) once the answer can no longer change.
func (s *seedParts) loadedLatched() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latch.Loaded
}

// observe records whether the cluster state says this node is joining. It is called with
// the state read from Raft before the node serves, and again on every driver pass.
func (s *seedParts) observe(state cluster.ClusterState, id string) {
	if state.StateSeq == 0 {
		return
	}
	if self, found := reconcileMember(state, id); found {
		s.joining.Store(self.State == cluster.MemberJoining)
		s.observed.Store(true)
	}
}

// dropLeftSeed empties the range of a seeding pair that left the copy set and forgets its
// record: the record is voided first (an attempt in phase reset, durable), the range is
// deleted under it, and the record is removed last, only if it is still that attempt. The
// removal used to come first, which left a pair with no record over a range that still held
// keys for as long as the rate-limited delete ran, and a pair with no record is admitted on
// an active member (BUG-0817). Now the pair says seeding until its range is empty,
// in memory and on disk: a crash anywhere leaves either the whole seed, or an attempt that
// owes a range reset and that nothing can resume, or no record over an empty range. It must
// run inside the pair's one pull run (Puller.dropLeft), so that no pull of the same pair is
// started beside the delete. An accept that was already in flight is not excluded by that:
// Puller.attempt looks for it, and removeLeftSeed keeps a record that registered. Its errors
// name the pair once: the record writes,
// the reconciler's refusals and Puller.resetRange each carry the slot and the database.
func dropLeftSeed(ctx context.Context, recon *Reconciler, key reconcileKey, deleteRange func(context.Context, reconcileKey) error) error {
	attempt, left, err := recon.voidLeftSeed(key.slot, key.db)
	if err != nil {
		return fmt.Errorf("dropping the left seed: %w", err)
	}
	if !left {
		return nil // the pair changed since the pass; the next pass judges it
	}
	if err := deleteRange(ctx, key); err != nil {
		return fmt.Errorf("dropping the left seed's range: %w", err)
	}
	if _, err := recon.removeLeftSeed(key.slot, key.db, attempt); err != nil {
		return fmt.Errorf("dropping the left seed: %w", err)
	}
	return nil
}

// queueLeftSeed is the sink's arm for a seeding pair that left the copy set
// (SlotDeleteRangeAndForget). It voids the attempt here, on the driver, so a pull that is
// running on the pair applies no further chunk and cannot finish, and hands the delete to the
// puller, whose worker runs dropLeftSeed as the pair's one run. The driver therefore waits
// for one record write and never for the rate-limited delete.
func (s *seedParts) queueLeftSeed(key reconcileKey) error {
	_, left, err := s.recon.voidLeftSeed(key.slot, key.db)
	if err != nil {
		return fmt.Errorf("queueing the left seed's drop: %w", err)
	}
	if left {
		s.puller.Queue(key.slot, key.db)
	}
	return nil
}

// forget is the node's `seeding forget`: the reconciler's Forget, then one more pass when a
// record went. Forget keeps the range, and a pair with no record over a range that holds a
// key applies no write (Reconciler.admitAdvance); when the node is in the pair's copy set,
// only a pass ends that, by queueing the seed whose reset deletes the range (BUG-0819), and
// the driver runs no pass while the state_seq stands still.
func (s *seedParts) forget(slot uint16, db uint64, anyway bool) (ForgetOutcome, error) {
	result, err := s.recon.Forget(slot, db, anyway)
	if err == nil && result.Outcome == ForgetRemoved {
		s.repass()
	}
	return result.Outcome, err
}

// repass asks the driver for one pass over the state it already reconciled.
func (s *seedParts) repass() {
	s.mu.Lock()
	s.repassAsked = true
	s.mu.Unlock()
	s.signal()
}

func (s *seedParts) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// recordSlots implements SlotStates from the seed records.
type recordSlots struct{ parts *seedParts }

// State implements SlotStates: the record's own state, and SlotAbsent when the node holds no
// record, on every node (seeding-completion plan §0.2, the owner's O3). The born-ready
// shortcut that answered ready here for a pair with no record is gone, so POSITION_REPLY
// never says ready at 0:0 for a pair the node does not hold; the admission it gave lives
// in Admits. The read gate, the joiner's own readiness check (selfPairsReady)
// and the handoff's position evidence read this state.
func (r recordSlots) State(slot uint16, db uint64) machine.SlotState {
	if record, found := r.parts.recon.Record(slot, db); found {
		return machine.SlotState(record.State)
	}
	return machine.SlotAbsent
}

// Serves implements SlotStates for a copy's read: the machine's own serve rule (ready, not
// asked to rebuild, reconciled in this process, in the target set), not the disk record alone.
func (r recordSlots) Serves(slot uint16, db uint64) bool {
	return r.parts.recon.Serves(slot, db)
}

// RetiredIntact says the pair's record is retired and its range is whole (Reconciler.RetiredIntact).
func (r recordSlots) RetiredIntact(slot uint16, db uint64) bool {
	return r.parts.recon.RetiredIntact(slot, db)
}

// Admits says whether a write or a pushed record for the pair may be applied here: a ready
// record admits, and so does no record on a node that is not joining and has observed the
// cluster state, which is the admission the born-ready shortcut gave (plan §0.2; decisions
// §16 item 45). A joiner holds nothing it has not pulled, and a node that has not seen the
// state cannot say it is not joining, so neither admits an absent pair. Its two callers are
// the Writer's NotReady seam (Node.peers) and the replicator's Ready (replicaReady); the
// advancer then judges a record-less pair against the target set (Reconciler.ApplyAdvance).
func (r recordSlots) Admits(slot uint16, db uint64) bool {
	switch r.State(slot, db) {
	case machine.SlotReady:
		return true
	case machine.SlotAbsent:
		return !r.parts.joining.Load() && r.parts.observed.Load()
	}
	return false
}

// positionState is the POSITION_REPLY state of a slot state.
func positionState(state machine.SlotState) uint8 {
	switch state {
	case machine.SlotReady:
		return PositionStateReady
	case machine.SlotRetired:
		return PositionStateRetired
	case machine.SlotRetiring:
		return PositionStateRetiring
	}
	return PositionStateSeeding // seeding, and a joiner's pair with no record yet
}

// seedSink hands the Reconciler's actions to the puller, the bit report to the reporter and
// the gate actions to the gate, which runs a pass when kicked. Catch-up is T5's seam.
type seedSink struct{ node *Node }

// Do implements ActionSink.
func (k seedSink) Do(action Action) error {
	parts := k.node.cl.seeding.Load()
	if action.ProposeRetire {
		if gate := k.node.cl.gate; gate != nil {
			gate.Kick()
		}
	}
	switch action.Do {
	case machine.SlotQueueSeed, machine.SlotResumeSeed, machine.SlotRestartSeed:
		parts.puller.Queue(action.Slot, action.DBID)
	case machine.SlotDeleteRangeAndForget:
		if err := parts.queueLeftSeed(reconcileKey{action.Slot, action.DBID}); err != nil {
			return err
		}
	case machine.SlotReportBit:
		if reporter := k.node.cl.reporter; reporter != nil {
			reporter.Note(action.DBID, action.Slot, false)
		}
	case machine.SlotRetire, machine.SlotGate, machine.SlotDeleteRange, machine.SlotDeleteRangeThenSeed, machine.SlotFinishDeleteFirst:
		if gate := k.node.cl.gate; gate != nil {
			gate.Kick()
		}
	default:
		// T5: SlotCatchUp is not acted on here.
	}
	return nil
}

// newSeedParts builds the reconciler and the puller of a networked node over its parts. The
// applier and the window are the node's own, which Node.wire builds and the replicator shares.
func (n *Node) newSeedParts(applier Applier, window machine.WindowConfig) *seedParts {
	c := n.cl
	opt := c.opts.Seed.withDefaults()
	parts := newSeedPartsFor(nil, n.raft.State(), n.cfg.NodeID)
	parts.dropQueue, parts.report = c.repl.DropCopyQueue, c.report
	parts.recon = NewReconciler(Config{
		NodeID: n.cfg.NodeID, Store: n.kv, Clock: wallClockNow{}, State: raftState{n: n}, Sink: seedSink{node: n}, Pairs: n.pairs,
		Current:     func(slot uint16, db uint64) bool { return c.repl.IsCurrent(slot, db) },
		Deferred:    func(slot uint16, db uint64) bool { return n.own.isDeferred(slot, db) }, // n.own is set after these parts are built
		WindowBytes: int64(opt.WindowBytes),
		Diverged:    n.raft.IsDiverged,
		Floor:       n.floor,
	})
	parts.puller = NewPuller(PullerConfig{
		Self: n.cfg.NodeID, Recon: parts.recon, KV: n.kv, Options: c.opts.Seed, Call: n.dialSeed,
		Committed: func() uint64 { return n.topology.Committed() },
		StateSeq:  func() uint64 { return uint64(n.raft.StateSeq()) },
		Sources:   n.seedSources,
		OnReady:   n.pairReady,
		DeriveExpiry: func(ctx context.Context, slot uint16, db uint64) error {
			return n.retire.RebuildExpiry(ctx, slot, db)
		},
		Diverged: n.raft.IsDiverged,
		OnError:  c.report,
		Budget:   n.reclaim,
		Applier:  applier, Replicator: c.repl, DedupeWindow: window,
		Buffers:     n.buffers,
		Pace:        newSeedPacer(seedPacerConfig{BytesPerSecond: opt.BytesPerSecond}),
		DiskUsedPct: func() (int, error) { return diskUsedPct(n.cfg.Dir) },
		Deficits:    n.seedDeficits,
	})
	return parts
}

// newSeedPartsFor is the state of a node's seeding before its Reconciler is built: it has
// read the cluster state it was given, or knows it has not (an empty state).
func newSeedPartsFor(recon *Reconciler, state cluster.ClusterState, id string) *seedParts {
	parts := &seedParts{recon: recon, wake: make(chan struct{}, 1)}
	parts.observe(state, id)
	return parts
}

// replicaReady is the replicator's Ready: a pushed write is applied only to a pair the node
// admits (a ready record, or none on an active node that has observed the state), so a
// replicated write to a record-less copy is applied instead of answered AckNotApplied until
// the first reconcile pass writes the copy's interim record (plan §0.2, the reconciler's R1).
func replicaReady(slots recordSlots) func(slot uint16, db uint64) bool {
	return slots.Admits
}

type wallClockNow struct{}

// Now implements Clock.
func (wallClockNow) Now() time.Time { return time.Now() }

// raftState is the StateSource of the Reconciler.
type raftState struct{ n *Node }

// State implements StateSource.
func (s raftState) State() cluster.ClusterState { return s.n.raft.State() }

// StateSeq is the committed state_seq without the copy State makes.
func (s raftState) StateSeq() int64 { return s.n.raft.StateSeq() }

// seedSources lists the nodes a pair may be pulled from: the owner first, then the other
// copies that are not down, never this node.
func (n *Node) seedSources(slot uint16, db uint64) []string {
	var sources []string
	if owner, ok := n.ownerOf(slot, db); ok && owner != n.cfg.NodeID {
		sources = append(sources, owner)
	}
	for _, id := range n.copiesOf(slot, db) {
		if !slices.Contains(sources, id) {
			sources = append(sources, id)
		}
	}
	return sources
}

// pairReady runs once when a pull made a pair ready: the declaration-driven sweeps rerun
// for the pair (seeding section 4 step 10) and the read side may now serve it.
func (n *Node) pairReady(slot uint16, db uint64) {
	if err := n.retire.RerunOnReady(slot, db); err != nil {
		n.cl.report(fmt.Errorf("rerunning the sweeps of slot %d of database %d after its seed: %w", slot, db, err))
	}
}

// loadSeedRecords reads the seed records at boot, before the node serves.
func (n *Node) loadSeedRecords() error {
	if n.cl == nil {
		return nil
	}
	if err := n.cl.seeding.Load().recon.LoadRecords(); err != nil {
		return fmt.Errorf("loading the seed records: %w", err)
	}
	return nil
}

// runSeedDriver reconciles the node when committed state moves.
func (n *Node) runSeedDriver(ctx context.Context) {
	parts := n.cl.seeding.Load()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-parts.wake:
		case <-tick.C:
		}
		if err := n.driveSeeding(ctx); err != nil {
			n.cl.report(err)
		}
	}
}

// driveSeeding runs one reconcile pass if the state moved, for every data member (plan
// §0.3): a joiner's first pass resumes its records or reconciles from its joining commit, as
// before; a member that is not joining runs OnStateSeq from its first pass, on the records
// LoadRecords loaded in Node.open (Reconciler.load refuses a second load, so OnBoot is not
// that pass and stays test-only); a witness holds no data and runs none. Until the first
// pass a reseed is delayed, not wrong: the machine's world is unknown and the horizon row
// answers nothing. The driver's context is passed into the pass, which checks it per
// database, so a closing node stops a pass between databases.
func (n *Node) driveSeeding(ctx context.Context) error {
	// First, before stateLoaded: a node that refused a committed entry holds a frozen state, and
	// a pass against it retires the pairs of every database that state lacks, which with
	// replication factor 1 are the only copy. stateLoaded refuses a diverged node too; this check does not rely on it.
	if n.raft.IsDiverged() {
		return nil
	}
	parts := n.cl.seeding.Load()
	seq := uint64(n.raft.StateSeq())
	if seq < n.seedFloor() {
		// A node admitted on an empty directory is still applying the cluster's history: the
		// state says what held before it was replaced or joined, not what it holds (BUG-0877).
		// A pass here would write every pair it was an active copy of ready and empty, and its
		// joining flag would say active. The apply that reaches the floor wakes the driver.
		return nil
	}
	if seq == 0 || !parts.stateLoaded(parts.loadedLatched() || n.raft.LoadedNow(), n.raft.IsDiverged()) {
		// The committed state is not loaded yet: after a restart the library re-applies the
		// log entry by entry and each apply wakes this driver, so a pass here would run against
		// an intermediate state in which a database does not exist yet and retire every pair
		// of it. The apply of the last entry wakes the driver again (clusterParts.onApply);
		// LoadedNow covers the boots on which no apply ever comes.
		return nil
	}
	parts.mu.Lock()
	asked := parts.repassAsked
	unchanged := seq == parts.lastSeq && !asked
	first := !parts.started
	// Taken before the pass: a request that arrives while the pass runs is for the next one.
	// A pass that fails gives it back below.
	parts.repassAsked = false
	parts.mu.Unlock()
	state := n.raft.State()
	self, found := reconcileMember(state, n.cfg.NodeID)
	if !found {
		return nil
	}
	parts.observe(state, n.cfg.NodeID)
	if unchanged {
		return nil
	}
	var err error
	switch seedPassFor(self, first, parts.recon.HasRecords()) {
	case seedPassNone:
		// A witness: nothing to reconcile, and nothing to re-derive on every tick either.
		parts.mu.Lock()
		parts.started, parts.lastSeq = true, seq
		parts.mu.Unlock()
		return nil
	case seedPassResumeJoining:
		err = parts.recon.ResumeJoining(ctx)
	case seedPassJoiningCommit:
		err = parts.recon.OnJoiningCommit(state)
	case seedPassStateSeq:
		err = parts.recon.OnStateSeq(ctx, state)
	}
	if err != nil {
		if asked {
			// A failed state_seq pass is run again because lastSeq stays behind. A pass that was
			// asked for over an unchanged state_seq has only the request, so it is put back: the
			// pass, or the dispatch that hands its actions to the sink, has not happened.
			parts.mu.Lock()
			parts.repassAsked = true
			parts.mu.Unlock()
		}
		return fmt.Errorf("reconciling the seed records at state_seq %d: %w", seq, err)
	}
	parts.mu.Lock()
	parts.started, parts.lastSeq = true, seq
	parts.mu.Unlock()
	parts.passes.Add(1)
	return nil
}

// noteAdmission records the state_seq of the member that admitted this node while it held no
// cluster state (serveIdentity). A repeated admission keeps the highest: a later ask from a member
// that is behind must not lower the floor back into the history.
func (c *clusterParts) noteAdmission(stateSeq uint64) {
	for {
		held := c.admittedAt.Load()
		if stateSeq <= held || c.admittedAt.CompareAndSwap(held, stateSeq) {
			return
		}
	}
}

// admitWithFloor ends the admission hold of an ask with the admit bit. On a node that holds no
// cluster state (fresh) it records the floor first and only then calls admit, which opens the Raft
// channel: the other order would let the leader's first entries apply, and the driver judge them,
// before the floor exists.
func admitWithFloor(fresh bool, askerSeq uint64, note func(uint64), admit func()) {
	if fresh {
		note(askerSeq)
	}
	admit()
}

// seedFloor is the lowest committed state_seq the seeding driver may judge pairs against: the
// state_seq of the member that admitted this node on an empty directory, 0 for every other node.
// It lives in memory only. A node that dies after its admission and before it applied that far
// restarts holding a part of the log, so it is not held for admission again and the floor is
// gone; its driver then waits only for that part (Raft.LoadedNow).
func (n *Node) seedFloor() uint64 { return n.cl.admittedAt.Load() }

// seedPass is which reconcile pass driveSeeding runs for a member on one wake.
type seedPass uint8

const (
	seedPassNone          seedPass = iota // no pass
	seedPassResumeJoining                 // a restarting joiner: Reconciler.ResumeJoining
	seedPassJoiningCommit                 // a joiner's first pass: Reconciler.OnJoiningCommit
	seedPassStateSeq                      // every other pass: Reconciler.OnStateSeq
)

// seedPassFor decides the pass for self, on its first wake or a later one, holding records
// or none: a witness holds no data (cluster/fsm.go keeps it out of every ranked set), a
// restarting joiner resumes (T3-2: never a forced reset), a joiner's first pass is its
// joining commit's, and every other member and every later pass is OnStateSeq.
func seedPassFor(self cluster.Member, first, hasRecords bool) seedPass {
	switch {
	case self.Witness:
		return seedPassNone
	case self.State != cluster.MemberJoining:
		return seedPassStateSeq
	case first && hasRecords:
		return seedPassResumeJoining
	case first:
		return seedPassJoiningCommit
	}
	return seedPassStateSeq
}

// ErrStillSeeding is what Activate returns for a joiner that has pairs left to pull.
var ErrStillSeeding = errors.New("the joiner is still seeding")

// seededFor checks that every pair a joiner will hold at its activation has its committed
// SEEDED bit, and returns the state_seq it checked at. The bits are the reporter's (seedreport.go):
// a pair's bit is set within about two seconds of its becoming ready, so a refused activation
// names how many pairs the joiner has reported and the caller asks again.
func (n *Node) seededFor(id string) (int64, error) {
	state := n.raft.State()
	member, found := reconcileMember(state, id)
	if !found || member.State != cluster.MemberJoining {
		return state.StateSeq, nil
	}
	if gap := activationGap(state, id); gap.Missing > 0 {
		return 0, fmt.Errorf("%w: %s has reported %d of the %d pairs it will hold as ready (database %q first). A pair it has pulled shows here within 2 seconds of being ready. Wait, then activate it again", ErrStillSeeding, id, gap.Total-gap.Missing, gap.Total, gap.Database)
	}
	return state.StateSeq, nil
}

func (s *seedParts) slotsView() SlotStates { return recordSlots{parts: s} }

// SeedStatus is the pull's status row for a pair that is still being pulled.
func (n *Node) SeedStatus(slot uint16, db uint64) (PullStatus, bool) {
	if n.cl == nil || n.cl.seeding.Load() == nil {
		return PullStatus{}, false
	}
	return n.cl.seeding.Load().puller.Status(slot, db)
}

// SlotRecordState is the state of the pair's seed record: absent when this node holds none.
func (n *Node) SlotRecordState(slot uint16, db uint64) machine.SlotState {
	if n.cl == nil || n.cl.seeding.Load() == nil {
		return machine.SlotAbsent
	}
	if record, found := n.cl.seeding.Load().recon.Record(slot, db); found {
		return machine.SlotState(record.State)
	}
	return machine.SlotAbsent
}

// SeedPasses is how many reconcile passes this node's seeding driver has finished. A pass
// runs once per committed state_seq change, so a test reads it to prove the driver does no
// work while the state holds still.
func (n *Node) SeedPasses() int64 {
	if n.cl == nil || n.cl.seeding.Load() == nil {
		return 0
	}
	return n.cl.seeding.Load().passes.Load()
}

// SeedingPairs is how many pairs the pull still has queued or running.
func (n *Node) SeedingPairs() int {
	if n.cl == nil || n.cl.seeding.Load() == nil {
		return 0
	}
	return n.cl.seeding.Load().puller.Pending()
}

// ReseedHooks are the seams the reseed leaves to later slices; a nil hook is skipped.
type ReseedHooks struct {
	// BeforeReset runs before the attempt is written, for a pair whose record is ready. A
	// non-nil error aborts the reseed: the record stays ready and nothing is written, and the
	// copy's next catch-up, answered the horizon again, retries (plan §1.2 step 3).
	BeforeReset func(ctx context.Context, slot uint16, db uint64) error
}

// nodeSeeder is the replicator's Seeder on a networked node: a pair that cannot be caught
// up is put back to seeding and pulled again.
type nodeSeeder struct{ node *Node }

// ResetAndSeed implements Seeder.
func (s nodeSeeder) ResetAndSeed(slot uint16, db uint64, cause ReseedCause) error {
	parts := s.node.cl.seeding.Load()
	if parts == nil {
		return fmt.Errorf("slot %d of database %d cannot be reseeded: the seeding of %s has not started yet. The owner ships the record again", slot, db, s.node.cfg.NodeID)
	}
	return parts.reseed(s.node.cl.ctx, slot, db, cause)
}

// ReseedPair puts a pair back to seeding from phase reset and queues the pull, as a
// catch-up answered LOG_BELOW_HORIZON does. It is exported because the harness lives in
// another package, for tests that hold a copy in the seeding state. A pair this node holds
// no record of is an error here, not the silent no-op the internal reseed keeps for the
// catch-up path: a caller that asks for a reseed by hand has a pair it believes it holds.
func (n *Node) ReseedPair(slot uint16, db uint64) error {
	if n.cl == nil || n.cl.seeding.Load() == nil {
		return errors.New("this node is not part of a cluster, so it has no seed to restart")
	}
	parts := n.cl.seeding.Load()
	if _, found := parts.recon.Record(slot, db); !found {
		return fmt.Errorf("slot %d of database %d has no seed record on this node, so there is nothing to reseed. Run 'status seeding' to see which pairs this node holds", slot, db)
	}
	return parts.reseed(n.cl.ctx, slot, db, ReseedHorizon)
}

// reseed is the whole of a reseed (plan §1.2): the hook first, while the record is still
// ready; then the horizon row, which writes the attempt and makes the pair not ready in one
// critical section; then the copy queue dropped and the pull queued. A pair with no record,
// or one the row leaves alone (seeding, retired, retiring), is a no-op: it has nothing to
// rebuild, and the call is not an error.
func (s *seedParts) reseed(ctx context.Context, slot uint16, db uint64, cause ReseedCause) error {
	record, found := s.recon.Record(slot, db)
	if !found {
		return nil
	}
	if record.State == machine.SeedReady && s.hooks.BeforeReset != nil {
		if err := s.hooks.BeforeReset(ctx, slot, db); err != nil {
			err = fmt.Errorf("reseeding slot %d of database %d (%s) was stopped before it began: %w. The copy asks again on its next catch-up", slot, db, cause, err)
			s.report(err)
			return err
		}
	}
	action, err := s.recon.Feed(slot, db, machine.LogBelowHorizon{})
	if err != nil {
		return fmt.Errorf("putting slot %d of database %d back to seeding: %w", slot, db, err)
	}
	if action.Do != machine.SlotRestartSeed {
		return nil
	}
	s.dropQueue(slot, db)
	s.puller.Queue(slot, db)
	return nil
}

// OwnerOfSlot is the owner of the pair at the committed epoch, for tests that compare a
// seeded copy with its source.
func (n *Node) OwnerOfSlot(slot uint16, db uint64) (string, bool) { return n.ownerOf(slot, db) }

// PositionOn asks node id for its position of the pair (POSITION_QUERY), the question the
// handoff and the activation gate ask; public because the harness lives in another package.
func (n *Node) PositionOn(ctx context.Context, id string, slot uint16, db uint64) (PositionReply, error) {
	if n.cl == nil {
		return PositionReply{}, errors.New("this node is not part of a cluster, so it has no peer to ask")
	}
	return n.cl.links.QueryPosition(ctx, id, PositionQuery{Slot: slot, DB: db})
}

// seedAwareAdvancer is the Writer's PairAdvancer on a networked node: every pair moves
// through the Reconciler, so its record is always a seed record that says where the pair is
// (seeding-completion plan §0.2; LocalAdvancer and its 32-byte record stay on a single node).
// A pair whose record is seeding, retired or retiring orders nothing and replays nothing; a
// pair with no record is the Reconciler's caller-dependent case (ApplyAdvance).
type seedAwareAdvancer struct {
	recon *Reconciler
}

// advanceBusyWait bounds how long a write waits for a reconcile pass's batch that holds its
// pair busy; advanceBusyPoll is how often it asks again. A pass commits a batch of at most
// reconcileBatchLimit records, milliseconds on a disk, so the bound is never reached unless
// the store is failing, when the pass's own error ends the wait sooner.
const (
	advanceBusyWait = 5 * time.Second
	advanceBusyPoll = 2 * time.Millisecond
)

// Advance implements PairAdvancer. A pair the reconciler has staged in a batch not yet
// committed (the first pass over a new database writes its interim this way) is busy, and
// the write waits the commit out instead of failing "retry in a moment": it holds no lock
// while it waits, and the pass's commit takes the pair lock only to install.
func (a seedAwareAdvancer) Advance(slot uint16, db uint64, step AdvanceStep) error {
	if record, found := a.recon.Record(slot, db); found && record.State != machine.SeedReady {
		return errNothingStaged
	}
	req := advanceRequestOf(step)
	ctx := step.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(advanceBusyWait)
	for {
		err := a.recon.ApplyAdvance(slot, db, req)
		if !errors.Is(err, ErrPairBusy) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for the reconcile pass that holds slot %d of database %d: %w", slot, db, ctx.Err())
		case <-time.After(advanceBusyPoll):
		}
	}
}

// AdvanceLocked implements LockedAdvancer for a copy's apply, which holds the pair's lock.
func (a seedAwareAdvancer) AdvanceLocked(pair *cluster.Pair, slot uint16, db uint64, step AdvanceStep) error {
	if record, found := a.recon.Record(slot, db); found && record.State != machine.SeedReady {
		return errNothingStaged
	}
	return a.recon.ApplyAdvanceLocked(slot, db, pair, advanceRequestOf(step))
}

// Recordless implements RecordlessReporter.
func (a seedAwareAdvancer) Recordless(slot uint16, db uint64) bool {
	return a.recon.Recordless(slot, db)
}

// PrepareLocked implements PipelineAdvancer for an owner write's first section, which holds the pair's
// lock: the checks Advance makes before its stage, without the wait for a busy pair, which the caller
// makes outside the lock. The floor hold of a pair with no cursor is taken here, once the checks passed:
// Reconciler.PrepareAdvanceLocked receives no pair (store-vouched-cut-build.md §2, review edit F7).
func (a seedAwareAdvancer) PrepareLocked(pair *cluster.Pair, slot uint16, db uint64) (bool, bool, error) {
	if record, found := a.recon.Record(slot, db); found && record.State != machine.SeedReady {
		return false, false, errNothingStaged
	}
	recordless, err := a.recon.PrepareAdvanceLocked(slot, db)
	if err != nil {
		return false, false, err
	}
	held := false
	if pair.TailCursor == 0 {
		held = a.recon.cfg.Floor.hold(pairKey{slot, db})
	}
	return recordless, held, nil
}

// AbandonLocked implements PipelineAdvancer.
func (a seedAwareAdvancer) AbandonLocked(slot uint16, db uint64) {
	a.recon.cfg.Floor.release(pairKey{slot, db})
}

// copyAdvancer is the Replicator's LockedAdvancer on a networked node. The Replicator is
// built before the seeding parts, so it reads them when it first applies; a record that
// arrives before they exist is not applied, and the owner ships it again.
type copyAdvancer struct {
	seeding *atomic.Pointer[seedParts]
}

// AdvanceLocked implements LockedAdvancer.
func (a copyAdvancer) AdvanceLocked(pair *cluster.Pair, slot uint16, db uint64, step AdvanceStep) error {
	parts := a.seeding.Load()
	if parts == nil {
		return errNothingStaged
	}
	return seedAwareAdvancer{recon: parts.recon}.AdvanceLocked(pair, slot, db, step)
}

func advanceRequestOf(step AdvanceStep) AdvanceRequest {
	return AdvanceRequest{
		Stage: func(batch storage.KVBatch, pair *cluster.Pair) (Advance, error) {
			to, err := step.Stage(batch, pair)
			return Advance(to), err
		},
		Commit:   step.Commit,
		Replay:   step.Replay,
		Pos:      step.Pos,
		Admitted: step.Admitted,
		Appended: step.Appended,
	}
}

// notReadyCause words why a pair in this state orders no write; "" for a ready pair.
func notReadyCause(state machine.SlotState) string {
	switch state {
	case machine.SlotReady:
		return ""
	case machine.SlotSeeding:
		return "still seeding"
	case machine.SlotRetired:
		return "retired (out of the copy set)"
	case machine.SlotRetiring:
		return "being deleted"
	}
	return "not seeded here yet"
}

// proposeAtSeq commits one command with a base_seq the caller chose, on the leader. Unlike
// propose it never stamps the command again after STATE_MOVED: a caller that decided on a
// state wants the FSM to refuse the command if the state has moved. A node that is not the
// leader says who is (the caller asks that node), and a leader change is waited out until
// bootTimeout.
func (n *Node) proposeAtSeq(ctx context.Context, kind byte, body []byte, baseSeq uint64) (cluster.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, bootTimeout)
	defer cancel()
	for {
		result, err := n.raft.ProposeContext(ctx, cluster.Command{Version: cluster.CommandVersion, Kind: kind, BaseSeq: baseSeq, Body: body})
		var notLeader *cluster.NotLeaderError
		if err == nil || !errors.As(err, &notLeader) || notLeader.Undecided {
			n.topology.Refresh()
			return result, err
		}
		select {
		case <-ctx.Done():
			return cluster.Result{}, fmt.Errorf("waiting for the node to lead its cluster: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
		if n.raft.Leader() != "" && !n.raft.IsLeader() {
			return cluster.Result{}, err // the leader is elsewhere: the caller asks it
		}
	}
}
