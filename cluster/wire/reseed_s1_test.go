package wire

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Seeding slice S1, steps 1 and 2: the trigger, the Seeder, what a reset clears and the hook
// that runs before it (the entry ring seeding completion spec §1.1 and §1.2).

// s1Events records what the reseed did and in which order.
type s1Events struct {
	mu       sync.Mutex
	order    []string
	reported []error
	dropped  []pairKey
	inDrop   func() // runs inside the dropQueue callback
}

func (e *s1Events) note(what string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.order = append(e.order, what)
}

func (e *s1Events) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.order...)
}

// s1Parts is the seedParts of a rig, as Node.newSeedParts builds it, with its queue drop and
// its error sink recorded.
func s1Parts(rig *t3Rig, events *s1Events) *seedParts {
	parts := &seedParts{recon: rig.rec, puller: rig.puller, wake: make(chan struct{}, 1)}
	parts.dropQueue = func(slot uint16, db uint64) {
		events.note("drop")
		events.mu.Lock()
		events.dropped = append(events.dropped, pairKey{slot, db})
		inDrop := events.inDrop
		events.mu.Unlock()
		if inDrop != nil {
			inDrop()
		}
	}
	parts.report = func(err error) {
		events.mu.Lock()
		events.reported = append(events.reported, err)
		events.mu.Unlock()
	}
	return parts
}

// s1ReadyRig is a rig whose pair is ready after one real pull from a source.
func s1ReadyRig(t *testing.T) *t3Rig {
	t.Helper()
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner = "x"
	rig.sources = map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 6, t3Cut(t, rig), 7)
	rig.pullUntil("the pair to be ready", rig.isReady)
	return rig
}

// TestADivergentTailResetsThePairThroughTheSameSeeder: the divergent paths reach the same
// reseed as the horizon (the Seeder is the node, not a stub that refuses), and the cause
// reaches it. [BREAK: leave seedStub in node_cluster.go]
func TestADivergentTailResetsThePairThroughTheSameSeeder(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	inserted := s0Insert(t, node, "u1")
	slot := s0SlotOf(t, "u1")
	if _, isNode := node.cl.repl.cfg.Seeds.(nodeSeeder); !isNode {
		t.Fatalf("the replicator's Seeder is %T, want the node", node.cl.repl.cfg.Seeds)
	}
	if err := node.cl.repl.cfg.Seeds.ResetAndSeed(slot, dbID, ReseedDivergent); err != nil {
		t.Fatalf("a divergent tail was refused by the Seeder: %v", err)
	}
	if state := node.SlotRecordState(slot, dbID); state != machine.SlotSeeding {
		t.Fatalf("after the reset the record of the pair written at %s is %v, want seeding", inserted.Pos, state)
	}
}

// TestAReseedOnAnActiveNodeFires is S0's precondition end to end through the Seeder: an
// active node's ready pair goes to seeding when the Seeder is called, because the first pass
// made its world known. [BREAK: restore the joining guard in Node.driveSeeding]
func TestAReseedOnAnActiveNodeFires(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	s0Insert(t, node, "u1")
	slot := s0SlotOf(t, "u1")
	if err := (nodeSeeder{node: node}).ResetAndSeed(slot, dbID, ReseedHorizon); err != nil {
		t.Fatal(err)
	}
	record, found := node.cl.seeding.Load().recon.Record(slot, dbID)
	if !found || record.State != machine.SeedSeeding || record.Phase != machine.PhaseReset {
		t.Fatalf("after the reseed the record is %+v (found %v), want seeding from phase reset", record, found)
	}
	if node.SeedingPairs() == 0 {
		t.Fatal("the reseed queued no pull")
	}
}

// TestAReseedIsANoOpForARecordThatIsNotReadyInTarget: the horizon row acts on ready only, so
// a seeding pair is not restarted, queues no second pull and is not an error. [BREAK: queue a
// pull for a seeding record too]
func TestAReseedIsANoOpForARecordThatIsNotReadyInTarget(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	before, found := rig.rec.Record(rig.slot, reconDB)
	if !found || before.State != machine.SeedSeeding {
		t.Fatalf("the rig's record is %+v (found %v), want seeding", before, found)
	}
	events := &s1Events{}
	parts := s1Parts(rig, events)
	if err := parts.reseed(context.Background(), rig.slot, reconDB, ReseedHorizon); err != nil {
		t.Fatal(err)
	}
	if rig.puller.Pending() != 0 || len(events.snapshot()) != 0 {
		t.Fatalf("a reseed of a seeding pair queued %d pulls and did %v, want nothing", rig.puller.Pending(), events.snapshot())
	}
	if after, _ := rig.rec.Record(rig.slot, reconDB); after.SeedID != before.SeedID {
		t.Fatalf("the seeding attempt changed from %x to %x", before.SeedID, after.SeedID)
	}
	if err := parts.reseed(context.Background(), rig.slot+1, reconDB+9, ReseedHorizon); err != nil || rig.puller.Pending() != 0 {
		t.Fatalf("a reseed of a pair with no record = %v with %d pulls queued, want a no-op", err, rig.puller.Pending())
	}
}

// TestAResetPairAnswersNotAppliedFromTheSameInstant: when the reseed reaches the point where
// it drops the queue, the pair already answers seeding, admits no write and has its applied
// position zeroed; there is no window in which it is ready while its attempt is written.
// [BREAK: mark the pair after dropping the queue]
func TestAResetPairAnswersNotAppliedFromTheSameInstant(t *testing.T) {
	rig := s1ReadyRig(t)
	events := &s1Events{}
	parts := s1Parts(rig, events)
	slots := recordSlots{parts: parts}
	if !slots.Admits(rig.slot, reconDB) {
		t.Fatal("the pair does not admit writes while ready")
	}
	events.inDrop = func() {
		if state := slots.State(rig.slot, reconDB); state != machine.SlotSeeding || slots.Admits(rig.slot, reconDB) {
			t.Errorf("at the queue drop the pair is %v and admits %v, want seeding and not admitting", state, slots.Admits(rig.slot, reconDB))
		}
		if applied := rig.pairs.Snapshot(rig.slot, reconDB).AppliedPos; !applied.Zero() {
			t.Errorf("at the queue drop the pair's applied position is %s, want 0:0", applied)
		}
	}
	if err := parts.reseed(context.Background(), rig.slot, reconDB, ReseedHorizon); err != nil {
		t.Fatal(err)
	}
	if got := events.snapshot(); len(got) != 1 {
		t.Fatalf("the queue was dropped %v, want once", got)
	}
}

// TestAResetDropsThePairsCopyQueue: the reseed drops the frames the copy had queued for the
// pair and only that pair's, and Replicator.DropCopyQueue returns their charge.
// [BREAK: leave the queue]
func TestAResetDropsThePairsCopyQueue(t *testing.T) {
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Window = 4 })
	for seq := uint64(1); seq <= 3; seq++ {
		adm, err := copyNode.rep.AdmitWrite(u32Frame(t, u32Epoch, u32Pos(u32Epoch, seq), cluster.Position{}))
		if err != nil || !adm.Admitted() {
			t.Fatalf("admitting %d: %v", seq, err)
		}
		if _, queued := copyNode.rep.EnqueueWrite(adm); !queued {
			t.Fatalf("frame %d was not queued", seq)
		}
	}
	copyNode.rep.copyQ.mu.Lock()
	depth, bytes := copyNode.rep.copyQ.pairs[pairKey{u32Slot, u32DB}].depth, copyNode.rep.copyQ.bytes
	copyNode.rep.copyQ.mu.Unlock()
	if depth != 3 || bytes == 0 {
		t.Fatalf("queued depth %d bytes %d, want 3 frames charged", depth, bytes)
	}
	copyNode.rep.DropCopyQueue(u32Slot, u32DB)
	copyNode.rep.copyQ.mu.Lock()
	defer copyNode.rep.copyQ.mu.Unlock()
	if len(copyNode.rep.copyQ.pairs) != 0 || len(copyNode.rep.copyQ.ready) != 0 || copyNode.rep.copyQ.bytes != 0 {
		t.Fatalf("after the drop: %d pairs, %d ready, %d bytes, want none", len(copyNode.rep.copyQ.pairs), len(copyNode.rep.copyQ.ready), copyNode.rep.copyQ.bytes)
	}
}

// TestAResetKeepsAWorkersFramesCharged: a pair a worker owns keeps its taken frames until
// finishPair gives them back; the drop empties the waiting ones only, so the byte count
// cannot go negative.
func TestAResetKeepsAWorkersFramesCharged(t *testing.T) {
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Window = 2 })
	for seq := uint64(1); seq <= 4; seq++ {
		adm, err := copyNode.rep.AdmitWrite(u32Frame(t, u32Epoch, u32Pos(u32Epoch, seq), cluster.Position{}))
		if err != nil || !adm.Admitted() {
			t.Fatalf("admitting %d: %v", seq, err)
		}
		copyNode.rep.EnqueueWrite(adm)
	}
	key, taken, ok := copyNode.rep.takeCopyWork()
	if !ok || len(taken) != 2 {
		t.Fatalf("a worker took %d frames (ok %v), want a window of 2", len(taken), ok)
	}
	copyNode.rep.DropCopyQueue(key.slot, key.db)
	copyNode.rep.finishPair(key, taken, true)
	copyNode.rep.copyQ.mu.Lock()
	defer copyNode.rep.copyQ.mu.Unlock()
	if copyNode.rep.copyQ.bytes != 0 || len(copyNode.rep.copyQ.pairs) != 0 {
		t.Fatalf("after the worker finished: %d bytes, %d pairs, want none", copyNode.rep.copyQ.bytes, len(copyNode.rep.copyQ.pairs))
	}
}

// TestTheResetHookRunsBeforeTheAttemptCommits: the hook sees the record still ready, the
// attempt is written after it. [BREAK: call it after the commit]
func TestTheResetHookRunsBeforeTheAttemptCommits(t *testing.T) {
	rig := s1ReadyRig(t)
	events := &s1Events{}
	parts := s1Parts(rig, events)
	parts.hooks.BeforeReset = func(_ context.Context, slot uint16, db uint64) error {
		record, found := rig.rec.Record(slot, db)
		if !found || record.State != machine.SeedReady {
			t.Errorf("the hook saw %+v (found %v), want the record still ready", record, found)
		}
		events.note("hook")
		return nil
	}
	if err := parts.reseed(context.Background(), rig.slot, reconDB, ReseedHorizon); err != nil {
		t.Fatal(err)
	}
	if got := events.snapshot(); len(got) != 2 || got[0] != "hook" || got[1] != "drop" {
		t.Fatalf("the reseed did %v, want the hook, then the drop", got)
	}
	if record, _ := rig.rec.Record(rig.slot, reconDB); record.State != machine.SeedSeeding {
		t.Fatalf("after the reseed the record is %v, want seeding", record.State)
	}
}

// TestAFailedResetHookAbortsTheReseed: a hook error stops the reseed before the attempt is
// written: the record is still ready, nothing was queued, the error was reported and
// returned. [BREAK: proceed on error]
func TestAFailedResetHookAbortsTheReseed(t *testing.T) {
	rig := s1ReadyRig(t)
	before, _ := rig.rec.Record(rig.slot, reconDB)
	events := &s1Events{}
	parts := s1Parts(rig, events)
	noLeader := errors.New("no leader answered inside the proposal budget")
	parts.hooks.BeforeReset = func(context.Context, uint16, uint64) error { return noLeader }
	err := parts.reseed(context.Background(), rig.slot, reconDB, ReseedHorizon)
	if !errors.Is(err, noLeader) {
		t.Fatalf("the reseed returned %v, want the hook's error", err)
	}
	after, _ := rig.rec.Record(rig.slot, reconDB)
	if after.State != machine.SeedReady || after.SeedID != before.SeedID || after.Applied != before.Applied {
		t.Fatalf("the record moved from %+v to %+v although the hook failed", before, after)
	}
	if rig.puller.Pending() != 0 || len(events.snapshot()) != 0 {
		t.Fatalf("a failed hook queued %d pulls and did %v, want nothing", rig.puller.Pending(), events.snapshot())
	}
	events.mu.Lock()
	reported := len(events.reported)
	events.mu.Unlock()
	if reported != 1 {
		t.Fatalf("%d errors were reported, want 1", reported)
	}
}

// TestADivergentTailAsksTheSeederWithItsCause: the divergent paths call the Seeder with
// ReseedDivergent, so the status row and the restart reason can tell the two causes apart.
// [BREAK: pass ReseedHorizon for a divergent chunk]
func TestADivergentTailAsksTheSeederWithItsCause(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.committed.Store(8)
	source.ownRun(8, 1, 3)
	copyNode := newU32Node(t, "copy", nil)
	copyNode.committed.Store(8)
	for seq := uint64(1); seq <= 4; seq++ {
		copyNode.write(u32Frame(t, 8, u32Pos(8, seq), cluster.Position{}))
	}
	copyNode.catchUpFrom(source, source)
	err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(8, 5), 1)
	if !errors.Is(err, ErrResetAndSeed) || copyNode.seeds.count() != 1 || copyNode.seeds.causes[0] != ReseedDivergent {
		t.Fatalf("err %v, resets %d, causes %v, want one reset with ReseedDivergent", err, copyNode.seeds.count(), copyNode.seeds.causes)
	}
}

// TestACatchUpRunningAcrossAResetAppliesNothing: a catch-up holds a fetched chunk when the
// pair's record turns to seeding; the chunk applies nothing, the pair's applied position does
// not move and the catch-up ends without marking the pair current. The check is in
// applyLocked, under the pair lock, so it covers every record of the chunk and not only the
// first. [BREAK: check readiness before the loop only (remove it from applyLocked)]
func TestACatchUpRunningAcrossAResetAppliesNothing(t *testing.T) {
	source := newU32Node(t, "source", func(cfg *ReplicatorConfig) { cfg.ChunkRecords = 4 })
	source.ownRun(u32Epoch, 1, 12)
	copyNode := newU32Node(t, "copy", nil)
	copyNode.catchUpFrom(source, source)
	var requests atomic.Int32
	copyNode.link.mu.Lock()
	copyNode.link.afterCatchUp = func(CatchUpRequest) {
		if requests.Add(1) == 2 {
			copyNode.ready.Store(false) // the reset turns the record to seeding under the second chunk
		}
	}
	copyNode.link.mu.Unlock()

	if err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, cluster.Position{}, 1); err != nil {
		t.Fatalf("a catch-up the reset replaced ended with %v, want a quiet end", err)
	}
	if applied := copyNode.applied(); applied != u32Pos(u32Epoch, 4) {
		t.Fatalf("the pair applied up to %s, want only the first chunk, 7:4", applied)
	}
	if copyNode.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatal("a catch-up that the reset replaced marked the pair current")
	}
	for seq := uint64(5); seq <= 12; seq++ {
		if copyNode.hasDocument(u32Pos(u32Epoch, seq)) {
			t.Fatalf("record 7:%d was applied into a range that is being reset", seq)
		}
	}
}

// s1Buffer records the frames offered to it.
type s1Buffer struct {
	mu     sync.Mutex
	frames []WriteFrame
}

func (b *s1Buffer) Offer(_ uint16, _ uint64, frame WriteFrame) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.frames = append(b.frames, frame)
	return true
}

// TestAdmitWriteOffersANonReadyFrameToTheBufferAndAcksNotApplied: the seam the live-write
// buffer fills. A frame for a pair that is not ready is offered whole and still answered not
// applied, and a ready pair offers nothing. [BREAK: drop the frame before offering it]
func TestAdmitWriteOffersANonReadyFrameToTheBufferAndAcksNotApplied(t *testing.T) {
	buffer := &s1Buffer{}
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Buffer = buffer })
	frame := u32Frame(t, u32Epoch, u32Pos(u32Epoch, 1), cluster.Position{})

	adm, err := copyNode.rep.AdmitWrite(frame)
	if err != nil || !adm.Admitted() || len(buffer.frames) != 0 {
		t.Fatalf("a ready pair: admitted %v, error %v, %d frames offered, want admitted and none offered", adm.Admitted(), err, len(buffer.frames))
	}
	copyNode.ready.Store(false)
	adm, err = copyNode.rep.AdmitWrite(frame)
	if err != nil || adm.Admitted() || adm.ack.Outcome != AckNotApplied {
		t.Fatalf("a pair that is not ready: admitted %v, ack %+v, error %v, want not applied", adm.Admitted(), adm.ack, err)
	}
	if len(buffer.frames) != 1 || buffer.frames[0].Pos != frame.Pos {
		t.Fatalf("the buffer was offered %d frames, want the frame at %s", len(buffer.frames), frame.Pos)
	}
}

// TestTheSweepsRerunOnceAfterReady: the node wires the puller's OnReady to the retire sweep's
// re-run for the pair, so keys a seed brought in from before a bump are swept. The re-run
// reads the names of the buckets the pair holds, which is the observable here. [BREAK: drop
// the OnReady wiring]
func TestTheSweepsRerunOnceAfterReady(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	s0Insert(t, node, "u1")
	slot := s0SlotOf(t, "u1")

	var nameReads atomic.Int32
	counting := NewRetireSweep()
	counting.Start(context.Background(), RetireDeps{
		KV: node.kv, Pairs: node.pairs, Manual: true, Declarations: node.fetcher,
		BucketName: func(context.Context, uint64, uint64) (string, bool, error) {
			nameReads.Add(1)
			return "", false, nil
		},
	})
	original := node.retire
	node.retire = counting // pairReady reads the field when it runs
	t.Cleanup(func() {
		counting.Close()
		node.retire = original // the node's Close stops the sweep it started
	})

	onReady := node.cl.seeding.Load().puller.cfg.OnReady
	if onReady == nil {
		t.Fatal("the puller has no OnReady")
	}
	onReady(slot, dbID)
	if nameReads.Load() == 0 {
		t.Fatal("a pair that became ready did not rerun the sweeps over its buckets")
	}
}

// TestACatchUpAnsweredBelowTheHorizonOverThePeerChannelResetsThePair: across the peer channel the
// source's refusal arrives as a wire error that carries the text and not the cluster code, so the
// copy recognises the answer by the text the source composes. The F7 harness test found it.
// [BREAK: recognise only the coded error]
func TestACatchUpAnsweredBelowTheHorizonOverThePeerChannelResetsThePair(t *testing.T) {
	source := newU32Node(t, "source", nil)
	horizon := source.rep.belowHorizon(CatchUpRequest{Slot: u32Slot, DB: u32DB, Node: "copy", From: u32Pos(u32Epoch, 3)}, u32Pos(u32Epoch, 90))
	var coded *cluster.Error
	if !errors.As(horizon, &coded) {
		t.Fatalf("the source's horizon answer is %T, want a coded error", horizon)
	}
	overTheWire := fmt.Errorf("asking source for a catch-up: %w", &transport.WireError{Code: transport.ErrInternal, Message: coded.Message})
	if !isBelowHorizon(overTheWire) {
		t.Fatalf("a horizon answer that crossed the peer channel as %q is not recognised", overTheWire)
	}
	for _, other := range []error{nil, errors.New("refused CATCHUP_REQ from n2 at epoch 7: this node is at epoch 6"), context.Canceled} {
		if isBelowHorizon(other) {
			t.Fatalf("%v is taken for a horizon answer", other)
		}
	}
}

// TestNoCatchUpSourceMarksCurrentUntilTheNextFrame pins a residual the plan records (§1.6): a copy
// above every reachable holder finds no catch-up source, so its catch-up marks the pair current
// and asks for no reset. The reset comes only when the owner's next pushed frame meets the
// divergent tail in applyLocked. It names today's behaviour and has no break.
func TestNoCatchUpSourceMarksCurrentUntilTheNextFrame(t *testing.T) {
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.CatchUpSource = func(uint16, uint64) (string, error) { return "", ErrNoCatchUpSource }
	})
	copyNode.committed.Store(8)
	for seq := uint64(1); seq <= 4; seq++ {
		copyNode.write(u32Frame(t, 8, u32Pos(8, seq), cluster.Position{}))
	}
	if err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, cluster.Position{}, 1); err != nil {
		t.Fatalf("a catch-up with no source answered %v, want nil", err)
	}
	if !copyNode.rep.IsCurrent(u32Slot, u32DB) || copyNode.seeds.count() != 0 {
		t.Fatalf("current %v, resets %d: a copy with no source is current and is not reset yet", copyNode.rep.IsCurrent(u32Slot, u32DB), copyNode.seeds.count())
	}

	// The owner of epoch 9 began at 8:2, so the copy's 8:3 and 8:4 are a tail nobody owns.
	copyNode.committed.Store(9)
	reply, err := copyNode.rep.OnWrite(u32Frame(t, 9, u32Pos(9, 1), u32Pos(8, 2)))
	if err != nil || reply.Ack.Outcome != AckRefused {
		t.Fatalf("the opener of the next epoch answered %+v (%v), want a refusal", reply.Ack, err)
	}
	if copyNode.seeds.count() != 1 || copyNode.seeds.causes[0] != ReseedDivergent {
		t.Fatalf("resets %d causes %v, want one divergent reset from the next frame", copyNode.seeds.count(), copyNode.seeds.causes)
	}
}
