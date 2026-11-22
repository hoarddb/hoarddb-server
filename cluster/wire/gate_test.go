package wire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// gateAsks is a POSITION_QUERY fake: the members in answers reply, every other member is down.
type gateAsks struct {
	mu      sync.Mutex
	answers map[string]PositionReply
	calls   []string
	// onAsk runs once per query, before the answer; the epoch test moves the epoch here.
	onAsk func()
}

func (f *gateAsks) ask(_ context.Context, node string, _ PositionQuery) (PositionReply, error) {
	f.mu.Lock()
	f.calls = append(f.calls, node)
	reply, ok := f.answers[node]
	onAsk := f.onAsk
	f.mu.Unlock()
	if onAsk != nil {
		onAsk()
	}
	if !ok {
		return PositionReply{}, fmt.Errorf("%s is down", node)
	}
	return reply, nil
}

func (f *gateAsks) asked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// gateFixture is a booted node that holds a retired copy of an outside slot: its
// two copy members are the only ones the gate may ask, and it holds nothing.
type gateFixture struct {
	rig     *rig
	kv      *storage.BadgerKV
	slot    uint16
	fence   cluster.Position
	copies  []string // the other copy members, sorted
	epoch   uint64
	deletes atomic.Int64
}

func newGateFixture(t *testing.T) *gateFixture {
	t.Helper()
	return newGateFixtureAt(t, func(s slotsOf) uint16 { return s.outside })
}

// newGateFixtureAt is newGateFixture for the slot that pick chooses: the copyOnly slot is
// one the node still targets, which is how a retired pair comes back into the target set.
func newGateFixtureAt(t *testing.T, pick func(slotsOf) uint16) *gateFixture {
	t.Helper()
	st := threeNodes(t)
	slot := pick(findSlots(t, st))
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	})
	fence := cluster.Position{Epoch: uint64(st.Epoch), Seq: 30}
	rec := retiredRecord()
	rec.Applied = fence
	putRecord(t, kv, slot, rec)
	putFence(t, kv, slot, fence)
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	copies := gateCopies(st, testSelf, slot, reconDB)
	slices.Sort(copies)
	return &gateFixture{rig: g, kv: kv, slot: slot, fence: fence, copies: copies, epoch: uint64(st.Epoch)}
}

// copiesOf names the members that hold slot in st at RF 2, other than testSelf.
func copiesOf(t *testing.T, st cluster.ClusterState, slot uint16) []string {
	t.Helper()
	var ids []string
	for _, m := range reconcilePlacingTable(st, testSelf).Copies(slot, 2) {
		if m.ID != testSelf {
			ids = append(ids, m.ID)
		}
	}
	if len(ids) != 2 {
		t.Fatalf("slot %d has %d other copies, want 2", slot, len(ids))
	}
	slices.Sort(ids)
	return ids
}

// ready is a POSITION_REPLY from a ready copy that has applied the fence.
func (fx *gateFixture) ready() PositionReply {
	return PositionReply{Slot: fx.slot, DB: reconDB, State: PositionStateReady, AppliedPos: fx.fence}
}

// readyAnswers has both copies answer ready.
func (fx *gateFixture) readyAnswers() map[string]PositionReply {
	answers := make(map[string]PositionReply, len(fx.copies))
	for _, id := range fx.copies {
		answers[id] = fx.ready()
	}
	return answers
}

// gateClock is a clock the test moves by hand.
type gateClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *gateClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *gateClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// reporter is the fixture's seed reporter, sending through send on the clock the test moves.
func (fx *gateFixture) reporter(clock *gateClock, send func(ctx context.Context, kind byte, body []byte) error) *seedReporter {
	return newSeedReporter(seedReportDeps{
		Self:      testSelf,
		Committed: func() cluster.ClusterState { return fx.rig.state },
		Record: func(slot uint16, db uint64) (uint8, bool) {
			rec, ok := fx.rig.rec.Record(slot, db)
			return rec.State, ok
		},
		Records: fx.rig.rec.Records,
		Send:    send,
		OnError: func(error) {},
		Now:     clock.Now,
	})
}

// deps is the gate's node wired to the fixture, with the asks and the error sink the test names.
// Its reporter sends nothing anywhere, so a test that wants to see the RETIRE replaces Report.
func (fx *gateFixture) deps(asks *gateAsks, errs *[]error) gateDeps {
	noSend := func(context.Context, byte, []byte) error { return nil }
	return gateDeps{
		Report:    fx.reporter(&gateClock{now: time.Unix(1_700_000_000, 0)}, noSend),
		Queue:     func(uint16, uint64) {},
		Self:      testSelf,
		Recon:     fx.rig.rec,
		Committed: func() cluster.ClusterState { return fx.rig.state },
		Epoch:     func() uint64 { return fx.epoch },
		Ask:       asks.ask,
		Delete: func(context.Context, reconcileKey) error {
			fx.deletes.Add(1)
			return nil
		},
		OnError: func(err error) { *errs = append(*errs, err) },
	}
}

// answer is the machine's answer for the slot now: one reconcile pass over the
// committed state as it is, and the action the pass hands the sink for the slot.
func (fx *gateFixture) answer(t *testing.T) machine.SlotAction {
	t.Helper()
	fx.rig.sink.actions = nil
	if err := fx.rig.rec.OnStateSeq(t.Context(), fx.rig.state); err != nil {
		t.Fatalf("OnStateSeq: %v", err)
	}
	acts := fx.rig.sink.of(fx.slot)
	if len(acts) != 1 {
		t.Fatalf("the pass handed slot %d %d actions, want 1: %+v", fx.slot, len(acts), acts)
	}
	return acts[0].SlotAction
}

// requireRetiredAndNothingDeleted fails unless the record is still retired and no range was deleted.
func (fx *gateFixture) requireRetiredAndNothingDeleted(t *testing.T) {
	t.Helper()
	if rec, ok := diskRecord(t, fx.kv, fx.slot); !ok || rec.State != machine.SeedRetired {
		t.Fatalf("the record is %+v, want it still retired", rec)
	}
	if n := fx.deletes.Load(); n != 0 {
		t.Fatalf("the gate deleted %d ranges, want none", n)
	}
}

// bitFor commits node's SEEDED bit for slot.
func bitFor(t *testing.T, st *cluster.ClusterState, node string, slot uint16) {
	t.Helper()
	bits := make([]byte, machine.BitmapBytes)
	bits[slot/8] |= 1 << (slot % 8)
	apply(t, st, cluster.CommandSeeded, bsonDoc(bsonStr("node", node), bsonI64("db_id", int64(reconDB)), bsonBin("bits", bits)))
}

// fenceKeys lists every 's' reclaim record of the pair, in key order.
func fenceKeys(t *testing.T, kv *storage.BadgerKV, slot uint16) []cluster.Position {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	lower := storage.NodeKey(storage.NSReclaim, []byte{reconcileFenceKind}, reconcilePairBytes(reconcileKey{slot, reconDB}))
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: reconcilePrefixEnd(lower)})
	defer it.Close()
	var out []cluster.Position
	for it.Seek(nil); it.Valid(); it.Next() {
		pos, err := cluster.DecodePosition(it.Key()[len(lower):])
		if err != nil {
			t.Fatalf("decode fence key: %v", err)
		}
		out = append(out, pos)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("read fence keys: %v", err)
	}
	return out
}

func TestGateWaitsForTheNamedMember(t *testing.T) {
	fx := newGateFixture(t)
	silent := fx.copies[1]
	asks := &gateAsks{answers: map[string]PositionReply{fx.copies[0]: fx.ready()}}
	var errs []error
	if err := newGateDriver(fx.deps(asks, &errs)).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	act := fx.answer(t)
	if act.Do != machine.SlotGate || !slices.Equal(act.Waiting, []string{silent}) {
		t.Fatalf("after the pass slot %d answers %+v, want the gate waiting for %s alone", fx.slot, act, silent)
	}
	fx.requireRetiredAndNothingDeleted(t)
}

func TestAClaimedPositionNeverOpensTheGate(t *testing.T) {
	fx := newGateFixture(t)
	for _, id := range fx.copies {
		if _, err := fx.rig.rec.Feed(fx.slot, reconDB, machine.PositionClaimed{Node: id, From: testSelf, Applied: fx.fence}); err != nil {
			t.Fatalf("feed a claim of %s: %v", id, err)
		}
	}
	var errs []error
	if err := newGateDriver(fx.deps(&gateAsks{}, &errs)).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	act := fx.answer(t)
	if act.Do != machine.SlotGate || !slices.Equal(act.Waiting, fx.copies) {
		t.Fatalf("after two claims and a pass slot %d answers %+v, want the gate still waiting for both copies", fx.slot, act)
	}
	fx.requireRetiredAndNothingDeleted(t)
}

func TestAGateNeverPassesOnABit(t *testing.T) {
	fx := newGateFixture(t)
	for _, id := range fx.copies {
		bitFor(t, &fx.rig.state, id, fx.slot)
	}
	var errs []error
	if err := newGateDriver(fx.deps(&gateAsks{}, &errs)).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	act := fx.answer(t)
	if act.Do != machine.SlotGate || !slices.Equal(act.Waiting, fx.copies) {
		t.Fatalf("with both committed bits set and nobody answering, slot %d answers %+v, want the gate still waiting for both copies", fx.slot, act)
	}
	fx.requireRetiredAndNothingDeleted(t)
}

func TestTheGateDriverSkipsARecordWhoseDatabaseIsGone(t *testing.T) {
	fx := newGateFixture(t)
	gone := fx.rig.state
	gone.Databases = nil
	asks := &gateAsks{answers: fx.readyAnswers()}
	var errs []error
	var epochReads atomic.Int64
	deps := fx.deps(asks, &errs)
	deps.Committed = func() cluster.ClusterState { return gone }
	deps.Epoch = func() uint64 {
		epochReads.Add(1)
		return fx.epoch
	}
	if err := newGateDriver(deps).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := asks.asked(); n != 0 {
		t.Fatalf("the gate asked %d members about a database that is gone, want none", n)
	}
	if n := epochReads.Load(); n != 0 {
		t.Fatalf("the gate read the epoch %d times for a database that is gone, want none", n)
	}
	fx.requireRetiredAndNothingDeleted(t)
}

func TestFencePositionHasOneWriter(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).outside
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	})
	fence := cluster.Position{Epoch: uint64(st.Epoch), Seq: 30}
	rec := retiredRecord()
	rec.Applied = fence
	putRecord(t, kv, slot, rec)
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	written := fenceKeys(t, kv, slot)
	if !slices.Equal(written, []cluster.Position{fence}) {
		t.Fatalf("boot wrote fences %v, want exactly %v from the reconciler", written, fence)
	}

	fx := &gateFixture{rig: g, kv: kv, slot: slot, fence: fence, copies: copiesOf(t, st, slot), epoch: uint64(st.Epoch)}
	var errs []error
	if err := newGateDriver(fx.deps(&gateAsks{answers: fx.readyAnswers()}, &errs)).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if got := fenceKeys(t, kv, slot); !slices.Equal(got, written) {
		t.Fatalf("after the pass the fences are %v, want the reconciler's %v alone", got, written)
	}
}

func TestAnEpochMoveDuringTheAsksDropsTheAnswers(t *testing.T) {
	fx := newGateFixture(t)
	var epoch atomic.Uint64
	epoch.Store(fx.epoch)
	asks := &gateAsks{answers: fx.readyAnswers(), onAsk: func() { epoch.Add(1) }}
	var errs []error
	deps := fx.deps(asks, &errs)
	deps.Epoch = epoch.Load
	if err := newGateDriver(deps).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	act := fx.answer(t)
	if act.Do != machine.SlotGate || !slices.Equal(act.Waiting, fx.copies) {
		t.Fatalf("answers from a pass whose epoch moved opened the gate: slot %d answers %+v", fx.slot, act)
	}
	fx.requireRetiredAndNothingDeleted(t)
}

// retiringAt reads the record of the fixture's slot from the disk, not from memory.
func (fx *gateFixture) retiringAt(t *testing.T) uint8 {
	t.Helper()
	rec, ok := diskRecord(t, fx.kv, fx.slot)
	if !ok {
		return 0
	}
	return rec.State
}

// TestRetiringIsDurableBeforeTheFirstDelete: the delete finds the record already
// retiring on the disk, so a crash inside it leaves a record that finishes the delete.
// [BREAK: feed RetireStarted after Delete]
func TestRetiringIsDurableBeforeTheFirstDelete(t *testing.T) {
	fx := newGateFixture(t)
	var errs []error
	deps := fx.deps(&gateAsks{answers: fx.readyAnswers()}, &errs)
	var atDelete uint8
	deps.Delete = func(context.Context, reconcileKey) error {
		atDelete = fx.retiringAt(t)
		fx.deletes.Add(1)
		return nil
	}
	if err := newGateDriver(deps).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := fx.deletes.Load(); n != 1 {
		t.Fatalf("the gate deleted %d ranges, want 1 (errors: %v)", n, errs)
	}
	if atDelete != machine.SeedRetiring {
		t.Fatalf("the record on disk was state %d when the delete began, want %d (retiring)", atDelete, machine.SeedRetiring)
	}
}

// TestRetireClearsTheBitBeforeTheRecordGoes: the record stays while the RETIRE is in
// flight and goes on the pass after it committed.
// [BREAK: feed RetireCommitted right after RetireDeleted]
func TestRetireClearsTheBitBeforeTheRecordGoes(t *testing.T) {
	fx := newGateFixture(t)
	clock := &gateClock{now: time.Unix(1_700_000_000, 0)}
	sending, release := make(chan byte, 1), make(chan struct{})
	report := fx.reporter(clock, func(_ context.Context, kind byte, _ []byte) error {
		sending <- kind
		<-release
		return nil
	})
	var errs []error
	deps := fx.deps(&gateAsks{answers: fx.readyAnswers()}, &errs)
	deps.Report = report
	gate := newGateDriver(deps)
	if err := gate.Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := fx.deletes.Load(); n != 1 {
		t.Fatalf("the gate deleted %d ranges, want 1 (errors: %v)", n, errs)
	}

	clock.advance(reportInterval)
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		report.Flush(t.Context())
	}()
	if kind := <-sending; kind != cluster.CommandRetire {
		t.Fatalf("the reporter sent command %d, want RETIRE (%d)", kind, cluster.CommandRetire)
	}
	if err := gate.Pass(t.Context()); err != nil {
		t.Fatalf("Pass while the RETIRE is in flight: %v", err)
	}
	if state := fx.retiringAt(t); state != machine.SeedRetired {
		t.Fatalf("with the RETIRE in flight the record is state %d, want it kept as retired (%d)", state, machine.SeedRetired)
	}

	close(release)
	<-flushed
	if err := gate.Pass(t.Context()); err != nil {
		t.Fatalf("Pass after the RETIRE committed: %v", err)
	}
	if _, held := fx.rig.rec.Record(fx.slot, reconDB); held {
		t.Fatal("the record is still held after the RETIRE committed")
	}
	if state := fx.retiringAt(t); state != 0 {
		t.Fatalf("the record on disk is state %d after the RETIRE committed, want it gone", state)
	}
}

// TestACrashBetweenRetireAndDeleteReReadsTheS: a node that comes back to a retiring
// record finishes the delete from the disk alone, with no evidence and no fence of its own.
// [BREAK: treat a retiring record as a retired one, which needs the evidence and the fence in memory]
func TestACrashBetweenRetireAndDeleteReReadsTheS(t *testing.T) {
	fx := newGateFixture(t)
	var errs []error
	crash := fx.deps(&gateAsks{answers: fx.readyAnswers()}, &errs)
	crash.Delete = func(context.Context, reconcileKey) error { return errors.New("the node died") }
	if err := newGateDriver(crash).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if state := fx.retiringAt(t); state != machine.SeedRetiring {
		t.Fatalf("after the crash the record on disk is state %d, want retiring (%d); errors: %v", state, machine.SeedRetiring, errs)
	}
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), fmt.Sprintf("slot %d of database %d", fx.slot, reconDB)) {
		t.Fatalf("the failed delete reported %v, want one error naming the slot and database", errs)
	}

	st := fx.rig.state
	restarted := newRig(t, fx.kv, st, testSelf, true)
	restarted.boot(t)
	reborn := &gateFixture{rig: restarted, kv: fx.kv, slot: fx.slot, fence: fx.fence, copies: fx.copies, epoch: fx.epoch}
	asks := &gateAsks{}
	errs = nil
	if err := newGateDriver(reborn.deps(asks, &errs)).Pass(t.Context()); err != nil {
		t.Fatalf("Pass after the restart: %v", err)
	}
	if n := reborn.deletes.Load(); n != 1 {
		t.Fatalf("the restarted gate deleted %d ranges, want 1 (errors: %v)", n, errs)
	}
	if n := asks.asked(); n != 0 {
		t.Fatalf("the restarted gate asked %d members for evidence it does not need, want none", n)
	}
	if got := fenceKeys(t, fx.kv, fx.slot); !slices.Equal(got, []cluster.Position{fx.fence}) {
		t.Fatalf("the fences are %v after the restart, want the disk's %v alone", got, fx.fence)
	}
}

// TestAReturnedSlotReseedsAndNeverCancelsTheRetire: a retired pair that is back in the
// target set is deleted, seeds again, and no RETIRE is noted for it.
// [BREAK: skip Delete for an in-target pair]
func TestAReturnedSlotReseedsAndNeverCancelsTheRetire(t *testing.T) {
	fx := newGateFixtureAt(t, func(s slotsOf) uint16 { return s.copyOnly })
	clock := &gateClock{now: time.Unix(1_700_000_000, 0)}
	var retires atomic.Int64
	report := fx.reporter(clock, func(context.Context, byte, []byte) error {
		retires.Add(1)
		return nil
	})
	var errs []error
	deps := fx.deps(&gateAsks{answers: fx.readyAnswers()}, &errs)
	deps.Report = report
	var queued []reconcileKey
	deps.Queue = func(slot uint16, db uint64) { queued = append(queued, reconcileKey{slot, db}) }
	if err := newGateDriver(deps).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := fx.deletes.Load(); n != 1 {
		t.Fatalf("the gate deleted %d ranges of a returned slot, want 1 (errors: %v)", n, errs)
	}
	if state := fx.retiringAt(t); state != machine.SeedSeeding {
		t.Fatalf("the returned slot's record is state %d, want seeding (%d)", state, machine.SeedSeeding)
	}
	if want := []reconcileKey{{fx.slot, reconDB}}; !slices.Equal(queued, want) {
		t.Fatalf("Queue was called for %v, want exactly %v", queued, want)
	}
	clock.advance(reportInterval)
	report.Flush(t.Context())
	if n := retires.Load(); n != 0 {
		t.Fatalf("%d RETIREs were sent for a pair that seeds again, want none", n)
	}
}

// TestTheGateDeleteIsChargedToTheReclaimBudget: the gate's delete goes through the
// puller's reset, so 3,000 keys against a 1,000-key credit wait for the other 2,000.
// [BREAK: Delete a fake that removes the keys without charging the budget]
func TestTheGateDeleteIsChargedToTheReclaimBudget(t *testing.T) {
	fx := newGateFixture(t)
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	puller := NewPuller(PullerConfig{Self: testSelf, Recon: fx.rig.rec, KV: fx.kv, Budget: s1Budget(clock, 1000, 1000)})
	s1FillRange(t, fx.kv, fx.slot, reconDB, 3000)
	var errs []error
	deps := fx.deps(&gateAsks{answers: fx.readyAnswers()}, &errs)
	deps.Delete = puller.resetRange
	if err := newGateDriver(deps).Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if left := s1CountRange(t, fx.kv, fx.slot, reconDB); left != 0 {
		t.Fatalf("%d keys survive the gate's delete (errors: %v)", left, errs)
	}
	if got := clock.total(); got < 1900*time.Millisecond || got > 2100*time.Millisecond {
		t.Fatalf("the gate's delete waited %v for 3,000 keys at 1,000 per second with a 1,000-key credit, want about 2s", got)
	}
}
