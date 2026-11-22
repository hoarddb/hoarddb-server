package wire

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u30iAdvancer wraps the local advancer to see how the Writer calls it: whether the Writer
// holds the pair lock (a second With on the same pair would never return) and how often.
type u30iAdvancer struct {
	inner    PairAdvancer
	pairs    *cluster.PairSet
	calls    atomic.Int64
	lockHeld atomic.Bool
}

func (a *u30iAdvancer) Advance(slot uint16, db uint64, step AdvanceStep) error {
	a.calls.Add(1)
	reached := make(chan struct{})
	go func() {
		a.pairs.With(slot, db, func(*cluster.Pair) {})
		close(reached)
	}()
	select {
	case <-reached:
	case <-time.After(2 * time.Second):
		a.lockHeld.Store(true)
		return errors.New("the writer holds the pair lock while calling the advancer")
	}
	return a.inner.Advance(slot, db, step)
}

// u30iDeclareQueue adds the fifoStore field the claim tests push into to the declaration.
func u30iDeclareQueue(fx *fixture) {
	table := &fx.schema.decl.Fields
	table.Entries = append(table.Entries, machine.FieldEntry{ID: claim34Field, Type: "fifoStore", Name: "Queue"})
	table.NextFieldID = claim34Field + 1
}

func u30iFixtureWith(t *testing.T, advancer func(*fixture) PairAdvancer, wall func() int64) *fixture {
	t.Helper()
	fx := newFixture(t)
	u30iDeclareQueue(fx)
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, nil).WithFieldEngines(claim34Engines{engine: store.FieldFIFO}).WithResultCap(1024),
		Clock:   fixedClock{}, Lease: fx.lease, Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl,
		WCTimeout: time.Second, DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
		Advancer: advancer(fx), WallMillis: wall,
	})
	return fx
}

func TestWriterMovesThePairRecordThroughTheAdvancerWithoutHoldingTheLock(t *testing.T) {
	var advancer *u30iAdvancer
	fx := u30iFixtureWith(t, func(fx *fixture) PairAdvancer {
		advancer = &u30iAdvancer{inner: NewLocalAdvancer(fx.pairs, fx.kv), pairs: fx.pairs}
		return advancer
	}, nil)
	answer := fx.mustWrite(testSlot, "p1")
	if advancer.lockHeld.Load() || advancer.calls.Load() != 1 {
		t.Fatalf("advancer calls = %d, lock held by the writer = %v, want one call with the lock free", advancer.calls.Load(), advancer.lockHeld.Load())
	}
	pairs := &cluster.PairSet{}
	cursor, err := LoadLocalPairRecords(fx.kv, pairs, false)
	if err != nil {
		t.Fatalf("LoadLocalPairRecords: %v", err)
	}
	if got := pairs.Snapshot(testSlot, testDB); got.AppliedPos != answer.Pos || got.Ordinal != 1 || cursor != 1 {
		t.Fatalf("reloaded pair = %+v, lowest cursor %d, want applied %s, ordinal 1, cursor 1", got, cursor, answer.Pos)
	}
}

// TestRefusedWriteMovesNoPairRecord: a refusal stages nothing, so the record, the ordinal
// and the log stay where they were.
func TestRefusedWriteMovesNoPairRecord(t *testing.T) {
	fx := newFixture(t)
	fx.mustWrite(testSlot, "p1")
	before := fx.pairs.Snapshot(testSlot, testDB)
	if _, err := fx.write(testSlot, "p1"); codeOf(t, err) != cluster.CodeDocumentExists {
		t.Fatalf("second insert err = %v, want DOCUMENT_EXISTS", err)
	}
	if after := fx.pairs.Snapshot(testSlot, testDB); after.AppliedPos != before.AppliedPos || after.Ordinal != before.Ordinal {
		t.Fatalf("a refusal moved the pair from %+v to %+v", before, after)
	}
}

func u30iClaimRequest(fx *fixture, id string, request byte) WriteRequest {
	op := claim34Op(cluster.OpClaim)
	op.EncID, op.DeclVersion, op.SchemaVersionSeen, op.Count = u30iEncode(id), testSeen, testSeen, 1
	return WriteRequest{Slot: testSlot, DBID: testDB, Op: op, RequestID: [16]byte{0xC1, request}, Deadline: time.Second}
}

func u30iEncode(id string) []byte {
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		panic(err)
	}
	return encoded
}

func u30iLiveDocWithQueue(t *testing.T, fx *fixture, id string) {
	t.Helper()
	fx.mustWrite(testSlot, id)
	owner := storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, u30iEncode(id))
	ref := store.FieldRef{Owner: owner, FieldID: claim34Field, Engine: store.FieldFIFO}
	snap := fx.kv.NewSnapshot()
	defer snap.Discard()
	batch := fx.kv.NewBatch()
	var parent store.BucketState
	if _, err := store.FieldPush(store.FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, claim34Doc(t, "m1")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// TestClaimIsStampedWithTheOwnersClockAndANonceBeforeTheAppend: the log record is what a
// replay reads, so it must already carry the clock and the nonce (conv 9.1).
func TestClaimIsStampedWithTheOwnersClockAndANonceBeforeTheAppend(t *testing.T) {
	fx := u30iFixtureWith(t, func(fx *fixture) PairAdvancer { return NewLocalAdvancer(fx.pairs, fx.kv) }, func() int64 { return 424242 })
	u30iLiveDocWithQueue(t, fx, "p1")
	answer, err := fx.w.Write(context.Background(), u30iClaimRequest(fx, "p1", 1))
	if err != nil || len(answer.Result) != 24 {
		t.Fatalf("claim = %+v, %v, want a 24-byte lease result", answer, err)
	}
	entries := fx.logged()
	record, err := storage.DecodeWALRecord(entries[len(entries)-1].Op, entries[len(entries)-1].Data)
	if err != nil {
		t.Fatal(err)
	}
	op, err := cluster.DecodeOp(record.Body)
	if err != nil {
		t.Fatal(err)
	}
	if op.Class != cluster.OpClaim || op.Now != 424242 || op.Nonce == ([16]byte{}) {
		t.Fatalf("logged claim = class %#02x now %d nonce %x, want the owner's clock and a nonce", op.Class, op.Now, op.Nonce)
	}
}

// TestOversizeClaimResultIsRefusedBeforeAPositionIsTaken: a result that cannot fit its
// dedupe entry would fail the apply after the append on every replay (spec-patch 84).
func TestOversizeClaimResultIsRefusedBeforeAPositionIsTaken(t *testing.T) {
	fx := newFixture(t)
	u30iDeclareQueue(fx)
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, nil).WithFieldEngines(claim34Engines{engine: store.FieldFIFO}).WithResultCap(16),
		Clock:   fixedClock{}, Lease: fx.lease, Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl,
		WCTimeout: time.Second, DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 16},
	})
	u30iLiveDocWithQueue(t, fx, "p1")
	logged := len(fx.logged())
	_, err := fx.w.Write(context.Background(), u30iClaimRequest(fx, "p1", 2))
	if codeOf(t, err) != cluster.CodeResultTooLarge || !strings.Contains(err.Error(), "Nothing was leased") {
		t.Fatalf("claim err = %v, want RESULT_TOO_LARGE saying nothing was leased", err)
	}
	if got := len(fx.logged()); got != logged {
		t.Fatalf("the log grew from %d to %d entries on a refused claim", logged, got)
	}
	if pair := fx.pairs.Snapshot(testSlot, testDB); pair.Flags&flagReplayNeeded != 0 {
		t.Fatalf("the refused claim left the pair needing a replay: flags %#b", pair.Flags)
	}
}

func TestClaimOfAMissingDocumentNamesTheDocumentNotTheBucket(t *testing.T) {
	fx := u30iFixtureWith(t, func(fx *fixture) PairAdvancer { return NewLocalAdvancer(fx.pairs, fx.kv) }, nil)
	_, err := fx.w.Write(context.Background(), u30iClaimRequest(fx, "ghost", 3))
	if codeOf(t, err) != cluster.CodeNotFound || strings.Contains(err.Error(), "bucket") && !strings.Contains(err.Error(), "document") {
		t.Fatalf("err = %v, want NOT_FOUND about the document", err)
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("err = %v, want the missing document named with the fix", err)
	}
}

func TestStoreVerbThatDoesNotFitTheFieldIsRefusedBeforeAPosition(t *testing.T) {
	fx := u30iFixtureWith(t, func(fx *fixture) PairAdvancer { return NewLocalAdvancer(fx.pairs, fx.kv) }, nil)
	u30iLiveDocWithQueue(t, fx, "p1")
	req := u31PopRequest("p1", 4)
	req.Op.Fields[0].Verb = cluster.VerbTake
	logged := len(fx.logged())
	_, err := fx.w.Write(context.Background(), req)
	if codeOf(t, err) != cluster.CodeInvalidArgument || !strings.Contains(err.Error(), "take takes a hashStore or btreeStore field with {k}") {
		t.Fatalf("take err = %v, want INVALID_ARGUMENT saying which store types take a take", err)
	}
	if got := len(fx.logged()); got != logged {
		t.Fatalf("the log grew on a refused pop: %d -> %d", logged, got)
	}
}

func TestRefetchOfADroppingBucketIsBucketDropping(t *testing.T) {
	rig := u38NewRig(t, machine.CacheConfig{})
	rig.reader.put(t, "users", u38Decl(10, 1, machine.StateDropping))
	err := NewCacheSchema(rig.fetcher).Refetch(context.Background(), testDB, 10)
	if !errors.Is(err, ErrBucketDropping) || refetchCode(err) != cluster.CodeBucketDropping {
		t.Fatalf("Refetch of a dropping bucket = %v (code %v), want ErrBucketDropping and BUCKET_DROPPING", err, refetchCode(err))
	}
	if code := refetchCode(ErrBucketNotFound); code != cluster.CodeNotFound {
		t.Fatalf("an absent bucket refetches to %v, want NOT_FOUND", code)
	}
	if code := refetchCode(ErrDeclUnavailable); code != cluster.CodeDeclUnavailable {
		t.Fatalf("a failed read refetches to %v, want DECL_UNAVAILABLE", code)
	}
}

func TestPairOwnershipMarksOnlyUnmarkedPairs(t *testing.T) {
	pairs := &cluster.PairSet{}
	own := NewPairOwnership(pairs)
	own.Acquire(5, testDB)
	if flags := pairs.Snapshot(5, testDB).Flags; flags != flagReplayed|flagStopProved {
		t.Fatalf("an unmarked pair has flags %#b after Acquire, want replayed and stopProved", flags)
	}
	pairs.With(6, testDB, func(p *cluster.Pair) { p.Flags = flagAwaiting })
	own.Acquire(6, testDB)
	if flags := pairs.Snapshot(6, testDB).Flags; flags != flagAwaiting {
		t.Fatalf("a pair in a barrier has flags %#b after Acquire, want it left awaiting", flags)
	}
	pairs.With(7, testDB, func(p *cluster.Pair) { p.Flags = flagReplayNeeded })
	own.Acquire(7, testDB)
	if flags := pairs.Snapshot(7, testDB).Flags; flags != flagReplayNeeded {
		t.Fatalf("a pair owing a replay has flags %#b after Acquire, want it left alone", flags)
	}
	own.Recovered(7, testDB)
	if flags := pairs.Snapshot(7, testDB).Flags; flags != flagReplayed {
		t.Fatalf("a recovered pair has flags %#b, want replayed only", flags)
	}
}

type u30iTargets map[uint16]cluster.Position

func (t u30iTargets) CachedTarget(slot uint16, _ uint64) (cluster.Position, bool) {
	pos, ok := t[slot]
	return pos, ok
}

func TestTopologyTargetIsTheHandoffsCachedTarget(t *testing.T) {
	fx := newFixture(t)
	want := cluster.Position{Epoch: testEpoch, Seq: 9}
	topo := WithHandoffTarget(fx.topo, u30iTargets{testSlot: want})
	if got := topo.Target(testSlot, testDB); got != want {
		t.Fatalf("Target = %s, want the cached %s", got, want)
	}
	if got := topo.Target(testSlot+1, testDB); !got.Zero() {
		t.Fatalf("Target of a pair with no cached target = %s, want zero", got)
	}
	if topo.Committed() != testEpoch || topo.Self() != "n1" {
		t.Fatal("the wrapper must pass every other Topology method through")
	}
}

// TestTargetHoldsTheWriterUntilItIsApplied: the writer reads the target through the
// Topology, so a pair behind the cached target holds instead of ordering.
func TestTargetHoldsTheWriterUntilItIsApplied(t *testing.T) {
	fx := newFixture(t)
	fx.w = NewWriter(WriterConfig{
		Topology: WithHandoffTarget(fx.topo, u30iTargets{testSlot: {Epoch: testEpoch, Seq: 3}}), Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, nil), Clock: fixedClock{}, Lease: fx.lease, Dedupe: NewKVDedupeReader(fx.kv),
		Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second, DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	answer, err := fx.write(testSlot, "p1")
	if err != nil || !answer.Held {
		t.Fatalf("write behind the cached target = %+v, %v, want a hold", answer, err)
	}
}

func TestUpsertDeleteAndReinsertThroughTheWriter(t *testing.T) {
	fx := u30iFixtureWith(t, func(fx *fixture) PairAdvancer { return NewLocalAdvancer(fx.pairs, fx.kv) }, nil)
	fx.mustWrite(testSlot, "p1")
	upsert := fx.request(testSlot, "p1")
	upsert.Op.Class = cluster.OpUpsert
	upsert.RequestID = [16]byte{0xB1}
	if _, err := fx.w.Write(context.Background(), upsert); err != nil {
		t.Fatalf("upsert over a live document: %v", err)
	}
	del := WriteRequest{Slot: testSlot, DBID: testDB, RequestID: [16]byte{0xB2}, Op: cluster.Op{Class: cluster.OpDelete, BucketID: testBucket, DeclVersion: testSeen, SchemaVersionSeen: testSeen, EncID: u30iEncode("p1")}}
	if _, err := fx.w.Write(context.Background(), del); err != nil {
		t.Fatalf("delete: %v", err)
	}
	snap := fx.kv.NewSnapshot()
	state, err := ReadHeaderState(snap, DocumentKey{Ref: storage.BucketRef{Slot: testSlot, DatabaseID: testDB, BucketID: testBucket}, EncID: u30iEncode("p1")})
	snap.Discard()
	if err != nil || state != HeaderTombstone {
		t.Fatalf("header after delete = %v, %v, want a tombstone", state, err)
	}
	again := fx.request(testSlot, "p1")
	again.RequestID = [16]byte{0xB3}
	if _, err := fx.w.Write(context.Background(), again); err != nil {
		t.Fatalf("insert over the tombstone: %v", err)
	}
	if !fx.hasDocument(testSlot, "p1") {
		t.Fatal("the document is gone after the reinsert")
	}
}

// TestScannerAdmitsItsMaskThroughTheReadGate: a slot another node owns, and a slot this
// node owns but may not serve yet, both refuse the whole call: the first NOT_OWNER, the second a retryable UNAVAILABLE.
func TestScannerAdmitsItsMaskThroughTheReadGate(t *testing.T) {
	fx := newFixture(t)
	mine := fx.withCluster(2, machine.ConcernOne)
	var other uint16
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		if owner, ok := fx.topo.table.Owner(slot, 2); ok && owner.ID != "n1" {
			other = slot
			break
		}
	}
	gate := NewReadGate(ReadGateConfig{Topology: fx.topo, Pairs: fx.pairs, Clock: fixedClock{}, Lease: fx.lease, Schema: fx.schema, Slots: u30iAllReady{}})
	scanner := NewSlotScanner(SlotScannerConfig{KV: fx.kv, Topology: fx.topo, Schema: fx.schema, Gate: gate})
	list := func(slot uint16) error {
		var mask cluster.SlotMask
		mask.Set(slot)
		_, err := scanner.List(context.Background(), cluster.SlotListRequest{Epoch: testEpoch, DBID: testDB, BucketID: testBucket, Mask: mask, Want: 10, MaxBytes: 1 << 20})
		return err
	}
	if err := list(mine); err != nil {
		t.Fatalf("List of a slot this node serves: %v", err)
	}
	if err := list(other); codeOf(t, err) != cluster.CodeNotOwner {
		t.Fatalf("List of a slot another node owns = %v, want NOT_OWNER", err)
	}
	fx.setFlags(mine, flagReplayNeeded)
	if err := list(mine); codeOf(t, err) != cluster.CodeUnavailable {
		t.Fatalf("List of a slot that must replay first = %v, want a retryable UNAVAILABLE (BUG-0764)", err)
	}
}

// u30iAllReady reports every slot ready, for tests whose topology is a fixture with
// several members that a real node would have to seed from first.
type u30iAllReady struct{}

// State implements SlotStates.
func (u30iAllReady) Serves(uint16, uint64) bool { return true }

func (u30iAllReady) State(uint16, uint64) machine.SlotState { return machine.SlotReady }
