package wire

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	testEpoch  uint64 = 7
	testDB     uint64 = 3
	testBucket uint64 = 41
	testSlot   uint16 = 5
	testSeen   int64  = 7
)

var errDiskOnFire = errors.New("disk on fire")

// fakeTopology is committed state the tests move by hand.
type fakeTopology struct {
	mu        sync.Mutex
	committed uint64
	self      string
	members   int
	table     *cluster.SlotTable
	dbRow     DatabaseInfo
	target    cluster.Position
	// stepDownMs is the committed step_down_ms the lease refusal prints.
	stepDownMs int64
	// beforeTarget runs at the start of every Target call; see onTarget.
	beforeTarget func()
	// beforeTable runs at the start of every Table call; see onTable.
	beforeTable func()
}

func (f *fakeTopology) Committed() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.committed
}
func (f *fakeTopology) Self() string { return f.self }

// StepDownMs implements StepDownReporter.
func (f *fakeTopology) StepDownMs() int64 { return f.stepDownMs }
func (f *fakeTopology) Members() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.members
}
func (f *fakeTopology) Table() *cluster.SlotTable {
	f.mu.Lock()
	hook := f.beforeTable
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return f.table
}

// onTable makes every later Table call run hook first. The writer reads the table for each owner check;
// calledFrom tells which of them a hook is in, so a test can move the epoch at the read the plan stage
// does, between the writer's read of the committed epoch and its assignment.
func (f *fakeTopology) onTable(hook func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforeTable = hook
}

// calledFrom reports whether a function whose name contains name is on the calling goroutine's stack.
func calledFrom(name string) bool {
	pcs := make([]uintptr, 32)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, name) {
			return true
		}
		if !more {
			return false
		}
	}
}

func (f *fakeTopology) Database(db uint64) (DatabaseInfo, bool) {
	return f.dbRow, db == testDB
}
func (f *fakeTopology) Target(uint16, uint64) cluster.Position {
	f.mu.Lock()
	hook := f.beforeTarget
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.target
}

// onTarget makes every later Target call run hook first, which is how a test moves the epoch exactly
// between the writer's read of the committed epoch and its assignment.
func (f *fakeTopology) onTarget(hook func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforeTarget = hook
}

func (f *fakeTopology) setCommitted(epoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = epoch
}
func (f *fakeTopology) setTarget(pos cluster.Position) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.target = pos
}
func (f *fakeTopology) setMembers(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members = n
}

// fakeSchema is the declaration cache. beforeView runs at the start of View call n, so a
// test can commit a bump exactly between the two evaluations of the fence.
type fakeSchema struct {
	mu         sync.Mutex
	decl       machine.Declaration
	seen       int64
	published  int64
	views      int
	refetches  int
	staleAfter bool // a refetch leaves seen where it was
	name       string
	beforeView func(n int, s *fakeSchema)
}

func (f *fakeSchema) View(db, bucketID uint64) SchemaView {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.views++
	if f.beforeView != nil {
		f.beforeView(f.views, f)
	}
	return SchemaView{Present: true, Decl: f.decl, Name: f.name, Seen: f.seen, Published: f.published}
}

func (f *fakeSchema) Refetch(context.Context, uint64, uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refetches++
	if !f.staleAfter {
		f.seen = f.published
	}
	return nil
}

func (f *fakeSchema) viewCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.views
}

func (f *fakeSchema) refetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refetches
}

type fakeLease struct {
	valid atomic.Bool
	// decision, when set, is the whole decision Serve returns: a lapse with its cause and time.
	decision atomic.Pointer[machine.LeaseDecision]
	// afterFirst, when set, replaces decision once Serve has returned it.
	afterFirst atomic.Pointer[machine.LeaseDecision]
	// marked is the highest epoch the owner or read path told the lease about (BehindMarker).
	marked atomic.Uint64
	// quorumLost makes the fake a QuorumReporter that reports no voter majority.
	quorumLost atomic.Bool
}

// QuorumLost implements QuorumReporter.
func (f *fakeLease) QuorumLost() bool { return f.quorumLost.Load() }

// MarkBehind implements BehindMarker.
func (f *fakeLease) MarkBehind(epoch uint64, _ bool) { f.marked.Store(epoch) }

// BehindEpoch implements BehindReporter: the fake is behind while its mark is above the epoch.
func (f *fakeLease) BehindEpoch(committed uint64) (uint64, bool) {
	marked := f.marked.Load()
	return marked, marked > committed
}

func (f *fakeLease) Serve(machine.ServeCheck) machine.LeaseDecision {
	if d := f.decision.Load(); d != nil {
		if next := f.afterFirst.Swap(nil); next != nil {
			f.decision.Store(next)
		}
		return *d
	}
	return machine.LeaseDecision{Valid: f.valid.Load()}
}

type fixedClock struct{}

func (fixedClock) BootNow() time.Duration { return time.Second }

type fakeDedupe struct {
	mu      sync.Mutex
	entries map[machine.DedupeKey]*machine.DedupeEntry
}

func (f *fakeDedupe) Lookup(_ uint16, _, _ uint64, key machine.DedupeKey) (*machine.DedupeEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entries[key], nil
}

// fakeRepl stands in for the replication stream: acks is how many copies confirm.
type fakeRepl struct {
	mu      sync.Mutex
	queued  []cluster.Position
	cursors []uint64
	acks    int
	covered bool
	// blockAwait makes Await wait for its context to end, as a wait on a copy that never answers
	// does; awaitDelay makes it wait that long (or for the context) and then return copies, as a
	// slow copy that does answer in time. noted records the timeouts the writer reported.
	blockAwait bool
	awaitDelay time.Duration
	noted      []cluster.Position
}

func (f *fakeRepl) Enqueue(_ uint16, _ uint64, pos, _ cluster.Position, cursor uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queued = append(f.queued, pos)
	f.cursors = append(f.cursors, cursor)
}

func (f *fakeRepl) Await(ctx context.Context, _ uint16, _ uint64, _ cluster.Position, copies int) int {
	f.mu.Lock()
	block, delay, acks := f.blockAwait, f.awaitDelay, f.acks
	f.mu.Unlock()
	switch {
	case block:
		<-ctx.Done()
	case delay > 0:
		select {
		case <-time.After(delay):
			return copies
		case <-ctx.Done():
		}
	}
	return min(acks, copies)
}

func (f *fakeRepl) NoteWaitTimeout(_ uint16, _ uint64, pos cluster.Position, _ time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noted = append(f.noted, pos)
}

func (f *fakeRepl) notedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.noted)
}

func (f *fakeRepl) Acked(uint16, uint64, cluster.Position) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acks
}

func (f *fakeRepl) Covered(uint16, uint64, cluster.Position) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.covered
}

func (f *fakeRepl) queuedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queued)
}

// failKV is the real Badger store whose next batches can be made to fail at commit.
type failKV struct {
	*storage.BadgerKV
	failing atomic.Bool
	// syncs counts every way a caller can ask Badger for durability: a commit with sync
	// on, or a Sync call.
	syncs atomic.Int64
}

type failBatch struct{ storage.KVBatch }

// syncWatch counts a commit that asks for sync.
type syncWatch struct {
	storage.KVBatch
	syncs *atomic.Int64
}

func (b syncWatch) Commit(sync bool) error {
	if sync {
		b.syncs.Add(1)
	}
	return b.KVBatch.Commit(sync)
}

func (f *failKV) Sync() error {
	f.syncs.Add(1)
	return f.BadgerKV.Sync()
}

func (b failBatch) Commit(bool) error {
	b.Discard()
	return errDiskOnFire
}

func (f *failKV) NewBatch() storage.KVBatch {
	if f.failing.Load() {
		return failBatch{f.BadgerKV.NewBatch()}
	}
	return syncWatch{KVBatch: f.BadgerKV.NewBatch(), syncs: &f.syncs}
}

// failLog is the real WAL whose appends can be made to fail.
type failLog struct {
	*storage.WAL
	failing atomic.Bool
	// unsyncedAppends and syncCalls count the split append of the owner's RF 1 write path: one
	// AppendUnsyncedFrom and one SyncThrough per write, whose flush the group commit shares.
	unsyncedAppends, syncCalls atomic.Int64
	// refuseUnsynced, when set, is what the next AppendUnsyncedFrom returns instead of appending: a
	// refusal that poisons nothing and writes nothing.
	refuseUnsynced atomic.Pointer[error]
	// syncOverride, when set, runs in place of SyncThrough for the cursor; handled false falls through
	// to the real call. syncedFloor, when set, is what SyncedSequence reports.
	syncOverride atomic.Pointer[func(seq uint64) (handled bool, err error)]
	syncedFloor  atomic.Pointer[uint64]
}

// SyncThrough counts a write's sync request and lets a test replace its result.
func (f *failLog) SyncThrough(seq uint64) error {
	f.syncCalls.Add(1)
	if override := f.syncOverride.Load(); override != nil {
		if handled, err := (*override)(seq); handled {
			return err
		}
	}
	return f.WAL.SyncThrough(seq)
}

// SyncedSequence reports the test's floor when one is set.
func (f *failLog) SyncedSequence() uint64 {
	if floor := f.syncedFloor.Load(); floor != nil {
		return *floor
	}
	return f.WAL.SyncedSequence()
}

// AppendUnsyncedFrom is the owner's RF 1 append; it fails like AppendFrom while failing is set, or with
// the refusal a test armed.
func (f *failLog) AppendUnsyncedFrom(bucket string, op byte, key, payload []byte, origin storage.WALOrigin) (uint64, error) {
	f.unsyncedAppends.Add(1)
	if f.failing.Load() {
		return 0, errDiskOnFire
	}
	if refusal := f.refuseUnsynced.Load(); refusal != nil {
		return 0, *refusal
	}
	return f.WAL.AppendUnsyncedFrom(bucket, op, key, payload, origin)
}

func (f *failLog) AppendFrom(bucket string, op byte, key, payload []byte, origin storage.WALOrigin) (uint64, error) {
	if f.failing.Load() {
		return 0, errDiskOnFire
	}
	return f.WAL.AppendFrom(bucket, op, key, payload, origin)
}

// pairRecorder writes a small stand-in for the conv §12 pair record into the batch.
type pairRecorder struct{}

func pairRecordKey(slot uint16, db uint64) []byte {
	var raw [10]byte
	binary.BigEndian.PutUint16(raw[:2], slot)
	binary.BigEndian.PutUint64(raw[2:], db)
	return storage.NodeKey(storage.NSSeedState, raw[:])
}

func (pairRecorder) RecordApplied(batch storage.KVBatch, rec AppliedRecord) error {
	var value [32]byte
	binary.BigEndian.PutUint64(value[0:], rec.Pos.Epoch)
	binary.BigEndian.PutUint64(value[8:], rec.Pos.Seq)
	binary.BigEndian.PutUint64(value[16:], rec.Cursor)
	binary.BigEndian.PutUint64(value[24:], rec.Ordinal)
	return batch.Set(pairRecordKey(rec.Slot, rec.DB), value[:])
}

type fixture struct {
	t      *testing.T
	wal    *failLog
	kv     *failKV
	pairs  *cluster.PairSet
	topo   *fakeTopology
	schema *fakeSchema
	lease  *fakeLease
	repl   *fakeRepl
	dedupe *fakeDedupe
	w      *Writer
	fsyncs atomic.Int64
	// wcTimeout is the Writer's wc_timeout; zero takes one second.
	wcTimeout time.Duration
}

func testMembers(ids ...string) []cluster.Member {
	members := make([]cluster.Member, len(ids))
	for i, id := range ids {
		members[i] = cluster.Member{ID: id, DataAddr: id + ":4433", State: cluster.MemberActive}
	}
	return members
}

// newFixture is a one-member cluster (RF 1, no lease) over a real WAL and a real
// Badger; tests that need a bigger cluster call withCluster.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	wal, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	})
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing Badger: %v", err)
		}
	})
	fx := &fixture{
		t:     t,
		wal:   &failLog{WAL: wal},
		kv:    &failKV{BadgerKV: badger},
		pairs: &cluster.PairSet{},
		topo: &fakeTopology{
			committed: testEpoch, self: "n1", members: 1,
			table: cluster.BuildTable(cluster.ClusterState{Epoch: int64(testEpoch), Members: testMembers("n1")}),
			dbRow: DatabaseInfo{RF: 1, DefaultConcern: machine.ConcernOne},
		},
		schema: &fakeSchema{
			decl:      machine.Declaration{BucketID: testBucket, Version: testSeen, State: machine.StateActive, Fields: machine.FieldTable{NextFieldID: 4, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"}, {ID: 3, Type: "int", Name: "Age"}}}},
			seen:      testSeen,
			published: testSeen,
		},
		lease:  &fakeLease{},
		repl:   &fakeRepl{},
		dedupe: &fakeDedupe{entries: map[machine.DedupeKey]*machine.DedupeEntry{}},
	}
	fx.lease.valid.Store(true)
	wal.SetFsyncHook(func() { fx.fsyncs.Add(1) })
	fx.rebuildWriter(fx.pairs)
	fx.markReady(testSlot)
	return fx
}

// rebuildWriter makes the Writer over pairs; a fresh PairSet is a restarted node.
func (fx *fixture) rebuildWriter(pairs *cluster.PairSet) {
	fx.pairs = pairs
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: cmp.Or(fx.wcTimeout, time.Second),
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
}

func (fx *fixture) markReady(slot uint16) {
	fx.pairs.With(slot, testDB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved })
}

func (fx *fixture) setFlags(slot uint16, flags uint8) {
	fx.pairs.With(slot, testDB, func(p *cluster.Pair) { p.Flags = flags })
}

// withCluster makes this node n1 of three members at the given RF and level, and gives
// it a slot it owns.
func (fx *fixture) withCluster(rf int, level machine.Concern) uint16 {
	fx.t.Helper()
	fx.topo.table = cluster.BuildTable(cluster.ClusterState{Epoch: int64(testEpoch), Members: testMembers("n1", "n2", "n3")})
	fx.topo.members = 3
	fx.topo.dbRow = DatabaseInfo{RF: rf, DefaultConcern: level}
	for slot := uint16(0); slot < 8192; slot++ {
		if owner, ok := fx.topo.table.Owner(slot, rf); ok && owner.ID == "n1" {
			fx.markReady(slot)
			return slot
		}
	}
	fx.t.Fatal("n1 owns no slot")
	return 0
}

func insertOp(id string) cluster.Op {
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		panic(err)
	}
	idValue := append([]byte{0x02, byte(len(id) + 1), 0, 0, 0}, append([]byte(id), 0)...)
	return cluster.Op{
		Class: cluster.OpInsert, BucketID: testBucket, DeclVersion: testSeen, SchemaVersionSeen: testSeen,
		EncID: encoded, IDValue: idValue,
		Fields: []cluster.FieldOp{{FieldID: 2, Value: []byte{0x02, 4, 0, 0, 0, 'A', 'd', 'a', 0}}},
	}
}

func (fx *fixture) request(slot uint16, id string) WriteRequest {
	return WriteRequest{Slot: slot, DBID: testDB, Op: insertOp(id), RequestID: [16]byte{1, 2, 3, byte(len(id))}, Deadline: time.Second}
}

func (fx *fixture) write(slot uint16, id string) (WriteAnswer, error) {
	return fx.w.Write(context.Background(), fx.request(slot, id))
}

func (fx *fixture) mustWrite(slot uint16, id string) WriteAnswer {
	fx.t.Helper()
	answer, err := fx.write(slot, id)
	if err != nil {
		fx.t.Fatalf("Write(%q): %v", id, err)
	}
	return answer
}

func (fx *fixture) hasDocument(slot uint16, id string) bool {
	fx.t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		fx.t.Fatalf("EncodeComponent: %v", err)
	}
	snapshot := fx.kv.NewSnapshot()
	defer snapshot.Discard()
	_, err = snapshot.Get(store.DocumentPrefix(slot, testDB, testBucket, encoded))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false
	}
	if err != nil {
		fx.t.Fatalf("reading document %q: %v", id, err)
	}
	return true
}

func (fx *fixture) hasPairRecord(slot uint16) bool {
	fx.t.Helper()
	snapshot := fx.kv.NewSnapshot()
	defer snapshot.Discard()
	_, err := snapshot.Get(pairRecordKey(slot, testDB))
	return err == nil
}

func (fx *fixture) logged() []storage.WALEntry {
	fx.t.Helper()
	if fx.wal.LastSequence() == 0 {
		return nil
	}
	entries, err := fx.wal.ReadFrom(1)
	if err != nil {
		fx.t.Fatalf("reading the log: %v", err)
	}
	return entries
}

func codeOf(t *testing.T, err error) cluster.Code {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) {
		t.Fatalf("want a *cluster.Error, got %v", err)
	}
	return coded.Code
}

func TestAWriteIsOrderedAppendedAppliedAndAnswered(t *testing.T) {
	fx := newFixture(t)
	answer := fx.mustWrite(testSlot, "p1")
	if answer.Outcome != machine.OutcomeApplied || answer.CopiesConfirmed != 1 {
		t.Fatalf("answer = %+v, want applied with copies_confirmed 1", answer)
	}
	if want := (cluster.Position{Epoch: testEpoch, Seq: 1}); answer.Pos != want {
		t.Fatalf("position = %s, want %s", answer.Pos, want)
	}
	if !fx.hasDocument(testSlot, "p1") || !fx.hasPairRecord(testSlot) {
		t.Fatal("the document and the pair record must be in Badger")
	}
	entries := fx.logged()
	if len(entries) != 1 {
		t.Fatalf("log holds %d entries, want 1", len(entries))
	}
	slot, db, pos, err := storage.DecodePositionKey(entries[0].Key)
	if err != nil || slot != testSlot || db != testDB || pos != (storage.LogPosition{Epoch: testEpoch, Seq: 1}) {
		t.Fatalf("log key = slot %d db %d pos %v err %v, want the position key of the write", slot, db, pos, err)
	}
	if entries[0].Origin.Kind != storage.WALOriginOwn || entries[0].Origin.Epoch != testEpoch {
		t.Fatalf("origin = %+v, want own at epoch %d", entries[0].Origin, testEpoch)
	}
	record, err := storage.DecodeWALRecord(entries[0].Op, entries[0].Data)
	if err != nil || record.Class != cluster.OpInsert || record.RequestID != fx.request(testSlot, "p1").RequestID {
		t.Fatalf("record = %+v err %v, want the insert and its request id", record, err)
	}
	pair := fx.pairs.Snapshot(testSlot, testDB)
	if pair.AppendedSeq != 1 || pair.AppliedPos != answer.Pos || pair.Ordinal != 1 {
		t.Fatalf("pair = %+v, want appended 1, applied %s, ordinal 1", pair, answer.Pos)
	}
	if got := fx.repl.queuedCount(); got != 1 {
		t.Fatalf("%d positions queued for the copies, want 1", got)
	}
}

func TestNothingAcknowledgedBeforeTheAppend(t *testing.T) {
	fx := newFixture(t)
	reached := make(chan struct{})
	release := make(chan struct{})
	var once, releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(letGo) // a failed assertion must not leave the append blocked in the hook
	fx.wal.SetBeforeSyncHook(func(uint64) {
		once.Do(func() { close(reached) })
		<-release
	})

	done := make(chan struct{})
	var answer WriteAnswer
	var writeErr error
	go func() {
		defer close(done)
		answer, writeErr = fx.write(testSlot, "p1")
	}()

	select {
	case <-reached:
	case <-done:
		t.Fatal("Write returned without its append reaching the fsync: it was acknowledged before the append")
	case <-time.After(10 * time.Second):
		t.Fatal("the append never reached the fsync")
	}
	select {
	case <-done:
		t.Fatal("Write returned while its append was still unsynced")
	default:
	}
	if fx.hasDocument(testSlot, "p1") {
		t.Error("the batch was applied before the append returned")
	}
	if got := fx.repl.queuedCount(); got != 0 {
		t.Errorf("%d positions queued for the copies before the append returned", got)
	}
	letGo()
	<-done
	if writeErr != nil || answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("answer = %+v, err = %v, want applied", answer, writeErr)
	}
	if fx.wal.SyncedSequence() < fx.wal.LastSequence() {
		t.Fatal("the write was acknowledged with its entry not yet synced")
	}
}

func TestBadgerBatchFailureKeepsThePosition(t *testing.T) {
	fx := newFixture(t)
	fx.kv.failing.Store(true)
	answer, err := fx.write(testSlot, "p1")
	if err != nil {
		t.Fatalf("a failed batch must not fail the write: %v", err)
	}
	if answer.Outcome != machine.OutcomeApplied || answer.ApplyFailure == nil || !errors.Is(answer.ApplyFailure, errDiskOnFire) {
		t.Fatalf("answer = %+v, want applied carrying the batch failure", answer)
	}
	pair := fx.pairs.Snapshot(testSlot, testDB)
	if pair.AppendedSeq != 1 {
		t.Fatalf("appendedSeq = %d after a batch failure, want 1: the position is durable and stays taken", pair.AppendedSeq)
	}
	if !pair.AppliedPos.Zero() || pair.Ordinal != 0 {
		t.Fatalf("applied = %s ordinal %d after a failed batch, want neither to move", pair.AppliedPos, pair.Ordinal)
	}
	if pair.Flags&flagReplayNeeded == 0 || pair.Flags&flagReplayed != 0 {
		t.Fatalf("flags = %05b, want replayNeeded set and replayed cleared", pair.Flags)
	}
	if fx.hasDocument(testSlot, "p1") || fx.hasPairRecord(testSlot) {
		t.Fatal("a failed batch must leave neither the document nor the pair record")
	}
	if got := len(fx.logged()); got != 1 {
		t.Fatalf("log holds %d entries, want the one durable record", got)
	}

	held, err := fx.write(testSlot, "p2")
	if err != nil || !held.Held {
		t.Fatalf("the next write = %+v, err %v, want held until the replay", held, err)
	}

	// The replay re-applies the record, then the pair orders again after it.
	fx.kv.failing.Store(false)
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	fx.markReady(testSlot)
	next := fx.mustWrite(testSlot, "p2")
	if want := (cluster.Position{Epoch: testEpoch, Seq: 2}); next.Pos != want {
		t.Fatalf("next position = %s, want %s: the failed write's position was reused", next.Pos, want)
	}
	if !fx.hasDocument(testSlot, "p1") || !fx.hasDocument(testSlot, "p2") {
		t.Fatal("the replayed write and the next one must both be in Badger")
	}
}

func TestCopiesConfirmedCountsTheOwner(t *testing.T) {
	cases := []struct {
		name  string
		rf    int
		level machine.Concern
		acks  int
		want  machine.Outcome
		count uint8
	}{
		{"majority with one copy confirmed", 3, machine.ConcernMajority, 1, machine.OutcomeApplied, 2},
		{"majority with no copy confirmed", 3, machine.ConcernMajority, 0, machine.OutcomeAppliedDegraded, 1},
		{"all with both copies confirmed", 3, machine.ConcernAll, 2, machine.OutcomeApplied, 3},
		{"all with one copy confirmed", 3, machine.ConcernAll, 1, machine.OutcomeAppliedDegraded, 2},
		{"one needs no copy", 3, machine.ConcernOne, 0, machine.OutcomeApplied, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			slot := fx.withCluster(tc.rf, tc.level)
			fx.repl.acks = tc.acks
			answer := fx.mustWrite(slot, "p1")
			if answer.Outcome != tc.want || answer.CopiesConfirmed != tc.count {
				t.Fatalf("answer = %v with %d confirmed, want %v with %d (the owner counts)", answer.Outcome, answer.CopiesConfirmed, tc.want, tc.count)
			}
		})
	}
}

func TestWritesToManyPairsShareOneFsync(t *testing.T) {
	const pairs = 16
	fx := newFixture(t)
	for slot := uint16(100); slot < 100+pairs; slot++ {
		fx.markReady(slot)
	}

	// Every append is written before any of them may sync: the group commit then has
	// all of them to cover. An append that took its own sync would never arrive here.
	var arrived atomic.Int64
	allArrived := make(chan struct{})
	fx.wal.SetBeforeSyncHook(func(uint64) {
		if arrived.Add(1) == pairs {
			close(allArrived)
		}
		// The gate opens when the last append is queued. The timeout is only a safety
		// for a writer that never reaches it, which then fails the fsync count below.
		select {
		case <-allArrived:
		case <-time.After(10 * time.Second):
		}
	})

	var wg sync.WaitGroup
	errs := make(chan error, pairs)
	for slot := uint16(100); slot < 100+pairs; slot++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := fx.write(slot, "p1"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Write: %v", err)
	}
	if got := fx.fsyncs.Load(); got != 1 {
		t.Fatalf("%d writes to %d pairs cost %d fsyncs, want 1 shared by all of them", pairs, pairs, got)
	}
	if calls, badger := fx.wal.syncCalls.Load(), fx.kv.syncs.Load(); calls != pairs || badger != 0 {
		t.Fatalf("writers made %d SyncThrough calls on the log and %d syncs on Badger, want one call per write (%d), sharing the flush, and none on Badger", calls, badger, pairs)
	}
	if got := len(fx.logged()); got != pairs {
		t.Fatalf("log holds %d entries, want %d", got, pairs)
	}
}

func TestOneWriteCostsOneFsyncAndNoBadgerSync(t *testing.T) {
	fx := newFixture(t)
	fx.mustWrite(testSlot, "p1")
	if got := fx.fsyncs.Load(); got != 1 {
		t.Fatalf("one write cost %d log fsyncs, want 1", got)
	}
	if calls := fx.wal.syncCalls.Load(); calls != 1 {
		t.Fatalf("the writer made %d SyncThrough calls, want exactly 1: the split append's one flush", calls)
	}
	if badger := fx.kv.syncs.Load(); badger != 0 {
		t.Fatalf("the Badger batch asked for %d syncs, want none: the log is the durability point and the batch is unsynced", badger)
	}
}

func TestAReplayAfterABumpAppliesTheAcknowledgedRecord(t *testing.T) {
	fx := newFixture(t)
	// The write is acknowledged but its batch never reached Badger: the node restarts
	// with the log ahead of the store.
	fx.kv.failing.Store(true)
	answer := fx.mustWrite(testSlot, "p1")
	if answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("answer = %+v, want applied", answer)
	}
	fx.kv.failing.Store(false)

	// A bump committed after the acknowledgement: the declaration moved, so the fence
	// would now refuse this record.
	fx.schema.mu.Lock()
	fx.schema.published, fx.schema.seen = 9, 9
	fx.schema.decl.Version, fx.schema.decl.State = 9, machine.StateDropping
	fx.schema.mu.Unlock()

	restarted := &cluster.PairSet{}
	fx.rebuildWriter(restarted)
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return restarted.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("Replay refused an acknowledged record: %v", err)
	}
	if !fx.hasDocument(testSlot, "p1") {
		t.Fatal("the replay did not apply the acknowledged record")
	}
	pair := restarted.Snapshot(testSlot, testDB)
	if want := (cluster.Position{Epoch: testEpoch, Seq: 1}); pair.AppliedPos != want || pair.AppendedSeq != 1 || pair.Ordinal != 1 {
		t.Fatalf("pair = %+v, want applied %s, appended 1, ordinal 1", pair, want)
	}
	if views := fx.schema.viewCount(); views != 2 {
		t.Fatalf("the declaration was read %d times, want 2 (the write's two evaluations); the replay must read none", views)
	}
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("a second replay of the same records: %v", err)
	}
	if got := restarted.Snapshot(testSlot, testDB).Ordinal; got != 1 {
		t.Fatalf("ordinal = %d after replaying twice, want 1: a held record must not be applied again", got)
	}
}

func TestTheFenceIsEvaluatedTwiceByTheOwner(t *testing.T) {
	t.Run("two evaluations in a plain write", func(t *testing.T) {
		fx := newFixture(t)
		fx.mustWrite(testSlot, "p1")
		if got := fx.schema.viewCount(); got != 2 {
			t.Fatalf("the fence read the declaration %d times, want 2: at admission and under the pair lock", got)
		}
	})
	t.Run("a bump between admission and the append refuses", func(t *testing.T) {
		fx := newFixture(t)
		// The bump lands after the first evaluation: the bucket's own table moves.
		fx.schema.beforeView = func(n int, s *fakeSchema) {
			if n == 2 {
				s.published, s.decl.Version = 9, 9
			}
		}
		_, err := fx.write(testSlot, "p1")
		if err == nil || codeOf(t, err) != cluster.CodeSchemaNotCurrent {
			t.Fatalf("err = %v, want SCHEMA_NOT_CURRENT: the write must not go under a retired table", err)
		}
		if got := len(fx.logged()); got != 0 {
			t.Fatalf("log holds %d entries, want none: a refusal appends nothing", got)
		}
		if pair := fx.pairs.Snapshot(testSlot, testDB); pair.AppendedSeq != 0 {
			t.Fatalf("appendedSeq = %d, want 0: a refusal consumes no position", pair.AppendedSeq)
		}
		if fx.hasDocument(testSlot, "p1") {
			t.Fatal("the refused write reached Badger")
		}
	})
	t.Run("a bump of another bucket is re-admitted once", func(t *testing.T) {
		fx := newFixture(t)
		fx.schema.beforeView = func(n int, s *fakeSchema) {
			if n == 2 {
				s.published = 8 // the database moved, this bucket's table did not
			}
		}
		answer := fx.mustWrite(testSlot, "p1")
		if answer.Pos.Seq != 1 || fx.schema.refetchCount() != 1 {
			t.Fatalf("position %s after %d refetches, want 1:1 after exactly one", answer.Pos, fx.schema.refetchCount())
		}
	})
}

func TestTheOneReAdmissionTakesNoPosition(t *testing.T) {
	t.Run("admission leaves the pair untouched", func(t *testing.T) {
		fx := newFixture(t)
		req := fx.request(testSlot, "p1")
		req.Op.SchemaVersionSeen = testSeen - 1
		fx.schema.published = testSeen
		fx.schema.seen = testSeen - 1
		adm, res := fx.w.admitAndReEnterOnce(context.Background(), req)
		if !res.Admitted() || !adm.ReAdmitted || adm.Seen != testSeen {
			t.Fatalf("admission = %+v, result %+v, want admitted after the one re-admission at seen %d", adm, res, testSeen)
		}
		if got := fx.schema.refetchCount(); got != 1 {
			t.Fatalf("%d refetches, want 1", got)
		}
		if pair := fx.pairs.Snapshot(testSlot, testDB); pair.AppendedSeq != 0 {
			t.Fatalf("appendedSeq = %d after the re-admission, want 0: it takes no position", pair.AppendedSeq)
		}
		if got := len(fx.logged()); got != 0 {
			t.Fatalf("log holds %d entries after an admission, want 0", got)
		}
	})
	t.Run("the write after it holds the first position", func(t *testing.T) {
		fx := newFixture(t)
		req := fx.request(testSlot, "p1")
		req.Op.SchemaVersionSeen = testSeen - 1
		fx.schema.seen = testSeen - 1
		answer, err := fx.w.Write(context.Background(), req)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		if want := (cluster.Position{Epoch: testEpoch, Seq: 1}); answer.Pos != want {
			t.Fatalf("position = %s, want %s: the refetch consumed a position", answer.Pos, want)
		}
		if fx.schema.refetchCount() != 1 || len(fx.logged()) != 1 {
			t.Fatalf("%d refetches and %d log entries, want 1 and 1", fx.schema.refetchCount(), len(fx.logged()))
		}
	})
	t.Run("the refetch in the write loop takes no position", func(t *testing.T) {
		fx := newFixture(t)
		first := fx.mustWrite(testSlot, "p0")
		// Admission of the second write passes (view 3); the bump lands before the
		// under-lock evaluation (view 4) and leaves this bucket's table alone, so the
		// loop re-admits once.
		fx.schema.beforeView = func(n int, s *fakeSchema) {
			if n == 4 {
				s.published = 8
			}
		}
		answer, err := fx.write(testSlot, "p1")
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		if want := (cluster.Position{Epoch: testEpoch, Seq: first.Pos.Seq + 1}); answer.Pos != want {
			t.Fatalf("position = %s, want %s: the loop's refetch consumed a position", answer.Pos, want)
		}
		if fx.schema.refetchCount() != 1 || len(fx.logged()) != 2 {
			t.Fatalf("%d refetches and %d log entries, want 1 and 2", fx.schema.refetchCount(), len(fx.logged()))
		}
		if pair := fx.pairs.Snapshot(testSlot, testDB); pair.AppendedSeq != 2 {
			t.Fatalf("appendedSeq = %d, want 2", pair.AppendedSeq)
		}
	})
	t.Run("a second miss is the refusal", func(t *testing.T) {
		fx := newFixture(t)
		fx.schema.staleAfter = true
		fx.schema.seen = testSeen - 1
		req := fx.request(testSlot, "p1")
		req.Op.SchemaVersionSeen = testSeen - 1
		_, err := fx.w.Write(context.Background(), req)
		if err == nil || codeOf(t, err) != cluster.CodeSchemaNotCurrent {
			t.Fatalf("err = %v, want SCHEMA_NOT_CURRENT after the one re-admission", err)
		}
		if fx.schema.refetchCount() != 1 || len(fx.logged()) != 0 {
			t.Fatalf("%d refetches and %d log entries, want 1 and 0", fx.schema.refetchCount(), len(fx.logged()))
		}
	})
}

func TestABatchOf256IsRefusedNaming255(t *testing.T) {
	fx := newFixture(t)
	if err := fx.w.checkBatch(255); err != nil {
		t.Fatalf("checkBatch(255) = %v, want nil", err)
	}
	err := fx.w.checkBatch(256)
	if err == nil || codeOf(t, err) != cluster.CodeInvalidArgument || !strings.Contains(err.Error(), "255") {
		t.Fatalf("checkBatch(256) = %v, want INVALID_ARGUMENT naming 255", err)
	}
	req := fx.request(testSlot, "p1")
	req.BatchSize = 256
	if _, err := fx.w.Write(context.Background(), req); err == nil || !strings.Contains(err.Error(), "255") {
		t.Fatalf("Write of a 256-op batch = %v, want the 255 refusal", err)
	}
	if got := len(fx.logged()); got != 0 {
		t.Fatalf("log holds %d entries, want none", got)
	}
}

func TestRankOneDoesNotServeBeforeItsBarrierCompletes(t *testing.T) {
	ready := flagReplayed | flagStopProved
	cases := []struct {
		name  string
		flags uint8
	}{
		{"sending", ready | flagSending},
		{"awaiting", ready | flagAwaiting},
		{"replay still needed", ready | flagReplayNeeded},
		{"log tail not replayed", flagStopProved},
		{"predecessor not proved stopped", flagReplayed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			fx.setFlags(testSlot, tc.flags)
			if fx.w.mayOrder(testSlot, testDB) {
				t.Fatal("mayOrder is true during a hold")
			}
			answer, err := fx.write(testSlot, "p1")
			if err != nil || !answer.Held || answer.Pos != (cluster.Position{}) {
				t.Fatalf("answer = %+v, err = %v, want held with no position", answer, err)
			}
			if len(fx.logged()) != 0 || fx.hasDocument(testSlot, "p1") {
				t.Fatal("a held write reached the log or Badger")
			}
		})
	}
	t.Run("applied position short of the target", func(t *testing.T) {
		fx := newFixture(t)
		fx.topo.setTarget(cluster.Position{Epoch: testEpoch - 1, Seq: 40})
		if fx.w.mayOrder(testSlot, testDB) {
			t.Fatal("mayOrder is true below the target")
		}
		if answer, err := fx.write(testSlot, "p1"); err != nil || !answer.Held {
			t.Fatalf("answer = %+v, err = %v, want held", answer, err)
		}
		fx.pairs.With(testSlot, testDB, func(p *cluster.Pair) { p.AppliedPos = cluster.Position{Epoch: testEpoch - 1, Seq: 40} })
		if !fx.w.mayOrder(testSlot, testDB) {
			t.Fatal("mayOrder is false once the pair applied through its target")
		}
	})
	t.Run("lapsed lease with other members", func(t *testing.T) {
		fx := newFixture(t)
		fx.topo.setMembers(3)
		fx.lease.valid.Store(false)
		if fx.w.mayOrder(testSlot, testDB) {
			t.Fatal("mayOrder is true without a lease")
		}
		if _, err := fx.write(testSlot, "p1"); err == nil || codeOf(t, err) != cluster.CodeLeaseLapsed {
			t.Fatalf("err = %v, want LEASE_LAPSED", err)
		}
	})
	t.Run("a completed barrier serves", func(t *testing.T) {
		fx := newFixture(t)
		if !fx.w.mayOrder(testSlot, testDB) {
			t.Fatal("mayOrder is false for a ready owner")
		}
	})
	t.Run("a pair this node does not own is not orderable and forwards", func(t *testing.T) {
		fx := newFixture(t)
		slot := fx.withCluster(1, machine.ConcernOne)
		var other uint16
		for s := uint16(0); s < 8192; s++ {
			if owner, ok := fx.topo.table.Owner(s, 1); ok && owner.ID != "n1" {
				other = s
				break
			}
		}
		fx.markReady(other)
		if fx.w.mayOrder(other, testDB) {
			t.Fatal("mayOrder is true for a slot another node owns")
		}
		if answer, err := fx.write(other, "p1"); err != nil || !answer.Forward {
			t.Fatalf("answer = %+v, err = %v, want forward", answer, err)
		}
		if answer, err := fx.write(slot, "p1"); err != nil || answer.Outcome == 0 {
			t.Fatalf("answer = %+v, err = %v, want an owned slot to be served", answer, err)
		}
		hopped := fx.request(other, "p2")
		hopped.Hop = 1
		if _, err := fx.w.Write(context.Background(), hopped); err == nil || codeOf(t, err) != cluster.CodeNotOwner {
			t.Fatalf("err = %v, want NOT_OWNER at hop 1", err)
		}
	})
}

func TestARefusalConsumesNoPosition(t *testing.T) {
	fx := newFixture(t)
	fx.mustWrite(testSlot, "p1")

	duplicate := fx.request(testSlot, "p1")
	duplicate.RequestID = [16]byte{9}
	if _, err := fx.w.Write(context.Background(), duplicate); err == nil || codeOf(t, err) != cluster.CodeDocumentExists {
		t.Fatalf("err = %v, want DOCUMENT_EXISTS", err)
	}
	stale := fx.request(testSlot, "p2")
	stale.ClientEpoch = testEpoch - 1
	if _, err := fx.w.Write(context.Background(), stale); err == nil || codeOf(t, err) != cluster.CodeStaleEpoch {
		t.Fatalf("err = %v, want STALE_EPOCH", err)
	}
	if pair := fx.pairs.Snapshot(testSlot, testDB); pair.AppendedSeq != 1 || len(fx.logged()) != 1 {
		t.Fatalf("appendedSeq %d with %d log entries, want 1 and 1: refusals take no position", pair.AppendedSeq, len(fx.logged()))
	}
	if next := fx.mustWrite(testSlot, "p3"); next.Pos.Seq != 2 {
		t.Fatalf("next position = %s, want seq 2", next.Pos)
	}
}

func TestADedupedRetryAppliesNothing(t *testing.T) {
	fx := newFixture(t)
	first := fx.mustWrite(testSlot, "p1")
	req := fx.request(testSlot, "p1")
	fx.dedupe.entries[machine.DedupeKey{RequestID: req.RequestID}] = &machine.DedupeEntry{Pos: first.Pos, Fingerprint: opFingerprint(req.Op), Result: []byte("kept")}

	retry, err := fx.w.Write(context.Background(), req)
	if err != nil || !retry.Deduped || retry.Pos != first.Pos || string(retry.Result) != "kept" {
		t.Fatalf("retry = %+v, err = %v, want the recorded position and result", retry, err)
	}
	if retry.Outcome != machine.OutcomeApplied {
		t.Fatalf("outcome = %v, want applied recomposed from the current count", retry.Outcome)
	}
	if len(fx.logged()) != 1 || fx.pairs.Snapshot(testSlot, testDB).AppendedSeq != 1 {
		t.Fatal("a deduped retry must append nothing and take no position")
	}
}

func TestAnAppendFailureFencesThePairAndReusesThePosition(t *testing.T) {
	fx := newFixture(t)
	fx.wal.failing.Store(true)
	_, err := fx.write(testSlot, "p1")
	if err == nil || codeOf(t, err) != cluster.CodeUnavailable || !errors.Is(err, errDiskOnFire) {
		t.Fatalf("err = %v, want UNAVAILABLE wrapping the cause", err)
	}
	if fx.hasDocument(testSlot, "p1") || fx.repl.queuedCount() != 0 {
		t.Fatal("nothing may be applied or queued when the append failed")
	}
	pair := fx.pairs.Snapshot(testSlot, testDB)
	if pair.AppendedSeq != 0 || pair.Flags&flagReplayNeeded == 0 {
		t.Fatalf("pair = %+v, want appendedSeq 0 and the pair fenced", pair)
	}
	if answer, err := fx.write(testSlot, "p2"); err != nil || !answer.Held {
		t.Fatalf("answer = %+v, err = %v, want held while fenced", answer, err)
	}
	fx.wal.failing.Store(false)
	fx.markReady(testSlot)
	if answer := fx.mustWrite(testSlot, "p2"); answer.Pos.Seq != 1 {
		t.Fatalf("position = %s, want 7:1 reused", answer.Pos)
	}
}

func TestTheFirstRecordOfAnEpochCarriesPrevEnd(t *testing.T) {
	fx := newFixture(t)
	first := fx.mustWrite(testSlot, "p1")
	fx.topo.mu.Lock()
	fx.topo.committed++
	fx.topo.mu.Unlock()
	opener := fx.mustWrite(testSlot, "p2")
	if want := (cluster.Position{Epoch: testEpoch + 1, Seq: 1}); opener.Pos != want {
		t.Fatalf("position = %s, want %s: a new epoch restarts at seq 1", opener.Pos, want)
	}
	entries := fx.logged()
	record, err := storage.DecodeWALRecord(entries[1].Op, entries[1].Data)
	if err != nil {
		t.Fatalf("DecodeWALRecord: %v", err)
	}
	op, err := cluster.DecodeOp(record.Body)
	if err != nil || op.PrevEnd != first.Pos {
		t.Fatalf("prevEnd = %s (err %v), want %s", op.PrevEnd, err, first.Pos)
	}
	firstRecord, _ := storage.DecodeWALRecord(entries[0].Op, entries[0].Data)
	if firstOp, _ := cluster.DecodeOp(firstRecord.Body); !firstOp.PrevEnd.Zero() {
		t.Fatalf("the first record of the pair carries prevEnd %s, want 0:0", firstOp.PrevEnd)
	}
}

func TestConcurrentWritersToManyPairsGetContiguousPositions(t *testing.T) {
	const (
		pairCount = 8
		writers   = 4
		perWriter = 10
	)
	fx := newFixture(t)
	for slot := uint16(0); slot < pairCount; slot++ {
		fx.markReady(slot)
	}
	var wg sync.WaitGroup
	for slot := uint16(0); slot < pairCount; slot++ {
		for writer := 0; writer < writers; writer++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					id := fmt.Sprintf("w%d-%d", writer, i)
					if _, err := fx.write(slot, id); err != nil {
						t.Errorf("Write(%d, %q): %v", slot, id, err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()
	for slot := uint16(0); slot < pairCount; slot++ {
		pair := fx.pairs.Snapshot(slot, testDB)
		if want := uint64(writers * perWriter); pair.AppendedSeq != want || pair.AppliedPos.Seq != want || pair.Ordinal != want {
			t.Fatalf("pair %d = %+v, want appended, applied and ordinal all %d", slot, pair, want)
		}
	}
	perPair := map[uint16]map[uint64]bool{}
	for _, entry := range fx.logged() {
		slot, _, pos, err := storage.DecodePositionKey(entry.Key)
		if err != nil {
			t.Fatalf("DecodePositionKey: %v", err)
		}
		if perPair[slot] == nil {
			perPair[slot] = map[uint64]bool{}
		}
		if perPair[slot][pos.Seq] {
			t.Fatalf("slot %d position %d was assigned twice", slot, pos.Seq)
		}
		perPair[slot][pos.Seq] = true
	}
	for slot, seqs := range perPair {
		if len(seqs) != writers*perWriter {
			t.Fatalf("slot %d logged %d positions, want %d", slot, len(seqs), writers*perWriter)
		}
	}
}

// restartedFrom is a node that restarted with its pair record current: the pair state
// starts at the recorded applied position and ordinal, as a reload of conv §12 gives.
func (fx *fixture) restartedFrom(applied cluster.Position, ordinal uint64) *cluster.PairSet {
	restarted := &cluster.PairSet{}
	restarted.With(testSlot, testDB, func(p *cluster.Pair) {
		p.AppliedPos, p.Ordinal, p.Flags = applied, ordinal, flagReplayed|flagStopProved
	})
	fx.rebuildWriter(restarted)
	return restarted
}

func (fx *fixture) replayAll(pairs *cluster.PairSet) {
	fx.t.Helper()
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		fx.t.Fatalf("Replay: %v", err)
	}
}

func TestARestartWithACurrentPairRecordDoesNotReuseAPosition(t *testing.T) {
	fx := newFixture(t)
	for _, id := range []string{"p1", "p2", "p3"} {
		fx.mustWrite(testSlot, id)
	}
	last := cluster.Position{Epoch: testEpoch, Seq: 3}

	restarted := fx.restartedFrom(last, 3)
	fx.replayAll(restarted)
	next := fx.mustWrite(testSlot, "p4")
	if want := (cluster.Position{Epoch: testEpoch, Seq: 4}); next.Pos != want {
		t.Fatalf("next position = %s, want %s: a current pair record must not hide the log's tail", next.Pos, want)
	}
	if pair := restarted.Snapshot(testSlot, testDB); pair.AppliedPos != next.Pos || pair.Ordinal != 4 {
		t.Fatalf("pair = %+v, want applied %s, ordinal 4: the record must never regress", pair, next.Pos)
	}

	// A second restart from the pair record the fourth write left applies nothing twice.
	again := fx.restartedFrom(next.Pos, 4)
	fx.replayAll(again)
	if pair := again.Snapshot(testSlot, testDB); pair.Ordinal != 4 || pair.AppliedPos != next.Pos {
		t.Fatalf("pair after the second restart = %+v, want ordinal 4 (four distinct records) at %s", pair, next.Pos)
	}
	if got := len(fx.logged()); got != 4 {
		t.Fatalf("log holds %d entries, want 4 distinct positions", got)
	}
}

func TestReplayingTheSameLogTwiceIsIdempotent(t *testing.T) {
	fx := newFixture(t)
	for _, id := range []string{"p1", "p2", "p3"} {
		fx.mustWrite(testSlot, id)
	}
	// Lost store: replay from nothing, then again, then offer each record directly.
	empty := fx.restartedFrom(cluster.Position{}, 0)
	fx.replayAll(empty)
	fx.replayAll(empty)
	for _, entry := range fx.logged() {
		slot, db, pos, err := storage.DecodePositionKey(entry.Key)
		if err != nil {
			t.Fatalf("DecodePositionKey: %v", err)
		}
		record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
		if err != nil {
			t.Fatalf("DecodeWALRecord: %v", err)
		}
		if err := fx.w.Replay(cluster.WALRecord{WALRecord: record, Slot: slot, DB: db, Pos: cluster.Position(pos), Cursor: cluster.Cursor(entry.Seq)}); err != nil {
			t.Fatalf("Replay of %v: %v", pos, err)
		}
	}
	pair := empty.Snapshot(testSlot, testDB)
	if pair.Ordinal != 3 || pair.AppendedSeq != 3 || pair.AppliedPos.Seq != 3 {
		t.Fatalf("pair = %+v, want ordinal, appended and applied all 3", pair)
	}
}

func TestEpochIsCheckedBeforeTheBatchCapAndTheClass(t *testing.T) {
	stale := func(req WriteRequest) WriteRequest {
		req.ClientEpoch = testEpoch - 1
		return req
	}
	bigBatch := func(req WriteRequest) WriteRequest {
		req.BatchSize = 256
		return req
	}
	unbuilt := func(req WriteRequest) WriteRequest {
		req.Op = cluster.Op{Class: cluster.OpUpdate, BucketID: testBucket, EncID: req.Op.EncID}
		return req
	}
	cases := []struct {
		name     string
		shape    func(WriteRequest) WriteRequest
		current  string
		wantText string
	}{
		{"a 256-op batch", bigBatch, "", "255"},
		{"an unbuilt class", unbuilt, "", "0x02"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			_, err := fx.w.Write(context.Background(), stale(tc.shape(fx.request(testSlot, "p1"))))
			if err == nil || codeOf(t, err) != cluster.CodeStaleEpoch {
				t.Fatalf("on a stale epoch err = %v, want STALE_EPOCH (conv §8 check 1 comes first)", err)
			}
			_, err = fx.w.Write(context.Background(), tc.shape(fx.request(testSlot, "p1")))
			if err == nil || codeOf(t, err) != cluster.CodeInvalidArgument || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("on a current epoch err = %v, want INVALID_ARGUMENT naming %s", err, tc.wantText)
			}
			if got := len(fx.logged()); got != 0 {
				t.Fatalf("log holds %d entries, want none", got)
			}
		})
	}
}

// TestOwnerFenceSchemaRefusalNamesTheBucketAndVersions is BUG-0748: the owner's fence text names
// the database and bucket the developer typed, shows both versions as numbers and quotes a
// command that parses, on a write and on a read.
func TestOwnerFenceSchemaRefusalNamesTheBucketAndVersions(t *testing.T) {
	assertNamed := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || codeOf(t, err) != cluster.CodeSchemaNotCurrent {
			t.Fatalf("err = %v, want SCHEMA_NOT_CURRENT", err)
		}
		text := err.Error()
		for _, banned := range []string{"<d>", "<s>", "3.41", "bucket 41", " in 3"} {
			if strings.Contains(text, banned) {
				t.Fatalf("the refusal contains %q, an id or an unfilled placeholder: %s", banned, text)
			}
		}
		for _, want := range []string{
			"app.users is not writable",
			"declaration is at schema version 8",
			"database's schema_version is 9",
			"run 'describe bucket users in app'",
			"run the same 'alter bucket users { add <field> <type> }' again",
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("the refusal lacks %q: %s", want, text)
			}
		}
	}
	bumpOnSecondView := func(n int, s *fakeSchema) {
		if n == 2 {
			s.published, s.decl.Version = 9, 8
		}
	}
	t.Run("a write", func(t *testing.T) {
		fx := newFixture(t)
		fx.topo.dbRow.Name, fx.schema.name = "app", "users"
		fx.schema.beforeView = bumpOnSecondView
		_, err := fx.write(testSlot, "p1")
		assertNamed(t, err)
	})
	t.Run("a read", func(t *testing.T) {
		world := newU36World(t)
		world.fx.topo.dbRow.Name, world.fx.schema.name = "app", "users"
		world.fx.mustWrite(world.slot, "p1")
		world.fx.schema.published, world.fx.schema.decl.Version = 9, 8
		world.fx.schema.staleAfter = true
		_, err := world.get("p1")
		assertNamed(t, err)
	})
	t.Run("a name the cache no longer holds", func(t *testing.T) {
		fx := newFixture(t)
		fx.schema.beforeView = bumpOnSecondView
		_, err := fx.write(testSlot, "p1")
		if err == nil || codeOf(t, err) != cluster.CodeSchemaNotCurrent {
			t.Fatalf("err = %v, want SCHEMA_NOT_CURRENT", err)
		}
		for _, banned := range []string{"<d>", "<s>", "<db>", "<bucket>", "3.41", "bucket 41"} {
			if strings.Contains(err.Error(), banned) {
				t.Fatalf("the unnamed refusal contains %q: %s", banned, err)
			}
		}
	})
}

// breakerFixture is RF 2 at majority with a wc_timeout short enough to wait out in a test.
func breakerFixture(t *testing.T) (*fixture, uint16) {
	t.Helper()
	fx := newFixture(t)
	slot := fx.withCluster(2, machine.ConcernMajority)
	fx.wcTimeout = 100 * time.Millisecond
	fx.rebuildWriter(fx.pairs)
	return fx, slot
}

// TestAWaitThatRunsTheWholeWCTimeoutIsReported: a concern wait that ends at wc_timeout with the copy
// missing is reported to the stream, once, and the answer is still applied_degraded.
// [BREAK: conclude does not call NoteWaitTimeout].
func TestAWaitThatRunsTheWholeWCTimeoutIsReported(t *testing.T) {
	fx, slot := breakerFixture(t)
	fx.repl.blockAwait = true
	answer := fx.mustWrite(slot, "p1")
	if answer.Outcome != machine.OutcomeAppliedDegraded || answer.CopiesConfirmed != 1 {
		t.Fatalf("answer = %v with %d confirmed, want applied_degraded with 1", answer.Outcome, answer.CopiesConfirmed)
	}
	if got := fx.repl.notedCount(); got != 1 {
		t.Fatalf("the writer reported %d timeouts, want 1", got)
	}
}

// TestAShorterClientDeadlineMarksNothing: a wait the client's deadline ended says nothing about
// the copy, so nothing is reported.
// [BREAK: conclude reports every wait that ends with copies missing].
func TestAShorterClientDeadlineMarksNothing(t *testing.T) {
	fx, slot := breakerFixture(t)
	fx.repl.blockAwait = true
	req := fx.request(slot, "p1")
	req.Deadline = 20 * time.Millisecond
	if _, err := fx.w.Write(context.Background(), req); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := fx.repl.notedCount(); got != 0 {
		t.Fatalf("a wait the client's deadline ended was reported %d times, want 0", got)
	}
}

// TestASlowPeerThatAnswersInTimeIsNotMarked: a copy that acks at 80 percent of the budget meets
// the wait, so the answer is applied and nothing is reported.
// [BREAK: conclude reports a wait by its elapsed time instead of by how it ended].
func TestASlowPeerThatAnswersInTimeIsNotMarked(t *testing.T) {
	fx, slot := breakerFixture(t)
	fx.repl.awaitDelay = 80 * time.Millisecond
	answer := fx.mustWrite(slot, "p1")
	if answer.Outcome != machine.OutcomeApplied || answer.CopiesConfirmed != 2 {
		t.Fatalf("answer = %v with %d confirmed, want applied with 2", answer.Outcome, answer.CopiesConfirmed)
	}
	if got := fx.repl.notedCount(); got != 0 {
		t.Fatalf("a wait that was met was reported %d times, want 0", got)
	}
}

// TestAnEarlyDegradedAnswerIsNotReportedAsATimeout: a wait that ends early because every missing
// copy is suspect did not time out, and reporting it would extend the hold with no new evidence.
// [BREAK: conclude reports when acked is below need, whatever ended the wait].
func TestAnEarlyDegradedAnswerIsNotReportedAsATimeout(t *testing.T) {
	fx, slot := breakerFixture(t)
	answer := fx.mustWrite(slot, "p1") // the fake returns at once with no ack
	if answer.Outcome != machine.OutcomeAppliedDegraded {
		t.Fatalf("answer = %v, want applied_degraded", answer.Outcome)
	}
	if got := fx.repl.notedCount(); got != 0 {
		t.Fatalf("an early return was reported %d times as a timeout, want 0", got)
	}
}
