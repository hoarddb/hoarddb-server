package wire

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// newGuardFixture is a one-member cluster whose applier takes updates, over the same Badger
// the dedupe reader reads, so a retry meets the entry the first attempt wrote.
func newGuardFixture(t *testing.T) *fixture {
	t.Helper()
	fx := newFixture(t)
	table := &fx.schema.decl.Fields
	table.Entries = append(table.Entries, machine.FieldEntry{ID: 4, Type: "array", Name: "Tags"},
		machine.FieldEntry{ID: 5, Type: "int", Name: "Count"}, machine.FieldEntry{ID: 6, Type: "int", Name: "Moved"})
	table.NextFieldID = 7
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, nil).WithFieldEngines(scalarOnlyEngines{}).WithResultCap(1024),
		Clock:   fixedClock{}, Lease: fx.lease, Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl,
		WCTimeout: time.Second, DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	return fx
}

// scalarOnlyEngines declares no store field, so every field id of the fixture is a scalar.
type scalarOnlyEngines struct{}

func (scalarOnlyEngines) FieldEngine(uint64, uint64, uint16) (store.FieldEngine, error) {
	return 0, errUndeclaredField
}

func guardString(text string) []byte {
	return append(binary.LittleEndian.AppendUint32([]byte{0x02}, uint32(len(text)+1)), append([]byte(text), 0)...)
}

func guardInt32(value int32) []byte {
	return binary.LittleEndian.AppendUint32([]byte{0x10}, uint32(value))
}

func guardInt64(value int64) []byte {
	return binary.LittleEndian.AppendUint64([]byte{0x12}, uint64(value))
}

func guardDouble(value float64) []byte {
	return binary.LittleEndian.AppendUint64([]byte{0x01}, math.Float64bits(value))
}

// guardNull is the typed null: a type byte and no value bytes.
var guardNull = []byte{0x0A}

func guardedUpdate(t *testing.T, id string, guard []cluster.GuardTerm, fields ...cluster.FieldOp) cluster.Op {
	t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatalf("EncodeComponent: %v", err)
	}
	return cluster.Op{Class: cluster.OpUpdate, BucketID: testBucket, DeclVersion: testSeen, SchemaVersionSeen: testSeen, EncID: encoded, Fields: fields, Guard: guard}
}

func setField(id uint16, typed []byte) cluster.FieldOp {
	return cluster.FieldOp{FieldID: id, OpCode: cluster.UpdateSet, Value: typed}
}

func (fx *fixture) writeOp(op cluster.Op, requestID byte) (WriteAnswer, error) {
	return fx.w.Write(context.Background(), WriteRequest{Slot: testSlot, DBID: testDB, Op: op, RequestID: [16]byte{9, 9, requestID}, Deadline: time.Second})
}

// storedScalar is the stored value of one field of document id, nil when it has none.
func (fx *fixture) storedScalar(id string, fieldID uint16) []byte {
	fx.t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		fx.t.Fatalf("EncodeComponent: %v", err)
	}
	snapshot := fx.kv.NewSnapshot()
	defer snapshot.Discard()
	stored, err := readScalar(snapshot, store.DocumentPrefix(testSlot, testDB, testBucket, encoded), fieldID)
	if err != nil {
		fx.t.Fatalf("readScalar: %v", err)
	}
	return stored
}

func TestGuardedUpdateAppliesWhenTheGuardHolds(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1") // Name = "Ada", no Age
	op := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(3, guardInt32(9)))
	answer, err := fx.writeOp(op, 1)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if answer.NotApplied || answer.Outcome != machine.OutcomeApplied || answer.Pos.Seq != 2 {
		t.Fatalf("answer = %+v, want applied at position 2", answer)
	}
	if got := fx.storedScalar("s1", 3); string(got) != string(guardInt32(9)) {
		t.Fatalf("Age = %x, want the update applied", got)
	}
}

func TestGuardedUpdateIsRefusedBeforeAPositionAndReturnsTheDocument(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	before := fx.pairs.Snapshot(testSlot, testDB)
	loggedBefore := len(fx.logged())

	op := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Bob")}}, setField(3, guardInt32(9)))
	op.Return = cluster.ReturnAfter
	requestID := [16]byte{9, 9, 9}
	answer, err := fx.w.Write(context.Background(), WriteRequest{Slot: testSlot, DBID: testDB, Op: op, RequestID: requestID, Deadline: time.Second})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !answer.NotApplied || answer.GuardField != 2 {
		t.Fatalf("answer = %+v, want NotApplied on field 2", answer)
	}
	scalars, err := DecodeScalarSet(answer.Result)
	if err != nil || string(scalars[2]) != string(guardString("Ada")) {
		t.Fatalf("the refusal must carry the current document: %v, %v", scalars, err)
	}
	if after := fx.pairs.Snapshot(testSlot, testDB); after.AppendedSeq != before.AppendedSeq || after.Ordinal != before.Ordinal || after.AppliedPos != before.AppliedPos {
		t.Fatalf("a refused guard took a position: %+v -> %+v", before, after)
	}
	if got := len(fx.logged()); got != loggedBefore {
		t.Fatalf("a refused guard wrote %d log records", got-loggedBefore)
	}
	if entry, _ := NewKVDedupeReader(fx.kv).Lookup(testSlot, testDB, testBucket, machine.DedupeKey{RequestID: requestID}); entry != nil {
		t.Fatalf("a refused guard left a dedupe entry")
	}
	if got := fx.storedScalar("s1", 3); got != nil {
		t.Fatalf("a refused guard changed Age to %x", got)
	}
}

func TestNullGuardMatchesAbsent(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1") // Age was never set
	taken, err := fx.writeOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardNull}}, setField(3, guardInt32(1))), 1)
	if err != nil || taken.NotApplied {
		t.Fatalf("a null guard on an absent field: %+v, %v, want it applied", taken, err)
	}
	again, err := fx.writeOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardNull}}, setField(3, guardInt32(2))), 2)
	if err != nil || !again.NotApplied || again.GuardField != 3 {
		t.Fatalf("a null guard on a set field: %+v, %v, want it refused on field 3", again, err)
	}
	named, err := fx.writeOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardNull}}, setField(3, guardInt32(3))), 3)
	if err != nil || !named.NotApplied {
		t.Fatalf("a null guard on a string: %+v, %v, want it refused", named, err)
	}
}

// A fresh document whose field was never set must not pass a guard on a value: the
// common state-machine case is a Status that is still unset.
func TestNonNullGuardOnAbsentFieldIsRefused(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1") // Age was never set
	refused, err := fx.writeOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardInt32(5)}}, setField(3, guardInt32(6))), 1)
	if err != nil || !refused.NotApplied || refused.GuardField != 3 {
		t.Fatalf("a guard on Age = 5 when Age is absent: %+v, %v, want it refused on field 3", refused, err)
	}
}

func TestGuardNumericEquality(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	if _, err := fx.writeOp(guardedUpdate(t, "s1", nil, setField(3, guardInt32(7))), 1); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// An ordered slice, not a map: the request id of each case is its position, so every
	// attempt is a new request and none is answered from the dedupe window.
	cases := []struct {
		name   string
		wanted []byte
		equal  bool
	}{
		{"a double", guardDouble(7), true},
		{"a long", guardInt64(7), true},
		{"a double 7.5", guardDouble(7.5), false},
	}
	for i, c := range cases {
		answer, err := fx.writeOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: c.wanted}}, setField(3, guardInt32(7))), byte(2+i))
		if err != nil {
			t.Fatalf("an int 7 guarded as %s: %v", c.name, err)
		}
		if answer.Deduped {
			t.Fatalf("an int 7 guarded as %s was answered from the dedupe window: %+v", c.name, answer)
		}
		if answer.NotApplied == c.equal {
			t.Errorf("an int 7 guarded as %s: %+v, want equal=%v by value", c.name, answer, c.equal)
		}
	}
}

func guardedDelete(t *testing.T, id string, guard []cluster.GuardTerm) cluster.Op {
	t.Helper()
	op := guardedUpdate(t, id, guard)
	op.Class, op.Fields = cluster.OpDelete, nil
	return op
}

func TestGuardedDeleteAndAbsentDeleteIsNotFound(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	refused, err := fx.writeOp(guardedDelete(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Bob")}}), 1)
	if err != nil || !refused.NotApplied || !fx.hasDocument(testSlot, "s1") {
		t.Fatalf("a failed guard on a delete: %+v, %v, want not applied and the document kept", refused, err)
	}
	removed, err := fx.writeOp(guardedDelete(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}), 2)
	if err != nil || removed.NotApplied || removed.Outcome != machine.OutcomeApplied {
		t.Fatalf("a held guard on a delete: %+v, %v, want applied", removed, err)
	}
	if _, err := fx.writeOp(guardedDelete(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}), 3); err == nil || codeOf(t, err) != cluster.CodeNotFound {
		t.Fatalf("a guarded delete of a deleted document: %v, want NOT_FOUND", err)
	}
	if _, err := fx.writeOp(guardedDelete(t, "ghost", nil), 4); err != nil {
		t.Fatalf("an unguarded delete of an absent document: %v, want it unchanged (no error)", err)
	}
}

func TestRetryOfAnAppliedGuardedUpdateIsNotReEvaluated(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	op := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardNull}}, setField(3, guardInt32(1)))
	first, err := fx.writeOp(op, 1)
	if err != nil || first.NotApplied {
		t.Fatalf("first attempt: %+v, %v", first, err)
	}
	retry, err := fx.writeOp(op, 1) // the guard no longer holds: Age is 1
	if err != nil || retry.NotApplied || !retry.Deduped || retry.Pos != first.Pos {
		t.Fatalf("retry = %+v, %v, want the first answer back, deduped", retry, err)
	}
}

func TestAGuardIsNeverInTheRecord(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	op := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(3, guardInt32(1)))
	op.Return = cluster.ReturnBefore
	if _, err := fx.writeOp(op, 1); err != nil {
		t.Fatalf("Write: %v", err)
	}
	entries := fx.logged()
	last := entries[len(entries)-1]
	record, err := storage.DecodeWALRecord(last.Op, last.Data)
	if err != nil {
		t.Fatalf("DecodeWALRecord: %v", err)
	}
	logged, err := cluster.DecodeOp(record.Body)
	if err != nil {
		t.Fatalf("DecodeOp: %v", err)
	}
	if len(logged.Guard) != 0 || logged.Return != cluster.ReturnBefore {
		t.Fatalf("logged op has guard %v and return %d, want no guard and return before", logged.Guard, logged.Return)
	}
}

// guardCounter is a DocumentApplier that counts the guard checks asked of it. Fill and
// FillWithResult are the real ones, so a count of zero means that no apply path, the replay
// or a copy's fillApplied, called CheckGuard on the applier it was given.
type guardCounter struct {
	*DocumentApplier
	checks int
}

func (c *guardCounter) CheckGuard(slot uint16, db uint64, op cluster.Op) (GuardVerdict, error) {
	c.checks++
	return c.DocumentApplier.CheckGuard(slot, db, op)
}

func TestReplayAndCopiesNeverEvaluateAGuard(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	live := &guardCounter{DocumentApplier: fx.w.cfg.Applier.(*DocumentApplier)}
	fx.w.cfg.Applier = live
	// The owner's write is the seam's proof: its guard is checked once, through the counter.
	owned, err := fx.writeOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardNull}}, setField(3, guardInt32(1))), 1)
	if err != nil || owned.NotApplied {
		t.Fatalf("Write: %+v, %v", owned, err)
	}
	if live.checks != 1 {
		t.Fatalf("the owner's write checked its guard %d times through the counter, want 1", live.checks)
	}
	entries := fx.logged()
	last := entries[len(entries)-1]
	record, err := storage.DecodeWALRecord(last.Op, last.Data)
	if err != nil {
		t.Fatalf("DecodeWALRecord: %v", err)
	}
	logged, err := cluster.DecodeOp(record.Body)
	if err != nil {
		t.Fatalf("DecodeOp: %v", err)
	}

	// A replay of the log runs over a document that already holds Age 1, where the guard
	// "Age is null" would fail: it must apply the record anyway, and ask no guard.
	pairs := fx.restartedFrom(cluster.Position{}, 0)
	replayed := &guardCounter{DocumentApplier: NewDocumentApplier(fx.kv, pairRecorder{})}
	fx.w.cfg.Applier = replayed
	fx.replayAll(pairs)
	if replayed.checks != 0 {
		t.Fatalf("the replay checked a guard %d times, want none", replayed.checks)
	}
	if got := fx.storedScalar("s1", 3); string(got) != string(guardInt32(1)) {
		t.Fatalf("Age = %x after the replay, want 1", got)
	}
	if pair := pairs.Snapshot(testSlot, testDB); pair.Flags&flagReplayNeeded != 0 {
		t.Fatalf("the replay fenced the pair: %+v", pair)
	}

	// A copy applies the same record through fillApplied on a node whose document has no Age
	// yet. It must take the record without a guard check, as the owner's Age 1 is what it stores.
	copyNode := newGuardFixture(t)
	copyNode.mustWrite(testSlot, "s1")
	copied := &guardCounter{DocumentApplier: NewDocumentApplier(copyNode.kv, nil).WithFieldEngines(scalarOnlyEngines{}).WithResultCap(1024)}
	batch := copyNode.kv.NewBatch()
	rec := AppliedRecord{Slot: testSlot, DB: testDB, Op: logged, Pos: owned.Pos, Ordinal: 2, RequestID: record.RequestID, OpIndex: record.OpIndex}
	if _, err := fillApplied(batch, copied, rec, machine.WindowConfig{Window: 8192, MaxResult: 1024}); err != nil {
		t.Fatalf("fillApplied on a copy: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the copy: %v", err)
	}
	if copied.checks != 0 {
		t.Fatalf("the copy checked a guard %d times, want none", copied.checks)
	}
	if got := copyNode.storedScalar("s1", 3); string(got) != string(guardInt32(1)) {
		t.Fatalf("copy Age = %x, want the owner's 1", got)
	}
}

// GuardObserved counts each guarded write once: a refusal as refused, an applied write as
// applied, and a deduped retry of an applied write not again. A plain update is not counted.
func TestGuardObservedCountsEachGuardedWriteOnce(t *testing.T) {
	fx := newGuardFixture(t)
	var applied, refused int
	fx.w.cfg.GuardObserved = func(ok bool) {
		if ok {
			applied++
		} else {
			refused++
		}
	}
	fx.mustWrite(testSlot, "s1")
	holds := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardNull}}, setField(3, guardInt32(1)))
	if _, err := fx.writeOp(holds, 1); err != nil {
		t.Fatalf("applied guarded write: %v", err)
	}
	if applied != 1 || refused != 0 {
		t.Fatalf("after an applied guarded write: applied=%d refused=%d, want 1 and 0", applied, refused)
	}
	fails := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardNull}}, setField(3, guardInt32(2)))
	if answer, err := fx.writeOp(fails, 2); err != nil || !answer.NotApplied {
		t.Fatalf("refused guarded write: %+v, %v", answer, err)
	}
	if applied != 1 || refused != 1 {
		t.Fatalf("after a refusal: applied=%d refused=%d, want 1 and 1", applied, refused)
	}
	if retry, err := fx.writeOp(holds, 1); err != nil || !retry.Deduped {
		t.Fatalf("retry: %+v, %v, want deduped", retry, err)
	}
	if applied != 1 || refused != 1 {
		t.Fatalf("after a deduped retry: applied=%d refused=%d, want still 1 and 1", applied, refused)
	}
	if _, err := fx.writeOp(guardedUpdate(t, "s1", nil, setField(3, guardInt32(9))), 3); err != nil {
		t.Fatalf("plain update: %v", err)
	}
	if applied != 1 || refused != 1 {
		t.Fatalf("after an unguarded update: applied=%d refused=%d, want unchanged", applied, refused)
	}
}

// A refusal writes no dedupe entry, so a retry of the same request id is evaluated again
// and applies once the guard holds.
func TestARetryOfARefusedGuardThatHasSinceBecomeTrueApplies(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	if _, err := fx.writeOp(guardedUpdate(t, "s1", nil, setField(3, guardInt32(5))), 1); err != nil {
		t.Fatalf("Age = 5: %v", err)
	}
	op := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 3, Value: guardNull}}, setField(3, guardInt32(9)))
	refused, err := fx.writeOp(op, 2)
	if err != nil || !refused.NotApplied {
		t.Fatalf("first attempt: %+v, %v, want refused", refused, err)
	}
	if _, err := fx.writeOp(guardedUpdate(t, "s1", nil, setField(3, guardNull)), 3); err != nil {
		t.Fatalf("clearing Age: %v", err)
	}
	retry, err := fx.writeOp(op, 2)
	if err != nil || retry.NotApplied || retry.Deduped {
		t.Fatalf("retry: %+v, %v, want it evaluated again and applied", retry, err)
	}
}
