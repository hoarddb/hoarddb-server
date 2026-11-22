package wire

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

func guardArray(t *testing.T, values ...any) []byte {
	t.Helper()
	kind, raw, err := bson.MarshalValue(bson.A(values))
	if err != nil {
		t.Fatalf("MarshalValue: %v", err)
	}
	return append([]byte{byte(kind)}, raw...)
}

func (fx *fixture) allStored(id string) map[uint16][]byte {
	fx.t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		fx.t.Fatalf("EncodeComponent: %v", err)
	}
	snapshot := fx.kv.NewSnapshot()
	defer snapshot.Discard()
	stored, err := allScalars(snapshot, store.DocumentPrefix(testSlot, testDB, testBucket, encoded))
	if err != nil {
		fx.t.Fatalf("allScalars: %v", err)
	}
	return stored
}

func equalScalars(a, b map[uint16][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for id, value := range a {
		if !bytes.Equal(value, b[id]) {
			return false
		}
	}
	return true
}

// seedGuardDocument inserts id with Name (2), Age (3), Tags (4) and Count (5) set.
func (fx *fixture) seedGuardDocument(t *testing.T, id string) {
	t.Helper()
	op := insertOp(id)
	op.Fields = []cluster.FieldOp{
		{FieldID: 2, Value: guardString("Ada")}, {FieldID: 3, Value: guardInt32(5)},
		{FieldID: 4, Value: guardArray(t, "x")}, {FieldID: 5, Value: guardInt32(10)},
	}
	if _, err := fx.w.Write(context.Background(), WriteRequest{Slot: testSlot, DBID: testDB, Op: op, RequestID: [16]byte{7, id[0]}, Deadline: time.Second}); err != nil {
		t.Fatalf("seeding %s: %v", id, err)
	}
}

// The document an update returns after is the one it stored: every operator, through the
// same code fillUpdate runs, then read back from Badger.
func TestUpdatedScalarsEqualTheStoredDocument(t *testing.T) {
	cases := map[string][]cluster.FieldOp{
		"set and unset":   {setField(2, guardString("Bob")), {FieldID: 3, OpCode: cluster.UpdateUnset}},
		"inc and mul":     {{FieldID: 3, OpCode: cluster.UpdateInc, Value: guardInt32(4)}, {FieldID: 5, OpCode: cluster.UpdateMul, Value: guardDouble(1.5)}},
		"min and max":     {{FieldID: 3, OpCode: cluster.UpdateMin, Value: guardInt32(2)}, {FieldID: 5, OpCode: cluster.UpdateMax, Value: guardInt64(99)}},
		"push":            {{FieldID: 4, OpCode: cluster.UpdatePush, Value: guardString("y")}},
		"addToSet":        {{FieldID: 4, OpCode: cluster.UpdateAddToSet, Value: guardString("x")}},
		"rename":          {{FieldID: 3, OpCode: cluster.UpdateRenameTo, Value: []byte{0, 6}}},
		"inc on an unset": {{FieldID: 6, OpCode: cluster.UpdateInc, Value: guardInt32(1)}},
	}
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	slices.Sort(names)
	for index, name := range names {
		t.Run(name, func(t *testing.T) {
			fx := newGuardFixture(t)
			id := fmt.Sprintf("d%d", index)
			fx.seedGuardDocument(t, id)
			op := guardedUpdate(t, id, nil, cases[name]...)
			op.Return = cluster.ReturnAfter
			answer, err := fx.writeOp(op, byte(index+1))
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			returned, truncated := splitGuardResult(answer.Result)
			if truncated || returned == nil {
				t.Fatalf("answer.Result = %x, want a document", answer.Result)
			}
			scalars, err := DecodeScalarSet(returned)
			if err != nil {
				t.Fatalf("DecodeScalarSet: %v", err)
			}
			if stored := fx.allStored(id); !equalScalars(scalars, stored) {
				t.Fatalf("returned %v, stored %v: the returned document drifted from the batch", scalars, stored)
			}
		})
	}
}

func TestReturnAfterAndBefore(t *testing.T) {
	fx := newGuardFixture(t)
	fx.seedGuardDocument(t, "r1")
	for mode, want := range map[byte]int32{cluster.ReturnBefore: 5, cluster.ReturnAfter: 6} {
		fresh := newGuardFixture(t)
		fresh.seedGuardDocument(t, "r1")
		op := guardedUpdate(t, "r1", nil, cluster.FieldOp{FieldID: 3, OpCode: cluster.UpdateInc, Value: guardInt32(1)})
		op.Return = mode
		answer, err := fresh.writeOp(op, 1)
		if err != nil {
			t.Fatalf("mode %d: %v", mode, err)
		}
		returned, _ := splitGuardResult(answer.Result)
		scalars, err := DecodeScalarSet(returned)
		if err != nil || string(scalars[3]) != string(guardInt32(want)) {
			t.Fatalf("mode %d: Age = %x (%v), want %d", mode, scalars[3], err, want)
		}
	}
}

func TestRetryOfAnAppliedGuardedUpdateIsDedupedWithItsDocument(t *testing.T) {
	fx := newGuardFixture(t)
	fx.seedGuardDocument(t, "r1")
	op := guardedUpdate(t, "r1", []cluster.GuardTerm{{FieldID: 3, Value: guardInt32(5)}}, cluster.FieldOp{FieldID: 3, OpCode: cluster.UpdateInc, Value: guardInt32(1)})
	op.Return = cluster.ReturnAfter
	first, err := fx.writeOp(op, 1)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := fx.writeOp(op, 1)
	if err != nil || !retry.Deduped || retry.DocumentTruncated {
		t.Fatalf("retry = %+v, %v, want deduped with its document", retry, err)
	}
	if !bytes.Equal(retry.Result, first.Result) {
		t.Fatalf("the retry's document %x differs from the first answer's %x", retry.Result, first.Result)
	}
}

func TestOverCapReturnIsTruncatedOnRetryNotOnFirstAnswer(t *testing.T) {
	fx := newGuardFixture(t)
	fx.seedGuardDocument(t, "r1")
	op := guardedUpdate(t, "r1", nil, setField(2, guardString(strings.Repeat("a", 3000))))
	op.Return = cluster.ReturnAfter
	first, err := fx.writeOp(op, 1)
	if err != nil {
		t.Fatalf("an over-cap document fenced or refused the write: %v", err)
	}
	if returned, truncated := splitGuardResult(first.Result); truncated || len(returned) < 3000 {
		t.Fatalf("the first answer is cut: %d bytes, truncated %v", len(returned), truncated)
	}
	retry, err := fx.writeOp(op, 1)
	if err != nil || !retry.Deduped || !retry.DocumentTruncated || len(retry.Result) != 0 {
		t.Fatalf("retry = %+v, %v, want deduped with DocumentTruncated and no result", retry, err)
	}
	if pair := fx.pairs.Snapshot(testSlot, testDB); pair.Flags&flagReplayNeeded != 0 {
		t.Fatalf("the over-cap result fenced the pair: %+v", pair)
	}
}

// A copy applies the logged record through fillApplied; its dedupe entry must be the owner's.
func TestACopyRecordsTheSameDedupeEntryAsTheOwner(t *testing.T) {
	owner := newGuardFixture(t)
	owner.seedGuardDocument(t, "r1")
	op := guardedUpdate(t, "r1", []cluster.GuardTerm{{FieldID: 3, Value: guardInt32(5)}}, cluster.FieldOp{FieldID: 3, OpCode: cluster.UpdateInc, Value: guardInt32(1)})
	op.Return = cluster.ReturnAfter
	answer, err := owner.writeOp(op, 1)
	if err != nil {
		t.Fatal(err)
	}
	ownerEntry, err := NewKVDedupeReader(owner.kv).Lookup(testSlot, testDB, testBucket, machine.DedupeKey{RequestID: [16]byte{9, 9, 1}})
	if err != nil || ownerEntry == nil || len(ownerEntry.Result) == 0 {
		t.Fatalf("the owner's entry = %+v, %v, want one that holds the document", ownerEntry, err)
	}

	copyNode := newGuardFixture(t)
	copyNode.seedGuardDocument(t, "r1")
	entries := owner.logged()
	last := entries[len(entries)-1]
	record, err := storage.DecodeWALRecord(last.Op, last.Data)
	if err != nil {
		t.Fatal(err)
	}
	logged, err := cluster.DecodeOp(record.Body)
	if err != nil {
		t.Fatal(err)
	}
	applier := NewDocumentApplier(copyNode.kv, nil).WithFieldEngines(scalarOnlyEngines{}).WithResultCap(1024)
	batch := copyNode.kv.NewBatch()
	rec := AppliedRecord{Slot: testSlot, DB: testDB, Op: logged, Pos: answer.Pos, Ordinal: 2, RequestID: record.RequestID, OpIndex: record.OpIndex}
	if _, err := fillApplied(batch, applier, rec, machine.WindowConfig{Window: 8192, MaxResult: 1024}); err != nil {
		t.Fatalf("fillApplied: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	copyEntry, err := NewKVDedupeReader(copyNode.kv).Lookup(testSlot, testDB, testBucket, machine.DedupeKey{RequestID: record.RequestID, OpIndex: record.OpIndex})
	if err != nil || copyEntry == nil {
		t.Fatalf("the copy's entry = %+v, %v", copyEntry, err)
	}
	if !bytes.Equal(copyEntry.Result, ownerEntry.Result) || copyEntry.Fingerprint != ownerEntry.Fingerprint {
		t.Fatalf("copy entry %+v, owner entry %+v: a retry after a failover would not match", copyEntry, ownerEntry)
	}
}

func TestGuardedUpdateCrossesAForward(t *testing.T) {
	op := guardedUpdate(t, "r1", []cluster.GuardTerm{{FieldID: 3, Value: guardInt32(5)}}, setField(3, guardInt32(6)))
	op.Return = cluster.ReturnAfter
	body, err := EncodeForwardWriteBody(WriteRequest{Op: op, RequestID: [16]byte{1}})
	if err != nil {
		t.Fatalf("EncodeForwardWriteBody: %v", err)
	}
	req, err := DecodeForwardWriteBody(body)
	if err != nil || len(req.Op.Guard) != 1 || req.Op.Return != cluster.ReturnAfter {
		t.Fatalf("forwarded request = %+v, %v, want the guard and return kept", req.Op, err)
	}
	for name, answer := range map[string]WriteAnswer{
		"not applied": {NotApplied: true, GuardField: 3, Result: EncodeScalarSet(map[uint16][]byte{0: guardString("r1"), 3: guardInt32(9)})},
		"truncated":   {Outcome: machine.OutcomeApplied, CopiesConfirmed: 1, Deduped: true, DocumentTruncated: true, Pos: cluster.Position{Epoch: 1, Seq: 2}},
		"applied":     {Outcome: machine.OutcomeApplied, CopiesConfirmed: 1, Pos: cluster.Position{Epoch: 1, Seq: 2}, Result: append([]byte{resultDocument}, EncodeScalarSet(map[uint16][]byte{3: guardInt32(6)})...)},
	} {
		got, err := DecodeForwardWriteReply(EncodeForwardWriteAnswer(answer))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.NotApplied != answer.NotApplied || got.GuardField != answer.GuardField || got.DocumentTruncated != answer.DocumentTruncated ||
			got.Deduped != answer.Deduped || !bytes.Equal(got.Result, answer.Result) || got.Pos != answer.Pos {
			t.Errorf("%s: crossed the forward as %+v, want %+v", name, got, answer)
		}
	}
	encoded := EncodeForwardWriteAnswer(WriteAnswer{Outcome: machine.OutcomeApplied, CopiesConfirmed: 1})
	encoded[1+1+16+1] |= 0x80 // the flags byte
	if _, err := DecodeForwardWriteReply(encoded); err == nil {
		t.Error("a reply with an unknown flag bit was accepted")
	}
}

// 100 allocations of the next number on one document, each reading before: the pair's
// critical section makes every one see a different value.
func TestIncWithReturnAllocatesASequence(t *testing.T) {
	fx := newGuardFixture(t)
	fx.seedGuardDocument(t, "r1") // Age is 5
	const allocations = 100
	got := make([]int32, allocations)
	var wait sync.WaitGroup
	for i := range allocations {
		wait.Add(1)
		go func() {
			defer wait.Done()
			op := guardedUpdate(t, "r1", nil, cluster.FieldOp{FieldID: 3, OpCode: cluster.UpdateInc, Value: guardInt32(1)})
			op.Return = cluster.ReturnBefore
			answer, err := fx.writeOp(op, byte(i+1))
			if err != nil {
				t.Errorf("allocation %d: %v", i, err)
				return
			}
			returned, _ := splitGuardResult(answer.Result)
			scalars, err := DecodeScalarSet(returned)
			if err != nil || len(scalars[3]) != 5 {
				t.Errorf("allocation %d: before = %x, %v", i, scalars[3], err)
				return
			}
			got[i] = int32(scalars[3][1]) | int32(scalars[3][2])<<8 | int32(scalars[3][3])<<16 | int32(scalars[3][4])<<24
		}()
	}
	wait.Wait()
	slices.Sort(got)
	for i, value := range got {
		if want := int32(5 + i); value != want {
			t.Fatalf("sorted allocations[%d] = %d, want %d: a number was handed out twice", i, value, want)
		}
	}
}
