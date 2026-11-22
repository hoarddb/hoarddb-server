package wire

import (
	"context"
	"fmt"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// A plain update (no guard, no return) logs its client body plus the flags byte and nothing
// else: the trailer is the one byte the format already carries, the guard adds none, and
// the decoded record has no guard. This pins the per-write cost the cost statement names;
// the per-bucket cost is zero by construction (no declaration member, no key).
func TestGuardAddsNoBytesToAPlainWrite(t *testing.T) {
	fx := newGuardFixture(t)
	fx.mustWrite(testSlot, "s1")
	op := guardedUpdate(t, "s1", nil, setField(3, guardInt32(9)))
	if _, err := fx.writeOp(op, 1); err != nil {
		t.Fatalf("Write: %v", err)
	}
	entries := fx.logged()
	last := entries[len(entries)-1]
	record, err := storage.DecodeWALRecord(last.Op, last.Data)
	if err != nil {
		t.Fatalf("DecodeWALRecord: %v", err)
	}
	client, err := cluster.EncodeClientOp(op)
	if err != nil {
		t.Fatalf("EncodeClientOp: %v", err)
	}
	if want := len(client) + 1; len(record.Body) != want {
		t.Fatalf("record body is %d bytes, want the client body's %d plus the flags byte", len(record.Body), len(client))
	}
	logged, err := cluster.DecodeOp(record.Body)
	if err != nil || len(logged.Guard) != 0 || logged.Return != cluster.ReturnNone {
		t.Fatalf("logged op = %+v, %v, want no guard and no return", logged, err)
	}
}

// TestGuardAddsNoResidentBytes is the per-bucket cost of a guard (guarded-update spec
// section 11): a bucket whose history holds guarded writes keeps no more on the heap than
// one whose history holds the same writes unguarded. The guard is read from the document
// under the pair lock and never kept, so it adds no catalog field, key or object per bucket.
func TestGuardAddsNoResidentBytes(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 200 documents per history; skipped under -short")
	}
	const writes = 200
	plainObjects, plainScannable := guardHistoryResidentPerWrite(t, writes, false)
	guardedObjects, guardedScannable := guardHistoryResidentPerWrite(t, writes, true)
	if extra := guardedObjects - plainObjects; extra >= 0.1 {
		t.Errorf("a guarded history keeps %.4f more heap objects per write than an unguarded one; the limit is 0.1", extra)
	}
	if extra := guardedScannable - plainScannable; extra >= 16 {
		t.Errorf("a guarded history keeps %.2f more scannable heap bytes per write than an unguarded one; the limit is 16", extra)
	}
}

// guardHistoryResidentPerWrite builds a bucket's history of writes plain or guarded (each
// document is inserted first, then updated once) and returns the heap objects and scannable
// bytes the updates added, per write, after the collector has settled.
func guardHistoryResidentPerWrite(t *testing.T, writes int, guarded bool) (objectsPerWrite, scannablePerWrite float64) {
	t.Helper()
	fx := newGuardFixture(t)
	documentID := func(i int) string { return fmt.Sprintf("d%05d", i) }
	requestID := func(i int) [16]byte { return [16]byte{7, byte(i >> 8), byte(i), 1} }
	for i := range writes {
		insert := WriteRequest{Slot: testSlot, DBID: testDB, Op: insertOp(documentID(i)), RequestID: requestID(i), Deadline: time.Second}
		if _, err := fx.w.Write(context.Background(), insert); err != nil {
			t.Fatalf("inserting %s: %v", documentID(i), err)
		}
	}
	settleGuardHeap()
	objectsBefore, scannableBefore := readGuardHeapCounts()
	for i := range writes {
		var guard []cluster.GuardTerm
		if guarded {
			guard = []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}
		}
		update := WriteRequest{Slot: testSlot, DBID: testDB, Op: guardedUpdate(t, documentID(i), guard, setField(3, guardInt32(int32(i)))),
			RequestID: [16]byte{8, byte(i >> 8), byte(i), 1}, Deadline: time.Second}
		answer, err := fx.w.Write(context.Background(), update)
		if err != nil || answer.NotApplied {
			t.Fatalf("updating %s: %+v, %v", documentID(i), answer, err)
		}
	}
	settleGuardHeap()
	objectsAfter, scannableAfter := readGuardHeapCounts()
	return (float64(objectsAfter) - float64(objectsBefore)) / float64(writes), (float64(scannableAfter) - float64(scannableBefore)) / float64(writes)
}

func settleGuardHeap() {
	runtime.GC()
	runtime.GC()
}

func readGuardHeapCounts() (objects, scannableBytes uint64) {
	samples := []metrics.Sample{{Name: "/gc/heap/objects:objects"}, {Name: "/gc/scan/heap:bytes"}}
	metrics.Read(samples)
	return samples[0].Value.Uint64(), samples[1].Value.Uint64()
}
