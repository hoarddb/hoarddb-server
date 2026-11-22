package wire

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Storage-format slice A, unit F5, the wire side: the ordering owner alone stamps, a
// forwarded frame carries the client's bytes unchanged, the fingerprint ignores the stamp,
// and a stamped record replays to the same applied state.

func f5Stamp() cluster.IndexStamp {
	return cluster.IndexStamp{Fields: []uint16{2}, Claims: []cluster.UniqueClaim{{FieldID: 2, ClaimedAt: 42}}}
}

func TestAForwardedWriteIsStampedByTheOwnerNotTheForwarder(t *testing.T) {
	req := WriteRequest{Slot: testSlot, DBID: testDB, Op: insertOp("p1"), RequestID: [16]byte{1, 2, 3}, Deadline: time.Second, Hop: 1}
	forwarded, err := EncodeForwardWriteBody(req)
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.EncodeClientOp(req.Op)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(forwarded, client) {
		t.Fatalf("the forwarded body does not carry the client's bytes unchanged:\n %x\n %x", forwarded, client)
	}
	if record, _ := cluster.EncodeOp(req.Op); bytes.Contains(forwarded, record) {
		t.Fatal("the forwarded body carries a record body (a flags byte): a forwarder stamps nothing")
	}
	decoded, err := DecodeForwardWriteBody(forwarded)
	if err != nil || !decoded.Op.Stamp.Empty() {
		t.Fatalf("DecodeForwardWriteBody = stamp %+v, %v; want no stamp", decoded.Op.Stamp, err)
	}

	// The owner stamps under the pair lock when it orders the write.
	fx := newFixture(t)
	stamped := 0
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
		StampIndexes: func(op cluster.Op) (cluster.IndexStamp, error) {
			stamped++
			return f5Stamp(), nil
		},
	})
	if _, err := fx.w.Write(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if stamped != 1 {
		t.Fatalf("the owner called StampIndexes %d times for one write, want 1", stamped)
	}
	entries := fx.logged()
	record, err := storage.DecodeWALRecord(entries[0].Op, entries[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	op, err := cluster.DecodeOp(record.Body)
	if err != nil {
		t.Fatalf("DecodeOp of the owner's record: %v", err)
	}
	if record.Body[1] != 0x01 || op.Stamp.Empty() || op.Stamp.Fields[0] != 2 || op.Stamp.Claims[0].ClaimedAt != 42 {
		t.Fatalf("the owner's record carries flags %#02x and stamp %+v, want flags 0x01 and %+v", record.Body[1], op.Stamp, f5Stamp())
	}
}

func TestTheFingerprintIgnoresFlagsAndStamp(t *testing.T) {
	plain := insertOp("p1")
	stamped := insertOp("p1")
	stamped.Stamp = f5Stamp()
	if opFingerprint(plain) != opFingerprint(stamped) {
		t.Fatal("the fingerprint changed with the stamp: a retry after an alter that changed the index set would be refused as a reused id")
	}
	other := insertOp("p2")
	if opFingerprint(plain) == opFingerprint(other) {
		t.Fatal("the fingerprint does not see the document")
	}
}

// TestAStampedRecordReplaysToTheSameState: a record the owner stamped is replayed from the
// log alone into a fresh store and yields the same document, so the stamp is read past by
// the replay and the applier.
func TestAStampedRecordReplaysToTheSameState(t *testing.T) {
	rig := newU31Rig(t, 8192)
	rig.w = NewWriter(WriterConfig{
		Topology: rig.topo, Pairs: rig.pairs, Log: rig.wal, KV: rig.kv,
		Applier: u31Applier{docs: NewDocumentApplier(rig.kv, pairRecorder{}), applied: &rig.applied},
		Clock:   fixedClock{}, Lease: rig.lease, Dedupe: NewKVDedupeReader(rig.kv), Schema: rig.schema,
		Replication: rig.repl, WCTimeout: time.Second, DedupeWindow: rig.window,
		StampIndexes: func(cluster.Op) (cluster.IndexStamp, error) { return f5Stamp(), nil },
	})
	if _, err := rig.w.Write(context.Background(), rig.request(testSlot, "p1")); err != nil {
		t.Fatal(err)
	}
	if !rig.hasDocument(testSlot, "p1") {
		t.Fatal("the owner did not apply the write")
	}
	record, err := storage.DecodeWALRecord(rig.logged()[0].Op, rig.logged()[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	if record.Body[1] != 0x01 {
		t.Fatalf("the logged body's flags are %#02x, want 0x01", record.Body[1])
	}

	rig.freshStore()
	pairs := &cluster.PairSet{}
	pairs.With(testSlot, testDB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved })
	rig.rebuild(pairs)
	rig.replayAll(pairs)
	if !rig.hasDocument(testSlot, "p1") {
		t.Fatal("the replay of the stamped record did not rebuild the document")
	}
	if got := pairs.Snapshot(testSlot, testDB).AppliedPos; got != (cluster.Position{Epoch: testEpoch, Seq: 1}) {
		t.Fatalf("applied position after the replay = %s, want %d:1", got, testEpoch)
	}
}
