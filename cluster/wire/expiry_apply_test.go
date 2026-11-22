package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	expiryDocField   uint16 = 4 // the datetime scalar the bucket form names
	expiryOtherField uint16 = 5
	expiryQueueField uint16 = claim34Field
	expiryPath              = "ts"
)

// expiryEngines is the engine and expire-table source of the applier tests: field 9 is a
// fifo that expires its elements at "ts".
type expiryEngines struct {
	engine store.FieldEngine
	table  machine.ExpireTable
}

func (e expiryEngines) FieldEngine(_, _ uint64, fieldID uint16) (store.FieldEngine, error) {
	if fieldID != expiryQueueField {
		return 0, errUndeclaredField
	}
	return e.engine, nil
}

// ExpireNames names the rig's bucket the way the node does from its cached declaration.
func (e expiryEngines) ExpireNames(uint64, uint64, uint16) (string, string, string, bool) {
	return "shop", "Session", "Log", true
}

func (e expiryEngines) ExpireTable(uint64, uint64) (machine.ExpireTable, error) { return e.table, nil }

func expiryTable() machine.ExpireTable {
	return machine.ExpireTable{
		BucketField: expiryDocField,
		Stores:      []machine.ExpireStore{{FieldID: expiryQueueField, Path: expiryPath}},
	}
}

// expiryRig applies records the way the owner and a copy do: one batch per record.
type expiryRig struct {
	t       *testing.T
	kv      storage.KV
	applier *DocumentApplier
	seq     uint64
	now     int64 // the owner's clock the rig stamps into a record that has expire items
	// declared is the expiry the sweep reads from the declaration; a test changes it as an alter would.
	declared *cluster.AdmittedExpire
}

func newExpiryRig(t *testing.T) *expiryRig { return newExpiryRigOf(t, store.FieldFIFO) }

func newExpiryRigOf(t *testing.T, engine store.FieldEngine) *expiryRig {
	t.Helper()
	kv := claim34New(t)
	declared := admittedExpireOf(expiryTable())
	return &expiryRig{t: t, kv: kv, now: 1, declared: declared, applier: NewDocumentApplier(kv, pairRecorder{}).
		WithFieldEngines(expiryEngines{engine: engine, table: expiryTable()}).WithResultCap(1 << 20)}
}

func datetimeScalar(millis int64) []byte {
	return binary.LittleEndian.AppendUint64([]byte{0x09}, uint64(millis))
}

func (rig *expiryRig) apply(op cluster.Op) []byte {
	rig.t.Helper()
	rig.seq++
	if len(op.Expire) > 0 && op.ExpireNow == 0 {
		op.ExpireNow = rig.now
	}
	logged := claim34Logged(rig.t, op)
	batch := rig.kv.NewBatch()
	result, err := rig.applier.FillWithResult(batch, AppliedRecord{
		Slot: testSlot, DB: testDB, Op: logged, Pos: cluster.Position{Epoch: 3, Seq: rig.seq}, Cursor: rig.seq, Ordinal: rig.seq,
	})
	if err != nil {
		rig.t.Fatalf("applying class 0x%02x: %v", op.Class, err)
	}
	if err := batch.Commit(true); err != nil {
		rig.t.Fatal(err)
	}
	return result
}

func expiryInsert(millis int64) cluster.Op {
	op := cluster.Op{
		Class: cluster.OpInsert, BucketID: testBucket, EncID: claim34EncID0(),
		IDValue: []byte{0x02, 4, 0, 0, 0, 'd', 'o', 'c', 0},
		Fields:  []cluster.FieldOp{{FieldID: expiryOtherField, Value: []byte{0x0a}}},
		Expire:  []cluster.ExpireItem{{FieldID: expiryDocField}},
	}
	if millis != 0 {
		op.Fields = append(op.Fields, cluster.FieldOp{FieldID: expiryDocField, Value: datetimeScalar(millis)})
	}
	return op
}

func expiryUpdate(code byte, millis int64) cluster.Op {
	field := cluster.FieldOp{FieldID: expiryDocField, OpCode: code}
	if code == cluster.UpdateSet {
		field.Value = datetimeScalar(millis)
	}
	return cluster.Op{
		Class: cluster.OpUpdate, BucketID: testBucket, EncID: claim34EncID0(),
		Fields: []cluster.FieldOp{field}, Expire: []cluster.ExpireItem{{FieldID: expiryDocField}},
	}
}

// expiryDelete is a delete stamped the way the owner stamps it: every declared item.
func expiryDelete() cluster.Op {
	op := cluster.Op{Class: cluster.OpDelete, BucketID: testBucket, EncID: claim34EncID0()}
	op.Expire = expireItemsOf(expiryTable(), op)
	return op
}

func expiryElement(t *testing.T, millis int64, label string) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: expiryPath, Value: primitive.DateTime(millis)}, {Key: "v", Value: label}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func expiryPush(t *testing.T, element []byte) cluster.Op {
	t.Helper()
	arg, err := bson.Marshal(elementArgDoc{V: element})
	if err != nil {
		t.Fatal(err)
	}
	return cluster.Op{
		Class: cluster.OpStoreVerb, BucketID: testBucket, EncID: claim34EncID0(),
		Fields: []cluster.FieldOp{{FieldID: expiryQueueField, Verb: cluster.VerbPush, Value: arg}},
		Expire: []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}},
	}
}

func expiryPop(t *testing.T, n int) cluster.Op {
	t.Helper()
	op := cluster.Op{
		Class: cluster.OpStoreVerb, BucketID: testBucket, EncID: claim34EncID0(),
		Fields: []cluster.FieldOp{{FieldID: expiryQueueField, Verb: cluster.VerbPop}},
		Expire: []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}},
	}
	if n > 0 {
		arg, err := bson.Marshal(popArgDoc{N: int32(n)})
		if err != nil {
			t.Fatal(err)
		}
		op.Fields[0].Value = arg
	}
	return op
}

// expiryQueue is every entry of the node's expiry queue, in key order.
func expiryQueue(t *testing.T, kv storage.KV) []storage.ExpiryEntry {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	prefix := storage.ExpiryPrefix()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer it.Close()
	var entries []storage.ExpiryEntry
	for it.Seek(nil); it.Valid(); it.Next() {
		entry, err := storage.ParseExpiryKey(bytes.Clone(it.Key()))
		if err != nil {
			t.Fatalf("a key in the expiry namespace does not parse: %v", err)
		}
		entries = append(entries, entry)
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return entries
}

func expiryTimes(entries []storage.ExpiryEntry) []int64 {
	times := make([]int64, 0, len(entries))
	for _, entry := range entries {
		times = append(times, entry.AtMillis)
	}
	return times
}

func TestExpiryEntryIsWrittenInTheDocumentBatchOnOwnerAndCopies(t *testing.T) {
	owner, copied := newExpiryRig(t), newExpiryRig(t)
	for _, rig := range []*expiryRig{owner, copied} {
		rig.apply(expiryInsert(1_700_000_000_000))
	}
	for name, rig := range map[string]*expiryRig{"owner": owner, "copy": copied} {
		entries := expiryQueue(t, rig.kv)
		if len(entries) != 1 {
			t.Fatalf("%s holds %d expiry entries after the insert, want 1", name, len(entries))
		}
		got := entries[0]
		if got.AtMillis != 1_700_000_000_000 || got.Kind != storage.ExpiryKindDocument || !bytes.Equal(got.EncID, claim34EncID0()) ||
			got.Slot != testSlot || got.DBID != testDB || got.BucketID != testBucket {
			t.Fatalf("%s entry = %+v, want the document at 1700000000000", name, got)
		}
		// The pair record is in the same batch as the entry: a batch that carries one carries both.
		snap := rig.kv.NewSnapshot()
		_, err := snap.Get(pairRecordKey(testSlot, testDB))
		snap.Discard()
		if err != nil {
			t.Fatalf("%s: the pair record is not with the entry: %v", name, err)
		}
	}
	if !reflect.DeepEqual(expiryQueue(t, owner.kv), expiryQueue(t, copied.kv)) {
		t.Fatal("the owner and the copy hold different queues after the same record")
	}
}

func TestInsertWithoutTheExpireFieldWritesNoEntry(t *testing.T) {
	rig := newExpiryRig(t)
	insert := expiryInsert(0)
	rig.apply(insert)
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("a document without the expire field has entries %+v, want none", entries)
	}
}

func TestUpdateOfTheExpireFieldMovesTheEntry(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	rig.apply(expiryUpdate(cluster.UpdateSet, 9_000))
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{9_000}) {
		t.Fatalf("after the update the queue holds %v, want only 9000: the old entry would remove the document early", got)
	}
}

func TestUnsetExpireFieldMakesTheDocumentPermanent(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	rig.apply(expiryUpdate(cluster.UpdateUnset, 0))
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("after unset the queue holds %+v, want none", entries)
	}
}

func TestDeleteDropsTheExpiryEntry(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	rig.apply(expiryDelete())
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("after the delete the queue holds %+v, want none", entries)
	}
}

// TestDeleteOfADocumentWithExpiringQueuesDropsTheirElementEntries: a store-form delete leaves no
// orphan queue-element entry for the elements it covers (build E2a: a delete stamps every item).
func TestDeleteOfADocumentWithExpiringQueuesDropsTheirElementEntries(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	for _, label := range []string{"a", "b", "c"} {
		rig.apply(expiryPush(t, expiryElement(t, 2_000, label)))
	}
	if entries := expiryQueue(t, rig.kv); len(entries) != 4 {
		t.Fatalf("before the delete the queue holds %d entries, want 4", len(entries))
	}
	rig.apply(expiryDelete())
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("after the delete the queue holds %+v, want none", entries)
	}
}

// TestDeleteOfAStoreOnlyExpiryBucketDropsTheElementEntries is the same for a bucket that declares
// no expiring scalar.
func TestDeleteOfAStoreOnlyExpiryBucketDropsTheElementEntries(t *testing.T) {
	rig := newExpiryRig(t)
	rig.applier.WithFieldEngines(expiryEngines{engine: store.FieldFIFO, table: machine.ExpireTable{Stores: expiryTable().Stores}})
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 2_000, "a")))
	op := cluster.Op{Class: cluster.OpDelete, BucketID: testBucket, EncID: claim34EncID0()}
	op.Expire = expireItemsOf(machine.ExpireTable{Stores: expiryTable().Stores}, op)
	rig.apply(op)
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("after the delete the queue holds %+v, want none", entries)
	}
}

func TestUpsertOverALiveDocumentMovesTheEntryAndAnEqualValueKeepsIt(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	upsert := expiryInsert(1_000)
	upsert.Class = cluster.OpUpsert
	rig.apply(upsert)
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{1_000}) {
		t.Fatalf("an upsert of the same time leaves %v, want [1000]", got)
	}
	upsert = expiryInsert(2_000)
	upsert.Class = cluster.OpUpsert
	rig.apply(upsert)
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{2_000}) {
		t.Fatalf("an upsert of a later time leaves %v, want [2000]", got)
	}
}

func TestInsertOverATombstoneWritesTheNewEntryEvenAtTheSameTime(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	rig.apply(expiryDelete())
	rig.apply(expiryInsert(1_000))
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{1_000}) {
		t.Fatalf("a document re-inserted at the same time holds %v, want [1000]: it would never expire", got)
	}
}

func TestQueuePushPopAckKeepTheEntriesExact(t *testing.T) {
	rig := newExpiryRig(t)
	// Two equal elements: the entries are keyed by position, not by value.
	rig.apply(expiryPush(t, expiryElement(t, 5_000, "a")))
	rig.apply(expiryPush(t, expiryElement(t, 5_000, "b")))
	rig.apply(expiryPush(t, expiryElement(t, 7_000, "c")))
	rig.apply(expiryPush(t, expiryElement(t, 8_000, "d")))
	entries := expiryQueue(t, rig.kv)
	if len(entries) != 4 {
		t.Fatalf("four pushes left %d entries, want 4", len(entries))
	}
	positions := map[uint64]bool{}
	for _, entry := range entries {
		if entry.Kind != storage.ExpiryKindElement || entry.FieldID != expiryQueueField {
			t.Fatalf("entry %+v is not a queue element of field %d", entry, expiryQueueField)
		}
		positions[entry.Position] = true
	}
	if len(positions) != 4 {
		t.Fatalf("the four entries share positions %v, want four distinct", positions)
	}
	rig.apply(expiryPop(t, 0)) // removes "a"
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{5_000, 7_000, 8_000}) {
		t.Fatalf("after a pop the queue holds %v, want [5000 7000 8000]", got)
	}
	rig.apply(expiryPop(t, 2)) // removes "b" and "c"
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{8_000}) {
		t.Fatalf("after pop {2} the queue holds %v, want [8000]", got)
	}
	claimOutcome := rig.claimAndAck()
	if claimOutcome == 0 {
		t.Fatal("the claim leased nothing")
	}
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("after the ack the queue holds %+v, want none", entries)
	}
}

// claimAndAck claims the one element left and acknowledges it, returning how many it leased.
func (rig *expiryRig) claimAndAck() int {
	rig.t.Helper()
	claim := claim34Op(cluster.OpClaim)
	claim.Count = 1
	claim = StampClaim(claim, 1_000, [16]byte{1, 2, 3})
	rig.seq++
	logged := claim34Logged(rig.t, claim)
	batch := rig.kv.NewBatch()
	result, err := rig.applier.FillWithResult(batch, AppliedRecord{Slot: testSlot, DB: testDB, Op: logged, Pos: cluster.Position{Epoch: 3, Seq: rig.seq}, Cursor: rig.seq, Ordinal: rig.seq})
	if err != nil {
		rig.t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		rig.t.Fatal(err)
	}
	var leaseID [16]byte
	copy(leaseID[:], result[:16])
	if got := expiryQueue(rig.t, rig.kv); len(got) != 1 {
		rig.t.Fatalf("a claim leaves %d entries, want 1: a lease holds the element, it does not remove it", len(got))
	}
	ack := claim34Lease(cluster.OpAck, leaseID, 2_000)
	ack.Expire = []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}
	rig.apply(ack)
	return 1
}

func TestExpiryWriteIsRefusedWhenTheElementPathIsNotADatetime(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	for name, tc := range map[string]struct {
		element []byte
		holds   string
	}{
		"a string": {mustMarshal(t, bson.D{{Key: expiryPath, Value: "tomorrow"}}), "a string"},
		"missing":  {mustMarshal(t, bson.D{{Key: "other", Value: int32(1)}}), "nothing"},
		"not bson": {[]byte("plain text"), "bytes that are not a BSON document"},
	} {
		op := expiryPush(t, tc.element)
		op.Expire = nil // the owner has not stamped yet at precondition time
		code, err := rig.applier.Precondition(testSlot, testDB, op)
		if err != nil || code != cluster.CodeInvalidArgument {
			t.Fatalf("%s: precondition = (%v, %v), want INVALID_ARGUMENT", name, code, err)
		}
		text, ok := rig.applier.Explain(code, testSlot, testDB, op)
		want := `Element pushed to "Session.Log" has no datetime at "ts" (it holds ` + tc.holds + `). The store expires its elements at that path, so the push was refused and nothing was written. Add the field to the element, e.g. {"ts": new Date()}, or run 'describe bucket Session in shop' to see the store's declaration.`
		if !ok || text != want {
			t.Fatalf("%s: explanation\n%q\nis not the verbatim W-E1 text\n%q", name, text, want)
		}
	}
	good := expiryPush(t, expiryElement(t, 1, "ok"))
	good.Expire = nil
	if code, err := rig.applier.Precondition(testSlot, testDB, good); err != nil || code != 0 {
		t.Fatalf("a datetime element: precondition = (%v, %v), want accepted", code, err)
	}
}

func mustMarshal(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestTheOwnerStampsTheRecordAndACopyBuildsTheSameQueue: a write through the Writer carries
// the expire items in its logged record, and a copy applying only that record (no declaration)
// ends with the owner's queue.
func TestTheOwnerStampsTheRecordAndACopyBuildsTheSameQueue(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3}
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	for _, id := range []string{"p1", "p2"} {
		op := insertOp(id)
		op.Fields = append(op.Fields, cluster.FieldOp{FieldID: 3, Value: datetimeScalar(1_700_000_000_000)})
		request := fx.request(testSlot, id)
		request.Op = op
		if _, err := fx.w.Write(context.Background(), request); err != nil {
			t.Fatalf("Write(%q): %v", id, err)
		}
	}
	owner := expiryQueue(t, fx.kv)
	if len(owner) != 2 {
		t.Fatalf("the owner holds %d entries after two inserts, want 2", len(owner))
	}
	copied := claim34New(t)
	applier := NewDocumentApplier(copied, pairRecorder{})
	for ordinal, entry := range fx.logged() {
		record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
		if err != nil {
			t.Fatal(err)
		}
		op, err := cluster.DecodeOp(record.Body)
		if err != nil {
			t.Fatal(err)
		}
		if len(op.Expire) != 1 || op.Expire[0].FieldID != 3 {
			t.Fatalf("the logged record carries expire items %+v, want field 3", op.Expire)
		}
		batch := copied.NewBatch()
		pos := cluster.Position{Epoch: testEpoch, Seq: uint64(ordinal + 1)}
		if err := applier.Fill(batch, AppliedRecord{Slot: testSlot, DB: testDB, Op: op, Pos: pos, Cursor: uint64(ordinal + 1), Ordinal: uint64(ordinal + 1)}); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(owner, expiryQueue(t, copied)) {
		t.Fatalf("the copy holds %+v, the owner %+v", expiryQueue(t, copied), owner)
	}
}

func TestRenameOntoTheExpireFieldMovesTheEntry(t *testing.T) {
	rig := newExpiryRig(t)
	insert := expiryInsert(0)
	insert.Expire = nil
	insert.Fields = append(insert.Fields, cluster.FieldOp{FieldID: 6, Value: datetimeScalar(3_000)})
	rig.apply(insert)
	rename := cluster.Op{
		Class: cluster.OpUpdate, BucketID: testBucket, EncID: claim34EncID0(),
		Fields: []cluster.FieldOp{{FieldID: 6, OpCode: cluster.UpdateRenameTo, Value: binary.BigEndian.AppendUint16(nil, expiryDocField)}},
	}
	if items := expireItemsOf(expiryTable(), rename); len(items) != 1 || items[0].FieldID != expiryDocField {
		t.Fatalf("expireItemsOf for a rename onto the expire field = %+v, want the bucket item", items)
	}
	rename.Expire = []cluster.ExpireItem{{FieldID: expiryDocField}}
	rig.apply(rename)
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{3_000}) {
		t.Fatalf("after the rename the queue holds %v, want [3000]", got)
	}
}

func TestUpdateOfAnExpiredDocumentIsRefusedNotFound(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	update := cluster.Op{Class: cluster.OpUpdate, BucketID: testBucket, EncID: claim34EncID0(), Fields: []cluster.FieldOp{{FieldID: expiryOtherField, OpCode: cluster.UpdateSet, Value: []byte{0x0a}}}}
	push := expiryPush(t, expiryElement(t, 9_000, "late"))
	push.Expire = nil
	for _, tc := range []struct {
		clock int64
		want  cluster.Code
	}{{500, 0}, {2_000, cluster.CodeNotFound}} {
		rig.applier.WithWallClock(func() int64 { return tc.clock })
		for name, op := range map[string]cluster.Op{"update": update, "push": push} {
			code, err := rig.applier.Precondition(testSlot, testDB, op)
			if err != nil || code != tc.want {
				t.Fatalf("%s at clock %d: precondition = (%v, %v), want %v: a write must not resurrect an expired document", name, tc.clock, code, err, tc.want)
			}
		}
	}
	text, ok := rig.applier.Explain(cluster.CodeNotFound, testSlot, testDB, update)
	if !ok || !strings.Contains(text, "expired") {
		t.Fatalf("explanation %q does not say the document expired", text)
	}
}

func TestInsertOverAnExpiredDocumentCleansBothPrefixes(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	rig.apply(expiryPush(t, expiryElement(t, 9_000, "old")))
	insert := expiryInsert(9_000)
	rig.applier.WithWallClock(func() int64 { return 500 })
	if code, err := rig.applier.Precondition(testSlot, testDB, insert); err != nil || code != cluster.CodeDocumentExists {
		t.Fatalf("insert over a live document: precondition = (%v, %v), want DOCUMENT_EXISTS", code, err)
	}
	rig.applier.WithWallClock(func() int64 { return 2_000 })
	if code, err := rig.applier.Precondition(testSlot, testDB, insert); err != nil || code != 0 {
		t.Fatalf("insert over an expired document: precondition = (%v, %v), want accepted", code, err)
	}
	rig.now = 2_000
	rig.apply(insert)
	snap := rig.kv.NewSnapshot()
	defer snap.Discard()
	ref := store.FieldRef{Owner: storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, claim34EncID0()), FieldID: expiryQueueField, Engine: store.FieldFIFO}
	if length, err := store.FieldLen(snap, ref); err != nil || length != 0 {
		t.Fatalf("the replaced document's queue holds %d elements (%v), want 0: the old data came back", length, err)
	}
	var documents []int64
	for _, entry := range expiryQueue(t, rig.kv) {
		if entry.Kind == storage.ExpiryKindDocument {
			documents = append(documents, entry.AtMillis)
		}
	}
	if !reflect.DeepEqual(documents, []int64{9_000}) {
		t.Fatalf("document entries after the replacement are %v, want [9000]: the old entry would remove the new document", documents)
	}
}

func TestExpiredQueueElementsAreSkippedAndRemovedByTheNextVerb(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "gone1")))
	rig.apply(expiryPush(t, expiryElement(t, 1_500, "gone2")))
	rig.apply(expiryPush(t, expiryElement(t, 9_000, "live")))
	rig.apply(expiryPush(t, expiryElement(t, 9_500, "later")))
	rig.now = 2_000
	result := rig.apply(expiryPop(t, 0))
	if text := claim34Text(t, result); text != "live" {
		t.Fatalf("pop returned %q, want live: the expired head must be skipped", text)
	}
	if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, []int64{9_500}) {
		t.Fatalf("after the pop the queue holds %v, want [9500]: the skipped elements and their entries are removed in the verb's batch", got)
	}
	snap := rig.kv.NewSnapshot()
	defer snap.Discard()
	ref := store.FieldRef{Owner: storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, claim34EncID0()), FieldID: expiryQueueField, Engine: store.FieldFIFO}
	if length, err := store.FieldLen(snap, ref); err != nil || length != 1 {
		t.Fatalf("the queue holds %d elements (%v), want 1", length, err)
	}
}

func TestPopOfAQueueOfOnlyExpiredElementsDeliversNothingAndEmptiesIt(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "a")))
	rig.apply(expiryPush(t, expiryElement(t, 1_100, "b")))
	rig.now = 5_000
	if result := rig.apply(expiryPop(t, 3)); len(result) != 0 {
		unpacked, _ := unpackPopped(result)
		if len(unpacked) != 0 {
			t.Fatalf("pop {3} delivered %d expired elements", len(unpacked))
		}
	}
	if entries := expiryQueue(t, rig.kv); len(entries) != 0 {
		t.Fatalf("entries remain after the queue emptied: %+v", entries)
	}
}

func TestPeekSkipsExpiredElementsWithoutRemoving(t *testing.T) {
	world := newExpiryReadWorld(t, 2_000)
	world.fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3, Stores: []machine.ExpireStore{{FieldID: 4, Path: expiryPath}}}
	world.put("q", 0)
	owner := storage.Key(world.slot, testDB, testBucket, storage.NSFieldStore, world.encID("q"))
	ref := store.FieldRef{Owner: owner, FieldID: 4, Engine: store.FieldFIFO}
	for _, element := range [][]byte{expiryElement(t, 1_000, "gone"), expiryElement(t, 9_000, "live")} {
		snap := world.fx.kv.NewSnapshot()
		batch := world.fx.kv.NewBatch()
		var parent store.BucketState
		if _, err := store.FieldPush(store.FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, element); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		snap.Discard()
	}
	request := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("q")}
	got, err := world.reader.Peek(context.Background(), PeekField{Request: request, FieldID: 4, Engine: store.FieldFIFO, Count: 2})
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(got.Elements) != 1 {
		t.Fatalf("peek returned %d elements, want only the live one", len(got.Elements))
	}
	snap := world.fx.kv.NewSnapshot()
	defer snap.Discard()
	if length, err := store.FieldLen(snap, ref); err != nil || length != 2 {
		t.Fatalf("after a peek the queue holds %d elements (%v), want 2: a read removes nothing", length, err)
	}
}

// TestTheStampUsesTheTableTheWriteWasAdmittedWith: the declaration cache is a bounded LRU, so a
// second read between admission and the append can find the entry gone or changed. The expire
// items must come from the declaration the write was admitted under, never from a fresh read.
func TestTheStampUsesTheTableTheWriteWasAdmittedWith(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3}
	admitted := fx.schema.decl.Expire
	fx.schema.beforeView = func(n int, s *fakeSchema) {
		if n >= 3 {
			s.decl.Expire = machine.ExpireTable{} // evicted and refetched without expiry, or published away
		}
	}
	op := insertOp("p1")
	op.Fields = append(op.Fields, cluster.FieldOp{FieldID: 3, Value: datetimeScalar(1_700_000_000_000)})
	request := fx.request(testSlot, "p1")
	request.Op = op
	if _, err := fx.w.Write(context.Background(), request); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := len(expiryQueue(t, fx.kv)); got != 1 {
		t.Fatalf("the write left %d expiry entries, want 1 from the admitted table %+v: it was silently unexpired", got, admitted)
	}
}

func encodeVerbDocForTest(key string) ([]byte, error) { return bson.Marshal(takeKeyArgDoc{K: key}) }

// TestPreconditionAndApplyAgreeAtTheBoundaryInstant: a document that expires exactly at the
// attempt's instant is absent to the insert precondition and replaced by the apply, on the owner
// and on a copy replaying the same record; one tick earlier it is live to both.
func TestPreconditionAndApplyAgreeAtTheBoundaryInstant(t *testing.T) {
	for _, tc := range []struct {
		now         int64
		wantCode    cluster.Code
		wantEntries []int64
	}{{4_999, cluster.CodeDocumentExists, []int64{5_000}}, {5_000, 0, []int64{9_000}}} {
		owner, copied := newExpiryRig(t), newExpiryRig(t)
		for _, rig := range []*expiryRig{owner, copied} {
			rig.apply(expiryInsert(5_000))
		}
		insert := expiryInsert(9_000)
		probe := insert
		probe.ExpireNow = tc.now
		if code, err := owner.applier.Precondition(testSlot, testDB, probe); err != nil || code != tc.wantCode {
			t.Fatalf("at %d the precondition = (%v, %v), want %v", tc.now, code, err, tc.wantCode)
		}
		if tc.wantCode != 0 {
			continue
		}
		for _, rig := range []*expiryRig{owner, copied} {
			rig.now = tc.now
			rig.apply(insert)
			if got := expiryTimes(expiryQueue(t, rig.kv)); !reflect.DeepEqual(got, tc.wantEntries) {
				t.Fatalf("at %d the apply left %v, want %v: the apply disagreed with the precondition", tc.now, got, tc.wantEntries)
			}
		}
	}
}

// TestTheWriterReadsTheClockOncePerAttempt: the precondition, the record's ExpireNow and the
// lease stamps share one instant.
func TestTheWriterReadsTheClockOncePerAttempt(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3}
	reads := 0
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}), Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
		WallMillis:   func() int64 { reads++; return 1_700_000_000_000 },
	})
	op := insertOp("p1")
	op.Fields = append(op.Fields, cluster.FieldOp{FieldID: 3, Value: datetimeScalar(1_800_000_000_000)})
	request := fx.request(testSlot, "p1")
	request.Op = op
	if _, err := fx.w.Write(context.Background(), request); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if reads != 1 {
		t.Fatalf("the writer read the wall clock %d times for one ordering attempt, want 1", reads)
	}
}

// TestPopOverAHugeExpiredBacklogStaysUnderTheBatchCap: a verb removes at most
// store.MaxExpiredPerVerb expired elements in its own batch and leaves the rest to the sweep, so
// a backlog larger than one batch can never fail the commit after the record was appended.
func TestPopOverAHugeExpiredBacklogStaysUnderTheBatchCap(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	total := store.MaxExpiredPerVerb*3 + 5
	for i := range total {
		rig.apply(expiryPush(t, expiryElement(t, int64(1_000+i), "old")))
	}
	rig.apply(expiryPush(t, expiryElement(t, 900_000, "live")))
	rig.now = 500_000
	rig.apply(expiryPop(t, 0))
	snap := rig.kv.NewSnapshot()
	defer snap.Discard()
	ref := store.FieldRef{Owner: storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, claim34EncID0()), FieldID: expiryQueueField, Engine: store.FieldFIFO}
	left, err := store.FieldLen(snap, ref)
	if err != nil {
		t.Fatal(err)
	}
	if removed := uint64(total+1) - left; removed != uint64(store.MaxExpiredPerVerb) {
		t.Fatalf("one pop removed %d elements, want exactly the cap %d: the rest belongs to the sweep", removed, store.MaxExpiredPerVerb)
	}
	if got := len(expiryQueue(t, rig.kv)); uint64(got) != left {
		t.Fatalf("%d entries for %d elements: every removed element's entry must go with it", got, left)
	}
}

// TestPeekUsesTheAdmittedDeclarationNotAFreshCacheRead: the declaration cache can lose or change
// an entry after the gate admitted the read. The peek must filter with the table it was admitted
// under; a second read that finds nothing would hand back expired elements.
func TestPeekUsesTheAdmittedDeclarationNotAFreshCacheRead(t *testing.T) {
	world := newExpiryReadWorld(t, 2_000)
	world.fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3, Stores: []machine.ExpireStore{{FieldID: 4, Path: expiryPath}}}
	world.put("q", 0)
	owner := storage.Key(world.slot, testDB, testBucket, storage.NSFieldStore, world.encID("q"))
	ref := store.FieldRef{Owner: owner, FieldID: 4, Engine: store.FieldFIFO}
	for _, element := range [][]byte{expiryElement(t, 1_000, "gone"), expiryElement(t, 9_000, "live")} {
		snap := world.fx.kv.NewSnapshot()
		batch := world.fx.kv.NewBatch()
		var parent store.BucketState
		if _, err := store.FieldPush(store.FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, element); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		snap.Discard()
	}
	request := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("q")}
	peek := PeekField{Request: request, FieldID: 4, Engine: store.FieldFIFO, Count: 2}
	admitted := world.fx.schema.decl.Expire
	world.fx.schema.decl.Expire = machine.ExpireTable{} // the cache has since lost the table
	snap := world.fx.kv.NewSnapshot()
	defer snap.Discard()
	got, err := world.reader.peekElements(snap, ref, peek, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Elements) != 1 {
		t.Fatalf("peek returned %d elements, want only the live one: it filtered with a table it read after admission", len(got.Elements))
	}
}

// TestAnUpsertThatOmitsTheExpireFieldReplacesAnExpiredDocumentWhole: the owner's precondition
// calls an expired document absent, so the apply must replace it too, though the upsert does not
// write the expire field. The record carries the instant for every insert and upsert into an
// expiring bucket, so the apply can tell; without it the old scalars and queue come back.
func TestAnUpsertThatOmitsTheExpireFieldReplacesAnExpiredDocumentWhole(t *testing.T) {
	for _, tc := range []struct {
		name         string
		now          int64
		wantOtherSet bool
		wantElements uint64
	}{{"expired at the instant", 2_000, false, 0}, {"not yet expired", 500, true, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newExpiryRig(t)
			first := expiryInsert(1_000)
			first.Fields = append(first.Fields, cluster.FieldOp{FieldID: 6, Value: []byte{0x0a}})
			rig.apply(first)
			rig.apply(expiryPush(t, expiryElement(t, 9_000, "old")))
			upsert := cluster.Op{
				Class: cluster.OpUpsert, BucketID: testBucket, EncID: claim34EncID0(),
				IDValue: []byte{0x02, 4, 0, 0, 0, 'd', 'o', 'c', 0},
				Fields:  []cluster.FieldOp{{FieldID: expiryOtherField, Value: []byte{0x0a}}},
			}
			upsert.Expire = expireItemsOf(expiryTable(), upsert)
			if len(upsert.Expire) != 1 {
				t.Fatalf("expireItemsOf stamped %d items for an upsert into an expiring bucket, want 1: the apply could not tell what the owner decided", len(upsert.Expire))
			}
			rig.now = tc.now
			rig.apply(upsert)
			snap := rig.kv.NewSnapshot()
			defer snap.Discard()
			prefix := store.DocumentPrefix(testSlot, testDB, testBucket, claim34EncID0())
			stale, err := readScalar(snap, prefix, 6)
			if err != nil {
				t.Fatal(err)
			}
			if (stale != nil) != tc.wantOtherSet {
				t.Fatalf("stale scalar present = %v, want %v", stale != nil, tc.wantOtherSet)
			}
			ref := store.FieldRef{Owner: storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, claim34EncID0()), FieldID: expiryQueueField, Engine: store.FieldFIFO}
			if length, err := store.FieldLen(snap, ref); err != nil || length != tc.wantElements {
				t.Fatalf("queue length %d (%v), want %d", length, err, tc.wantElements)
			}
		})
	}
}

func TestClaimSkipsAndRemovesExpiredElements(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "gone")))
	rig.apply(expiryPush(t, expiryElement(t, 9_000, "live")))
	rig.now = 2_000
	claim := claim34Op(cluster.OpClaim)
	claim.Count = 2
	claim = StampClaim(claim, 2_000, [16]byte{3})
	claim.Expire = []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}
	var leased [][]byte
	rig.applier.WithFieldEngines(expiryEngines{engine: store.FieldFIFO, table: expiryTable()})
	rig.seq++
	claim.ExpireNow = rig.now
	logged := claim34Logged(t, claim)
	batch := rig.kv.NewBatch()
	if _, err := rig.applier.FillWithResult(batch, AppliedRecord{
		Slot: testSlot, DB: testDB, Op: logged, Pos: cluster.Position{Epoch: 3, Seq: rig.seq}, Cursor: rig.seq, Ordinal: rig.seq,
		ClaimSink: func(elements [][]byte) { leased = elements },
	}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 || claim34Text(t, leased[0]) != "live" {
		t.Fatalf("the claim leased %d elements, want only the live one: it delivered a message past its time", len(leased))
	}
	// The expired element left the queue and its entry went; the leased one keeps its entry.
	if got := expiryTimes(expiryQueue(t, rig.kv)); len(got) != 1 || got[0] != 9_000 {
		t.Fatalf("entries after the claim = %v, want [9000]", got)
	}
}

func TestListAndCountOnAnExpiringQueueIgnoreExpiredElements(t *testing.T) {
	world := newExpiryReadWorld(t, 2_000)
	world.fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3, Stores: []machine.ExpireStore{{FieldID: 4, Path: expiryPath}}}
	world.put("q", 0)
	owner := storage.Key(world.slot, testDB, testBucket, storage.NSFieldStore, world.encID("q"))
	ref := store.FieldRef{Owner: owner, FieldID: 4, Engine: store.FieldFIFO}
	for _, element := range [][]byte{expiryElement(t, 1_000, "gone"), expiryElement(t, 9_000, "live"), expiryElement(t, 1_500, "gone2"), expiryElement(t, 9_500, "live2")} {
		snap := world.fx.kv.NewSnapshot()
		batch := world.fx.kv.NewBatch()
		var parent store.BucketState
		if _, err := store.FieldPush(store.FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, element); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		snap.Discard()
	}
	read := FieldRead{Request: ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("q")}, FieldID: 4, Engine: store.FieldFIFO, Limit: 10}
	count, err := world.reader.CountField(context.Background(), read)
	if err != nil || count != 2 {
		t.Fatalf("count = %d (%v), want 2 live elements", count, err)
	}
	page, err := world.reader.ListField(context.Background(), read)
	if err != nil || len(page.Elements) != 2 {
		t.Fatalf("list returned %d elements (%v), want 2", len(page.Elements), err)
	}
}

func TestAClientTakeOnAQueueIsStillRefused(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 9_000, "live")))
	take := cluster.Op{
		Class: cluster.OpStoreVerb, BucketID: testBucket, EncID: claim34EncID0(),
		Fields: []cluster.FieldOp{{FieldID: expiryQueueField, Verb: cluster.VerbTake, Value: takeArgAt(0, 9_000)}},
	}
	if code, err := rig.applier.Precondition(testSlot, testDB, take); err != nil || code != cluster.CodeInvalidArgument {
		t.Fatalf("a client take on a queue: precondition = (%v, %v), want INVALID_ARGUMENT: only the owner's own system write may take by position", code, err)
	}
}

func TestAPopRemovesTheExpiredHeadInTheSameBatchOnAllCopies(t *testing.T) {
	rigs := []*expiryRig{newExpiryRig(t), newExpiryRig(t), newExpiryRig(t)} // owner, copy, replay
	for _, rig := range rigs {
		rig.apply(expiryInsert(0))
		rig.apply(expiryPush(t, expiryElement(t, 1_000, "gone")))
		rig.apply(expiryPush(t, expiryElement(t, 9_000, "live")))
		rig.apply(expiryPush(t, expiryElement(t, 9_500, "later")))
		rig.now = 2_000
		rig.apply(expiryPop(t, 0))
	}
	want := expiryQueue(t, rigs[0].kv)
	if len(want) != 1 || want[0].AtMillis != 9_500 {
		t.Fatalf("owner queue = %+v, want only the last element's entry", want)
	}
	for i, rig := range rigs[1:] {
		if !reflect.DeepEqual(want, expiryQueue(t, rig.kv)) {
			t.Fatalf("holder %d holds a different queue: the copy kept the expired head or its entry", i+1)
		}
		snap := rig.kv.NewSnapshot()
		ref := store.FieldRef{Owner: storage.Key(testSlot, testDB, testBucket, storage.NSFieldStore, claim34EncID0()), FieldID: expiryQueueField, Engine: store.FieldFIFO}
		length, err := store.FieldLen(snap, ref)
		snap.Discard()
		if err != nil || length != 1 {
			t.Fatalf("holder %d queue length %d (%v), want 1", i+1, length, err)
		}
	}
}

func TestALeasedExpiredElementIsNotRemovedUntilReturned(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 5_000, "leased")))
	claim := claim34Op(cluster.OpClaim)
	claim.Count = 1
	claim = StampClaim(claim, 1_000, [16]byte{9})
	claim.Expire = []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}
	leaseID := rig.apply(claim)
	rig.now = 6_000 // the element is now past its time, but it is held under the lease
	if result := rig.apply(expiryPop(t, 0)); len(result) != 0 {
		t.Fatalf("a pop delivered %q: a leased element is not in the queue to pop", claim34Text(t, result))
	}
	if len(expiryQueue(t, rig.kv)) != 1 {
		t.Fatal("the leased element's entry was dropped while the lease held it")
	}
	ack := claim34Lease(cluster.OpAck, [16]byte(leaseID[:16]), 1_500)
	ack.Expire = []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}
	rig.apply(ack)
	if len(expiryQueue(t, rig.kv)) != 0 {
		t.Fatal("the ack left the entry")
	}
}

// The insert reserve (4 KB) must cover the largest expiry entry key an insert or a push adds:
// a 1024-byte id is the longest, and the key and its batch overhead stay a small part of it.
func TestTheInsertReserveCoversTheLargestExpiryEntryKey(t *testing.T) {
	longest := storage.ExpiryKey(storage.ExpiryEntry{
		AtMillis: 1, Slot: 1, DBID: 1, BucketID: 1, Kind: storage.ExpiryKindElement,
		EncID: make([]byte, 1024), FieldID: 1, Position: 1,
	})
	if cost := len(longest) + batchEntryOverhead; cost > insertBatchReserve/2 {
		t.Fatalf("the largest expiry key costs %d bytes of a %d byte reserve: charge it explicitly", cost, insertBatchReserve)
	}
}

// TestAWriteSeesAPendingExpiryOnlyOnceItsVersionIsPublished: `set expire at` rides the pending
// table, so a write admitted before the bump carries no expiry and one admitted after it does,
// on the same declaration.
func TestAWriteSeesAPendingExpiryOnlyOnceItsVersionIsPublished(t *testing.T) {
	for _, tc := range []struct {
		name      string
		published int64
		want      int
	}{{"before the bump", testSeen, 0}, {"after the bump", testSeen + 1, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			expire := machine.ExpireTable{BucketField: 3}
			fx.schema.decl.Pending = &machine.Pending{Version: testSeen + 1, Table: fx.schema.decl.Fields, Expire: &expire}
			fx.schema.published, fx.schema.seen = tc.published, tc.published
			op := insertOp("p1")
			op.DeclVersion, op.SchemaVersionSeen = tc.published, tc.published
			op.Fields = append(op.Fields, cluster.FieldOp{FieldID: 3, Value: datetimeScalar(1_800_000_000_000)})
			request := fx.request(testSlot, "p1")
			request.Op = op
			if _, err := fx.w.Write(context.Background(), request); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if got := len(fx.logged()); got != 1 {
				t.Fatalf("%d records logged", got)
			}
			entry := fx.logged()[0]
			record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
			if err != nil {
				t.Fatal(err)
			}
			logged, err := cluster.DecodeOp(record.Body)
			if err != nil {
				t.Fatal(err)
			}
			if len(logged.Expire) != tc.want {
				t.Fatalf("%s: the record carries %d expire items, want %d", tc.name, len(logged.Expire), tc.want)
			}
		})
	}
}

// TestThePreconditionJudgesExpiryUnderTheAdmittedDeclaration: the cache can lose or change the
// declaration between admission and the precondition. A write admitted under an expiry is judged
// under it, and one admitted under none is not judged under a later one.
func TestThePreconditionJudgesExpiryUnderTheAdmittedDeclaration(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(1_000))
	update := cluster.Op{Class: cluster.OpUpdate, BucketID: testBucket, EncID: claim34EncID0(),
		Fields: []cluster.FieldOp{{FieldID: expiryOtherField, OpCode: cluster.UpdateSet, Value: []byte{0x0a}}}, ExpireNow: 2_000}
	// The cache says the bucket has no expiry (an entry that was evicted and refetched), but the
	// write was admitted under one: the document is expired and the update is refused.
	rig.applier.WithFieldEngines(expiryEngines{engine: store.FieldFIFO, table: machine.ExpireTable{}})
	update.AdmittedExpire = &cluster.AdmittedExpire{BucketField: expiryDocField}
	if code, err := rig.applier.Precondition(testSlot, testDB, update); err != nil || code != cluster.CodeNotFound {
		t.Fatalf("admitted under an expiry: precondition = (%v, %v), want NOT_FOUND from the admitted table", code, err)
	}
	// The cache now says expiry, but the write was admitted under none: it is not judged expired.
	rig.applier.WithFieldEngines(expiryEngines{engine: store.FieldFIFO, table: expiryTable()})
	update.AdmittedExpire = &cluster.AdmittedExpire{}
	if code, err := rig.applier.Precondition(testSlot, testDB, update); err != nil || code != 0 {
		t.Fatalf("admitted under no expiry: precondition = (%v, %v), want accepted", code, err)
	}
}

// A pop's result is judged on the elements the apply delivers, not on the expired ones it steps
// over; otherwise a result over the dedupe cap is refused after the append and fences the pair.
func TestPopResultCapJudgesTheDeliveredElementNotTheExpiredHead(t *testing.T) {
	rig := newExpiryRig(t)
	rig.applier.WithResultCap(1024)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "small")))
	rig.apply(expiryPush(t, expiryElement(t, 9_000, strings.Repeat("L", 1500))))
	rig.now = 2_000
	pop := expiryPop(t, 0)
	pop.ExpireNow = rig.now
	if code, err := rig.applier.Precondition(testSlot, testDB, pop); err != nil || code != cluster.CodeResultTooLarge {
		t.Fatalf("precondition = (%v, %v), want RESULT_TOO_LARGE before a position: the live element behind the expired head is over the cap", code, err)
	}
}

func TestPopNResultCapJudgesTheDeliveredElements(t *testing.T) {
	rig := newExpiryRig(t)
	rig.applier.WithResultCap(1024)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "a")))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "b")))
	for range 3 {
		rig.apply(expiryPush(t, expiryElement(t, 9_000, strings.Repeat("x", 400))))
	}
	rig.now = 2_000
	pop := expiryPop(t, 3)
	pop.ExpireNow = rig.now
	if code, err := rig.applier.Precondition(testSlot, testDB, pop); err != nil || code != cluster.CodeResultTooLarge {
		t.Fatalf("precondition = (%v, %v), want RESULT_TOO_LARGE: pop {3} would deliver three 400-byte live elements", code, err)
	}
	// Large expired elements must not make a pop that delivers small live ones refuse.
	small := newExpiryRig(t)
	small.applier.WithResultCap(1024)
	small.apply(expiryInsert(0))
	small.apply(expiryPush(t, expiryElement(t, 1_000, strings.Repeat("e", 900))))
	small.apply(expiryPush(t, expiryElement(t, 9_000, "ok")))
	small.now = 2_000
	one := expiryPop(t, 0)
	one.ExpireNow = small.now
	if code, err := small.applier.Precondition(testSlot, testDB, one); err != nil || code != 0 {
		t.Fatalf("a pop that delivers a small live element was refused (%v, %v) because of the large expired head", code, err)
	}
}

// A claim skips expired elements as a pop does: they do not count toward N, so claim {n: 1} over
// one expired element and one live one leases the live one instead of leasing nothing.
func TestClaimOfOneOverAnExpiredHeadLeasesTheLiveElement(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(0))
	rig.apply(expiryPush(t, expiryElement(t, 1_000, "gone")))
	rig.apply(expiryPush(t, expiryElement(t, 9_000, "live")))
	rig.apply(expiryPush(t, expiryElement(t, 9_500, "later")))
	rig.now = 2_000
	claim := claim34Op(cluster.OpClaim)
	claim.Count = 1
	claim = StampClaim(claim, 2_000, [16]byte{7})
	claim.Expire = []cluster.ExpireItem{{FieldID: expiryQueueField, Path: []byte(expiryPath)}}
	claim.ExpireNow = rig.now
	var leased [][]byte
	rig.seq++
	logged := claim34Logged(t, claim)
	batch := rig.kv.NewBatch()
	if _, err := rig.applier.FillWithResult(batch, AppliedRecord{
		Slot: testSlot, DB: testDB, Op: logged, Pos: cluster.Position{Epoch: 3, Seq: rig.seq}, Cursor: rig.seq, Ordinal: rig.seq,
		ClaimSink: func(elements [][]byte) { leased = elements },
	}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 || claim34Text(t, leased[0]) != "live" {
		t.Fatalf("claim {n:1} leased %d elements, want the one live element behind the expired head", len(leased))
	}
	if got := expiryTimes(expiryQueue(t, rig.kv)); len(got) != 2 {
		t.Fatalf("entries after the claim = %v, want the leased and the later element's (the expired one's is gone)", got)
	}
}

// TestTheWriterJudgesThePreconditionAtItsOwnInstant pins the precondition to the attempt's instant at
// the Writer: the writer's clock reads the boundary instant while the applier's own clock reads earlier,
// and an insert over a document that expires exactly then must be admitted (it is absent at that
// instant), not refused DOCUMENT_EXISTS by the applier's earlier clock.
func TestTheWriterJudgesThePreconditionAtItsOwnInstant(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3}
	now := int64(1_000)
	applier := NewDocumentApplier(fx.kv, pairRecorder{}).
		WithFieldEngines(expiryEngines{engine: store.FieldFIFO, table: machine.ExpireTable{BucketField: 3}}).
		WithWallClock(func() int64 { return 4_000 }) // the applier's own clock stays earlier than the boundary
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Applier: applier, Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
		WallMillis:   func() int64 { return now },
	})
	write := func(id string) error {
		op := insertOp(id)
		op.Fields = append(op.Fields, cluster.FieldOp{FieldID: 3, Value: datetimeScalar(5_000)})
		request := fx.request(testSlot, id)
		request.Op = op
		request.RequestID = [16]byte{byte(now), 9, 9, 9}
		_, err := fx.w.Write(context.Background(), request)
		return err
	}
	if err := write("p1"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	now = 4_999
	if err := write("p1"); err == nil {
		t.Fatal("an insert over a live document was admitted one millisecond before it expires")
	}
	now = 5_000
	if err := write("p1"); err != nil {
		t.Fatalf("an insert at the boundary instant was refused (%v): the precondition judged at the applier's earlier clock", err)
	}
}

// TestTheWriterHandsThePreconditionTheAdmittedExpiry: the applier's own table (the cache it reads when
// nobody hands it one) has lost the expiry, but the write was admitted under a declaration that has it.
// An update of an expired document must be refused NOT_FOUND from the admitted table; a precondition
// that read the cache again would accept it and resurrect the document by changing it.
func TestTheWriterHandsThePreconditionTheAdmittedExpiry(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3}
	now := int64(500)
	applier := NewDocumentApplier(fx.kv, pairRecorder{}).
		WithFieldEngines(expiryEngines{engine: store.FieldFIFO, table: machine.ExpireTable{}}). // the cache has lost it
		WithWallClock(func() int64 { return now })
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Applier: applier, Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: fx.dedupe, Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
		WallMillis:   func() int64 { return now },
	})
	insert := insertOp("p1")
	insert.Fields = append(insert.Fields, cluster.FieldOp{FieldID: 3, Value: datetimeScalar(1_000)})
	request := fx.request(testSlot, "p1")
	request.Op = insert
	request.RequestID = [16]byte{1, 1, 1}
	if _, err := fx.w.Write(context.Background(), request); err != nil {
		t.Fatalf("insert: %v", err)
	}
	now = 2_000
	update := insertOp("p1")
	update.Class = cluster.OpUpdate
	update.IDValue = nil
	update.Fields = []cluster.FieldOp{{FieldID: 2, OpCode: cluster.UpdateSet, Value: []byte{0x02, 4, 0, 0, 0, 'B', 'o', 'b', 0}}}
	request.Op = update
	request.RequestID = [16]byte{2, 2, 2}
	_, err := fx.w.Write(context.Background(), request)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeNotFound {
		t.Fatalf("an update of an expired document: %v, want NOT_FOUND from the admitted expiry", err)
	}
}
