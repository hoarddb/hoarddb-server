package wire

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// BUG-0771: outside a boot replay a bare "bucket not found" from the declaration read was
// taken for a drop, so a copy applying a shipped record, and recoverPair, wrote an empty
// record and moved past an element of a live bucket.

// bug0771Box is a one-member cluster node with Box { Name, Queue fifoStore } and the ids a
// store-verb record names.
type bug0771Box struct {
	node     *Node
	dbID     uint64
	bucketID uint64
	fieldID  uint16
	encID    []byte
	gone     *atomic.Bool
}

func bug0771Open(t *testing.T) bug0771Box {
	t.Helper()
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := bug0760Box(t, node, u30iDB)
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("b1")
	if err != nil {
		t.Fatal(err)
	}
	gone := &atomic.Bool{}
	node.fetcher.cfg.Reader = transientGone{inner: node.fetcher.cfg.Reader, gone: gone}
	return bug0771Box{node: node, dbID: dbID, bucketID: ref.entry.Decl.BucketID, fieldID: u40ID(t, ref.table, "Queue"), encID: encID, gone: gone}
}

// push is the record the owner would ship for a push of item to Queue.
func (b bug0771Box) push(t *testing.T, bucketID uint64, pos cluster.Position, item string) PairRecord {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "item", Value: item}})
	if err != nil {
		t.Fatal(err)
	}
	arg, err := encodeElementArg(raw)
	if err != nil {
		t.Fatal(err)
	}
	op := cluster.Op{Class: cluster.OpStoreVerb, BucketID: bucketID, DeclVersion: 7, SchemaVersionSeen: 7, EncID: b.encID,
		Fields: []cluster.FieldOp{{FieldID: b.fieldID, Verb: cluster.VerbPush, Value: arg}}}
	body, err := cluster.EncodeOp(op)
	if err != nil {
		t.Fatal(err)
	}
	return PairRecord{Pos: pos, Record: storage.WALRecord{Class: cluster.OpStoreVerb, Body: body}}
}

// elements are the items the copy's store holds for the queue, in key order.
func (b bug0771Box) elements(t *testing.T, copyNode *u32Node, bucketID uint64) []string {
	t.Helper()
	prefix := storage.Key(u32Slot, b.dbID, bucketID, storage.NSFieldStore, b.encID)
	snapshot := copyNode.kv.NewSnapshot()
	defer snapshot.Discard()
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer iter.Close()
	var items []string
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range []string{"e1", "e2", "e3"} {
			if bytes.Contains(value, []byte(item)) {
				items = append(items, item)
			}
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	return items
}

// applied is the copy's applied position for the box's database.
func (b bug0771Box) applied(copyNode *u32Node) cluster.Position {
	return copyNode.pairs.Snapshot(u32Slot, b.dbID).AppliedPos
}

// bug0771Copy is a copy whose applier resolves fields through the box node's declaration
// cache and reader, as the copy of a node does, with no SchemaReady in front of the append.
func bug0771Copy(t *testing.T, box bug0771Box) *u32Node {
	t.Helper()
	return newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.Applier = NewDocumentApplier(cfg.KV.(storage.KV), nil).WithFieldEngines(declaredEngines{node: box.node})
	})
}

func (b bug0771Box) frame(rec PairRecord) WriteFrame {
	return WriteFrame{Slot: u32Slot, DB: b.dbID, From: "owner", Epoch: u32Epoch, PairRecord: rec}
}

// TestACopyApplyOfAPushNeverTakesATransientNotFoundForADrop: the bucket is live, the copy's
// declaration cache is cold and the declaration owner answers "not found" once. The copy must
// refuse the record, append nothing and acknowledge nothing; once the owner answers, the owner's
// re-shipped records apply and the queue holds every element in order.
// [BREAK: a bare not-found is an empty write outside a boot replay]
func TestACopyApplyOfAPushNeverTakesATransientNotFoundForADrop(t *testing.T) {
	box := bug0771Open(t)
	copyNode := bug0771Copy(t, box)
	box.node.fetcher.Forget(box.dbID)
	first := box.frame(box.push(t, box.bucketID, u32Pos(7, 1), "e1"))
	second := box.frame(box.push(t, box.bucketID, u32Pos(7, 2), "e2"))

	box.gone.Store(true)
	result, err := copyNode.rep.OnWrite(first)
	if err == nil && result.Ack.Outcome == AckApplied {
		t.Fatalf("the copy acknowledged position 7:1 as applied while the declaration read said not found (applied %s, elements %v)", box.applied(copyNode), box.elements(t, copyNode, box.bucketID))
	}
	if !box.applied(copyNode).Zero() || copyNode.wal.LastSequence() != 0 {
		t.Fatalf("a refused record left traces: applied %s, log at %d", box.applied(copyNode), copyNode.wal.LastSequence())
	}
	if flags := copyNode.pairs.Snapshot(u32Slot, box.dbID).Flags; flags&flagReplayNeeded != 0 {
		t.Fatalf("the refusal fenced the pair for replay (flags %#x); nothing was appended", flags)
	}

	box.gone.Store(false)
	for _, frame := range []WriteFrame{first, second} {
		if got := copyNode.write(frame); got.Ack.Outcome != AckApplied || got.Ack.Pos != frame.Pos {
			t.Fatalf("the re-shipped record %s: ack %+v, want applied", frame.Pos, got.Ack)
		}
	}
	if got := box.elements(t, copyNode, box.bucketID); len(got) != 2 || got[0] != "e1" || got[1] != "e2" {
		t.Fatalf("the copy holds %v, want [e1 e2]", got)
	}
}

// TestACopyAppliesARecordOfAProvenDroppedBucketAsAnEmptyWriteAndKeepsGoing: a record naming a
// bucket whose drop this node holds the 'b' record for is an empty write (U42): the copy moves
// past it and the next record, of a live bucket, applies. A bucket with no proof is not.
func TestACopyAppliesARecordOfAProvenDroppedBucketAsAnEmptyWriteAndKeepsGoing(t *testing.T) {
	box := bug0771Open(t)
	copyNode := bug0771Copy(t, box)
	box.node.fetcher.Forget(box.dbID)
	const deadBucket = 0xDEAD0771
	dead := box.frame(box.push(t, deadBucket, u32Pos(7, 1), "e1"))
	live := box.frame(box.push(t, box.bucketID, u32Pos(7, 2), "e2"))

	if result, err := copyNode.rep.OnWrite(dead); err == nil && result.Ack.Outcome == AckApplied {
		t.Fatal("a bucket that reads as not found with no drop record was applied as an empty write")
	}
	// The retire loop deletes any 'b' record it finds on a live node, so it is held off until the
	// test ends, or it can sweep the record below before the copy reads it.
	box.node.retire.loopMu.Lock()
	t.Cleanup(box.node.retire.loopMu.Unlock)
	batch := box.node.kv.NewBatch()
	if err := batch.Set(BucketRecord{Slot: u32Slot, DBID: box.dbID, BucketID: deadBucket}.Key(), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	if got := copyNode.write(dead); got.Ack.Outcome != AckApplied || got.Ack.Pos != dead.Pos {
		t.Fatalf("a record of a proven-dropped bucket: ack %+v, want applied (empty write)", got.Ack)
	}
	if got := box.elements(t, copyNode, deadBucket); len(got) != 0 {
		t.Fatalf("an empty write stored %v", got)
	}
	if got := copyNode.write(live); got.Ack.Outcome != AckApplied || got.Ack.Pos != live.Pos {
		t.Fatalf("the record after the dropped bucket's: ack %+v, want applied: the stream wedged", got.Ack)
	}
	if got := box.elements(t, copyNode, box.bucketID); len(got) != 1 || got[0] != "e2" {
		t.Fatalf("the copy holds %v, want [e2]", got)
	}
}

// bug0771Failing makes the first store verb apply fail after its append, as a disk error
// would, and runs onFail then: the replay that follows is recoverPair's.
type bug0771Failing struct {
	*DocumentApplier
	onFail func()
	failed atomic.Bool
}

func (a *bug0771Failing) FillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	if rec.Op.Class == cluster.OpStoreVerb && a.failed.CompareAndSwap(false, true) {
		a.onFail()
		return nil, errors.New("disk on fire")
	}
	return a.DocumentApplier.FillWithResult(batch, rec)
}

// TestRecoverPairNeverTakesATransientNotFoundForADrop: a push is durable and its apply failed;
// before recoverPair replays it the declaration cache is cold and the owner answers "not
// found". The replay must not skip the record: the pair stays fenced, and once the owner
// answers the next write recovers it and the queue holds every acknowledged element.
// [BREAK: a bare not-found is an empty write outside a boot replay]
func TestRecoverPairNeverTakesATransientNotFoundForADrop(t *testing.T) {
	box := bug0771Open(t)
	node := box.node
	failing := &bug0771Failing{DocumentApplier: node.writer.cfg.Applier.(*DocumentApplier)}
	failing.onFail = func() {
		node.fetcher.Forget(box.dbID)
		box.gone.Store(true)
	}
	node.writer.cfg.Applier = failing

	target := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}
	if err := node.Push(context.Background(), PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: "e1"}}}); err == nil {
		t.Fatal("a push whose apply failed was acknowledged")
	}
	box.gone.Store(false)
	bug0760Push(t, node, u30iDB, "e2")
	got := bug0760Queue(t, node, u30iDB)
	if len(got) != 2 || got[0] != "e1" || got[1] != "e2" {
		t.Fatalf("queue = %v, want [e1 e2]: recoverPair skipped the durable push on a not-found", got)
	}
}

// TestAWarmOwnerPathIsUnchangedByTheNotFoundRule: the owner admits a write with a warm cache, so
// a declaration owner that answers "not found" afterwards changes nothing for it.
func TestAWarmOwnerPathIsUnchangedByTheNotFoundRule(t *testing.T) {
	box := bug0771Open(t)
	bug0760Push(t, box.node, u30iDB, "e1")
	box.gone.Store(true)
	bug0760Push(t, box.node, u30iDB, "e2")
	box.gone.Store(false)
	if got := bug0760Queue(t, box.node, u30iDB); len(got) != 2 || got[0] != "e1" || got[1] != "e2" {
		t.Fatalf("queue = %v, want [e1 e2]", got)
	}
}
