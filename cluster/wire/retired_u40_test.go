package wire

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u40GateLog holds the first write of a class until released, which is the write's fsync in miniature: the
// write has passed its checks and is in the log, and is not applied. A locked-path write is held in its
// append, which holds the pair lock; an RF 1 owner write is held in the flush that follows its unsynced
// append, which is the state the pair's pending list stands for.
type u40GateLog struct {
	class   byte
	inner   Appender
	reached chan struct{}
	release chan struct{}
	taken   atomic.Bool
	// gated is the cursor of the unsynced append the flush will hold.
	gated atomic.Uint64
}

func (g *u40GateLog) AppendFrom(bucket string, op byte, key, record []byte, origin storage.WALOrigin) (uint64, error) {
	if op == g.class {
		if g.taken.CompareAndSwap(false, true) {
			close(g.reached)
			<-g.release
		}
	}
	return g.inner.AppendFrom(bucket, op, key, record, origin)
}

// AppendUnsyncedFrom forwards, and remembers the first entry of the gated class for SyncThrough to hold.
func (g *u40GateLog) AppendUnsyncedFrom(bucket string, op byte, key, record []byte, origin storage.WALOrigin) (uint64, error) {
	cursor, err := g.inner.AppendUnsyncedFrom(bucket, op, key, record, origin)
	if err == nil && op == g.class {
		g.gated.CompareAndSwap(0, cursor)
	}
	return cursor, err
}

// SyncThrough holds the flush of the gated entry once, then forwards.
func (g *u40GateLog) SyncThrough(seq uint64) error {
	if gated := g.gated.Load(); gated != 0 && seq >= gated && g.taken.CompareAndSwap(false, true) {
		close(g.reached)
		<-g.release
	}
	return g.inner.SyncThrough(seq)
}

// SyncedSequence forwards to the inner log.
func (g *u40GateLog) SyncedSequence() uint64 { return g.inner.SyncedSequence() }

// u40LaterDocument finds a document id whose pair has a higher slot than the declaration's,
// so a bump's pass publishes on the declaration pair first and then waits for the document's.
func u40LaterDocument(t *testing.T, node *Node) string {
	t.Helper()
	declSlot := u40DeclSlot(t, "User")
	for i := range 500 {
		id := fmt.Sprintf("doc%d", i)
		if u40DocSlot(t, node, "User", id) > declSlot {
			return id
		}
	}
	t.Fatal("no document id lands after the declaration's slot")
	return ""
}

// TestADropWhileAStoreWriteIsInFlightWedgesNoPair: a push has passed its checks against the
// table that declares Purchases and holds its pair when an alter drops Purchases and the bump
// publishes the new table on another pair. The push must apply (its value lands under a retired
// id and is swept later), the pair must keep ordering, and a restart must replay the record.
func TestADropWhileAStoreWriteIsInFlightWedgesNoPair(t *testing.T) {
	node, dir := u30iFresh(t)
	ctx := context.Background()
	id := u40LaterDocument(t, node)
	if err := func() error { _, err := node.Insert(ctx, u30iDB, "User", u30iUser(id, "Ada", 36)); return err }(); err != nil {
		t.Fatal(err)
	}
	dbID, _ := node.database(u30iDB)
	gate := &u40GateLog{class: cluster.OpStoreVerb, inner: node.writer.cfg.Log, reached: make(chan struct{}), release: make(chan struct{})}
	node.writer.cfg.Log = gate

	pushed := make(chan error, 1)
	go func() {
		pushed <- node.Push(ctx, PushRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "User", ID: id, Field: "Purchases"}, Element: bson.D{{Key: "Item", Value: "tea"}}})
	}()
	select {
	case <-gate.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the push never reached its append")
	}
	dropped := make(chan error, 1)
	go func() {
		_, err := node.AlterBucket(ctx, u30iDB, "User", AlterChange{Drop: []string{"Purchases"}})
		dropped <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for node.topology.Published(dbID) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("the drop never published on the declaration pair")
		}
		time.Sleep(time.Millisecond)
	}
	// Any use of the bucket after the bump makes the node fetch the new declaration.
	if _, err := node.bucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	close(gate.release)

	if err := <-pushed; err != nil {
		t.Fatalf("the push admitted before the drop failed its apply: %v", err)
	}
	if err := <-dropped; err != nil {
		t.Fatalf("the drop: %v", err)
	}
	// The pair keeps ordering.
	if _, err := node.Upsert(ctx, u30iDB, "User", u30iUser(id, "Grace", 40)); err != nil {
		t.Fatalf("a later write to the pair: %v", err)
	}
	if flags := node.pairs.Snapshot(u40DocSlot(t, node, "User", id), dbID).Flags; flags&flagReplayNeeded != 0 {
		t.Fatalf("the pair is fenced for replay (flags %b)", flags)
	}
	// A restart replays the log, the push included.
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := u30iOpen(t, dir)
	doc, err := reopened.Get(ctx, u30iDB, "User", id)
	if err != nil || u30iString(t, doc, "Name") != "Grace" {
		t.Fatalf("after the restart: %v %v", doc, err)
	}
}

// retiredEngines is a FieldEngineSource for a bucket whose field 5 was a fifoStore and is now retired.
type retiredEngines struct{}

func (retiredEngines) FieldEngine(_, _ uint64, fieldID uint16) (store.FieldEngine, error) {
	if fieldID == 5 {
		return 0, fmt.Errorf("field 5 is retired: %w: %w", errRetiredField, errUndeclaredField)
	}
	return 0, errUndeclaredField
}

// TestARecordNamingARetiredStoreAppliesAsAnEmptyWrite: the apply of a store verb, claim or pop on
// a field retired since the record was admitted writes no element, records an empty result and
// returns no error, so the owner, a copy and a replay all reach the same state. This is the
// path a copy takes (Replicator -> Applier.Fill), so it is pinned without a stream.
func TestARecordNamingARetiredStoreAppliesAsAnEmptyWrite(t *testing.T) {
	node, _ := u30iFresh(t)
	applier := NewDocumentApplier(node.kv, nil).WithFieldEngines(retiredEngines{})
	for _, op := range []cluster.Op{
		{Class: cluster.OpStoreVerb, BucketID: 7, EncID: []byte("e"), Fields: []cluster.FieldOp{{FieldID: 5, Verb: cluster.VerbPop}}},
		{Class: cluster.OpClaim, BucketID: 7, EncID: []byte("e"), Count: 1, Fields: []cluster.FieldOp{{FieldID: 5}}},
		{Class: cluster.OpAck, BucketID: 7, EncID: []byte("e"), Fields: []cluster.FieldOp{{FieldID: 5}}},
	} {
		batch := node.kv.NewBatch()
		result, err := applier.FillWithResult(batch, AppliedRecord{Slot: 1, DB: 1, Op: op, Pos: cluster.Position{Epoch: 1, Seq: 1}})
		if err != nil {
			t.Errorf("class 0x%02x on a retired field failed its apply: %v", op.Class, err)
		}
		if len(result) != 0 {
			t.Errorf("class 0x%02x on a retired field recorded a result: %x", op.Class, result)
		}
		batch.Discard()
	}
}

// TestFieldEngineNamesARetiredField: a field in the retired list is told apart from one never declared.
func TestFieldEngineNamesARetiredField(t *testing.T) {
	r := newU40Rig(t)
	ctx := context.Background()
	ref, err := r.node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	purchases := u40ID(t, ref.table, "Purchases")
	r.run(AlterChange{Drop: []string{"Purchases"}})
	if _, err := r.node.bucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	engines := declaredEngines{node: r.node}
	_, err = engines.FieldEngine(r.dbID, ref.entry.Decl.BucketID, purchases)
	if !errors.Is(err, errRetiredField) || !errors.Is(err, errUndeclaredField) {
		t.Fatalf("a retired id = %v, want errRetiredField wrapping errUndeclaredField", err)
	}
	if _, err := engines.FieldEngine(r.dbID, ref.entry.Decl.BucketID, 999); errors.Is(err, errRetiredField) || !errors.Is(err, errUndeclaredField) {
		t.Fatalf("a never-declared id = %v", err)
	}
}

// TestADropWhileAnUpdateIsInFlightWedgesNoPair: an update that passed its checks naming Age is held
// at its append while a drop of Age publishes on another pair. The update's apply reads no
// declaration, so it writes its key under the retired id (swept later), the pair keeps ordering and
// a restart replays the record.
func TestADropWhileAnUpdateIsInFlightWedgesNoPair(t *testing.T) {
	node, dir := u30iFresh(t)
	ctx := context.Background()
	id := u40LaterDocument(t, node)
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(id, "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	dbID, _ := node.database(u30iDB)
	gate := &u40GateLog{class: cluster.OpUpdate, inner: node.writer.cfg.Log, reached: make(chan struct{}), release: make(chan struct{})}
	node.writer.cfg.Log = gate

	updated := make(chan error, 1)
	go func() {
		_, err := node.Update(ctx, UpdateRequest{Database: u30iDB, Bucket: "User", ID: id, Set: bson.D{{Key: "Age", Value: int32(41)}}})
		updated <- err
	}()
	select {
	case <-gate.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the update never reached its append")
	}
	dropped := make(chan error, 1)
	go func() {
		_, err := node.AlterBucket(ctx, u30iDB, "User", AlterChange{Drop: []string{"Age"}})
		dropped <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for node.topology.Published(dbID) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("the drop never published on the declaration pair")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := node.bucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	close(gate.release)
	if err := <-updated; err != nil {
		t.Fatalf("the update admitted before the drop failed: %v", err)
	}
	if err := <-dropped; err != nil {
		t.Fatalf("the drop: %v", err)
	}
	if _, err := node.Upsert(ctx, u30iDB, "User", u30iUser(id, "Grace", 40)); err == nil {
		t.Log("later write accepted")
	} else if !errors.Is(err, errUndeclaredField) {
		u30iCode(t, err, cluster.CodeFieldType)
	}
	if _, err := node.Update(ctx, UpdateRequest{Database: u30iDB, Bucket: "User", ID: id, Set: bson.D{{Key: "Name", Value: "Grace"}}}); err != nil {
		t.Fatalf("a later update to the pair: %v", err)
	}
	if flags := node.pairs.Snapshot(u40DocSlot(t, node, "User", id), dbID).Flags; flags&flagReplayNeeded != 0 {
		t.Fatalf("the pair is fenced for replay (flags %b)", flags)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := u30iOpen(t, dir)
	doc, err := reopened.Get(ctx, u30iDB, "User", id)
	if err != nil || u30iString(t, doc, "Name") != "Grace" {
		t.Fatalf("after the restart: %v %v", doc, err)
	}
	if _, err := bson.Raw(doc.Body).LookupErr("Age"); err == nil {
		t.Fatal("the retired Age reads back")
	}
}
