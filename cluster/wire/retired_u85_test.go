package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestARecordNamingARetiredStoreAppliesAPopNAsAnEmptyWrite: a pop {n} on a field retired since the
// record was admitted writes nothing and records no result, as a single pop does.
func TestARecordNamingARetiredStoreAppliesAPopNAsAnEmptyWrite(t *testing.T) {
	node, _ := u30iFresh(t)
	applier := NewDocumentApplier(node.kv, nil).WithFieldEngines(retiredEngines{})
	arg, err := encodePopArg(3)
	if err != nil {
		t.Fatal(err)
	}
	op := cluster.Op{Class: cluster.OpStoreVerb, BucketID: 7, EncID: []byte("e"), Fields: []cluster.FieldOp{{FieldID: 5, Verb: cluster.VerbPop, Value: arg}}}
	batch := node.kv.NewBatch()
	defer batch.Discard()
	result, err := applier.FillWithResult(batch, AppliedRecord{Slot: 1, DB: 1, Op: op, Pos: cluster.Position{Epoch: 1, Seq: 1}})
	if err != nil || len(result) != 0 {
		t.Fatalf("a pop {n: 3} on a retired field = %x, %v, want an empty no-op", result, err)
	}
}

// TestADropWhileAPopNIsInFlightWedgesNoPair: a pop of three has passed its checks and holds its
// pair when an alter drops the field. It must apply as an empty no-op, the pair must keep
// ordering, and a restart must replay the record.
func TestADropWhileAPopNIsInFlightWedgesNoPair(t *testing.T) {
	node, dir := u30iFresh(t)
	ctx := context.Background()
	id := u40LaterDocument(t, node)
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(id, "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	target := StoreTarget{Database: u30iDB, Bucket: "User", ID: id, Field: "Purchases"}
	for _, label := range []string{"a", "b", "c", "d"} {
		if err := node.Push(ctx, PushRequest{StoreTarget: target, Element: u63Item(label)}); err != nil {
			t.Fatal(err)
		}
	}
	dbID, _ := node.database(u30iDB)
	gate := &u40GateLog{class: cluster.OpStoreVerb, inner: node.writer.cfg.Log, reached: make(chan struct{}), release: make(chan struct{})}
	node.writer.cfg.Log = gate

	popped := make(chan error, 1)
	go func() {
		_, err := node.PopN(ctx, PopNRequest{StoreTarget: target, Count: 3})
		popped <- err
	}()
	select {
	case <-gate.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the pop never reached its append")
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
	if _, err := node.bucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	close(gate.release)

	if err := <-popped; err != nil {
		t.Fatalf("the pop {n} admitted before the drop failed its apply: %v", err)
	}
	if err := <-dropped; err != nil {
		t.Fatalf("the drop: %v", err)
	}
	if _, err := node.Upsert(ctx, u30iDB, "User", u30iUser(id, "Grace", 40)); err != nil {
		t.Fatalf("a later write to the pair: %v", err)
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
}

// TestStoreGetOnARetiredFieldIsRefusedBeforeReading: the field is no longer declared, so a keyed
// read names FIELD_TYPE and reads nothing.
func TestStoreGetOnARetiredFieldIsRefusedBeforeReading(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.AlterBucket(ctx, u30iDB, "User", AlterChange{Drop: []string{"Purchases"}}); err != nil {
		t.Fatal(err)
	}
	_, err := node.GetField(ctx, StoreGetRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "User", ID: "x", Field: "Purchases"}, Key: "k"})
	u30iCode(t, err, cluster.CodeFieldType)
}
