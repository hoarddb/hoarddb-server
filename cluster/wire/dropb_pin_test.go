package wire

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// u42Recreate drops User with a second coordinator, creates it again under a new id and gives
// it documents, and returns the new id.
func u42Recreate(t *testing.T, rig *u42Rig) uint64 {
	t.Helper()
	ctx := context.Background()
	if err := NewDropBucket(rig.drop.cfg).Run(ctx, rig.dbID, "User"); err != nil {
		t.Fatalf("the second drop: %v", err)
	}
	id, err := rig.node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"}, Field{Name: "Purchases", Type: "fifoStore"})
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range []string{"fresh1", "fresh2", "fresh3"} {
		if _, err := rig.node.Insert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: doc}, {Key: "Name", Value: doc}}); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func u42WantSurvivor(t *testing.T, rig *u42Rig, id uint64, keys int) {
	t.Helper()
	decl, found := rig.declaration()
	if !found || decl.BucketID != id || decl.State != machine.StateActive || decl.Pending != nil {
		t.Fatalf("the re-created bucket's declaration is %+v (found %v), want active with id %016x and no pending table", decl, found, id)
	}
	for _, doc := range []string{"fresh1", "fresh2", "fresh3"} {
		if _, err := rig.node.Get(context.Background(), u30iDB, "User", doc); err != nil {
			t.Fatalf("the re-created bucket lost %s: %v", doc, err)
		}
	}
	if got := rig.keys(id); got != keys {
		t.Fatalf("the re-created bucket holds %d keys, want %d", got, keys)
	}
}

// TestASlowDropRunNeverDropsTheRecreatedBucket: drop A is paused after its first sweep, a second
// drop finishes A, the name is created again as B, and the first drop resumes. Run, with its
// retry loop, must stop at the changed identity and leave B whole.
func TestASlowDropRunNeverDropsTheRecreatedBucket(t *testing.T) {
	rig := newU42Rig(t)
	slow := rig.drop
	reached, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	slow.crashAfter = func(step string) error {
		if step == u42Sweep {
			once.Do(func() { close(reached); <-resume })
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- slow.Run(context.Background(), rig.dbID, "User") }()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the slow drop never reached its sweep")
	}
	id := u42Recreate(t, rig)
	keys := rig.keys(id)
	close(resume)
	select {
	case err := <-done:
		_ = err
	case <-time.After(20 * time.Second):
		t.Fatal("the slow drop never returned")
	}
	u42WantSurvivor(t, rig, id, keys)
}

// TestADropRunOverwhelmedBetweenReadAndMarkerLeavesTheRecreatedBucket: the run has read the
// active declaration of A when A is dropped and created again as B; the owner-side rewrite of the
// marker must see B's id and refuse.
func TestADropRunOverwhelmedBetweenReadAndMarkerLeavesTheRecreatedBucket(t *testing.T) {
	rig := newU42Rig(t)
	slow := rig.drop
	reached, resume := make(chan struct{}), make(chan struct{})
	slow.beforeMark = func() { close(reached); <-resume }
	done := make(chan error, 1)
	go func() { done <- slow.Run(context.Background(), rig.dbID, "User") }()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the slow drop never reached its marker")
	}
	id := u42Recreate(t, rig)
	keys := rig.keys(id)
	close(resume)
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the slow drop never returned")
	}
	u42WantSurvivor(t, rig, id, keys)
}

// TestAnAlterNeverWritesOntoARecreatedBucket: the alter has read A when A is dropped and created
// again as B; the pending table must not land on B.
func TestAnAlterNeverWritesOntoARecreatedBucket(t *testing.T) {
	r := newU40Rig(t)
	ctx := context.Background()
	recreated := false
	var id uint64
	r.sender.before = func(n int) {
		if n != 0 || recreated {
			return
		}
		recreated = true
		if err := r.node.DropBucket(ctx, u30iDB, "User"); err != nil {
			t.Error(err)
			return
		}
		var err error
		if id, err = r.node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"}); err != nil {
			t.Error(err)
		}
	}
	_, err := r.alter.Run(ctx, r.dbID, "User", AlterChange{Drop: []string{"City"}})
	if err == nil {
		t.Fatal("an alter of a bucket that was dropped and created again reported success")
	}
	decl := r.declaration()
	if decl.BucketID != id || decl.Pending != nil || len(decl.Fields.Retired) != 0 {
		t.Fatalf("the re-created bucket's declaration was changed by the old alter: %+v", decl)
	}
	_ = cluster.CodeNotFound
}

// TestAnAlterQueuedBehindAnotherNeverWritesOntoARecreatedBucket: the pin holds across the turn in
// the alter queue. The alter waits behind another; the bucket is dropped and created again meanwhile.
func TestAnAlterQueuedBehindAnotherNeverWritesOntoARecreatedBucket(t *testing.T) {
	r := newU40Rig(t)
	ctx := context.Background()
	queue := r.node.alterer.cfg.Queue
	if queue == nil {
		t.Skip("the node has no alter queue")
	}
	release, err := queue.wait(ctx, r.dbID)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := r.alter.Run(ctx, r.dbID, "User", AlterChange{Drop: []string{"City"}})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := r.node.DropBucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	id, err := r.node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"})
	if err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-done; err == nil {
		t.Fatal("an alter that queued behind the drop of its bucket was applied to the new one")
	}
	decl := r.declaration()
	if decl.BucketID != id || decl.Pending != nil || len(decl.Fields.Retired) != 0 {
		t.Fatalf("the re-created bucket was changed: %+v", decl)
	}
}
