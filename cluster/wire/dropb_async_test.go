package wire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// u42Async is the rig of the drop that returns before its sweep: the node's own hook frees the
// name, and the background loop is held off until a test lets it run.
func newU42Async(t *testing.T) *u42Rig {
	t.Helper()
	rig := newU42Rig(t)
	rig.node.retire.setOnSwept(rig.node.dropper.finishSwept)
	return rig
}

func (r *u42Rig) goneEventually() {
	r.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, found := r.declaration(); !found && !r.reverseFound() && r.keys(r.bucketID) == 0 {
			return
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("the drop never finished in the background: declaration %v, reverse %v, %d keys", func() bool { _, f := r.declaration(); return f }(), r.reverseFound(), r.keys(r.bucketID))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStartDropReturnsBeforeTheSweepEndsAndTheNodeFinishesIt: Start returns with the bucket
// dropping and every document still there; the node's own pass then deletes them and frees the
// name. A caller that has gone away loses nothing.
func TestStartDropReturnsBeforeTheSweepEndsAndTheNodeFinishesIt(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	orderKeys := rig.keys(rig.orderBucket())
	total := rig.keys(rig.bucketID)
	ctx, cancel := context.WithCancel(context.Background())
	id, err := rig.node.StartDropBucket(ctx, u30iDB, "User")
	cancel() // the client disconnects the moment the call returns
	if err != nil || id != rig.bucketID {
		t.Fatalf("StartDropBucket = %016x, %v", id, err)
	}
	if decl, found := rig.declaration(); !found || decl.State != machine.StateDropping {
		t.Fatalf("declaration %+v (found %v), want dropping", decl, found)
	}
	if got := rig.keys(rig.bucketID); got != total {
		t.Fatalf("the call returned with %d of %d keys: it swept inline", got, total)
	}
	if _, err := rig.node.Get(context.Background(), u30iDB, "User", rig.userDocs[0]); err == nil {
		t.Fatal("a dropping bucket answered a read")
	} else {
		u30iCode(t, err, cluster.CodeBucketDropping)
	}
	if _, err := rig.node.CreateBucket(context.Background(), u30iDB, "User", Field{Name: "Name", Type: "string"}); err == nil {
		t.Fatal("the name was free before the sweep ended")
	}
	if err := rig.node.retire.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	rig.wantGone(orderKeys)
	if _, err := rig.node.CreateBucket(context.Background(), u30iDB, "User", Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatalf("the name is not free after the sweep: %v", err)
	}
}

// TestARepeatedStartOfADroppingBucketFinishesIt: the second Start of a bucket that is already
// dropping answers its id and queues the sweep again.
func TestARepeatedStartOfADroppingBucketFinishesIt(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	first, err := rig.node.StartDropBucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	again, err := rig.node.StartDropBucket(context.Background(), u30iDB, "User")
	if err != nil || again != first {
		t.Fatalf("a repeat = %016x, %v, want %016x", again, err, first)
	}
	if rig.node.topology.Published(rig.dbID) != 2 {
		t.Fatalf("published = %d: the repeat fenced the database again", rig.node.topology.Published(rig.dbID))
	}
	if err := rig.node.retire.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, found := rig.declaration(); found {
		t.Fatal("the declaration survived")
	}
	_, err = rig.node.StartDropBucket(context.Background(), u30iDB, "User")
	u30iCode(t, err, cluster.CodeNotFound)
}

// TestARestartAfterAStartedDropFinishesItInTheBackground: the node stops right after Start
// returned; the reopened node sweeps by itself and frees the name.
func TestARestartAfterAStartedDropFinishesItInTheBackground(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	if _, err := rig.node.StartDropBucket(context.Background(), u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	rig.node.retire.cancel()
	rig.resume()
	if err := rig.node.Close(); err != nil {
		t.Fatal(err)
	}
	rig.node = u30iOpen(t, rig.dir)
	rig.goneEventually()
}

// TestACrashBetweenTheDeclarationAndReverseDeleteIsRepairedByTheLoop: freeing the name dies after
// the declaration is deleted; the last 'b' record is still there, and the next pass deletes the
// reverse document that a bare retry of the drop could not.
func TestACrashBetweenTheDeclarationAndReverseDeleteIsRepairedByTheLoop(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	dropper := rig.node.dropper
	dropper.crashAfter = func(step string) error {
		if step == dropStepDeclaration {
			return errU42Crash
		}
		return nil
	}
	if _, err := rig.node.StartDropBucket(context.Background(), u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	if err := rig.node.retire.Drain(context.Background()); !errors.Is(err, errU42Crash) {
		t.Fatalf("Drain = %v, want the crash", err)
	}
	if _, found := rig.declaration(); found {
		t.Fatal("the declaration survived its delete step")
	}
	if !rig.reverseFound() {
		t.Fatal("the reverse document is gone before its step")
	}
	dropper.crashAfter = nil
	if err := rig.node.retire.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rig.reverseFound() {
		t.Fatal("the orphan reverse document was not reclaimed")
	}
}
