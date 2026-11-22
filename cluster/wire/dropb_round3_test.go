package wire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestADropThatLostItsNameToARecreateSaysSoAndDropsNothing: Start has pinned bucket A when another
// run drops A and a client creates B. Start must not answer "dropping A" while User is live as
// B: it is refused, naming the new bucket and the command that would drop it, and B is whole.
func TestADropThatLostItsNameToARecreateSaysSoAndDropsNothing(t *testing.T) {
	rig := newU42Rig(t)
	slow := rig.drop
	reached, resume := make(chan struct{}), make(chan struct{})
	slow.beforeMark = func() { close(reached); <-resume }
	type result struct {
		id  uint64
		err error
	}
	done := make(chan result, 1)
	go func() {
		id, err := slow.Start(context.Background(), rig.dbID, "User")
		done <- result{id, err}
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("Start never reached its marker")
	}
	id := u42Recreate(t, rig)
	keys := rig.keys(id)
	close(resume)
	var got result
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Start never returned")
	}
	if got.err == nil {
		t.Fatalf("Start answered %016x, nil for a name that is live as %016x", got.id, id)
	}
	u30iCode(t, got.err, cluster.CodeBucketExists)
	for _, want := range []string{"created again", "drop bucket User in app", id16(id)} {
		if !strings.Contains(got.err.Error(), want) {
			t.Fatalf("the refusal %q lacks %q", got.err, want)
		}
	}
	u42WantSurvivor(t, rig, id, keys)
}

// TestARepeatedDropRacingTheFinishDoesNotResurrectTheReverseDocument: a repeat of Start has read
// the dropping declaration when the loop deletes the declaration and the reverse document; the
// repeat's reverse write must see that and do nothing.
func TestARepeatedDropRacingTheFinishDoesNotResurrectTheReverseDocument(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	ctx := context.Background()
	if _, err := rig.node.StartDropBucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	rig.node.dropper.beforeReverse = func() {
		if err := rig.node.retire.Drain(ctx); err != nil {
			t.Error(err)
		}
	}
	if _, err := rig.node.StartDropBucket(ctx, u30iDB, "User"); err != nil {
		t.Logf("the repeat: %v", err)
	}
	if _, found := rig.declaration(); found {
		t.Fatal("the declaration survived")
	}
	if rig.reverseFound() {
		t.Fatal("the repeat resurrected the reverse document, and no record will ever delete it")
	}
}

// TestTheLoopNeverFreesAnIdAnotherCreateHasClaimed: the last run of the freeing step died after it
// deleted the reverse document; a create then draws the same id (injected) and has written its
// reverse document, which names the same bucket and is active, but not yet its declaration. The
// next run must see that the document is not dropping and leave it.
func TestTheLoopNeverFreesAnIdAnotherCreateHasClaimed(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	ctx := context.Background()
	dropper := rig.node.dropper
	dropper.crashAfter = func(step string) error {
		if step == dropStepDone {
			return errU42Crash
		}
		return nil
	}
	if _, err := rig.node.StartDropBucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	if err := rig.node.retire.Drain(ctx); !errors.Is(err, errU42Crash) {
		t.Fatalf("Drain = %v, want the crash", err)
	}
	if rig.reverseFound() {
		t.Fatal("setup: the reverse document survived its delete")
	}
	claim := &createRun{CreateBucket: rig.node.creator, ctx: ctx, dbID: rig.dbID, dbName: u30iDB, name: "User", version: rig.node.topology.Published(rig.dbID)}
	if err := claim.writeReverse(rig.bucketID, 0); err != nil {
		t.Fatal(err)
	}
	dropper.crashAfter = nil
	if err := rig.node.retire.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if !rig.reverseFound() {
		t.Fatal("the loop freed an id that a live create had claimed")
	}
}

// TestAnIdIsFreedOnlyWhenNoDeclarationStillCarriesIt: the reverse delete of a reclaiming run is
// refused while a declaration under the name still carries the id.
func TestAnIdIsFreedOnlyWhenNoDeclarationStillCarriesIt(t *testing.T) {
	rig := newU42Rig(t)
	rig.crashAt(u42Bump, 1)
	_ = rig.run()
	run := &dropRun{DropBucket: rig.drop, ctx: context.Background(), dbID: rig.dbID, dbName: u30iDB, name: "User", pinned: rig.bucketID, reclaim: true}
	if err := run.checkReverse(rig.bucketID); !errors.Is(err, errDropMoved) {
		t.Fatalf("checkReverse with the declaration still carrying the id = %v, want errDropMoved", err)
	}
	if decl, found := rig.declaration(); !found || decl.State != machine.StateDropping {
		t.Fatalf("setup: %+v %v", decl, found)
	}
}

// TestEnqueueAfterTheBumpWritesNoSecondSyncedCommit: the note already wrote the 'b' records, and
// the queueing that follows writes only the ones that are missing.
func TestEnqueueAfterTheBumpWritesNoSecondSyncedCommit(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	counting := &u43CountingKV{KV: rig.node.kv}
	rig.node.retire.deps.KV = counting
	if _, err := rig.node.StartDropBucket(context.Background(), u30iDB, "User"); err != nil {
		t.Fatal(err)
	}
	if counting.commits.Load() != 1 || counting.synced.Load() != 1 {
		t.Fatalf("a drop made %d commits (%d synced) to the sweep records, want the note's one", counting.commits.Load(), counting.synced.Load())
	}
}

// TestAStartExpectingAnotherIdDropsNothing: a caller that holds the id of the bucket it means is
// refused when the name carries another, and the bucket is untouched; the right id drops it.
func TestAStartExpectingAnotherIdDropsNothing(t *testing.T) {
	rig := newU42Async(t)
	rig.pause()
	ctx := context.Background()
	_, err := rig.node.StartDropBucketExpecting(ctx, u30iDB, "User", rig.bucketID+1)
	u30iCode(t, err, cluster.CodeNotFound)
	if !strings.Contains(err.Error(), "describe bucket User in app") {
		t.Fatalf("the refusal lacks the fix: %v", err)
	}
	if decl, found := rig.declaration(); !found || decl.State != machine.StateActive {
		t.Fatalf("a refused drop changed the bucket: %+v %v", decl, found)
	}
	if id, err := rig.node.StartDropBucketExpecting(ctx, u30iDB, "User", rig.bucketID); err != nil || id != rig.bucketID {
		t.Fatalf("with the right id: %016x, %v", id, err)
	}
}
