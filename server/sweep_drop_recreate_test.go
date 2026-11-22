package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// heldRound is a sweep round on the owner of a bucket whose hand-off the
// joiner never confirms, held by a test at one stage of the round.
type heldRound struct {
	s       *leftOutside
	owner   *Server
	epoch   atomic.Uint64
	atStage chan struct{}
	release chan struct{}
	ended   chan struct{}
}

// holdRoundAt arms a hook that blocks the owner's next round at stage and
// reports when the round has ended (sweepEnded) after release.
func holdRoundAt(t *testing.T, s *leftOutside, stage sweepStage) *heldRound {
	t.Helper()
	held := &heldRound{s: s, owner: s.tc.nodes[s.owner].server.core,
		atStage: make(chan struct{}), release: make(chan struct{}), ended: make(chan struct{})}
	ownerID := held.owner.cluster.self.ID
	var armed, afterRelease atomic.Bool
	var endedOnce sync.Once
	hook := func(nodeID string, epoch uint64, at sweepStage) {
		if nodeID != ownerID {
			return
		}
		switch {
		case at == stage && armed.CompareAndSwap(true, false):
			held.epoch.Store(epoch)
			close(held.atStage)
			<-held.release
			afterRelease.Store(true)
		case at == sweepEnded && afterRelease.Load():
			endedOnce.Do(func() { close(held.ended) })
		}
	}
	sweepStageHook.Store(&hook)
	t.Cleanup(func() { sweepStageHook.Store(nil) })
	armed.Store(true)
	return held
}

func (held *heldRound) waitForStage(t *testing.T) {
	t.Helper()
	select {
	case <-held.atStage:
	case <-time.After(30 * time.Second):
		t.Fatal("the owner's round never reached the held stage")
	}
}

func (held *heldRound) releaseAndWaitForEnd(t *testing.T) {
	t.Helper()
	close(held.release)
	select {
	case <-held.ended:
	case <-time.After(30 * time.Second):
		t.Fatal("the owner's round never ended")
	}
}

// TestARecreatedBucketIsNotLeftOutOfTheRoundsLists: a bucket is dropped and
// created again under the same name while one round runs, and the new bucket
// is not settled when the round ends. The round must keep the name, and the
// node must not be marked settled while the new bucket is unsettled
// (BUG-0662). Guard: filter a dropped name by name alone, without the
// incarnation the drop took off.
func TestARecreatedBucketIsNotLeftOutOfTheRoundsLists(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, 0)
	defer s.tc.stop()
	owner := s.tc.nodes[s.owner].server.core
	if !eventually(func() bool { return s.askedBy(s.owner) >= 2 && owner.cluster.retryingNames(s.bucket) }) {
		t.Fatal("setup: the owner is not retrying the bucket's hand-off")
	}
	held := holdRoundAt(t, s, passFinished)
	held.waitForStage(t)

	dropAtOne(s.tc, s.owner, testDatabase, s.bucket)
	s.tc.ok(s.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: s.bucket, Type: BucketTypeHash})
	// The give-up bound has passed by the time the held pass returns, so the
	// round ends on its first look.
	sweepRetryGiveUp.set(time.Nanosecond)
	held.releaseAndWaitForEnd(t)

	if !owner.cluster.residueNames(s.bucket) {
		t.Error("the round left the re-created bucket out of its residue although the new bucket is not settled")
	}
	if epoch := held.epoch.Load(); owner.cluster.settled.Load() >= epoch {
		t.Errorf("the node was marked settled at epoch %d while the re-created bucket is not settled", epoch)
	}
}

// heldRoundAboutToRecord holds the owner's round once it has given up
// retrying and is about to record what it left (residueRecording), with the
// dropped bucket still on the list it hands to recordResidue.
func heldRoundAboutToRecord(t *testing.T) *heldRound {
	t.Helper()
	s := setUpACopyLeftOutsideTheSet(t, 0)
	t.Cleanup(s.tc.stop)
	owner := s.tc.nodes[s.owner].server.core
	if !eventually(func() bool { return s.askedBy(s.owner) >= 2 && owner.cluster.retryingNames(s.bucket) }) {
		t.Fatal("setup: the owner is not retrying the bucket's hand-off")
	}
	held := holdRoundAt(t, s, residueRecording)
	sweepRetryGiveUp.set(time.Nanosecond)
	held.waitForStage(t)
	if !owner.cluster.retryingNames(s.bucket) {
		t.Fatal("setup: the round gave up without the bucket on its retry list")
	}
	return held
}

// TestADropBetweenTheLastPassAndTheResidueIsNotRecorded: the round has given
// up and holds the list it will record, the bucket is dropped, and the round
// then records. The residue must not name the dropped bucket (BUG-0659).
// Guard: record the list as the round holds it, without leaving out what a
// drop took off since the round began (recordResidue).
func TestADropBetweenTheLastPassAndTheResidueIsNotRecorded(t *testing.T) {
	held := heldRoundAboutToRecord(t)
	owner := held.owner.cluster
	dropAtOne(held.s.tc, held.s.owner, testDatabase, held.s.bucket)
	held.releaseAndWaitForEnd(t)
	if owner.residueNames(held.s.bucket) {
		t.Error("the round recorded the bucket dropped after its last pass as residue")
	}
}

// TestARoundThatKeptNothingMarksTheNodeSettled: the only bucket the round
// could not settle is dropped before it records, so it leaves nothing, and
// the node is settled at the round's epoch. Guard: settle on the length of
// the list the round holds instead of on what recordResidue kept
// (settleSweep).
func TestARoundThatKeptNothingMarksTheNodeSettled(t *testing.T) {
	held := heldRoundAboutToRecord(t)
	owner := held.owner.cluster
	dropAtOne(held.s.tc, held.s.owner, testDatabase, held.s.bucket)
	held.releaseAndWaitForEnd(t)
	if epoch := held.epoch.Load(); owner.settled.Load() < epoch {
		t.Errorf("the node is not settled at epoch %d although the round left nothing but a dropped bucket", epoch)
	}
}

// TestADropBetweenARearmsSnapshotAndItsPassIsNotWrittenBack: a rearm takes the
// residue's list and its round stamp in one step under sweepLeftMu; a drop
// that lands right after is newer than the stamp and the rearm must not
// write the name back. Guard: take the stamp after the lock is released
// (rearmSweep), which makes that drop look older than the round.
func TestADropBetweenARearmsSnapshotAndItsPassIsNotWrittenBack(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, 0)
	defer s.tc.stop()
	owner := s.tc.nodes[s.owner].server.core.cluster
	sweepRetryGiveUp.set(time.Nanosecond)
	if !eventually(func() bool { return owner.residueNames(s.bucket) }) {
		t.Fatal("setup: the owner's round never recorded the bucket as residue")
	}
	held := holdRoundAt(t, s, rearmSnapshotted)
	owner.sweepLeftMu.Lock()
	owner.lastRearmAt = time.Time{}
	owner.sweepLeftMu.Unlock()
	go owner.rearmSweep()
	held.waitForStage(t)

	dropAtOne(s.tc, s.owner, testDatabase, s.bucket)
	held.releaseAndWaitForEnd(t)
	if owner.residueNames(s.bucket) {
		t.Error("the rearm wrote the dropped bucket back to the residue")
	}
	if owner.retryingNames(s.bucket) {
		t.Error("the rearm wrote the dropped bucket back to the retry list")
	}
}

// TestAPulledBucketOfTheSameNameIsNotLeftOutOfTheRoundsLists: the new
// incarnation arrives on the node by a pull (materialise) while a round holds
// the drop note of the old one. The round keeps the name and the node is not
// marked settled (BUG-0662). Guard: leave noteAttachedToSweep out of
// materializeRange.
func TestAPulledBucketOfTheSameNameIsNotLeftOutOfTheRoundsLists(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, 0)
	defer s.tc.stop()
	owner := s.tc.nodes[s.owner].server.core
	if !eventually(func() bool { return s.askedBy(s.owner) >= 2 && owner.cluster.retryingNames(s.bucket) }) {
		t.Fatal("setup: the owner is not retrying the bucket's hand-off")
	}
	held := holdRoundAt(t, s, passFinished)
	held.waitForStage(t)

	dropAtOne(s.tc, s.owner, testDatabase, s.bucket)
	live, err := owner.liveGenerations(testDatabase)
	if err != nil || len(live) == 0 {
		t.Fatalf("setup: the live generations of %q are %v, %v", testDatabase, live, err)
	}
	pulled := migratedBucket{database: testDatabase, name: s.bucket, storeType: BucketTypeHash, generation: live[0]}
	if err := owner.materializeRange(pulled, nil, 0); err != nil {
		t.Fatalf("materialising the new incarnation: %v", err)
	}
	sweepRetryGiveUp.set(time.Nanosecond)
	held.releaseAndWaitForEnd(t)

	if !owner.cluster.residueNames(s.bucket) {
		t.Error("the round left a bucket that arrived by a pull out of its residue although the new bucket is not settled")
	}
	if epoch := held.epoch.Load(); owner.cluster.settled.Load() >= epoch {
		t.Errorf("the node was marked settled at epoch %d while the pulled bucket is not settled", epoch)
	}
}

// TestAnAttachOfTheDroppedIncarnationKeepsTheDropNote: the note of a drop of
// bucket id 7 stands when a bucket with the same id attaches (the same
// incarnation attached again), and comes off when another id attaches.
// Guard: take the note off on any attach, without comparing ids
// (noteAttachedInRound).
func TestAnAttachOfTheDroppedIncarnationKeepsTheDropNote(t *testing.T) {
	cluster := &Cluster{}
	name := heldBucket{database: "app", bucket: "events"}
	cluster.beginRound(sweepRound{})
	cluster.noteDroppedInRound(name, 7)

	cluster.noteAttachedInRound(name, 7)
	if _, standing := cluster.droppedInRound[name]; !standing {
		t.Error("an attach of the dropped incarnation's own id took the drop note off")
	}
	cluster.noteAttachedInRound(name, 8)
	if _, standing := cluster.droppedInRound[name]; standing {
		t.Error("an attach of another incarnation left the drop note standing")
	}
}

// TestADropNotesTheIDOfTheBucketItDropped: the note a drop leaves for the
// running round carries the catalog id of the bucket dropped. Guard: pass 0
// (or any other id) to forgetPendingName from DeleteBucketCtx.
func TestADropNotesTheIDOfTheBucketItDropped(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, 0)
	defer s.tc.stop()
	owner := s.tc.nodes[s.owner].server.core
	if !eventually(func() bool { return s.askedBy(s.owner) >= 2 && owner.cluster.retryingNames(s.bucket) }) {
		t.Fatal("setup: the owner is not retrying the bucket's hand-off")
	}
	held := holdRoundAt(t, s, passFinished)
	held.waitForStage(t)
	facts, served := owner.peekSettled(testDatabase, s.bucket)
	if !served || facts.id == 0 {
		t.Fatal("setup: the owner does not serve the bucket")
	}

	dropAtOne(s.tc, s.owner, testDatabase, s.bucket)
	owner.cluster.sweepLeftMu.Lock()
	note, noted := owner.cluster.droppedInRound[heldBucket{database: testDatabase, bucket: s.bucket}]
	owner.cluster.sweepLeftMu.Unlock()
	if !noted || note.bucketID != facts.id {
		t.Errorf("the round's note of the drop is %+v (noted %v); want the dropped bucket's id %d", note, noted, facts.id)
	}
	sweepRetryGiveUp.set(time.Nanosecond)
	held.releaseAndWaitForEnd(t)
}

// TestACreateOfADroppedNameWaitsForTheDropToNoteIt: the drop unlinks the name
// and notes it for the sweep in two steps, and a create of the same name that
// attached between them would find no note to take off, so the drop would then
// note the old id and the new bucket would be filtered. The create takes the
// bucket's fence exclusively, as the drop does (fenceModeFor), so it waits
// until the drop has returned; a pull's materialise takes it shared. Guard: let a create of the name run while the
// drop holds the fence.
func TestACreateOfADroppedNameWaitsForTheDropToNoteIt(t *testing.T) {
	s := setUpACopyLeftOutsideTheSet(t, 0)
	defer s.tc.stop()
	ownerNode := s.tc.nodes[s.owner]
	atUnlinked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func(database, bucket string, _ uint64, _ uint16) {
		if database == testDatabase && bucket == s.bucket {
			once.Do(func() { close(atUnlinked); <-release })
		}
	}
	afterDropHiddenHook.Store(&hook)
	t.Cleanup(func() { afterDropHiddenHook.Store(nil) })

	send := func(op transport.Opcode, request any) <-chan error {
		answered := make(chan error, 1)
		go func() {
			payload, err := bson.Marshal(request)
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				_, err = ownerNode.client.SendRequest(ctx, op, payload)
			}
			answered <- err
		}()
		return answered
	}
	dropped := send(transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": s.bucket, "write_concern": "one"})
	select {
	case <-atUnlinked:
	case <-time.After(30 * time.Second):
		t.Fatal("the drop never unlinked the name")
	}
	// A pull's materialise takes the fence shared, a create exclusively: neither
	// gets it while the drop holds it exclusively.
	if hold := ownerNode.server.core.tryAcquireFenceShared(placementOf(testDatabase, s.bucket)); hold != nil {
		hold.release()
		t.Error("the bucket's fence was free between the drop's unlink and its note, so a pull or a create of the name could attach there")
	}
	created := send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: s.bucket, Type: BucketTypeHash})
	select {
	case <-created:
		t.Error("a create of the name was answered while the drop was between unlinking it and noting it")
	case <-time.After(time.Second):
	}
	close(release)
	for _, answered := range []<-chan error{dropped, created} {
		select {
		case err := <-answered:
			if err != nil {
				t.Errorf("a request failed: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("a request never answered after the drop was released")
		}
	}
}
