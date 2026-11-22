package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// survivorsAfterARemoval is a three-node factor-1 cluster after the removal of
// node 3 while it runs, with node 3's asks to the survivors held: the two
// survivors stay members, and the bucket's new owner has not been told of it.
type survivorsAfterARemoval struct {
	tc     *testCluster
	bucket string
	// owner is the index of the survivor the ring without node 3 names as
	// the bucket's owner.
	owner int
	// fresh are names node 3 owned before the removal that no node holds.
	fresh []string
	// refuse holds node 3's asks for bucket while it is true; asked counts
	// them.
	refuse *atomic.Bool
	asked  *atomic.Int64
}

// removedWhileItsAsksAreHeld builds the BUG-0627 scenario: five acknowledged
// writes to a bucket node 3 owns, node 3 removed while it runs, and its asks
// to the survivors held until the caller clears refuse.
func removedWhileItsAsksAreHeld(t *testing.T) *survivorsAfterARemoval {
	t.Helper()
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	removedID := tc.addrs[2]
	bucket := bucketNameOwnedBy(t, clusterOf(tc, 0), testDatabase, removedID)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	for i := 0; i < 5; i++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%02d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	var fresh []string
	for i := 0; len(fresh) < 2 && i < 512; i++ {
		name := fmt.Sprintf("fresh-%d", i)
		if owner, err := clusterOf(tc, 0).Owner(placementOf(testDatabase, name)); err == nil && owner.ID == removedID {
			fresh = append(fresh, name)
		}
	}
	if len(fresh) < 2 {
		t.Fatal("setup: fewer than two fresh names are placed on node 3")
	}
	scenario := &survivorsAfterARemoval{tc: tc, bucket: bucket, fresh: fresh, refuse: &atomic.Bool{}, asked: &atomic.Int64{}}
	scenario.refuse.Store(true)
	hook := func(fromID, _, _, name string) error {
		if fromID != removedID || name != bucket {
			return nil
		}
		scenario.asked.Add(1)
		if scenario.refuse.Load() {
			return errors.New("test: the ask is held")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&hook)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	if !holdsWithin(10*time.Second, func() bool { return scenario.asked.Load() > 0 }) {
		t.Fatal("setup: the removed node never asked")
	}
	scenario.owner = -1
	if !holdsWithin(10*time.Second, func() bool {
		for index := 0; index < 2; index++ {
			if !clusterOf(tc, index).Enabled() || nodeListed(clusterOf(tc, index).Members(), removedID) {
				return false
			}
			if clusterOf(tc, index).ownsBucket(placementOf(testDatabase, bucket)) {
				scenario.owner = index
			}
		}
		return scenario.owner >= 0
	}) {
		t.Fatal("setup: the survivors never adopted the removal, or neither owns the bucket")
	}
	return scenario
}

// TestACreateAtAMultiMemberOwnerWaitsForTheRemovedNodesBucket is BUG-0627: a
// client creates, at the survivor that now owns it, the bucket whose only copy
// is on the removed node, before the removed node's ask made the owner take it
// over. The owner's create path (Cluster.fenceUnheldBucket) must ask the
// removed node for the name, as the lone survivor's door does (BUG-0625): the
// create waits for the copy and is answered from it, or is refused with
// retry, and never makes an empty bucket of the name. Guard: the owner asks
// only a take-over already running and the current members, so the create
// makes an empty twin, the removed node's copy is refused as one, and the five
// acknowledged writes never reach the owner.
func TestACreateAtAMultiMemberOwnerWaitsForTheRemovedNodesBucket(t *testing.T) {
	s := removedWhileItsAsksAreHeld(t)
	owner := s.tc.nodes[s.owner]
	if owner.server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the new owner holds the bucket before the create")
	}
	frame := s.tc.frame(s.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: s.bucket, Type: BucketTypeHash})
	t.Logf("the create at the new owner, node %d, was answered %s: %q", s.owner+1, frame.Opcode, string(frame.Payload))
	s.refuse.Store(false)
	held := func() bool {
		for i := 0; i < 5; i++ {
			if !holdsKeyIn(owner, testDatabase, s.bucket, fmt.Sprintf("k%02d", i)) {
				return false
			}
		}
		return true
	}
	if !holdsWithin(takeOverWait, held) {
		for i := 0; i < 5; i++ {
			if key := fmt.Sprintf("k%02d", i); !holdsKeyIn(owner, testDatabase, s.bucket, key) {
				t.Errorf("the new owner lacks acknowledged write %s", key)
			}
		}
		t.Fatalf("after a create sent to the new owner before it took %s over, the removed node's acknowledged writes never reached it (the create was answered %s)",
			s.bucket, frame.Opcode)
	}
}

// TestAMultiMemberOwnerStillCreatesANewNameAfterARemoval is the guard on the
// wait BUG-0627 adds: a create of a name that the removed node owned before
// the removal and that no node holds is created, promptly, while the removed
// node runs and once it is stopped. The take-over finds the name nowhere and
// the create goes on. Guard: refuse a create whenever the wait lands no copy,
// and both creates are refused.
func TestAMultiMemberOwnerStillCreatesANewNameAfterARemoval(t *testing.T) {
	s := removedWhileItsAsksAreHeld(t)
	for index, phase := range []string{"removed node running", "removed node stopped"} {
		if phase == "removed node stopped" {
			if err := s.tc.nodes[2].server.Stop(); err != nil {
				t.Fatalf("stopping the removed node: %v", err)
			}
			s.tc.nodes[2].server = nil
		}
		name := s.fresh[index]
		at := -1
		for survivor := 0; survivor < 2; survivor++ {
			if clusterOf(s.tc, survivor).ownsBucket(placementOf(testDatabase, name)) {
				at = survivor
			}
		}
		if at < 0 {
			t.Fatalf("setup: no survivor owns %s", name)
		}
		began := time.Now()
		frame := s.tc.frame(at, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		took := time.Since(began).Round(time.Millisecond)
		if text := string(frame.Payload); frame.Opcode == transport.OpError || !strings.Contains(text, "created") {
			t.Errorf("%s: a create of the new name %s at its owner was answered %s after %s: %q", phase, name, frame.Opcode, took, text)
			continue
		}
		if took > DefaultWriteConcernTimeout {
			t.Errorf("%s: a create of the new name %s took %s, longer than the write-concern timeout", phase, name, took)
		}
		t.Logf("%s: the new name %s was created in %s", phase, name, took)
	}
}

// TestACreateAtAMultiMemberOwnerOutlastsASlowRemovedNode pins the bound of the
// wait BUG-0627 adds when the removed node is slow to answer, as a host that
// went away is: the take-over's ask of it as the name's previous owner and the
// fallback's ask each take most of a write-concern timeout, so one round
// outlasts awaitOwnCopy's own wait. The create of a name the removed node
// owned and no node holds is still created, after that one round, and not
// refused for every retry. Guard: wait for the take-over once, and the create
// is refused with the hand-off's retry, as every retry would be.
func TestACreateAtAMultiMemberOwnerOutlastsASlowRemovedNode(t *testing.T) {
	s := removedWhileItsAsksAreHeld(t)
	name := s.fresh[0]
	at := -1
	for survivor := 0; survivor < 2; survivor++ {
		if clusterOf(s.tc, survivor).ownsBucket(placementOf(testDatabase, name)) {
			at = survivor
		}
	}
	if at < 0 {
		t.Fatalf("setup: no survivor owns %s", name)
	}
	wait := clusterOf(s.tc, at).wcTimeout
	removedID := s.tc.addrs[2]
	slow := func(_, nodeID string) error {
		if nodeID != removedID {
			return nil
		}
		time.Sleep(wait * 3 / 4)
		return errors.New("test: the removed node did not answer")
	}
	askHolderHook.Store(&slow)
	defer askHolderHook.Store(nil)
	began := time.Now()
	frame := s.tc.frame(at, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	took := time.Since(began).Round(time.Millisecond)
	if text := string(frame.Payload); frame.Opcode == transport.OpError || !strings.Contains(text, "created") {
		t.Fatalf("a create of the new name %s at its owner, while the removed node takes %s to fail each question, was answered %s after %s: %q",
			name, wait*3/4, frame.Opcode, took, text)
	}
	t.Logf("the new name %s was created in %s with a slow removed node", name, took)
}

// TestAMultiMemberOwnerAnswersTheFirstPutOfAnUnheldBucketAsTheHandoff is
// BUG-0636: the first put sent to the new owner of a bucket only the removed
// node held, before the owner's take-over of it has said what it waits for, is
// answered the hand-off, as the second put and a get are, and not the join
// fence's "never created or not moved yet". The fence wakes the take-over
// worker and must give it the moment it needs to record its status, as the
// lone survivor's door does (BUG-0632). The refusal is not_applied. A name the
// removed node owned that exists nowhere is still answered the fence's plain
// text, not the hand-off. Guard: read the status at once after the wake, and
// the first put is answered the never-created text.
func TestAMultiMemberOwnerAnswersTheFirstPutOfAnUnheldBucketAsTheHandoff(t *testing.T) {
	s := removedWhileItsAsksAreHeld(t)
	owner := s.tc.nodes[s.owner]
	if owner.server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the new owner holds the bucket before the put")
	}
	takeOver := holdTakeOverOf(t, s.tc.addrs[s.owner], s.bucket)
	put := InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k99", Data: map[string]interface{}{"n": 99}}
	get := FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "k00"}

	putFrame := s.tc.frame(s.owner, transport.OpPut, put)
	if text := string(putFrame.Payload); putFrame.Opcode != transport.OpError || strings.Contains(text, "never created") || !strings.Contains(text, "Retry in a moment") {
		t.Errorf("the first put of %s, which only the removed node holds, sent to its new owner was answered %s: %q, want the hand-off", s.bucket, putFrame.Opcode, text)
	}
	if outcome := writeOutcomeOf(t, putFrame); outcome != outcomeNotApplied {
		t.Errorf("the put's refusal says write_outcome %q, want %q", outcome, outcomeNotApplied)
	}
	getFrame := s.tc.frame(s.owner, transport.OpGet, get)
	// The removed node still runs, so the owner may forward the read to it and
	// serve the data; what a get must never be is the never-created text.
	if text := string(getFrame.Payload); getFrame.Opcode == transport.OpError && !strings.Contains(text, "Retry in a moment") {
		t.Errorf("a get of %s sent to its new owner was answered %s: %q, want the hand-off or the data", s.bucket, getFrame.Opcode, text)
	}

	// A name the removed node owned that no node holds is, while its hand-off
	// is awaited, the retry naming the removed node: R8 extends to writes
	// (owner ruling 2026-10-23, BUG-0693).
	survivor := clusterOf(s.tc, s.owner)
	nowhere := ""
	for i := 0; nowhere == "" && i < 4096; i++ {
		name := fmt.Sprintf("never-made-%d", i)
		if survivor.ownsBucket(placementOf(testDatabase, name)) && survivor.ownedByARemovedNode(placementOf(testDatabase, name)) {
			nowhere = name
		}
	}
	if nowhere == "" {
		t.Fatal("setup: no name is owned by this survivor and was owned by the removed node")
	}
	absent := s.tc.frame(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: nowhere, Key: "k", Data: map[string]interface{}{"n": 1}})
	retryNamingRemovedNode(t, fmt.Sprintf("a put of %s, which exists nowhere, inside the window", nowhere), absent, s.tc.addrs[2])

	s.refuse.Store(false)
	takeOver.release()
	if !holdsWithin(takeOverWait, func() bool { return owner.server.core.bucketStoreOpen(testDatabase, s.bucket) }) {
		t.Fatalf("the new owner never took %s over", s.bucket)
	}
	s.tc.ok(s.owner, transport.OpPut, put)
	if frame := s.tc.frame(s.owner, transport.OpGet, get); frame.Opcode == transport.OpError {
		t.Errorf("after the take-over a get of %s was refused: %q", s.bucket, string(frame.Payload))
	}
}
