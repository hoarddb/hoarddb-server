package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0589: a placement pass merges a bucket's marks from a read of them and
// commits the merge later, in a batch of up to a thousand. A mark another
// writer records for that bucket in between must survive the commit.

// markOnDisk is the 'h' record of the test database's bucket on node s.
func markOnDisk(t *testing.T, s *Server, bucket string) placementMarks {
	t.Helper()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	stored, err := snap.Get(placementMarkKey(placementOf(testDatabase, bucket)))
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return placementMarks{}
		}
		t.Fatalf("reading %s's marks of %q: %v", s.cluster.self.ID, bucket, err)
	}
	var marks placementMarks
	if err := bson.Unmarshal(stored, &marks); err != nil {
		t.Fatalf("decoding %s's marks of %q: %v", s.cluster.self.ID, bucket, err)
	}
	return marks
}

// movedByTheJoin names a bucket of the test database whose owner the joiner
// becomes, and a member, not its owner before, that is in its set at 3 before
// and after the join: the member's pass earns the bucket a moved mark and
// nothing else.
func movedByTheJoin(t *testing.T, tc *testCluster, joiner string) (bucket string, member int) {
	t.Helper()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joiner)...)
	for attempt := 0; attempt < 8000; attempt++ {
		name := fmt.Sprintf("moved%04d", attempt)
		key := placementOf(testDatabase, name).encoded
		oldSet, err := before.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		newSet, err := after.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		if newSet[0].ID != joiner {
			continue
		}
		for _, node := range oldSet[1:] {
			if nodeListed(newSet, node.ID) {
				return name, nodeIndexByAddr(t, tc.addrs, node.ID)
			}
		}
	}
	t.Fatalf("no bucket name is taken over by %s with another holder staying in its set", joiner)
	return "", 0
}

// requirePassVisitsFirst fails the test unless c's placement pass walks first
// before second: the pass lists the buckets it holds in forEachKnownBucket's
// order. A pass held at second has then merged first's marks into the batch
// it has not committed. Visited the other way round, the hold would come
// before first's read, and the test would pass with the bug in place.
func requirePassVisitsFirst(t *testing.T, c *Cluster, first, second string) {
	t.Helper()
	var order []string
	c.forEachKnownBucket(func(database, bucket string) {
		if database == testDatabase && (bucket == first || bucket == second) {
			order = append(order, bucket)
		}
	})
	if len(order) != 2 || order[0] != first {
		t.Fatalf("setup: %s's placement pass visits %v, want %q before %q", c.self.ID, order, first, second)
	}
}

// TestAMarkRecordedWhileAPassIsBetweenItsReadAndItsCommitSurvives is
// BUG-0589's test. A join makes the joiner the owner of a bucket, so a member
// that keeps a copy earns it a moved mark in its placement pass. The pass is
// held at its factor read of a later bucket, after the moved mark is merged
// into the batch it has not committed. Meanwhile a decrease to 1 is applied on
// the member under the bucket's exclusive fence (§5.3 row 2), which marks the
// copy outside. Released, the pass commits. The copy's marks, in memory and on
// disk, still carry the outside mark beside the moved one.
func TestAMarkRecordedWhileAPassIsBetweenItsReadAndItsCommitSurvives(t *testing.T) {
	takeOverOff(t)
	joinFreezeOff(t)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	bucket, member := movedByTheJoin(t, tc, ports.ringNode().ID)
	later := bucket + "-later"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	for _, name := range []string{bucket, later} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, name, 3)
	}
	core := tc.nodes[member].server.core
	requirePassVisitsFirst(t, core.cluster, bucket, later)
	facts, _ := core.peekSettled(testDatabase, bucket)

	reached, releasePass := holdPassReads(t, map[string]string{tc.addrs[member]: later})
	defer releasePass()
	joiner := joinFifth(t, tc, ports)
	defer func() { _ = joiner.client.Close() }()
	awaitHeld(t, reached[tc.addrs[member]], fmt.Sprintf("node %d's placement pass at %q", member+1, later))

	epoch := core.cluster.Epoch()
	key := placementOf(testDatabase, bucket)
	fence := core.acquireFence(key, fenceExclusive)
	answer, err := core.alterAsMember(context.Background(), AlterBucketRequest{Database: testDatabase, Name: bucket,
		ReplicationFactor: 1, PreviousFactor: 3, Epoch: epoch, Generation: facts.generation})
	fence.release()
	if err != nil || answer == nil || !answer.Success {
		t.Fatalf("node %d's apply of the decrease: %+v, %v", member+1, answer, err)
	}
	if marked := core.cluster.recordedMarks(key); marked.Outside == 0 {
		t.Fatalf("setup: the decrease did not mark node %d's copy outside: %+v", member+1, marked)
	}
	releasePass()
	waitPassFinished(t, tc, member)

	if inMemory := core.cluster.recordedMarks(key); inMemory.Outside == 0 || inMemory.Moved == 0 {
		t.Errorf("after the pass committed, node %d's copy's marks in memory are %+v; want the decrease's outside mark beside the pass's moved one", member+1, inMemory)
	}
	if onDisk := markOnDisk(t, core, bucket); onDisk.Outside == 0 || onDisk.Moved == 0 {
		t.Errorf("after the pass committed, node %d's copy's marks on disk are %+v; want the decrease's outside mark beside the pass's moved one", member+1, onDisk)
	}
}
