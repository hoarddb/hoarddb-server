package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	// slowWriterHold bounds how long the slow writer is held between the fence
	// release and the enqueue. On the unfixed code the fast writer is
	// acknowledged long before it expires. On fixed code the fast writer waits
	// for the slow one, so the hold runs to its end; it has to stay under the
	// write-concern timeout (DefaultWriteConcernTimeout) or the fast writer
	// fails on the queue deadline instead.
	slowWriterHold = 2 * time.Second
	// replicaConvergeWait is how long the replica gets to show both writes
	// after both were acknowledged. Replication of an acknowledged write has
	// already happened by then; the wait only absorbs scheduling.
	replicaConvergeWait = 3 * time.Second
)

// TestReplicaAppliesEveryAcknowledgedWriteWhateverTheEnqueueOrder reproduces
// the case where a replica misses an acknowledged write. Two writers on one
// owner take WAL sequences N and N+1; the writer holding N is held between the
// fence release and the enqueue, so N+1 reaches the replica first. Both writes
// are acknowledged at w=majority, which on RF=2 means the replica confirmed them — so both must be on the replica.
//
// The same-bucket case is here because the per-bucket fence is taken shared by
// every writer, so it does not order two writers of one bucket either: the
// interleaving is not confined to different buckets.
func TestReplicaAppliesEveryAcknowledgedWriteWhateverTheEnqueueOrder(t *testing.T) {
	cases := []struct {
		name       string
		sameBucket bool
	}{
		{name: "two buckets on one owner", sameBucket: false},
		{name: "one bucket, two writers", sameBucket: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			runReplicaApplyOrderCase(t, tt.sameBucket)
		})
	}
}

func runReplicaApplyOrderCase(t *testing.T, sameBucket bool) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	placement := tc.nodes[0].server.core.cluster
	slowBucket, fastBucket := bucketsOnOneOwner(t, placement, sameBucket)
	owner, err := placement.Owner(placementOf(testDatabase, slowBucket))
	if err != nil {
		t.Fatalf("resolve owner of %q: %v", slowBucket, err)
	}
	ownerIdx := nodeIndexByAddr(t, tc.addrs, owner.ID)
	replicaIdx := 1 - ownerIdx
	ownerCluster := tc.nodes[ownerIdx].server.core.cluster

	tc.ok(ownerIdx, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range uniqueBuckets(slowBucket, fastBucket) {
		tc.ok(ownerIdx, transport.OpCreateBucket, CreateBucketRequest{
			Database: testDatabase,
			Name:     bucket,
			Type:     BucketTypeHash,
		})
	}

	// The slow writer is the first PUT to slowBucket after arming. It reports
	// its sequence, then stays in the window until the fast writer has been
	// acknowledged or the hold runs out.
	var armed atomic.Bool
	armed.Store(true)
	slowSeq := make(chan uint64, 1)
	fastAcked := make(chan struct{})
	hook := func(entry ReplEntry) {
		if entry.Bucket != slowBucket || transport.Opcode(entry.Op) != transport.OpPut {
			return
		}
		if !armed.CompareAndSwap(true, false) {
			return
		}
		slowSeq <- entry.Seq
		select {
		case <-fastAcked:
		case <-time.After(slowWriterHold):
		}
	}
	replicaEnqueueHook.Store(&hook)
	defer replicaEnqueueHook.Store(nil)

	// DriverClient serialises its requests, so the slow writer needs a
	// connection of its own to be in flight while the fast one runs.
	slowClient := tc.dial(ownerIdx)
	defer func() { _ = slowClient.Close() }()

	type writeOutcome struct {
		frame *transport.Frame
		err   error
	}
	slowDone := make(chan writeOutcome, 1)
	slowPayload := mustMarshalPut(t, slowBucket, "slow")
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		frame, sendErr := slowClient.SendRequest(ctx, transport.OpPut, slowPayload)
		slowDone <- writeOutcome{frame: frame, err: sendErr}
	}()

	var lowSeq uint64
	select {
	case lowSeq = <-slowSeq:
	case <-time.After(15 * time.Second):
		t.Fatalf("the slow writer never reached the enqueue window: the hook did not fire, so nothing was reproduced")
	}

	fastFrame := tc.frame(ownerIdx, transport.OpPut, putRequest(fastBucket, "fast"))
	close(fastAcked)
	if fastFrame.Opcode == transport.OpError {
		t.Fatalf("the fast write (after seq %d) was refused, so the test did not reach its assertion: %s",
			lowSeq, string(fastFrame.Payload))
	}

	slow := <-slowDone
	if slow.err != nil {
		t.Fatalf("the slow write (seq %d) did not complete: %v", lowSeq, slow.err)
	}
	if slow.frame.Opcode == transport.OpError {
		t.Fatalf("the slow write (seq %d) was refused, so the test did not reach its assertion: %s",
			lowSeq, string(slow.frame.Payload))
	}

	// Both writes are acknowledged at w=majority: on RF=2 the owner counted the
	// replica as holding each of them.
	for _, want := range []struct{ bucket, key string }{{fastBucket, "fast"}, {slowBucket, "slow"}} {
		if found, message := readKeyFromNode(tc, ownerIdx, want.bucket, want.key); !found {
			t.Fatalf("owner is missing its own acknowledged write %q in bucket %q: %s", want.key, want.bucket, message)
		}
	}

	replicaPeer := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIdx]))
	deadline := time.Now().Add(replicaConvergeWait)
	for {
		fastFound, _ := readKeyFromNode(tc, replicaIdx, fastBucket, "fast")
		slowFound, _ := readKeyFromNode(tc, replicaIdx, slowBucket, "slow")
		if fastFound && slowFound {
			return
		}
		if time.Now().After(deadline) {
			// Give the owner's repair path its turn before calling the entry
			// lost: one explicit catch-up round, then read again.
			replayCtx, cancelReplay := context.WithTimeout(context.Background(), 10*time.Second)
			catchUpErr := replayToPeer(replayCtx, replicaPeer)
			cancelReplay()
			slowAfterCatchUp, _ := readKeyFromNode(tc, replicaIdx, slowBucket, "slow")
			t.Fatalf("replica %s is missing an acknowledged write: fast (later seq) present=%v, "+
				"slow (seq %d, bucket %q) present=%v. Both were acknowledged at w=majority on RF=2, so the owner "+
				"counted the replica as holding them. The owner records the replica as applied through seq %d; "+
				"after an explicit catch-up round (error: %v) the slow write is present=%v",
				tc.addrs[replicaIdx], fastFound, lowSeq, slowBucket, slowFound,
				replicaPeer.ackedThrough(ownerCluster.self.ID), catchUpErr, slowAfterCatchUp)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// bucketsOnOneOwner returns the slow writer's bucket and the fast writer's
// bucket in testDatabase, both placed on the same owner. Placement is by the
// (database, bucket) placement key, so the names are searched rather than
// assumed.
func bucketsOnOneOwner(t *testing.T, c *Cluster, sameBucket bool) (string, string) {
	t.Helper()
	const slowBucket = "order-slow"
	if sameBucket {
		return slowBucket, slowBucket
	}
	slowOwner, err := c.Owner(placementOf(testDatabase, slowBucket))
	if err != nil {
		t.Fatalf("resolve owner of %q: %v", slowBucket, err)
	}
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("order-fast-%d", i)
		candidateOwner, err := c.Owner(placementOf(testDatabase, candidate))
		if err != nil {
			t.Fatalf("resolve owner of %q: %v", candidate, err)
		}
		if candidateOwner.ID == slowOwner.ID {
			return slowBucket, candidate
		}
	}
	t.Fatalf("no bucket name among 1000 candidates shares an owner with %q", slowBucket)
	return "", ""
}

func uniqueBuckets(first, second string) []string {
	if first == second {
		return []string{first}
	}
	return []string{first, second}
}

func putRequest(bucket, key string) InsertRequest {
	return InsertRequest{
		Database:     testDatabase,
		Bucket:       bucket,
		Key:          key,
		Data:         map[string]interface{}{"key": key},
		WriteConcern: "majority",
	}
}

func mustMarshalPut(t *testing.T, bucket, key string) []byte {
	t.Helper()
	payload, err := bson.Marshal(putRequest(bucket, key))
	if err != nil {
		t.Fatalf("marshal put %q to %q: %v", key, bucket, err)
	}
	return payload
}

// readKeyFromNode asks one node for a key from its own copy of the bucket, the
// way readFromNode does for testBucket.
func readKeyFromNode(tc *testCluster, index int, bucket, key string) (bool, string) {
	tc.t.Helper()
	frame := tc.frame(index, transport.OpGet, FindRequest{
		Database: testDatabase,
		Bucket:   bucket,
		Key:      key,
	})
	if frame.Opcode == transport.OpError {
		return false, string(frame.Payload)
	}
	var findResp FindResponse
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &findResp); err != nil {
			tc.t.Fatalf("decode find response from node %d: %v", index+1, err)
		}
	}
	return findResp.Found, findResp.Message
}
