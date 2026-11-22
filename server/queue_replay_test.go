package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// queueReplayCluster is a two-node, RF=1 cluster with a fifo test bucket and
// the index of its owner: two nodes because a one-node ring runs in
// single-node mode, which has no owner write path, and RF=1 so the owner
// alone decides what a restart replays.
func queueReplayCluster(t *testing.T) (*testCluster, int) {
	t.Helper()
	tc := newTestCluster(t, 2, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner of %q: %v", testBucket, err)
	}
	return tc, nodeIndexByAddr(t, tc.addrs, owner.ID)
}

func pushQueueElement(tc *testCluster, index, n int) *transport.Frame {
	tc.t.Helper()
	return tc.frame(index, transport.OpFIFOPush, PushRequest{
		Database:     testDatabase,
		Bucket:       testBucket,
		Data:         map[string]interface{}{"n": n},
		WriteConcern: "majority",
	})
}

// popQueueElement pops through node index and returns the element's n, or
// false for an empty queue.
func popQueueElement(tc *testCluster, index int) (int, bool) {
	tc.t.Helper()
	frame := tc.ok(index, transport.OpFIFOPop, PopRequest{Database: testDatabase, Bucket: testBucket})
	// An empty queue's PopResponse{Found: false} arrives as a zero-length
	// payload (the nil bson.Raw does not marshal; see heap_store_test.go).
	if len(frame.Payload) == 0 {
		return 0, false
	}
	var resp PopResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		tc.t.Fatalf("decoding pop response: %v", err)
	}
	if !resp.Found {
		return 0, false
	}
	n, ok := bson.Raw(resp.Items[0]).Lookup("n").Int32OK()
	if !ok {
		tc.t.Fatalf("popped element %v has no n", resp.Items[0])
	}
	return int(n), true
}

// TestCrashBetweenAppendAndApplyReplaysPushes is T-09: a push appended to the
// WAL whose store apply failed at the commit (the in-process crash between the
// append and the store write) is restored by replay at the position it would
// have taken, and the cursors agree with the range, so the next push overwrites
// nothing. It failed on the pin: fifo was exempt from replay.
func TestCrashBetweenAppendAndApplyReplaysPushes(t *testing.T) {
	tc, ownerIdx := queueReplayCluster(t)
	defer tc.stop()

	for n := 0; n < 2; n++ {
		if frame := pushQueueElement(tc, ownerIdx, n); frame.Opcode == transport.OpError {
			t.Fatalf("push %d: %s", n, string(frame.Payload))
		}
	}
	failed := failNextApplyOn(t, tc.nodes[ownerIdx].server.core.kv, anySeq)
	pushQueueElement(tc, ownerIdx, 2)
	if failed.Load() == 0 {
		t.Fatal("the third push's apply never reached the store; the test built no crash between append and apply")
	}
	if got := tc.localCount(ownerIdx); got != 2 {
		t.Fatalf("before the restart the owner holds %d elements, want 2: the third push's apply was meant to fail", got)
	}

	tc.restartNode(ownerIdx)

	if got := tc.localCount(ownerIdx); got != 3 {
		t.Fatalf("after replay the owner holds %d elements, want 3: the pushed-but-unapplied element was not restored", got)
	}
	st := bucketStateOf(t, tc.nodes[ownerIdx].server.core, testDatabase, testBucket)
	if st.Next != 3 || st.Tail != 2 || st.Entries != 3 {
		t.Fatalf("after replay next %d, tail %d, entries %d; want 3, 2, 3", st.Next, st.Tail, st.Entries)
	}
	if frame := pushQueueElement(tc, ownerIdx, 3); frame.Opcode == transport.OpError {
		t.Fatalf("the fourth push: %s", string(frame.Payload))
	}
	var popped []int
	for {
		n, found := popQueueElement(tc, ownerIdx)
		if !found {
			break
		}
		popped = append(popped, n)
	}
	if want := []int{0, 1, 2, 3}; !reflect.DeepEqual(popped, want) {
		t.Fatalf("popped %v after the fourth push, want %v: a push overwrote an element", popped, want)
	}
}

// TestReplayedPopIsGated is T-10: a pop is replayed at a restart and again by
// hand, and neither takes a second element — the bucket's applied_seq gates
// both, so a replayed pop never pops whatever is at the head now.
func TestReplayedPopIsGated(t *testing.T) {
	tc, ownerIdx := queueReplayCluster(t)
	defer tc.stop()

	for n := 0; n < 3; n++ {
		if frame := pushQueueElement(tc, ownerIdx, n); frame.Opcode == transport.OpError {
			t.Fatalf("push %d: %s", n, string(frame.Payload))
		}
	}
	if n, found := popQueueElement(tc, ownerIdx); !found || n != 0 {
		t.Fatalf("the live pop took %d (found %v), want 0", n, found)
	}
	popSeq := tc.nodes[ownerIdx].server.core.wal.LastSequence()

	tc.restartNode(ownerIdx)

	core := tc.nodes[ownerIdx].server.core
	assertAfterPop := func(when string) {
		t.Helper()
		if got := tc.localCount(ownerIdx); got != 2 {
			t.Fatalf("%s the owner holds %d elements, want 2", when, got)
		}
		if st := bucketStateOf(t, core, testDatabase, testBucket); st.Head != 1 || st.AppliedSeq < popSeq {
			t.Fatalf("%s head %d, applied_seq %d; want head 1 and applied_seq at least the pop's %d", when, st.Head, st.AppliedSeq, popSeq)
		}
	}
	assertAfterPop("after the restart's replay")

	entries, err := core.wal.ReadFrom(popSeq)
	if err != nil || len(entries) == 0 || entries[0].Seq != popSeq {
		t.Fatalf("reading the pop's WAL entry at %d: %v (%d entries)", popSeq, err, len(entries))
	}
	pop := entries[0]
	if _, err := core.dispatch(withReplay(withApplySeq(context.Background(), pop.Seq)),
		&transport.Frame{Opcode: transport.Opcode(pop.Op), Payload: pop.Data}); err != nil {
		t.Fatalf("replaying the pop again: %v", err)
	}
	assertAfterPop("after replaying the pop again")
	if n, found := popQueueElement(tc, ownerIdx); !found || n != 1 {
		t.Fatalf("the next live pop took %d (found %v), want 1", n, found)
	}
}
