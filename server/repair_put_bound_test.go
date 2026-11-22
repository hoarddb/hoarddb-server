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

// BUG-0480: a consistent get's put repair is bounded by the version the read
// saw, evaluated in the apply on every copy, as the delete repair is.

// newRepairPutCluster is three nodes at factor 3 holding the test bucket, of
// bucketType, with its owner and replicas by placement.
func newRepairPutCluster(t *testing.T, bucketType BucketType) absenceCluster {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: bucketType})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
	return absenceCluster{tc: tc, owner: owner, replicas: replicas}
}

// TestConsistentGetRepairSparesAWriteMadeDuringTheRead: a key written at all
// as A, with one replica holding an older copy, is read by a consistent get
// on the owner; the coordinator is held after every holder's read and before
// its repair put, a client put of B at all lands, and the read then sends its
// repair of A. The read answers A, what it read, and once a later write at
// all has reached every copy, every node holds B: the repair, logged after
// B, found B newer than the version it read and left it. Guard: send the
// repair put without its bound (every node holds A).
func TestConsistentGetRepairSparesAWriteMadeDuringTheRead(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			a := newRepairPutCluster(t, bucketType)
			defer a.tc.stop()
			a.put(t, "k", "A", "all")
			stale := a.tc.nodes[a.replicas[0]].server.core
			writeBackAt(t, stale, testDatabase, testBucket, "k", map[string]interface{}{"v": "stale"},
				versionOn(t, stale, testDatabase, testBucket, "k")-1)

			var putErr string
			var during hookOnce
			hold := holdWhile(t, &during, a.tc.addrs[a.owner], func() { putErr = upsertAllDuringRead(a.tc, a.owner, "k", "B") })
			beforeRepairPutHook.Store(&hold)
			resp := a.read(t, "k")
			beforeRepairPutHook.Store(nil)
			if !during.fired || putErr != "" {
				t.Fatalf("setup: the put during the read did not land (fired=%v): %s", during.fired, putErr)
			}
			if !resp.Found || docField(t, resp.Data, "v") != "A" {
				t.Errorf("the read answered found=%v data=%s; want A, the value it read", resp.Found, resp.Data)
			}
			// The repair is logged on the owner before the read answers, so a
			// write at all after it is acknowledged only once every copy has
			// applied the repair in log order.
			a.put(t, "barrier", "after", "all")
			a.heldEverywhere(t, "k", "B")
		})
	}
}

// TestAClientCannotSendARepairBoundOnAPut: a client's put or btree insert
// carrying repair_bound is refused at the client port, INVALID_ARGUMENT,
// naming the field, and the key keeps its value. Guard: refuse it on a delete
// only (the put is applied).
func TestAClientCannotSendARepairBoundOnAPut(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "Tree", Type: BucketTypeBTree})
	for _, arm := range []struct {
		op     transport.Opcode
		bucket string
	}{{op: transport.OpPut, bucket: testBucket}, {op: transport.OpBTreeInsert, bucket: "Tree"}} {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: arm.bucket, Key: "k", Data: map[string]interface{}{"v": "kept"}})
		frame := tc.frame(0, arm.op, bson.M{"database": testDatabase, "bucket": arm.bucket, "key": "k",
			"data": bson.M{"v": "overwritten"}, "repair_bound": int64(1)})
		var refusal ErrorResponse
		if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
			t.Errorf("%s: a client write carrying repair_bound was answered %s: %s", arm.op, frame.Opcode, frame.Payload)
			continue
		}
		if transport.ErrorCode(refusal.Code) != transport.ErrInvalidArgument || refusal.Message !=
			`The field "repair_bound" is internal to hoardDB's own repair path and cannot be set by a client. Remove it from the request.` {
			t.Errorf("%s: the refusal has code %d and text %q", arm.op, refusal.Code, refusal.Message)
		}
		found, data, _, err := tc.nodes[0].server.core.versionedFind(testDatabase, arm.bucket, "k")
		if err != nil || !found || docField(t, data, "v") != "kept" {
			t.Errorf("%s: the refused write changed k (found=%v, %v)", arm.op, found, err)
		}
	}
}

// TestASparedRepairPutIsAnsweredAndLeavesTheKey: a put bounded at the stored
// version is answered Success, says the document is newer, and leaves the
// key's value and version; one bounded above it writes the key. A key with no
// version side-key, or none at all, is version 0, below any bound. Guard: let
// a bound equal to the stored version write (the newer value is overwritten).
func TestASparedRepairPutIsAnsweredAndLeavesTheKey(t *testing.T) {
	s := benchServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatal(err)
	}
	const v = uint64(1)<<44 | 9
	put := func(b boundedBucket, key, title string, bound uint64) *InsertResponse {
		t.Helper()
		resp, err := s.InsertCtx(context.Background(), InsertRequest{Database: "app", Bucket: b.name, Key: key,
			Data: map[string]interface{}{"title": title}, Admitted: true, RepairBound: bound})
		if err != nil || !resp.Success {
			t.Fatalf("%s: the bounded put of %s was answered %+v, %v", b.name, key, resp, err)
		}
		return resp
	}
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		b := newBoundedBucket(t, s, bucketType)
		b.write(t, "k", v)
		if resp := put(b, "k", "repair", v); resp.Message != "document is newer than the repair; left in place" {
			t.Errorf("%s: a put bounded at the stored version was answered %q", bucketType, resp.Message)
		}
		found, data, held, err := s.versionedFind("app", b.name, "k")
		if err != nil || !found || held != v || docField(t, data, "title") != "k" {
			t.Errorf("%s: the spared put changed k (found=%v version=%d, %v)", bucketType, found, held, err)
		}
		put(b, "k", "repair", v+1)
		if _, data, _, err := s.versionedFind("app", b.name, "k"); err != nil || docField(t, data, "title") != "repair" {
			t.Errorf("%s: a put bounded above the stored version did not write k (%v)", bucketType, err)
		}
		b.write(t, "unstamped", 0)
		put(b, "unstamped", "repair", 1)
		put(b, "absent", "repair", 1)
		for _, key := range []string{"unstamped", "absent"} {
			if found, data, _, err := s.versionedFind("app", b.name, key); err != nil || !found || docField(t, data, "title") != "repair" {
				t.Errorf("%s: a put bounded at 1 did not write %s, a key at version 0 (found=%v, %v)", bucketType, key, found, err)
			}
		}
	}
}

// BUG-0603: a delete removes the version side-key BUG-0480's bound compares
// with, so the owner decides before it logs a repair put: inside the bucket's
// fence, taken exclusively, it reads its own log after the position the read
// saw and drops the repair when a put or delete of the key is there, or when
// the log no longer reaches back that far.

// deleteAllDuringRead is a client delete at all, for a seam that runs on a
// server goroutine and cannot fail the test; it returns why the delete
// failed, or "".
func deleteAllDuringRead(tc *testCluster, index int, key string) string {
	payload, err := bson.Marshal(deleteAt(testDatabase, testBucket, key, "all"))
	if err != nil {
		return err.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := tc.dial(index)
	defer func() { _ = client.Close() }()
	frame, err := client.SendRequest(ctx, transport.OpDelete, payload)
	switch {
	case err != nil:
		return err.Error()
	case frame.Opcode == transport.OpError:
		return string(frame.Payload)
	}
	var answer DeleteResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		return fmt.Sprintf("decoding the delete's answer: %v", err)
	}
	if !answer.Success || answer.CopiesConfirmed != 3 {
		return fmt.Sprintf("the delete was answered %+v", answer)
	}
	return ""
}

// heldNowhere fails if any node holds key.
func (a absenceCluster) heldNowhere(t *testing.T, key string) {
	t.Helper()
	for index := range a.tc.nodes {
		found, data, version, err := a.tc.nodes[index].server.core.versionedFind(testDatabase, testBucket, key)
		if err != nil || found {
			t.Errorf("node %d holds deleted %s after the repair (found=%v version=%d data=%s, %v)", index+1, key, found, version, data, err)
		}
	}
}

// TestConsistentGetRepairSparesADeleteMadeDuringTheRead: a key written at all
// as A, with one replica holding an older copy, is read by a consistent get
// on the owner; the coordinator is held after every holder's read and before
// its repair put, a client delete of the key at all lands, and the read then
// sends its repair of A. Once a later write at all has reached every copy, no
// node holds the key: the owner found the delete in its log after the read's
// position and dropped the repair before logging it. Guard: skip the log
// check (every node holds A, as before the fix).
func TestConsistentGetRepairSparesADeleteMadeDuringTheRead(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			a := newRepairPutCluster(t, bucketType)
			defer a.tc.stop()
			a.put(t, "k", "A", "all")
			stale := a.tc.nodes[a.replicas[0]].server.core
			writeBackAt(t, stale, testDatabase, testBucket, "k", map[string]interface{}{"v": "stale"},
				versionOn(t, stale, testDatabase, testBucket, "k")-1)

			var deleteErr string
			var during hookOnce
			hold := holdWhile(t, &during, a.tc.addrs[a.owner], func() { deleteErr = deleteAllDuringRead(a.tc, a.owner, "k") })
			beforeRepairPutHook.Store(&hold)
			resp := a.read(t, "k")
			beforeRepairPutHook.Store(nil)
			if !during.fired || deleteErr != "" {
				t.Fatalf("setup: the delete during the read did not land (fired=%v): %s", during.fired, deleteErr)
			}
			if !resp.Found || docField(t, resp.Data, "v") != "A" {
				t.Errorf("the read answered found=%v data=%s; want A, the value it read", resp.Found, resp.Data)
			}
			a.put(t, "barrier", "after", "all")
			a.heldNowhere(t, "k")
		})
	}
}

// TestARepairPutIsDroppedWhenTheOwnersLogNoLongerReachesTheRead: the read
// finds a stale replica; during the window another key of the bucket is
// written at all and the owner's log is pruned past the read's position, so
// the owner cannot tell whether the key was written after the read. It drops
// the repair rather than apply it: the stale replica keeps its copy, which
// the next consistent get repairs. Guard: treat a log that no longer reaches
// the position as holding no write of the key (the stale copy is repaired).
func TestARepairPutIsDroppedWhenTheOwnersLogNoLongerReachesTheRead(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			a := newRepairPutCluster(t, bucketType)
			defer a.tc.stop()
			a.put(t, "k", "A", "all")
			stale := a.tc.nodes[a.replicas[0]].server.core
			writeBackAt(t, stale, testDatabase, testBucket, "k", map[string]interface{}{"v": "stale"},
				versionOn(t, stale, testDatabase, testBucket, "k")-1)
			wal := a.tc.nodes[a.owner].server.core.wal
			wal.SetMaxSegmentBytes(512)
			readAt := a.ownerLog()

			var setupErr string
			var during hookOnce
			hold := holdWhile(t, &during, a.tc.addrs[a.owner], func() {
				for i := range 4 {
					if setupErr = putAllDuringRead(a.tc, a.owner, fmt.Sprintf("other%d", i), "x"); setupErr != "" {
						return
					}
				}
				if err := wal.Truncate(wal.LastSequence()); err != nil {
					setupErr = err.Error()
				}
			})
			beforeRepairPutHook.Store(&hold)
			a.read(t, "k")
			beforeRepairPutHook.Store(nil)
			if !during.fired || setupErr != "" {
				t.Fatalf("setup: the window's writes and prune did not happen (fired=%v): %s", during.fired, setupErr)
			}
			if first, err := wal.FirstSequence(); err != nil || first <= readAt+1 {
				t.Fatalf("setup: the owner's log still reaches the read at %d (first %d, %v)", readAt, first, err)
			}
			a.put(t, "barrier", "after", "all")
			found, data, _, err := stale.versionedFind(testDatabase, testBucket, "k")
			if err != nil || !found || docField(t, data, "v") != "stale" {
				t.Errorf("the stale replica was repaired from a read the owner's log no longer reaches (found=%v data=%s, %v)", found, data, err)
			}
		})
	}
}

// TestAnOrdinaryPutDoesNotWaitForAnotherPutOfItsBucket: only a repair put
// takes its bucket's fence exclusively. A client put held inside the owner's
// fence does not stop a second client put of another key in the same bucket
// from being applied and answered. Guard: take the fence exclusively for
// every put (the second put waits for the first and times out here).
func TestAnOrdinaryPutDoesNotWaitForAnotherPutOfItsBucket(t *testing.T) {
	a := newRepairPutCluster(t, BucketTypeHash)
	defer a.tc.stop()
	ownerID := a.tc.addrs[a.owner]
	reached, release := make(chan struct{}), make(chan struct{})
	// Not a sync.Once: its Do makes a second caller wait for the first.
	var held atomic.Bool
	hold := func(nodeID string, op transport.Opcode, bucket string) {
		if nodeID != ownerID || op != transport.OpPut || bucket != testBucket || !held.CompareAndSwap(false, true) {
			return
		}
		close(reached)
		<-release
	}
	inOwnerFenceHook.Store(&hold)
	defer inOwnerFenceHook.Store(nil)
	firstDone := make(chan string, 1)
	go func() { firstDone <- putAllDuringRead(a.tc, a.owner, "first", "1") }()
	<-reached

	secondDone := make(chan string, 1)
	go func() { secondDone <- putAllDuringRead(a.tc, a.owner, "second", "2") }()
	waited := false
	select {
	case failure := <-secondDone:
		if failure != "" {
			t.Errorf("the second put failed while the first was held: %s", failure)
		}
	case <-time.After(5 * time.Second):
		waited = true
		t.Error("a put of another key waited for a put of its bucket held inside the owner's fence")
	}
	close(release)
	if failure := <-firstDone; failure != "" {
		t.Errorf("the held put failed: %s", failure)
	}
	if waited {
		<-secondDone
	}
}

// TestARepairPutReadFromAnotherCopyIsDropped: a repair put whose read position
// names another owner, another incarnation or term, or none at all, is
// answered not_applied by the owner and logs nothing, and the key keeps its
// value; one that matches the owner's copy is applied. Guard: skip the owner,
// incarnation and term comparison (the mismatched repairs are applied).
func TestARepairPutReadFromAnotherCopyIsDropped(t *testing.T) {
	a := newRepairPutCluster(t, BucketTypeHash)
	defer a.tc.stop()
	a.put(t, "k", "A", "all")
	c := clusterOf(a.tc, a.owner)
	st, err := c.ownersBucketState(testDatabase, testBucket)
	if err != nil {
		t.Fatal(err)
	}
	matching := RepairReadPosition{Owner: c.self.ID, Incarnation: st.Incarnation, Term: st.Term, Through: st.Through}
	repair := func(read *RepairReadPosition) InsertResponse {
		t.Helper()
		payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k",
			Data: map[string]interface{}{"v": "repaired"}, Admitted: true, RepairBound: 1 << 62, RepairRead: read})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.write(context.Background(), ReplEntry{Op: int32(transport.OpUpsert), Bucket: testBucket, Key: []byte("k"), Payload: payload},
			placementOf(testDatabase, testBucket), WcOne)
		var answer InsertResponse
		if err != nil || bson.Unmarshal(resp.Payload, &answer) != nil {
			t.Fatalf("the repair put was answered %s, %v", resp.Payload, err)
		}
		return answer
	}
	otherOwner, otherIncarnation, otherTerm := matching, matching, matching
	otherOwner.Owner = "elsewhere:1"
	otherIncarnation.Incarnation++
	otherTerm.Term++
	for name, read := range map[string]*RepairReadPosition{"no position": nil, "another owner": &otherOwner,
		"another incarnation": &otherIncarnation, "another term": &otherTerm} {
		logged := a.ownerLog()
		if answer := repair(read); answer.WriteOutcome != outcomeNotApplied {
			t.Errorf("%s: the repair was answered %+v, not dropped", name, answer)
		}
		if a.ownerLog() != logged {
			t.Errorf("%s: the dropped repair was logged", name)
		}
	}
	a.heldEverywhere(t, "k", "A")
	if answer := repair(&matching); answer.WriteOutcome == outcomeNotApplied {
		t.Errorf("a repair read from the owner's own copy was dropped: %s", answer.Message)
	}
}
