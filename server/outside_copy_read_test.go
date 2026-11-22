package server

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0560: a non-owner holder decided whether to answer a read from the
// registry and an open-store flag only. A copy marked outside (it left the
// bucket's replica set, and the receiving rule refuses its entries until it is
// copied again) that a later membership change places in the set again
// therefore answered reads from data that stopped where it left. The holder's
// read gate now reads the marks as the write side does.

// outsideHolder is a bucket at factor 3 whose owner's stream to one replica is
// frozen after 'before' reached every copy.
type outsideHolder struct {
	tc       *testCluster
	key      placementKey
	owner    int
	replica  int
	unfreeze func()
}

// frozenHolderHoldingBefore starts three nodes at factor 3, creates the test
// bucket, writes 'before' at all, and freezes the owner's pushes to one
// replica.
func frozenHolderHoldingBefore(t *testing.T) *outsideHolder {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	h := &outsideHolder{tc: tc, key: placementOf(testDatabase, testBucket), owner: ownerIndexOf(t, tc, testBucket)}
	h.replica = (h.owner + 1) % len(tc.nodes)
	tc.ok(h.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "before",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	ownerCluster := clusterOf(tc, h.owner)
	h.unfreeze = freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[h.replica])))
	return h
}

func (h *outsideHolder) stop() {
	h.unfreeze()
	h.tc.stop()
}

// writeAfter writes 'after' at majority through the owner: the owner and the
// replica that is not frozen acknowledge it.
func (h *outsideHolder) writeAfter(t *testing.T) {
	t.Helper()
	h.tc.ok(h.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "after",
		Data: map[string]interface{}{"n": 2}, WriteConcern: "majority"})
	if holdsKeyLocally(h.tc.nodes[h.replica], testBucket, "after") {
		t.Fatal("setup: the frozen replica holds 'after'; its stream was not frozen")
	}
}

// TestAnOutsideCopyPlacedInItsSetAgainDoesNotAnswerReads is BUG-0560's
// acceptance test. The replica's copy is marked outside, as the placement pass
// marks a copy that leaves its set, while the ring places the replica in the
// set: the state a second membership change that places it back leaves. A
// plain read of 'after', acknowledged at majority after the copy stopped
// receiving, is forwarded to the owner or refused with the take-over's
// message; it is never answered "not found" from the frozen copy. Once the
// worker runs, the read has named the bucket to it: the copy is copied again
// and the mark is gone, with no restart. Guard: drop the marks check from
// holderServes.
func TestAnOutsideCopyPlacedInItsSetAgainDoesNotAnswerReads(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	h := frozenHolderHoldingBefore(t)
	defer h.stop()
	replica := clusterOf(h.tc, h.replica)
	if err := replica.markOutside(h.key); err != nil {
		t.Fatalf("setup: marking the replica's copy outside: %v", err)
	}
	if !replica.Hosts(h.key) || !h.tc.nodes[h.replica].server.core.bucketStoreOpen(testDatabase, testBucket) {
		t.Fatal("setup: the replica is not a holder with an open copy; the read would be forwarded for another reason")
	}
	h.writeAfter(t)
	t.Logf("the replica's marks: %+v", replica.placementMarksOf(h.key))

	assertForwardedOrRefused(t, h.tc.frame(h.replica, transport.OpGet,
		FindRequest{Database: testDatabase, Bucket: testBucket, Key: "after"}), "a get of 'after'")
	count := h.tc.frame(h.replica, transport.OpCount, CountRequest{Database: testDatabase, Bucket: testBucket})
	if count.Opcode != transport.OpError {
		var answer CountResponse
		if err := bson.Unmarshal(count.Payload, &answer); err != nil {
			t.Fatalf("decoding the count at the replica: %v", err)
		}
		if answer.Count != 2 {
			t.Errorf("a count at the replica whose copy is marked outside answered %d, want 2 (forwarded to the owner) or the take-over's refusal", answer.Count)
		}
	}
	if t.Failed() {
		return
	}

	// The worker's idle wait is takeOverRetryCeiling (30 s); a copy copied
	// well inside it was copied because a read woke the worker. The stream
	// stays frozen: an entry refused by the receiving rule would wake the
	// worker too, and hide a read that does not.
	takeOverOffHook.Store(false)
	deadline := time.Now().Add(takeOverRetryCeiling / 3)
	for replica.placementMarksOf(h.key).Outside != 0 || !holdsKeyLocally(h.tc.nodes[h.replica], testBucket, "after") {
		if time.Now().After(deadline) {
			t.Fatalf("%s after the worker was let run, the replica's marks are %+v and its copy holds 'after': %v; the read did not start the copy again",
				takeOverRetryCeiling/3, replica.placementMarksOf(h.key), holdsKeyLocally(h.tc.nodes[h.replica], testBucket, "after"))
		}
		assertForwardedOrRefused(t, h.tc.frame(h.replica, transport.OpGet,
			FindRequest{Database: testDatabase, Bucket: testBucket, Key: "after"}), "a get of 'after' while the copy is copied again")
		if t.Failed() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	if found, message := foundAt(t, h.tc, h.replica, "after"); !found {
		t.Errorf("once the copy was copied again, a get of 'after' at the replica answered %q", message)
	}
}

// assertForwardedOrRefused asserts that a get of 'after' at the replica whose
// copy is marked outside answered the document (forwarded to the owner) or
// refused with a message that names the take-over.
func assertForwardedOrRefused(t *testing.T, frame *transport.Frame, what string) {
	t.Helper()
	if frame.Opcode == transport.OpError {
		if message := refusalOf(t, frame).Message; !strings.Contains(message, "moving") && !strings.Contains(message, "copied again") {
			t.Errorf("%s at the replica whose copy is marked outside was refused with %q, want the take-over's refusal", what, message)
		}
		return
	}
	var answer FindResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding %s: %v", what, err)
	}
	if !answer.Found {
		t.Errorf("%s at the replica whose copy is marked outside answered %s found false %q: the copy stopped receiving before the write was acknowledged",
			what, frame.Opcode, answer.Message)
	}
}

// TestAnUnmarkedHolderStillAnswersReadsLocally is BUG-0560's green path: the
// marks check refuses only a marked copy. With the owner stopped, an unmarked
// holder answers 'before' from its own copy, which a forward to the owner
// could not. Guard: refuse every non-owner holder in holderServes.
func TestAnUnmarkedHolderStillAnswersReadsLocally(t *testing.T) {
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	h := frozenHolderHoldingBefore(t)
	defer h.stop()
	h.unfreeze()
	if marks := clusterOf(h.tc, h.replica).placementMarksOf(h.key); marks.Outside != 0 || marks.Moved != 0 {
		t.Fatalf("setup: the replica's copy carries marks %+v; want none", marks)
	}
	h.tc.stopNodeKeepingData(h.owner)
	if found, message := foundAt(t, h.tc, h.replica, "before"); !found {
		t.Errorf("an unmarked holder, with the owner stopped, answered a get of 'before' with %q; want the document from its own copy", message)
	}
}
