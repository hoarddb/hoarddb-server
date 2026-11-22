package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 9, step 9.2: the term on a shipped entry.

// replEntryBeforeTerms is a ReplEntry as the tree before dispatch 9 encoded it
// (46e5fc27): sequence 7, a put to bucket "q" with key "k", the owner's
// outcome "applied", and the payload of termZeroEntry.
const replEntryBeforeTerms = "a900000012736571000700000000000000106f700010000000026275636b657400020000007100056b65790001000000006b057061796c6f616400550000000055000000026461746162617365000400000061707000026275636b657400020000007100026b657900020000006b000364617461000c000000106e000100000000027772697465636f6e6365726e00010000000000106f776e65725f6f7574636f6d65000100000000"

// termZeroEntry is the entry replEntryBeforeTerms encodes, in term 0.
func termZeroEntry(t *testing.T) ReplEntry {
	t.Helper()
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "q", Key: "k", Data: map[string]interface{}{"n": int32(1)}})
	if err != nil {
		t.Fatalf("encoding the payload: %v", err)
	}
	return ReplEntry{Seq: 7, Op: int32(transport.OpPut), Bucket: "q", Key: []byte("k"), Payload: payload, OwnerOutcome: ownerOutcomeApplied}
}

// TestAnEntryOfTermZeroEncodesAsItDidBefore: an entry of a bucket that
// predates this build ships in term 0, and is the bytes it was before the
// field existed. Guard: the term without omitempty.
func TestAnEntryOfTermZeroEncodesAsItDidBefore(t *testing.T) {
	want, err := hex.DecodeString(replEntryBeforeTerms)
	if err != nil {
		t.Fatalf("decoding the frozen bytes: %v", err)
	}
	encoded, err := bson.Marshal(termZeroEntry(t))
	if err != nil {
		t.Fatalf("encoding the entry: %v", err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("an entry in term 0 encodes as\n%x\nbefore this build it encoded as\n%x", encoded, want)
	}
}

// termRecordingConn wraps a peer's real connection and remembers every entry
// of one bucket pushed on it.
type termRecordingConn struct {
	peerConn
	bucket string
	mu     sync.Mutex
	sent   []ReplEntry
}

func (r *termRecordingConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		var request ReplPushRequest
		if err := bson.Unmarshal(payload, &request); err == nil {
			r.mu.Lock()
			for _, entry := range request.Entries {
				if entry.Bucket == r.bucket {
					r.sent = append(r.sent, entry)
				}
			}
			r.mu.Unlock()
		}
	}
	return r.peerConn.SendRequest(ctx, lane, op, payload)
}

func (r *termRecordingConn) entries() []ReplEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ReplEntry(nil), r.sent...)
}

// bucketOutsideOf returns a bucket name the three-copy ring of addrs does not
// place on outside, so removing outside leaves its set as it was.
func bucketOutsideOf(t *testing.T, addrs []string, outside string) string {
	t.Helper()
	full, without := ring.New(3), ring.New(3)
	for _, id := range addrs {
		if err := full.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
		if id == outside {
			continue
		}
		if err := without.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
	}
	// Placed off the node and with the same set, owner first, once it is
	// removed: on a bounded-load ring the removal of a node outside a set can
	// still reorder it, and a bucket whose owner changes is refused while its
	// new owner takes it over (replication step 10b.3).
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("termed%03d", i)
		set, err := full.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		after, err := without.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s without %s: %v", name, outside, err)
		}
		if !nodeListed(set, outside) && slices.Equal(nodeIDs(set), nodeIDs(after)) {
			return name
		}
	}
	t.Fatalf("no bucket name of 1000 is placed off %s with the same set once it is removed", outside)
	return ""
}

// TestBucketOutsideOfRejectsAReorderedSet (BUG-0378): on these addresses the
// removal of the last node reorders termed006's set without removing it from
// the set, which moves the owner; an entry of the bucket is then stranded on
// the previous owner and a replay to a replica ships nothing (I-10).
// bucketOutsideOf must not return such a bucket: with the predicate that only
// checks the removed node is outside the set, termed006 is the first name it
// qualifies, and the term-stamp test finds its replay empty. It returns a
// bucket whose ordered set the removal leaves as it was instead. Guard: accept
// any bucket off the removed node.
func TestBucketOutsideOfRejectsAReorderedSet(t *testing.T) {
	addrs := []string{"127.0.0.1:48590", "127.0.0.1:33632", "127.0.0.1:43098", "127.0.0.1:38553"}
	outside := addrs[len(addrs)-1]
	full := ringWith(t, 3, addrs...)
	without := ringWith(t, 3, addrs[:len(addrs)-1]...)
	if keptInPlace(full, without, "termed006") {
		t.Fatal("test setup: the removal does not move termed006's owner on these addresses")
	}
	picked := bucketOutsideOf(t, addrs, outside)
	if !keptInPlace(full, without, picked) {
		t.Errorf("bucketOutsideOf returned %s, whose owner the removal of %s moves", picked, outside)
	}
}

// TestAnEntryOfALaterTermCarriesItsTerm: a bucket created at epoch 1 is in
// term 1; after a membership change to epoch 2 that leaves its set as it was,
// every entry of it its owner ships — live, and from its log in a replay —
// carries term 1: the bucket's term, not the epoch the write was made in.
// Guards: stamp the current epoch; stamp nothing on a replay.
func TestAnEntryOfALaterTermCarriesItsTerm(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	const outsideIndex = 3
	bucket := bucketOutsideOf(t, tc.addrs, tc.addrs[outsideIndex])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	ownerIndex := ownerIndexOf(t, tc, bucket)
	owner := clusterOf(tc, ownerIndex)
	createdIn := owner.Epoch()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := owner.proposeRemoval(ctx, tc.addrs[outsideIndex]); err != nil {
		t.Fatalf("removing node %d: %v", outsideIndex+1, err)
	}
	if owner.Epoch() == createdIn {
		t.Fatalf("test setup: the owner is still at epoch %d after the removal", createdIn)
	}

	var replica ring.Node
	for _, id := range mustReplicaIDs(t, owner, testDatabase, bucket) {
		if id != owner.self.ID {
			replica = mustNode(t, owner, id)
			break
		}
	}
	recorder := &termRecordingConn{bucket: bucket}
	wrapPeerConn(owner.peerFor(replica), func(inner peerConn) peerConn {
		recorder.peerConn = inner
		return recorder
	})
	for i := 0; i < 5; i++ {
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	shipped := recorder.entries()
	if len(shipped) == 0 {
		t.Fatal("test setup: the owner shipped no entry of the bucket to the recorded replica")
	}
	for _, entry := range shipped {
		if entry.Term != createdIn {
			t.Errorf("the owner shipped sequence %d in term %d; the bucket was created at epoch %d, which is its term (the write was made at epoch %d)",
				entry.Seq, entry.Term, createdIn, owner.Epoch())
		}
	}

	logged, err := owner.s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the owner's log: %v", err)
	}
	replayed, _, err := owner.replayableEntries(logged, owner.peerFor(replica))
	if err != nil {
		t.Fatalf("reading what a replay to the replica ships: %v", err)
	}
	found := 0
	for _, entry := range replayed {
		if entry.Bucket != bucket {
			continue
		}
		found++
		if entry.Term != createdIn {
			t.Errorf("a replay ships sequence %d (%s) in term %d; the bucket's term is %d", entry.Seq, transport.Opcode(entry.Op), entry.Term, createdIn)
		}
	}
	if found == 0 {
		t.Fatal("test setup: a replay to the replica ships no entry of the bucket")
	}
}
