package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// forceStale overwrites a key with an unversioned write (version 0, i.e. as if
// written before versioning existed), so the node holds a genuinely stale copy
// of the document the rest of the cluster has moved past.
func forceStale(t *testing.T, s *Server, database, bucket, key string, val map[string]interface{}) {
	t.Helper()
	b, err := s.resolveBucket(database, bucket)
	if err != nil {
		t.Fatalf("resolve bucket: %v", err)
	}
	b.rec.mu.Lock()
	defer b.rec.mu.Unlock()
	if err := b.hashStore.Put(store.ApplyInfo{}, key, val, 0); err != nil {
		t.Fatalf("write stale copy: %v", err)
	}
	// A genuinely stale holder is one that never got the versioning write: drop
	// the side-key so GetVersion reads 0 (older than any acknowledged write).
	if err := b.hashStore.ClearVersion(key); err != nil {
		t.Fatalf("clear stale version: %v", err)
	}
}

// consistentGet sends a quorum read to the given node and decodes the response.
func consistentGet(t *testing.T, n *testNode, database, bucket, key string) ConsistentGetResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload, _ := bson.Marshal(ConsistentGetRequest{Database: database, Bucket: bucket, Key: key})
	f, err := n.client.SendRequest(ctx, transport.OpConsistentGet, payload)
	if err != nil {
		t.Fatalf("consistent get from %s: %v", n.addr, err)
	}
	var resp ConsistentGetResponse
	if err := bson.Unmarshal(f.Payload, &resp); err != nil {
		t.Fatalf("decode consistent get response: %v", err)
	}
	return resp
}

func docField(t *testing.T, data []byte, field string) string {
	t.Helper()
	var doc map[string]interface{}
	if err := bson.Unmarshal(data, &doc); err != nil {
		t.Fatalf("decode doc: %v", err)
	}
	return fmt.Sprintf("%v", doc[field])
}

// TestConsistentGetReturnsFreshCopyAndRepairs: a stale replica is detected by a
// quorum read and fixed in place through the normal replication path.
// Re-aimed by step 7.6 (D-2): the write is checked to be on all three copies by its copies_confirmed.
func TestConsistentGetRepairsStaleReplica(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the write of k1")

	// Make node 2 hold a stale copy (version 0).
	forceStale(t, tc.nodes[2].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": 0})

	resp := consistentGet(t, tc.nodes[0], testDatabase, testBucket, "k1")
	if !resp.Found || resp.Version == 0 {
		t.Fatalf("quorum read lost the document (found=%v version=%d)", resp.Found, resp.Version)
	}
	if !resp.Repaired {
		t.Fatalf("a stale replica was not repaired")
	}
	if got := docField(t, resp.Data, "n"); got != "1" {
		t.Fatalf("quorum read returned %s, want the acknowledged value 1", got)
	}

	// The stale replica converges through replication (the repair is confirmed
	// at write-concern one locally and shipped to replicas in the background),
	// so poll until it holds the fresh value at a version at least as new as
	// the read reported.
	deadline := time.Now().Add(15 * time.Second)
	for {
		found, data, v, err := tc.nodes[2].server.core.versionedFind(testDatabase, testBucket, "k1")
		if err == nil && found && v >= resp.Version && docField(t, data, "n") == "1" {
			break
		}
		if time.Now().After(deadline) {
			found, data, v, err := tc.nodes[2].server.core.versionedFind(testDatabase, testBucket, "k1")
			t.Fatalf("repaired replica did not converge (found=%v version=%d err=%v data=%s)",
				found, v, err, string(data))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestConsistentGetRepairsAStaleOwner: a quorum read from a non-owner returns
// the fresh value held by replicas and repairs the owner through the owner
// write path.
// Re-aimed by step 7.6 (D-2): the write is checked to be on all three copies by its copies_confirmed.
func TestConsistentGetRepairsAStaleOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1", Data: map[string]interface{}{"n": 2}, WriteConcern: "all"}), "the write of k1")

	oc := tc.nodes[0].server.core.cluster
	owner, err := oc.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	var ownerIdx int
	for i, a := range tc.addrs {
		if a == owner.ID {
			ownerIdx = i
			break
		}
	}
	// A different member runs the read so the coordinator is not the owner.
	coordIdx := (ownerIdx + 1) % 3
	forceStale(t, tc.nodes[ownerIdx].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": 0})

	resp := consistentGet(t, tc.nodes[coordIdx], testDatabase, testBucket, "k1")
	if !resp.Found || !resp.Repaired {
		t.Fatalf("quorum read from a non-owner did not repair (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	if got := docField(t, resp.Data, "n"); got != "2" {
		t.Fatalf("quorum read returned %s, want 2", got)
	}
	// The stale owner was healed.
	found, data, _, err := tc.nodes[ownerIdx].server.core.versionedFind(testDatabase, testBucket, "k1")
	if err != nil || !found {
		t.Fatalf("owner after repair (found=%v err=%v)", found, err)
	}
	if got := docField(t, data, "n"); got != "2" {
		t.Fatalf("owner repaired to %s, want 2", got)
	}
}

// TestConsistentGetMissingKey: a key on no holder is not-found, never repaired.
func TestConsistentGetMissingKey(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	resp := consistentGet(t, tc.nodes[0], testDatabase, testBucket, "absent")
	if resp.Found || resp.Repaired {
		t.Fatalf("missing key returned found=%v repaired=%v", resp.Found, resp.Repaired)
	}
}

// TestConsistentGetSingleNodeDegrades: a one-member cluster is a plain local
// read, with no peer traffic and no repair.
func TestConsistentGetSingleNodeDegrades(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1", Data: map[string]interface{}{"n": 3}})

	resp := consistentGet(t, tc.nodes[0], testDatabase, testBucket, "k1")
	if !resp.Found || resp.Repaired {
		t.Fatalf("single-node consistent read (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	if got := docField(t, resp.Data, "n"); got != "3" {
		t.Fatalf("single-node read returned %s, want 3", got)
	}
}

// TestConsistentGetIsNotAMissWhenHoldersCannotAnswer guards the read path. With
// both peers down and no local copy, nobody answered "absent": the read is
// unknown, and replying Found:false with no message made the CLI print
// "not found" for a key that may exist. The reply must carry a message that
// says how many holders could not answer.
func TestConsistentGetIsNotAMissWhenHoldersCannotAnswer(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	// The surviving node is the bucket's owner, so the read is answered locally
	// rather than forwarded to a holder that is gone.
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	survivor := nodeIndexByAddr(t, tc.addrs, owner.ID)
	stopEveryNodeExcept(t, tc, survivor)

	resp := consistentGet(t, tc.nodes[survivor], testDatabase, testBucket, "never-written-here")
	if resp.Found {
		t.Fatalf("a key no holder returned was reported found")
	}
	if resp.Message == "" {
		t.Fatalf("consistent get answered Found:false with no message although 2 of 3 holders could not answer; " +
			"the CLI prints that as \"not found\"")
	}
	for _, want := range []string{"2 of 3", "could not answer", "may exist"} {
		if !strings.Contains(resp.Message, want) {
			t.Errorf("message %q does not contain %q", resp.Message, want)
		}
	}
}

// consistentGetFrame sends a quorum read to node n and returns the raw frame,
// so a test can tell an answer from a refusal (an error frame).
func consistentGetFrame(t *testing.T, n *testNode, database, bucket, key string) *transport.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload, err := bson.Marshal(ConsistentGetRequest{Database: database, Bucket: bucket, Key: key})
	if err != nil {
		t.Fatalf("encode consistent get: %v", err)
	}
	frame, err := n.client.SendRequest(ctx, transport.OpConsistentGet, payload)
	if err != nil {
		t.Fatalf("consistent get from %s: %v", n.addr, err)
	}
	return frame
}

// requireConsistentGetRefused fails the test unless frame is a refusal with
// code want whose message names every one of names, and returns the message.
func requireConsistentGetRefused(t *testing.T, frame *transport.Frame, want transport.ErrorCode, names ...string) string {
	t.Helper()
	if frame.Opcode != transport.OpError {
		var answered ConsistentGetResponse
		_ = bson.Unmarshal(frame.Payload, &answered)
		t.Fatalf("consistent get answered (found=%v repaired=%v version=%d message=%q), want a refusal with code %d",
			answered.Found, answered.Repaired, answered.Version, answered.Message, want)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decode the refusal: %v", err)
	}
	if transport.ErrorCode(refusal.Code) != want {
		t.Fatalf("consistent get refused with code %d (%q), want %d", refusal.Code, refusal.Message, want)
	}
	for _, name := range names {
		if !strings.Contains(refusal.Message, name) {
			t.Errorf("refusal %q does not name %q", refusal.Message, name)
		}
	}
	return refusal.Message
}

// answeredConsistentGet fails the test unless frame is an answer, and decodes
// it.
func answeredConsistentGet(t *testing.T, frame *transport.Frame) ConsistentGetResponse {
	t.Helper()
	if frame.Opcode == transport.OpError {
		var refusal ErrorResponse
		_ = bson.Unmarshal(frame.Payload, &refusal)
		t.Fatalf("consistent get refused (code %d): %s", refusal.Code, refusal.Message)
	}
	var resp ConsistentGetResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decode consistent get response: %v", err)
	}
	return resp
}

// writeBackAt writes val under key into one node's own store at version,
// bypassing the log: a copy that missed a later write. Version 0 leaves the
// key with no version side-key.
func writeBackAt(t *testing.T, s *Server, database, bucket, key string, val map[string]interface{}, version uint64) {
	t.Helper()
	b, err := s.resolveBucket(database, bucket)
	if err != nil {
		t.Fatalf("resolve bucket: %v", err)
	}
	b.rec.mu.Lock()
	defer b.rec.mu.Unlock()
	if b.bucketType == BucketTypeBTree {
		if err := b.btreeStore.Put(store.ApplyInfo{}, key, val, version); err != nil {
			t.Fatalf("write back %s at version %d: %v", key, version, err)
		}
		return
	}
	if err := b.hashStore.Put(store.ApplyInfo{}, key, val, version); err != nil {
		t.Fatalf("write back %s at version %d: %v", key, version, err)
	}
}

// versionOn reads key's version side-key on one node's own copy, failing the
// test when the key is not there.
func versionOn(t *testing.T, s *Server, database, bucket, key string) uint64 {
	t.Helper()
	found, _, version, err := s.versionedFind(database, bucket, key)
	if err != nil || !found {
		t.Fatalf("%s: reading %s's version (found=%v): %v", s.cluster.self.ID, key, found, err)
	}
	return version
}

// ownerAndReplicas is the index of a bucket's owner and of its other holders,
// as node 1 places it now.
func ownerAndReplicas(t *testing.T, tc *testCluster, database, bucket string) (int, []int) {
	t.Helper()
	set, err := clusterOf(tc, 0).Replicas(placementOf(database, bucket))
	if err != nil {
		t.Fatalf("placing %s: %v", bucket, err)
	}
	var replicas []int
	for _, member := range set[1:] {
		replicas = append(replicas, nodeIndexByAddr(t, tc.addrs, member.ID))
	}
	return nodeIndexByAddr(t, tc.addrs, set[0].ID), replicas
}

// removeCommitted removes the node at removed through the member at via,
// fails unless the change committed (a change that did not commit moved
// nothing yet, and may land later in the test), and brings every other
// running member to the new epoch as gossip would.
func removeCommitted(t *testing.T, tc *testCluster, via, removed int) {
	t.Helper()
	proposer := clusterOf(tc, via)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	outcome, err := proposer.proposeRemovalOutcome(ctx, tc.addrs[removed])
	if err != nil || outcome.kind != changeCommitted {
		t.Fatalf("removing node %d: outcome %q, %v; want committed", removed+1, outcome.kind, err)
	}
	for index, node := range tc.nodes {
		if index == via || node.server == nil {
			continue
		}
		if err := clusterOf(tc, index).reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("bringing node %d to the removal: %v", index+1, err)
		}
	}
	epoch := proposer.Epoch()
	pollUntil(t, "every running member adopts the removal", func() bool {
		for _, node := range tc.nodes {
			if node.server != nil && node.server.core.cluster.Epoch() != epoch {
				return false
			}
		}
		return true
	})
}

// requireNowhere waits up to ten seconds until no running node in indexes
// holds key, failing with the ones that still do.
func requireNowhere(t *testing.T, tc *testCluster, database, bucket, key string, indexes ...int) {
	t.Helper()
	var holding []string
	if !holdsWithin(10*time.Second, func() bool {
		holding = nil
		for _, index := range indexes {
			found, _, _, err := tc.nodes[index].server.core.versionedFind(database, bucket, key)
			if err != nil || found {
				holding = append(holding, fmt.Sprintf("node %d (found=%v err=%v)", index+1, found, err))
			}
		}
		return len(holding) == 0
	}) {
		t.Errorf("%s is still held after the read: %s", key, strings.Join(holding, ", "))
	}
}

// TestConsistentGetDoesNotResurrectADeletedKey: after a delete acknowledged at
// all, a replica that still holds the key — written back at the version its
// put gave it, at version 0 as the bug's own construction does, or left
// behind for real by a frozen stream — is not the answer: a consistent get
// from every node answers not found, and afterwards no node holds the key.
// The stale copy goes to a replica that is not the owner, chosen by placement:
// the owner's copy is the one whose absence the read trusts. Guard: skip the
// holders that answered not found when the winner is chosen.
func TestConsistentGetDoesNotResurrectADeletedKey(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
	stale := replicas[0]
	every := []int{0, 1, 2}

	putAt := func(key, concern string) {
		requireEveryCopyOrAcknowledged(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Key: key, Data: map[string]interface{}{"n": 1}, WriteConcern: concern}), concern, "the put of "+key)
	}
	deleteKey := func(key, concern string) {
		tc.ok(owner, transport.OpDelete, deleteAt(testDatabase, testBucket, key, concern))
	}
	readEverywhere := func(key string) {
		t.Helper()
		for _, index := range every {
			resp := answeredConsistentGet(t, consistentGetFrame(t, tc.nodes[index], testDatabase, testBucket, key))
			if resp.Found {
				t.Errorf("consistent get of the deleted %s from node %d answered found (version %d, repaired=%v)", key, index+1, resp.Version, resp.Repaired)
			}
		}
	}

	// The bug's arm, with the stale copy at the version its put gave it.
	putAt("k1", "all")
	putVersion := versionOn(t, tc.nodes[stale].server.core, testDatabase, testBucket, "k1")
	deleteKey("k1", "all")
	writeBackAt(t, tc.nodes[stale].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": 1}, putVersion)
	readEverywhere("k1")
	requireNowhere(t, tc, testDatabase, testBucket, "k1", every...)

	// The bug file's own construction: the stale copy written by forceStale,
	// with no version.
	putAt("k2", "all")
	deleteKey("k2", "all")
	forceStale(t, tc.nodes[stale].server.core, testDatabase, testBucket, "k2", map[string]interface{}{"n": 1})
	readEverywhere("k2")
	requireNowhere(t, tc, testDatabase, testBucket, "k2", every...)

	// Built for real: the replica's stream from the owner is frozen across
	// the delete, which is acknowledged at majority without it.
	putAt("k3", "all")
	unfreeze := freezePeer(clusterOf(tc, owner).peerFor(mustNode(t, clusterOf(tc, owner), tc.addrs[stale])))
	defer unfreeze()
	deleteKey("k3", "majority")
	if found, _, _, err := tc.nodes[stale].server.core.versionedFind(testDatabase, testBucket, "k3"); err != nil || !found {
		t.Fatalf("setup: the frozen replica, node %d, does not hold k3 after the delete (found=%v err=%v)", stale+1, found, err)
	}
	readEverywhere("k3")
	unfreeze()
	requireNowhere(t, tc, testDatabase, testBucket, "k3", every...)
}

// deleteAt is a delete of key at a write concern: DeleteRequest carries none of
// its own, and the write path reads the concern from any request's payload.
func deleteAt(database, bucket, key, concern string) bson.M {
	return bson.M{"database": database, "bucket": bucket, "key": key, "write_concern": concern}
}

// requireEveryCopyOrAcknowledged fails unless frame acknowledges a write: at
// all, by every copy.
func requireEveryCopyOrAcknowledged(t *testing.T, frame *transport.Frame, concern, what string) {
	t.Helper()
	if concern == "all" {
		requireEveryCopy(t, frame, what)
		return
	}
	if frame.Opcode == transport.OpError {
		t.Fatalf("%s at %s was refused: %s", what, concern, frame.Payload)
	}
}

// TestConsistentGetWithoutItsOwnerDoesNotResurrectADelete is D6: while the
// bucket's owner cannot be read, a consistent get answers only if every other
// holder answered and they agree. (a) One replica holds a key the owner
// deleted: refused, naming the owner, and nothing is repaired. (b) Both
// replicas agree the key is absent, and then both hold the same version of a
// live key: the read answers. Guards: keep today's comparison when the owner
// errs ((a) answers found); refuse whenever the owner errs ((b) is refused).
func TestConsistentGetWithoutItsOwnerDoesNotResurrectADelete(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
	stale, coordinator := replicas[0], replicas[1]
	for _, key := range []string{"k1", "k2"} {
		requireEveryCopy(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Key: key, Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of "+key)
		tc.ok(owner, transport.OpDelete, deleteAt(testDatabase, testBucket, key, "all"))
	}
	requireEveryCopy(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
		Key: "live", Data: map[string]interface{}{"n": 7}, WriteConcern: "all"}), "the put of live")
	writeBackAt(t, tc.nodes[stale].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": 1}, 1)
	stopNode(t, tc, owner)
	logsBefore := []uint64{clusterOf(tc, stale).walLastSequence(), clusterOf(tc, coordinator).walLastSequence()}

	// (a) The copies disagree: refused, naming the owner.
	requireConsistentGetRefused(t, consistentGetFrame(t, tc.nodes[coordinator], testDatabase, testBucket, "k1"),
		transport.ErrUnavailable, tc.addrs[owner])
	if after := []uint64{clusterOf(tc, stale).walLastSequence(), clusterOf(tc, coordinator).walLastSequence()}; after[0] != logsBefore[0] || after[1] != logsBefore[1] {
		t.Errorf("the refused read logged a repair: the replicas' logs went from %v to %v", logsBefore, after)
	}
	if found, _, _, err := tc.nodes[stale].server.core.versionedFind(testDatabase, testBucket, "k1"); err != nil || !found {
		t.Errorf("the refused read changed node %d's copy of k1 (found=%v err=%v)", stale+1, found, err)
	}

	// (b) The copies agree: the read answers, absent and then found.
	absent := answeredConsistentGet(t, consistentGetFrame(t, tc.nodes[coordinator], testDatabase, testBucket, "k2"))
	if absent.Found || absent.Repaired {
		t.Errorf("k2, absent on both replicas, answered found=%v repaired=%v", absent.Found, absent.Repaired)
	}
	live := answeredConsistentGet(t, consistentGetFrame(t, tc.nodes[coordinator], testDatabase, testBucket, "live"))
	if !live.Found || docField(t, live.Data, "n") != "7" {
		t.Errorf("live, held alike by both replicas, answered found=%v", live.Found)
	}
}

// holdUntil holds the calling goroutine, which a hook runs on, until cond
// holds or takeOverWait passes. A hold that times out writes every
// goroutine's stack to stderr, so the next sighting of a stalled hand-off shows
// what each side was waiting on (BUG-0487).
func holdUntil(what string, cond func() bool) {
	if holdsWithin(takeOverWait, cond) {
		return
	}
	fmt.Fprintf(os.Stderr, "the hold on %s timed out after %s; goroutines:\n", what, takeOverWait)
	if err := pprof.Lookup("goroutine").WriteTo(os.Stderr, 2); err != nil {
		fmt.Fprintf(os.Stderr, "writing the goroutine dump: %v\n", err)
	}
}

// placeOwnerChange finds, for the ring addresses of four nodes, the placement
// setUpAnOwnerChange needs. The replay-first order needs a filler whose owner
// survives the removal: the removed node delivers nothing (addressing), so a
// filler it owns reaches the new owner by the pull alone, the replay never
// comes and the hold on the pull runs out its 60 s (BUG-0487). That is so on
// about two draws of ring addresses in five, and some draws have no filler with
// a surviving owner at all, so the addresses are drawn for one (class A).
func placeOwnerChange(t *testing.T, order handOffOrder, addrs []string) (scene filledRing, found bool) {
	t.Helper()
	scene.full = ringWith(t, 3, addrs...)
	scene.oldOwnerID = ownerOn(scene.full, testBucket)
	var remaining []string
	for _, addr := range addrs {
		if addr != scene.oldOwnerID {
			remaining = append(remaining, addr)
		}
	}
	scene.newOwnerID = ownerOn(ringWith(t, 3, remaining...), testBucket)
	scene.filler, found = findBucketName("filler", func(name string) bool {
		set, err := scene.full.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil || !nodeListed(set, scene.oldOwnerID) || nodeListed(set, scene.newOwnerID) {
			return false
		}
		return order != theReplayComesFirst || set[0].ID != scene.oldOwnerID
	})
	return scene, found
}

// TestTheReplayFirstFillerIsOwnedByASurvivor is the deterministic form of the
// replay-first hold's 60 s failure (BUG-0487): over many draws of ring
// addresses, a filler the replay-first setup picks is never owned by the node
// it removes, while the pull-first setup, which has no such rule, does pick
// one on draws the replay-first setup rejects, so the rule is what separates
// them.
func TestTheReplayFirstFillerIsOwnedByASurvivor(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 487))
	var ownedByRemoved, replayFirstFound int
	const draws = 400
	for range draws {
		addrs := make([]string, 4)
		for index := range addrs {
			addrs[index] = fmt.Sprintf("127.0.0.1:%d", 20000+rng.IntN(40000))
		}
		replayFirst, found := placeOwnerChange(t, theReplayComesFirst, addrs)
		if found {
			replayFirstFound++
			set, err := replayFirst.full.Replicas(placementOf(testDatabase, replayFirst.filler).encoded, 3)
			if err != nil {
				t.Fatalf("placing %s: %v", replayFirst.filler, err)
			}
			if set[0].ID == replayFirst.oldOwnerID {
				t.Errorf("addresses %v: the replay-first filler %s is owned by the removed node %s, which delivers nothing", addrs, replayFirst.filler, set[0].ID)
			}
		}
		pullFirst, pullFirstFound := placeOwnerChange(t, theNewOwnerPullsFirst, addrs)
		if !pullFirstFound || found {
			continue
		}
		set, err := pullFirst.full.Replicas(placementOf(testDatabase, pullFirst.filler).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s: %v", pullFirst.filler, err)
		}
		if set[0].ID == pullFirst.oldOwnerID {
			ownedByRemoved++
		}
	}
	if ownedByRemoved == 0 {
		t.Errorf("no draw of %d had only a pull-first filler owned by the removed node: the rule changes nothing here", draws)
	}
	if replayFirstFound < draws/2 {
		t.Errorf("a replay-first filler with a surviving owner exists on %d of %d draws; the 16 draws of newTestClusterPlaced would miss too often", replayFirstFound, draws)
	}
}

// settleMember is one node as the settle diagnostic shows it.
type settleMember struct {
	addr   string
	roles  []string
	log    uint64
	open   bool
	hasKey bool
}

// settleReport is what setUpAnOwnerChange knows when its settle wait runs out.
type settleReport struct {
	draw             int
	filler           string
	ownerRuleApplies bool
	drawn            filledRing
	holdsEntered     int32
	holdsEnded       int32
	unsettled        string
	members          []settleMember
}

// awaitSettleOrReport waits up to limit for settled to hold. When it does not, report
// writes the diagnostic to w, so the next sighting of a hand-off that does not
// settle (BUG-0487) names the draw, each node's role and log, and the holds.
func awaitSettleOrReport(limit time.Duration, settled func() bool, w io.Writer, report func(io.Writer) error) (bool, error) {
	if holdsWithin(limit, settled) {
		return true, nil
	}
	if err := report(w); err != nil {
		return false, fmt.Errorf("writing the settle diagnostic: %w", err)
	}
	return false, nil
}

// probeMembers reads each node's log length and whether it holds the filler,
// for the settle diagnostic. A node's role is by the ring it was drawn on.
func probeMembers(tc *testCluster, drawn filledRing, filler string) []settleMember {
	set, err := drawn.full.Replicas(placementOf(testDatabase, filler).encoded, 3)
	fillerOwner := ""
	if err == nil {
		fillerOwner = set[0].ID
	}
	members := make([]settleMember, 0, len(tc.nodes))
	for index, node := range tc.nodes {
		addr := tc.addrs[index]
		var roles []string
		if addr == drawn.oldOwnerID {
			roles = append(roles, "old owner", "removed")
		}
		if addr == drawn.newOwnerID {
			roles = append(roles, "new owner")
		}
		if addr == fillerOwner {
			roles = append(roles, "filler owner")
		}
		if len(roles) == 0 {
			roles = append(roles, "fourth node")
		}
		member := settleMember{addr: addr, roles: roles, log: clusterOf(tc, index).walLastSequence()}
		member.open = node.server.core.bucketStoreOpen(testDatabase, filler)
		found, _, _, findErr := node.server.core.versionedFind(testDatabase, filler, fmt.Sprintf("f%03d", fillerWrites-1))
		member.hasKey = findErr == nil && found
		members = append(members, member)
	}
	return members
}

// writeSettleReport writes the settle diagnostic, one line per fact, in the
// style of holdUntil's goroutine dump.
func writeSettleReport(w io.Writer, r settleReport) error {
	var out strings.Builder
	fillerOwner := ""
	for _, member := range r.members {
		if slices.Contains(member.roles, "filler owner") {
			fillerOwner = member.addr
		}
	}
	survivor := fillerOwner != "" && fillerOwner != r.drawn.oldOwnerID
	fmt.Fprintf(&out, "SETTLE-DIAG the hand-off did not settle after %s; ring-address draw %d, filler %s owned by %s (a survivor: %v; the owner rule applies: %v)\n",
		takeOverWait, r.draw, r.filler, fillerOwner, survivor, r.ownerRuleApplies)
	for _, member := range r.members {
		fmt.Fprintf(&out, "SETTLE-DIAG member %s roles [%s] log %d filler open %v holds the last filler key %v\n",
			member.addr, strings.Join(member.roles, ", "), member.log, member.open, member.hasKey)
	}
	fmt.Fprintf(&out, "SETTLE-DIAG holds: %d entered, %d ended (all released: %v)\n", r.holdsEntered, r.holdsEnded, r.holdsEntered == r.holdsEnded)
	fmt.Fprintf(&out, "SETTLE-DIAG unsettled: %s\n", r.unsettled)
	_, err := io.WriteString(w, out.String())
	return err
}

// TestASettleWaitThatRunsOutWritesItsDiagnostic makes the settle wait time out
// at once and checks the diagnostic names the draw, the filler's owner, each
// node's role and log, and the holds (BUG-0487).
func TestASettleWaitThatRunsOutWritesItsDiagnostic(t *testing.T) {
	drawn := filledRing{oldOwnerID: "old:1", newOwnerID: "new:2"}
	report := settleReport{
		draw: 3, filler: "filler010", ownerRuleApplies: true, drawn: drawn, holdsEntered: 3, holdsEnded: 3,
		unsettled: "new:2 does not hold filler010",
		members: []settleMember{
			{addr: "old:1", roles: []string{"old owner", "removed"}, log: 604},
			{addr: "new:2", roles: []string{"new owner"}, log: 3, open: true},
			{addr: "own:3", roles: []string{"filler owner"}, log: 604, open: true, hasKey: true},
			{addr: "fourth:4", roles: []string{"fourth node"}, log: 604},
		},
	}
	var written strings.Builder
	settled, err := awaitSettleOrReport(20*time.Millisecond, func() bool { return false }, &written, func(w io.Writer) error { return writeSettleReport(w, report) })
	if settled || err != nil {
		t.Fatalf("a wait on a condition that never holds returned settled %v, error %v", settled, err)
	}
	for _, want := range []string{
		"draw 3, filler filler010 owned by own:3 (a survivor: true; the owner rule applies: true)",
		"member old:1 roles [old owner, removed] log 604",
		"member new:2 roles [new owner] log 3 filler open true holds the last filler key false",
		"member fourth:4 roles [fourth node]",
		"holds: 3 entered, 3 ended (all released: true)",
		"unsettled: new:2 does not hold filler010",
	} {
		if !strings.Contains(written.String(), want) {
			t.Errorf("the diagnostic lacks %q; it wrote:\n%s", want, written.String())
		}
	}
	var quiet strings.Builder
	settled, err = awaitSettleOrReport(time.Second, func() bool { return true }, &quiet, func(io.Writer) error {
		t.Error("a settled wait wrote the diagnostic")
		return nil
	})
	if !settled || err != nil || quiet.Len() != 0 {
		t.Errorf("a wait that settles returned settled %v, error %v and wrote %q", settled, err, quiet.String())
	}
}

// fillerWrites is how many writes setUpAnOwnerChange makes to the filler.
const fillerWrites = 600

// filledRing is the placement setUpAnOwnerChange needs from a draw of ring
// addresses: who owns testBucket before and after the removal, and a filler.
type filledRing struct {
	full                   *ring.Ring
	oldOwnerID, newOwnerID string
	filler                 string
}

// handOffOrder says which of two arrivals at a new member of the filler's set
// the setup holds back until the other has landed.
type handOffOrder int

const (
	// theNewOwnerPullsFirst holds the old owner's replay to the new owner until
	// the new owner's pull of the filler has landed, so the entries the replay
	// carries are passed over as covered by the pull and never logged.
	theNewOwnerPullsFirst handOffOrder = iota
	// theReplayComesFirst holds the new owner's pull until the replay has
	// logged the filler entries, which is the order a loaded run falls into.
	theReplayComesFirst
)

// ownerChangeScene is the cluster of TestConsistentGetAfterAnOwnerChangeReturnsTheNewerValue
// once the old owner is removed and the hand-off has settled.
type ownerChangeScene struct {
	tc             *testCluster
	oldOwner       int
	newOwner       int
	live           []int
	held           string
	oldLog, newLog uint64
}

// setUpAnOwnerChange builds four nodes at factor 3, has the old owner of
// testBucket coordinate 600 writes to a bucket its set holds and the new owner's
// does not, writes kc at all, removes the old owner and waits for the hand-off
// to settle. The new owner takes the filler on removal, and it can come by two
// routes: its own pull, which writes the store and no log entries, or the
// replay of the filler's owner, whose entries it logs one by one unless the
// pull has landed first. Which route lands first is a race the old test left
// to the machine's load (BUG-0487), so the order is chosen here.
func setUpAnOwnerChange(t *testing.T, order handOffOrder) *ownerChangeScene {
	t.Helper()
	draws := 0
	tc := newTestClusterPlaced(t, 4, 3, func(addrs []string) bool {
		draws++
		_, found := placeOwnerChange(t, order, addrs)
		return found
	})
	drawn, _ := placeOwnerChange(t, order, tc.addrs)
	oldOwnerID, newOwnerID, filler := drawn.oldOwnerID, drawn.newOwnerID, drawn.filler
	oldOwner := nodeIndexByAddr(t, tc.addrs, oldOwnerID)
	tc.ok(oldOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: filler, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	waitForServing(t, tc, testDatabase, filler, 3)
	for i := 0; i < fillerWrites; i++ {
		tc.ok(oldOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: filler, Key: fmt.Sprintf("f%03d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	requireEveryCopy(t, tc.frame(oldOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
		Key: "kc", Data: map[string]interface{}{"v": "old-value"}, WriteConcern: "all"}), "kc = old-value")

	via := nodeIndexByAddr(t, tc.addrs, newOwnerID)
	newCore := clusterOf(tc, via)
	// holdOver ends every hold when the setup returns or fails, so a held
	// hand-off does not keep tc.stop waiting for the bound.
	holdOver := make(chan struct{})
	defer close(holdOver)
	until := func(cond func() bool) func() bool {
		return func() bool {
			select {
			case <-holdOver:
				return true
			default:
				return cond()
			}
		}
	}
	var holdsEntered, holdsEnded atomic.Int32
	hold := func(what string, cond func() bool) {
		holdsEntered.Add(1)
		defer holdsEnded.Add(1)
		holdUntil(what, until(cond))
	}
	pulled := func() bool {
		found, _, _, err := tc.nodes[via].server.core.versionedFind(testDatabase, filler, "f599")
		return err == nil && found
	}
	switch order {
	case theNewOwnerPullsFirst:
		holdReplay := func(peerID string, frame []ReplEntry) {
			if peerID == newOwnerID && frame[0].Bucket == filler {
				hold("the replay of the filler to the new owner", pulled)
			}
		}
		beforeReplayShipHook.Store(&holdReplay)
		defer beforeReplayShipHook.Store(nil)
	case theReplayComesFirst:
		holdPull := func(nodeID, database, bucket string) error {
			if nodeID == newOwnerID && bucket == filler {
				hold("the new owner's pull of the filler", func() bool { return newCore.walLastSequence() >= fillerWrites })
			}
			return nil
		}
		ensureMigratedHook.Store(&holdPull)
		defer ensureMigratedHook.Store(nil)
	}
	removeCommitted(t, tc, via, oldOwner)
	members := map[string]*Server{}
	var live []int
	for index, node := range tc.nodes {
		if index != oldOwner {
			members[tc.addrs[index]] = node.server.core
			live = append(live, index)
		}
	}
	var unsettled string
	report := func(w io.Writer) error {
		return writeSettleReport(w, settleReport{
			draw: draws, filler: filler, ownerRuleApplies: order == theReplayComesFirst, drawn: drawn,
			holdsEntered: holdsEntered.Load(), holdsEnded: holdsEnded.Load(), unsettled: unsettled,
			members: probeMembers(tc, drawn, filler),
		})
	}
	settled, reportErr := awaitSettleOrReport(takeOverWait, func() bool {
		unsettled = handOffUnsettled(members, testDatabase, []string{testBucket, filler})
		// The hand-off reads as settled once the replayed create has opened the
		// filler on the new owner, with the replay's entries still arriving; the
		// log is read only when the replay has delivered them all.
		if logged := newCore.walLastSequence(); unsettled == "" && order == theReplayComesFirst && logged < fillerWrites {
			unsettled = fmt.Sprintf("the new owner's log holds %d of the %d entries the replay carries", logged, fillerWrites)
		}
		return unsettled == ""
	}, os.Stderr, report)
	if reportErr != nil {
		t.Logf("%v", reportErr)
	}
	if !settled {
		t.Fatalf("the hand-off did not settle: %s", unsettled)
	}
	set, err := newCore.Replicas(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("placing %s after the removal: %v", testBucket, err)
	}
	return &ownerChangeScene{tc: tc, oldOwner: oldOwner, newOwner: via, live: live, held: set[1].ID,
		oldLog: clusterOf(tc, oldOwner).walLastSequence(), newLog: newCore.walLastSequence()}
}

// TestTheOrderOfAReplayAndAPullDecidesTheNewOwnersLog is the deterministic form
// of the BUG-0487 member TestConsistentGetAfterAnOwnerChangeReturnsTheNewerValue.
// That test needs the new owner's log shorter than the old owner's. The new
// owner reaches the old owner's length when the filler's owner replays its log
// to it before its own pull lands, and stays near zero when the pull lands
// first; under load the first order came up about one run in fifteen. Each
// order is forced here, so the test fails on every run if the setup stops
// holding the pull-first order or the replay-first order stops being the one
// that breaks the old test's precondition. The replay-first order is told by
// the new owner's log holding every one of the filler's entries, not by its
// length against the old owner's: the new owner starts with one to three
// entries of its own, so its log ends one to three short of the old owner's
// 604 on some runs (603 against 604 was seen) without the order having changed.
func TestTheOrderOfAReplayAndAPullDecidesTheNewOwnersLog(t *testing.T) {
	for _, tc := range []struct {
		name         string
		order        handOffOrder
		wantReplayed bool
	}{
		{"the pull lands first", theNewOwnerPullsFirst, false},
		{"the replay lands first", theReplayComesFirst, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scene := setUpAnOwnerChange(t, tc.order)
			defer scene.tc.stop()
			if replayed := scene.newLog >= fillerWrites; replayed != tc.wantReplayed {
				t.Errorf("the new owner's log is %d; want it to hold the filler's %d replayed entries: %v", scene.newLog, fillerWrites, tc.wantReplayed)
			}
			if !tc.wantReplayed && scene.newLog >= scene.oldLog {
				t.Errorf("the new owner's log is %d and the old owner's %d; want the new owner's shorter", scene.newLog, scene.oldLog)
			}
		})
	}
}

// TestConsistentGetAfterAnOwnerChangeReturnsTheNewerValue is spec §20.17's
// probe 2. Four nodes at factor 3; the owner coordinates 600 writes to a
// bucket it holds and the next owner does not, so its log is the longer one;
// kc is written at
// all. The owner is removed (committed) and the new owner takes the bucket
// over; kc is overwritten through it at majority while one replica's apply is
// held. A consistent get from every remaining node answers the overwrite, and
// once the replica is released every copy holds it. Guard: stamp a write's
// version from its sequence alone.
func TestConsistentGetAfterAnOwnerChangeReturnsTheNewerValue(t *testing.T) {
	scene := setUpAnOwnerChange(t, theNewOwnerPullsFirst)
	defer scene.tc.stop()
	tc, newOwner, live, held := scene.tc, scene.newOwner, scene.live, scene.held
	if scene.newLog >= scene.oldLog {
		t.Fatalf("setup: the new owner's log (%d) is not shorter than the old owner's (%d)", scene.newLog, scene.oldLog)
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	var reachOnce, releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHeld()
	hold := func(nodeID string, entry ReplEntry) {
		if nodeID != held || !bytes.Contains(entry.Payload, []byte("new-value")) {
			return
		}
		reachOnce.Do(func() { close(reached) })
		<-release
	}
	beforeLocalApplyHook.Store(&hold)
	defer beforeLocalApplyHook.Store(nil)

	frame := tc.frame(newOwner, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: testBucket,
		Key: "kc", Data: map[string]interface{}{"v": "new-value"}, WriteConcern: "majority"})
	if frame.Opcode == transport.OpError {
		t.Fatalf("kc = new-value at majority through the new owner: %s", frame.Payload)
	}
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatalf("setup: the held replica %s never reached its apply of kc = new-value", held)
	}
	for _, index := range live {
		resp := answeredConsistentGet(t, consistentGetFrame(t, tc.nodes[index], testDatabase, testBucket, "kc"))
		if !resp.Found || docField(t, resp.Data, "v") != "new-value" {
			value := ""
			if resp.Found {
				value = docField(t, resp.Data, "v")
			}
			t.Errorf("consistent get of kc from node %d answered %q at version %d (repaired=%v), want new-value", index+1, value, resp.Version, resp.Repaired)
		}
	}
	releaseHeld()
	beforeLocalApplyHook.Store(nil)
	var stale []string
	if !holdsWithin(10*time.Second, func() bool {
		stale = nil
		for _, index := range live {
			found, data, _, err := tc.nodes[index].server.core.versionedFind(testDatabase, testBucket, "kc")
			if err != nil || !found || docField(t, data, "v") != "new-value" {
				stale = append(stale, tc.addrs[index])
			}
		}
		return len(stale) == 0
	}) {
		t.Errorf("after the read, these nodes do not hold kc = new-value: %s", strings.Join(stale, ", "))
	}
}

// loweredOwnerLimitRepair stores a 2 MiB document in a two-node hash bucket,
// lowers the owner's document limit below it, and leaves the owner with a
// stale copy. A consistent get then repairs the owner through the owner with a
// put whose value is over the owner's own limit, which is the shape read
// repair takes (BUG-0582). It returns the cluster, the owner's index and a
// coordinator that is not the owner.
func loweredOwnerLimitRepair(t *testing.T) (*testCluster, int, int) {
	t.Helper()
	const body = 2 << 20
	tc := newTestCluster(t, 2, 2)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1", Data: docOfEncodedSize(body)})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolving the owner of %q: %v", testBucket, err)
	}
	ownerIdx := nodeIndexByAddr(t, tc.addrs, owner.ID)
	// The owner already stores the value; only its limit is lowered.
	tc.nodes[ownerIdx].server.config.Storage.MaxDocumentBytes = 1 << 20
	forceStale(t, tc.nodes[ownerIdx].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"body": "stale"})
	return tc, ownerIdx, (ownerIdx + 1) % 2
}

// TestReadRepairOfADocumentAboveALoweredLimitIsNotRefusedByTheOwner is the
// BUG-0582 rework's first defect: read repair rebuilds a value the owner
// already stores as an ordinary put, so the owner must not measure it against
// its own limit. The owner holds a 2 MiB document while its limit says 1 MiB;
// a repair that was measured would be refused and the stale copy would stay
// stale.
func TestReadRepairOfADocumentAboveALoweredLimitIsNotRefusedByTheOwner(t *testing.T) {
	tc, ownerIdx, coordIdx := loweredOwnerLimitRepair(t)

	resp := consistentGet(t, tc.nodes[coordIdx], testDatabase, testBucket, "k1")
	if !resp.Repaired {
		t.Fatalf("a stale owner was not repaired (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	found, data, _, err := tc.nodes[ownerIdx].server.core.versionedFind(testDatabase, testBucket, "k1")
	if err != nil || !found {
		t.Fatalf("the owner after its repair (found=%v err=%v)", found, err)
	}
	if body, ok := bson.Raw(data).Lookup("body").StringValueOK(); !ok || len(body) != (2<<20)-16 {
		t.Fatalf("the owner's repaired document holds %d bytes of body (ok=%v), want %d: the repair was refused",
			len(body), ok, (2<<20)-16)
	}
}

// TestARefusedRepairIsNotReportedRepaired is the other half of BUG-0582's
// first defect: a repair the owner refused comes back as a refusal frame with
// a nil Go error, so a caller that checks only the error reports a repair that
// never happened. A read that reports a repair must have left the repaired
// document on the holder.
func TestARefusedRepairIsNotReportedRepaired(t *testing.T) {
	tc, ownerIdx, coordIdx := loweredOwnerLimitRepair(t)

	resp := consistentGet(t, tc.nodes[coordIdx], testDatabase, testBucket, "k1")
	if !resp.Repaired {
		return // nothing was claimed, so there is nothing to disprove
	}
	found, data, _, err := tc.nodes[ownerIdx].server.core.versionedFind(testDatabase, testBucket, "k1")
	if err != nil || !found {
		t.Fatalf("the read reported a repair but the owner holds no document (found=%v err=%v)", found, err)
	}
	if body, ok := bson.Raw(data).Lookup("body").StringValueOK(); !ok || len(body) != (2<<20)-16 {
		t.Fatalf("the read reported a repair that did not happen: the owner holds %d bytes of body (ok=%v), want %d",
			len(body), ok, (2<<20)-16)
	}
}

// TestARefusedRepairFrameIsNotReportedAsRepaired forces the owner to refuse
// the repair put with a member-feature refusal, which reaches repairKey as a
// refusal frame with a nil Go error. The refusal is persistent (a member that
// does not advertise the gated feature stays that way), so this pins the
// frame check itself: the read must not report a repair the owner refused, and
// the stale copy must stay stale.
func TestARefusedRepairFrameIsNotReportedAsRepaired(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1", Data: map[string]interface{}{"n": int32(1)}})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolving the owner of %q: %v", testBucket, err)
	}
	ownerIdx := nodeIndexByAddr(t, tc.addrs, owner.ID)
	forceStale(t, tc.nodes[ownerIdx].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": int32(0)})
	// Every member now lacks the feature a gated OpUpsert names, so the owner
	// refuses every repair put before it is logged or applied.
	waitForFeatureConvergence(t, tc)
	withGatedOpcode(t, transport.OpUpsert, featureNoBuildAdvertises)

	resp := consistentGet(t, tc.nodes[(ownerIdx+1)%2], testDatabase, testBucket, "k1")
	if resp.Repaired {
		t.Fatalf("the read reported a repair the owner refused (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	found, data, _, err := tc.nodes[ownerIdx].server.core.versionedFind(testDatabase, testBucket, "k1")
	if err != nil || !found || docField(t, data, "n") != "0" {
		t.Fatalf("the refused repair changed the owner's stale copy (found=%v data=%s err=%v)", found, data, err)
	}
}

// TestARefusedDeleteRepairIsNotReportedRepaired forces the owner to refuse the
// repair delete with a member-feature refusal, which reaches repairKeyByDelete
// as a refusal frame with a nil Go error. The read must not report a repair the
// owner refused, and the stray copy the delete was meant to remove must stay.
func TestARefusedDeleteRepairIsNotReportedRepaired(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1",
		Data: map[string]interface{}{"n": int32(1)}, WriteConcern: "all"}), "the write of k1")

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolving the owner of %q: %v", testBucket, err)
	}
	ownerIdx := nodeIndexByAddr(t, tc.addrs, owner.ID)
	strayIdx := (ownerIdx + 1) % 2
	// Delete at all so the owner holds nothing, then write the stray copy back
	// into the other holder at version 0: the owner is absent and only that
	// holder still has the key, the shape answerOwnersAbsence repairs by delete.
	tc.ok(0, transport.OpDelete, deleteAt(testDatabase, testBucket, "k1", "all"))
	forceStale(t, tc.nodes[strayIdx].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": int32(0)})
	// Every member now lacks the feature a gated OpDelete names, so the owner
	// refuses every repair delete before it is logged or applied.
	waitForFeatureConvergence(t, tc)
	withGatedOpcode(t, transport.OpDelete, featureNoBuildAdvertises)

	resp := consistentGet(t, tc.nodes[(ownerIdx+1)%2], testDatabase, testBucket, "k1")
	if resp.Repaired {
		t.Fatalf("the read reported a repair the owner refused (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	found, data, _, err := tc.nodes[strayIdx].server.core.versionedFind(testDatabase, testBucket, "k1")
	if err != nil || !found || docField(t, data, "n") != "0" {
		t.Fatalf("the refused repair delete removed the stray copy (found=%v data=%s err=%v)", found, data, err)
	}
}
