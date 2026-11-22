package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ownerChange is a three-node factor-3 cluster in which the bucket's owner has
// been removed: newOwner is the survivor the ring now names, and survivor is the
// other one, which already holds every write the old owner acknowledged.
type ownerChange struct {
	tc       *testCluster
	newOwner int
	survivor int
}

// removeTheBucketsOwner removes the owner of the test bucket with the same call
// the CLI's remove-node ends in, and brings the other survivor to the new epoch
// the way gossip would.
func removeTheBucketsOwner(t *testing.T, tc *testCluster) ownerChange {
	t.Helper()
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve the owner: %v", err)
	}
	oldOwner := nodeIndexByAddr(t, tc.addrs, owner.ID)
	proposer := (oldOwner + 1) % len(tc.nodes)
	other := (oldOwner + 2) % len(tc.nodes)

	proposerCluster := tc.nodes[proposer].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposerCluster.proposeRemoval(ctx, owner.ID); err != nil {
		t.Fatalf("remove the owner %s: %v", owner.ID, err)
	}
	otherCluster := tc.nodes[other].server.core.cluster
	if err := otherCluster.reconcileMembership(tc.addrs[proposer], proposerCluster.memberNodes(), proposerCluster.memberIDs(), proposerCluster.Epoch()); err != nil {
		t.Fatalf("bring node %d to the new epoch: %v", other+1, err)
	}

	newOwner, err := proposerCluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve the new owner: %v", err)
	}
	newOwnerIndex := nodeIndexByAddr(t, tc.addrs, newOwner.ID)
	survivor := proposer
	if newOwnerIndex == proposer {
		survivor = other
	}
	// The new owner serves the bucket once it has taken it over (step 10b.5),
	// and the survivor reads its own copy once it follows the new term.
	waitTakenOver(t, tc.nodes[newOwnerIndex].server.core, testDatabase, testBucket)
	waitFollowing(t, tc.nodes[survivor].server.core, tc.nodes[newOwnerIndex].server.core, testDatabase, testBucket)
	return ownerChange{tc: tc, newOwner: newOwnerIndex, survivor: survivor}
}

// catchUp does what Cluster.verifyPeer does after a gossip: the new owner
// replays its log to the other survivor.
func (oc ownerChange) catchUp(t *testing.T) {
	t.Helper()
	cluster := oc.tc.nodes[oc.newOwner].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := replayToPeer(ctx, cluster.peerFor(mustNode(t, cluster, oc.tc.addrs[oc.survivor]))); err != nil {
		t.Fatalf("catch-up from node %d to node %d: %v", oc.newOwner+1, oc.survivor+1, err)
	}
}

func pushQueueElements(t *testing.T, tc *testCluster, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		frame := tc.ok(0, transport.OpFIFOPush, PushRequest{
			Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"n": i}, WriteConcern: "all",
		})
		if frame.Opcode == transport.OpError {
			t.Fatalf("push %d: %s", i, frame.Payload)
		}
	}
}

func createTestQueue(t *testing.T, tc *testCluster, bucketType BucketType) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: bucketType})
}

func waitForEveryNodeToCount(t *testing.T, tc *testCluster, want int64) {
	t.Helper()
	pollUntil(t, "every node counts the elements the owner acknowledged", func() bool {
		for i := range tc.nodes {
			if tc.localCount(i) != want {
				return false
			}
		}
		return true
	})
}

func expectCounts(t *testing.T, oc ownerChange, want int64) {
	t.Helper()
	for _, index := range []int{oc.newOwner, oc.survivor} {
		if got := oc.tc.localCount(index); got != want {
			t.Errorf("node %d counts %d elements, want %d", index+1, got, want)
		}
	}
}

// counterOf reads the numeric field n of key from one node's own copy.
func counterOf(t *testing.T, tc *testCluster, index int, key string) int64 {
	t.Helper()
	frame := tc.ok(index, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: key})
	var found FindResponse
	if err := bson.Unmarshal(frame.Payload, &found); err != nil {
		t.Fatalf("decode the find response from node %d: %v", index+1, err)
	}
	value, ok := bson.Raw(found.Data).Lookup("n").AsInt64OK()
	if !found.Found || !ok {
		t.Fatalf("node %d has no numeric n in %q", index+1, key)
	}
	return value
}

// TestNewOwnerCatchUpDoesNotReshipEntriesItAppliedAsAReplica is the
// test: a node that becomes an owner replays its log, and its log holds what it
// applied as a replica of the previous owner. Re-sending those to the other
// survivor applies them a second time.
func TestNewOwnerCatchUpDoesNotReshipEntriesItAppliedAsAReplica(t *testing.T) {
	t.Run("five pushes", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		createTestQueue(t, tc, BucketTypeFIFO)
		pushQueueElements(t, tc, 5)
		waitForEveryNodeToCount(t, tc, 5)

		oc := removeTheBucketsOwner(t, tc)
		oc.catchUp(t)
		expectCounts(t, oc, 5)
		// Again: a second replay must not add what the first did not.
		oc.catchUp(t)
		expectCounts(t, oc, 5)
	})

	t.Run("a pop", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		createTestQueue(t, tc, BucketTypeFIFO)
		pushQueueElements(t, tc, 5)
		tc.ok(0, transport.OpFIFOPop, PopRequest{Database: testDatabase, Bucket: testBucket})
		waitForEveryNodeToCount(t, tc, 4)

		oc := removeTheBucketsOwner(t, tc)
		oc.catchUp(t)
		expectCounts(t, oc, 4)
	})

	// A keyed bucket hides the second apply: the replayed put resets the
	// document before its increments run again, so the starting tree passes this
	// case too. It stays because the fix must not stop a hash bucket replaying.
	t.Run("an $inc on a hash bucket", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		createTestQueue(t, tc, BucketTypeHash)
		tc.ok(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: "c",
			Data: map[string]interface{}{"n": int64(0)}, WriteConcern: "all",
		})
		for i := 0; i < 3; i++ {
			tc.ok(0, transport.OpUpdate, UpdateRequest{
				Database: testDatabase, Bucket: testBucket, Key: "c", Inc: map[string]int64{"n": 1},
			})
		}
		pollUntil(t, "every node holds the counter at 3", func() bool {
			for i := range tc.nodes {
				if counterOf(t, tc, i, "c") != 3 {
					return false
				}
			}
			return true
		})

		oc := removeTheBucketsOwner(t, tc)
		oc.catchUp(t)
		for _, index := range []int{oc.newOwner, oc.survivor} {
			if got := counterOf(t, tc, index, "c"); got != 3 {
				t.Errorf("node %d holds the counter at %d, want 3", index+1, got)
			}
		}
	})

	t.Run("a bucket lost to a join and regained by a removal is replayed again", func(t *testing.T) {
		regainedBucketIsReplayed(t)
	})
}

// restartNodeOnLog stops a node, puts segment in as the whole of its write-ahead
// log, and starts it again on that directory.
func (tc *testCluster) restartNodeOnLog(index int, segment []byte) *testNode {
	tc.t.Helper()
	oldConfig := tc.nodes[index].cfg
	if tc.nodes[index].client != nil {
		_ = tc.nodes[index].client.Close()
		tc.nodes[index].client = nil
	}
	if err := tc.nodes[index].server.Stop(); err != nil {
		tc.t.Fatalf("stop node %d: %v", index+1, err)
	}
	tc.nodes[index].server = nil

	logPath := filepath.Join(oldConfig.Storage.DataDir, "wal", "wal_00001.log")
	if err := os.WriteFile(logPath, segment, 0o600); err != nil {
		tc.t.Fatalf("placing the log of node %d: %v", index+1, err)
	}
	restarted := tc.startNode(index, filepath.Dir(oldConfig.Storage.DataDir), tc.token)
	tc.nodes[index] = restarted
	return restarted
}

// regainedBucketIsReplayed is review A-1.4's third case. A node owns a bucket,
// a join gives it to a newcomer, a removal gives it back, and the entries the
// node logs after it regains it are replayed to a replica that missed them.
// Nothing is asserted about the entries it logged before the loss.
//
// The newcomer is a member that is proposed and never started (a join needs a
// majority of the current members, not the newcomer): nothing is copied to it
// and no sweep moves data, so the only thing that changes is who owns the bucket.
func regainedBucketIsReplayed(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOffOnNodeZero)
	defer tc.stop()
	newcomer := ring.Node{ID: fabricatedMemberAddr(t), Addr: ""}
	newcomer.Addr = newcomer.ID

	// A bucket the newcomer takes from its owner, who stays in the replica set.
	before := ring.New(tc.rf)
	after := ring.New(tc.rf)
	for _, addr := range tc.addrs {
		member := ring.Node{ID: addr, Addr: addr}
		for _, r := range []*ring.Ring{before, after} {
			if err := r.AddNode(member); err != nil {
				t.Fatalf("add %s to the placement ring: %v", addr, err)
			}
		}
	}
	if err := after.AddNode(newcomer); err != nil {
		t.Fatalf("add the newcomer to the placement ring: %v", err)
	}
	bucket := ""
	ownerIndex := -1
	for i := 0; i < 200 && bucket == ""; i++ {
		name := fmt.Sprintf("regain%03d", i)
		key := string(storage.PlacementKey(testDatabase, name, nil))
		ownerBefore, err := before.Owner(key)
		if err != nil {
			t.Fatalf("owner of %s before the join: %v", name, err)
		}
		replicasAfter, err := after.Replicas(key, tc.rf)
		if err != nil {
			t.Fatalf("replicas of %s after the join: %v", name, err)
		}
		if replicasAfter[0].ID != newcomer.ID {
			continue
		}
		for _, replica := range replicasAfter[1:] {
			if replica.ID == ownerBefore.ID {
				bucket, ownerIndex = name, nodeIndexByAddr(t, tc.addrs, ownerBefore.ID)
			}
		}
	}
	if bucket == "" {
		t.Fatalf("no bucket regain000..regain199 moves to the newcomer %s and keeps its old owner as a replica", newcomer.ID)
	}

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	// A keyed bucket: the owner's sender and this test's catch-up can both
	// deliver the same entry to the restarted replica, and only a
	// keyed put survives that unharmed.
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	put := func(key, wc string) {
		t.Helper()
		request := putRequest(bucket, key)
		request.WriteConcern = wc
		if frame := tc.frame(ownerIndex, transport.OpPut, request); frame.Opcode == transport.OpError {
			t.Fatalf("put %s at %s: %s", key, wc, frame.Payload)
		}
	}
	put("before-1", "all")
	put("before-2", "all")

	// The join takes the bucket; the removal gives it back.
	proposer := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	syncMembers := func() {
		t.Helper()
		for i, n := range tc.nodes {
			if i == 0 {
				continue
			}
			if err := n.server.core.cluster.reconcileMembership(tc.addrs[0], proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
				t.Fatalf("member %d resync: %v", i+1, err)
			}
		}
	}
	if err := proposer.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("join the newcomer: %v", err)
	}
	syncMembers()
	if lost, err := proposer.Owner(placementOf(testDatabase, bucket)); err != nil || lost.ID != newcomer.ID {
		t.Fatalf("precondition: the newcomer must own %s after the join (owner %v, %v)", bucket, lost, err)
	}
	if err := proposer.proposeRemoval(ctx, newcomer.ID); err != nil {
		t.Fatalf("remove the newcomer: %v", err)
	}
	syncMembers()
	if regained, err := proposer.Owner(placementOf(testDatabase, bucket)); err != nil || regained.ID != tc.addrs[ownerIndex] {
		t.Fatalf("precondition: node %d must own %s again after the removal (owner %v, %v)", ownerIndex+1, bucket, regained, err)
	}
	// The regained owner takes the bucket over before it serves it (step
	// 10b.5): with a member down its fallback would wait for it.
	waitTakenOver(t, tc.nodes[ownerIndex].server.core, testDatabase, bucket)

	// A replica goes down; the regained owner takes three writes it misses.
	downIndex := (ownerIndex + 1) % len(tc.nodes)
	if downIndex == 0 {
		// Node 0's background sweep is off (autoRebalanceOffOnNodeZero), and
		// the follow the restart below waits for is that sweep's work: with
		// auto_rebalance off the node moves nothing and the operator drives
		// migration with an explicit migrate (the configuration page of the user docs).
		// Restart the other replica, whose sweep is on, so the follow is the
		// sweep's and not a race with a disabled worker.
		downIndex = (ownerIndex + 2) % len(tc.nodes)
	}
	_ = tc.nodes[downIndex].client.Close()
	tc.nodes[downIndex].client = nil
	if err := tc.nodes[downIndex].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", downIndex+1, err)
	}
	tc.nodes[downIndex].server = nil
	regainedKeys := []string{"after-1", "after-2", "after-3"}
	for _, key := range regainedKeys {
		put(key, "majority")
	}
	restarted := tc.startNode(downIndex, filepath.Dir(tc.nodes[downIndex].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[downIndex].cfg
	tc.nodes[downIndex] = restarted

	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	// The replica takes the regained owner's term before the term's entries
	// apply to it (step 10b.6).
	waitFollowing(t, restarted.server.core, tc.nodes[ownerIndex].server.core, testDatabase, bucket)
	catchUpCtx, cancelCatchUp := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCatchUp()
	if err := replayToPeer(catchUpCtx, ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[downIndex]))); err != nil {
		t.Fatalf("catch-up from the regained owner: %v", err)
	}
	for _, key := range regainedKeys {
		if found, message := readKeyFromNode(tc, downIndex, bucket, key); !found {
			t.Errorf("the replica that missed %q does not hold it after catch-up (%s)", key, message)
		}
	}

	// And the filter itself, without the network: every entry the owner logged
	// after it regained the bucket is one it replays to the replica.
	logged, err := tc.nodes[ownerIndex].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("read the owner's log: %v", err)
	}
	replayable, _, err := ownerCluster.replayableEntries(logged, ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[downIndex])))
	if err != nil {
		t.Fatalf("filter the owner's log: %v", err)
	}
	// A put's key travels BSON-encoded, so it is found inside the entry's key.
	replaysKey := func(key string) bool {
		for _, entry := range replayable {
			if bytes.Contains(entry.Key, []byte(key)) {
				return true
			}
		}
		return false
	}
	for _, key := range regainedKeys {
		if !replaysKey(key) {
			t.Errorf("the owner's replay skips %q, which it logged after regaining the bucket", key)
		}
	}
}

// recordingPeerConn wraps a peer's real connection and remembers the sequence
// of every bucket entry sent on it.
type recordingPeerConn struct {
	peerConn
	mu   sync.Mutex
	sent []uint64
}

func (r *recordingPeerConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		var request ReplPushRequest
		if err := bson.Unmarshal(payload, &request); err == nil {
			r.mu.Lock()
			for _, entry := range request.Entries {
				if entry.Bucket != "" {
					r.sent = append(r.sent, entry.Seq)
				}
			}
			r.mu.Unlock()
		}
	}
	return r.peerConn.SendRequest(ctx, lane, op, payload)
}

func (r *recordingPeerConn) sentSequences() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.sent...)
}

func containsSequence(sequences []uint64, want uint64) bool {
	for _, seq := range sequences {
		if seq == want {
			return true
		}
	}
	return false
}

// TestCatchUpNeverShipsAnEntryTheLogHasNotSynced covers the second trigger:
// a replay that reads an entry the log has written and not yet fsynced can ship
// it, and a power cut then gives its sequence to a different write.
func TestCatchUpNeverShipsAnEntryTheLogHasNotSynced(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, putRequestAt(testBucket, "first", "all"))

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve the owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	replicaIndex := (ownerIndex + 1) % len(tc.nodes)
	ownerCore := tc.nodes[ownerIndex].server.core
	replicaPeer := ownerCore.cluster.peerFor(mustNode(t, ownerCore.cluster, tc.addrs[replicaIndex]))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	realConn, err := replicaPeer.dial(ctx)
	if err != nil {
		t.Fatalf("dial the replica: %v", err)
	}
	recorder := &recordingPeerConn{peerConn: realConn}
	replicaPeer.mu.Lock()
	replicaPeer.client = recorder
	replicaPeer.mu.Unlock()

	heldSeq := ownerCore.wal.LastSequence() + 1
	written := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHeldEntry := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered after the deferred tc.stop so it runs first: a node cannot stop
	// while an append is still waiting inside its fsync.
	defer releaseHeldEntry()
	ownerCore.wal.SetBeforeSyncHook(func(seq uint64) {
		if seq == heldSeq {
			close(written)
			<-release
		}
	})
	appendDone := make(chan error, 1)
	go func() {
		_, err := ownerCore.cluster.appendWAL(ReplEntry{
			Op: int32(transport.OpPut), Bucket: testBucket, Payload: mustMarshalPut(t, testBucket, "held"),
		}, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: ownerCore.cluster.Epoch()})
		appendDone <- err
	}()
	<-written

	if err := replayToPeer(ctx, replicaPeer); err != nil {
		t.Fatalf("catch-up while an entry is written and not synced: %v", err)
	}
	if containsSequence(recorder.sentSequences(), heldSeq) {
		t.Fatalf("catch-up shipped sequence %d, which the log had written and not yet synced", heldSeq)
	}

	releaseHeldEntry()
	if err := <-appendDone; err != nil {
		t.Fatalf("the held append: %v", err)
	}
	if err := replayToPeer(ctx, replicaPeer); err != nil {
		t.Fatalf("catch-up after the entry was synced: %v", err)
	}
	if !containsSequence(recorder.sentSequences(), heldSeq) {
		t.Fatalf("catch-up never shipped sequence %d once it was synced: the test held an entry that could not be shipped, so it proved nothing", heldSeq)
	}
}

func putRequestAt(bucket, key, writeConcern string) InsertRequest {
	request := putRequest(bucket, key)
	request.WriteConcern = writeConcern
	return request
}
