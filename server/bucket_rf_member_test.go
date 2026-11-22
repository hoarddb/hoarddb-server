package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The member's side of the factor alter for a held copy
// (bucket-replication-rev8-spec §5.3 rows 1 to 3 and 6, rule F4, the term
// table's outside row; G3 build doc step 4). A copy the alter keeps rewrites
// its factor and moves its lineage; every other held copy rewrites its factor
// in all three of its stores, is marked outside in the alter's batch, and
// keeps its lineage where it was, so it is never served, never applied to,
// and never chosen by a take-over over a copy that holds what it lacks.

// pushRecorder counts the entries a peer's stream pushes, by opcode and
// bucket.
type pushRecorder struct {
	mu      sync.Mutex
	entries map[string]int
}

// recordPushes counts every entry the owner's stream pushes to p from now on.
func recordPushes(p *peer) *pushRecorder {
	recorder := &pushRecorder{entries: make(map[string]int)}
	wrapPeerConn(p, func(inner peerConn) peerConn { return &recordingPushConn{peerConn: inner, recorder: recorder} })
	return recorder
}

// entriesOf is how many entries of op for bucket were pushed.
func (r *pushRecorder) entriesOf(op transport.Opcode, bucket string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.entries[op.String()+"\x00"+bucket]
}

// recordingPushConn passes every request on, counting the entries of each
// replication push.
type recordingPushConn struct {
	peerConn
	recorder *pushRecorder
}

func (r *recordingPushConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		var push ReplPushRequest
		if err := bson.Unmarshal(payload, &push); err == nil {
			r.recorder.mu.Lock()
			for _, entry := range push.Entries {
				r.recorder.entries[transport.Opcode(entry.Op).String()+"\x00"+entry.Bucket]++
			}
			r.recorder.mu.Unlock()
		}
	}
	return r.peerConn.SendRequest(ctx, lane, op, payload)
}

// heldFactor is a bucket's factor in each of the three places a node holds it:
// its catalog entry, its record and its registry, 0 meaning inherit.
type heldFactor struct {
	catalog, record, registry int32
}

// heldFactorOf reads node s's three stores of the test database's bucket.
func heldFactorOf(t *testing.T, s *Server, bucket string) heldFactor {
	t.Helper()
	entry, catalogued, err := s.readCatalogEntry(testDatabase, bucket)
	if err != nil || !catalogued {
		t.Fatalf("reading %s's catalog entry of %q: catalogued %v, %v", s.cluster.self.ID, bucket, catalogued, err)
	}
	facts, _ := s.peekSettled(testDatabase, bucket)
	registry, _ := registeredFactor(s.cluster, testDatabase, bucket)
	return heldFactor{catalog: entry.ReplicationFactor, record: facts.replicationFactor, registry: registry}
}

// requireHeldAt fails unless node s holds bucket at factor want in its
// catalog entry, its record and its registry (§3.1, RF-2).
func requireHeldAt(t *testing.T, s *Server, bucket string, want int32, when string) {
	t.Helper()
	if got := heldFactorOf(t, s, bucket); got != (heldFactor{catalog: want, record: want, registry: want}) {
		t.Errorf("%s, %s holds %q at factor %d in its catalog entry, %d in its record and %d in its registry; want %d in all three",
			when, s.cluster.self.ID, bucket, got.catalog, got.record, got.registry, want)
	}
}

// outsideMarkOf is the outside mark node s's copy of bucket carries now.
func outsideMarkOf(s *Server, bucket string) uint64 {
	return s.cluster.placementMarksOf(placementOf(testDatabase, bucket)).Outside
}

// takeOverOff stops every node's take-over worker for the rest of the test, so
// a copy an alter marks outside is not copied afresh before it is read.
func takeOverOff(t *testing.T) {
	t.Helper()
	takeOverOffHook.Store(true)
	t.Cleanup(func() { takeOverOffHook.Store(false) })
}

// demotingCluster is three nodes at default 3; a bucket owned by node 1 held
// by every node; and another bucket every node holds, whose writes at concern
// all show every member's stream is in sync. The survivor is the second node
// of the bucket's walk and the demoted node its third: a decrease to 2 keeps
// the one and demotes the other.
func demotingCluster(t *testing.T, configure func(index int, cfg *config.Config)) (tc *testCluster, bucket, other string, survivor, demoted int) {
	t.Helper()
	tc = newTestClusterWith(t, 3, 3, configure)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket = bucketOwnedBy(t, tc, 0, "sessions")
	other = "other"
	for _, name := range []string{bucket, other} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, name, 3)
	}
	walk := mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, 3)
	return tc, bucket, other, nodeIndexByAddr(t, tc.addrs, walk[1]), nodeIndexByAddr(t, tc.addrs, walk[2])
}

// alterWithin sends an alter to node index and fails unless it is accepted
// within ten seconds (T-34's deadline): an alter a member could not apply
// would hold its concern until the ship gives up.
func alterWithin(t *testing.T, tc *testCluster, index int, req AlterBucketRequest) AlterBucketResponse {
	t.Helper()
	started := time.Now()
	answer := alterAccepted(t, alterFrame(t, tc, index, req))
	if took := time.Since(started); took > 10*time.Second {
		t.Fatalf("the alter of %q to %d took %s, over its ten-second deadline", req.Name, req.ReplicationFactor, took)
	}
	return answer
}

// ownerSeqOfAlter is the sequence node index's own log gave its alter of
// bucket to factor, the sequence a member's copy moves its lineage to when it
// applies that alter as row 1.
func ownerSeqOfAlter(t *testing.T, tc *testCluster, index int, bucket string, factor int32) uint64 {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node %d's WAL: %v", index+1, err)
	}
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpAlterBucket && entry.Bucket == bucket && alteredFactorOf(entry.Data) == factor {
			return entry.Seq
		}
	}
	t.Fatalf("node %d logged no alter of %q to %d", index+1, bucket, factor)
	return 0
}

// readAt sends a plain get of key in bucket to node index and returns whether
// the document was found and the answer, for a failure message.
func readAt(t *testing.T, tc *testCluster, index int, bucket, key string) (bool, string) {
	t.Helper()
	frame := tc.frame(index, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
	if frame.Opcode == transport.OpError {
		return false, string(frame.Payload)
	}
	var answer FindResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding a get of %q at node %d: %v", key, index+1, err)
	}
	return answer.Found, fmt.Sprintf("found %v %q", answer.Found, answer.Message)
}

// requireReadsFound fails unless a get of every key at node index answers the
// document: a copy that lacks some of them must forward the read to the owner,
// never answer it from what it holds.
func requireReadsFound(t *testing.T, tc *testCluster, index int, bucket string, keys []string, when string) {
	t.Helper()
	for _, key := range keys {
		if found, answer := readAt(t, tc, index, bucket, key); !found {
			t.Errorf("%s, a get of %q, an acknowledged write, at node %d answered %s: the node answered from what it holds rather than forward the read",
				when, key, index+1, answer)
		}
	}
}

// waitFreshLocally waits until node index's own copy of bucket carries no
// outside mark and holds every key, as a copy pulled afresh does.
func waitFreshLocally(t *testing.T, tc *testCluster, index int, bucket string, keys []string) {
	t.Helper()
	core := tc.nodes[index].server.core
	fresh := func() bool {
		if !core.bucketStoreOpen(testDatabase, bucket) || outsideMarkOf(core, bucket) != 0 {
			return false
		}
		for _, key := range keys {
			if !holdsKeyLocally(tc.nodes[index], bucket, key) {
				return false
			}
		}
		return true
	}
	if !holdsWithin(30*time.Second, fresh) {
		t.Fatalf("node %d's copy of %q was never copied afresh: open %v, outside mark %d", index+1, bucket,
			core.bucketStoreOpen(testDatabase, bucket), outsideMarkOf(core, bucket))
	}
}

// TestAnAltersApplyOnAMemberNeverStallsItsStream is T-174: every row of §5.3,
// and the term table's outside row, applies the alter and acknowledges it, so
// the owner's stream to the member keeps carrying every other bucket. The
// rows of a member that holds nothing are bucket_rf_needs_copy_test.go's
// emptyRows.
func TestAnAltersApplyOnAMemberNeverStallsItsStream(t *testing.T) {
	t.Run("rows 1, 2 and 3", func(t *testing.T) {
		takeOverOff(t)
		tc, bucket, other, survivor, demoted := demotingCluster(t, nil)
		defer tc.stop()
		survivorCore, demotedCore := tc.nodes[survivor].server.core, tc.nodes[demoted].server.core
		// Since G5 the rearm releases a demoted copy its survivors cover, and
		// this test reads the copy after the decrease and re-places it.
		releaseSweeps := holdSweepsOn(t, tc.addrs[demoted])
		defer releaseSweeps()

		alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
		waitForBarrier(t, tc, 0, "after-the-decrease", survivor, demoted)
		requireWriteReachesEveryCopy(t, tc, 0, other, "after-the-decrease", 3)
		requireHeldAt(t, survivorCore, bucket, 2, "after the decrease (row 1)")
		if mark := outsideMarkOf(survivorCore, bucket); mark != 0 {
			t.Errorf("the decrease marked node %d's copy, which it keeps, outside at %d", survivor+1, mark)
		}
		requireHeldAt(t, demotedCore, bucket, 2, "after the decrease (row 2)")
		if outsideMarkOf(demotedCore, bucket) == 0 {
			t.Errorf("the decrease left node %d's copy, which it demotes, unmarked", demoted+1)
		}

		// The demoted copy is not in the set at 2: the increase re-places it,
		// row 3, through the outside row of the term table.
		alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
		waitForBarrier(t, tc, 0, "after-the-increase", survivor, demoted)
		requireWriteReachesEveryCopy(t, tc, 0, other, "after-the-increase", 3)
		requireHeldAt(t, survivorCore, bucket, 3, "after the increase (row 1)")
		requireHeldAt(t, demotedCore, bucket, 3, "after the increase (row 3)")
		if outsideMarkOf(demotedCore, bucket) == 0 {
			t.Errorf("after the increase node %d's re-placed stale copy carries no outside mark", demoted+1)
		}
	})

	t.Run("the outside row: a copy marked outside that both sets place", func(t *testing.T) {
		takeOverOff(t)
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		waitForAlterFeature(t, tc)
		bucket := bucketOwnedBy(t, tc, 0, "sessions")
		createBucketAt(tc, 0, testDatabase, bucket, 2)
		waitForServing(t, tc, testDatabase, bucket, 2)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "other", Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, "other", 3)
		marked := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, 3)[1])
		markedCore := tc.nodes[marked].server.core
		if err := markedCore.cluster.markOutside(placementOf(testDatabase, bucket)); err != nil {
			t.Fatalf("marking node %d's copy outside: %v", marked+1, err)
		}

		alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
		waitForBarrier(t, tc, 0, "after-the-increase", marked)
		requireWriteReachesEveryCopy(t, tc, 0, "other", "after-the-increase", 3)
		requireHeldAt(t, markedCore, bucket, 3, "after the increase (the outside row)")
		if outsideMarkOf(markedCore, bucket) == 0 {
			t.Errorf("the increase cleared node %d's outside mark", marked+1)
		}
	})

	t.Run("rows 4 and 5: a member that holds nothing", emptyRows)

	// Row 6: an alter whose epoch's ring this member no longer remembers is
	// applied as row 3, a copy afresh. The ring of epoch 0 is in no history.
	t.Run("row 6: a ring the alter needs is missing", func(t *testing.T) {
		tc, bucket, other, survivor, _ := demotingCluster(t, nil)
		defer tc.stop()
		member := tc.nodes[survivor].server.core
		key := placementOf(testDatabase, bucket)
		facts, _ := member.peekSettled(testDatabase, bucket)
		req := AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2, PreviousFactor: 3,
			Epoch: InitialEpoch - 1, Generation: facts.generation}
		fence := member.acquireFence(key, fenceExclusive)
		answer, err := member.alterAsMember(context.Background(), req)
		fence.release()
		if err != nil || answer == nil || !answer.Success {
			t.Fatalf("node %d's apply of an alter whose ring it lacks: %+v, %v", survivor+1, answer, err)
		}
		requireHeldAt(t, member, bucket, 2, "after an alter whose ring is missing (row 6)")
		if outsideMarkOf(member, bucket) == 0 {
			t.Errorf("an alter whose ring node %d lacks left its copy unmarked", survivor+1)
		}
		requireWriteReachesEveryCopy(t, tc, 0, other, "after-row-6", 3)
	})
}

// TestADemotedCopyKeepsItsLineageTrue is T-151. The node a decrease from 3 to
// 2 demotes is down while a write and the decrease are logged; when it comes
// back the owner's replay addresses the write at 2, without it, and the
// decrease to the union, with it. Its copy is marked outside and its lineage
// stays below the write it lacks, so a take-over that compares it with the
// survivor's never prefers it. Then the owner is stopped and removed, and the
// bucket's new owner holds the write the demoted copy lacks.
func TestADemotedCopyKeepsItsLineageTrue(t *testing.T) {
	tc, bucket, _, survivor, demoted := demotingCluster(t, nil)
	defer tc.stop()
	// Since G5 the rearm releases a demoted copy its survivors cover, and this
	// test reads the copy after the decrease.
	releaseSweeps := holdSweepsOn(t, tc.addrs[demoted])
	defer releaseSweeps()
	putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
	startDemoted := stopNode(t, tc, demoted)
	putDocumentAt(tc, 0, testDatabase, bucket, "missed", "majority")
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
	alterSeq := ownerSeqOfAlter(t, tc, 0, bucket, 2)
	startDemoted()
	waitForWALCount(t, tc, demoted, transport.OpAlterBucket, bucket, 1)
	demotedCore, survivorCore := tc.nodes[demoted].server.core, tc.nodes[survivor].server.core
	pollUntil(t, "the demoted node applies the decrease", func() bool { return heldFactorOf(t, demotedCore, bucket).catalog == 2 })
	if holdsKeyLocally(tc.nodes[demoted], bucket, "missed") {
		t.Fatalf("setup: node %d holds 'missed'; the replay was meant to address it at 2, without the demoted node", demoted+1)
	}

	ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}
	demotedCopy, err := demotedCore.cluster.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading node %d's copy: %v", demoted+1, err)
	}
	if demotedCopy.Outside == 0 {
		t.Errorf("node %d's demoted copy carries no outside mark: %+v", demoted+1, demotedCopy)
	}
	if demotedCopy.Through >= alterSeq {
		t.Errorf("node %d's demoted copy, which lacks 'missed', claims the decrease's position: through %d, the decrease at %d",
			demoted+1, demotedCopy.Through, alterSeq)
	}
	survivorCopy, err := survivorCore.cluster.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading node %d's copy: %v", survivor+1, err)
	}
	if !lineageAbove(survivorCopy, demotedCopy) {
		t.Errorf("a take-over that compares the two copies could choose the demoted one, which lacks 'missed': "+
			"the survivor's (term, through) is (%d, %d), the demoted copy's (%d, %d)",
			survivorCopy.Term, survivorCopy.Through, demotedCopy.Term, demotedCopy.Through)
	}
	if t.Failed() {
		return
	}

	ownerID := clusterOf(tc, 0).self.ID
	stopNode(t, tc, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := survivorCore.cluster.proposeRemoval(ctx, ownerID); err != nil {
		t.Fatalf("removing the owner: %v", err)
	}
	newOwner, err := survivorCore.cluster.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("resolving the new owner: %v", err)
	}
	newOwnerIndex := nodeIndexByAddr(t, tc.addrs, newOwner.ID)
	waitTakenOver(t, tc.nodes[newOwnerIndex].server.core, testDatabase, bucket)
	if !holdsKeyLocally(tc.nodes[newOwnerIndex], bucket, "missed") {
		t.Errorf("the bucket's new owner, node %d, took it over from a copy without 'missed'", newOwnerIndex+1)
	}
}

// TestARepromotedNodeIsReseededNotResurrected is T-34, its five cases. In
// cases 1, 2 and 4 a node a decrease, or a join, takes out of a bucket's set
// keeps its copy; an increase then places it again. (Case 4's body, a join's,
// is bucket_rf_pass_test.go's repromotedAfterAJoin; cases 3 and 5, the pull
// window, are bucket_rf_needs_copy_test.go's.)
// Its copy, which missed the writes of the factor between, serves no read
// until it has been copied afresh from the owner, and then holds every
// acknowledged write.
func TestARepromotedNodeIsReseededNotResurrected(t *testing.T) {
	// Case 1: auto-rebalance off across the decrease and the increase, so
	// the copy is refreshed only by leave to copy it (what an operator's
	// migrate gives a copy it may not pull on its own).
	t.Run("case 1: auto-rebalance off across a decrease and an increase", func(t *testing.T) {
		tc, bucket, _, _, demoted := demotingCluster(t, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
		defer tc.stop()
		keys := []string{"before"}
		putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
		alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("at-2-%d", i)
			putDocument(tc, 0, testDatabase, bucket, key)
			keys = append(keys, key)
		}
		alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
		waitForBarrier(t, tc, 0, "after-the-increase", demoted)
		requireReadsFound(t, tc, demoted, bucket, keys, "after the increase re-placed the demoted copy")

		demotedCore := tc.nodes[demoted].server.core
		demotedCore.cluster.giveCopyLeave(testDatabase, bucket)
		demotedCore.cluster.wakeTakeOver(testDatabase, bucket)
		waitFreshLocally(t, tc, demoted, bucket, keys)
		putDocument(tc, 0, testDatabase, bucket, "after")
		waitForBarrier(t, tc, 0, "after-the-copy", demoted)
		requireHeldLocally(t, demotedCore, bucket, append(keys, "after"))
	})

	// Case 2: the demoted node is down across the decrease, the writes at 2
	// and the increase, and learns both alters from the owner's replay when
	// it returns, in order.
	t.Run("case 2: a decrease the node never received, then an increase", func(t *testing.T) {
		tc, bucket, _, _, demoted := demotingCluster(t, nil)
		defer tc.stop()
		keys := []string{"before"}
		putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
		startDemoted := stopNode(t, tc, demoted)
		alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("at-2-%d", i)
			putDocument(tc, 0, testDatabase, bucket, key)
			keys = append(keys, key)
		}
		alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
		startDemoted()
		waitForWALCount(t, tc, demoted, transport.OpAlterBucket, bucket, 2)
		requireReadsFound(t, tc, demoted, bucket, keys, "once the returning node applied both alters")

		waitFreshLocally(t, tc, demoted, bucket, keys)
		putDocument(tc, 0, testDatabase, bucket, "after")
		waitForBarrier(t, tc, 0, "after-the-copy", demoted)
		requireHeldLocally(t, tc.nodes[demoted].server.core, bucket, append(keys, "after"))
	})

	t.Run("case 3: a decrease while the increase's pull is held after its snapshot", pullHeldAtMaterialize)
	t.Run("case 4: a stale copy from a membership change, auto-rebalance on", repromotedAfterAJoin)
	t.Run("case 5: a decrease while the increase's pull is held before its snapshot", pullHeldBeforeTheSnapshot)
}

// TestMigrateCopiesAfreshAHeldCopyMarkedOutsideInItsSet is BUG-0590. With
// auto_rebalance off, a copy marked outside that is in its set again is left
// for an operator's migrate (Cluster.copyAfresh, A-10.6): here an increase
// re-places a copy a decrease demoted, so it lacks the writes made at 2. The
// operator's migrate on that node must return only once the copy is copied
// afresh: its mark cleared and every acknowledged write held. Before the fix
// migrate took the held branch, found the copy in its owner's term, and
// answered success with the copy still stale and marked.
func TestMigrateCopiesAfreshAHeldCopyMarkedOutsideInItsSet(t *testing.T) {
	tc, bucket, _, _, demoted := demotingCluster(t, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	keys := []string{"before"}
	putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("at-2-%d", i)
		putDocument(tc, 0, testDatabase, bucket, key)
		keys = append(keys, key)
	}
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
	waitForBarrier(t, tc, 0, "after-the-increase", demoted)
	demotedCore := tc.nodes[demoted].server.core
	if outsideMarkOf(demotedCore, bucket) == 0 {
		t.Fatalf("the increase left node %d's re-placed stale copy unmarked; the test needs it marked", demoted+1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := demotedCore.cluster.ensureMigrated(ctx, testDatabase, bucket); err != nil {
		t.Fatalf("migrate of %q on node %d, whose copy is marked outside in its set: %v", bucket, demoted+1, err)
	}
	if mark := outsideMarkOf(demotedCore, bucket); mark != 0 || !demotedCore.bucketStoreOpen(testDatabase, bucket) {
		t.Fatalf("migrate on node %d answered success while its copy of %q is open %v with outside mark %d: it was never copied afresh",
			demoted+1, bucket, demotedCore.bucketStoreOpen(testDatabase, bucket), mark)
	}
	requireHeldLocally(t, demotedCore, bucket, keys)
}

// TestAnOutsideCopyBackInTheSetAnswersNoRead is T-171's increase case. A
// stale copy that a decrease left outside the set at 2 is placed again by an
// increase to 3: its registry says 3, so the node hosts the bucket, and the
// outside mark the alter keeps refuses the read, which is forwarded to the
// owner, never answered "not found". Then the take-over copies it afresh. The
// membership half is BUG-0560's
// TestAnOutsideCopyPlacedInItsSetAgainDoesNotAnswerReads.
func TestAnOutsideCopyBackInTheSetAnswersNoRead(t *testing.T) {
	takeOverOff(t)
	tc, bucket, _, _, demoted := demotingCluster(t, nil)
	defer tc.stop()
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
	putDocument(tc, 0, testDatabase, bucket, "at-2")
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
	waitForBarrier(t, tc, 0, "after-the-increase", demoted)
	demotedCore := tc.nodes[demoted].server.core
	if !demotedCore.cluster.Hosts(placementOf(testDatabase, bucket)) {
		t.Fatalf("setup: node %d does not host %q after the increase; the read would be forwarded for another reason", demoted+1, bucket)
	}
	requireReadsFound(t, tc, demoted, bucket, []string{"at-2"}, "with the re-placed copy marked outside")
	if t.Failed() {
		return
	}
	takeOverOffHook.Store(false)
	demotedCore.cluster.wakeTakeOver(testDatabase, bucket)
	waitFreshLocally(t, tc, demoted, bucket, []string{"at-2"})
}
