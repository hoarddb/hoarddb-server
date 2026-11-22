package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 10b.4 (spec §20.8.3): the sender's rule. A node ships a bucket entry
// it originated only while its copy is opened and the entry is of the copy's
// term; a ceded or removed owner finishes delivering its term to the members
// that were in the bucket's set when the entry was logged, and to no one
// else.

// TestALegacyEntryIsNotShippedIntoALaterTerm is the last criterion
// of the re-ship defect, rev 7's step 1.4 pinned case re-aimed (A-10.4): two survivors open a log
// the starting tree of dispatch 1 wrote (five pushes the node applied as a
// replica, with no origin), and the bucket's owner is removed, which makes one
// of them the owner. It takes the bucket over, opening a term, and its replay
// to the other survivor — which has no record of those entries' stream — ships
// none of them: the peer counts 5, not 10. The log keeps working after the
// upgrade: a write appends beside the old entries and a restart replays the
// mixed log to the same count. Guard: stamp the bucket's term on a legacy
// entry — the peer applies the five again.
func TestALegacyEntryIsNotShippedIntoALaterTerm(t *testing.T) {
	segment, err := os.ReadFile(filepath.Join("..", "storage", "testdata", "wal-segment-before-origin.bin"))
	if err != nil {
		t.Fatalf("reading the log written by the starting tree: %v", err)
	}
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve the owner: %v", err)
	}
	oldOwner := nodeIndexByAddr(t, tc.addrs, owner.ID)
	for index := range tc.nodes {
		if index != oldOwner {
			tc.restartNodeOnLog(index, segment)
		}
	}
	for index := range tc.nodes {
		if index == oldOwner {
			continue
		}
		if got := tc.localCount(index); got != 5 {
			t.Fatalf("node %d counts %d after opening and replaying a log the starting tree wrote, want 5", index+1, got)
		}
	}

	oc := removeTheBucketsOwner(t, tc)
	oc.catchUp(t)
	if got := tc.localCount(oc.newOwner); got != 5 {
		t.Errorf("the new owner counts %d, want 5", got)
	}
	if got := tc.localCount(oc.survivor); got != 5 {
		t.Errorf("the other survivor counts %d after the new owner's replay, want 5: the legacy entries were shipped into the new term", got)
	}

	frame := tc.ok(oc.newOwner, transport.OpFIFOPush, PushRequest{
		Database: testDatabase, Bucket: testBucket,
		Data: map[string]interface{}{"n": 99}, WriteConcern: "one",
	})
	if frame.Opcode == transport.OpError {
		t.Fatalf("a write after the upgrade: %s", frame.Payload)
	}
	waitForWALSequence(t, tc.nodes[oc.newOwner], 8)
	tc.restartNode(oc.newOwner)
	if got := tc.localCount(oc.newOwner); got != 6 {
		t.Errorf("the new owner counts %d after restarting on a log of old and new entries, want 6", got)
	}
}

// TestARemovedNodeDoesNotShipItsCreatesToNewMembers decides the dispatch 9
// report's open item by the sender's rule: a create is one entry of its term,
// so a removed node ships it — as any of its entries — only to the members of
// the bucket's current set that were in its set when the create was logged.
// Four founders at factor 3; a bucket whose set leaves out the fourth node N
// is created and written through its owner X; X is removed while it runs, and
// N enters the set. X's replay to N addresses none of the bucket's entries,
// the create included; its replay to a member of both sets addresses them all.
// Guard: ship creates by today's rule — the create is addressed to N.
func TestARemovedNodeDoesNotShipItsCreatesToNewMembers(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	before := ringWith(t, 3, tc.addrs...)
	var removed, newcomer, staying int
	bucket := bucketNamed(t, "created", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		set, _ := before.Replicas(key, 3)
		var outside string
		for _, addr := range tc.addrs {
			if !nodeListed(set, addr) {
				outside = addr
			}
		}
		removed, newcomer = nodeIndexByAddr(t, tc.addrs, set[0].ID), nodeIndexByAddr(t, tc.addrs, outside)
		staying = nodeIndexByAddr(t, tc.addrs, set[1].ID)
		return true
	})
	tc.ok(removed, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(removed, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, bucket, 3)
	for n := 0; n < 3; n++ {
		tc.ok(removed, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": n}, WriteConcern: "all"})
	}
	removeAndTell(t, tc, staying, removed)
	for index := range tc.nodes {
		if index == removed || index == staying {
			continue
		}
		via := clusterOf(tc, staying)
		if err := clusterOf(tc, index).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
			t.Fatalf("bringing node %d to the removal: %v", index+1, err)
		}
	}
	x := clusterOf(tc, removed)
	if !x.removed() {
		t.Fatal("setup: the removed node does not know it was removed")
	}
	entries, err := x.s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the removed node's log: %v", err)
	}
	var created uint64
	var own []uint64
	for _, entry := range entries {
		if entry.Bucket != bucket || entry.Origin.Kind != storage.WALOriginOwn {
			continue
		}
		own = append(own, entry.Seq)
		if transport.Opcode(entry.Op) == transport.OpCreateBucket {
			created = entry.Seq
		}
	}
	if created == 0 || len(own) != 4 {
		t.Fatalf("setup: the removed node's log holds %d own entries of %s, create at %d; want the create and three pushes", len(own), bucket, created)
	}
	addressed := func(to int) []uint64 {
		shipped, _, err := x.replayableEntries(entries, x.peerFor(mustNode(t, x, tc.addrs[to])))
		if err != nil {
			t.Fatalf("the removed node's replay to node %d: %v", to+1, err)
		}
		var seqs []uint64
		for _, entry := range shipped {
			if entry.Bucket == bucket {
				seqs = append(seqs, entry.Seq)
			}
		}
		return seqs
	}
	if seqs := addressed(newcomer); len(seqs) != 0 {
		t.Errorf("the removed node addresses %v of %s's entries (the create at %d) to node %d, which joined the set after them", seqs, bucket, created, newcomer+1)
	}
	if seqs := addressed(staying); len(seqs) != len(own) {
		t.Errorf("the removed node addresses %v of %s's entries to node %d, in both sets; want all of %v: a removed owner finishes delivering its term", seqs, bucket, staying+1, own)
	}
}

// bucketsCededTogether picks a FIFO and a hash bucket name that a join of
// joiner moves to it, both from one old owner, and keeps one replica of that
// owner in both sets: the old owner cedes them, and the replica follows the
// new owner. It returns the names and the indexes of the old owner and of the
// replica that stays.
func bucketsCededTogether(t *testing.T, tc *testCluster, joiner string) (fifo, hash string, oldOwner, staying int) {
	t.Helper()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joiner)...)
	oldOwner, staying = -1, -1
	pick := func(prefix string) string {
		for i := 0; i < 4000; i++ {
			name := fmt.Sprintf("%s%04d", prefix, i)
			key := placementOf(testDatabase, name).encoded
			oldSet, _ := before.Replicas(key, 3)
			newSet, _ := after.Replicas(key, 3)
			if newSet[0].ID != joiner {
				continue
			}
			owner := nodeIndexByAddr(t, tc.addrs, oldSet[0].ID)
			if oldOwner >= 0 && owner != oldOwner {
				continue
			}
			for _, stays := range newSet[1:] {
				index := nodeIndexByAddr(t, tc.addrs, stays.ID)
				if index == owner || (staying >= 0 && index != staying) {
					continue
				}
				oldOwner, staying = owner, index
				return name
			}
		}
		t.Fatalf("setup: no %s bucket name moves to %s from node %d keeping node %d", prefix, joiner, oldOwner+1, staying+1)
		return ""
	}
	fifo = pick("cededfifo")
	hash = pick("cededhash")
	return fifo, hash, oldOwner, staying
}

// followsTerm reports whether follower's copy of the bucket is in owner's term
// with no placement mark.
func followsTerm(follower, owner *Server, bucket string) bool {
	ownerTerm, _, ownerErr := owner.bucketLineage(testDatabase, bucket)
	term, _, err := follower.bucketLineage(testDatabase, bucket)
	marks := follower.cluster.placementMarksOf(placementOf(testDatabase, bucket))
	return ownerErr == nil && err == nil && follower.bucketStoreOpen(testDatabase, bucket) && term == ownerTerm &&
		marks.Moved == 0 && marks.Outside == 0
}

// TestAnOldOwnerFinishesDeliveringAfterItLosesABucket is rev 7 step 10.4's
// test with A-10.10's freeze (F-8 item 6). Sixteen writers at the default
// concern write a FIFO and a hash bucket for six seconds; after one second one
// replica's connection from the old owner is frozen and a fourth node joins
// that becomes both buckets' owner. The replica is behind when the old owner
// adopts the join, and is released once the new owner has adopted it too.
// Then, within ten seconds, every member of each bucket's set follows the new
// owner's term — asserted, not filtered on; every write answered a success is
// on every member, a queue element once; and no node that held the buckets
// before the join made a whole copy: the old owner finished delivering its
// term to the replica it had left behind. Guard: strand a ceded owner's
// entries — the replica it left behind is copied whole.
func TestAnOldOwnerFinishesDeliveringAfterItLosesABucket(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	joinerID := ports.ringNode().ID
	fifo, hash, oldOwner, staying := bucketsCededTogether(t, tc, joinerID)
	tc.ok(oldOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifo, Type: BucketTypeFIFO})
	tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: hash, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, fifo, 3)
	waitForServing(t, tc, testDatabase, hash, 3)
	copiesBefore := make([]int64, len(tc.nodes))
	for index := range tc.nodes {
		copiesBefore[index] = int64(replicationGauge(t, tc.nodes[index].server.core, "hoarddb_handoff_copies_total"))
	}

	type writer struct {
		client *transport.DriverClient
		name   string
	}
	var writers []writer
	for i := 0; i < 16; i++ {
		index := i % len(tc.nodes)
		writers = append(writers, writer{client: tc.dial(index), name: fmt.Sprintf("w%02d-n%d", i, index+1)})
	}
	defer func() {
		for _, w := range writers {
			_ = w.client.Close()
		}
	}()
	stop := make(chan struct{})
	results := make([][]loadWrite, len(writers))
	var wg sync.WaitGroup
	for i, w := range writers {
		wg.Add(1)
		go func(i int, w writer) {
			defer wg.Done()
			results[i] = joinWindowWriter(w.client, w.name, "", []string{hash}, []string{fifo}, stop)
		}(i, w)
	}
	started := time.Now()
	stopWriters := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		wg.Wait()
	}
	defer stopWriters()

	time.Sleep(time.Second)
	owner := clusterOf(tc, oldOwner)
	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[staying])))
	defer unfreeze()
	time.Sleep(300 * time.Millisecond)
	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelJoin()
	if err := owner.proposeJoin(joinCtx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	replicaCore := tc.nodes[staying].server.core
	ownerCore := tc.nodes[oldOwner].server.core
	if held, ownerHeld := localCountOf(t, replicaCore, fifo), localCountOf(t, ownerCore, fifo); held >= ownerHeld {
		t.Fatalf("setup: the frozen replica, node %d, holds %d elements of %s when the old owner adopts the join and the owner %d; it is not behind",
			staying+1, held, fifo, ownerHeld)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	defer func() { _ = joiner.client.Close() }()
	pollUntil(t, "the joiner adopts the join", func() bool { return joiner.server.core.cluster.Epoch() >= owner.Epoch() })
	unfreeze()
	time.Sleep(time.Until(started.Add(6 * time.Second)))
	stopWriters()

	servers := map[string]*Server{joinerID: joiner.server.core}
	for index, addr := range tc.addrs {
		servers[addr] = tc.nodes[index].server.core
	}
	newOwner := joiner.server.core
	members := make(map[string][]*Server)
	for _, bucket := range []string{fifo, hash} {
		set, err := newOwner.cluster.Replicas(placementOf(testDatabase, bucket))
		if err != nil {
			t.Fatalf("placing %s after the join: %v", bucket, err)
		}
		for _, member := range set {
			members[bucket] = append(members[bucket], servers[member.ID])
		}
	}
	var notFollowing []string
	if !holdsWithin(10*time.Second, func() bool {
		notFollowing = nil
		for bucket, set := range members {
			for _, member := range set {
				if !followsTerm(member, newOwner, bucket) {
					notFollowing = append(notFollowing, fmt.Sprintf("%s on %s", bucket, member.cluster.self.ID))
				}
			}
		}
		return len(notFollowing) == 0
	}) {
		t.Errorf("ten seconds after the writes, these members of the set do not follow the new owner's term: %s", strings.Join(notFollowing, ", "))
	}

	// A follower takes the new term as soon as its base matches, at position 0,
	// while the new owner's stream still has a backlog for it, and a majority
	// write promises two of three copies when it is answered (BUG-0518). The
	// writers are stopped, so the owner's position is fixed: wait for every
	// member to reach it.
	var behind []string
	if !holdsWithin(10*time.Second, func() bool {
		behind = membersBehindOwner(t, members, newOwner, fifo)
		return len(behind) == 0
	}) {
		t.Errorf("ten seconds after every member followed the new owner's term, these members have not reached its position: %s", strings.Join(behind, "; "))
	}

	for bucket, set := range members {
		for _, member := range set {
			var queued map[string]int
			if bucket == fifo {
				queued = queueKeys(t, member, fifo)
				for key, count := range queued {
					if count > 1 {
						t.Errorf("%s holds %s %d times in %s", member.cluster.self.ID, key, count, fifo)
					}
				}
			}
			for _, writes := range results {
				for _, write := range writes {
					if !write.acknowledged || write.bucket != bucket {
						continue
					}
					if write.queue && queued[write.key] != 1 {
						t.Errorf("%s holds %s, a push answered a success, %d times in %s", member.cluster.self.ID, write.key, queued[write.key], fifo)
					}
				}
			}
			if bucket == hash {
				requireAcknowledgedPuts(t, member, hash, results)
			}
		}
	}
	for index := range tc.nodes {
		if copies := int64(replicationGauge(t, tc.nodes[index].server.core, "hoarddb_handoff_copies_total")) - copiesBefore[index]; copies != 0 {
			t.Errorf("node %d, which held both buckets before the join, made %d whole copies; the old owner's stream should have delivered what it lacked", index+1, copies)
		}
	}
	// The delayed replica applied the old owner's late entries while its copy
	// carried the join's mark: what hoarddb_handoff_entries_reconciled_total
	// counts.
	reconciled := replicationGauge(t, replicaCore, "hoarddb_handoff_entries_reconciled_total")
	t.Logf("the delayed replica, node %d, counted %v entries reconciled from the old owner", staying+1, reconciled)
	if reconciled == 0 {
		t.Errorf("the delayed replica, node %d, counted no entry reconciled from the old owner after the join", staying+1)
	}
}

// membersBehindOwner names each member of a bucket's set whose copy has not
// reached the new owner's term and position, or, for the FIFO bucket, whose
// local element count differs from the owner's. It is empty once every member
// has caught up with the stream (spec T-125's convergence promise).
func membersBehindOwner(t *testing.T, members map[string][]*Server, owner *Server, fifo string) []string {
	t.Helper()
	var behind []string
	for bucket, set := range members {
		ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}
		ownerCopy, err := owner.cluster.answerCopy(ref)
		if err != nil {
			t.Fatalf("reading the new owner's copy of %s: %v", bucket, err)
		}
		for _, member := range set {
			memberCopy, err := member.cluster.answerCopy(ref)
			if err != nil {
				t.Fatalf("reading %s's copy of %s: %v", member.cluster.self.ID, bucket, err)
			}
			if memberCopy.Term != ownerCopy.Term || memberCopy.Through != ownerCopy.Through {
				behind = append(behind, fmt.Sprintf("%s on %s is at term %d position %d, the owner at term %d position %d",
					bucket, member.cluster.self.ID, memberCopy.Term, memberCopy.Through, ownerCopy.Term, ownerCopy.Through))
				continue
			}
			if bucket == fifo {
				if held, ownerHeld := localCountOf(t, member, bucket), localCountOf(t, owner, bucket); held != ownerHeld {
					behind = append(behind, fmt.Sprintf("%s on %s holds %d elements, the owner %d", bucket, member.cluster.self.ID, held, ownerHeld))
				}
			}
		}
	}
	return behind
}

// requireAcknowledgedPuts fails for each put answered a success that member
// still lacks in the hash bucket after a bounded re-check. The re-check covers
// the short window, seen once in twenty probe runs (BUG-0518), between a
// copy's position and its applied data being readable; a key still missing
// after it is the loss this test exists to catch.
func requireAcknowledgedPuts(t *testing.T, member *Server, hash string, results [][]loadWrite) {
	t.Helper()
	var missing []string
	if holdsWithin(10*time.Second, func() bool {
		missing = missing[:0]
		for _, writes := range results {
			for _, write := range writes {
				if !write.acknowledged || write.queue || write.bucket != hash {
					continue
				}
				found, err := member.Find(FindRequest{Database: testDatabase, Bucket: hash, Key: write.key})
				if err != nil || !found.Found {
					missing = append(missing, write.key)
				}
			}
		}
		return len(missing) == 0
	}) {
		return
	}
	for _, key := range missing {
		t.Errorf("%s lacks %s, a put answered a success, in %s", member.cluster.self.ID, key, hash)
	}
}

// TestAnOwnEntryOfAnEarlierTermIsNotAddressed pins the clause of the sender's
// rule that keeps a version's term the term its entry was logged in: a
// replica packs the version from the term the entry ships in, so an own entry
// of an earlier term, shipped in a later one, would pack in the later term and
// outrank every write of the terms between. The control call addresses the
// owner's put to the other two members; the same call against the copy's state
// with a later term, still opened, addresses it to nobody and calls it
// stranded. Opened makes Cluster.copyIsOpened true, so only the term clause
// refuses. Guard: remove `|| e.Origin.Epoch < st.Term` from
// Cluster.bucketEntryAddressing (the second call returns the two members).
func TestAnOwnEntryOfAnEarlierTermIsNotAddressed(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	owner, _ := ownerAndReplicas(t, tc, testDatabase, testBucket)
	requireEveryCopy(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of k")
	o := tc.nodes[owner].server.core
	entries, err := o.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the owner's log: %v", err)
	}
	var put *storage.WALEntry
	for i := range entries {
		if entries[i].Bucket == testBucket && transport.Opcode(entries[i].Op) == transport.OpPut && entries[i].Origin.Kind == storage.WALOriginOwn {
			put = &entries[i]
		}
	}
	if put == nil {
		t.Fatalf("setup: the owner's log holds no own put of %s", testBucket)
	}
	st, err := o.bucketStateNamed(testDatabase, testBucket)
	if err != nil {
		t.Fatalf("reading the owner's state of %s: %v", testBucket, err)
	}

	targets, stranded, err := o.cluster.bucketEntryAddressing(*put, st)
	if err != nil || stranded || len(targets) != 2 {
		t.Fatalf("control: the owner addresses its own put to %v (stranded=%v, %v); want the two other members", targets, stranded, err)
	}
	later := st
	later.Term, later.Opened = put.Origin.Epoch+1, true
	targets, stranded, err = o.cluster.bucketEntryAddressing(*put, later)
	if err != nil {
		t.Fatalf("addressing the put against a later term: %v", err)
	}
	if len(targets) != 0 || !stranded {
		t.Errorf("an own entry of epoch %d is addressed to %v (stranded=%v) by a copy opened in term %d; want nobody and stranded",
			put.Origin.Epoch, nodeIDs(targets), stranded, later.Term)
	}
}

// streamFailingConn fails every push of the sender's stream that carries
// entries or an advance notice, and counts them, until released; the
// handshake and every question pass. A stream it fails moves no cursor.
type streamFailingConn struct {
	peerConn
	failed   *atomic.Int64
	released *atomic.Bool
}

func (s *streamFailingConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush && !s.released.Load() {
		var push ReplPushRequest
		if err := bson.Unmarshal(payload, &push); err == nil && (len(push.Entries) > 0 || push.Stream.advanceTo() > 0) {
			s.failed.Add(1)
			return nil, fmt.Errorf("a test fails this stream frame")
		}
	}
	return s.peerConn.SendRequest(ctx, lane, op, payload)
}

// bringNodesTo hands every running node but via via's membership and the
// history above its own epoch, as the next gossip round would.
func bringNodesTo(t *testing.T, nodes []*testNode, via *Cluster) {
	t.Helper()
	for _, node := range nodes {
		if node.server == nil || node.server.core.cluster == via {
			continue
		}
		member := node.server.core.cluster
		answer := GossipResponse{Nodes: via.memberNodes(), Members: via.memberIDs(), Epoch: via.Epoch(), History: via.historyAbove(member.Epoch())}
		if err := member.reconcileAnswer(via.self.ID, answer); err != nil {
			t.Fatalf("bringing %s to %s's membership: %v", member.self.ID, via.self.ID, err)
		}
	}
	pollUntil(t, "every running node adopts the change", func() bool {
		for _, node := range nodes {
			if node.server != nil && node.server.core.cluster.Epoch() != via.Epoch() {
				return false
			}
		}
		return true
	})
}

// TestAGivenBackBucketDoesNotReplayAnEarlierTermIntoTheNewOne is the
// multi-node pin of the term clause of the sender's rule, behind
// TestAnOwnEntryOfAnEarlierTermIsNotAddressed. Founders A, R and X at factor 3
// and a joiner D: the bucket's owner is A, and D's join makes D its owner
// with A and R kept. A's stream to R fails every frame from before A's put of
// v1 (A and X apply it); D takes the bucket over and R copies it from D, so
// R's position for A stays below the put; v2 is written through D at all.
// D is removed and A takes the bucket back at a later term, which R accepts
// without a copy. Once A's stream to R is released, R still holds v2 at A's
// version: the put of v1, logged by A in the earlier term, is not replayed
// into the later one. Guard: remove `|| e.Origin.Epoch < st.Term` from
// Cluster.bucketEntryAddressing (R holds v1, packed in the later term).
func TestAGivenBackBucketDoesNotReplayAnEarlierTermIntoTheNewOne(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	joinerID := ports.ringNode().ID
	founders := ringWith(t, 3, tc.addrs...)
	joined := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joinerID)...)
	var a, r int
	bucket := bucketNamed(t, "givenback", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		before, _ := founders.Replicas(key, 3)
		after, _ := joined.Replicas(key, 3)
		if after[0].ID != joinerID || !nodeListed(after, before[0].ID) {
			return false
		}
		a = nodeIndexByAddr(t, tc.addrs, before[0].ID)
		for index, addr := range tc.addrs {
			if index != a && nodeListed(after, addr) {
				r = index
			}
		}
		return true
	})
	tc.ok(a, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(a, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	ownerA := clusterOf(tc, a)
	var failed atomic.Int64
	var released atomic.Bool
	defer released.Store(true)
	wrapPeerConn(ownerA.peerFor(mustNode(t, ownerA, tc.addrs[r])), func(inner peerConn) peerConn {
		return &streamFailingConn{peerConn: inner, failed: &failed, released: &released}
	})

	tc.ok(a, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"v": "v1"}, WriteConcern: "majority"})
	entries, err := tc.nodes[a].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading A's log: %v", err)
	}
	var putSeq uint64
	for _, entry := range entries {
		if entry.Bucket == bucket && entry.Origin.Kind == storage.WALOriginOwn && transport.Opcode(entry.Op) == transport.OpPut {
			putSeq = entry.Seq
		}
	}
	if putSeq == 0 {
		t.Fatal("setup: A's log holds no own put of the bucket")
	}

	// D joins and takes the bucket over; R copies it from D.
	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelJoin()
	outcome, err := ownerA.proposeJoinOutcome(joinCtx, ports.ringNode())
	if err != nil || outcome.kind != changeCommitted {
		t.Fatalf("joining D: outcome %q, %v; want committed", outcome.kind, err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	defer func() { _ = joiner.client.Close() }()
	all := append(append([]*testNode(nil), tc.nodes...), joiner)
	bringNodesTo(t, all, ownerA)
	d := joiner.server.core
	waitTakenOver(t, d, testDatabase, bucket)
	waitFollowing(t, tc.nodes[r].server.core, d, testDatabase, bucket)
	waitFollowing(t, tc.nodes[a].server.core, d, testDatabase, bucket)
	requireEveryCopy(t, tc.frameOn(joiner, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k",
		Data: map[string]interface{}{"v": "v2"}, WriteConcern: "all"}), "v2 through D")

	// D is removed; A takes the bucket back and R accepts its term.
	copiesBefore := replicationGauge(t, tc.nodes[r].server.core, "hoarddb_handoff_copies_total")
	removeCtx, cancelRemove := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelRemove()
	removal, err := ownerA.proposeRemovalOutcome(removeCtx, joinerID)
	if err != nil || removal.kind != changeCommitted {
		t.Fatalf("removing D: outcome %q, %v; want committed", removal.kind, err)
	}
	bringNodesTo(t, all, ownerA)
	waitTakenOver(t, tc.nodes[a].server.core, testDatabase, bucket)
	waitFollowing(t, tc.nodes[r].server.core, tc.nodes[a].server.core, testDatabase, bucket)
	if copies := replicationGauge(t, tc.nodes[r].server.core, "hoarddb_handoff_copies_total") - copiesBefore; copies != 0 {
		t.Fatalf("setup: R made %v whole copies across the give-back; it must accept A's term without one", copies)
	}

	// The preconditions, before the release.
	entries, err = tc.nodes[a].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading A's log: %v", err)
	}
	for _, entry := range entries {
		if entry.Bucket == bucket && entry.Origin.Kind == storage.WALOriginOwn && entry.Seq > putSeq {
			t.Fatalf("setup: A logged an own entry of the bucket (sequence %d) after the put of v1 at %d", entry.Seq, putSeq)
		}
	}
	stream := ownerA.peerFor(mustNode(t, ownerA, tc.addrs[r]))
	if cursor, _ := stream.confirmed(); cursor >= putSeq || failed.Load() == 0 {
		t.Fatalf("setup: A's cursor for R is %d with the put at %d, and %d stream frames were failed", cursor, putSeq, failed.Load())
	}

	// Released, A replays its log to R from R's cursor. An entry the replay
	// passes over moves no cursor, so the wait is for the replay to finish,
	// not for the cursor to reach A's tail.
	released.Store(true)
	replayCtx, cancelReplay := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelReplay()
	if err := replayToPeer(replayCtx, stream); err != nil {
		t.Fatalf("A's replay to R after the release: %v", err)
	}
	_, data, version, err := tc.nodes[r].server.core.versionedFind(testDatabase, bucket, "k")
	if err != nil {
		t.Fatalf("reading k on R: %v", err)
	}
	ownerVersion := versionOn(t, tc.nodes[a].server.core, testDatabase, bucket, "k")
	if got := docField(t, data, "v"); got != "v2" || version != ownerVersion {
		t.Errorf("R holds k = %s at version %d and A holds version %d; want v2 at A's version: A's put of v1, logged in an earlier term, was replayed into the later one",
			got, version, ownerVersion)
	}
}
