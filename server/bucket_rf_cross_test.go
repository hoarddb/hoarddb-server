package server

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The cross-cutting tests of the factor alter (bucket-replication-rev8-spec
// §14.1, §14.2, §14.3; G3 build doc step 7): two alters in one epoch under
// load, an increase that reaches a member which was down, and an alter that
// never joins a replica run. Nothing here adds code; each pins what steps 1 to
// 6 built.

// queueElement is one element the writers of T-155 push: its writer and its
// number.
type queueElement struct {
	Writer int32 `bson:"writer"`
	Number int32 `bson:"number"`
}

// heldElements pops node s's own copy of a queue bucket empty, locally, and
// counts each element.
func heldElements(t *testing.T, s *Server, bucket string) map[queueElement]int {
	t.Helper()
	held := make(map[queueElement]int)
	for {
		popped, err := s.Pop(PopRequest{Database: testDatabase, Bucket: bucket})
		if err != nil {
			t.Fatalf("popping %s's own copy of %q: %v", s.cluster.self.ID, bucket, err)
		}
		if !popped.Found {
			return held
		}
		var element queueElement
		if err := bson.Unmarshal(popped.Items[0], &element); err != nil {
			t.Fatalf("decoding an element of %s's copy of %q: %v", s.cluster.self.ID, bucket, err)
		}
		held[element]++
	}
}

// TestTwoAltersInOneEpochLoseAndRepeatNothing is T-155. A queue bucket at 3
// on three nodes; eight writers push to it through its owner while it is
// lowered to 2 and raised to 3 again, all in one epoch. The node the decrease
// demotes is marked outside and keeps its lineage; the increase places it
// again, so it is copied afresh, and its pull is held for a while: the owner's
// awaiting record keeps every push off it, read on the wire, until its
// snapshot clears the record. Then every holder's own copy is read with
// multiplicity: every acknowledged push is there exactly once, and nothing is
// there twice.
func TestTwoAltersInOneEpochLoseAndRepeatNothing(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "queue")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := clusterOf(tc, 0)
	epoch := owner.Epoch()
	demoted := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, owner, testDatabase, bucket, 3)[2])
	pullHeld, releasePull := holdPullsOf(t, bucket)
	defer releasePull()

	const writers = 8
	var (
		mu       sync.Mutex
		acked    []queueElement
		stop     atomic.Bool
		pushing  sync.WaitGroup
		refusals atomic.Int64
	)
	for writer := 0; writer < writers; writer++ {
		pushing.Add(1)
		go func(writer int32) {
			defer pushing.Done()
			for number := int32(0); !stop.Load(); number++ {
				element := queueElement{Writer: writer, Number: number}
				frame := tc.frame(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket,
					Data: map[string]interface{}{"writer": element.Writer, "number": element.Number}})
				if frame.Opcode == transport.OpError {
					refusals.Add(1)
					continue
				}
				mu.Lock()
				acked = append(acked, element)
				mu.Unlock()
			}
		}(int32(writer))
	}
	// The writers stop before the cluster does, however the test ends.
	defer func() {
		stop.Store(true)
		pushing.Wait()
	}()
	phase := func() { time.Sleep(time.Second) }
	phase()
	lowerRetryingWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
	// Once a barrier shows the demoted node has been sent everything logged
	// before it, no push of the bucket is addressed to it again until its
	// copy's snapshot: at 2 it is outside the set, at 3 it awaits its copy.
	waitForBarrier(t, tc, 0, "after-the-decrease", demoted)
	pushed := recordPushes(peerOf(t, tc, 0, demoted))
	phase()
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
	awaitHeld(t, pullHeld, fmt.Sprintf("node %d's pull of its re-placed copy", demoted+1))
	phase()
	if sent := pushed.entriesOf(transport.OpFIFOPush, bucket); sent != 0 {
		t.Errorf("the owner pushed %d of the bucket's entries to node %d while it was outside the set or awaiting its copy: "+
			"it refuses them, and the owner's stream to it stalls", sent, demoted+1)
	}
	releasePull()
	phase()
	stop.Store(true)
	pushing.Wait()
	if now := owner.Epoch(); now != epoch {
		t.Fatalf("setup: the epoch moved from %d to %d; both alters must be in one epoch", epoch, now)
	}
	t.Logf("%d pushes acknowledged, %d refused", len(acked), refusals.Load())

	demotedCore := tc.nodes[demoted].server.core
	pollUntil(t, fmt.Sprintf("node %d's re-placed copy is copied afresh", demoted+1), func() bool {
		return demotedCore.bucketStoreOpen(testDatabase, bucket) && outsideMarkOf(demotedCore, bucket) == 0 &&
			len(awaitingInMap(owner, placementOf(testDatabase, bucket))) == 0
	})
	waitForBarrier(t, tc, 0, "after-the-writes", 1, 2)
	for index := range tc.nodes {
		held := heldElements(t, tc.nodes[index].server.core, bucket)
		var missing, twice int
		for _, element := range acked {
			if held[element] == 0 {
				missing++
			}
		}
		for element, count := range held {
			if count > 1 {
				twice++
				if twice <= 3 {
					t.Errorf("node %d's own copy holds %+v %d times", index+1, element, count)
				}
			}
		}
		if missing > 0 || twice > 0 {
			t.Errorf("node %d's own copy lacks %d of the %d acknowledged pushes and holds %d elements more than once", index+1, missing, len(acked), twice)
		}
	}
}

// TestAnIncreaseReachesAMemberThatWasDownBehindEarlierEntries is T-51. A
// bucket at 1 is written while the member an increase to 2 will place is
// down; the increase is logged, and more writes. When the member returns, the
// owner's replay from its cursor reads the bucket's earlier entries at 2,
// which places it, and leaves them out because it awaits its copy: the member
// receives the alter, writes its needs-a-copy record, and its stream carries
// every other bucket at full count. Addressed to it, the earlier entries
// would be refused, and its stream would stall before the alter.
func TestAnIncreaseReachesAMemberThatWasDownBehindEarlierEntries(t *testing.T) {
	// Auto-rebalance off: the copy stays pending, which the pending-copy
	// worker would otherwise make at once.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	createBucketAt(tc, 0, testDatabase, "other", 3)
	waitForServing(t, tc, testDatabase, "other", 3)
	member := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, 2)[1])

	startMember := stopNode(t, tc, member)
	for i := 0; i < 5; i++ {
		putDocument(tc, 0, testDatabase, bucket, fmt.Sprintf("before-%d", i))
	}
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
	for i := 0; i < 3; i++ {
		putDocument(tc, 0, testDatabase, bucket, fmt.Sprintf("after-%d", i))
	}
	startMember()

	waitForWALCount(t, tc, member, transport.OpAlterBucket, bucket, 1)
	waitForBarrier(t, tc, 0, "barrier", member)
	core := tc.nodes[member].server.core
	if puts := walOpCount(t, core.wal, transport.OpPut, bucket); puts != 0 {
		t.Errorf("node %d, awaiting its copy, was sent %d of the bucket's writes", member+1, puts)
	}
	if _, found := needsCopyOnDisk(t, core, bucket); !found {
		t.Errorf("node %d holds no needs-a-copy record of %q after the increase reached it", member+1, bucket)
	}
	requireWriteReachesEveryCopy(t, tc, 0, "other", "after-the-return", 3)
}

// TestAnAlterNeverJoinsAReplicaRun is T-186, guard-only (P-2: the run's
// admission was built by BUG-0521's rework). The owner's stream to a member is
// frozen while writes, the alter and more writes are logged, so the push that
// follows the release carries data entries, then the alter, then data
// entries. A run admits only an entry that takes its bucket's fence shared,
// and the alter's is exclusive, so the member settles the run before it
// before it admits the alter: every write logged before the alter is applied
// when the alter is admitted, and none after it is.
func TestAnAlterNeverJoinsAReplicaRun(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := clusterOf(tc, 0)
	member := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, owner, testDatabase, bucket, 3)[1])
	memberNode := tc.nodes[member]

	var before, after []string
	for i := 0; i < 4; i++ {
		before = append(before, fmt.Sprintf("before-%d", i))
		after = append(after, fmt.Sprintf("after-%d", i))
	}
	var admitted atomic.Bool
	var mu sync.Mutex
	var unapplied, early []string
	hook := func(c *Cluster, entry ReplEntry) {
		if c.self.ID != tc.addrs[member] || transport.Opcode(entry.Op) != transport.OpAlterBucket || entry.Bucket != bucket {
			return
		}
		admitted.Store(true)
		mu.Lock()
		defer mu.Unlock()
		for _, key := range before {
			if !holdsKeyLocally(memberNode, bucket, key) {
				unapplied = append(unapplied, key)
			}
		}
		for _, key := range after {
			if holdsKeyLocally(memberNode, bucket, key) {
				early = append(early, key)
			}
		}
	}
	afterReplicaAdmittedHook.Store(&hook)
	defer afterReplicaAdmittedHook.Store(nil)

	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[member])))
	defer unfreeze()
	for _, key := range before {
		putDocumentAt(tc, 0, testDatabase, bucket, key, "one")
	}
	// An inheriting bucket declaring the factor it inherits is a logged alter
	// every holder keeps (owner R9-Q7): row 1 on the member.
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	for _, key := range after {
		putDocumentAt(tc, 0, testDatabase, bucket, key, "one")
	}
	unfreeze()
	waitForBarrier(t, tc, 0, "barrier", member)
	if !admitted.Load() {
		t.Fatalf("node %d never admitted the alter", member+1)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(unapplied) > 0 || len(early) > 0 {
		t.Errorf("node %d admitted the alter with %v, logged before it, not yet applied, and %v, logged after it, applied: "+
			"the alter joined a replica run", member+1, unapplied, early)
	}
	requireHeldLocally(t, memberNode.server.core, bucket, append(before, after...))
}

// lowerRetryingWithin is alterWithin for a decrease made while writes go on:
// R11 compares each survivor's copy with the demoted one's at slightly
// different instants, so a write in flight can leave the survivor a few
// entries short and the decrease is refused UNAVAILABLE, which its message
// says to retry (spec §4.3). The decrease is retried on that refusal alone,
// within alterWithin's ten-second deadline.
func lowerRetryingWithin(t *testing.T, tc *testCluster, index int, req AlterBucketRequest) AlterBucketResponse {
	t.Helper()
	started := time.Now()
	for {
		frame := alterFrame(t, tc, index, req)
		if frame.Opcode != transport.OpError || !strings.Contains(refusalOf(t, frame).Message, "cannot be lowered from") {
			answer := alterAccepted(t, frame)
			if took := time.Since(started); took > 10*time.Second {
				t.Fatalf("the decrease of %q to %d took %s, over its ten-second deadline", req.Name, req.ReplicationFactor, took)
			}
			return answer
		}
		if time.Since(started) > 10*time.Second {
			t.Fatalf("the decrease of %q to %d was refused for its coverage for ten seconds: %s", req.Name, req.ReplicationFactor, refusalOf(t, frame).Message)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
