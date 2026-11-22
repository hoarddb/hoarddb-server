package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// loseTheLocalCopy removes one node's copy of a bucket without telling the
// cluster, the state a node is in before it is (re)seeded: placed, and holding
// nothing.
func loseTheLocalCopy(t *testing.T, tc *testCluster, index int, database, bucket string) {
	t.Helper()
	core := tc.nodes[index].server.core
	if resp, err := core.DeleteBucketCtx(context.Background(), DeleteBucketRequest{Database: database, Name: bucket}); err != nil || !resp.Success {
		t.Fatalf("dropping node %d's local copy of %s.%s: %+v, %v", index+1, database, bucket, resp, err)
	}
	if core.bucketStoreOpen(database, bucket) {
		t.Fatalf("node %d still serves %s.%s after its copy was dropped", index+1, database, bucket)
	}
}

// TestMigratedBucketKeepsItsFactor is T-13b. A pulled bucket is re-attached from
// the source's start response, which names its store type, indexes and store
// fields and now its factor. The materialise builds its catalog entry from a
// literal, so a factor left out is not an error: the bucket quietly goes back to
// the node default, on the node that was seeded because the bucket needed it.
func TestMigratedBucketKeepsItsFactor(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 2)
	waitForRegistrations(t, tc, testDatabase, "wide", 2)
	putDocument(tc, 0, testDatabase, "wide", "k")

	var placed []string
	for _, node := range tc.nodes {
		if _, holds := registeredFactor(node.server.core.cluster, testDatabase, "wide"); holds {
			placed = mustReplicaIDs(t, node.server.core.cluster, testDatabase, "wide")
			break
		}
	}
	replicaIndex := -1
	for index, node := range tc.nodes {
		if node.server.core.cluster.self.ID == placed[1] {
			replicaIndex = index
		}
	}
	if replicaIndex < 0 {
		t.Fatalf("test setup: no node is the second holder %s", placed[1])
	}
	loseTheLocalCopy(t, tc, replicaIndex, testDatabase, "wide")

	replica := tc.nodes[replicaIndex].server.core
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := replica.cluster.ensureMigrated(ctx, testDatabase, "wide"); err != nil {
		t.Fatalf("re-seeding the replica: %v", err)
	}

	entry, catalogued, err := replica.readCatalogEntry(testDatabase, "wide")
	if err != nil || !catalogued || entry.ReplicationFactor != 2 {
		t.Fatalf("the migrated bucket's catalog entry is %+v (catalogued %v, %v), want factor 2", entry, catalogued, err)
	}
	if factor, registered := registeredFactor(replica.cluster, testDatabase, "wide"); !registered || factor != 2 {
		t.Fatalf("the migrated bucket is registered as %d (registered %v), want 2", factor, registered)
	}
	if got := servedBucket(t, replica, testDatabase, "wide").rec.replicationFactor.Load(); got != 2 {
		t.Fatalf("the migrated bucket holds factor %d, want 2", got)
	}
}

// TestMigrateOntoANodeOnlyItsOwnDefaultPlacesIsRefused is T-37. A node that
// never held a bucket declared below its default computes a set that includes
// itself, so a pull it starts would be a copy nobody ships to. The source knows
// the factor and decides: it refuses a node its own placement excludes, before
// it builds a snapshot it would throw away, and names the placed set. Nothing
// is left on the refused node.
func TestMigrateOntoANodeOnlyItsOwnDefaultPlacesIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "solo", 1)
	putDocument(tc, 0, testDatabase, "solo", "k")

	holder, outsider := -1, -1
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, "solo") {
			holder = index
		} else if outsider < 0 {
			outsider = index
		}
	}
	if holder < 0 || outsider < 0 {
		t.Fatalf("test setup: holder %d, outsider %d", holder, outsider)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	target := tc.nodes[outsider].server.core
	err := target.cluster.ensureMigrated(ctx, testDatabase, "solo")
	if err == nil {
		t.Fatalf("node %d, which only its own default places, was seeded with a copy nobody ships to", outsider+1)
	}
	holderID := tc.nodes[holder].server.core.cluster.self.ID
	if !strings.Contains(err.Error(), holderID) || !strings.Contains(err.Error(), "replica set") {
		t.Fatalf("the refusal does not name the placed set (%s): %v", holderID, err)
	}
	if target.bucketStoreOpen(testDatabase, "solo") {
		t.Fatal("the refused pull left the bucket attached")
	}
	if _, catalogued, _ := target.readCatalogEntry(testDatabase, "solo"); catalogued {
		t.Fatal("the refused pull left a catalog entry")
	}
	if _, registered := registeredFactor(target.cluster, testDatabase, "solo"); registered {
		t.Fatal("the refused pull left a registered factor")
	}
	if !tc.nodes[holder].server.core.bucketStoreOpen(testDatabase, "solo") {
		t.Fatal("the source lost its copy")
	}
}

// pruneEveryLog rotates and prunes each node's write-ahead log to its tail,
// the state a running cluster is in after its first retention passes: history a
// new member could be replayed from is gone, so it can only be seeded by a pull.
// It writes 20 MB documents (a 64 MB segment has to fill before a prune deletes
// anything) into a bucket each node owns.
func pruneEveryLog(t *testing.T, tc *testCluster) {
	t.Helper()
	blob := strings.Repeat("x", 20<<20)
	for index := range tc.nodes {
		bulk := bucketOwnedBy(t, tc, index, fmt.Sprintf("bulk%d-", index))
		createBucketAt(tc, index, testDatabase, bulk, 0)
		for i := 0; i < 4; i++ {
			putDocumentWith(tc, index, testDatabase, bulk, fmt.Sprintf("big%d", i), map[string]interface{}{"blob": blob})
		}
		wal := tc.nodes[index].server.core.wal
		if err := wal.Truncate(wal.LastSequence()); err != nil {
			t.Fatalf("pruning node %d's log: %v", index+1, err)
		}
		if first, err := wal.FirstSequence(); err != nil || first <= 1 {
			t.Fatalf("node %d's log was not pruned (first %d, %v)", index+1, first, err)
		}
	}
}

// TestAMemberThatNeverHeldTheBucketIsSeededAfterAJoin is review F-4, D-2 and
// N-4. After a join the holders ask each placed member to pull, and the asked
// node used to decide whether it belonged from its own placement (F-4). A source
// that has not yet learned the new epoch refuses the joiner's pull (D-2), and
// the displaced holder's hand-off, launched once per adoption, was never asked
// again (D-2). Left to the ring, that lagging source happened in about half of
// the runs, so the test forces it: a bucket is chosen whose new owner is an old
// member O, the joiner a non-owner replica, and O is held on the old epoch by a
// hook while the displaced holder H, the third member and the joiner move on.
// H's first sweep is refused by O; only a retry can settle it once O learns the
// epoch. Then the whole set is asserted: exactly the placed members hold each
// bucket, each with its entry, and no node outside it keeps a copy.
func TestAMemberThatNeverHeldTheBucketIsSeededAfterAJoin(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()

	// Placements on the ring the join will produce, before anything is created.
	oldRing, futureRing := ring.New(3), ring.New(3)
	for _, addr := range tc.addrs {
		for _, r := range []*ring.Ring{oldRing, futureRing} {
			if err := r.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := futureRing.AddNode(newcomer); err != nil {
		t.Fatal(err)
	}
	var target, laggingSource, displaced, proposer string
	names := []string{}
	for i := 0; i < 400 && target == ""; i++ {
		name := fmt.Sprintf("wide%03d", i)
		key := placementOf(testDatabase, name).encoded
		future, _ := futureRing.Replicas(key, 3)
		oldOwner, _ := oldRing.Owner(key)
		if len(future) != 3 || future[0].ID != oldOwner.ID || future[0].ID == newcomer.ID {
			continue
		}
		inSet := map[string]bool{}
		for _, node := range future {
			inSet[node.ID] = true
		}
		if !inSet[newcomer.ID] {
			continue
		}
		target, laggingSource = name, future[0].ID
		for _, addr := range tc.addrs {
			switch {
			case !inSet[addr]:
				displaced = addr
			case addr != laggingSource:
				proposer = addr
			}
		}
	}
	if target == "" || displaced == "" || proposer == "" {
		t.Fatal("test setup: no bucket name has an old owner, the joiner as a non-owner replica, and a displaced holder")
	}
	names = append(names, target)
	for i := 0; i < 5; i++ {
		names = append(names, fmt.Sprintf("other%02d", i))
	}

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, name := range names {
		createBucketAt(tc, 0, testDatabase, name, 3)
		putDocument(tc, 0, testDatabase, name, "k")
	}
	for _, name := range names {
		waitForServing(t, tc, testDatabase, name, 3)
	}
	pruneEveryLog(t, tc)

	oldFloor, oldCeiling := sweepRetryFloor.get(), sweepRetryCeiling.get()
	sweepRetryFloor.set(100 * time.Millisecond)
	sweepRetryCeiling.set(500 * time.Millisecond)
	defer func() { sweepRetryFloor.set(oldFloor); sweepRetryCeiling.set(oldCeiling) }()

	hold := func(nodeID string, epoch uint64) error {
		if nodeID == laggingSource && epoch > InitialEpoch {
			return fmt.Errorf("test: %s is held on the old membership epoch", nodeID)
		}
		return nil
	}
	adoptMembershipHook.Store(&hold)
	defer adoptMembershipHook.Store(nil)

	indexOf := func(addr string) int { return nodeIndexByAddr(t, tc.addrs, addr) }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	proposing := tc.nodes[indexOf(proposer)].server.core.cluster
	if err := proposing.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	if err := joiner.server.core.cluster.reconcileMembership(proposer, proposing.memberNodes(), proposing.memberIDs(), proposing.Epoch()); err != nil {
		t.Fatalf("joining node resync: %v", err)
	}
	nodes := []*testNode{tc.nodes[0], tc.nodes[1], tc.nodes[2], joiner}
	held := tc.nodes[indexOf(laggingSource)].server.core.cluster

	// The displaced holder learns the epoch and sweeps while the source lags.
	for _, addr := range []string{displaced, proposer} {
		deadline := time.Now().Add(60 * time.Second)
		for tc.nodes[indexOf(addr)].server.core.cluster.Epoch() != proposing.Epoch() {
			if time.Now().After(deadline) {
				t.Fatalf("%s never learned the new epoch", addr)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if held.Epoch() == proposing.Epoch() {
		t.Fatal("test setup: the held member adopted the epoch anyway")
	}
	time.Sleep(3 * time.Second) // H's first sweep, refused by the lagging source, and its retries
	if !tc.nodes[indexOf(displaced)].server.core.bucketStoreOpen(testDatabase, target) {
		t.Fatal("test setup: the displaced holder released its copy while the source lagged")
	}

	// The source learns the epoch, as it does in a real cluster when gossip
	// reaches it.
	adoptMembershipHook.Store(nil)
	deadline := time.Now().Add(90 * time.Second)
	for held.Epoch() != proposing.Epoch() {
		_ = held.peerFor(mustNode(t, held, proposer)).gossip(ctx)
		if time.Now().After(deadline) {
			t.Fatal("the held member never learned the epoch")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The set must settle on exactly the placed members, each holding every
	// entry, and no node outside it may keep a copy that no longer receives
	// writes.
	settled := func() (string, bool) {
		for _, name := range names {
			placed, _ := proposing.replicasAtFactor(placementOf(testDatabase, name), 3)
			inSet := map[string]bool{}
			for _, node := range placed {
				inSet[node.ID] = true
			}
			for _, node := range nodes {
				id := node.server.core.cluster.self.ID
				holds := node.server.core.bucketStoreOpen(testDatabase, name)
				switch {
				case inSet[id] && !holds:
					return fmt.Sprintf("%s is placed on %s and does not hold it", name, id), false
				case !inSet[id] && holds:
					return fmt.Sprintf("%s still holds a copy of %s that is no longer placed on it: an orphan that stops receiving writes", id, name), false
				case inSet[id]:
					if resp, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: name, Key: "k"}); err != nil || !resp.Found {
						return fmt.Sprintf("%s holds %s without its entry", id, name), false
					}
				}
			}
		}
		return "", true
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		problem, ok := settled()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the set never settled on exactly the placed members: %s", problem)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestMigrateOnANodeHoldingACopyOutsideTheSetIsRefused: the membership check
// comes before the already-held shortcut. A node that holds a copy it is not
// placed on — the primitive will put one anywhere it is told — is refused by
// `migrate` naming the set, not told the migration worked.
//
// Ported from 5f92547's reorder (GAP-23 step 1.7); its test, T-15, waits for the
// alter in Part 3, so this one builds the state with the pull primitive, on a
// bucket that inherits (the source refuses the primitive for one that declared a
// factor its puller is not placed on).
func TestMigrateOnANodeHoldingACopyOutsideTheSetIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "solo", 0)
	putDocument(tc, 0, testDatabase, "solo", "k")

	holder, outsider := -1, -1
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, "solo") {
			holder = index
		} else if outsider < 0 {
			outsider = index
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	target := tc.nodes[outsider].server.core.cluster
	source := mustNode(t, target, tc.nodes[holder].server.core.cluster.self.ID)
	if err := target.pullBucket(ctx, testDatabase, "solo", source); err != nil {
		t.Fatalf("the pull primitive: %v", err)
	}
	if !tc.nodes[outsider].server.core.bucketStoreOpen(testDatabase, "solo") {
		t.Fatal("test setup: the outsider holds no copy")
	}

	err := target.ensureMigrated(ctx, testDatabase, "solo")
	if err == nil {
		t.Fatal("migrate on a node holding a copy outside the bucket's set was told it worked")
	}
	if !strings.Contains(err.Error(), "replica set") || !strings.Contains(err.Error(), tc.nodes[holder].server.core.cluster.self.ID) {
		t.Fatalf("the refusal does not name the set: %v", err)
	}
}

// TestANodeTheSourceRefusesAsOutsideTheSetIsSettledNotRetried is verifier N-2. A
// node that released a copy of a declared bucket has forgotten its factor, so
// by its own default it counts itself placed, asks to pull, and is refused by
// the source that knows the factor. That refusal is the answer -- "you are
// outside the set at factor N" -- and it settles the question: retrying it every
// fifteen seconds is a false alarm on a healthy cluster, and after ten minutes a
// warning about buckets that were never unsettled.
func TestANodeTheSourceRefusesAsOutsideTheSetIsSettledNotRetried(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "solo", 1)
	putDocument(tc, 0, testDatabase, "solo", "k")

	outsider := -1
	for index, node := range tc.nodes {
		if !node.server.core.bucketStoreOpen(testDatabase, "solo") {
			outsider = index
			break
		}
	}
	if outsider < 0 {
		t.Fatal("test setup: every node holds the bucket")
	}
	c := tc.nodes[outsider].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.migrateIfMissing(ctx, testDatabase, "solo"); err != nil {
		t.Fatalf("a node the source refused as outside the set is reported unsettled: %v", err)
	}
	if pending := c.sweepPass(ctx, sweepRound{}, []heldBucket{{database: testDatabase, bucket: "solo"}}); len(pending) != 0 {
		t.Fatalf("the sweep would ask again about %v", pending)
	}
	if tc.nodes[outsider].server.core.bucketStoreOpen(testDatabase, "solo") {
		t.Fatal("the refused node took a copy")
	}
}

// TestTheGiveUpWarningNamesNoRemedyThatIsRefused: when a sweep does give up, it
// says which buckets and that the next membership change or restart asks again;
// it must not tell the operator to run `migrate`, which the same rule refuses on
// a node outside the set.
func TestTheGiveUpWarningNamesNoRemedyThatIsRefused(t *testing.T) {
	var logged bytes.Buffer
	var mu sync.Mutex
	writer := syncWriter{mu: &mu, buffer: &logged}
	clusterTestLogger.Store(slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer clusterTestLogger.Store(nil)

	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "y")
	createBucketAt(tc, 0, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	loseTheLocalCopy(t, tc, 1, testDatabase, bucket)
	stopNode(t, tc, 0) // the owner: nothing can seed node 2, so the bucket stays unsettled

	oldFloor, oldGiveUp := sweepRetryFloor.get(), sweepRetryGiveUp.get()
	sweepRetryFloor.set(20 * time.Millisecond)
	sweepRetryGiveUp.set(300 * time.Millisecond)
	defer func() { sweepRetryFloor.set(oldFloor); sweepRetryGiveUp.set(oldGiveUp) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// The bucket was held when the sweep began (that is how it came to be
	// pending) and has since been released.
	survivor := tc.nodes[1].server.core.cluster
	survivor.retrySettling(ctx, sweepRound{generation: survivor.sweepGeneration.Load()}, []heldBucket{{database: testDatabase, bucket: bucket}})

	mu.Lock()
	defer mu.Unlock()
	line := ""
	for _, l := range strings.Split(logged.String(), "\n") {
		if strings.Contains(l, "gave up") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("the sweep did not give up; log:\n%s", logged.String())
	}
	if strings.Contains(line, "migrate") {
		t.Fatalf("the give-up warning names `migrate`, which the placement rule refuses on a node outside the set: %s", line)
	}
}

// syncWriter serialises writes to a buffer several nodes log into.
type syncWriter struct {
	mu     *sync.Mutex
	buffer *bytes.Buffer
}

func (w syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}

// TestAMemberBecomingReachableAgainRearmsTheSweepForWhatItGaveUpOn is verifier
// N-3. A sweep that gave up on a bucket because a member was down was never
// asked again when the member came back: the orphan stayed until a restart. A
// peer going from unreachable to reachable is an event the node already
// detects (peer.recordReachable); it now re-runs the sweep for exactly the
// buckets that were left unsettled -- not a pass over every bucket, and not a
// timer per bucket -- at most once in a while however often a peer flaps.
func TestAMemberBecomingReachableAgainRearmsTheSweepForWhatItGaveUpOn(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "y")
	createBucketAt(tc, 0, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	loseTheLocalCopy(t, tc, 1, testDatabase, bucket)

	survivor := tc.nodes[1].server.core.cluster
	start := stopNode(t, tc, 0)
	// The survivor notices the owner is down before the sweep gives up.
	deadline := time.Now().Add(30 * time.Second)
	for {
		peer := survivor.peerFor(mustNode(t, survivor, tc.addrs[0]))
		if _, down := peer.downSince(); down {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the survivor never recorded the owner as unreachable")
		}
		time.Sleep(100 * time.Millisecond)
	}

	oldFloor, oldGiveUp := sweepRetryFloor.get(), sweepRetryGiveUp.get()
	sweepRetryFloor.set(20 * time.Millisecond)
	sweepRetryGiveUp.set(200 * time.Millisecond)
	defer func() { sweepRetryFloor.set(oldFloor); sweepRetryGiveUp.set(oldGiveUp) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	survivor.recordResidue(sweepRound{generation: survivor.sweepGeneration.Load(), epoch: survivor.Epoch()}, survivor.retrySettling(ctx, sweepRound{generation: survivor.sweepGeneration.Load()}, []heldBucket{{database: testDatabase, bucket: bucket}}), nil)
	if got := survivor.sweepLeftCount(); got != 1 {
		t.Fatalf("after giving up the survivor remembers %d unsettled buckets, want 1", got)
	}

	before := survivor.sweepRearms.Load()
	start()
	deadline = time.Now().Add(60 * time.Second)
	for survivor.sweepRearms.Load() == before || survivor.sweepLeftCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the owner returned and the sweep was not re-armed (re-arms %d, unsettled %d)", survivor.sweepRearms.Load()-before, survivor.sweepLeftCount())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !tc.nodes[1].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Fatal("the re-armed sweep did not restore the survivor's copy")
	}

	// A peer that flaps does not start a sweep each time.
	survivor.recordResidue(sweepRound{generation: survivor.sweepGeneration.Load(), epoch: survivor.Epoch()}, []heldBucket{{database: testDatabase, bucket: bucket}}, nil)
	rearmed := survivor.sweepRearms.Load()
	survivor.rearmSweep()
	survivor.rearmSweep()
	if got := survivor.sweepRearms.Load() - rearmed; got > 1 {
		t.Fatalf("two events in a row started %d sweeps", got)
	}
}
