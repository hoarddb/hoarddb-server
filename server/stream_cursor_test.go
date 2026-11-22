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

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 5 of the replication stream: retention over every member's cursor,
// and the advance notice that keeps an idle stream from holding the log
// (spec §19.3.9, §19.3.10).

// withoutRetentionLoop turns the timed retention pass off, so the passes a
// test makes with pruneWAL are the only ones.
func withoutRetentionLoop(_ int, cfg *config.Config) {
	cfg.Server.WALRetentionSeconds = 0
}

// smallSegments makes a node's log rotate every few entries, so a retention
// pass has whole segments to remove without writing 64 MB. A restart opens the
// log at its default size again.
func smallSegments(node *testNode) {
	node.server.core.wal.SetMaxSegmentBytes(256)
}

// waitListsDatabase polls until node's own catalog does or does not list the
// database.
func waitListsDatabase(t *testing.T, node *testNode, name string, want bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for listsDatabase(t, node, name) != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s lists database %q: %v, want %v", node.addr, name, !want, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// streamTo is the peer object of from's stream to the node at index.
func streamTo(t *testing.T, tc *testCluster, from, to int) *peer {
	t.Helper()
	cluster := tc.nodes[from].server.core.cluster
	return cluster.peerFor(mustNode(t, cluster, tc.addrs[to]))
}

// caughtUpWithin waits until p's stream has replayed the log to its peer and
// the peer has acknowledged it.
func caughtUpWithin(t *testing.T, p *peer, wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := catchUpPeer(ctx, p); err != nil {
		t.Fatalf("the stream to %s did not catch up: %v", p.node.ID, err)
	}
}

// advanceEveryStream brings the cursor of every stream of c to the end of c's
// synced log, for a test whose subject is not retention's bound but needs a
// retention pass that can truncate: with the bound over every member, an idle
// member holds the log until its cursor is advanced. It asks for the notice as
// a retention pass does, and waits until every stream has received it.
func advanceEveryStream(t *testing.T, c *Cluster) {
	t.Helper()
	tail := c.s.wal.SyncedSequence()
	for _, member := range c.otherMembers() {
		p := c.peerFor(member)
		caughtUpWithin(t, p, 20*time.Second)
		deadline := time.Now().Add(20 * time.Second)
		for p.ackedThrough(c.self.ID) < tail {
			if time.Now().After(deadline) {
				t.Fatalf("the stream to %s was not advanced to %d: its cursor is %d", p.node.ID, tail, p.ackedThrough(c.self.ID))
			}
			c.askForAdvances()
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestRetentionHoldsTheLogForAMemberThatIsDown is T-95. Retention's bound is
// the lowest cursor of every member, not of the members that host a bucket
// this node owns: at factor 1 no member hosts anything of another's, and a
// member that is down when a database is created must still find that entry in
// the log when it returns.
func TestRetentionHoldsTheLogForAMemberThatIsDown(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, withoutRetentionLoop)
	defer tc.stop()
	const owner, member = 0, 1

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bulk := bucketOwnedBy(t, tc, owner, "bulk")
	createBucketAt(tc, owner, testDatabase, bulk, 0)
	toMember := streamTo(t, tc, owner, member)
	caughtUpWithin(t, toMember, 20*time.Second)
	cursor := toMember.ackedThrough(tc.nodes[owner].server.core.cluster.self.ID)
	startMember := stopNode(t, tc, member)

	smallSegments(tc.nodes[owner])
	for i := 0; i < 20; i++ {
		putDocument(tc, owner, testDatabase, bulk, fmt.Sprintf("k%02d", i))
	}
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: "whiledown"})
	ownerCore := tc.nodes[owner].server.core
	if err := ownerCore.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	first, err := ownerCore.wal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	if first > cursor+1 {
		t.Fatalf("a retention pass pruned the log to sequence %d with a member down whose cursor is %d: "+
			"what it missed is no longer in the log", first, cursor)
	}

	returned := startMember()
	waitListsDatabase(t, returned, "whiledown", true)
}

// TestABucketCreatedInADatabaseCreatedWhileThePeerWasDownArrivesAfterIt: a
// database and a bucket in it are created while a member is down, through a
// node that owns no bucket, and that node's log is pruned before the member
// returns. The member gets the database from the coordinator's log and the
// bucket from its owner's, and holds both.
func TestABucketCreatedInADatabaseCreatedWhileThePeerWasDownArrivesAfterIt(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, withoutRetentionLoop)
	defer tc.stop()
	const coordinator, owner, member = 0, 1, 2
	const database = "late"

	startMember := stopNode(t, tc, member)
	smallSegments(tc.nodes[coordinator])
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	bucket := bucketOwnedIn(t, tc, owner, database, "inlate")
	createBucketAt(tc, coordinator, database, bucket, 0)
	for i := 0; i < 4; i++ {
		tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: fmt.Sprintf("pad%02d", i)})
	}
	if err := tc.nodes[coordinator].server.core.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass on the coordinator: %v", err)
	}

	returned := startMember()
	waitListsDatabase(t, returned, database, true)
	deadline := time.Now().Add(20 * time.Second)
	for !returned.server.core.bucketStoreOpen(database, bucket) {
		if time.Now().After(deadline) {
			t.Fatalf("the member that was down does not serve %s.%s", database, bucket)
		}
		time.Sleep(50 * time.Millisecond)
	}
	putDocumentAt(tc, owner, database, bucket, "after", "all")
	if !holdsKey(t, returned.server.core, database, bucket, "after") {
		t.Fatalf("the member that was down does not hold a write at all to %s.%s", database, bucket)
	}
}

// TestAPeerThatHoldsNoneOfThisNodesBucketsReceivesDDLAndIsNeverBehindTheHorizon
// is the last criterion for peer horizons: five nodes at factor 3 and one bucket, so two
// members hold nothing of its owner's. 200 writes, a retention pass and a
// restart of the owner later, a `create database` through it reaches such a
// member, and no horizon refusal is logged for it.
func TestAPeerThatHoldsNoneOfThisNodesBucketsReceivesDDLAndIsNeverBehindTheHorizon(t *testing.T) {
	var logged bytes.Buffer
	var mu sync.Mutex
	clusterTestLogger.Store(slog.New(slog.NewTextHandler(syncWriter{mu: &mu, buffer: &logged}, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer clusterTestLogger.Store(nil)

	tc := newTestClusterWith(t, 5, 3, withoutRetentionLoop)
	defer tc.stop()
	const owner = 0

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, owner, "only")
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	hosts := mustReplicaIDs(t, tc.nodes[owner].server.core.cluster, testDatabase, bucket)
	outsider := -1
	for index, addr := range tc.addrs {
		if !strings.Contains(strings.Join(hosts, " "), addr) {
			outsider = index
			break
		}
	}
	if outsider < 0 {
		t.Fatalf("setup: every member hosts %s (%v)", bucket, hosts)
	}
	caughtUpWithin(t, streamTo(t, tc, owner, outsider), 20*time.Second)

	smallSegments(tc.nodes[owner])
	for i := 0; i < 200; i++ {
		putDocument(tc, owner, testDatabase, bucket, fmt.Sprintf("k%03d", i))
	}
	if err := tc.nodes[owner].server.core.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	tc.restartNode(owner)

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: "afterwards"})
	waitListsDatabase(t, tc.nodes[outsider], "afterwards", true)
	caughtUpWithin(t, streamTo(t, tc, owner, outsider), 20*time.Second)

	mu.Lock()
	defer mu.Unlock()
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, horizonRefusal) {
			t.Fatalf("a member that holds none of the owner's buckets was refused below the horizon: %s", line)
		}
	}
}

// TestACreateThatPlacesABucketOnAPeerWithNoneOfTheOwnersBucketsDoesNotWedgeItAfterAStall
// covers a stalled peer. At factor 1 a member hosts nothing of the owner's; its stream
// stalls; the owner writes and prunes; then a create declared at 3 places a
// bucket on it. When the member answers again the owner's log must still hold
// everything from its cursor on: dispatch 4 deleted the start-at-create record
// and retention ignored the member, so it was refused below the horizon for
// good ("needs this node's log from sequence 2 and the log starts at 8").
func TestACreateThatPlacesABucketOnAPeerWithNoneOfTheOwnersBucketsDoesNotWedgeItAfterAStall(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, withoutRetentionLoop)
	defer tc.stop()
	const owner, member = 0, 2

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bulk := bucketOwnedBy(t, tc, owner, "bulk")
	createBucketAt(tc, owner, testDatabase, bulk, 0)
	toMember := streamTo(t, tc, owner, member)
	caughtUpWithin(t, toMember, 20*time.Second)
	unfreeze := freezePeer(toMember)
	defer unfreeze()

	smallSegments(tc.nodes[owner])
	for i := 0; i < 20; i++ {
		putDocument(tc, owner, testDatabase, bulk, fmt.Sprintf("k%02d", i))
	}
	if err := tc.nodes[owner].server.core.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	wide := bucketOwnedBy(t, tc, owner, "wide")
	createBucketAt(tc, owner, testDatabase, wide, 3)
	// The create is on the member's queue; its ship times out on the frozen
	// connection and the stream stalls, so what the member gets next comes
	// from the log.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waitForStream(ctx, toMember, func() bool {
		state, _ := toMember.streamStatus()
		return state == streamStalled
	}); err != nil {
		t.Fatalf("setup: the frozen stream did not stall: %v", err)
	}
	unfreeze()

	caughtUpWithin(t, toMember, 30*time.Second)
	waitForServing(t, tc, testDatabase, wide, 3)
	putDocumentAt(tc, owner, testDatabase, wide, "after", "all")
	waitForLocalCopies(t, tc, testDatabase, wide, "after", 3)
}

// putConcurrently writes n keys to bucket in testDatabase through the node at
// index, from workers connections at once.
func putConcurrently(t *testing.T, tc *testCluster, index int, bucket, prefix string, n, workers int) {
	t.Helper()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		client := tc.dial(index)
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			defer func() { _ = client.Close() }()
			for i := worker; i < n; i += workers {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				frame, err := client.SendRequest(ctx, transport.OpPut, mustMarshalPut(t, bucket, fmt.Sprintf("%s%05d", prefix, i)))
				cancel()
				if err != nil || frame.Opcode == transport.OpError {
					t.Errorf("put %s%05d: %v %s", prefix, i, err, frameText(frame))
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

// frameText is a response's payload as text, or nothing for no response.
func frameText(frame *transport.Frame) string {
	if frame == nil {
		return ""
	}
	return string(frame.Payload)
}

// waitCursorAtLeast polls until this node's cursor for p's stream reaches
// want, and reports whether it did within wait.
func waitCursorAtLeast(p *peer, want uint64, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for p.ackedThrough(p.c.self.ID) < want {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// TestAnIdlePeerDoesNotHoldTheLog is T-93. Three nodes; a bucket at factor 2
// takes 5,000 writes, and the third member is addressed none of them. With the
// bound over every member, that member's cursor would sit at the last entry it
// was sent and hold the log for ever; the retention pass's advance notice moves
// it, so after two passes the owner's log no longer starts at or below it.
func TestAnIdlePeerDoesNotHoldTheLog(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, withoutRetentionLoop)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, testBucket, 2)
	waitForServing(t, tc, testDatabase, testBucket, 2)
	owner := ownerIndexOf(t, tc, testBucket)
	hosts := strings.Join(mustReplicaIDs(t, tc.nodes[owner].server.core.cluster, testDatabase, testBucket), " ")
	idle := -1
	for index, addr := range tc.addrs {
		if !strings.Contains(hosts, addr) {
			idle = index
		}
	}
	if idle < 0 {
		t.Fatalf("setup: every member hosts %s (%v)", testBucket, hosts)
	}
	// The last entry the owner sends the idle member: a database create,
	// which is addressed to every member.
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: "marker"})
	toIdle := streamTo(t, tc, owner, idle)
	caughtUpWithin(t, toIdle, 20*time.Second)
	ownerCore := tc.nodes[owner].server.core
	idleLast := toIdle.ackedThrough(ownerCore.cluster.self.ID)
	if idleLast == 0 {
		t.Fatal("setup: the idle member acknowledged nothing of the owner's")
	}

	ownerCore.wal.SetMaxSegmentBytes(64 << 10)
	putConcurrently(t, tc, owner, testBucket, "k", 5000, 8)

	if err := ownerCore.cluster.pruneWAL(); err != nil {
		t.Fatalf("first retention pass: %v", err)
	}
	// The notice is sent by the stream's sender, after the pass that asked for
	// it read its bound; the second pass is the one that can prune.
	waitCursorAtLeast(toIdle, idleLast+1, 10*time.Second)
	if err := ownerCore.cluster.pruneWAL(); err != nil {
		t.Fatalf("second retention pass: %v", err)
	}
	first, err := ownerCore.wal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	if first <= idleLast+1 {
		t.Fatalf("after two retention passes the owner's log starts at %d, not past sequence %d, the last entry sent to a member "+
			"addressed nothing since (its cursor is %d, the log ends at %d): an idle stream holds the log",
			first, idleLast, toIdle.ackedThrough(ownerCore.cluster.self.ID), ownerCore.wal.LastSequence())
	}
}

// TestAnAdvanceNeverPassesAnEntryThatWasNotDelivered is T-94 (A-5.1). A writer
// is held after its local apply and before it routes its entry, so the entry
// is in the log, synced, and on no queue. A retention pass asks the replica's
// stream to advance: the notice must stop below the held entry, or the replica
// records a position past it and skips it when it arrives.
func TestAnAdvanceNeverPassesAnEntryThatWasNotDelivered(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, withoutRetentionLoop)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, testBucket, 2)
	waitForServing(t, tc, testDatabase, testBucket, 2)
	owner := ownerIndexOf(t, tc, testBucket)
	solo := bucketOwnedBy(t, tc, owner, "solo")
	createBucketAt(tc, owner, testDatabase, solo, 1)
	ownerCluster := tc.nodes[owner].server.core.cluster
	replicaID := ""
	for _, id := range mustReplicaIDs(t, ownerCluster, testDatabase, testBucket) {
		if id != ownerCluster.self.ID {
			replicaID = id
		}
	}
	replica := nodeIndexByAddr(t, tc.addrs, replicaID)
	toReplica := streamTo(t, tc, owner, replica)
	putDocument(tc, owner, testDatabase, testBucket, "before")
	// Entries the replica is not addressed, so a notice has something to
	// pass over below the held entry.
	for i := 0; i < 3; i++ {
		putDocument(tc, owner, testDatabase, solo, fmt.Sprintf("s%d", i))
	}
	caughtUpWithin(t, toReplica, 20*time.Second)

	reached := make(chan uint64, 1)
	release := make(chan struct{})
	hold := func(entry ReplEntry) {
		if entry.Bucket == testBucket && bytes.Contains(entry.Key, []byte("held")) {
			reached <- entry.Seq
			<-release
		}
	}
	replicaEnqueueHook.Store(&hold)
	defer replicaEnqueueHook.Store(nil)
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	written := make(chan *transport.Frame, 1)
	go func() {
		client := tc.dial(owner)
		defer func() { _ = client.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		frame, err := client.SendRequest(ctx, transport.OpPut, mustMarshalPut(t, testBucket, "held"))
		if err != nil {
			t.Errorf("the held write: %v", err)
		}
		written <- frame
	}()
	var heldSeq uint64
	select {
	case heldSeq = <-reached:
	case <-time.After(20 * time.Second):
		t.Fatal("setup: the held write never reached the hook")
	}

	if err := ownerCluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	if !waitCursorAtLeast(toReplica, heldSeq-1, 10*time.Second) {
		t.Fatalf("the retention pass sent the replica no advance notice: its cursor is %d, the entries up to %d are acknowledged or not addressed to it",
			toReplica.ackedThrough(ownerCluster.self.ID), heldSeq-1)
	}
	released = true
	close(release)
	select {
	case frame := <-written:
		if frame != nil && frame.Opcode == transport.OpError {
			t.Fatalf("the held write: %s", frame.Payload)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the held write never answered")
	}
	waitForLocalCopies(t, tc, testDatabase, testBucket, "held", 2)
}

// TestAnAdvanceIsDurableBeforeItIsAcknowledged is T-96. An idle member's cursor
// is advanced; the member is killed (no clean-stop checkpoint) and the owner's
// log is pruned past the last entry the member holds. Restarted, the member
// must report the advanced position — which it learned from no log entry — or
// it is behind the horizon for good.
func TestAnAdvanceIsDurableBeforeItIsAcknowledged(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, withoutRetentionLoop)
	defer tc.stop()
	const owner, idle = 0, 1

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bulk := bucketOwnedBy(t, tc, owner, "bulk")
	createBucketAt(tc, owner, testDatabase, bulk, 0)
	toIdle := streamTo(t, tc, owner, idle)
	caughtUpWithin(t, toIdle, 20*time.Second)
	ownerCore := tc.nodes[owner].server.core
	idleLast := toIdle.ackedThrough(ownerCore.cluster.self.ID)

	smallSegments(tc.nodes[owner])
	for i := 0; i < 20; i++ {
		putDocument(tc, owner, testDatabase, bulk, fmt.Sprintf("k%02d", i))
	}
	tail := ownerCore.wal.SyncedSequence()
	if err := ownerCore.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	if !waitCursorAtLeast(toIdle, tail, 10*time.Second) {
		t.Fatalf("the retention pass sent the idle member no advance notice: its cursor is %d, the log is synced to %d",
			toIdle.ackedThrough(ownerCore.cluster.self.ID), tail)
	}
	advanced := toIdle.ackedThrough(ownerCore.cluster.self.ID)

	tc.nodes[idle].server.core.cluster.skipStopCheckpoint = true
	startIdle := stopNode(t, tc, idle)
	for i := 0; i < 2; i++ {
		if err := ownerCore.cluster.pruneWAL(); err != nil {
			t.Fatalf("retention pass with the member down: %v", err)
		}
	}
	if first, err := ownerCore.wal.FirstSequence(); err != nil || first <= idleLast+1 {
		t.Fatalf("setup: the owner's log starts at %d (%v), not past %d, the last entry the member holds", first, err, idleLast)
	}

	returned := startIdle()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := replayToPeer(ctx, toIdle); err != nil {
		t.Fatalf("the stream to the restarted member did not open and replay: %v", err)
	}
	if reported := returned.server.core.cluster.recordedPosition(ownerCore.cluster.self.ID).AppliedThrough; reported < advanced {
		t.Fatalf("the restarted member reports position %d for the owner, below the %d it acknowledged as advanced", reported, advanced)
	}
}

// TestAJoinerDoesNotLearnAnEmptyDatabasePrunedFromEveryLog is SA-5.2 (spec
// §19.3.10's residual, Q29), written to show what happens, not to fix it. A
// database with no bucket is created, every member confirms it, and the log
// that holds it is pruned past it. A node that joins afterwards has nothing
// recorded for that log, so its stream starts at the horizon, and no bucket of
// the database exists for it to learn the database from by copying one. It
// does not list the database. If this test fails, the residual is gone and the
// sentence on the "limitations" user-docs page must go with it.
func TestAJoinerDoesNotLearnAnEmptyDatabasePrunedFromEveryLog(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) {
		cfg.Server.WALRetentionSeconds = 0
		cfg.Server.AutoRebalance = false
	})
	defer tc.stop()
	const coordinator = 0

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bulk := bucketOwnedBy(t, tc, coordinator, "bulk")
	createBucketAt(tc, coordinator, testDatabase, bulk, 0)
	smallSegments(tc.nodes[coordinator])
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: "empty"})
	for _, node := range tc.nodes {
		waitListsDatabase(t, node, "empty", true)
	}
	for i := 0; i < 10; i++ {
		putDocument(tc, coordinator, testDatabase, bulk, fmt.Sprintf("k%02d", i))
	}
	coordinatorCore := tc.nodes[coordinator].server.core
	emptyAt := uint64(0)
	entries, err := coordinatorCore.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the coordinator's log: %v", err)
	}
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpCreateDB && bytes.Contains(entry.Data, []byte("empty")) {
			emptyAt = entry.Seq
		}
	}
	advanceEveryStream(t, coordinatorCore.cluster)
	if err := coordinatorCore.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	if first, err := coordinatorCore.wal.FirstSequence(); err != nil || first <= emptyAt {
		t.Fatalf("setup: the coordinator's log starts at %d (%v), not past the create of the empty database at %d", first, err, emptyAt)
	}

	joiner := joinLiveReplica(t, tc, reserveNodePorts(t), nil)
	// The joiner may be placed on the coordinator's bucket; it copies it
	// first, as the sweep would, and learns testDatabase with it.
	if strings.Contains(strings.Join(mustReplicaIDs(t, joiner.server.core.cluster, testDatabase, bulk), " "), joiner.addr) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := joiner.server.core.cluster.ensureMigrated(ctx, testDatabase, bulk); err != nil {
			t.Fatalf("copying %s onto the joiner: %v", bulk, err)
		}
	}
	caughtUpWithin(t, coordinatorCore.cluster.peerFor(mustNode(t, coordinatorCore.cluster, joiner.addr)), 30*time.Second)
	if listsDatabase(t, joiner, "empty") {
		t.Fatal("the joiner lists the empty database: Q29's residual is gone; remove the sentence from docs/user/limitations.md")
	}
}
