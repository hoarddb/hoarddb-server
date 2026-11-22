package server

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The member's pending-copy worker (bucket-replication-rev8-spec §7.5; the G4
// to G7 build doc step 1). A member an increase newly places, which holds
// nothing of the bucket, keeps a needs-a-copy record; with auto-rebalance on,
// its worker pulls the bucket from the owner by itself, keeps the record on a
// failed pull and backs off per bucket, and says once an hour per bucket why a
// copy refused for the disk waits.

// waitForPendingCopyLanded waits until node index's worker has landed its copy
// of bucket: the bucket served there and its needs-a-copy record gone.
func waitForPendingCopyLanded(t *testing.T, tc *testCluster, index int, bucket string) {
	t.Helper()
	core := tc.nodes[index].server.core
	landed := func() bool {
		if !core.bucketStoreOpen(testDatabase, bucket) {
			return false
		}
		_, recorded := needsCopyOnDisk(t, core, bucket)
		return !recorded
	}
	if !holdsWithin(30*time.Second, landed) {
		_, recorded := needsCopyOnDisk(t, core, bucket)
		t.Fatalf("node %d's pending-copy worker never landed its copy of %q (served %v, needs-a-copy record %v)",
			index+1, bucket, core.bucketStoreOpen(testDatabase, bucket), recorded)
	}
}

// injectPendingCopyPulls makes every pending-copy worker pull of a bucket for
// which refusal returns an error fail with it, before anything is asked of the
// source, and counts the attempts per node and bucket.
func injectPendingCopyPulls(t *testing.T, refusal func(nodeID, bucket string) error) *pullAttempts {
	t.Helper()
	attempts := &pullAttempts{counts: make(map[string]int)}
	hook := func(nodeID, _, bucket string) error {
		err := refusal(nodeID, bucket)
		if err != nil {
			attempts.add(nodeID, bucket)
		}
		return err
	}
	pendingCopyPullHook.Store(&hook)
	t.Cleanup(func() { pendingCopyPullHook.Store(nil) })
	return attempts
}

// pullAttempts counts the refused pulls injectPendingCopyPulls saw.
type pullAttempts struct {
	mu     sync.Mutex
	counts map[string]int
}

func (a *pullAttempts) add(nodeID, bucket string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts[nodeID+"/"+bucket]++
}

func (a *pullAttempts) of(nodeID, bucket string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.counts[nodeID+"/"+bucket]
}

// captureClusterLogs sends the log of every node started afterwards, at level
// and above, to the returned buffer.
func captureClusterLogs(t *testing.T, level slog.Level) func() string {
	t.Helper()
	var mu sync.Mutex
	var logged bytes.Buffer
	previous := clusterTestLogger.Swap(slog.New(slog.NewTextHandler(syncWriter{mu: &mu, buffer: &logged}, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { clusterTestLogger.Store(previous) })
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return logged.String()
	}
}

// linesWith is the lines of logged that contain every one of parts.
func linesWith(logged string, parts ...string) []string {
	var lines []string
	for _, line := range strings.Split(logged, "\n") {
		matches := line != ""
		for _, part := range parts {
			matches = matches && strings.Contains(line, part)
		}
		if matches {
			lines = append(lines, line)
		}
	}
	return lines
}

// rebalancingPendingOn is three nodes at default 1 with auto-rebalance on
// everywhere and count buckets owned by node 1 created at 2: once each is
// raised to 3 its copy is pending on member alone.
func rebalancingPendingOn(t *testing.T, count int, prefix string) (tc *testCluster, buckets []string, member int) {
	t.Helper()
	tc = newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = true })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	buckets, member = pendingOn(t, tc, count, prefix)
	for _, bucket := range buckets {
		createBucketAt(tc, 0, testDatabase, bucket, 2)
		waitForServing(t, tc, testDatabase, bucket, 2)
	}
	return tc, buckets, member
}

// raiseToThree raises bucket, created at 2, to 3, and fails unless only node
// member is newly placed.
func raiseToThree(t *testing.T, tc *testCluster, bucket string, member int) {
	t.Helper()
	answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if len(answer.Pending) != 1 || answer.Pending[0] != tc.addrs[member] {
		t.Fatalf("the increase of %q names %v as pending, want only node %d", bucket, answer.Pending, member+1)
	}
}

// TestAFailedPullStaysPendingAndADroppedBucketEndsIt is T-54's first half
// (spec §14.4): every pull the workers make of the increased bucket fails at
// the source. Each new member keeps its needs-a-copy record and its queue
// entry, whose last outcome names the failure, and tries again; the bucket's
// drop then deletes the record, and the queue entry with it.
func TestAFailedPullStaysPendingAndADroppedBucketEndsIt(t *testing.T) {
	refusal := errors.New("the source refused the pull for this test")
	attempts := injectPendingCopyPulls(t, func(string, string) error { return refusal })
	tc, bucket, _, epoch, generation := increasedOnEmptyMembersWith(t, BucketTypeHash, true)
	defer tc.stop()
	key := placementOf(testDatabase, bucket)
	for member := 1; member <= 2; member++ {
		core := tc.nodes[member].server.core
		pollUntil(t, fmt.Sprintf("node %d's worker tries its pull of %q twice", member+1, bucket), func() bool {
			return attempts.of(core.cluster.self.ID, bucket) >= 2
		})
		requireNeedsCopyOf(t, core, bucket, 3, epoch, generation)
		outcome, pending := core.cluster.pendingCopyOf(key)
		if !pending {
			t.Fatalf("node %d's worker dropped %q from its queue after a failed pull", member+1, bucket)
		}
		if !strings.Contains(outcome.reason, refusal.Error()) || outcome.lastAttempt.IsZero() {
			t.Errorf("node %d's last outcome for %q is %+v, want the attempt's time and its reason %q", member+1, bucket, outcome, refusal)
		}
		if core.bucketStoreOpen(testDatabase, bucket) {
			t.Errorf("node %d serves %q although every pull of it failed", member+1, bucket)
		}
	}
	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	waitForBarrier(t, tc, 0, "after-the-drop", 1, 2)
	for member := 1; member <= 2; member++ {
		core := tc.nodes[member].server.core
		requireNoNeedsCopy(t, core, bucket, "after the bucket's drop")
		if outcome, pending := core.cluster.pendingCopyOf(key); pending {
			t.Errorf("after the bucket's drop node %d's worker still has %q queued: %+v", member+1, bucket, outcome)
		}
	}
}

// pruneToTheTail prunes a stopped node's open log as retention does once its
// entries' effects are synced: it rotates to a fresh segment behind one
// padding entry, a database create, and drops every segment before it. A
// replay at start then applies none of what it held.
func pruneToTheTail(t *testing.T, wal *storage.WAL) {
	t.Helper()
	wal.SetMaxSegmentBytes(1)
	pad, err := bson.Marshal(CreateDatabaseRequest{Name: "pad"})
	if err != nil {
		t.Fatalf("encoding the padding entry: %v", err)
	}
	if _, err := wal.Append("", uint8(transport.OpCreateDB), nil, pad); err != nil {
		t.Fatalf("appending the padding entry: %v", err)
	}
	if err := wal.Truncate(wal.LastSequence()); err != nil {
		t.Fatalf("pruning the stopped node's WAL: %v", err)
	}
}

// pruneLogPastTheAlter prunes the log of a stopped node, in dataDir, past its
// alter of bucket (pruneToTheTail). A replay at start then cannot apply the
// alter again, and only the node's needs-a-copy record says the copy is
// pending.
func pruneLogPastTheAlter(t *testing.T, dataDir, bucket string) {
	t.Helper()
	wal, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("opening the stopped node's WAL: %v", err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Fatalf("closing the stopped node's WAL: %v", err)
		}
	}()
	if walOpCount(t, wal, transport.OpAlterBucket, bucket) == 0 {
		t.Fatalf("setup: the stopped node logged no alter of %q", bucket)
	}
	pruneToTheTail(t, wal)
	if walOpCount(t, wal, transport.OpAlterBucket, bucket) != 0 {
		t.Fatalf("setup: the stopped node's WAL still holds its alter of %q after the prune", bucket)
	}
}

// TestARestartInsideTheWindowKeepsThePendingCopyAndFinishesIt is T-36. The
// worker's pull is held after the owner's snapshot, before the copy lands, and
// the member is stopped there; the stop's landing fails at its catalog commit,
// as a node killed before that commit lands nothing. Its log is pruned past
// the alter, so what carries the pending copy across the restart is the
// needs-a-copy record alone. Started again with auto-rebalance on, the
// member's worker finds the record by its start-up scan and lands the copy,
// and every acknowledged write is read from the member's own storage.
func TestARestartInsideTheWindowKeepsThePendingCopyAndFinishesIt(t *testing.T) {
	tc, buckets, member := rebalancingPendingOn(t, 1, "window")
	defer tc.stop()
	bucket := buckets[0]
	keys := []string{writeOneTo(t, tc, 0, bucket, BucketTypeHash, 0), writeOneTo(t, tc, 0, bucket, BucketTypeHash, 1)}
	held, release := holdBucketAt(t, &beforeMaterializeHook, bucket)
	raiseToThree(t, tc, bucket, member)
	awaitHeld(t, held, fmt.Sprintf("node %d's worker pull of %q", member+1, bucket))

	node := tc.nodes[member]
	if node.client != nil {
		_ = node.client.Close()
		node.client = nil
	}
	stopping := node.server.core.cluster
	stopped := make(chan error, 1)
	go func() { stopped <- node.server.Stop() }()
	pollUntil(t, fmt.Sprintf("node %d's stop cancels its cluster", member+1), func() bool { return stopping.ctx.Err() != nil })
	killed := func(uint64) error { return errors.New("the node was killed before its catalog commit") }
	materializeCatalogHook.Store(&killed)
	release()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stopping node %d: %v", member+1, err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("node %d never stopped", member+1)
	}
	materializeCatalogHook.Store(nil)
	if node.server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Fatalf("setup: node %d landed %q before it stopped", member+1, bucket)
	}
	node.server = nil
	pruneLogPastTheAlter(t, node.cfg.Storage.DataDir, bucket)

	restarted := tc.startNode(member, filepath.Dir(node.cfg.Storage.DataDir), tc.token)
	tc.nodes[member] = restarted
	waitForPendingCopyLanded(t, tc, member, bucket)
	keys = append(keys, writeOneTo(t, tc, 0, bucket, BucketTypeHash, 2))
	waitForBarrier(t, tc, 0, "after-the-restart", member)
	requireHeldLocally(t, restarted.server.core, bucket, keys)
}

// TestAnIncreaseOntoAFullMemberWaitsAndSaysWhy is T-175's first half (spec
// §7.5, owner Q7): an increase onto a member above the disk high-water mark is
// accepted; the member's worker keeps the record, and says once, not on every
// retry, that the copy waits for the disk, with the remedy; the owner's other
// bucket keeps the member in sync meanwhile; once the disk is freed the copy
// lands.
func TestAnIncreaseOntoAFullMemberWaitsAndSaysWhy(t *testing.T) {
	logged := captureClusterLogs(t, slog.LevelWarn)
	restoreCeiling := pendingCopyRetryCeiling.get()
	pendingCopyRetryCeiling.set(2 * time.Second)
	t.Cleanup(func() { pendingCopyRetryCeiling.set(restoreCeiling) })
	tc, buckets, member := rebalancingPendingOn(t, 1, "full")
	defer tc.stop()
	bucket := buckets[0]
	third := bucketOwnedBy(t, tc, 0, "third")
	createBucketAt(tc, 0, testDatabase, third, 3)
	waitForServing(t, tc, testDatabase, third, 3)
	core := tc.nodes[member].server.core
	setCapacityProbes(t, core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) })

	raiseToThree(t, tc, bucket, member)
	remedy := fmt.Sprintf("migrate %s %s", testDatabase, bucket)
	waiting := func() []string {
		return linesWith(logged(), "node_id="+core.cluster.self.ID, "bucket="+bucket, "pending copy", remedy)
	}
	pollUntil(t, fmt.Sprintf("node %d says why its copy of %q waits, with the remedy", member+1, bucket), func() bool { return len(waiting()) > 0 })
	for round := 0; round < 3; round++ {
		requireWriteReachesEveryCopy(t, tc, 0, third, fmt.Sprintf("full-%d", round), 3)
	}
	// Two retries at the shortened ceiling: the line is the bucket's for the
	// hour, not the retry's.
	time.Sleep(5 * time.Second)
	if lines := waiting(); len(lines) != 1 {
		t.Errorf("node %d logged %d lines saying its copy of %q waits for the disk, want 1 an hour:\n%s", member+1, len(lines), bucket, strings.Join(lines, "\n"))
	}
	if _, recorded := needsCopyOnDisk(t, core, bucket); !recorded || core.bucketStoreOpen(testDatabase, bucket) {
		t.Fatalf("node %d, above the mark, holds %q (%v) or lost its needs-a-copy record (%v)", member+1, bucket,
			core.bucketStoreOpen(testDatabase, bucket), recorded)
	}

	setCapacityProbes(t, core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	waitForPendingCopyLanded(t, tc, member, bucket)
}

// TestTheWorkerPullsOnlyFromTheOwner is the worker's choice of source (spec
// §7.5 step 2; the build doc's fail-silently 5): the holder that kept the
// bucket at 2 has not applied the increase, its stream from the owner frozen,
// so it places the bucket at 2 and answers the new member NotPlaced. The
// member's worker pulls from the owner, never from a holder, and lands the
// copy at the new factor.
func TestTheWorkerPullsOnlyFromTheOwner(t *testing.T) {
	tc, buckets, member := rebalancingPendingOn(t, 1, "source")
	defer tc.stop()
	bucket := buckets[0]
	key := placementOf(testDatabase, bucket)
	walk := mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, 3)
	holder := nodeIndexByAddr(t, tc.addrs, walk[1])
	owner := clusterOf(tc, 0)
	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[holder])))
	defer unfreeze()

	raiseToThree(t, tc, bucket, member)
	waitForPendingCopyLanded(t, tc, member, bucket)
	if factor := clusterOf(tc, holder).effectiveRF(key); factor != 2 {
		t.Fatalf("setup: node %d, the holder whose stream is frozen, places %q at %d, want 2: it applied the increase", holder+1, bucket, factor)
	}
	if factor := clusterOf(tc, member).effectiveRF(key); factor != 3 {
		t.Errorf("node %d's landed copy of %q is at factor %d, want the owner's 3", member+1, bucket, factor)
	}
}

// TestAStuckBucketDoesNotHoldUpAnother is the back-off's being per bucket
// (spec §7.5; fail-silently 4): one bucket's every pull is refused for the
// disk, so it waits at the ceiling; a second bucket raised meanwhile lands at
// once, and the first is still pending.
func TestAStuckBucketDoesNotHoldUpAnother(t *testing.T) {
	var stuck string
	var stuckMu sync.Mutex
	attempts := injectPendingCopyPulls(t, func(_, bucket string) error {
		stuckMu.Lock()
		defer stuckMu.Unlock()
		if bucket == stuck {
			return fmt.Errorf("%w: refused for this test", ErrMemberDiskFull)
		}
		return nil
	})
	tc, buckets, member := rebalancingPendingOn(t, 2, "stuck")
	defer tc.stop()
	stuckMu.Lock()
	stuck = buckets[0]
	stuckMu.Unlock()
	core := tc.nodes[member].server.core

	raiseToThree(t, tc, buckets[0], member)
	pollUntil(t, fmt.Sprintf("node %d's worker is refused %q for the disk", member+1, buckets[0]), func() bool {
		return attempts.of(core.cluster.self.ID, buckets[0]) >= 1
	})
	raiseToThree(t, tc, buckets[1], member)
	if !holdsWithin(10*time.Second, func() bool { return core.bucketStoreOpen(testDatabase, buckets[1]) }) {
		t.Fatalf("node %d's copy of %q waited behind %q, which its disk refuses", member+1, buckets[1], buckets[0])
	}
	waitForPendingCopyLanded(t, tc, member, buckets[1])
	if _, pending := core.cluster.pendingCopyOf(placementOf(testDatabase, buckets[0])); !pending || core.bucketStoreOpen(testDatabase, buckets[0]) {
		t.Errorf("node %d's refused copy of %q is no longer pending (served %v)", member+1, buckets[0], core.bucketStoreOpen(testDatabase, buckets[0]))
	}
}

// pendingCopiesStallNothingThroughTheWorker is T-39's worker half (OQ-G3-5):
// with auto-rebalance on, each pending bucket is pulled by the member's own
// worker once its alter is applied, held after its snapshot and before it
// lands. A write of the bucket in that window is routed to the member, which
// refuses it, so the third bucket loses the member from its in-sync set while
// the pull runs (spec §7.3, the documented limit), and regains it once the
// copy has landed.
func pendingCopiesStallNothingThroughTheWorker(t *testing.T) {
	tc, pending, member := rebalancingPendingOn(t, 2, "pending")
	defer tc.stop()
	third := bucketOwnedBy(t, tc, 0, "third")
	createBucketAt(tc, 0, testDatabase, third, 3)
	waitForServing(t, tc, testDatabase, third, 3)
	for _, bucket := range pending {
		held, release := holdBucketAt(t, &beforeMaterializeHook, bucket)
		raiseToThree(t, tc, bucket, member)
		awaitHeld(t, held, fmt.Sprintf("node %d's worker pull of %q", member+1, bucket))
		putDocument(tc, 0, testDatabase, bucket, "during-the-pull")
		probe := 0
		confirmsOn := func(want int32) func() bool {
			return func() bool {
				probe++
				return writeConfirms(t, tc, 0, third, fmt.Sprintf("probe-%s-%03d", bucket, probe)) == want
			}
		}
		if !holdsWithin(10*time.Second, confirmsOn(2)) {
			t.Fatalf("node %d never left the in-sync set of %q while its worker's pull of %q ran", member+1, third, bucket)
		}
		release()
		waitForPendingCopyLanded(t, tc, member, bucket)
		if !holdsWithin(20*time.Second, confirmsOn(3)) {
			t.Fatalf("node %d never rejoined the in-sync set of %q once its worker landed %q", member+1, third, bucket)
		}
	}
}
