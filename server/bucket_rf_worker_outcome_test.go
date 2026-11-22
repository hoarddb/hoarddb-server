package server

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// What the member says of its pending copies and what the owner lists
// (bucket-replication-rev8-spec §7.5; the G4 to G7 build doc step 3): the
// last outcome per pending bucket, the line that lists what is pending when
// auto-rebalance is off, and the owner's confirm pass, which lists an alter's
// awaiting members beside the old-term copies.

// shortenPendingCopyListing makes the auto-rebalance-off listing come every
// interval for the rest of the test.
func shortenPendingCopyListing(t *testing.T, interval time.Duration) {
	t.Helper()
	restore := pendingCopyListInterval.get()
	pendingCopyListInterval.set(interval)
	t.Cleanup(func() { pendingCopyListInterval.set(restore) })
}

// TestAnIncreaseOntoAFullMemberRemembersWhyItWaits is T-175's second half,
// the accessor (spec §7.5, "what the member remembers"): after a pull refused
// for the disk the member's outcome names the refusal, its next retry is the
// ceiling after the attempt, and once the disk is freed and the copy lands
// there is no outcome at all.
func TestAnIncreaseOntoAFullMemberRemembersWhyItWaits(t *testing.T) {
	restoreCeiling := pendingCopyRetryCeiling.get()
	pendingCopyRetryCeiling.set(3 * time.Second)
	t.Cleanup(func() { pendingCopyRetryCeiling.set(restoreCeiling) })
	tc, buckets, member := rebalancingPendingOn(t, 1, "full")
	defer tc.stop()
	bucket := buckets[0]
	key := placementOf(testDatabase, bucket)
	core := tc.nodes[member].server.core
	setCapacityProbes(t, core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) })

	raiseToThree(t, tc, bucket, member)
	var outcome pendingCopyOutcome
	pollUntil(t, fmt.Sprintf("node %d records why its copy of %q waits", member+1, bucket), func() bool {
		var pending bool
		outcome, pending = core.cluster.pendingCopyOf(key)
		return pending && !outcome.lastAttempt.IsZero()
	})
	if !strings.Contains(outcome.reason, "refused for this node's disk") || !strings.Contains(outcome.reason, "migrate "+testDatabase+" "+bucket) {
		t.Errorf("node %d's outcome for %q is %q, want the disk refusal with its remedy", member+1, bucket, outcome.reason)
	}
	if wait := outcome.nextRetry.Sub(outcome.lastAttempt); wait != 3*time.Second {
		t.Errorf("node %d tries %q again %v after a disk refusal, want the ceiling, 3s", member+1, bucket, wait)
	}
	if outcome.since.IsZero() {
		t.Errorf("node %d's outcome for %q carries no record time", member+1, bucket)
	}

	setCapacityProbes(t, core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	waitForPendingCopyLanded(t, tc, member, bucket)
	if outcome, pending := core.cluster.pendingCopyOf(key); pending {
		t.Errorf("node %d still has an outcome for %q after its copy landed: %+v", member+1, bucket, outcome)
	}
}

// pendingOnAMemberWithAutoRebalanceOff is three nodes at default 1 with
// auto-rebalance off, the log captured at WARN, and count buckets of node 1
// created at 2 and raised to 3, each pending on member.
func pendingOnAMemberWithAutoRebalanceOff(t *testing.T, count int, prefix string) (tc *testCluster, buckets []string, member int, logged func() string) {
	t.Helper()
	logged = captureClusterLogs(t, slog.LevelWarn)
	tc = newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	buckets, member = pendingOn(t, tc, count, prefix)
	for _, bucket := range buckets {
		createBucketAt(tc, 0, testDatabase, bucket, 2)
		waitForServing(t, tc, testDatabase, bucket, 2)
		raiseToThree(t, tc, bucket, member)
	}
	waitForBarrier(t, tc, 0, "after-the-increases", member)
	return tc, buckets, member, logged
}

// listingLines is node's lines listing its pending copies.
func listingLines(logged, node string) []string {
	return linesWith(logged, "node_id="+node, "auto_rebalance is off")
}

// TestWithAutoRebalanceOffTheWorkerListsWhatIsPending is SA-1's listing: with
// auto-rebalance off and eleven copies pending, the member's worker pulls
// nothing and logs one line with the count, eleven, and at most ten
// commands, each 'migrate <database> <bucket>', again each interval.
func TestWithAutoRebalanceOffTheWorkerListsWhatIsPending(t *testing.T) {
	shortenPendingCopyListing(t, 2*time.Second)
	tc, buckets, member, logged := pendingOnAMemberWithAutoRebalanceOff(t, 11, "listed")
	defer tc.stop()
	core := tc.nodes[member].server.core
	pollUntil(t, fmt.Sprintf("node %d lists its eleven pending copies", member+1), func() bool {
		lines := listingLines(logged(), core.cluster.self.ID)
		return len(lines) > 0 && strings.Contains(lines[len(lines)-1], "pending=11")
	})
	lines := listingLines(logged(), core.cluster.self.ID)
	last := lines[len(lines)-1]
	named := 0
	for _, bucket := range buckets {
		if strings.Contains(last, fmt.Sprintf("migrate %s %s", testDatabase, bucket)) {
			named++
		}
	}
	if named != 10 {
		t.Errorf("node %d's listing names %d of its eleven pending copies, want ten: %s", member+1, named, last)
	}
	if !holdsWithin(10*time.Second, func() bool { return len(listingLines(logged(), core.cluster.self.ID)) > len(lines) }) {
		t.Errorf("node %d listed its pending copies once and not again at the next interval", member+1)
	}
	for _, bucket := range buckets {
		if core.bucketStoreOpen(testDatabase, bucket) {
			t.Errorf("node %d, auto-rebalance off, copied %q", member+1, bucket)
		}
	}
}

// TestPendingCopiesAreListedWithAutoRebalanceOff is T-176's worker and
// listing half (SA-5): the member lists its pending copy with the count and
// the command, an operator's migrate then lands it, and the queue holds it no
// more.
func TestPendingCopiesAreListedWithAutoRebalanceOff(t *testing.T) {
	tc, buckets, member, logged := pendingOnAMemberWithAutoRebalanceOff(t, 1, "listed")
	defer tc.stop()
	bucket := buckets[0]
	core := tc.nodes[member].server.core
	command := fmt.Sprintf("migrate %s %s", testDatabase, bucket)
	pollUntil(t, fmt.Sprintf("node %d lists its pending copy of %q", member+1, bucket), func() bool {
		return len(linesWith(logged(), "node_id="+core.cluster.self.ID, "auto_rebalance is off", "pending=1", command)) > 0
	})
	awaitMigrated(t, migrateInBackground(clusterOf(tc, member), bucket), fmt.Sprintf("node %d's migrate of %q", member+1, bucket))
	requireNoNeedsCopy(t, core, bucket, "once the migrate landed the copy")
	requireNotQueued(t, core, bucket, "once the migrate landed the copy")
}

// TestConfirmPassListsAwaitingMembers is the owner's half of spec §7.5's
// listing (SA-21). The confirm pass lists an alter's awaiting member beside
// the old-term copies even when that member answers it holds a copy: the
// awaiting record, not the answer, says the copy is still to make. And an
// increase's apply on the owner runs the pass for its bucket once the alter
// is applied, which with auto-rebalance off names the migrate command on the
// member.
func TestConfirmPassListsAwaitingMembers(t *testing.T) {
	t.Run("an awaiting member is listed", func(t *testing.T) {
		tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		bucket := bucketOwnedBy(t, tc, 0, "listed")
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		owner := clusterOf(tc, 0)
		key := placementOf(testDatabase, bucket)
		held := heldBucket{database: testDatabase, bucket: bucket}
		term, _, err := owner.s.bucketLineage(testDatabase, bucket)
		if err != nil {
			t.Fatalf("reading the owner's term of %q: %v", bucket, err)
		}
		pollUntil(t, fmt.Sprintf("every member of %q follows its owner", bucket), func() bool {
			left, _, _ := owner.confirmPass(t.Context(), []heldBucket{held})
			return len(left) == 0
		})
		awaited := tc.addrs[2]
		facts, _ := owner.s.peekBucket(testDatabase, bucket)
		owner.publishAwaiting(key, []string{awaited}, awaitingRecord{Term: int64(term), Generation: facts.generation, Since: time.Now().Unix()})
		left, unconfirmed, _ := owner.confirmPass(t.Context(), []heldBucket{held})
		if len(left) != 1 {
			t.Errorf("the confirm pass settled %q while %s awaits its copy", bucket, awaited)
		}
		want := PlacementToCopy{Database: testDatabase, Bucket: bucket, Node: awaited}
		if listed := countCopies(unconfirmed, want); listed != 1 {
			t.Errorf("the confirm pass lists %v, want %+v once", unconfirmed, want)
		}
	})

	t.Run("an increase runs it", func(t *testing.T) {
		tc, buckets, member, logged := pendingOnAMemberWithAutoRebalanceOff(t, 1, "increase")
		defer tc.stop()
		bucket := buckets[0]
		owner := clusterOf(tc, 0)
		pollUntil(t, fmt.Sprintf("the owner lists the copy of %q its increase awaits", bucket), func() bool {
			return len(linesWith(logged(), "node_id="+owner.self.ID, "bucket="+bucket,
				fmt.Sprintf("migrate %s %s on %s", testDatabase, bucket, tc.addrs[member]))) > 0
		})
	})
}

// countCopies counts want in copies.
func countCopies(copies []PlacementToCopy, want PlacementToCopy) int {
	count := 0
	for _, copyToMake := range copies {
		if copyToMake == want {
			count++
		}
	}
	return count
}
