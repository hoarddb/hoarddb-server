package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The gauge of buckets with fewer copies in sync than their factor, and its
// warning (bucket-replication-rev8-spec §12.3; the G4 to G7 build doc step 11).

const gaugeName = "hoarddb_buckets_under_replicated"

// shortenUnderReplicatedPass makes the gauge's pass come at most every
// interval for the rest of the test.
func shortenUnderReplicatedPass(t *testing.T, interval time.Duration) {
	t.Helper()
	restore := underReplicatedPassInterval.get()
	underReplicatedPassInterval.set(interval)
	t.Cleanup(func() { underReplicatedPassInterval.set(restore) })
}

// ownedAtThree creates count buckets owned by node 0 at factor 3 and waits
// until all three nodes serve each.
func ownedAtThree(t *testing.T, tc *testCluster, count int, prefix string) []string {
	t.Helper()
	names := make([]string, 0, count)
	for i := range count {
		name := bucketOwnedBy(t, tc, 0, fmt.Sprintf("%s%02d-", prefix, i))
		createBucketAt(tc, 0, testDatabase, name, 3)
		waitForServing(t, tc, testDatabase, name, 3)
		names = append(names, name)
	}
	return names
}

// writeThroughOwner writes total documents to bucket through node 0 from
// writers goroutines.
func writeThroughOwner(t *testing.T, tc *testCluster, bucket string, writers, total int) {
	t.Helper()
	var group sync.WaitGroup
	failures := make(chan error, writers)
	for w := range writers {
		client := tc.dial(0)
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { _ = client.Close() }()
			for i := range total / writers {
				if err := putKey(client, bucket, fmt.Sprintf("w%d-%04d", w, i)); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("writing through the owner: %v", err)
	}
}

// TestTheBucketGaugeDoesNotMoveUnderConcurrentWritesOnAHealthyCluster is T-22:
// 2,000 writes from 16 writers recompute nothing and move nothing; one stop
// moves it once, to the number of buckets the node owns at factor 3, and the
// writes that follow recompute nothing either; and a decrease, which is
// intentional, moves it not at all and logs no warning.
func TestTheBucketGaugeDoesNotMoveUnderConcurrentWritesOnAHealthyCluster(t *testing.T) {
	shortenUnderReplicatedPass(t, 300*time.Millisecond)
	logged := captureClusterLogs(t, slog.LevelWarn)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	buckets := ownedAtThree(t, tc, 5, "gauge")
	owner := clusterOf(tc, 0)
	pollUntil(t, "the gauge's first pass has run", func() bool { return owner.underReplicated.passes.Load() >= 1 })
	pollUntil(t, "every stream is in sync", func() bool { return owner.recomputeUnderReplicated() == 0 })
	passes := owner.underReplicated.passes.Load()

	writeThroughOwner(t, tc, buckets[0], 16, 2000)
	time.Sleep(3 * underReplicatedPassInterval.get())
	if got := replicationGauge(t, tc.nodes[0].server.core, gaugeName); got != 0 {
		t.Errorf("after 2,000 writes on a healthy cluster the gauge reads %v, want 0", got)
	}
	if after := owner.underReplicated.passes.Load(); after != passes {
		t.Errorf("2,000 writes made %d gauge passes, want none: a write recomputes nothing", after-passes)
	}

	tc.stopNodeKeepingData(2)
	writeThroughOwner(t, tc, buckets[0], 4, 8)
	waitForGauge(t, tc.nodes[0].server.core, gaugeName, 30*time.Second, "node 3 is stopped", func(v float64) bool { return v == 5 })
	passes = owner.underReplicated.passes.Load()
	writeThroughOwner(t, tc, buckets[0], 16, 800)
	time.Sleep(3 * underReplicatedPassInterval.get())
	if got := replicationGauge(t, tc.nodes[0].server.core, gaugeName); got != 5 {
		t.Errorf("one stopped node moved the gauge to %v, want it to stay at the five buckets", got)
	}
	if after := owner.underReplicated.passes.Load(); after-passes > 3 {
		t.Errorf("800 writes after the stop made %d gauge passes, want at most 3: a write recomputes nothing", after-passes)
	}
	if lines := linesWith(logged(), "fewer copies in sync"); len(lines) != 0 {
		t.Errorf("a stream stalling logged %d under-replication warnings, want none: the warning is the membership adoption's", len(lines))
	}
}

// TestADecreaseMovesNoGaugeAndLogsNoWarning is T-22's intentional half: after a
// decrease the stored factor is the new one, so nothing is under-replicated.
func TestADecreaseMovesNoGaugeAndLogsNoWarning(t *testing.T) {
	shortenUnderReplicatedPass(t, 200*time.Millisecond)
	logged := captureClusterLogs(t, slog.LevelWarn)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := ownedAtThree(t, tc, 1, "lowered")[0]
	owner := clusterOf(tc, 0)
	pollUntil(t, "every stream is in sync", func() bool { return owner.recomputeUnderReplicated() == 0 })

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
	for range 5 {
		time.Sleep(2 * underReplicatedPassInterval.get())
		if got := owner.recomputeUnderReplicated(); got != 0 {
			t.Fatalf("after a decrease to 2 the owner counts %d buckets under-replicated, want 0: the demoted member is not missing", got)
		}
		if got := replicationGauge(t, tc.nodes[0].server.core, gaugeName); got != 0 {
			t.Fatalf("after a decrease to 2 the gauge reads %v, want 0", got)
		}
	}
	if lines := linesWith(logged(), "fewer copies in sync"); len(lines) != 0 {
		t.Errorf("a decrease logged %d under-replication warnings, want none: %v", len(lines), lines)
	}
}

// TestAnAwaitingRecordMovesTheGaugeOnce: a pending copy counts, the writes that
// follow recompute nothing, and landing the copy takes it back to zero.
func TestAnAwaitingRecordMovesTheGaugeOnce(t *testing.T) {
	shortenUnderReplicatedPass(t, 300*time.Millisecond)
	tc, buckets, member, _ := pendingOnAMemberWithAutoRebalanceOff(t, 1, "counted")
	defer tc.stop()
	bucket := buckets[0]
	owner := tc.nodes[0].server.core
	waitForGauge(t, owner, gaugeName, 30*time.Second, "a copy of "+bucket+" is pending", func(v float64) bool { return v == 1 })
	passes := owner.cluster.underReplicated.passes.Load()

	writeThroughOwner(t, tc, bucket, 16, 800)
	time.Sleep(3 * underReplicatedPassInterval.get())
	if after := owner.cluster.underReplicated.passes.Load(); after-passes > 2 {
		t.Errorf("800 writes with one copy pending made %d gauge passes, want at most 2", after-passes)
	}

	awaitMigrated(t, migrateInBackground(clusterOf(tc, member), bucket), fmt.Sprintf("node %d's migrate of %q", member+1, bucket))
	waitForGauge(t, owner, gaugeName, 30*time.Second, "the copy of "+bucket+" landed", func(v float64) bool { return v == 0 })
}

// TestOneWarnPerMembershipAdoptionNamesTen: removing a member leaves every
// bucket at factor 3 with two copies; the adoption logs one warning with the
// count and ten names, not a line per bucket.
func TestOneWarnPerMembershipAdoptionNamesTen(t *testing.T) {
	shortenUnderReplicatedPass(t, 200*time.Millisecond)
	logged := captureClusterLogs(t, slog.LevelWarn)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	buckets := ownedAtThree(t, tc, 30, "warned")
	tc.stopNodeKeepingData(2)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	proposer := clusterOf(tc, 1)
	if err := proposer.proposeRemoval(ctx, tc.addrs[2]); err != nil {
		t.Fatalf("removing node 3: %v", err)
	}
	owner := clusterOf(tc, 0)
	if err := owner.reconcileMembership(tc.addrs[1], proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
		t.Fatalf("bringing node 1 to the removal's epoch: %v", err)
	}
	owned := 0
	for _, bucket := range buckets {
		if owner.ownsBucket(placementOf(testDatabase, bucket)) {
			owned++
		}
	}
	if owned < 11 {
		t.Fatalf("setup: node 1 still owns %d of its 30 buckets after the removal, want at least 11", owned)
	}
	pollUntil(t, "node 1 logs its warning", func() bool { return len(linesWith(logged(), "fewer copies in sync", "node_id="+owner.self.ID)) > 0 })
	time.Sleep(3 * underReplicatedPassInterval.get())
	lines := linesWith(logged(), "fewer copies in sync", "node_id="+owner.self.ID)
	if len(lines) != 1 {
		t.Fatalf("node 1 logged %d warnings for one adoption, want 1: %v", len(lines), lines)
	}
	if want := fmt.Sprintf("buckets=%d", owned); !strings.Contains(lines[0], want) {
		t.Errorf("the warning %q does not carry the count, want %s", lines[0], want)
	}
	named := 0
	for _, bucket := range buckets {
		if strings.Contains(lines[0], testDatabase+"."+bucket) {
			named++
		}
	}
	if named != 10 {
		t.Errorf("the warning names %d buckets, want ten: %s", named, lines[0])
	}
}
