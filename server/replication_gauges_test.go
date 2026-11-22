package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 8.1: what an operator sees of the replication stream (spec §19.3.13).
// Five unlabelled collectors; nothing is labelled by bucket or by database.

// replicationCollectors are the five families of spec §19.3.13 and the gauge
// of buckets under-replicated (replication spec §12.3, T-40).
var replicationCollectors = []string{
	"hoarddb_replication_peers_not_in_sync",
	"hoarddb_replication_lag_entries",
	"hoarddb_replication_lag_seconds",
	"hoarddb_cluster_peers_unreachable",
	"hoarddb_replication_entries_stranded_total",
	"hoarddb_buckets_under_replicated",
}

// servedMetrics gathers what a node's /metrics serves, by family name.
func servedMetrics(t testing.TB, s *Server) map[string][]*dto.Metric {
	t.Helper()
	families, err := metricsGatherer(s).Gather()
	if err != nil {
		t.Fatalf("gathering what /metrics serves: %v", err)
	}
	byName := make(map[string][]*dto.Metric, len(families))
	for _, family := range families {
		byName[family.GetName()] = family.GetMetric()
	}
	return byName
}

// replicationGauge reads one unlabelled replication gauge a node serves. The
// test fails when the node does not serve it.
func replicationGauge(t testing.TB, s *Server, name string) float64 {
	t.Helper()
	series := servedMetrics(t, s)[name]
	if len(series) != 1 {
		t.Fatalf("%s: the node serves %d series, want one", name, len(series))
	}
	return metricValue(series[0])
}

// waitForGauge polls a gauge until want holds, and fails with the last
// reading after limit.
func waitForGauge(t *testing.T, s *Server, name string, limit time.Duration, what string, want func(float64) bool) float64 {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		reading := replicationGauge(t, s, name)
		if want(reading) {
			return reading
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s still reads %v after %v", what, name, reading, limit)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// putKey writes one document to database.bucket through client. It upserts:
// the callers measure replication volume, not new-key counts, and several of
// them send the same keys again, which a plain put refuses (DOCUMENT_EXISTS).
func putKey(client *transport.DriverClient, bucket, key string) error {
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
		Data: map[string]interface{}{"key": key}})
	if err != nil {
		return fmt.Errorf("encoding %s: %w", key, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := client.SendRequest(ctx, transport.OpUpsert, payload)
	if err != nil {
		return fmt.Errorf("writing %s: %w", key, err)
	}
	if frame.Opcode == transport.OpError {
		return fmt.Errorf("writing %s: refused: %s", key, frame.Payload)
	}
	return nil
}

// runningWriters keeps writers writing to bucket through one node until stop
// is called, which returns how many writes were answered and the first error.
func runningWriters(t *testing.T, tc *testCluster, index, writers int, bucket string) (written func() int64, stop func() error) {
	t.Helper()
	var count atomic.Int64
	var firstErr error
	var errOnce sync.Once
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		client := tc.dial(index)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			defer func() { _ = client.Close() }()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				if err := putKey(client, bucket, fmt.Sprintf("run-w%d-%05d", w, i)); err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				count.Add(1)
			}
		}(w)
	}
	var stopOnce sync.Once
	return count.Load, func() error {
		stopOnce.Do(func() {
			close(done)
			wg.Wait()
		})
		return firstErr
	}
}

// TestPeersNotInSyncGaugeFollowsTheStream is T-111. The owner of a bucket on
// three nodes at factor 3 reads 1 while its stream to one replica is frozen, 0
// once it is released, 1 while the replica is stopped, and 0 again once the
// replica has returned and its replay has caught up — with eight writers
// running through the owner the whole time it returns.
// Guard: count the members whose gossip fails instead of the streams that are
// stalled (a frozen stream gossips; the gauge then reads 0 while it is frozen).
func TestPeersNotInSyncGaugeFollowsTheStream(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, replicaIndex := ownerAndReplica(t, tc)
	owner := tc.nodes[ownerIndex].server.core
	const gauge = "hoarddb_replication_peers_not_in_sync"
	if answer, err := writeConfirmed(tc.nodes[ownerIndex].client, "before", "all"); err != nil || answer.confirmed != 3 {
		t.Fatalf("test setup: the write before the freeze was answered %+v (%v), want copies_confirmed 3", answer, err)
	}
	waitForGauge(t, owner, gauge, 15*time.Second, "every stream in sync", func(v float64) bool { return v == 0 })

	unfreeze := freezePeer(owner.cluster.peerFor(mustNode(t, owner.cluster, tc.addrs[replicaIndex])))
	defer unfreeze()
	if _, err := writeConfirmed(tc.nodes[ownerIndex].client, "while-frozen", "all"); err != nil {
		t.Fatal(err)
	}
	waitForGauge(t, owner, gauge, 15*time.Second, "a replica frozen", func(v float64) bool { return v == 1 })
	unfreeze()
	if _, err := writeConfirmed(tc.nodes[ownerIndex].client, "after-thaw", "all"); err != nil {
		t.Fatal(err)
	}
	waitForGauge(t, owner, gauge, 30*time.Second, "the frozen replica released", func(v float64) bool { return v == 0 })

	base := tc.stopNodeKeepingData(replicaIndex)
	if _, err := writeConfirmed(tc.nodes[ownerIndex].client, "while-stopped", "all"); err != nil {
		t.Fatal(err)
	}
	waitForGauge(t, owner, gauge, 15*time.Second, "a replica stopped", func(v float64) bool { return v == 1 })
	time.Sleep(time.Second)
	if reading := replicationGauge(t, owner, gauge); reading != 1 {
		t.Fatalf("a replica stopped for a second: %s reads %v, want 1", gauge, reading)
	}

	written, stopWriters := runningWriters(t, tc, ownerIndex, 8, testBucket)
	defer func() { _ = stopWriters() }()
	time.Sleep(500 * time.Millisecond)
	tc.nodes[replicaIndex] = tc.startNode(replicaIndex, base, tc.token)
	waitForGauge(t, owner, gauge, 60*time.Second, "the stopped replica returned under eight writers", func(v float64) bool { return v == 0 })
	before := written()
	time.Sleep(300 * time.Millisecond)
	if written() == before {
		t.Fatal("the writers had stopped when the gauge returned to 0, so it was not read under load")
	}
	if err := stopWriters(); err != nil {
		t.Fatalf("a writer failed: %v", err)
	}
}

// idleAndBusyReplica creates a bucket on three nodes at factor 2 and returns
// its name, its owner's index, its replica's index and the index of the node
// that holds no copy of it.
func idleAndBusyReplica(t *testing.T, tc *testCluster) (string, int, int, int) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	cluster := tc.nodes[0].server.core.cluster
	bucket := "busy"
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	replicas, err := cluster.Replicas(placementOf(testDatabase, bucket))
	if err != nil || len(replicas) != 2 {
		t.Fatalf("the replica set of %s: %v (%v), want two nodes", bucket, replicas, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, replicas[0].ID)
	replicaIndex := nodeIndexByAddr(t, tc.addrs, replicas[1].ID)
	idleIndex := 3 - ownerIndex - replicaIndex
	return bucket, ownerIndex, replicaIndex, idleIndex
}

// TestLagGaugesAreZeroForAnIdlePeerAndGrowForOneThatIsDown is T-112. Three
// nodes at factor 2: 5,000 entries go to a bucket whose copies are on the owner
// and one replica. The third node is sent none of them, so it is owed nothing,
// and both lag gauges on the owner read 0 once the replica has confirmed them.
// Then the replica is stopped: both gauges grow while writes continue.
// Guard: lag as tail minus cursor with no idle rule — the idle peer reads
// 5,000.
func TestLagGaugesAreZeroForAnIdlePeerAndGrowForOneThatIsDown(t *testing.T) {
	tc := newTestClusterWith(t, 3, 2, func(_ int, cfg *config.Config) {
		// No retention pass during the test: its advance notice would move the
		// idle peer's cursor, and the idle rule is what is under test.
		cfg.Server.WALRetentionSeconds = 3600
	})
	defer tc.stop()
	bucket, ownerIndex, replicaIndex, idleIndex := idleAndBusyReplica(t, tc)
	owner := tc.nodes[ownerIndex].server.core
	idleCursorBefore, _ := owner.cluster.peerFor(mustNode(t, owner.cluster, tc.addrs[idleIndex])).cursor(owner.cluster.self.ID)

	const writers, perWriter = 8, 625
	var wg sync.WaitGroup
	failures := make(chan error, writers)
	for w := 0; w < writers; w++ {
		client := tc.dial(ownerIndex)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			defer func() { _ = client.Close() }()
			for i := 0; i < perWriter; i++ {
				if err := putKey(client, bucket, fmt.Sprintf("w%d-%04d", w, i)); err != nil {
					failures <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	idlePeer := owner.cluster.peerFor(mustNode(t, owner.cluster, tc.addrs[idleIndex]))
	if cursor, _ := idlePeer.cursor(owner.cluster.self.ID); cursor != idleCursorBefore {
		t.Fatalf("test setup: the idle peer's cursor moved from %d to %d; it was sent something", idleCursorBefore, cursor)
	}
	if behind := owner.wal.SyncedSequence() - idleCursorBefore; behind < writers*perWriter {
		t.Fatalf("test setup: the idle peer's cursor is %d entries below the synced tail, want at least %d", behind, writers*perWriter)
	}
	waitForGauge(t, owner, "hoarddb_replication_lag_entries", 15*time.Second,
		"5,000 entries addressed to another peer, all confirmed", func(v float64) bool { return v == 0 })
	if seconds := replicationGauge(t, owner, "hoarddb_replication_lag_seconds"); seconds != 0 {
		t.Fatalf("5,000 entries addressed to another peer, all confirmed: hoarddb_replication_lag_seconds reads %v, want 0", seconds)
	}

	tc.stopNodeKeepingData(replicaIndex)
	ownerClient := tc.nodes[ownerIndex].client
	for i := 0; i < 50; i++ {
		if err := putKey(ownerClient, bucket, fmt.Sprintf("down-a-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	entries := waitForGauge(t, owner, "hoarddb_replication_lag_entries", 15*time.Second,
		"the replica stopped and 50 writes made", func(v float64) bool { return v >= 50 })
	firstSeconds := waitForGauge(t, owner, "hoarddb_replication_lag_seconds", 15*time.Second,
		"the replica stopped and 50 writes made", func(v float64) bool { return v > 0 })
	time.Sleep(1500 * time.Millisecond)
	for i := 0; i < 50; i++ {
		if err := putKey(ownerClient, bucket, fmt.Sprintf("down-b-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if later := replicationGauge(t, owner, "hoarddb_replication_lag_entries"); later < entries+50 {
		t.Errorf("50 more writes with the replica stopped: hoarddb_replication_lag_entries went from %v to %v, want at least %v", entries, later, entries+50)
	}
	if later := replicationGauge(t, owner, "hoarddb_replication_lag_seconds"); later < firstSeconds+1 {
		t.Errorf("1.5 s later with the replica stopped: hoarddb_replication_lag_seconds went from %v to %v, want it to grow by at least 1", firstSeconds, later)
	}
}

// TestNoReplicationCollectorIsLabelledByBucketOrDatabase is T-113 (rule 11).
// Every node serves the five collectors as one unlabelled series each, and
// 200 more buckets add no series to any of them.
func TestNoReplicationCollectorIsLabelledByBucketOrDatabase(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	countSeries := func() map[string]int {
		counts := make(map[string]int, len(replicationCollectors))
		for _, node := range tc.nodes {
			served := servedMetrics(t, node.server.core)
			for _, name := range replicationCollectors {
				for _, series := range served[name] {
					counts[name]++
					for _, label := range series.GetLabel() {
						t.Errorf("node %s serves %s with label %s=%q; the replication collectors carry no label",
							node.addr, name, label.GetName(), label.GetValue())
					}
				}
			}
		}
		return counts
	}
	before := countSeries()
	for _, name := range replicationCollectors {
		if before[name] != len(tc.nodes) {
			t.Errorf("%s: %d series over %d nodes, want one per node", name, before[name], len(tc.nodes))
		}
	}
	for i := 0; i < 200; i++ {
		tc.ok(i%len(tc.nodes), transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fmt.Sprintf("b%03d", i), Type: BucketTypeHash})
	}
	for _, node := range tc.nodes {
		if err := putKey(node.client, "b000", "k-"+node.addr); err != nil {
			t.Fatal(err)
		}
	}
	after := countSeries()
	for _, name := range replicationCollectors {
		if after[name] != before[name] {
			t.Errorf("%s: %d series before 200 buckets, %d after; a series per bucket is unusable at a million", name, before[name], after[name])
		}
	}
}
