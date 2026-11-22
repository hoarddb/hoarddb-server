package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 9, step 9.5: the placement pass (T-119).

// recordedPlacementMarks reads every mark a node's store holds, by placement
// key.
func recordedPlacementMarks(t *testing.T, kv storage.KV) map[string]placementMarks {
	t.Helper()
	prefix := placementPrefix()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	marks := make(map[string]placementMarks)
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		suffix := string(iterator.Key()[len(prefix):])
		if suffix == "" {
			continue
		}
		stored, err := iterator.Value()
		if err != nil {
			t.Fatalf("reading a placement mark: %v", err)
		}
		var mark placementMarks
		if err := bson.Unmarshal(stored, &mark); err != nil {
			t.Fatalf("decoding a placement mark: %v", err)
		}
		marks[suffix] = mark
	}
	return marks
}

// factorTwoRingOf builds the ring of ids.
func factorTwoRingOf(t *testing.T, ids []string) *ring.Ring {
	t.Helper()
	built := ring.New(2)
	for _, id := range ids {
		if err := built.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
	}
	return built
}

// expectedPlacementMarks is what the pass of epoch should record on node self
// for the buckets it holds, from the ring before and the ring after, at
// factor 2: computed here, not by the code under test.
func expectedPlacementMarks(t *testing.T, held []string, self string, before, after *ring.Ring, epoch uint64) map[string]placementMarks {
	t.Helper()
	expected := make(map[string]placementMarks)
	for _, name := range held {
		key := placementOf(testDatabase, name).encoded
		oldSet, err := before.Replicas(key, 2)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		newSet, err := after.Replicas(key, 2)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		var marks placementMarks
		if oldSet[0].ID != newSet[0].ID {
			marks.Moved = epoch
		}
		if nodeListed(oldSet, self) && !nodeListed(newSet, self) {
			marks.Outside = epoch
		}
		if marks != (placementMarks{}) {
			expected[key] = marks
		}
	}
	return expected
}

// waitForPlacementPassOf waits until the cluster's pass for its current epoch
// has finished.
func waitForPlacementPassOf(t *testing.T, c *Cluster) {
	t.Helper()
	pollUntil(t, fmt.Sprintf("%s finishes its placement pass", c.self.ID), c.placementPassFinished)
}

// TestThePlacementPassMarksWhatMovedAndNothingElse is T-119: 1,000 buckets at
// factor 2 on three nodes; a fourth joins. On each node the marks recorded are
// exactly those of the buckets it holds whose owner changed, or whose set it
// left, computed in the test from the two rings. A second change does not
// overwrite a mark the first set. Guard: mark every bucket.
func TestThePlacementPassMarksWhatMovedAndNothingElse(t *testing.T) {
	// The marks are read as the pass recorded them, before the take-over
	// worker settles the copies and clears them (step 10b.5).
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	tc := newTestClusterWith(t, 3, 2, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	const buckets = 1000
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	names := make([]string, 0, buckets)
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("placed%04d", i)
		names = append(names, name)
		tc.ok(i%3, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}
	pollUntil(t, "every copy of every bucket is in place", func() bool {
		held := 0
		for index := range tc.nodes {
			for _, name := range names {
				if tc.nodes[index].server.core.bucketStoreOpen(testDatabase, name) {
					held++
				}
			}
		}
		return held == 2*buckets
	})

	ports := reserveNodePorts(t)
	joiner := joinNewcomer(t, tc, 0, ports)
	before := factorTwoRingOf(t, tc.addrs)
	after := factorTwoRingOf(t, append(append([]string(nil), tc.addrs...), ports.ringAddr))
	joinEpoch := clusterOf(tc, 0).Epoch()
	firstMarks := make([]map[string]placementMarks, len(tc.nodes))
	for index := range tc.nodes {
		c := clusterOf(tc, index)
		waitForPlacementPassOf(t, c)
		var held []string
		for _, name := range names {
			if c.s.bucketStoreOpen(testDatabase, name) {
				held = append(held, name)
			}
		}
		want := expectedPlacementMarks(t, held, c.self.ID, before, after, joinEpoch)
		got := recordedPlacementMarks(t, c.s.kv)
		firstMarks[index] = got
		if len(want) == 0 {
			t.Fatalf("test setup: the join moves none of node %d's %d buckets", index+1, len(held))
		}
		missing, extra, wrong := 0, 0, 0
		for key, mark := range want {
			recorded, present := got[key]
			switch {
			case !present:
				missing++
			case recorded != mark:
				wrong++
			}
		}
		for key := range got {
			if _, expected := want[key]; !expected {
				extra++
			}
		}
		if missing+extra+wrong > 0 {
			t.Errorf("node %d holds %d buckets, %d of which the join changed for it: its pass recorded %d marks, %d missing, %d on buckets it did not change, %d with the wrong epochs",
				index+1, len(held), len(want), len(got), missing, extra, wrong)
		}
	}

	// A second change: the joiner is removed again. Every mark the first pass
	// set keeps its epoch.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeRemoval(ctx, joiner.cluster().self.ID); err != nil {
		t.Fatalf("removing the joiner: %v", err)
	}
	for index := range tc.nodes {
		c := clusterOf(tc, index)
		pollUntil(t, fmt.Sprintf("node %d adopts the removal", index+1), func() bool { return c.Epoch() > joinEpoch })
		waitForPlacementPassOf(t, c)
		got := recordedPlacementMarks(t, c.s.kv)
		for key, first := range firstMarks[index] {
			second := got[key]
			if (first.Moved != 0 && second.Moved != first.Moved) || (first.Outside != 0 && second.Outside != first.Outside) {
				t.Errorf("node %d: a mark set at epoch %d, %+v, reads %+v after the next change: a mark is never overwritten", index+1, joinEpoch, first, second)
				break
			}
		}
	}
}

// TestPlacementPassAtAHundredThousandBuckets reports how long the pass takes
// over 100,000 held buckets (build doc §6 item 5; SA-9.2 stops at a second).
// It runs only when HOARDDB_MEASURE_PLACEMENT is set: it creates 100,000
// buckets.
func TestPlacementPassAtAHundredThousandBuckets(t *testing.T) {
	if os.Getenv("HOARDDB_MEASURE_PLACEMENT") == "" {
		t.Skip("set HOARDDB_MEASURE_PLACEMENT=1 to measure the placement pass at 100,000 buckets")
	}
	const buckets = 100_000
	dataDir := t.TempDir()
	kv, err := storage.OpenBadgerKV(filepath.Join(dataDir, "store"), storage.BadgerTuning{MemTableMB: 64}, nil)
	if err != nil {
		t.Fatalf("opening the node store: %v", err)
	}
	defer func() { _ = kv.Close() }()
	s := newKVFaultServer(dataDir, kv)
	created := time.Now()
	for i := 0; i < buckets; i++ {
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: testDatabase, Name: fmt.Sprintf("b%06d", i), Type: BucketTypeHash}); err != nil || !resp.Success {
			t.Fatalf("creating bucket %d: %+v, %v", i, resp, err)
		}
	}
	t.Logf("created %d buckets in %s", buckets, time.Since(created).Round(time.Millisecond))
	c := retentionTestCluster(t, s, dataDir)
	defer c.cancel()
	c.rf = 2
	self := c.self.ID
	three := []ring.Node{{ID: self, Addr: self}, {ID: "127.0.0.1:2", Addr: "127.0.0.1:2"}, {ID: "127.0.0.1:3", Addr: "127.0.0.1:3"}}
	four := append(append([]ring.Node(nil), three...), ring.Node{ID: "127.0.0.1:4", Addr: "127.0.0.1:4"})
	if err := c.recordMembership(1, three); err != nil {
		t.Fatalf("recording epoch 1: %v", err)
	}
	if err := c.recordMembership(2, four); err != nil {
		t.Fatalf("recording epoch 2: %v", err)
	}
	began := time.Now()
	if err := c.placementPass(2); err != nil {
		t.Fatalf("the pass: %v", err)
	}
	took := time.Since(began)
	marked := len(recordedPlacementMarks(t, s.kv))
	t.Logf("placement pass over %d held buckets: %s, %d marked", buckets, took.Round(time.Millisecond), marked)
	if took > time.Second {
		t.Errorf("the placement pass took %s over %d buckets; SA-9.2 stops above one second", took, buckets)
	}
}
