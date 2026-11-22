package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0664: a step that replaces this node's copy of a bucket with a fresh
// one — a follower copying afresh from its owner, a take-over over an own copy —
// keeps its copy until the fresh one has been fetched, checked and fits on the
// disk beside it. A pull refused before that leaves the node's copy as it was,
// still serving; a take-over whose holder sends a copy this node refuses tries
// the next holder of an equally complete copy.

// plantDeepDocument stores, straight into node's own store, a document under
// bucket nested one level past what a client may store: the corrupt or
// hostile copy a holder can send. It returns the key, so the test removes it.
func plantDeepDocument(t *testing.T, node *Server, bucket string) []byte {
	t.Helper()
	held := lookupBucket(node, testDatabase, bucket)
	if held == nil || held.id.Load() == 0 {
		t.Fatalf("setup: %s is not served on the node a deep document is planted on", bucket)
	}
	key := append(storage.Scope(held.slot, held.databaseID, held.id.Load()), storage.NSDocuments)
	key = append(key, "deep"...)
	batch := node.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(key, nestedDocumentBytes(maxDocumentDepth+1, false)); err != nil {
		t.Fatalf("setup: planting a deep document: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("setup: committing the deep document: %v", err)
	}
	return key
}

// removePlanted deletes a key plantDeepDocument stored.
func removePlanted(t *testing.T, node *Server, key []byte) {
	t.Helper()
	batch := node.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(key); err != nil {
		t.Fatalf("removing the planted document: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the removal of the planted document: %v", err)
	}
}

// replacedCopy is a node's copy of a bucket, read the same way before and
// after a step: its lineage, its count and every key's stored bytes.
type replacedCopy struct {
	lineage ReplCopy
	count   int64
	stored  map[string]string
}

func readReplacedCopy(t *testing.T, node *testNode, c *Cluster, bucket string, keys []string) replacedCopy {
	t.Helper()
	lineage, err := c.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil {
		t.Fatalf("reading %s's copy of %s: %v", c.self.ID, bucket, err)
	}
	stored := map[string]string{}
	for _, key := range keys {
		found, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
		if err == nil && found != nil && found.Found {
			stored[key] = string(found.Data)
		}
	}
	return replacedCopy{lineage: lineage, count: localCountOf(t, node.server.core, bucket), stored: stored}
}

// landedCount counts the copies of bucket that reach materializeBucket.
func landedCount(t *testing.T, bucket string) *atomic.Int64 {
	t.Helper()
	var landed atomic.Int64
	materialized := func(database, name string) {
		if database == testDatabase && name == bucket {
			landed.Add(1)
		}
	}
	beforeMaterializeHook.Store(&materialized)
	t.Cleanup(func() { beforeMaterializeHook.Store(nil) })
	return &landed
}

// seedReplacedBucket creates a hash bucket owned by tc.addrs[0] on every
// member at factor rf and writes keys to it, waiting until every member holds
// them all.
func seedReplacedBucket(t *testing.T, tc *testCluster, rf int, prefix string, keys []string) string {
	t.Helper()
	placement := ringWith(t, rf, tc.addrs...)
	bucket := bucketNamed(t, prefix, func(name string) bool { return ownerOn(placement, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, rf)
	for i, key := range keys {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	pollUntil(t, "every member holds every key", func() bool {
		for _, node := range tc.nodes {
			if !holdsKeyLocally(node, bucket, keys[len(keys)-1]) {
				return false
			}
		}
		return true
	})
	return bucket
}

// replacingPaths are the two steps that replace this node's copy with one
// pulled from another member: recopyPaths' copyAfresh and copyForTakeOver.
func replacingPaths() []recopyPath {
	var paths []recopyPath
	for _, path := range recopyPaths {
		if path.name != "diverged replica is re-seeded" {
			paths = append(paths, path)
		}
	}
	return paths
}

// TestARefusedReplacingPullKeepsTheCopyServing: two members at factor 2 hold
// a bucket of five keys, and the member the copy is pulled from holds one
// document nested past the bound as well, so this node refuses the copy it
// sends. The step must fail with ErrDocumentTooDeep and leave this node's copy
// as it was: open, the same lineage, count and stored bytes, answering reads,
// and taking the owner's next write. With the planted document gone, the same
// step replaces the copy.
func TestARefusedReplacingPullKeepsTheCopyServing(t *testing.T) {
	keys := []string{"k0", "k1", "k2", "k3", "k4"}
	for _, path := range replacingPaths() {
		t.Run(path.name, func(t *testing.T) {
			tc := newTestCluster(t, 2, 2)
			defer tc.stop()
			bucket := seedReplacedBucket(t, tc, 2, "keepserving", keys)
			peerIndex := 1 - path.actor
			actorNode, actor, peer := tc.nodes[path.actor], clusterOf(tc, path.actor), clusterOf(tc, peerIndex)
			before := readReplacedCopy(t, actorNode, actor, bucket, keys)
			if !before.lineage.Holds || len(before.stored) != len(keys) {
				t.Fatalf("setup: %s holds %d of %d keys of %s (%+v)", actor.self.ID, len(before.stored), len(keys), bucket, before.lineage)
			}
			planted := plantDeepDocument(t, tc.nodes[peerIndex].server.core, bucket)
			landed := landedCount(t, bucket)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			err := path.run(ctx, actor, peer, bucket)
			if !errors.Is(err, ErrDocumentTooDeep) {
				t.Errorf("%s from a holder whose copy carries a document past the bound = %v, want ErrDocumentTooDeep", path.name, err)
			}
			if !actorNode.server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Fatalf("%s released its copy of %s before the pull it then refused", actor.self.ID, bucket)
			}
			after := readReplacedCopy(t, actorNode, actor, bucket, keys)
			if !reflect.DeepEqual(after, before) {
				t.Errorf("%s's copy of %s changed across the refused pull:\nbefore %+v\nafter  %+v", actor.self.ID, bucket, before, after)
			}
			if n := landed.Load(); n != 0 {
				t.Errorf("%s landed %d copies of %s from a copy it refused", actor.self.ID, n, bucket)
			}
			tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after",
				Data: map[string]interface{}{"n": 9}, WriteConcern: "all"})
			pollUntil(t, "the kept copy takes the owner's next write", func() bool {
				return holdsKeyLocally(actorNode, bucket, "after")
			})

			removePlanted(t, tc.nodes[peerIndex].server.core, planted)
			if err := path.run(ctx, actor, peer, bucket); err != nil {
				t.Fatalf("%s from the holder's clean copy: %v", path.name, err)
			}
			if n := landed.Load(); n != 1 {
				t.Errorf("%s landed %d copies of %s from the clean copy, want 1", actor.self.ID, n, bucket)
			}
			for _, key := range append(keys, "after") {
				if !holdsKeyLocally(actorNode, bucket, key) {
					t.Errorf("%s's replaced copy of %s lacks %s", actor.self.ID, bucket, key)
				}
			}
		})
	}
}

// TestATakeOverTriesTheNextHolderWhenACopyIsRefused: three members at factor
// 3. The owner takes the bucket over over its own copy, from the holder of the
// best copy, which carries a document past the bound; the third member holds
// an equally complete clean copy. The owner must refuse the first, copy the
// second, and hold every key and not the planted one.
func TestATakeOverTriesTheNextHolderWhenACopyIsRefused(t *testing.T) {
	keys := []string{"k0", "k1", "k2", "k3", "k4"}
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	bucket := seedReplacedBucket(t, tc, 3, "nextholder", keys)
	owner := clusterOf(tc, 0)
	ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}
	own, err := owner.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading the owner's copy: %v", err)
	}
	holderCopy := func(index int) heldCopy {
		t.Helper()
		answer, err := clusterOf(tc, index).answerCopy(ref)
		if err != nil {
			t.Fatalf("reading %s's copy: %v", tc.addrs[index], err)
		}
		return heldCopy{node: clusterOf(tc, index).self, answer: answer, current: true}
	}
	planted := plantDeepDocument(t, tc.nodes[1].server.core, bucket)
	landed := landedCount(t, bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	plan := takeOverPlan{best: holderCopy(1), alternatives: []heldCopy{holderCopy(2)}}
	if err := owner.copyForTakeOver(ctx, heldBucket{database: testDatabase, bucket: bucket}, placementOf(testDatabase, bucket), own, plan); err != nil {
		t.Fatalf("the take-over did not copy the clean holder's copy after the first holder's was refused: %v", err)
	}
	if n := landed.Load(); n != 1 {
		t.Errorf("the take-over landed %d copies, want 1", n)
	}
	for _, key := range keys {
		if !holdsKeyLocally(tc.nodes[0], bucket, key) {
			t.Errorf("the owner's copy lacks %s", key)
		}
	}
	held := lookupBucket(tc.nodes[0].server.core, testDatabase, bucket)
	if held == nil || held.id.Load() == 0 {
		t.Fatal("the owner serves no copy after the take-over")
	}
	snap := tc.nodes[0].server.core.kv.NewSnapshot()
	defer snap.Discard()
	deep := append(storage.Scope(held.slot, held.databaseID, held.id.Load()), planted[len(storage.Scope(0, 0, 0)):]...)
	if stored, err := snap.Get(deep); err == nil {
		t.Errorf("the owner landed the refused holder's planted document (%d bytes)", len(stored))
	}
}

// TestAReplacingPullThatCannotFitBesideTheCopyIsRefusedBeforeTheRelease: the
// copy a step replaces is kept until the new one has landed, so both must fit
// below the high-water mark. A follower whose disk is just under the mark —
// the plain guard lets the pull through — must refuse the copy, name the disk
// and the fix, and keep its copy; with room, the same step replaces it.
func TestAReplacingPullThatCannotFitBesideTheCopyIsRefusedBeforeTheRelease(t *testing.T) {
	keys := []string{"k0", "k1", "k2", "k3", "k4"}
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	bucket := seedReplacedBucket(t, tc, 2, "bothfit", keys)
	path := replacingPaths()[0]
	actorNode, actor, peer := tc.nodes[path.actor], clusterOf(tc, path.actor), clusterOf(tc, 1-path.actor)
	before := readReplacedCopy(t, actorNode, actor, bucket, keys)
	landed := landedCount(t, bucket)

	// 84.999% used of 100,000 bytes, under the mark of 85: any copy of more
	// than a byte takes the disk past it.
	nearlyFull := diskStat{Blocks: 100_000, Bavail: 15_001, Frsize: 1}
	if limit := actorNode.server.core.config.Server.DiskHighWaterPct; limit != 85 || !diskBelowMark(nearlyFull, limit) {
		t.Fatalf("setup: the fixture must be just under a mark of 85 (mark %d)", limit)
	}
	setCapacityProbes(t, actorNode.server.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(nearlyFull) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := path.run(ctx, actor, peer, bucket)
	if !errors.Is(err, ErrMemberDiskFull) {
		t.Fatalf("a replacing pull that cannot fit beside the copy = %v, want ErrMemberDiskFull", err)
	}
	for _, phrase := range []string{actor.self.ID, fmt.Sprintf("%q", bucket), "high-water mark", "kept", "Free space",
		fmt.Sprintf("'migrate %s %s' on %s", testDatabase, bucket, actor.self.ID), "remove-node " + actor.self.ID} {
		if !strings.Contains(err.Error(), phrase) {
			t.Errorf("the refusal does not name %q: %v", phrase, err)
		}
	}
	if after := readReplacedCopy(t, actorNode, actor, bucket, keys); !reflect.DeepEqual(after, before) {
		t.Errorf("%s's copy of %s changed across the refused pull:\nbefore %+v\nafter  %+v", actor.self.ID, bucket, before, after)
	}
	if n := landed.Load(); n != 0 {
		t.Errorf("%s landed %d copies of %s on a disk with no room for both", actor.self.ID, n, bucket)
	}

	setCapacityProbes(t, actorNode.server.core, harnessProbes)
	if err := path.run(ctx, actor, peer, bucket); err != nil {
		t.Fatalf("%s with room for both: %v", path.name, err)
	}
	if n := landed.Load(); n != 1 {
		t.Errorf("%s landed %d copies of %s with room for both, want 1", actor.self.ID, n, bucket)
	}
}

// TestAnEntryShippedDuringAReplacingPullIsNotLost: while a follower's copy
// afresh has fetched the owner's copy and not yet released its own, the owner
// takes a write. The follower's copy must not take that entry — the copy that
// replaces it was taken before the write — so the stream re-sends it, and the
// replaced copy ends up holding it. Guard: an entry acknowledged into the
// copy being replaced is never re-sent, and the replaced copy lacks it.
func TestAnEntryShippedDuringAReplacingPullIsNotLost(t *testing.T) {
	keys := []string{"k0", "k1", "k2"}
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	bucket := seedReplacedBucket(t, tc, 2, "duringpull", keys)
	path := replacingPaths()[0]
	follower, owner := clusterOf(tc, path.actor), clusterOf(tc, 1-path.actor)

	var wrote atomic.Bool
	writeDuring := func(database, name string) {
		if database != testDatabase || name != bucket || !wrote.CompareAndSwap(false, true) {
			return
		}
		tc.ok(1-path.actor, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "during",
			Data: map[string]interface{}{"n": 7}})
		// Long enough for the owner's stream to deliver the entry, were the
		// follower to take it.
		time.Sleep(300 * time.Millisecond)
	}
	replacingPullCheckedHook.Store(&writeDuring)
	t.Cleanup(func() { replacingPullCheckedHook.Store(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := path.run(ctx, follower, owner, bucket); err != nil {
		t.Fatalf("%s: %v", path.name, err)
	}
	if !wrote.Load() {
		t.Fatal("setup: the write during the pull did not run")
	}
	pollUntil(t, "the replaced copy holds the write taken during the pull", func() bool {
		owner.wakeSenderOf(follower.self.ID)
		return holdsKeyLocally(tc.nodes[path.actor], bucket, "during")
	})
}
