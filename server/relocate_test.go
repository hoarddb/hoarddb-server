package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestBucketHeldUnderTheBareNameKeyRelocatesOnStart is the upgrade
// test for placement by database: a bucket written before the fix sits on the owner of its bare name.
// When the cluster starts on the fixed code, the bucket moves to the owner of
// its (database, bucket) placement key, with its data, and leaves the old
// holder. The old layout is built by creating the bucket and its document
// directly on the bare name's owner, the node the unfixed ring placed it on;
// the expected owners are computed through a ring.Ring over the member ids,
// so the test compiles and runs against the unfixed placement too.
func TestBucketHeldUnderTheBareNameKeyRelocatesOnStart(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	expected := ring.New(1)
	for _, addr := range tc.addrs {
		if err := expected.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			t.Fatalf("building the expected ring: %v", err)
		}
	}
	ownerIndexOf := func(key string) int {
		t.Helper()
		owner, err := expected.Owner(key)
		if err != nil {
			t.Fatalf("expected owner of %q: %v", key, err)
		}
		return nodeIndexByAddr(t, tc.addrs, owner.ID)
	}
	name, oldIndex, newIndex := "", -1, -1
	for i := 0; i < 256 && name == ""; i++ {
		candidate := fmt.Sprintf("legacy%d", i)
		bare, qualified := ownerIndexOf(candidate), ownerIndexOf(string(storage.PlacementKey(testDatabase, candidate, nil)))
		if bare != qualified {
			name, oldIndex, newIndex = candidate, bare, qualified
		}
	}
	if name == "" {
		t.Fatal("precondition: no name in legacy0..legacy255 whose bare and placement-key owners differ")
	}

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	old := tc.nodes[oldIndex].server.core
	if resp, err := old.CreateBucket(CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("create %s/%s on its bare-name owner: %+v, %v", testDatabase, name, resp, err)
	}
	if resp, err := old.Insert(InsertRequest{Database: testDatabase, Bucket: name, Key: "k", Data: map[string]interface{}{"n": 1}}); err != nil || !resp.Success {
		t.Fatalf("insert into %s/%s on its bare-name owner: %+v, %v", testDatabase, name, resp, err)
	}

	// The upgrade: the whole cluster stops and starts on the fixed binary. The
	// old holder starts last, so no peer holds a pooled connection to its
	// previous process — a single-node restart leaves its peers such sockets,
	// and every pull through one used to fail until the pool was rebuilt
	// (fixed: the pool now replaces a failed socket itself, so each
	// stale socket costs at most one request). This test still restarts the
	// whole cluster because that is the upgrade the user note prescribes.
	base := filepath.Dir(tc.nodes[0].cfg.Storage.DataDir)
	for i, n := range tc.nodes {
		if n.client != nil {
			_ = n.client.Close()
			n.client = nil
		}
		if err := n.server.Stop(); err != nil {
			t.Fatalf("stop node %d: %v", i+1, err)
		}
		n.server = nil
	}
	for i := range tc.nodes {
		if i != oldIndex {
			tc.nodes[i] = tc.startNode(i, base, tc.token)
		}
	}
	tc.nodes[oldIndex] = tc.startNode(oldIndex, base, tc.token)

	deadline := time.Now().Add(30 * time.Second)
	for {
		target := tc.nodes[newIndex].server.core
		moved := target.bucketStoreOpen(testDatabase, name) && holdsKey(t, target, testDatabase, name, "k")
		left := !tc.nodes[oldIndex].server.core.bucketStoreOpen(testDatabase, name)
		if moved && left {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s/%s after the cluster restarted, old holder node %d: placement-key owner node %d holds its document = %v, old holder released it = %v; want both",
				testDatabase, name, oldIndex+1, newIndex+1, moved, left)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestRelocationReleasesACopyItWasNotThePullSourceOf checks that the
// load-time relocation, which hands a misplaced bucket to its replica set
// through the same notification as the sweep, does not inherit the sweep's
// gap. At
// RF 2 the owner already holds the bucket and the other member is seeded from
// the owner, so the misplaced holder is never a pull's source; before the fix
// it therefore never dropped its copy, and every later start found it
// misplaced again.
func TestRelocationReleasesACopyItWasNotThePullSourceOf(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	const name = "misplaced"
	replicas, err := tc.nodes[0].server.core.cluster.Replicas(placementOf(testDatabase, name))
	if err != nil {
		t.Fatalf("replica set of %s: %v", name, err)
	}
	ownerIndex, memberIndex := nodeIndexByAddr(t, tc.addrs, replicas[0].ID), nodeIndexByAddr(t, tc.addrs, replicas[1].ID)
	outsiderIndex := 3 - ownerIndex - memberIndex

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, i := range []int{ownerIndex, outsiderIndex} {
		core := tc.nodes[i].server.core
		// Stamped as a replica's create: a bare create would mint an
		// incarnation per node (BUG-0624), and the two copies of one bucket
		// must carry one number or the release rightly keeps the outsider's.
		createLocally(t, tc.nodes[i], name)
		if resp, err := core.Insert(InsertRequest{Database: testDatabase, Bucket: name, Key: "k", Data: map[string]interface{}{"n": 1}}); err != nil || !resp.Success {
			t.Fatalf("insert into %s/%s on node %d: %+v, %v", testDatabase, name, i+1, resp, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	outsider := tc.nodes[outsiderIndex].server.core
	if err := outsider.cluster.relocateBucket(ctx, heldBucket{database: testDatabase, bucket: name}); err != nil {
		t.Fatalf("relocate %s/%s from node %d: %v", testDatabase, name, outsiderIndex+1, err)
	}
	if !holdsKey(t, tc.nodes[memberIndex].server.core, testDatabase, name, "k") {
		t.Fatalf("precondition: the relocation did not seed node %d, the set's other member", memberIndex+1)
	}
	if outsider.bucketStoreOpen(testDatabase, name) {
		t.Fatalf("node %d relocated %s/%s to a replica set that now holds it everywhere, but still holds its own copy",
			outsiderIndex+1, testDatabase, name)
	}
	if misplaced := outsider.cluster.misplacedBuckets(); len(misplaced) != 0 {
		t.Fatalf("node %d still finds %d misplaced buckets after relocating them", outsiderIndex+1, len(misplaced))
	}
}
