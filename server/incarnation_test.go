package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.4 (R8-A): a bucket's incarnation is minted once —
// by the owner before the append in a cluster, by createLineage on a node with
// no cluster — and every copy's state document carries that one number.

// ringWith is a ring of the given node addresses at factor rf, as every
// member derives it from the same member list.
func ringWith(t *testing.T, rf int, addrs ...string) *ring.Ring {
	t.Helper()
	placed := ring.New(rf)
	for _, addr := range addrs {
		if err := placed.AddNode(ring.Node{ID: addr, Addr: addr}); err != nil {
			t.Fatalf("setup: building a ring of %v: %v", addrs, err)
		}
	}
	return placed
}

// placedOn reports whether name in testDatabase is placed on node by r.
func placedOn(t *testing.T, r *ring.Ring, name, node string) bool {
	t.Helper()
	replicas, err := r.Replicas(placementOf(testDatabase, name).encoded, 0)
	if err != nil {
		t.Fatalf("setup: placing %s: %v", name, err)
	}
	for _, replica := range replicas {
		if replica.ID == node {
			return true
		}
	}
	return false
}

// TestEveryCopyOfABucketCarriesItsIncarnation: at factor 3, a hash and a FIFO
// bucket. Every node's state document has the same non-zero incarnation: on a
// replica that was down at the create and learned it from the owner's log, on
// a replica restarted after it (its own replay), and on a fourth node that
// joins and pulls the bucket. Guard: mint where the state document is built
// (nodestore.go, trap 10) — every node draws its own number.
func TestEveryCopyOfABucketCarriesItsIncarnation(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	joined := ringWith(t, 3, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	// Node 3 is down at the create, so it must not own either name; and the
	// joiner must be placed on both, to pull them.
	now := ringWith(t, 3, tc.addrs...)
	placed := func(name string) bool {
		return ownerOn(now, name) != tc.addrs[2] && placedOn(t, joined, name, newcomer.ID)
	}
	hashBucket := bucketNamed(t, "inc_hash", placed)
	fifoBucket := bucketNamed(t, "inc_fifo", placed)

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	// Node 3 is down at the create: it learns it from its sender's log.
	base := tc.stopNodeKeepingData(2)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: hashBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifoBucket, Type: BucketTypeFIFO})
	tc.nodes[2] = tc.startNode(2, base, tc.token)
	for _, bucket := range []string{hashBucket, fifoBucket} {
		waitForServing(t, tc, testDatabase, bucket, 3)
	}
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: hashBucket, Key: "k",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	incarnations := map[string]uint64{}
	for _, bucket := range []string{hashBucket, fifoBucket} {
		owner := lineageOf(t, tc.nodes[ownerIndexOf(t, tc, bucket)].server.core, testDatabase, bucket)
		if owner.Incarnation == 0 {
			t.Fatalf("bucket %s: the owner's copy has incarnation 0; a create logged by this build is minted one", bucket)
		}
		incarnations[bucket] = owner.Incarnation
		for index := range tc.nodes {
			if got := lineageOf(t, tc.nodes[index].server.core, testDatabase, bucket).Incarnation; got != owner.Incarnation {
				t.Errorf("bucket %s: node %d's copy has incarnation %d, the owner's %d; one create is one incarnation on every copy", bucket, index+1, got, owner.Incarnation)
			}
		}
	}

	// A replica restarts and replays its own log.
	replica := (ownerIndexOf(t, tc, hashBucket) + 1) % 3
	tc.restartNode(replica)
	tc.waitReady()
	for bucket, want := range incarnations {
		waitForServing(t, tc, testDatabase, bucket, 3)
		if got := lineageOf(t, tc.nodes[replica].server.core, testDatabase, bucket).Incarnation; got != want {
			t.Errorf("bucket %s: after a restart node %d's copy has incarnation %d, want %d", bucket, replica+1, got, want)
		}
	}

	// A fourth node joins and pulls both.
	joiner := joinNewcomer(t, tc, 0, ports)
	for bucket, want := range incarnations {
		pullNow(t, joiner.cluster(), testDatabase, bucket)
		if got := lineageOf(t, joiner.node.server.core, testDatabase, bucket).Incarnation; got != want {
			t.Errorf("bucket %s: the joiner's pulled copy has incarnation %d, want %d", bucket, got, want)
		}
	}
}

// TestABucketCreatedStandaloneHasAnIncarnationAfterItJoinsACluster (review
// B-2): a server with replication factor 1 and no seeds is standalone. It
// creates a bucket, and is restarted on its data directory as a cluster member
// (the "configuration" user-docs page: seeds make the node a member). The bucket's
// incarnation is non-zero, unchanged by the restart, and a node that joins and
// pulls it holds the same number. Guard: mint only in stampCreateLineage — the
// standalone create is never stamped, and its copy reads 0.
func TestABucketCreatedStandaloneHasAnIncarnationAfterItJoinsACluster(t *testing.T) {
	standalone := func(_ int, cfg *config.Config) { cfg.HashRing.SeedNodes = nil; cfg.HashRing.SeedFingerprints = nil }
	tc := startTestClusterTuned(t, 1, 1, standalone, nil)
	defer tc.stop()
	if tc.nodes[0].server.core.cluster != nil {
		t.Fatal("setup: a server with factor 1 and no seeds started as a cluster member")
	}
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	joined := ringWith(t, 1, tc.addrs[0], newcomer.ID)
	bucket := bucketNamed(t, "solo", func(name string) bool { return ownerOn(joined, name) == newcomer.ID })

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}})
	minted := lineageOf(t, tc.nodes[0].server.core, testDatabase, bucket).Incarnation
	if minted == 0 {
		t.Fatalf("a bucket created on a standalone server has incarnation 0; createLineage mints one when the node has no cluster")
	}

	// Restarted with seeds: a member of a cluster of one. users.json is
	// sealed under the standalone key, which a member does not load (a
	// known gap, open); the file is removed so the restart reaches the bucket at all.
	tc.configure = nil
	base := tc.stopNodeKeepingData(0)
	if err := os.Remove(filepath.Join(base, "n1", "users.json")); err != nil {
		t.Fatalf("setup: removing the standalone users file: %v", err)
	}
	tc.nodes[0] = tc.startNode(0, base, tc.token)
	tc.waitReady()
	if tc.nodes[0].server.core.cluster == nil {
		t.Fatal("setup: the restart with seeds did not make the node a cluster member")
	}
	if got := lineageOf(t, tc.nodes[0].server.core, testDatabase, bucket).Incarnation; got != minted {
		t.Errorf("after the restart as a member the bucket's incarnation is %d, want %d", got, minted)
	}

	joiner := joinNewcomer(t, tc, 0, ports)
	pullNow(t, joiner.cluster(), testDatabase, bucket)
	if got := lineageOf(t, joiner.node.server.core, testDatabase, bucket).Incarnation; got != minted {
		t.Errorf("the joiner pulled the bucket with incarnation %d, want the standalone node's %d", got, minted)
	}
}
