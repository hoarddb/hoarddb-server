package server

import (
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0455: a database drop forgets, by name, the declared factor and TTL
// registration of a bucket. A bucket of the same name created in the drop's
// window (in the database's other live incarnation) has registered its own by
// then, and must keep them.

// recreateInDropWindow builds a two-node cluster at factor 1 with a bucket in
// the first incarnation of a database, then drops that incarnation alone while
// recreate runs inside the drop's window (after the bucket is closed, before
// the registries are forgotten). It returns the owner's server and the bucket.
func recreateInDropWindow(t *testing.T, recreate CreateBucketRequest) (*Server, string) {
	t.Helper()
	tc := newTestCluster(t, 2, 1)
	t.Cleanup(tc.stop)
	const ownerIndex = 0
	owner := tc.nodes[ownerIndex]
	core := owner.server.core

	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: owedDropDatabase})
	bucket := bucketOwnedIn(t, tc, ownerIndex, owedDropDatabase, "declared")
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: owedDropDatabase, Name: bucket, Type: BucketTypeHash})
	live, err := core.liveGenerations(owedDropDatabase)
	if err != nil || len(live) != 1 {
		t.Fatalf("setup: the database's live generations are %v (%v), want one", live, err)
	}

	const source = "bug-0455-source"
	const second = int64(424243)
	base := sourcePosition(owner, source)
	pushToReceiver(t, owner, source, []ReplEntry{{Seq: base + 1, Op: int32(transport.OpCreateDB),
		Payload: requestBody(t, CreateDatabaseRequest{Name: owedDropDatabase, Generation: second})}})

	seams := generationSeams{noRelog: true, noCarried: true}
	generationSeamsHook.Store(&seams)
	t.Cleanup(func() { generationSeamsHook.Store(nil) })

	var once sync.Once
	hook := func(database string) {
		if database != owedDropDatabase {
			return
		}
		once.Do(func() {
			recreate.Database, recreate.Name, recreate.Type = owedDropDatabase, bucket, BucketTypeHash
			tc.ok(ownerIndex, transport.OpCreateBucket, recreate)
		})
	}
	afterDetachBeforeForgetHook.Store(&hook)
	t.Cleanup(func() { afterDetachBeforeForgetHook.Store(nil) })

	pushToReceiver(t, owner, source, []ReplEntry{{Seq: base + 2, Op: int32(transport.OpDeleteDB),
		Payload: requestBody(t, DeleteDatabaseRequest{Name: owedDropDatabase, Generations: live})}})
	afterDetachBeforeForgetHook.Store(nil)
	return core, bucket
}

// TestADatabaseDropKeepsTheFactorOfARecreatedBucket: the recreated bucket
// declared factor 2 on a cluster of factor 1; the drop must not reset it.
//
// Guard: forget by name (unregisterBucketFactor with no check for a bucket of
// that name served again).
func TestADatabaseDropKeepsTheFactorOfARecreatedBucket(t *testing.T) {
	core, bucket := recreateInDropWindow(t, CreateBucketRequest{ReplicationFactor: 2})
	if got := core.cluster.effectiveRF(placementOf(owedDropDatabase, bucket)); got != 2 {
		t.Fatalf("bucket %q recreated with replication_factor 2 in a drop's window has factor %d after the drop, want 2: "+
			"the drop forgot the new bucket's declaration by its name", bucket, got)
	}
}

// TestADatabaseDropKeepsTheTTLRegistrationOfARecreatedBucket is the same for a
// TTL index declared by the recreated bucket.
//
// Guard: forget by name (ttlRegistry.forget with no check for a bucket of that
// name served again).
func TestADatabaseDropKeepsTheTTLRegistrationOfARecreatedBucket(t *testing.T) {
	core, bucket := recreateInDropWindow(t, CreateBucketRequest{
		Options: map[string]interface{}{"indices": []interface{}{ttlDeclaration("CreatedAt", 3600)}}})
	for _, registered := range core.ttl.list() {
		if registered.database == owedDropDatabase && registered.bucket == bucket {
			return
		}
	}
	t.Fatalf("bucket %q recreated with a TTL index in a drop's window is not in the TTL registry after the drop, which holds %v: "+
		"the drop forgot the new bucket's registration by its name", bucket, core.ttl.list())
}

// TestADatabaseDropForgetsTheFactorOfABucketNotRecreated is the other side of
// the guard: a dropped bucket nobody recreated must still lose its
// declaration, and take the node default if a later bucket of that name
// declares nothing.
func TestADatabaseDropForgetsTheFactorOfABucketNotRecreated(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	t.Cleanup(tc.stop)
	const ownerIndex = 0
	core := tc.nodes[ownerIndex].server.core
	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: owedDropDatabase})
	bucket := bucketOwnedIn(t, tc, ownerIndex, owedDropDatabase, "plain")
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: owedDropDatabase, Name: bucket, Type: BucketTypeHash, ReplicationFactor: 2})
	key := placementOf(owedDropDatabase, bucket)
	if got := core.cluster.effectiveRF(key); got != 2 {
		t.Fatalf("setup: factor is %d, want 2", got)
	}
	tc.ok(ownerIndex, transport.OpDeleteDB, DeleteDatabaseRequest{Name: owedDropDatabase})
	if got := core.cluster.effectiveRF(key); got != 1 {
		t.Fatalf("factor of a dropped, unrecreated bucket is %d, want the node default 1", got)
	}
}
