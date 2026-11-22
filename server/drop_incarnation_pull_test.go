package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.5 (R8-C, trap 11): a drop entry removes one
// incarnation, and nothing of any other. That covers what is in flight for
// the name, not only the copy: a pull of a newer incarnation that a late drop
// of an older one meets — on the node pulling it, or on the node serving it —
// is not cancelled. The drop record refuses a copy of the dropped incarnation
// at the materialise, so the drop needs no cancel by name to keep it out.

// TestAnOldDropDoesNotCancelAPullOfANewIncarnation: three nodes at factor 2.
// The member outside the bucket's set, which holds no copy, pulls the live
// incarnation J from the owner. While the pulled stream waits to be
// materialised, a drop of an older incarnation I is applied — in one arm on
// the puller, in the other on the source. The pull lands J, and the source
// confirms it. Guard: cancel the name's pulls, and forget the snapshot it
// serves, before the incarnation is checked (DeleteBucketCtx's dropClearsPulls
// ahead of the check) — the puller's arm answers "dropped on this node while
// it was being pulled", the source's arm refuses the pull's later requests.
func TestAnOldDropDoesNotCancelAPullOfANewIncarnation(t *testing.T) {
	for _, arm := range []string{"on the puller", "on the source"} {
		t.Run(arm, func(t *testing.T) { runOldDropDuringAPull(t, arm == "on the source") })
	}
}

func runOldDropDuringAPull(t *testing.T, atSource bool) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	placed := ringWith(t, 2, tc.addrs...)
	bucket := bucketNamed(t, "pulled", func(string) bool { return true })
	set, err := placed.Replicas(placementOf(testDatabase, bucket).encoded, 2)
	if err != nil {
		t.Fatalf("setup: the set of %s: %v", bucket, err)
	}
	owner := nodeIndexByAddr(t, tc.addrs, set[0].ID)
	var puller int
	for index := range tc.addrs {
		if index != owner && tc.addrs[index] != set[1].ID {
			puller = index
		}
	}

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1},
		WriteConcern: "all"})
	live := lineageOf(t, tc.nodes[owner].server.core, testDatabase, bucket).Incarnation
	older := live - 1
	if live == 1 {
		older = math.MaxInt64
	}
	dropped, err := bson.Marshal(bson.M{"database": testDatabase, "name": bucket, "incarnation": int64(older)})
	if err != nil {
		t.Fatal(err)
	}
	// The older incarnation's drop, as a late stream from another sender
	// would deliver it.
	at := puller
	if atSource {
		at = owner
	}
	sender := tc.addrs[3-owner-puller]
	dropErr := make(chan error, 1)
	inject := func(database, name string) {
		if name != bucket {
			return
		}
		entry := ReplEntry{Seq: 1 << 40, Op: int32(transport.OpDeleteBucket), Bucket: bucket, Payload: dropped}
		_, err := clusterOf(tc, at).applyAsReplica(context.Background(), entry, true, sender, false)
		dropErr <- err
	}
	beforeMaterializeHook.Store(&inject)
	defer beforeMaterializeHook.Store(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pullErr := clusterOf(tc, puller).pullBucket(ctx, testDatabase, bucket, ring.Node{ID: tc.addrs[owner], Addr: tc.addrs[owner]})
	select {
	case err := <-dropErr:
		if err != nil {
			t.Fatalf("applying the drop of incarnation %d on node %d: %v", older, at+1, err)
		}
	default:
		t.Fatal("setup: the drop was never applied during the pull")
	}
	if pullErr != nil {
		t.Fatalf("a drop of incarnation %d on node %d stopped the pull of the live incarnation %d: %v", older, at+1, live, pullErr)
	}
	pulled := tc.nodes[puller].server.core
	if !pulled.bucketStoreOpen(testDatabase, bucket) {
		t.Fatalf("node %d does not serve the pulled bucket", puller+1)
	}
	if got := lineageOf(t, pulled, testDatabase, bucket).Incarnation; got != live {
		t.Fatalf("node %d landed incarnation %d, want %d", puller+1, got, live)
	}
	if resp, err := pulled.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}); err != nil || !resp.Found {
		t.Fatalf("the pulled copy lost its document: %v", err)
	}
}
