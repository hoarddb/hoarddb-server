package wire

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestGetManyTakesNoPairLock (spec section 10, test 12): a writer holds the pair of a key across a
// blocked critical section; a frame over that key and over others is answered while the lock is
// still held, because a read never takes it.
func TestGetManyTakesNoPairLock(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	held := getManyRead(t, world.near, "u0")
	slot := world.near.slot(held.EncID)
	locked, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		world.server.node.pairs.With(slot, world.near.dbID, func(*cluster.Pair) {
			close(locked)
			<-release
		})
	}()
	defer func() { <-finished }()
	defer close(release)
	<-locked

	answered := make(chan RingReply, 1)
	go func() {
		answered <- world.serve(held, getManyRead(t, world.near, "u1"), getManyRead(t, world.near, "u2"))
	}()
	select {
	case reply := <-answered:
		for i, want := range []string{"u0", "u1", "u2"} {
			if got := getManyID(t, i, reply.Answers[i]); got != want {
				t.Errorf("key %d answered %q, want %q", i, got, want)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a get-many frame waited on a pair lock a writer holds: a read must never take it")
	}
}

// TestGetManyHoldsNoPerBucketState (spec section 10, test 19, bucket density): a frame over 255
// distinct buckets keeps nothing per bucket once it is answered. The node's bucket-id cache is
// bounded and filled by lone reads too, so each bucket is admitted once through the lone path
// first; then one frame over all 255 must leave well under one live object per bucket (an entry
// kept per bucket, however small, is 255), the node-wide counters have no per-bucket series, and
// no map or slice sits in them.
func TestGetManyHoldsNoPerBucketState(t *testing.T) {
	stats := reflect.TypeOf(GetManyStats{})
	for i := range stats.NumField() {
		if kind := stats.Field(i).Type.Kind(); kind == reflect.Map || kind == reflect.Slice {
			t.Errorf("GetManyStats.%s is a %s: the counters are node-wide, never per bucket", stats.Field(i).Name, kind)
		}
	}
	world := newGetManyWorld(t, u70AllowAll{})
	node, ctx := world.server.node, t.Context()
	const buckets = maxGetManyKeys
	reads := make([]RingRead, buckets)
	for i := range reads {
		name := fmt.Sprintf("Density%03d", i)
		if _, err := node.CreateBucket(ctx, u30iDB, name, Field{Name: "Title", Type: "string"}); err != nil {
			t.Fatal(err)
		}
		reads[i] = getManyRead(t, world.bucket(t, name), "absent")
		if _, err := world.server.admit(ctx, RingKeyedRequest{Read: &reads[i]}); err != nil {
			t.Fatal(err)
		}
	}
	live := func() uint64 {
		runtime.GC()
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		return memory.HeapObjects
	}
	before := live()
	// The answers are the caller's, and are dropped here before the count.
	if outcome := world.serve(reads...).Outcome; outcome != ringBatchOK {
		t.Fatalf("outcome %q", outcome)
	}
	after := live()
	if after > before && after-before > buckets/4 {
		t.Fatalf("a frame over %d distinct buckets left %d more live objects: it keeps state per bucket", buckets, after-before)
	}
	if size := node.bucketIDs.len(); size > bucketIDCacheSize {
		t.Fatalf("the bucket-id cache holds %d ids, its bound is %d", size, bucketIDCacheSize)
	}
}
