package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// What a bucket owes — the sequences retention must
// keep for its replay, and its failed-apply carry — is forgotten by the drop
// that removes the bucket. A database drop forgets after it has closed its
// buckets and released the registry's locks, so a bucket of the same name
// created in that window, and owing a write, must not be what is forgotten.

const owedDropDatabase = "tenant"

// owedRecreate is a bucket recreated inside a database drop's window, which
// owes the write that failed in it.
type owedRecreate struct {
	tc         *testCluster
	ownerIndex int
	bucket     string
	failedSeq  uint64
}

// recreateOwingBucketDuringADrop builds the window: a database with two
// incarnations (the second created through a node this one had not heard
// from), a bucket in the first, and a drop of the first incarnation only, so
// the database stays live. In afterDetachBeforeForgetHook — the dropped bucket
// closed, its debts not yet forgotten — the same name is created again in the
// second incarnation and one of its writes fails in the store, so the new
// bucket owes it. The drop and the create are applied without their second
// paths (G-12's relog, SA-DG-14's carried drops), which would each wait on the
// learning mutex the drop holds.
func recreateOwingBucketDuringADrop(t *testing.T, beforeFailure func(*testCluster, int)) owedRecreate {
	t.Helper()
	tc := newTestCluster(t, 2, 1)
	t.Cleanup(tc.stop)
	const ownerIndex = 0
	owner := tc.nodes[ownerIndex]
	core := owner.server.core

	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: owedDropDatabase})
	bucket := bucketOwnedIn(t, tc, ownerIndex, owedDropDatabase, "owed")
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: owedDropDatabase, Name: bucket, Type: BucketTypeHash})
	live, err := core.liveGenerations(owedDropDatabase)
	if err != nil || len(live) != 1 {
		t.Fatalf("setup: the database's live generations are %v (%v), want one", live, err)
	}
	first := live[0]

	const source = "owed-test-source"
	const second = int64(424242)
	base := sourcePosition(owner, source)
	pushToReceiver(t, owner, source, []ReplEntry{{Seq: base + 1, Op: int32(transport.OpCreateDB),
		Payload: requestBody(t, CreateDatabaseRequest{Name: owedDropDatabase, Generation: second})}})

	seams := generationSeams{noRelog: true, noCarried: true}
	generationSeamsHook.Store(&seams)
	t.Cleanup(func() { generationSeamsHook.Store(nil) })

	var (
		once   sync.Once
		failed *atomic.Uint64
	)
	hook := func(database string) {
		if database != owedDropDatabase {
			return
		}
		once.Do(func() {
			tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: owedDropDatabase, Name: bucket, Type: BucketTypeHash})
			if beforeFailure != nil {
				beforeFailure(tc, ownerIndex)
			}
			failed = failNextApplyOn(t, core.kv, anySeq)
			putInto(t, tc, ownerIndex, owedDropDatabase, bucket, "owed", "o1")
		})
	}
	afterDetachBeforeForgetHook.Store(&hook)
	t.Cleanup(func() { afterDetachBeforeForgetHook.Store(nil) })

	pushToReceiver(t, owner, source, []ReplEntry{{Seq: base + 2, Op: int32(transport.OpDeleteDB),
		Payload: requestBody(t, DeleteDatabaseRequest{Name: owedDropDatabase, Generations: []int64{first}})}})
	afterDetachBeforeForgetHook.Store(nil)
	if failed == nil || failed.Load() == 0 {
		t.Fatal("setup: the recreated bucket's write never reached the store inside the drop; the test failed nothing")
	}
	return owedRecreate{tc: tc, ownerIndex: ownerIndex, bucket: bucket, failedSeq: failed.Load()}
}

// putInto writes key=value to database/bucket through node index.
func putInto(t *testing.T, tc *testCluster, index int, database, bucket, key, value string) *transport.Frame {
	t.Helper()
	return tc.frame(index, transport.OpPut, InsertRequest{
		Database: database,
		Bucket:   bucket,
		Key:      key,
		Data:     map[string]interface{}{"v": value},
	})
}

// TestADatabaseDropDoesNotForgetWhatARecreatedBucketOwes: the
// recreated bucket owes the write that failed, so it must say so after the
// drop's forget has run.
//
// Guard: forget by name (owedRegistry.dropBucket(database, name)).
func TestADatabaseDropDoesNotForgetWhatARecreatedBucketOwes(t *testing.T) {
	built := recreateOwingBucketDuringADrop(t, nil)
	core := built.tc.nodes[built.ownerIndex].server.core
	if !core.bucketOwesApplies(owedDropDatabase, built.bucket) {
		t.Fatalf("the bucket %q recreated during the drop owes its failed write at sequence %d, but bucketOwesApplies says it owes nothing: "+
			"the drop forgot the new bucket's carry by its name", built.bucket, built.failedSeq)
	}
	if lowest, owner, owed := core.owed.lowest(); !owed || lowest != built.failedSeq || owner.bucket != built.bucket {
		t.Fatalf("the owed registry holds (%d, %q, %v), want sequence %d of %q: the drop forgot the new bucket's owed sequence",
			lowest, owner.bucket, owed, built.failedSeq, built.bucket)
	}
}

// TestRetentionKeepsTheOwedWriteOfABucketRecreatedInADatabaseDrop:
// the failed write's only copy is the log, so a retention pass must keep it,
// and the restart that retries it applies it.
//
// Guard: forget by name.
func TestRetentionKeepsTheOwedWriteOfABucketRecreatedInADatabaseDrop(t *testing.T) {
	built := recreateOwingBucketDuringADrop(t, func(tc *testCluster, index int) {
		// Every entry its own segment, so a pass can cut the log right up to its bound.
		tc.nodes[index].server.core.wal.SetMaxSegmentBytes(1)
	})
	tc, ownerIndex := built.tc, built.ownerIndex
	core := tc.nodes[ownerIndex].server.core
	for _, key := range []string{"after1", "after2", "after3"} {
		if frame := putInto(t, tc, ownerIndex, owedDropDatabase, built.bucket, key, key); frame.Opcode == transport.OpError {
			t.Fatalf("write %q: %s", key, string(frame.Payload))
		}
	}
	advanceEveryStream(t, core.cluster)
	if err := core.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	first, err := core.wal.FirstSequence()
	if err != nil {
		t.Fatalf("reading the log's first sequence: %v", err)
	}
	if first > built.failedSeq {
		t.Fatalf("retention cut the log to start at %d, past the recreated bucket's failed write at %d: "+
			"the drop forgot what the new bucket owes, and that write's only copy is gone", first, built.failedSeq)
	}

	tc.restartNode(ownerIndex)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for !holdsKeyIn(tc.nodes[ownerIndex], owedDropDatabase, built.bucket, "owed") {
		if ctx.Err() != nil {
			t.Fatalf("after the restart the failed write (sequence %d) is missing from %q: replay did not retry it", built.failedSeq, built.bucket)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
