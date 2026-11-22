package server

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// memtableMixedCluster is three nodes at replication factor 3, two at the
// test default of 64 MiB and node 3 at 32 MiB: the misconfiguration this
// cluster is about, which a template start on unequal hosts can produce.
func memtableMixedCluster(t *testing.T) *testCluster {
	t.Helper()
	return newTestClusterWith(t, 3, 3, func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = config.MinMemtableMB
		}
	})
}

// largeBatch is documents totalling count x size bytes.
func largeBatch(database, bucket string, count, size int) BatchInsertRequest {
	docs := make([]map[string]interface{}, count)
	for i := range docs {
		docs[i] = map[string]interface{}{"_id_": strings.Repeat("k", 4) + string(rune('A'+i%26)) + string(rune('a'+i/26)), "body": strings.Repeat("x", size)}
	}
	return BatchInsertRequest{Database: database, Bucket: bucket, Data: docs, WriteConcern: "one"}
}

// TestASmallerReplicaStallsOnABatchItsOwnerCommitted: the owner, at
// 64 MiB, commits a 7.2 MB batch that a 32 MiB replica cannot commit in one
// transaction (its bound is 5 MB). The replica must not take that for the
// operation's answer and move on: it holds none of the batch, so marking the
// entry applied silently loses it. It must refuse the push, so the owner's
// stream to it stalls with a reason that names the memtable size and the fix.
//
// It must keep refusing: the first push carries the owner's outcome from
// memory, and every re-send after it is rebuilt from the log, so the stall is
// sampled across several re-sends and again after the owner restarts, when
// nothing of the first push is left in memory.
func TestASmallerReplicaStallsOnABatchItsOwnerCommitted(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	tc.ok(0, transport.OpBatchPut, largeBatch("app", bucket, 60, 120_000))
	ownerSeq := owner.walLastSequence()

	reason := tc.stallStaysOnSmallReplica(t, bucket, 0, ownerSeq, "before any restart")
	for _, want := range []string{"this member runs a 32 MiB memtable", "needs at least 64 MiB", "Set storage.memtable_mb", "SIGTERM"} {
		if !strings.Contains(reason, want) {
			t.Errorf("the owner's stall reason for the 32 MiB replica %q does not contain %q", reason, want)
		}
	}

	tc.restartNode(0)
	tc.stallStaysOnSmallReplica(t, bucket, 0, ownerSeq, "after the owner restarted")

	// The fix the reason names: resize the member and restart it. The stream
	// then delivers the batch whole.
	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.restartNode(2)
	owner = tc.nodes[0].server.core.cluster
	toSmall := owner.peerFor(mustNode(t, owner, tc.addrs[2]))
	deadline := time.Now().Add(60 * time.Second)
	held := int64(0)
	for time.Now().Before(deadline) {
		if held = tc.bucketCountOn(2, "app", bucket); held == 60 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if held != 60 {
		state, reason := toSmall.streamStatus()
		t.Fatalf("node 3 resized to 64 MiB and restarted holds %d of the batch's 60 documents: stream %v, %q", held, state, reason)
	}
}

// TestASmallerReplicaStallsOnABorderlineBatch: a request just over the 32 MiB
// bound. The stall must hold across re-sends from the log exactly as a clearly
// oversized batch's does.
func TestASmallerReplicaStallsOnABorderlineBatch(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	bound := config.MemtableBatchBytes(config.MinMemtableMB)
	request := borderlineBatch(t, "app", bucket, bound+512)
	tc.ok(0, transport.OpBatchPut, request)
	ownerSeq := owner.walLastSequence()

	tc.stallStaysOnSmallReplica(t, bucket, 0, ownerSeq, "before any restart")

	// A batch this size is logged on the replica before its apply refuses it.
	// Restarted at the same size, the replica must not count that log entry
	// as applied: its replay refuses the batch again, and the stall holds.
	tc.restartNode(2)
	tc.stallStaysOnSmallReplica(t, bucket, 0, ownerSeq, "after the replica restarted unresized")
}

// TestASmallerReplicaCrashingBeforeItsRefusalRecordStillStalls is BUG-0248.
// The 32 MiB replica logs the owner's 7.2 MB batch, its apply refuses it, and
// the node dies before the durable record of that refusal commits: the fault
// hook fails the record and the node is stopped before anything else. Its log
// holds the entry, its store holds none of the batch, and no record says so.
// Restarted unresized, it must not count the logged entry as applied: the
// owner's re-send must reach it and be refused with the memtable stall, and,
// resized to the owner's size, it must then hold the batch whole.
func TestASmallerReplicaCrashingBeforeItsRefusalRecordStillStalls(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	small := tc.nodes[2].server.core.cluster
	var crashed atomic.Bool
	hook := func(c *Cluster, _ string, _ uint64) error {
		if c != small {
			return nil
		}
		crashed.Store(true)
		return errors.New("injected crash before the size refusal record commits")
	}
	sizeRefusalRecordFaultHook.Store(&hook)
	defer sizeRefusalRecordFaultHook.Store(nil)

	const documents = 60
	tc.ok(0, transport.OpBatchPut, largeBatch("app", bucket, documents, 120_000))
	ownerSeq := owner.walLastSequence()
	for deadline := time.Now().Add(15 * time.Second); !crashed.Load(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the 32 MiB replica never reached its size refusal record for the owner's batch")
		}
	}
	if records := tc.sizeRefusalRecords(2); records != 0 {
		t.Fatalf("setup: the crashing replica holds %d size refusal records; the crash is before the record", records)
	}
	smallDir := tc.stopNodeKeepingData(2)
	sizeRefusalRecordFaultHook.Store(nil)

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	if !tc.nodes[2].server.core.cluster.shouldApply(tc.addrs[0], ownerSeq) {
		t.Fatalf("the 32 MiB replica, restarted unresized after a crash before its refusal record, marked the owner's sequence %d applied with none of the batch written", ownerSeq)
	}
	// The restarted replica's replay records the refusal the crash took, so
	// every re-send is refused before it is logged, which
	// stallStaysOnSmallReplica checks.
	for deadline := time.Now().Add(30 * time.Second); tc.sizeRefusalRecords(2) != 1; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the restarted 32 MiB replica never recorded its refusal of the owner's re-sent batch")
		}
	}
	tc.stallStaysOnSmallReplica(t, bucket, 0, ownerSeq, "after the replica crashed before its refusal record and restarted unresized")

	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.restartNode(2)
	deadline := time.Now().Add(60 * time.Second)
	held := int64(0)
	for ; time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if held = tc.bucketCountOn(2, "app", bucket); held == documents {
			break
		}
	}
	if held != documents {
		t.Fatalf("node 3 resized to 64 MiB and restarted holds %d of the batch's %d documents", held, documents)
	}
}

// TestASmallerReplicaCrashingBeforeItsRefusalRecordHoldsAQueueBatchOnce is
// BUG-0248 for the bucket types whose batch pushes elements: a second apply of
// the batch is not an overwrite there, so a replica that applies it twice
// holds every element twice. The crash is the one above, before the size
// refusal record; the replica restarts unresized and stalls, and its log must
// still hold the batch once, so that, resized and restarted, it holds what
// its owner holds, in the owner's order.
func TestASmallerReplicaCrashingBeforeItsRefusalRecordHoldsAQueueBatchOnce(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		t.Run(string(bucketType), func(t *testing.T) {
			tc := memtableMixedCluster(t)
			defer tc.stop()
			owner := tc.nodes[0].server.core.cluster
			bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: bucketType})

			small := tc.nodes[2].server.core.cluster
			var crashed atomic.Bool
			hook := func(c *Cluster, _ string, _ uint64) error {
				if c != small {
					return nil
				}
				crashed.Store(true)
				return errors.New("injected crash before the size refusal record commits")
			}
			sizeRefusalRecordFaultHook.Store(&hook)
			defer sizeRefusalRecordFaultHook.Store(nil)

			const documents = 60
			tc.ok(0, transport.OpBatchPut, largeBatch("app", bucket, documents, 120_000))
			ownerSeq := owner.walLastSequence()
			for deadline := time.Now().Add(15 * time.Second); !crashed.Load(); time.Sleep(20 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the 32 MiB replica never reached its size refusal record for the owner's batch")
				}
			}
			smallDir := tc.stopNodeKeepingData(2)
			sizeRefusalRecordFaultHook.Store(nil)

			tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
			loggedAtRestart := tc.nodes[2].server.core.cluster.walLastSequence()
			for deadline := time.Now().Add(30 * time.Second); tc.sizeRefusalRecords(2) != 1; time.Sleep(100 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the restarted 32 MiB replica never recorded its refusal of the owner's batch")
				}
			}
			tc.stallStaysOnSmallReplica(t, bucket, 0, ownerSeq, "after the replica crashed before its refusal record and restarted unresized")
			if grown := tc.nodes[2].server.core.cluster.walLastSequence() - loggedAtRestart; grown != 0 {
				t.Errorf("the restarted 32 MiB replica logged the owner's re-sent batch again (%d entries); its log already holds it", grown)
			}

			tc.configure = func(index int, cfg *config.Config) {
				if index == 2 {
					cfg.Storage.MemtableMB = 64
				}
			}
			tc.restartNode(2)
			for deadline := time.Now().Add(60 * time.Second); tc.nodes[2].server.core.cluster.shouldApply(tc.addrs[0], ownerSeq); time.Sleep(200 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("node 3 resized to 64 MiB and restarted never moved past the owner's sequence %d", ownerSeq)
				}
			}
			if held, owned := tc.bucketCountOn(2, "app", bucket), tc.bucketCountOn(0, "app", bucket); held != owned {
				t.Fatalf("node 3 resized to 64 MiB and restarted holds %d elements of the %s bucket, its owner %d", held, bucketType, owned)
			}
			if held, owned := tc.popOrderLocal(t, 2, bucket), tc.popOrderLocal(t, 0, bucket); !slices.Equal(held, owned) {
				t.Fatalf("node 3 resized to 64 MiB and restarted holds the %s bucket in pop order %v, its owner %v", bucketType, held, owned)
			}
		})
	}
}

// TestASmallerReplicaHoldsABatchAndTheEntryAfterItOnceAfterTheResize: the
// 32 MiB replica is down while its owner commits a batch over the replica's
// entry bound and then one small put, so its catch-up carries the put behind
// the batch. It must stall on the batch and hold neither, restarted
// unresized, and resized and restarted it must hold both, once. The size
// refusal handling assumes nothing of the source above the refused entry is
// in the replica's log; this pins the case where an entry after it was
// already sent (BUG-0248; the run admission of BUG-0521 logs it).
func TestASmallerReplicaHoldsABatchAndTheEntryAfterItOnceAfterTheResize(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "seed", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	smallDir := tc.stopNodeKeepingData(2)
	const documents = 27_000
	batch := smallDocumentBatch("app", bucket, documents)
	batch.WriteConcern = "one"
	frame := tc.ok(0, transport.OpBatchPut, batch)
	var answer BatchInsertResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil || !answer.Success {
		t.Fatalf("setup: the 64 MiB owner did not commit %d small documents: %s (%v)", documents, frame.Payload, err)
	}
	batchSeq := owner.walLastSequence()
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "after", Data: map[string]interface{}{"n": 2}, WriteConcern: "one"})

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	tc.stallStaysOnSmallReplica(t, bucket, 1, batchSeq, "after node 3 started")
	tc.restartNode(2)
	tc.stallStaysOnSmallReplica(t, bucket, 1, batchSeq, "after node 3 restarted unresized")

	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.restartNode(2)
	afterSeq := owner.walLastSequence()
	for deadline := time.Now().Add(60 * time.Second); tc.nodes[2].server.core.cluster.shouldApply(tc.addrs[0], afterSeq); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("node 3 resized to 64 MiB and restarted never moved past the owner's sequence %d", afterSeq)
		}
	}
	if held, owned := tc.bucketCountOn(2, "app", bucket), tc.bucketCountOn(0, "app", bucket); held != owned || owned != documents+2 {
		t.Fatalf("node 3 resized to 64 MiB and restarted holds %d documents of the bucket, its owner %d (want %d)", held, owned, documents+2)
	}
}

// popOrderLocal is the _id_ of every element of a fifo, lifo or heap bucket
// in one node's own copy, in the order a pop takes them.
func (tc *testCluster) popOrderLocal(t *testing.T, index int, bucket string) []string {
	t.Helper()
	resp, err := tc.nodes[index].server.core.Peek(PeekRequest{Database: "app", Bucket: bucket, Limit: maxPeekLimit})
	if err != nil {
		t.Fatalf("node %d: local peek of %q: %v", index+1, bucket, err)
	}
	order := make([]string, 0, len(resp.Items))
	for _, element := range resp.Items {
		id, ok := element.Lookup("_id_").StringValueOK()
		if !ok {
			t.Fatalf("node %d: an element of %q has no _id_: %v", index+1, bucket, element)
		}
		order = append(order, id)
	}
	return order
}

// TestASmallerReplicaStallsOnABatchItsOwnersStoreFailed is BUG-0247. The owner's
// store fails its first commit of a batch above the byte threshold, so the
// batch is owed, not committed, and the owner records no outcome for it. The
// smaller replica is re-sent the entry from the owner's log. It must not take
// its own batch-too-large as the answer and mark the sequence applied: it must
// stall, as it does for a batch the owner committed.
func TestASmallerReplicaStallsOnABatchItsOwnersStoreFailed(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	// Fail the owner's first commit after this point, and only the owner's: the
	// hook sees the storage of every node in this process.
	ownerKV := tc.nodes[0].server.core.kv
	var failed atomic.Bool
	hook := func(kv storage.KV, _ uint16, _ uint64, _ uint64) error {
		if kv == ownerKV && failed.CompareAndSwap(false, true) {
			return errors.New("injected store failure on the owner")
		}
		return nil
	}
	store.ApplyFaultHook.Store(&hook)
	defer store.ApplyFaultHook.Store(nil)

	bound := config.MemtableBatchBytes(config.MinMemtableMB)
	tc.frame(0, transport.OpBatchPut, borderlineBatch(t, "app", bucket, bound+512))
	if !failed.Load() {
		t.Fatal("the hook never failed the owner's commit of the batch")
	}
	ownerSeq := owner.walLastSequence()

	// The 32 MiB replica must not mark the owner's sequence applied: it can
	// never commit the batch. Its re-sent entry is sampled for 15 s before the
	// stall itself is checked, so a replica that marks it applied fails here.
	small := tc.nodes[2].server.core.cluster
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if held := tc.bucketCountOn(2, "app", bucket); held != 0 {
			t.Fatalf("the 32 MiB replica holds %d documents of a batch the owner has not applied", held)
		}
		if !small.shouldApply(tc.addrs[0], ownerSeq) {
			t.Fatalf("the 32 MiB replica marked the owner's sequence %d applied after the owner's store failed the batch", ownerSeq)
		}
	}

	// Replay is the owed apply's only retry, and it runs at start: restarting
	// the owner makes it apply the batch. Then the replica's stall is the
	// memtable one the other tests check.
	tc.restartNode(0)
	owner = tc.nodes[0].server.core.cluster
	toSmall := owner.peerFor(mustNode(t, owner, tc.addrs[2]))
	for deadline := time.Now().Add(120 * time.Second); ; time.Sleep(250 * time.Millisecond) {
		if _, reason := toSmall.streamStatus(); strings.Contains(reason, "memtable") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the owner's replay did not apply the batch within 120 s; the 32 MiB replica never reached its memtable stall")
		}
	}
	tc.stallStaysOnSmallReplica(t, bucket, 0, ownerSeq, "after the owner's replay applied the batch")
}

// TestASmallerReplicaStallsOnABatchOverItsEntryBoundButUnderTheByteThreshold
// is the verifier's probe made deterministic. 27,000 small documents
// are 54,002 entries live (each document and its read-repair version, the
// state document, Badger's commit marker): over the 32 MiB member's entry
// bound of 52,428 and far under the byte threshold at which the owner records
// that it committed a batch. Node 3 is down while the owner commits it, so it
// first meets the batch as a re-send rebuilt from the owner's log. That
// re-send must say the owner committed it, so node 3 refuses and stalls
// rather than consume its own batch-too-large as the answer and stay at one
// document of 27,001 for good. The stall is sampled across re-sends and again
// after the owner restarts, when every re-send is rebuilt from its log.
func TestASmallerReplicaStallsOnABatchOverItsEntryBoundButUnderTheByteThreshold(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "seed", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	smallDir := tc.stopNodeKeepingData(2)

	const documents = 27_000
	batch := smallDocumentBatch("app", bucket, documents)
	batch.WriteConcern = "one"
	if encoded, err := bson.Marshal(batch); err != nil || int64(len(encoded)) >= largeCommitBytes {
		t.Fatalf("the batch must be under the byte threshold of %d: encoded %d bytes (%v)", largeCommitBytes, len(encoded), err)
	}
	frame := tc.ok(0, transport.OpBatchPut, batch)
	var answer BatchInsertResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil || !answer.Success {
		t.Fatalf("the 64 MiB owner did not commit %d small documents: %s (%v)", documents, frame.Payload, err)
	}
	ownerSeq := owner.walLastSequence()

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	tc.stallStaysOnSmallReplica(t, bucket, 1, ownerSeq, "after node 3 started")
	tc.restartNode(0)
	tc.stallStaysOnSmallReplica(t, bucket, 1, ownerSeq, "after the owner restarted")

	// Node 3 logged the entry at the first re-send, before its apply refused
	// it, and the entry stays above its mark. Its replay stamps each
	// document's read-repair version as its live apply does, so the batch is
	// as many entries on replay as live: restarted unresized, it refuses the
	// batch again without consuming it and still stalls on it.
	tc.restartNode(2)
	tc.stallStaysOnSmallReplica(t, bucket, 1, ownerSeq, "after node 3 restarted unresized")

	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.restartNode(2)
	tc.requireCounts(t, bucket, documents+1, "after node 3 was resized to 64 MiB and restarted")
}

// TestAnEntryCountStallNamesTheMemtableTheBatchNeeds checks the stall reason on the
// same probe: 27,000 small documents are about 1 MB, far under the 32 MiB
// member's byte bound, and over its entry bound. The stall's reason used to
// size the needed memtable from the bytes alone and say the batch "needs at
// least 32 MiB" of a member already running 32 MiB. It must name the 64 MiB
// the batch needs.
func TestAnEntryCountStallNamesTheMemtableTheBatchNeeds(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "seed", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	smallDir := tc.stopNodeKeepingData(2)
	batch := smallDocumentBatch("app", bucket, 27_000)
	batch.WriteConcern = "one"
	tc.ok(0, transport.OpBatchPut, batch)
	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)

	toSmall := owner.peerFor(mustNode(t, owner, tc.addrs[2]))
	deadline := time.Now().Add(30 * time.Second)
	var reason string
	for time.Now().Before(deadline) {
		if _, reason = toSmall.streamStatus(); strings.Contains(reason, "memtable") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(reason, "memtable") {
		t.Fatalf("the owner's stream to the 32 MiB replica never stalled on the memtable: %q", reason)
	}
	if !strings.Contains(reason, "needs at least 64 MiB") || strings.Contains(reason, "needs at least 32 MiB") {
		t.Errorf("the stall reason does not name the 64 MiB the batch needs: %q", reason)
	}
}

// TestBatchTooLargeReason pins the words of the stall by the batch's two
// bounds: the needed memtable is the larger of what the bytes need
// and what the keys need, and a member that already runs that size is told to
// restart, not to resize.
func TestBatchTooLargeReason(t *testing.T) {
	smallDocuments := func(count int) ReplEntry {
		payload, err := bson.Marshal(smallDocumentBatch("app", "b", count))
		if err != nil {
			t.Fatalf("encoding %d small documents: %v", count, err)
		}
		return ReplEntry{Seq: 7, Op: int32(transport.OpBatchPut), Bucket: "b", Payload: payload}
	}
	largeDocuments := func(count, size int) ReplEntry {
		payload, err := bson.Marshal(largeBatch("app", "b", count, size))
		if err != nil {
			t.Fatalf("encoding %d large documents: %v", count, err)
		}
		return ReplEntry{Seq: 7, Op: int32(transport.OpBatchPut), Bucket: "b", Payload: payload}
	}

	tests := []struct {
		name    string
		ownMB   int
		entry   ReplEntry
		want    []string
		wantNot []string
	}{
		{
			name: "entry count over a 32 MiB member's bound, bytes far under it", ownMB: 32, entry: smallDocuments(27_000),
			want:    []string{"runs a 32 MiB memtable", "needs at least 64 MiB", "54,002 keys", "52,428", "Set storage.memtable_mb", "SIGTERM"},
			wantNot: []string{"needs at least 32 MiB"},
		},
		{
			name: "bytes over a 32 MiB member's bound", ownMB: 32, entry: largeDocuments(60, 120_000),
			want:    []string{"runs a 32 MiB memtable", "needs at least 64 MiB", "Set storage.memtable_mb"},
			wantNot: []string{"needs at least 32 MiB"},
		},
		{
			name: "a member already at the size the batch needs is told to restart", ownMB: 64, entry: smallDocuments(27_000),
			want:    []string{"runs a 64 MiB memtable", "already", "restart", "SIGTERM", "from its own log"},
			wantNot: []string{"Set storage.memtable_mb", "needs at least"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reason := batchTooLargeReason(tc.ownMB, tc.entry)
			for _, want := range tc.want {
				if !strings.Contains(reason, want) {
					t.Errorf("reason %q does not contain %q", reason, want)
				}
			}
			for _, unwanted := range tc.wantNot {
				if strings.Contains(reason, unwanted) {
					t.Errorf("reason %q contains %q", reason, unwanted)
				}
			}
		})
	}
}

// stopNodeKeepingData stops a node and returns the base directory startNode
// brings it back on.
func (tc *testCluster) stopNodeKeepingData(index int) string {
	tc.t.Helper()
	base := filepath.Dir(tc.nodes[index].cfg.Storage.DataDir)
	_ = tc.nodes[index].client.Close()
	tc.nodes[index].client = nil
	if err := tc.nodes[index].server.Stop(); err != nil {
		tc.t.Fatalf("stopping node %d: %v", index+1, err)
	}
	tc.nodes[index].server = nil
	return base
}

// sizeRefusalRecords counts the durable stall records (kind s) on one node.
func (tc *testCluster) sizeRefusalRecords(index int) int {
	tc.t.Helper()
	snapshot := tc.nodes[index].server.core.kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSReplication, []byte{sizeRefusalRecordKind})
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyPrefixEnd(lower)})
	defer iterator.Close()
	records := 0
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		records++
	}
	return records
}

// prefixedSmallBatch is count small documents keyed under prefix, at w=one.
func prefixedSmallBatch(database, bucket, prefix string, count int) BatchInsertRequest {
	batch := smallDocumentBatch(database, bucket, count)
	for _, document := range batch.Data {
		document["_id_"] = prefix + document["_id_"].(string)
	}
	batch.WriteConcern = "one"
	return batch
}

// TestEqualMemtablesRecordNoSizeRefusalUnderLargeEntryCountBatches guards the
// other side of the entry-count fix: with every member at the same memtable, the
// batches the owner now records as large commits by their key count, re-sent
// from its log to a member that was down and pushed live to one that was up,
// apply on every member. None stalls and none writes a stall record.
func TestEqualMemtablesRecordNoSizeRefusalUnderLargeEntryCountBatches(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	// 14,000 documents are 28,001 keys live, over largeCommitEntries; 50,000
	// are 100,001, just under the 64 MiB bound of 104,857.
	written, ownerSeqs := 0, []uint64{}
	write := func(prefix string, count int) {
		frame := tc.ok(0, transport.OpBatchPut, prefixedSmallBatch("app", bucket, prefix, count))
		var answer BatchInsertResponse
		if err := bson.Unmarshal(frame.Payload, &answer); err != nil || !answer.Success {
			t.Fatalf("batch %s of %d documents was not committed: %s (%v)", prefix, count, frame.Payload, err)
		}
		written += count
		ownerSeqs = append(ownerSeqs, owner.walLastSequence())
	}
	downDir := tc.stopNodeKeepingData(2)
	write("a", 14_000)
	write("b", 50_000)
	// The owner restarts before node 3 returns, so node 3 gets those two
	// from re-sends rebuilt from the log, carrying the recorded outcome.
	tc.restartNode(0)
	owner = tc.nodes[0].server.core.cluster
	tc.nodes[2] = tc.startNode(2, downDir, tc.token)
	write("c", 14_000)
	write("d", 14_000)
	tc.requireCounts(t, bucket, int64(written), "after the batches")

	st, err := tc.nodes[0].server.core.bucketStateNamed("app", bucket)
	if err != nil {
		t.Fatalf("reading the owner's bucket state: %v", err)
	}
	for _, seq := range ownerSeqs {
		if !st.CommittedLarge(seq) {
			t.Fatalf("the owner did not record sequence %d as a large commit (LargeCommits %v): the batches do not exercise the entry threshold", seq, st.LargeCommits)
		}
	}
	for index := range tc.nodes {
		if records := tc.sizeRefusalRecords(index); records != 0 {
			t.Errorf("node %d wrote %d stall records with every member at the same memtable", index+1, records)
		}
		cluster := tc.nodes[index].server.core.cluster
		for _, peer := range tc.addrs {
			if peer == tc.addrs[index] {
				continue
			}
			if state, reason := cluster.peerFor(mustNode(t, cluster, peer)).streamStatus(); strings.Contains(reason, "memtable") {
				t.Errorf("node %d's stream to %s stalls on the memtable with every member at the same size: %v %q", index+1, peer, state, reason)
			}
		}
	}
}

// TestAnOwnerRefusedEntryCountBatchIsConsumedOnEveryMember guards the consumed-refusal rule
// against the entry-count fix: a batch the owner itself refuses for its entry count
// committed nothing, so it is not recorded as a large commit, and its re-send
// from the log carries no owner outcome. Node 3, down while the owner refused
// it and until the owner restarted, meets it only as that re-send, and must
// take its own refusal for the answer and move on: no stall, no stall record,
// and a later write reaches it.
func TestAnOwnerRefusedEntryCountBatchIsConsumedOnEveryMember(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	downDir := tc.stopNodeKeepingData(2)
	// 110,000 small documents reach the 64 MiB owner's bound of 104,857 entries.
	entryBoundIn(t, tc.frame(0, transport.OpBatchPut, prefixedSmallBatch("app", bucket, "r", 110_000)))
	refusedSeq := owner.walLastSequence()
	st, err := tc.nodes[0].server.core.bucketStateNamed("app", bucket)
	if err != nil {
		t.Fatalf("reading the owner's bucket state: %v", err)
	}
	if st.CommittedLarge(refusedSeq) {
		t.Fatalf("the owner recorded its own refused batch, sequence %d, as a large commit", refusedSeq)
	}
	// Restarted, the owner holds nothing of the entry in memory: what node 3
	// gets is the re-send rebuilt from its log, the path the record decides.
	tc.restartNode(0)
	owner = tc.nodes[0].server.core.cluster
	tc.nodes[2] = tc.startNode(2, downDir, tc.token)
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "after", Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	tc.requireCounts(t, bucket, 1, "after the refusal and one later write")
	if tc.nodes[2].server.core.cluster.shouldApply(tc.addrs[0], refusedSeq) {
		t.Errorf("node 3 did not consume the owner's refused sequence %d", refusedSeq)
	}
	if records := tc.sizeRefusalRecords(2); records != 0 {
		t.Errorf("node 3 wrote %d stall records for a batch its owner refused", records)
	}
	if state, reason := owner.peerFor(mustNode(t, owner, tc.addrs[2])).streamStatus(); strings.Contains(reason, "memtable") {
		t.Errorf("the stream to node 3 stalls on a batch its owner refused: %v %q", state, reason)
	}
}

// TestTheEntryThresholdCoversABatchTheOwnerFirstCommitsAtReplay pins
// largeCommitEntries' quarter. Badger fits a batch of K keys when
// K+2 is under its entry bound (its count starts at 1 for the commit marker),
// and a replica's live apply of N documents is 2N+1 keys: each document, its
// read-repair version, and the state document. An owner that first commits
// the entry at replay writes N+1. The smallest N the 32 MiB replica refuses
// must be recorded from the owner's replay count, which half the bound does
// not do by one document.
func TestTheEntryThresholdCoversABatchTheOwnerFirstCommitsAtReplay(t *testing.T) {
	bound := storage.BatchEntryBound(config.MemtableBatchBytes(config.MinMemtableMB))
	documents := (bound - 1) / 2
	if live := 2*documents + 1; live+2 < bound {
		t.Fatalf("arithmetic: %d documents are %d keys live, which a %d-entry bound fits", documents, live, bound)
	}
	if replay := documents + 1; replay <= largeCommitEntries {
		t.Fatalf("a batch of %d documents that the 32 MiB replica refuses live (%d keys against %d) is %d keys at the owner's replay, not over largeCommitEntries %d: its re-send would carry no owner outcome",
			documents, 2*documents+1, bound, replay, largeCommitEntries)
	}
}

// borderlineBatch is a batch whose encoded request is target bytes, give or
// take a document's field overhead.
func borderlineBatch(t *testing.T, database, bucket string, target int64) BatchInsertRequest {
	t.Helper()
	const count = 40
	request := largeBatch(database, bucket, count, 1)
	encoded, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encoding the batch: %v", err)
	}
	perDoc := int(target-int64(len(encoded))) / count
	request = largeBatch(database, bucket, count, 1+perDoc)
	if encoded, err = bson.Marshal(request); err != nil {
		t.Fatalf("encoding the batch: %v", err)
	}
	if size := int64(len(encoded)); size <= target-count || size > target+count {
		t.Fatalf("the borderline batch encodes to %d bytes, not about %d", size, target)
	}
	return request
}

// stallStaysOnSmallReplica waits for the owner's stream to node 3 to stall on
// the memtable, then samples it every 300 ms for at least 6 s and until the
// owner has re-sent the entry from its log at least twice (at most 60 s, as
// the stream's retry backs off): at every sample the replica has not marked
// ownerSeq applied, holds nothing of the bucket, and the stream still stalls
// on the memtable, and the replica's log has not grown: a re-send it cannot
// apply is refused before it is logged. heldBefore is what the
// replica held of the bucket before the batch. It returns the reason.
func (tc *testCluster) stallStaysOnSmallReplica(t *testing.T, bucket string, heldBefore int64, ownerSeq uint64, when string) string {
	t.Helper()
	owner := tc.nodes[0].server.core.cluster
	small := tc.nodes[2].server.core.cluster
	toSmall := owner.peerFor(mustNode(t, owner, tc.addrs[2]))
	deadline := time.Now().Add(15 * time.Second)
	var state streamState
	var reason string
	for time.Now().Before(deadline) {
		if state, reason = toSmall.streamStatus(); strings.Contains(reason, "memtable") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(reason, "memtable") {
		t.Fatalf("%s: the owner's stream to the 32 MiB replica never stalled on the memtable: state %v, reason %q", when, state, reason)
	}
	readsBefore := toSmall.logReads.Load()
	smallLogBefore := small.walLastSequence()
	resent := func() uint64 { return toSmall.logReads.Load() - readsBefore }
	sampled := time.Now()
	for ; time.Since(sampled) < 6*time.Second || resent() < 2; time.Sleep(300 * time.Millisecond) {
		if time.Since(sampled) > 60*time.Second {
			t.Fatalf("%s: the owner read its log for the 32 MiB replica %d times in 60 s; the stall was not re-tested by a re-send", when, resent())
		}
		if !small.shouldApply(tc.addrs[0], ownerSeq) {
			t.Fatalf("%s: the 32 MiB replica marked the owner's sequence %d applied although it could not commit the batch (%.1f s into sampling)",
				when, ownerSeq, time.Since(sampled).Seconds())
		}
		if held := tc.bucketCountOn(2, "app", bucket); held != heldBefore {
			t.Fatalf("%s: the 32 MiB replica holds %d documents of the bucket, %d before a batch it cannot commit whole", when, held, heldBefore)
		}
		if state, reason = toSmall.streamStatus(); !strings.Contains(reason, "memtable") {
			t.Fatalf("%s: the stream to the 32 MiB replica stopped stalling on the memtable %.1f s into sampling: state %v, reason %q",
				when, time.Since(sampled).Seconds(), state, reason)
		}
	}
	if grown := small.walLastSequence() - smallLogBefore; grown != 0 {
		t.Fatalf("%s: the 32 MiB replica's log grew by %d entries over %d re-sends it refused", when, grown, resent())
	}
	return reason
}

// bucketCountOn is the count one node answers from its own copy of a bucket.
func (tc *testCluster) bucketCountOn(index int, database, bucket string) int64 {
	tc.t.Helper()
	frame := tc.ok(index, transport.OpCount, CountRequest{Database: database, Bucket: bucket})
	var resp CountResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		tc.t.Fatalf("decoding the count from node %d: %v", index+1, err)
	}
	return resp.Count
}

// TestAnOwnerRefusedBatchDoesNotStallItsReplicas: a batch too large for the
// owner itself is refused to the client exactly as before, and that refusal is
// the operation's answer on every member: no replica stalls on it.
func TestAnOwnerRefusedBatchDoesNotStallItsReplicas(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	frame := tc.frame(0, transport.OpBatchPut, largeBatch("app", bucket, 100, 120_000))
	refusal := errorAnswerOf(t, frame, "a 12 MB batch at a 64 MiB owner")
	if transport.ErrorCode(refusal.Code) != transport.ErrBatchTooLarge ||
		!strings.Contains(refusal.Message, "so none of it was written.") ||
		!strings.Contains(refusal.Message, "The store commits at most 10,066,329 bytes") {
		t.Fatalf("a 12 MB batch at a 64 MiB owner was not refused with BATCH_TOO_LARGE and its bound: code %d: %s", refusal.Code, refusal.Message)
	}
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "after", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	for _, index := range []int{1, 2} {
		if state, reason := owner.peerFor(mustNode(t, owner, tc.addrs[index])).streamStatus(); strings.Contains(reason, "memtable") {
			t.Errorf("the stream to node %d stalled on a batch the owner itself refused: %v %s", index+1, state, reason)
		}
	}
}

// TestASizeRefusalRecordDoesNotStallABatchItsOwnerRefused: a replica's replay
// cannot tell a batch its owner committed from one its owner refused, so the
// size refusal record replay writes for a logged batch it refuses (BUG-0248)
// can stand for a batch the owner refused, when a crash took the replica's
// consume of that refusal. The record must then not refuse the owner's
// re-send: the re-send says the owner did not commit it, and on a member at
// the owner's own size only a resize past the owner's would lift that stall,
// whose reason says the owner committed the batch. The record is
// written here before the batch reaches the replica, as that replay leaves it.
func TestASizeRefusalRecordDoesNotStallABatchItsOwnerRefused(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeFIFO})

	refusedSeq := owner.walLastSequence() + 1
	for _, index := range []int{1, 2} {
		if err := tc.nodes[index].server.core.cluster.recordSizeRefusal(tc.addrs[0], refusedSeq); err != nil {
			t.Fatalf("recording node %d's size refusal of the owner's sequence %d: %v", index+1, refusedSeq, err)
		}
	}
	frame := tc.frame(0, transport.OpBatchPut, largeBatch("app", bucket, 100, 120_000))
	if !strings.Contains(string(frame.Payload), "This batch exceeds the largest batch the store commits atomically") {
		t.Fatalf("a 12 MB batch at a 64 MiB owner was not refused with the batch-too-large answer: %s", frame.Payload)
	}
	if logged := owner.walLastSequence(); logged != refusedSeq {
		t.Fatalf("setup: the owner logged the refused batch at %d, the record names %d", logged, refusedSeq)
	}
	for _, index := range []int{1, 2} {
		replica := tc.nodes[index].server.core.cluster
		toReplica := owner.peerFor(mustNode(t, owner, tc.addrs[index]))
		for deadline := time.Now().Add(15 * time.Second); replica.shouldApply(tc.addrs[0], refusedSeq); time.Sleep(100 * time.Millisecond) {
			if time.Now().After(deadline) {
				state, reason := toReplica.streamStatus()
				t.Fatalf("node %d never took the owner's refusal of its sequence %d as the answer: stream %v, %q", index+1, refusedSeq, state, reason)
			}
		}
		if records := tc.sizeRefusalRecords(index); records != 0 {
			t.Errorf("node %d still holds %d size refusal records after the owner's refused batch was answered", index+1, records)
		}
		if held := tc.bucketCountOn(index, "app", bucket); held != 0 {
			t.Errorf("node %d holds %d elements of a batch its owner refused", index+1, held)
		}
	}
}

// TestAResizedReplicaAppliesTheStalledBatchFromItsOwnLog guards the resized-replica case
// against the consumed-refusal rule: the 32 MiB replica's refusal of a batch its owner
// committed is not the operation's answer, so it must not be consumed. Once
// the member is resized and restarted, its own replay applies the logged
// entry, with the owner stopped so that no re-send can stand in for it.
func TestAResizedReplicaAppliesTheStalledBatchFromItsOwnLog(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	tc.ok(0, transport.OpBatchPut, largeBatch("app", bucket, 60, 120_000))
	toSmall := owner.peerFor(mustNode(t, owner, tc.addrs[2]))
	deadline := time.Now().Add(15 * time.Second)
	for _, reason := toSmall.streamStatus(); !strings.Contains(reason, "memtable"); _, reason = toSmall.streamStatus() {
		if time.Now().After(deadline) {
			t.Fatalf("the owner's stream to the 32 MiB replica never stalled on the memtable: %q", reason)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := tc.nodes[0].server.Stop(); err != nil {
		t.Fatalf("stopping the owner: %v", err)
	}
	tc.nodes[0].server = nil
	_ = tc.nodes[0].client.Close()
	tc.nodes[0].client = nil

	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.restartNode(2)
	if held := tc.bucketCountOn(2, "app", bucket); held != 60 {
		t.Fatalf("node 3, resized to 64 MiB and restarted with its owner stopped, holds %d of the batch's 60 documents: its replay did not apply the entry it logged", held)
	}
}

// TestARemovedSourcesSizeRefusalIsCleared is BUG-0250 item 2. The 32 MiB
// replica records that it refused a source's entry as too large for its store,
// and its start holds the source's position below it (lowerPastSizeRefusals).
// Nothing asks about that record once the source is removed from the
// membership: no re-send can arrive from a node that is gone, and a restart
// would otherwise load a stale record and lower a position for a member that
// no longer exists. The adoption that removes the source must delete it.
func TestARemovedSourcesSizeRefusalIsCleared(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()

	small := tc.nodes[2].server.core.cluster
	const refusedSeq = 42
	if err := small.recordSizeRefusal(tc.addrs[0], refusedSeq); err != nil {
		t.Fatalf("recording the 32 MiB replica's size refusal of the owner's sequence %d: %v", refusedSeq, err)
	}
	if records := tc.sizeRefusalRecords(2); records != 1 {
		t.Fatalf("setup: the 32 MiB replica holds %d size refusal records, want 1", records)
	}

	// A survivor removes the owner, the source the record names, and node 3
	// adopts the membership without it.
	remover := tc.nodes[1].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := remover.proposeRemoval(ctx, tc.addrs[0]); err != nil {
		t.Fatalf("removing the owner from the cluster: %v", err)
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if small.Epoch() >= InitialEpoch+1 && !nodeListed(small.Members(), tc.addrs[0]) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the 32 MiB replica did not adopt the owner's removal: epoch %d, members %v", small.Epoch(), small.Members())
		}
	}
	if records := tc.sizeRefusalRecords(2); records != 0 {
		t.Fatalf("the 32 MiB replica still holds %d size refusal records for the removed owner %s", records, tc.addrs[0])
	}

	// The record is gone from the store, not only forgotten in memory: a
	// restart's start scan (lowerPastSizeRefusals) must not load it again.
	tc.restartNode(2)
	if records := tc.sizeRefusalRecords(2); records != 0 {
		t.Fatalf("the restarted 32 MiB replica re-loaded %d size refusal records for the removed owner", records)
	}
}

// TestASnapshotSeedRebasePastASizeRefusalClearsIt is BUG-0250 item 2's rebase
// half. A replica records that it refused a source's entry as too large for
// its store. A snapshot of the refused bucket from that source, taken past the
// entry, then lands here: the covered entry is skipped and the position moves
// past it with no log entry of the source's origin (handleReplPush's
// snapshotCovers branch). The source's log is rebased under this node's
// record, and the rebase validates through a sequence past the refusal. The
// refused sequence is now below the position the rebase declares, so nothing
// of it is owed: the record must be cleared with the rebase. Left, the next
// start's scan (lowerPastSizeRefusals) lowers the position below an entry
// that the snapshot and the rebase both passed.
func TestASnapshotSeedRebasePastASizeRefusalClearsIt(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	c := tc.nodes[0].server.core.cluster
	const source = "seed-source"
	const refusedSeq = 42
	if err := c.recordSizeRefusal(source, refusedSeq); err != nil {
		t.Fatalf("recording the size refusal of %s's sequence %d: %v", source, refusedSeq, err)
	}
	if records := tc.sizeRefusalRecords(0); records != 1 {
		t.Fatalf("setup: the replica holds %d size refusal records, want 1", records)
	}

	// A snapshot of the refused bucket, taken from source past the refusal,
	// has landed: the covered entries are skipped and raise the position past
	// them, as handleReplPush's snapshotCovers branch does.
	const seededAt = refusedSeq + 8
	if err := c.recordSeeded(source, testDatabase, testBucket, seededAt); err != nil {
		t.Fatalf("recording the snapshot position of %s/%s from %s at %d: %v", testDatabase, testBucket, source, seededAt, err)
	}
	c.raisePositionOwingCheckpoint(source, seededAt)

	// The source's log is rebased under this node's record; the rebase
	// validates through the seeded sequence and so passes the refusal.
	incarnation := []byte{0x5e, 0xed}
	c.appliedMu.Lock()
	c.recordedStream[source] = recordedStream{incarnation: incarnation}
	c.appliedMu.Unlock()
	if _, err := c.rebaseStream(source, incarnation, ReplRebase{From: incarnation, ValidThrough: seededAt}); err != nil {
		t.Fatalf("rebasing %s's position through its seeded sequence %d: %v", source, seededAt, err)
	}

	if records := tc.sizeRefusalRecords(0); records != 0 {
		t.Fatalf("the replica still holds %d size refusal records for %s after a rebase passed the refused sequence %d", records, source, refusedSeq)
	}
	c.memtableMu.Lock()
	_, held := c.batchSizeRefusals[source]
	c.memtableMu.Unlock()
	if held {
		t.Fatalf("the replica still holds %s's size refusal in memory after a rebase passed the refused sequence %d", source, refusedSeq)
	}
}

// TestAClampedRebaseBelowASizeRefusalKeepsIt is the guard of BUG-0250 item 2's
// rebase half. A rebase that validates only through a sequence below the
// refused one does not pass the refused entry: the source is still stalled on
// it, so the record must stay in the store and in memory and the position must
// stay below it. Clearing on any record would let the next apply from the
// source skip an entry this node never applied.
func TestAClampedRebaseBelowASizeRefusalKeepsIt(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	c := tc.nodes[0].server.core.cluster
	const source = "clamp-source"
	const refusedSeq = 42
	const validThrough = 10
	if err := c.recordSizeRefusal(source, refusedSeq); err != nil {
		t.Fatalf("recording the size refusal of %s's sequence %d: %v", source, refusedSeq, err)
	}
	c.raisePositionOwingCheckpoint(source, refusedSeq-1)

	incarnation := []byte{0xc1, 0xa5}
	c.appliedMu.Lock()
	c.recordedStream[source] = recordedStream{incarnation: incarnation}
	c.appliedMu.Unlock()
	position, err := c.rebaseStream(source, incarnation, ReplRebase{From: incarnation, ValidThrough: validThrough})
	if err != nil {
		t.Fatalf("rebasing %s's position down to %d: %v", source, validThrough, err)
	}

	c.appliedMu.Lock()
	applied := c.appliedFrom[source]
	c.appliedMu.Unlock()
	if applied != validThrough {
		t.Fatalf("%s's position is %d after a rebase clamped to %d (reply %+v), want %d", source, applied, validThrough, position, validThrough)
	}
	if records := tc.sizeRefusalRecords(0); records != 1 {
		t.Fatalf("the replica holds %d size refusal records after a rebase below the refused sequence %d, want 1", records, refusedSeq)
	}
	c.memtableMu.Lock()
	refusal, held := c.batchSizeRefusals[source]
	c.memtableMu.Unlock()
	if !held || refusal.Seq != refusedSeq {
		t.Fatalf("the in-memory size refusal of %s is %+v (held %v) after a rebase below it, want sequence %d kept", source, refusal, held, refusedSeq)
	}
}
