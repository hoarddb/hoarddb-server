package server

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Added by the pop-n D3 verifier (not the fixer), to pin the side effect the
// D3 blocker fix (8894011f) argued and did not test. A replica now knows its
// store refused an entry as too large from the apply's typed outcome, not from
// the answer's wording, so a cascade delete or a TTL expiry that its owner
// committed and a smaller replica cannot commit becomes a size stall, as a
// batch put and a pop are. The store leaves that refusal unconsumed
// (store.ApplyRequest.OwnerCommitted), so the resized member applies the
// entry from its own log. Before the fix the same refusal was recorded as a
// divergence and the copy was re-seeded from its owner (measured on
// 3d3d4203). This test pins the stall: copies are counted honestly, no
// divergence is recorded, and a resize converges the copy.

// cascadeOverTheSmallBound is how many field-store elements a document
// carries so that deleting them in one batch is over a 32 MiB member's key
// bound and under a 64 MiB member's: half as many again as the smaller bound.
func cascadeOverTheSmallBound() int {
	return int(storage.BatchEntryBound(config.MemtableBatchBytes(config.MinMemtableMB)) * 3 / 2)
}

// seedCommentsIn pushes count elements onto key's Comments in s's own store
// and nowhere else, so every node holds the same cascade without shipping it.
func seedCommentsIn(t *testing.T, s *Server, database, bucket, key string, count int) {
	t.Helper()
	b := servedBucket(t, s, database, bucket)
	owner, err := store.FieldOwnerPrefix(b.slot, b.databaseID, b.id.Load(), key)
	if err != nil {
		t.Fatalf("owner prefix of %s: %v", key, err)
	}
	comments, declared := b.storeFields().find("Comments", 0)
	if !declared {
		t.Fatalf("%s declares no Comments", bucket)
	}
	ref := store.FieldRef{Owner: owner, FieldID: uint16(comments.ID), Engine: store.FieldFIFO}
	element, err := bson.Marshal(map[string]interface{}{"n": 1})
	if err != nil {
		t.Fatalf("encoding an element: %v", err)
	}
	b.rec.mu.Lock()
	defer b.rec.mu.Unlock()
	for pushed := range count {
		_, err := store.Apply(store.ApplyRequest{KV: s.kv, Slot: b.slot, DatabaseID: b.databaseID, ID: b.id.Load(), Owed: b.hashStore.Carry(),
			Op: func(snap storage.KVSnapshot, st *store.BucketState, batch storage.KVBatch) error {
				_, err := store.FieldPush(store.FieldWrite{Snap: snap, State: st, Batch: batch}, ref, element)
				return err
			}})
		if err != nil {
			t.Fatalf("pushing element %d: %v", pushed, err)
		}
	}
}

// TestASmallerReplicaStallsOnACascadeItsOwnerCommitted: on a 64/64/32 MiB
// cluster, the owner deletes a document whose field store is too large for
// the 32 MiB member's batch and not for its own, once by a TTL expiry and
// once by a client's delete. The 64 MiB replica removes the document and its
// elements; the 32 MiB replica keeps both, records no divergence, and its
// stream stalls on the memtable. Resized and restarted, it removes them too.
func TestASmallerReplicaStallsOnACascadeItsOwnerCommitted(t *testing.T) {
	for _, path := range []string{"ttl expiry", "client delete"} {
		t.Run(path, func(t *testing.T) {
			now := time.Now()
			tc := memtableMixedCluster(t)
			defer tc.stop()
			owner := tc.nodes[0].server.core.cluster
			small := tc.nodes[2].server.core.cluster
			bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
			options := storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
			createdAt := now
			if path == "ttl expiry" {
				options["indices"] = []interface{}{ttlDeclaration("CreatedAt", 3600)}
				createdAt = now.Add(-2 * time.Hour)
			}
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash, Options: options})
			tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "big",
				Data: map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(createdAt)}, WriteConcern: "all"})
			count := cascadeOverTheSmallBound()
			keysBefore := make([]int, len(tc.nodes))
			for index := range tc.nodes {
				seedCommentsIn(t, tc.nodes[index].server.core, "app", bucket, "big", count)
				keysBefore[index] = fieldStoreKeysOf(t, tc.nodes[index].server.core, "app", bucket)
				if keysBefore[index] < count {
					t.Fatalf("setup: node %d holds %d field-store keys after seeding %d", index+1, keysBefore[index], count)
				}
			}

			if path == "ttl expiry" {
				pass := tc.nodes[0].server.core.sweepTTL(context.Background(), now)
				if documentOf(t, tc.nodes[0].server.core, "app", bucket, "big") != nil {
					t.Fatalf("setup: the 64 MiB owner's sweep (%+v) did not expire big, whose %d elements fit its batch", pass, count)
				}
			} else {
				answer := tc.frame(0, transport.OpDelete, bson.M{"database": "app", "bucket": bucket, "key": "big", "writeconcern": "all"})
				if documentOf(t, tc.nodes[0].server.core, "app", bucket, "big") != nil {
					t.Fatalf("setup: the 64 MiB owner did not delete big, whose %d elements fit its batch: %.300s", count, bson.Raw(answer.Payload))
				}
				if confirmed, factor, _, _ := copiesConfirmedOf(answer.Payload); confirmed >= factor {
					t.Errorf("the delete answered copies_confirmed %d of %d while the 32 MiB replica still holds the document: %.300s",
						confirmed, factor, bson.Raw(answer.Payload))
				}
			}
			ownerSeq := owner.walLastSequence()

			reason := tc.stallStaysOnSmallReplica(t, bucket, 1, ownerSeq, "after the "+path)
			t.Logf("the stall reason for the %s: %s", path, reason)
			if strings.Contains(reason, "do not resize") || !strings.Contains(reason, "Set storage.memtable_mb") {
				t.Errorf("the stall reason for the %s must tell the operator to resize the member, not to restart it unchanged: %q", path, reason)
			}
			if record, recorded := small.divergenceFrom(tc.addrs[0]); recorded {
				t.Errorf("the 32 MiB replica recorded a divergence for the owner's %s at sequence %d (%+v): a batch too large for its memtable is a size stall, not a divergence",
					path, ownerSeq, record)
			}
			if keys := fieldStoreKeysOf(t, tc.nodes[2].server.core, "app", bucket); keys != keysBefore[2] {
				t.Errorf("the 32 MiB replica holds %d field-store keys, %d before a cascade it cannot commit whole", keys, keysBefore[2])
			}
			for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
				keys := fieldStoreKeysOf(t, tc.nodes[1].server.core, "app", bucket)
				gone := documentOf(t, tc.nodes[1].server.core, "app", bucket, "big") == nil
				if keys == 0 && gone {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the 64 MiB replica holds the document (%v) and %d field-store keys after the owner's %s", !gone, keys, path)
				}
			}

			// A restart at the same size refuses the same cascade again from the
			// member's own log, which is why the reason must not offer it as the fix.
			tc.restartNode(2)
			again := tc.stallStaysOnSmallReplica(t, bucket, 1, ownerSeq, "after restarting the 32 MiB replica without resizing it")
			if keys := fieldStoreKeysOf(t, tc.nodes[2].server.core, "app", bucket); keys != keysBefore[2] {
				t.Errorf("the 32 MiB replica restarted at the same size holds %d field-store keys, %d before a cascade it cannot commit whole", keys, keysBefore[2])
			}
			if strings.Contains(again, "do not resize") {
				t.Errorf("the stall reason after a restart at the same size still says not to resize: %q", again)
			}

			tc.configure = func(index int, cfg *config.Config) {
				if index == 2 {
					cfg.Storage.MemtableMB = 64
				}
			}
			tc.restartNode(2)
			for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
				node := tc.nodes[2].server.core
				keys := fieldStoreKeysOf(t, node, "app", bucket)
				gone := documentOf(t, node, "app", bucket, "big") == nil
				if keys == 0 && gone {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("node 3 resized to 64 MiB and restarted holds the document (%v) and %d field-store keys the owner's %s removed", !gone, keys, path)
				}
			}
			if record, recorded := tc.nodes[2].server.core.cluster.divergenceFrom(tc.addrs[0]); recorded {
				t.Errorf("node 3, resized and converged, still records a divergence from its owner: %+v", record)
			}
		})
	}
}

// TestASmallerReplicaReplayingACascadeItCouldNotCommitIsNoDivergence is the
// replay side of the cascade stall (added by the pop-n merge verifier, not the
// fixer). The 32 MiB replica logs its owner's delete, or TTL expiry, of a
// document whose field store is too large for its batch, its apply refuses
// the cascade as too large, and it dies before the durable record of that
// refusal commits. The answer is success false with the cascade's own advice
// and without the store's sentence, so a replay that judged it by the
// answer's wording took it for a divergence (BUG-0581's replay judgement as
// it stood on main): counted, recorded, and the copy re-seeded. Restarted
// unresized, its replay must judge the cascade from the apply's outcome as
// the live apply does: no divergence, the owner's sequence unapplied, and the
// stream stalled on the memtable again with the document still held.
func TestASmallerReplicaReplayingACascadeItCouldNotCommitIsNoDivergence(t *testing.T) {
	for _, path := range []string{"ttl expiry", "client delete"} {
		t.Run(path, func(t *testing.T) {
			now := time.Now()
			tc := memtableMixedCluster(t)
			defer tc.stop()
			owner := tc.nodes[0].server.core.cluster
			bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
			options := storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
			createdAt := now
			if path == "ttl expiry" {
				options["indices"] = []interface{}{ttlDeclaration("CreatedAt", 3600)}
				createdAt = now.Add(-2 * time.Hour)
			}
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash, Options: options})
			tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "big",
				Data: map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(createdAt)}, WriteConcern: "all"})
			count := cascadeOverTheSmallBound()
			for index := range tc.nodes {
				seedCommentsIn(t, tc.nodes[index].server.core, "app", bucket, "big", count)
			}

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

			if path == "ttl expiry" {
				pass := tc.nodes[0].server.core.sweepTTL(context.Background(), now)
				if documentOf(t, tc.nodes[0].server.core, "app", bucket, "big") != nil {
					t.Fatalf("setup: the 64 MiB owner's sweep (%+v) did not expire big, whose %d elements fit its batch", pass, count)
				}
			} else {
				answer := tc.frame(0, transport.OpDelete, bson.M{"database": "app", "bucket": bucket, "key": "big", "writeconcern": "all"})
				if documentOf(t, tc.nodes[0].server.core, "app", bucket, "big") != nil {
					t.Fatalf("setup: the 64 MiB owner did not delete big, whose %d elements fit its batch: %.300s", count, bson.Raw(answer.Payload))
				}
			}
			ownerSeq := owner.walLastSequence()
			for deadline := time.Now().Add(15 * time.Second); !crashed.Load(); time.Sleep(20 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("setup: the 32 MiB replica never reached a size refusal record for the owner's %s at sequence %d (marked applied: %v)",
						path, ownerSeq, !small.shouldApply(tc.addrs[0], ownerSeq))
				}
			}
			smallDir := tc.stopNodeKeepingData(2)
			sizeRefusalRecordFaultHook.Store(nil)

			countedBefore := divergencesCounted(t)
			tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
			restarted := tc.nodes[2].server.core.cluster
			// The count, not only the record: a recorded divergence wakes the
			// re-seed, which can settle it before the record is read.
			if counted := divergencesCounted(t) - countedBefore; counted != 0 {
				t.Fatalf("the 32 MiB replica, restarted unresized, counted %v divergences replaying the owner's %s it cannot commit; it is a size refusal", counted, path)
			}
			if record, recorded := restarted.divergenceFrom(tc.addrs[0]); recorded {
				t.Fatalf("the 32 MiB replica, restarted unresized, judged the owner's %s it cannot commit a divergence (%+v), not a size refusal", path, record)
			}
			if !restarted.shouldApply(tc.addrs[0], ownerSeq) {
				t.Fatalf("the 32 MiB replica, restarted unresized, replayed the owner's %s at sequence %d and marked it applied", path, ownerSeq)
			}
			if documentOf(t, tc.nodes[2].server.core, "app", bucket, "big") == nil {
				t.Fatalf("the 32 MiB replica, restarted unresized, lost the document whose %s it cannot commit whole", path)
			}
			tc.stallStaysOnSmallReplica(t, bucket, 1, ownerSeq, "after the replica crashed before its refusal record for the "+path+" and restarted unresized")
		})
	}
}
