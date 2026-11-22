package server

import (
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRestoreTwiceConverges: restoring one dump twice into one bucket leaves
// the count unchanged. It guards the restore sender: once insert refuses an
// existing _id_, a restore that still sent PUT would fail its second run.
func TestRestoreTwiceConverges(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			s := newTestServer(t)
			ctx := rootContext()
			bucket := "twice_" + string(bucketType)
			createTestBucket(t, s, "app", bucket, bucketType)
			for _, key := range []string{"a", "b", "c"} {
				if _, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: key, Data: map[string]interface{}{"tag": "v1"}}); err != nil {
					t.Fatalf("insert %s: %v", key, err)
				}
			}
			manifest, raw := dumpBucketRaw(t, s, ctx, "app", bucket, 3)
			req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
			for run := 1; run <= 2; run++ {
				_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 3)
				if msg != "" {
					t.Fatalf("restore run %d refused: %s", run, msg)
				}
				if done.Applied != 3 {
					t.Fatalf("restore run %d applied %d, want 3", run, done.Applied)
				}
			}
			count, err := s.Count(CountRequest{Database: "app", Bucket: bucket})
			if err != nil {
				t.Fatalf("count: %v", err)
			}
			if count.Count != 3 {
				t.Fatalf("bucket holds %d documents after two restores of one dump, want 3", count.Count)
			}
		})
	}
}

// TestReadRepairOfExistingKeyApplies: a consistent get repairs a stale copy
// that holds the key. The repair is a replace, so it must reach a copy that
// already has the _id_ once insert refuses one.
func TestReadRepairOfExistingKeyApplies(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the write of k1")

	forceStale(t, tc.nodes[2].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": 0})

	resp := consistentGet(t, tc.nodes[0], testDatabase, testBucket, "k1")
	if !resp.Found || !resp.Repaired {
		t.Fatalf("a stale copy that holds the key was not repaired (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		found, data, version, err := tc.nodes[2].server.core.versionedFind(testDatabase, testBucket, "k1")
		if err == nil && found && version >= resp.Version && docField(t, data, "n") == "1" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stale copy did not converge (found=%v version=%d err=%v)", found, version, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestRepairFenceMatchesUpsert: isRepairPut is true for an UPSERT entry
// carrying repair_bound and false for a PUT carrying it, because the repair
// is a replace and a PUT with the field is a client write the door refuses.
func TestRepairFenceMatchesUpsert(t *testing.T) {
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1",
		Data: map[string]interface{}{"n": 1}, RepairBound: 7})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	if !isRepairPut(ReplEntry{Op: int32(transport.OpUpsert), Payload: payload}) {
		t.Errorf("isRepairPut(UPSERT with repair_bound) = false, want true")
	}
	if isRepairPut(ReplEntry{Op: int32(transport.OpPut), Payload: payload}) {
		t.Errorf("isRepairPut(PUT with repair_bound) = true, want false")
	}
}

// TestRestoreSendsUpsertToTheOwner: the opcode a restore routes for each
// restored document of a hash or btree bucket is UPSERT, seen where the owner
// takes the bucket's fence. Without this pin a restore that fell back to PUT
// would pass until insert refuses an existing _id_.
func TestRestoreSendsUpsertToTheOwner(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			tc := newTestCluster(t, 2, 2)
			defer tc.stop()
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: bucketType})
			tc.ok(0, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "seed", Data: map[string]interface{}{"tag": "v"}})
			manifest, dump := dumpBucketOverWire(t, tc, 0, testDatabase, testBucket, 1)

			target := "restored_" + string(bucketType)
			ownerIdx, _ := ownerAndReplicaIndex(t, tc, target)
			var mu sync.Mutex
			var seen []transport.Opcode
			observe := func(_ string, op transport.Opcode, bucket string) {
				if bucket != target {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				seen = append(seen, op)
			}
			inOwnerFenceHook.Store(&observe)
			defer inOwnerFenceHook.Store(nil)

			restoreBucketOverWire(t, tc, ownerIdx, RestoreStartRequest{
				Database: testDatabase, Bucket: target,
				FormatVersion: manifest.FormatVersion, StoreType: manifest.StoreType, Bytes: int64(len(dump)),
			}, dump, 1)

			mu.Lock()
			defer mu.Unlock()
			upserts := 0
			for _, op := range seen {
				switch op {
				case transport.OpUpsert:
					upserts++
				case transport.OpPut, transport.OpBTreeInsert:
					t.Errorf("the owner took a restored document as %s, want UPSERT", op)
				}
			}
			if upserts != 1 {
				t.Errorf("the owner took %d UPSERTs of the restored bucket (all ops %v), want 1", upserts, seen)
			}
		})
	}
}
