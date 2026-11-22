package server

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRestoreReplicatesAcrossCluster is fix round 1's Critical finding: a
// restored document must be WAL-durable and shipped to replicas on a
// multi-node cluster, exactly like a live client's write
// (dump-restore-spec.md §9.2) — not merely written to the bucket owner's
// local storage. The original applyRestoreDocument called Server.InsertCtx
// (and, for heap, HeapStore.PushItem) directly, which never touches
// Cluster.applyAsOwner (server/write_path.go) — the only place WAL append
// and replica shipping happen — so a restored document landed only on the
// owner. Invisible on the single-node path every other test in this build
// runs at (s.cluster is nil there, so InsertCtx and the fixed
// routeOrdinaryWrite path both just dispatch locally), which is exactly
// why a real multi-node cluster is required to catch it.
//
// Two nodes, ReplicationFactor 2: every bucket lives on both nodes, so
// "replica" below just means "the node that is not currently the owner of
// this particular bucket" — reading directly from that node's own storage
// is the proof restication actually happened.
func TestRestoreReplicatesAcrossCluster(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	t.Run("hash_via_OpPut", func(t *testing.T) {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: "seed", Data: map[string]interface{}{"tag": "seed-v"},
		})

		manifest, all := dumpBucketOverWire(t, tc, 0, testDatabase, testBucket, 1)

		target := "docs_restored"
		ownerIdx, replicaIdx := ownerAndReplicaIndex(t, tc, target)

		restoreStart := restoreBucketOverWire(t, tc, ownerIdx, RestoreStartRequest{
			Database: testDatabase, Bucket: target,
			FormatVersion: manifest.FormatVersion, StoreType: manifest.StoreType, Bytes: int64(len(all)),
		}, all, 1)
		if restoreStart.Applied != 1 {
			t.Fatalf("restore applied %d, want 1", restoreStart.Applied)
		}

		// The critical assertion: read the restored document directly from
		// the REPLICA node's own connection. OpGet is answered locally by
		// serveRead whenever the receiving node holds a copy of the
		// bucket — which is exactly what "the document was actually
		// replicated" means. Before this fix, this read comes back
		// Found:false on any node but the owner.
		readFrame := tc.frame(replicaIdx, transport.OpGet, FindRequest{Database: testDatabase, Bucket: target, Key: "seed"})
		if readFrame.Opcode == transport.OpError {
			var errResp ErrorResponse
			_ = bson.Unmarshal(readFrame.Payload, &errResp)
			t.Fatalf("reading the restored document from the replica node (index %d) failed: %s", replicaIdx, errResp.Message)
		}
		var found FindResponse
		if err := bson.Unmarshal(readFrame.Payload, &found); err != nil {
			t.Fatalf("decode find response: %v", err)
		}
		if !found.Found {
			t.Fatalf("restored document is not present on replica node %d — restore is not replicating writes on a multi-node cluster", replicaIdx)
		}
		var doc bson.M
		if err := bson.Unmarshal(found.Data, &doc); err != nil {
			t.Fatalf("unmarshal restored document: %v", err)
		}
		if doc["tag"] != "seed-v" {
			t.Fatalf("replica's copy of the restored document = %v, want tag \"seed-v\" (value-by-value, not just presence)", doc)
		}
	})

	t.Run("heap_via_OpHeapPush", func(t *testing.T) {
		bucket := "priorities"
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHeap})
		tc.ok(0, transport.OpHeapPush, PushRequest{
			Database: testDatabase, Bucket: bucket, Priority: 5, Data: map[string]interface{}{"tag": "heap-v"},
		})

		manifest, all := dumpBucketOverWire(t, tc, 0, testDatabase, bucket, 1)

		target := "priorities_restored"
		ownerIdx, replicaIdx := ownerAndReplicaIndex(t, tc, target)

		restoreDone := restoreBucketOverWire(t, tc, ownerIdx, RestoreStartRequest{
			Database: testDatabase, Bucket: target,
			FormatVersion: manifest.FormatVersion, StoreType: manifest.StoreType, Bytes: int64(len(all)),
		}, all, 1)
		if restoreDone.Applied != 1 {
			t.Fatalf("restore applied %d, want 1", restoreDone.Applied)
		}

		// Heap has no wire-level non-destructive read that a client can aim
		// at one specific node (OpHeapPop is a replicated write — the
		// coordinator would route it to the owner regardless of which
		// node's connection sent it, the same way OpPut is), so this reads
		// the replica node's own in-process Server directly: Server.Peek
		// touches only that node's local heapStore, with no cluster
		// routing of its own, which is exactly what's needed to prove that
		// node's local copy specifically.
		peekResp, err := tc.nodes[replicaIdx].server.core.Peek(PeekRequest{Database: testDatabase, Bucket: target})
		if err != nil {
			t.Fatalf("peeking the restored heap bucket on replica node %d: %v", replicaIdx, err)
		}
		if !peekResp.Found {
			t.Fatalf("restored heap element is not present on replica node %d (%s) — restore is not replicating writes on a multi-node cluster",
				replicaIdx, peekResp.Message)
		}
		var doc bson.M
		if err := bson.Unmarshal(peekResp.Items[0], &doc); err != nil {
			t.Fatalf("unmarshal replica's copy of the restored heap element: %v", err)
		}
		if doc["tag"] != "heap-v" {
			t.Fatalf("replica's copy of the restored heap element = %v, want tag \"heap-v\"", doc)
		}
	})
}

// TestRestoreIntoANewBucketSucceedsThroughANonOwner: restoring into
// a bucket that does not exist yet creates it when the client's node serves the
// restore itself, and was refused with a join-sync error when the frame had to
// be forwarded to the ring owner. On a cluster where the replication factor is
// below the node count, whether it worked depended on the connection. Here the
// restore goes through a node outside the target's replica set, and the
// restored document must be on the owner and on its replica.
func TestRestoreIntoANewBucketSucceedsThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "seed", Data: map[string]interface{}{"tag": "seed-v"}, WriteConcern: "all",
	})
	manifest, all := dumpBucketOverWire(t, tc, 0, testDatabase, testBucket, 1)

	// A target name whose replica set leaves node 0 out, so node 0 can only forward.
	cluster := tc.nodes[0].server.core.cluster
	target := ""
	for attempt := 0; attempt < 512 && target == ""; attempt++ {
		name := fmt.Sprintf("restored-%d", attempt)
		replicas, err := cluster.Replicas(placementOf(testDatabase, name))
		if err != nil {
			t.Fatalf("replicas of %q: %v", name, err)
		}
		if !slices.ContainsFunc(replicas, func(node ring.Node) bool { return node.ID == cluster.self.ID }) {
			target = name
		}
	}
	if target == "" {
		t.Fatal("no bucket name in 512 tries is placed away from node 0")
	}

	restored := restoreBucketOverWire(t, tc, 0, RestoreStartRequest{
		Database: testDatabase, Bucket: target,
		FormatVersion: manifest.FormatVersion, StoreType: manifest.StoreType, Bytes: int64(len(all)),
	}, all, 1)
	if restored.Applied != 1 {
		t.Fatalf("restore through a non-owner applied %d documents, want 1", restored.Applied)
	}

	replicas, err := cluster.Replicas(placementOf(testDatabase, target))
	if err != nil {
		t.Fatalf("replicas of %q: %v", target, err)
	}
	for _, holder := range replicas {
		index := nodeIndexByAddr(t, tc.addrs, holder.ID)
		deadline := time.Now().Add(20 * time.Second)
		for {
			found, err := tc.nodes[index].server.core.Find(FindRequest{Database: testDatabase, Bucket: target, Key: "seed"})
			if err == nil && found.Found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the restored document is not on holder node %d: %+v, %v", index+1, found, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// TestRestoreChunkForABucketThatStillDoesNotExistIsRefused holds the half of
// the restore-creates-its-bucket rule that must not change: only the start of a restore may create its
// bucket. A chunk or a done for a bucket that does not exist, sent through a
// non-owner, is still refused, by the restore handler's own refusal.
func TestRestoreChunkForABucketThatStillDoesNotExistIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	for index := range 3 {
		answer := tc.frame(index, transport.OpRestoreChunk, RestoreChunkRequest{
			Database: testDatabase, Bucket: "never-started", SessionID: "no-such-session", Data: []byte{0},
		})
		if answer.Opcode != transport.OpError {
			t.Errorf("node %d answered a restore chunk for a bucket that does not exist with %s, want an error", index+1, answer.Opcode)
			continue
		}
		if message := answerText(answer); !strings.Contains(message, "no active restore session") {
			t.Errorf("node %d refused the chunk with %q, want the restore handler's own refusal that names the missing session", index+1, message)
		}
	}
}

// dumpBucketOverWire drives OpDumpStart/Chunk/Done against node index i of
// tc and returns the manifest plus the concatenated dumped bytes — the
// cluster-test equivalent of dumpBucketRaw (restore_test.go), built on
// tc.ok/tc.frame since a cluster test's nodes only expose the real wire
// protocol, not direct dispatch.
func dumpBucketOverWire(t *testing.T, tc *testCluster, i int, database, bucket string, wantDocs int64) (*DumpBucketManifest, []byte) {
	t.Helper()
	startFrame := tc.ok(i, transport.OpDumpStart, DumpStartRequest{Database: database, Bucket: bucket, AcceptStores: true, AcceptDead: true})
	var start DumpStartResponse
	if err := bson.Unmarshal(startFrame.Payload, &start); err != nil {
		t.Fatalf("decode dump start: %v", err)
	}
	if start.Manifest == nil {
		t.Fatalf("dump start %s/%s returned no manifest", database, bucket)
	}

	var all []byte
	for {
		chunkFrame := tc.ok(i, transport.OpDumpChunk, DumpChunkRequest{Database: database, Bucket: bucket})
		var chunk DumpChunkResponse
		if err := bson.Unmarshal(chunkFrame.Payload, &chunk); err != nil {
			t.Fatalf("decode dump chunk: %v", err)
		}
		all = append(all, chunk.Data...)
		if chunk.EOF {
			break
		}
	}
	doneFrame := tc.ok(i, transport.OpDumpDone, DumpDoneRequest{Database: database, Bucket: bucket})
	var done DumpDoneResponse
	if err := bson.Unmarshal(doneFrame.Payload, &done); err != nil {
		t.Fatalf("decode dump done: %v", err)
	}
	if done.Documents != wantDocs {
		t.Fatalf("dump of %s/%s reported %d documents, want %d", database, bucket, done.Documents, wantDocs)
	}
	return start.Manifest, all
}

// restoreBucketOverWire drives OpRestoreStart/Chunk/Done against node index
// i of tc and returns the Done response.
func restoreBucketOverWire(t *testing.T, tc *testCluster, i int, req RestoreStartRequest, all []byte, documents int64) *RestoreDoneResponse {
	t.Helper()
	startFrame := tc.ok(i, transport.OpRestoreStart, req)
	var start RestoreStartResponse
	if err := bson.Unmarshal(startFrame.Payload, &start); err != nil {
		t.Fatalf("decode restore start: %v", err)
	}
	tc.ok(i, transport.OpRestoreChunk, RestoreChunkRequest{
		Database: req.Database, Bucket: req.Bucket, SessionID: start.SessionID, Data: all,
	})
	doneFrame := tc.ok(i, transport.OpRestoreDone, RestoreDoneRequest{
		Database: req.Database, Bucket: req.Bucket, SessionID: start.SessionID, Documents: documents,
	})
	var done RestoreDoneResponse
	if err := bson.Unmarshal(doneFrame.Payload, &done); err != nil {
		t.Fatalf("decode restore done: %v", err)
	}
	return &done
}

// ownerAndReplicaIndex resolves the ring owner of testDatabase/bucket on tc's first
// node's view (a pure function of membership, so any node agrees) and
// returns its index alongside the index of the other node in a two-node
// cluster — the "replica" a restored write must reach for this test to
// pass.
func ownerAndReplicaIndex(t *testing.T, tc *testCluster, bucket string) (ownerIdx, replicaIdx int) {
	t.Helper()
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("resolve owner for %q: %v", bucket, err)
	}
	ownerIdx = nodeIndexByAddr(t, tc.addrs, owner.ID)
	replicaIdx = 1 - ownerIdx
	return ownerIdx, replicaIdx
}
