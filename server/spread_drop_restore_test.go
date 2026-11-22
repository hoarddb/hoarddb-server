package server

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests of step 4 where dump, restore and the drop meet (spread MVP note,
// section 4 and test 14): the printed sequence of build step 12b, restore into
// a spread collection, and restore --drop of one.

// restoreRequestOf is the restore a CLI makes of one bucket's dump: the
// manifest's declaration, and the dump's size.
func restoreRequestOf(manifest *DumpBucketManifest, target string, size int, drop bool) RestoreStartRequest {
	return RestoreStartRequest{
		Database: manifest.Database, Bucket: target, FormatVersion: manifest.FormatVersion, StoreType: manifest.StoreType,
		Indexes: manifest.Indexes, IndexSpecs: manifest.IndexSpecs, Stores: manifest.Stores,
		ReplicationFactor: manifest.ReplicationFactor, Placement: manifest.Placement, Bytes: int64(size), Drop: drop,
	}
}

// restoreThrough runs a restore session through node index and returns the
// refusal that stopped it, or "" when it finished.
func restoreThrough(t *testing.T, sc *spreadCluster, index int, req RestoreStartRequest, data []byte, documents int64) string {
	t.Helper()
	start := sc.frame(index, transport.OpRestoreStart, req)
	if start.Opcode == transport.OpError {
		return refusalTextOrEmptyTB(t, start)
	}
	var started RestoreStartResponse
	if err := bson.Unmarshal(start.Payload, &started); err != nil {
		t.Fatalf("decoding the restore start: %v", err)
	}
	if reply := sc.frame(index, transport.OpRestoreChunk, RestoreChunkRequest{Database: req.Database, Bucket: req.Bucket, SessionID: started.SessionID, Data: data}); reply.Opcode == transport.OpError {
		return refusalTextOrEmptyTB(t, reply)
	}
	if reply := sc.frame(index, transport.OpRestoreDone, RestoreDoneRequest{Database: req.Database, Bucket: req.Bucket, SessionID: started.SessionID, Documents: documents}); reply.Opcode == transport.OpError {
		return refusalTextOrEmptyTB(t, reply)
	}
	return ""
}

// collectionCount is count() of a collection through node index.
func collectionCount(t *testing.T, sc *spreadCluster, index int, bucket string) int64 {
	t.Helper()
	return fanOutCount(t, sc.frame(index, transport.OpCount, CountRequest{Database: "app", Bucket: bucket}))
}

// TestUniqueRefusalSequenceKeepsEveryDocument is test 14 (B3): the printed
// sequence of build step 12b (dump, drop, create whole, restore) on 3,000
// spread documents gives 3,000 documents in a whole bucket. It needs the
// dump's fan-in and the router drop both; without the fan-in the dump holds
// none.
func TestUniqueRefusalSequenceKeepsEveryDocument(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	const documents = 3000
	sc.insertMany(0, "doc", documents)
	manifest, data := dumpBucketOverWire(t, sc.testCluster, 1, "app", "User", documents)
	requireDropped(t, sc.dropOn(2))
	sc.requireGoneEverywhere("by the printed sequence")
	sc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{"placement": "whole"}})
	if refused := restoreThrough(t, sc, 1, restoreRequestOf(manifest, "User", len(data), false), data, documents); refused != "" {
		t.Fatalf("the restore was refused: %s", refusedOrEmpty(refused))
	}
	for index := range sc.nodes {
		if count := collectionCount(t, sc, index, "User"); count != documents {
			t.Errorf("count() of the whole bucket through node %d is %d, want %d", index+1, count, documents)
		}
	}
	if partsHeldOf(sc.nodes[0].server.core, "User") != 0 {
		t.Error("a whole bucket made by the sequence holds parts")
	}
}

func refusedOrEmpty(text string) string { return strings.TrimSpace(text) }

// TestRestoreIntoSpreadCollections: a dump of a spread collection with a store
// field restores into a new spread collection (created spread, with its store
// field); into an existing empty spread collection that already declares the
// same store field (the declare is an identity); and is refused, with the
// way to do it, by one that lacks the field. restore --drop of a spread
// collection drops it through the router and restores into the new one.
func TestRestoreIntoSpreadCollections(t *testing.T) {
	stores := map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}}
	sc := newSpreadCluster(t, 3, 2, "User", stores)
	const documents = 400
	sc.insertMany(0, "doc", documents)
	manifest, data := dumpBucketOverWire(t, sc.testCluster, 0, "app", "User", documents)
	if manifest.Placement != "spread" || len(manifest.Stores) != 1 {
		t.Fatalf("the dump records placement %q and %d store fields", manifest.Placement, len(manifest.Stores))
	}

	t.Run("a new spread collection", func(t *testing.T) {
		if refused := restoreThrough(t, sc, 1, restoreRequestOf(manifest, "Fresh", len(data), false), data, documents); refused != "" {
			t.Fatalf("restore into a new collection was refused: %s", refused)
		}
		if count := collectionCount(t, sc, 2, "Fresh"); count != documents {
			t.Fatalf("count() of the restored collection is %d, want %d", count, documents)
		}
		if partsHeldOf(sc.nodes[0].server.core, "Fresh")+partsHeldOf(sc.nodes[1].server.core, "Fresh")+partsHeldOf(sc.nodes[2].server.core, "Fresh") == 0 {
			t.Fatal("the restored collection holds no part: it was not created spread")
		}
	})

	t.Run("an existing spread collection with the same store fields", func(t *testing.T) {
		sc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "Same", Type: BucketTypeHash, Options: map[string]interface{}{
			"placement": "spread", "stores": stores["stores"]}})
		sc.waitHeldBy("Same", sc.templateSet("Same"))
		if refused := restoreThrough(t, sc, 2, restoreRequestOf(manifest, "Same", len(data), false), data, documents); refused != "" {
			t.Fatalf("restore into an existing spread collection with the same store fields was refused: %s", refused)
		}
		if count := collectionCount(t, sc, 0, "Same"); count != documents {
			t.Fatalf("count() is %d, want %d", count, documents)
		}
	})

	t.Run("an existing spread collection without the store field", func(t *testing.T) {
		sc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "Bare", Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}})
		sc.waitHeldBy("Bare", sc.templateSet("Bare"))
		refused := restoreThrough(t, sc, 0, restoreRequestOf(manifest, "Bare", len(data), false), data, documents)
		for _, want := range []string{`spread bucket "Bare"`, "set when it is created", "dump --out ./dump-Bare app Bare", "drop bucket Bare in app", "restore --dir ./dump-Bare"} {
			if !strings.Contains(refused, want) {
				t.Fatalf("the refusal %q lacks %q", refused, want)
			}
		}
		if strings.Contains(refused, unitSeparator) {
			t.Fatalf("the refusal names a part: %q", refused)
		}
	})

	t.Run("restore --drop of a spread collection", func(t *testing.T) {
		sc.insertMany(1, "extra", 100)
		// A part drop that stops partway (a busy host) is answered with the retryable "is being
		// dropped, run it again to finish it" refusal, raised before any document is restored; a
		// second restore --drop finishes the drop. Any other refusal, or that one after the bound,
		// still fails the test.
		var refused string
		for attempt := 0; attempt < 20; attempt++ {
			refused = restoreThrough(t, sc, 1, restoreRequestOf(manifest, "User", len(data), true), data, documents)
			if refused == "" || !strings.Contains(refused, droppingText) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if refused != "" {
			t.Fatalf("restore --drop was refused: %s", refused)
		}
		for index := range sc.nodes {
			if count := collectionCount(t, sc, index, "User"); count != documents {
				t.Errorf("count() through node %d after restore --drop is %d, want %d (the extra documents are gone)", index+1, count, documents)
			}
		}
	})
}

// TestDeclareOfStoreFieldsOnASpreadCollection: a declare of a store field the
// collection has is the identity a whole bucket answers; a new one is refused
// with the sequence that adds it, named for the bucket, and ALTER_BUCKET with
// the one for the factor.
func TestDeclareAndAlterRefusalsOfASpreadCollection(t *testing.T) {
	n := newSpreadNode(t, map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}})
	same := n.send(transport.OpDeclareStoreFields, DeclareStoreFieldsRequest{Database: "app", Name: "User",
		Stores: []map[string]interface{}{{"name": "Purchases", "engine": "fifoStore"}}})
	if same.Opcode == transport.OpError || !strings.Contains(string(same.Payload), "already declares") {
		t.Fatalf("declaring a store field the collection has answered %s %q, want the identity", same.Opcode, same.Payload)
	}
	other := n.send(transport.OpDeclareStoreFields, DeclareStoreFieldsRequest{Database: "app", Name: "User",
		Stores: []map[string]interface{}{{"name": "Purchases", "engine": "lifoStore"}}})
	if other.Opcode != transport.OpError {
		t.Fatalf("declaring a field with another engine answered %s %q, want a refusal", other.Opcode, other.Payload)
	}
	wantDeclare := `The store fields of spread bucket "User" (database "app") are set when it is created, so declaring new ones is refused ` +
		`(declaring fields it already has is accepted, which is what a restore of its own dump does). ` +
		`To add a store field: dump --out ./dump-User app User, then drop bucket User in app, then create bucket User in app { placement: spread, <the old store fields and the new one> }, then restore --dir ./dump-User`
	fresh := n.send(transport.OpDeclareStoreFields, DeclareStoreFieldsRequest{Database: "app", Name: "User",
		Stores: []map[string]interface{}{{"name": "Wishes", "engine": "hashStore"}}})
	requireRefusedWith(t, "declaring a new store field", fresh, wantDeclare)
	wantAlter := `The replication factor of spread bucket "User" (database "app") is set when it is created, so 'alter bucket' is refused. ` +
		`To change it: dump --out ./dump-User app User, then drop bucket User in app, then create bucket User in app { placement: spread, replication_factor: <N> } with the bucket's other fields, then restore --dir ./dump-User`
	requireRefusedWith(t, "alter bucket", n.send(transport.OpAlterBucket, AlterBucketRequest{Database: "app", Name: "User", ReplicationFactor: 1}), wantAlter)
	for _, text := range []string{wantDeclare, wantAlter} {
		if strings.Contains(text, unitSeparator) {
			t.Fatal("a refusal names a part")
		}
	}
}

// TestMigrateOfASpreadTemplateAtANodeThatLacksItIsRefused: the operator's
// migrate is refused by the node that holds the template when another node,
// which lacks it, pulls it: the puller cannot tell a template from any other
// bucket, so the source says so. A peer's hand-off of the same template is
// still served.
func TestMigrateOfASpreadTemplateAtANodeThatLacksItIsRefused(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	sc.insertMany(0, "doc", 100)
	set := sc.templateSet("User")
	// A node in the template's set that has dropped its copy: the operator asks
	// it to migrate the bucket back.
	replica := set[1]
	core := sc.nodes[replica].server.core
	if resp, err := core.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "User"}); err != nil || !resp.Success {
		t.Fatalf("emptying the replica: %+v %v", resp, err)
	}
	reply := sc.frame(replica, transport.OpMigrate, MigrateRequest{Database: "app", Bucket: "User"})
	text := refusalTextOrEmptyTB(t, reply)
	if reply.Opcode != transport.OpError || !strings.Contains(text, "moves part by part") {
		t.Fatalf("an operator's migrate at a node that lacks the template answered %s %q", reply.Opcode, text)
	}
	if core.bucketStoreOpen("app", "User") {
		t.Fatal("the template was pulled to the node that asked")
	}
	if text == "" || strings.Contains(text, unitSeparator) {
		t.Fatalf("the refusal %q names a part", text)
	}
}
