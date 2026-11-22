package server

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The factor's three fields on the restore wire are read and written here by
// their wire names, through bson.M, rather than through the Go structs: the
// names are what a dump file and a raw client carry, and a test that compiles
// only once the struct fields exist cannot be run against the code it fixes.

// wireInt reads a BSON number field of m as an int, zero when absent.
func wireInt(t *testing.T, m bson.M, name string) int {
	t.Helper()
	switch number := m[name].(type) {
	case nil:
		return 0
	case int32:
		return int(number)
	case int64:
		return int(number)
	default:
		t.Fatalf("field %q is a %T (%v), want a number", name, number, number)
		return 0
	}
}

// asWire re-encodes v as the document it travels as.
func asWire(t *testing.T, v any) bson.M {
	t.Helper()
	encoded, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("encoding %T: %v", v, err)
	}
	var doc bson.M
	if err := bson.Unmarshal(encoded, &doc); err != nil {
		t.Fatalf("decoding %T: %v", v, err)
	}
	return doc
}

// restoreStartFromManifest is the start request the CLI builds from a
// metadata file that records factor (0: the file has no field), aimed at
// target.
func restoreStartFromManifest(t *testing.T, manifest *DumpBucketManifest, target string, factor int, raw []byte, drop, dryRun bool) bson.M {
	t.Helper()
	start := asWire(t, RestoreStartRequest{
		Database: manifest.Database, Bucket: target, FormatVersion: manifest.FormatVersion, StoreType: manifest.StoreType,
		Indexes: manifest.Indexes, IndexSpecs: manifest.IndexSpecs, Stores: manifest.Stores, Bytes: int64(len(raw)), Drop: drop, DryRun: dryRun,
	})
	if factor != 0 {
		start["replication_factor"] = factor
	}
	return start
}

// restoreWithFactorOverWire runs one restore on node i of tc and returns the
// start answer as it travelled.
func restoreWithFactorOverWire(t *testing.T, tc *testCluster, i int, start bson.M, raw []byte, documents int64) bson.M {
	t.Helper()
	answer := asWireFrame(t, tc.ok(i, transport.OpRestoreStart, start))
	database, bucket := start["database"].(string), start["bucket"].(string)
	session, _ := answer["session_id"].(string)
	tc.ok(i, transport.OpRestoreChunk, RestoreChunkRequest{Database: database, Bucket: bucket, SessionID: session, Data: raw})
	tc.ok(i, transport.OpRestoreDone, RestoreDoneRequest{Database: database, Bucket: bucket, SessionID: session, Documents: documents})
	return answer
}

// asWireFrame decodes an answer's payload.
func asWireFrame(t *testing.T, frame *transport.Frame) bson.M {
	t.Helper()
	var doc bson.M
	if err := bson.Unmarshal(frame.Payload, &doc); err != nil {
		t.Fatalf("decoding the %s answer: %v", frame.Opcode, err)
	}
	return doc
}

// dumpWithStoresOverWire dumps database/bucket on node i as a client that
// writes store fields, and returns the manifest as it travelled, the decoded
// manifest, and the documents. The bucket's store fields hold no elements, so
// every chunk is documents.
func dumpWithStoresOverWire(t *testing.T, tc *testCluster, i int, database, bucket string) (bson.M, *DumpBucketManifest, []byte) {
	t.Helper()
	startFrame := tc.ok(i, transport.OpDumpStart, DumpStartRequest{Database: database, Bucket: bucket, AcceptStores: true})
	answer := asWireFrame(t, startFrame)
	wireManifest, ok := answer["manifest"].(bson.M)
	if !ok {
		t.Fatalf("dump start of %s/%s carried no manifest: %v", database, bucket, answer)
	}
	var start DumpStartResponse
	if err := bson.Unmarshal(startFrame.Payload, &start); err != nil {
		t.Fatalf("decoding dump start: %v", err)
	}
	var documents []byte
	for {
		var chunk DumpChunkResponse
		if err := bson.Unmarshal(tc.ok(i, transport.OpDumpChunk, DumpChunkRequest{Database: database, Bucket: bucket}).Payload, &chunk); err != nil {
			t.Fatalf("decoding dump chunk: %v", err)
		}
		if chunk.Section == "stores" && len(chunk.Data) > 0 {
			t.Fatalf("dump of %s/%s streamed store elements; this test declares the field and writes none", database, bucket)
		}
		documents = append(documents, chunk.Data...)
		if chunk.EOF {
			break
		}
	}
	tc.ok(i, transport.OpDumpDone, DumpDoneRequest{Database: database, Bucket: bucket})
	return wireManifest, start.Manifest, documents
}

// declaredFactorOn is the factor node i's catalog entry for database/bucket
// declares, waiting for a replica to apply the create its owner shipped.
func declaredFactorOn(t *testing.T, tc *testCluster, i int, database, bucket string) int32 {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		entry, catalogued, err := tc.nodes[i].server.core.readCatalogEntry(database, bucket)
		if err != nil {
			t.Fatalf("reading node %d's catalog entry for %s/%s: %v", i+1, database, bucket, err)
		}
		if catalogued {
			return entry.ReplicationFactor
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d has no catalog entry for %s/%s after 20s", i+1, database, bucket)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ownerIndex is the index in tc of the ring owner of testDatabase/bucket.
func ownerIndex(t *testing.T, tc *testCluster, bucket string) int {
	t.Helper()
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("owner of %q: %v", bucket, err)
	}
	return nodeIndexByAddr(t, tc.addrs, owner.ID)
}

// TestDumpRestoreRoundTripsTheFactorBesideIndexesAndStores is T-13a
// (bucket-replication spec §17.4.10, §18.9; BUG-0497). A dump records the factor
// a bucket declared, beside its index specs and store fields, and a restore
// re-creates the bucket with it on every path that creates: into a new name,
// and under --drop. A dump written before the field restores inheriting; a
// merge leaves the target's factor alone; a factor the cluster cannot grant
// is restored inheriting; --drop from a dump with no factor over a bucket that
// declared one lowers it. Each of the last three is said in the answer.
func TestDumpRestoreRoundTripsTheFactorBesideIndexesAndStores(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: "wide", Type: BucketTypeHash, ReplicationFactor: 3,
		Options: map[string]interface{}{
			"indices": []interface{}{map[string]interface{}{"fields": []string{"tag"}, "name": "by_tag"}},
			"stores":  []interface{}{map[string]interface{}{"name": "Comments", "engine": "fifoStore"}},
		},
	})
	tc.ok(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: "wide", Key: "seed", Data: map[string]interface{}{"tag": "seed-v"}, WriteConcern: "all",
	})
	wireManifest, manifest, raw := dumpWithStoresOverWire(t, tc, 0, testDatabase, "wide")
	if got := wireInt(t, wireManifest, "replication_factor"); got != 3 {
		t.Errorf("the dump of a bucket created at replication_factor 3 records %d; want 3 beside index_specs %v and stores %v", got, manifest.IndexSpecs, manifest.Stores)
	}
	if len(manifest.IndexSpecs) != 1 || len(manifest.Stores) != 1 {
		t.Fatalf("the dump records index specs %v and stores %v; want one of each", manifest.IndexSpecs, manifest.Stores)
	}
	recorded := wireInt(t, wireManifest, "replication_factor")

	t.Run("round_trip_after_a_drop", func(t *testing.T) {
		tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: "wide"})
		owner := ownerIndex(t, tc, "wide")
		restoreWithFactorOverWire(t, tc, owner, restoreStartFromManifest(t, manifest, "wide", recorded, raw, false, false), raw, 1)
		for i := range tc.nodes {
			if got := declaredFactorOn(t, tc, i, testDatabase, "wide"); got != 3 {
				t.Errorf("node %d restored wide declaring replication_factor %d, want 3", i+1, got)
			}
		}
		replicas, err := tc.nodes[owner].server.core.cluster.Replicas(placementOf(testDatabase, "wide"))
		if err != nil || len(replicas) != 3 {
			t.Fatalf("the restored bucket is placed on %d node(s) (%v), want 3", len(replicas), err)
		}
		entry, _, err := tc.nodes[owner].server.core.readCatalogEntry(testDatabase, "wide")
		if err != nil || len(entry.Indices) != 1 || len(entry.Stores) != 1 {
			t.Fatalf("the restored bucket declares indexes %v and stores %v (%v); want the dump's one of each", entry.Indices, entry.Stores, err)
		}
	})

	t.Run("a_dump_without_the_field_restores_inheriting", func(t *testing.T) {
		owner := ownerIndex(t, tc, "from_old_dump")
		answer := restoreWithFactorOverWire(t, tc, owner, restoreStartFromManifest(t, manifest, "from_old_dump", 0, raw, false, false), raw, 1)
		if got := declaredFactorOn(t, tc, owner, testDatabase, "from_old_dump"); got != 0 {
			t.Fatalf("a dump without a factor restored declaring %d, want none (inherit)", got)
		}
		for _, field := range []string{"factor_not_restored", "factor_kept", "factor_lowered"} {
			if _, present := answer[field]; present {
				t.Errorf("a plain restore answered with %s: %v", field, answer)
			}
		}
	})

	t.Run("a_merge_keeps_the_targets_factor_and_says_so", func(t *testing.T) {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "merge_target", Type: BucketTypeHash})
		owner := ownerIndex(t, tc, "merge_target")
		dry := asWireFrame(t, tc.ok(owner, transport.OpRestoreStart, restoreStartFromManifest(t, manifest, "merge_target", recorded, raw, false, true)))
		answer := restoreWithFactorOverWire(t, tc, owner, restoreStartFromManifest(t, manifest, "merge_target", recorded, raw, false, false), raw, 1)
		for name, reply := range map[string]bson.M{"the dry run": dry, "the restore": answer} {
			if got := wireInt(t, reply, "factor_kept"); got != 1 {
				t.Errorf("%s of a dump at 3 into a bucket at the default of 1 answered factor_kept %d, want 1: %v", name, got, reply)
			}
		}
		if got := declaredFactorOn(t, tc, owner, testDatabase, "merge_target"); got != 0 {
			t.Fatalf("a merge changed the target's declared factor to %d; a merge restores documents, not placement", got)
		}
	})

	t.Run("a_factor_the_cluster_cannot_grant_restores_inheriting_and_says_so", func(t *testing.T) {
		owner := ownerIndex(t, tc, "too_wide")
		dry := asWireFrame(t, tc.ok(owner, transport.OpRestoreStart, restoreStartFromManifest(t, manifest, "too_wide", 5, raw, false, true)))
		answer := restoreWithFactorOverWire(t, tc, owner, restoreStartFromManifest(t, manifest, "too_wide", 5, raw, false, false), raw, 1)
		for name, reply := range map[string]bson.M{"the dry run": dry, "the restore": answer} {
			if got := wireInt(t, reply, "factor_not_restored"); got != 5 {
				t.Errorf("%s of a dump at 5 onto 3 members answered factor_not_restored %d, want 5: %v", name, got, reply)
			}
		}
		if got := declaredFactorOn(t, tc, owner, testDatabase, "too_wide"); got != 0 {
			t.Fatalf("an ungrantable factor was stored as %d; it must restore inheriting", got)
		}
	})

	t.Run("drop_recreates_with_the_dumps_factor", func(t *testing.T) {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "drop_target", Type: BucketTypeHash})
		owner := ownerIndex(t, tc, "drop_target")
		restoreWithFactorOverWire(t, tc, owner, restoreStartFromManifest(t, manifest, "drop_target", recorded, raw, true, false), raw, 1)
		for i := range tc.nodes {
			if got := declaredFactorOn(t, tc, i, testDatabase, "drop_target"); got != 3 {
				t.Errorf("node %d: restore --drop re-created drop_target declaring %d, want the dump's 3", i+1, got)
			}
		}
	})

	t.Run("drop_from_a_dump_without_the_field_lowers_and_says_so", func(t *testing.T) {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "lowered", Type: BucketTypeHash, ReplicationFactor: 3})
		owner := ownerIndex(t, tc, "lowered")
		dry := asWireFrame(t, tc.ok(owner, transport.OpRestoreStart, restoreStartFromManifest(t, manifest, "lowered", 0, raw, true, true)))
		answer := restoreWithFactorOverWire(t, tc, owner, restoreStartFromManifest(t, manifest, "lowered", 0, raw, true, false), raw, 1)
		for name, reply := range map[string]bson.M{"the dry run": dry, "the restore": answer} {
			if got := wireInt(t, reply, "factor_lowered"); got != 3 {
				t.Errorf("%s --drop from a dump without a factor over a bucket at 3 answered factor_lowered %d, want 3: %v", name, got, reply)
			}
		}
		if got := declaredFactorOn(t, tc, owner, testDatabase, "lowered"); got != 0 {
			t.Fatalf("restore --drop from a dump without a factor re-created the bucket declaring %d, want none", got)
		}
	})
}

// TestStandaloneDumpRestoreKeepsTheFactorItCanGrant: a standalone server grants
// a declared factor of one and no more. A bucket that declared one dumps with
// it and restores declaring it; a dump that records three restores inheriting,
// and the answer names the factor that was not restored rather than failing
// the restore (BUG-0497).
func TestStandaloneDumpRestoreKeepsTheFactorItCanGrant(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	createTestBucket(t, s, "app", "plain", BucketTypeHash)
	if _, msg := dumpDispatch(t, s, ctx, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "one", Type: BucketTypeHash, ReplicationFactor: 1}); msg != "" {
		t.Fatalf("creating a bucket at replication_factor 1 on a standalone server: %s", msg)
	}
	for _, name := range []string{"plain", "one"} {
		if _, err := s.Insert(InsertRequest{Database: "app", Bucket: name, Key: "a", Data: map[string]interface{}{"tag": "v"}}); err != nil {
			t.Fatalf("insert into %s: %v", name, err)
		}
	}

	plainManifest, _ := dumpBucketRaw(t, s, ctx, "app", "plain", 1)
	if got := wireInt(t, asWire(t, plainManifest), "replication_factor"); got != 0 {
		t.Fatalf("the dump of a bucket that declared no factor records %d, want no field", got)
	}
	manifest, raw := dumpBucketRaw(t, s, ctx, "app", "one", 1)
	if got := wireInt(t, asWire(t, manifest), "replication_factor"); got != 1 {
		t.Errorf("the dump of a bucket created at replication_factor 1 records %d, want 1", got)
	}

	restore := func(target string, factor int) bson.M {
		t.Helper()
		frame, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, restoreStartFromManifest(t, manifest, target, factor, raw, false, false))
		if msg != "" {
			t.Fatalf("restore of %s at factor %d refused: %s", target, factor, msg)
		}
		answer := asWireFrame(t, frame)
		session, _ := answer["session_id"].(string)
		if _, msg := dumpDispatch(t, s, ctx, transport.OpRestoreChunk, RestoreChunkRequest{Database: "app", Bucket: target, SessionID: session, Data: raw}); msg != "" {
			t.Fatalf("restore chunk of %s: %s", target, msg)
		}
		if _, msg := dumpDispatch(t, s, ctx, transport.OpRestoreDone, RestoreDoneRequest{Database: "app", Bucket: target, SessionID: session, Documents: 1}); msg != "" {
			t.Fatalf("restore done of %s: %s", target, msg)
		}
		return answer
	}

	restore("one_restored", 1)
	if entry, _, err := s.readCatalogEntry("app", "one_restored"); err != nil || entry.ReplicationFactor != 1 {
		t.Fatalf("the restored bucket declares %d (%v), want the dump's 1", entry.ReplicationFactor, err)
	}

	answer := restore("three_restored", 3)
	if got := wireInt(t, answer, "factor_not_restored"); got != 3 {
		t.Fatalf("a standalone restore of a dump at 3 answered factor_not_restored %d, want 3: %v", got, answer)
	}
	if entry, _, err := s.readCatalogEntry("app", "three_restored"); err != nil || entry.ReplicationFactor != 0 {
		t.Fatalf("an ungrantable factor was stored as %d (%v); it must restore inheriting", entry.ReplicationFactor, err)
	}
	if message, _ := answer["message"].(string); message != "" && !strings.Contains(message, "replication_factor") {
		t.Fatalf("the answer's message %q does not concern the factor", message)
	}
}
