package server

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// TestIndexOnAPathIsRefused: plain, unique, compound and TTL indexes on a path
// are refused at create with E-17, and nothing is created (spec §4.9.7). This
// closes the original reproduction: two distinct {a: {b: …}} documents can no
// longer meet a null-keyed unique index, because the index cannot be declared.
func TestIndexOnAPathIsRefused(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	for name, declared := range map[string]interface{}{
		"plain":    "Address.City",
		"unique":   map[string]interface{}{"field": "a.b", "unique": true},
		"compound": map[string]interface{}{"fields": []interface{}{"Customer", "Lines[0].Sku"}},
		"ttl":      map[string]interface{}{"field": "Audit.Seen", "expire_after_seconds": int64(60)},
	} {
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash,
			Options: map[string]interface{}{"indices": []interface{}{declared}}})
		if err != nil {
			t.Fatalf("CreateBucket(%s): %v", name, err)
		}
		if resp.Success {
			t.Errorf("a %s index on a path was accepted: %+v", name, resp)
			continue
		}
		if !strings.Contains(resp.Message, "this release indexes top-level fields only") || !strings.Contains(resp.Message, "keep a top-level copy") {
			t.Errorf("the refusal of a %s index on a path = %q; want E-17", name, resp.Message)
		}
		if _, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: name}); err == nil {
			t.Errorf("a refused create left bucket %q behind", name)
		}
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "top", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"City"}}}); err != nil || !resp.Success {
		t.Fatalf("an index on a top-level field was refused: %+v, %v", resp, err)
	}
}

// TestDottedPredicateIsServedByAScan: no index holds a path, so a predicate on
// one is served by a scan, which resolves the path, and search answers what
// find answers.
func TestDottedPredicateIsServedByAScan(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "places", "City")
	for key, document := range map[string]map[string]interface{}{
		"a": {"City": "Paris", "Address": map[string]interface{}{"City": "Tirana"}},
		"b": {"City": "Tirana", "Address": map[string]interface{}{"City": "Paris"}},
	} {
		mustInsert(t, s, "app", "places", key, document)
	}
	filter := map[string]interface{}{"Address.City": "Tirana"}
	searched, used := pathSearchKeys(t, s, "places", filter)
	if used != indexUsedFullScan {
		t.Errorf("search(%v) was served by index %q; no index holds a path", filter, used)
	}
	if found := pathFindKeys(t, s, "places", filter); !slices.Equal(searched, []string{"a"}) || !slices.Equal(found, searched) {
		t.Errorf("search(%v) = %v, find = %v, want [a] from both", filter, searched, found)
	}
}

// TestRecordedIndexOnAPathIsUnusableOnLoad: an index on a path recorded before
// the refusal (a development data directory that accepted it) is
// unusable when the bucket loads. The planner skips it, writes neither
// maintain nor enforce it, describe names it, a dump leaves it out and says
// so, and its catalog bytes are written back unchanged (spec §4.9.7, review A
// F-13). The catalog entry is written directly, as the spec says.
func TestRecordedIndexOnAPathIsUnusableOnLoad(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if _, err := first.core.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "app", Name: "legacy", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	mustInsert(t, first.core, "app", "legacy", "u0", map[string]interface{}{"Name": "zero"})
	// The path index in the field order this binary writes, so a rewrite of
	// the record that kept it would reproduce its bytes; the pre-build Name
	// index makes the start rebuild and rewrite the record.
	pathIndex := bson.D{{Key: "id", Value: int32(0)}, {Key: "name", Value: "a.b_1"}, {Key: "fields", Value: bson.A{"a.b"}},
		{Key: "unique", Value: true}, {Key: "version", Value: int32(store.IndexSpecVersion)}}
	writeOrderedCatalogIndices(t, first.core, "app", "legacy", bson.A{pathIndex, bson.D{{Key: "field", Value: "Name"}}})
	pathElement := func(s *Server) []byte {
		snap := s.kv.NewSnapshot()
		defer snap.Discard()
		encoded, err := snap.Get(catalogKey("app", "legacy"))
		if err != nil {
			t.Fatalf("reading the catalog record: %v", err)
		}
		element, err := bson.Raw(encoded).LookupErr("indices", "0")
		if err != nil {
			t.Fatalf("the record has no first index: %v", err)
		}
		return append([]byte(nil), element.Value...)
	}
	written := pathElement(first.core)

	second := restartCatalog(t, first, dataDir)
	s := second.core

	// The planner skips it: a predicate on a.b is a scan.
	_, used := pathSearchKeys(t, s, "legacy", map[string]interface{}{"a.b": int32(5)})
	if used != indexUsedFullScan {
		t.Errorf("search({a.b: 5}) was served by index %q; a recorded index on a path is unusable", used)
	}
	// Writes neither maintain nor enforce it: two distinct values, which its
	// unique form refused as duplicates of null (the original symptom).
	for key, value := range map[string]int32{"u1": 5, "u2": 6} {
		if refusal := insertOutcome(t, s, "app", "legacy", key, map[string]interface{}{"a": map[string]interface{}{"b": value}}); refusal != "" {
			t.Errorf("inserting %s was refused by an unusable index: %s", key, refusal)
		}
	}
	if got := pathFindKeys(t, s, "legacy", map[string]interface{}{"a.b": int32(6)}); !slices.Equal(got, []string{"u2"}) {
		t.Errorf("find({a.b: 6}) = %v, want [u2]", got)
	}

	// describe names it.
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "legacy"})
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	if !slices.ContainsFunc(stats.Indexes, func(line string) bool {
		return strings.HasPrefix(line, "a.b_1") && strings.Contains(line, "unusable: indexes a path; ignored")
	}) {
		t.Errorf("describe lists %v; want a.b_1 named unusable: indexes a path; ignored", stats.Indexes)
	}

	// A dump leaves it out of index_specs, and says so.
	dumped, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "legacy", AcceptStores: true})
	if err != nil {
		t.Fatalf("startDump: %v", err)
	}
	for _, spec := range dumped.Manifest.IndexSpecs {
		if spec.Name == "a.b_1" {
			t.Errorf("the dump manifest carries the unusable index: %+v", dumped.Manifest.IndexSpecs)
		}
	}
	if !strings.Contains(dumped.Message, "a.b_1") || !strings.Contains(dumped.Message, "path") {
		t.Errorf("the dump said %q; want a note that a.b_1 was left out because it indexes a path", dumped.Message)
	}
	if _, err := s.dumpDone(rootContext(), DumpDoneRequest{Database: "app", Bucket: "legacy"}); err != nil {
		t.Fatalf("releasing the dump: %v", err)
	}

	// It stays recorded, byte for byte, through the start's rewrite of the
	// record.
	stored := storedCatalogIndices(t, s, "app", "legacy")
	if len(stored) != 2 || stored[1]["version"] != int32(store.IndexSpecVersion) {
		t.Fatalf("precondition: the pre-build Name index was not rebuilt, so nothing rewrote the record: %v", stored)
	}
	if after := pathElement(s); !bytes.Equal(after, written) {
		t.Errorf("the record's index on a path changed:\n  written %v\n  now     %v", bson.Raw(written), bson.Raw(after))
	}
}

// writeOrderedCatalogIndices is writeCatalogIndices with each element's field
// order kept, so a test can compare an element's bytes after a rewrite.
func writeOrderedCatalogIndices(t *testing.T, s *Server, database, bucket string, indices bson.A) {
	t.Helper()
	snap := s.kv.NewSnapshot()
	encoded, err := snap.Get(catalogKey(database, bucket))
	snap.Discard()
	if err != nil {
		t.Fatalf("reading the catalog record of %s/%s: %v", database, bucket, err)
	}
	var record bson.D
	if err := bson.Unmarshal(encoded, &record); err != nil {
		t.Fatalf("decoding the catalog record: %v", err)
	}
	record = slices.DeleteFunc(record, func(element bson.E) bool { return element.Key == "indices" })
	record = append(record, bson.E{Key: "indices", Value: indices})
	rewritten, err := bson.Marshal(record)
	if err != nil {
		t.Fatalf("encoding the catalog record: %v", err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(catalogKey(database, bucket), rewritten); err != nil {
		t.Fatalf("writing the catalog record: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the catalog record: %v", err)
	}
}
