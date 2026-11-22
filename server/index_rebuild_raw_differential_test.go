package server

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The raw-byte index rebuild differential (bson-codec-spec 7.2, build doc
// step 7a, path "index rebuild"). A rebuild walked every stored document of
// the bucket and decoded it into a map to compute its entries, although an
// entry reads only the index's own top-level fields. The oracle below is that
// map path, copied from server/index_rebuild.go:(*Server).writeRebuiltEntries
// as it stood before the move (next at bbf315c7). The tests run the oracle
// and the server's rebuild over the same stored documents, one after the
// other from a cleared range, and compare the documents walked, the
// conflicts, the error, and every entry written.
//
// store.EntryKey's input does not change (it still takes the map, now of the
// indexed fields only), so TestEntryKeyMatchesTheMapInput is not needed (build
// doc step 7a, index rebuild row).

// oracleWriteRebuiltEntries is writeRebuiltEntries as it was.
func (s *Server) oracleWriteRebuiltEntries(bucket *inMemoryBucket, targets []store.IndexSpec) (int, map[uint16]rebuildConflict, error) {
	indexes := make([]*store.SecondaryIndex, len(targets))
	for i, spec := range targets {
		indexes[i] = store.NewSecondaryIndex(s.kv, bucket.slot, bucket.databaseID, bucket.id.Load(), spec.ID)
	}
	batch := newBoundedBatch(s.kv)
	defer batch.discard()
	checks := newRebuildChecks(s.kv)
	defer checks.close()
	batch.committed = func(number int) error {
		checks.committedBatch()
		return nil
	}
	conflicts := make(map[uint16]rebuildConflict)
	documents := 0
	err := bucket.hashStore.IterateDocuments(func(key, value []byte) error {
		var document map[string]interface{}
		if err := bson.Unmarshal(value, &document); err != nil {
			return fmt.Errorf("decoding document %q: %w", key, err)
		}
		for i, spec := range targets {
			encoded, indexed, err := store.EntryKey(spec, document)
			if err != nil {
				return fmt.Errorf("document %q: %w", key, err)
			}
			if !indexed {
				continue
			}
			if _, seen := conflicts[spec.ID]; spec.Unique && !seen {
				holder, held, err := checks.holder(indexes[i], spec.ID, encoded, key)
				if err != nil {
					return fmt.Errorf("checking unique index %s: %w", spec.Name, err)
				}
				if held {
					conflicts[spec.ID] = rebuildConflict{first: holder, second: string(key)}
				}
			}
			if err := batch.add(func(b storage.KVBatch) error { return indexes[i].Add(b, encoded, key) }, len(key)+len(encoded)); err != nil {
				return err
			}
			if spec.Unique {
				checks.claim(spec.ID, encoded, key)
			}
		}
		documents++
		return nil
	})
	if err != nil {
		return documents, nil, err
	}
	return documents, conflicts, batch.flush()
}

// rebuildAnswer is what one rebuild reports and leaves in its indexes.
type rebuildAnswer struct {
	documents int
	conflicts string
	err       string
	entries   string
}

// runRebuild clears targets' ranges, runs rebuild over the bucket and reads
// back what it wrote.
func runRebuild(t *testing.T, s *Server, bucket *inMemoryBucket, targets []store.IndexSpec,
	rebuild func(*inMemoryBucket, []store.IndexSpec) (int, map[uint16]rebuildConflict, error)) rebuildAnswer {
	t.Helper()
	for _, spec := range targets {
		if err := s.clearIndexRange(bucket.slot, bucket.databaseID, bucket.id.Load(), spec); err != nil {
			t.Fatalf("clearing %s: %v", spec.Name, err)
		}
	}
	documents, conflicts, err := rebuild(bucket, targets)
	answer := rebuildAnswer{documents: documents, err: "<nil>"}
	if err != nil {
		answer.err = err.Error()
	}
	ids := make([]int, 0, len(conflicts))
	for id := range conflicts {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, id := range ids {
		answer.conflicts += fmt.Sprintf("%d:%s/%s ", id, conflicts[uint16(id)].first, conflicts[uint16(id)].second)
	}
	var entries strings.Builder
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	for _, spec := range targets {
		it := snap.NewIterator(store.NewSecondaryIndex(s.kv, bucket.slot, bucket.databaseID, bucket.id.Load(), spec.ID).EntryRange())
		for it.Seek(nil); it.Valid(); it.Next() {
			fmt.Fprintf(&entries, "%x\n", it.Key())
		}
		if err := it.Err(); err != nil {
			t.Fatalf("reading %s: %v", spec.Name, err)
		}
		it.Close()
	}
	answer.entries = entries.String()
	return answer
}

// rebuildBucket stores each document's bytes as they are under its own key
// in a new hash bucket, and returns the bucket.
func rebuildBucket(t *testing.T, s *Server, name string, documents [][]byte) *inMemoryBucket {
	t.Helper()
	createTestBucket(t, s, "app", name, BucketTypeHash)
	bucket, err := s.resolveBucket("app", name)
	if err != nil {
		t.Fatalf("resolving %s: %v", name, err)
	}
	for position, raw := range documents {
		if err := bucket.hashStore.Put(store.ApplyInfo{}, fmt.Sprintf("d%05d", position), bson.Raw(raw), 0); err != nil {
			t.Fatalf("storing document %d: %v", position, err)
		}
	}
	return bucket
}

// indexableDocuments are generated documents whose top-level values are all
// ones an index encodes, so a rebuild over them walks every document; names
// repeat within a document and values repeat across documents, so unique
// indexes conflict.
func indexableDocuments(seed int64, count int) [][]byte {
	random := rand.New(rand.NewSource(seed))
	names := []string{"a", "b", "name", "", "0"}
	documents := make([][]byte, 0, count)
	for number := 0; number < count; number++ {
		var elements [][]byte
		for element := random.Intn(6); element > 0; element-- {
			name := names[random.Intn(len(names))]
			switch random.Intn(5) {
			case 0:
				elements = append(elements, int32Element(name, random.Intn(4)))
			case 1:
				elements = append(elements, elemOf(0x02, name, bsonString(fmt.Sprintf("v%d", random.Intn(4)))...))
			case 2:
				elements = append(elements, elemOf(0x0A, name))
			case 3:
				elements = append(elements, elemOf(0x08, name, byte(random.Intn(2))))
			default:
				elements = append(elements, elemOf(0x01, name, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f))
			}
		}
		documents = append(documents, docOf(elements...))
	}
	return documents
}

// rebuildTargets are the index sets the differential rebuilds: each name
// alone, before this build and after it, unique and not, and compounds.
func rebuildTargets(names []string) [][]store.IndexSpec {
	var sets [][]store.IndexSpec
	spec := func(id int, unique bool, version uint8, fields ...string) store.IndexSpec {
		parts := make([]store.IndexKeyPart, len(fields))
		for position, field := range fields {
			parts[position] = store.IndexKeyPart{Field: field}
		}
		return store.IndexSpec{ID: uint16(id), Name: fmt.Sprintf("index%d", id), Fields: parts, Unique: unique, Version: version}
	}
	for _, name := range names {
		sets = append(sets,
			[]store.IndexSpec{spec(0, false, store.IndexSpecVersion, name)},
			[]store.IndexSpec{spec(0, true, store.IndexSpecVersion, name), spec(1, false, 0, name)})
	}
	for position := 0; position+1 < len(names); position++ {
		sets = append(sets, []store.IndexSpec{spec(0, true, store.IndexSpecVersion, names[position], names[position+1]),
			spec(1, false, store.IndexSpecVersion, names[position+1])})
	}
	return sets
}

// TestIndexRebuildMatchesTheMapPath rebuilds every target set over two
// buckets, the differential's documents as stored and generated documents an
// index can encode whole, through the map path and the server's, and compares
// the two answers and the entries each wrote.
func TestIndexRebuildMatchesTheMapPath(t *testing.T) {
	s := newTestServer(t)
	var everything [][]byte
	for _, document := range pageDifferentialDocuments(t) {
		everything = append(everything, document.raw)
	}
	buckets := map[string]*inMemoryBucket{
		"rebuildall":       rebuildBucket(t, s, "rebuildall", everything),
		"rebuildindexable": rebuildBucket(t, s, "rebuildindexable", indexableDocuments(81, 600)),
	}
	names := []string{"a", "b", "name", "", "0", "seq", "_id_", "k", "label", "double", "absent"}
	compared, walkedWhole, conflicted := 0, 0, 0
	for bucketName, bucket := range buckets {
		for _, targets := range rebuildTargets(names) {
			want := runRebuild(t, s, bucket, targets, s.oracleWriteRebuiltEntries)
			got := runRebuild(t, s, bucket, targets, s.writeRebuiltEntries)
			compared++
			if want != got {
				t.Fatalf("%s, indexes %v: the map path gives %+v, the server %+v", bucketName, targets, want, got)
			}
			if want.err == "<nil>" {
				walkedWhole++
			}
			if want.conflicts != "" {
				conflicted++
			}
		}
	}
	if walkedWhole == 0 || conflicted == 0 {
		t.Fatalf("of %d rebuilds, %d walked every document and %d found a conflict; the comparison needs both", compared, walkedWhole, conflicted)
	}
	t.Logf("%d rebuilds compared: %d walked every document, %d found a conflict", compared, walkedWhole, conflicted)
}

// TestIndexRebuildReadsTheIndexedFieldsOfACorruptDocument: a stored document
// whose indexed field reads but whose other bytes are not BSON is indexed by
// a rebuild, where the map path failed the rebuild on it and left the index
// building. One whose top level does not walk still fails it.
func TestIndexRebuildReadsTheIndexedFieldsOfACorruptDocument(t *testing.T) {
	s := newTestServer(t)
	readable := docOf(elemOf(0x02, "a", bsonString("dalek")...), elemOf(0x03, "rest", docOf(elemOf(0x20, "x", 1, 2, 3, 4))...))
	unwalkable := docOf(elemOf(0x02, "a", append(le32(500), 'x', 0)...))
	target := []store.IndexSpec{{ID: 0, Name: "a_1", Fields: []store.IndexKeyPart{{Field: "a"}}, Version: store.IndexSpecVersion}}

	bucket := rebuildBucket(t, s, "corruptrest", nil)
	writeStoredBytes(t, s, bucket, "d1", readable)
	documents, _, err := s.writeRebuiltEntries(bucket, target)
	if err != nil || documents != 1 {
		t.Fatalf("rebuilding over a document whose field rest is corrupt: %d documents, %v; want 1 and no error", documents, err)
	}
	wantKey, _, err := store.EntryKey(target[0], map[string]interface{}{"a": "dalek"})
	if err != nil {
		t.Fatalf("EntryKey: %v", err)
	}
	got := runRebuild(t, s, bucket, target, s.writeRebuiltEntries)
	want := fmt.Sprintf("%x\n", append(store.NewSecondaryIndex(s.kv, bucket.slot, bucket.databaseID, bucket.id.Load(), 0).EntryRange().Lower, append(wantKey, "d1"...)...))
	if got.entries != want {
		t.Fatalf("the rebuilt index holds %q, want %q", got.entries, want)
	}

	bucket = rebuildBucket(t, s, "corrupttop", nil)
	writeStoredBytes(t, s, bucket, "d1", unwalkable)
	if _, _, err := s.writeRebuiltEntries(bucket, target); err == nil || !strings.Contains(err.Error(), `decoding document "d1"`) {
		t.Fatalf("rebuilding over a document whose top level does not walk: %v; want the decode refusal", err)
	}
}

// writeStoredBytes writes raw as a stored document of bucket directly, as a
// document stored before a value check existed would sit in the store.
func writeStoredBytes(t *testing.T, s *Server, bucket *inMemoryBucket, key string, raw []byte) {
	t.Helper()
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(storage.Key(bucket.slot, bucket.databaseID, bucket.id.Load(), storage.NSDocuments, []byte(key)), raw); err != nil {
		t.Fatalf("writing %s: %v", key, err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("writing %s: %v", key, err)
	}
}

// BenchmarkRawPathIndexRebuild is a rebuild of one index over the 2,000
// ten-field documents of the raw-path benchmarks, through the map path and
// through the field read (bson-codec-spec 9.2: informational, not a gate).
func BenchmarkRawPathIndexRebuild(b *testing.B) {
	s := seedRawPathBench(b)
	bucket, err := s.resolveBucket("bench", "docs")
	if err != nil {
		b.Fatalf("resolving the bucket: %v", err)
	}
	targets := []store.IndexSpec{{ID: 0, Name: "city_1", Fields: []store.IndexKeyPart{{Field: "city"}}, Version: store.IndexSpecVersion}}
	for _, path := range []struct {
		name    string
		rebuild func(*inMemoryBucket, []store.IndexSpec) (int, map[uint16]rebuildConflict, error)
	}{{"map", s.oracleWriteRebuiltEntries}, {"fields", s.writeRebuiltEntries}} {
		b.Run(path.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, err := path.rebuild(bucket, targets); err != nil {
					b.Fatalf("rebuilding: %v", err)
				}
			}
		})
	}
}
