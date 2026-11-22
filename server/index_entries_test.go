package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// Index entries keyed by descriptor (GAP-14 build doc Step 3). The entry of a
// document for an index is [s][id][0x03] ‖ index id(2) ‖ EncodeKey(values) ‖
// document key, with an empty value; these tests read the keys themselves.

// indexEntryPrefix is the prefix of every entry of one index of a bucket.
func indexEntryPrefix(b *inMemoryBucket, indexID uint16) []byte {
	var idBytes [2]byte
	binary.BigEndian.PutUint16(idBytes[:], indexID)
	return storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSIndex, idBytes[:])
}

// indexEntries returns every entry key of one index of a bucket.
func indexEntries(t *testing.T, s *Server, b *inMemoryBucket, indexID uint16) [][]byte {
	t.Helper()
	prefix := indexEntryPrefix(b, indexID)
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: upper})
	defer it.Close()
	var keys [][]byte
	for it.Seek(nil); it.Valid(); it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}
	return keys
}

// indexedView builds a second in-memory bucket over an existing bucket's
// range, declared by options — the attach path without the attach, so a test
// can drive an index manager built from a pre-build record.
func indexedView(t *testing.T, s *Server, served *inMemoryBucket, options map[string]interface{}) *inMemoryBucket {
	t.Helper()
	view := &inMemoryBucket{databaseID: served.databaseID}
	resp, err := s.prepareBucketLocked(view, CreateBucketRequest{Database: "app", Name: "view", Type: BucketTypeHash, Options: options})
	if err != nil || !resp.Success {
		t.Fatalf("prepareBucketLocked = %+v, %v", resp, err)
	}
	if err := s.buildEngines(view, s.kv, served.slot, served.id.Load()); err != nil {
		t.Fatalf("buildEngines: %v", err)
	}
	return view
}

// TestMissingFieldIndexesAsNull (spec §10, §5.5): an index this build
// declares writes one null (0x10) entry for a document that lacks the field
// and one for a document whose field is BSON null — the entry the unique
// "one document missing the field" rule and a compound key both need. An
// index declared before this build writes none until it is rebuilt: null
// entries in half of a pre-build index would leave a mixture no one could
// identify (§4 F-9).
func TestMissingFieldIndexesAsNull(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "declared", []interface{}{"Email"})
	for key, doc := range map[string]map[string]interface{}{
		"missing": {"Name": "no email"},
		"null":    {"Email": nil},
		"present": {"Email": "a@example.com"},
	} {
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "declared", Key: key, Data: doc}); err != nil || !resp.Success {
			t.Fatalf("Insert(%s) = %+v, %v", key, resp, err)
		}
	}
	declared := servedBucket(t, s, "app", "declared")
	nullPrefix := append(indexEntryPrefix(declared, 0), 0x10)
	var nullEntries []string
	for _, entry := range indexEntries(t, s, declared, 0) {
		if bytes.HasPrefix(entry, nullPrefix) {
			nullEntries = append(nullEntries, string(entry[len(nullPrefix):]))
		}
	}
	if strings.Join(nullEntries, ",") != "missing,null" {
		t.Errorf("the declared index holds null entries for %v, want [missing null]", nullEntries)
	}

	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "plain", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket(plain) = %+v, %v", resp, err)
	}
	plain := servedBucket(t, s, "app", "plain")
	preBuild := indexedView(t, s, plain, indexOptions([]catalogIndex{{Field: "Email"}}))
	for key, doc := range map[string]map[string]interface{}{"missing": {"Name": "no email"}, "null": {"Email": nil}} {
		if err := preBuild.hashStore.Put(store.ApplyInfo{}, key, doc, 0); err != nil {
			t.Fatalf("Put(%s) through a pre-build index: %v", key, err)
		}
	}
	if entries := indexEntries(t, s, plain, 0); len(entries) != 0 {
		t.Errorf("a pre-build index wrote %d entries for documents without a value, want none: %q", len(entries), entries)
	}
}

// TestUnindexableValueWritesNothing (spec §10's
// TestArrayValuedFieldIsRefusedAndWritesNothing and
// TestNaNAndInfInAnIndexedFieldAreRefusedAndWriteNothing, parameterised): a
// value no index key can hold refuses the write inside its batch, naming the
// field and the index, and nothing of the write lands.
func TestUnindexableValueWritesNothing(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "users", []interface{}{"Email"})
	users := servedBucket(t, s, "app", "users")
	for name, value := range map[string]interface{}{
		"array":        []interface{}{"a", "b"},
		"sub-document": map[string]interface{}{"local": "a"},
		"decimal128":   primitive.NewDecimal128(1, 0),
		"NaN":          math.NaN(),
		"+Inf":         math.Inf(1),
		"-Inf":         math.Inf(-1),
	} {
		before, err := users.hashStore.Len()
		if err != nil {
			t.Fatalf("Len: %v", err)
		}
		resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "k-" + name, Data: map[string]interface{}{"Email": value}})
		if err != nil {
			t.Fatalf("%s: Insert: %v", name, err)
		}
		if resp.Success {
			t.Errorf("%s: an unindexable value was written", name)
			continue
		}
		for _, want := range []string{`"Email"`, "Email_1"} {
			if !strings.Contains(resp.Message, want) {
				t.Errorf("%s: the refusal does not name %s: %s", name, want, resp.Message)
			}
		}
		if found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "k-" + name}); err != nil || found.Found {
			t.Errorf("%s: the refused document is present (%+v, %v)", name, found, err)
		}
		if entries := indexEntries(t, s, users, 0); len(entries) != 0 {
			t.Errorf("%s: the refused write left %d index entries", name, len(entries))
		}
		if after, err := users.hashStore.Len(); err != nil || after != before {
			t.Errorf("%s: the entry count moved from %d to %d (%v)", name, before, after, err)
		}
	}
}

// TestIndexKeysAreNotCountedAsDocuments (spec §10, a pin): index entries live
// in their own namespace, so a recount of the documents does not count them —
// the "count doubles on restart" class, now a namespace byte (C-33).
func TestIndexKeysAreNotCountedAsDocuments(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "users", []interface{}{"Email", "Age"})
	for i := 0; i < 5; i++ {
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: fmt.Sprintf("u%d", i),
			Data: map[string]interface{}{"Email": fmt.Sprintf("u%d@example.com", i), "Age": i}}); err != nil || !resp.Success {
			t.Fatalf("Insert = %+v, %v", resp, err)
		}
	}
	users := servedBucket(t, s, "app", "users")
	recounted, err := users.hashStore.Recount()
	if err != nil || recounted != 5 {
		t.Fatalf("Recount = %d, %v; want the 5 documents and none of their 10 index entries", recounted, err)
	}
	if entries := indexEntries(t, s, users, 0); len(entries) != 5 {
		t.Fatalf("%d entries in the Email index, want 5: the pin above proves nothing without them", len(entries))
	}
}

// TestEntryPrefixIsUnchangedForAPreBuildIndex: an index declared before this
// build keeps its entry bytes — the prefix is the list position, which is the
// id — so the upgrade rebuild can clear and rewrite a known range rather than
// hunt for orphans. The expected key is written out here from the key grammar.
func TestEntryPrefixIsUnchangedForAPreBuildIndex(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "plain", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	plain := servedBucket(t, s, "app", "plain")
	preBuild := indexedView(t, s, plain, indexOptions([]catalogIndex{{Field: "Email"}, {Field: "Age", Type: "range"}}))
	if err := preBuild.hashStore.Put(store.ApplyInfo{}, "k", map[string]interface{}{"Email": "x@example.com", "Age": 5}, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for position, value := range []interface{}{"x@example.com", 5} {
		encoded, err := store.EncodeComponent(value)
		if err != nil {
			t.Fatalf("EncodeComponent(%v): %v", value, err)
		}
		want := append(append(indexEntryPrefix(plain, uint16(position)), encoded...), "k"...)
		entries := indexEntries(t, s, plain, uint16(position))
		if len(entries) != 1 || !bytes.Equal(entries[0], want) {
			t.Errorf("index %d holds %x, want exactly %x", position, entries, want)
		}
	}
}

// TestDocumentIsDecodedOncePerWrite: a write decodes its document once for
// every index it maintains, not once per index — extractField marshalled and
// unmarshalled the whole document per indexed field, twice on an update
// (C-57). Measured as allocations: the extra cost of two more indexes on a
// 200-field document must be their entries, not two more decodes of it.
func TestDocumentIsDecodedOncePerWrite(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "one", []interface{}{"A"})
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "three", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"A", "B", "C"}}}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket(three) = %+v, %v", resp, err)
	}
	document := map[string]interface{}{"A": "a", "B": "b", "C": "c"}
	for i := 0; i < 200; i++ {
		document[fmt.Sprintf("f%03d", i)] = fmt.Sprintf("value %d", i)
	}
	allocsPerOverwrite := func(bucket string) float64 {
		return testing.AllocsPerRun(20, func() {
			written := make(map[string]interface{}, len(document))
			for k, v := range document {
				written[k] = v
			}
			if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: "k", Data: written}); err != nil || !resp.Success {
				t.Fatalf("Insert(%s) = %+v, %v", bucket, resp, err)
			}
		})
	}
	one, three := allocsPerOverwrite("one"), allocsPerOverwrite("three")
	perExtraIndex := (three - one) / 2
	t.Logf("allocations per overwrite: one index %.0f, three indexes %.0f; %.0f per extra index", one, three, perExtraIndex)
	if perExtraIndex > 150 {
		t.Fatalf("each extra index costs %.0f allocations on a 200-field document: the document is decoded per index", perExtraIndex)
	}
}

// TestCLIFloatSearchFindsAnInt32Document (the server half of spec §10's
// TestNumericKeyIsTheMathematicalValue; a pin): the CLI sends 30 as a float64,
// a driver may have stored it as an int32, and the index key is the
// mathematical value either way.
func TestCLIFloatSearchFindsAnInt32Document(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "people", []interface{}{"Age"})
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "people", Key: "p", Data: map[string]interface{}{"Age": int32(30)}}); err != nil || !resp.Success {
		t.Fatalf("Insert = %+v, %v", resp, err)
	}
	hits, err := s.Search(SearchRequest{Database: "app", Bucket: "people", Filter: map[string]interface{}{"Age": 30.0}})
	if err != nil || hits.Total != 1 {
		t.Fatalf("search({Age: 30.0}) = %+v, %v; want the int32(30) document", hits, err)
	}
}

// TestObjectIDIndexedFieldIsWritten (U3): an ObjectID is a key type (conventions
// §5), so a document whose indexed field holds one gets an index entry, and a
// search by that value finds the document.
func TestObjectIDIndexedFieldIsWritten(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "users", []interface{}{"Owner"})
	users := servedBucket(t, s, "app", "users")
	owner := primitive.NewObjectID()
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "u1", Data: map[string]interface{}{"Owner": owner}}); err != nil || !resp.Success {
		t.Fatalf("Insert = %+v, %v", resp, err)
	}
	if entries := indexEntries(t, s, users, 0); len(entries) != 1 {
		t.Fatalf("the index holds %d entries for one ObjectID document, want 1", len(entries))
	}
	hits, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Owner": owner}})
	if err != nil || hits.Total != 1 {
		t.Fatalf("search by ObjectID = %+v, %v; want the one document", hits, err)
	}
}
