package store

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/internal/bsonoracle"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// The raw-byte hash store differential (bson-codec-spec 7.2, build doc step
// 7a, path "hash store"). The hash store decoded every stored document it
// indexed, unindexed or checked for expiry into a map, although an index
// reads only its own top-level fields. The oracle below is that map path,
// copied from store/hash.go as it stood before the move (next at bbf315c7),
// and the tests put the oracle and the store's path through the same
// documents on twin stores: the documents of the existing tests, the v1.0
// corpus and generated documents with every BSON type. They compare every
// error and every key the two leave in their stores.
//
// A document that repeats a top-level key is not a ruled difference here: the
// field reader keeps the last element of a repeated name, as the map decode
// does, so it is compared like any other.

// --- byte builders, with no codec ---

func rawLE32(value int) []byte {
	var out [4]byte
	binary.LittleEndian.PutUint32(out[:], uint32(int32(value)))
	return out[:]
}

func rawDoc(elements ...[]byte) []byte {
	body := []byte{}
	for _, element := range elements {
		body = append(body, element...)
	}
	return append(append(rawLE32(4+len(body)+1), body...), 0)
}

func rawElem(typeByte byte, name string, value ...byte) []byte {
	out := append([]byte{typeByte}, name...)
	out = append(out, 0)
	return append(out, value...)
}

func rawString(text string) []byte {
	return append(append(rawLE32(len(text)+1), text...), 0)
}

func rawInt32(name string, number int) []byte { return rawElem(0x10, name, rawLE32(number)...) }

// rawEveryType holds one value of every BSON type at the top level.
func rawEveryType() []byte {
	inner := rawDoc(rawInt32("i", 7))
	scope := append(rawLE32(4+len(rawString("f()"))+len(inner)), rawString("f()")...)
	scope = append(scope, inner...)
	date := make([]byte, 8)
	binary.LittleEndian.PutUint64(date, uint64(time.Date(2026, 9, 24, 18, 30, 15, 0, time.UTC).UnixMilli()))
	double := make([]byte, 8)
	binary.LittleEndian.PutUint64(double, math.Float64bits(2.5))
	nan := make([]byte, 8)
	binary.LittleEndian.PutUint64(nan, math.Float64bits(math.NaN()))
	return rawDoc(
		rawElem(0x01, "double", double...),
		rawElem(0x01, "nan", nan...),
		rawElem(0x02, "string", rawString("hello")...),
		rawElem(0x03, "doc", inner...),
		rawElem(0x04, "array", inner...),
		rawElem(0x05, "binary", append(append(rawLE32(3), 0x00), 1, 2, 3)...),
		rawElem(0x05, "oldbinary", append(append(rawLE32(6), 0x02), append(rawLE32(2), 'a', 'b')...)...),
		rawElem(0x06, "undefined"),
		rawElem(0x07, "oid", make([]byte, 12)...),
		rawElem(0x08, "bool", 1),
		rawElem(0x09, "date", date...),
		rawElem(0x0A, "null"),
		rawElem(0x0B, "regex", 'a', 0, 'i', 0),
		rawElem(0x0C, "dbpointer", append(rawString("ns"), make([]byte, 12)...)...),
		rawElem(0x0D, "code", rawString("x")...),
		rawElem(0x0E, "symbol", rawString("s")...),
		rawElem(0x0F, "cws", scope...),
		rawInt32("int32", 1),
		rawElem(0x11, "timestamp", make([]byte, 8)...),
		rawElem(0x12, "int64", make([]byte, 8)...),
		rawElem(0x13, "decimal", make([]byte, 16)...),
		rawElem(0x7F, "maxkey"),
		rawElem(0xFF, "minkey"),
	)
}

// storedDocument is a document as a store holds it, with a note on what it
// probes.
type storedDocument struct {
	name string
	raw  []byte
}

// handBuiltStoredDocuments are the shapes an index reads differently from a
// whole-document decode if the field reader is wrong: repeated top-level keys,
// the empty name, every type, documents and arrays in an indexed field.
func handBuiltStoredDocuments() []storedDocument {
	return []storedDocument{
		{"empty document", rawDoc()},
		{"repeated key: the last wins", rawDoc(rawInt32("a", 1), rawInt32("b", 5), rawInt32("a", 2), rawInt32("a", 3))},
		{"repeated key of two types", rawDoc(rawElem(0x02, "a", rawString("x")...), rawInt32("a", 9))},
		{"repeated key, the last null", rawDoc(rawInt32("a", 1), rawElem(0x0A, "a"))},
		{"empty name", rawDoc(rawInt32("", 4), rawElem(0x02, "b", rawString("y")...))},
		{"every type", rawEveryType()},
		{"document in an indexed field", rawDoc(rawElem(0x03, "a", rawDoc(rawInt32("x", 1))...), rawInt32("b", 2))},
		{"array in an indexed field", rawDoc(rawElem(0x04, "a", rawDoc(rawInt32("0", 1))...), rawInt32("b", 2))},
		{"names that look like paths", rawDoc(rawInt32("a.b", 1), rawInt32("0", 2), rawElem(0x02, "k", rawString("z")...))},
	}
}

// mapStoredDocuments are documents as the server writes them, from maps:
// every Go type the decode produces in an indexed field, and the values an
// index refuses.
func mapStoredDocuments(t *testing.T) []storedDocument {
	t.Helper()
	stamp := time.Date(2026, 9, 24, 18, 30, 15, 123_000_000, time.UTC)
	shapes := []map[string]interface{}{
		{"a": "alpha", "b": int32(1), "name": "x", "when": primitive.NewDateTimeFromTime(stamp)},
		{"a": "alpha", "b": int64(1), "name": nil, "when": stamp.Add(-time.Hour)},
		{"a": 1.0, "b": int64(math.MaxInt64), "name": true, "when": int64(5)},
		{"a": math.NaN(), "b": math.Inf(1), "name": "y"},
		{"a": primitive.Binary{Subtype: 0x00, Data: []byte{0, 1}}, "b": primitive.Null{}, "name": false},
		{"a": primitive.NewObjectIDFromTimestamp(stamp), "b": primitive.NewDecimal128(1, 2), "name": "z"},
		{"a": map[string]interface{}{"x": int32(1)}, "b": []interface{}{"p", int32(2)}, "name": "w"},
		{"b": "only b", "name": "absent a"},
		{"a": "", "b": int32(-0), "name": strings.Repeat("n", 300), "when": 1.5},
		{"a": primitive.Timestamp{T: 5, I: 6}, "b": primitive.Undefined{}, "name": primitive.Symbol("s")},
	}
	documents := make([]storedDocument, 0, len(shapes))
	for position, shape := range shapes {
		encoded, err := bson.Marshal(shape)
		if err != nil {
			t.Fatalf("encoding map document %d: %v", position, err)
		}
		documents = append(documents, storedDocument{fmt.Sprintf("map document %d", position), encoded})
	}
	return documents
}

var storedFieldNames = []string{"a", "b", "c", "0", "1", "", "name", "a.b", "when"}

// generatedStoredDocuments are count documents from seed with every BSON type
// at the top level and below it, repeated keys among them.
func generatedStoredDocuments(seed int64, count int) []storedDocument {
	random := rand.New(rand.NewSource(seed))
	documents := make([]storedDocument, 0, count)
	for number := 0; number < count; number++ {
		documents = append(documents, storedDocument{fmt.Sprintf("generated %d/%d", seed, number), generatedStoredContainer(random, 1, false)})
	}
	return documents
}

func generatedStoredContainer(random *rand.Rand, depth int, isArray bool) []byte {
	count := random.Intn(7)
	elements := make([][]byte, 0, count)
	for position := 0; position < count; position++ {
		key := storedFieldNames[random.Intn(len(storedFieldNames))]
		if isArray {
			key = strconv.Itoa(position)
		}
		elements = append(elements, generatedStoredElement(random, depth, key))
	}
	return rawDoc(elements...)
}

func generatedStoredElement(random *rand.Rand, depth int, key string) []byte {
	kind := random.Intn(14)
	if depth >= 3 && (kind == 8 || kind == 9) {
		kind = 0
	}
	eight := make([]byte, 8)
	random.Read(eight)
	switch kind {
	case 0:
		return rawInt32(key, random.Intn(5)-2)
	case 1:
		return rawElem(0x12, key, byte(random.Intn(3)), 0, 0, 0, 0, 0, 0, 0)
	case 2:
		double := make([]byte, 8)
		binary.LittleEndian.PutUint64(double, math.Float64bits(float64(random.Intn(5))/2))
		return rawElem(0x01, key, double...)
	case 3:
		return rawElem(0x02, key, rawString(strings.Repeat("s", random.Intn(3)))...)
	case 4:
		return rawElem(0x08, key, byte(random.Intn(2)))
	case 5:
		return rawElem(0x0A, key)
	case 6:
		return rawElem(0x09, key, eight...)
	case 7:
		return rawElem(0x05, key, append(append(rawLE32(2), byte(random.Intn(3))), 1, 2)...)
	case 8:
		return rawElem(0x03, key, generatedStoredContainer(random, depth+1, false)...)
	case 9:
		return rawElem(0x04, key, generatedStoredContainer(random, depth+1, true)...)
	case 10:
		return rawElem(0x07, key, append(eight, 1, 2, 3, 4)...)
	case 11:
		return rawElem(0x13, key, append(eight, eight...)...)
	case 12:
		return rawElem(0x0B, key, 'a', 0, 'i', 0)
	}
	return rawElem(0x7F, key)
}

// corpusStoredDocuments is the v1.0 corpus, every stride-th record.
func corpusStoredDocuments(t *testing.T, stride int) []storedDocument {
	t.Helper()
	records, err := bsonoracle.V1Corpus()
	if err != nil {
		t.Fatalf("loading the v1.0 corpus: %v", err)
	}
	var documents []storedDocument
	for position := 0; position < len(records); position += stride {
		documents = append(documents, storedDocument{fmt.Sprintf("v1 record %d (%s)", position, records[position].Class), records[position].Value})
	}
	return documents
}

// differentialStoredDocuments are the documents the map path decodes: the
// others have no map to compare with, and the corrupt-document tests below
// take them.
func differentialStoredDocuments(t *testing.T) []storedDocument {
	t.Helper()
	documents := append(handBuiltStoredDocuments(), mapStoredDocuments(t)...)
	documents = append(documents, generatedStoredDocuments(21, 400)...)
	stride := 5
	if raceEnabled {
		stride = 100
	}
	documents = append(documents, corpusStoredDocuments(t, stride)...)
	decodable := documents[:0]
	for _, document := range documents {
		var decoded map[string]interface{}
		if bson.Unmarshal(document.raw, &decoded) == nil {
			decodable = append(decodable, document)
		}
	}
	return decodable
}

// differentialSpecs are the indexes a document is put through: one per
// top-level name it holds (up to four) and one on a name it lacks, a
// compound of its first two names, a unique index on its first, and an index
// declared before this build (version 0, no entry for an absent field).
func differentialSpecs(raw []byte) []IndexSpec {
	names := topLevelNames(raw)
	if len(names) > 4 {
		names = names[:4]
	}
	names = append(names, "absent")
	specs := make([]IndexSpec, 0, len(names)+3)
	add := func(spec IndexSpec) {
		spec.ID = uint16(len(specs))
		spec.Name = fmt.Sprintf("index%d", spec.ID)
		specs = append(specs, spec)
	}
	for _, name := range names {
		add(IndexSpec{Fields: []IndexKeyPart{{Field: name}}, Version: IndexSpecVersion})
	}
	if len(names) >= 2 {
		add(IndexSpec{Fields: []IndexKeyPart{{Field: names[0]}, {Field: names[1]}}, Version: IndexSpecVersion})
	}
	add(IndexSpec{Fields: []IndexKeyPart{{Field: names[0]}}, Unique: true, Version: IndexSpecVersion})
	add(IndexSpec{Fields: []IndexKeyPart{{Field: names[0]}}})
	return specs
}

// topLevelNames are the distinct top-level names of a decodable document, in
// order.
func topLevelNames(raw []byte) []string {
	var names []string
	seen := map[string]bool{}
	elements, err := bson.Raw(raw).Elements()
	if err != nil {
		return nil
	}
	for _, element := range elements {
		if key := element.Key(); !seen[key] {
			seen[key] = true
			names = append(names, key)
		}
	}
	return names
}

// --- the oracle: the map path, as it was ---

// oracleIndexDocument is HashStore.indexDocument as it was.
func oracleIndexDocument(h *HashStore, snap storage.KVSnapshot, claims *UniqueClaims, b storage.KVBatch, key string, previous []byte, stored bool, encoded []byte) error {
	var document map[string]interface{}
	if err := bson.Unmarshal(encoded, &document); err != nil {
		return fmt.Errorf("decoding key %s to index it: %w", key, err)
	}
	var previousDoc map[string]interface{}
	if stored {
		if err := bson.Unmarshal(previous, &previousDoc); err != nil {
			return fmt.Errorf("decoding the stored key %s to reindex it: %w", key, err)
		}
	}
	if err := h.idxMgr.CheckUnique(snap, claims, []byte(key), previousDoc, document); err != nil {
		return fmt.Errorf("indexing key %s: %w", key, err)
	}
	if !stored {
		if err := h.idxMgr.OnInsert(b, []byte(key), document); err != nil {
			return fmt.Errorf("indexing key %s: %w", key, err)
		}
		return nil
	}
	if err := h.idxMgr.OnUpdate(b, []byte(key), previousDoc, document); err != nil {
		return fmt.Errorf("reindexing key %s: %w", key, err)
	}
	return nil
}

// oracleRemoveStored is HashStore.removeStored's index half as it was.
func oracleRemoveStored(h *HashStore, b storage.KVBatch, key string, previous []byte) error {
	var previousDoc map[string]interface{}
	if err := bson.Unmarshal(previous, &previousDoc); err != nil {
		return fmt.Errorf("decoding deleted key %s to unindex it: %w", key, err)
	}
	if err := h.idxMgr.OnDelete(b, []byte(key), previousDoc); err != nil {
		return fmt.Errorf("unindexing key %s: %w", key, err)
	}
	return nil
}

// oracleExpired is deleteCascading's expiry check as it was.
func oracleExpired(previous []byte, condition ExpiryCondition) (bool, error) {
	var document map[string]interface{}
	if err := bson.Unmarshal(previous, &document); err != nil {
		return false, fmt.Errorf("decoding to check its expiry: %w", err)
	}
	return condition.Expired(document[condition.Field]), nil
}

// --- the twin stores ---

// twinStore is one side of the differential: a hash store with indexes on a
// store of its own.
type twinStore struct {
	kv storage.KV
	h  *HashStore
}

func newTwinStore(t *testing.T, specs []IndexSpec) twinStore {
	t.Helper()
	kv := kvfault.New()
	im, err := NewIndexManager(kv, 1, 1, 1, specs)
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}
	h := NewHashStore(kv, 1, 1, 1, new(OwedApplies))
	h.SetIndexManager(im)
	return twinStore{kv: kv, h: h}
}

// write runs one index write against a snapshot and commits its batch when it
// succeeds, as Apply does.
func (twin twinStore) write(t *testing.T, op func(storage.KVSnapshot, storage.KVBatch) error) error {
	t.Helper()
	snap := twin.kv.NewSnapshot()
	defer snap.Discard()
	batch := twin.kv.NewBatch()
	defer batch.Discard()
	if err := op(snap, batch); err != nil {
		return err
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing: %v", err)
	}
	return nil
}

// contents is every key and value in the store, in order.
func (twin twinStore) contents(t *testing.T) string {
	t.Helper()
	snap := twin.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	var out strings.Builder
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			t.Fatalf("reading a value: %v", err)
		}
		fmt.Fprintf(&out, "%x=%x\n", it.Key(), value)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("walking the store: %v", err)
	}
	return out.String()
}

func errorText(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// hashSteps are the index writes a document pair goes through: an insert, an
// overwrite, a second key that may collide on the unique index, and the
// removal of the first key.
type hashStep struct {
	name     string
	key      string
	previous []byte
	stored   bool
	encoded  []byte
	remove   bool
}

func hashSteps(first, second []byte) []hashStep {
	return []hashStep{
		{name: "insert", key: "k1", encoded: first},
		{name: "overwrite", key: "k1", previous: first, stored: true, encoded: second},
		{name: "second key", key: "k2", encoded: first},
		{name: "remove", key: "k1", previous: second, remove: true},
	}
}

// TestHashStoreMatchesTheMapPath runs each document, paired with the next,
// through the hash store's index writes on twin stores, the oracle's map path
// on one and the store's own on the other, and compares every error and the
// two stores' whole contents after every step. It then checks the expiry of
// every field of the document both ways.
func TestHashStoreMatchesTheMapPath(t *testing.T) {
	documents := differentialStoredDocuments(t)
	steps, refusals := 0, 0
	for position, document := range documents {
		next := documents[(position+1)%len(documents)]
		specs := differentialSpecs(document.raw)
		oracle, server := newTwinStore(t, specs), newTwinStore(t, specs)
		for _, step := range hashSteps(document.raw, next.raw) {
			steps++
			var oracleErr, serverErr error
			if step.remove {
				oracleErr = oracle.write(t, func(_ storage.KVSnapshot, b storage.KVBatch) error {
					return oracleRemoveStored(oracle.h, b, step.key, step.previous)
				})
				serverErr = server.write(t, func(_ storage.KVSnapshot, b storage.KVBatch) error {
					return server.h.removeStored(&BucketState{Entries: 1}, b, step.key, step.previous)
				})
				// removeStored also deletes the document key; the oracle did not
				// write one, so take it out of the comparison by deleting it on
				// the oracle's side too.
				if oracleErr == nil {
					if err := oracle.write(t, func(_ storage.KVSnapshot, b storage.KVBatch) error {
						return b.Delete(oracle.h.documentKey(step.key))
					}); err != nil {
						t.Fatalf("deleting the oracle's document key: %v", err)
					}
				}
			} else {
				oracleErr = oracle.write(t, func(snap storage.KVSnapshot, b storage.KVBatch) error {
					return oracleIndexDocument(oracle.h, snap, nil, b, step.key, step.previous, step.stored, step.encoded)
				})
				serverErr = server.write(t, func(snap storage.KVSnapshot, b storage.KVBatch) error {
					return server.h.indexDocument(snap, nil, b, step.key, step.previous, step.stored, step.encoded)
				})
			}
			if errorText(oracleErr) != errorText(serverErr) {
				t.Fatalf("%s, then %s, step %s: the map path says %v, the store says %v", document.name, next.name, step.name, oracleErr, serverErr)
			}
			if oracleErr != nil {
				refusals++
			}
			if oracleContents, serverContents := oracle.contents(t), server.contents(t); oracleContents != serverContents {
				t.Fatalf("%s, then %s, step %s: the stores differ\nmap path:\n%s\nstore:\n%s", document.name, next.name, step.name, oracleContents, serverContents)
			}
		}
		compareExpiry(t, document)
	}
	t.Logf("%d documents, %d index steps compared, %d refused alike", len(documents), steps, refusals)
}

// compareExpiry checks, for every top-level name of a document and one it
// lacks, that DeleteIfExpired spares or deletes it as the map path's check
// says, under bounds that expire every date and number and under bounds that
// expire none.
func compareExpiry(t *testing.T, document storedDocument) {
	t.Helper()
	for _, field := range append(topLevelNames(document.raw), "absent") {
		for _, condition := range []ExpiryCondition{
			{Field: field, DateBefore: math.MaxInt64, SecondsBefore: math.MaxInt64},
			{Field: field, DateBefore: math.MinInt64, SecondsBefore: math.MinInt64},
		} {
			want, err := oracleExpired(document.raw, condition)
			if err != nil {
				t.Fatalf("%s: the map path cannot read a decodable document: %v", document.name, err)
			}
			kv := kvfault.New()
			h := NewHashStore(kv, 1, 1, 1, new(OwedApplies))
			if err := h.Put(ApplyInfo{}, "k", bson.Raw(document.raw), 0); err != nil {
				t.Fatalf("%s: storing it: %v", document.name, err)
			}
			spared, _, err := h.DeleteIfExpired(ApplyInfo{}, "k", condition, nil)
			if err != nil {
				t.Fatalf("%s, field %q: DeleteIfExpired: %v", document.name, field, err)
			}
			if spared == want {
				t.Fatalf("%s, field %q, bounds %d/%d: the map path says expired %v, the store spared %v", document.name, field,
					condition.DateBefore, condition.SecondsBefore, want, spared)
			}
		}
	}
}

// --- corrupt documents ---

// corruptNested is a document whose indexed fields are well formed and whose
// field "rest" holds an embedded document with a byte that is no BSON type:
// framed, and readable at the top level, but not decodable as a whole.
func corruptNested(indexed string) []byte {
	return rawDoc(rawElem(0x02, "a", rawString(indexed)...),
		rawElem(0x03, "rest", rawDoc(rawElem(0x20, "x", 1, 2, 3, 4))...),
		rawInt32("b", 7))
}

// putStoredBytes writes raw as key's stored document directly, as a document
// stored before a value check existed would sit in the store.
func putStoredBytes(t *testing.T, kv storage.KV, h *HashStore, key string, raw []byte) {
	t.Helper()
	if err := inBatch(kv, func(b storage.KVBatch) error { return b.Set(h.documentKey(key), raw) }); err != nil {
		t.Fatalf("writing the stored bytes of %s: %v", key, err)
	}
}

// indexEntries are the document keys of index spec under each encoded value,
// rendered "value=document".
func indexEntries(t *testing.T, kv storage.KV, spec IndexSpec) []string {
	t.Helper()
	index := NewSecondaryIndex(kv, 1, 1, 1, spec.ID)
	bounds := index.EntryRange()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(bounds)
	defer it.Close()
	var entries []string
	for it.Seek(nil); it.Valid(); it.Next() {
		entries = append(entries, fmt.Sprintf("%x", it.Key()[len(bounds.Lower):]))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("walking index %s: %v", spec.Name, err)
	}
	sort.Strings(entries)
	return entries
}

func wantEntries(t *testing.T, spec IndexSpec, documents map[string]map[string]interface{}) []string {
	t.Helper()
	var entries []string
	for key, document := range documents {
		encoded, indexed, err := EntryKey(spec, document)
		if err != nil {
			t.Fatalf("EntryKey of %s: %v", key, err)
		}
		if indexed {
			entries = append(entries, fmt.Sprintf("%x", append(encoded, key...)))
		}
	}
	sort.Strings(entries)
	return entries
}

// TestHashStoreReadsTheIndexedFieldsOfACorruptDocument: a stored document
// framed correctly whose indexed fields are readable, but whose other bytes
// are not BSON, is overwritten, deleted and checked for expiry with its index
// entries kept exact. The map path refused every one of those writes, because
// it decoded the whole document; the store now reads only the fields its
// indexes name, every top-level element still walked.
func TestHashStoreReadsTheIndexedFieldsOfACorruptDocument(t *testing.T) {
	spec := IndexSpec{ID: 0, Name: "a_1", Fields: []IndexKeyPart{{Field: "a"}}, Unique: true, Version: IndexSpecVersion}
	kv := kvfault.New()
	im, err := NewIndexManager(kv, 1, 1, 1, []IndexSpec{spec})
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}
	h := NewHashStore(kv, 1, 1, 1, new(OwedApplies))
	h.SetIndexManager(im)
	if err := h.Put(ApplyInfo{}, "k1", map[string]interface{}{"a": "old", "b": int32(7)}, 0); err != nil {
		t.Fatalf("storing k1: %v", err)
	}
	putStoredBytes(t, kv, h, "k1", corruptNested("old"))

	if err := h.Put(ApplyInfo{}, "k1", map[string]interface{}{"a": "new"}, 0); err != nil {
		t.Fatalf("overwriting a stored document whose field rest is corrupt: %v", err)
	}
	if got, want := indexEntries(t, kv, spec), wantEntries(t, spec, map[string]map[string]interface{}{"k1": {"a": "new"}}); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("after the overwrite the index holds %v, want %v", got, want)
	}

	putStoredBytes(t, kv, h, "k1", corruptNested("new"))
	if err := h.Delete(ApplyInfo{}, "k1"); err != nil {
		t.Fatalf("deleting a stored document whose field rest is corrupt: %v", err)
	}
	if got := indexEntries(t, kv, spec); len(got) != 0 {
		t.Fatalf("after the delete the index holds %v, want nothing", got)
	}

	if err := h.Put(ApplyInfo{}, "k2", map[string]interface{}{"a": "dated", "b": int32(7)}, 0); err != nil {
		t.Fatalf("storing k2: %v", err)
	}
	putStoredBytes(t, kv, h, "k2", corruptNested("dated"))
	spared, _, err := h.DeleteIfExpired(ApplyInfo{}, "k2", ExpiryCondition{Field: "b", SecondsBefore: math.MaxInt64}, nil)
	if err != nil || spared {
		t.Fatalf("expiring a stored document whose field rest is corrupt: spared %v, err %v; want deleted", spared, err)
	}
	if got := indexEntries(t, kv, spec); len(got) != 0 {
		t.Fatalf("after the expiry the index holds %v, want nothing", got)
	}
}

// TestHashStoreRefusesACorruptIndexedField: a stored document whose top level
// cannot be walked, or whose indexed field's own bytes do not decode, is
// refused as the map path refused it, and its index entries are left as they
// were: its entries cannot be told, so none is removed.
func TestHashStoreRefusesACorruptIndexedField(t *testing.T) {
	spec := IndexSpec{ID: 0, Name: "a_1", Fields: []IndexKeyPart{{Field: "a"}}, Version: IndexSpecVersion}
	for _, corrupt := range []storedDocument{
		// A string length that runs past the end of the document: the top-level
		// walk stops there.
		{"top level not walkable", rawDoc(rawElem(0x02, "a", append(rawLE32(500), 'x', 0)...), rawInt32("b", 7))},
		// A byte that is no BSON type inside the indexed field's own document.
		{"indexed field not decodable", rawDoc(rawElem(0x03, "a", rawDoc(rawElem(0x20, "x", 1, 2, 3, 4))...), rawInt32("b", 7))},
	} {
		t.Run(corrupt.name, func(t *testing.T) {
			kv := kvfault.New()
			im, err := NewIndexManager(kv, 1, 1, 1, []IndexSpec{spec})
			if err != nil {
				t.Fatalf("NewIndexManager: %v", err)
			}
			h := NewHashStore(kv, 1, 1, 1, new(OwedApplies))
			h.SetIndexManager(im)
			if err := h.Put(ApplyInfo{}, "k1", map[string]interface{}{"a": "old", "b": int32(7)}, 0); err != nil {
				t.Fatalf("storing k1: %v", err)
			}
			before := indexEntries(t, kv, spec)
			putStoredBytes(t, kv, h, "k1", corrupt.raw)
			if err := h.Put(ApplyInfo{}, "k1", map[string]interface{}{"a": "new"}, 0); err == nil {
				t.Fatal("overwriting it succeeded; want a refusal, since its index entries cannot be read")
			}
			if err := h.Delete(ApplyInfo{}, "k1"); err == nil {
				t.Fatal("deleting it succeeded; want a refusal, since its index entries cannot be read")
			}
			if after := indexEntries(t, kv, spec); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("the refusals changed the index from %v to %v", before, after)
			}
		})
	}
}

// TestHashStoreFieldReadsOfMutatedDocuments: over framed documents with one
// byte changed, wherever the map path decodes the document, the store's
// writes agree with it; where the map path refuses, the store either refuses
// too or reads the indexed fields from a top level it walked whole. Both
// counts are logged.
func TestHashStoreFieldReadsOfMutatedDocuments(t *testing.T) {
	random := rand.New(rand.NewSource(23))
	sources := append(handBuiltStoredDocuments(), generatedStoredDocuments(24, 60)...)
	// Four indexed names first, then a large field no index names, where most
	// changed bytes land.
	sources = append(sources, storedDocument{"unindexed every-type field", rawDoc(rawElem(0x02, "a", rawString("x")...),
		rawInt32("b", 1), rawInt32("c", 2), rawElem(0x02, "name", rawString("n")...), rawElem(0x03, "rest", rawEveryType()...))})
	agreed, refusedBoth, readPastCorruption := 0, 0, 0
	for _, source := range sources {
		if len(source.raw) < 8 {
			continue
		}
		for number := 0; number < 40; number++ {
			changed := append([]byte{}, source.raw...)
			changed[4+random.Intn(len(changed)-5)] = []byte{0x00, 0x01, 0x02, 0x20, 0x7f, 0xff}[random.Intn(6)]
			specs := differentialSpecs(source.raw)
			oracle, server := newTwinStore(t, specs), newTwinStore(t, specs)
			fresh, err := bson.Marshal(map[string]interface{}{"a": "fresh"})
			if err != nil {
				t.Fatalf("encoding: %v", err)
			}
			oracleErr := oracle.write(t, func(snap storage.KVSnapshot, b storage.KVBatch) error {
				return oracleIndexDocument(oracle.h, snap, nil, b, "k", changed, true, fresh)
			})
			serverErr := server.write(t, func(snap storage.KVSnapshot, b storage.KVBatch) error {
				return server.h.indexDocument(snap, nil, b, "k", changed, true, fresh)
			})
			switch {
			case oracleErr == nil:
				if serverErr != nil || oracle.contents(t) != server.contents(t) {
					t.Fatalf("%s mutated: the map path decodes it and writes %q; the store says %v and writes %q",
						source.name, oracle.contents(t), serverErr, server.contents(t))
				}
				agreed++
			case serverErr != nil:
				refusedBoth++
			default:
				if !topLevelWalks(changed) {
					t.Fatalf("%s mutated: the store read fields of a document whose top level does not walk", source.name)
				}
				readPastCorruption++
			}
		}
	}
	t.Logf("mutated documents: %d agreed with the map path, %d refused by both, %d read by their indexed fields past corruption elsewhere",
		agreed, refusedBoth, readPastCorruption)
}

// topLevelWalks reports whether the library's cursor reads every top-level
// element of a framed document.
func topLevelWalks(raw []byte) bool {
	document, err := bson.TrustStored(raw)
	if err != nil {
		return false
	}
	cursor := document.Cursor()
	for cursor.Next() {
	}
	return cursor.Err() == nil
}
