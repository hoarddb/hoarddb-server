package storage_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	mathrand "math/rand/v2"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// bloomPair is one stored key and its value, as a walk returns them.
type bloomPair struct {
	key   []byte
	value []byte
}

// bloomStore is a store the prefix bloom tests can flush: closing a Badger store writes its
// memtable out as one Level-0 table, so a reopen is a forced flush.
type bloomStore struct {
	t    *testing.T
	dir  string
	open func(dir string) (*storage.BadgerKV, error)
	kv   *storage.BadgerKV
}

func openBloomStore(t *testing.T, open func(dir string) (*storage.BadgerKV, error)) *bloomStore {
	t.Helper()
	s := &bloomStore{t: t, dir: t.TempDir(), open: open}
	kv, err := open(s.dir)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	s.kv = kv
	t.Cleanup(func() {
		if err := s.kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	return s
}

// openNodeStore opens a store exactly as the node does.
func openNodeStore(dir string) (*storage.BadgerKV, error) {
	return storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
}

// flush closes and reopens the store: one more Level-0 table when anything was written.
func (s *bloomStore) flush() {
	s.t.Helper()
	if err := s.kv.Close(); err != nil {
		s.t.Fatalf("closing the store to flush it: %v", err)
	}
	kv, err := s.open(s.dir)
	if err != nil {
		s.t.Fatalf("reopening the store: %v", err)
	}
	s.kv = kv
}

// apply commits sets (a nil value is a delete) in one batch.
func (s *bloomStore) apply(writes []bloomPair) {
	s.t.Helper()
	batch := s.kv.NewBatch()
	for _, w := range writes {
		var err error
		if w.value == nil {
			err = batch.Delete(w.key)
		} else {
			err = batch.Set(w.key, w.value)
		}
		if err != nil {
			s.t.Fatalf("adding %x to a batch: %v", w.key, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		s.t.Fatalf("committing a batch: %v", err)
	}
}

// walk returns every live key and value inside opts, in the walk's order.
func (s *bloomStore) walk(opts storage.IterOptions) []bloomPair {
	s.t.Helper()
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	it := snapshot.NewIterator(opts)
	defer it.Close()
	var pairs []bloomPair
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			s.t.Fatalf("reading the value of %x: %v", it.Key(), err)
		}
		pairs = append(pairs, bloomPair{key: bytes.Clone(it.Key()), value: value})
	}
	if err := it.Err(); err != nil {
		s.t.Fatalf("walking %x..%x: %v", opts.Lower, opts.Upper, err)
	}
	return pairs
}

func describePairs(pairs []bloomPair) string {
	var out bytes.Buffer
	for _, p := range pairs {
		fmt.Fprintf(&out, " %x=%q", p.key, p.value)
	}
	return out.String()
}

func samePairs(a, b []bloomPair) bool {
	return slices.EqualFunc(a, b, func(x, y bloomPair) bool {
		return bytes.Equal(x.key, y.key) && bytes.Equal(x.value, y.value)
	})
}

// bloomIDs are encoded ids of all seven tags, with the neighbours a prefix rule could
// confuse: "a", "a\x00" and "ab" share bytes and none is a prefix of another once encoded.
func bloomIDs() [][]byte {
	number := func(fill byte) []byte { return append([]byte{storage.TagNumber}, bytes.Repeat([]byte{fill}, 17)...) }
	datetime := func(ms uint64) []byte {
		return binary.BigEndian.AppendUint64([]byte{storage.TagDateTime}, ms)
	}
	objectID := func(fill byte) []byte {
		return append([]byte{storage.TagObjectID}, bytes.Repeat([]byte{fill}, 12)...)
	}
	text := func(tag byte, escaped ...byte) []byte {
		return append(append([]byte{tag}, escaped...), 0x00, 0x00)
	}
	return [][]byte{
		{storage.TagNull},
		{storage.TagBool, 0x00}, {storage.TagBool, 0x01},
		number(0x80), number(0x81), number(0x00),
		datetime(1), datetime(2), datetime(1 << 40),
		objectID(0x01), objectID(0x02), objectID(0xFF),
		text(storage.TagString), text(storage.TagString, 'a'), text(storage.TagString, 'a', 0x00, 0xFF),
		text(storage.TagString, 'a', 'b'), text(storage.TagString, 'a', 'b', 'c'), text(storage.TagString, 'b'),
		text(storage.TagString, 0x00, 0xFF), text(storage.TagString, 0x00, 0xFF, 0x00, 0xFF),
		text(storage.TagString, 'a', 0x00, 0xFF, 'b'), text(storage.TagString, 0xFF, 0xFF),
		text(storage.TagBinary), text(storage.TagBinary, 'a'), text(storage.TagBinary, 0x00, 0xFF), text(storage.TagBinary, 0xFF),
	}
}

func fieldKeyOf(docPrefix []byte, fieldID uint16) []byte {
	return binary.BigEndian.AppendUint16(bytes.Clone(docPrefix), fieldID)
}

// TestPrefixBloomNeverSkipsAPresentDocument is the prefix bloom spec's T1: over 200 seeded
// rounds of inserts, field-only updates, unsets, deletes and re-inserts, every document read
// through the filter equals the same bounds read without it and equals the model, in the
// memtable and after every flush and reopen; and, on a store whose compactions cut their
// output into tables of a few keys, after every compaction and the reopen that follows it. A
// filter that drops one prefix hash makes a present document read as absent or lose a field,
// with no error.
// Against: dropping the prefixHashes append; writing the rule bit without the hashes; building
// the filter from the key hashes only.
func TestPrefixBloomNeverSkipsAPresentDocument(t *testing.T) {
	if testing.Short() {
		t.Skip("reopens a store 200 times")
	}
	t.Run("flushes", func(t *testing.T) {
		documentsNeverSkipped(t, openNodeStore, 0)
	})
	t.Run("splitting compactions", func(t *testing.T) {
		documentsNeverSkipped(t, storage.OpenBadgerKVSplittingTables, 3)
	})
}

// documentsNeverSkipped runs T1's rounds on a store opened by open. With compactEvery above
// zero the store is compacted after every compactEvery-th round, and the run must have cut at
// least one live document across two tables: the second then starts with a field key whose
// header is in the first, which is the case a compaction adds to the flushes'.
func documentsNeverSkipped(t *testing.T, open func(dir string) (*storage.BadgerKV, error), compactEvery int) {
	const rounds = 200
	store := openBloomStore(t, open)
	rng := mathrand.New(mathrand.NewPCG(2026, 1115))
	ids := bloomIDs()

	type document struct {
		prefix []byte
		// keys maps a stored key (the header or a field key) to its value.
		keys map[string][]byte
	}
	var documents []*document
	// live finds a document by its prefix; documentsCut counts the live documents a
	// compaction left with keys in two tables.
	live := map[string]*document{}
	documentsCut := 0
	for _, bucket := range []uint64{9, 10} {
		for _, slot := range []uint16{0, 77} {
			for _, id := range ids {
				doc := &document{prefix: storage.Key(slot, 7, bucket, storage.NSDocuments, id), keys: map[string][]byte{}}
				documents = append(documents, doc)
				live[string(doc.prefix)] = doc
			}
		}
	}

	check := func(phase string) {
		t.Helper()
		for _, doc := range documents {
			filtered := store.walk(storage.DocumentIterOptions(doc.prefix))
			unfiltered := store.walk(storage.IterOptions{Lower: doc.prefix, Upper: storage.PrefixEnd(doc.prefix)})
			var want []bloomPair
			for key, value := range doc.keys {
				want = append(want, bloomPair{key: []byte(key), value: value})
			}
			slices.SortFunc(want, func(a, b bloomPair) int { return bytes.Compare(a.key, b.key) })
			if !samePairs(unfiltered, want) {
				t.Fatalf("%s: document %x read without the filter is%s, the model has%s", phase, doc.prefix, describePairs(unfiltered), describePairs(want))
			}
			if !samePairs(filtered, want) {
				t.Fatalf("%s: document %x read through the filter is%s, want%s", phase, doc.prefix, describePairs(filtered), describePairs(want))
			}
			reversed := store.walk(func() storage.IterOptions {
				opts := storage.DocumentIterOptions(doc.prefix)
				opts.Reverse = true
				return opts
			}())
			slices.Reverse(reversed)
			if !samePairs(reversed, want) {
				t.Fatalf("%s: document %x read in reverse through the filter is%s, want%s", phase, doc.prefix, describePairs(reversed), describePairs(want))
			}
		}
	}

	for round := range rounds {
		var writes []bloomPair
		set := func(doc *document, key []byte) {
			value := fmt.Appendf(nil, "r%d-%d", round, rng.IntN(1000))
			doc.keys[string(key)] = value
			writes = append(writes, bloomPair{key: key, value: value})
		}
		remove := func(doc *document, key []byte) {
			delete(doc.keys, string(key))
			writes = append(writes, bloomPair{key: key})
		}
		for range 1 + rng.IntN(12) {
			doc := documents[rng.IntN(len(documents))]
			_, live := doc.keys[string(doc.prefix)]
			switch op := rng.IntN(10); {
			case !live || op == 0:
				// Insert, or re-insert over whatever a delete left: the header and some fields.
				for key := range doc.keys {
					remove(doc, []byte(key))
				}
				set(doc, doc.prefix)
				for range rng.IntN(4) {
					set(doc, fieldKeyOf(doc.prefix, uint16(2+rng.IntN(6))))
				}
			case op <= 5:
				// A field-only update: the header is not written, so the table this lands in
				// holds a key of the document without its header.
				set(doc, fieldKeyOf(doc.prefix, uint16(2+rng.IntN(6))))
			case op <= 7:
				remove(doc, fieldKeyOf(doc.prefix, uint16(2+rng.IntN(6))))
			default:
				for key := range doc.keys {
					remove(doc, []byte(key))
				}
			}
		}
		store.apply(writes)
		check(fmt.Sprintf("round %d, in the memtable", round))
		store.flush()
		check(fmt.Sprintf("round %d, after the flush and reopen", round))
		if compactEvery == 0 || (round+1)%compactEvery != 0 {
			continue
		}
		if err := store.kv.Compact(); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		spans := store.kv.TableSpans()
		if len(spans) > 0 && spans[0].Level == 0 {
			t.Fatalf("round %d: the compaction left a table in Level 0", round)
		}
		for i := 1; i < len(spans); i++ {
			if doc := live[string(documentCutBetween(spans[i-1], spans[i]))]; doc != nil && len(doc.keys) > 0 {
				documentsCut++
			}
		}
		check(fmt.Sprintf("round %d, after the compaction into %d tables", round, len(spans)))
		store.flush()
		check(fmt.Sprintf("round %d, after the compaction and a reopen", round))
	}

	stats := store.kv.PrefixBloomStats()
	if stats.Document.Iterators == 0 || stats.Document.Candidates == 0 {
		t.Fatalf("no filtered read was counted since the last reopen: %+v", stats)
	}
	if stats.Document.Unmarked != 0 {
		t.Fatalf("%d candidate tables carried no rule bit; every table of this store was built under the rules", stats.Document.Unmarked)
	}
	if compactEvery > 0 && documentsCut == 0 {
		t.Fatalf("no compaction cut a live document across two tables in %d rounds, so the split phase tested nothing; the table size is too large for these documents", rounds)
	}
	t.Logf("since the last reopen: %+v; %d tables; %d live documents cut across tables by compactions", stats, store.kv.TableCount(), documentsCut)
}

// documentCutBetween returns the document prefix that the last key of table before and the
// first key of table after share, when the two tables are neighbours in one level and both
// keys belong to one document; nil otherwise.
func documentCutBetween(before, after storage.TableSpan) []byte {
	if before.Level != after.Level {
		return nil
	}
	n := storage.DocumentPrefixLen(before.Biggest)
	if n == 0 || n != storage.DocumentPrefixLen(after.Smallest) || !bytes.Equal(before.Biggest[:n], after.Smallest[:n]) {
		return nil
	}
	return before.Biggest[:n]
}
