package store

import (
	"bytes"
	"cmp"
	"fmt"
	mathrand "math/rand/v2"
	"slices"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"

	"github.com/hoarddb/hoarddb-server/storage"
)

// bloomModelElement is one element the model expects in a store instance.
type bloomModelElement struct {
	n        int
	priority int64  // heap
	key      string // hash
}

// bloomModelStore is the model of one store instance, in the order FieldList returns it.
type bloomModelStore struct {
	ref      FieldRef
	name     string
	elements []bloomModelElement
}

// listOrder sorts the model the way the engine's keys sort: positions for fifo and lifo
// (already in push order), highest priority first then entry id for heap, element key for hash.
func (m *bloomModelStore) listOrder() {
	switch m.ref.Engine {
	case FieldHeap:
		slices.SortFunc(m.elements, func(a, b bloomModelElement) int {
			return cmp.Or(cmp.Compare(b.priority, a.priority), cmp.Compare(fmt.Sprintf("e%d", a.n), fmt.Sprintf("e%d", b.n)))
		})
	case FieldHash:
		slices.SortFunc(m.elements, func(a, b bloomModelElement) int { return cmp.Compare(a.key, b.key) })
	}
}

// next is the index of the element a pop or peek takes, or -1 when the instance is empty.
func (m *bloomModelStore) next() int {
	if len(m.elements) == 0 {
		return -1
	}
	if m.ref.Engine == FieldLIFO {
		return len(m.elements) - 1
	}
	return 0
}

func (m *bloomModelStore) numbers() []int {
	numbers := make([]int, 0, len(m.elements))
	for _, element := range m.elements {
		numbers = append(numbers, element.n)
	}
	return numbers
}

// unfilteredNumbers walks the instance's element range with bounds built by hand, which carry
// no filter, and returns each element's number in key order.
func (p *fieldParent) unfilteredNumbers(ref FieldRef) []int {
	p.t.Helper()
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	prefix, inner := ref.StorePrefix(), ref.elementInner()
	it := snap.NewIterator(storage.IterOptions{Lower: FieldElementKey(prefix, inner, nil), Upper: FieldElementKey(prefix, inner+1, nil)})
	defer it.Close()
	numbers := []int{}
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			p.t.Fatalf("reading an element without the filter: %v", err)
		}
		numbers = append(numbers, entryNumber(p.t, value))
	}
	if err := it.Err(); err != nil {
		p.t.Fatalf("walking the elements without the filter: %v", err)
	}
	return numbers
}

// peekNumber is the number of the element a peek returns, or -1 for an empty instance.
func (p *fieldParent) peekNumber(ref FieldRef) int {
	p.t.Helper()
	snap := p.kv.NewSnapshot()
	defer snap.Discard()
	entry, found, err := FieldPeek(snap, ref)
	if err != nil {
		p.t.Fatalf("FieldPeek: %v", err)
	}
	if !found {
		return -1
	}
	return entryNumber(p.t, entry.Value)
}

// TestPrefixBloomNeverSkipsAPresentElement is the prefix bloom spec's T12: T1's property for
// store fields. Over 200 seeded rounds of pushes, pops, puts and deletes on fifo, lifo, heap
// and hash fields of many documents, and on a store declared on an element of a store, every
// list, peek and pop (which read through the filter: their bounds come from
// FieldRef.elementBounds) equals a walk of the same range without the filter and equals the
// model, in the memtable and after every flush and reopen; and, in the second run, after
// compactions that cut their output into tables of a few keys, so one store's elements sit in
// several tables of one level.
// Against: dropping the rule 2 append.
// What it cannot see: a table whose only key under a store prefix is a delete marker. Every
// pop and delete here also writes the store's state key under the same prefix in the same
// batch, so such a table always gets the prefix hash from a live key, and this test passes
// when delete markers add none. storage's TestDeleteMarkerOnlyTableIsNotSkipped builds that
// table key by key and covers it. Nor can it see a store prefix hashed without the field id:
// the builder and the reader take the length from one function and still agree; storage's
// TestPrefixLenFunctions (T8) pins the length.
func TestPrefixBloomNeverSkipsAPresentElement(t *testing.T) {
	if testing.Short() {
		t.Skip("reopens a store 200 times")
	}
	t.Run("flushes", func(t *testing.T) {
		elementsNeverSkipped(t, 0)
	})
	t.Run("splitting compactions", func(t *testing.T) {
		elementsNeverSkipped(t, 3)
	})
}

// nodeBadgerOptions returns the Badger options the node opens its store with, read from a
// store opened on a scratch directory: the one list of prefix rules, set in one place.
func nodeBadgerOptions(t *testing.T) badger.Options {
	t.Helper()
	instance, err := storage.OpenBadger(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening a scratch store for its options: %v", err)
	}
	nodeOptions := instance.DB.Opts()
	if err := instance.DB.Close(); err != nil {
		t.Fatalf("closing the scratch store: %v", err)
	}
	return nodeOptions
}

// compactIntoSmallTables compacts the closed store at dir with the node's options, except
// that a table below Level 0 holds about 2 KiB in 512-byte blocks, so the compaction's output
// is many tables and the cuts fall inside store fields. Compression is off because a builder
// with compression learns its size from goroutines, late, and the cuts would depend on timing.
// It returns how many cuts fell between two keys of one store field's one inner namespace
// (two elements of one instance, for example): the table after such a cut holds elements of a
// store and not the keys before them. Badger leaves a store alone whose tables all sit in
// Level 0 and number no more than the Level-0 cap, so a call can return 0 having done nothing.
func compactIntoSmallTables(t *testing.T, dir string, nodeOptions badger.Options) int {
	t.Helper()
	opts := nodeOptions
	opts.Dir, opts.ValueDir = dir, dir
	opts.NumCompactors = 0
	opts.BaseTableSize = 2 << 10
	opts.TableSizeMultiplier = 1
	opts.BlockSize = 512
	opts.Compression = options.None
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("opening the store to compact it: %v", err)
	}
	if err := db.Flatten(1); err != nil {
		t.Fatalf("compacting the store: %v", err)
	}
	tables := db.Tables()
	if err := db.Close(); err != nil {
		t.Fatalf("closing the store after the compaction: %v", err)
	}
	slices.SortFunc(tables, func(a, b badger.TableInfo) int {
		return cmp.Or(cmp.Compare(a.Level, b.Level), bytes.Compare(a.Left, b.Left))
	})
	cuts := 0
	for i := 1; i < len(tables); i++ {
		last, first := y.ParseKey(tables[i-1].Right), y.ParseKey(tables[i].Left)
		n := storage.StorePrefixLen(last)
		if tables[i-1].Level != 0 && tables[i-1].Level == tables[i].Level && n > 0 && len(last) > n && len(first) > n && bytes.Equal(last[:n+1], first[:n+1]) {
			cuts++
		}
	}
	return cuts
}

// elementsNeverSkipped runs T12's rounds. With compactEvery above zero the store is closed
// and compacted into small tables after every compactEvery-th round, and the run must have
// cut at least one store field across two tables.
func elementsNeverSkipped(t *testing.T, compactEvery int) {
	const rounds = 200
	p := newFieldParent(t)
	rng := mathrand.New(mathrand.NewPCG(2026, 1116))
	var nodeOptions badger.Options
	if compactEvery > 0 {
		nodeOptions = nodeBadgerOptions(t)
	}
	storesCut := 0

	var stores []*bloomModelStore
	for _, document := range []string{"a", "a\x00", "ab", "abc", "b", "", "\x00", "a\x00b", "user:1", "user:10", "user:2", "\xff"} {
		for fieldID, engine := range map[uint16]FieldEngine{2: FieldFIFO, 3: FieldLIFO, 4: FieldHeap, 5: FieldHash} {
			stores = append(stores, &bloomModelStore{ref: p.ref(document, fieldID, engine), name: fmt.Sprintf("%q field %d (%s)", document, fieldID, engine)})
		}
		// A fifo declared on one element of the document's hash field 6. The element is put
		// once and never deleted, so the nested store lives for the whole test; its keys sit
		// under the outer store's prefix.
		outer := p.ref(document, 6, FieldHash)
		p.put(outer, "anchor", 0)
		anchor, err := KeyedRemainder("anchor")
		if err != nil {
			t.Fatalf("encoding the anchor element: %v", err)
		}
		nested := FieldRef{Owner: FieldElementAnchor(outer.StorePrefix(), anchor), FieldID: 7, Engine: FieldFIFO}
		stores = append(stores, &bloomModelStore{ref: nested, name: fmt.Sprintf("%q field 6 element anchor field 7 (nested fifo)", document)})
	}
	// Map order above is random; the draw below must not be, so the seed reproduces a failure.
	slices.SortFunc(stores, func(a, b *bloomModelStore) int { return cmp.Compare(a.name, b.name) })

	check := func(phase string) {
		t.Helper()
		for _, m := range stores {
			want := m.numbers()
			unfiltered := p.unfilteredNumbers(m.ref)
			if !slices.Equal(unfiltered, want) {
				t.Fatalf("%s: %s walked without the filter holds %v, the model %v", phase, m.name, unfiltered, want)
			}
			listed, hasMore := p.list(m.ref, FieldListRequest{Limit: 1 << 20, MaxBytes: 1 << 30})
			filtered := make([]int, 0, len(listed))
			for _, entry := range listed {
				filtered = append(filtered, entryNumber(t, entry.Value))
			}
			if hasMore || !slices.Equal(filtered, want) {
				t.Fatalf("%s: %s listed through the filter is %v (more: %v), want %v", phase, m.name, filtered, hasMore, want)
			}
			if !m.ref.Engine.ordered() {
				continue
			}
			wantPeek := -1
			if i := m.next(); i >= 0 {
				wantPeek = m.elements[i].n
			}
			if got := p.peekNumber(m.ref); got != wantPeek {
				t.Fatalf("%s: a peek of %s returned %d, want %d", phase, m.name, got, wantPeek)
			}
		}
	}

	serial := 0
	for round := range rounds {
		for range 1 + rng.IntN(10) {
			m := stores[rng.IntN(len(stores))]
			serial++
			switch op := rng.IntN(10); {
			case m.ref.Engine == FieldHash && op < 7:
				key := fmt.Sprintf("k%d", rng.IntN(6))
				p.put(m.ref, key, serial)
				m.elements = slices.DeleteFunc(m.elements, func(e bloomModelElement) bool { return e.key == key })
				m.elements = append(m.elements, bloomModelElement{n: serial, key: key})
			case m.ref.Engine == FieldHash:
				key := fmt.Sprintf("k%d", rng.IntN(6))
				_, wasThere := slices.BinarySearchFunc(m.elements, key, func(e bloomModelElement, k string) int { return cmp.Compare(e.key, k) })
				var removed bool
				if _, err := p.apply(ApplyInfo{}, true, func(w FieldWrite) error {
					var err error
					removed, err = FieldDelete(w, m.ref, key)
					return err
				}); err != nil {
					t.Fatalf("round %d: deleting %q from %s: %v", round, key, m.name, err)
				}
				if removed != wasThere {
					t.Fatalf("round %d: deleting %q from %s reported %v, the model says %v", round, key, m.name, removed, wasThere)
				}
				m.elements = slices.DeleteFunc(m.elements, func(e bloomModelElement) bool { return e.key == key })
			case op < 6 && m.ref.Engine == FieldHeap:
				priority := int64(rng.IntN(5))
				p.heapPush(m.ref, priority, serial)
				m.elements = append(m.elements, bloomModelElement{n: serial, priority: priority})
			case op < 6:
				p.push(m.ref, serial)
				m.elements = append(m.elements, bloomModelElement{n: serial})
			default:
				want := -1
				if i := m.next(); i >= 0 {
					want = m.elements[i].n
					m.elements = slices.Delete(m.elements, i, i+1)
				}
				if got := p.pop(m.ref); got != want {
					t.Fatalf("round %d: a pop of %s returned %d, want %d", round, m.name, got, want)
				}
			}
			m.listOrder()
		}
		check(fmt.Sprintf("round %d, in the memtable", round))
		p.reopen()
		check(fmt.Sprintf("round %d, after the flush and reopen", round))
		if compactEvery == 0 || (round+1)%compactEvery != 0 {
			continue
		}
		if err := p.kv.Close(); err != nil {
			t.Fatalf("round %d: closing the store before the compaction: %v", round, err)
		}
		storesCut += compactIntoSmallTables(t, p.dir, nodeOptions)
		reopened := openFieldParent(t, p.dir)
		p.kv, p.hash = reopened.kv, reopened.hash
		check(fmt.Sprintf("round %d, after the splitting compaction", round))
	}
	if compactEvery > 0 && storesCut == 0 {
		t.Fatalf("no compaction cut a store field across two tables in %d rounds, so the split phase tested nothing; the table size is too large for these stores", rounds)
	}

	stats := p.kv.(*storage.BadgerKV).PrefixBloomStats()
	if stats.Store.Iterators == 0 || stats.Store.Candidates == 0 {
		t.Fatalf("no filtered store read was counted since the last reopen: %+v", stats)
	}
	if stats.Store.Unmarked != 0 || stats.Refused != 0 {
		t.Fatalf("unmarked tables or refused filters were counted: %+v", stats)
	}
	t.Logf("since the last reopen: %+v; %d cuts inside a store field by compactions", stats, storesCut)
}
