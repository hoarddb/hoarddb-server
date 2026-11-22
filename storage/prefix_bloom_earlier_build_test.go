package storage_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// tablesBeforePrefixBloom is a Badger directory's MANIFEST and two Level-0 tables written by
// the last build whose table builder knew no prefix rules
// (testdata/tables-before-prefix-bloom.generator.txt says how and what they hold).
const tablesBeforePrefixBloom = "testdata/tables-before-prefix-bloom"

// TestTablesOfAnEarlierBuildAreNeverSkipped reads tables that an earlier build wrote, where
// T4 (TestUnmarkedTableIsNeverSkipped) builds its unmarked tables with today's builder and no
// rules. Their index has no bloom_prefix_rules field at all, so this is the test that the
// field reads as "no rule" from bytes this code did not write: every candidate table is
// counted as unmarked and none is skipped, for a prefix the tables hold and for one they do
// not, and the second table's field and element, which sit there without the document's
// header, are read.
// Against: reading a rule mark out of an index that has none; skipping a table with no mark.
func TestTablesOfAnEarlierBuildAreNeverSkipped(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"MANIFEST", "000001.sst", "000002.sst"} {
		content, err := os.ReadFile(filepath.Join(tablesBeforePrefixBloom, name))
		if err != nil {
			t.Fatalf("reading the fixture: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			t.Fatalf("copying the fixture: %v", err)
		}
	}
	kv, err := storage.OpenBadgerKVNoCompaction(dir)
	if err != nil {
		t.Fatalf("opening the earlier build's tables: %v", err)
	}
	store := &bloomStore{t: t, dir: dir, open: storage.OpenBadgerKVNoCompaction, kv: kv}
	t.Cleanup(func() {
		if err := store.kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	if tables := store.kv.TableCount(); tables != 2 {
		t.Fatalf("the store holds %d tables, want the fixture's two", tables)
	}

	id := func(letter byte) []byte { return []byte{storage.TagString, letter, 0x00, 0x00} }
	document := func(letter byte) []byte { return storage.Key(3, 7, 9, storage.NSDocuments, id(letter)) }
	elements := func(letter byte) []byte {
		return storage.Key(3, 7, 9, storage.NSFieldStore, id(letter), []byte{0x00, 0x04, storage.NSQueue})
	}
	element := func(letter byte, position uint64) []byte {
		return binary.BigEndian.AppendUint64(elements(letter), position)
	}
	listOf := func(letter byte) storage.IterOptions {
		return storage.StoreIterOptions(elements(letter), storage.PrefixEnd(elements(letter)))
	}
	documentRule := func(stats storage.PrefixBloomStats) storage.PrefixBloomRuleStats { return stats.Document }
	storeRule := func(stats storage.PrefixBloomStats) storage.PrefixBloomRuleStats { return stats.Store }

	for _, read := range []struct {
		what          string
		opts          storage.IterOptions
		want          []bloomPair
		rule          func(storage.PrefixBloomStats) storage.PrefixBloomRuleStats
		minCandidates uint64
	}{
		{
			"the document whose newer field sits in a table without its header", storage.DocumentIterOptions(document('m')),
			[]bloomPair{{document('m'), []byte("header")}, {fieldKeyOf(document('m'), 2), []byte("new")}}, documentRule, 2,
		},
		{
			"the store with an element in each table", listOf('m'),
			[]bloomPair{{element('m', 0), []byte("first")}, {element('m', 1), []byte("second")}}, storeRule, 2,
		},
		{"a document no table holds", storage.DocumentIterOptions(document('q')), nil, documentRule, 1},
		{"a store no table holds", listOf('q'), nil, storeRule, 1},
	} {
		before := store.kv.PrefixBloomStats()
		got := store.walk(read.opts)
		after := store.kv.PrefixBloomStats()
		candidates := read.rule(after).Candidates - read.rule(before).Candidates
		skipped := read.rule(after).Skipped - read.rule(before).Skipped
		unmarked := read.rule(after).Unmarked - read.rule(before).Unmarked
		if !samePairs(got, read.want) {
			t.Fatalf("the read of %s returned%s, want%s (candidates %d, skipped %d, unmarked %d)",
				read.what, describePairs(got), describePairs(read.want), candidates, skipped, unmarked)
		}
		if d := read.rule(after).Iterators - read.rule(before).Iterators; d != 1 || after.Refused != before.Refused {
			t.Fatalf("the read of %s counted %d filtered iterators and %d refusals, want 1 and 0; it did not ask the filter",
				read.what, d, after.Refused-before.Refused)
		}
		if candidates < read.minCandidates {
			t.Fatalf("the read of %s met %d candidate tables, want at least %d", read.what, candidates, read.minCandidates)
		}
		if skipped != 0 || unmarked != candidates {
			t.Fatalf("the read of %s skipped %d of %d tables of the earlier build and counted %d as unmarked, want 0 skipped and all unmarked",
				read.what, skipped, candidates, unmarked)
		}
	}
}
