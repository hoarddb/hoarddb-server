package storage_test

import (
	"encoding/binary"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestUnmarkedTableIsNeverSkipped is the prefix bloom spec's T4, the proof of I-2: a table
// built with no prefix rules (a store written before the rules existed) carries no rule bit,
// and its filter holds no prefix hash, so "the filter does not hold the prefix" says nothing
// about it. Reopened under both rules it is never skipped: a field written without its header
// and a store element are read from it, and it is counted as unmarked. After a compaction the
// rewritten tables carry both bits, and an absent prefix skips them.
// Against: dropping the rule-bit check in bloomPrefixPick.skips, the gate that runs before
// DoesNotHavePrefix is ever asked.
func TestUnmarkedTableIsNeverSkipped(t *testing.T) {
	store := openBloomStore(t, storage.OpenBadgerKVWithoutPrefixRules)
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
	doc, field := document('m'), fieldKeyOf(document('m'), 2)

	// Table A: the header, the field's first value, and neighbours on both sides in both
	// namespaces, so its range covers every prefix read below.
	store.apply([]bloomPair{
		{doc, []byte("header")}, {field, []byte("old")},
		{document('a'), []byte("low")}, {document('z'), []byte("high")},
		{element('a', 0), []byte("low")}, {element('z', 0), []byte("high")},
	})
	store.flush()
	// Table B: the field without its header and one element of a store, so no key in it
	// equals the document prefix and its filter holds neither prefix.
	store.apply([]bloomPair{{field, []byte("new")}, {element('m', 0), []byte("element")}})

	// Reopened as the node opens it, under both rules.
	store.open = storage.OpenBadgerKVNoCompaction
	store.flush()
	if tables := store.kv.TableCount(); tables != 2 {
		t.Fatalf("the store holds %d tables, want the two built without rules", tables)
	}

	wantDocument := []bloomPair{{doc, []byte("header")}, {field, []byte("new")}}
	wantElements := []bloomPair{{element('m', 0), []byte("element")}}
	// read walks opts and returns what the walk moved in rule's counters.
	read := func(what string, opts storage.IterOptions, want []bloomPair, rule func(storage.PrefixBloomStats) storage.PrefixBloomRuleStats) (candidates, skipped, unmarked uint64) {
		t.Helper()
		before := store.kv.PrefixBloomStats()
		got := store.walk(opts)
		after := store.kv.PrefixBloomStats()
		if !samePairs(got, want) {
			t.Fatalf("%s returned%s, want%s", what, describePairs(got), describePairs(want))
		}
		if d := rule(after).Iterators - rule(before).Iterators; d != 1 {
			t.Fatalf("%s counted %d filtered iterators, want 1", what, d)
		}
		if after.Refused != before.Refused {
			t.Fatalf("%s had its filter refused", what)
		}
		return rule(after).Candidates - rule(before).Candidates, rule(after).Skipped - rule(before).Skipped, rule(after).Unmarked - rule(before).Unmarked
	}
	documentRule := func(stats storage.PrefixBloomStats) storage.PrefixBloomRuleStats { return stats.Document }
	storeRule := func(stats storage.PrefixBloomStats) storage.PrefixBloomRuleStats { return stats.Store }

	for _, unmarkedRead := range []struct {
		what          string
		opts          storage.IterOptions
		want          []bloomPair
		rule          func(storage.PrefixBloomStats) storage.PrefixBloomRuleStats
		minCandidates uint64
	}{
		{"the document whose field sits in a table without its header", storage.DocumentIterOptions(doc), wantDocument, documentRule, 2},
		{"the store whose element sits in an unmarked table", listOf('m'), wantElements, storeRule, 2},
		{"a document no table holds", storage.DocumentIterOptions(document('q')), nil, documentRule, 1},
		{"a store no table holds", listOf('q'), nil, storeRule, 1},
	} {
		candidates, skipped, unmarked := read(unmarkedRead.what, unmarkedRead.opts, unmarkedRead.want, unmarkedRead.rule)
		if candidates < unmarkedRead.minCandidates {
			t.Fatalf("the read of %s met %d candidate tables, want at least %d", unmarkedRead.what, candidates, unmarkedRead.minCandidates)
		}
		if skipped != 0 {
			t.Fatalf("the read of %s skipped %d tables built without the rules; such a table's filter says nothing about a prefix", unmarkedRead.what, skipped)
		}
		if unmarked != candidates {
			t.Fatalf("the read of %s counted %d of its %d candidate tables as unmarked, want all", unmarkedRead.what, unmarked, candidates)
		}
	}

	// A third Level-0 table, built under the rules, takes Level 0 past its cap, and the
	// compaction rewrites all three under both rules.
	store.apply([]bloomPair{{document('c'), []byte("third")}})
	store.flush()
	if err := store.kv.Compact(); err != nil {
		t.Fatal(err)
	}
	spans := store.kv.TableSpans()
	if len(spans) == 0 || spans[0].Level == 0 {
		t.Fatalf("the compaction did not rewrite Level 0: %d tables, %+v", len(spans), spans)
	}

	for _, rewrittenRead := range []struct {
		what   string
		opts   storage.IterOptions
		want   []bloomPair
		rule   func(storage.PrefixBloomStats) storage.PrefixBloomRuleStats
		absent bool
	}{
		{"the document after the compaction", storage.DocumentIterOptions(doc), wantDocument, documentRule, false},
		{"the store after the compaction", listOf('m'), wantElements, storeRule, false},
		{"a document no table holds, after the compaction", storage.DocumentIterOptions(document('q')), nil, documentRule, true},
		{"a store no table holds, after the compaction", listOf('q'), nil, storeRule, true},
	} {
		candidates, skipped, unmarked := read(rewrittenRead.what, rewrittenRead.opts, rewrittenRead.want, rewrittenRead.rule)
		if candidates == 0 {
			t.Fatalf("the read of %s met no candidate table", rewrittenRead.what)
		}
		if unmarked != 0 {
			t.Fatalf("the read of %s met %d unmarked tables; the compaction's output must carry both rule bits", rewrittenRead.what, unmarked)
		}
		if rewrittenRead.absent && skipped != candidates {
			t.Fatalf("the read of %s skipped %d of %d rewritten tables, want all: none holds the prefix", rewrittenRead.what, skipped, candidates)
		}
	}
}
