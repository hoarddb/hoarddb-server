/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-15: tests of which iterators the prefix
 * bloom filter touches (IteratorOptions.BloomPrefix): none without the field,
 * and none whose BloomPrefix no rule names as a whole prefix.
 * See hoardDB/third_party/README.md.
 */

package badger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// prefixBloomTables writes each batch of keys, with the batch's number as value, and closes
// the DB after it, so every batch is one Level-0 table. It returns the DB reopened under
// rules. A key in two batches has two versions, the newer in the later table.
func prefixBloomTables(t *testing.T, dir string, rules []BloomPrefixRule, batches ...[]string) *DB {
	t.Helper()
	for number, batch := range batches {
		db, err := Open(prefixBloomOptions(dir, rules...))
		require.NoError(t, err)
		for _, key := range batch {
			txnSet(t, db, []byte(key), []byte{byte('0' + number)}, 0)
		}
		require.NoError(t, db.Close())
	}
	db, err := Open(prefixBloomOptions(dir, rules...))
	require.NoError(t, err)
	require.Len(t, db.Tables(), len(batches), "every batch is one table")
	return db
}

// iterated is one item an iterator yielded.
type iterated struct {
	key, value string
}

// collect walks an iterator made with opt from its start and returns every item.
func collect(t *testing.T, db *DB, opt IteratorOptions) []iterated {
	t.Helper()
	var items []iterated
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			items = append(items, iterated{key: string(it.Item().Key()), value: string(value)})
		}
		return nil
	}))
	return items
}

// TestIteratorsWithoutBloomPrefixAreUnchanged is the prefix bloom spec's T6, the pin of I-4:
// an iterator made without BloomPrefix (a plain one, one with a Prefix, a reverse one, a key
// iterator) yields what it always did and moves no prefix bloom counter, on a DB whose tables
// were built under the rules. A Prefix that happens to be a rule's whole prefix, present or
// absent, is still only a Prefix.
// Against: applying the probe when BloomPrefix is nil.
func TestIteratorsWithoutBloomPrefixAreUnchanged(t *testing.T) {
	db := prefixBloomTables(t, t.TempDir(), []BloomPrefixRule{testDocumentRule(), testStoreRule()},
		[]string{"d:1:", "d:1:a", "d:9:z", "s:1:a", "plain"},
		[]string{"d:2:a", "s:2:a", "s:2:b"},
		[]string{"d:1:a", "s:1:b"},
	)
	defer func() { require.NoError(t, db.Close()) }()

	all := []iterated{
		{"d:1:", "0"}, {"d:1:a", "2"}, {"d:2:a", "1"}, {"d:9:z", "0"}, {"plain", "0"},
		{"s:1:a", "0"}, {"s:1:b", "2"}, {"s:2:a", "1"}, {"s:2:b", "1"},
	}
	reversed := make([]iterated, 0, len(all))
	for index := len(all) - 1; index >= 0; index-- {
		reversed = append(reversed, all[index])
	}

	before := db.BloomPrefixStats()

	require.Equal(t, all, collect(t, db, DefaultIteratorOptions), "a plain iterator")
	require.Equal(t, reversed, collect(t, db, IteratorOptions{Reverse: true}), "a reverse iterator")
	require.Equal(t, all[:4], collect(t, db, IteratorOptions{Prefix: []byte("d:")}), "Prefix d:")
	require.Equal(t, all[:2], collect(t, db, IteratorOptions{Prefix: []byte("d:1:")}), "Prefix d:1:, a rule's whole prefix")
	require.Equal(t, all[7:], collect(t, db, IteratorOptions{Prefix: []byte("s:2:")}), "Prefix s:2:, a rule's whole prefix")
	require.Empty(t, collect(t, db, IteratorOptions{Prefix: []byte("d:5:")}), "Prefix d:5:, a rule's whole prefix no table holds")
	require.Equal(t, all[4:5], collect(t, db, IteratorOptions{Prefix: []byte("plain")}), "Prefix plain")

	// A key iterator yields the key's versions, newest first.
	var versions []string
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewKeyIterator([]byte("d:1:a"), IteratorOptions{AllVersions: true})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			require.Equal(t, "d:1:a", string(it.Item().Key()))
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			versions = append(versions, string(value))
		}
		return nil
	}))
	require.Equal(t, []string{"2", "0"}, versions, "a key iterator over d:1:a")

	require.Equal(t, before, db.BloomPrefixStats(), "an iterator without BloomPrefix moved a prefix bloom counter")
}

// TestNonRulePrefixIsRefused is the Badger half of the prefix bloom spec's T7: a BloomPrefix
// is used only when exactly one rule of the DB names its whole length and it agrees with a
// non-empty Prefix. Every other BloomPrefix is refused and counted, moves no rule's counters,
// and the iterator yields what it yields without the field.
// Against: dropping the whole-length check (a BloomPrefix longer than its rule's prefix is
// then hashed whole, no filter holds that hash, and every table is skipped).
func TestNonRulePrefixIsRefused(t *testing.T) {
	batches := [][]string{
		{"d:1:", "d:1:f1", "d:9:z", "s:1:a", "x:1:a"},
		{"d:1:f2", "d:2:a", "s:1:b"},
	}
	rules := []BloomPrefixRule{testDocumentRule(), testStoreRule()}
	db := prefixBloomTables(t, t.TempDir(), rules, batches...)
	defer func() { require.NoError(t, db.Close()) }()

	cases := []struct {
		name                string
		prefix, bloomPrefix string
	}{
		{"a prefix no rule answers", "x:1:", "x:1:"},
		{"a rule's letter alone", "d:", "d:"},
		{"an id cut before its end", "d:1", "d:1"},
		{"longer than the rule's prefix", "d:1:f", "d:1:f"},
		{"longer than the rule's prefix, under the shorter Prefix", "d:1:", "d:1:f"},
		{"a whole key under the rule", "d:1:f1", "d:1:f1"},
		{"a BloomPrefix that contradicts Prefix", "s:1:", "d:1:"},
		{"no Prefix", "", "d:1:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := collect(t, db, IteratorOptions{Prefix: []byte(tc.prefix)})
			before := db.BloomPrefixStats()
			got := collect(t, db, IteratorOptions{Prefix: []byte(tc.prefix), BloomPrefix: []byte(tc.bloomPrefix)})
			after := db.BloomPrefixStats()
			require.Equal(t, want, got, "the iterator with the refused BloomPrefix differs from the one without it")
			require.Equal(t, before.Refused+1, after.Refused, "the refusal was not counted once")
			require.Equal(t, before.Rules, after.Rules, "a refused BloomPrefix moved a rule's counters")
		})
	}
	// The cases above must have had something to lose.
	require.Len(t, collect(t, db, IteratorOptions{Prefix: []byte("d:1:f")}), 2)

	t.Run("two rules that answer the same key", func(t *testing.T) {
		twice, err := Open(prefixBloomOptions(t.TempDir(), testDocumentRule(), BloomPrefixRule{ID: 3, Len: testPrefixLen('d')}))
		require.NoError(t, err)
		defer func() { require.NoError(t, twice.Close()) }()
		txnSet(t, twice, []byte("d:1:a"), []byte("v"), 0)
		got := collect(t, twice, IteratorOptions{Prefix: []byte("d:1:"), BloomPrefix: []byte("d:1:")})
		require.Equal(t, []iterated{{"d:1:a", "v"}}, got)
		stats := twice.BloomPrefixStats()
		require.Equal(t, uint64(1), stats.Refused)
		require.Equal(t, uint64(0), stats.Rules[1].Iterators+stats.Rules[3].Iterators)
	})

	t.Run("a DB without rules", func(t *testing.T) {
		require.NoError(t, db.Close())
		plain, err := Open(getTestOptions(db.Opts().Dir).WithBloomFalsePositive(0.01))
		require.NoError(t, err)
		for _, bloomPrefix := range []string{"d:1:", "d:5:", "s:1:"} {
			want := collect(t, plain, IteratorOptions{Prefix: []byte(bloomPrefix)})
			got := collect(t, plain, IteratorOptions{Prefix: []byte(bloomPrefix), BloomPrefix: []byte(bloomPrefix)})
			require.Equal(t, want, got, "BloomPrefix %q on a DB without rules", bloomPrefix)
		}
		stats := plain.BloomPrefixStats()
		require.Equal(t, uint64(3), stats.Refused)
		require.Empty(t, stats.Rules)
		require.NoError(t, plain.Close())
		// Reopened so the deferred Close above has a DB to close.
		db, err = Open(prefixBloomOptions(db.Opts().Dir, rules...))
		require.NoError(t, err)
	})
}
