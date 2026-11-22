/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-15: tests of the prefix bloom filter
 * (Options.BloomPrefixRules, IteratorOptions.BloomPrefix).
 * See hoardDB/third_party/README.md.
 */

package badger

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// testPrefixLen answers for keys "<letter>:<id>:<rest>": the prefix runs through the second
// colon. It reads nothing after the prefix, as a rule function must.
func testPrefixLen(letter byte) func([]byte) int {
	return func(userKey []byte) int {
		if len(userKey) < 3 || userKey[0] != letter || userKey[1] != ':' {
			return 0
		}
		end := bytes.IndexByte(userKey[2:], ':')
		if end < 0 {
			return 0
		}
		return 2 + end + 1
	}
}

func testDocumentRule() BloomPrefixRule { return BloomPrefixRule{ID: 1, Len: testPrefixLen('d')} }
func testStoreRule() BloomPrefixRule    { return BloomPrefixRule{ID: 2, Len: testPrefixLen('s')} }

// prefixBloomOptions are test options with a filter and the given rules.
func prefixBloomOptions(dir string, rules ...BloomPrefixRule) Options {
	return getTestOptions(dir).WithBloomFalsePositive(0.01).WithBloomPrefixRules(rules...)
}

// keysUnder walks [prefix, ...) with the given iterator options and returns the user keys
// that start with prefix.
func keysUnder(t *testing.T, db *DB, opt IteratorOptions, prefix []byte) []string {
	t.Helper()
	var keys []string
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(prefix); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, prefix) {
				break
			}
			keys = append(keys, string(key))
		}
		return nil
	}))
	return keys
}

// bloomIter is a keys-only iterator over table prefix "d:" or "s:" that asks the filter
// about bloomPrefix.
func bloomIter(tablePrefix, bloomPrefix string) IteratorOptions {
	return IteratorOptions{Prefix: []byte(tablePrefix), BloomPrefix: []byte(bloomPrefix)}
}

// TestRuleBitsAndOptionChecks is the prefix bloom spec's T5: a table answers for a rule only
// when it was built under that rule (I-2), a table with rule bits opens under a DB that has
// no rules, and Open refuses a rule list it cannot keep apart on disk.
// Against: testing "any bit" instead of the rule's bit.
func TestRuleBitsAndOptionChecks(t *testing.T) {
	dir := t.TempDir()

	// Built under rule 1 only: one table that holds a document and a store key.
	db, err := Open(prefixBloomOptions(dir, testDocumentRule()))
	require.NoError(t, err)
	txnSet(t, db, []byte("d:1:a"), []byte("v"), 0)
	txnSet(t, db, []byte("d:9:a"), []byte("v"), 0)
	txnSet(t, db, []byte("s:1:a"), []byte("v"), 0)
	txnSet(t, db, []byte("s:9:a"), []byte("v"), 0)
	require.NoError(t, db.Close())

	// Reopened under both rules.
	db, err = Open(prefixBloomOptions(dir, testDocumentRule(), testStoreRule()))
	require.NoError(t, err)
	require.Len(t, db.Tables(), 1, "the close flushed the memtable into one table")

	before := db.BloomPrefixStats()
	// Rule 2 asks a table that has only rule 1's bit: never skipped, counted as unmarked,
	// and the store key in it is read.
	require.Equal(t, []string{"s:1:a"}, keysUnder(t, db, bloomIter("s:", "s:1:"), []byte("s:1:")))
	require.Empty(t, keysUnder(t, db, bloomIter("s:", "s:5:"), []byte("s:5:")))
	after := db.BloomPrefixStats()
	require.Equal(t, uint64(2), after.Rules[2].Iterators-before.Rules[2].Iterators)
	require.Equal(t, uint64(2), after.Rules[2].Candidates-before.Rules[2].Candidates)
	require.Equal(t, uint64(2), after.Rules[2].Unmarked-before.Rules[2].Unmarked)
	require.Equal(t, uint64(0), after.Rules[2].Skipped-before.Rules[2].Skipped,
		"a table built without rule 2 was skipped for a rule 2 prefix")
	require.Equal(t, before.Rules[1], after.Rules[1], "rule 2's iterators moved rule 1's counters")

	// Rule 1 asks the same table: present is read, absent is skipped.
	require.Equal(t, []string{"d:1:a"}, keysUnder(t, db, bloomIter("d:", "d:1:"), []byte("d:1:")))
	require.Empty(t, keysUnder(t, db, bloomIter("d:", "d:5:"), []byte("d:5:")))
	last := db.BloomPrefixStats()
	require.Equal(t, uint64(2), last.Rules[1].Candidates-after.Rules[1].Candidates)
	require.Equal(t, uint64(1), last.Rules[1].Skipped-after.Rules[1].Skipped)
	require.Equal(t, uint64(0), last.Rules[1].Unmarked-after.Rules[1].Unmarked)
	require.Equal(t, uint64(0), last.Refused)
	require.NoError(t, db.Close())

	// A table with rule bits opens under a DB with no rules, which reads every key and
	// refuses the filter.
	db, err = Open(getTestOptions(dir).WithBloomFalsePositive(0.01))
	require.NoError(t, err)
	require.Equal(t, []string{"d:1:a"}, keysUnder(t, db, bloomIter("d:", "d:1:"), []byte("d:1:")))
	require.Equal(t, []string{"s:9:a"}, keysUnder(t, db, bloomIter("s:", "s:9:"), []byte("s:9:")))
	require.Empty(t, keysUnder(t, db, bloomIter("d:", "d:5:"), []byte("d:5:")))
	plain := db.BloomPrefixStats()
	require.Equal(t, uint64(3), plain.Refused)
	require.Empty(t, plain.Rules)
	require.NoError(t, db.Close())

	// Open refuses a rule list it cannot keep apart on disk.
	lenOf := testPrefixLen('d')
	for name, rules := range map[string][]BloomPrefixRule{
		"id 0":          {{ID: 0, Len: lenOf}},
		"id 33":         {{ID: 33, Len: lenOf}},
		"a repeated id": {{ID: 4, Len: lenOf}, {ID: 4, Len: testPrefixLen('s')}},
		"a nil Len":     {{ID: 1, Len: nil}},
	} {
		refused, err := Open(prefixBloomOptions(t.TempDir(), rules...))
		if err == nil {
			require.NoError(t, refused.Close())
			t.Fatalf("Open accepted a rule list with %s", name)
		}
		require.Contains(t, err.Error(), "BloomPrefixRules", fmt.Sprintf("the refusal of %s does not name the option", name))
	}
	// The whole id range is accepted.
	accepted, err := Open(prefixBloomOptions(t.TempDir(), BloomPrefixRule{ID: 1, Len: lenOf}, BloomPrefixRule{ID: 32, Len: testPrefixLen('s')}))
	require.NoError(t, err)
	require.NoError(t, accepted.Close())
}
