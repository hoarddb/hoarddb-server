/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-15: tests of what a table's index and
 * bloom filter carry under Options.BloomPrefixRules.
 * See hoardDB/third_party/README.md.
 */

package table

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

// colonPrefixLen answers for keys "<letter>:<id>:<rest>": the prefix runs through the second
// colon. It reads nothing after the prefix, as a rule function must.
func colonPrefixLen(letter byte) func([]byte) int {
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

// TestIndexCarriesTheRules is the prefix bloom spec's T9: what a built table records. The
// rule bits survive a close and OpenTable, plain and encrypted; KeyCount counts keys and not
// prefix hashes; the filter is sized for the keys plus the prefix hashes at Badger's 7 bits
// an entry and ends in its probe count, 4; the builder reports the prefix hashes per rule;
// and a table built without a filter carries no bit and is never skipped.
// Against: counting prefix hashes in KeyCount; sizing the filter for the key hashes only.
func TestIndexCarriesTheRules(t *testing.T) {
	const documentRuleID, storeRuleID, unusedRuleID = 1, 5, 2
	rules := []options.BloomPrefixRule{
		{ID: documentRuleID, Len: colonPrefixLen('d')},
		{ID: storeRuleID, Len: colonPrefixLen('s')},
	}
	// In key order. A header ("d:1:") is its own prefix and adds no hash; a run of keys under
	// one prefix with no header adds one.
	userKeys := []string{
		"d:1:", "d:1:a", "d:1:b", // header first: no prefix hash
		"d:2:a", "d:2:b", // one
		"d:3:",           // none
		"d:4:x",          // one
		"s:1:a", "s:1:b", // one
		"s:2:a",      // one
		"x:1", "x:2", // no rule answers
	}
	presentPrefixes := map[string]uint32{
		"d:1:": documentRuleID, "d:2:": documentRuleID, "d:3:": documentRuleID, "d:4:": documentRuleID,
		"s:1:": storeRuleID, "s:2:": storeRuleID,
	}
	const documentPrefixHashes, storePrefixHashes = 2, 2
	filterEntries := len(userKeys) + documentPrefixHashes + storePrefixHashes
	wantFilterBytes := (max(64, 7*filterEntries)+7)/8 + 1
	require.NotEqual(t, wantFilterBytes, (max(64, 7*len(userKeys))+7)/8+1,
		"the prefix hashes must change the filter's size for the size to prove they were counted")

	encryptionKey := make([]byte, 32)
	for index := range encryptionKey {
		encryptionKey[index] = byte(index)
	}
	indexCache, err := ristretto.NewCache(&ristretto.Config[uint64, *fb.TableIndex]{NumCounters: 1000, MaxCost: 1 << 20, BufferItems: 64})
	require.NoError(t, err)
	defer indexCache.Close()

	build := func(t *testing.T, opts Options) (*Table, string) {
		t.Helper()
		builder := NewTableBuilder(opts)
		defer builder.Close()
		for _, userKey := range userKeys {
			builder.Add(y.KeyWithTs([]byte(userKey), 1), y.ValueStruct{Value: []byte("v"), Meta: 'A'}, 0)
		}
		filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
		built, err := CreateTable(filename, builder)
		require.NoError(t, err)
		return built, filename
	}

	for _, tc := range []struct {
		name    string
		dataKey *pb.DataKey
	}{
		{name: "plain"},
		{name: "encrypted", dataKey: &pb.DataKey{Data: encryptionKey, KeyId: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counters := &BloomPrefixCounters{}
			opts := Options{BlockSize: 4 * 1024, BloomFalsePositive: 0.01, BloomPrefixRules: rules, BloomPrefixCounters: counters, DataKey: tc.dataKey}
			if tc.dataKey != nil {
				opts.IndexCache = indexCache
			}
			built, filename := build(t, opts)

			require.Equal(t, uint64(documentPrefixHashes), counters.Rule(documentRuleID).PrefixHashes.Load(), "prefix hashes reported under the document rule")
			require.Equal(t, uint64(storePrefixHashes), counters.Rule(storeRuleID).PrefixHashes.Load(), "prefix hashes reported under the store rule")
			require.Equal(t, uint64(0), counters.Rule(unusedRuleID).PrefixHashes.Load(), "prefix hashes reported under a rule the table was not built under")

			check := func(t *testing.T, opened *Table) {
				t.Helper()
				require.Equal(t, uint32(len(userKeys)), opened.KeyCount(), "KeyCount counts keys, not prefix hashes")
				require.Equal(t, BloomPrefixRuleBit(documentRuleID)|BloomPrefixRuleBit(storeRuleID), opened.cheapIndex().BloomPrefixRules, "the index's rule bits")
				require.True(t, opened.HasBloomPrefixRule(documentRuleID))
				require.True(t, opened.HasBloomPrefixRule(storeRuleID))
				require.False(t, opened.HasBloomPrefixRule(unusedRuleID), "the table answers for a rule it was not built under")

				filter := opened.fetchIndex().BloomFilterBytes()
				require.Len(t, filter, wantFilterBytes, "the filter is sized for %d keys and %d prefix hashes at 7 bits each", len(userKeys), filterEntries-len(userKeys))
				require.Equal(t, byte(4), filter[len(filter)-1], "the filter's probe count")

				for prefix, ruleID := range presentPrefixes {
					require.False(t, opened.DoesNotHavePrefix(y.Hash([]byte(prefix)), ruleID), "the table holds a key under %q and says the prefix is absent", prefix)
				}
				// Twenty prefixes no key has: at the filter's false-positive rate nearly all
				// must read as absent.
				var absent int
				for i := range 20 {
					if opened.DoesNotHavePrefix(y.Hash(fmt.Appendf(nil, "d:absent-%d:", i)), documentRuleID) {
						absent++
					}
				}
				require.GreaterOrEqual(t, absent, 15, "of 20 prefixes no key has, the filter calls only %d absent", absent)
			}
			check(t, built)

			// Closed and opened again from the file: what OpenTable reads is what was built.
			require.NoError(t, built.Close(-1))
			mapped, err := z.OpenMmapFile(filename, os.O_RDWR, 0)
			require.NoError(t, err)
			reopened, err := OpenTable(mapped, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.DecrRef()) }()
			check(t, reopened)
		})
	}

	t.Run("no filter", func(t *testing.T) {
		counters := &BloomPrefixCounters{}
		built, _ := build(t, Options{BlockSize: 4 * 1024, BloomFalsePositive: 0, BloomPrefixRules: rules, BloomPrefixCounters: counters})
		defer func() { require.NoError(t, built.DecrRef()) }()
		require.Equal(t, uint32(len(userKeys)), built.KeyCount())
		require.Empty(t, built.fetchIndex().BloomFilterBytes(), "a table built with BloomFalsePositive 0 has a filter")
		require.Equal(t, uint32(0), built.cheapIndex().BloomPrefixRules, "a table with no filter carries rule bits, which promise a filter that holds the prefixes")
		for _, ruleID := range []uint32{documentRuleID, storeRuleID} {
			require.False(t, built.HasBloomPrefixRule(ruleID))
			require.False(t, built.DoesNotHavePrefix(y.Hash([]byte("d:absent:")), ruleID), "a table with no filter was skipped")
		}
	})
}
