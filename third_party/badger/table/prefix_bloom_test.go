/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-15: tests of the prefix hashes the
 * builder adds to a table's bloom filter (Options.BloomPrefixRules).
 * See hoardDB/third_party/README.md.
 */

package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"
)

// zeroHashPrefix is four bytes whose y.Hash is 0: the hash of one four-byte block is
// ((seed ^ 4m) + block) * m, then an xor-shift, and both are zero exactly when the block is
// minus (seed ^ 4m).
func zeroHashPrefix() []byte {
	seed, m, length := uint32(0xbc9f1d34), uint32(0xc6a4a793), uint32(4)
	start := seed ^ length*m
	return binary.LittleEndian.AppendUint32(nil, -start)
}

// buildPrefixTable builds a table of the given user keys, in order, under rules.
func buildPrefixTable(t *testing.T, userKeys [][]byte, rules []options.BloomPrefixRule) *Table {
	t.Helper()
	builder := NewTableBuilder(Options{BlockSize: 4 * 1024, BloomFalsePositive: 0.01, BloomPrefixRules: rules})
	defer builder.Close()
	for _, userKey := range userKeys {
		builder.Add(y.KeyWithTs(userKey, 1), y.ValueStruct{Value: []byte("v"), Meta: 'A'}, 0)
	}
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	built, err := CreateTable(filename, builder)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, built.DecrRef()) })
	return built
}

// TestFieldOnlyTableIsNotSkipped is the builder half of the prefix bloom spec's T2: a table
// whose first key is a field key with no header before it, under a prefix whose hash is 0,
// must hold that prefix in its filter. A builder that remembers "the last prefix hash" in a
// variable that starts at 0, with no flag saying whether it has seen one, takes the first
// prefix for a repeat and drops it, and every read of that document then skips the table.
// Against: comparing against a zero "last hash" with no seen flag.
func TestFieldOnlyTableIsNotSkipped(t *testing.T) {
	prefix := zeroHashPrefix()
	require.Equal(t, uint32(0), y.Hash(prefix), "the chosen prefix must hash to 0")
	rule := options.BloomPrefixRule{ID: 1, Len: func(userKey []byte) int {
		if len(userKey) >= len(prefix) && bytes.Equal(userKey[:len(prefix)], prefix) {
			return len(prefix)
		}
		return 0
	}}
	// Two field keys and no header: no stored key equals the prefix.
	fieldKeys := [][]byte{append(bytes.Clone(prefix), 0x00, 0x02), append(bytes.Clone(prefix), 0x00, 0x03)}

	// The case only bites when the key hashes alone leave hash 0 out of the filter.
	keyHashes := []uint32{y.Hash(fieldKeys[0]), y.Hash(fieldKeys[1])}
	require.False(t, y.NewFilter(keyHashes, y.BloomBitsPerKey(len(keyHashes), 0.01)).MayContain(0),
		"the key hashes alone already cover hash 0; pick other field ids")

	built := buildPrefixTable(t, fieldKeys, []options.BloomPrefixRule{rule})
	require.True(t, built.HasBloomPrefixRule(1), "the table does not carry rule 1's bit")
	require.False(t, built.DoesNotHavePrefix(0, 1),
		"the table holds two keys under the prefix and its filter says the prefix is absent")
	require.Equal(t, uint32(2), built.KeyCount(), "KeyCount counts keys, not prefix hashes")

	// The ordinary shape: a header (the prefix itself) and then its fields.
	withHeader := buildPrefixTable(t, [][]byte{prefix, fieldKeys[0], fieldKeys[1]}, []options.BloomPrefixRule{rule})
	require.False(t, withHeader.DoesNotHavePrefix(0, 1))

	// A table with no key under the rule carries the bit and says "absent".
	other := buildPrefixTable(t, [][]byte{[]byte("other-1"), []byte("other-2")}, []options.BloomPrefixRule{rule})
	require.True(t, other.HasBloomPrefixRule(1))
	require.True(t, other.DoesNotHavePrefix(0, 1), "a table with no key under the prefix was not skippable")
	// And never for a rule it was not built under.
	require.False(t, other.DoesNotHavePrefix(0, 2))
}
