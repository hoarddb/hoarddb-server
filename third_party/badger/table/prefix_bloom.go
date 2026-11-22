/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-15: the build side of the prefix bloom
 * filter (Options.BloomPrefixRules) and its counters.
 * See hoardDB/third_party/README.md.
 */

package table

import (
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"
)

// BloomPrefixRuleBit is the bit of prefix rule ruleID in a table index's
// bloom_prefix_rules, or 0 for an id no rule can have.
func BloomPrefixRuleBit(ruleID uint32) uint32 {
	if ruleID < 1 || ruleID > options.MaxBloomPrefixRuleID {
		return 0
	}
	return 1 << (ruleID - 1)
}

// BloomPrefixRuleCounters counts what the prefix bloom filter did under one
// rule; badger.BloomPrefixRuleStats documents each.
type BloomPrefixRuleCounters struct {
	Iterators, Candidates, Skipped, Unmarked, PrefixHashes atomic.Uint64
}

// BloomPrefixCounters are one DB's prefix bloom counters: one set per rule id
// and the refused iterators. The zero value is ready to use.
type BloomPrefixCounters struct {
	rules   [options.MaxBloomPrefixRuleID]BloomPrefixRuleCounters
	Refused atomic.Uint64
}

// Rule returns the counters of rule ruleID, which must be a valid rule id.
func (c *BloomPrefixCounters) Rule(ruleID uint32) *BloomPrefixRuleCounters {
	return &c.rules[ruleID-1]
}

// prefixHashes is the builder's state for Options.BloomPrefixRules.
type prefixHashes struct {
	// hashes are the prefix hashes to add to the filter beside the key hashes.
	hashes []uint32
	// last is the hash of the newest key's prefix, and seen says whether there
	// has been one. The flag is not optional: 0 is a hash a prefix can have,
	// so a "last" that starts at 0 would take such a prefix, met first, for a
	// repeat and leave it out of the filter.
	last uint32
	seen bool
	// perRule[i] counts the hashes added under Options.BloomPrefixRules[i].
	perRule []uint32
}

// addPrefixHash is called once for every key that enters the table, right
// after the key's own hash is recorded. The first rule that answers the key
// names its prefix; the prefix's hash goes into the filter unless it is
// already there: the key before it had the same prefix hash, or the prefix is
// the whole key, whose own hash is the same number and is in keyHashes. Equal
// hashes set equal filter bits, so neither shortcut can leave a needed bit
// unset; key order only decides how many duplicates are saved.
func (b *Builder) addPrefixHash(userKey []byte) {
	for i := range b.opts.BloomPrefixRules {
		n := b.opts.BloomPrefixRules[i].Len(userKey)
		// A length outside the key is not one a rule may return (BloomPrefixRule);
		// such a key is treated as not answered rather than sliced out of range.
		if n <= 0 || n > len(userKey) {
			continue
		}
		hash := y.Hash(userKey[:n])
		if b.prefixes.seen && hash == b.prefixes.last {
			return
		}
		b.prefixes.seen, b.prefixes.last = true, hash
		if n == len(userKey) {
			return
		}
		b.prefixes.hashes = append(b.prefixes.hashes, hash)
		if b.prefixes.perRule == nil {
			b.prefixes.perRule = make([]uint32, len(b.opts.BloomPrefixRules))
		}
		b.prefixes.perRule[i]++
		return
	}
}

// filterHashes is every hash the table's bloom filter is built over: one per
// key and the prefix hashes.
func (b *Builder) filterHashes() []uint32 {
	if len(b.prefixes.hashes) == 0 {
		return b.keyHashes
	}
	hashes := make([]uint32, 0, len(b.keyHashes)+len(b.prefixes.hashes))
	hashes = append(hashes, b.keyHashes...)
	return append(hashes, b.prefixes.hashes...)
}

// bloomPrefixRuleBits is the index's bloom_prefix_rules for a table built with
// a filter: the bit of every rule in the options, whether or not a key of the
// table was answered by it. A table with no key under a rule must carry the
// rule's bit too: its filter holding none of the rule's prefixes is exactly
// what lets a reader leave it out.
func (b *Builder) bloomPrefixRuleBits() uint32 {
	var bits uint32
	for i := range b.opts.BloomPrefixRules {
		bits |= BloomPrefixRuleBit(b.opts.BloomPrefixRules[i].ID)
	}
	return bits
}

// countPrefixHashes reports the built table's prefix hashes to the DB.
func (b *Builder) countPrefixHashes() {
	if b.opts.BloomPrefixCounters == nil {
		return
	}
	for i, added := range b.prefixes.perRule {
		if added > 0 {
			b.opts.BloomPrefixCounters.Rule(b.opts.BloomPrefixRules[i].ID).PrefixHashes.Add(uint64(added))
		}
	}
}
