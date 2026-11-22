/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-11-15: the prefix bloom filter. A table's
 * bloom filter also holds a hash of each key's prefix (Options.BloomPrefixRules),
 * and an iterator over one prefix (IteratorOptions.BloomPrefix) leaves out the
 * tables whose filter does not hold it. See hoardDB/third_party/README.md.
 */

package badger

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

// checkBloomPrefixRules refuses a rule list whose ids a table's index could not
// keep apart, or a rule with no function.
func checkBloomPrefixRules(rules []BloomPrefixRule) error {
	var seen uint32
	for _, rule := range rules {
		if rule.ID < 1 || rule.ID > options.MaxBloomPrefixRuleID {
			return fmt.Errorf("BloomPrefixRules: rule id %d is outside 1 to %d", rule.ID, options.MaxBloomPrefixRuleID)
		}
		if rule.Len == nil {
			return fmt.Errorf("BloomPrefixRules: rule %d has no Len function", rule.ID)
		}
		bit := table.BloomPrefixRuleBit(rule.ID)
		if seen&bit != 0 {
			return fmt.Errorf("BloomPrefixRules: rule id %d is used twice", rule.ID)
		}
		seen |= bit
	}
	return nil
}

// BloomPrefixRuleStats is what the prefix bloom filter did under one rule
// since the DB was opened.
type BloomPrefixRuleStats struct {
	// Iterators is the iterators whose BloomPrefix was accepted under the rule.
	Iterators uint64
	// Candidates is the tables those iterators would have opened: the ones
	// that passed the key-range check.
	Candidates uint64
	// Skipped is the candidates left out because their filter does not hold
	// the prefix.
	Skipped uint64
	// Unmarked is the candidates that were not built under the rule, which
	// are never asked and never left out.
	Unmarked uint64
	// PrefixHashes is the prefix hashes the builder added to table filters
	// under the rule, over all tables built.
	PrefixHashes uint64
}

// BloomPrefixStats is BloomPrefixRuleStats for each rule of the DB, by rule
// id, and the iterators whose BloomPrefix was refused (no rule of the DB names
// it as a whole prefix, or it contradicts Prefix): those ran unfiltered.
type BloomPrefixStats struct {
	Rules   map[uint32]BloomPrefixRuleStats
	Refused uint64
}

// BloomPrefixStats returns the DB's prefix bloom counters. They start at zero
// at every Open.
func (db *DB) BloomPrefixStats() BloomPrefixStats {
	stats := BloomPrefixStats{Rules: make(map[uint32]BloomPrefixRuleStats, len(db.opt.BloomPrefixRules)), Refused: db.bloomPrefix.Refused.Load()}
	for _, rule := range db.opt.BloomPrefixRules {
		counters := db.bloomPrefix.Rule(rule.ID)
		stats.Rules[rule.ID] = BloomPrefixRuleStats{
			Iterators:    counters.Iterators.Load(),
			Candidates:   counters.Candidates.Load(),
			Skipped:      counters.Skipped.Load(),
			Unmarked:     counters.Unmarked.Load(),
			PrefixHashes: counters.PrefixHashes.Load(),
		}
	}
	return stats
}

// bloomPrefixPick is an iterator's accepted BloomPrefix: the rule that names
// it, its hash, and where the picks count. The zero value is "no filter".
type bloomPrefixPick struct {
	ruleID   uint32
	hash     uint32
	counters *table.BloomPrefixRuleCounters
	// candidates, skipped and unmarked collect one level's picks, so the
	// shared counters are written once per level and not once per table.
	candidates, skipped, unmarked uint64
}

// acceptBloomPrefix decides whether opt.BloomPrefix may filter tables. It may
// when the iterator has a Prefix that agrees with it, exactly one rule of the
// DB answers the BloomPrefix's whole length and no other rule answers it at
// all: then every key under BloomPrefix was hashed by that rule to this very
// prefix (BloomPrefixRule's two properties), so a table whose filter lacks the
// hash holds no such key. Anything else is refused and counted, and the
// iterator is the one it would have been without the field.
func (db *DB) acceptBloomPrefix(opt *IteratorOptions) {
	opt.bloom = bloomPrefixPick{}
	if len(opt.BloomPrefix) == 0 {
		return
	}
	ruleID := db.bloomPrefixRuleOf(opt.Prefix, opt.BloomPrefix)
	if ruleID == 0 {
		opt.BloomPrefix = nil
		db.bloomPrefix.Refused.Add(1)
		return
	}
	opt.bloom = bloomPrefixPick{ruleID: ruleID, hash: y.Hash(opt.BloomPrefix), counters: db.bloomPrefix.Rule(ruleID)}
	opt.bloom.counters.Iterators.Add(1)
}

// bloomPrefixRuleOf returns the id of the one rule that names bloomPrefix as a
// whole prefix, or 0 when the filter must not be used.
func (db *DB) bloomPrefixRuleOf(prefix, bloomPrefix []byte) uint32 {
	if len(prefix) == 0 {
		return 0
	}
	common := min(len(prefix), len(bloomPrefix))
	if !bytes.Equal(prefix[:common], bloomPrefix[:common]) {
		return 0
	}
	var ruleID uint32
	for _, rule := range db.opt.BloomPrefixRules {
		switch n := rule.Len(bloomPrefix); {
		case n == 0:
		case n == len(bloomPrefix) && ruleID == 0:
			ruleID = rule.ID
		default:
			// A rule that answers a shorter prefix, or a second rule that
			// answers at all: the builder may have hashed these keys to
			// another prefix.
			return 0
		}
	}
	return ruleID
}

// skips reports whether t can be left out of the iterator: it was built under
// the rule and its filter does not hold the prefix. It tallies the pick.
func (pick *bloomPrefixPick) skips(t table.TableInterface) bool {
	pick.candidates++
	if !t.HasBloomPrefixRule(pick.ruleID) {
		pick.unmarked++
		return false
	}
	if !t.DoesNotHavePrefix(pick.hash, pick.ruleID) {
		return false
	}
	pick.skipped++
	return true
}

// flush adds the tally of one level's picks to the DB's counters.
func (pick *bloomPrefixPick) flush() {
	if pick.candidates == 0 {
		return
	}
	pick.counters.Candidates.Add(pick.candidates)
	if pick.skipped > 0 {
		pick.counters.Skipped.Add(pick.skipped)
	}
	if pick.unmarked > 0 {
		pick.counters.Unmarked.Add(pick.unmarked)
	}
	pick.candidates, pick.skipped, pick.unmarked = 0, 0, 0
}
