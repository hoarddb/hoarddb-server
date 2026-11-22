package store

import (
	"errors"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
)

// ErrStopShort ends a take walk early without failing it: a visitor or a
// per-entry check returns it (wrapped or not) to say "stop before this entry
// and keep what was taken" (claim-ack-spec §4.9, OQ-S9). It is one sentinel for
// every stop a claim makes: the delivery budget, an entry over the hard cap
// that is not the first, and the result ceiling. A walk that does not allow a
// stop, which is pop, treats it as the ordinary abort every other visitor
// error is, so pop keeps its rule that it never takes fewer than it was asked.
var ErrStopShort = errors.New("the walk stops short and keeps what it took")

// The delivery budget (claim-ack-spec §4.4). They are server/cluster_memtable.go's
// largeCommitBytes and largeCommitEntries, computed the same way from the
// smallest memtable any member may run, so every copy stops at the same entry
// and every batch fits every member's own bound. The halving is the headroom
// the state document, the mark and the exempt unit are written into.
var (
	deliveryBudgetBytes = config.MemtableBatchBytes(config.MinMemtableMB) / 2
	deliveryBudgetKeys  = storage.BatchEntryBound(config.MemtableBatchBytes(config.MinMemtableMB)) / 4
)

// DeliveryBudget is the most bytes and the most keys one delivery apply adds to
// its batch, apart from its exempt first unit. The server pins it equal to the
// owner's large-commit thresholds.
func DeliveryBudget() (bytes, keys int64) {
	return deliveryBudgetBytes, deliveryBudgetKeys
}

// deliveryKeyOverhead is what Badger charges one key toward the batch bound
// beyond the key's and value's own bytes. Measured by
// TestDeliveryChargeCommitsAtTheBudget on the store at the floor memtable: a
// full batch of 40-byte keys and 100-byte values holds 33,112 keys, 152.0 bytes
// each, 12 beyond key and value (the version stamp Badger appends to a key and
// its meta bytes). The next multiple of 8 above that, so a change in Badger's
// estimate is a failure of the test and not a drift. The number of keys a
// batch may hold is a separate bound, charged through deliveryBudgetKeys.
const deliveryKeyOverhead = 16

// deliveryCharge totals what an apply has added to its batch, conservatively:
// every Set and every Delete costs its key and value bytes plus
// deliveryKeyOverhead, and one key, even when a later operation on the same key
// cancels an earlier one.
type deliveryCharge struct {
	Bytes int64
	Keys  int64
}

// add charges one Set (valueLen > 0) or one Delete (valueLen 0).
func (c *deliveryCharge) add(keyLen, valueLen int) {
	c.Bytes += int64(keyLen + valueLen + deliveryKeyOverhead)
	c.Keys++
}

const (
	// claimEntryHeadroom is what a claimed entry's worst single apply may add
	// beyond its own bytes: the state document, the 'h' and 'd' keys, the 'x'
	// wrapper and Badger's per-key overhead. The build measures the real
	// figure with an 8 KiB heap id and stops if it exceeds 32 KiB.
	claimEntryHeadroom = 64 << 10
	// maxClaimIDBytes is the longest heap id a claim takes (OQ-S10). It keeps
	// the deletes of pop and ack, whose 'c' and 'e' keys carry the id, inside
	// the headroom; it implies the key-limit condition.
	maxClaimIDBytes = 8 << 10
	// MaxClaimIDBytes is maxClaimIDBytes for the server, which refuses an ack's
	// item id longer than a heap origin suffix can be (8 bytes of priority and
	// an id of this length) before the store reads a key for it.
	MaxClaimIDBytes = maxClaimIDBytes
	// entryKeyBytes is an 'e' key without its origin suffix: the 18-byte bucket
	// prefix, the namespace byte, the kind byte and the 20-byte lease id.
	entryKeyBytes = scopeBytes + 1 + 1 + 20
	// deadWrapperBytes is the 'x' record around the stored entry: the BSON
	// framing of data, deliveries, dead_at and reason (71 bytes around a queue
	// entry) and a heap's {_id_, priority, value} document around its value,
	// rounded up.
	deadWrapperBytes = 128
	// claimFixedBytes is the footprint of a claimed entry beyond its value and
	// the suffix copies: the 0x04, 'e' and 'x' key prefixes and the 'x' wrapper.
	claimFixedBytes = (scopeBytes + 1) + entryKeyBytes + (scopeBytes + 1 + 1) + deadWrapperBytes
)

// maxClaimEntryBytes is the most an entry's delivery footprint may be: the floor
// memtable's batch bound less claimEntryHeadroom, 4,967,628 bytes. An entry
// over it could not be claimed and then returned in one batch.
var maxClaimEntryBytes = config.MemtableBatchBytes(config.MinMemtableMB) - claimEntryHeadroom

// claimLayout is the shape of an engine's origin suffix, which decides how many
// keys and values repeat it.
type claimLayout int

const (
	// claimLayoutQueue is fifo and lifo: an 8-byte position, repeated by the
	// 0x04, 'e' and 'x' keys.
	claimLayoutQueue claimLayout = iota
	// claimLayoutHeap is [inverted priority : 8][entry id], repeated by those
	// three keys and by the 'x' value's _id_.
	claimLayoutHeap
)

// claimRefusal says which condition of the hard cap an entry failed.
type claimRefusal int

const (
	claimOK claimRefusal = iota
	claimFootprintTooLarge
	claimIDTooLong
	claimKeyTooLong
)

// claimFootprint is what the worst single apply for an entry writes for it: its
// value, the origin suffix in every key and value that repeats it (three on fifo
// and lifo, four on a heap), and the fixed prefixes and wrapper.
func claimFootprint(layout claimLayout, valueLen, suffixLen int) int64 {
	repeats := int64(3)
	if layout == claimLayoutHeap {
		repeats = 4
	}
	return int64(valueLen) + repeats*int64(suffixLen) + claimFixedBytes
}

// claimable reports whether an entry may be claimed, and the first condition of
// claim-ack-spec §4.4 Edge 2 it fails. Every copy decides alike: the thresholds
// are constants and the sizes are in the snapshot.
func claimable(layout claimLayout, valueLen, suffixLen int) claimRefusal {
	if claimFootprint(layout, valueLen, suffixLen) > maxClaimEntryBytes {
		return claimFootprintTooLarge
	}
	if layout == claimLayoutHeap && suffixLen-8 > maxClaimIDBytes {
		return claimIDTooLong
	}
	// With maxClaimIDBytes at 8 KiB the id bound refuses every entry whose 'e'
	// key could pass Badger's limit, so this never fires. It is the tripwire for
	// a raised id bound (TestClaimEntryKeyLimitStaysAboveTheIDBound).
	if entryKeyBytes+suffixLen > storage.MaxKeyBytes {
		return claimKeyTooLong
	}
	return claimOK
}

// scopeBytes is the width of storage.Scope: slot 2, database id 8, bucket id 8.
// The budget constants above count it where they once counted the 10-byte
// bucket prefix.
const scopeBytes = 18
