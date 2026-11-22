/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by the hoardDB project, 2026-10-26: TableSyncMode selects how a new
 * table file is made durable. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-11-15: BloomPrefixRule is one prefix
 * rule of the table bloom filter. See hoardDB/third_party/README.md.
 */

package options

// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
type ChecksumVerificationMode int

const (
	// NoVerification indicates DB should not verify checksum for SSTable blocks.
	NoVerification ChecksumVerificationMode = iota
	// OnTableRead indicates checksum should be verified while opening SSTtable.
	OnTableRead
	// OnBlockRead indicates checksum should be verified on every SSTable block read.
	OnBlockRead
	// OnTableAndBlockRead indicates checksum should be verified
	// on SSTable opening and on every block read.
	OnTableAndBlockRead
)

// CompressionType specifies how a block should be compressed.
type CompressionType uint32

const (
	// None mode indicates that a block is not compressed.
	None CompressionType = 0
	// Snappy mode indicates that a block is compressed using Snappy algorithm.
	Snappy CompressionType = 1
	// ZSTD mode indicates that a block is compressed using ZSTD algorithm.
	ZSTD CompressionType = 2
)

// MaxBloomPrefixRuleID is the highest id a BloomPrefixRule can have: a table's
// index records the rules it was built under as one bit per id in a uint32.
// hoardDB.
const MaxBloomPrefixRuleID = 32

// BloomPrefixRule tells the table builder to add, for every key the rule
// answers, a hash of the key's prefix to the table's bloom filter, so an
// iterator over that prefix can leave out the tables that hold no key under
// it (IteratorOptions.BloomPrefix). hoardDB.
//
// ID, 1 to MaxBloomPrefixRuleID, is written into every table built under the
// rule and names Len on disk: a Len that changes one answer must come with a
// new ID, or a table built under the old Len is skipped for keys it holds.
//
// Len returns the length n of the prefix userKey (a key without its
// timestamp) starts with, or 0 when the rule does not apply to the key. Two
// properties are the caller's to keep, and a read loses keys silently when
// either fails:
//   - Len reads only userKey[:n] to decide n, so for every tail
//     Len(userKey[:n] ‖ tail) == n;
//   - no key is answered (n > 0) by two rules of one DB.
type BloomPrefixRule struct {
	ID  uint32
	Len func(userKey []byte) int
}

// TableSyncMode selects how a new table file (a memtable flush or a compaction
// output) is made durable before the MANIFEST names it. hoardDB.
type TableSyncMode int

const (
	// TableSyncMsync is upstream Badger's one blocking msync(MS_SYNC) of the
	// whole file, which also commits the file's metadata. On ext4 each one
	// waits for a journal commit, and the writes behind it stall. The zero
	// value, so it is the default: an Options built by hand, or by a tool
	// that never heard of this setting, keeps upstream's behaviour.
	TableSyncMsync TableSyncMode = iota
	// TableSyncStepwise writes the table back in ranges of at most the table
	// sync step, without waiting on each, then waits once for the whole file.
	// It does not commit the file's metadata (its size and block allocation):
	// the MANIFEST fsync that follows does, but only on a filesystem that
	// commits its journal in order (ext4 or XFS with a journal; not btrfs,
	// ZFS or ext4 without a journal). Opt-in, because the filesystem cannot be
	// checked from here. On a platform without ranged writeback (anything but
	// Linux) it is TableSyncMsync.
	TableSyncStepwise
)
