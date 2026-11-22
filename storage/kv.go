package storage

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrKeyNotFound is returned by KVSnapshot.Get for a key the snapshot does not
// hold, including one whose newest version is a delete.
var ErrKeyNotFound = errors.New("key not found")

// ErrBatchTooLarge is returned by KVBatch.Commit when the batch holds more
// than the store can commit atomically (KV.MaxBatchBytes). Nothing of the
// batch is written: a store that split it into two commits could separate a
// write from the state that records it, and a crash between them would be
// silent.
var ErrBatchTooLarge = errors.New("batch exceeds the largest batch the store commits atomically")

// BatchTooLargeError is the typed form of ErrBatchTooLarge: it carries the
// bounds the batch ran into so the layer that answers a client can state them
// without parsing text. errors.Is(err, ErrBatchTooLarge) still matches, and
// Error keeps the sentinel's text, which the ingest command matches to halve a
// batch.
type BatchTooLargeError struct {
	// Bytes is the most the store commits atomically, in bytes.
	Bytes int64
	// Entries is the most keys the store commits atomically; 0 when the store
	// has no entry bound.
	Entries int64
	// EntryCountReached is true when the batch ran out of entries before it
	// ran out of bytes, which is what small documents do.
	EntryCountReached bool
	// Key is the storage key that did not fit. It is an internal encoding and
	// belongs in a log, not in a message to a client.
	Key []byte
}

// Error keeps the text the refusal has always had.
func (e *BatchTooLargeError) Error() string {
	return fmt.Sprintf("%v (bound %d bytes, reached at key %x)", ErrBatchTooLarge, e.Bytes, e.Key)
}

// Unwrap makes errors.Is(err, ErrBatchTooLarge) true.
func (e *BatchTooLargeError) Unwrap() error { return ErrBatchTooLarge }

// MaxKeyBytes is the longest key the node store holds. It is Badger's own cap
// (maxKeySize in its txn.go, which does not export it); the kv_badger test
// TestBatchRefusesAKeyOverMaxKeyBytes fails if the two drift apart.
const MaxKeyBytes = 65000

// ErrKeyTooLarge is under every refusal of a key longer than MaxKeyBytes.
// Nothing of the batch is written.
var ErrKeyTooLarge = errors.New("key exceeds the longest key the store holds")

// KeyTooLargeError is the typed form of ErrKeyTooLarge. Badger's own text for
// this refusal ends in a hex dump of the key's first kilobyte, so Error leaves
// it out; Key keeps the whole key for the operator's log.
type KeyTooLargeError struct {
	// Size is the length of the refused key, in bytes.
	Size int
	// Limit is MaxKeyBytes.
	Limit int
	// Key is the refused storage key: an internal encoding that holds the
	// caller's data, so it belongs in a log and not in a message to a client.
	Key []byte
}

// Error names the sizes and never the key.
func (e *KeyTooLargeError) Error() string {
	return fmt.Sprintf("%v (%d bytes, limit %d)", ErrKeyTooLarge, e.Size, e.Limit)
}

// Unwrap makes errors.Is(err, ErrKeyTooLarge) true.
func (e *KeyTooLargeError) Unwrap() error { return ErrKeyTooLarge }

// KeyError is a failed operation at one node-store key: a read, a value copy,
// or adding a key to a batch. The key is an internal encoding — a slot, a
// bucket id, a namespace and the caller's key — that a client never sent and
// cannot act on, so Error names the operation and the cause but not the key: an
// operator diagnosing the failure reads Key, which an operator surface logs.
// Unwrap exposes the cause, so errors.Is still finds ErrKeyNotFound and a
// DamagedReadError.
type KeyError struct {
	// Operation is what failed at the key, e.g. "reading a key".
	Operation string
	// Key is the storage key the operation named.
	Key []byte
	// Cause is the failure under the operation.
	Cause error
}

// Error names the operation and the cause, never the key.
func (e *KeyError) Error() string {
	return fmt.Sprintf("%s: %v", e.Operation, e.Cause)
}

// Unwrap exposes the cause to errors.Is and errors.As.
func (e *KeyError) Unwrap() error { return e.Cause }

// KV is the node's one ordered key-value store: every engine layout is
// written against it, and nothing above it reaches the engine underneath. It
// is the seam the shared-storage spec (§14.4 item 1) requires — an atomic
// batch with a per-batch sync, a snapshot that opens without I/O, and a
// bounded iterator whose visit count includes delete markers.
type KV interface {
	// NewBatch starts an atomic write batch.
	NewBatch() KVBatch
	// NewSnapshot opens a consistent read view. It does no I/O, so a caller
	// may open one while holding a lock that must not wait on the disk.
	NewSnapshot() KVSnapshot
	// Sync makes every batch committed so far durable.
	Sync() error
	// MaxBatchBytes is the most a batch may hold and still commit atomically.
	MaxBatchBytes() int64
	// Close releases the store. Every snapshot and iterator must be closed first.
	Close() error
}

// KVBatch is an atomic set of writes: after Commit returns nil every write is
// visible to later snapshots, and after it returns an error none is. The batch
// keeps its own copy of every key and value it is handed.
type KVBatch interface {
	// Set writes value under key. An error sticks: Commit returns it and
	// writes nothing.
	Set(key, value []byte) error
	// Delete removes key, leaving a delete marker. It is the plain delete the
	// reclaimer uses; it maintains no cursor and no count.
	Delete(key []byte) error
	// Commit applies the batch atomically and, when sync is true, makes it
	// and every batch committed before it durable before returning.
	Commit(sync bool) error
	// Discard abandons an uncommitted batch. It is safe after Commit.
	Discard()
}

// KVSnapshot is a read view of live keys fixed at the moment it was opened.
// Delete markers are not part of that guarantee: compaction may drop a marker
// under an open snapshot once no deeper level overlaps it, so no caller may
// treat markers as stable across iterators (see IterOptions.IncludeDeleted).
type KVSnapshot interface {
	// Get returns a copy of key's newest live value, or ErrKeyNotFound.
	Get(key []byte) ([]byte, error)
	// NewIterator walks the snapshot within opts' bounds. The iterator starts
	// unpositioned; call Seek first.
	NewIterator(opts IterOptions) KVIterator
	// Discard releases the snapshot. Every iterator it opened must be closed first.
	Discard()
}

// IterOptions bounds an iterator to the half-open range [Lower, Upper). A nil
// bound is unbounded on that side.
type IterOptions struct {
	Lower   []byte
	Upper   []byte
	Reverse bool
	// IncludeDeleted makes the iterator stop at every key in its range, also
	// one whose newest version is a delete marker; Value then returns
	// ErrKeyNotFound for it. Only the reclaimer sets it: a step bounded in
	// visits must be able to stop inside a run of markers and record the last
	// one it passed as its cursor, and an iterator that walks such a run
	// inside a single Seek or Next cannot stop there. Live keys are fixed for
	// the snapshot; the markers are not: two iterators opened on one snapshot
	// with IncludeDeleted can return different sets of markers, because
	// compaction may drop a marker no deeper level overlaps.
	IncludeDeleted bool
	// bloomPrefix, when set, is the document or store prefix every key inside
	// the bounds starts with: the store may then leave out each table whose
	// bloom filter says it holds no key under that prefix (prefix bloom spec
	// §3.3). Only DocumentIterOptions and StoreIterOptions set it, and the
	// store checks it against the bounds again when the iterator is made,
	// because a table left out may hold the newest version of a key outside
	// the prefix: an iterator that asked the filter must never be read
	// outside it.
	bloomPrefix []byte
}

// PrefixEnd is the smallest key above every key that begins with prefix, or nil
// when there is none (prefix is empty or all 0xFF), which as an upper bound is
// "unbounded".
func PrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// DocumentIterOptions bounds an iterator to the keys of one document, its
// header and its scalar fields: [prefix, PrefixEnd(prefix)), where prefix is the
// document's header key. The iterator asks the tables' bloom filters about the
// document, so a table that holds no key of it is not opened. A prefix that is
// not one document's whole prefix gets the same bounds and no filter.
func DocumentIterOptions(prefix []byte) IterOptions {
	opts := IterOptions{Lower: prefix, Upper: PrefixEnd(prefix)}
	if DocumentPrefixLen(prefix) == len(prefix) {
		opts.bloomPrefix = prefix
	}
	return opts
}

// StoreIterOptions bounds an iterator to [lower, upper) inside one store field,
// such as the element range of one store instance. The iterator asks the
// tables' bloom filters about the store field lower lies in (for a store
// declared on a store's element, the outer store), so a table that holds no
// key of it is not opened. When lower is not a store-field key, or upper
// leaves lower's store field, the bounds are returned with no filter.
func StoreIterOptions(lower, upper []byte) IterOptions {
	opts := IterOptions{Lower: lower, Upper: upper}
	if n := StorePrefixLen(lower); n > 0 && len(upper) >= n && bytes.Equal(upper[:n], lower[:n]) {
		opts.bloomPrefix = lower[:n]
	}
	return opts
}

// filterPrefix returns the prefix the iterator may ask the bloom filters
// about, or nil: bloomPrefix when the bounds still lie inside it. The
// constructors guarantee that; this holds it for options whose bounds were
// changed after they were made.
func (o IterOptions) filterPrefix() []byte {
	prefix := o.bloomPrefix
	if len(prefix) == 0 || !bytes.HasPrefix(o.Lower, prefix) {
		return nil
	}
	if bytes.HasPrefix(o.Upper, prefix) || isPrefixEnd(o.Upper, prefix) {
		return prefix
	}
	return nil
}

// isPrefixEnd reports whether end equals PrefixEnd(prefix) for a prefix that
// has one, without building it.
func isPrefixEnd(end, prefix []byte) bool {
	last := len(prefix) - 1
	for last >= 0 && prefix[last] == 0xFF {
		last--
	}
	return last >= 0 && len(end) == last+1 && end[last] == prefix[last]+1 && bytes.Equal(end[:last], prefix[:last])
}

// PrefixBloomRuleStats is what the table bloom filter did for one prefix rule
// since the store was opened (prefix bloom spec §3.4). Internal: tests and the
// bench read it.
type PrefixBloomRuleStats struct {
	// Iterators is the reads that asked the filter.
	Iterators uint64
	// Candidates is the tables those reads would have opened without it.
	Candidates uint64
	// Skipped is the candidates the filter left out.
	Skipped uint64
	// Unmarked is the candidates that were built without the rule, which are
	// never left out. In a ring directory of the current format it is 0.
	Unmarked uint64
	// PrefixHashes is the filter entries the rule added to the tables built.
	PrefixHashes uint64
}

// PrefixBloomStats is PrefixBloomRuleStats for the document rule and the store
// rule, and the reads whose filter the store refused and served unfiltered.
// The mean tables per read is Candidates / Iterators; the filter's effect is
// Skipped / Candidates.
type PrefixBloomStats struct {
	Document PrefixBloomRuleStats
	Store    PrefixBloomRuleStats
	Refused  uint64
}

// KVIterator walks the live, newest version of each key in its range, in key
// order (or reverse key order). Visits counts everything the walk touched to
// get there — older versions and delete markers included — because that, not
// the number of keys returned, is what a walk over a dropped range costs.
type KVIterator interface {
	// Seek positions the iterator at the first live key at or after key
	// (forward) or at or before key (reverse), clamped to the bounds; a nil
	// key starts at the bound the direction begins from. With
	// IncludeDeleted, "live" reads "stored, live or not".
	Seek(key []byte)
	// Valid reports whether the iterator is positioned at a key.
	Valid() bool
	// Key returns the current key. It is valid until the next Seek or Next.
	// The iterator copies it out the first time it is asked for at a position,
	// so a walk that never asks for a key pays for no copy.
	Key() []byte
	// Value returns a copy of the current key's value, or ErrKeyNotFound when
	// its newest version is a delete (only with IncludeDeleted).
	Value() ([]byte, error)
	// Next advances to the next live key (with IncludeDeleted, the next key).
	Next()
	// Visits is the number of stored versions the walk has touched so far,
	// live or not.
	Visits() int64
	// Err is the error that ended the walk, or nil when it ended because the
	// range did. Valid turns false on an error just as it does at the end, so
	// a walk that does not ask Err cannot tell a damaged store file from the
	// end of its range: every caller asks once Valid is false.
	Err() error
	// Close releases the iterator.
	Close()
}
