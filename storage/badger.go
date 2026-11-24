package storage

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// BadgerInstance wraps a BadgerDB instance of its own. No server path opens
// one: every bucket lives on the node's store (OpenBadgerKV). It is kept for
// tools that measure the per-bucket instances the server used to open,
// with the same options.
type BadgerInstance struct {
	DB   *badger.DB
	Path string
}

// BadgerTuning holds the cache and compaction knobs of the node's store. They
// are node-wide: every bucket shares the one instance OpenBadgerKV opens, so
// the costs no longer multiply by bucket count, and the block cache is shared
// by every bucket's hot data. A zero field falls back to the default it
// replaces (see badgerOptions).
type BadgerTuning struct {
	BlockCacheMB  int // 0 = 128 MB
	IndexCacheMB  int // 0 = 64 MB
	NumCompactors int // 0 = 4
	// MemTableMB sets the memtable, and with it the atomic batch bound, which
	// Badger derives as 15% of it (5,033,164 bytes at 32 MiB). It has no
	// default: the server resolves it (the store's own choice, or an explicit
	// setting) before it opens the store, and zero is refused, so a path that
	// skipped resolving cannot be given a size silently.
	MemTableMB int
	// MemTableRequestedBy and MemTableRequestedByChoice say where MemTableMB
	// came from, for the shrink refusal (MemtableShrinkError).
	MemTableRequestedBy       string
	MemTableRequestedByChoice bool
	// TableSync is how a new table file is made durable: TableSyncMsync (or
	// empty) or TableSyncStepwise, the values of storage.badger_table_sync.
	TableSync string
}

// The values of BadgerTuning.TableSync; config.TableSyncStepwise and
// config.TableSyncMsync are the same strings.
const (
	TableSyncMsync    = "msync"
	TableSyncStepwise = "stepwise"
)

// tableSyncMode maps BadgerTuning.TableSync onto Badger's option. Empty is
// the msync default, so a caller that builds its own tuning (a bench, a test)
// gets what a node gets, and never the stepwise writeback without asking.
func tableSyncMode(tableSync string) (options.TableSyncMode, error) {
	switch tableSync {
	case "", TableSyncMsync:
		return options.TableSyncMsync, nil
	case TableSyncStepwise:
		return options.TableSyncStepwise, nil
	default:
		return 0, fmt.Errorf("unknown table sync %q: use %q or %q (storage.badger_table_sync)", tableSync, TableSyncStepwise, TableSyncMsync)
	}
}

// OpenBadger opens a BadgerDB of its own at the given path with the node
// store's options. key may be nil for plaintext operation or a 32-byte
// encryption key.
func OpenBadger(path string, tuning BadgerTuning, key []byte) (*BadgerInstance, error) {
	opts, err := badgerOptions(path, tuning, key)
	if err != nil {
		return nil, err
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening badger at %s: %w", path, err)
	}

	return &BadgerInstance{DB: db, Path: path}, nil
}

// applyExperimentOverrides reads test-only environment variables and applies
// Badger option overrides when set. The overrides are intended for benchmarking
// only and are not supported. Returns the modified options and any parsing errors.
// An unparseable or out-of-range value refuses to start with a descriptive error.
func applyExperimentOverrides(opts badger.Options) (badger.Options, error) {
	var appliedOverrides []string

	// WithCompression: accepted values are "none", "snappy", "zstd"
	if val := os.Getenv("HOARDDB_EXPERIMENT_BADGER_COMPRESSION"); val != "" {
		switch strings.ToLower(val) {
		case "none":
			opts = opts.WithCompression(options.None)
			appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_COMPRESSION="+val)
		case "snappy":
			opts = opts.WithCompression(options.Snappy)
			appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_COMPRESSION="+val)
		case "zstd":
			opts = opts.WithCompression(options.ZSTD)
			appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_COMPRESSION="+val)
		default:
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_COMPRESSION invalid value %q; accepted: none, snappy, zstd", val)
		}
	}

	// WithDetectConflicts: accepted values are "true", "false"
	if val := os.Getenv("HOARDDB_EXPERIMENT_BADGER_DETECT_CONFLICTS"); val != "" {
		switch strings.ToLower(val) {
		case "true":
			opts = opts.WithDetectConflicts(true)
			appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_DETECT_CONFLICTS="+val)
		case "false":
			opts = opts.WithDetectConflicts(false)
			appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_DETECT_CONFLICTS="+val)
		default:
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_DETECT_CONFLICTS invalid value %q; accepted: true, false", val)
		}
	}

	// WithNumCompactors: integer 2–16
	if val := os.Getenv("HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS"); val != "" {
		num, err := strconv.Atoi(val)
		if err != nil {
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS invalid value %q; must be integer 2-16: %w", val, err)
		}
		if num < 2 || num > 16 {
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS invalid value %d; must be integer 2-16", num)
		}
		opts = opts.WithNumCompactors(num)
		appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS="+val)
	}

	// WithNumLevelZeroTables: integer 2–64; also set WithNumLevelZeroTablesStall to value + 5
	if val := os.Getenv("HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES"); val != "" {
		num, err := strconv.Atoi(val)
		if err != nil {
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES invalid value %q; must be integer 2-64: %w", val, err)
		}
		if num < 2 || num > 64 {
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES invalid value %d; must be integer 2-64", num)
		}
		opts = opts.WithNumLevelZeroTables(num)
		opts = opts.WithNumLevelZeroTablesStall(num + 5)
		appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES="+val)
	}

	// WithBaseTableSize: integer 1–64 MB (converted to bytes with << 20)
	if val := os.Getenv("HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB"); val != "" {
		num, err := strconv.Atoi(val)
		if err != nil {
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB invalid value %q; must be integer 1-64: %w", val, err)
		}
		if num < 1 || num > 64 {
			return badger.Options{}, fmt.Errorf("HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB invalid value %d; must be integer 1-64", num)
		}
		opts = opts.WithBaseTableSize(int64(num) << 20)
		appliedOverrides = append(appliedOverrides, "HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB="+val)
	}

	// Log a warning if any overrides were applied
	if len(appliedOverrides) > 0 {
		overrideStr := strings.Join(appliedOverrides, "; ")
		// Use fmt.Fprintf to stderr for the warning, as this is pre-logger setup
		fmt.Fprintf(os.Stderr, "WARN: experimental Badger overrides in effect: %s; these are for benchmarking only, are not supported, and may be removed\n", overrideStr)
	}

	return opts, nil
}

// badgerOptions is the one place the Badger options are set, so the node's
// store (OpenBadgerKV) and a bench's own instance (OpenBadger) run the same
// literals.
func badgerOptions(path string, tuning BadgerTuning, key []byte) (badger.Options, error) {
	var err error

	// Validate key length if provided.
	if len(key) > 0 && len(key) != 32 {
		return badger.Options{}, fmt.Errorf("encryption key must be exactly 32 bytes, got %d", len(key))
	}

	blockCacheMB := tuning.BlockCacheMB
	if blockCacheMB <= 0 {
		blockCacheMB = 128
	}
	indexCacheMB := tuning.IndexCacheMB
	if indexCacheMB <= 0 {
		indexCacheMB = 64
	}
	numCompactors := tuning.NumCompactors
	if numCompactors <= 0 {
		numCompactors = 4
	}

	// Guard against Badger's panic when encryption is enabled and BlockCacheSize is zero.
	// Badger panics with "BlockCacheSize should be set since compression/encryption are enabled"
	// if a key is set and BlockCacheSize ends up as zero. The current defaulting makes this
	// unreachable today, but we refuse explicitly rather than rely on that assumption.
	if len(key) > 0 && blockCacheMB <= 0 {
		return badger.Options{}, fmt.Errorf("encryption requires BlockCacheSize > 0; got %d MB", blockCacheMB)
	}

	memTableMB := tuning.MemTableMB
	if memTableMB <= 0 {
		return badger.Options{}, fmt.Errorf("opening the store at %s: no memtable size was given (BadgerTuning.MemTableMB is %d). "+
			"The server resolves the size before it opens the store; a caller that opens one itself must pass a size of 32 MiB or more",
			path, memTableMB)
	}

	tableSync, err := tableSyncMode(tuning.TableSync)
	if err != nil {
		return badger.Options{}, fmt.Errorf("opening the store at %s: %w", path, err)
	}

	// Two Level-0 tables, stall at seven. Badger's default of five and fifteen
	// leaves more L0 tables for a point read to seek. Measured 2026-11-12 on the
	// read path: the L0 seek share fell from 7.9 to 2.3 percent of server CPU,
	// CPU per request from 179.8 to 164.3 us, requests per second rose from 13.6k
	// to 14.4k. The write side (compaction work and stalls under a write burst)
	// was not measured in that run.
	opts := badger.DefaultOptions(path).
		WithNumLevelZeroTables(2).
		WithNumLevelZeroTablesStall(7).
		WithCompression(options.Snappy).
		WithValueLogFileSize(128 << 20).               // 128 MB
		WithMemTableSize(int64(memTableMB) << 20).     // atomic batch bound is 15% of this
		WithNumMemtables(3).                           // flush headroom
		WithSyncWrites(false).                         // async for performance
		WithNumCompactors(numCompactors).              // parallel compaction
		WithBlockCacheSize(int64(blockCacheMB) << 20). // block cache for reads
		WithIndexCacheSize(int64(indexCacheMB) << 20). // index cache
		WithBloomFalsePositive(0.01).                  // 1% false positive rate for bloom filters
		// Every block read from disk is checked against its checksum; a block
		// in the block cache was checked when it was loaded. Without it a
		// plaintext block with a flipped bit decodes to a wrong value with no
		// error. It catches accidental damage only: an adversary
		// rewrites the checksum too, and for an encrypted store repairs it
		// through the same keystream.
		WithChecksumVerificationMode(options.OnBlockRead).
		WithTableSyncMode(tableSync)

	// Every table also holds, in its bloom filter, the document prefix or the
	// store prefix of each key, so a document read and a store-field list leave
	// out the tables that hold none of their keys (prefix bloom spec). Always
	// on, for every instance: the rule bits are part of ring format 6, and a
	// store opened without the rules would write tables a later open cannot
	// skip.
	opts = opts.WithBloomPrefixRules(
		badger.BloomPrefixRule{ID: DocumentPrefixRule, Len: DocumentPrefixLen},
		badger.BloomPrefixRule{ID: StorePrefixRule, Len: StorePrefixLen},
	)

	// Wire the encryption key if provided.
	if len(key) > 0 {
		opts = opts.WithEncryptionKey(key)
	}

	// Apply test-only environment variable overrides
	opts, err = applyExperimentOverrides(opts)
	if err != nil {
		return badger.Options{}, err
	}

	return opts, nil
}

// Close closes the underlying BadgerDB instance.
func (b *BadgerInstance) Close() error {
	return b.DB.Close()
}

// Put stores a key-value pair in Badger.
func (b *BadgerInstance) Put(key, value []byte) error {
	return b.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		if corpusTapEnabled && err == nil {
			recordCorpusValue(key, value)
		}
		return err
	})
}

// PrefixIterator returns an iterator positioned at the first key with the given
// prefix, scoped to that prefix.
//
// It previously returned an iterator sitting at the instance's first key with
// the prefix unused, so any caller whose prefix was not the very first key in
// the instance iterated nothing at all.
func (b *BadgerInstance) PrefixIterator(prefix []byte) (*badger.Iterator, *badger.Txn) {
	txn := b.DB.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	iter := txn.NewIterator(opts)
	iter.Seek(prefix)
	return iter, txn
}
