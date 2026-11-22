package storage

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"
)

// OpenBadgerKVSmallTables opens a store whose tables are 1 MiB at every level,
// so a test-sized store holds hundreds of them. It exists for the tests that
// show a read's cost does not grow with the table count.
func OpenBadgerKVSmallTables(dir string, memTableMB int) (*BadgerKV, error) {
	opts, err := badgerOptions(dir, BadgerTuning{MemTableMB: memTableMB}, nil)
	if err != nil {
		return nil, err
	}
	opts.BaseTableSize = 1 << 20
	opts.TableSizeMultiplier = 1
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening the small-table store: %w", err)
	}
	return newBadgerKV(db, dir)
}

// TableCount reports how many SST files the store holds.
func (kv *BadgerKV) TableCount() int {
	return len(kv.db.Tables())
}

// CorpusTapEnabled exposes the tap's compile-time switch to the external test
// package, so one test file can pin it in each build.
const CorpusTapEnabled = corpusTapEnabled

// FailBadgerSync makes every later sync of kv fail with cause, as a disk error does.
func FailBadgerSync(kv *BadgerKV, cause error) {
	kv.syncDB = func() (uint64, error) { return 0, cause }
}

// FailBadgerSyncOnce fails the next sync of kv with cause and lets every later one reach
// Badger, as a transient disk error does.
func FailBadgerSyncOnce(kv *BadgerKV, cause error) {
	real := kv.db.SyncVouched
	failed := false
	kv.syncDB = func() (uint64, error) {
		if !failed {
			failed = true
			return 0, cause
		}
		return real()
	}
}

// PoisonDuringNextSync makes the next sync of kv stand for one that began while another
// commit's sync failed: the failure is recorded in the middle of it, and Badger's own sync
// then reports success.
func PoisonDuringNextSync(kv *BadgerKV, cause error) {
	real := kv.db.SyncVouched
	kv.syncDB = func() (uint64, error) {
		// poisonSync echoes cause back; the point here is that this sync reports success.
		if err := kv.poisonSync(cause); !errors.Is(err, cause) {
			return 0, err
		}
		return real()
	}
}

// OpenBadgerKVNoCompaction opens a store with the node's options and no compactor, so every
// flush stays its own Level-0 table and a test knows which tables a read can meet.
func OpenBadgerKVNoCompaction(dir string) (*BadgerKV, error) {
	opts, err := badgerOptions(dir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		return nil, err
	}
	opts.NumCompactors = 0
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening the store without compaction: %w", err)
	}
	return newBadgerKV(db, dir)
}

// OpenBadgerKVWithoutPrefixRules opens a store with the node's options less the prefix rules
// of the table bloom filter, and no compactor: its tables carry no rule bit, as the tables of
// a store written before the rules existed, and each flush stays its own Level-0 table.
func OpenBadgerKVWithoutPrefixRules(dir string) (*BadgerKV, error) {
	opts, err := badgerOptions(dir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		return nil, err
	}
	opts.BloomPrefixRules = nil
	opts.NumCompactors = 0
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening the store without prefix rules: %w", err)
	}
	return newBadgerKV(db, dir)
}

// OpenBadgerKVSplittingTables opens a store with the node's options, no compactor, and tables
// below Level 0 of about 2 KiB in 512-byte blocks, so a compaction (Compact) cuts its output
// into many tables and the cuts fall inside documents and store fields. Compression is off
// because a builder with compression learns its size from goroutines, late, and the cuts
// would then depend on timing.
func OpenBadgerKVSplittingTables(dir string) (*BadgerKV, error) {
	opts, err := badgerOptions(dir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		return nil, err
	}
	opts.NumCompactors = 0
	opts.BaseTableSize = 2 << 10
	opts.TableSizeMultiplier = 1
	opts.BlockSize = 512
	opts.Compression = options.None
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening the store with splitting tables: %w", err)
	}
	return newBadgerKV(db, dir)
}

// Compact rewrites the store's tables into one level, as compactions do over time. Badger
// leaves a store alone whose tables all sit in Level 0 and number no more than the Level-0
// cap (two), so a test that needs the rewrite flushes three times first, or checks TableSpans.
func (kv *BadgerKV) Compact() error {
	if err := kv.db.Flatten(1); err != nil {
		return fmt.Errorf("compacting the store: %w", err)
	}
	return nil
}

// TableSpan is one table's level and its smallest and biggest stored key.
type TableSpan struct {
	Level             int
	Smallest, Biggest []byte
}

// TableSpans returns every table of the store, by level and then by smallest key.
func (kv *BadgerKV) TableSpans() []TableSpan {
	tables := kv.db.Tables()
	spans := make([]TableSpan, 0, len(tables))
	for _, table := range tables {
		spans = append(spans, TableSpan{Level: table.Level, Smallest: y.ParseKey(table.Left), Biggest: y.ParseKey(table.Right)})
	}
	slices.SortFunc(spans, func(a, b TableSpan) int {
		if a.Level != b.Level {
			return a.Level - b.Level
		}
		return bytes.Compare(a.Smallest, b.Smallest)
	})
	return spans
}

// BloomPrefixOf returns the prefix opts would ask the table bloom filters about, as the
// constructor set it, or nil for plain bounds.
func BloomPrefixOf(opts IterOptions) []byte {
	return opts.bloomPrefix
}

// WithBloomPrefix returns opts asking the table bloom filters about prefix whatever the
// constructors would have decided: what a caller could do if the field were exported.
func WithBloomPrefix(opts IterOptions, prefix []byte) IterOptions {
	opts.bloomPrefix = prefix
	return opts
}
