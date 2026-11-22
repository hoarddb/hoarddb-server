package storage

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// This file is a measurement, not a regression gate: it answers whether a
// level-0 table with a damaged data block stalls writes (BUG-0381). It is slow
// by design (it waits out a stall to prove it), so it runs only when asked for
// explicitly with HOARDDB_BUG0381_MEASURE=1. It changes no production code.
//
// The damaged run happens in a child process because a store stalled this way
// cannot be closed from the same process: Close waits for room in flushChan,
// which the flush goroutine holds while it waits in addLevel0Table for level 0
// to drain, and only a successful compaction can drain it. The child prints its
// numbers and exits, and the OS reclaims the stalled store.
const (
	bug0381MeasureEnv   = "HOARDDB_BUG0381_MEASURE"
	bug0381HelperEnv    = "HOARDDB_BUG0381_HELPER"
	bug0381MemTableSize = 1 << 20
	bug0381KeyUniverse  = 20000
	bug0381ValueSize    = 256
	bug0381BatchEntries = 128
	// bug0381BatchLimit bounds a healthy run so the measurement terminates even
	// if compaction is slower than the writer.
	bug0381BatchLimit = 800
	// bug0381StallBound is how long one transaction may fail to commit before
	// it is called a stall. A 128-entry batch into a 1 MiB memtable takes
	// microseconds to a few milliseconds on this hardware, so 20 s is far
	// beyond normal jitter.
	bug0381StallBound = 20 * time.Second
)

// bug0381BadgerOptions is the node store's own option set with the compaction
// knobs turned down so a handful of tiny tables reaches the stall threshold.
// The fields are set after badgerOptions so an experiment override in the
// environment cannot change the measurement.
func bug0381BadgerOptions(dir string, numCompactors int) (badger.Options, error) {
	opts, err := badgerOptions(dir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		return badger.Options{}, err
	}
	return opts.
		WithMemTableSize(bug0381MemTableSize).
		WithValueThreshold(1 << 10).
		WithNumLevelZeroTables(4).
		WithNumLevelZeroTablesStall(6).
		WithNumCompactors(numCompactors).
		WithBaseTableSize(256 << 10).
		WithBlockCacheSize(8 << 20).
		WithIndexCacheSize(0).
		WithCompactL0OnClose(false).
		WithChecksumVerificationMode(options.OnBlockRead), nil
}

// bug0381Payload is stable, incompressible bytes for one key, so a table holds
// real data blocks and a flipped bit lands in one of them.
func bug0381Payload(index int) []byte {
	var payloadBytes []byte
	seed := []byte(fmt.Sprintf("bug0381-%08d", index))
	for len(payloadBytes) < bug0381ValueSize {
		digest := sha256.Sum256(seed)
		payloadBytes = append(payloadBytes, digest[:]...)
		seed = digest[:]
	}
	return payloadBytes[:bug0381ValueSize]
}

// bug0381Payloads builds one payload per key once, so the writer loop spends
// its time in Badger rather than in SHA-256.
func bug0381Payloads() [][]byte {
	payloads := make([][]byte, bug0381KeyUniverse)
	for index := range payloads {
		payloads[index] = bug0381Payload(index)
	}
	return payloads
}

// bug0381Key returns the key at index.
func bug0381Key(index int) []byte {
	return []byte(fmt.Sprintf("doc-%08d", index))
}

// bug0381WriteBatch writes the next bug0381BatchEntries keys. The key order is
// sequential from the writer's cursor, so every table is a sub-range of the
// seed table, which spans the whole universe.
func bug0381WriteBatch(db *badger.DB, payloads [][]byte, cursor *int) error {
	return db.Update(func(txn *badger.Txn) error {
		for written := 0; written < bug0381BatchEntries; written++ {
			index := *cursor % bug0381KeyUniverse
			if err := txn.Set(bug0381Key(index), payloads[index]); err != nil {
				return err
			}
			*cursor++
		}
		return nil
	})
}

// bug0381SeedIndex writes key 0 and the last key first, so the first memtable
// (and therefore the table the run damages) spans the whole key universe; every
// later table is then a sub-range that the damaged table overlaps.
func bug0381SeedIndex(written int) int {
	switch written {
	case 0:
		return 0
	case 1:
		return bug0381KeyUniverse - 1
	default:
		return written - 1
	}
}

// bug0381SeedTables writes small transactions until one table has flushed. On
// close the active memtable flushes as a second table, so the seeding leaves
// two level-0 tables, the first spanning the whole key universe.
func bug0381SeedTables(db *badger.DB, payloads [][]byte) error {
	written := 0
	for bug0381L0Count(db) < 1 {
		if written > bug0381KeyUniverse {
			return fmt.Errorf("seeded %d keys without a level-0 table", written)
		}
		err := db.Update(func(txn *badger.Txn) error {
			for batch := 0; batch < bug0381BatchEntries; batch++ {
				index := bug0381SeedIndex(written)
				if err := txn.Set(bug0381Key(index), payloads[index]); err != nil {
					return err
				}
				written++
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// bug0381L0Count is how many level-0 tables the store reports right now.
func bug0381L0Count(db *badger.DB) int {
	count := 0
	for _, table := range db.Tables() {
		if table.Level == 0 {
			count++
		}
	}
	return count
}

// bug0381TableFiles lists the store's table files in the order initTables uses
// for level 0 (by ascending ID, oldest first).
func bug0381TableFiles(t *testing.T, dir string) []string {
	t.Helper()
	tablePaths, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("listing tables in %s: %v", dir, err)
	}
	if len(tablePaths) == 0 {
		t.Fatalf("the seeded store at %s holds no table file", dir)
	}
	sort.Strings(tablePaths)
	return tablePaths
}

// bug0381FlipDataBlock flips one bit a third of the way into a table, inside
// its data blocks (the index and its checksum sit at the end of the file).
func bug0381FlipDataBlock(t *testing.T, tablePath string) {
	t.Helper()
	tableBytes, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatalf("reading %s: %v", tablePath, err)
	}
	tableBytes[len(tableBytes)/3] ^= 0x10
	if err := os.WriteFile(tablePath, tableBytes, 0o600); err != nil {
		t.Fatalf("writing %s: %v", tablePath, err)
	}
}

// bug0381RecordingLogger keeps Badger's own words: how often a compaction
// failed, and whether it announced a held-open level-0 stall.
type bug0381RecordingLogger struct {
	mu                 sync.Mutex
	compactionFailures int
	compactionFirstErr string
	l0StallLines       int
}

func (logger *bug0381RecordingLogger) Errorf(format string, args ...interface{}) {
	logger.record(fmt.Sprintf(format, args...))
}

func (logger *bug0381RecordingLogger) Warningf(format string, args ...interface{}) {
	logger.record(fmt.Sprintf(format, args...))
}

func (logger *bug0381RecordingLogger) Infof(format string, args ...interface{}) {
	logger.record(fmt.Sprintf(format, args...))
}

func (logger *bug0381RecordingLogger) Debugf(string, ...interface{}) {}

func (logger *bug0381RecordingLogger) record(line string) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if strings.Contains(line, "LOG Compact FAILED") {
		logger.compactionFailures++
		if logger.compactionFirstErr == "" {
			logger.compactionFirstErr = line
		}
	}
	if strings.Contains(line, "L0 was stalled") {
		logger.l0StallLines++
	}
}

func (logger *bug0381RecordingLogger) snapshot() (failures int, firstErr string, stallLines int) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.compactionFailures, logger.compactionFirstErr, logger.l0StallLines
}

// bug0381Outcome is what one run of the measurement observed.
type bug0381Outcome struct {
	damaged          bool
	batchesCommitted int
	stalled          bool
	l0AtStall        int
	failureCount     int
	firstFailureLine string
	blockReadFailure bool
	stallLines       int
	slowestBatch     time.Duration
	wall             time.Duration
}

// outcomeLine renders the outcome as one machine-readable line for the child
// process to print and the parent to parse.
func (outcome bug0381Outcome) outcomeLine() string {
	return fmt.Sprintf("BUG0381OUTCOME damaged=%t stalled=%t batches=%d l0=%d failures=%d blockread=%t stall_lines=%d slowest_ms=%d wall_ms=%d",
		outcome.damaged, outcome.stalled, outcome.batchesCommitted, outcome.l0AtStall,
		outcome.failureCount, outcome.blockReadFailure, outcome.stallLines,
		outcome.slowestBatch.Milliseconds(), outcome.wall.Milliseconds())
}

func parseOutcomeLine(t *testing.T, line string) bug0381Outcome {
	t.Helper()
	var outcome bug0381Outcome
	var slowestMs, wallMs int64
	if _, err := fmt.Sscanf(line, "BUG0381OUTCOME damaged=%t stalled=%t batches=%d l0=%d failures=%d blockread=%t stall_lines=%d slowest_ms=%d wall_ms=%d",
		&outcome.damaged, &outcome.stalled, &outcome.batchesCommitted, &outcome.l0AtStall,
		&outcome.failureCount, &outcome.blockReadFailure, &outcome.stallLines, &slowestMs, &wallMs); err != nil {
		t.Fatalf("parsing outcome line %q: %v", line, err)
	}
	outcome.slowestBatch = time.Duration(slowestMs) * time.Millisecond
	outcome.wall = time.Duration(wallMs) * time.Millisecond
	return outcome
}

// bug0381Run seeds two level-0 tables, optionally damages the older one, reopens
// the store and writes batches until either the batch limit is reached or one
// batch fails to commit within bug0381StallBound.
//
// When a stall is found the store is deliberately left open: closing it from
// this process deadlocks (see the file comment), so there is no DB handle to
// close and the caller must not touch it again. A healthy run is closed.
func bug0381Run(t *testing.T, damaged bool) bug0381Outcome {
	t.Helper()
	dir := t.TempDir()
	payloads := bug0381Payloads()

	seedOpts, err := bug0381BadgerOptions(dir, 0)
	if err != nil {
		t.Fatalf("building the seed options: %v", err)
	}
	seedDB, err := badger.Open(seedOpts)
	if err != nil {
		t.Fatalf("opening the store to seed tables: %v", err)
	}
	if err := bug0381SeedTables(seedDB, payloads); err != nil {
		t.Fatalf("seeding tables: %v", err)
	}
	if err := seedDB.Close(); err != nil {
		t.Fatalf("closing the seeded store: %v", err)
	}

	tables := bug0381TableFiles(t, dir)
	if damaged {
		bug0381FlipDataBlock(t, tables[0])
	}

	logger := &bug0381RecordingLogger{}
	runOpts, err := bug0381BadgerOptions(dir, 2)
	if err != nil {
		t.Fatalf("building the run options: %v", err)
	}
	runOpts = runOpts.WithLogger(logger)
	runDB, err := badger.Open(runOpts)
	if err != nil {
		t.Fatalf("reopening the store (damaged=%v): %v", damaged, err)
	}

	outcome := bug0381Outcome{damaged: damaged}
	cursor := 0
	start := time.Now()
	for batch := 0; batch < bug0381BatchLimit; batch++ {
		committed := make(chan error, 1)
		batchStart := time.Now()
		go func() {
			committed <- bug0381WriteBatch(runDB, payloads, &cursor)
		}()

		timer := time.NewTimer(bug0381StallBound)
		select {
		case writeErr := <-committed:
			timer.Stop()
			if writeErr != nil {
				t.Fatalf("writing batch %d (damaged=%v): %v", batch, damaged, writeErr)
			}
			outcome.batchesCommitted++
			if elapsed := time.Since(batchStart); elapsed > outcome.slowestBatch {
				outcome.slowestBatch = elapsed
			}
		case <-timer.C:
			outcome.stalled = true
			outcome.l0AtStall = bug0381L0Count(runDB)
		}
		if outcome.stalled {
			break
		}
	}
	outcome.wall = time.Since(start)

	if !outcome.stalled {
		if err := runDB.Close(); err != nil {
			t.Fatalf("closing the healthy store (damaged=%v): %v", damaged, err)
		}
	}
	outcome.failureCount, outcome.firstFailureLine, outcome.stallLines = logger.snapshot()
	outcome.blockReadFailure = strings.Contains(outcome.firstFailureLine, "table block could not be read")
	return outcome
}

// TestBug0381MeasureHelperProcess is not a test on its own: run as a child it
// performs one run (damaged or healthy, from HOARDDB_BUG0381_HELPER) and prints
// the outcome line, then exits without closing a stalled store.
func TestBug0381MeasureHelperProcess(t *testing.T) {
	kind := os.Getenv(bug0381HelperEnv)
	if kind == "" {
		t.Skip("helper process only")
	}
	outcome := bug0381Run(t, kind == "damaged")
	fmt.Println(outcome.outcomeLine())
	os.Exit(0)
}

// bug0381InChild runs one scenario in a child test process and returns its
// outcome.
func bug0381InChild(t *testing.T, kind string) bug0381Outcome {
	t.Helper()
	child := exec.Command(os.Args[0], "-test.run=^TestBug0381MeasureHelperProcess$")
	child.Env = append(os.Environ(), bug0381HelperEnv+"="+kind)
	childOutput, runErr := child.CombinedOutput()
	if runErr != nil {
		t.Fatalf("the %s child failed: %v\n%s", kind, runErr, childOutput)
	}
	for _, line := range strings.Split(string(childOutput), "\n") {
		if strings.HasPrefix(line, "BUG0381OUTCOME ") {
			return parseOutcomeLine(t, line)
		}
	}
	t.Fatalf("the %s child printed no outcome line:\n%s", kind, childOutput)
	return bug0381Outcome{}
}

// TestBug0381MeasureDamagedL0TableStall measures BUG-0381: a level-0 table with
// a damaged data block is kept by a failing compaction, so level 0 cannot
// shrink and writes stall once it reaches NumLevelZeroTablesStall. The control
// run is identical but for the flipped bit, and must not stall.
//
// It is slow and deliberately drives the store into a stall, so it only runs
// when asked for explicitly:
//
//	HOARDDB_BUG0381_MEASURE=1 go test ./storage -run TestBug0381MeasureDamagedL0TableStall -v -timeout 15m
func TestBug0381MeasureDamagedL0TableStall(t *testing.T) {
	if os.Getenv(bug0381MeasureEnv) == "" {
		t.Skip("slow measurement; run it explicitly with " + bug0381MeasureEnv + "=1 and -run " +
			"TestBug0381MeasureDamagedL0TableStall")
	}

	healthy := bug0381InChild(t, "healthy")
	t.Logf("control: %s", healthy.outcomeLine())

	damaged := bug0381InChild(t, "damaged")
	t.Logf("damaged: %s", damaged.outcomeLine())

	if healthy.stalled {
		t.Fatalf("the undamaged control stalled, so the harness is not a fair measurement")
	}
	if !damaged.stalled {
		t.Errorf("the damaged store did not stall: writes kept committing with the damaged table in level 0")
	}
	if damaged.l0AtStall != 6 {
		t.Errorf("the damaged store stalled with %d level-0 tables, want the stall threshold 6", damaged.l0AtStall)
	}
	if damaged.failureCount == 0 {
		t.Errorf("the damaged store stalled with no logged compaction failure")
	}
	if !damaged.blockReadFailure {
		t.Errorf("the damaged store's compaction failures did not report an unreadable table block")
	}
}
