package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// openPositionTestStore opens a node store in dir, closed when the test ends.
func openPositionTestStore(t *testing.T, dir string) storage.KV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(filepath.Join(dir, "store"), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the test node store: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the test node store: %v", err)
		}
	})
	return kv
}

// newPositionTestCluster builds the least a Cluster needs to keep its
// replication positions: a data directory, the node store, optionally the log,
// and the position maps. Two clusters built over one store and one log are a
// node and its restart.
func newPositionTestCluster(t *testing.T, dataDir string, kv storage.KV, wal *storage.WAL) *Cluster {
	t.Helper()
	return &Cluster{
		s: &Server{
			config: &config.Config{Storage: config.StorageConfig{DataDir: dataDir}},
			kv:     kv,
			wal:    wal,
		},
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		appliedFrom:    make(map[string]uint64),
		seededThrough:  make(map[string]seedFloors),
		legacySeeded:   make(map[string]map[string]uint64),
		rebasedAt:      make(map[string]uint64),
		seedCeiling:    make(map[string]uint64),
		recordedStream: make(map[string]recordedStream),
		streamFromMu:   make(map[string]*sync.Mutex),
		checkpointOwed: make(map[string]uint64),
	}
}

// restartPositionTestCluster is the same node started again: the same data
// directory, store and log, and none of the memory.
func restartPositionTestCluster(t *testing.T, previous *Cluster) *Cluster {
	t.Helper()
	return newPositionTestCluster(t, previous.s.config.Storage.DataDir, previous.s.kv, previous.s.wal)
}

// writeCountingKV counts what is written through it: every key set or deleted, the
// bytes of each set, the commits and how many of them were synced.
type writeCountingKV struct {
	storage.KV
	// smallBatchLimit, when set, is the MaxBatchBytes the store reports.
	smallBatchLimit int64
	mu              sync.Mutex
	sets            []writeCountedSet
	deletd          [][]byte
	commit          int
	synced          int
	syncs           int
}

type writeCountedSet struct {
	key       []byte
	valueSize int
}

func (kv *writeCountingKV) NewBatch() storage.KVBatch {
	return &writeCountingBatch{KVBatch: kv.KV.NewBatch(), owner: kv}
}

func (kv *writeCountingKV) MaxBatchBytes() int64 {
	if kv.smallBatchLimit > 0 {
		return kv.smallBatchLimit
	}
	return kv.KV.MaxBatchBytes()
}

func (kv *writeCountingKV) Sync() error {
	kv.mu.Lock()
	kv.syncs++
	kv.mu.Unlock()
	return kv.KV.Sync()
}

// reset forgets what has been counted so far.
func (kv *writeCountingKV) reset() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.sets, kv.deletd, kv.commit, kv.synced, kv.syncs = nil, nil, 0, 0, 0
}

type writeCountingBatch struct {
	storage.KVBatch
	owner   *writeCountingKV
	sets    []writeCountedSet
	deleted [][]byte
}

func (batch *writeCountingBatch) Set(key, value []byte) error {
	batch.sets = append(batch.sets, writeCountedSet{key: append([]byte(nil), key...), valueSize: len(value)})
	return batch.KVBatch.Set(key, value)
}

func (batch *writeCountingBatch) Delete(key []byte) error {
	batch.deleted = append(batch.deleted, append([]byte(nil), key...))
	return batch.KVBatch.Delete(key)
}

func (batch *writeCountingBatch) Commit(sync bool) error {
	if err := batch.KVBatch.Commit(sync); err != nil {
		return err
	}
	batch.owner.mu.Lock()
	defer batch.owner.mu.Unlock()
	batch.owner.sets = append(batch.owner.sets, batch.sets...)
	batch.owner.deletd = append(batch.owner.deletd, batch.deleted...)
	batch.owner.commit++
	if sync {
		batch.owner.synced++
	}
	return nil
}

// legacyStateFixture is the replication_state.bson the starting tree wrote, for
// these positions; server/testdata/replication_state_before_positions.generator.txt
// says how it was made.
const (
	legacyFixtureApplied = "node-a:7000"
	legacyFixtureSeeded  = "node-a:7000"
	legacyFixtureBare    = "node-b:7000"
)

func installLegacyStateFixture(t *testing.T, cluster *Cluster) {
	t.Helper()
	fixture, err := os.ReadFile(filepath.Join("testdata", "replication_state_before_positions.bson"))
	if err != nil {
		t.Fatalf("reading the legacy state fixture: %v", err)
	}
	if err := os.WriteFile(legacyStatePathOf(cluster), fixture, 0o644); err != nil {
		t.Fatalf("installing the legacy state fixture: %v", err)
	}
}

// legacyStatePathOf is the legacy state file's path for a test cluster's data
// directory.
func legacyStatePathOf(cluster *Cluster) string {
	return replicationStatePath(cluster.s.config.Storage.DataDir)
}

// importLegacyStateOf runs the start's legacy-file step for a test cluster's
// data directory, as NewService does for every start.
func importLegacyStateOf(t *testing.T, cluster *Cluster) error {
	t.Helper()
	return importLegacyReplicationState(cluster.s.config.Storage.DataDir, cluster.s.kv, cluster.logger, nil)
}

// requireLegacyFixturePositions checks the three positions the fixture holds
// are in force on c.
func requireLegacyFixturePositions(t *testing.T, cluster *Cluster, when string) {
	t.Helper()
	if cluster.shouldApply(legacyFixtureApplied, 41) || !cluster.shouldApply(legacyFixtureApplied, 42) {
		t.Errorf("%s: the applied position of %s is not 41 (shouldApply(41)=%v, shouldApply(42)=%v)", when, legacyFixtureApplied,
			cluster.shouldApply(legacyFixtureApplied, 41), cluster.shouldApply(legacyFixtureApplied, 42))
	}
	users := func(seq uint64) ReplEntry {
		return ReplEntry{Seq: seq, Op: int32(transport.OpPut), Bucket: "users", Payload: putPayload(t, "db1", "users", "k")}
	}
	if !cluster.seededCovers(legacyFixtureSeeded, users(30)) || cluster.seededCovers(legacyFixtureSeeded, users(31)) {
		t.Errorf("%s: the snapshot position of db1/users from %s is not 30", when, legacyFixtureSeeded)
	}
	for _, database := range []string{"db1", "db2"} {
		logs := func(seq uint64) ReplEntry {
			return ReplEntry{Seq: seq, Op: int32(transport.OpPut), Bucket: "logs", Payload: putPayload(t, database, "logs", "k")}
		}
		if !cluster.seededCovers(legacyFixtureBare, logs(12)) || cluster.seededCovers(legacyFixtureBare, logs(13)) {
			t.Errorf("%s: the bare-name snapshot position of logs from %s is not 12 for %s", when, legacyFixtureBare, database)
		}
	}
}

// TestTheLegacyStateFileIsImportedOnce: a state file written by the previous
// build is read into the store by the first start, in one synced batch, and
// then deleted, not renamed: a sealed node must not keep a plaintext copy of
// its peers' ids and bucket names. The second start finds the positions in the
// store and no file.
func TestTheLegacyStateFileIsImportedOnce(t *testing.T) {
	dataDir := t.TempDir()
	store := &writeCountingKV{KV: openPositionTestStore(t, t.TempDir())}
	first := newPositionTestCluster(t, dataDir, store, nil)
	installLegacyStateFixture(t, first)

	if err := importLegacyStateOf(t, first); err != nil {
		t.Fatalf("first start: %v", err)
	}
	if err := first.loadPositions(); err != nil {
		t.Fatalf("first start: %v", err)
	}
	requireLegacyFixturePositions(t, first, "first start")
	if store.commit != 1 || store.synced != 1 {
		t.Errorf("the import made %d commits, %d synced; want one batch, synced", store.commit, store.synced)
	}
	if _, err := os.Stat(legacyStatePathOf(first)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the legacy state file is still in place after the first start: %v", err)
	}
	if leftovers, err := filepath.Glob(filepath.Join(dataDir, "replication_state.bson*")); err != nil || len(leftovers) != 0 {
		t.Errorf("the data directory keeps a copy of the legacy state after the import: %v (%v)", leftovers, err)
	}

	store.reset()
	second := restartPositionTestCluster(t, first)
	if err := importLegacyStateOf(t, second); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if err := second.loadPositions(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	requireLegacyFixturePositions(t, second, "second start")
	if store.commit != 0 {
		t.Errorf("the second start wrote %d batches; it has nothing to import", store.commit)
	}
}

// TestACrashBetweenTheImportSyncAndTheDeleteImportsNothingTwice is the window
// the build doc does not name: the positions are durable and the file has not
// been deleted. The next start finds the file again. It must neither lose a
// position the store has moved to in the meantime nor count the file's twice.
// A delete that fails stops the start and says what to remove.
func TestACrashBetweenTheImportSyncAndTheDeleteImportsNothingTwice(t *testing.T) {
	dataDir := t.TempDir()
	kv := openPositionTestStore(t, t.TempDir())
	crashed := newPositionTestCluster(t, dataDir, kv, nil)
	installLegacyStateFixture(t, crashed)

	err := importLegacyReplicationState(dataDir, kv, crashed.logger, func(string) error {
		return errors.New("power cut before the delete")
	})
	if err == nil || !strings.Contains(err.Error(), legacyStatePathOf(crashed)) || !strings.Contains(err.Error(), "remove") {
		t.Fatalf("a start that cannot delete the legacy state must stop and name the file to remove, got: %v", err)
	}
	if _, err := os.Stat(legacyStatePathOf(crashed)); err != nil {
		t.Fatalf("the file should still be in place after the simulated crash: %v", err)
	}

	// The node then ran: the applied position moved on, and the snapshot
	// position of db1/users was passed and deleted by the checkpoint.
	crashed.markApplied(legacyFixtureApplied, 90)
	if err := crashed.checkpointPosition(legacyFixtureApplied, true); err != nil {
		t.Fatalf("checkpoint after the crash: %v", err)
	}

	restarted := restartPositionTestCluster(t, crashed)
	if err := importLegacyStateOf(t, restarted); err != nil {
		t.Fatalf("the start after the crash: %v", err)
	}
	if err := restarted.loadPositions(); err != nil {
		t.Fatalf("the start after the crash: %v", err)
	}
	if restarted.shouldApply(legacyFixtureApplied, 90) || !restarted.shouldApply(legacyFixtureApplied, 91) {
		t.Errorf("the applied position of %s is not 90 after the file was imported a second time: the second import lowered it "+
			"(shouldApply(90)=%v, shouldApply(91)=%v)", legacyFixtureApplied,
			restarted.shouldApply(legacyFixtureApplied, 90), restarted.shouldApply(legacyFixtureApplied, 91))
	}
	if _, err := os.Stat(legacyStatePathOf(restarted)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the second start did not delete the file: %v", err)
	}
	if leftovers, err := filepath.Glob(filepath.Join(dataDir, "replication_state.bson*")); err != nil || len(leftovers) != 0 {
		t.Errorf("the data directory keeps a copy of the legacy state: %v (%v)", leftovers, err)
	}
	bare := ReplEntry{Seq: 12, Op: int32(transport.OpPut), Bucket: "logs", Payload: putPayload(t, "db1", "logs", "k")}
	if !restarted.seededCovers(legacyFixtureBare, bare) {
		t.Errorf("the bare-name snapshot position of logs from %s was lost by the second import", legacyFixtureBare)
	}
}

// TestAnUnreadableLegacyStateFileStopsTheStart: a state file that does not
// decode holds positions and snapshot positions nobody can read, so starting
// without them may apply re-sent entries twice. That is the operator's call,
// not the server's: the start stops, names the file, says what is lost and how
// to accept it, and leaves the file where it is. After the operator moves it
// aside, the node starts.
func TestAnUnreadableLegacyStateFileStopsTheStart(t *testing.T) {
	dataDir := t.TempDir()
	kv := openPositionTestStore(t, t.TempDir())
	refused := newPositionTestCluster(t, dataDir, kv, nil)
	statePath := legacyStatePathOf(refused)
	unreadable := []byte("this is not a BSON document")
	if err := os.WriteFile(statePath, unreadable, 0o644); err != nil {
		t.Fatalf("writing the unreadable state file: %v", err)
	}

	err := importLegacyStateOf(t, refused)
	if err == nil {
		t.Fatal("a start over an unreadable legacy state file went ahead")
	}
	movedAside := statePath + ".unreadable"
	for _, want := range []string{
		statePath,
		"snapshot positions",
		"applied twice",
		fmt.Sprintf("mv %s %s", statePath, movedAside),
		"encrypted",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
	if kept, readErr := os.ReadFile(statePath); readErr != nil || string(kept) != string(unreadable) {
		t.Errorf("the refused start did not leave the file as it was: %q, %v", kept, readErr)
	}
	if _, statErr := os.Stat(movedAside); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("the server moved the file aside itself; that is the operator's decision: %v", statErr)
	}

	if err := os.Rename(statePath, movedAside); err != nil {
		t.Fatalf("moving the file aside, as the message says: %v", err)
	}
	accepted := restartPositionTestCluster(t, refused)
	if err := importLegacyStateOf(t, accepted); err != nil {
		t.Fatalf("the start after the file was moved aside: %v", err)
	}
	if err := accepted.loadPositions(); err != nil {
		t.Fatalf("the start after the file was moved aside: %v", err)
	}
	if kept, readErr := os.ReadFile(movedAside); readErr != nil || string(kept) != string(unreadable) {
		t.Errorf("the start touched the moved-aside file: %q, %v", kept, readErr)
	}
}

// TestRecordingASnapshotPositionWritesOneKey states the cost as a count:
// with 10,000 positions already recorded, one more writes one key of under 200
// bytes and no file, where the previous build rewrote a file holding all of them.
func TestRecordingASnapshotPositionWritesOneKey(t *testing.T) {
	dataDir := t.TempDir()
	store := &writeCountingKV{KV: openPositionTestStore(t, t.TempDir())}
	cluster := newPositionTestCluster(t, dataDir, store, nil)
	const source = "192.0.2.1:7000"
	for position := 0; position < 10_000; position++ {
		if err := cluster.recordSeeded(source, "app", fmt.Sprintf("bucket-%05d", position), uint64(position+1)); err != nil {
			t.Fatalf("recording position %d: %v", position, err)
		}
	}

	store.reset()
	if err := cluster.recordSeeded(source, "app", "one-more", 10_001); err != nil {
		t.Fatalf("recording one more position: %v", err)
	}
	if len(store.sets) != 1 || len(store.deletd) != 0 {
		t.Fatalf("one recordSeeded wrote %d keys and deleted %d; want exactly one key written", len(store.sets), len(store.deletd))
	}
	if written := store.sets[0]; len(written.key)+written.valueSize >= 200 {
		t.Errorf("the record is %d bytes (key %d, value %d); want under 200", len(written.key)+written.valueSize, len(written.key), written.valueSize)
	}
	if _, err := os.Stat(legacyStatePathOf(cluster)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("recording a snapshot position created the replication state file: %v", err)
	}
	if got := cluster.seededThroughFor(source, "app", "one-more"); got != 10_001 {
		t.Errorf("the position is not in force in memory: %d", got)
	}
	restarted := restartPositionTestCluster(t, cluster)
	if err := restarted.loadPositions(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := restarted.seededThroughFor(source, "app", "one-more"); got != 10_001 {
		t.Errorf("after a restart the position is %d, want 10001", got)
	}
}

// TestAnImportTooBigForOneBatchIsCommittedInPieces: a state file that held a
// position per bucket ever seeded can be bigger than the store commits
// atomically. The pieces are each a raise, so they may land in any number and
// the last is the synced one.
func TestAnImportTooBigForOneBatchIsCommittedInPieces(t *testing.T) {
	const source = "192.0.2.1:7000"
	seeded := make(map[string]uint64)
	for position := 0; position < 500; position++ {
		seeded[seededStateKey("app", fmt.Sprintf("bucket-%03d", position))] = uint64(position + 1)
	}
	stateBytes, err := bson.Marshal(replicationState{AppliedFrom: map[string]uint64{source: 7}, SeededThrough: map[string]map[string]uint64{source: seeded}})
	if err != nil {
		t.Fatalf("marshaling the state file: %v", err)
	}
	store := &writeCountingKV{KV: openPositionTestStore(t, t.TempDir()), smallBatchLimit: 4096}
	cluster := newPositionTestCluster(t, t.TempDir(), store, nil)
	if err := os.WriteFile(legacyStatePathOf(cluster), stateBytes, 0o644); err != nil {
		t.Fatalf("writing the state file: %v", err)
	}
	if err := importLegacyStateOf(t, cluster); err != nil {
		t.Fatalf("importing: %v", err)
	}
	if err := cluster.loadPositions(); err != nil {
		t.Fatalf("loading: %v", err)
	}
	if store.commit < 2 || store.synced != 1 {
		t.Errorf("the import made %d commits, %d synced; want several pieces and exactly the last one synced", store.commit, store.synced)
	}
	for position := 0; position < 500; position++ {
		bucket := fmt.Sprintf("bucket-%03d", position)
		if got := cluster.seededThroughFor(source, "app", bucket); got != uint64(position+1) {
			t.Fatalf("snapshot position of app/%s = %d, want %d", bucket, got, position+1)
		}
	}
}
