package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Seeding slice S0: one record shape on a cluster node, the admission seams, the active-node
// reconcile driver and the interim record (the entry ring seeding completion spec §0).

// s0Errors collects the background errors a cluster node reports.
type s0Errors struct {
	mu   sync.Mutex
	seen []string
}

func (e *s0Errors) report(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen = append(e.seen, err.Error())
}

func (e *s0Errors) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.seen...)
}

// s0OpenClusterNode opens a one-member cluster node (n.cl != nil, bootstrapped) on dir.
func s0OpenClusterNode(t *testing.T, dir string, errs *s0Errors) *Node {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opts := &ClusterOptions{Listener: listener, InsecureTestOnly: true, RaftLogger: hclog.NewNullLogger()}
	if errs != nil {
		opts.OnError = errs.report
	}
	node, err := OpenNode(ctx, vouchedTestConfig(t, NodeConfig{Dir: dir, NodeID: "s0", Cluster: opts}))
	if err != nil {
		t.Fatalf("opening the cluster node in %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = node.Close() })
	return node
}

// s0Schema creates database app (RF 1) with bucket User { Name string } on a cluster node and
// waits for the node to apply the committed state.
func s0Schema(t *testing.T, node *Node) uint64 {
	t.Helper()
	ctx := context.Background()
	dbID, err := node.CreateDatabaseRF(ctx, u30iDB, 1)
	if err != nil {
		t.Fatalf("CreateDatabaseRF: %v", err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return dbID
}

// s0Insert inserts a User document, retrying the one refusal a write racing the first
// reconcile pass of a new database can meet: the pass has the pair's record staged in a batch
// not yet committed and the advancer answers ErrPairBusy, "retry in a moment" (S0 build
// status, finding 2).
func s0Insert(t *testing.T, node *Node, id string) Inserted {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		inserted, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}})
		if err == nil {
			return inserted
		}
		if !errors.Is(err, ErrPairBusy) || time.Now().After(deadline) {
			t.Fatalf("Insert %s: %v", id, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// s0InsertRetrying inserts through a node that may still hold the pair after a restart (a
// one-member ring answers "held" once while its first catch-up step runs).
func s0InsertRetrying(t *testing.T, node *Node, id string) Inserted {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		inserted, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}})
		if err == nil {
			return inserted
		}
		if time.Now().After(deadline) {
			t.Fatalf("Insert %s was still refused after the restart: %v", id, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// s0SlotOf is the slot of a User document in database app.
func s0SlotOf(t *testing.T, id string) uint16 {
	t.Helper()
	slot, err := PlacementOf(u30iDB, "User", id)
	if err != nil {
		t.Fatal(err)
	}
	return slot
}

// s0AwaitFirstPass waits until the node's driver has completed a reconcile pass at the
// committed state_seq (seedParts.lastSeq), then checks that the pass wrote the interim for
// an idle pair of the database.
func s0AwaitFirstPass(t *testing.T, node *Node, dbID uint64) {
	t.Helper()
	parts := node.cl.seeding.Load()
	s0Await(t, "a reconcile pass at the committed state_seq", 15*time.Second, func() bool {
		seq := uint64(node.raft.StateSeq())
		parts.mu.Lock()
		defer parts.mu.Unlock()
		return seq != 0 && parts.started && parts.lastSeq >= seq
	})
	if state := node.SlotRecordState(s0IdleSlot, dbID); state != machine.SlotReady {
		t.Fatalf("after the first pass an idle in-target pair is %v, want ready (the interim)", state)
	}
}

// s0HoldPasses keeps the node's reconcile passes from running until the returned release is
// called, so a test can reach a pair before the first pass writes its interim. The writer's
// advancer takes the pair lock and the reconciler's mu, never the pass lock.
func s0HoldPasses(node *Node) (release func()) {
	node.cl.seeding.Load().recon.pass.Lock()
	return node.cl.seeding.Load().recon.pass.Unlock
}

// s0IdleSlot is a slot no test document is placed in, so its record can only be the interim.
const s0IdleSlot uint16 = 8191

// s0Await polls cond until it holds or the deadline passes.
func s0Await(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("gave up after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// s0Badger opens the closed node's Badger directly, for a test that edits records between boots.
func s0Badger(t *testing.T, dir string) *storage.BadgerKV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(filepath.Join(dir, "badger"), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening Badger in %s: %v", dir, err)
	}
	return kv
}

// putPositionRecord writes the 32-byte position record LocalAdvancer leaves behind.
func putPositionRecord(t *testing.T, kv storage.KV, slot uint16, db uint64, applied cluster.Position, cursor, ordinal uint64) {
	t.Helper()
	var record [localPairRecordLen]byte
	binary.BigEndian.PutUint64(record[0:], applied.Epoch)
	binary.BigEndian.PutUint64(record[8:], applied.Seq)
	binary.BigEndian.PutUint64(record[16:], cursor)
	binary.BigEndian.PutUint64(record[24:], ordinal)
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(LocalPairRecordKey(slot, db), record[:]); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// rawRecord reads the bytes under the pair's record key.
func rawRecord(t *testing.T, kv storage.KV, slot uint16, db uint64) ([]byte, bool) {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	raw, err := snapshot.Get(LocalPairRecordKey(slot, db))
	if err != nil {
		return nil, false
	}
	return raw, true
}

// --- step 1: the loader rewrite, its guard, the tail-scan start and recoverPair's cursor ---

// TestAThirtyTwoByteRecordIsRewrittenAsReadyAtBoot (plan §0.2): on a cluster node the boot
// loader rewrites the 32-byte position record as a ready seed record at the same position,
// synced, before the tail scan. [BREAK: skip the rewrite]
func TestAThirtyTwoByteRecordIsRewrittenAsReadyAtBoot(t *testing.T) {
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	applied := cluster.Position{Epoch: 3, Seq: 9}
	putPositionRecord(t, kv, 17, reconDB, applied, 5, 4)
	pairs := &cluster.PairSet{}
	if _, err := LoadLocalPairRecords(kv, pairs, true); err != nil {
		t.Fatal(err)
	}
	record, found := diskRecord(t, kv, 17)
	if !found {
		t.Fatal("the 32-byte record was not rewritten as a seed record")
	}
	if record.State != machine.SeedReady || record.Applied != applied || record.SeedID != [16]byte{} || record.Source != "" || record.Phase != machine.PhaseNone {
		t.Fatalf("the rewritten record is %+v, want ready at %s with a zero seed id and no source", record, applied)
	}
	if pair := pairs.Snapshot(17, reconDB); pair.AppliedPos != applied || pair.Ordinal != 4 || pair.TailCursor != 5 {
		t.Fatalf("the pair set after the rewrite is %+v, want applied %s, ordinal 4, cursor 5", pair, applied)
	}
}

// TestTheRewrittenRecordKeepsCursorAndOrdinal (the T3 review's duplicate-apply window): the
// rewritten record carries the WAL cursor and the ordinal, so a Reconciler that loads it at the
// next boot installs the same ordinal and the dedupe window does not regress. [BREAK: zero the
// ordinal]
func TestTheRewrittenRecordKeepsCursorAndOrdinal(t *testing.T) {
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	putPositionRecord(t, kv, 17, reconDB, cluster.Position{Epoch: 3, Seq: 9}, 5, 42)
	if _, err := LoadLocalPairRecords(kv, &cluster.PairSet{}, true); err != nil {
		t.Fatal(err)
	}
	record, _ := diskRecord(t, kv, 17)
	if record.WALCursor != 5 || record.Ordinal != 42 {
		t.Fatalf("the rewritten record has cursor %d and ordinal %d, want 5 and 42", record.WALCursor, record.Ordinal)
	}
	st := threeNodes(t)
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	if pair := g.pairs.Snapshot(17, reconDB); pair.Ordinal != 42 || pair.TailCursor != 5 {
		t.Fatalf("the Reconciler installed %+v from the rewritten record, want ordinal 42 and cursor 5", pair)
	}
}

// TestASingleNodeKeepsItsThirtyTwoByteRecordsAcrossARestart: a single node has no
// Reconciler to read a seed record, so its loader keeps the 32-byte shape; rewritten, the
// record would be unread at the next boot. [BREAK: rewrite unconditionally]
func TestASingleNodeKeepsItsThirtyTwoByteRecordsAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	node, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	u30iSchema(t, node)
	inserted, err := node.Insert(context.Background(), u30iDB, "User", u30iUser("u1", "Ada", 1))
	if err != nil {
		t.Fatal(err)
	}
	slot := node.slotOf(u30iDB, "User", "u1")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	for boot := 1; boot <= 2; boot++ {
		reopened := u30iOpen(t, dir)
		raw, found := rawRecord(t, reopened.kv, slot, 1)
		if !found || len(raw) != localPairRecordLen {
			t.Fatalf("boot %d: the single node's pair record is %d bytes (found %v), want the 32-byte position record", boot, len(raw), found)
		}
		if pair := reopened.pairs.Snapshot(slot, 1); pair.AppliedPos != inserted.Pos || pair.Ordinal != 1 {
			t.Fatalf("boot %d: the pair is %+v, want applied %s ordinal 1", boot, pair, inserted.Pos)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestTheTailScanStartsAtTheLowestCursorOfTheSeedRecords (pi BL-4b, N4): the loader folds the
// lowest non-zero WAL cursor of every ready seed record into the tail-scan start, so a cluster
// node whose records were all rewritten does not replay from segment one at every boot. A
// seeding record's cursor is the pull's and a retired record is not folded. [BREAK: return 1]
func TestTheTailScanStartsAtTheLowestCursorOfTheSeedRecords(t *testing.T) {
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	ready := func(cursor uint64) SeedRecord {
		rec := readyRecord(cluster.Position{Epoch: 1, Seq: 4})
		rec.WALCursor = cursor
		return rec
	}
	putRecord(t, kv, 10, ready(12))
	putRecord(t, kv, 11, ready(7))
	putRecord(t, kv, 12, ready(0)) // seeded and never written: no cursor to fold
	seeding := seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: 1, Seq: 2}, "k")
	seeding.WALCursor = 3
	putRecord(t, kv, 13, seeding)
	retired := retiredRecord()
	retired.WALCursor = 2
	putRecord(t, kv, 14, retired)
	from, err := LoadLocalPairRecords(kv, &cluster.PairSet{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if from != 7 {
		t.Fatalf("the tail scan starts at %d, want 7: the lowest cursor of the ready records", from)
	}
}

// TestRecoverPairStartsAtTheSeedRecordsCursor (pi SF3): after a failed apply recoverPair replays
// from the pair's record; on a cluster node that record is the seed record, read through the
// Reconciler, not the 32-byte record it no longer has. [BREAK: return 1]
func TestRecoverPairStartsAtTheSeedRecordsCursor(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := s0Schema(t, node)
	var last Inserted
	for i := range 3 {
		last = s0Insert(t, node, fmt.Sprintf("u%d", i))
	}
	slot := s0SlotOf(t, "u2")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := s0OpenClusterNode(t, dir, nil)
	record, found := reopened.cl.seeding.Load().recon.Record(slot, dbID)
	if !found || record.Applied != last.Pos || record.WALCursor <= 1 {
		t.Fatalf("after the restart the pair's record is %+v (found %v), want a seed record at %s with a cursor past 1", record, found, last.Pos)
	}
	from, err := reopened.recoverFrom(slot, dbID)
	if err != nil {
		t.Fatal(err)
	}
	if from != record.WALCursor {
		t.Fatalf("recoverPair would replay from %d, want the seed record's cursor %d", from, record.WALCursor)
	}
}

// TestTheRewriteOfThousandsOfPairsIsOneSyncedBatchAndTheSecondBootStartsAtTheirCursor measures
// the loader at the per-node pair count of N=5, RF=2 (3,277 pairs, plan §6): the first boot
// rewrites every 32-byte record in one synced batch, the second reads seed records only and
// returns their lowest cursor, not 1. The timings are logged for the build status. [BREAK:
// fold a ready seed record's cursor only when it is below the 32-byte records' lowest, so
// the second boot, which reads seed records alone, answers 1]
func TestTheRewriteOfThousandsOfPairsIsOneSyncedBatchAndTheSecondBootStartsAtTheirCursor(t *testing.T) {
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	const held = 3277
	batch := kv.NewBatch()
	for i := range held {
		var record [localPairRecordLen]byte
		binary.BigEndian.PutUint64(record[0:], 2)
		binary.BigEndian.PutUint64(record[8:], uint64(i+1))
		binary.BigEndian.PutUint64(record[16:], uint64(i+100)) // the lowest cursor is 100
		binary.BigEndian.PutUint64(record[24:], uint64(i+1))
		if err := batch.Set(LocalPairRecordKey(uint16(i), reconDB), record[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Discard()
	started := time.Now()
	first, err := LoadLocalPairRecords(kv, &cluster.PairSet{}, true)
	if err != nil {
		t.Fatal(err)
	}
	rewrite := time.Since(started)
	started = time.Now()
	second, err := LoadLocalPairRecords(kv, &cluster.PairSet{}, true)
	if err != nil {
		t.Fatal(err)
	}
	reread := time.Since(started)
	t.Logf("%d pairs: the first boot (the rewrite, one synced batch) took %s, the second boot (seed records only) %s", held, rewrite, reread)
	if first != 100 || second != 100 {
		t.Fatalf("the tail-scan start is %d at the first boot and %d at the second, want 100 at both", first, second)
	}
	if raw, found := rawRecord(t, kv, held-1, reconDB); !found || len(raw) == localPairRecordLen {
		t.Fatalf("the last pair's record is %d bytes after the rewrite, want a seed record", len(raw))
	}
}

// --- step 2: every pair advances through the reconciler; the first write creates the record ---

// batchWatch wraps a store so a test can see which keys each committed batch carried.
type batchWatch struct {
	storage.KV
	mu        sync.Mutex
	committed [][]string
}

func (w *batchWatch) NewBatch() storage.KVBatch {
	return &watchedBatch{KVBatch: w.KV.NewBatch(), kv: w}
}

type watchedBatch struct {
	storage.KVBatch
	kv   *batchWatch
	keys []string
}

func (b *watchedBatch) Set(key, value []byte) error {
	b.keys = append(b.keys, string(key))
	return b.KVBatch.Set(key, value)
}

func (b *watchedBatch) Commit(sync bool) error {
	err := b.KVBatch.Commit(sync)
	if err == nil {
		b.kv.mu.Lock()
		b.kv.committed = append(b.kv.committed, append([]string(nil), b.keys...))
		b.kv.mu.Unlock()
	}
	return err
}

// s0Advancer is the cluster node's advancer over a rig's reconciler.
func s0Advancer(g *rig) seedAwareAdvancer {
	return seedAwareAdvancer{recon: g.rec}
}

// s0ReconcilerRig is a reconciler for n2 of threeNodes with its records loaded and no pass run.
func s0ReconcilerRig(t *testing.T, store Store) (*rig, cluster.ClusterState, slotsOf) {
	t.Helper()
	st := threeNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	if store == nil {
		store = kv
	}
	g := newRigOn(t, kv, store, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	return g, st, findSlots(t, st)
}

// seedStateRecords lists every value under the seed-state namespace.
func seedStateRecords(t *testing.T, kv storage.KV) [][]byte {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSSeedState)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyRangeEnd(lower)})
	defer iter.Close()
	var values [][]byte
	for iter.Seek(lower); iter.Valid(); iter.Next() {
		raw, err := iter.Value()
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, raw)
	}
	return values
}

// forgetPair removes a pair's record and every key of its range from a closed node's Badger,
// which is the store of a node whose first write's batch never committed, or that dropped the
// range: the log still holds the pair's records.
func forgetPair(t *testing.T, dir string, slot uint16, db uint64) {
	t.Helper()
	kv := s0Badger(t, dir)
	defer func() { _ = kv.Close() }()
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(LocalPairRecordKey(slot, db)); err != nil {
		t.Fatal(err)
	}
	snapshot := kv.NewSnapshot()
	prefix := storage.PairPrefix(slot, db)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		if err := batch.Delete(append([]byte(nil), iter.Key()...)); err != nil {
			t.Fatal(err)
		}
	}
	iter.Close()
	snapshot.Discard()
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// s0IDsInOneSlot finds count User ids that share a slot, so one pair takes several positions.
func s0IDsInOneSlot(t *testing.T, count int) (ids []string, slot uint16) {
	t.Helper()
	ids = []string{"pair-0"}
	slot = s0SlotOf(t, ids[0])
	for i := 1; i < 1_000_000 && len(ids) < count; i++ {
		if candidate := fmt.Sprintf("pair-%d", i); s0SlotOf(t, candidate) == slot {
			ids = append(ids, candidate)
		}
	}
	if len(ids) < count {
		t.Fatalf("found %d ids in slot %d, want %d", len(ids), slot, count)
	}
	return ids, slot
}

// TestEveryPairHasOneRecordShape (plan §0.2): on a cluster node every pair the write path
// touches has a seed record; the 32-byte position record is gone from the cluster path.
// [BREAK: keep LocalAdvancer for a pair with no record]
func TestEveryPairHasOneRecordShape(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	s0Schema(t, node)
	s0Insert(t, node, "u1")
	s0Insert(t, node, "u2")
	records := seedStateRecords(t, node.kv)
	if len(records) < 2 {
		t.Fatalf("%d records after two writes, want at least one per pair", len(records))
	}
	for _, raw := range records {
		if len(raw) == localPairRecordLen {
			t.Fatalf("a cluster node wrote a %d-byte position record; every pair must have a seed record", len(raw))
		}
		if _, err := DecodeSeedRecord(raw); err != nil {
			t.Fatalf("a record on a cluster node is not a seed record: %v", err)
		}
	}
}

// TestAnAbsentInTargetPairsFirstWriteCreatesItsRecord (plan §0.2, entered through
// Writer.Write so the admission seam is on the path): the first write to a pair with no
// record, in the target set, creates its ready record at the write's position.
// [BREAK: keep ErrPairNotReady for a pair with no record]
func TestAnAbsentInTargetPairsFirstWriteCreatesItsRecord(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node) // no pass writes the interim first
	defer release()
	dbID := s0Schema(t, node)
	slot := s0SlotOf(t, "u1")
	if _, found := node.cl.seeding.Load().recon.Record(slot, dbID); found {
		t.Fatal("the pair has a record before its first write; the test needs an absent pair")
	}
	inserted := s0Insert(t, node, "u1")
	record, found := node.cl.seeding.Load().recon.Record(slot, dbID)
	if !found || record.State != machine.SeedReady || record.Applied != inserted.Pos || record.Ordinal != 1 || record.WALCursor == 0 {
		t.Fatalf("after the first write the pair's record is %+v (found %v), want ready at %s, ordinal 1, with the write's cursor", record, found, inserted.Pos)
	}
	if record.SeedID != [16]byte{} || record.Source != "" {
		t.Fatalf("the created record is %+v, want a zero seed id and no source", record)
	}
	if pair := node.pairs.Snapshot(slot, dbID); pair.AppliedPos != inserted.Pos || pair.Ordinal != 1 {
		t.Fatalf("the pair set is %+v, want the write installed", pair)
	}
}

// TestAFirstWriteCreatesAReadyRecordInItsOwnBatch: the created record rides the batch that
// carries the write's data, so a crash leaves both or neither. [BREAK: write the record in a
// second batch]
func TestAFirstWriteCreatesAReadyRecordInItsOwnBatch(t *testing.T) {
	watch := &batchWatch{}
	g, st, slots := s0ReconcilerRig(t, nil)
	watch.KV = g.kv
	g.rec.cfg.Store = watch
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(g.kv, slots.owner, 8, uint64(st.Epoch))); err != nil {
		t.Fatal(err)
	}
	dataKey := string(storage.Key(slots.owner, reconDB, 5, storage.NSDocuments, []byte("d1")))
	recordKey := string(SeedRecordKey(slots.owner, reconDB))
	watch.mu.Lock()
	defer watch.mu.Unlock()
	for _, keys := range watch.committed {
		hasData, hasRecord := false, false
		for _, key := range keys {
			hasData, hasRecord = hasData || key == dataKey, hasRecord || key == recordKey
		}
		if hasData && hasRecord {
			return
		}
		if hasData != hasRecord {
			t.Fatalf("a batch carried the data (%v) without the record (%v): keys %q", hasData, hasRecord, keys)
		}
	}
	t.Fatalf("no committed batch carried the data and the record together; batches: %v", watch.committed)
}

// TestAnAbsentPairOutsideTheTargetSetIsStillRefused (the write side): a live write to a pair
// with no record that is not in the target set is refused ErrPairNotReady and stages nothing;
// the same write to an in-target pair creates its record. [BREAK: create for every pair with
// no record]
func TestAnAbsentPairOutsideTheTargetSetIsStillRefused(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	e := uint64(st.Epoch)
	staged := false
	outside := advanceTo(g.kv, slots.outside, 8, e)
	outside.Stage = func(storage.KVBatch, *cluster.Pair) (Advance, error) { staged = true; return Advance{}, nil }
	if err := g.rec.ApplyAdvance(slots.outside, reconDB, outside); !errors.Is(err, ErrPairNotReady) || staged {
		t.Fatalf("a write to a record-less pair outside the target set: err %v, staged %v; want ErrPairNotReady before staging", err, staged)
	}
	if _, found := diskRecord(t, g.kv, slots.outside); found {
		t.Fatal("the refused write left a record behind")
	}
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(g.kv, slots.owner, 8, e)); err != nil {
		t.Fatalf("a write to a record-less pair in the target set: %v", err)
	}
	if record, found := diskRecord(t, g.kv, slots.owner); !found || record.State != machine.SeedReady || record.Applied.Seq != 8 || record.Ordinal != 23 || record.WALCursor != 99 {
		t.Fatalf("the created record is %+v (found %v), want ready at seq 8, cursor 99, ordinal 23", record, found)
	}
}

// TestABootReplayOfAForgottenPairsLogDoesNotRefuse (plan §0.2, pi BLOCKER-1): a node that
// removed a pair's record while its log still holds the pair's records must reopen, and the
// replay must not resurrect the range. A replay of a record-less pair is a no-op whatever the
// state says: at the advancer, for a pair outside the target set (first half); and through
// Node.open on a cluster node whose log holds a record of a pair no target set names, where
// after the restart and a reconcile pass the pair holds nothing and has no record (second
// half). [BREAK 1: return ErrPairNotReady on the replay path (the node cannot reopen);
// BREAK 2: create on the replay path (the data is back)]
func TestABootReplayOfAForgottenPairsLogDoesNotRefuse(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	staged := false
	err := s0Advancer(g).Advance(slots.outside, reconDB, AdvanceStep{
		Replay: true,
		Stage: func(storage.KVBatch, *cluster.Pair) (AdvanceTo, error) {
			staged = true
			return AdvanceTo{Applied: cluster.Position{Epoch: uint64(st.Epoch), Seq: 1}}, nil
		},
		Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(false) },
	})
	if !errors.Is(err, errNothingStaged) || staged {
		t.Fatalf("the replay of a record-less pair outside the target set: err %v, staged %v; want errNothingStaged with nothing staged", err, staged)
	}
	if _, found := diskRecord(t, g.kv, slots.outside); found {
		t.Fatal("the replay created a record")
	}

	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := s0Schema(t, node)
	s0Insert(t, node, "u1") // the database's own pair stays whole
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	// A record of a pair of database 99, which no committed state holds, so no target set
	// names it: the one forgotten pair a one-member ring can have.
	const forgotten uint64 = 99
	wal, err := storage.OpenWALWithOptions(filepath.Join(dir, "wal"), nil, storage.WALOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pos := cluster.Position{Epoch: 1, Seq: 1}
	body, err := cluster.EncodeOp(u32Insert(t, "lost", cluster.Position{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.AppendFrom("", cluster.OpInsert, storage.PositionKey(u32Slot, forgotten, storage.LogPosition(pos)), storage.EncodeWALRecord(0, [16]byte{9}, body), storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := s0OpenClusterNode(t, dir, nil)
	s0AwaitFirstPass(t, reopened, dbID)
	if state := reopened.SlotRecordState(u32Slot, forgotten); state != machine.SlotAbsent {
		t.Fatalf("after the restart and a pass the forgotten pair's record is %v, want none", state)
	}
	if state := reopened.SlotRecordState(s0SlotOf(t, "u1"), dbID); state != machine.SlotReady {
		t.Fatalf("the database's own pair came back %v, want ready", state)
	}
	if keys := t3Dump(t, reopened.kv, u32Slot, forgotten); len(keys) != 0 {
		t.Fatalf("the replay resurrected %d keys of the forgotten pair", len(keys))
	}
}

// TestABootWithNoSnapshotCreatesNoRecordOnReplay (decisions §16 item 61): at tail.Scan the
// committed state is not applied yet (StateSeq 0 without a snapshot), so a replay must never
// create a record from it: an in-target record-less pair whose log holds records is skipped,
// the first pass writes its interim, and the store lacks the logged positions. forgetPair
// stands for a crash between the first write's log fsync and its batch commit: the batch that
// creates a pair's record commits synced and is answered only after (BUG-0809), so a pair with
// no record at boot never had a write answered as applied. The answers s0Insert got below are the test's
// setup, not what such a crash answers. [BREAK: create on an unknown state]
func TestABootWithNoSnapshotCreatesNoRecordOnReplay(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := s0Schema(t, node)
	ids, slot := s0IDsInOneSlot(t, 2)
	s0Insert(t, node, ids[0])
	s0Insert(t, node, ids[1])
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	forgetPair(t, dir, slot, dbID) // the first write's batch never committed (a crash before it): the log alone holds the pair
	var atScan int64 = -1
	beforeTailScan = func(n *Node) { atScan = n.raft.StateSeq() }
	t.Cleanup(func() { beforeTailScan = nil })
	reopened := s0OpenClusterNode(t, dir, nil)
	t.Logf("StateSeq at tail.Scan on the restart: %d", atScan)
	if atScan != 0 {
		t.Logf("the committed state was already applied at the scan; the rule holds whatever the state says")
	}
	s0AwaitFirstPass(t, reopened, dbID)
	s0Await(t, "the interim of the forgotten pair", 10*time.Second, func() bool {
		_, found := reopened.cl.seeding.Load().recon.Record(slot, dbID)
		return found
	})
	record, found := reopened.cl.seeding.Load().recon.Record(slot, dbID)
	if found && !record.Applied.Zero() {
		t.Fatalf("after the restart the pair's record is %+v: the replay created it at a log position", record)
	}
	if !found || record.State != machine.SeedReady {
		t.Fatalf("after the first pass the pair's record is %+v (found %v), want the interim: ready at 0:0", record, found)
	}
	for _, id := range ids {
		if _, err := reopened.Get(context.Background(), u30iDB, "User", id); err == nil {
			t.Fatalf("%s is readable after the restart: the replay re-applied an unacknowledged position", id)
		}
	}
}

// s0ReplayRecordlessPair writes two positions of one pair on a cluster node, forgets the
// pair's record and range, and reopens the node: the replay skips the pair. It returns the
// reopened node, the pair's slot, the ids of the slot (the third unused) and the last position.
func s0ReplayRecordlessPair(t *testing.T) (reopened *Node, dbID uint64, slot uint16, ids []string, last Inserted) {
	t.Helper()
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID = s0Schema(t, node)
	ids, slot = s0IDsInOneSlot(t, 3)
	s0Insert(t, node, ids[0])
	last = s0Insert(t, node, ids[1])
	epoch := node.State().Epoch
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	forgetPair(t, dir, slot, dbID)
	reopened = s0OpenClusterNode(t, dir, nil)
	s0Await(t, "the committed state", 10*time.Second, func() bool { return reopened.State().StateSeq != 0 })
	if reopened.State().Epoch != epoch {
		t.Fatalf("the epoch moved from %d to %d across the restart; the test needs the log's epoch to stay committed", epoch, reopened.State().Epoch)
	}
	return reopened, dbID, slot, ids, last
}

// TestAReplayOfARecordlessPairStillLearnsAppendedSeq (pi SF-1 of the re-check): the replay
// skips a record-less pair's records but still raises its AppendedSeq to the log's highest
// seq of the committed epoch, through Writer.Replay's own pair acquisition. [BREAK: raise
// AppendedSeq inside the Stage only]
func TestAReplayOfARecordlessPairStillLearnsAppendedSeq(t *testing.T) {
	reopened, dbID, slot, _, last := s0ReplayRecordlessPair(t)
	pair := reopened.pairs.Snapshot(slot, dbID)
	if pair.AppendedSeq != last.Pos.Seq {
		t.Fatalf("after the replay the pair's AppendedSeq is %d, want the log's highest %d", pair.AppendedSeq, last.Pos.Seq)
	}
	if !pair.AppliedPos.Zero() {
		t.Fatalf("after the replay the pair's applied position is %s, want 0:0: the skipped records were not applied", pair.AppliedPos)
	}
}

// TestTheNextOrderAfterASkippedReplayTakesThePositionAfterTheLog (S0 step 6, finding 1): the
// replay of a record-less pair raises AppendedSeq for the committed epoch and the next order
// must take the position after the log's highest, never one the log holds. Before the
// append-epoch marker, writepath.go:appendedAt answered 0 whenever AppliedPos.Epoch was
// below the committed epoch, which a record-less pair at 0:0 always is, and the next order
// took 1:1 while the log held 1:1 and 1:2. [BREAK: appendedAt answers 0 when
// AppliedPos.Epoch < committed, ignoring the marker]
func TestTheNextOrderAfterASkippedReplayTakesThePositionAfterTheLog(t *testing.T) {
	reopened, _, _, ids, last := s0ReplayRecordlessPair(t)
	third := s0InsertRetrying(t, reopened, ids[2])
	if third.Pos.Epoch != last.Pos.Epoch || third.Pos.Seq != last.Pos.Seq+1 {
		t.Fatalf("the next order took %s, want %d:%d, the position after the log's highest", third.Pos, last.Pos.Epoch, last.Pos.Seq+1)
	}
}

// --- step 3: the admission seams (the owner's O3; plan §0.2) ---

// s0Slots is a recordSlots over a reconciler with no records, on a node that has observed
// the state and is not joining.
func s0Slots(t *testing.T) (recordSlots, *rig) {
	t.Helper()
	g, _, _ := s0ReconcilerRig(t, nil)
	parts := &seedParts{recon: g.rec}
	parts.observed.Store(true)
	return recordSlots{parts: parts}, g
}

// TestAFirstWriteToAnAbsentInTargetPairIsAdmitted: on a non-joining node that has observed the
// state, the Writer's NotReady seam admits a write to a pair with no record, so the write
// reaches the advancer's create branch instead of being answered UNAVAILABLE (pi BL-A).
// [BREAK: notReady calls State again]
func TestAFirstWriteToAnAbsentInTargetPairIsAdmitted(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node) // no pass writes the interim first
	defer release()
	dbID := s0Schema(t, node)
	slot := s0SlotOf(t, "u1")
	if _, found := node.cl.seeding.Load().recon.Record(slot, dbID); found {
		t.Fatal("the pair has a record before its first write; the test needs an absent pair")
	}
	if why := node.writer.cfg.NotReady(slot, dbID); why != "" {
		t.Fatalf("the Writer's NotReady seam answers %q for a record-less pair on an active node, want admission", why)
	}
	if !(recordSlots{parts: node.cl.seeding.Load()}).Admits(slot, dbID) {
		t.Fatal("Admits is false for a record-less pair on a node that is not joining and has observed the state")
	}
	if _, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: "u1"}, {Key: "Name", Value: "Ada"}}); err != nil {
		t.Fatalf("the first write to a record-less pair through Writer.Write: %v", err)
	}
}

// TestAReplicatedWriteToARecordlessCopyIsApplied (the reconciler's R1): the replicator's Ready
// admits a pushed record for a pair with no record on an active copy, so the copy applies it
// instead of answering AckNotApplied until the first pass. [BREAK: replicaReady calls State
// again]
func TestAReplicatedWriteToARecordlessCopyIsApplied(t *testing.T) {
	view, _ := s0Slots(t)
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Ready = replicaReady(view) })
	first := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{}))
	if first.Ack.Outcome != AckApplied || first.Ack.Pos != u32Pos(7, 1) {
		t.Fatalf("the first record pushed to a record-less copy: ack %+v, want applied at 7:1", first.Ack)
	}
	if !copyNode.hasDocument(u32Pos(7, 1)) {
		t.Fatal("the copy answered applied without the document")
	}
}

// TestAPushedRecordDuringALeftSeedsDeleteIsNotApplied (BUG-0817): the replicator half of the
// window. The range of a seeding pair that left the copy set is being deleted, the copy set
// names the node again, and the owner pushes the pair's first record. The replicator runs on
// the node's own store, pair set and advancer, so an admitted record is really applied into
// the range under the delete. It must be answered AckNotApplied, which the owner does not
// count, and leave nothing behind. [guard: remove the record before the range is deleted]
func TestAPushedRecordDuringALeftSeedsDeleteIsNotApplied(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	parts := newSeedPartsFor(l.g.rec, l.back, testSelf)
	view := recordSlots{parts: parts}
	copyNode := newU32Node(t, testSelf, func(cfg *ReplicatorConfig) {
		cfg.KV, cfg.Pairs, cfg.Applier = l.kv, l.g.pairs, NewDocumentApplier(l.kv, nil)
		cfg.Ready, cfg.Advancer = replicaReady(view), s0Advancer(l.g)
	})
	pushed := u32Frame(t, u32Epoch, u32Pos(u32Epoch, 1), cluster.Position{})
	pushed.Slot, pushed.DB = l.slot, reconDB

	var reply WriteResult
	var pushErr error
	hooked := &snapshotHookKV{BadgerKV: l.kv}
	hooked.afterSnapshot = func() {
		l.g.state = l.back
		reply, pushErr = copyNode.rep.OnWrite(pushed)
	}
	puller := &Puller{cfg: PullerConfig{KV: hooked, Budget: newReclaimBudget(reclaimBudgetConfig{})}}
	if err := dropLeftSeed(t.Context(), l.g.rec, l.key, puller.resetRange); err != nil {
		t.Fatalf("dropLeftSeed: %v", err)
	}
	if hooked.afterSnapshot != nil {
		t.Fatal("the range delete never took its snapshot, so the window was not opened")
	}

	if pushErr != nil {
		t.Fatalf("OnWrite during the delete: %v", pushErr)
	}
	if reply.Ack.Outcome != AckNotApplied {
		t.Errorf("a record pushed while the pair's range was being deleted was answered %+v, want AckNotApplied", reply.Ack)
	}
	if applied := l.g.pairs.Snapshot(l.slot, reconDB).AppliedPos; !applied.Zero() {
		t.Errorf("the pair's applied position moved to %s during the delete", applied)
	}
	if rec, ok := l.g.rec.Record(l.slot, reconDB); ok && rec.State == machine.SeedReady {
		t.Errorf("the pushed record left a ready record at %s over the range under the delete", rec.Applied)
	}
	if held := rangeKeys(t, l.kv, l.slot); held != 0 {
		t.Errorf("the range holds %d keys after its delete: the pushed record was applied into it", held)
	}
}

// TestAJoinersAbsentPairStillRefusesWrites: a joiner holds nothing it has not pulled, and a
// node that has not observed the state cannot say it is not joining: both refuse a write and
// a pushed record for a pair with no record, as today. [BREAK: Admits ignores joining]
func TestAJoinersAbsentPairStillRefusesWrites(t *testing.T) {
	view, _ := s0Slots(t)
	view.parts.joining.Store(true)
	if view.Admits(3, reconDB) {
		t.Fatal("a joiner admits a write to a pair it never pulled")
	}
	if notReadyCause(view.State(3, reconDB)) == "" {
		t.Fatal("a joiner's absent pair words no refusal")
	}
	if replicaReady(view)(3, reconDB) {
		t.Fatal("a joiner's replicator applies a pushed record to a pair it never pulled")
	}
	unobserved := recordSlots{parts: &seedParts{recon: view.parts.recon}}
	if unobserved.Admits(3, reconDB) || replicaReady(unobserved)(3, reconDB) {
		t.Fatal("a node that has not observed the cluster state admits a write to a pair with no record")
	}
}

// TestAPositionQueryForAPairWithNoRecordIsNotReady (plan §0.2): recordSlots.State is honest:
// no record is SlotAbsent on every node, so POSITION_REPLY answers seeding, never ready at 0:0,
// for a pair the node does not hold. Admission is Admits' answer, not State's.
// [BREAK: restore the born-ready shortcut in State]
func TestAPositionQueryForAPairWithNoRecordIsNotReady(t *testing.T) {
	view, _ := s0Slots(t)
	if got := view.State(3, reconDB); got != machine.SlotAbsent {
		t.Fatalf("a pair with no record on an active node is %v, want absent", got)
	}
	if positionState(view.State(3, reconDB)) != PositionStateSeeding {
		t.Fatal("POSITION_REPLY answers ready for a pair the node has no record of")
	}
	if !view.Admits(3, reconDB) {
		t.Fatal("the honest state took the admission with it")
	}
}

// --- step 5: the interim rule for an absent in-target pair on an active node (plan §0.3) ---

// s0ActivePass runs one OnStateSeq pass on a rig for n2, active, with its records loaded.
func s0ActivePass(t *testing.T, g *rig, st cluster.ClusterState) {
	t.Helper()
	if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
		t.Fatalf("the active node's pass: %v", err)
	}
}

// TestAnAbsentInTargetPairOnAnActiveNodeIsReadyEmptyUntilTheBits (the owner's Q5 ruling): on
// an active node an absent pair in the target set is written ready, empty at 0:0, by the
// first pass, instead of a fresh seeding attempt; a pair outside the target set gets nothing.
// [BREAK: queue a seed]
func TestAnAbsentInTargetPairOnAnActiveNodeIsReadyEmptyUntilTheBits(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	s0ActivePass(t, g, st)
	for name, slot := range map[string]uint16{"owner": slots.owner, "copy": slots.copyOnly} {
		record, found := diskRecord(t, g.kv, slot)
		if !found || record.State != machine.SeedReady || !record.Applied.Zero() {
			t.Fatalf("after the first pass the %s pair's record is %+v (found %v), want ready at 0:0", name, record, found)
		}
		if asked := g.sink.of(slot); len(asked) != 1 || asked[0].Do != machine.SlotReportBit {
			t.Fatalf("the pass asked for %+v on the %s pair, want only the report of its bit: the interim is written, not seeded", asked, name)
		}
	}
	if _, found := diskRecord(t, g.kv, slots.outside); found {
		t.Fatal("a pair outside the target set got a record")
	}
}

// TestAnInterimPairAnswersTheHorizonRow (pi SF1): the interim is decided after the table ran
// on the Reconciled event, so the machine's world is known and the pair answers the
// horizon/ready-in-target row like any other ready pair. [BREAK: write the record without
// the Reconciled event]
func TestAnInterimPairAnswersTheHorizonRow(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	s0ActivePass(t, g, st)
	act, err := g.rec.Feed(slots.copyOnly, reconDB, machine.LogBelowHorizon{})
	if err != nil || act.Do != machine.SlotRestartSeed || act.Restart != 2 {
		t.Fatalf("LogBelowHorizon on an interim pair = %+v, %v; want SlotRestartSeed with restart reason 2", act, err)
	}
	if record, _ := diskRecord(t, g.kv, slots.copyOnly); record.State != machine.SeedSeeding || record.Phase != machine.PhaseReset {
		t.Fatalf("after the horizon row the record is %+v, want seeding from reset", record)
	}
}

// TestAnInterimRecordHasTheSignature: ready, 0:0, a zero cut, ordinal 0, a zero SeedID and no
// source; the zero SeedID is how S3b tells the interim from a pair seeded from an empty
// range, which keeps its SeedID. [BREAK: mint a SeedID for it]
func TestAnInterimRecordHasTheSignature(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	s0ActivePass(t, g, st)
	record, found := diskRecord(t, g.kv, slots.owner)
	if !found {
		t.Fatal("no interim record")
	}
	want := SeedRecord{}
	want.State = machine.SeedReady
	if record.State != want.State || record.Phase != machine.PhaseNone || !record.Applied.Zero() || !record.Cut.Zero() ||
		record.Ordinal != 0 || record.WALCursor != 0 || record.SeedID != [16]byte{} || record.Source != "" || len(record.Cursor) != 0 {
		t.Fatalf("the interim record is %+v, want ready at 0:0, zero cut, ordinal 0, cursor 0, zero seed id, no source", record)
	}
}

// TestAnInterimRecordKeepsALivePosition (pi's fourth review, SF-1): a copy that applied before
// the first pass has a position, an ordinal and a tail cursor in cluster.Pair; the interim
// carries all three, and a restart's load installs them, or the next write would take
// ordinal 1 against dedupe entries already at N. [BREAK: copy AppliedPos only]
func TestAnInterimRecordKeepsALivePosition(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	live := cluster.Position{Epoch: uint64(st.Epoch), Seq: 5}
	g.pairs.With(slots.copyOnly, reconDB, func(pair *cluster.Pair) {
		pair.AppliedPos, pair.Ordinal, pair.TailCursor = live, 7, 9
	})
	s0ActivePass(t, g, st)
	record, found := diskRecord(t, g.kv, slots.copyOnly)
	if !found || record.State != machine.SeedReady || record.Applied != live || record.Ordinal != 7 || record.WALCursor != 9 {
		t.Fatalf("the interim of a pair with a live position is %+v (found %v), want ready at %s, ordinal 7, cursor 9", record, found, live)
	}
	restarted := newRig(t, g.kv, st, testSelf, true)
	if err := restarted.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	if pair := restarted.pairs.Snapshot(slots.copyOnly, reconDB); pair.AppliedPos != live || pair.Ordinal != 7 || pair.TailCursor != 9 {
		t.Fatalf("after a restart the pair is %+v, want applied %s, ordinal 7, cursor 9 from the interim", pair, live)
	}
}

// --- step 4: the driver runs for every data member (plan §0.3) ---

// TestAnActiveNodeReconcilesOnItsFirstPassAndOnEveryStateSeq: a member that is not joining
// runs OnStateSeq on its first pass after the state is loaded and on every later state_seq
// change, so its idle in-target pairs get the interim and a reseed has a known world.
// [BREAK: keep the joining guard in driveSeeding]
func TestAnActiveNodeReconcilesOnItsFirstPassAndOnEveryStateSeq(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	second, err := node.CreateDatabaseRF(context.Background(), "second", 1)
	if err != nil {
		t.Fatal(err)
	}
	s0Await(t, "the pass of the next state_seq", 15*time.Second, func() bool {
		return node.SlotRecordState(s0IdleSlot, second) == machine.SlotReady
	})
}

// TestAnActiveNodesFirstPassNeedsNoSecondLoad (the reconciler's R1): the records are loaded
// once, by LoadRecords in Node.open; the first pass is OnStateSeq and never OnBoot, whose
// load refuses to run twice. [BREAK: call OnBoot after LoadRecords]
func TestAnActiveNodesFirstPassNeedsNoSecondLoad(t *testing.T) {
	errs := &s0Errors{}
	node := s0OpenClusterNode(t, t.TempDir(), errs)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	for _, reported := range errs.all() {
		if strings.Contains(reported, "already ran") || strings.Contains(reported, "not loaded") {
			t.Fatalf("the driver reported %q: the first pass must run on the records LoadRecords loaded", reported)
		}
	}
}

// s0RunPass runs the pass seedPassFor chose on a rig, as driveSeeding does.
func s0RunPass(t *testing.T, g *rig, pass seedPass, st cluster.ClusterState) {
	t.Helper()
	var err error
	switch pass {
	case seedPassResumeJoining:
		err = g.rec.ResumeJoining(t.Context())
	case seedPassJoiningCommit:
		err = g.rec.OnJoiningCommit(st)
	case seedPassStateSeq:
		err = g.rec.OnStateSeq(t.Context(), st)
	}
	if err != nil {
		t.Fatalf("pass %d: %v", pass, err)
	}
}

// TestARestartingJoinerKeepsItsResumeCursor (T3-2): a joiner that restarts mid-seed with a
// seeding record takes ResumeJoining on its first pass, never a forced reset, so the record
// keeps its cursor and seed id. [BREAK: send a restarting joiner through a forced reset]
func TestARestartingJoinerKeepsItsResumeCursor(t *testing.T) {
	st := joiningNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	seeding := seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: uint64(st.Epoch), Seq: 3}, "cursor-key")
	putRecord(t, kv, slots.copyOnly, seeding)
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	self, _ := reconcileMember(st, testSelf)
	pass := seedPassFor(self, true, g.rec.HasRecords())
	if pass != seedPassResumeJoining {
		t.Fatalf("a restarting joiner with records takes pass %d, want ResumeJoining", pass)
	}
	s0RunPass(t, g, pass, st)
	kept, _ := g.rec.Record(slots.copyOnly, reconDB)
	if kept.State != machine.SeedSeeding || string(kept.Cursor) != "cursor-key" || kept.SeedID != seeding.SeedID {
		t.Fatalf("after the restart the seeding record is %+v, want its cursor and seed id kept", kept)
	}
}

// TestAJoinerStillTakesTheResumeAndJoiningPaths: the joining branch is untouched: a joiner's
// first pass is OnJoiningCommit without records and ResumeJoining with them, every later pass
// OnStateSeq; a member that is not joining takes OnStateSeq from its first pass.
// [BREAK: send a restarting joiner through the active-node branch]
func TestAJoinerStillTakesTheResumeAndJoiningPaths(t *testing.T) {
	joining := cluster.Member{ID: testSelf, State: cluster.MemberJoining}
	active := cluster.Member{ID: testSelf, State: cluster.MemberActive}
	for _, tc := range []struct {
		name    string
		self    cluster.Member
		first   bool
		records bool
		want    seedPass
	}{
		{"joiner, first pass, no records", joining, true, false, seedPassJoiningCommit},
		{"joiner, first pass, records", joining, true, true, seedPassResumeJoining},
		{"joiner, later pass", joining, false, true, seedPassStateSeq},
		{"active, first pass", active, true, true, seedPassStateSeq},
		{"active, first pass, no records", active, true, false, seedPassStateSeq},
		{"active, later pass", active, false, true, seedPassStateSeq},
		{"down, first pass", cluster.Member{ID: testSelf, State: cluster.MemberDown}, true, true, seedPassStateSeq},
	} {
		if got := seedPassFor(tc.self, tc.first, tc.records); got != tc.want {
			t.Errorf("%s: pass %d, want %d", tc.name, got, tc.want)
		}
	}
	// The joiner's first pass without records reconciles from the joining commit.
	st := joiningNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	self, _ := reconcileMember(st, testSelf)
	s0RunPass(t, g, seedPassFor(self, true, g.rec.HasRecords()), st)
	if rec, found := diskRecord(t, g.kv, findSlots(t, st).copyOnly); !found || rec.State != machine.SeedSeeding {
		t.Fatalf("a joiner's first pass left %+v (found %v), want a queued seed", rec, found)
	}
}

// TestTheDriverSkipsAWitness: a witness holds no data (the FSM excludes it from every ranked
// set), so the driver runs no pass on it. [BREAK: run the pass on a witness]
func TestTheDriverSkipsAWitness(t *testing.T) {
	for _, state := range []cluster.MemberState{cluster.MemberActive, cluster.MemberJoining, cluster.MemberDown} {
		witness := cluster.Member{ID: "w1", State: state, Witness: true}
		for _, first := range []bool{true, false} {
			if got := seedPassFor(witness, first, false); got != seedPassNone {
				t.Errorf("a witness in state %v (first %v) takes pass %d, want none", state, first, got)
			}
		}
	}
}

// TestAReseedOnAnActiveNodeWithARecordFires (the test that fails without the driver): a ready
// record on an active node whose world the first pass made known answers LogBelowHorizon
// with SlotRestartSeed; without a pass the machine's horizon/nothing-to-catch-up row answers
// SlotDoNothing. [BREAK: never run the first pass]
func TestAReseedOnAnActiveNodeWithARecordFires(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	inserted := s0Insert(t, node, "u1")
	slot := s0SlotOf(t, "u1")
	if record, found := node.cl.seeding.Load().recon.Record(slot, dbID); !found || record.Applied != inserted.Pos {
		t.Fatalf("the written pair's record is %+v (found %v), want ready at %s", record, found, inserted.Pos)
	}
	act, err := node.cl.seeding.Load().recon.Feed(slot, dbID, machine.LogBelowHorizon{})
	if err != nil || act.Do != machine.SlotRestartSeed || act.Restart != 2 {
		t.Fatalf("LogBelowHorizon on an active node's ready pair = %+v, %v; want SlotRestartSeed with reason 2", act, err)
	}
	if state := node.SlotRecordState(slot, dbID); state != machine.SlotSeeding {
		t.Fatalf("after the horizon row the record is %v, want seeding", state)
	}
}

// --- a write that meets the pass's uncommitted batch waits it out ---

// holdingStore blocks the first batch commit that carries two or more seed records (a
// reconcile pass's batch) until release is closed; every other batch commits at once.
type holdingStore struct {
	storage.KV
	hold    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *holdingStore) NewBatch() storage.KVBatch {
	return &holdingBatch{KVBatch: s.KV.NewBatch(), store: s}
}

type holdingBatch struct {
	storage.KVBatch
	store   *holdingStore
	records int
}

func (b *holdingBatch) Set(key, value []byte) error {
	if len(key) == len(SeedRecordKey(0, 0)) && key[0] == 0xFF {
		b.records++
	}
	return b.KVBatch.Set(key, value)
}

func (b *holdingBatch) Commit(sync bool) error {
	if b.records >= 2 {
		b.store.once.Do(func() {
			close(b.store.hold)
			<-b.store.release
		})
	}
	return b.KVBatch.Commit(sync)
}

// TestAWriteWaitsForThePassThatStagedItsPair (S0 build status, finding 2): the first pass
// over a new database stages a pair's interim in a batch of up to 256 records and the pair
// is busy until that batch commits; a write that arrives meanwhile waits the commit out
// instead of failing with "the pair is being changed", as every write of the database's
// first moments otherwise would. [BREAK: return ErrPairBusy from the advancer at once]
func TestAWriteWaitsForThePassThatStagedItsPair(t *testing.T) {
	store := &holdingStore{hold: make(chan struct{}), release: make(chan struct{})}
	g, st, slots := s0ReconcilerRig(t, nil)
	store.KV = g.kv
	g.rec.cfg.Store = store
	passDone := make(chan error, 1)
	go func() { passDone <- g.rec.OnStateSeq(t.Context(), st) }()
	<-store.hold // the pass is in its first commit with slots.owner staged and busy
	to := cluster.Position{Epoch: uint64(st.Epoch), Seq: 1}
	advanced := make(chan error, 1)
	go func() {
		advanced <- s0Advancer(g).Advance(slots.owner, reconDB, AdvanceStep{
			Stage: func(batch storage.KVBatch, _ *cluster.Pair) (AdvanceTo, error) {
				return AdvanceTo{Applied: to, Ordinal: 1}, batch.Set(append(storage.PairPrefix(slots.owner, reconDB), "k"...), []byte("v"))
			},
			Commit: func(batch storage.KVBatch, _ bool) error { return batch.Commit(false) },
		})
	}()
	select {
	case err := <-advanced:
		t.Fatalf("the write answered %v while the pass's batch was uncommitted, want it to wait", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(store.release)
	if err := <-passDone; err != nil {
		t.Fatalf("the pass: %v", err)
	}
	if err := <-advanced; err != nil {
		t.Fatalf("the write after the pass's commit: %v", err)
	}
	if record, found := diskRecord(t, g.kv, slots.owner); !found || record.State != machine.SeedReady || record.Applied != to {
		t.Fatalf("the record after the write is %+v (found %v), want ready at %s", record, found, to)
	}
}

// TestAnOwnerRestartInTheSameEpochResumesAfterItsLastAppend (write path §3.1): a restart
// inside the committed epoch resumes at appendedSeq + 1, whether the pair's record restored
// the applied position (the floor) or the replay learned the append. [BREAK: drop the
// AppliedPos floor in appendedAt]
func TestAnOwnerRestartInTheSameEpochResumesAfterItsLastAppend(t *testing.T) {
	const e = 4
	applied := cluster.Position{Epoch: e, Seq: 2}
	fromRecord := cluster.Pair{AppliedPos: applied} // the record restored the position; no replay staged anything
	if got := appendedAt(fromRecord, e); got != 2 {
		t.Fatalf("appendedAt from the record alone = %d, want 2: the applied seq is the floor", got)
	}
	learned := cluster.Pair{AppliedPos: applied, AppendedSeq: 5, AppendedEpoch: e} // the log holds up to e:5
	if got := appendedAt(learned, e); got != 5 {
		t.Fatalf("appendedAt with a learned append = %d, want 5", got)
	}
	seq := machine.PairSeq{Epoch: e, AppendedSeq: appendedAt(learned, e), PrevEnd: applied, AppliedPos: applied, Owner: true, Replayed: true, LeaseOK: true, Members: 1}
	decision, _ := seq.Apply(machine.OrderRequest{RequestID: [16]byte{1}})
	if decision.Assign == nil || decision.Assign.Pos != (cluster.Position{Epoch: e, Seq: 6}) {
		t.Fatalf("the next order after a same-epoch restart = %+v, want e:6", decision.Assign)
	}
}

// TestAnAppendLearnedForAnOlderEpochDoesNotCarryIntoTheNewOne (write path §3.1): a committed
// epoch that leaves this node the owner resets appendedSeq, so an append learned for epoch e
// counts for nothing at e+1: the next order is e+1:1 (the opener), and a catch-up source's
// appended position is what it applied, never e+1:<old seq>. [BREAK: appendedAt and
// appendedPos read AppendedSeq without the marker]
func TestAnAppendLearnedForAnOlderEpochDoesNotCarryIntoTheNewOne(t *testing.T) {
	const e = 4
	applied := cluster.Position{Epoch: e, Seq: 2}
	stale := cluster.Pair{AppliedPos: applied, AppendedSeq: 2, AppendedEpoch: e}
	if got := appendedAt(stale, e+1); got != 0 {
		t.Fatalf("appendedAt at the next epoch = %d, want 0: the append belongs to epoch %d", got, e)
	}
	seq := machine.PairSeq{Epoch: e + 1, AppendedSeq: appendedAt(stale, e+1), PrevEnd: applied, AppliedPos: applied, Owner: true, Replayed: true, LeaseOK: true, Members: 1}
	decision, _ := seq.Apply(machine.OrderRequest{RequestID: [16]byte{1}})
	if decision.Assign == nil || decision.Assign.Pos != (cluster.Position{Epoch: e + 1, Seq: 1}) || decision.Assign.PrevEnd != applied {
		t.Fatalf("the first order of the new epoch = %+v, want e+1:1 with prevEnd %s", decision.Assign, applied)
	}
	source := newU32Node(t, "source", nil)
	source.committed.Store(e + 1)
	source.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) { *p = stale })
	if got := source.rep.appendedPos(pairKey{u32Slot, u32DB}); got != applied {
		t.Fatalf("a catch-up source's appended position at the next epoch = %s, want its applied %s: the old append is not e+1:2", got, applied)
	}
}

// --- B-1: the replay raises the append with the record's own epoch, whatever the state ---

// TestAReplayWhileTheStateIsUnappliedStillLearnsTheAppendsEpoch (pi B-1): on a multi-node
// boot the committed state is epoch 0 while tail.Scan runs (the library applies the log after
// NewRaft returns, and a follower applies nothing before a leader's heartbeat), so a raise
// conditioned on the committed epoch is skipped for every record and the next order after
// the restart reuses a position the log holds. The replay must learn the append from the
// record's own epoch; appendedAt's committed-epoch test decides whether it counts, so an
// older or a not-yet-committed epoch learned at boot is inert until that epoch commits.
// The unapplied state is forced by a topology whose committed epoch is 0. [BREAK: raise only
// when rec.Pos.Epoch == committed]
func TestAReplayWhileTheStateIsUnappliedStillLearnsTheAppendsEpoch(t *testing.T) {
	g, _, slots := s0ReconcilerRig(t, nil)
	unapplied := &fakeTopology{committed: 0}
	w := NewWriter(WriterConfig{Topology: unapplied, Pairs: g.pairs, Advancer: s0Advancer(g)})
	body, err := cluster.EncodeOp(u32Insert(t, "lost", cluster.Position{}))
	if err != nil {
		t.Fatal(err)
	}
	logged := cluster.Position{Epoch: 3, Seq: 2}
	record := cluster.WALRecord{Slot: slots.owner, DB: reconDB, Pos: logged, Cursor: 7}
	record.Body, record.RequestID = body, [16]byte{9}
	if err := w.Replay(record); err != nil {
		t.Fatal(err)
	}
	pair := g.pairs.Snapshot(slots.owner, reconDB)
	if pair.AppendedSeq != logged.Seq || pair.AppendedEpoch != logged.Epoch {
		t.Fatalf("after a replay against the unapplied state the pair's append is %d at epoch %d, want %s learned from the record itself", pair.AppendedSeq, pair.AppendedEpoch, logged)
	}
	if !pair.AppliedPos.Zero() {
		t.Fatalf("the replay applied %s to a record-less pair", pair.AppliedPos)
	}
	if got := appendedAt(pair, 3); got != 2 {
		t.Fatalf("once epoch 3 is committed the append counts as %d, want 2", got)
	}
	if got := appendedAt(pair, 4); got != 0 {
		t.Fatalf("at epoch 4 the append learned for epoch 3 counts as %d, want 0", got)
	}
}

// --- S-1: the applied-log gate on a snapshot-only boot and the stable-state fallback ---

// TestASnapshotOnlyBootStillRunsTheFirstPass (pi S-1): a node whose snapshot is at the tip of
// its log restarts with no further command: the library restores the snapshot and applies
// nothing, so a gate that waits for the FSM to apply the last command never opens, no first
// pass runs and no interim is written. The restored snapshot's index counts as applied.
// [BREAK: ignore the restored snapshot index in LogApplied]
func TestASnapshotOnlyBootStillRunsTheFirstPass(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := s0Schema(t, node)
	s0Insert(t, node, "u1")
	if err := node.raft.TakeSnapshot(); err != nil {
		t.Fatalf("taking the snapshot: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	forgetPair(t, dir, s0IdleSlot, dbID) // an idle pair with no record: only a pass gives it one
	reopened := s0OpenClusterNode(t, dir, nil)
	s0AwaitFirstPass(t, reopened, dbID)
}

// TestTheGateLatchesOnlyOnAnExactLoadedAnswer (pi S-1, review N1): the driver's latch opens on
// the first loaded answer of Raft.LoadedNow and stays open; no number of quiet ticks opens it
// (the old fallback did, and opened mid-replay behind a stalled FSM). The trailing-command boot
// that LoadedNow answers true for is TestAnUncommittedTrailingCommandReplacedByANoopStillLoadsTheState
// in package cluster. [BREAK: put back the stable-state_seq fallback in cluster.LoadLatch]
func TestTheGateLatchesOnlyOnAnExactLoadedAnswer(t *testing.T) {
	parts := &seedParts{}
	for tick := range 10 {
		if parts.stateLoaded(false, false) {
			t.Fatalf("open on tick %d with no loaded answer", tick)
		}
	}
	if !parts.stateLoaded(true, false) {
		t.Fatal("not open on a loaded answer")
	}
	if !parts.stateLoaded(false, false) {
		t.Fatal("the gate closed again after opening")
	}
}
