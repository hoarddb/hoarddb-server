package wire

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// BUG-0809 review follow-up (sync-only failure): the batch that creates a pair's record commits
// synced (part A), and Badger's Commit(true) commits the transaction before it syncs. A failed sync
// therefore returns an error for a batch that is in the store. The owner's Commit callback took any
// commit error as row 9: it fenced the pair and marked the owed first record (part B), the advancer
// returned before it installed the record, and recoverPair's owed replay saw an applied position of 0
// and applied the record a second time.

// bug0809SyncSchema creates database app (RF 1) with bucket User { Name string, Age int } on a
// cluster node and returns the database and bucket ids.
func bug0809SyncSchema(t *testing.T, node *Node) (dbID, bucketID uint64) {
	t.Helper()
	ctx := context.Background()
	dbID, err := node.CreateDatabaseRF(ctx, u30iDB, 1)
	if err != nil {
		t.Fatalf("CreateDatabaseRF: %v", err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"}, Field{Name: "Age", Type: "int"}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	ref, err := node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	return dbID, ref.entry.Decl.BucketID
}

// bug0809ArmSyncFault puts the store every advancer batch is made from behind a kvfault.SyncFault
// and arms it, so the next synced commit (the one that creates a pair's record) commits and then
// reports a failed sync, and every later commit through it is refused, as Badger does. The field
// is assigned without a lock: passes are held by the caller, and the write path, which reads it
// too (every batch, and the range look of BUG-0819), is not running yet.
func bug0809ArmSyncFault(node *Node) *kvfault.SyncFault {
	recon := node.cl.seeding.Load().recon
	fault := kvfault.WrapSyncFault(node.kv)
	recon.cfg.Store = fault
	fault.FailNextSync()
	return fault
}

// bug0809ForgetRecord removes the pair's record from the reconciler's memory and the store and
// resets the pair's applied state, keeping its documents and its log. It is the one way to make an
// update the first write of a record-less pair: on a node an update of an absent document is
// refused before it takes a position, so a pair's first write is otherwise always an insert.
func bug0809ForgetRecord(t *testing.T, node *Node, slot uint16, dbID uint64) {
	t.Helper()
	recon := node.cl.seeding.Load().recon
	node.pairs.With(slot, dbID, func(p *cluster.Pair) {
		recon.mu.Lock()
		delete(recon.pairs, reconcileKey{slot, dbID})
		recon.mu.Unlock()
		p.AppliedPos, p.Ordinal, p.TailCursor = cluster.Position{}, 0, 0
	})
	batch := node.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(SeedRecordKey(slot, dbID)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	if !recon.Recordless(slot, dbID) {
		t.Fatal("the pair still has a record after it was forgotten")
	}
}

// bug0809RangeBlind hides one pair's key range from the reconciler's own snapshots. A pair with
// no record over a range that holds a document is what bug0809ForgetRecord builds and what
// `seeding forget` leaves, and since BUG-0819 the reconciler looks at the range and refuses to
// create a record over it. The $inc tests need that manufactured pair to take a first write, so
// they blindfold the look; the write path reads the document through the node's store, not this one.
type bug0809RangeBlind struct {
	Store
	prefix []byte
}

func (b bug0809RangeBlind) NewSnapshot() storage.KVSnapshot {
	return bug0809BlindSnapshot{KVSnapshot: b.Store.NewSnapshot(), prefix: b.prefix}
}

type bug0809BlindSnapshot struct {
	storage.KVSnapshot
	prefix []byte
}

func (s bug0809BlindSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	if bytes.Equal(opts.Lower, s.prefix) {
		opts.Lower = opts.Upper // an empty span: the pair's range, as the look asks for it
	}
	return s.KVSnapshot.NewIterator(opts)
}

// bug0809CopyAgrees applies the pair's logged records of the bucket, in log order, to a fresh store
// as a copy does, and compares the documents with the owner's, key by key.
func bug0809CopyAgrees(t *testing.T, node *Node, slot uint16, dbID, bucketID uint64) {
	t.Helper()
	copyKV := kvfault.New()
	applier := NewDocumentApplier(copyKV, nil)
	for _, record := range loggedOps(t, node) {
		if record.Slot != slot || record.DB != dbID || record.Op.BucketID != bucketID {
			continue
		}
		batch := copyKV.NewBatch()
		if err := applier.Fill(batch, record); err != nil {
			t.Fatalf("a copy applying position %s: %v", record.Pos, err)
		}
		if err := batch.Commit(false); err != nil {
			t.Fatal(err)
		}
	}
	prefix := storage.Key(slot, dbID, bucketID, storage.NSDocuments)
	owner, copied := storeRange(t, node.kv, prefix), storeRange(t, copyKV, prefix)
	if len(owner) != len(copied) {
		t.Errorf("the owner holds %d document keys of the pair, a copy that applies its log once holds %d", len(owner), len(copied))
	}
	for key, value := range owner {
		if got, ok := copied[key]; !ok || got != value {
			t.Errorf("document key %x differs: owner %x, copy %x (present %v): the owner applied the write a second time", key, value, got, ok)
		}
	}
}

// bug0809LoggedAt counts the pair's logged records at pos.
func bug0809LoggedAt(t *testing.T, node *Node, slot uint16, dbID uint64, pos cluster.Position) int {
	t.Helper()
	count := 0
	for _, record := range loggedOps(t, node) {
		if record.Slot == slot && record.DB == dbID && record.Pos == pos {
			count++
		}
	}
	return count
}

// bug0809RequireUnknownOutcome: a write whose batch is in the store but not known to be on disk
// is answered as the pair-lock spec's F3 and F12 answer a failed flush: an error with no refusal
// code, so the driver treats the outcome as unknown and retries with the same request id.
func bug0809RequireUnknownOutcome(t *testing.T, what string, err error) {
	t.Helper()
	var coded *cluster.Error
	if err == nil || errors.As(err, &coded) {
		t.Errorf("%s answered %v; want an unknown outcome with no refusal code (pair-lock spec F3, F12): the batch is in the store, but whether it survives a restart is not known", what, err)
	}
}

// TestASyncOnlyFailureOfTheFirstInsertAppliesItOnce: the first insert to a record-less pair commits
// its batch and then fails the sync. The insert is applied once, its record is installed, the client
// gets the unknown outcome, a retry with the same request id is deduped at the first position and
// appends nothing, and a copy that applies the log once holds what the owner holds.
// [BREAK: take a sync-only failure as row 9]
func TestASyncOnlyFailureOfTheFirstInsertAppliesItOnce(t *testing.T) {
	bug0809SyncOnlyFailure(t, false, false)
}

// TestASyncOnlyFailureOfTheFirstInsertAppliesItOnceOnTheLockedPath is the same on the locked path.
func TestASyncOnlyFailureOfTheFirstInsertAppliesItOnceOnTheLockedPath(t *testing.T) {
	bug0809SyncOnlyFailure(t, true, false)
}

// TestASyncOnlyFailureOfTheFirstIncAppliesItOnce: the same with an $inc as the first write of the
// record-less pair (bug0809ForgetRecord), where a second apply changes the value: Age must be 36+2,
// not 36+2+2.
func TestASyncOnlyFailureOfTheFirstIncAppliesItOnce(t *testing.T) {
	bug0809SyncOnlyFailure(t, false, true)
}

// TestASyncOnlyFailureOfTheFirstIncAppliesItOnceOnTheLockedPath is the $inc on the locked path.
func TestASyncOnlyFailureOfTheFirstIncAppliesItOnceOnTheLockedPath(t *testing.T) {
	bug0809SyncOnlyFailure(t, true, true)
}

func bug0809SyncOnlyFailure(t *testing.T, locked, inc bool) {
	t.Helper()
	ctx := context.Background()
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node)
	defer release()
	dbID, bucketID := bug0809SyncSchema(t, node)
	ids, slot := s0IDsInOneSlot(t, 1)
	id := ids[0]
	recon := node.cl.seeding.Load().recon
	if _, found := recon.Record(slot, dbID); found {
		t.Fatal("the pair has a record before its first write; the test needs a record-less pair")
	}
	if inc {
		if _, err := node.Insert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}, {Key: "Age", Value: int32(36)}}); err != nil {
			t.Fatalf("the insert the $inc changes: %v", err)
		}
		bug0809ForgetRecord(t, node, slot, dbID)
	}
	if locked {
		advancer, ok := node.writer.cfg.Advancer.(seedAwareAdvancer)
		if !ok {
			t.Fatalf("the node's advancer is %T, want seedAwareAdvancer", node.writer.cfg.Advancer)
		}
		node.writer.cfg.Advancer = bug0809LockedOnly{inner: advancer}
	}
	bug0809ArmSyncFault(node)
	if inc {
		recon.cfg.Store = bug0809RangeBlind{Store: recon.cfg.Store, prefix: storage.PairPrefix(slot, dbID)}
	}

	requestID := [16]byte{0x08, 0x09, 0x5c}
	write := func() (cluster.Position, bool, error) {
		if inc {
			updated, err := node.Update(ctx, UpdateRequest{Database: u30iDB, Bucket: "User", ID: id, Inc: bson.D{{Key: "Age", Value: int32(2)}}}, WithRequestID(requestID))
			return updated.Pos, updated.Deduped, err
		}
		inserted, err := node.Insert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}, {Key: "Age", Value: int32(36)}}, WithRequestID(requestID))
		return inserted.Pos, inserted.Deduped, err
	}
	_, _, err := write()
	bug0809RequireUnknownOutcome(t, "the write whose sync failed", err)
	pair := node.pairs.Snapshot(slot, dbID)
	pos := cluster.Position{Epoch: pair.AppendedEpoch, Seq: pair.AppendedSeq}

	wantAge := int32(36)
	if inc {
		wantAge = 38
	}
	doc, err := node.Get(ctx, u30iDB, "User", id)
	if err != nil {
		t.Fatalf("after the sync-only failure the document is not readable: %v", err)
	}
	if got := bson.Raw(doc.Body).Lookup("Age").Int32(); got != wantAge {
		t.Errorf("after the sync-only failure Age is %d, want %d: the write at %s was applied %s", got, wantAge, pos, map[bool]string{true: "twice", false: "not once"}[got > wantAge])
	}
	if after := node.pairs.Snapshot(slot, dbID); after.AppliedPos != pos || after.Ordinal != 1 || after.Flags&flagFirstApplyFailed != 0 {
		t.Errorf("the pair is applied %s, ordinal %d, flags %#x; want %s, ordinal 1, no owed first record", after.AppliedPos, after.Ordinal, after.Flags, pos)
	}
	if record, found := recon.Record(slot, dbID); !found || record.State != machine.SeedReady || record.Applied != pos || record.Ordinal != 1 {
		t.Errorf("the pair's record is %+v (found %v), want ready at %s, ordinal 1: the committed batch created it", record, found, pos)
	}
	bug0809CopyAgrees(t, node, slot, dbID, bucketID)

	retryPos, deduped, err := write()
	if err != nil || !deduped || retryPos != pos {
		t.Errorf("a retry with the same request id answered %s (deduped %v), err %v; want deduped at %s", retryPos, deduped, err, pos)
	}
	if got := bug0809LoggedAt(t, node, slot, dbID, pos); got != 1 {
		t.Errorf("the log holds %d records of the pair at %s, want 1", got, pos)
	}
	if doc, err := node.Get(ctx, u30iDB, "User", id); err != nil || bson.Raw(doc.Body).Lookup("Age").Int32() != wantAge {
		t.Errorf("after the retry the document reads %v, err %v; want Age %d", doc.Body, err, wantAge)
	}

	// The store is poisoned: no later write is acknowledged on top of a batch that may not be on disk.
	_, err = node.Update(ctx, UpdateRequest{Database: u30iDB, Bucket: "User", ID: id, Set: bson.D{{Key: "Name", Value: "later"}}})
	var refused *storage.StoreRefusesWritesError
	if !errors.As(err, &refused) {
		t.Errorf("a write after the failed sync answered %v; want it refused by the poisoned store", err)
	}
}

// TestTheClusterNodesAdvancerReportsRecordlessPairs: markFirstApplyFailed asks the advancer through
// an optional interface, so an advancer that stopped implementing it would switch BUG-0809's owed
// replay off without a compile error. The advancer a cluster node wires must implement it.
// [BREAK: drop seedAwareAdvancer.Recordless]
func TestTheClusterNodesAdvancerReportsRecordlessPairs(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	if _, ok := node.writer.cfg.Advancer.(RecordlessReporter); !ok {
		t.Fatalf("the cluster node's advancer %T does not implement RecordlessReporter, so a row 9 on a record-less pair is never marked owed and its first write is skipped by recovery (BUG-0809)", node.writer.cfg.Advancer)
	}
}
