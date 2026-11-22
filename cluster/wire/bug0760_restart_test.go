package wire

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// BUG-0760: a node killed hard restarts with its log tail ahead of its pair records. A
// replayed push needs the bucket's declaration, which a cold cache reads from the ring
// through the committed state, and a booting node has not applied that state yet.

// savedPair is a pair's record and every key under its prefix, as one moment left them.
type savedPair struct {
	slot   uint16
	db     uint64
	record []byte
	keys   map[string][]byte
}

// savePair reads the pair of slot and db from the closed node in dir.
func savePair(t *testing.T, dir string, slot uint16, db uint64) savedPair {
	t.Helper()
	kv := s0Badger(t, dir)
	defer func() { _ = kv.Close() }()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	saved := savedPair{slot: slot, db: db, keys: map[string][]byte{}}
	record, err := snapshot.Get(LocalPairRecordKey(slot, db))
	if err != nil {
		t.Fatalf("reading the pair record of slot %d: %v", slot, err)
	}
	saved.record = append([]byte(nil), record...)
	prefix := storage.PairPrefix(slot, db)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		if err != nil {
			t.Fatal(err)
		}
		saved.keys[string(iter.Key())] = append([]byte(nil), value...)
	}
	return saved
}

// rewindPair puts the closed node's pair back to what saved holds: the state a kill left when
// the log held records the pair's store and record had not counted yet.
func rewindPair(t *testing.T, dir string, saved savedPair) {
	t.Helper()
	forgetPair(t, dir, saved.slot, saved.db)
	kv := s0Badger(t, dir)
	defer func() { _ = kv.Close() }()
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(LocalPairRecordKey(saved.slot, saved.db), saved.record); err != nil {
		t.Fatal(err)
	}
	for key, value := range saved.keys {
		if err := batch.Set([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// bug0760Box creates database db (RF 1) with bucket Box { Name string, Queue fifoStore } and
// one document, b1, on a cluster node.
func bug0760Box(t *testing.T, node *Node, db string) uint64 {
	t.Helper()
	ctx := context.Background()
	dbID, err := node.CreateDatabaseRF(ctx, db, 1)
	if err != nil {
		t.Fatalf("CreateDatabaseRF %s: %v", db, err)
	}
	if _, err := node.CreateBucket(ctx, db, "Box", Field{Name: "Name", Type: "string"}, Field{Name: "Queue", Type: "fifoStore"}); err != nil {
		t.Fatalf("CreateBucket in %s: %v", db, err)
	}
	if _, err := node.Insert(ctx, db, "Box", bson.D{{Key: "_id_", Value: "b1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatalf("Insert into %s: %v", db, err)
	}
	return dbID
}

func bug0760Push(t *testing.T, node *Node, db string, items ...string) {
	t.Helper()
	for _, item := range items {
		target := StoreTarget{Database: db, Bucket: "Box", ID: "b1", Field: "Queue"}
		bug0760Retry(t, "Push "+item+" to "+db, func() error {
			return node.Push(context.Background(), PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: item}}})
		})
	}
}

// bug0760Retry runs write until it succeeds: a restarted node holds its pairs for a few
// seconds (the restart barrier) and answers with a refusal that says to retry.
func bug0760Retry(t *testing.T, what string, write func() error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := write()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was still refused after 30 s: %v", what, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// bug0760Queue reads the Queue of Box b1 in db once the node serves its pair.
func bug0760Queue(t *testing.T, node *Node, db string) []string {
	t.Helper()
	target := StoreTarget{Database: db, Bucket: "Box", ID: "b1", Field: "Queue"}
	deadline := time.Now().Add(30 * time.Second)
	for {
		elements, err := node.Peek(context.Background(), PeekRequest{StoreTarget: target, Count: 100})
		if err == nil {
			items := make([]string, 0, len(elements))
			for _, element := range elements {
				items = append(items, u62Item(t, element))
			}
			return items
		}
		if time.Now().After(deadline) {
			t.Fatalf("Peek of %s still refused 30 s after the restart: %v", db, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// bug0760Errors collects what a reopened node reports in the background; a test reads it and
// resets it with bug0760Errors = &s0Errors{} before the open.
var bug0760Errors = &s0Errors{}

// holdRaftApply holds every Raft apply until the returned release is called, so a booting
// node has no committed state at its log-tail replay.
func holdRaftApply(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	raftApplyGate = func(byte) { <-gate }
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() {
		release()
		raftApplyGate = nil
	})
	return release
}

// openHeld reopens the cluster node in dir while its Raft apply is held, and reports the
// committed state at the replay. The apply stays held until release is called.
func openHeld(t *testing.T, dir string, hooks ...func(*Node)) (opened *Node, stateSeqAtScan int64, release func()) {
	t.Helper()
	release = holdRaftApply(t)
	stateSeqAtScan = -1
	beforeTailScan = func(n *Node) {
		stateSeqAtScan = int64(n.raft.StateSeq())
		for _, hook := range hooks {
			hook(n)
		}
	}
	t.Cleanup(func() { beforeTailScan = nil })
	// Join is how a node that is a member of a ring restarts: it does not bootstrap, so the
	// open does not wait for the log to be applied, as a node of three waits for a leader.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opened, err = OpenNode(ctx, NodeConfig{Dir: dir, NodeID: "s0", Cluster: &ClusterOptions{Listener: listener, InsecureTestOnly: true, Join: true, RaftLogger: hclog.NewNullLogger(), OnError: bug0760Errors.report}})
	if err != nil {
		t.Fatalf("opening the node in %s with no committed state: %v", dir, err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	// Registered last, so it runs before the Close above: Raft.Close waits for the held apply,
	// and a test that fails before it releases would hang for the whole test timeout.
	t.Cleanup(release)
	return opened, stateSeqAtScan, release
}

// TestARestartReplaysAPushBeforeTheCommittedStateIsApplied (BUG-0760): a push is in the log
// and the pair's record and store are behind it, as a kill -9 leaves them. The node reopens
// with no committed state; it starts, and once the state arrives it holds every element.
// [BREAK: the replay asks the ring for the declaration and OpenNode fails]
func TestARestartReplaysAPushBeforeTheCommittedStateIsApplied(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	slot := bug0760Slot(t, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	before := savePair(t, dir, slot, dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "first", "second")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)

	reopened, atScan, release := openHeld(t, dir)
	if atScan != 0 {
		t.Fatalf("StateSeq at the replay is %d, want 0: the test needs a boot with no committed state", atScan)
	}
	// Before the state arrives the pair is fenced: it holds a log tail it has not applied, so it
	// must neither serve nor order.
	if flags := reopened.pairs.Snapshot(slot, dbID).Flags; flags&flagReplayNeeded == 0 {
		t.Fatalf("the pair's flags are %#x before the state is applied, want flagReplayNeeded set", flags)
	}
	target := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}
	if err := reopened.Push(context.Background(), PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: "early"}}}); err == nil {
		t.Fatal("a push was acknowledged on a pair that has not replayed its log tail")
	}
	release()
	got := bug0760Queue(t, reopened, u30iDB)
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("Queue after the restart = %v, want [first second]: an acknowledged push was lost or reordered", got)
	}
	bug0760Push(t, reopened, u30iDB, "third")
	if got := bug0760Queue(t, reopened, u30iDB); len(got) != 3 || got[2] != "third" {
		t.Fatalf("Queue after a push on the restarted node = %v, want [first second third]", got)
	}
}

// TestARestartReplaysPushesOfSeveralDatabasesBeforeTheStateIsApplied (BUG-0760): two
// databases each have a pushed position the records have not counted; both pairs wait for the
// state, both replay in log order.
func TestARestartReplaysPushesOfSeveralDatabasesBeforeTheStateIsApplied(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbs := []string{u30iDB, "other"}
	ids := map[string]uint64{}
	for _, db := range dbs {
		ids[db] = bug0760Box(t, node, db)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	before := map[string]savedPair{}
	for _, db := range dbs {
		before[db] = savePair(t, dir, bug0760Slot(t, db), ids[db])
	}
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "a1")
	bug0760Push(t, node, "other", "b1", "b2")
	bug0760Push(t, node, u30iDB, "a2")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	for _, db := range dbs {
		rewindPair(t, dir, before[db])
	}
	reopened, atScan, release := openHeld(t, dir)
	release()
	if atScan != 0 {
		t.Fatalf("StateSeq at the replay is %d, want 0", atScan)
	}
	want := map[string][]string{u30iDB: {"a1", "a2"}, "other": {"b1", "b2"}}
	for _, db := range dbs {
		got := bug0760Queue(t, reopened, db)
		if len(got) != len(want[db]) || got[0] != want[db][0] || got[len(got)-1] != want[db][len(got)-1] {
			t.Fatalf("Queue of %s after the restart = %v, want %v", db, got, want[db])
		}
	}
}

// TestARestartReplaysAnUpsertOnlyTailWithoutWaitingForTheState (BUG-0760): a tail with
// scalar writes needs no declaration, so it replays at the boot, whatever the state.
func TestARestartReplaysAnUpsertOnlyTailWithoutWaitingForTheState(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	slot := bug0760Slot(t, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	before := savePair(t, dir, slot, dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Retry(t, "Update", func() error {
		_, err := node.Update(context.Background(), UpdateRequest{Database: u30iDB, Bucket: "Box", ID: "b1", Set: bson.D{{Key: "Name", Value: "two"}}})
		return err
	})
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)
	reopened, atScan, release := openHeld(t, dir)
	release()
	if atScan != 0 {
		t.Fatalf("StateSeq at the replay is %d, want 0", atScan)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		doc, err := reopened.Get(context.Background(), u30iDB, "Box", "b1")
		if err == nil && u30iString(t, doc, "Name") == "two" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Name after the restart: %v, %v; want the replayed update", doc, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// bug0760Slot is the slot of Box b1 in database db.
func bug0760Slot(t *testing.T, db string) uint16 {
	t.Helper()
	slot, err := PlacementOf(db, "Box", "b1")
	if err != nil {
		t.Fatal(err)
	}
	return slot
}
