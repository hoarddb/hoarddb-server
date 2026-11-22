package harness_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Seeding slice S0 end to end (the entry ring seeding completion spec §0): one record
// shape on every cluster node, and an active node that reconciles.

// heldSlots lists the slots node id holds at the committed state of database dbName.
func heldSlots(t *testing.T, c *harness.Cluster, id, dbName string) (slots []uint16, dbID uint64) {
	t.Helper()
	state := c.Node(id).State()
	table := cluster.BuildTable(state)
	var rf int
	for _, database := range state.Databases {
		if database.Name == dbName {
			dbID, rf = uint64(database.ID), int(database.RF)
		}
	}
	if rf == 0 {
		t.Fatalf("database %q is not in %s's committed state", dbName, id)
	}
	for slot := range uint16(storage.SlotCount) {
		for _, member := range table.Copies(slot, rf) {
			if member.ID == id {
				slots = append(slots, slot)
				break
			}
		}
	}
	return slots, dbID
}

// awaitRecords waits until every slot node id holds has a ready record: the first reconcile
// pass after a restart writes the interim for the idle ones.
func awaitRecords(t *testing.T, c *harness.Cluster, id string, slots []uint16, dbID uint64) {
	t.Helper()
	c.Await(fmt.Sprintf("every pair %s holds to have a ready record", id), 30*time.Second, func() bool {
		for _, slot := range slots {
			if c.Node(id).SlotRecordState(slot, dbID) != machine.SlotReady {
				return false
			}
		}
		return true
	})
}

// positionOn asks node asker for id's position of the pair, asking again when the pooled
// connection to a just-restarted node answers with its closed socket (as the activation gate
// does).
func positionOn(t *testing.T, c *harness.Cluster, asker, id string, slot uint16, dbID uint64) wire.PositionReply {
	t.Helper()
	var reply wire.PositionReply
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if reply, err = c.Node(asker).PositionOn(proofCtx(t), id, slot, dbID); err == nil {
			return reply
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("asking %s for %s's position of slot %d: %v", asker, id, slot, err)
	return reply
}

// TestANodeBootsWithEveryPairRecordedAndReconciled: a three-node cluster with data, every
// node restarted: every held pair has a record whose state is ready and the pair answers
// ready to POSITION_QUERY; the applied position is asserted for the pairs the owner path
// wrote (a copy's seed record is not advanced by replication until the BUG-0752 fix, and a
// copy's position after a restart is the replay's). [BREAK 1: keep the joining guard in
// driveSeeding: a restarted node's idle pairs stay absent and answer seeding. BREAK 2: drop
// the LogApplied gate in driveSeeding: the first pass after a restart runs against an
// intermediate state of the re-applied Raft log, in which the database does not exist, and
// retires every held pair. The loader rewrite's own break is caught by
// TestAThirtyTwoByteRecordIsRewrittenAsReadyAtBoot, because the first pass's interim copies a
// 32-byte record's live position and hides it here]
func TestANodeBootsWithEveryPairRecordedAndReconciled(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	type written struct {
		id, owner string
		pos       cluster.Position
	}
	var writes []written
	for i := range 3 {
		id, owner, _, _ := proofID(t, c, fmt.Sprintf("boot-%d", i))
		got := proofInsert(t, c, owner, id, "Ada")
		writes = append(writes, written{id, owner, got.Pos})
	}
	for _, id := range c.IDs() {
		c.Kill(id)
		c.Restart(id)
	}
	for _, id := range c.IDs() {
		slots, dbID := heldSlots(t, c, id, proofDB)
		awaitRecords(t, c, id, slots, dbID)
		// POSITION_QUERY on a sample of the held pairs, asked by another node.
		asker := c.IDs()[0]
		if asker == id {
			asker = c.IDs()[1]
		}
		for i, slot := range slots {
			if i%64 != 0 {
				continue
			}
			reply := positionOn(t, c, asker, id, slot, dbID)
			if reply.State != wire.PositionStateReady {
				t.Fatalf("%s answers POSITION_QUERY for slot %d with state %d, want ready", id, slot, reply.State)
			}
		}
	}
	for _, w := range writes {
		slot, err := wire.PlacementOf(proofDB, proofBucket, w.id)
		if err != nil {
			t.Fatal(err)
		}
		_, dbID := heldSlots(t, c, w.owner, proofDB)
		if state := c.Node(w.owner).SlotRecordState(slot, dbID); state != machine.SlotReady {
			t.Fatalf("after the restart the owner %s holds %s's pair in state %v, want ready", w.owner, w.id, state)
		}
		pos, err := c.Node(w.owner).PairPosition(proofCtx(t), proofDB, proofBucket, w.id)
		if err != nil || pos != w.pos {
			t.Fatalf("after the restart the owner %s is at %s for %s's pair (err %v), want %s", w.owner, pos, w.id, err, w.pos)
		}
		if _, err := c.Node(w.owner).Get(proofCtx(t), proofDB, proofBucket, w.id); err != nil {
			t.Fatalf("after the restart %s cannot read %s: %v", w.owner, w.id, err)
		}
	}
}

// TestAHandoffIsNotStalledByIdlePairsBeforeTheFirstPass (plan §0.2, the one window): a
// handoff takes a remote copy's position as evidence only from a ready reply, and an idle
// record-less pair on a restarted copy answers seeding until that copy's first pass writes
// its interim. At RF 3 a failover makes the rank-2 copy the owner, which must hear from the
// other copy (need = 2, itself and one more), so a copy restarted before the failover must
// have reconciled or the handoff waits on errCopyNotReady for ever. The window is one pass:
// the copy's first pass after its restart. [BREAK: never run the first pass on the copy]
func TestAHandoffIsNotStalledByIdlePairsBeforeTheFirstPass(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, WCTimeout: 5 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "rf3", 3); err != nil {
		t.Fatalf("creating the RF 3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "rf3", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	owner, copies, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), "rf3", proofBucket, "stall-1")
	if err != nil || len(copies) != 3 {
		t.Fatalf("placing stall-1: owner %s copies %v err %v, want three copies", owner, copies, err)
	}
	next, far := copies[1], copies[2]
	if _, err := c.Node(owner).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: "stall-1"}, {Key: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("insert through the owner %s: %v", owner, err)
	}
	// The far copy restarts: its pairs come back record-less (a copy persists no record until
	// the BUG-0752 fix) and answer seeding until its first pass writes the interim.
	c.Kill(far)
	restarted := time.Now()
	c.Restart(far)
	slots, dbID := heldSlots(t, c, far, "rf3")
	awaitRecords(t, c, far, slots, dbID)
	passWindow := time.Since(restarted)

	epochBefore := c.Node(next).State().Epoch
	c.Kill(owner)
	c.Await("the survivors to commit the owner down", 40*time.Second, func() bool {
		return c.Node(next).State().Epoch > epochBefore && c.Node(far).State().Epoch > epochBefore
	})
	c.AwaitLeader(10 * time.Second)
	if now, _, err := c.Node(next).Owner(proofCtx(t), "rf3", proofBucket, "stall-1"); err != nil || now != next {
		t.Fatalf("after the failover the owner is %q (err %v), want %s", now, err, next)
	}
	// The handoff opens the pair once the far copy's position is evidence: the write lands.
	deadline := time.Now().Add(quorumFailoverBudget)
	for {
		_, err := c.Node(next).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: "stall-2"}, {Key: "name", Value: "Bea"}})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the new owner %s still held the pair %s after the failover: %v (far copy's pass took %s after its restart)", next, quorumFailoverBudget, err, passWindow)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := c.Node(next).Get(context.Background(), "rf3", proofBucket, "stall-1"); err != nil {
		t.Fatalf("the new owner %s cannot read the record written before the failover: %v", next, err)
	}
	t.Logf("the far copy's first pass after its restart took %s; the handoff then completed", passWindow)
}

// forgetPair removes the pair's record and range from a killed node's Badger: the store of
// an owner whose first batch never committed, while its log still holds the pair's records.
func forgetPair(t *testing.T, c *harness.Cluster, id string, slot uint16, dbID uint64) {
	t.Helper()
	kv, err := storage.OpenBadgerKV(filepath.Join(c.DataDir(id), "badger"), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening %s's Badger: %v", id, err)
	}
	defer func() { _ = kv.Close() }()
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(wire.SeedRecordKey(slot, dbID)); err != nil {
		t.Fatal(err)
	}
	snapshot := kv.NewSnapshot()
	prefix := storage.PairPrefix(slot, dbID)
	end := append(append([]byte(nil), prefix...), 0xFF, 0xFF, 0xFF, 0xFF)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: end})
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

// TestAMultiNodeBootReplayOfARecordlessPairOrdersAfterTheLog (pi B-1, the production shape):
// a whole three-node RF 2 cluster restarts, the owner of a pair whose first batch never
// committed first. Its boot replay runs while no majority exists, so its FSM cannot have
// applied anything and the committed state is epoch 0 at tail.Scan (forced, not raced: the
// other two nodes are down until the owner's open has returned; a follower rejoining a live
// leader would instead apply within a heartbeat). The append must be learned from the
// record's own epoch, so that the next order after the restart is e:N+1, never a position
// the log and the copy already hold. [BREAK: raise only when rec.Pos.Epoch == committed]
func TestAMultiNodeBootReplayOfARecordlessPairOrdersAfterTheLog(t *testing.T) {
	// RF 1: the pair has no copy for the restart's catch-up to repair the owner from, so the
	// position after the restart is what the owner's own replay learned.
	const db = "rf1"
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), db, 1); err != nil {
		t.Fatalf("creating the RF 1 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), db, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	var ids []string
	var owner string
	var slot uint16
	for i := 0; i < 200000 && len(ids) < 3; i++ {
		id := fmt.Sprintf("multi-%d", i)
		o, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), db, proofBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		s, err := wire.PlacementOf(db, proofBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) == 0 {
			owner, slot = o, s
		}
		if s == slot {
			ids = append(ids, id)
		}
	}
	if len(ids) < 3 {
		t.Fatal("no three ids in one slot")
	}
	_, dbID := heldSlots(t, c, owner, db)
	insert := func(id, name string) wire.Inserted {
		t.Helper()
		deadline := time.Now().Add(quorumFailoverBudget)
		for {
			got, err := c.Node(owner).Insert(proofCtx(t), db, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: name}})
			if err == nil {
				return got
			}
			if time.Now().After(deadline) {
				t.Fatalf("insert of %s through %s: %v", id, owner, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	insert(ids[0], "one")
	last := insert(ids[1], "two")
	others := []string{}
	for _, id := range c.IDs() {
		if id != owner {
			others = append(others, id)
		}
	}
	for _, id := range c.IDs() {
		c.Kill(id)
	}
	forgetPair(t, c, owner, slot, dbID)
	// Restart blocks until the cluster converges, which needs a majority: the owner opens
	// alone, its open returns (it is then Running) with its replay done against epoch 0, and
	// only then do the others come up.
	ownerUp := make(chan struct{})
	go func() { defer close(ownerUp); c.Restart(owner) }()
	c.Await("the owner's open to return while it is alone", 30*time.Second, func() bool {
		for _, id := range c.Running() {
			if id == owner {
				return true
			}
		}
		return false
	})
	for _, id := range others {
		id := id
		go func() { c.Restart(id) }()
	}
	<-ownerUp
	c.AwaitConverged(30 * time.Second)
	c.AwaitLeases(30 * time.Second)
	if got := insert(ids[2], "three"); got.Pos != (cluster.Position{Epoch: last.Pos.Epoch, Seq: last.Pos.Seq + 1}) {
		t.Fatalf("the next order after the restart took %s, want %d:%d: the log holds %s", got.Pos, last.Pos.Epoch, last.Pos.Seq+1, last.Pos)
	}
}
