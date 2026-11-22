package wire

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dropSyncBound is the most store syncs one drop may make on a node: one for the records' pre-pass
// (8,192 records of about 40 bytes fit one batch), and a little room for whatever else the drop
// syncs. One per held slot, about 8,192, is the bug.
const dropSyncBound = 4

// TestADropSyncsTheStoreOnceNotOncePerSlot is BUG-0861: a drop on a node that holds every slot
// used to sync the store once per slot (about 8,192 times), because SweepBucket wrote each missing
// 'b' record synced, alone. The drop writes them all in one synced pre-pass.
//
// The records are missing for the slots the node came to hold after the drop's bump (the bump's
// note wrote one per slot it held then): the drop dies after its bump, a listing makes the node hold
// every slot it owns, and the drop is run again, as TestADroppingBucketRefusesReadsAndWrites does.
func TestADropSyncsTheStoreOnceNotOncePerSlot(t *testing.T) {
	rig := newU42Rig(t)
	rig.pause()
	rig.crashAt(u42Bump, 1)
	if err := rig.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v, want the crash after the bump", err)
	}
	if _, err := rig.node.List(context.Background(), u30iDB, "Order", nil, 0); err != nil {
		t.Fatal(err)
	}
	if held := len(rig.node.pairs.HeldSlots(rig.dbID)); held < 8000 {
		t.Fatalf("SCENARIO NOT REACHED: the node holds %d slots, want it to hold them all", held)
	}
	rig.install()
	before, _, _ := rig.node.kv.VouchedCounts()
	rig.mustRun()
	after, _, _ := rig.node.kv.VouchedCounts()
	if syncs := after - before; syncs > dropSyncBound {
		t.Fatalf("a drop of a bucket on a node holding every slot made %d store syncs, want at most %d: one per slot is BUG-0861", syncs, dropSyncBound)
	}
}

// TestADropOnANodeWhoseLoopIsRunningSyncsOnceNotOncePerSlot is the same bug as a real node meets it:
// the bump's note queued a record for every pair held then, and the background loop sweeps and removes
// them while the drop's own sweep runs, so the drop finds most records gone. Writing each one back,
// synced, before sweeping its slot (the first form of the fix) still cost one sync per held slot.
func TestADropOnANodeWhoseLoopIsRunningSyncsOnceNotOncePerSlot(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.CreateBucket(ctx, u30iDB, "Many", Field{Name: "N", Type: "int"}); err != nil {
		t.Fatal(err)
	}
	dbID, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 80 {
		doc := bson.D{{Key: "_id_", Value: fmt.Sprintf("doc%03d", i)}, {Key: "N", Value: int32(i)}}
		if _, err := node.Insert(ctx, u30iDB, "Many", doc); err != nil {
			t.Fatal(err)
		}
	}
	if held := len(node.pairs.HeldSlots(dbID)); held < 60 {
		t.Fatalf("SCENARIO NOT REACHED: the node holds %d slots, want the documents to have spread over at least 60", held)
	}
	before, _, _ := node.kv.VouchedCounts()
	if err := node.DropBucket(ctx, u30iDB, "Many"); err != nil {
		t.Fatal(err)
	}
	after, _, _ := node.kv.VouchedCounts()
	if syncs := after - before; syncs > 12 {
		t.Fatalf("a drop across %d held slots made %d store syncs, want a number that does not grow with the slots (at most 12)", len(node.pairs.HeldSlots(dbID)), syncs)
	}
}
