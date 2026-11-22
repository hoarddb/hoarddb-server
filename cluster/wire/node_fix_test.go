package wire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// u30iStaticState is a StateSource that always reports one fixed cluster state.
type u30iStaticState struct{ state cluster.ClusterState }

// State implements StateSource.
func (s u30iStaticState) State() cluster.ClusterState { return s.state }

func u30iMembers(ids ...string) []cluster.Member {
	members := make([]cluster.Member, len(ids))
	for i, id := range ids {
		members[i] = cluster.Member{ID: id, State: cluster.MemberActive}
	}
	return members
}

func TestNodeClaimRefusesACountThatIsNotAPositiveU16(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	if err := node.Push(ctx, PushRequest{StoreTarget: u30iTarget("u1", "Purchases"), Element: bson.D{{Key: "item", Value: "book"}}}); err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{0, -1, 65536, 1 << 40} {
		before := node.pairs.Snapshot(node.slotOf(u30iDB, "User", "u1"), 1)
		_, err := node.Claim(ctx, ClaimRequest{StoreTarget: u30iTarget("u1", "Purchases"), Count: count})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if after := node.pairs.Snapshot(node.slotOf(u30iDB, "User", "u1"), 1); after.AppendedSeq != before.AppendedSeq {
			t.Fatalf("a claim of %d took a position (%d -> %d); a refused count must write nothing", count, before.AppendedSeq, after.AppendedSeq)
		}
	}
	lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: u30iTarget("u1", "Purchases"), Count: 65535})
	if err != nil || len(lease.Elements) != 1 {
		t.Fatalf("Claim of the largest u16 count = %+v, %v, want the one element there is", lease, err)
	}
}

func TestNodeClaimOfAnEmptyFieldReturnsNoLease(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: u30iTarget("u1", "Purchases"), Count: 3})
	if err != nil || lease.ID != ([16]byte{}) || len(lease.Elements) != 0 {
		t.Fatalf("Claim of an empty field = %+v, %v, want the zero lease and no elements", lease, err)
	}
}

func TestNodeUpsertOverATombstoneKeepsOnlyTheNewFields(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	if err := node.Delete(ctx, u30iDB, "User", "u1"); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Upsert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: "u1"}, {Key: "City", Value: "Rome"}}); err != nil {
		t.Fatalf("Upsert over a deleted document: %v", err)
	}
	doc, err := node.Get(ctx, u30iDB, "User", "u1")
	if err != nil {
		t.Fatalf("Get after the upsert: %v", err)
	}
	if got := u30iString(t, doc, "City"); got != "Rome" {
		t.Fatalf("City = %q, want Rome", got)
	}
	for _, field := range []string{"Name", "Age"} {
		if _, err := bson.Raw(doc.Body).LookupErr(field); err == nil {
			t.Fatalf("field %s of the deleted document came back through the upsert: %s", field, bson.Raw(doc.Body))
		}
	}
}

// u30iCountingSlots counts the slot-state lookups the read gate makes.
type u30iCountingSlots struct {
	inner SlotStates
	calls *atomic.Int64
}

// State implements SlotStates.
func (c u30iCountingSlots) Serves(slot uint16, db uint64) bool {
	return c.State(slot, db) == machine.SlotReady
}

func (c u30iCountingSlots) State(slot uint16, db uint64) machine.SlotState {
	c.calls.Add(1)
	return c.inner.State(slot, db)
}

func TestNodeListAdmitsItsSlotsOncePerCallNotPerPage(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	for i := 0; i < listPageDocs*2+50; i++ {
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(fmt.Sprintf("u%d", i), "Ada", int32(i))); err != nil {
			t.Fatal(err)
		}
	}
	var calls atomic.Int64
	gate := node.scanner.cfg.Gate
	gate.cfg.Slots = u30iCountingSlots{inner: gate.cfg.Slots, calls: &calls}
	if _, err := node.List(ctx, u30iDB, "User", nil, 1); err != nil {
		t.Fatal(err)
	}
	onePage := calls.Swap(0)
	listed, err := node.List(ctx, u30iDB, "User", nil, 0)
	if err != nil || len(listed) != listPageDocs*2+50 {
		t.Fatalf("List = %d, %v, want %d documents over three pages", len(listed), err, listPageDocs*2+50)
	}
	if allPages := calls.Load(); onePage == 0 || allPages != onePage {
		t.Fatalf("slot-state lookups: %d for a one-page list, %d for a three-page list; admission must run once per call", onePage, allPages)
	}
}

func TestOwnedSlotsAreTheSlotsTheTableGivesThisNode(t *testing.T) {
	state := cluster.ClusterState{Epoch: 1, StateSeq: 1, Members: u30iMembers("n1", "n2", "n3"), Databases: []cluster.Database{{Name: "app", ID: 1, RF: 1, SchemaVersion: 1}}}
	topo := NewClusterTopology("n1", u30iStaticState{state})
	mask := ownedSlots(topo, 1)
	owned := 0
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		owner, _ := topo.Table().Owner(slot, 1)
		if mine := owner.ID == "n1"; mine != mask.Has(slot) {
			t.Fatalf("slot %d: owner %s, in the mask %v", slot, owner.ID, mask.Has(slot))
		} else if mine {
			owned++
		}
	}
	if owned == 0 || owned == storage.SlotCount {
		t.Fatalf("n1 owns %d of %d slots among three members, want some but not all", owned, storage.SlotCount)
	}
	alone := ownedSlots(NewClusterTopology("n1", u30iStaticState{cluster.ClusterState{Epoch: 1, StateSeq: 1, Members: u30iMembers("n1"), Databases: state.Databases}}), 1)
	if alone.Count() != storage.SlotCount {
		t.Fatalf("a lone member owns %d slots, want all %d", alone.Count(), storage.SlotCount)
	}
}

func TestSingleVoterSlotsAreReadyOnlyForOneMember(t *testing.T) {
	one := NewClusterTopology("n1", u30iStaticState{cluster.ClusterState{Epoch: 1, StateSeq: 1, Members: u30iMembers("n1")}})
	two := NewClusterTopology("n1", u30iStaticState{cluster.ClusterState{Epoch: 1, StateSeq: 1, Members: u30iMembers("n1", "n2")}})
	if got := (SingleVoterSlots{Topology: one}).State(7, 1); got != machine.SlotReady {
		t.Fatalf("one member: slot state %v, want ready", got)
	}
	if got := (SingleVoterSlots{Topology: two}).State(7, 1); got == machine.SlotReady {
		t.Fatal("two members: a slot reported ready with nothing seeded; this node only knows the one-voter case")
	}
}

// TestNodeReloadsPairRecordsWithoutTheLog: with the log deleted only the pair records can
// supply the applied position and the ordinal, so this pins LoadLocalPairRecords.
func TestNodeReloadsPairRecordsWithoutTheLog(t *testing.T) {
	dir := t.TempDir()
	node, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	u30iSchema(t, node)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1x"+fmt.Sprint(i), "Ada", int32(i))); err != nil {
			t.Fatal(err)
		}
	}
	slot := node.slotOf(u30iDB, "User", "u1x0")
	want := node.pairs.Snapshot(slot, 1)
	if want.AppliedPos.Zero() || want.Ordinal == 0 {
		t.Fatalf("the pair holds %+v before the close, want applied work", want)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "wal")); err != nil {
		t.Fatal(err)
	}
	reopened := u30iOpen(t, dir)
	got := reopened.pairs.Snapshot(slot, 1)
	if got.AppliedPos != want.AppliedPos || got.Ordinal != want.Ordinal {
		t.Fatalf("pair after reopen without a log = applied %s ordinal %d, want applied %s ordinal %d from the pair record",
			got.AppliedPos, got.Ordinal, want.AppliedPos, want.Ordinal)
	}
}

// TestNodeReloadsTheTailCursorOfAPair (BUG-0746): the cursor of a pair's last record is read back
// from its pair record at boot, so a restarted owner can send the tail without walking the log.
// [GUARD: do not copy the cursor in LoadLocalPairRecords]
func TestNodeReloadsTheTailCursorOfAPair(t *testing.T) {
	dir := t.TempDir()
	node, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	u30iSchema(t, node)
	for i := 0; i < 3; i++ {
		if _, err := node.Insert(context.Background(), u30iDB, "User", u30iUser("t1x"+fmt.Sprint(i), "Ada", int32(i))); err != nil {
			t.Fatal(err)
		}
	}
	slot := node.slotOf(u30iDB, "User", "t1x0")
	want := node.pairs.Snapshot(slot, 1)
	if want.TailCursor == 0 {
		t.Fatalf("the owner's pair holds no tail cursor after three writes: %+v", want)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	got := u30iOpen(t, dir).pairs.Snapshot(slot, 1)
	if got.TailCursor != want.TailCursor {
		t.Fatalf("tail cursor after reopen = %d, want %d", got.TailCursor, want.TailCursor)
	}
}
