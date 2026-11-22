package wire

import (
	"maps"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// gainersLeaver is n3, the member of threeNodes that leaves; at RF 2 it holds about two
// slots in three, and its loss gives each of those pairs one new copy.
const gainersLeaver = "n3"

// leaveTables builds the committed table and the table without the leaver, the same two
// tables gainersReady compares.
func leaveTables(st cluster.ClusterState, leaving string) (now, after *cluster.SlotTable) {
	next := st
	next.Members = slices.DeleteFunc(slices.Clone(st.Members), func(m cluster.Member) bool { return m.ID == leaving })
	return cluster.BuildTable(st), cluster.BuildTable(next)
}

// copyIDs lists the copy set of slot at RF 2, in rank order.
func copyIDs(table *cluster.SlotTable, slot int) []string {
	var ids []string
	for _, m := range table.Copies(uint16(slot), 2) {
		ids = append(ids, m.ID)
	}
	return ids
}

// leaverSlots lists the slots of reconDB where leaving is a copy at RF 2 in table now.
func leaverSlots(now *cluster.SlotTable, leaving string) []int {
	var slots []int
	for slot := range storage.SlotCount {
		if slices.Contains(copyIDs(now, slot), leaving) {
			slots = append(slots, slot)
		}
	}
	return slots
}

// gainsBySlots groups, per member of the next copy set, the slots of the leaver's pairs
// where that member is in the copy set without the leaver.
func gainsBySlots(after *cluster.SlotTable, slots []int) map[string][]int {
	gains := make(map[string][]int)
	for _, slot := range slots {
		for _, id := range copyIDs(after, slot) {
			gains[id] = append(gains[id], slot)
		}
	}
	return gains
}

func TestGainersReadyReadsTheBits(t *testing.T) {
	st := threeNodes(t)
	now, after := leaveTables(st, gainersLeaver)
	slots := leaverSlots(now, gainersLeaver)
	if len(slots) == 0 {
		t.Fatal("n3 holds no pair at RF 2, so its leaving gains nothing to check")
	}

	gap, ready := gainersReady(st, gainersLeaver)
	if ready {
		t.Fatal("no bits: ready, want the first gainer's pair named as missing")
	}
	if !slices.Contains(slots, int(gap.Slot)) || !slices.Contains(copyIDs(after, int(gap.Slot)), gap.Member) {
		t.Fatalf("no bits: gap %+v names a slot or member that gains nothing from n3 leaving", gap)
	}

	gains := gainsBySlots(after, slots)
	for _, id := range slices.Sorted(maps.Keys(gains)) {
		withBitsOf(t, &st, id, gains[id]...)
	}
	if gap, ready := gainersReady(st, gainersLeaver); !ready {
		t.Fatalf("every gainer holds its bit: not ready, gap %+v", gap)
	}

	first := slots[0]
	member := copyIDs(after, first)[0]
	clearBitOf(t, &st, member, first)
	gap, ready = gainersReady(st, gainersLeaver)
	want := gainerGap{Database: "app", Slot: uint16(first), Member: member}
	if ready || gap != want {
		t.Fatalf("one gainer bit cleared: ready %v, gap %+v, want not ready at %+v", ready, gap, want)
	}
}

func TestGainersReadyIgnoresTheLeaversOwnBit(t *testing.T) {
	st := threeNodes(t)
	now, _ := leaveTables(st, gainersLeaver)
	slots := leaverSlots(now, gainersLeaver)

	withBitsOf(t, &st, gainersLeaver, slots...)
	if _, ready := gainersReady(st, gainersLeaver); ready {
		t.Fatal("only the leaver's own bits set: ready, want not ready")
	}

	// Each pair's other copy holds its bit too. The gainer is the one copy the leaver's
	// loss adds and holds nothing, so the pair is still not ready. A leaver counted as its
	// own next copy would find both holders and say ready.
	others := make(map[string][]int)
	for _, slot := range slots {
		for _, id := range copyIDs(now, slot) {
			if id != gainersLeaver {
				others[id] = append(others[id], slot)
			}
		}
	}
	for _, id := range slices.Sorted(maps.Keys(others)) {
		withBitsOf(t, &st, id, others[id]...)
	}
	gap, ready := gainersReady(st, gainersLeaver)
	if ready {
		t.Fatal("the leaver and the other copy hold their bits, the gainer does not: ready, want not ready")
	}
	if gap.Member == gainersLeaver || slices.Contains(copyIDs(now, int(gap.Slot)), gap.Member) {
		t.Fatalf("gap %+v names the leaver or a member that already holds the pair, want the gainer", gap)
	}
}

func TestGainersReadyOfAMemberThatHoldsNothingIsReady(t *testing.T) {
	st := threeNodes(t)
	apply(t, &st, cluster.CommandRegisterMember, bsonDoc(bsonStr("id", "w1"), bsonI64("incarnation", 1),
		bsonStr("data_addr", "w1:1"), bsonBool("witness", true), bsonBin("placing_set", make([]byte, 1024)), bsonLevels()))

	for _, leaving := range []string{"ghost", "w1"} {
		if gap, ready := gainersReady(st, leaving); !ready {
			t.Errorf("%s is not a ranked member, so it holds nothing: not ready, gap %+v", leaving, gap)
		}
	}
}
