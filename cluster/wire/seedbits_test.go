package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// withBitsOf ORs node's committed SEEDED bits for the given slots of reconDB into the
// state, in one command, the way the reporter's CommandSeeded does.
func withBitsOf(t *testing.T, st *cluster.ClusterState, node string, slots ...int) {
	t.Helper()
	bits := make([]byte, machine.BitmapBytes)
	for _, slot := range slots {
		bits[slot/8] |= 1 << (slot % 8)
	}
	apply(t, st, cluster.CommandSeeded, bsonDoc(bsonStr("node", node), bsonI64("db_id", int64(reconDB)), bsonBin("bits", bits)))
}

// withBitOf sets node's committed SEEDED bit for one slot of reconDB.
func withBitOf(t *testing.T, st *cluster.ClusterState, node string, slot int) {
	t.Helper()
	withBitsOf(t, st, node, slot)
}

// clearBitOf clears node's committed SEEDED bit for one slot of reconDB, the way a
// RETIRE does.
func clearBitOf(t *testing.T, st *cluster.ClusterState, node string, slot int) {
	t.Helper()
	bits := make([]byte, machine.BitmapBytes)
	bits[slot/8] |= 1 << (slot % 8)
	apply(t, st, cluster.CommandRetire, bsonDoc(bsonStr("node", node), bsonI64("db_id", int64(reconDB)), bsonBin("bits", bits)))
}

// placingSlots lists the slots of reconDB where node is a copy in its placing set (a
// joining node ranks itself, as the gate does).
func placingSlots(st cluster.ClusterState, node string) []int {
	table := reconcilePlacingTable(st, node)
	var slots []int
	for slot := range storage.SlotCount {
		for _, copyOf := range table.Copies(uint16(slot), 2) {
			if copyOf.ID == node {
				slots = append(slots, slot)
				break
			}
		}
	}
	return slots
}

func TestTheActivationGateNeedsEveryBitOfThePlacingSet(t *testing.T) {
	st := joiningNodes(t)
	placing := placingSlots(st, testSelf)
	if len(placing) == 0 {
		t.Fatal("the joiner places no pairs, so the gate has nothing to read")
	}

	gap := activationGap(st, testSelf)
	if gap.Total != len(placing) || gap.Missing != gap.Total {
		t.Fatalf("no bits: Missing %d of Total %d, want all %d placing pairs missing", gap.Missing, gap.Total, len(placing))
	}
	if gap.Database != "app" {
		t.Fatalf("no bits: Database %q, want the first database %q", gap.Database, "app")
	}

	// The reporter commits one slot at a time, so one bit must count on its own.
	withBitOf(t, &st, testSelf, placing[0])
	if gap := activationGap(st, testSelf); gap.Missing != gap.Total-1 {
		t.Fatalf("one placing bit set: Missing %d of Total %d, want one fewer", gap.Missing, gap.Total)
	}

	withBitsOf(t, &st, testSelf, placing...)
	if gap := activationGap(st, testSelf); gap.Missing != 0 {
		t.Fatalf("every placing bit set: Missing %d, want 0", gap.Missing)
	}

	clearBitOf(t, &st, testSelf, placing[0])
	gap = activationGap(st, testSelf)
	if gap.Missing != 1 || gap.Database != "app" {
		t.Fatalf("one placing bit cleared: Missing %d (Database %q), want exactly 1 in %q", gap.Missing, gap.Database, "app")
	}
}

func TestABitOfAnotherNodeIsNotTheJoinersBit(t *testing.T) {
	st := joiningNodes(t)
	placing := placingSlots(st, testSelf)
	every := make([]int, storage.SlotCount)
	for slot := range every {
		every[slot] = slot
	}
	withBitsOf(t, &st, "n1", every...)

	gap := activationGap(st, testSelf)
	if gap.Total != len(placing) || gap.Missing != gap.Total {
		t.Fatalf("n1 holds every bit and the joiner none: Missing %d of Total %d, want all %d missing", gap.Missing, gap.Total, len(placing))
	}
}

func TestTheBitReaderHoldsOnlyWhatTheRowSays(t *testing.T) {
	const slot = 5
	key := seedRowKey{node: "n2", db: 1}
	bitsOf := func(slots ...int) []byte {
		bits := make([]byte, machine.BitmapBytes)
		for _, s := range slots {
			bits[s/8] |= 1 << (s % 8)
		}
		return bits
	}
	cases := []struct {
		name string
		rows seedBitRows
		want bool
	}{
		{"no row for the node and database", seedBitRows{}, false},
		{"a row without the slot's bit", seedBitRows{key: bitsOf(6)}, false},
		{"a row with the slot's bit", seedBitRows{key: bitsOf(slot)}, true},
		{"a row shorter than the bitmap", seedBitRows{key: []byte{0xff}}, false},
	}
	for _, c := range cases {
		if got := c.rows.Holds("n2", 1, slot); got != c.want {
			t.Errorf("%s: Holds = %v, want %v", c.name, got, c.want)
		}
	}
}
