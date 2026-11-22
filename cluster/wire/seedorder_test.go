package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// orderBits is a 1024-byte bitmap with the given slots set.
func orderBits(slots ...uint16) []byte {
	bits := make([]byte, 1024)
	for _, slot := range slots {
		bits[slot/8] |= 1 << (slot % 8)
	}
	return bits
}

// orderCopies returns the two members that hold slot at RF 2 in the committed state.
func orderCopies(t *testing.T, state cluster.ClusterState, table *cluster.SlotTable, slot uint16) []string {
	t.Helper()
	copies := table.Copies(slot, 2)
	if len(copies) != 2 {
		t.Fatalf("slot %d has %d copies at RF 2, want 2", slot, len(copies))
	}
	return []string{copies[0].ID, copies[1].ID}
}

// orderOutsider returns a member that is not a copy of slot.
func orderOutsider(t *testing.T, state cluster.ClusterState, copies []string) string {
	t.Helper()
	for _, member := range state.Members {
		if member.ID != copies[0] && member.ID != copies[1] {
			return member.ID
		}
	}
	t.Fatalf("no member outside %v", copies)
	return ""
}

func TestADeficitCountsTheCopySetsBitsFromTheCommittedState(t *testing.T) {
	state := threeNodes(t)
	table := cluster.BuildTable(state)
	const slot = uint16(5)
	copies := orderCopies(t, state, table, slot)
	outsider := orderOutsider(t, state, copies)

	row := func(node string, bits []byte) cluster.SeedBitmap {
		return cluster.SeedBitmap{Node: node, DBID: int64(reconDB), Bits: bits}
	}

	state.SeedBitmaps = nil
	if got := seedDeficit(state, table, slot, reconDB); got != 2 {
		t.Fatalf("no bit set: deficit %d, want 2", got)
	}

	state.SeedBitmaps = []cluster.SeedBitmap{row(copies[0], orderBits(slot))}
	if got := seedDeficit(state, table, slot, reconDB); got != 1 {
		t.Fatalf("one copy holds the pair: deficit %d, want 1", got)
	}

	state.SeedBitmaps = []cluster.SeedBitmap{row(copies[0], orderBits(slot)), row(copies[1], orderBits(slot))}
	if got := seedDeficit(state, table, slot, reconDB); got != 0 {
		t.Fatalf("both copies hold the pair: deficit %d, want 0", got)
	}

	// A member outside the copy set that has the bit does not make up for a missing copy.
	state.SeedBitmaps = []cluster.SeedBitmap{row(copies[0], orderBits(slot)), row(outsider, orderBits(slot))}
	if got := seedDeficit(state, table, slot, reconDB); got != 1 {
		t.Fatalf("a bit outside the copy set was counted: deficit %d, want 1", got)
	}

	// A row that is not a valid bitmap counts as clear.
	state.SeedBitmaps = []cluster.SeedBitmap{row(copies[0], orderBits(slot)), row(copies[1], []byte{0xff})}
	if got := seedDeficit(state, table, slot, reconDB); got != 1 {
		t.Fatalf("a short bitmap row: deficit %d, want 1", got)
	}

	// A bit for another slot is not this slot's bit.
	state.SeedBitmaps = []cluster.SeedBitmap{row(copies[0], orderBits(slot+1))}
	if got := seedDeficit(state, table, slot, reconDB); got != 2 {
		t.Fatalf("another slot's bit was counted: deficit %d, want 2", got)
	}

	// A database that is not in the state has nothing missing.
	if got := seedDeficit(state, table, slot, reconDB+99); got != 0 {
		t.Fatalf("an unknown database: deficit %d, want 0", got)
	}
}

// orderRanked is a ranked queue that holds the pairs and was ranked by deficits.
func orderRanked(deficits map[reconcileKey]int, keys ...reconcileKey) *pendingQueue {
	q := newPendingQueue(true)
	for _, key := range keys {
		q.Add(key)
	}
	q.Rerank(1, func(slot uint16, db uint64) int { return deficits[reconcileKey{slot, db}] })
	return q
}

// orderDrain takes every pair the queue holds, in order.
func orderDrain(q *pendingQueue) []reconcileKey {
	var taken []reconcileKey
	for {
		key, ok := q.Take()
		if !ok {
			return taken
		}
		taken = append(taken, key)
	}
}

func orderWant(t *testing.T, got, want []reconcileKey) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("took %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("took %v, want %v", got, want)
		}
	}
}

func TestTheMostUnderReplicatedPairIsTakenFirst(t *testing.T) {
	zero, two, one := reconcileKey{1, 1}, reconcileKey{2, 1}, reconcileKey{3, 1}
	q := orderRanked(map[reconcileKey]int{zero: 0, two: 2, one: 1}, zero, two, one)
	orderWant(t, orderDrain(q), []reconcileKey{two, one, zero})
}

func TestEqualDeficitsTakeSlotThenDatabaseAscending(t *testing.T) {
	// Added in an order that is neither slot nor database ascending.
	a, b, c, d := reconcileKey{9, 2}, reconcileKey{4, 7}, reconcileKey{9, 1}, reconcileKey{4, 3}
	q := orderRanked(map[reconcileKey]int{a: 1, b: 1, c: 1, d: 1}, a, b, c, d)
	orderWant(t, orderDrain(q), []reconcileKey{d, b, c, a})
}

func TestAnUnrankedQueueIsFirstInFirstOut(t *testing.T) {
	q := newPendingQueue(false)
	arrivals := []reconcileKey{{30, 1}, {20, 1}, {10, 1}, {20, 0}}
	for _, key := range arrivals {
		q.Add(key)
	}
	if q.Len() != len(arrivals) {
		t.Fatalf("queue holds %d, want %d", q.Len(), len(arrivals))
	}
	orderWant(t, orderDrain(q), arrivals)
	if q.Len() != 0 {
		t.Fatalf("a drained queue holds %d", q.Len())
	}
}

func TestAnAddedPairMakesARankedQueueStaleUntilReranked(t *testing.T) {
	key := reconcileKey{1, 1}
	rank := func(slot uint16, db uint64) int { return 0 }

	q := newPendingQueue(true)
	q.Add(key)
	if !q.Stale(1) {
		t.Fatal("a ranked queue that was never ranked and holds a pair is not stale")
	}
	q.Rerank(1, rank)
	if q.Stale(1) {
		t.Fatal("a ranked queue is stale right after Rerank at the same state_seq")
	}
	if !q.Stale(2) {
		t.Fatal("a moved state_seq did not make the ranked queue stale")
	}
	q.Add(reconcileKey{2, 1})
	if !q.Stale(1) {
		t.Fatal("an added pair did not make the ranked queue stale")
	}
	q.Rerank(1, rank)
	if q.Stale(1) {
		t.Fatal("a ranked queue is stale after a second Rerank")
	}

	unranked := newPendingQueue(false)
	unranked.Add(key)
	if unranked.Stale(1) || unranked.Stale(2) {
		t.Fatal("an unranked queue is never stale")
	}
}
