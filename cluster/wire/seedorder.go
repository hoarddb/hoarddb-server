package wire

import (
	"container/heap"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// rerankMinInterval is how often the puller re-ranks its queue at most. Between re-ranks a state_seq move only marks the
// queue dirty: the order is a heuristic, and a node that seeds thousands of pairs moves state_seq continuously (its own
// SEEDED reports), so re-ranking on every move would leave no time to pull. The first ranking of a queue is immediate.
const rerankMinInterval = time.Second

// seedDeficit is how many copies of the pair the committed state says are missing: rf minus the members of its copy set whose SEEDED bit for the pair is set.
// A database that is not in state, and a row that is not a valid bitmap, count as nothing missing and nothing held. The result is never below 0.
func seedDeficit(state cluster.ClusterState, table *cluster.SlotTable, slot uint16, db uint64) int {
	rf := -1
	for _, database := range state.Databases {
		if database.ID == int64(db) {
			rf = int(database.RF)
			break
		}
	}
	if rf < 0 {
		return 0
	}
	held := 0
	for _, member := range table.Copies(slot, rf) {
		for _, row := range state.SeedBitmaps {
			if row.Node != member.ID || row.DBID != int64(db) {
				continue
			}
			// An invalid row is "clear": the pair is still missing there, so it stays in the deficit.
			if set, err := machine.Bit(row.Bits, slot); err == nil && set {
				held++
			}
			break
		}
	}
	return max(rf-held, 0)
}

// pendingItem is one queued pair with the rank the last Rerank gave it.
type pendingItem struct {
	key     reconcileKey
	deficit int
	arrival uint64
}

// pendingQueue orders queued pairs. Ranked: most missing copies first, then slot ascending, then database ascending. Unranked: arrival order (today's behaviour).
// It is not safe for concurrent use: the Puller's mutex guards it. Its memory is one pendingItem per queued pair, never per bucket.
type pendingQueue struct {
	items    []pendingItem
	ranked   bool
	arrivals uint64
	at       uint64 // the state_seq of the last Rerank
	dirty    bool   // a pair was added since the last Rerank
}

// newPendingQueue returns an empty queue; ranked picks the deficit order over arrival order.
func newPendingQueue(ranked bool) *pendingQueue {
	return &pendingQueue{ranked: ranked}
}

// Len is the number of queued pairs (heap.Interface).
func (q *pendingQueue) Len() int { return len(q.items) }

// Less ranks by deficit, slot and database when ranked, and by arrival otherwise (heap.Interface).
func (q *pendingQueue) Less(i, j int) bool {
	a, b := q.items[i], q.items[j]
	if q.ranked {
		if a.deficit != b.deficit {
			return a.deficit > b.deficit
		}
		if a.key.slot != b.key.slot {
			return a.key.slot < b.key.slot
		}
		if a.key.db != b.key.db {
			return a.key.db < b.key.db
		}
	}
	return a.arrival < b.arrival
}

// Swap exchanges two items (heap.Interface).
func (q *pendingQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }

// Push appends an item; callers use Add (heap.Interface).
func (q *pendingQueue) Push(entry any) { q.items = append(q.items, entry.(pendingItem)) }

// Pop removes the last item; callers use Take (heap.Interface).
func (q *pendingQueue) Pop() any {
	last := len(q.items) - 1
	entry := q.items[last]
	q.items = q.items[:last]
	return entry
}

// Add queues a pair with deficit 0, so a ranked queue is stale until the next Rerank.
func (q *pendingQueue) Add(key reconcileKey) {
	q.arrivals++
	heap.Push(q, pendingItem{key: key, arrival: q.arrivals})
	q.dirty = q.ranked
}

// Take removes and returns the best pair, or false when the queue is empty.
func (q *pendingQueue) Take() (reconcileKey, bool) {
	if len(q.items) == 0 {
		return reconcileKey{}, false
	}
	taken := heap.Pop(q).(pendingItem)
	return taken.key, true
}

// Stale reports whether a ranked queue needs a Rerank: a pair was added, or the committed state moved.
func (q *pendingQueue) Stale(stateSeq uint64) bool {
	return q.ranked && (q.dirty || stateSeq != q.at)
}

// Rerank recomputes every item's deficit, restores the heap order and remembers stateSeq. It is O(queued pairs) and
// leaves an unranked queue alone, whose order never depends on the state.
func (q *pendingQueue) Rerank(stateSeq uint64, deficit func(slot uint16, db uint64) int) {
	if !q.ranked {
		return
	}
	for i := range q.items {
		q.items[i].deficit = deficit(q.items[i].key.slot, q.items[i].key.db)
	}
	heap.Init(q)
	q.at = stateSeq
	q.dirty = false
}

// seedDeficits reads the committed state once and returns the deficit of any pair under it, so the
// puller's re-rank costs one state read however many pairs are queued.
func (n *Node) seedDeficits() func(slot uint16, db uint64) int {
	state := n.raft.State()
	table := reconcilePlacingTable(state, n.cfg.NodeID)
	return func(slot uint16, db uint64) int { return seedDeficit(state, table, slot, db) }
}
