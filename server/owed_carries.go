package server

import (
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/store"
)

// owedCarries is the failed-apply carry table (catalog-gc spec §5.1 item 2,
// §5.2 "side tables"). store.OwedApplies is replication state — the sequences
// a bucket's next batch must record in its failed_seqs — and it used to live
// in the bucket's engine, surviving because the engine did. Engines are going
// to be built per operation, so the carry is kept here, keyed by bucket id,
// and every engine built for the bucket is handed the same one.
//
// It holds an entry for exactly the buckets whose carry owed an apply after
// their last one (noteApplyOutcome settles it), so it is O(outstanding
// failures), like owedRegistry, and empty on a healthy node. An entry whose
// recorded count is above zero stays until the node restarts, because replay
// is the only retry. Its zero value is ready to use.
type owedCarries struct {
	// owing is the number of entries. It is read before the lock: zero means
	// no bucket on the node owes, and every write and engine build answers
	// without taking mu.
	owing atomic.Int64
	// mu is a leaf: nothing is acquired while it is held. It is taken inside
	// bucket.mu by a writer's settle, and with no lock held elsewhere.
	mu   sync.Mutex
	byID map[uint64]*store.OwedApplies
}

// take returns the carry of bucket id if it owes an apply, or nil when it owes
// none and the caller supplies an empty one.
func (c *owedCarries) take(id uint64) *store.OwedApplies {
	if c.owing.Load() == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byID[id]
}

// settle records bucket id's carry after an apply: kept while it owes, gone
// once it does not. The caller holds the bucket's mutex, which guards owed.
func (c *owedCarries) settle(id uint64, owed *store.OwedApplies) {
	owes := owed.Owes()
	if !owes && c.owing.Load() == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owes {
		if c.byID == nil {
			c.byID = make(map[uint64]*store.OwedApplies)
		}
		c.byID[id] = owed
	} else {
		delete(c.byID, id)
	}
	c.owing.Store(int64(len(c.byID)))
}

// owes reports whether bucket id owed an apply after its last one. It reads
// the table alone, so it needs no engine and no bucket lock.
func (c *owedCarries) owes(id uint64) bool {
	if c.owing.Load() == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, owed := c.byID[id]
	return owed
}

// forgetBucketID drops the carry of the dropped bucket id: its state document
// goes with its range, so nothing will record what it owed. By id, so a bucket
// of the same name created since keeps its own. O(1).
func (c *owedCarries) forgetBucketID(id uint64) {
	if c.owing.Load() == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.byID, id)
	c.owing.Store(int64(len(c.byID)))
}
