package cluster

import "sync"

// BumpApplier holds the published schema_version of each database on this node and publishes
// a committed bump the way conv 20 step 6 says: pair by pair, holding that pair's lock.
//
// Two numbers exist for a database on a node: the committed one in the applied ClusterState
// and the published one here, which the fence, the declaration cache tags and the fetch
// discard rule read. A bump moves the first at once and the second in Apply.
//
// Rule 11: one 8-byte word per database, nothing per bucket, no goroutine and no timer.
type BumpApplier struct {
	cells sync.Map // database id -> *publishedCell
}

type publishedCell struct {
	mu      sync.Mutex
	version int64
}

// Stage records version as database dbID's published version when none is recorded yet. It
// is called on the FSM goroutine in the same apply that commits the version, before any
// reader can see the committed one, so a reader never finds a database whose committed
// version is ahead of a published version that does not exist yet.
func (b *BumpApplier) Stage(dbID uint64, version int64) {
	b.cells.LoadOrStore(dbID, &publishedCell{version: version})
}

// Published is the published schema_version of database dbID, and false when none is
// recorded (the caller then has only the committed version to go on).
func (b *BumpApplier) Published(dbID uint64) (int64, bool) {
	entry, ok := b.cells.Load(dbID)
	if !ok {
		return 0, false
	}
	cell := entry.(*publishedCell)
	cell.mu.Lock()
	defer cell.mu.Unlock()
	return cell.version, true
}

// Forget drops the record of a dropped database.
func (b *BumpApplier) Forget(dbID uint64) { b.cells.Delete(dbID) }

// Apply publishes schema_version to for database dbID: for every (slot, database) lock this
// node holds, in slot order, acquire it, publish, release. The release of a pair's lock is the
// bump's linearisation point for that pair, so a writer either commits its batch under the
// lock before the publication or sees the new version and is refused at check 4. The pass
// touches no cache entry and no epoch, and it holds one pair lock at a time and nothing else.
//
// Cost: one short lock acquisition per held pair, at most 8192 per database, no bucket touched.
func (b *BumpApplier) Apply(dbID uint64, to int64, pairs *PairSet) {
	// A database never staged has no published word for a reader to have seen, so it starts at to.
	entry, _ := b.cells.LoadOrStore(dbID, &publishedCell{version: to})
	cell := entry.(*publishedCell)
	publish := func() {
		cell.mu.Lock()
		cell.version = max(cell.version, to)
		cell.mu.Unlock()
	}
	cycled := make(map[uint16]bool)
	published := false
	for {
		fresh := false
		for _, slot := range pairs.HeldSlots(dbID) {
			if cycled[slot] {
				continue
			}
			fresh = true
			cycled[slot] = true
			if pairs.WithHeld(slot, dbID, func(*Pair) { publish() }) {
				published = true
			}
		}
		if fresh {
			continue
		}
		if published {
			return
		}
		// No pair is held yet: publish with no lock to hold, then look again, because a pair
		// created while we published may have read the old version.
		publish()
		published = true
	}
}
