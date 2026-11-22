package wire

import (
	"sync"

	"github.com/hoarddb/hoarddb-server/storage"
)

// logFloor is the node's tracker of the log floor (store-vouched-cut-spec.md §3.6): a lower
// bound of every log record whose commit may be above the store's next cut and that no pair
// record's cursor covers. It holds, per pair, the hold of a first write in flight or failed
// (the pair has no cursor yet), and the boot hold while the boot replay still owes records.
// A store sync samples it (sample) and carries the result in the sync marker, and the next
// boot scans from it.
//
// Cost: nothing per pair at rest; a hold lives from a first write's registration until its
// commit returns or the step ends with nothing appended. Lock order: pair lock, then mu; mu is
// never held across a WAL call that locks or a store sync (trap 2).
//
// A nil *logFloor tracks nothing: an advancer built without one (a test rig) registers no hold.
type logFloor struct {
	wal   *storage.WAL
	mu    sync.Mutex
	holds map[pairKey]uint64 // a first write in flight or failed
	boot  uint64             // the boot hold, 0 when released
}

// betweenFloorReads, when set, runs in sample between the read of the synced sequence and the
// read of the holds (T13 (a)); nil in production.
var betweenFloorReads func()

// beforeAdvanceCommit, when set, runs in both advancers immediately before a step's commit,
// replays included (T12, T12c); nil in production.
var beforeAdvanceCommit func(slot uint16, db uint64)

func newLogFloor(wal *storage.WAL) *logFloor {
	return &logFloor{wal: wal, holds: make(map[pairKey]uint64)}
}

// hold registers a hold for key at the log's next sequence, before the caller appends the
// pair's first write. It reports whether this call added it: a held key is left as it is,
// because that hold belongs to a first write still in flight or to a failed one awaiting its
// recovery. The sequence is read before mu is taken.
func (f *logFloor) hold(key pairKey) (added bool) {
	if f == nil {
		return false
	}
	at := f.wal.LastSequence() + 1
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, held := f.holds[key]; held {
		return false
	}
	f.holds[key] = at
	return true
}

// release removes key's hold.
func (f *logFloor) release(key pairKey) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.holds, key)
}

// releaseUnappended releases key's hold after a step that ended with nothing appended, and only
// when the step's own registration added it (§2's release rule). A step that does not say
// whether it appended (appended nil) keeps the hold: the safe direction.
func (f *logFloor) releaseUnappended(key pairKey, added bool, appended func() bool) {
	if added && appended != nil && !appended() {
		f.release(key)
	}
}

// holdBoot registers the boot hold at the boot scan's start.
func (f *logFloor) holdBoot(at uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.boot = at
}

// raiseBoot moves the boot hold up to at, the lowest cursor of the pairs still waiting for
// their replay; it never moves it down.
func (f *logFloor) raiseBoot(at uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.boot != 0 && at > f.boot {
		f.boot = at
	}
}

// releaseBoot ends the boot hold: no pair waits for its replay any more.
func (f *logFloor) releaseBoot() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.boot = 0
}

// lowest returns the lowest hold, the boot hold included, and whether there is one. It is the
// H accessor log retention (BUG-0829, B2b) reads.
func (f *logFloor) lowest() (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	low, found := f.boot, f.boot != 0
	for _, at := range f.holds {
		if !found || at < low {
			low, found = at, true
		}
	}
	return low, found
}

// sample is the floor a store sync carries (§3.6): the log's synced sequence + 1, then the
// lowest hold, the smaller. Read in that order, before the sync reads its version: a hold
// registered after the holds are read belongs to a record appended after the sequence was
// read, so above it; a hold released before is one whose commit returned, so at or below the
// version read afterwards. Never WAL.LastSequence: it counts entries not yet fsynced, which a
// power cut gives out again.
func (f *logFloor) sample() uint64 {
	floor := f.wal.SyncedSequence() + 1
	if betweenFloorReads != nil {
		betweenFloorReads()
	}
	if low, found := f.lowest(); found && low < floor {
		floor = low
	}
	return floor
}
