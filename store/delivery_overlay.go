package store

import (
	"bytes"
	"slices"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The overlay (claim-ack-spec §4.3). Apply hands an operation a snapshot and a
// write-only batch, so a 0x04 key the return step writes back is invisible to
// every read the same operation makes after it: the walk would step over the
// returned entries and strand them below the cursor with every count agreeing,
// and a heap's collision check would let two returns to one key overwrite each
// other. Every 0x04 key a delivery apply writes back goes through the overlay
// as well as the batch, and every 0x04 read of the apply reads through it.
//
// The view rule: only 0x04 reads go through the overlay. 'h', 'd', 'e', 'c' and
// 'x' are read from the snapshot, never the batch, and two verbs depend on it:
// the named lease's header Get (a lease the return step returned in this apply
// still answers expired) and ack's 'e' Gets (which is why ack makes its item
// ids distinct first).
//
// It lives for one apply and holds only what the return step and a release
// wrote back, which the delivery budget bounds.

// overlayEntry is one 0x04 key written back in this apply: the entry, and how
// many times it has been returned, which its 'c' record holds too.
type overlayEntry struct {
	value   []byte
	returns uint64
}

type overlay struct {
	written map[string]overlayEntry
	// taken holds the written-back keys the apply took again, so a later
	// point read finds them gone.
	taken map[string]bool
}

func (o *overlay) put(key, value []byte, returns uint64) {
	if o.written == nil {
		o.written = map[string]overlayEntry{}
	}
	o.written[string(key)] = overlayEntry{value: value, returns: returns}
}

// get is the overlay's entry at key, if this apply wrote it back and has not
// taken it again.
func (o *overlay) get(key []byte) (overlayEntry, bool) {
	entry, ok := o.written[string(key)]
	if !ok || o.taken[string(key)] {
		return overlayEntry{}, false
	}
	return entry, true
}

func (o *overlay) take(key []byte) {
	if o.taken == nil {
		o.taken = map[string]bool{}
	}
	o.taken[string(key)] = true
}

func (o *overlay) empty() bool { return len(o.written) == 0 }

// keysInOrder is the written-back keys in the walk's direction.
func (o *overlay) keysInOrder(reverse bool) [][]byte {
	keys := make([][]byte, 0, len(o.written))
	for key := range o.written {
		keys = append(keys, []byte(key))
	}
	slices.SortFunc(keys, bytes.Compare)
	if reverse {
		slices.Reverse(keys)
	}
	return keys
}

// takeSource is what a take walk reads: storage.KVIterator's walk, which the
// plain snapshot iterator satisfies and mergedWalk implements (stop-and-ask 2:
// no signature a pop's caller sees changes).
type takeSource interface {
	Seek(key []byte)
	Valid() bool
	Key() []byte
	Value() ([]byte, error)
	Next()
	Err() error
	Close()
}

// snapshotRun is the snapshot side's share of an armed cursor's run
// (claim-ack-spec §4.3, A-3). While the cursor is armed the snapshot holds
// exactly the loaded ResumeBelow live keys on the run's side of ResumeAt; once
// the snapshot side has yielded that many it seeks to ResumeAt rather than
// stepping, because every key between its last run key and ResumeAt is a
// delete marker, the wall the remembered cursor exists to skip.
type snapshotRun struct {
	left   int64
	resume []byte
	onRun  func(key []byte) bool
}

// walkSide says which side the merged walk's current key came from.
type walkSide int

const (
	sideNone walkSide = iota
	sideSnapshot
	sideOverlay
)

// mergedWalk merges the snapshot's 0x04 keys with the overlay's in the walk's
// direction. Every move is lazy: Next and Seek only record the move, which the
// next Valid makes, so a walk that stops after its n-th take never steps either
// side past it (the delete markers beyond it are what a step would walk).
type mergedWalk struct {
	snap    storage.KVIterator
	run     snapshotRun
	over    *overlay
	keys    [][]byte
	at      int
	reverse bool

	current               walkSide
	advanceSnapshot       bool
	advanceOverlay        bool
	pendingSeek           []byte
	seekPending, prepared bool
}

// newMergedWalk starts the merge: the snapshot side already positioned at the
// loaded cursor, the overlay side at its first key at or past from in the
// walk's direction.
func newMergedWalk(snap storage.KVIterator, run snapshotRun, over *overlay, reverse bool, from []byte) *mergedWalk {
	walk := &mergedWalk{snap: snap, run: run, over: over, keys: over.keysInOrder(reverse), reverse: reverse}
	walk.seekOverlay(from)
	return walk
}

// seekOverlay positions the overlay side at the first key at or past key in
// the walk's direction; nil is the start.
func (w *mergedWalk) seekOverlay(key []byte) {
	w.at = 0
	if key == nil {
		return
	}
	for w.at < len(w.keys) && w.before(w.keys[w.at], key) {
		w.at++
	}
}

// before reports whether a comes before b in the walk's direction.
func (w *mergedWalk) before(a, b []byte) bool {
	if w.reverse {
		return bytes.Compare(a, b) > 0
	}
	return bytes.Compare(a, b) < 0
}

func (w *mergedWalk) Seek(key []byte) {
	w.pendingSeek, w.seekPending = key, true
	w.advanceSnapshot, w.advanceOverlay, w.prepared = false, false, false
}

func (w *mergedWalk) Next() {
	switch w.current {
	case sideSnapshot:
		w.advanceSnapshot = true
	case sideOverlay:
		w.advanceOverlay = true
	}
	w.prepared = false
}

// prepare makes the recorded move and picks the side whose key comes first.
func (w *mergedWalk) prepare() {
	if w.prepared {
		return
	}
	w.prepared = true
	if w.seekPending {
		w.seekPending = false
		// After a jump the run is drained: the cursor is on the far side of
		// every run key, and the snapshot side seeks there with it.
		w.run.left = 0
		w.snap.Seek(w.pendingSeek)
		w.seekOverlay(w.pendingSeek)
	}
	if w.advanceSnapshot {
		w.advanceSnapshot = false
		w.stepSnapshot()
	}
	if w.advanceOverlay {
		w.advanceOverlay = false
		w.at++
	}
	snapValid, overValid := w.snap.Valid(), w.at < len(w.keys)
	switch {
	case snapValid && overValid:
		w.current = sideSnapshot
		if w.before(w.keys[w.at], w.snap.Key()) {
			w.current = sideOverlay
		}
	case snapValid:
		w.current = sideSnapshot
	case overValid:
		w.current = sideOverlay
	default:
		w.current = sideNone
	}
}

// stepSnapshot moves the snapshot side past its current key: a step, or the
// seek to ResumeAt once the run's last snapshot key has been yielded.
func (w *mergedWalk) stepSnapshot() {
	if w.run.left > 0 && w.run.onRun(w.snap.Key()) {
		w.run.left--
		if w.run.left == 0 {
			w.snap.Seek(w.run.resume)
			return
		}
	}
	w.snap.Next()
}

func (w *mergedWalk) Valid() bool {
	w.prepare()
	return w.current != sideNone
}

func (w *mergedWalk) Key() []byte {
	if w.current == sideOverlay {
		return w.keys[w.at]
	}
	return w.snap.Key()
}

func (w *mergedWalk) Value() ([]byte, error) {
	if w.current == sideOverlay {
		return append([]byte(nil), w.over.written[string(w.keys[w.at])].value...), nil
	}
	return w.snap.Value()
}

func (w *mergedWalk) Err() error { return w.snap.Err() }

func (w *mergedWalk) Close() { w.snap.Close() }
