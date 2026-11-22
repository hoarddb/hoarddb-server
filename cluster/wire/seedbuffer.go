package wire

import (
	"container/heap"
	"sync"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// seedFrameOverhead is the bookkeeping a held frame costs beyond its encoded record: its map
// entry and its heap slot. Counting it keeps a flood of empty records from outgrowing the bound.
const seedFrameOverhead = 48

// seedPhaseMirror is the puller's own copy of the record's phase for one attempt. The buffer
// keeps a frame only while the mirror says the pull is still seeding, so a frame offered
// after the pull stopped listening is refused rather than held for a drain that never comes.
type seedPhaseMirror uint8

const (
	mirrorNone seedPhaseMirror = iota
	mirrorSnapshot
	mirrorBuffer
)

// seedBuffers is the node's whole live-write buffer budget, shared by every pull in flight.
type seedBuffers struct {
	mu       sync.Mutex
	bound    int64
	attempts map[pairKey]*seedAttemptBuffer
}

var _ SeedBuffer = (*seedBuffers)(nil)

// newSeedBuffers returns a budget of boundBytes for the whole node.
func newSeedBuffers(boundBytes int64) *seedBuffers {
	return &seedBuffers{bound: boundBytes, attempts: make(map[pairKey]*seedAttemptBuffer)}
}

// Begin opens the buffer for the pull of pair (slot, db). It takes no share up front: each
// Offer divides the bound over the pulls open at that moment, so a pull that ends frees its
// share for the others at once. A second Begin for a pair that is still open replaces the
// first attempt in the map; the End of the replaced attempt would then close the new one.
func (b *seedBuffers) Begin(slot uint16, db uint64) *seedAttemptBuffer {
	a := &seedAttemptBuffer{frames: make(map[cluster.Position]bufferedFrame)}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attempts[pairKey{slot, db}] = a
	return a
}

// End closes the pull of pair (slot, db) and drops whatever it still holds. The attempt's
// mutex is taken after the budget's, never inside it, so the two never nest.
func (b *seedBuffers) End(slot uint16, db uint64) {
	key := pairKey{slot, db}
	b.mu.Lock()
	a := b.attempts[key]
	delete(b.attempts, key)
	b.mu.Unlock()
	if a != nil {
		a.release()
	}
}

// Offer keeps the frame for the pull of its pair if the pair is seeding and the frame fits
// the pull's share of the bound. It reports whether the frame was kept.
func (b *seedBuffers) Offer(slot uint16, db uint64, frame WriteFrame) bool {
	b.mu.Lock()
	a := b.attempts[pairKey{slot, db}]
	pulls := int64(len(b.attempts))
	b.mu.Unlock()
	if a == nil {
		return false
	}
	return a.offer(frame, b.bound/pulls)
}

// seedAttemptBuffer is one pull's buffer: the frames it holds by position, and a min-heap of
// those positions so the drain applies them in order.
type seedAttemptBuffer struct {
	mu          sync.Mutex
	phase       seedPhaseMirror
	frames      map[cluster.Position]bufferedFrame
	order       positionHeap
	held        int64
	overflowed  bool
	overflowPos cluster.Position
}

// bufferedFrame is one held WRITE and the bytes it counts against the bound.
type bufferedFrame struct {
	frame WriteFrame
	cost  int64
}

// SetPhase records the phase the puller has reached for this attempt.
func (a *seedAttemptBuffer) SetPhase(phase seedPhaseMirror) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.phase = phase
}

// Overflowed reports whether a frame was refused for size during this attempt.
func (a *seedAttemptBuffer) Overflowed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.overflowed
}

// OverflowPos is the position of the first frame refused for size.
func (a *seedAttemptBuffer) OverflowPos() cluster.Position {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.overflowPos
}

// Pop removes and returns the held frame with the lowest position.
func (a *seedAttemptBuffer) Pop() (bufferedFrame, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.order.Len() == 0 {
		return bufferedFrame{}, false
	}
	pos := heap.Pop(&a.order).(cluster.Position)
	f := a.frames[pos]
	delete(a.frames, pos)
	a.held -= f.cost
	return f, true
}

// Empty reports whether the attempt holds no frame.
func (a *seedAttemptBuffer) Empty() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.frames) == 0
}

// EndIfEmpty ends the attempt's buffering only if it holds nothing. The check and the phase
// change are one critical section: a frame offered between them would be kept by a buffer
// whose mirror says none, and no drain would ever apply it.
func (a *seedAttemptBuffer) EndIfEmpty() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.frames) > 0 {
		return false
	}
	a.phase = mirrorNone
	return true
}

// offer keeps the frame if the mirror is listening, the position is new and the frame fits
// within share bytes. A refused frame for size marks the attempt as overflowed at its position.
func (a *seedAttemptBuffer) offer(frame WriteFrame, share int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.phase == mirrorNone {
		return false
	}
	pos := frame.Pos
	if _, held := a.frames[pos]; held {
		return true
	}
	cost := frameCost(frame)
	if a.held+cost > share {
		if !a.overflowed {
			a.overflowed = true
			a.overflowPos = pos
		}
		return false
	}
	a.frames[pos] = bufferedFrame{frame: frame, cost: cost}
	heap.Push(&a.order, pos)
	a.held += cost
	return true
}

// release drops every held frame and stops listening. It runs when the pull ends, so no
// frame outlives the attempt that was meant to drain it.
func (a *seedAttemptBuffer) release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.phase = mirrorNone
	clear(a.frames)
	a.order = nil
	a.held = 0
}

// frameCost is the bytes a WRITE takes from the bound: its encoded record and the overhead.
func frameCost(frame WriteFrame) int64 {
	return int64(storage.WALRecordHeaderLen+len(frame.Record.Body)) + seedFrameOverhead
}

// positionHeap is a min-heap of record positions, ordered as cluster.Position.Less orders them.
type positionHeap []cluster.Position

func (h positionHeap) Len() int           { return len(h) }
func (h positionHeap) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h positionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *positionHeap) Push(x any) { *h = append(*h, x.(cluster.Position)) }

func (h *positionHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}
