package wire

import (
	"context"
	"sync"
)

// manyResult is what one read of readMany came to.
type manyResult struct {
	doc Document
	err error
	// read is false for a read that was never launched: proceed said stop, or ctx ended first.
	read bool
}

// readManyPlan is how readMany reads and what it does with each answer.
type readManyPlan struct {
	// fetch reads one document; it is the normal read path (Node.readDocumentFrom) outside tests.
	fetch expandFetcher
	// ownerOf names the owner of a slot, so each owner's reads are bounded and launched apart.
	ownerOf func(ref bucketRef, slot uint16) string
	// frameInFlight bounds the reads in flight across every owner of the call; 0 means only the
	// per-owner bound applies. With a bound, the owners still launching share it evenly, so reads
	// parked on one owner do not take the slots of the others.
	frameInFlight int
	// proceed is asked before each launch; false stops the launches of that owner. Nil always proceeds.
	proceed func() bool
	// settle is called as each read ends, one at a time and before its slot is given back, with
	// refs[at]'s result; what it returns is kept. Nil keeps the result as it is.
	settle func(at int, result manyResult) manyResult
}

// readMany reads every ref and returns one result per ref, results[i] for refs[i] (each ref's at
// must be its index). Refs are grouped by owner and each owner's are launched in order, at most
// ringBatchInFlight at a time, all owners in parallel. A read's error is its own result: readMany
// never cancels a sibling, and a caller that wants to stop on an error cancels ctx itself. Every
// goroutine it starts has returned when it returns.
func readMany(ctx context.Context, refs []expandRef, plan readManyPlan) []manyResult {
	results := make([]manyResult, len(refs))
	groups := groupByOwner(refs, plan.ownerOf)
	slots := newManySlots(plan.frameInFlight, len(groups))
	stop := context.AfterFunc(ctx, slots.wake)
	defer stop()
	var (
		wait sync.WaitGroup
		mu   sync.Mutex
	)
	proceed := func() bool {
		if plan.proceed == nil {
			return true
		}
		mu.Lock()
		defer mu.Unlock()
		return plan.proceed()
	}
	for owner, group := range groups {
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer slots.retire()
			for _, ref := range group.refs {
				if !slots.acquire(ctx, owner) {
					return
				}
				if !proceed() {
					slots.release(owner)
					return
				}
				wait.Add(1)
				go func() {
					defer wait.Done()
					defer slots.release(owner)
					doc, err := plan.fetch(ctx, ref.ref, ref.encID, ref.project, ref.local)
					result := manyResult{doc: doc, err: err, read: true}
					mu.Lock()
					defer mu.Unlock()
					if plan.settle != nil {
						result = plan.settle(ref.at, result)
					}
					results[ref.at] = result
				}()
			}
		}()
	}
	wait.Wait()
	return results
}

// manySlots hands out the in-flight slots of one readMany call: ringBatchInFlight per owner and,
// when frame is set, frame across the call, shared evenly by the owners that still launch.
type manySlots struct {
	mu       sync.Mutex
	room     *sync.Cond
	frame    int
	inFlight int
	owners   map[int]int
	// active is how many owners still have reads to launch.
	active int
}

func newManySlots(frame, owners int) *manySlots {
	slots := &manySlots{frame: frame, owners: make(map[int]int, owners), active: owners}
	slots.room = sync.NewCond(&slots.mu)
	return slots
}

// share is the most reads one owner may have in flight now. Without a frame bound it is the
// per-owner bound; with one it is the frame split over the owners still launching, so 16 reads
// parked on one owner cannot hold every slot of the frame while another owner waits.
func (s *manySlots) share() int {
	if s.frame == 0 {
		return ringBatchInFlight
	}
	return min(ringBatchInFlight, max(1, s.frame/max(1, s.active)))
}

// acquire waits for a slot for owner and reports false when ctx ends first.
func (s *manySlots) acquire(ctx context.Context, owner int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return false
		}
		if s.owners[owner] < s.share() && (s.frame == 0 || s.inFlight < s.frame) {
			s.owners[owner]++
			s.inFlight++
			return true
		}
		s.room.Wait()
	}
}

func (s *manySlots) release(owner int) {
	s.mu.Lock()
	s.owners[owner]--
	s.inFlight--
	s.mu.Unlock()
	s.room.Broadcast()
}

// retire says an owner launches nothing more, which widens the share of the others.
func (s *manySlots) retire() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	s.room.Broadcast()
}

// wake makes every waiter look at its context again. A sync.Cond cannot select on a context, so
// it takes the lock before it broadcasts: that orders the broadcast after a waiter has parked.
func (s *manySlots) wake() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.room.Broadcast()
}
