package store

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// The dead verbs at the store (claim/ack build step 8).

// deadData is the data of a dead entry numbered n with a body of pad bytes: the
// stored document for fifo and lifo, {_id_, priority, value} for a heap, whose
// key is the one push(n, n+1) makes.
func (s *deliverySubject) deadData(n, pad int) bson.Raw {
	s.t.Helper()
	stored, err := bson.Marshal(numbered(s.t, n, pad))
	if err != nil {
		s.t.Fatalf("encoding entry %d: %v", n, err)
	}
	if s.heap == nil {
		return stored
	}
	suffix := append(bigEndian(invertedPriority(int64(1000-n))), fmt.Sprintf("e%04d", n)...)
	document, err := heapEntryDocument(suffix, stored)
	if err != nil {
		s.t.Fatalf("encoding the heap document of entry %d: %v", n, err)
	}
	return document
}

func (s *deliverySubject) restoreRecord(record DeadRecord) error {
	if s.heap != nil {
		return s.heap.RestoreDead(ApplyInfo{}, record)
	}
	return s.queue.RestoreDead(ApplyInfo{}, record)
}

// restore restores entry n as a dead entry that died of MaxDeliveries.
func (s *deliverySubject) restore(n, pad int) {
	s.t.Helper()
	record := DeadRecord{Data: s.deadData(n, pad), Deliveries: MaxDeliveries, DeadAt: testNow, Reason: DeadReasonMaxDeliveries}
	if err := s.restoreRecord(record); err != nil {
		s.t.Fatalf("%s RestoreDead(%d): %v", s.name, n, err)
	}
}

func (s *deliverySubject) requeue(sel DeadSelector) (DeadMove, error) {
	if s.heap != nil {
		return s.heap.Requeue(ApplyInfo{}, sel)
	}
	return s.queue.Requeue(ApplyInfo{}, sel)
}

func (s *deliverySubject) discard(sel DeadSelector) (DeadMove, error) {
	if s.heap != nil {
		return s.heap.Discard(ApplyInfo{}, sel)
	}
	return s.queue.Discard(ApplyInfo{}, sel)
}

func (s *deliverySubject) deadListN(limit int, after []byte, visit func(DeadRecord) error) ([]byte, bool, error) {
	if s.heap != nil {
		return s.heap.DeadList(limit, after, visit)
	}
	return s.queue.DeadList(limit, after, visit)
}

// deadPage lists up to limit dead entries after the id.
func (s *deliverySubject) deadPage(limit int, after []byte) ([]DeadRecord, []byte) {
	s.t.Helper()
	var listed []DeadRecord
	next, _, err := s.deadListN(limit, after, func(record DeadRecord) error {
		listed = append(listed, record)
		return nil
	})
	if err != nil {
		s.t.Fatalf("%s DeadList(%d, %x): %v", s.name, limit, after, err)
	}
	return listed, next
}

// deadNumber is the entry number a dead record holds.
func (s *deliverySubject) deadNumber(record DeadRecord) int {
	s.t.Helper()
	if s.heap == nil {
		return s.number(record.Data)
	}
	stored, err := heapStoredValue(record.Data)
	if err != nil {
		s.t.Fatalf("reading the value of dead record %x: %v", record.DeadID, err)
	}
	return s.number(stored)
}

// kill claims the first entry in pop order and lets its lease expire until it
// is dead-lettered. base keeps the clock rising across calls.
func (s *deliverySubject) kill(base int64) {
	s.t.Helper()
	for cycle := int64(1); cycle <= MaxDeliveries; cycle++ {
		s.claimReturnCycle(1, base+cycle*10*testLeaseMs)
	}
}

// liveInPopOrder pops everything and returns the numbers, to compare with the
// order the bucket must pop in.
func (s *deliverySubject) liveInPopOrder() []int {
	s.t.Helper()
	return s.pop(100)
}

// TestRequeueRestoresOrderAndResetsTheCount (CA-25, OQ-C): a dead entry goes back
// to its origin, so it pops in its old place, and its count starts again: the
// next claim delivers it for the first time and no 'c' record is left.
func TestRequeueRestoresOrderAndResetsTheCount(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 4)
			s.kill(testNow)
			if st := s.state(); st.Dead != 1 || st.Entries != 3 {
				t.Fatalf("after the kill: dead %d, entries %d, want 1 and 3", st.Dead, st.Entries)
			}
			listed, _ := s.deadPage(10, nil)
			if len(listed) != 1 {
				t.Fatalf("the dead list holds %d entries, want 1", len(listed))
			}
			move, err := s.requeue(DeadSelector{DeadID: listed[0].DeadID})
			if err != nil || move.Done != 1 || move.Left != 0 || len(move.Refused) != 0 || !move.Wrote {
				t.Fatalf("requeue = %+v, %v, want one done and none left", move, err)
			}
			if st := s.state(); st.Dead != 0 || st.DeadBytes != 0 || st.Entries != 4 || st.Returned != 0 {
				t.Fatalf("after the requeue: dead %d (%d bytes), entries %d, returned %d, want 0 (0), 4, 0", st.Dead, st.DeadBytes, st.Entries, st.Returned)
			}
			if counts := len(s.keysOfKind(kindReturnCount)); counts != 0 {
				t.Fatalf("the requeue left %d 'c' records, want its count reset", counts)
			}
			s.invariants()
			claim, taken := s.claim(1, testNow+1_000*testLeaseMs)
			want := firstInPopOrder(s.kind, 4, 1)
			if !slices.Equal(taken, want) || claim.Items[0].Deliveries != 1 {
				t.Fatalf("the next claim took %v with deliveries %d, want %v delivered once", taken, claim.Items[0].Deliveries, want)
			}
			s.invariants()
		})
	}
}

// TestRequeueRestoresPopOrder: after the requeue every entry pops in the order it
// had before it died.
func TestRequeueRestoresPopOrder(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 4)
			s.kill(testNow)
			listed, _ := s.deadPage(10, nil)
			if _, err := s.requeue(DeadSelector{DeadID: listed[0].DeadID}); err != nil {
				t.Fatalf("requeue: %v", err)
			}
			if got, want := s.liveInPopOrder(), firstInPopOrder(s.kind, 4, 4); !slices.Equal(got, want) {
				t.Fatalf("pop order after the requeue = %v, want %v", got, want)
			}
		})
	}
}

// TestRequeueOfAnOccupiedHeapKeyIsRefusedByName (CA-25): a heap key a live twin
// holds is not written over; the refusal names the id and priority and the dead
// copy stays.
func TestRequeueOfAnOccupiedHeapKeyIsRefusedByName(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 1)
	s.claim(1, testNow)
	s.pushHeapTwin(0)
	s.returnDue(expired)
	listed, _ := s.deadPage(10, nil)
	if len(listed) != 1 || listed[0].Reason != DeadReasonCollided {
		t.Fatalf("the dead list is %+v, want the one collided copy", listed)
	}
	for _, sel := range []DeadSelector{{DeadID: listed[0].DeadID}, {All: true}} {
		move, err := s.requeue(sel)
		if err != nil || move.Done != 0 || move.Left != 1 || len(move.Refused) != 1 || move.Wrote {
			t.Fatalf("requeue %+v = %+v, %v, want nothing done and the copy refused", sel, move, err)
		}
		refusal := move.Refused[0]
		if refusal.Cause != DeadOriginOccupied || refusal.EntryID != "e0000" || refusal.Priority != 1000 || !bytes.Equal(refusal.DeadID, listed[0].DeadID) {
			t.Fatalf("the refusal = %+v, want origin_occupied naming e0000 at priority 1000", refusal)
		}
	}
	if st := s.state(); st.Dead != 1 || st.Entries != 1 {
		t.Fatalf("after the refusals: dead %d, entries %d, want 1 and 1", st.Dead, st.Entries)
	}
	s.invariants()
}

// TestRequeueAllRefusesTheSecondCopyOfOneHeapKey: two dead copies of one heap
// key requeued in one apply: the first goes back and the second finds the key
// taken, through the overlay, because the snapshot does not hold the first.
func TestRequeueAllRefusesTheSecondCopyOfOneHeapKey(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.restore(0, 0)
	s.restore(0, 0)
	move, err := s.requeue(DeadSelector{All: true})
	if err != nil || move.Done != 1 || move.Left != 1 || len(move.Refused) != 1 || move.Refused[0].Cause != DeadOriginOccupied {
		t.Fatalf("requeue all = %+v, %v, want one done and the second refused", move, err)
	}
	if st := s.state(); st.Entries != 1 || st.Dead != 1 {
		t.Fatalf("after the requeue: entries %d, dead %d, want 1 and 1", st.Entries, st.Dead)
	}
	s.invariants()
}

// TestRequeueAllReachesEntriesBehindARefusedHead (BUG-0637): 100 dead heap
// entries whose keys live twins hold sit at the head of the list, 10 requeueable
// ones behind them. A request considers 100 entries, refused ones included, so a
// caller that always starts at the head never reaches the 10; one that sends the
// last refused id back as After does, and every refusal stays dead.
func TestRequeueAllReachesEntriesBehindARefusedHead(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	for n := range MaxDeadMoveLimit {
		s.restore(n, 0)
	}
	if move, err := s.requeue(DeadSelector{All: true}); err != nil || move.Done != MaxDeadMoveLimit {
		t.Fatalf("setup requeue = %+v, %v, want %d done", move, err, MaxDeadMoveLimit)
	}
	for n := range MaxDeadMoveLimit {
		s.restore(n, 0)
	}
	for n := MaxDeadMoveLimit; n < MaxDeadMoveLimit+10; n++ {
		s.restore(n, 0)
	}

	var moved int
	var after []byte
	for request := 0; request < 5; request++ {
		move, err := s.requeue(DeadSelector{All: true, After: after})
		if err != nil {
			t.Fatalf("requeue all after %x: %v", after, err)
		}
		moved += move.Done
		if len(move.Refused) == 0 {
			break
		}
		after = move.Refused[len(move.Refused)-1].DeadID
	}
	if moved != 10 {
		t.Fatalf("requeue all moved %d of the 10 entries behind the refused head, want 10", moved)
	}
	if st := s.state(); st.Entries != MaxDeadMoveLimit+10 || st.Dead != MaxDeadMoveLimit {
		t.Fatalf("after the requeue: entries %d, dead %d, want %d and %d", st.Entries, st.Dead, MaxDeadMoveLimit+10, MaxDeadMoveLimit)
	}
	s.invariants()
}

// TestRequeueAndDiscardOfNoSuchEntryAreRefusedInBand: a dead id nobody holds is a
// refusal that writes nothing, not an error.
func TestRequeueAndDiscardOfNoSuchEntryAreRefusedInBand(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.restore(0, 0)
			missing := DeadSelector{DeadID: bytes.Repeat([]byte{0xee}, 16)}
			for name, verb := range map[string]func(DeadSelector) (DeadMove, error){"requeue": s.requeue, "discard": s.discard} {
				move, err := verb(missing)
				if err != nil || move.Done != 0 || move.Wrote || len(move.Refused) != 1 || move.Refused[0].Cause != DeadNotFound || move.Left != 1 {
					t.Fatalf("%s of no such entry = %+v, %v, want one not_found refusal", name, move, err)
				}
			}
			s.invariants()
		})
	}
}

// TestDeadSelectorNamesOneOrAll: a selector naming both or neither is refused by
// name before anything is written.
func TestDeadSelectorNamesOneOrAll(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			for _, sel := range []DeadSelector{{}, {DeadID: []byte{1}, All: true}, {DeadID: []byte{1}, After: []byte{1}}} {
				if _, err := s.requeue(sel); !errors.Is(err, ErrDeadSelector) {
					t.Fatalf("requeue %+v = %v, want ErrDeadSelector", sel, err)
				}
				if _, err := s.discard(sel); !errors.Is(err, ErrDeadSelector) {
					t.Fatalf("discard %+v = %v, want ErrDeadSelector", sel, err)
				}
			}
		})
	}
}

// TestDiscardDeletesTheDeadEntry: a discard removes the entry and its bytes from
// the counts and never touches a live key.
func TestDiscardDeletesTheDeadEntry(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.kill(testNow)
			before := s.state()
			listed, _ := s.deadPage(10, nil)
			move, err := s.discard(DeadSelector{DeadID: listed[0].DeadID})
			if err != nil || move.Done != 1 || move.Left != 0 || !move.Wrote {
				t.Fatalf("discard = %+v, %v, want one done", move, err)
			}
			st := s.state()
			if st.Dead != 0 || st.DeadBytes != 0 || st.Entries != before.Entries || st.LogicalBytes != before.LogicalBytes-before.DeadBytes {
				t.Fatalf("after the discard: dead %d (%d bytes), entries %d, logical %d; before %+v", st.Dead, st.DeadBytes, st.Entries, st.LogicalBytes, before)
			}
			if page, _ := s.deadPage(10, nil); len(page) != 0 {
				t.Fatalf("the dead list still holds %d entries", len(page))
			}
			s.invariants()
		})
	}
}

// TestRequeueAllAndDiscardAllStopAtTheBudgetAndReportLeft (CA-25, Edge 3, Badger
// at the floor): 100 dead entries of 64 KiB do not fit one requeue batch, so
// the all form stops at the delivery budget and says how many are left; each
// call commits and the calls add up to every entry. A discard of 250 small ones
// stops at MaxDeadMoveLimit.
func TestRequeueAllAndDiscardAllStopAtTheBudgetAndReportLeft(t *testing.T) {
	for _, s := range badgerDeliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			for n := 0; n < 100; n++ {
				s.restore(n, 64<<10)
			}
			calls, done := 0, 0
			for left := int64(100); left > 0; calls++ {
				move, err := s.requeue(DeadSelector{All: true})
				if err != nil {
					t.Fatalf("requeue all, call %d: %v", calls+1, err)
				}
				if move.Done == 0 || move.Left != left-int64(move.Done) {
					t.Fatalf("call %d = %+v with %d left before it, want progress and left to add up", calls+1, move, left)
				}
				done += move.Done
				left = move.Left
				s.invariants()
			}
			if calls < 2 || done != 100 {
				t.Fatalf("requeue all took %d calls for %d entries, want several calls and all 100", calls, done)
			}
			if st := s.state(); st.Entries != 100 || st.Dead != 0 {
				t.Fatalf("after the requeues: entries %d, dead %d, want 100 and 0", st.Entries, st.Dead)
			}

			small := newDeliverySubject(t, s.kind, openFloorBadger(t))
			for n := 0; n < 250; n++ {
				small.restore(n, 0)
			}
			var batches []int
			for left := int64(250); left > 0; {
				move, err := small.discard(DeadSelector{All: true})
				if err != nil {
					t.Fatalf("discard all: %v", err)
				}
				batches = append(batches, move.Done)
				left = move.Left
			}
			if !slices.Equal(batches, []int{MaxDeadMoveLimit, MaxDeadMoveLimit, 50}) {
				t.Fatalf("discard all moved %v, want %d, %d and 50", batches, MaxDeadMoveLimit, MaxDeadMoveLimit)
			}
			small.invariants()
		})
	}
}

// TestDeadListPagesInKeyOrder (CA-24): pages by after, exclusive, and next is the
// last id listed when more remain. A heap lists the highest priority first.
func TestDeadListPagesInKeyOrder(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			for _, n := range []int{3, 1, 4, 0, 5, 2, 6} {
				s.restore(n, 0)
			}
			// fifo and lifo key by position, so restore order; a heap by priority
			// 1000-n, so by number.
			want := []int{3, 1, 4, 0, 5, 2, 6}
			if s.heap != nil {
				want = []int{0, 1, 2, 3, 4, 5, 6}
			}
			var got []int
			var after []byte
			for page := 0; ; page++ {
				listed, next := s.deadPage(3, after)
				for _, record := range listed {
					got = append(got, s.deadNumber(record))
				}
				if next == nil {
					if len(listed) != 1 {
						t.Fatalf("the last page holds %d entries, want 1", len(listed))
					}
					break
				}
				if !bytes.Equal(next, listed[len(listed)-1].DeadID) || len(listed) != 3 {
					t.Fatalf("page %d = %d entries with next %x, want 3 and the last id", page, len(listed), next)
				}
				after = next
			}
			if !slices.Equal(got, want) {
				t.Fatalf("the pages listed %v, want %v", got, want)
			}
		})
	}
}

// TestDeadListStopsWhereTheVisitorRefuses (CA-24, §8.3): a visitor that stops
// short ends the list before the entry it refused, which the next page starts at.
func TestDeadListStopsWhereTheVisitorRefuses(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			for n := 0; n < 5; n++ {
				s.restore(n, 0)
			}
			var listed []int
			next, stoppedShort, err := s.deadListN(5, nil, func(record DeadRecord) error {
				if len(listed) == 2 {
					return ErrStopShort
				}
				listed = append(listed, s.deadNumber(record))
				return nil
			})
			if err != nil || !slices.Equal(listed, []int{0, 1}) {
				t.Fatalf("the list stopped after %v with %v", listed, err)
			}
			if !stoppedShort {
				t.Fatal("a list a visitor stopped short did not say so")
			}
			rest, _ := s.deadPage(10, next)
			if len(rest) != 3 || s.deadNumber(rest[0]) != 2 {
				t.Fatalf("the next page starts at %d of %d entries, want entry 2 of 3", s.deadNumber(rest[0]), len(rest))
			}
			boom := errors.New("boom")
			if _, _, err := s.deadListN(5, nil, func(DeadRecord) error { return boom }); !errors.Is(err, boom) {
				t.Fatalf("a visitor error came back as %v, want it wrapped", err)
			}
		})
	}
}

// TestRestoreDeadValidatesItsRecord (§4.12, rev 2 A-7): a reason outside the two,
// fewer than one delivery or data that is not an entry is refused by name and
// writes nothing; both reasons are accepted.
func TestRestoreDeadValidatesItsRecord(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			good := DeadRecord{Data: s.deadData(0, 0), Deliveries: 1, DeadAt: testNow, Reason: DeadReasonCollided}
			bad := map[string]DeadRecord{
				"unknown reason":      {Data: good.Data, Deliveries: 1, Reason: "poisoned"},
				"empty reason":        {Data: good.Data, Deliveries: 1},
				"zero deliveries":     {Data: good.Data, Reason: DeadReasonMaxDeliveries},
				"negative deliveries": {Data: good.Data, Deliveries: -3, Reason: DeadReasonMaxDeliveries},
				"data not a document": {Data: bson.Raw("not bson"), Deliveries: 1, Reason: DeadReasonMaxDeliveries},
			}
			if s.heap != nil {
				bad["heap data without a value"] = DeadRecord{Data: mustMarshalRaw(t, map[string]any{"_id_": "x", "priority": int64(1)}), Deliveries: 1, Reason: DeadReasonMaxDeliveries}
				bad["heap data without an id"] = DeadRecord{Data: mustMarshalRaw(t, map[string]any{"priority": int64(1), "value": map[string]any{"n": 1}}), Deliveries: 1, Reason: DeadReasonMaxDeliveries}
			}
			before := s.state()
			for name, record := range bad {
				if err := s.restoreRecord(record); !errors.Is(err, ErrDeadRecordInvalid) {
					t.Fatalf("%s: RestoreDead = %v, want ErrDeadRecordInvalid", name, err)
				}
			}
			if after := s.state(); after.Dead != 0 || after.Next != before.Next || len(s.keysOfKind(kindDead)) != 0 {
				t.Fatalf("a refused record wrote: state %+v, %d dead keys", after, len(s.keysOfKind(kindDead)))
			}
			// max_deliveries means the entry was delivered MaxDeliveries times,
			// which the invariant checker holds every such record to.
			for _, accepted := range []DeadRecord{
				{Data: good.Data, Deliveries: MaxDeliveries, DeadAt: testNow, Reason: DeadReasonMaxDeliveries},
				{Data: good.Data, Deliveries: 1, DeadAt: testNow, Reason: DeadReasonCollided},
			} {
				if err := s.restoreRecord(accepted); err != nil {
					t.Fatalf("RestoreDead with reason %q = %v", accepted.Reason, err)
				}
			}
			if st := s.state(); st.Dead != 2 {
				t.Fatalf("after two valid records: dead %d, want 2", st.Dead)
			}
			s.invariants()
		})
	}
}

func mustMarshalRaw(t testing.TB, document any) bson.Raw {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("encoding a test document: %v", err)
	}
	return encoded
}

// TestRestoreDeadFifoTakesAFreshPosition (§4.12): the dead entry's position is
// Next, which is then spent, so a push after it lands elsewhere and a later
// requeue has a free origin and writes over nothing.
func TestRestoreDeadFifoTakesAFreshPosition(t *testing.T) {
	for _, s := range deliverySubjects(t)[:2] {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.pop(3)
			next := s.state().Next
			s.restore(100, 0)
			listed, _ := s.deadPage(10, nil)
			if len(listed) != 1 || !bytes.Equal(listed[0].DeadID, bigEndian(next)) {
				t.Fatalf("the restored entry is %+v, want the id of position %d", listed, next)
			}
			if got := s.state().Next; got != next+1 {
				t.Fatalf("restore left next at %d, want %d", got, next+1)
			}
			s.push(200, 201)
			if _, err := s.requeue(DeadSelector{DeadID: listed[0].DeadID}); err != nil {
				t.Fatalf("requeue: %v", err)
			}
			if st := s.state(); st.Entries != 2 || s.liveEntryKeys() != 2 {
				t.Fatalf("after the requeue: entries %d, live keys %d, want 2 and 2: the requeue wrote over the push", st.Entries, s.liveEntryKeys())
			}
			want := []int{100, 200}
			if s.kind == invariantLIFO {
				want = []int{200, 100}
			}
			if got := s.pop(2); !slices.Equal(got, want) {
				t.Fatalf("pop order = %v, want %v", got, want)
			}
			s.invariants()
		})
	}
}

// TestRestoreDeadHeapKeepsEachCopy: a heap restore uses the record's own priority
// and id with the next dead sequence, so two restores of one entry are two dead
// entries.
func TestRestoreDeadHeapKeepsEachCopy(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.restore(7, 0)
	s.restore(7, 0)
	listed, _ := s.deadPage(10, nil)
	if len(listed) != 2 || s.deadNumber(listed[0]) != 7 || s.deadNumber(listed[1]) != 7 || bytes.Equal(listed[0].DeadID, listed[1].DeadID) {
		t.Fatalf("the dead list is %+v, want two copies of entry 7 under two ids", listed)
	}
	if st := s.state(); st.Dead != 2 || st.DeadSeq != 2 {
		t.Fatalf("after two restores: dead %d, dead_seq %d, want 2 and 2", st.Dead, st.DeadSeq)
	}
	s.invariants()
}

// TestDeadVerbsCallTheReclaimHook (F-19): every store verb added here announces
// itself, as the reclaimer's test counts them.
func TestDeadVerbsCallTheReclaimHook(t *testing.T) {
	var seen []string
	hook := func(verb string) { seen = append(seen, verb) }
	ReclaimVerbHook.Store(&hook)
	t.Cleanup(func() { ReclaimVerbHook.Store(nil) })
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			seen = nil
			s.restore(0, 0)
			s.deadPage(1, nil)
			if _, err := s.requeue(DeadSelector{All: true}); err != nil {
				t.Fatalf("requeue: %v", err)
			}
			if _, err := s.discard(DeadSelector{All: true}); err != nil {
				t.Fatalf("discard: %v", err)
			}
			if len(seen) != 4 {
				t.Fatalf("the four dead verbs announced %v, want four", seen)
			}
			engine := "queue "
			if s.heap != nil {
				engine = "heap "
			}
			for _, verb := range seen {
				if !strings.HasPrefix(verb, engine) {
					t.Fatalf("verb %q does not name its engine %q", verb, engine)
				}
			}
		})
	}
}

// TestDeadListFirstVisitRefusalIsNotACompleteList (coordinator decision
// 2026-10-18): a visitor that refuses the very first record leaves next nil on a
// first page, which a caller reads as "list complete" while every entry remains.
// DeadList returns the caller's cursor unchanged and says it stopped short.
func TestDeadListFirstVisitRefusalIsNotACompleteList(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			for n := 0; n < 3; n++ {
				s.restore(n, 0)
			}
			refuseAll := func(DeadRecord) error { return ErrStopShort }
			next, stoppedShort, err := s.deadListN(5, nil, refuseAll)
			if err != nil {
				t.Fatalf("a refused first visit is an error: %v", err)
			}
			if !stoppedShort || next != nil {
				t.Fatalf("a first page refused at its first record = (next %x, stoppedShort %v), want the unchanged nil cursor and stoppedShort", next, stoppedShort)
			}
			rest, _ := s.deadPage(10, next)
			if len(rest) != 3 {
				t.Fatalf("listing again from the returned cursor found %d entries, want all 3", len(rest))
			}
			first, _ := s.deadPage(1, nil)
			resumeFrom := first[0].DeadID
			next, stoppedShort, err = s.deadListN(5, resumeFrom, refuseAll)
			if err != nil || !stoppedShort || !bytes.Equal(next, resumeFrom) {
				t.Fatalf("a refused first visit after %x = (next %x, stoppedShort %v, %v), want the cursor back unchanged", resumeFrom, next, stoppedShort, err)
			}
		})
	}
}
