package store

import (
	"bytes"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// counts reads the subject's DeliveryCounts through Counts and through PeekCounts.
func (s *deliverySubject) counts() (viaCounts, viaPeek DeliveryCounts) {
	s.t.Helper()
	var err error
	if s.heap != nil {
		if viaCounts, err = s.heap.Counts(); err != nil {
			s.t.Fatalf("%s Counts: %v", s.name, err)
		}
		viaPeek, err = s.heap.PeekCounts(1, func([]byte) error { return nil })
	} else {
		if viaCounts, err = s.queue.Counts(); err != nil {
			s.t.Fatalf("%s Counts: %v", s.name, err)
		}
		viaPeek, err = s.queue.PeekCounts(1, func([]byte) error { return nil })
	}
	if err != nil {
		s.t.Fatalf("%s PeekCounts: %v", s.name, err)
	}
	return viaCounts, viaPeek
}

// TestCountsReportPendingDeadAndTheSoonestDeadline (claim-ack-spec 8.1, 8.2): a
// read reports the claimed and dead entries and the soonest lease deadline from
// the one state document, and two reads of the same snapshot agree.
func TestCountsReportPendingDeadAndTheSoonestDeadline(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 6)
			s.claim(2, testNow)
			s.claim(1, testNow+5_000)
			s.restore(7, 0)
			byCounts, byPeek := s.counts()
			want := DeliveryCounts{Entries: 3, Pending: 3, Dead: 1, NextDeadline: testNow + testLeaseMs}
			if byCounts != want || byPeek != want {
				t.Fatalf("Counts = %+v, PeekCounts = %+v; want both %+v", byCounts, byPeek, want)
			}
		})
	}
}

// TestCountsOfABucketThatNeverClaimedReadNoDeadlineKey (rule 11, CA-34): with no
// lease there is no seek for a deadline and no deadline; with one there is
// exactly one iterator on the 'd' range, for Counts and for PeekCounts alike.
func TestCountsOfABucketThatNeverClaimedReadNoDeadlineKey(t *testing.T) {
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String(), func(t *testing.T) {
			var lowers [][]byte
			s := newDeliverySubject(t, kind, seekRecorder{KV: kvfault.New(), lowers: &lowers})
			s.push(0, 3)
			deadlineRange := func() int {
				n := 0
				for _, lower := range lowers {
					if bytes.HasPrefix(lower, kindKey(s.slot, 1, s.id, kindDeadline)) {
						n++
					}
				}
				return n
			}
			lowers = nil
			byCounts, byPeek := s.counts()
			if byCounts != (DeliveryCounts{Entries: 3}) || byPeek != byCounts {
				t.Fatalf("an idle bucket's counts are %+v and %+v, want 3 entries and nothing else", byCounts, byPeek)
			}
			if got := deadlineRange(); got != 0 {
				t.Fatalf("a bucket that never claimed opened %d iterators on 'd'", got)
			}
			s.claim(1, testNow)
			lowers = nil
			s.counts()
			if got := deadlineRange(); got != 2 {
				t.Fatalf("a bucket with a lease opened %d iterators on 'd' for two reads, want one each", got)
			}
		})
	}
}

// TestPopResultCarriesTheCountsOfTheStateItHolds: a pop that finds nothing says
// how many entries are claimed, so an empty answer can say why.
func TestPopResultCarriesTheCountsOfTheStateItHolds(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 2)
			s.claim(2, testNow)
			popped, err := s.popN(ApplyInfo{}, 1, testNow, func([]byte) error { return nil })
			if err != nil || popped.Removed != 0 || popped.Pending != 2 || popped.Dead != 0 {
				t.Fatalf("pop of a bucket whose entries are all claimed = %+v, %v; want nothing removed and 2 pending", popped, err)
			}
		})
	}
}
