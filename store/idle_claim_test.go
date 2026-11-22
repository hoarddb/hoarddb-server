package store

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestIdleClaimAgreesWithTheClaim (claim/ack build step 14): IdleClaim answers
// true exactly when a claim stamped at the same time takes nothing and returns
// nothing, across the states a worker fleet polls: empty, an entry waiting,
// every entry under a live lease, a lease at its deadline, an owner clock a
// little behind (the clamp holds) and far behind (the stamp wins), and a lease
// granted while the clock was a day ahead (the forward-jump guard). Guards: the
// idle answer narrowed to Leases == 0; the guard's seek dropped.
func TestIdleClaimAgreesWithTheClaim(t *testing.T) {
	const day = int64(24 * 3600 * 1000)
	rows := []struct {
		name    string
		pushed  int
		claimAt int64 // 0: no claim before the check
		checkAt int64
		idle    bool
	}{
		{"empty", 0, 0, testNow, true},
		{"an entry waiting", 1, 0, testNow, false},
		{"every entry under a live lease", 2, testNow, testNow + 1_000, true},
		{"a lease at its deadline", 2, testNow, testNow + testLeaseMs, false},
		{"the owner's clock 2 s behind the lease clock", 2, testNow, testNow - 2_000, true},
		{"the owner's clock 10 minutes behind the lease clock", 2, testNow + 600_000, testNow, true},
		{"a lease granted while the clock was a day ahead", 2, testNow + day, testNow + 10_000, false},
	}
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		for _, row := range rows {
			t.Run(kind.String()+"/"+row.name, func(t *testing.T) {
				s := newDeliverySubject(t, kind, kvfault.New())
				if row.pushed > 0 {
					s.push(0, row.pushed)
				}
				if row.claimAt != 0 {
					s.claim(row.pushed, row.claimAt)
				}
				idleClaim := s.queue.IdleClaim
				if s.heap != nil {
					idleClaim = s.heap.IdleClaim
				}
				counts, idle, err := idleClaim(row.checkAt)
				if err != nil {
					t.Fatalf("IdleClaim(%d): %v", row.checkAt, err)
				}
				if idle != row.idle {
					t.Errorf("IdleClaim(%d) = %v, want %v (state %+v)", row.checkAt, idle, row.idle, s.state())
				}
				if st := s.state(); counts.Pending != st.Pending || counts.Dead != st.Dead || counts.Entries != st.Entries {
					t.Errorf("IdleClaim counted %+v, the state holds entries %d, pending %d, dead %d", counts, st.Entries, st.Pending, st.Dead)
				}
				claimed, _ := s.claim(1, row.checkAt)
				if tookOrReturned := len(claimed.Items) > 0 || claimed.Delivery.Returned > 0; tookOrReturned == idle {
					t.Errorf("IdleClaim(%d) = %v, and a claim at that time took %d entries and returned %d: the unlogged answer must agree with the logged one",
						row.checkAt, idle, len(claimed.Items), claimed.Delivery.Returned)
				}
				s.invariants()
			})
		}
	}
}
