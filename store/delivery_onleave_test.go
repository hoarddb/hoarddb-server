package store

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestPartialAckOfNamedItemsSubtractsOnlyThose: an ack that names items reports exactly the
// entries it removes to OnLeave, and a whole ack reports every entry of the lease; an entry
// that stays leased is never reported.
func TestPartialAckOfNamedItemsSubtractsOnlyThose(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 4)
			claim, taken := s.claim(4, testNow)
			if len(taken) != 4 {
				t.Fatalf("claimed %d entries, want 4", len(taken))
			}
			var left []int
			args := s.leaseArgs(claim.LeaseID, testNow+1)
			args.OnLeave = func(entry []byte) error {
				left = append(left, s.number(entry))
				return nil
			}
			named := [][]byte{claim.Items[1].ItemID, claim.Items[3].ItemID}
			if _, err := s.ack(args, named); err != nil {
				t.Fatal(err)
			}
			if len(left) != 2 || left[0] != taken[1] || left[1] != taken[3] {
				t.Fatalf("OnLeave saw entries %v, want only the named %d and %d", left, taken[1], taken[3])
			}
			left = nil
			if _, err := s.ack(args, nil); err != nil {
				t.Fatal(err)
			}
			if len(left) != 2 {
				t.Fatalf("a whole ack of the 2 that remained reported %v", left)
			}
		})
	}
}

// TestAnAckWithoutOnLeaveStillAcks: the common path is unchanged; the whole-lease ack
// walks keys only unless a verb asked to be told.
func TestAnAckWithoutOnLeaveStillAcks(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			claim, _ := s.claim(3, testNow)
			result, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+1), nil)
			if err != nil || result.Acked != 3 {
				t.Fatalf("ack = %+v, %v; want 3 acked", result, err)
			}
		})
	}
}

// TestADeadLetterChargesWhatOnLeaveWritesToTheBudget: the return step pays for the keys OnLeave adds for
// each entry it dead-letters out of the delivery budget, so a return step that would write more than
// the batch holds stops after the exempt first lease and leaves the rest leased for the next apply.
func TestADeadLetterChargesWhatOnLeaveWritesToTheBudget(t *testing.T) {
	deadLettered := func(t *testing.T, leaveBytes int) int {
		s := newDeliverySubject(t, invariantFIFO, kvfault.New())
		s.push(0, 2)
		now := testNow
		for round := 0; round < MaxDeliveries-1; round++ {
			claim, _ := s.claim(2, now)
			if _, err := s.release(s.leaseArgs(claim.LeaseID, now+1)); err != nil {
				t.Fatal(err)
			}
			now += 2
		}
		s.claim(1, now)
		s.claim(1, now)
		args := s.claimArgs(1, now+10*testLeaseMs)
		args.LeaveBytes, args.LeaveKeys = leaveBytes, 8
		args.OnLeave = func([]byte) error { return nil }
		claim, err := s.claimN(ApplyInfo{}, args, func([]byte) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		return claim.Delivery.DeadLettered
	}
	if got := deadLettered(t, 0); got != 2 {
		t.Fatalf("with nothing charged both expired leases dead-letter in one apply, got %d", got)
	}
	if got := deadLettered(t, int(deliveryBudgetBytes)); got != 1 {
		t.Fatalf("with a charge of the whole budget per entry only the exempt first lease dead-letters, got %d", got)
	}
}
