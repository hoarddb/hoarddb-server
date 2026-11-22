package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u57r19Hold is a Hold func with a fixed answer that counts its calls.
type u57r19Hold struct {
	held, decided bool
	calls         int
}

func (h *u57r19Hold) hold(uint16, uint64) (bool, bool) {
	h.calls++
	return h.held, h.decided
}

func u57r19Own(restarted bool, hold *u57r19Hold) (*PairOwnership, *cluster.PairSet) {
	pairs := &cluster.PairSet{}
	return NewRestartedPairOwnership(pairs, PairOwnershipConfig{Restarted: restarted, Hold: hold.hold}), pairs
}

// TestAcquireAfterARestartLeavesAHeldPairUnflagged: the pair the Handoff holds keeps flags 0
// (both gates then hold it) and is not asked about again. [GUARD: Acquire claiming a held pair]
func TestAcquireAfterARestartLeavesAHeldPairUnflagged(t *testing.T) {
	hold := &u57r19Hold{held: true, decided: true}
	own, pairs := u57r19Own(true, hold)
	own.Acquire(5, testDB)
	own.Acquire(5, testDB)
	if flags := pairs.Snapshot(5, testDB).Flags; flags != 0 {
		t.Fatalf("a held pair has flags %#b after Acquire, want 0", flags)
	}
	if hold.calls != 1 {
		t.Fatalf("Hold was asked %d times for one pair, want once", hold.calls)
	}
}

// TestAcquireAfterARestartClaimsACopy: a pair this node does not own keeps today's rule, so a
// copy applies records. [GUARD: the held=false row]
func TestAcquireAfterARestartClaimsACopy(t *testing.T) {
	own, pairs := u57r19Own(true, &u57r19Hold{held: false, decided: true})
	own.Acquire(5, testDB)
	if flags := pairs.Snapshot(5, testDB).Flags; flags != flagReplayed|flagStopProved {
		t.Fatalf("a copy has flags %#b after Acquire, want replayed and stopProved", flags)
	}
}

// TestAcquireBeforeTheStateIsAppliedFailsClosedAndAsksAgain: an undecided pair keeps flags 0
// and the next touch asks again. [GUARD: decided=false treated as not held]
func TestAcquireBeforeTheStateIsAppliedFailsClosedAndAsksAgain(t *testing.T) {
	hold := &u57r19Hold{}
	own, pairs := u57r19Own(true, hold)
	own.Acquire(5, testDB)
	if flags := pairs.Snapshot(5, testDB).Flags; flags != 0 {
		t.Fatalf("an undecided pair has flags %#b, want 0", flags)
	}
	hold.held, hold.decided = false, true
	own.Acquire(5, testDB)
	if hold.calls != 2 || pairs.Snapshot(5, testDB).Flags != flagReplayed|flagStopProved {
		t.Fatalf("after the state applied: %d Hold calls, flags %#b; want a second ask and the ordinary rule", hold.calls, pairs.Snapshot(5, testDB).Flags)
	}
}

// TestAcquireOnAFirstBootNeverAsks: a fresh cluster serves its first write at once.
// [GUARD: ignoring Restarted]
func TestAcquireOnAFirstBootNeverAsks(t *testing.T) {
	hold := &u57r19Hold{held: true, decided: true}
	own, pairs := u57r19Own(false, hold)
	own.Acquire(5, testDB)
	if hold.calls != 0 || pairs.Snapshot(5, testDB).Flags != flagReplayed|flagStopProved {
		t.Fatalf("first boot: %d Hold calls, flags %#b; want none and the ordinary rule", hold.calls, pairs.Snapshot(5, testDB).Flags)
	}
}
