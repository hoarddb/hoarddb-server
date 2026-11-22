package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestTheNodeWiresTheReporter: a networked node owns one reporter and its reseed hook is the
// reporter's Clear, so a reseed cannot leave a committed bit over a range it is about to
// reset. [BREAK: leave parts.hooks.BeforeReset nil in startCluster]
func TestTheNodeWiresTheReporter(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	if node.cl.reporter == nil {
		t.Fatal("the node has no seed reporter")
	}
	if node.cl.seeding.Load().hooks.BeforeReset == nil {
		t.Fatal("the reseed has no BeforeReset hook, so a reseed would leave its committed bit set")
	}
}

// TestTheReseedWaitsForItsClearWithinTheBudget: the hook the node installs returns only after
// the RETIRE that clears the bit was answered, and not before. [BREAK: send the RETIRE from a
// goroutine in Clear and return at once]
func TestTheReseedWaitsForItsClearWithinTheBudget(t *testing.T) {
	const slowSend = 300 * time.Millisecond
	w := newReportWorld(1)
	w.setCommittedBit(t, 1, 9)
	answered := make(chan time.Time, 1)
	w.sendFn = func(int) error {
		time.Sleep(slowSend)
		answered <- time.Now()
		return nil
	}
	r := w.reporter()
	hook := func(ctx context.Context, slot uint16, db uint64) error { return r.Clear(ctx, db, slot) }

	started := time.Now()
	if err := hook(context.Background(), 9, 1); err != nil {
		t.Fatalf("the hook: %v", err)
	}
	returned := time.Now()
	select {
	case at := <-answered:
		if returned.Before(at) {
			t.Fatalf("the hook returned %v before the RETIRE was answered", at.Sub(returned))
		}
	default:
		t.Fatal("the hook returned before the RETIRE was answered")
	}
	if took := returned.Sub(started); took < slowSend || took > relayCallTimeout {
		t.Fatalf("the hook took %v, want between the send's %v and the budget %v", took, slowSend, relayCallTimeout)
	}
	if sends := w.proposals(cluster.CommandRetire); len(sends) != 1 || len(setSlots(t, sends[0].bits)) != 1 || setSlots(t, sends[0].bits)[0] != 9 {
		t.Fatalf("proposals %+v, want one RETIRE for slot 9", w.allSends())
	}
}
