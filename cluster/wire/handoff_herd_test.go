package wire

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u57herd tests that a restart's herd of hold pairs cannot stall the supervisor: Tick is
// the supervisor's last step, and a long one delays the failure detector and every later step.
// (Before lease-renewer-cadence S2 it also delayed the lease renewal; the renewer now runs apart.)

// u57herdLink answers position queries like u33Link, except that a silent peer blocks until
// the caller's context ends, as a peer with open sockets that never answers does.
type u57herdLink struct {
	*u33Link
	mu      sync.Mutex
	blocked map[string]bool
	asked   int
}

func (l *u57herdLink) QueryPosition(ctx context.Context, peer string, query PositionQuery) (PositionReply, error) {
	l.mu.Lock()
	l.asked++
	block := l.blocked[peer]
	l.mu.Unlock()
	if block {
		<-ctx.Done()
		return PositionReply{}, ctx.Err()
	}
	return l.u33Link.QueryPosition(ctx, peer, query)
}

func (l *u57herdLink) queries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.asked
}

// u57herdNode is node B of a two-member RF 3 cluster (two copies, so both must answer) at epoch 1 that owns about half of the
// 8192 slots, every one of them in a restart hold, with the clock past the bound.
func u57herdNode(t *testing.T, blocked bool) (*u33Node, *u57herdLink, []uint16) {
	t.Helper()
	state := u33State(1, 3, u33Active("A", "B"))
	table := cluster.BuildTable(state)
	node := newU33Node("B")
	link := &u57herdLink{u33Link: newU33Link(), blocked: map[string]bool{"A": blocked}}
	link.replies["A"] = PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 1, Seq: 2}}
	node.h = NewHandoff(HandoffConfig{
		Self: "B", Pairs: node.pairs, Link: link, CatchUp: node.catch, Drain: node.drain, Clock: node.clock, Retry: u33Retry,
	})
	node.h.OnEpoch(state)
	var owned []uint16
	for slot := range uint16(barrierSlots) {
		if owner, _ := table.Owner(slot, 3); owner.ID == "B" {
			owned = append(owned, slot)
		}
	}
	if len(owned) < 4000 {
		t.Fatalf("B owns only %d slots; the herd needs 4000", len(owned))
	}
	for _, slot := range owned {
		u33SetApplied(node.pairs, slot, u33DB, cluster.Position{Epoch: 1, Seq: 2})
		if held, decided := node.h.RestartHold(slot, u33DB, 3, 1); !held || !decided {
			t.Fatalf("slot %d: held %v decided %v", slot, held, decided)
		}
	}
	node.clock.set(u57r19Bound)
	return node, link, owned
}

func u57herdOpen(node *u33Node, owned []uint16) int {
	open := 0
	for _, slot := range owned {
		if u33Flags(node.pairs, slot)&flagAwaiting == 0 {
			open++
		}
	}
	return open
}

// TestARestartHerdWithASilentCopyDoesNotStallTick: 4000+ pairs become provable on the same
// Tick and the copy never answers; one Tick must still end quickly and ask a bounded number of
// pairs, so the supervisor renews its lease. [GUARD: the per-Tick cap and budget]
func TestARestartHerdWithASilentCopyDoesNotStallTick(t *testing.T) {
	node, link, owned := u57herdNode(t, true)
	done := make(chan time.Duration, 1)
	go func() {
		began := time.Now()
		_ = node.h.Tick(context.Background())
		done <- time.Since(began)
	}()
	select {
	case took := <-done:
		if took > time.Second {
			t.Fatalf("one Tick took %s with a silent copy, want it bounded well under the 2 s step-down", took)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Tick was still running after 10 s with %d pairs and a silent copy: the supervisor would lose its lease", len(owned))
	}
	if asked := link.queries(); asked > handoffAdvancePerTick {
		t.Fatalf("one Tick asked about %d pairs, want at most %d", asked, handoffAdvancePerTick)
	}
	if open := u57herdOpen(node, owned); open != 0 {
		t.Fatalf("%d pairs opened with a silent copy that is not down", open)
	}
}

// TestARestartHerdOpensOverSeveralTicksAndEveryPairOpens: on a healthy cluster no Tick
// advances more than the cap and every pair opens in the end. The count of Ticks is logged.
func TestARestartHerdOpensOverSeveralTicksAndEveryPairOpens(t *testing.T) {
	node, link, owned := u57herdNode(t, false)
	ticks, most := 0, 0
	began := time.Now()
	for u57herdOpen(node, owned) < len(owned) {
		before := link.queries()
		if err := node.h.Tick(context.Background()); err != nil {
			t.Fatalf("Tick %d: %v", ticks, err)
		}
		ticks++
		most = max(most, link.queries()-before)
		if ticks > len(owned) {
			t.Fatalf("%d pairs of %d still held after %d Ticks", len(owned)-u57herdOpen(node, owned), len(owned), ticks)
		}
	}
	if most > handoffAdvancePerTick {
		t.Fatalf("a Tick advanced %d pairs, want at most %d", most, handoffAdvancePerTick)
	}
	t.Logf("%d pairs opened in %d Ticks (at most %d pairs per Tick) in %s of unit time", len(owned), ticks, most, time.Since(began))
}
