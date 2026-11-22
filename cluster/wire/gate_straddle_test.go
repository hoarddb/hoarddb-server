package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// straddle is a read gate over a real Handoff whose Settled callback runs the epoch feed
// the first time it is asked: the supervisor landing between the gate's reads (BUG-0758).
type straddle struct {
	gate *ReadGate
	slot uint16
	fed  bool
	node *u33Node
}

func newStraddle(t *testing.T) *straddle {
	t.Helper()
	from := u33State(1, 2, u33Active("n1", "n2", "n3"), int64(testDB))
	to := u33State(2, 2, []cluster.Member{u33Member("n1", cluster.MemberDown), u33Member("n2", cluster.MemberActive), u33Member("n3", cluster.MemberActive)}, int64(testDB))
	slot := u33Moved(t, from, to, 2, "n1", "n2")[0]
	node := newU33Node("n2")
	node.h.OnEpoch(from)
	// The pair was first touched while n2 was only a copy, so it carries both bits.
	NewPairOwnership(node.pairs).Acquire(slot, testDB)

	fx := newFixture(t)
	fx.topo.committed, fx.topo.self, fx.topo.members = 2, "n2", 3
	fx.topo.table = cluster.BuildTable(to)
	fx.topo.dbRow.RF = 2
	s := &straddle{slot: slot, node: node}
	s.gate = NewReadGate(ReadGateConfig{
		Topology: fx.topo, Pairs: node.pairs, Clock: fixedClock{}, Lease: fx.lease, Schema: fx.schema,
		Slots: settledSlots{}, Settled: func(committed uint64) bool {
			if !s.fed {
				s.fed = true
				node.h.OnEpoch(to)
			}
			return node.h.Applied(committed)
		},
	})
	return s
}

// TestEvaluateJudgesAPairReadAfterTheFeedItsSettledAnswerSawBeforeIt (BUG-0758): a Settled
// true must judge a pair read at or after the feed, or the gate admits a pair whose barrier
// opened after its snapshot.
func TestEvaluateJudgesAPairReadAfterTheFeedItsSettledAnswerSawBeforeIt(t *testing.T) {
	s := newStraddle(t)
	req := ReadRequest{Slot: s.slot, DBID: testDB}
	run := s.gate.evaluate(req, DatabaseInfo{RF: 2}, false)
	if !s.fed {
		t.Fatal("setup: Settled was never asked")
	}
	if run.cause == "" || !run.decision.Hold {
		t.Fatalf("slot %d admitted (cause %q, hold %v) although its barrier opened after the pair snapshot", s.slot, run.cause, run.decision.Hold)
	}
}

// TestScanAdmissionJudgesAPairReadAfterTheFeed (BUG-0758): a scan has no second admission,
// so the same straddle serves SLOT_LIST and SLOT_COUNT before the barrier.
func TestScanAdmissionJudgesAPairReadAfterTheFeed(t *testing.T) {
	s := newStraddle(t)
	var mask cluster.SlotMask
	mask.Set(s.slot)
	if err := s.gate.AdmitMask(t.Context(), testDB, testBucket, &mask); err == nil {
		t.Fatalf("a scan of slot %d was admitted although its barrier opened after the pair snapshot", s.slot)
	}
}
