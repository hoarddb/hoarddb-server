package wire

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

func TestTheMemberMarkKeepsTheHighestEpochAndEndsWhenTheNodeCatchesUp(t *testing.T) {
	var mark behindMark
	if _, behind := mark.Behind(0, 0); behind {
		t.Fatal("a node that saw nothing is behind")
	}
	mark.MarkMember(9, time.Second)
	mark.MarkMember(7, 2*time.Second)
	if _, behind := mark.Behind(8, 3*time.Second); !behind {
		t.Fatal("not behind at 8 after a mark at 9")
	}
	if _, behind := mark.Behind(9, 3*time.Second); behind {
		t.Fatal("still behind at 9, the epoch it saw")
	}
}

// TestADelayedGrantCannotClearAMarkSetAfterItWasAsked (pi, mark-clearing race): the leader
// issued a grant at E, committed E+1, and a member's frame at E+1 marked this node; the grant
// then arrives. The grant was asked before the mark, so it proves nothing about E+1.
// [GUARD: clear on any grant]
func TestADelayedGrantCannotClearAMarkSetAfterItWasAsked(t *testing.T) {
	var mark behindMark
	mark.MarkMember(6, 10*time.Second) // a frame stamped with epoch 6; this node is at 5
	mark.Confirm(5, 8*time.Second)     // a grant at epoch 5 to a request sent at 8 s
	if _, behind := mark.Behind(5, 11*time.Second); !behind {
		t.Fatal("a grant to a request sent before the mark cleared it")
	}
	mark.Confirm(5, 12*time.Second) // asked after the mark and still at 5: the frame lied
	if _, behind := mark.Behind(5, 13*time.Second); behind {
		t.Fatal("a grant to a request sent after the mark, naming a lower epoch, did not clear it")
	}
}

// TestAClientMarkExpiresAndCannotBeRearmedAtOnce (pi): a client that repeats a forged epoch
// ten times a second silences the node for one short window, not for as long as it asks.
// [GUARD: no expiry; no quiet period]
func TestAClientMarkExpiresAndCannotBeRearmedAtOnce(t *testing.T) {
	var mark behindMark
	start := 10 * time.Second
	mark.MarkClient(99, start)
	if _, behind := mark.Behind(5, start+clientMarkTTL/2); !behind {
		t.Fatal("a client mark did not stop the node while fresh")
	}
	if _, behind := mark.Behind(5, start+clientMarkTTL+time.Millisecond); behind {
		t.Fatal("a client mark outlived its time")
	}
	for at := start + clientMarkTTL; at < start+clientMarkTTL+clientMarkQuiet; at += 100 * time.Millisecond {
		mark.MarkClient(99, at)
		if _, behind := mark.Behind(5, at+time.Millisecond); behind {
			t.Fatalf("a repeated client mark re-armed the node at +%s, inside the quiet period", at-start)
		}
	}
	mark.MarkClient(99, start+clientMarkTTL+clientMarkQuiet+time.Millisecond)
	if _, behind := mark.Behind(5, start+clientMarkTTL+clientMarkQuiet+2*time.Millisecond); !behind {
		t.Fatal("a client mark after the quiet period was ignored")
	}
}

type stepTwoBLease struct{ marked atomic.Uint64 }

func (l *stepTwoBLease) Serve(machine.ServeCheck) machine.LeaseDecision {
	return machine.LeaseDecision{Valid: true}
}
func (l *stepTwoBLease) MarkBehind(epoch uint64, _ bool) { l.marked.Store(epoch) }

func TestMarkBehindReachesALeaseThatHoldsTheMark(t *testing.T) {
	lease := &stepTwoBLease{}
	markBehind(lease, 12, false)
	if lease.marked.Load() != 12 {
		t.Fatalf("the lease was told %d, want 12", lease.marked.Load())
	}
	markBehind(NewServedLease(machine.LeaseConfig{}), 12, false) // a one-member node holds no mark: no panic, no effect
}

func TestSeenEpochIsTheFramesForANodeAndThePrologueForAClient(t *testing.T) {
	if got := seenEpoch(true, 7, 3); got != 7 {
		t.Errorf("node frame: %d, want 7", got)
	}
	if got := seenEpoch(false, 7, 3); got != 3 {
		t.Errorf("client: %d, want 3", got)
	}
}

// TestACopyThatSawALaterEpochTellsTheMarkAndAppliesNothingAtTheOldOne: a pushed frame from a
// later epoch marks the node; while it is marked a frame at the old epoch is not applied
// and is answered not-applied, so the owner keeps it in flight. [GUARD: skip either]
func TestACopyThatSawALaterEpochTellsTheMarkAndAppliesNothingAtTheOldOne(t *testing.T) {
	var mark behindMark
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.MarkBehind = func(epoch uint64) { mark.MarkMember(epoch, time.Second) }
		cfg.Behind = func(committed uint64) bool { _, behind := mark.Behind(committed, time.Second); return behind }
	})
	copyNode.ownerID = "owner"
	if _, err := copyNode.rep.OnWrite(u32Frame(t, u32Epoch+1, u32Pos(7, 2), cluster.Position{})); u32Code(err) != cluster.CodeEpochFenced {
		t.Fatalf("a frame from a later epoch: %v, want EPOCH_FENCED", err)
	}
	if _, behind := mark.Behind(u32Epoch, time.Second); !behind {
		t.Fatal("the later epoch was not marked")
	}
	reply, err := copyNode.rep.OnWrite(u32Frame(t, u32Epoch, u32Pos(u32Epoch, 1), cluster.Position{}))
	if err != nil {
		t.Fatalf("a frame at the old epoch while behind: %v", err)
	}
	if reply.Ack.Outcome != AckNotApplied {
		t.Fatalf("a frame at the old epoch while behind was answered %v, want not-applied", reply.Ack.Outcome)
	}
	if copyNode.pairs.Snapshot(u32Slot, u32DB).AppliedPos != (cluster.Position{}) {
		t.Fatal("a record was applied by a node that had stopped serving")
	}
}

// TestARequestAheadOfTheCommittedEpochMarksTheNodeBehindAndOneBelowDoesNot: the owner path
// and the read path both tell the lease, for a client prologue and for a node frame.
// [GUARD: drop noteBehind; drop the read gate's call]
func TestARequestAheadOfTheCommittedEpochMarksTheNodeBehindAndOneBelowDoesNot(t *testing.T) {
	t.Run("write, client epoch ahead", func(t *testing.T) {
		fx := newFixture(t)
		req := fx.request(testSlot, "p1")
		req.ClientEpoch = testEpoch + 3
		if _, err := fx.w.Write(context.Background(), req); err == nil || codeOf(t, err) != cluster.CodeStaleEpoch {
			t.Fatalf("err = %v, want STALE_EPOCH", err)
		}
		if got := fx.lease.marked.Load(); got != testEpoch+3 {
			t.Fatalf("the lease was told epoch %d, want %d", got, uint64(testEpoch+3))
		}
	})
	t.Run("write, node frame ahead", func(t *testing.T) {
		fx := newFixture(t)
		req := fx.request(testSlot, "p1")
		req.FromNode, req.FrameEpoch = true, testEpoch+2
		if _, err := fx.w.Write(context.Background(), req); err == nil || codeOf(t, err) != cluster.CodeEpochFenced {
			t.Fatalf("err = %v, want EPOCH_FENCED", err)
		}
		if got := fx.lease.marked.Load(); got != testEpoch+2 {
			t.Fatalf("the lease was told epoch %d, want %d", got, uint64(testEpoch+2))
		}
	})
	t.Run("write, client epoch behind", func(t *testing.T) {
		fx := newFixture(t)
		req := fx.request(testSlot, "p1")
		req.ClientEpoch = testEpoch - 1
		_, _ = fx.w.Write(context.Background(), req)
		if got := fx.lease.marked.Load(); got != 0 {
			t.Fatalf("a client behind the node marked the node behind at %d", got)
		}
	})
	t.Run("read, client epoch ahead", func(t *testing.T) {
		world := newU36World(t)
		_, err := world.reader.Read(context.Background(), ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ClientEpoch: testEpoch + 1})
		u36Refusal(t, err, cluster.CodeStaleEpoch)
		if got := world.fx.lease.marked.Load(); got != testEpoch+1 {
			t.Fatalf("the lease was told epoch %d, want %d", got, uint64(testEpoch+1))
		}
	})
}

type stepTwoBMarkingLease struct {
	ServeChecker
	marked atomic.Uint64
}

func (l *stepTwoBMarkingLease) MarkBehind(epoch uint64, _ bool) { l.marked.Store(epoch) }

// TestAClientEpochAheadOfTheNodeAtTheRingFrontMarksTheNodeBehind: the ring server checks the
// client prologue before the write or read path sees it, so it tells the lease itself.
// [GUARD: drop the call in checkClientEpoch]
func TestAClientEpochAheadOfTheNodeAtTheRingFrontMarksTheNodeBehind(t *testing.T) {
	env := u66Start(t)
	lease := &stepTwoBMarkingLease{ServeChecker: env.node.lease}
	env.node.lease = lease
	c := env.login(t)
	u66Reply(t, c.keyed(env.doc(t, cluster.OpInsert, "mark", "x")))

	ahead := env.read(t, RingClassGet, "mark", "", 0)
	ahead.Prologue.Epoch = env.node.topology.Committed() + 7
	u66Refusal(t, c.keyed(ahead), cluster.CodeStaleEpoch)
	if got, want := lease.marked.Load(), env.node.topology.Committed()+7; got != want {
		t.Fatalf("the lease was told epoch %d, want %d", got, want)
	}
}

// TestABehindNodeRefusesWithTheEpochTextNotTheLeaseText (pi F12): a node that learned the
// cluster moved is not one whose lease lapsed, and the words must not send the caller
// chasing a renewal. [GUARD: drop behindRefusal]
func TestABehindNodeRefusesWithTheEpochTextNotTheLeaseText(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		fx := newFixture(t)
		fx.topo.setMembers(3)
		fx.lease.valid.Store(false)
		fx.lease.marked.Store(testEpoch + 3)
		_, err := fx.w.Write(context.Background(), fx.request(testSlot, "p1"))
		if err == nil || codeOf(t, err) != cluster.CodeStaleEpoch || strings.Contains(err.Error(), "lease") {
			t.Fatalf("err = %v, want STALE_EPOCH text that does not mention a lease", err)
		}
		node := fx.request(testSlot, "p2")
		node.FromNode, node.FrameEpoch = true, testEpoch
		if _, err := fx.w.Write(context.Background(), node); err == nil || codeOf(t, err) != cluster.CodeEpochFenced {
			t.Fatalf("a node frame: err = %v, want EPOCH_FENCED", err)
		}
	})
	t.Run("read", func(t *testing.T) {
		world := newU36World(t)
		world.fx.topo.setMembers(3)
		world.fx.lease.valid.Store(false)
		world.fx.lease.marked.Store(testEpoch + 3)
		_, err := world.reader.Read(context.Background(), ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1")})
		u36Refusal(t, err, cluster.CodeStaleEpoch)
	})
	t.Run("a lapsed lease keeps its own text", func(t *testing.T) {
		fx := newFixture(t)
		fx.topo.setMembers(3)
		fx.lease.valid.Store(false)
		if _, err := fx.w.Write(context.Background(), fx.request(testSlot, "p1")); err == nil || codeOf(t, err) != cluster.CodeLeaseLapsed {
			t.Fatalf("err = %v, want LEASE_LAPSED", err)
		}
	})
}
