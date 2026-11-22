package wire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// A restart hold waits for nobody, so its refusals must say that this node restarted and for
// how long, and must not send the developer to look at a previous owner.

func u57refusal(t *testing.T, err error) string {
	t.Helper()
	var refusal *cluster.Error
	if !errors.As(err, &refusal) {
		t.Fatalf("got %v, want a *cluster.Error", err)
	}
	return refusal.Message
}

func u57restartHeld(t *testing.T) (*u33Node, uint16) {
	t.Helper()
	node, slot := u57r19Node(t, cluster.Position{Epoch: 1, Seq: 4})
	if held, decided := node.h.RestartHold(slot, u33DB, 3, 1); !held || !decided {
		t.Fatalf("RestartHold = %v %v", held, decided)
	}
	return node, slot
}

// TestAReadParkedByARestartHoldIsRefusedNamingTheRestart: past its deadline, a read held by
// the peer-less restart barrier says this node restarted, gives the wait and says to retry; a
// handoff with a named previous owner keeps its own text. [GUARD: the peer-less branch of overdue]
func TestAReadParkedByARestartHoldIsRefusedNamingTheRestart(t *testing.T) {
	node, slot := u57restartHeld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	got := u33Result(t, u33Park(node, ctx, HeldRequest{Slot: slot, DB: u33DB, RequestID: [16]byte{1}, Read: true}))
	text := u57refusal(t, got.err)
	for _, want := range []string{"restarted", "step_down_ms", "Retry in a few seconds", "Nothing was returned"} {
		if !strings.Contains(text, want) {
			t.Errorf("refusal %q lacks %q", text, want)
		}
	}
	if strings.Contains(text, "previous owner") {
		t.Errorf("refusal %q sends the developer to a previous owner nobody waits for", text)
	}

	moved, movedSlots := u33Awaiting(t, cluster.Position{})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	other := u33Result(t, u33Park(moved, ctx2, HeldRequest{Slot: movedSlots[0], DB: u33DB, RequestID: [16]byte{2}, Body: []byte("w")}))
	if text := u57refusal(t, other.err); !strings.Contains(text, "previous owner") {
		t.Errorf("a handoff with a named previous owner lost its text: %q", text)
	}
}

// TestTheReadGateNamesTheRestartHold: the cause the gate prints for a peer-less awaiting pair
// is the restart, and its refusal does not point at 'nodes' for a previous owner.
func TestTheReadGateNamesTheRestartHold(t *testing.T) {
	// The refusal names the bucket, so the gate needs the topology row and the declaration cache.
	gate := NewReadGate(ReadGateConfig{Topology: &fakeTopology{committed: testEpoch, self: "n1", members: 1}, Schema: &fakeSchema{}})
	req := ReadRequest{Slot: 7, DBID: 1}
	restart := gate.holdCause(cluster.Pair{Flags: flagAwaiting}, true, true, req)
	if !strings.Contains(restart, "restarted") {
		t.Fatalf("cause of a peer-less awaiting pair = %q, want it to name the restart", restart)
	}
	named := gate.holdCause(cluster.Pair{Flags: flagAwaiting, Peer: "A"}, true, true, req)
	if strings.Contains(named, "restarted") {
		t.Fatalf("cause of a handoff from A = %q, must not name a restart", named)
	}
	text := u57refusal(t, gate.holdRefusal(req, restart))
	if !strings.Contains(text, "Retry in a few seconds") || strings.Contains(text, "previous owner") {
		t.Fatalf("restart refusal %q must say retry and not point at a previous owner", text)
	}
}

// TestAWriteHeldByARestartIsRefusedWithItsCause: the answer a held write comes back with
// names the restart in the one text every writer uses. [GUARD: HoldCause set in nodeSender.Write]
func TestAWriteHeldByARestartIsRefusedWithItsCause(t *testing.T) {
	node, slot := u57restartHeld(t)
	answer := WriteAnswer{Held: true, HoldCause: node.h.HoldCause(slot, u33DB)}
	text := answer.notOrderedBecause()
	if !strings.Contains(text, "restarted") || strings.Contains(text, "held true") {
		t.Fatalf("write refusal cause %q must name the restart and not print internal flags", text)
	}
	if forward := (WriteAnswer{Forward: true}).notOrderedBecause(); !strings.Contains(forward, "not the owner") {
		t.Fatalf("a forward answer's cause is %q", forward)
	}
	if none := node.h.HoldCause(slot+1, u33DB); none != "" {
		t.Fatalf("a pair with no hold has cause %q", none)
	}
}
