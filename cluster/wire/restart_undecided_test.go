package wire

import (
	"context"
	"strings"
	"testing"
)

// BUG-0813: after a restart a pair stays closed until the node has applied its own log and its
// ownership check has seen the applied state. The refusal in that window must say which of the
// two it waits for, not that "the log tail has not been replayed".

// TestTheUndecidedCauseNamesWhatTheNodeWaitsFor: one text per half of the window, none once decided.
func TestTheUndecidedCauseNamesWhatTheNodeWaitsFor(t *testing.T) {
	log := restartUndecidedCause(false, false)
	check := restartUndecidedCause(true, false)
	for name, cause := range map[string]string{"log not applied": log, "state not seen": check} {
		if !strings.HasPrefix(cause, restartUndecidedPrefix) {
			t.Errorf("%s: cause %q lacks the prefix %q", name, cause, restartUndecidedPrefix)
		}
		if strings.Contains(cause, "log tail") || strings.Contains(cause, "replayed") {
			t.Errorf("%s: cause %q blames a log tail replay, which is not what the node waits for", name, cause)
		}
	}
	if !strings.Contains(log, "own log") {
		t.Errorf("first half: cause %q should say the node's own log is not applied", log)
	}
	if strings.Contains(check, "not applied its own log") {
		t.Errorf("second half: cause %q says the log is not applied, but it is", check)
	}
	if got := restartUndecidedCause(true, true); got != "" {
		t.Errorf("a decided node has the cause %q", got)
	}
}

// TestAReadInTheUndecidedWindowIsRefusedRetryablyWithItsCause: the owner's own read and a read another
// node forwarded to it are both refused (retryable, no previous owner to look at) while the window
// is open, and served once the handoff has seen the applied state. The forwarded read is refused
// like any read of a pair that is not decided: fail closed, as for BUG-0758. [BUG-0813]
func TestAReadInTheUndecidedWindowIsRefusedRetryablyWithItsCause(t *testing.T) {
	world := newU36World(t)
	for name, req := range map[string]ReadRequest{
		"a driver read":         {Slot: world.slot, DBID: testDB, BucketID: testBucket, ID: "p1"},
		"a peer-forwarded read": {Slot: world.slot, DBID: testDB, BucketID: testBucket, ID: "p1", FromNode: true, Hop: 1, FrameEpoch: testEpoch},
	} {
		t.Run(name, func(t *testing.T) {
			world.fx.setFlags(world.slot, 0)
			handoff := newU33Node("n1").h
			logApplied := false
			gate := NewReadGate(ReadGateConfig{
				Topology: world.fx.topo, Pairs: world.fx.pairs, Clock: fixedClock{}, Lease: world.fx.lease, Schema: world.fx.schema, Slots: world.states,
				Undecided: func() string { return restartUndecidedCause(logApplied, handoff.SawState(2)) },
			})
			for stage, want := range []string{"own log", "ownership"} {
				verdict, err := gate.Admit(context.Background(), req)
				if err != nil || !verdict.Held || !strings.HasPrefix(verdict.HoldCause, restartUndecidedPrefix) || !strings.Contains(verdict.HoldCause, want) {
					t.Fatalf("stage %d: verdict %+v err %v, want held with the undecided cause naming %q", stage, verdict, err, want)
				}
				text := u57refusal(t, gate.holdRefusal(req, verdict.HoldCause))
				for _, need := range []string{"restarted", "Retry in a few seconds", "Nothing was returned", "'nodes'"} {
					if !strings.Contains(text, need) {
						t.Errorf("stage %d: refusal %q lacks %q", stage, text, need)
					}
				}
				if strings.Contains(text, "previous owner") || strings.Contains(text, "replayed since the restart") {
					t.Errorf("stage %d: refusal %q blames something that is not the cause", stage, text)
				}
				logApplied = true
			}
			handoff.OnEpoch(u33State(2, 2, u33Active("n1", "n2")))
			world.fx.setFlags(world.slot, flagReplayed|flagStopProved)
			if verdict, err := gate.Admit(context.Background(), req); err != nil || verdict.Held || verdict.Forward {
				t.Fatalf("after the handoff saw the state: verdict %+v err %v, want the read served here", verdict, err)
			}
		})
	}
	// Guard: with no undecided window a pair that has not replayed keeps its own cause.
	plain := NewReadGate(ReadGateConfig{Topology: world.fx.topo, Pairs: world.fx.pairs, Clock: fixedClock{}, Lease: world.fx.lease, Schema: world.fx.schema, Slots: world.states})
	world.fx.setFlags(world.slot, flagStopProved)
	verdict, err := plain.Admit(context.Background(), ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, ID: "p1"})
	if err != nil || !verdict.Held || !strings.Contains(verdict.HoldCause, "replayed") {
		t.Fatalf("a decided node's unreplayed pair: verdict %+v err %v, want the replay cause", verdict, err)
	}
}

// TestAWriteInTheUndecidedWindowNamesTheCause: the text a held write comes back with carries the
// undecided cause, not the generic "handoff or a replay". [BUG-0813]
func TestAWriteInTheUndecidedWindowNamesTheCause(t *testing.T) {
	text := WriteAnswer{Held: true, HoldCause: restartUndecidedCause(false, false)}.notOrderedBecause()
	if !strings.Contains(text, "restarted") || !strings.Contains(text, "own log") || strings.Contains(text, "handoff or a replay") {
		t.Fatalf("write refusal cause %q must name the restart and what it waits for", text)
	}
}
