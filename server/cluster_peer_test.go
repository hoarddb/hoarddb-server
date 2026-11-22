package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// stubPeerConn is a peerConn whose failed state a test sets directly, so
// peer.dial's choice between reusing and redialing can be checked without a
// network.
type stubPeerConn struct {
	failed bool
	closed bool
}

func (s *stubPeerConn) SendRequest(context.Context, transport.Lane, transport.Opcode, []byte) (*transport.Frame, error) {
	return nil, errors.New("stubPeerConn sends nothing")
}

func (s *stubPeerConn) Generation() uint64 { return 1 }

func (s *stubPeerConn) SendRequestIfUnchanged(context.Context, transport.Lane, transport.Opcode, []byte, uint64) (*transport.Frame, error) {
	return nil, errors.New("stubPeerConn sends nothing")
}

func (s *stubPeerConn) Close() error {
	s.closed = true
	return nil
}

func (s *stubPeerConn) Failed() bool { return s.failed }

// TestPeerDialReplacesFailedClient is the cluster half of the fix: a
// pool that has gone out of service must not be handed to the next caller.
// Six internode callers never call peer.dropClient on error, so dial itself
// has to notice the failed pool and open a fresh one; before the fix it
// returned whatever client was installed. The peer's address is unparsable,
// so a redial attempt shows up as an address error rather than a network dial.
func TestPeerDialReplacesFailedClient(t *testing.T) {
	healthy := &stubPeerConn{}
	p := &peer{node: ring.Node{ID: "peer-b", Addr: "no-port-here"}, client: healthy}
	cli, err := p.dial(context.Background())
	if err != nil {
		t.Fatalf("dial with a healthy client installed: %v", err)
	}
	if cli != healthy {
		t.Fatal("dial redialed although the installed client was healthy")
	}

	healthy.failed = true
	if _, err := p.dial(context.Background()); err == nil {
		t.Fatal("dial returned the installed client although it has failed; want a redial (which fails here on the unparsable address)")
	}
	if healthy.closed {
		t.Fatal("dial closed the failed client; its in-flight requests on other sockets must be left to finish")
	}

	replacement := &stubPeerConn{}
	if got := p.installClient(replacement); got != replacement {
		t.Fatal("installClient kept a failed client instead of the freshly dialed one")
	}
	if replacement.closed {
		t.Fatal("installClient closed the fresh client it installed")
	}
}

// TestPeerLivenessEscalation verifies the alert/repeat/recover logic verifyPeer
// relies on: silent below the threshold, one loud alert on crossing it, no
// repeat before peerUnreachableAlertInterval has passed, another alert once
// it has, and a clean reset (with the correct downtime duration) on recovery.
func TestPeerLivenessEscalation(t *testing.T) {
	p := &peer{}
	t0 := time.Now()

	// Still within the threshold: no alert yet, but the clock has started.
	if alert, downFor := p.recordUnreachable(t0); alert {
		t.Fatalf("recordUnreachable at t0: alert = true, want false (downFor=%v)", downFor)
	}
	if alert, downFor := p.recordUnreachable(t0.Add(peerUnreachableAlertThreshold - time.Second)); alert {
		t.Fatalf("recordUnreachable just under threshold: alert = true, want false (downFor=%v)", downFor)
	}

	// Crossing the threshold fires exactly one alert.
	tAlert := t0.Add(peerUnreachableAlertThreshold + time.Second)
	alert, downFor := p.recordUnreachable(tAlert)
	if !alert {
		t.Fatalf("recordUnreachable crossing threshold: alert = false, want true")
	}
	if downFor < peerUnreachableAlertThreshold {
		t.Fatalf("downFor = %v, want >= %v", downFor, peerUnreachableAlertThreshold)
	}

	// Immediately after: still down, but no repeat until the alert interval
	// passes — otherwise a long outage would spam the log every retry.
	if alert, _ := p.recordUnreachable(tAlert.Add(time.Second)); alert {
		t.Fatal("recordUnreachable right after an alert fired again before the repeat interval elapsed")
	}

	// Past the repeat interval: alerts again.
	tRepeat := tAlert.Add(peerUnreachableAlertInterval + time.Second)
	if alert, _ := p.recordUnreachable(tRepeat); !alert {
		t.Fatal("recordUnreachable past the repeat interval did not alert again")
	}

	// Recovery reports the total downtime and resets tracking.
	tRecovered := tRepeat.Add(5 * time.Second)
	total := p.recordReachable(tRecovered)
	wantTotal := tRecovered.Sub(t0)
	if total != wantTotal {
		t.Fatalf("recordReachable downtime = %v, want %v", total, wantTotal)
	}

	// A second recordReachable call with nothing unreachable in between
	// reports zero — there is nothing to recover from.
	if total := p.recordReachable(tRecovered.Add(time.Minute)); total != 0 {
		t.Fatalf("recordReachable with no prior outage = %v, want 0", total)
	}

	// The cycle repeats cleanly: a fresh outage starts its own threshold
	// countdown rather than inheriting the previous one's state.
	if alert, _ := p.recordUnreachable(tRecovered.Add(time.Minute)); alert {
		t.Fatal("recordUnreachable on a fresh outage alerted immediately; want it to start below threshold again")
	}
}
