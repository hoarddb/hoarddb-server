package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// answeringPeerConn answers every send at once and runs after on each, so a
// test decides what a frame does to the replica behind the stream.
type answeringPeerConn struct {
	stubPeerConn
	after func()
}

func (a *answeringPeerConn) SendRequest(context.Context, transport.Lane, transport.Opcode, []byte) (*transport.Frame, error) {
	if a.after != nil {
		a.after()
	}
	return &transport.Frame{}, nil
}

// TestPushPaceDeliversNoMoreThanItsCap guards BUG-0510's second half: the cap
// was checked before the delay and counted after it, so pushes in flight
// together all passed the check and the stream delivered more than deliver.
func TestPushPaceDeliversNoMoreThanItsCap(t *testing.T) {
	const deliverCap, inFlight = 3, 12
	pace := &pushPace{delay: 20 * time.Millisecond, deliver: deliverCap}
	conn := pace.wrap(&answeringPeerConn{})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var answered atomic.Int64
	var wg sync.WaitGroup
	for range inFlight {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := conn.SendRequest(ctx, transport.LaneControl, transport.OpReplPush, nil); err == nil {
				answered.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := answered.Load(); got != deliverCap {
		t.Errorf("%d pushes were answered with deliver %d", got, deliverCap)
	}
	if got := pace.delivered.Load(); got != deliverCap {
		t.Errorf("the pace counts %d delivered with deliver %d", got, deliverCap)
	}
}

// TestPushPaceTimesTheLastAdvanceNotTheLastFrame guards BUG-0510's first
// half: a push that carries none of the bucket's entries is a frame answered
// but not an advance of the replica, and the follower's wait clock ignores it.
func TestPushPaceTimesTheLastAdvanceNotTheLastFrame(t *testing.T) {
	var held atomic.Int64
	pace := &pushPace{delay: 30 * time.Millisecond, held: held.Load}
	conn := &answeringPeerConn{}
	wrapped := pace.wrap(conn)
	send := func(raises int64) {
		t.Helper()
		conn.after = func() { held.Add(raises) }
		if _, err := wrapped.SendRequest(context.Background(), transport.LaneControl, transport.OpReplPush, nil); err != nil {
			t.Fatalf("a push failed: %v", err)
		}
	}
	send(64)
	advanced := pace.lastAdvanced()
	if advanced.IsZero() {
		t.Fatal("a push that raised the count was not recorded as an advance")
	}
	send(0)
	if got := pace.lastAdvanced(); !got.Equal(advanced) {
		t.Errorf("a push that raised nothing moved the last advance by %v", got.Sub(advanced))
	}
	if !pace.last.After(advanced) {
		t.Error("the last frame answered is not later than the last advance; the test shows nothing")
	}
}
