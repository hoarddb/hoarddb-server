package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// poolHarness is a caller node and an owner node on plain TCP (no TLS), so a test can wrap
// the caller's connections and watch the owner's handler.
type poolHarness struct {
	caller, owner *Network
	ownerAddr     string
	handler       func(ctx context.Context, body string) error
	// ownerIdle is the owner's idle limit for a connection; zero keeps the default.
	ownerIdle time.Duration
	// callerMaxIdle is the caller's ConnMaxIdle.
	callerMaxIdle time.Duration

	dials   atomic.Int32
	mu      sync.Mutex
	conns   []net.Conn
	running atomic.Int32
	peak    atomic.Int32
	seen    atomic.Int32
}

func newPoolHarness(t *testing.T, handler func(ctx context.Context, body string) error) *poolHarness {
	t.Helper()
	return newIdlePoolHarness(t, 0, 0, handler)
}

func newIdlePoolHarness(t *testing.T, ownerIdle, callerMaxIdle time.Duration, handler func(ctx context.Context, body string) error) *poolHarness {
	t.Helper()
	h := &poolHarness{handler: handler, ownerIdle: ownerIdle, callerMaxIdle: callerMaxIdle}
	h.owner, h.ownerAddr = h.startOwner(t, "127.0.0.1:0")
	h.caller = h.startCaller(t)
	return h
}

func (h *poolHarness) startOwner(t *testing.T, listen string) (*Network, string) {
	t.Helper()
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := New(Config{
		Self: "owner", Listener: ln, PeerLimits: transport.ChannelLimits{Idle: h.ownerIdle}, Addr: func(string) (string, bool) { return "", false },
		Hello: func() transport.Handshake { return transport.Handshake{NodeID: "owner", Incarnation: 1} },
		Data: func(ctx context.Context, _ transport.Handshake, req transport.Message) (*transport.Message, error) {
			now := h.running.Add(1)
			defer h.running.Add(-1)
			h.seen.Add(1)
			for {
				peak := h.peak.Load()
				if now <= peak || h.peak.CompareAndSwap(peak, now) {
					break
				}
			}
			if err := h.handler(ctx, string(req.Body)); err != nil {
				return nil, err
			}
			return &transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: req.Env.Kind}, Body: req.Body}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	return owner, owner.Addr().String()
}

func (h *poolHarness) startCaller(t *testing.T) *Network {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	caller, err := New(Config{
		Self: "caller", Listener: ln,
		Addr:  func(string) (string, bool) { return h.ownerAddr, true },
		Hello: func() transport.Handshake { return transport.Handshake{NodeID: "caller", Incarnation: 1} },
		Dial: func(ctx context.Context, _ string, _ transport.Channel, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err == nil {
				h.dials.Add(1)
				h.mu.Lock()
				h.conns = append(h.conns, conn)
				h.mu.Unlock()
			}
			return conn, err
		},
		BackoffBase: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, ConnMaxIdle: h.callerMaxIdle,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	return caller
}

// waitRunning waits until the owner is handling at least n calls at once.
func (h *poolHarness) waitRunning(t *testing.T, n int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for h.running.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("the owner never handled %d calls at once (it handles %d): the lane holds the rest back", n, h.running.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *poolHarness) forward(ctx context.Context, body string) (*transport.Message, error) {
	return h.caller.Call(ctx, "owner", transport.ChannelData, LaneForward, peerWrite(0x03, body))
}

// TestForwardedCallsToOnePeerRunConcurrently is BUG-0769: four callers, each owner round
// trip taking the same delay, finish in about one delay and not four, and the owner sees
// them at once.
func TestForwardedCallsToOnePeerRunConcurrently(t *testing.T) {
	const delay = 300 * time.Millisecond
	const callers = 4 // the least a lane must carry at once to be worth a pool
	if ForwardConns < callers {
		t.Errorf("ForwardConns is %d: a peer's forward lane must carry at least %d calls at once", ForwardConns, callers)
	}
	h := newPoolHarness(t, func(ctx context.Context, _ string) error {
		select {
		case <-time.After(delay):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.forward(peerCtx(t), fmt.Sprintf("w%d", i))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a forwarded call failed: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > delay*callers/2 {
		t.Fatalf("%d forwarded calls to one peer took %s with an owner round trip of %s: the lane ran them one at a time", callers, elapsed, delay)
	}
	if got := h.peak.Load(); got < callers {
		t.Fatalf("the owner saw at most %d forwarded calls at once, want %d", got, callers)
	}
}

// TestOneSlowForwardedCallDoesNotBlockAnother: a call that waits on the owner holds one
// connection of the pool; a second call is answered meanwhile.
func TestOneSlowForwardedCallDoesNotBlockAnother(t *testing.T) {
	release := make(chan struct{})
	h := newPoolHarness(t, func(ctx context.Context, body string) error {
		if body == "slow" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	slowDone := make(chan error, 1)
	go func() { _, err := h.forward(peerCtx(t), "slow"); slowDone <- err }()
	h.waitRunning(t, 1)
	fastCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.forward(fastCtx, "fast"); err != nil {
		t.Fatalf("a quick forwarded call queued behind a slow one: %v", err)
	}
	close(release)
	if err := <-slowDone; err != nil {
		t.Fatalf("the slow call: %v", err)
	}
}

// TestTheForwardPoolIsBoundedAndTheMainLaneStaysSerial: many more callers than ForwardConns
// never put more than ForwardConns calls on the owner, nor open more than ForwardConns
// connections, and the replication lane still carries one call at a time.
func TestTheForwardPoolIsBoundedAndTheMainLaneStaysSerial(t *testing.T) {
	h := newPoolHarness(t, func(context.Context, string) error { time.Sleep(20 * time.Millisecond); return nil })
	var wg sync.WaitGroup
	for i := 0; i < 4*ForwardConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.forward(peerCtx(t), "x"); err != nil {
				t.Errorf("forward: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := h.peak.Load(); got > int32(ForwardConns) {
		t.Fatalf("the owner saw %d forwarded calls at once, the bound is ForwardConns = %d", got, ForwardConns)
	}
	if got := h.dials.Load(); got > int32(ForwardConns) {
		t.Fatalf("%d connections opened for the forward lane, the bound is %d", got, ForwardConns)
	}

	h.peak.Store(0)
	h.dials.Store(0)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.caller.Call(peerCtx(t), "owner", transport.ChannelData, LaneMain, peerWrite(0x03, "m")); err != nil {
				t.Errorf("main lane: %v", err)
			}
		}()
	}
	wg.Wait()
	if got, dials := h.peak.Load(), h.dials.Load(); got != 1 || dials != 1 {
		t.Fatalf("the main lane ran %d calls at once over %d connections, want 1 and 1", got, dials)
	}
}

// TestSequentialForwardedCallsKeepTheirOrderOnOneConnection: a caller that waits for each
// reply sees its calls reach the owner in order, on the one connection it always had.
func TestSequentialForwardedCallsKeepTheirOrderOnOneConnection(t *testing.T) {
	var mu sync.Mutex
	var order []string
	h := newPoolHarness(t, func(_ context.Context, body string) error {
		mu.Lock()
		order = append(order, body)
		mu.Unlock()
		return nil
	})
	var want []string
	for i := 0; i < 50; i++ {
		body := fmt.Sprintf("op-%02d", i)
		want = append(want, body)
		if _, err := h.forward(peerCtx(t), body); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("call %d reached the owner as %q, want %q", i, order[i], want[i])
		}
	}
	if got := h.dials.Load(); got != 1 || h.peak.Load() != 1 {
		t.Fatalf("one sequential caller used %d connections and %d concurrent calls, want 1 and 1", got, h.peak.Load())
	}
}

// TestABrokenForwardConnectionFailsItsCallAndIsNeverRetried: the connection under an
// in-flight write dies; that write reports an error and reaches the owner exactly once (a
// retry of a write whose reply was lost could apply it twice), while a call on another
// connection is unaffected and the next call redials.
func TestABrokenForwardConnectionFailsItsCallAndIsNeverRetried(t *testing.T) {
	release := make(chan struct{})
	var inbound sync.Map
	h := newPoolHarness(t, func(ctx context.Context, body string) error {
		n, _ := inbound.LoadOrStore(body, new(atomic.Int32))
		n.(*atomic.Int32).Add(1)
		if body == "doomed" || body == "bystander" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	doomed := make(chan error, 1)
	go func() { _, err := h.forward(peerCtx(t), "doomed"); doomed <- err }()
	h.waitRunning(t, 1)
	bystander := make(chan error, 1)
	go func() { _, err := h.forward(peerCtx(t), "bystander"); bystander <- err }()
	h.waitRunning(t, 2)
	// Break the first connection opened: the one "doomed" is waiting on.
	h.mu.Lock()
	first := h.conns[0]
	h.mu.Unlock()
	_ = first.Close()
	if err := <-doomed; err == nil {
		t.Fatal("a write whose connection broke before its reply reported success")
	}
	close(release)
	if err := <-bystander; err != nil {
		t.Fatalf("a call on another connection failed when one connection broke: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // a retry, if there were one, would have landed by now
	n, _ := inbound.Load("doomed")
	if got := n.(*atomic.Int32).Load(); got != 1 {
		t.Fatalf("the broken write reached the owner %d times, want exactly 1", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := h.forward(peerCtx(t), "after")
		if err == nil {
			break
		}
		if !errors.Is(err, ErrBackoff) || time.Now().After(deadline) {
			t.Fatalf("the next forwarded call after the break: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func settledGoroutines(t *testing.T, base int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("%d goroutines remain, started with %d:\n%s", runtime.NumGoroutine(), base, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestClosingTheNetworkEndsEveryPooledForwardCall: Close and Forget end calls in flight on
// every connection of the pool and leave no goroutine behind.
func TestClosingTheNetworkEndsEveryPooledForwardCall(t *testing.T) {
	base := runtime.NumGoroutine()
	for _, how := range []string{"close", "forget"} {
		t.Run(how, func(t *testing.T) {
			h := newPoolHarness(t, func(ctx context.Context, _ string) error { <-ctx.Done(); return ctx.Err() })
			var wg sync.WaitGroup
			for i := 0; i < ForwardConns+2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := h.forward(peerCtx(t), "hang"); err == nil {
						t.Error("a call answered after its connection was closed")
					}
				}()
			}
			h.waitRunning(t, int32(min(ForwardConns, 2)))
			if how == "close" {
				_ = h.caller.Close()
			} else {
				_ = h.caller.Forget("owner")
			}
			wg.Wait()
			_ = h.caller.Close()
			_ = h.owner.Close()
		})
	}
	settledGoroutines(t, base)
}

// TestAPeerRestartLeavesNoStaleForwardConnection: the owner stops while calls are in flight
// and comes back on the same address; calls resume and no goroutine leaks.
func TestAPeerRestartLeavesNoStaleForwardConnection(t *testing.T) {
	base := runtime.NumGoroutine()
	h := newPoolHarness(t, func(ctx context.Context, _ string) error { time.Sleep(10 * time.Millisecond); return nil })
	for i := 0; i < ForwardConns; i++ {
		if _, err := h.forward(peerCtx(t), "warm"); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < ForwardConns; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = h.forward(peerCtx(t), "x") }()
	}
	_ = h.owner.Close()
	wg.Wait()
	owner, _ := h.startOwner(t, h.ownerAddr)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := h.forward(peerCtx(t), "back"); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("no forwarded call succeeded after the owner came back: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The pool must come back to its full width, not stay at the one connection that redialled.
	h.peak.Store(0)
	h.handler = func(context.Context, string) error { time.Sleep(100 * time.Millisecond); return nil }
	for i := 0; i < ForwardConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.forward(peerCtx(t), "burst"); err != nil {
				t.Errorf("a burst call after the restart: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := h.peak.Load(); got != int32(ForwardConns) {
		t.Errorf("after the restart a burst of %d reached the owner %d at once, want the pool back to %d", ForwardConns, got, ForwardConns)
	}
	_ = h.caller.Close()
	_ = owner.Close()
	settledGoroutines(t, base)
}

// TestAPoolOlderThanTheOwnersIdleLimitIsRedialledNotWrittenInto: the owner closes a
// connection idle for its limit; after a quiet spell longer than that, a burst must find
// every pooled connection redialled, not dead (which reported "unknown" for writes that
// were never sent and started the lane's backoff).
func TestAPoolOlderThanTheOwnersIdleLimitIsRedialledNotWrittenInto(t *testing.T) {
	h := newIdlePoolHarness(t, 300*time.Millisecond, 100*time.Millisecond,
		func(context.Context, string) error { time.Sleep(50 * time.Millisecond); return nil })
	burst := func(when string) {
		var wg sync.WaitGroup
		for i := 0; i < ForwardConns; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := h.forward(peerCtx(t), "b"); err != nil {
					t.Errorf("%s: a forwarded call failed: %v", when, err)
				}
			}()
		}
		wg.Wait()
	}
	burst("warm-up")
	if got := h.peak.Load(); got != int32(ForwardConns) {
		t.Fatalf("the warm-up reached the owner %d at once, want %d (the pool was not full)", got, ForwardConns)
	}
	time.Sleep(700 * time.Millisecond)
	burst("after the quiet spell")
	for i := 0; i < 10; i++ {
		if _, err := h.forward(peerCtx(t), "seq"); err != nil {
			t.Fatalf("sequential call %d after the quiet spell: %v", i, err)
		}
	}
}
