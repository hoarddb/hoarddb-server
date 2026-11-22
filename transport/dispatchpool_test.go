package transport

import (
	"bufio"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDispatchPoolReusesAnIdleWorker is the point of the pool: frames that arrive one
// after another run on one goroutine, so its stack stays grown.
func TestDispatchPoolReusesAnIdleWorker(t *testing.T) {
	var tracked sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	pool := newDispatchPool(ctx, &tracked)
	for i := 0; i < 50; i++ {
		done := make(chan struct{})
		pool.submit(func() { close(done) })
		<-done
		// The worker is back at its receive a moment after the task returns.
		time.Sleep(time.Millisecond)
	}
	if got := pool.spawned.Load(); got > 3 {
		t.Fatalf("50 sequential tasks started %d workers; want them reused (at most 3)", got)
	}
	cancel()
	tracked.Wait()
}

// TestDispatchPoolDoesNotCapConcurrency: a task that blocks must not hold up another, or
// the window's concurrency, and a reply that waits on another frame, would deadlock.
func TestDispatchPoolDoesNotCapConcurrency(t *testing.T) {
	var tracked sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	pool := newDispatchPool(ctx, &tracked)
	const tasks = 40
	var running atomic.Int64
	release := make(chan struct{})
	var finished sync.WaitGroup
	for i := 0; i < tasks; i++ {
		finished.Add(1)
		pool.submit(func() {
			defer finished.Done()
			running.Add(1)
			<-release
		})
	}
	deadline := time.Now().Add(10 * time.Second)
	for running.Load() < tasks {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d blocked tasks are running at once", running.Load(), tasks)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	finished.Wait()
	cancel()
	tracked.Wait()
}

// TestDispatchPoolWorkersExit: an idle worker leaves after its idle life, and a parked one
// leaves when the context ends, so nothing outlives the listener.
func TestDispatchPoolWorkersExit(t *testing.T) {
	var tracked sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	pool := newDispatchPool(ctx, &tracked)
	pool.idle = 20 * time.Millisecond
	done := make(chan struct{})
	pool.submit(func() { close(done) })
	<-done
	waitDrained(t, &tracked, "an idle worker did not exit after its idle life")

	pool.idle = time.Hour
	pool.submit(func() {})
	cancel()
	waitDrained(t, &tracked, "a parked worker did not exit when the context ended")
}

func waitDrained(t *testing.T, group *sync.WaitGroup, message string) {
	t.Helper()
	drained := make(chan struct{})
	go func() { group.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal(message)
	}
}

// TestTCPDispatchOnReusedWorkers drives the real listener: sequential frames on one
// connection are served by reused workers, a slow frame does not hold back a later one
// (replies are matched by request id, not by order), and Close returns with workers parked.
func TestTCPDispatchOnReusedWorkers(t *testing.T) {
	cfg := testTCPConfig(TCPConfig{PipelineFrames: 4})
	slow := make(chan struct{})
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if frame.Opcode == OpGet && string(frame.Payload) == "slow" {
			<-slow
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, cfg, handler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatal(err)
	}
	if resp := mustDecode(t, br); resp.Opcode != OpAuthOK {
		t.Fatalf("auth answered %s", resp.Opcode)
	}

	slowFrame := NewFrame(OpGet, []byte("slow"))
	if _, err := conn.Write(slowFrame.Encode()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		quick := NewFrame(OpGet, []byte("quick"))
		if _, err := conn.Write(quick.Encode()); err != nil {
			t.Fatal(err)
		}
		resp := mustDecode(t, br)
		if resp.RequestID != quick.RequestID {
			t.Fatalf("reply %d is for request %x, want the quick one %x: a slow frame held it back", i, resp.RequestID, quick.RequestID)
		}
	}
	if got := ln.dispatch.spawned.Load(); got > 8 {
		t.Fatalf("101 frames on one connection started %d workers; want reuse (at most 8)", got)
	}
	close(slow)
	if resp := mustDecode(t, br); resp.RequestID != slowFrame.RequestID {
		t.Fatalf("the slow frame's reply is %x, want %x", resp.RequestID, slowFrame.RequestID)
	}

	closed := make(chan struct{})
	go func() { _ = ln.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return with idle workers parked")
	}
}

// TestTCPParkedHandlersOnManyConnectionsStarveNoOne parks one handler on each of many
// connections, more than any fixed worker count a pool might be tempted to use, then
// checks that another connection is still served at once and that Close returns, with the
// parked handlers released by cancellation and their workers gone.
func TestTCPParkedHandlersOnManyConnectionsStarveNoOne(t *testing.T) {
	const parkedConns = 48
	baseline := runtime.NumGoroutine()
	var parked atomic.Int64
	handler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if frame.Opcode == OpGet && string(frame.Payload) == "park" {
			parked.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return authEchoHandler(ctx, frame)
	}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), handler, nil)
	for i := 0; i < parkedConns; i++ {
		conn, _ := authenticatedTestConn(t, ln)
		if _, err := conn.Write(NewFrame(OpGet, []byte("park")).Encode()); err != nil {
			t.Fatalf("parking a frame on connection %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for parked.Load() < parkedConns {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d handlers are parked: the pool is capping concurrency", parked.Load(), parkedConns)
		}
		time.Sleep(time.Millisecond)
	}

	other, otherReader := authenticatedTestConn(t, ln)
	request := NewFrame(OpGet, []byte("quick"))
	if _, err := other.Write(request.Encode()); err != nil {
		t.Fatal(err)
	}
	if reply := mustDecode(t, otherReader); reply.RequestID != request.RequestID {
		t.Fatalf("reply is for %x, want %x", reply.RequestID, request.RequestID)
	}
	stopAndExpectNoLeak(t, ln, baseline)
}
