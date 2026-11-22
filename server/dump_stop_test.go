package server

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// A dump walk reads the node store from a goroutine that neither
// Stop path waited for, so Stop could close the store under it (Badger's
// Txn.NewIterator panics "DB Closed"), and an OpDumpDone landing before the
// walk was launched left a walk nobody waited for at all. Every test here
// runs on a real store behind a real Service.

// stopPaths are the two functions that close the node store. Both must wait
// for a dump walk.
var stopPaths = []struct {
	name string
	stop func(cs *catalogServer) error
}{
	{"Service.Stop", func(cs *catalogServer) error { return cs.qs.Stop() }},
	{"Server.Stop", func(cs *catalogServer) error { return cs.core.Stop() }},
}

// startDumpStopServer builds a server with one hash bucket holding docCount
// documents, and marks it stopped for its cleanup: each test calls Stop
// itself, and a second Stop after a hung first one would only obscure the
// failure.
func startDumpStopServer(t *testing.T, docCount int) *catalogServer {
	t.Helper()
	cs := startCatalogServerAt(t, t.TempDir())
	createTestBucket(t, cs.core, "app", "docs", BucketTypeHash)
	for i := range docCount {
		if _, err := cs.core.Insert(InsertRequest{Database: "app", Bucket: "docs", Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}}); err != nil {
			t.Fatalf("insert k%d: %v", i, err)
		}
	}
	return cs
}

// holdDumpWalks makes every dump walk block in dumpWalkTestHook until the
// returned release is called. walkHeld is closed when the first walk arrives.
// A test whose assertion fails must not call release: the walk would then
// read a store Stop has already closed, and the panic would take the whole
// test binary down with it.
func holdDumpWalks(t *testing.T) (walkHeld <-chan struct{}, release func()) {
	t.Helper()
	held := make(chan struct{})
	releaseWalk := make(chan struct{})
	hook := func() {
		close(held)
		<-releaseWalk
	}
	dumpWalkTestHook.Store(&hook)
	t.Cleanup(func() { dumpWalkTestHook.Store(nil) })
	return held, func() { close(releaseWalk) }
}

// parkLaunchedWalk waits for a walk a failing test found launched to reach
// the hook, so the hook is still set when the walk reads it. Failing before
// that would let the test's cleanup clear the hook first, and the walk would
// go on to read a closed store and panic the binary.
func parkLaunchedWalk(t *testing.T, walkHeld <-chan struct{}) {
	t.Helper()
	select {
	case <-walkHeld:
	case <-time.After(10 * time.Second):
		t.Error("a launched dump walk never reached the test hook")
	}
}

func currentDumpSession(s *Server) *dumpSession {
	s.dumpMu.Lock()
	defer s.dumpMu.Unlock()
	return s.dump
}

// walkRunning reports whether session has a walk that was launched and has
// not returned.
func walkRunning(session *dumpSession) bool {
	session.setupMu.Lock()
	walkDone := session.walkDone
	session.setupMu.Unlock()
	if walkDone == nil {
		return false
	}
	select {
	case <-walkDone:
		return false
	default:
		return true
	}
}

// TestStopWaitsForDumpWalk: a walk that exists but has not yet opened its
// iterator must hold Stop off the store. Before the fix Stop returned at once,
// and releasing that walk would have panicked "DB Closed".
func TestStopWaitsForDumpWalk(t *testing.T) {
	for _, stopPath := range stopPaths {
		t.Run(stopPath.name, func(t *testing.T) {
			cs := startDumpStopServer(t, 3)
			walkHeld, releaseWalk := holdDumpWalks(t)

			if _, err := cs.core.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "docs"}); err != nil {
				t.Fatalf("dump start: %v", err)
			}
			select {
			case <-walkHeld:
			case <-time.After(10 * time.Second):
				t.Fatal("the dump walk never started")
			}
			session := currentDumpSession(cs.core)

			cs.stopped = true
			stopReturned := make(chan error, 1)
			go func() { stopReturned <- stopPath.stop(cs) }()

			select {
			case <-stopReturned:
				// The walk is deliberately left held: released, it would open
				// an iterator on the closed store and panic.
				t.Fatalf("%s returned while a dump walk still held the node store; it closed the store under the walk", stopPath.name)
			case <-time.After(500 * time.Millisecond):
			}

			releaseWalk()
			select {
			case err := <-stopReturned:
				if err != nil {
					t.Fatalf("%s: %v", stopPath.name, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s did not return within 10s of the dump walk being released", stopPath.name)
			}
			if walkRunning(session) {
				t.Fatalf("%s returned with the dump walk still running", stopPath.name)
			}
		})
	}
}

// TestStopDoesNotDeadlockOnUndrainedDumpWalk: a walk blocked sending to
// session.docs, with no OpDumpChunk left to drain it, must be cancelled by
// Stop before Stop waits for it. Waiting first hangs Stop forever, which is
// worse than the panic the wait exists to prevent. Before the fix Stop
// returned and the walk stayed blocked, iterator open, on a closed store.
func TestStopDoesNotDeadlockOnUndrainedDumpWalk(t *testing.T) {
	for _, stopPath := range stopPaths {
		t.Run(stopPath.name, func(t *testing.T) {
			cs := startDumpStopServer(t, 5)
			if _, err := cs.core.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "docs"}); err != nil {
				t.Fatalf("dump start: %v", err)
			}
			session := currentDumpSession(cs.core)

			// Five documents, a two-slot buffer and no reader: once the
			// buffer is full the walk is blocked on the third send, with its
			// iterator open.
			deadline := time.Now().Add(10 * time.Second)
			for len(session.docs) < cap(session.docs) {
				if time.Now().After(deadline) {
					t.Fatalf("the dump walk never filled its %d-document buffer", cap(session.docs))
				}
				time.Sleep(time.Millisecond)
			}

			cs.stopped = true
			stopReturned := make(chan error, 1)
			go func() { stopReturned <- stopPath.stop(cs) }()
			select {
			case err := <-stopReturned:
				if err != nil {
					t.Fatalf("%s: %v", stopPath.name, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s hung: it is waiting for a dump walk blocked on a document nobody will read", stopPath.name)
			}
			if walkRunning(session) {
				t.Fatalf("%s returned with the dump walk still running against the closed store", stopPath.name)
			}
			// The walk stopped short, so the dump must not end as a clean
			// EOF: a client would record a truncated bucket as complete.
			for range session.docs {
			}
			if walkErr := <-session.walkErr; !errors.Is(walkErr, ErrDumpNodeStopping) {
				t.Fatalf("walk error after %s = %v, want %v", stopPath.name, walkErr, ErrDumpNodeStopping)
			}
		})
	}
}

// TestEarlyDumpDoneLeavesNoWalk: an OpDumpDone that lands after the dump is
// claimed but before its walk is launched ends the session. startDump must
// then launch no walk: nothing would ever wait for it, and its digest writes
// would race the digest dumpDone already read. Before the fix the walk was
// launched anyway.
func TestEarlyDumpDoneLeavesNoWalk(t *testing.T) {
	cs := startDumpStopServer(t, 1)
	s := cs.core
	walkHeld, releaseWalk := holdDumpWalks(t)

	claimed := make(chan *dumpSession, 1)
	releaseStart := make(chan struct{})
	startDumpTestBarrier = func() {
		claimed <- currentDumpSession(s)
		<-releaseStart
	}
	t.Cleanup(func() { startDumpTestBarrier = nil })

	startErr := make(chan error, 1)
	go func() {
		_, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "docs"})
		startErr <- err
	}()
	session := <-claimed

	// dumpDone runs to completion inside the window: startDump holds nothing
	// dumpDone needs, so there is no scheduling luck in this interleaving.
	if _, err := s.dumpDone(rootContext(), DumpDoneRequest{Database: "app", Bucket: "docs"}); err != nil {
		t.Fatalf("dump done inside the start window: %v", err)
	}
	close(releaseStart)
	if err := <-startErr; err != nil {
		t.Fatalf("dump start after an early dump done: %v", err)
	}

	// A launched walk cannot have finished: the hook holds it. So a running
	// walk is exactly a launched one.
	if walkRunning(session) {
		// Left held, not released: nothing waits for this walk, so releasing
		// it would only race the server's teardown. The server is leaked
		// rather than stopped, since a Stop that waits for walks would wait
		// for this one forever.
		cs.stopped = true
		parkLaunchedWalk(t, walkHeld)
		t.Fatal("startDump launched a walk for a session OpDumpDone had already ended; nothing waits for that walk")
	}
	releaseWalk()
}

// TestStopRefusesDumpWalkLaunchedAfterIt: a dump that claimed its slot before
// Stop began, and launches its walk after Stop has waited, must not launch
// it: the store is closed or about to be. Before the fix the walk launched
// against the closed store.
func TestStopRefusesDumpWalkLaunchedAfterIt(t *testing.T) {
	for _, stopPath := range stopPaths {
		t.Run(stopPath.name, func(t *testing.T) {
			cs := startDumpStopServer(t, 1)
			s := cs.core
			walkHeld, _ := holdDumpWalks(t)

			claimed := make(chan *dumpSession, 1)
			releaseStart := make(chan struct{})
			startDumpTestBarrier = func() {
				claimed <- currentDumpSession(s)
				<-releaseStart
			}
			t.Cleanup(func() { startDumpTestBarrier = nil })

			startErr := make(chan error, 1)
			go func() {
				_, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: "docs"})
				startErr <- err
			}()
			session := <-claimed

			cs.stopped = true
			stopReturned := make(chan error, 1)
			go func() { stopReturned <- stopPath.stop(cs) }()
			select {
			case err := <-stopReturned:
				if err != nil {
					t.Fatalf("%s: %v", stopPath.name, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s hung with a dump claimed but no walk launched", stopPath.name)
			}

			close(releaseStart)
			err := <-startErr
			// The hook holds a launched walk, so a running walk is exactly a
			// launched one. It is never released: it would read the closed
			// store.
			if walkRunning(session) {
				parkLaunchedWalk(t, walkHeld)
				t.Fatalf("startDump launched a dump walk after %s had returned", stopPath.name)
			}
			if err == nil {
				t.Fatalf("dump start reported success after %s, for a dump whose walk it cannot run", stopPath.name)
			}
		})
	}
}
