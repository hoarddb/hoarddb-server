package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// fakeService is a ringService whose Start blocks, like a boot wait, until AbortBootWait.
type fakeService struct {
	t          *testing.T
	registered *bool
	startErr   error
	aborted    chan struct{}
	abortOnce  sync.Once
	waitedFor  chan os.Signal
}

func newFakeService(t *testing.T, registered *bool) *fakeService {
	return &fakeService{t: t, registered: registered, aborted: make(chan struct{}), waitedFor: make(chan os.Signal, 1)}
}

func (f *fakeService) Start() error {
	if !*f.registered {
		f.t.Error("Start ran before the signals were registered: a SIGTERM during its boot wait kills the process with no Stop")
	}
	if f.startErr != nil {
		return f.startErr
	}
	select {
	case <-f.aborted:
	case <-time.After(5 * time.Second):
		f.t.Error("the boot wait was not aborted by the signal")
	}
	return nil
}

func (f *fakeService) AbortBootWait() { f.abortOnce.Do(func() { close(f.aborted) }) }

func (f *fakeService) WaitForExit(signals <-chan os.Signal) int {
	select {
	case sig := <-signals:
		f.waitedFor <- sig
		return 0
	case <-time.After(5 * time.Second):
		f.t.Error("the signal that aborted the boot wait never reached WaitForExit")
		return 1
	}
}

// TestRunServiceRegistersSignalsBeforeStartAndASignalAbortsTheBootWait (review M2 and n3): the
// signal channel is registered before Start runs, and the first signal received during Start's
// boot wait aborts that wait, so Start returns at once and the signal reaches WaitForExit (which
// stops the server) instead of sitting buffered for up to 30 s. Break one: register after Start
// (the fake's Start reports it); break two: do not abort the boot wait (Start times out).
func TestRunServiceRegistersSignalsBeforeStartAndASignalAbortsTheBootWait(t *testing.T) {
	registered := false
	srv := newFakeService(t, &registered)
	sent := make(chan struct{})
	register := func(ch chan<- os.Signal) {
		registered = true
		go func() {
			time.Sleep(50 * time.Millisecond) // inside the fake's blocking Start
			ch <- syscall.SIGTERM
			close(sent)
		}()
	}
	began := time.Now()
	if code := runService(srv, register, slog.New(slog.NewTextHandler(io.Discard, nil))); code != 0 {
		t.Fatalf("exit status %d, want 0", code)
	}
	<-sent
	if took := time.Since(began); took > 2*time.Second {
		t.Fatalf("a signal during the boot wait took %s to end the start", took)
	}
	if sig := <-srv.waitedFor; sig != syscall.SIGTERM {
		t.Fatalf("WaitForExit got %v", sig)
	}
}

// TestRunServiceMapsAFailedStartToItsExitStatus: a start refused for a foreign snapshot exits 70,
// any other failure 1, and WaitForExit is not reached.
func TestRunServiceMapsAFailedStartToItsExitStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{
		{&cluster.ForeignSnapshotError{Text: "a newer build wrote it"}, cluster.DivergedExitCode},
		{errors.New("address in use"), 1},
	} {
		registered := false
		srv := newFakeService(t, &registered)
		srv.startErr = tc.err
		got := runService(srv, func(chan<- os.Signal) { registered = true }, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if got != tc.want {
			t.Errorf("%v: exit status %d, want %d", tc.err, got, tc.want)
		}
	}
}
