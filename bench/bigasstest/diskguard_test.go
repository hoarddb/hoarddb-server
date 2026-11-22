package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiskGuardAborts(t *testing.T) {
	var free atomic.Int64
	free.Store(50 << 30)
	guard := diskGuard{free: func(context.Context) (int64, error) { return free.Load(), nil }, minFree: 3 << 30, interval: 2 * time.Millisecond}
	aborted := make(chan int64, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		guard.watch(ctx, func(freeBytes int64, _ error) { aborted <- freeBytes })
		close(done)
	}()
	select {
	case <-aborted:
		t.Fatal("aborted with plenty of space")
	case <-time.After(20 * time.Millisecond):
	}
	free.Store(1 << 30)
	select {
	case got := <-aborted:
		if got != 1<<30 {
			t.Errorf("abort reported %d free", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the guard did not abort below the minimum")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the guard goroutine must exit after it aborts")
	}
}

func TestDiskGuardAbortsWhenItCannotSeeTheDisk(t *testing.T) {
	boom := errors.New("ssh: no route")
	guard := diskGuard{free: func(context.Context) (int64, error) { return 0, boom }, minFree: 1, interval: time.Millisecond}
	got := make(chan error, 1)
	guard.watch(context.Background(), func(_ int64, err error) { got <- err })
	if err := <-got; !errors.Is(err, boom) {
		t.Errorf("got %v", err)
	}
}

func TestDiskGuardStopsWithTheContext(t *testing.T) {
	guard := diskGuard{free: func(context.Context) (int64, error) { return 1 << 40, nil }, minFree: 1, interval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	guard.watch(ctx, func(int64, error) { t.Error("must not abort") })
}

func TestFreeSources(t *testing.T) {
	got, err := statfsFree(t.TempDir())(context.Background())
	if err != nil || got <= 0 {
		t.Errorf("statfs free = %d, %v", got, err)
	}
	got, err = cmdFree("echo 12345")(context.Background())
	if err != nil || got != 12345 {
		t.Errorf("-free-cmd free = %d, %v", got, err)
	}
	if _, err := cmdFree("echo not-a-number")(context.Background()); err == nil {
		t.Error("a -free-cmd that prints no integer must be an error")
	}
}
