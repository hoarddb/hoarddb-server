package wire

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Seeding slice S4, step A1: the node's one seed byte rate, divided over the pulls in flight
// (the entry ring seeding completion spec §4).

const s4MiB = 1 << 20

// s4FixedClock is a clock that only advance moves; its Sleep records the wait and does not
// move time, so two pulls that wait "at the same moment" each show their own wait.
type s4FixedClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func (c *s4FixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *s4FixedClock) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	return nil
}

func (c *s4FixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// takeSleeps returns the waits recorded since the last call.
func (c *s4FixedClock) takeSleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	taken := c.sleeps
	c.sleeps = nil
	return taken
}

func s4PacerOn(rate int64, now func() time.Time, sleep func(context.Context, time.Duration) error) *seedPacer {
	return newSeedPacer(seedPacerConfig{BytesPerSecond: rate, Now: now, Sleep: sleep})
}

func TestNodeWideByteRateIsDividedBetweenThePulls(t *testing.T) {
	clock := &s4FixedClock{now: time.Unix(1_700_000_000, 0)}
	pacer := s4PacerOn(2*s4MiB, clock.Now, clock.Sleep)
	ctx := context.Background()
	pacer.Begin(1, 10)
	pacer.Begin(2, 10)

	pacer.Spent(1, 10, s4MiB)
	pacer.Spent(2, 10, s4MiB)
	for _, slot := range []uint16{1, 2} {
		if err := pacer.Wait(ctx, slot, 10, 0); err != nil {
			t.Fatalf("slot %d: Wait: %v", slot, err)
		}
	}
	got := clock.takeSleeps()
	if len(got) != 2 || got[0] != time.Second || got[1] != time.Second {
		t.Fatalf("two pulls sharing 2 MiB/s, 1 MiB each: waits = %v, want [1s 1s] (a share is 1 MiB/s)", got)
	}

	// The charges are paid; one pull ends and the other has the whole rate.
	clock.advance(time.Second)
	pacer.End(2, 10)
	pacer.Spent(1, 10, s4MiB)
	if err := pacer.Wait(ctx, 1, 10, 0); err != nil {
		t.Fatalf("Wait after End: %v", err)
	}
	got = clock.takeSleeps()
	if len(got) != 1 || got[0] != 500*time.Millisecond {
		t.Fatalf("one pull left with 2 MiB/s, 1 MiB: waits = %v, want [500ms]", got)
	}
}

func TestZeroIsUnlimited(t *testing.T) {
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	pacer := s4PacerOn(0, clock.Now, clock.Sleep)
	pacer.Begin(1, 10)
	pacer.Spent(1, 10, 1<<30)
	if err := pacer.Wait(context.Background(), 1, 10, 0); err != nil {
		t.Fatalf("Wait with no rate: %v", err)
	}
	if slept := clock.total(); slept != 0 {
		t.Fatalf("rate 0 is unlimited, but Wait slept %v", slept)
	}
}

func TestAWaitIsCappedByItsLimit(t *testing.T) {
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	pacer := s4PacerOn(s4MiB, clock.Now, clock.Sleep)
	ctx := context.Background()
	pacer.Begin(1, 10)
	pacer.Spent(1, 10, 100*s4MiB)

	if err := pacer.Wait(ctx, 1, 10, time.Minute); err != nil {
		t.Fatalf("Wait with a limit: %v", err)
	}
	if slept := clock.total(); slept != time.Minute {
		t.Fatalf("100 MiB at 1 MiB/s with a 60s limit: slept %v, want 1m0s", slept)
	}
	// The cap shortens one wait, it does not forgive the debt: 40 s of the 100 s remain.
	if err := pacer.Wait(ctx, 1, 10, 0); err != nil {
		t.Fatalf("Wait with no limit: %v", err)
	}
	if slept := clock.total(); slept != 100*time.Second {
		t.Fatalf("after the uncapped second wait the pull slept %v in all, want 1m40s", slept)
	}
}

func TestAChargeIsPaidByTheTimeThatPassed(t *testing.T) {
	ctx := context.Background()

	// Time that passes pays the charge: nothing is left to wait for.
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	pacer := s4PacerOn(s4MiB, clock.Now, clock.Sleep)
	pacer.Begin(1, 10)
	pacer.Spent(1, 10, s4MiB)
	if err := clock.Sleep(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := pacer.Wait(ctx, 1, 10, 0); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if slept := clock.total(); slept != 2*time.Second {
		t.Fatalf("a 1 s charge, 2 s gone by: Wait slept %v more", slept-2*time.Second)
	}

	// Charges made back to back add up, and idle time is not banked as credit.
	clock = &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	pacer = s4PacerOn(s4MiB, clock.Now, clock.Sleep)
	pacer.Begin(1, 10)
	pacer.Spent(1, 10, s4MiB)
	pacer.Spent(1, 10, s4MiB)
	if err := pacer.Wait(ctx, 1, 10, 0); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if slept := clock.total(); slept != 2*time.Second {
		t.Fatalf("two 1 MiB charges at 1 MiB/s: slept %v, want 2s", slept)
	}
	if err := clock.Sleep(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	before := clock.total()
	pacer.Spent(1, 10, s4MiB)
	if err := pacer.Wait(ctx, 1, 10, 0); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if slept := clock.total() - before; slept != time.Second {
		t.Fatalf("a fresh 1 MiB charge after 2 idle seconds: slept %v, want 1s", slept)
	}
}

func TestAPairThatNeverBeganWaitsForNothing(t *testing.T) {
	clock := &s1VirtualClock{now: time.Unix(1_700_000_000, 0)}
	pacer := s4PacerOn(s4MiB, clock.Now, clock.Sleep)
	pacer.Spent(3, 30, 100*s4MiB)
	if err := pacer.Wait(context.Background(), 3, 30, 0); err != nil {
		t.Fatalf("Wait for a pair that never began: %v", err)
	}
	// A pair that has ended is forgotten as well.
	pacer.Begin(4, 40)
	pacer.End(4, 40)
	pacer.Spent(4, 40, 100*s4MiB)
	if err := pacer.Wait(context.Background(), 4, 40, 0); err != nil {
		t.Fatalf("Wait for a pair that ended: %v", err)
	}
	if slept := clock.total(); slept != 0 {
		t.Fatalf("pairs not in flight waited %v", slept)
	}
}
