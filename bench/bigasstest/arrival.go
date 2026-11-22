package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// arrivalSchedule is the open-loop clock: Poisson arrivals at a fixed offered
// rate, shared by every worker of one process. A worker claims the next
// scheduled time, waits for it, runs the operation and charges the operation
// from that scheduled time, not from when it got round to it. A server that
// stalls therefore shows the delay every queued request would have felt, where
// a closed loop (which simply stops asking while it waits) hides it.
type arrivalSchedule struct {
	mutex     sync.Mutex
	rng       *rand.Rand
	meanGap   time.Duration
	scheduled time.Time
}

// newArrivalSchedule starts a schedule at start with rate arrivals per second.
// rate must be positive: validate rejects anything else before a run starts.
func newArrivalSchedule(start time.Time, rate float64, seed uint64) *arrivalSchedule {
	return &arrivalSchedule{
		rng:       rand.New(rand.NewPCG(seed, 0x0fe9109)),
		meanGap:   time.Duration(float64(time.Second) / rate),
		scheduled: start,
	}
}

// next claims the next scheduled arrival. Gaps are exponential, which makes the
// arrival count in any window Poisson.
func (s *arrivalSchedule) next() time.Time {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.scheduled = s.scheduled.Add(time.Duration(s.rng.ExpFloat64() * float64(s.meanGap)))
	return s.scheduled
}

// waitUntil sleeps until moment or until ctx is cancelled, in which case it
// returns the context's error. A moment already past returns at once: that
// worker is running late and the lateness is exactly what gets charged.
func waitUntil(ctx context.Context, moment time.Time) error {
	remaining := time.Until(moment)
	if remaining <= 0 {
		return nil
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for the next scheduled arrival: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
