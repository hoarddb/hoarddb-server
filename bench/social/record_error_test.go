package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// mustRecord records a latency the test knows is in range.
func mustRecord(t *testing.T, hist *histogram, latency time.Duration) {
	t.Helper()
	if err := hist.record(latency); err != nil {
		t.Fatalf("record(%v): %v", latency, err)
	}
}

func TestRecordRejectsLatencyAboveRange(t *testing.T) {
	hist := newHistogram()
	err := hist.record(61 * time.Second)
	if err == nil {
		t.Fatal("record(61s) succeeded: a latency above the 60 s range must be an error, not a clamped sample")
	}
	if hist.count() != 0 {
		t.Errorf("a rejected latency left %d samples in the histogram", hist.count())
	}
	if err := hist.record(0); err != nil {
		t.Errorf("record(0) failed: %v; a sub-microsecond latency is stored as 1 us", err)
	}
}

// slowClockSession reports an operation that took 61 s by moving the clock's
// start into the past, the way an engine adjusts it, without waiting 61 s.
type slowClockSession struct{ stallSession }

func (s *slowClockSession) readTimeline(_ context.Context, request readRequest) (timelinePage, error) {
	request.Clock.start = time.Now().Add(-61 * time.Second)
	return timelinePage{}, nil
}

type slowClockEngine struct{ stallEngine }

func (e *slowClockEngine) newSession() (engineSession, error) { return &slowClockSession{}, nil }

func TestLatencyRecordErrorReachesWorkerFailure(t *testing.T) {
	cfg := &config{Users: 10, ZipfS: 1.1, Conc: 1, ReadPct: 100, ReadPosts: 20, RandSeed: 1, ClientVMs: 1}
	begin := time.Now()
	results := runWorkers(context.Background(), cfg, &slowClockEngine{}, make([]userState, cfg.Users), begin, begin.Add(time.Second))
	if results[0].recordFailure == nil {
		t.Fatal("a 61 s latency was recorded without error: the histogram failure never reached the worker result")
	}
}

func TestHistogramRangeErrorNamesBothCauses(t *testing.T) {
	cause := errors.New("recording latency 61s: above the 1m0s the histogram holds")
	err := histogramRangeError(cause)
	if !errors.Is(err, cause) {
		t.Errorf("the underlying error is lost: %v", err)
	}
	text := err.Error()
	// A closed loop gets this error from one slow request; an open loop also gets it from
	// queueing delay, which counts toward latency, and lowering -op-timeout does not fix that.
	for _, want := range []string{"-op-timeout", "-rate", "queue"} {
		if !strings.Contains(text, want) {
			t.Errorf("the message does not mention %q, so it is wrong for one of the two causes:\n%s", want, text)
		}
	}
}
