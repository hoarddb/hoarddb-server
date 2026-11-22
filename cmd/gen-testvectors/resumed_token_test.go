package main

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

// TestResumedTokenCaptureSurvivesASlowGap: the "auth with a resumption token"
// case mints a one-second token and presents it later; the server expires it
// at the end of the unix second it was minted in, so a host that is slow
// between the two (BUG-0487) outlives the token. Before the fix this case,
// held 400 ms between mint and request, failed in about four of ten captures.
func TestResumedTokenCaptureSurvivesASlowGap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	g := &generation{
		progress: log.New(io.Discard, "", 0),
		workDir:  t.TempDir(),
		ports:    &portBook{},
		servers:  make(map[string]*inProcessServer),
	}
	defer g.close()
	if err := g.makeKeys(); err != nil {
		t.Fatal(err)
	}
	var resumption exchangeCase
	for _, build := range exchangeCases() {
		if candidate := build("v000"); candidate.name == "auth with a resumption token" {
			resumption = candidate
		}
	}
	if resumption.name == "" {
		t.Fatal("the resumption-token case is gone from exchangeCases")
	}
	resumption.before = func(ctx context.Context, _ *generation, _ string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(400 * time.Millisecond):
			return nil
		}
	}
	for captureNumber := 1; captureNumber <= 10; captureNumber++ {
		if _, err := g.capture(ctx, captureNumber, "v000", resumption); err != nil {
			t.Fatalf("capture %d of 10: %v", captureNumber, err)
		}
	}
}

func TestWaitForSecondStart(t *testing.T) {
	start := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		nanos int
		waits bool
	}{
		{"just after a boundary", 20_000_000, false},
		{"late in a second", 900_000_000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := start.Add(time.Duration(tc.nanos))
			begin := time.Now()
			now := func() time.Time { return clock.Add(time.Since(begin)) }
			if err := waitForSecondStart(context.Background(), now, 100*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if got := now().Nanosecond(); got >= 150_000_000 {
				t.Fatalf("returned %d ms into the second, want under the 100 ms window", got/1_000_000)
			}
			if waited := time.Since(begin); tc.waits != (waited > 50*time.Millisecond) {
				t.Fatalf("waited %v, waits=%v", waited, tc.waits)
			}
		})
	}
}
