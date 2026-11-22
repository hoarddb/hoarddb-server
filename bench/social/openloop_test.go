package main

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

// stallEngine answers every timeline read after a fixed service time, except
// the stallAt-th, which takes stall: a server that hangs once and recovers.
type stallEngine struct {
	calls   atomic.Int64
	stallAt int64
	service time.Duration
	stall   time.Duration
}

func (e *stallEngine) setup(context.Context) (schemaInfo, error)  { return schemaInfo{}, nil }
func (e *stallEngine) attach(context.Context) (schemaInfo, error) { return schemaInfo{}, nil }
func (e *stallEngine) newSession() (engineSession, error)         { return &stallSession{engine: e}, nil }
func (e *stallEngine) desynced(error) bool                        { return false }
func (e *stallEngine) durability() map[string]string              { return nil }
func (e *stallEngine) target() string                             { return "stall-engine" }
func (e *stallEngine) close()                                     {}

type stallSession struct{ engine *stallEngine }

func (s *stallSession) readTimeline(context.Context, readRequest) (timelinePage, error) {
	delay := s.engine.service
	if s.engine.calls.Add(1) == s.engine.stallAt {
		delay = s.engine.stall
	}
	time.Sleep(delay)
	return timelinePage{}, nil
}
func (s *stallSession) writePost(context.Context, string, map[string]interface{}) error { return nil }
func (s *stallSession) putProfile(context.Context, string, map[string]interface{}) error {
	return nil
}
func (s *stallSession) attachState(context.Context, string) (int64, int64, error) { return 0, 0, nil }
func (s *stallSession) close()                                                    {}

// runStallLeg drives the stall engine with one worker for three seconds at the
// given offered rate (0 means closed loop) and returns the read latencies.
func runStallLeg(t *testing.T, offeredRate float64) *histogram {
	t.Helper()
	engine := &stallEngine{stallAt: 500, service: time.Millisecond, stall: 200 * time.Millisecond}
	cfg := &config{Users: 100, ZipfS: 1.1, Conc: 1, ReadPct: 100, ReadPosts: 20, RandSeed: 1, Rate: offeredRate, ClientVMs: 1}
	states := make([]userState, cfg.Users)
	begin := time.Now()
	results := runWorkers(context.Background(), cfg, engine, states, begin, begin.Add(3*time.Second))
	merged := newHistogram()
	for _, result := range results {
		merged.merge(result.readLatencies)
	}
	if merged.count() < 1000 {
		t.Fatalf("only %d samples recorded in three seconds", merged.count())
	}
	return merged
}

func TestOpenLoopChargesQueueingDelay(t *testing.T) {
	closed := runStallLeg(t, 0)
	open := runStallLeg(t, 500)
	if closedMax := closed.quantile(1); closedMax < 150*time.Millisecond {
		t.Fatalf("closed loop never saw the 200 ms stall (max %v): the stub did not stall", closedMax)
	}
	if closedP99 := closed.quantile(0.99); closedP99 > 20*time.Millisecond {
		t.Errorf("closed-loop p99 is %v: the stall should be hidden by coordinated omission", closedP99)
	}
	if openP999 := open.quantile(0.999); openP999 < 150*time.Millisecond {
		t.Errorf("open-loop p99.9 is %v, want at least 150 ms: requests that arrived during the stall must be charged from their scheduled time", openP999)
	}
}

func TestPoissonArrivalsMeanRate(t *testing.T) {
	const arrivals = 100_000
	const rate = 200.0
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	schedule := newArrivalSchedule(start, rate, 9)
	var gaps []float64
	previous := start
	for range arrivals {
		scheduled := schedule.next()
		gaps = append(gaps, scheduled.Sub(previous).Seconds())
		previous = scheduled
	}
	elapsed := previous.Sub(start).Seconds()
	if achieved := arrivals / elapsed; math.Abs(achieved-rate)/rate > 0.02 {
		t.Errorf("mean arrival rate %.1f/s, want %v/s within 2%%", achieved, rate)
	}
	var mean, squares float64
	for _, gap := range gaps {
		mean += gap
	}
	mean /= arrivals
	for _, gap := range gaps {
		squares += (gap - mean) * (gap - mean)
	}
	// Exponential gaps have a standard deviation equal to their mean; evenly spaced ones have none.
	if variation := math.Sqrt(squares/arrivals) / mean; variation < 0.95 || variation > 1.05 {
		t.Errorf("gap standard deviation over mean is %.3f, want 1.0 for Poisson arrivals", variation)
	}
}

func TestRateAndDepthSplitAcrossClients(t *testing.T) {
	for _, tc := range []struct {
		clients       int
		depth         int
		wantDepths    []int
		wantRateEach  float64
		totalRate     float64
		totalDepthSum int
	}{
		{clients: 2, depth: 64, wantDepths: []int{32, 32}, wantRateEach: 50, totalRate: 100, totalDepthSum: 64},
		{clients: 2, depth: 5, wantDepths: []int{3, 2}, wantRateEach: 50, totalRate: 100, totalDepthSum: 5},
		{clients: 1, depth: 64, wantDepths: []int{64}, wantRateEach: 100, totalRate: 100, totalDepthSum: 64},
	} {
		var depthSum int
		for index := range tc.clients {
			rate, depth := clientShare(tc.totalRate, tc.depth, tc.clients, index)
			if rate != tc.wantRateEach || depth != tc.wantDepths[index] {
				t.Errorf("%d clients, client %d: rate %v depth %d, want rate %v depth %d", tc.clients, index, rate, depth, tc.wantRateEach, tc.wantDepths[index])
			}
			depthSum += depth
		}
		if depthSum != tc.totalDepthSum {
			t.Errorf("%d clients: shares sum to depth %d, want %d", tc.clients, depthSum, tc.totalDepthSum)
		}
	}

	// Each client VM writes its own histogram; the report merges what they sent.
	whole, first, second := newHistogram(), newHistogram(), newHistogram()
	for i := range 4000 {
		latency := time.Duration(1+i%50) * time.Millisecond
		mustRecord(t, whole, latency)
		if i%2 == 0 {
			mustRecord(t, first, latency)
		} else {
			mustRecord(t, second, latency)
		}
	}
	merged := newHistogram()
	for _, part := range []*histogram{first, second} {
		encoded, err := part.encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := decodeHistogram(encoded)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		merged.merge(decoded)
	}
	if merged.count() != whole.count() || merged.quantile(0.99) != whole.quantile(0.99) {
		t.Errorf("merged histogram has %d samples, p99 %v; recorded together %d, p99 %v", merged.count(), merged.quantile(0.99), whole.count(), whole.quantile(0.99))
	}
}
