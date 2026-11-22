package main

import (
	"testing"
	"time"
)

// TestLoginTimeIsNotOperationLatency: a worker logs in lazily, before the
// operation's clock starts (mixWorker.loop takes `started` after newSocialOps),
// so a slow login shows up in no recorded latency.
func TestLoginTimeIsNotOperationLatency(t *testing.T) {
	const loginTime = 200 * time.Millisecond
	cfg := mixTestConfig("rh", "social_login_window")
	// No warmup: each worker's first login must land inside the measured window,
	// or a login timed as part of the operation would be discarded with the warmup.
	cfg.Duration, cfg.Warmup = 600*time.Millisecond, 0
	fake := newFakeServer()
	engine := fakeHoardEngineOn(cfg, fake)
	fastDial := engine.dial
	engine.dial = func(addr string) (wireCaller, func(), error) {
		time.Sleep(loginTime)
		return fastDial(addr)
	}
	record := completedMixRecord(t, engine, cfg)
	if record.Latency.All.Count == 0 {
		t.Fatal("no operation was measured, so the test says nothing")
	}
	if slowest := float64(record.Latency.All.Max); slowest >= float64(loginTime/time.Millisecond)/2 {
		t.Errorf("slowest recorded operation %.1f ms: a %v login leaked into the operation latency", slowest, loginTime)
	}
}
