package cluster

import (
	"runtime"
	"testing"
	"time"
)

// TestOpeningAndClosingARaftSeatLeaksNothing: the library's NetworkTransport has its own
// accept loop that only its Close ends; twenty open and close cycles leave no goroutine.
func TestOpeningAndClosingARaftSeatLeaksNothing(t *testing.T) {
	settle := func() int {
		deadline := time.Now().Add(4 * time.Second)
		for {
			n := runtime.NumGoroutine()
			if n <= baselineGoroutines+3 || time.Now().After(deadline) {
				return n
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	baselineGoroutines = runtime.NumGoroutine()
	for range 20 {
		cfg := quickRaft(t, t.TempDir(), "n1")
		r, err := OpenRaft(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := settle(); got > baselineGoroutines+3 {
		t.Fatalf("%d goroutines after 20 open/close cycles, %d before: each Close leaves one behind", got, baselineGoroutines)
	}
}

var baselineGoroutines int

// TestAnEmptyFingerprintNeverMatchesAnEmptyFingerprint: a connection that authenticated
// with no certificate carries "", and a source that records "" for an id must not make the
// two equal.
func TestAnEmptyFingerprintNeverMatchesAnEmptyFingerprint(t *testing.T) {
	cfg := quickRaft(t, t.TempDir(), "n1")
	cfg.PeerFingerprint = func(string) (string, bool) { return "", true }
	r := openOrFail(t, cfg)
	hello := r.Identity()
	hello.PeerFingerprint = ""
	if err := r.AdmitPeer(hello, true); err == nil {
		t.Fatal("a peer with no authenticated key was admitted because the recorded key was empty too")
	}
}
