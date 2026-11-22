package transport

import "testing"

// TestConnLimiterClientBudget verifies the client budget accepts up to its
// capacity, refuses beyond it, and frees slots on release.
func TestConnLimiterClientBudget(t *testing.T) {
	lim := NewConnLimiter(2, 2)

	r1, ok := lim.AcquireClient()
	if !ok {
		t.Fatal("first client acquire failed on an empty budget")
	}
	r2, ok := lim.AcquireClient()
	if !ok {
		t.Fatal("second client acquire failed on a budget of 2")
	}
	if _, ok := lim.AcquireClient(); ok {
		t.Fatal("third client acquire succeeded on a budget of 2")
	}
	if clients, _ := lim.Counts(); clients != 2 {
		t.Fatalf("client count = %d, want 2", clients)
	}
	r1()
	if _, ok := lim.AcquireClient(); !ok {
		t.Fatal("client acquire failed after a release freed a slot")
	}
	r2()
}

// TestConnLimiterPeerBudgetIndependent verifies the reserved peer budget
// still admits peers while the client budget is full — a client flood must
// not starve replication (spec 5.6).
func TestConnLimiterPeerBudgetIndependent(t *testing.T) {
	lim := NewConnLimiter(1, 1)

	r1, ok := lim.AcquireClient()
	if !ok {
		t.Fatal("client acquire failed")
	}
	if _, ok := lim.AcquireClient(); ok {
		t.Fatal("second client acquire succeeded on a budget of 1")
	}
	peerRelease, ok := lim.AcquirePeer()
	if !ok {
		t.Fatal("peer acquire refused while only the client budget is full")
	}
	if clients, peers := lim.Counts(); clients != 1 || peers != 1 {
		t.Fatalf("counts = (%d, %d), want (1, 1)", clients, peers)
	}
	peerRelease()
	r1()
}

// TestConnLimiterReleaseIdempotent verifies a release call is safe to make
// twice (the error-path defer pattern must never double-return a slot).
func TestConnLimiterReleaseIdempotent(t *testing.T) {
	lim := NewConnLimiter(1, 1)

	release, ok := lim.AcquireClient()
	if !ok {
		t.Fatal("acquire failed on an empty budget")
	}
	release()
	release() // second call must be a no-op, not a second free

	// The cap is still 1: one fresh acquire succeeds, a second one fails.
	r2, ok := lim.AcquireClient()
	if !ok {
		t.Fatal("budget did not recover after release: double-release leaked capacity")
	}
	if _, ok := lim.AcquireClient(); ok {
		t.Fatal("cap was raised: a budget of 1 accepted two held slots")
	}
	r2()
}
