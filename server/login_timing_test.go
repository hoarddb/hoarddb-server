package server

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// newLoginTimingManager seeds alice (enabled) and bob (disabled).
func newLoginTimingManager(t *testing.T) *AuthManager {
	t.Helper()
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	seedUser(t, am, "bob")
	am.mu.Lock()
	am.users["bob"].Enabled = false
	am.mu.Unlock()
	return am
}

// TestLoginDerivesForEveryUsername is the regression test: a password check runs the
// credential derivation once whether the account exists, is disabled, or does
// not exist, and still reports which it was to the caller.
func TestLoginDerivesForEveryUsername(t *testing.T) {
	am := newLoginTimingManager(t)
	var derivations atomic.Int64
	verifyUserHook = func() { derivations.Add(1) }
	defer func() { verifyUserHook = nil }()

	for _, attempt := range []struct {
		username string
		want     error
	}{
		{"alice", ErrBadPassword},
		{"bob", ErrUserDisabled},
		{"nobody", ErrUserNotFound},
	} {
		derivations.Store(0)
		ok, err := am.VerifyUser(attempt.username, "wrong-password-123")
		if ok || !errors.Is(err, attempt.want) {
			t.Errorf("VerifyUser(%q) = %v, %v; want false, %v", attempt.username, ok, err, attempt.want)
		}
		if got := derivations.Load(); got != 1 {
			t.Errorf("VerifyUser(%q) ran %d derivations, want 1", attempt.username, got)
		}
	}
}

// TestLoginTimeDoesNotDependOnTheAccount is the regression test by the clock: a wrong
// password for an unknown or disabled account takes about as long as one for
// an existing account. The bound is loose (half the median) so a busy host
// does not fail it; without the derivation the gap is two orders of magnitude.
func TestLoginTimeDoesNotDependOnTheAccount(t *testing.T) {
	am := newLoginTimingManager(t)
	median := func(username string) time.Duration {
		const runs = 5
		took := make([]time.Duration, 0, runs)
		for range runs {
			start := time.Now()
			_, _ = am.VerifyUser(username, "wrong-password-123")
			took = append(took, time.Since(start))
		}
		slices.Sort(took)
		return took[runs/2]
	}
	existing := median("alice")
	for _, username := range []string{"nobody", "bob"} {
		if got := median(username); got < existing/2 {
			t.Errorf("a wrong password for %q took %v, against %v for an existing account; want at least half", username, got, existing)
		}
	}
}
