package main

import (
	"errors"
	"fmt"
	"time"
)

const (
	// defaultLoginConcurrency is how many logins one client has in flight. A
	// default hoardDB node refuses the excess beyond roughly a hundred from one
	// address (BUG-0697's limit), so 16 leaves wide margin and costs a 128-socket
	// setup well under a second.
	defaultLoginConcurrency = 16
	// loginAttempts and loginFirstBackoff bound the retry of a login refused as
	// RATE_LIMITED: about ten seconds in all before the run fails.
	loginAttempts     = 8
	loginFirstBackoff = 100 * time.Millisecond
	loginMaxBackoff   = 4 * time.Second
)

// errLoginRateLimited marks a login the node refused with RATE_LIMITED; no
// password was checked, so waiting and trying again is the right response.
var errLoginRateLimited = errors.New("login refused as RATE_LIMITED")

// loginGate admits a bounded number of logins at once from this process; the
// rest wait their turn. Every worker of a leg dials at the same moment, and one
// client address sending them all together is what a default node refuses.
type loginGate struct {
	slots        chan struct{}
	attempts     int
	firstBackoff time.Duration
}

// logins is the gate every dialWire goes through; main sizes it from -login-concurrency.
var logins = newLoginGate(defaultLoginConcurrency, loginAttempts, loginFirstBackoff)

func newLoginGate(slots, attempts int, firstBackoff time.Duration) *loginGate {
	return &loginGate{slots: make(chan struct{}, slots), attempts: attempts, firstBackoff: firstBackoff}
}

// run calls attempt while holding a slot. A RATE_LIMITED refusal releases the
// slot, waits with a doubling backoff and tries again, up to the attempt
// budget; any other result is returned as is.
func (g *loginGate) run(attempt func() error) error {
	backoff := g.firstBackoff
	for tried := 1; ; tried++ {
		g.slots <- struct{}{}
		err := attempt()
		<-g.slots
		if !errors.Is(err, errLoginRateLimited) {
			return err
		}
		if tried >= g.attempts {
			return fmt.Errorf("%d attempts, each refused: %w; lower -login-concurrency (now %d) or check the node's login limits", tried, err, cap(g.slots))
		}
		time.Sleep(backoff)
		backoff = min(backoff*2, loginMaxBackoff)
	}
}
