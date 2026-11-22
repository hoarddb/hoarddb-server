package wire

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// reclaimBudget is the node's one token bucket for background deletion
// (storage.reclaim_keys_per_second): the retired-field and dropped-bucket sweeps, the dedupe
// sweep and the range reset of a reseed all charge it, so their sum stays under the key rate
// whatever mix is running (seeding-completion plan §1.3). A charge spends its cost, and when
// the bucket is overdrawn the caller sleeps until the refill has paid the overdraft back. It
// holds at most one step's credit, so a pass cannot save up a burst.
//
// Cost: one object per node, a mutex and four numbers; nothing per pair or per bucket.
type reclaimBudget struct {
	mu        sync.Mutex
	perSecond float64
	capacity  float64
	tokens    float64
	settled   time.Time
	now       func() time.Time
	sleep     func(ctx context.Context, d time.Duration) error
}

// reclaimBudgetConfig builds a reclaimBudget; a zero field takes its default.
type reclaimBudgetConfig struct {
	// KeysPerSecond is storage.reclaim_keys_per_second.
	KeysPerSecond int
	// StepKeys is the largest credit the bucket holds: the keys one step visits.
	StepKeys int
	// Now is the bucket's clock; nil is time.Now.
	Now func() time.Time
	// Sleep waits out an overdraft; nil sleeps on the wall clock.
	Sleep func(ctx context.Context, d time.Duration) error
}

func newReclaimBudget(cfg reclaimBudgetConfig) *reclaimBudget {
	if cfg.KeysPerSecond <= 0 {
		cfg.KeysPerSecond = defaultSweepKeysPerSecond
	}
	if cfg.StepKeys <= 0 {
		cfg.StepKeys = defaultSweepStepKeys
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepOn
	}
	capacity := float64(cfg.StepKeys)
	return &reclaimBudget{perSecond: float64(cfg.KeysPerSecond), capacity: capacity, tokens: capacity, now: cfg.Now, sleep: cfg.Sleep}
}

func (b *reclaimBudget) charge(ctx context.Context, cost int) error {
	b.mu.Lock()
	b.refill()
	b.tokens -= float64(cost)
	var wait time.Duration
	if b.tokens < 0 {
		wait = time.Duration(-b.tokens / b.perSecond * float64(time.Second))
	}
	b.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	if err := b.sleep(ctx, wait); err != nil {
		return fmt.Errorf("waiting for the reclaim budget: %w", err)
	}
	b.mu.Lock()
	b.refill()
	b.mu.Unlock()
	return nil
}

func (b *reclaimBudget) refill() {
	at := b.now()
	if !b.settled.IsZero() {
		b.tokens = min(b.capacity, b.tokens+b.perSecond*at.Sub(b.settled).Seconds())
	}
	b.settled = at
}
