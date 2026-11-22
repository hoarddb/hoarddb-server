package wire

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// seedPacerConfig is what a seedPacer reads.
type seedPacerConfig struct {
	// BytesPerSecond is the node's whole seed rate (replication.seed_bytes_per_second); zero
	// or less is unlimited.
	BytesPerSecond int64
	// Now is the pacer's clock; nil is time.Now.
	Now func() time.Time
	// Sleep waits out a charge; nil waits on a timer and returns ctx's error when ctx ends.
	Sleep func(ctx context.Context, d time.Duration) error
}

// seedPaceState is one pull's debt: the moment its last charge has been paid for.
type seedPaceState struct {
	readyAt time.Time
}

// seedPacer is the node's one seed byte rate, divided evenly over the pulls in flight
// (seeding-completion plan section 4). The rate decides when a pull's next SEED_ACK leaves,
// never how large a grant is: a chunk is never split, so a credit below one entry would make
// the source refuse it.
//
// Cost: one object per node and a time value per pull in flight (at most
// replication.seed_slots_in_flight, default 2); nothing per bucket, no goroutine, no metric.
type seedPacer struct {
	mu       sync.Mutex
	cfg      seedPacerConfig
	attempts map[reconcileKey]*seedPaceState
}

func newSeedPacer(cfg seedPacerConfig) *seedPacer {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepOn
	}
	return &seedPacer{cfg: cfg, attempts: make(map[reconcileKey]*seedPaceState)}
}

// Begin starts counting the pair's pull as one of the pulls the rate is divided over.
func (s *seedPacer) Begin(slot uint16, db uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[reconcileKey{slot: slot, db: db}] = &seedPaceState{}
}

// End stops counting the pair's pull, so the others' shares grow from their next charge.
func (s *seedPacer) End(slot uint16, db uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.attempts, reconcileKey{slot: slot, db: db})
}

// Spent charges the pair for an applied chunk of bytes at its share of the rate as it stands
// now; an earlier charge is not re-priced when the share changes. Time that has passed pays
// a charge, so a pull that was idle starts from now and does not bank credit.
func (s *seedPacer) Spent(slot uint16, db uint64, bytes int64) {
	if s.cfg.BytesPerSecond <= 0 || bytes <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, began := s.attempts[reconcileKey{slot: slot, db: db}]
	if !began {
		return
	}
	share := max(s.cfg.BytesPerSecond/int64(len(s.attempts)), 1)
	start := s.cfg.Now()
	if state.readyAt.After(start) {
		start = state.readyAt
	}
	// A float so that a very large chunk cannot overflow a Duration.
	state.readyAt = start.Add(time.Duration(float64(bytes) / float64(share) * float64(time.Second)))
}

// Wait blocks until the pair may send its next SEED_ACK, or for limit if that is sooner (zero
// is no limit). The limit keeps the source from dropping a stream it hears nothing on; the
// debt stays, so the next Wait pays what this one did not.
func (s *seedPacer) Wait(ctx context.Context, slot uint16, db uint64, limit time.Duration) error {
	if s.cfg.BytesPerSecond <= 0 {
		return nil
	}
	s.mu.Lock()
	state, began := s.attempts[reconcileKey{slot: slot, db: db}]
	var wait time.Duration
	if began {
		wait = state.readyAt.Sub(s.cfg.Now())
	}
	s.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	if limit > 0 && wait > limit {
		wait = limit
	}
	if err := s.cfg.Sleep(ctx, wait); err != nil {
		return fmt.Errorf("waiting for the seed byte rate (slot %d, database %d): %w", slot, db, err)
	}
	return nil
}
