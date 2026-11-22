package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// The fixed mix, in per-mille of operations: 400 point reads, 100 reads by
// owner through the secondary index, 480 updates by key, 20 inserts of new
// keys. Reads are half of the operations and writes the other half.
const (
	mixPointRead  = 400
	mixOwnerRead  = 100
	mixUpdate     = 480
	mixTotal      = 1000
	insertCapDiv  = 50 // inserts per phase (warm-up, measured) are capped at records/50 so the table barely grows
	maxLatencyUS  = 60_000_000
	latencyDigits = 3
)

// opKind names an operation for the per-kind latency record.
type opKind int

const (
	opPointRead opKind = iota
	opOwnerRead
	opUpdate
	opInsert
	opKinds
)

var opNames = [opKinds]string{"point_read", "owner_read", "update", "insert"}

// legConfig is one leg's parameters.
type legConfig struct {
	Records   int64 // records stored when the leg starts
	Workers   int
	Warmup    time.Duration
	Measured  time.Duration
	BodyBytes int
	Seed      uint64
	Fanout    int64 // records per owner; 0 means ownerFanout
}

// legCounters are what one worker collects. Histograms are per kind and merged
// at the end, so no lock sits on the hot path.
type legCounters struct {
	hists  [opKinds]*hdrhistogram.Histogram
	errors map[string]int64
}

func newLegCounters() *legCounters {
	counters := &legCounters{errors: map[string]int64{}}
	for kind := range counters.hists {
		counters.hists[kind] = hdrhistogram.New(1, maxLatencyUS, latencyDigits)
	}
	return counters
}

// pickOp maps a draw in [0, mixTotal) to an operation kind.
func pickOp(draw int) opKind {
	switch {
	case draw < mixPointRead:
		return opPointRead
	case draw < mixPointRead+mixOwnerRead:
		return opOwnerRead
	case draw < mixPointRead+mixOwnerRead+mixUpdate:
		return opUpdate
	default:
		return opInsert
	}
}

// legRunner shares the key counters between workers.
type legRunner struct {
	db          store
	cfg         legConfig
	nextKey     atomic.Int64 // next new key to insert
	insertsLeft atomic.Int64 // inserts still allowed in this leg
	measuring   atomic.Bool
	version     atomic.Int64
}

// runOne performs one operation and returns what it was and how long the call
// took. When the insert cap is spent an insert becomes an update, so the write
// share stays at half.
func (r *legRunner) runOne(ctx context.Context, gen *rand.Rand) (opKind, time.Duration, error) {
	kind := pickOp(gen.IntN(mixTotal))
	if kind == opInsert && r.insertsLeft.Add(-1) < 0 {
		kind = opUpdate
	}
	existing := gen.Int64N(r.cfg.Records)
	start := time.Now()
	var err error
	switch kind {
	case opPointRead:
		err = r.db.get(ctx, existing)
	case opOwnerRead:
		var found int
		if found, err = r.db.byOwner(ctx, existing/r.cfg.Fanout); err == nil && found == 0 {
			err = errors.New("owner read found no records")
		}
	case opUpdate:
		err = r.db.update(ctx, existing, r.version.Add(1), r.cfg.BodyBytes)
	case opInsert:
		err = r.db.insert(ctx, r.nextKey.Add(1)-1, r.cfg.BodyBytes)
	case opKinds:
		err = errors.New("unreachable operation kind")
	}
	return kind, time.Since(start), err
}

// worker runs closed-loop operations until ctx ends, recording only while the
// measured window is open.
func (r *legRunner) worker(ctx context.Context, index int, counters *legCounters) {
	gen := rand.New(rand.NewPCG(r.cfg.Seed, uint64(index)+1)) //nolint:gosec // workload generator, not a secret
	for ctx.Err() == nil {
		kind, took, err := r.runOne(ctx, gen)
		if ctx.Err() != nil || !r.measuring.Load() {
			continue
		}
		if err != nil {
			counters.errors[err.Error()]++
			continue
		}
		if recErr := counters.hists[kind].RecordValue(max(1, took.Microseconds())); recErr != nil {
			counters.errors["latency out of histogram range"]++
		}
	}
}

// runLeg runs the warm-up and the measured window and returns the merged
// histograms, the error counts and the measured wall time.
func runLeg(ctx context.Context, db store, cfg legConfig) (*legCounters, int64, time.Duration) {
	if cfg.Fanout <= 0 {
		cfg.Fanout = ownerFanout
	}
	runner := &legRunner{db: db, cfg: cfg}
	runner.nextKey.Store(cfg.Records)
	runner.insertsLeft.Store(max(1, cfg.Records/insertCapDiv))
	legCtx, stop := context.WithCancel(ctx)
	defer stop()
	all := make([]*legCounters, cfg.Workers)
	var group sync.WaitGroup
	for index := range all {
		all[index] = newLegCounters()
		group.Add(1)
		go func() {
			defer group.Done()
			runner.worker(legCtx, index, all[index])
		}()
	}
	sleepOrDone(legCtx, cfg.Warmup)
	// Warm-up inserts must not use up the allowance, or a fast engine would
	// measure no inserts at all: the measured window gets its own.
	runner.insertsLeft.Store(max(1, cfg.Records/insertCapDiv))
	startedAt := time.Now()
	runner.measuring.Store(true)
	sleepOrDone(legCtx, cfg.Measured)
	runner.measuring.Store(false)
	measured := time.Since(startedAt)
	stop()
	group.Wait()
	merged := newLegCounters()
	for _, one := range all {
		for kind := range one.hists {
			merged.hists[kind].Merge(one.hists[kind])
		}
		for text, n := range one.errors {
			merged.errors[text] += n
		}
	}
	inserted := runner.nextKey.Load() - cfg.Records
	return merged, inserted, measured
}

// sleepOrDone waits for d or for ctx to end.
func sleepOrDone(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
