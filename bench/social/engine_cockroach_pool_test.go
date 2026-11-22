package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestCockroachLikeWithColdStatementCacheDoesNotNeedASecondConnection: -mix
// op:W2 at a depth equal to the pool size. Every worker begins its like
// transaction before any statement is prepared, so each holds one of the
// -depth pooled connections; preparing a statement then needs a connection
// nobody can give back (the MySQL engine's BUG-0713, which database/sql shares).
func TestCockroachLikeWithColdStatementCacheDoesNotNeedASecondConnection(t *testing.T) {
	const depth = 4
	engine, state := newFakeCockroachEngine(t)
	engine.cfg.Conc, engine.cfg.OpTimeout = depth, 500*time.Millisecond
	engine.db.SetMaxOpenConns(depth)
	state.onBegin = holdUntilAll(depth)

	var workers sync.WaitGroup
	failures := make(chan error, depth)
	started := time.Now()
	for worker := range depth {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ops := &cockroachOps{engine: engine}
			if err := ops.likePost(context.Background(), worker+10, 3, 1); err != nil {
				failures <- err
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("a like failed on a pool of %d connections with %d workers: %v", depth, depth, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Errorf("the likes took %v; they should not wait on a connection", elapsed)
	}
}

// TestMixOpW2OnCockroachCompletesAtPoolDepth is the same through the -mix op:W2
// leg a harness runs, on a cold statement cache.
func TestMixOpW2OnCockroachCompletesAtPoolDepth(t *testing.T) {
	const depth = 4
	engine, state := newFakeCockroachEngine(t)
	engine.cfg.Conc, engine.cfg.OpTimeout = depth, 500*time.Millisecond
	engine.cfg.Mix, engine.cfg.Dataset, engine.cfg.Attach = "op:W2", "tiny", true
	engine.cfg.Users, engine.cfg.ZipfS, engine.cfg.RandSeed, engine.cfg.VerifyPct = tinySpec.Users, 1.1, 3, 100
	engine.cfg.Duration, engine.cfg.Warmup, engine.cfg.ClientVMs, engine.cfg.WarmConverged = 300*time.Millisecond, 20*time.Millisecond, 1, true
	engine.db.SetMaxOpenConns(depth)
	state.onBegin = holdUntilAll(depth)

	done := make(chan *legRecord, 1)
	go func() {
		record, _ := runMixLeg(context.Background(), engine.cfg, tinySpec, engine)
		done <- record
	}()
	select {
	case record := <-done:
		if record == nil || record.Ops["W2"].Count == 0 || len(record.ErrorsByType) != 0 {
			t.Errorf("the op:W2 leg recorded %v likes with errors %v; want completed likes and no errors", record.Ops["W2"].Count, record.ErrorsByType)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the op:W2 leg did not finish in 10 s")
	}
}

// TestCockroachStatementsArePreparedOnceOnThePool: a statement a transaction had
// to prepare on its own connection is prepared on the pool afterwards, once.
func TestCockroachStatementsArePreparedOnceOnThePool(t *testing.T) {
	engine, _ := newFakeCockroachEngine(t)
	ops := &cockroachOps{engine: engine}
	if err := ops.likePost(context.Background(), 4, 3, 1); err != nil {
		t.Fatal(err)
	}
	cachedAfterFirst := len(engine.social)
	if err := ops.likePost(context.Background(), 5, 3, 1); err != nil {
		t.Fatal(err)
	}
	if cachedAfterFirst != 3 || len(engine.social) != cachedAfterFirst || len(ops.unprepared) != 0 {
		t.Errorf("%d statements cached after the first like, %d after the second, %d left to prepare; want 3, 3 and 0", cachedAfterFirst, len(engine.social), len(ops.unprepared))
	}
}
