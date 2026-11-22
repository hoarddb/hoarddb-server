package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// run executes the mode against the engine; stdout is where next and an
// un-filed leg record go.
func run(ctx context.Context, opt sweepFlags, stdout io.Writer) error {
	db, err := openStore(ctx, opt)
	if err != nil {
		return err
	}
	return runWith(ctx, db, opt, stdout)
}

// runWith is run against an already opened store, so a test can pass a fake.
func runWith(ctx context.Context, db store, opt sweepFlags, stdout io.Writer) error {
	defer db.close()
	switch opt.mode {
	case "init":
		return db.prepare(ctx)
	case "next":
		next, err := db.nextKey(ctx)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, next)
		return err
	case "load":
		if opt.from < 0 || opt.to <= opt.from {
			return fmt.Errorf("-from %d -to %d is not a range; give -from lower than -to", opt.from, opt.to)
		}
		return db.loadRange(ctx, opt.from, opt.to, opt.bodyBytes)
	case "leg":
		return runLegMode(ctx, db, opt, stdout)
	default:
		return fmt.Errorf("unknown -mode %q: use init, next, load or leg", opt.mode)
	}
}

func runLegMode(ctx context.Context, db store, opt sweepFlags, stdout io.Writer) error {
	if err := db.prepare(ctx); err != nil {
		return err
	}
	next, err := db.nextKey(ctx)
	if err != nil {
		return err
	}
	if next == 0 {
		return fmt.Errorf("the %s database holds no records; load some first (-mode load)", db.name())
	}
	fanout := opt.fanout
	if fanout <= 0 {
		fanout = ownerFanout
	}
	cfg := legConfig{Records: next, Workers: opt.workers, Warmup: opt.warmup, Measured: opt.measured, BodyBytes: opt.bodyBytes, Seed: opt.seed, Fanout: fanout}
	counters, inserted, measured := runLeg(ctx, db, cfg)
	encoded, err := buildRecord(db, cfg, counters, inserted, measured).line()
	if err != nil {
		return err
	}
	if opt.out == "" {
		_, err = fmt.Fprintf(stdout, "%s\n", encoded)
		return err
	}
	file, err := os.OpenFile(opt.out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening -out: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing -out: %w", err)
	}
	return file.Close()
}

// parallelBatches cuts from..to into loadBatch-key batches and runs them over
// loadParallel goroutines; the first error wins and stops the rest.
func parallelBatches(ctx context.Context, from, to int64, one func(batchFrom, batchTo int64) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan [2]int64)
	var group sync.WaitGroup
	var firstErr error
	var once sync.Once
	for range loadParallel {
		group.Add(1)
		go func() {
			defer group.Done()
			for job := range jobs {
				if err := one(job[0], job[1]); err != nil {
					once.Do(func() { firstErr = err; cancel() })
				}
			}
		}()
	}
feed:
	for start := from; start < to; start += loadBatch {
		select {
		case jobs <- [2]int64{start, min(start+loadBatch, to)}:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	group.Wait()
	return firstErr
}
