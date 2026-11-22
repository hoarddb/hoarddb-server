package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// opFunc is one measured operation for the worker with the given index. It
// returns the duration of the database call alone: id and payload preparation
// stay outside the timer.
type opFunc func(ctx context.Context, w worker, workerIndex int) (time.Duration, error)

// splitOps divides total operations across workers; the first total%workers
// workers take one more, so the sum is exact.
func splitOps(total, workers int) []int {
	share := make([]int, workers)
	for i := range share {
		share[i] = total / workers
		if i < total%workers {
			share[i]++
		}
	}
	return share
}

// runPhase runs ops operations over the given workers at once, one goroutine
// per worker, and returns every latency and the wall time. The first error
// cancels the rest and is returned; every goroutine has exited when it returns.
func runPhase(ctx context.Context, workers []worker, ops int, do opFunc) ([]time.Duration, time.Duration, error) {
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	share := splitOps(ops, len(workers))
	latencies := make([][]time.Duration, len(workers))
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
		start    = make(chan struct{})
	)
	for i, w := range workers {
		latencies[i] = make([]time.Duration, 0, share[i])
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < share[i]; n++ {
				took, err := do(phaseCtx, w, i)
				if err != nil {
					errOnce.Do(func() { firstErr = err; cancel() })
					return
				}
				latencies[i] = append(latencies[i], took)
			}
		}()
	}
	begun := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(begun)
	if firstErr != nil {
		return nil, 0, firstErr
	}
	merged := make([]time.Duration, 0, ops)
	for _, part := range latencies {
		merged = append(merged, part...)
	}
	return merged, elapsed, nil
}

// benchmark holds what the phases share.
type benchmark struct {
	cfg     *config
	workers []worker
	ids     *idGenerator
	payload string
	loaded  []string
	ledger  *ackLedger
}

// randomPayload is payloadBytes of hex text, drawn once per run: a payload of
// one repeated byte would flatter an engine that compresses.
func randomPayload(payloadBytes int) (string, error) {
	raw := make([]byte, (payloadBytes+1)/2)
	if _, err := crand.Read(raw); err != nil {
		return "", fmt.Errorf("drawing the payload: %w", err)
	}
	return hex.EncodeToString(raw)[:payloadBytes], nil
}

// insertOp times an insert of a fresh id. When record is set the id joins the
// preloaded set; the append is guarded, and outside the timer.
func (b *benchmark) insertOp(record bool, mu *sync.Mutex) opFunc {
	return func(ctx context.Context, w worker, _ int) (time.Duration, error) {
		id, err := b.ids.next()
		if err != nil {
			return 0, err
		}
		opCtx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
		defer cancel()
		began := time.Now()
		if err := w.insert(opCtx, id, b.payload); err != nil {
			return 0, err
		}
		took := time.Since(began)
		if err := b.ledger.record(id); err != nil {
			return 0, err
		}
		if record {
			mu.Lock()
			b.loaded = append(b.loaded, id)
			mu.Unlock()
		}
		return took, nil
	}
}

// getOp times a get of a random preloaded id. Each worker draws from its own
// generator seeded by its index, so a rerun asks the same questions.
func (b *benchmark) getOp(workerCount int) opFunc {
	draws := make([]*rand.Rand, workerCount)
	for i := range draws {
		draws[i] = rand.New(rand.NewPCG(uint64(i)+1, 0x9e3779b97f4a7c15))
	}
	return func(ctx context.Context, w worker, index int) (time.Duration, error) {
		id := b.loaded[draws[index].IntN(len(b.loaded))]
		opCtx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
		defer cancel()
		began := time.Now()
		if err := w.get(opCtx, id); err != nil {
			return 0, err
		}
		return time.Since(began), nil
	}
}

// runBenchmark runs the four phases in order against workers the engine
// opened, and returns their results. The preload between the insert and get
// phases is not measured.
func runBenchmark(ctx context.Context, cfg *config, workers []worker, progress func(string)) ([]phaseResult, error) {
	payload, err := randomPayload(cfg.Payload)
	if err != nil {
		return nil, err
	}
	b := &benchmark{cfg: cfg, workers: workers, ids: newIDGenerator(time.Now, crand.Reader), payload: payload, ledger: cfg.ledger}
	var results []phaseResult
	measure := func(name string, active []worker, ops int, do opFunc) error {
		progress(fmt.Sprintf("phase %s: %d ops over %d connection(s)", name, ops, len(active)))
		latencies, elapsed, err := runPhase(ctx, active, ops, do)
		if err != nil {
			return fmt.Errorf("phase %s: %w", name, err)
		}
		results = append(results, phaseResult{Name: name, Workers: len(active), latencySummary: summarize(latencies, elapsed)})
		return nil
	}
	var mu sync.Mutex
	for _, name := range allPhases {
		if !cfg.runs(name) {
			continue
		}
		var err error
		switch name {
		case "insert_serial":
			err = measure(name, workers[:1], cfg.Ops, b.insertOp(false, &mu))
		case "insert_parallel":
			err = measure(name, workers, cfg.Ops, b.insertOp(false, &mu))
		default:
			if len(b.loaded) == 0 {
				progress(fmt.Sprintf("preloading %d documents (not measured)", cfg.Preload))
				if _, _, perr := runPhase(ctx, workers[:min(len(workers), cfg.Preload)], cfg.Preload, b.insertOp(true, &mu)); perr != nil {
					return nil, fmt.Errorf("preload: %w", perr)
				}
			}
			active := workers
			if name == "get_serial" {
				active = workers[:1]
			}
			err = measure(name, active, cfg.Ops, b.getOp(len(workers)))
		}
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}
