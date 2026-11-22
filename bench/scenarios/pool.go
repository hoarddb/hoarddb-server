package main

import (
	"context"
	"io"
	"iter"
	"sync"
	"sync/atomic"
	"time"
)

const queueDepth = 256

// pool feeds items to workers and returns the first error. The first failure cancels the
// context the workers run under and the producer stops, so no goroutine outlives the call.
func pool[T any](ctx context.Context, workers int, queued iter.Seq[T], do func(context.Context, T) error, progress *progress) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan T, queueDepth)
	var group sync.WaitGroup
	var first atomic.Pointer[error]
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for task := range queue {
				if first.Load() != nil {
					continue // drain, so the producer never blocks
				}
				if err := do(ctx, task); err != nil {
					first.CompareAndSwap(nil, &err)
					cancel()
				}
			}
		}()
	}
produce:
	for task := range queued {
		select {
		case queue <- task:
		case <-ctx.Done():
			break produce
		}
	}
	close(queue)
	group.Wait()
	progress.flush()
	if failure := first.Load(); failure != nil {
		return *failure
	}
	return ctx.Err()
}

// progress prints the running write count and rate to out about every two seconds.
type progress struct {
	out   io.Writer
	total atomic.Int64
	start time.Time
	last  atomic.Int64
}

func newProgress(out io.Writer) *progress {
	if out == nil {
		out = io.Discard
	}
	return &progress{out: out, start: time.Now()}
}

// phase announces the next stage of a seed.
func (p *progress) phase(name string) { say(p.out, "seeding %s\n", name) }

// add counts n writes and, about every two seconds, prints the rate.
func (p *progress) add(n int64) {
	total := p.total.Add(n)
	now := time.Since(p.start).Milliseconds()
	last := p.last.Load()
	if now-last >= 2000 && p.last.CompareAndSwap(last, now) {
		say(p.out, "  %d writes, %.0f writes/s\n", total, float64(total)/(float64(now)/1000))
	}
}

func (p *progress) flush() {
	total := p.total.Load()
	seconds := time.Since(p.start).Seconds()
	say(p.out, "  %d writes so far, %.0f writes/s\n", total, float64(total)/max(seconds, 0.001))
}
