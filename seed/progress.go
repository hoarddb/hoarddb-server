package seed

import (
	"context"
	"fmt"
	"io"
	"iter"
	"sync"
	"sync/atomic"
	"time"
)

const queueDepth = 256

// Pool feeds items to workers and returns the first error. The first failure cancels the
// context the workers run under and the producer stops, so no goroutine outlives the call.
func Pool[T any](ctx context.Context, workers int, queued iter.Seq[T], do func(context.Context, T) error, progress *Progress) error {
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

// Progress prints the running write count to out about every two seconds.
type Progress struct {
	// Rate adds the writes-per-second figure to the progress lines. An engine-test feature:
	// 'hoarddb seed' leaves it off and prints counts only.
	Rate  bool
	out   io.Writer
	total atomic.Int64
	start time.Time
	last  atomic.Int64
}

// NewProgress returns a Progress that prints to out; a nil out discards.
func NewProgress(out io.Writer) *Progress {
	if out == nil {
		out = io.Discard
	}
	return &Progress{out: out, start: time.Now()}
}

// Phase announces the next stage of a seed.
func (p *Progress) Phase(name string) { say(p.out, "seeding %s\n", name) }

// Add counts n writes and, about every two seconds, prints the rate.
func (p *Progress) Add(n int64) {
	total := p.total.Add(n)
	now := time.Since(p.start).Milliseconds()
	last := p.last.Load()
	if now-last >= 2000 && p.last.CompareAndSwap(last, now) {
		p.say(total, float64(now)/1000)
	}
}

// Total is the number of writes counted so far.
func (p *Progress) Total() int64 { return p.total.Load() }

func (p *Progress) flush() {
	total := p.total.Load()
	seconds := time.Since(p.start).Seconds()
	p.say(total, seconds)
}

// say prints one progress line.
func (p *Progress) say(total int64, seconds float64) {
	if p.Rate {
		say(p.out, "  %d writes, %.0f writes/s\n", total, float64(total)/max(seconds, 0.001))
		return
	}
	say(p.out, "  %d writes\n", total)
}

// say writes a progress line. A failed write to a terminal or a closed pipe is not worth stopping a seed for.
func say(out io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(out, format, args...) }
