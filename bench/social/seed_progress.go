package main

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

const (
	// bucketProgressEvery and opProgressEvery are how many buckets created or
	// operations seeded pass between two progress lines.
	bucketProgressEvery = 1000
	opProgressEvery     = 10000
)

// seedProgress prints `seed: <what> <done>[/<total>] <elapsed>s` every `every`
// items, so a long seed shows it is alive. It writes to stderr because stdout
// carries only the JSON record the harness reads.
type seedProgress struct {
	out   io.Writer
	what  string
	total int
	every int64
	start time.Time
	done  atomic.Int64
}

// newProgress starts a progress counter; total 0 means the total is not known.
func (h *hoardEngine) newProgress(what string, total, every int) *seedProgress {
	out := h.progressOut
	if out == nil {
		out = os.Stderr
	}
	return &seedProgress{out: out, what: what, total: total, every: int64(every), start: time.Now()}
}

func (p *seedProgress) tick() {
	done := p.done.Add(1)
	if done%p.every != 0 {
		return
	}
	of := ""
	if p.total > 0 {
		of = fmt.Sprintf("/%d", p.total)
	}
	// A progress line that cannot be written must not fail the seed.
	_, _ = fmt.Fprintf(p.out, "seed: %s %d%s %.1fs\n", p.what, done, of, time.Since(p.start).Seconds())
}

// counted wraps write so each successful item counts toward the next line.
func counted[T any](progress *seedProgress, write func(caller wireCaller, item T) error) func(caller wireCaller, item T) error {
	return func(caller wireCaller, item T) error {
		if err := write(caller, item); err != nil {
			return err
		}
		progress.tick()
		return nil
	}
}
