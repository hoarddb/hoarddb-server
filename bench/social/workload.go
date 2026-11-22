package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// userState is the writer-side bookkeeping the verifier checks reads against.
// Fields written by workers are atomics; baseTotal and seqBase are set before
// the workers start and never change.
type userState struct {
	baseTotal int64
	seqBase   int64
	issued    atomic.Int64 // writes started
	acked     atomic.Int64 // writes the server confirmed
	inflight  atomic.Int64
	overlaps  atomic.Int64 // writes that began while another was in flight
	// tainted is set when a write failed with an unknown outcome, after
	// which only count bounds (not exact counts) can be asserted.
	tainted atomic.Bool
}

// forEachRank runs fn for every user rank across conc workers, each with its
// own resource from open, and returns the first error. It is the shared shape
// of setup, seeding and attach.
func forEachRank[S any](cfg *config, label string, open func() (S, func(), error), fn func(resource S, rank int) error) error {
	ranks := make(chan int, 1024)
	var group sync.WaitGroup
	var firstErr atomic.Pointer[error]
	for w := 0; w < cfg.Conc; w++ {
		group.Add(1)
		go func() {
			defer group.Done()
			resource, release, err := open()
			if err != nil {
				firstErr.CompareAndSwap(nil, &err)
				for range ranks { // drain so the producer never blocks
				}
				return
			}
			defer release()
			for rank := range ranks {
				if firstErr.Load() != nil {
					continue
				}
				if err := fn(resource, rank); err != nil {
					err = fmt.Errorf("%s user %d: %w", label, rank, err)
					firstErr.CompareAndSwap(nil, &err)
				}
			}
		}()
	}
	for rank := 0; rank < cfg.Users; rank++ {
		ranks <- rank
	}
	close(ranks)
	group.Wait()
	if err := firstErr.Load(); err != nil {
		return *err
	}
	return nil
}

// openSession adapts an engine to forEachRank.
func openSession(eng engine) func() (engineSession, func(), error) {
	return func() (engineSession, func(), error) {
		session, err := eng.newSession()
		if err != nil {
			return nil, nil, err
		}
		return session, session.close, nil
	}
}

// seedData writes one profile per user and cfg.SeedPosts posts per user,
// filling states[rank] to match. The generator is seeded per user, so every
// engine receives byte-identical profiles and posts.
func seedData(ctx context.Context, cfg *config, eng engine, states []userState) error {
	return forEachRank(cfg, "seeding", openSession(eng), func(session engineSession, rank int) error {
		gen := newPostGenerator(rand.New(rand.NewPCG(cfg.RandSeed, 1<<32+uint64(rank))), cfg.PostMin, cfg.PostMax)
		id := userID(rank)
		if err := session.putProfile(ctx, id, gen.profile(id)); err != nil {
			return err
		}
		for seq := int64(0); seq < int64(cfg.SeedPosts); seq++ {
			if err := session.writePost(ctx, id, gen.post(id, seq)); err != nil {
				return err
			}
		}
		states[rank].baseTotal = int64(cfg.SeedPosts)
		states[rank].seqBase = int64(cfg.SeedPosts)
		return nil
	})
}

// attachData reads each existing user's post count and next sequence number so
// the verifier can continue from a dataset an earlier run left behind.
func attachData(ctx context.Context, cfg *config, eng engine, states []userState) error {
	return forEachRank(cfg, "attaching to", openSession(eng), func(session engineSession, rank int) error {
		total, nextSeq, err := session.attachState(ctx, userID(rank))
		if err != nil {
			return err
		}
		states[rank].baseTotal = total
		states[rank].seqBase = nextSeq
		return nil
	})
}

// workerResult is one worker's measurements, merged after the run.
type workerResult struct {
	readLatencies, writeLatencies *histogram
	errors                        map[string]int64
	verified                      int64
	failure                       error
	// recordFailure is a latency the histogram could not hold; it stops the run
	// because the report would otherwise silently lack that sample.
	recordFailure error
}

// workerRun is everything one worker needs; a struct because it is passed
// around the read, write and verify helpers.
type workerRun struct {
	cfg         *config
	eng         engine
	states      []userState
	session     engineSession
	gen         *postGenerator
	rng         *rand.Rand
	pick        func() int
	measureFrom time.Time
	result      *workerResult
	clock       opClock
	// arrivals is the shared open-loop schedule; nil means a closed loop.
	arrivals *arrivalSchedule
}

func runWorkers(ctx context.Context, cfg *config, eng engine, states []userState, measureFrom, stopAt time.Time) []*workerResult {
	results := make([]*workerResult, cfg.Conc)
	var group sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var arrivals *arrivalSchedule
	if rate := cfg.offeredRate(); rate > 0 {
		arrivals = newArrivalSchedule(time.Now(), rate, cfg.RandSeed)
	}
	for w := range results {
		results[w] = &workerResult{errors: map[string]int64{}, readLatencies: newHistogram(), writeLatencies: newHistogram()}
		group.Add(1)
		go func(w int) {
			defer group.Done()
			rng := rand.New(rand.NewPCG(cfg.RandSeed, uint64(w)+1))
			run := &workerRun{
				cfg: cfg, eng: eng, states: states, rng: rng, measureFrom: measureFrom, result: results[w], arrivals: arrivals,
				gen:  newPostGenerator(rng, cfg.PostMin, cfg.PostMax),
				pick: newUserPicker(rng, cfg.Users, cfg.ZipfS),
			}
			if run.loop(ctx, stopAt) != nil {
				cancel()
			}
		}(w)
	}
	group.Wait()
	return results
}

// loop runs operations until stopAt, ctx is cancelled, or verification fails.
// It returns the verification failure, which is also recorded in the result.
func (w *workerRun) loop(ctx context.Context, stopAt time.Time) error {
	defer func() {
		if w.session != nil {
			w.session.close()
		}
	}()
	for ctx.Err() == nil && time.Now().Before(stopAt) {
		scheduled, ok := w.awaitArrival(ctx, stopAt)
		if !ok {
			return nil
		}
		if w.session == nil {
			session, err := w.eng.newSession()
			if err != nil {
				w.result.errors["dial"]++
				time.Sleep(100 * time.Millisecond)
				continue
			}
			w.session = session
		}
		rank := w.pick()
		isRead := w.rng.IntN(100) < w.cfg.ReadPct
		started := time.Now()
		windowStart, queueDelay := started, time.Duration(0)
		if w.arrivals != nil {
			windowStart, queueDelay = scheduled, max(started.Sub(scheduled), 0)
		}
		w.clock = opClock{start: started}
		var err error
		var verifyErr error
		if isRead {
			err, verifyErr = w.read(ctx, rank)
		} else {
			err = w.write(ctx, rank)
		}
		elapsed := w.clock.elapsed() + queueDelay
		if err != nil {
			if ctx.Err() != nil {
				return nil // another worker failed verification; this error is its echo
			}
			w.result.errors[errorKind(err)]++
			if w.eng.desynced(err) {
				w.session.close()
				w.session = nil
			}
		} else if !windowStart.Before(w.measureFrom) {
			latencies := w.result.writeLatencies
			if isRead {
				latencies = w.result.readLatencies
			}
			if err := latencies.record(elapsed); err != nil {
				w.result.recordFailure = err
				return err
			}
		}
		if verifyErr != nil {
			w.result.failure = verifyErr
			return verifyErr
		}
	}
	return nil
}

// awaitArrival returns the moment the next operation is charged from. A closed
// loop has no schedule and returns the zero time. An open loop claims the next scheduled arrival and
// waits for it, but never for one at or after stopAt; false means stop.
func (w *workerRun) awaitArrival(ctx context.Context, stopAt time.Time) (time.Time, bool) {
	if w.arrivals == nil {
		return time.Time{}, true
	}
	scheduled := w.arrivals.next()
	if !scheduled.Before(stopAt) || waitUntil(ctx, scheduled) != nil {
		return time.Time{}, false
	}
	return scheduled, true
}

// read fetches the newest posts of a timeline and the profile. The first
// error is a transport or server failure; the second is a wrong answer, which
// is checked after the operation's clock has stopped.
func (w *workerRun) read(ctx context.Context, rank int) (error, error) {
	id := userID(rank)
	state := &w.states[rank]
	sampled := w.rng.Float64()*100 < w.cfg.VerifyPct
	var issuedBefore, ackedBefore int64
	var quiescentBefore bool
	if sampled {
		issuedBefore = state.issued.Load()
		ackedBefore = state.acked.Load()
		quiescentBefore = state.inflight.Load() == 0 && !state.tainted.Load() && state.issued.Load() == issuedBefore
	}
	page, err := w.session.readTimeline(ctx, readRequest{Author: id, Limit: w.cfg.ReadPosts, Sampled: sampled, Clock: &w.clock})
	if err != nil {
		return err, nil
	}
	if !sampled {
		return nil, nil
	}
	issuedAfter := state.issued.Load()
	quiescent := quiescentBefore && issuedAfter == issuedBefore && state.inflight.Load() == 0 && !state.tainted.Load()
	w.result.verified++
	return nil, w.verifyRead(id, &page, readExpectation{
		Author: id, Limit: w.cfg.ReadPosts, BaseTotal: state.baseTotal, SeqBase: state.seqBase,
		AckedBefore: ackedBefore, IssuedAfter: issuedAfter, Quiescent: quiescent, Overlaps: state.overlaps.Load(), PriorOrdered: !w.cfg.Attach, Slack: int64(w.cfg.Conc),
	})
}

func (w *workerRun) verifyRead(id string, page *timelinePage, expect readExpectation) error {
	total, err := resolveTotal(expect.Limit, page.TotalLow, page.TotalHigh, len(page.Posts))
	if err != nil {
		return fmt.Errorf("timeline %s: %w", id, err)
	}
	if err := verifyTimeline(expect, total, page.Posts); err != nil {
		return err
	}
	if page.ProfileOwner != id {
		return fmt.Errorf("profile read for user %s returned the profile of %q", id, page.ProfileOwner)
	}
	return nil
}

// write inserts one new post. The sequence number is taken before the insert
// so that concurrent writers to a hot timeline never reuse one.
func (w *workerRun) write(ctx context.Context, rank int) error {
	id := userID(rank)
	state := &w.states[rank]
	if state.inflight.Add(1) > 1 {
		state.overlaps.Add(1)
	}
	defer state.inflight.Add(-1)
	seq := state.seqBase + state.issued.Add(1) - 1
	if err := w.session.writePost(ctx, id, w.gen.post(id, seq)); err != nil {
		// A timed-out insert may have applied and a refused one did not, but
		// the sequence number is spent either way, so exact counts and
		// consecutive numbering no longer hold for this timeline.
		state.tainted.Store(true)
		return err
	}
	state.acked.Add(1)
	return nil
}
