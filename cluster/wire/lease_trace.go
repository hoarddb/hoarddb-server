package wire

import (
	"context"
	"math"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
)

// This file measures why a lease lapsed; it changes no rule. The renewer books each stretch between
// two ticks against the phase that spent it, a watchdog notes time the process did not run, and
// the one log line written when a lapse ends says which of them it was (spec lease-renewer-cadence,
// section 6; since S2 the renewer, not the supervisor step, owns the gap).
//
// Cost per node: about 9.3 KB of records, histogram buffers and copies (measured by
// TestTheMeasurementCostsAreSmall), one goroutine that wakes 100 times a second, about 6 boot-clock
// reads and one runtime/metrics read per renewer round, nothing per bucket, pair or request.

// leasePhase is one stretch of the renewer's loop between two ticks.
type leasePhase uint8

// The phases: the round after a tick, the wait that paces the next round, and the loop's own work
// between that wait and the next tick.
const (
	phRound leasePhase = iota
	phSleep
	phWake
	phCount
)

var phaseNames = [phCount]string{"round", "sleep", "wake"}

func (p leasePhase) String() string { return phaseNames[p] }

// roundSplit is what the lease round after a tick measured.
type roundSplit struct {
	// Leader is true for the leader's own VerifyLeader, false for a follower's ask.
	Leader bool
	// Grantor is the grantor's VerifyLeader round trip: the one a follower's grant reports, or the
	// leader's own.
	Grantor time.Duration
	// TurnWait is the wait for the control connection's turn and TurnHolder the kind of call that
	// held it, as transport.HolderName spells it.
	TurnWait   time.Duration
	TurnHolder string
}

// gapRecord is everything between two ticks, booked on the lease clock.
type gapRecord struct {
	// Phase is the time each phase spent. They are chained, so they add up to Gap less Unaccounted.
	Phase [phCount]time.Duration
	// SleepAsked is the sleep the loop asked for; Phase[phSleep] is what it took.
	SleepAsked time.Duration
	// WakeCut is true when something ended the sleep early. The renewer's wait is never cut, so it
	// stays false since S2; the line keeps the field.
	WakeCut bool
	// Gap is the time between the two ticks; Unaccounted is what no phase owns.
	Gap, Unaccounted time.Duration
	// From and To are the two ticks on the lease clock, so the freeze inside the gap can be read
	// when the line is written, after the watchdog has had time to record it.
	From, To time.Duration
	// Round is the round that began this gap.
	Round roundSplit
	// SchedLat and GCPause are the longest scheduler latency and GC pause seen inside the gap. Both
	// are bucket upper bounds, up to twice the true figure, and SchedLat counts every goroutine in
	// the process, not the supervisor alone. The watchdog's freeze is read when the line is written.
	SchedLat, GCPause time.Duration
}

// PreTick is the loop's work between the wait and the tick that ended the gap.
func (g gapRecord) PreTick() time.Duration { return g.Phase[phWake] }

// Longest is the phase that spent the most, the sleep apart.
func (g gapRecord) Longest() (leasePhase, time.Duration) {
	longest, took := phRound, time.Duration(-1)
	for p := leasePhase(0); p < phCount; p++ {
		if p != phSleep && g.Phase[p] > took {
			longest, took = p, g.Phase[p]
		}
	}
	return longest, took
}

// gapTrace books renewer time. Every reading it is given chains onto the one before, so a stretch
// is charged to the phase that ended it. It is used by the renewer goroutine alone.
type gapTrace struct {
	cur      gapRecord
	mark     time.Duration
	lastTick time.Duration
}

// phase charges the time since the previous reading to p.
func (g *gapTrace) phase(p leasePhase, now time.Duration) {
	g.cur.Phase[p] += now - g.mark
	g.mark = now
}

// slept books the sleep that ended at now.
func (g *gapTrace) slept(asked time.Duration, cut bool, now time.Duration) {
	g.cur.SleepAsked, g.cur.WakeCut = asked, cut
	g.phase(phSleep, now)
}

// tick closes the gap that ends at the tick read at at and starts the next one. The first tick has
// no gap to close.
func (g *gapTrace) tick(at time.Duration) (gapRecord, bool) {
	done := g.cur
	had := g.lastTick != 0
	done.Gap = at - g.lastTick
	done.From, done.To = g.lastTick, at
	var spent time.Duration
	for _, took := range done.Phase {
		spent += took
	}
	done.Unaccounted = done.Gap - spent
	g.cur = gapRecord{}
	g.mark, g.lastTick = at, at
	return done, had
}

// freezeInterval is the watchdog's timer; a wake more than freezeLate after it is a freeze.
const (
	freezeInterval = 10 * time.Millisecond
	freezeLate     = 50 * time.Millisecond
	freezeRing     = 16
)

// freezeEvent is time the process or the watchdog goroutine did not run.
type freezeEvent struct {
	// WallStart is the wall time the watchdog should have woken; BootStart is the same on the lease clock.
	WallStart time.Time
	BootStart time.Duration
	Length    time.Duration
}

// freezeWatch is the freeze watchdog: it reads the boot and wall clocks at each wake of a 10 ms
// timer and keeps the last 16 wakes that were more than 50 ms late. It trusts the clock, not the
// timer, which stops with the process.
type freezeWatch struct {
	clock bootclock.Clock
	wall  func() time.Time

	mu    sync.Mutex
	last  time.Duration
	ring  [freezeRing]freezeEvent
	count int
	wakes int
}

func newFreezeWatch(clock bootclock.Clock, wall func() time.Time) *freezeWatch {
	return &freezeWatch{clock: clock, wall: wall, last: clock.BootNow()}
}

// run observes each wake until ctx ends. ticks is the timer's channel.
func (w *freezeWatch) run(ctx context.Context, ticks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			w.observe()
		}
	}
}

// runTimer is run on a real timer.
func (w *freezeWatch) runTimer(ctx context.Context) {
	// The watch was built before the node started serving; time before this goroutine began is not a freeze.
	w.mu.Lock()
	w.last = w.clock.BootNow()
	w.mu.Unlock()
	ticker := time.NewTicker(freezeInterval)
	defer ticker.Stop()
	w.run(ctx, ticker.C)
}

func (w *freezeWatch) observe() {
	now, wall := w.clock.BootNow(), w.wall()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.wakes++
	late := now - w.last - freezeInterval
	if late > freezeLate {
		w.ring[w.count%freezeRing] = freezeEvent{WallStart: wall.Add(-late), BootStart: now - late, Length: late}
		w.count++
	}
	w.last = now
}

// wakeCount is how many wakes the watchdog has seen.
func (w *freezeWatch) wakeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.wakes
}

// events lists the recorded freezes, oldest first.
func (w *freezeWatch) events() []freezeEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	kept := min(w.count, freezeRing)
	out := make([]freezeEvent, 0, kept)
	for i := w.count - kept; i < w.count; i++ {
		out = append(out, w.ring[i%freezeRing])
	}
	return out
}

// longestOverlap is the longest stretch of a recorded freeze that lies between from and to on the
// lease clock, so a freeze that began before the window and ran into it counts for what ran inside.
func (w *freezeWatch) longestOverlap(from, to time.Duration) time.Duration {
	var longest time.Duration
	for _, event := range w.events() {
		longest = max(longest, min(to, event.BootStart+event.Length)-max(from, event.BootStart))
	}
	return longest
}

// lapseGap is the gap a lapse is described by. A lapse the tick caused is described by the gap that
// ended at it. A lapse the round caused (a failed VerifyLeader, a grant too slow or ahead) is
// described by that round, which has not yet been closed into any gap: its split and the time it has
// taken so far replace the previous step's.
func lapseGap(last gapRecord, round roundSplit, roundSoFar time.Duration, afterRound bool) gapRecord {
	if afterRound {
		last.Round = round
		last.Phase[phRound] = roundSoFar
	}
	return last
}

// runtimeDeltas reads the scheduler latency and GC pause histograms and reports, for each read, the
// largest bucket that grew since the one before.
type runtimeDeltas struct {
	samples [2]metrics.Sample
	prev    [2][]uint64
}

func newRuntimeDeltas() *runtimeDeltas {
	d := &runtimeDeltas{}
	d.samples[0].Name = "/sched/latencies:seconds"
	d.samples[1].Name = "/sched/pauses/total/gc:seconds"
	return d
}

// read returns the longest scheduler latency and the longest GC pause that were recorded since the
// previous read; the first read only sets the baseline. A metric this runtime lacks reports 0.
func (d *runtimeDeltas) read() (sched, gcPause time.Duration) {
	metrics.Read(d.samples[:])
	out := [2]time.Duration{}
	for i := range d.samples {
		if d.samples[i].Value.Kind() != metrics.KindFloat64Histogram {
			continue
		}
		hist := d.samples[i].Value.Float64Histogram()
		if len(d.prev[i]) == len(hist.Counts) {
			out[i] = grownBucket(hist, d.prev[i])
		}
		d.prev[i] = append(d.prev[i][:0], hist.Counts...)
	}
	return out[0], out[1]
}

// grownBucket is the upper bound of the highest bucket whose count grew past prev, or its lower
// bound when the bucket is open at the top.
func grownBucket(hist *metrics.Float64Histogram, prev []uint64) time.Duration {
	for i := len(hist.Counts) - 1; i >= 0; i-- {
		if hist.Counts[i] == prev[i] {
			continue
		}
		bound := hist.Buckets[i+1]
		if math.IsInf(bound, 1) {
			bound = hist.Buckets[i]
		}
		return time.Duration(bound * float64(time.Second))
	}
	return 0
}

// lapseLine holds the fields of the one line written when a lapse ends.
type lapseLine struct {
	Node, Role, Cause string
	Follower          bool
	Began             time.Time
	Lasted, Gap       time.Duration
	// TickLate is true when the tick that noticed the lapse came more than three heartbeats (the
	// tick bound) after the one before, whatever the cause says: an expiry after a long stall is
	// also a late tick.
	TickLate       bool
	Round, Grantor time.Duration
	TurnWait       time.Duration
	TurnHolder     string
	PhaseMax       string
	PhaseMaxTook   time.Duration
	PreTick, Sleep time.Duration
	WakeCut        bool
	Unaccounted    time.Duration
	Freeze         time.Duration
	GCPauseMax     time.Duration
	SchedLatMax    time.Duration
	VerifyErr      string
	// Dropped is how many lines the cap dropped since the last one written.
	Dropped int
}

func ms(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

// String is the fixed template of the lapse line.
func (l lapseLine) String() string {
	var b strings.Builder
	b.WriteString("lease lapse node=" + l.Node + " role=" + l.Role + " cause=" + l.Cause)
	b.WriteString(" began=" + l.Began.UTC().Format("2006-01-02T15:04:05.000Z07:00") + " lasted_ms=" + ms(l.Lasted))
	b.WriteString(" gap_ms=" + ms(l.Gap) + " tick_late=" + strconv.FormatBool(l.TickLate) + " round_ms=" + ms(l.Round) + " grantor_ms=" + ms(l.Grantor))
	if l.Follower {
		b.WriteString(" turn_wait_ms=" + ms(l.TurnWait) + " turn_holder=" + l.TurnHolder)
	}
	b.WriteString(" phase_max=" + l.PhaseMax + ":" + ms(l.PhaseMaxTook))
	b.WriteString(" pre_tick_ms=" + ms(l.PreTick) + " sleep_ms=" + ms(l.Sleep) + " wake_cut=" + strconv.FormatBool(l.WakeCut))
	b.WriteString(" unaccounted_ms=" + ms(l.Unaccounted) + " freeze_ms=" + ms(l.Freeze))
	b.WriteString(" gc_pause_max_ms=" + ms(l.GCPauseMax) + " sched_lat_max_ms=" + ms(l.SchedLatMax))
	if l.VerifyErr != "" {
		b.WriteString(" verify_err=" + strconv.Quote(l.VerifyErr))
	}
	if l.Dropped > 0 {
		b.WriteString(" dropped=" + strconv.Itoa(l.Dropped))
	}
	return b.String()
}

// lapseLimiter caps the lapse line at ten a second per node and counts what it drops.
type lapseLimiter struct {
	window  time.Duration
	written int
	dropped int
}

const lapseLinesPerSecond = 10

// allow says whether a line may be written at now, and how many were dropped since the last one that was.
func (l *lapseLimiter) allow(now time.Duration) (ok bool, dropped int) {
	if now-l.window >= time.Second {
		l.window, l.written = now, 0
	}
	if l.written >= lapseLinesPerSecond {
		l.dropped++
		return false, 0
	}
	l.written++
	dropped, l.dropped = l.dropped, 0
	return true, dropped
}
