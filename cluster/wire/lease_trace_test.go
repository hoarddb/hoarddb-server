package wire

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
)

const unit = time.Millisecond

func sumPhases(g gapRecord) time.Duration {
	var total time.Duration
	for _, d := range g.Phase {
		total += d
	}
	return total
}

// TestTheGapRecordAddsUp is spec lease-renewer-cadence O4, adapted for S2: since the renewer owns
// the gap, it is the round, the paced wait and the loop's own work before the next tick. The phases
// plus the unaccounted remainder are the gap between the two ticks, and the work after the wait is
// pre-tick work, not unaccounted time.
func TestTheGapRecordAddsUp(t *testing.T) {
	var trace gapTrace
	trace.tick(1000 * unit) // tick k: the gap that ends at the next tick starts here
	trace.phase(phRound, 1040*unit)
	trace.slept(60*unit, false, 1100*unit) // the rest of the heartbeat after a 40 ms round
	trace.phase(phWake, 1103*unit)         // a slow config read before the next tick
	gap, ok := trace.tick(1105 * unit)
	if !ok {
		t.Fatal("the second tick closed no gap")
	}
	if gap.Gap != 105*unit {
		t.Fatalf("gap = %v, want 105ms", gap.Gap)
	}
	if got := sumPhases(gap) + gap.Unaccounted; got != gap.Gap {
		t.Fatalf("phases %v + unaccounted %v = %v, want the gap %v", sumPhases(gap), gap.Unaccounted, got, gap.Gap)
	}
	if gap.PreTick() != 3*unit {
		t.Errorf("pre-tick = %v, want the 3ms between the wait and the last booked reading", gap.PreTick())
	}
	if gap.Unaccounted != 2*unit {
		t.Errorf("unaccounted = %v, want only the 2ms between the last phase and the tick", gap.Unaccounted)
	}
	if gap.WakeCut || gap.SleepAsked != 60*unit || gap.Phase[phSleep] != 60*unit {
		t.Errorf("sleep record = asked %v taken %v cut %v, want 60ms asked and taken, not cut", gap.SleepAsked, gap.Phase[phSleep], gap.WakeCut)
	}
	if name, took := gap.Longest(); name != phRound || took != 40*unit {
		t.Errorf("longest phase = %v %v, want round 40ms (the sleep apart)", name, took)
	}
	if _, ok := trace.tick(1400 * unit); !ok {
		t.Error("a gap that follows a closed gap is not recorded")
	}
}

// scriptedClock is a boot and wall clock the test moves by hand.
type scriptedClock struct {
	mu   sync.Mutex
	boot time.Duration
	wall time.Time
}

func (c *scriptedClock) BootNow() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.boot
}

func (c *scriptedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

func (c *scriptedClock) set(boot time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall = c.wall.Add(boot - c.boot)
	c.boot = boot
}

// TestTheWatchdogRecordsAFreeze is O7: a 120 ms jump between two wakes is a freeze with its wall
// start, and an ordinary late wake is not. The timer channel carries nominal times throughout,
// so a watchdog that trusted its timer would see nothing.
func TestTheWatchdogRecordsAFreeze(t *testing.T) {
	clock := &scriptedClock{boot: 5 * time.Second, wall: time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)}
	watch := newFreezeWatch(clock, clock.now)
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { watch.run(ctx, ticks); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	nominal := time.Unix(0, 0)
	wakes := 0
	wake := func(boot time.Duration) {
		clock.set(boot)
		nominal = nominal.Add(freezeInterval)
		ticks <- nominal
		wakes++
		for watch.wakeCount() != wakes { // the loop has read the clock for this wake before the next move
			runtime.Gosched()
		}
	}
	wake(5010 * unit)
	wake(5020 * unit)
	wake(5075 * unit) // 45 ms late: below the 50 ms line
	wake(5195 * unit) // 110 ms late: a 120 ms gap between wakes
	wake(5205 * unit)
	events := watch.events()
	if len(events) != 1 {
		t.Fatalf("freezes = %+v, want exactly one", events)
	}
	if got, want := events[0].Length, 110*unit; got != want {
		t.Errorf("length = %v, want %v", got, want)
	}
	if want := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC).Add(195*unit - 110*unit + 0); !events[0].WallStart.Equal(want) {
		t.Errorf("wall start = %v, want %v", events[0].WallStart, want)
	}
	if got := watch.longestOverlap(5000*unit, 5300*unit); got != 110*unit {
		t.Errorf("longest freeze in the window = %v, want 110ms", got)
	}
	if got := watch.longestOverlap(5300*unit, 5400*unit); got != 0 {
		t.Errorf("a window after the freeze reports %v", got)
	}
}

// TestTheLapseLineIsCappedAtTenASecond: past ten lines in one second the rest are dropped, and the
// next line that is written says how many.
func TestTheLapseLineIsCappedAtTenASecond(t *testing.T) {
	var limit lapseLimiter
	written := 0
	for i := range 15 {
		if ok, _ := limit.allow(time.Duration(i) * 10 * unit); ok {
			written++
		}
	}
	if written != 10 {
		t.Fatalf("lines written in one second = %d, want 10", written)
	}
	ok, dropped := limit.allow(1500 * unit)
	if !ok || dropped != 5 {
		t.Fatalf("first line of the next second: written %v, dropped %d, want written with 5 dropped", ok, dropped)
	}
}

// TestTheLapseLineTemplate fixes the fixed template of spec section 6 item 6.
func TestTheLapseLineTemplate(t *testing.T) {
	line := lapseLine{
		Node: "n2", Role: "follower", Cause: "tick_gap", Began: time.Date(2026, 11, 17, 12, 0, 1, 250_000_000, time.UTC), Lasted: 140 * unit,
		Gap: 216 * unit, TickLate: true, Round: 116 * unit, Grantor: 3 * unit, TurnWait: 90 * unit, TurnHolder: "probe", PhaseMax: "round", PhaseMaxTook: 116 * unit,
		PreTick: 4 * unit, Sleep: 100 * unit, WakeCut: false, Unaccounted: 1 * unit, Freeze: 0, GCPauseMax: 2 * unit, SchedLatMax: 5 * unit,
		VerifyErr: "", Follower: true,
	}
	want := `lease lapse node=n2 role=follower cause=tick_gap began=2026-11-17T12:00:01.250Z lasted_ms=140 ` +
		`gap_ms=216 tick_late=true round_ms=116 grantor_ms=3 turn_wait_ms=90 turn_holder=probe phase_max=round:116 ` +
		`pre_tick_ms=4 sleep_ms=100 wake_cut=false unaccounted_ms=1 freeze_ms=0 gc_pause_max_ms=2 ` +
		`sched_lat_max_ms=5`
	if got := line.String(); got != want {
		t.Fatalf("line =\n%s\nwant\n%s", got, want)
	}
	leader := lapseLine{Node: "n1", Role: "leader", Cause: "quorum_failed", Began: line.Began, VerifyErr: "no quorum: 1 of 3", PhaseMax: "round"}
	if got := leader.String(); !strings.Contains(got, `verify_err="no quorum: 1 of 3"`) || !strings.Contains(got, "grantor_ms=0") || strings.Contains(got, "turn_wait_ms") {
		t.Fatalf("a leader line = %s, want verify_err and its own round as grantor_ms, and no turn fields", got)
	}
	if got := (lapseLine{Node: "n1", Role: "leader", Cause: "tick_gap", Began: line.Began, Dropped: 5}).String(); !strings.HasSuffix(got, " dropped=5") {
		t.Fatalf("a line after drops = %s, want the dropped count at the end", got)
	}
}

// TestTheRuntimeDeltasSeeAGCPause: a forced collection shows in the GC pause delta and a quiet
// interval shows none.
func TestTheRuntimeDeltasSeeAGCPause(t *testing.T) {
	deltas := newRuntimeDeltas()
	deltas.read()
	runtime.GC()
	sched, gcPause := deltas.read()
	if gcPause <= 0 {
		t.Errorf("gc pause max after runtime.GC = %v, want more than 0", gcPause)
	}
	if sched < 0 {
		t.Errorf("scheduler latency max = %v", sched)
	}
}

// TestTheMeasurementCostsAreSmall states what the measurement costs a node. It asserts only that
// the per-step costs are small; the numbers go to the log for the build report.
func TestTheMeasurementCostsAreSmall(t *testing.T) {
	deltas := newRuntimeDeltas()
	deltas.read()
	allocs := testing.AllocsPerRun(200, func() { deltas.read() })
	const rounds = 2000
	began := time.Now()
	for range rounds {
		deltas.read()
	}
	perRead := time.Since(began) / rounds
	clock := bootclock.New()
	began = time.Now()
	for range rounds {
		clock.BootNow()
	}
	perClock := time.Since(began) / rounds
	var renewing renewer
	size := func(value any) uintptr { return reflect.TypeOf(value).Size() }
	retained := size(renewing.trace) + size(renewing.lastGap) + size(renewing.lapse) + size(renewing.limit) +
		size(freezeWatch{}) + size(runtimeDeltas{}) + uintptr(8*(len(deltas.prev[0])+len(deltas.prev[1])))
	// The samples keep the histograms the runtime filled in, which the prev copies do not include.
	for i := range deltas.samples {
		hist := deltas.samples[i].Value.Float64Histogram()
		retained += uintptr(8 * (len(hist.Counts) + len(hist.Buckets)))
	}
	t.Logf("retained per node: %d bytes (sched %d buckets, gc %d buckets, each held twice: the sample's histogram and the copy); runtime/metrics read: %v and %.0f allocs; BootNow: %v",
		retained, len(deltas.prev[0]), len(deltas.prev[1]), perRead, allocs, perClock)
	if perRead > time.Millisecond {
		t.Errorf("one runtime/metrics read takes %v, want well under a millisecond: it runs once per renewer round", perRead)
	}
}

// TestAGapRecordKeepsItsWindow: the window a gap covers travels with it, so the freeze inside it can
// be read after the watchdog has had time to record it.
func TestAGapRecordKeepsItsWindow(t *testing.T) {
	var trace gapTrace
	trace.tick(1000 * unit)
	gap, _ := trace.tick(1300 * unit)
	if gap.From != 1000*unit || gap.To != 1300*unit {
		t.Fatalf("window = %v to %v, want 1s to 1.3s", gap.From, gap.To)
	}
}

// TestAFreezeRecordedAfterTheTickStillCountsInItsGap: after a stall the supervisor can reach its tick
// before the watchdog has recorded the freeze. Reading the freeze when the line is written finds it.
func TestAFreezeRecordedAfterTheTickStillCountsInItsGap(t *testing.T) {
	clock := &scriptedClock{boot: 1010 * unit, wall: time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)}
	watch := newFreezeWatch(clock, clock.now)
	var trace gapTrace
	trace.tick(1000 * unit)
	gap, _ := trace.tick(1300 * unit) // the tick wins the race: the watchdog has not woken yet
	if got := watch.longestOverlap(gap.From, gap.To); got != 0 {
		t.Fatalf("freeze before the watchdog woke = %v, want 0", got)
	}
	clock.set(1300 * unit) // the watchdog's first wake after a 290 ms stall
	watch.observe()
	if got := watch.longestOverlap(gap.From, gap.To); got != 280*unit {
		t.Fatalf("freeze read when the line is written = %v, want 280ms", got)
	}
}

// TestAFreezeThatStraddlesTheWindowStartCountsByOverlap.
func TestAFreezeThatStraddlesTheWindowStartCountsByOverlap(t *testing.T) {
	clock := &scriptedClock{boot: 5 * time.Second, wall: time.Now()}
	watch := newFreezeWatch(clock, clock.now)
	watch.ring[0] = freezeEvent{BootStart: 950 * unit, Length: 100 * unit} // 950 to 1050 ms
	watch.count = 1
	if got := watch.longestOverlap(1000*unit, 1300*unit); got != 50*unit {
		t.Fatalf("overlap = %v, want 50ms", got)
	}
	if got := watch.longestOverlap(1100*unit, 1300*unit); got != 0 {
		t.Fatalf("a window after the freeze overlaps %v", got)
	}
}

// TestALapseCausedByTheRoundCarriesThatRound: a lapse that the round itself causes (the leader's
// failed VerifyLeader, a grant too slow or ahead) is described by that round, not the step before.
func TestALapseCausedByTheRoundCarriesThatRound(t *testing.T) {
	var last gapRecord
	last.Round = roundSplit{Grantor: 1 * unit, TurnHolder: "none"}
	last.Phase[phRound] = 1 * unit
	cur := roundSplit{Leader: true, Grantor: 240 * unit}
	after := lapseGap(last, cur, 241*unit, true)
	if after.Round.Grantor != 240*unit || !after.Round.Leader || after.Phase[phRound] != 241*unit {
		t.Fatalf("round-caused lapse describes %+v / %v, want the failing round", after.Round, after.Phase[phRound])
	}
	before := lapseGap(last, cur, 241*unit, false)
	if before.Round.Grantor != 1*unit || before.Phase[phRound] != 1*unit {
		t.Fatalf("a tick-caused lapse describes %+v, want the round inside the gap", before.Round)
	}
}
