package main

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// fakeRun returns parameters for a short run on a freshly loaded fake.
func fakeRun(t *testing.T, mutate func(*fakeEngine)) (runParams, *fakeEngine) {
	t.Helper()
	fake := newFakeEngine("bat")
	if mutate != nil {
		mutate(fake)
	}
	for _, layout := range benchLayouts {
		if err := fake.load(layout, 7, 1); err != nil {
			t.Fatal(err)
		}
	}
	w, err := newWorld(7, 1)
	if err != nil {
		t.Fatal(err)
	}
	mix, _ := newMix(benchLayouts, "")
	return runParams{Engine: fake, World: w, Mix: mix, Base: "bat", VUsers: 4, Repetition: 1, Loop: "closed",
		Window: 150 * time.Millisecond, RandSeed: 3, Seed: 7, Scale: 1, Level: "L-A", EngineName: "fake"}, fake
}

func TestRunnerClosedLoopOnTheFake(t *testing.T) {
	p, _ := fakeRun(t, nil)
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Failed || !rec.AuditOK {
		t.Fatalf("failed=%v audit=%q verify=%d errors=%d samples=%v", rec.Failed, rec.AuditError, rec.VerifyFailures, rec.Errors, rec.ErrorSamples)
	}
	if rec.VUsers != 4 || rec.Loop != "closed" || rec.Overall.Requests < 100 {
		t.Errorf("record %+v", rec.Overall)
	}
	if rec.Hits == 0 || rec.Misses != 0 {
		t.Errorf("hits %d misses %d", rec.Hits, rec.Misses)
	}
	for _, layout := range benchLayouts {
		if rec.Layouts[string(layout)].Requests == 0 {
			t.Errorf("layout %s got no requests: the vusers must serve all four at once", layout)
		}
	}
	if rec.Reads == 0 || rec.Writes == 0 || rec.Reads < rec.Writes {
		t.Errorf("reads %d writes %d, want about 67.5/32.5 (profile R)", rec.Reads, rec.Writes)
	}
	if rec.Access != "uniform" || rec.ZipfS != 0 {
		t.Errorf("the record must carry the access mode: %q %v", rec.Access, rec.ZipfS)
	}
	if rec.Profile != "R" || rec.ReadShares["social"] != 0.9 || rec.ReadShares["agent"] != 0.5 {
		t.Errorf("the record must carry the profile and per-layout shares: %q %v", rec.Profile, rec.ReadShares)
	}
	if len(rec.Fingerprint) != 4 || rec.Ops["social.feed"].Count == 0 || rec.Ops["social.feed"].RoundTripsPerOp < 1 {
		t.Errorf("fingerprint %v feed %+v", rec.Fingerprint, rec.Ops["social.feed"])
	}
}

func TestRunnerOpenLoopOffersTheRate(t *testing.T) {
	p, _ := fakeRun(t, nil)
	p.Loop, p.Rate, p.Window = "open", 400, 500*time.Millisecond
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Overall.Requests; got < 100 || got > 300 {
		t.Errorf("open loop at 400/s for 0.5 s issued %d requests, want about 200", got)
	}
	if rec.Rate != 400 {
		t.Errorf("rate %v not recorded", rec.Rate)
	}
}

func TestRunnerFailsAnEngineThatReturnsEmptyPages(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.emptyPages = true })
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Failed || rec.Misses == 0 || rec.VerifyFailures < rec.Misses {
		t.Errorf("failed=%v misses=%d verify=%d", rec.Failed, rec.Misses, rec.VerifyFailures)
	}
}

func TestRunnerAuditFailsOnLostWrites(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.dropWrites = true })
	p.Window = 400 * time.Millisecond
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.AuditOK || !rec.Failed || rec.AuditError == "" {
		t.Errorf("audit ok=%v err=%q failed=%v", rec.AuditOK, rec.AuditError, rec.Failed)
	}
}

func TestRunnerAbortsWhenTheDiskGuardTrips(t *testing.T) {
	p, _ := fakeRun(t, nil)
	p.Window = 10 * time.Second
	p.Guard = &diskGuard{free: func(context.Context) (int64, error) { return 1 << 20, nil }, minFree: 3 << 30, interval: 5 * time.Millisecond}
	started := time.Now()
	rec, err := runRepetition(context.Background(), p)
	if !errors.Is(err, errDiskGuard) {
		t.Fatalf("err = %v, want errDiskGuard", err)
	}
	if rec == nil || rec.Aborted != "disk_guard" || !rec.Failed {
		t.Errorf("record %+v", rec)
	}
	if time.Since(started) > 3*time.Second {
		t.Error("the run must stop promptly")
	}
}

func TestRunnerRefusesAStaleDataset(t *testing.T) {
	p, fake := fakeRun(t, nil)
	ops, _ := (&fakeSession{fake}).Layout(model.Social, "bat_social")
	if _, err := ops.Write(context.Background(), WriteOp{Layout: model.Social, Elements: []model.Element{{Bucket: "User", Owner: "u0", Store: "Feed", Body: []model.Field{{Name: "post", Value: "x"}}}}}); err != nil {
		t.Fatal(err)
	}
	_, err := runRepetition(context.Background(), p)
	if err == nil {
		t.Fatal("a data set that is not freshly seeded must be refused")
	}
}

func TestWarmupSettlesOrGivesUp(t *testing.T) {
	p, _ := fakeRun(t, nil)
	p.Window = 50 * time.Millisecond
	p.WarmWindow, p.WarmMin, p.WarmMax, p.WarmTol = 40*time.Millisecond, 80*time.Millisecond, time.Second, 10.0
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Warm || len(rec.WarmWindows) < 2 {
		t.Errorf("a generous tolerance must settle: warm=%v windows=%v", rec.Warm, rec.WarmWindows)
	}
	p, _ = fakeRun(t, nil)
	p.Window = 50 * time.Millisecond
	p.WarmWindow, p.WarmMin, p.WarmMax, p.WarmTol = 30*time.Millisecond, 30*time.Millisecond, 150*time.Millisecond, 0
	rec, err = runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Warm {
		t.Error("a zero tolerance can never settle: warm must be false")
	}
	found := false
	for _, l := range rec.Labels {
		found = found || l == "warm=false"
	}
	if !found {
		t.Errorf("labels %v lack warm=false", rec.Labels)
	}
}

func TestSweepRunsLevelsInOrderAndReseedsBetweenRepetitions(t *testing.T) {
	var fake *fakeEngine
	var reseeds int
	var levels []int
	base := runParams{Mix: nil, Base: "bat", Loop: "closed", Window: 40 * time.Millisecond, RandSeed: 3, Seed: 7, Scale: 1, Level: "L-A", EngineName: "fake"}
	base.World, _ = newWorld(7, 1)
	base.Mix, _ = newMix(benchLayouts, "")
	load := func() {
		fake = newFakeEngine("bat")
		for _, layout := range benchLayouts {
			if err := fake.load(layout, 7, 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	load()
	cfg := sweepConfig{Base: base, VUsers: []int{1, 4, 2}, Repetitions: 2,
		Reseed: func(context.Context) error { reseeds++; load(); return nil },
		Open: func(_ context.Context, vusers int) (Engine, error) {
			levels = append(levels, vusers)
			return fake, nil
		}}
	var got []*Record
	cfg.Emit = func(rec *Record) error { got = append(got, rec); return nil }
	if err := runSweep(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if reseeds != 5 {
		t.Errorf("reseeded %d times, want 5 (before every repetition but the first)", reseeds)
	}
	want := []int{1, 1, 4, 4, 2, 2}
	for i, rec := range got {
		if rec.VUsers != want[i] || levels[i] != want[i] || rec.Repetition != 1+i%2 {
			t.Errorf("record %d: vusers %d rep %d, want %d", i, rec.VUsers, rec.Repetition, want[i])
		}
	}
	cfg.Reseed = nil
	if err := runSweep(context.Background(), cfg); err == nil {
		t.Error("more than one repetition without a reseed command must be refused: the data set would not be fresh")
	}
}

func TestRunnerSumsRetriesAndReadsSettingsTwice(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.retryWrites = true })
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Retries == 0 || rec.Retries != rec.Ops["social.post"].Retries+rec.Ops["social.comment"].Retries+rec.Ops["social.like"].Retries+rec.Ops["social.follow"].Retries+
		rec.Ops["social.group_post"].Retries+rec.Ops["social.settings"].Retries+rec.Ops["chat.message"].Retries+rec.Ops["chat.join"].Retries+rec.Ops["ledger.expense"].Retries+
		rec.Ops["ledger.invoice"].Retries+rec.Ops["agent.message"].Retries+rec.Ops["agent.task"].Retries+rec.Ops["agent.scratch"].Retries+rec.Ops["agent.memory"].Retries {
		t.Errorf("retries %d, per op %+v", rec.Retries, rec.Ops)
	}
	if !slices.Contains(rec.Labels, "retries") {
		t.Errorf("a record with retries must say so, labels %v", rec.Labels)
	}
	if rec.Settings["settings_reads"] != "1" || rec.SettingsAfter["settings_reads"] != "2" {
		t.Errorf("settings before %v after %v", rec.Settings, rec.SettingsAfter)
	}
}

// onlyOp runs the fake with a mix of one write kind, so the writes of that kind dominate.
func onlyOp(t *testing.T, op string, mutate func(*fakeEngine)) *Record {
	t.Helper()
	p, _ := fakeRun(t, mutate)
	mix, err := newMix(benchLayouts, op)
	if err != nil {
		t.Fatal(err)
	}
	p.Mix = mix
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestAuditCatchesALostReplacingWrite(t *testing.T) {
	for _, op := range []string{"agent.scratch", "social.settings"} {
		if rec := onlyOp(t, op, nil); !rec.AuditOK {
			t.Fatalf("%s on a good engine: %s", op, rec.AuditError)
		}
		rec := onlyOp(t, op, func(f *fakeEngine) { f.ignoreReplace = true })
		if rec.AuditOK || !strings.Contains(rec.AuditError, "acknowledged write") {
			t.Errorf("%s: an engine that drops replacing puts passed the audit: ok=%v %q", op, rec.AuditOK, rec.AuditError)
		}
	}
}

func TestAuditCatchesMissingCounters(t *testing.T) {
	for _, op := range []string{"ledger.expense", "social.like", "chat.message"} {
		if rec := onlyOp(t, op, nil); !rec.AuditOK {
			t.Fatalf("%s on a good engine: %s", op, rec.AuditError)
		}
		rec := onlyOp(t, op, func(f *fakeEngine) { f.skipCounters = true })
		if rec.AuditOK || !strings.Contains(rec.AuditError, "counter") {
			t.Errorf("%s: an engine that skips counters passed the audit: ok=%v %q", op, rec.AuditOK, rec.AuditError)
		}
	}
}

func TestRunnerTimesOutAHungRequestAndKeepsTheLevelMoving(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.hangReads = true })
	p.RequestTimeout = 20 * time.Millisecond
	p.Window = 150 * time.Millisecond
	done := make(chan struct{})
	var rec *Record
	var err error
	// The post-run audit reads with the run's context, so a hung engine needs a deadline here
	// or the audit's own reads would hang with it (profile R writes enough to reach the audit).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { rec, err = runRepetition(ctx, p); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("one hung request stalled the whole level")
	}
	if err != nil {
		t.Fatal(err)
	}
	if rec.Errors == 0 || !rec.Failed {
		t.Errorf("timeouts must count as errors: errors %d failed %v", rec.Errors, rec.Failed)
	}
	if float64(rec.Overall.Max) < 15 {
		t.Errorf("a timed-out request keeps its elapsed time in the latency, max %v ms", rec.Overall.Max)
	}
	if len(rec.ErrorSamples) == 0 || !strings.Contains(rec.ErrorSamples[0], "timed out") {
		t.Errorf("error samples %v", rec.ErrorSamples)
	}
}

func TestOpenLoopClampsAnOverlongLatencyInsteadOfFailing(t *testing.T) {
	u := newVuserState()
	u.record(result{Layout: "social", Kind: "social.feed"}, 90*time.Second, true)
	if u.errs != 0 || u.overall.clamped != 1 {
		t.Errorf("errors %d clamped %d", u.errs, u.overall.clamped)
	}
}

// A run at -page-depth 1 records how many reads of each paged operation started inside the
// store, and none of the operations that cannot.
func TestRunnerRecordsOffsetReads(t *testing.T) {
	p, _ := fakeRun(t, nil)
	if err := p.World.configureAccess(accessConfig{Mode: accessUniform, ZipfS: 1, PageDepth: 1}); err != nil {
		t.Fatal(err)
	}
	p.Window = 400 * time.Millisecond
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Failed {
		t.Fatalf("a run with offset reads failed: %v %v", rec.AuditError, rec.ErrorSamples)
	}
	if rec.PageDepth != 1 {
		t.Errorf("page_depth %v", rec.PageDepth)
	}
	var deep int64
	for kind, stats := range rec.Ops {
		deep += stats.OffsetReads
		if stats.OffsetReads > 0 && !offsetEligible(ReadKind(kind)) {
			t.Errorf("%s recorded offset reads but cannot take an offset", kind)
		}
	}
	if deep == 0 {
		t.Error("no offset read was recorded")
	}
}

// A short run has operations under 1,000 samples: the record marks them thin, and every
// operation, layout and the overall figures carry the latency budget shares.
func TestRunnerRecordsThinOpsAndLatencyBudget(t *testing.T) {
	p, _ := fakeRun(t, nil)
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	var thin int
	for kind, stats := range rec.Ops {
		if stats.Thin != (stats.Count < thinOpSamples) {
			t.Errorf("%s: count %d thin %v", kind, stats.Count, stats.Thin)
		}
		if stats.Thin {
			thin++
		}
		if stats.Count > 0 {
			u10, u50 := float64(stats.Under10Pct), float64(stats.Under50Pct)
			if math.IsNaN(u10) || math.IsNaN(u50) || u10 < 0 || u10 > u50 || u50 > 100 {
				t.Errorf("%s: shares under 10 and 50 ms are %v and %v", kind, u10, u50)
			}
		}
	}
	if thin == 0 {
		t.Error("a 150 ms run must have thin operations")
	}
	if !slices.Contains(rec.Labels, "thin-ops") {
		t.Errorf("labels %v", rec.Labels)
	}
	// The in-memory fake answers in microseconds: nearly every request is within both budgets.
	if float64(rec.Overall.Under10Pct) < 90 || float64(rec.Overall.Under50Pct) < float64(rec.Overall.Under10Pct) {
		t.Errorf("overall shares %v and %v", rec.Overall.Under10Pct, rec.Overall.Under50Pct)
	}
	for layout, s := range rec.Layouts {
		if s.Requests > 0 && math.IsNaN(float64(s.Under10Pct)) {
			t.Errorf("layout %s has no budget share", layout)
		}
	}
}

// Only the measured window counts: a slow warm-up request is not in the shares.
func TestBudgetSharesCountOnlyMeasuredRequests(t *testing.T) {
	u := newVuserState()
	u.record(result{Layout: "social", Kind: "social.feed"}, 80*time.Millisecond, false)
	u.record(result{Layout: "social", Kind: "social.feed"}, 2*time.Millisecond, true)
	s := u.byOp["social.feed"].hist.summary()
	if s.Count != 1 || float64(s.Under10Pct) != 100 {
		t.Errorf("count %d under 10 ms %v: the warm-up request must not be counted", s.Count, s.Under10Pct)
	}
}

// The post-run audit reads under the same per-request deadline as the load, so an engine that
// hangs on reads cannot stall the audit (and with it the sweep) for ever.
func TestAuditHasItsOwnRequestDeadline(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.hangReads = true })
	mix, err := newMix(benchLayouts, "ledger.expense")
	if err != nil {
		t.Fatal(err)
	}
	p.Mix, p.RequestTimeout, p.Window = mix, 30*time.Millisecond, 100*time.Millisecond
	done := make(chan struct{})
	var rec *Record
	var runErr error
	go func() { rec, runErr = runRepetition(context.Background(), p); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the audit of a hung engine never returned")
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
	if rec.AuditOK || !strings.Contains(rec.AuditError, "deadline") {
		t.Errorf("a timed-out audit read must fail the audit and name the deadline: ok=%v %q", rec.AuditOK, rec.AuditError)
	}
}
