package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// errDiskGuard marks a repetition stopped because the data disk ran low.
var errDiskGuard = errors.New("disk guard")

// maxErrorSamples is how many error messages a record keeps.
const maxErrorSamples = 5

// runParams is one repetition at one virtual-user level.
type runParams struct {
	// LedgerExpenses is the expenses per ledger account the data was seeded with; 0 is the default.
	LedgerExpenses int

	Engine Engine
	// EngineName is the name the record carries ("ring", "mongo", ...).
	EngineName string
	World      *world
	Mix        *Mix
	Base       string
	VUsers     int
	Repetition int
	// Loop is "closed" (one request in flight per virtual user) or "open" (Poisson
	// arrivals at Rate per second, offered whether or not the engine keeps up).
	Loop   string
	Rate   float64
	Window time.Duration
	// The warm-up runs unmeasured windows of WarmWindow until the last two are within WarmTol
	// of each other and at least WarmMin has passed, giving up at WarmMax (warm=false). A zero
	// WarmMax skips the warm-up.
	WarmWindow, WarmMin, WarmMax time.Duration
	WarmTol                      float64
	RandSeed, Seed               uint64
	Scale                        int
	Level, Op                    string
	Facts                        map[string]any
	Guard                        *diskGuard
	CPU                          cpuProbe
	// ServerCPU, when set, reads the engine's server process CPU time before and after the
	// measured window.
	ServerCPU cpuProbe
	// RequestTimeout bounds one request; a request past it is an error with its elapsed time
	// in the latency. 0 sets no bound.
	RequestTimeout time.Duration
	// RunFlags go into the record unchanged, see describeRunFlags.
	RunFlags map[string]string
	// Mark, when set, is told when the measured window opens and closes (markMeasureStart,
	// markMeasureEnd). Its error is printed and otherwise ignored.
	Mark func(vusers, repetition int, event string) error
	// Profiled labels the record: profile samplers were attached to the engine.
	Profiled bool
}

// mark reports a window event to p.Mark. A mark that cannot be written is said on stderr and
// the run goes on: it costs a profile, never a measurement.
func (p runParams) mark(event string) {
	if p.Mark == nil {
		return
	}
	if err := p.Mark(p.VUsers, p.Repetition, event); err != nil {
		fmt.Fprintf(os.Stderr, "bigasstest: %v\n", err)
	}
}

// opAgg is the measured figures of one operation.
type opAgg struct {
	count, errors, roundTrips, commits, retries int64
	// offsetReads counts the reads that started inside their store (ReadOp.Offset above 0).
	offsetReads int64
	// latencySum is the summed charged latency of the recorded samples; documents and
	// elements are the sizes of the successful replies.
	latencySum          time.Duration
	documents, elements int64
	hist                *histogram
}

// vuserState is one virtual user: its connection, its request stream and its figures.
type vuserState struct {
	session Session
	ops     map[model.Layout]LayoutOps
	planner *planner

	overall   *histogram
	byLayout  map[model.Layout]*histogram
	layoutReq map[model.Layout]int64
	byOp      map[string]*opAgg
	reads     int64
	writes    int64
	measured  int64

	hits, misses, verifyFailures, errs int64
	samples                            []string
}

func newVuserState() *vuserState {
	u := &vuserState{overall: newHistogram(), byLayout: map[model.Layout]*histogram{}, layoutReq: map[model.Layout]int64{}, byOp: map[string]*opAgg{}}
	for _, layout := range benchLayouts {
		u.byLayout[layout] = newHistogram()
	}
	return u
}

func (u *vuserState) sample(message string) {
	if len(u.samples) < maxErrorSamples {
		u.samples = append(u.samples, redactAddresses(message))
	}
}

// record accounts for one finished request. Checks and errors count in every phase;
// latency and throughput only in the measured one.
func (u *vuserState) record(res result, charged time.Duration, measuring bool) {
	agg := u.byOp[res.Kind]
	if agg == nil {
		agg = &opAgg{hist: newHistogram()}
		u.byOp[res.Kind] = agg
	}
	switch {
	case res.OpErr != nil:
		u.errs++
		u.sample(fmt.Sprintf("%s: %v", res.Kind, res.OpErr))
	case res.VerifyErr != nil:
		u.verifyFailures++
		if errors.Is(res.VerifyErr, errShortPage) {
			u.misses++
		}
		u.sample(fmt.Sprintf("%s: %v", res.Kind, res.VerifyErr))
	case !res.Write:
		u.hits++
	}
	if !measuring {
		return
	}
	u.measured++
	u.layoutReq[res.Layout]++
	agg.count++
	if res.Offset > 0 {
		agg.offsetReads++
	}
	agg.retries += int64(res.Reply.Retries)
	if res.Write {
		u.writes++
	} else {
		u.reads++
	}
	if res.OpErr != nil {
		agg.errors++
		if res.TimedOut {
			// The request ran for its whole timeout: leaving it out would hide the stall.
			u.recordLatency(agg, res.Layout, charged)
		}
		return
	}
	agg.roundTrips += int64(res.Reply.RoundTrips)
	agg.commits += int64(res.Reply.Commits)
	agg.documents += int64(len(res.Reply.Documents))
	agg.elements += int64(len(res.Reply.Elements))
	u.recordLatency(agg, res.Layout, charged)
}

// recordLatency adds a latency to the overall, layout and operation histograms.
func (u *vuserState) recordLatency(agg *opAgg, layout model.Layout, charged time.Duration) {
	for _, h := range []*histogram{u.overall, u.byLayout[layout], agg.hist} {
		if err := h.record(charged); err != nil {
			u.errs++
			u.sample(err.Error())
			return
		}
	}
	agg.latencySum += charged
}

// runPhase runs every virtual user for duration and returns how many requests completed and
// how long it took. In an open loop the users share one Poisson schedule. A request cut off
// by ctx is dropped, not counted.
func runPhase(ctx context.Context, e *env, users []*vuserState, p runParams, duration time.Duration, measuring bool) (int64, time.Duration) {
	started := time.Now()
	deadline := started.Add(duration)
	var sched *arrivalSchedule
	if p.Loop == "open" {
		sched = newArrivalSchedule(started, p.Rate, p.RandSeed)
	}
	var completed atomic.Int64
	var wg sync.WaitGroup
	for _, u := range users {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				var scheduled time.Time
				if sched != nil {
					scheduled = sched.next()
					if scheduled.After(deadline) || waitUntil(ctx, scheduled) != nil {
						return
					}
				} else if !time.Now().Before(deadline) {
					return
				}
				req := u.planner.next()
				began := time.Now()
				res := e.runRequest(ctx, u.ops[req.Layout], req)
				if ctx.Err() != nil {
					return
				}
				charged := res.Latency
				if sched != nil {
					charged += max(0, began.Sub(scheduled))
				}
				u.record(res, charged, measuring)
				completed.Add(1)
			}
		}()
	}
	wg.Wait()
	return completed.Load(), time.Since(started)
}

func settled(a, b, tolerance float64) bool {
	if a == 0 && b == 0 {
		return false
	}
	return math.Abs(a-b)/math.Max(a, b) <= tolerance
}

// freshRefs are stores whose seeded counts show whether the data set was freshly seeded.
func freshRefs(w *world, layout model.Layout) []StoreRef {
	ds := w.sets[layout]
	pairs := map[model.Layout][][2]string{
		model.Social: {{"User", "Feed"}, {"User", "Inbox"}, {"User", "Followers"}, {"User", "Notifications"}},
		model.Chat:   {{"Room", "Messages"}, {"Room", "Members"}},
		model.Ledger: {{"Account", "Expenses"}, {"Account", "Invoices"}},
		model.Agent:  {{"User", "Memories"}, {"Run", "Tasks"}, {"Session", "Messages"}},
	}[layout]
	var refs []StoreRef
	for _, pair := range pairs {
		for _, id := range sampleIDs(ds.ids(pair[0])) {
			refs = append(refs, StoreRef{pair[0], id, pair[1]})
		}
	}
	for bucket := range ds.idList {
		refs = append(refs, StoreRef{Bucket: bucket})
	}
	sort.Slice(refs, func(i, j int) bool { return fmt.Sprint(refs[i]) < fmt.Sprint(refs[j]) })
	return refs
}

// freshSample is how many owners of a store checkFresh compares, spread over the whole list.
const freshSample = 8

// sampleIDs picks up to freshSample ids spread evenly over ids, always the first and the last.
func sampleIDs(ids []string) []string {
	if len(ids) <= freshSample {
		return ids
	}
	var out []string
	for i := range freshSample {
		out = append(out, ids[i*(len(ids)-1)/(freshSample-1)])
	}
	return out
}

// checkFresh refuses a data set that is not the one just seeded: a rerun without a reseed
// would audit against the wrong baseline.
func checkFresh(ctx context.Context, w *world, layout model.Layout, ops LayoutOps) error {
	refs := freshRefs(w, layout)
	counts, err := ops.Count(ctx, refs)
	if err != nil {
		return fmt.Errorf("checking the %s data set: %w", layout, err)
	}
	for _, ref := range refs {
		want := int64(len(w.sets[layout].elements(ref)))
		if ref.Store == "" {
			want = w.sets[layout].count(ref.Bucket)
		}
		if counts[ref] != want {
			return fmt.Errorf("the %s data set is not freshly seeded: %s/%s/%s holds %d, the seed holds %d. Reseed before every repetition (stop, clean, start, 'bigasstest seed')", layout, ref.Bucket, ref.Owner, ref.Store, counts[ref], want)
		}
	}
	return nil
}

// runRepetition runs warm-up, the measured window and the audit for one level. An error is
// returned for a repetition that could not run or was aborted; a repetition whose checks
// failed returns its record with Failed set.
func runRepetition(ctx context.Context, p runParams) (*Record, error) {
	rec := &Record{Schema: recordSchema, Engine: p.EngineName, Level: p.Level, Facts: p.Facts, VUsers: p.VUsers, Repetition: p.Repetition,
		Loop: p.Loop, Op: p.Op, LedgerExpenses: p.LedgerExpenses, Scale: p.Scale, Seed: p.Seed, RandSeed: p.RandSeed, Fingerprint: map[string]string{},
		Started: time.Now().UTC().Format(time.RFC3339)}
	if p.Loop == "open" {
		rec.Rate = p.Rate
	}
	rec.Profile, rec.ReadShares = p.Mix.profile.Name, map[string]float64{}
	for layout, share := range p.Mix.profile.ReadShare {
		rec.ReadShares[string(layout)] = share
	}
	rec.Access, rec.PageDepth = p.World.access.Mode, p.World.access.PageDepth
	if rec.Access == accessZipf {
		rec.ZipfS = p.World.access.ZipfS
	}
	rec.ReadElements = p.World.elementsPerRead()
	rec.ReadPages = p.World.pageInfo()
	rec.RunFlags = p.RunFlags
	for layout, ds := range p.World.sets {
		rec.Fingerprint[string(layout)] = ds.fingerprint()
	}
	settings, err := p.Engine.Settings(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading the engine settings: %w", err)
	}
	rec.Settings = settings
	rec.Batch = settings["batch"]
	rec.MultiGet = settings["multiget"]
	rec.Native = settings["native"]
	rec.Aggregates = settings["aggregates"]
	if rec.Native == "on" {
		rec.NativeFingerprint = map[string]string{}
		for layout, ds := range p.World.sets {
			if ds.nativeFinger != "" {
				rec.NativeFingerprint[string(layout)] = ds.nativeFinger
			}
		}
	}
	rec.Profiled = p.Profiled

	users := make([]*vuserState, p.VUsers)
	var sessions []Session
	defer func() {
		for _, session := range sessions {
			if err := session.Close(); err != nil && len(rec.CloseErrors) < maxErrorSamples {
				rec.CloseErrors = append(rec.CloseErrors, redactAddresses(fmt.Sprintf("closing a session: %v", err)))
			}
		}
	}()
	for i := range users {
		session, err := p.Engine.Session(ctx, i)
		if err != nil {
			return nil, fmt.Errorf("opening the session of virtual user %d: %w", i, err)
		}
		sessions = append(sessions, session)
		u := newVuserState()
		u.session, u.ops = session, map[model.Layout]LayoutOps{}
		for _, layout := range benchLayouts {
			ops, err := session.Layout(layout, layoutDatabase(p.Base, layout))
			if err != nil {
				return nil, fmt.Errorf("opening layout %s for virtual user %d: %w", layout, i, err)
			}
			u.ops[layout] = ops
		}
		u.planner = newPlanner(p.World, p.Mix, p.Repetition, i, p.RandSeed)
		users[i] = u
	}
	for _, layout := range benchLayouts {
		if err := checkFresh(ctx, p.World, layout, users[0].ops[layout]); err != nil {
			return nil, err
		}
	}
	environment := &env{w: p.World, reg: newRegistry(), book: newBook(p.World), timeout: p.RequestTimeout}
	environment.book.timeout = p.RequestTimeout
	environment.book.native = rec.Native == "on"
	environment.book.aggregates = rec.Aggregates == "on"
	environment.book.ledgerWrites = slices.Contains(p.Mix.kinds(), string(WriteLedgerExpense))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var guardWG sync.WaitGroup
	var tripped atomic.Bool
	var trippedWith string
	if p.Guard != nil {
		guardWG.Add(1)
		go func() {
			defer guardWG.Done()
			p.Guard.watch(runCtx, func(free int64, err error) {
				trippedWith = fmt.Sprintf("%d bytes free, the minimum is %d", free, p.Guard.minFree)
				if err != nil {
					trippedWith = err.Error()
				}
				tripped.Store(true)
				cancel()
			})
		}()
	}
	finish := func() { cancel(); guardWG.Wait() }

	if p.WarmMax > 0 {
		warmStart := time.Now()
		for runCtx.Err() == nil {
			done, took := runPhase(runCtx, environment, users, p, p.WarmWindow, false)
			rec.WarmWindows = append(rec.WarmWindows, float64(done)/took.Seconds())
			elapsed := time.Since(warmStart)
			if n := len(rec.WarmWindows); n >= 2 && elapsed >= p.WarmMin && settled(rec.WarmWindows[n-1], rec.WarmWindows[n-2], p.WarmTol) {
				rec.Warm = true
				break
			}
			if elapsed >= p.WarmMax {
				break
			}
		}
		rec.WarmSeconds = time.Since(warmStart).Seconds()
	}

	probe := p.CPU
	if probe == nil {
		probe = procProbe{dir: "/proc/self"}
	}
	cpuBefore, cpuErr := probe.cpuTime()
	var serverBefore time.Duration
	var serverErr error
	if p.ServerCPU != nil {
		serverBefore, serverErr = p.ServerCPU.cpuTime()
	}
	p.mark(markMeasureStart)
	_, took := runPhase(runCtx, environment, users, p, p.Window, true)
	p.mark(markMeasureEnd)
	cpuAfter, cpuErr2 := probe.cpuTime()
	var serverAfter time.Duration
	if p.ServerCPU != nil && serverErr == nil {
		serverAfter, serverErr = p.ServerCPU.cpuTime()
	}
	finish()
	rec.Seconds = took.Seconds()
	if cpuErr == nil && cpuErr2 == nil && took > 0 {
		rec.ClientCPUPct = 100 * (cpuAfter - cpuBefore).Seconds() / (took.Seconds() * float64(runtime.NumCPU()))
	}
	if p.ServerCPU != nil {
		if serverErr != nil {
			// A broken probe costs the server CPU figure, never the measurement.
			fmt.Fprintf(os.Stderr, "bigasstest: server CPU not recorded: %v\n", serverErr)
		} else if took > 0 {
			rec.ServerCPUSeconds = (serverAfter - serverBefore).Seconds()
			rec.ServerCPUCores = rec.ServerCPUSeconds / took.Seconds()
		}
	}
	collect(rec, users, took)
	// An operation of the mix that was never drawn still has a row: zero samples is thin, not absent.
	for _, kind := range p.Mix.kinds() {
		if _, seen := rec.Ops[kind]; !seen {
			empty := newHistogram().summary()
			rec.Ops[kind] = OpStats{Thin: true, latencySummary: empty}
		}
	}
	if tripped.Load() {
		rec.Aborted, rec.Failed = "disk_guard", true
		rec.Capabilities = capabilitiesOf(rec)
		rec.label()
		return rec, fmt.Errorf("%w: %s", errDiskGuard, trippedWith)
	}
	if ctx.Err() != nil {
		rec.Aborted, rec.Failed = "interrupted", true
		rec.Capabilities = capabilitiesOf(rec)
		rec.label()
		return rec, fmt.Errorf("interrupted: %w", ctx.Err())
	}

	settingsAfter, err := p.Engine.Settings(ctx)
	if err != nil {
		rec.Capabilities = capabilitiesOf(rec)
		return rec, fmt.Errorf("reading the engine settings after the run: %w", err)
	}
	rec.SettingsAfter = settingsAfter
	rec.label()

	rec.AuditOK = true
	for _, layout := range benchLayouts {
		if err := environment.book.audit(ctx, layout, users[0].ops[layout]); err != nil {
			rec.AuditOK = false
			if rec.AuditError != "" {
				rec.AuditError += "; "
			}
			rec.AuditError += err.Error()
		}
	}
	var seeded int64
	for _, ds := range p.World.sets {
		for _, count := range ds.counts {
			seeded += count
		}
	}
	rec.GrowthPct = 100 * float64(environment.book.ackedWrites()) / float64(seeded)
	rec.Failed = !rec.AuditOK || rec.VerifyFailures > 0 || rec.Errors > 0
	rec.MaintainedSumsAudited = environment.book.sumsAudited
	rec.Capabilities = capabilitiesOf(rec)
	rec.label()
	return rec, nil
}

// collect merges the virtual users' figures into the record.
func collect(rec *Record, users []*vuserState, took time.Duration) {
	overall := newHistogram()
	layouts := map[model.Layout]*histogram{}
	layoutReq := map[model.Layout]int64{}
	ops := map[string]*opAgg{}
	var measured int64
	for _, u := range users {
		overall.merge(u.overall)
		for layout, h := range u.byLayout {
			if layouts[layout] == nil {
				layouts[layout] = newHistogram()
			}
			layouts[layout].merge(h)
			layoutReq[layout] += u.layoutReq[layout]
		}
		for kind, agg := range u.byOp {
			total := ops[kind]
			if total == nil {
				total = &opAgg{hist: newHistogram()}
				ops[kind] = total
			}
			total.count += agg.count
			total.offsetReads += agg.offsetReads
			total.errors += agg.errors
			total.roundTrips += agg.roundTrips
			total.commits += agg.commits
			total.latencySum += agg.latencySum
			total.documents += agg.documents
			total.elements += agg.elements
			total.retries += agg.retries
			total.hist.merge(agg.hist)
		}
		measured += u.measured
		rec.Reads += u.reads
		rec.Writes += u.writes
		rec.Hits += u.hits
		rec.Misses += u.misses
		rec.VerifyFailures += u.verifyFailures
		rec.Errors += u.errs
		for _, s := range u.samples {
			if len(rec.ErrorSamples) < maxErrorSamples {
				rec.ErrorSamples = append(rec.ErrorSamples, s)
			}
		}
	}
	rec.LatencyClamped = overall.clamped
	seconds := took.Seconds()
	rec.Overall = Summary{Requests: measured, Throughput: float64(measured) / seconds, latencySummary: overall.summary()}
	rec.Layouts = map[string]Summary{}
	for layout, h := range layouts {
		rec.Layouts[string(layout)] = Summary{Requests: layoutReq[layout], Throughput: float64(layoutReq[layout]) / seconds, latencySummary: h.summary()}
	}
	rec.Ops = map[string]OpStats{}
	for kind, agg := range ops {
		rec.Retries += agg.retries
		stats := OpStats{Count: agg.count, Errors: agg.errors, Retries: agg.retries, OffsetReads: agg.offsetReads, Thin: agg.count < thinOpSamples, LatencySumMs: float64(agg.latencySum) / float64(time.Millisecond), latencySummary: agg.hist.summary()}
		if ok := agg.count - agg.errors; ok > 0 {
			stats.RoundTripsPerOp = float64(agg.roundTrips) / float64(ok)
			stats.CommitsPerOp = float64(agg.commits) / float64(ok)
			stats.DocumentsPerOp = float64(agg.documents) / float64(ok)
			stats.ElementsPerOp = float64(agg.elements) / float64(ok)
		}
		rec.Ops[kind] = stats
	}
}

// sweepConfig is a whole sweep: every virtual-user level, each with its repetitions.
type sweepConfig struct {
	Base        runParams
	VUsers      []int
	Repetitions int
	// Reseed makes the data set fresh again (stop, clean, start, seed). It runs before every
	// repetition except the first, because the wrapper seeds before the sweep starts.
	Reseed func(ctx context.Context) error
	// Open opens the engine for a level, with pools sized to the virtual users. It is called
	// after a reseed, because a restart drops every connection.
	Open func(ctx context.Context, vusers int) (Engine, error)
	// Emit receives every record, including that of an aborted repetition.
	Emit func(rec *Record) error
}

// runSweep runs the levels in the order given. It stops at the first abort and otherwise
// finishes the sweep, then reports how many repetitions failed their checks.
func runSweep(ctx context.Context, cfg sweepConfig) error {
	if len(cfg.VUsers) == 0 || cfg.Repetitions < 1 {
		return errors.New("the sweep needs at least one virtual-user level and one repetition")
	}
	if len(cfg.VUsers)*cfg.Repetitions > 1 && cfg.Reseed == nil {
		return errors.New("a sweep of more than one repetition needs -reseed-cmd: each repetition must start from freshly seeded data, or the audit baseline is wrong")
	}
	first, failed := true, 0
	for _, vusers := range cfg.VUsers {
		for rep := 1; rep <= cfg.Repetitions; rep++ {
			if !first {
				if err := cfg.Reseed(ctx); err != nil {
					return fmt.Errorf("reseeding before %d virtual users, repetition %d: %w", vusers, rep, err)
				}
			}
			first = false
			engine, err := cfg.Open(ctx, vusers)
			if err != nil {
				return fmt.Errorf("opening the engine for %d virtual users: %w", vusers, err)
			}
			p := cfg.Base
			p.Engine, p.VUsers, p.Repetition = engine, vusers, rep
			rec, runErr := runRepetition(ctx, p)
			if err := engine.Close(); err != nil && runErr == nil {
				runErr = fmt.Errorf("closing the engine: %w", err)
			}
			if rec != nil {
				if err := cfg.Emit(rec); err != nil {
					return err
				}
				if rec.Failed {
					failed++
				}
			}
			if runErr != nil {
				return fmt.Errorf("%d virtual users, repetition %d: %w", vusers, rep, runErr)
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d repetitions failed their checks (see verify_failures, errors and audit_error in the records)", failed)
	}
	return nil
}
