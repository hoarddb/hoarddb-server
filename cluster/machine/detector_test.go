package machine

import (
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"
)

const ms = time.Millisecond

// leaderCfg is the production defaults (3 failures, 1000 ms interval, 500 ms
// timeout, 5000 ms failover, 600000 ms dead) with n1 leading a four-member
// cluster of three voters and one non-voter.
func leaderCfg() DetectorConfig {
	return DetectorConfig{
		SelfID: "n1", Leader: "n1",
		ProbeInterval: 1000 * ms, ProbeTimeout: 500 * ms,
		Failures: 3, Failover: 5000 * ms, Dead: 600000 * ms,
		Voter:     true,
		MemberIDs: []string{"n1", "n2", "n3", "n4"},
		VoterIDs:  []string{"n1", "n2", "n3"},
	}
}

// followerCfg is the same cluster seen from n1 while n2 leads.
func followerCfg() DetectorConfig {
	cfg := leaderCfg()
	cfg.Leader = "n2"
	return cfg
}

// quiet removes the probe targets so a tick's action shows only stage 2.
func quiet(cfg DetectorConfig) DetectorConfig {
	cfg.MemberIDs, cfg.VoterIDs = nil, nil
	return cfg
}

func norm(a DetectorAction) DetectorAction {
	if len(a.Probe) == 0 {
		a.Probe = nil
	}
	if len(a.Report) == 0 {
		a.Report = nil
	}
	if len(a.ProposeDown) == 0 {
		a.ProposeDown = nil
	}
	if len(a.ProposeDead) == 0 {
		a.ProposeDead = nil
	}
	return a
}

func wantAction(t *testing.T, got, want DetectorAction) {
	t.Helper()
	if !reflect.DeepEqual(norm(got), norm(want)) {
		t.Fatalf("action = %+v, want %+v", norm(got), norm(want))
	}
}

// failed is one failed probe of subject: started at start, timed out 500 ms later.
func failed(subject string, start time.Duration) ProbeResult {
	return ProbeResult{Subject: subject, StartedAt: start, At: start + 500*ms}
}

func answered(subject string, at time.Duration) ProbeResult {
	return ProbeResult{Subject: subject, StartedAt: at - 10*ms, At: at, Answered: true}
}

// failRun feeds n failed probes of subject, one per second from start, and
// returns the action of the last one.
func failRun(d *DetectorState, cfg DetectorConfig, subject string, start time.Duration, n int) DetectorAction {
	var last DetectorAction
	for i := 0; i < n; i++ {
		last = d.Apply(failed(subject, start+time.Duration(i)*1000*ms), cfg)
	}
	return last
}

type tableRow struct {
	name  string
	cfg   DetectorConfig
	pre   []DetectorEvent
	after *DetectorConfig // config for ev when it differs from cfg
	ev    DetectorEvent
	want  DetectorAction
	check func(t *testing.T, d *DetectorState)
}

// TestDetectorDecisionTable has one row per line of the brief's decision
// table (build doc 4.2), in the brief's order, plus the rows for the three
// places the table is silent and the builder had to choose (marked "ext").
func TestDetectorDecisionTable(t *testing.T) {
	foll := followerCfg()
	noLeader := leaderCfg()
	noLeader.Leader = ""
	mixed := leaderCfg()
	mixed.MemberIDs = []string{"n1", "n2"}
	both := leaderCfg()
	both.MemberIDs = []string{"n1", "n2", "n3"}
	nonVoter := leaderCfg()
	nonVoter.SelfID, nonVoter.Leader, nonVoter.Voter = "n4", "n1", false
	deadAt := 1000*ms + leaderCfg().Dead

	rows := []tableRow{
		{name: "1 ProbeTick interval not elapsed: no probe", cfg: leaderCfg(),
			pre:  []DetectorEvent{ProbeTick{At: 0}},
			ev:   ProbeTick{At: 999 * ms},
			want: DetectorAction{}},
		{name: "1 ProbeTick interval elapsed: probe exactly the subjects whose interval elapsed", cfg: mixed,
			pre:   []DetectorEvent{ProbeTick{At: 0}},
			after: &both,
			ev:    ProbeTick{At: 600 * ms},
			want:  DetectorAction{Probe: []string{"n3"}}},
		{name: "1 ProbeTick at exactly one interval probes every member but self", cfg: leaderCfg(),
			pre:  []DetectorEvent{ProbeTick{At: 0}},
			ev:   ProbeTick{At: 1000 * ms},
			want: DetectorAction{Probe: []string{"n2", "n3", "n4"}}},
		{name: "1 ProbeTick of a non-voter probes the voters only", cfg: nonVoter,
			ev:   ProbeTick{At: 0},
			want: DetectorAction{Probe: []string{"n1", "n2", "n3"}}},
		{name: "2 ProbeResult answered: fails, firstFailAt and reported reset", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n2", 0), failed("n2", 1000*ms), failed("n2", 2000*ms)},
			ev:   answered("n2", 3500*ms),
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.fails["n2"] != 0 {
					t.Errorf("fails = %d, want 0", d.fails["n2"])
				}
				if _, ok := d.firstFailAt["n2"]; ok {
					t.Errorf("firstFailAt kept: %v", d.firstFailAt["n2"])
				}
				if d.reported["n2"] {
					t.Errorf("reported kept")
				}
			}},
		{name: "3 ProbeResult failed, fails+1 < Failures: counts, no action", cfg: leaderCfg(),
			ev:   failed("n2", 100*ms),
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.fails["n2"] != 1 || d.firstFailAt["n2"] != 100*ms || d.reported["n2"] {
					t.Errorf("fails=%d firstFailAt=%v reported=%v", d.fails["n2"], d.firstFailAt["n2"], d.reported["n2"])
				}
			}},
		{name: "3 ProbeResult second failure keeps the first firstFailAt", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n2", 100*ms)},
			ev:   failed("n2", 1100*ms),
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.fails["n2"] != 2 || d.firstFailAt["n2"] != 100*ms {
					t.Errorf("fails=%d firstFailAt=%v", d.fails["n2"], d.firstFailAt["n2"])
				}
			}},
		{name: "4 ProbeResult failed, fails+1 == Failures: one report", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n2", 0), failed("n2", 1000*ms)},
			ev:   failed("n2", 2000*ms),
			want: DetectorAction{Report: []string{"n2"}},
			check: func(t *testing.T, d *DetectorState) {
				if d.fails["n2"] != 3 || !d.reported["n2"] {
					t.Errorf("fails=%d reported=%v", d.fails["n2"], d.reported["n2"])
				}
			}},
		{name: "5 ProbeResult failed, fails+1 > Failures: counted, not reported again", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n2", 0), failed("n2", 1000*ms), failed("n2", 2000*ms)},
			ev:   failed("n2", 3000*ms),
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.fails["n2"] != 4 {
					t.Errorf("fails = %d, want 4", d.fails["n2"])
				}
			}},
		{name: "6 ReportReceived from self is ignored", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n2", 0), failed("n2", 1000*ms), failed("n2", 2000*ms)},
			ev:   ReportReceived{Subject: "n2", Observer: "n1", At: 2600 * ms},
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if len(d.observers["n2"]) != 0 {
					t.Errorf("observers = %v, want none", d.observers["n2"])
				}
			}},
		{name: "7 ReportReceived from an observer already counted is ignored", cfg: leaderCfg(),
			pre:  []DetectorEvent{ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms}},
			ev:   ReportReceived{Subject: "n2", Observer: "n3", At: 200 * ms},
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.observers["n2"]["n3"] != 100*ms || len(d.observers["n2"]) != 1 {
					t.Errorf("observers = %v, want n3 at 100ms only", d.observers["n2"])
				}
			}},
		{name: "8 ReportReceived later than first arrival + Failover is discarded", cfg: leaderCfg(),
			pre:  []DetectorEvent{ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms}},
			ev:   ReportReceived{Subject: "n2", Observer: "n4", At: 5101 * ms},
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if _, ok := d.observers["n2"]["n4"]; ok {
					t.Errorf("late report recorded: %v", d.observers["n2"])
				}
			}},
		{name: "8 ReportReceived exactly at first arrival + Failover is inside the window", cfg: leaderCfg(),
			pre:  []DetectorEvent{ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms}},
			ev:   ReportReceived{Subject: "n2", Observer: "n4", At: 5100 * ms},
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.observers["n2"]["n4"] != 5100*ms {
					t.Errorf("boundary report not recorded: %v", d.observers["n2"])
				}
			}},
		{name: "9 ReportReceived first report for a subject: arrival recorded", cfg: leaderCfg(),
			ev:   ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms},
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.observers["n2"]["n3"] != 100*ms {
					t.Errorf("observers = %v", d.observers["n2"])
				}
			}},
		{name: "10 ReportReceived, leader, own fails >= Failures, distinct observer in window: propose down", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n2", 0), failed("n2", 1000*ms), failed("n2", 2000*ms)},
			ev:   ReportReceived{Subject: "n2", Observer: "n3", At: 2600 * ms},
			want: DetectorAction{ProposeDown: []string{"n2"}}},
		{name: "11 ReportReceived, leader, own fails < Failures: recorded, no proposal", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n2", 0), failed("n2", 1000*ms)},
			ev:   ReportReceived{Subject: "n2", Observer: "n3", At: 2100 * ms},
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.observers["n2"]["n3"] != 2100*ms {
					t.Errorf("observers = %v", d.observers["n2"])
				}
			}},
		{name: "11 ReportReceived at a node that is not the leader never proposes", cfg: foll,
			pre:  []DetectorEvent{failed("n3", 0), failed("n3", 1000*ms), failed("n3", 2000*ms)},
			ev:   ReportReceived{Subject: "n3", Observer: "n4", At: 2600 * ms},
			want: DetectorAction{}},
		{name: "12 LeaderChanged, became leader: observers cleared, fails and downSince kept", cfg: foll,
			pre: []DetectorEvent{
				ReportReceived{Subject: "n3", Observer: "n4", At: 100 * ms},
				failed("n3", 0), failed("n3", 1000*ms),
				DownApplied{Subject: "n4", At: 50 * ms},
			},
			after: ptr(leaderCfg()),
			ev:    LeaderChanged{LeaderID: "n1", At: 2000 * ms},
			want:  DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if len(d.observers["n3"]) != 0 {
					t.Errorf("observers kept: %v", d.observers)
				}
				if d.fails["n3"] != 2 {
					t.Errorf("fails = %d, want 2", d.fails["n3"])
				}
				if d.downSince["n4"] != 50*ms {
					t.Errorf("downSince = %v, want 50ms", d.downSince)
				}
			}},
		{name: "13 LeaderChanged, no longer leader: observers cleared", cfg: leaderCfg(),
			pre: []DetectorEvent{
				ReportReceived{Subject: "n3", Observer: "n4", At: 100 * ms},
				failed("n3", 0), failed("n3", 1000*ms),
			},
			after: &foll,
			ev:    LeaderChanged{LeaderID: "n2", At: 2000 * ms},
			want:  DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if len(d.observers["n3"]) != 0 {
					t.Errorf("observers kept: %v", d.observers)
				}
			}},
		{name: "14 DownApplied starts the node-local stage-2 timer", cfg: leaderCfg(),
			ev:   DownApplied{Subject: "n2", At: 7000 * ms},
			want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.downSince["n2"] != 7000*ms {
					t.Errorf("downSince = %v", d.downSince)
				}
			}},
		{name: "15 ProbeTick, leader, down for Dead: propose dead and delete the entry", cfg: quiet(leaderCfg()),
			pre: []DetectorEvent{DownApplied{Subject: "n2", At: 1000 * ms}},
			ev:  ProbeTick{At: deadAt}, want: DetectorAction{ProposeDead: []string{"n2"}},
			check: func(t *testing.T, d *DetectorState) {
				if _, ok := d.downSince["n2"]; ok {
					t.Errorf("entry kept")
				}
			}},
		{name: "15 ProbeTick one tick short of Dead: nothing, entry kept", cfg: quiet(leaderCfg()),
			pre: []DetectorEvent{DownApplied{Subject: "n2", At: 1000 * ms}},
			ev:  ProbeTick{At: deadAt - 1}, want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if _, ok := d.downSince["n2"]; !ok {
					t.Errorf("entry deleted early")
				}
			}},
		{name: "15 ProbeTick at a non-leader never proposes dead and keeps the timer", cfg: quiet(foll),
			pre: []DetectorEvent{DownApplied{Subject: "n2", At: 1000 * ms}},
			ev:  ProbeTick{At: deadAt}, want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if d.downSince["n2"] != 1000*ms {
					t.Errorf("timer lost: %v", d.downSince)
				}
			}},
		{name: "15 ProbeTick at a node with no leader never proposes dead", cfg: quiet(noLeader),
			pre: []DetectorEvent{DownApplied{Subject: "n2", At: 1000 * ms}},
			ev:  ProbeTick{At: deadAt}, want: DetectorAction{}},
		{name: "15 ext: a probe answered after down means the committed state is not still down", cfg: quiet(leaderCfg()),
			pre: []DetectorEvent{DownApplied{Subject: "n2", At: 1000 * ms}, answered("n2", 60000*ms)},
			ev:  ProbeTick{At: deadAt}, want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if _, ok := d.downSince["n2"]; ok {
					t.Errorf("timer survived an answered probe")
				}
			}},
		{name: "16 never propose down for self", cfg: leaderCfg(),
			pre:  []DetectorEvent{failed("n1", 0), failed("n1", 1000*ms), failed("n1", 2000*ms)},
			ev:   ReportReceived{Subject: "n1", Observer: "n3", At: 2600 * ms},
			want: DetectorAction{}},
		{name: "16 never propose dead for self", cfg: quiet(leaderCfg()),
			pre: []DetectorEvent{DownApplied{Subject: "n1", At: 1000 * ms}},
			ev:  ProbeTick{At: deadAt}, want: DetectorAction{}},
		{name: "ext: the leader's own third failure proposes down when a distinct report is already in the window", cfg: leaderCfg(),
			pre: []DetectorEvent{
				ReportReceived{Subject: "n2", Observer: "n3", At: 1800 * ms},
				failed("n2", 0), failed("n2", 1000*ms),
			},
			ev:   failed("n2", 2000*ms),
			want: DetectorAction{Report: []string{"n2"}, ProposeDown: []string{"n2"}}},
		{name: "ext: the leader's own third failure after the report's window closed proposes nothing", cfg: leaderCfg(),
			pre: []DetectorEvent{
				ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms},
				failed("n2", 5000*ms), failed("n2", 6000*ms),
			},
			ev:   failed("n2", 7000*ms),
			want: DetectorAction{Report: []string{"n2"}}},
		{name: "ext: a probe answered cancels the observers recorded for that subject", cfg: leaderCfg(),
			pre: []DetectorEvent{ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms}},
			ev:  answered("n2", 200*ms), want: DetectorAction{},
			check: func(t *testing.T, d *DetectorState) {
				if len(d.observers["n2"]) != 0 {
					t.Errorf("observers survived recovery: %v", d.observers["n2"])
				}
			}},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			var d DetectorState
			for _, ev := range row.pre {
				d.Apply(ev, row.cfg)
			}
			cfg := row.cfg
			if row.after != nil {
				cfg = *row.after
			}
			wantAction(t, d.Apply(row.ev, cfg), row.want)
			if row.check != nil {
				row.check(t, &d)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// TestOnlyTwoObserversInsideOneWindow: the leader's own failures never commit
// down, a distinct member's report does, a report Failover later is
// discarded, and three 500 ms timeouts at 1000 ms report at t0 + 2500 ms.
// [propose on the leader's own observation]
func TestOnlyTwoObserversInsideOneWindow(t *testing.T) {
	cfg := leaderCfg()

	t.Run("three timeouts at one second report at t0 plus 2500 ms, not before", func(t *testing.T) {
		var d DetectorState
		t0 := 10000 * ms
		for i := 0; i < 2; i++ {
			start := t0 + time.Duration(i)*cfg.ProbeInterval
			if got := d.Apply(failed("n2", start), cfg); len(norm(got).Report) != 0 {
				t.Fatalf("reported after %d failures, want 3: %+v", i+1, got)
			}
		}
		last := d.Apply(ProbeResult{Subject: "n2", StartedAt: t0 + 2000*ms, At: t0 + 2500*ms}, cfg)
		wantAction(t, last, DetectorAction{Report: []string{"n2"}})
	})

	t.Run("the leader's own three failures alone never propose down", func(t *testing.T) {
		var d DetectorState
		wantAction(t, failRun(&d, cfg, "n2", 0, 3), DetectorAction{Report: []string{"n2"}})
		for i := 3; i < 20; i++ {
			wantAction(t, d.Apply(failed("n2", time.Duration(i)*1000*ms), cfg), DetectorAction{})
			wantAction(t, d.Apply(ProbeTick{At: time.Duration(i) * 1000 * ms}, quiet(cfg)), DetectorAction{})
		}
	})

	t.Run("one distinct report after the leader's own failures proposes down", func(t *testing.T) {
		var d DetectorState
		failRun(&d, cfg, "n2", 0, 3)
		wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 2600 * ms}, cfg),
			DetectorAction{ProposeDown: []string{"n2"}})
	})

	t.Run("a report from the leader itself is not a second observer", func(t *testing.T) {
		var d DetectorState
		failRun(&d, cfg, "n2", 0, 3)
		wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n1", At: 2600 * ms}, cfg), DetectorAction{})
	})

	t.Run("reports without the leader's own failures never propose", func(t *testing.T) {
		var d DetectorState
		failRun(&d, cfg, "n2", 0, 2)
		wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 2100 * ms}, cfg), DetectorAction{})
		wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n4", At: 2200 * ms}, cfg), DetectorAction{})
	})

	t.Run("a report Failover after the first arrival is discarded and counts for nothing", func(t *testing.T) {
		var d DetectorState
		d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms}, cfg)
		wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n4", At: 100*ms + cfg.Failover + 1}, cfg), DetectorAction{})
		if _, ok := d.observers["n2"]["n4"]; ok {
			t.Fatalf("late report recorded")
		}
	})

	t.Run("a report arriving long after the leader's own failures still proposes inside the window", func(t *testing.T) {
		var d DetectorState
		failRun(&d, cfg, "n2", 0, 3)
		got := d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 4000 * ms}, cfg)
		wantAction(t, got, DetectorAction{ProposeDown: []string{"n2"}})
	})
}

// TestFailoverIsACeilingNotAFloor: the proposal does not wait for Failover.
// [propose down only after Failover has elapsed]
func TestFailoverIsACeilingNotAFloor(t *testing.T) {
	cfg := leaderCfg()
	var d DetectorState
	failRun(&d, cfg, "n2", 0, 3) // known at t0 + 2500 ms
	got := d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 2600 * ms}, cfg)
	wantAction(t, got, DetectorAction{ProposeDown: []string{"n2"}})
	if 2600*ms >= cfg.Failover {
		t.Fatalf("test is vacuous: the proposal must come before Failover (%v)", cfg.Failover)
	}

	// The report that arrives first, before the leader's own third failure:
	// the proposal comes with that failure, still long before Failover.
	var e DetectorState
	e.Apply(failed("n2", 0), cfg)
	e.Apply(failed("n2", 1000*ms), cfg)
	e.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 2300 * ms}, cfg)
	wantAction(t, e.Apply(failed("n2", 2000*ms), cfg),
		DetectorAction{Report: []string{"n2"}, ProposeDown: []string{"n2"}})
}

// TestDuplicateReportsDoNotShortenTheWindow: a second report from an observer
// already counted is ignored: it adds no observer, moves no arrival and so
// does not move the window. [count reports, not observers]
func TestDuplicateReportsDoNotShortenTheWindow(t *testing.T) {
	cfg := leaderCfg()

	t.Run("a duplicate does not move the first arrival, so the window ends where it did", func(t *testing.T) {
		var d DetectorState
		d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 100 * ms}, cfg)
		d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 4000 * ms}, cfg)
		// the window is [100, 5100]: a distinct report at 5200 is outside it.
		d.Apply(ReportReceived{Subject: "n2", Observer: "n4", At: 5200 * ms}, cfg)
		if _, ok := d.observers["n2"]["n4"]; ok {
			t.Fatalf("a duplicate refreshed the window: n4 at 5200ms was accepted")
		}
		if d.observers["n2"]["n3"] != 100*ms {
			t.Fatalf("n3 arrival = %v, want 100ms", d.observers["n2"]["n3"])
		}
	})

	t.Run("a duplicate never proposes a second time", func(t *testing.T) {
		var d DetectorState
		failRun(&d, cfg, "n2", 0, 3)
		wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 2600 * ms}, cfg),
			DetectorAction{ProposeDown: []string{"n2"}})
		for _, at := range []time.Duration{2700 * ms, 3000 * ms, 4000 * ms} {
			wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: at}, cfg), DetectorAction{})
		}
	})

	t.Run("many duplicates from one observer are still one observer", func(t *testing.T) {
		var d DetectorState
		for i := 0; i < 10; i++ {
			d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: time.Duration(100+i) * ms}, cfg)
		}
		if len(d.observers["n2"]) != 1 {
			t.Fatalf("observers = %v, want exactly n3", d.observers["n2"])
		}
	})
}

// TestProbeCadenceIsStartToStart: the next probe is due one interval after
// the last one started, however long the last one took. [measure from completion]
func TestProbeCadenceIsStartToStart(t *testing.T) {
	cfg := leaderCfg()
	cfg.MemberIDs = []string{"n1", "n2"}
	var d DetectorState
	wantAction(t, d.Apply(ProbeTick{At: 0}, cfg), DetectorAction{Probe: []string{"n2"}})
	// the probe times out 500 ms after it started
	d.Apply(failed("n2", 0), cfg)
	wantAction(t, d.Apply(ProbeTick{At: 999 * ms}, cfg), DetectorAction{})
	wantAction(t, d.Apply(ProbeTick{At: 1000 * ms}, cfg), DetectorAction{Probe: []string{"n2"}})
	d.Apply(failed("n2", 1000*ms), cfg)
	wantAction(t, d.Apply(ProbeTick{At: 2000 * ms}, cfg), DetectorAction{Probe: []string{"n2"}})
	// a late tick restarts the cadence from the start it was probed at
	wantAction(t, d.Apply(ProbeTick{At: 2400 * ms}, cfg), DetectorAction{})
	wantAction(t, d.Apply(ProbeTick{At: 3000 * ms}, cfg), DetectorAction{Probe: []string{"n2"}})
}

// TestLeaderNeverProposesDownForItself: a member count, a report and a run of
// failures all naming the leader still never propose down or dead for it.
// [remove the self check]
func TestLeaderNeverProposesDownForItself(t *testing.T) {
	cfg := leaderCfg()
	var d DetectorState
	failRun(&d, cfg, "n1", 0, 4)
	for _, observer := range []string{"n2", "n3", "n4"} {
		wantAction(t, d.Apply(ReportReceived{Subject: "n1", Observer: observer, At: 3000 * ms}, cfg), DetectorAction{})
	}
	d.Apply(DownApplied{Subject: "n1", At: 3000 * ms}, cfg)
	got := d.Apply(ProbeTick{At: 3000*ms + cfg.Dead}, quiet(cfg))
	wantAction(t, got, DetectorAction{})
	// the same run for another subject does propose, so the test is not vacuous
	failRun(&d, cfg, "n2", 0, 3)
	wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 3100 * ms}, cfg),
		DetectorAction{ProposeDown: []string{"n2"}})
}

// TestStageTwoTimerOnlyDelays: the stage-2 timer is node-local and in memory;
// a leader change, a restart and a recovery each delay dead and none advances
// it. [persist the timer / advance it on a leader change]
func TestStageTwoTimerOnlyDelays(t *testing.T) {
	cfg := quiet(leaderCfg())
	down := 1000 * ms

	t.Run("dead is proposed at exactly Dead and not one nanosecond before", func(t *testing.T) {
		var d DetectorState
		d.Apply(DownApplied{Subject: "n2", At: down}, cfg)
		wantAction(t, d.Apply(ProbeTick{At: down + cfg.Dead - 1}, cfg), DetectorAction{})
		wantAction(t, d.Apply(ProbeTick{At: down + cfg.Dead}, cfg), DetectorAction{ProposeDead: []string{"n2"}})
		// the entry is spent: no second proposal
		wantAction(t, d.Apply(ProbeTick{At: down + cfg.Dead + 1000*ms}, cfg), DetectorAction{})
	})

	t.Run("a leader change one minute short of Dead keeps the timer and delays nothing earlier", func(t *testing.T) {
		var d DetectorState
		follower := quiet(followerCfg())
		d.Apply(DownApplied{Subject: "n2", At: down}, follower)
		d.Apply(LeaderChanged{LeaderID: "n1", At: down + cfg.Dead - 60000*ms}, cfg)
		wantAction(t, d.Apply(ProbeTick{At: down + cfg.Dead - 1}, cfg), DetectorAction{})
		wantAction(t, d.Apply(ProbeTick{At: down + cfg.Dead}, cfg), DetectorAction{ProposeDead: []string{"n2"}})
	})

	t.Run("the timer is this node's own application of the commit, so a later applier proposes later", func(t *testing.T) {
		var early, late DetectorState
		early.Apply(DownApplied{Subject: "n2", At: down}, cfg)
		late.Apply(DownApplied{Subject: "n2", At: down + 90000*ms}, cfg)
		at := down + cfg.Dead
		wantAction(t, early.Apply(ProbeTick{At: at}, cfg), DetectorAction{ProposeDead: []string{"n2"}})
		wantAction(t, late.Apply(ProbeTick{At: at}, cfg), DetectorAction{})
		wantAction(t, late.Apply(ProbeTick{At: at + 90000*ms}, cfg), DetectorAction{ProposeDead: []string{"n2"}})
	})

	t.Run("a restart forgets the timer: a fresh state proposes nothing", func(t *testing.T) {
		var restarted DetectorState
		wantAction(t, restarted.Apply(ProbeTick{At: down + cfg.Dead}, cfg), DetectorAction{})
		wantAction(t, restarted.Apply(ProbeTick{At: down + 2*cfg.Dead}, cfg), DetectorAction{})
	})

	t.Run("a node that answers a probe is not continuously down: no dead is proposed", func(t *testing.T) {
		var d DetectorState
		d.Apply(DownApplied{Subject: "n2", At: down}, cfg)
		d.Apply(answered("n2", down+120000*ms), cfg)
		wantAction(t, d.Apply(ProbeTick{At: down + cfg.Dead}, cfg), DetectorAction{})
	})

	t.Run("dead is never proposed for a subject that was never applied down", func(t *testing.T) {
		var d DetectorState
		failRun(&d, cfg, "n2", 0, 5)
		wantAction(t, d.Apply(ProbeTick{At: cfg.Dead * 3}, cfg), DetectorAction{})
	})
}

// refDetector is the reference model: it keeps the whole event log and
// recomputes every decision from it, with no running state, so it can only be
// wrong by being wrong about the rules, never about bookkeeping.
type refDetector struct {
	log []refEntry
}

type refEntry struct {
	ev  DetectorEvent
	cfg DetectorConfig
	out DetectorAction
}

func refLeader(cfg DetectorConfig) bool { return cfg.SelfID != "" && cfg.Leader == cfg.SelfID }

// resetIdx is the index of the last event that wiped the evidence recorded
// for subject: its probe answered, or any leader change. -1 if none.
func (r *refDetector) resetIdx(subject string) int {
	for i := len(r.log) - 1; i >= 0; i-- {
		switch e := r.log[i].ev.(type) {
		case ProbeResult:
			if e.Subject == subject && e.Answered {
				return i
			}
		case LeaderChanged:
			return i
		}
	}
	return -1
}

// accepted replays the reports for subject since the last reset and returns
// the arrivals that were accepted, in order, with their observers.
func (r *refDetector) accepted(subject string) (observers []string, arrivals []time.Duration) {
	failover := time.Duration(0)
	for i := r.resetIdx(subject) + 1; i < len(r.log); i++ {
		e, ok := r.log[i].ev.(ReportReceived)
		if !ok || e.Subject != subject {
			continue
		}
		failover = r.log[i].cfg.Failover
		if e.Observer == r.log[i].cfg.SelfID || slices.Contains(observers, e.Observer) {
			continue
		}
		if len(arrivals) > 0 && e.At > arrivals[0]+failover {
			continue
		}
		observers = append(observers, e.Observer)
		arrivals = append(arrivals, e.At)
	}
	return observers, arrivals
}

// consecutiveFails counts the failed probes of subject since its last answer.
func (r *refDetector) consecutiveFails(subject string) int {
	n := 0
	for i := len(r.log) - 1; i >= 0; i-- {
		if e, ok := r.log[i].ev.(ProbeResult); ok && e.Subject == subject {
			if e.Answered {
				return n
			}
			n++
		}
	}
	return n
}

func (r *refDetector) apply(ev DetectorEvent, cfg DetectorConfig) DetectorAction {
	var out DetectorAction
	switch e := ev.(type) {
	case ProbeTick:
		ids := cfg.VoterIDs
		if cfg.Voter {
			ids = cfg.MemberIDs
		}
		for _, id := range ids {
			if id == cfg.SelfID {
				continue
			}
			last, probed := time.Duration(0), false
			for i := len(r.log) - 1; i >= 0 && !probed; i-- {
				if tick, ok := r.log[i].ev.(ProbeTick); ok && slices.Contains(r.log[i].out.Probe, id) {
					last, probed = tick.At, true
				}
			}
			if !probed || e.At-last >= cfg.ProbeInterval {
				out.Probe = append(out.Probe, id)
			}
		}
		if refLeader(cfg) {
			subjects := map[string]bool{}
			for _, entry := range r.log {
				if down, ok := entry.ev.(DownApplied); ok {
					subjects[down.Subject] = true
				}
			}
			var names []string
			for s := range subjects {
				names = append(names, s)
			}
			sort.Strings(names)
			for _, s := range names {
				if s == cfg.SelfID {
					continue
				}
				since, live := r.downSince(s)
				if live && e.At-since >= cfg.Dead {
					out.ProposeDead = append(out.ProposeDead, s)
				}
			}
		}
	case ProbeResult:
		if e.Answered {
			break
		}
		n := r.consecutiveFails(e.Subject) + 1
		if n == cfg.Failures {
			out.Report = []string{e.Subject}
			_, arrivals := r.accepted(e.Subject)
			if refLeader(cfg) && e.Subject != cfg.SelfID && len(arrivals) > 0 && e.At <= arrivals[0]+cfg.Failover {
				out.ProposeDown = []string{e.Subject}
			}
		}
	case ReportReceived:
		before, _ := r.accepted(e.Subject)
		r.log = append(r.log, refEntry{ev: ev, cfg: cfg})
		after, arrivals := r.accepted(e.Subject)
		r.log = r.log[:len(r.log)-1]
		if len(after) > len(before) && refLeader(cfg) && e.Subject != cfg.SelfID &&
			r.consecutiveFails(e.Subject) >= cfg.Failures && e.At <= arrivals[0]+cfg.Failover {
			out.ProposeDown = []string{e.Subject}
		}
	}
	r.log = append(r.log, refEntry{ev: ev, cfg: cfg, out: out})
	return out
}

// downSince is the latest DownApplied for subject that no answered probe or
// proposed dead has followed.
func (r *refDetector) downSince(subject string) (time.Duration, bool) {
	for i := len(r.log) - 1; i >= 0; i-- {
		switch e := r.log[i].ev.(type) {
		case DownApplied:
			if e.Subject == subject {
				return e.At, true
			}
		case ProbeResult:
			if e.Subject == subject && e.Answered {
				return 0, false
			}
		}
		if slices.Contains(r.log[i].out.ProposeDead, subject) {
			return 0, false
		}
	}
	return 0, false
}

// modelStep is one generated event with the config it is applied under.
type modelStep struct {
	ev  DetectorEvent
	cfg DetectorConfig
}

// modelRun generates one seed's sequence online (DownApplied answers the
// machine's own ProposeDown), applies it to the machine and the reference,
// and returns every step and the machine's outputs.
func modelRun(t *testing.T, seed int64) (steps []modelStep, outs []DetectorAction) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	base := leaderCfg()
	base.Dead, base.Failover = 30000*ms, 5000*ms
	followers := base
	followers.Leader = "n2"
	cfg := base

	var d DetectorState
	var ref refDetector
	now := time.Duration(0)
	subjects := []string{"n1", "n2", "n3", "n4"}
	observers := []string{"n1", "n2", "n3", "n4"}
	var queue []modelStep // events the generator owes (bursts, DownApplied)

	emit := func(ev DetectorEvent) {
		steps = append(steps, modelStep{ev, cfg})
		got := d.Apply(ev, cfg)
		want := ref.apply(ev, cfg)
		if !reflect.DeepEqual(norm(got), norm(want)) {
			t.Fatalf("seed %d step %d %T %+v: machine %+v, reference %+v", seed, len(steps)-1, ev, ev, norm(got), norm(want))
		}
		outs = append(outs, got)
		for _, s := range got.ProposeDown {
			queue = append(queue, modelStep{DownApplied{Subject: s, At: now}, cfg})
		}
	}

	for len(steps) < 700 {
		now += time.Duration(50+rng.Intn(1450)) * ms
		if len(queue) > 0 && rng.Intn(10) < 7 {
			next := queue[0]
			queue = queue[1:]
			if da, ok := next.ev.(DownApplied); ok {
				da.At = now
				emit(da)
			}
			continue
		}
		subject := subjects[rng.Intn(len(subjects))]
		switch k := rng.Intn(100); {
		case k < 22: // tick
			emit(ProbeTick{At: now})
		case k < 37: // heartbeat seen
			emit(answered(subject, now))
		case k < 60: // heartbeat missed
			emit(ProbeResult{Subject: subject, StartedAt: now - 500*ms, At: now})
		case k < 75: // observer report
			emit(ReportReceived{Subject: subject, Observer: observers[rng.Intn(len(observers))], At: now})
		case k < 87: // partition: the subject stops answering for a few probes and others report it
			for i := 0; i < 3+rng.Intn(2); i++ {
				now += 1000 * ms
				emit(ProbeResult{Subject: subject, StartedAt: now - 500*ms, At: now})
				if rng.Intn(2) == 0 {
					now += time.Duration(rng.Intn(300)) * ms
					emit(ReportReceived{Subject: subject, Observer: observers[rng.Intn(len(observers))], At: now})
				}
			}
		case k < 94: // recovery
			emit(answered(subject, now))
			emit(ProbeTick{At: now + 1})
		case k < 97: // leader change
			if cfg.Leader == "n1" {
				cfg = followers
				emit(LeaderChanged{LeaderID: "n2", At: now})
			} else {
				cfg = base
				emit(LeaderChanged{LeaderID: "n1", At: now})
			}
		default: // a long quiet stretch so dead can come due
			now += time.Duration(5000+rng.Intn(20000)) * ms
			emit(ProbeTick{At: now})
		}
	}
	return steps, outs
}

// checkInvariants states the four rules in terms of the raw event log only,
// without the reference's replay or the machine's state.
func checkInvariants(t *testing.T, seed int64, steps []modelStep, outs []DetectorAction) (downs, deads int) {
	t.Helper()
	proposedDownAt := map[string][]int{} // subject -> step indexes of ProposeDown
	for i, out := range outs {
		cfg := steps[i].cfg
		at := stepTime(steps[i].ev)
		for _, s := range out.ProposeDown {
			downs++
			proposedDownAt[s] = append(proposedDownAt[s], i)
			if !refLeader(cfg) || s == cfg.SelfID {
				t.Fatalf("seed %d step %d: down for %q proposed by a non-leader or for self", seed, i, s)
			}
			// the leader's own observation: Failures consecutive failures
			run := 0
			for j := i; j >= 0; j-- {
				if pr, ok := steps[j].ev.(ProbeResult); ok && pr.Subject == s {
					if pr.Answered {
						break
					}
					run++
				}
			}
			if run < cfg.Failures {
				t.Fatalf("seed %d step %d: down for %q on %d failures, want >= %d", seed, i, s, run, cfg.Failures)
			}
			// the second observer: a report from another member since the
			// subject last answered or leadership last changed, inside Failover
			found := false
			for j := i; j >= 0; j-- {
				if pr, ok := steps[j].ev.(ProbeResult); ok && pr.Subject == s && pr.Answered {
					break
				}
				if _, ok := steps[j].ev.(LeaderChanged); ok {
					break
				}
				if rr, ok := steps[j].ev.(ReportReceived); ok && rr.Subject == s && rr.Observer != cfg.SelfID && at-rr.At <= cfg.Failover {
					found = true
				}
			}
			if !found {
				t.Fatalf("seed %d step %d: down for %q without a second observer inside the window (the leader's own observation alone, or a stale or cancelled report)", seed, i, s)
			}
		}
		for _, s := range out.ProposeDead {
			deads++
			if !refLeader(cfg) || s == cfg.SelfID {
				t.Fatalf("seed %d step %d: dead for %q proposed by a non-leader or for self", seed, i, s)
			}
			since, applied := time.Duration(0), false
			for j := i; j >= 0; j-- {
				if pr, ok := steps[j].ev.(ProbeResult); ok && pr.Subject == s && pr.Answered {
					break
				}
				if da, ok := steps[j].ev.(DownApplied); ok && da.Subject == s {
					since, applied = da.At, true
					break
				}
			}
			if !applied {
				t.Fatalf("seed %d step %d: dead for %q with no applied down", seed, i, s)
			}
			if at-since < cfg.Dead {
				t.Fatalf("seed %d step %d: dead for %q after %v, want >= %v", seed, i, s, at-since, cfg.Dead)
			}
			// never before down: the machine itself proposed down for it earlier
			before := false
			for _, idx := range proposedDownAt[s] {
				if idx < i {
					before = true
				}
			}
			if !before {
				t.Fatalf("seed %d step %d: dead for %q never preceded by a down proposal", seed, i, s)
			}
		}
	}
	return downs, deads
}

func stepTime(ev DetectorEvent) time.Duration {
	switch e := ev.(type) {
	case ProbeTick:
		return e.At
	case ProbeResult:
		return e.At
	case ReportReceived:
		return e.At
	case LeaderChanged:
		return e.At
	case DownApplied:
		return e.At
	}
	return 0
}

// TestDetectorModel runs 20 fixed seeds of random events (heartbeat seen,
// heartbeat missed, observer report, partition, recovery, leader change)
// against the reference model, checks the four rules on every output, and
// runs each seed twice to pin determinism.
func TestDetectorModel(t *testing.T) {
	totalDown, totalDead := 0, 0
	for seed := int64(1); seed <= 20; seed++ {
		steps, outs := modelRun(t, seed)
		again, outsAgain := modelRun(t, seed)
		if !reflect.DeepEqual(outs, outsAgain) || len(steps) != len(again) {
			t.Fatalf("seed %d: two runs of the same sequence gave different proposals", seed)
		}
		downs, deads := checkInvariants(t, seed, steps, outs)
		totalDown += downs
		totalDead += deads
		t.Logf("seed %2d: %d events, %d down proposals, %d dead proposals", seed, len(steps), downs, deads)
	}
	if totalDown == 0 || totalDead == 0 {
		t.Fatalf("the model test is vacuous: %d down and %d dead proposals over 20 seeds", totalDown, totalDead)
	}
}

// TestRecoveredNodeCancelsAPendingProposal: the observers recorded against a
// subject do not survive its recovery, so a fresh failure run needs fresh
// evidence. [keep the observers when a probe is answered]
func TestRecoveredNodeCancelsAPendingProposal(t *testing.T) {
	cfg := leaderCfg()
	var d DetectorState
	d.Apply(ReportReceived{Subject: "n2", Observer: "n3", At: 1000 * ms}, cfg)
	failRun(&d, cfg, "n2", 0, 2)
	d.Apply(answered("n2", 2500*ms), cfg) // recovered before the third failure
	got := failRun(&d, cfg, "n2", 10000*ms, 3)
	wantAction(t, got, DetectorAction{Report: []string{"n2"}}) // no proposal from the stale report
	wantAction(t, d.Apply(ReportReceived{Subject: "n2", Observer: "n4", At: 12600 * ms}, cfg),
		DetectorAction{ProposeDown: []string{"n2"}})
}
