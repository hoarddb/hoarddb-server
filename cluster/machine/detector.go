// Package machine holds the entry ring's pure state machines: values with an
// Apply method that take an event and an injected clock reading and return a
// decision. Nothing here reads a clock, starts a goroutine, takes a lock or
// touches a socket, so every decision table is testable row by row.
package machine

import (
	"sort"
	"time"
)

// DetectorConfig is the failure detector's input that does not change between
// events: who this node is, who leads, and the cluster timing keys.
type DetectorConfig struct {
	// SelfID is this node's id.
	SelfID string
	// Leader is the id this node is told leads; it equals SelfID exactly when
	// this node leads, and "" means not leader.
	Leader string
	// ProbeInterval is cluster.probe_interval_ms, the start-to-start cadence.
	ProbeInterval time.Duration
	// ProbeTimeout is cluster.probe_timeout_ms. The caller applies it to each
	// probe; the machine only receives the outcome.
	ProbeTimeout time.Duration
	// Failures is cluster.probe_failures.
	Failures int
	// Failover is cluster.failover_ms: a ceiling on when the second
	// observer's report may arrive, never a wait before proposing down.
	Failover time.Duration
	// Dead is cluster.dead_ms, the continuous time a member stays down
	// before stage 2.
	Dead time.Duration
	// Voter is true when this node probes every member; a non-voter probes
	// the voters.
	Voter bool
	// MemberIDs and VoterIDs are the ids to probe, in the order the actions
	// list them.
	MemberIDs, VoterIDs []string
}

// DetectorState is the detector's memory, all of it node-local and in memory.
type DetectorState struct {
	fails       map[string]int                      // consecutive failed probes per subject
	firstFailAt map[string]time.Duration            // start of the first probe of the current failure run
	reported    map[string]bool                     // one report per subject per failure run
	observers   map[string]map[string]time.Duration // subject -> distinct observer -> arrival
	downSince   map[string]time.Duration            // node-local stage-2 timer
	probeStart  map[string]time.Duration            // when the last probe of each subject was started
}

// DetectorEvent is an input to Apply.
type DetectorEvent interface{ isDetectorEvent() }

// ProbeTick says the caller's probe timer fired at At.
type ProbeTick struct{ At time.Duration }

// ProbeResult is the outcome of one probe of Subject.
type ProbeResult struct {
	Subject   string
	StartedAt time.Duration
	At        time.Duration
	Answered  bool
}

// ReportReceived is a FAILURE_REPORT from Observer about Subject that arrived
// at At on this node's clock.
type ReportReceived struct {
	Subject, Observer string
	At                time.Duration
}

// LeaderChanged says the Raft leader is now LeaderID.
type LeaderChanged struct {
	LeaderID string
	At       time.Duration
}

// DownApplied says this node applied the commit that set Subject down.
type DownApplied struct {
	Subject string
	At      time.Duration
}

func (ProbeTick) isDetectorEvent()      {}
func (ProbeResult) isDetectorEvent()    {}
func (ReportReceived) isDetectorEvent() {}
func (LeaderChanged) isDetectorEvent()  {}
func (DownApplied) isDetectorEvent()    {}

// DetectorAction is what the caller must do after an event.
type DetectorAction struct {
	// Probe lists the subjects to probe now.
	Probe []string
	// Report lists the subjects to report to the leader, once per failure run.
	Report []string
	// ProposeDown lists the subjects the leader proposes down for.
	ProposeDown []string
	// ProposeDead lists the subjects the leader proposes dead for.
	ProposeDead []string
}

// isLeader reports whether this node is the Raft leader the caller told it is.
func (c DetectorConfig) isLeader() bool {
	return c.SelfID != "" && c.Leader == c.SelfID
}

// probeTargets lists who this node probes: every member for a voter, the
// voters for a non-voter, never itself.
func (c DetectorConfig) probeTargets() []string {
	ids := c.VoterIDs
	if c.Voter {
		ids = c.MemberIDs
	}
	targets := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != c.SelfID {
			targets = append(targets, id)
		}
	}
	return targets
}

// Apply feeds one event to the detector and returns the decision. The zero
// DetectorState is ready to use.
func (d *DetectorState) Apply(ev DetectorEvent, cfg DetectorConfig) DetectorAction {
	d.ensureMaps()
	switch e := ev.(type) {
	case ProbeTick:
		return d.onTick(e, cfg)
	case ProbeResult:
		return d.onProbeResult(e, cfg)
	case ReportReceived:
		return d.onReport(e, cfg)
	case LeaderChanged:
		// Reports gathered under one leader are not evidence for the next:
		// the window is the leader's own clock, and a new leader has not
		// watched it. fails and downSince are this node's own observations
		// and survive.
		clear(d.observers)
	case DownApplied:
		d.downSince[e.Subject] = e.At
	}
	return DetectorAction{}
}

func (d *DetectorState) ensureMaps() {
	if d.fails != nil {
		return
	}
	d.fails = map[string]int{}
	d.firstFailAt = map[string]time.Duration{}
	d.reported = map[string]bool{}
	d.observers = map[string]map[string]time.Duration{}
	d.downSince = map[string]time.Duration{}
	d.probeStart = map[string]time.Duration{}
}

// onTick starts the probes whose interval has elapsed and, on the leader,
// proposes dead for every member that has been down for Dead.
func (d *DetectorState) onTick(e ProbeTick, cfg DetectorConfig) DetectorAction {
	var act DetectorAction
	for _, subject := range cfg.probeTargets() {
		// start to start (membership section 5): the cadence counts from when
		// the last probe began, so a slow probe never delays the next one.
		if began, probed := d.probeStart[subject]; probed && e.At-began < cfg.ProbeInterval {
			continue
		}
		d.probeStart[subject] = e.At
		act.Probe = append(act.Probe, subject)
	}
	if !cfg.isLeader() {
		return act
	}
	for _, subject := range d.sortedDownSubjects() {
		if subject == cfg.SelfID || e.At-d.downSince[subject] < cfg.Dead {
			continue
		}
		delete(d.downSince, subject)
		act.ProposeDead = append(act.ProposeDead, subject)
	}
	return act
}

func (d *DetectorState) sortedDownSubjects() []string {
	subjects := make([]string, 0, len(d.downSince))
	for subject := range d.downSince {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	return subjects
}

func (d *DetectorState) onProbeResult(e ProbeResult, cfg DetectorConfig) DetectorAction {
	subject := e.Subject
	if e.Answered {
		// Recovery cancels everything gathered against the subject: the
		// failure run, the second-observer reports that would have completed
		// a proposal, and the stage-2 timer, because a member that answers is
		// not continuously down.
		delete(d.fails, subject)
		delete(d.firstFailAt, subject)
		delete(d.reported, subject)
		delete(d.observers, subject)
		delete(d.downSince, subject)
		return DetectorAction{}
	}
	d.fails[subject]++
	if d.fails[subject] == 1 {
		d.firstFailAt[subject] = e.StartedAt
	}
	if d.fails[subject] < cfg.Failures || d.reported[subject] {
		return DetectorAction{}
	}
	d.reported[subject] = true
	act := DetectorAction{Report: []string{subject}}
	// The leader's own failures may be the last piece: a second observer's
	// report can already be in hand.
	if d.secondObserverInWindow(subject, e.At, cfg) {
		act.ProposeDown = []string{subject}
	}
	return act
}

func (d *DetectorState) onReport(e ReportReceived, cfg DetectorConfig) DetectorAction {
	if e.Observer == cfg.SelfID {
		return DetectorAction{} // the leader is the first observer, never the second
	}
	seen := d.observers[e.Subject]
	if _, again := seen[e.Observer]; again {
		return DetectorAction{} // distinct observers only
	}
	if len(seen) > 0 && e.At > firstArrival(seen)+cfg.Failover {
		return DetectorAction{} // outside the window
	}
	if seen == nil {
		seen = map[string]time.Duration{}
		d.observers[e.Subject] = seen
	}
	seen[e.Observer] = e.At
	if d.secondObserverInWindow(e.Subject, e.At, cfg) {
		return DetectorAction{ProposeDown: []string{e.Subject}}
	}
	return DetectorAction{}
}

// secondObserverInWindow is the whole of the leader's down rule (membership
// section 5): its own probes failed Failures times in a row, a different
// member's report names the subject, and now is still inside Failover of the
// first report's arrival. It never holds for the leader itself.
func (d *DetectorState) secondObserverInWindow(subject string, now time.Duration, cfg DetectorConfig) bool {
	if !cfg.isLeader() || subject == cfg.SelfID {
		return false
	}
	if d.fails[subject] < cfg.Failures {
		return false
	}
	seen := d.observers[subject]
	return len(seen) > 0 && now <= firstArrival(seen)+cfg.Failover
}

// firstArrival is the earliest recorded arrival; the window opens there.
func firstArrival(seen map[string]time.Duration) time.Duration {
	var first time.Duration
	started := false
	for _, at := range seen {
		if !started || at < first {
			first, started = at, true
		}
	}
	return first
}
