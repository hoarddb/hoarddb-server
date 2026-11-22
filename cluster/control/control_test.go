package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// stepClock advances one millisecond on every reading and logs each one, so a
// test can see what was read before what.
type stepClock struct {
	mu   sync.Mutex
	now  time.Duration
	step time.Duration
	log  *eventLog
	name string
}

func (c *stepClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += d
}

func (c *stepClock) setStep(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.step = d
}

func (c *stepClock) BootNow() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += c.step
	c.log.add(c.name + ":clock")
	return c.now
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *eventLog) index(event string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, got := range l.events {
		if got == event {
			return i
		}
	}
	return -1
}

// snapshot is a copy of the events so far.
func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// testNode is one Control behind a real loopback listener.
type testNode struct {
	id string
	// leaderView, when set, is who this node itself believes leads.
	leaderView atomic.Value
	control    *Control
	server     *transport.ChannelServer
	epoch      atomic.Uint64
	term       atomic.Uint64
	rtt        atomic.Int64
	// verifyTakes is how long the injected clock says VerifyLeader took.
	verifyTakes atomic.Int64
	log         *eventLog
	reports     chan machine.ReportReceived
	notices     chan [2]uint64
	requests    chan machine.RequestSent
	clock       *stepClock
	wallClock   atomic.Int64
}

type testCluster struct {
	t      *testing.T
	nodes  map[string]*testNode
	addrs  map[string]string
	leader atomic.Value
	log    *eventLog
}

func newCluster(t *testing.T, leader string, ids ...string) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, nodes: map[string]*testNode{}, addrs: map[string]string{}, log: &eventLog{}}
	tc.leader.Store(leader)
	for _, id := range ids {
		tc.addNode(id)
	}
	return tc
}

func (tc *testCluster) addNode(id string) *testNode {
	tc.t.Helper()
	n := &testNode{id: id, log: tc.log, reports: make(chan machine.ReportReceived, 8),
		notices: make(chan [2]uint64, 8), requests: make(chan machine.RequestSent, 8)}
	n.clock = &stepClock{now: time.Second, step: time.Millisecond, log: tc.log, name: id}
	n.wallClock.Store(int64(1_700_000_000 * time.Second))
	n.epoch.Store(5)
	n.term.Store(2)
	control, err := New(Config{
		SelfID:    id,
		StepDown:  leaseConfig.StepDown,
		Clock:     n.clock,
		Wall:      func() time.Duration { return time.Duration(n.wallClock.Add(int64(time.Millisecond))) },
		Committed: func() (uint64, uint64) { return n.epoch.Load(), 40 },
		Term:      n.term.Load,
		State:     func() cluster.MemberState { return cluster.MemberActive },
		VerifyLeader: func(context.Context) (time.Duration, error) {
			n.clock.advance(time.Duration(n.verifyTakes.Load()))
			return time.Duration(n.rtt.Load()), nil
		},
		Hello: func() transport.Handshake {
			tc.log.add(id + ":hello")
			return transport.Handshake{NodeID: id, Epoch: n.epoch.Load()}
		},
		Leader: func() (string, bool) {
			if view := n.leaderView.Load(); view != nil {
				return view.(string), true
			}
			return tc.leader.Load().(string), true
		},
		Members: func() []string {
			ids := make([]string, 0, len(tc.addrs))
			for member := range tc.addrs {
				ids = append(ids, member)
			}
			return ids
		},
		Addr: func(member string) (string, bool) { addr, ok := tc.addrs[member]; return addr, ok },
		RequestSent: func(ev machine.RequestSent) {
			tc.log.add(id + ":requestsent")
			n.control.Apply(ev, leaseConfig)
			n.requests <- ev
		},
		ReportReceived: func(ev machine.ReportReceived) { n.reports <- ev },
		NoticeReceived: func(epoch, stateSeq uint64) { n.notices <- [2]uint64{epoch, stateSeq} },
	})
	if err != nil {
		tc.t.Fatal(err)
	}
	n.control = control
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tc.t.Fatal(err)
	}
	n.server = transport.ServeChannel(ln, transport.ChannelControl, nil, n.control.Handle)
	tc.addrs[id] = ln.Addr().String()
	tc.nodes[id] = n
	tc.t.Cleanup(func() {
		if err := n.control.Close(); err != nil {
			tc.t.Errorf("%s: closing control: %v", id, err)
		}
		_ = n.server.Close() // already closed by a test that stopped this node
	})
	return n
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// askLease asks for a lease as the renewer does: with the round's tick reading, read from c's
// own clock just before the ask.
func askLease(t *testing.T, c *Control) (machine.GrantReceived, error) {
	t.Helper()
	return c.AskLease(testCtx(t), c.cfg.Clock.BootNow())
}

var leaseConfig = machine.LeaseConfig{Heartbeat: 100 * time.Millisecond, StepDown: 2 * time.Second, Members: 3, WallClock: true}

func TestGrantCarriesTheGrantorTerm(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	tc.nodes["b"].term.Store(9)
	grant, err := askLease(t, tc.nodes["a"].control)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Term != 9 {
		t.Fatalf("GrantReceived.Term = %d, want the grantor's term 9: a deposed leader's grant could not be told apart", grant.Term)
	}
}

func TestProbeUsesTheControlChannelAndAnyEpoch(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	tc.nodes["a"].epoch.Store(3)
	tc.nodes["b"].epoch.Store(8)
	for _, pair := range [][2]string{{"a", "b"}, {"b", "a"}} {
		result := tc.nodes[pair[0]].control.Probe(testCtx(t), pair[1])
		if !result.Answered || result.Subject != pair[1] {
			t.Fatalf("%s probing %s across epochs 3 and 8: %+v; the control channel is never fenced", pair[0], pair[1], result)
		}
		if result.At <= result.StartedAt {
			t.Fatalf("probe finished at %v, started at %v", result.At, result.StartedAt)
		}
	}
}

func TestProbeOfAnUnreachableMemberIsUnanswered(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	if err := tc.nodes["b"].server.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if result := tc.nodes["a"].control.Probe(ctx, "b"); result.Answered {
		t.Fatalf("a closed member answered: %+v", result)
	}
	if result := tc.nodes["a"].control.Probe(ctx, "nobody"); result.Answered {
		t.Fatalf("an unknown member answered: %+v", result)
	}
}

// TestLeaseRequestReadsTheClockBeforeTheWrite: the reading the request carries is the caller's,
// taken before the connection is opened and the request written (the renewer's tick reading).
func TestLeaseRequestReadsTheClockBeforeTheWrite(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	if _, err := askLease(t, tc.nodes["a"].control); err != nil {
		t.Fatal(err)
	}
	stamp, dial := tc.log.index("a:clock"), tc.log.index("a:hello")
	if stamp < 0 || dial < 0 || stamp > dial {
		t.Fatalf("event order %v: sentAt must be read before the connection is opened and the request written", tc.log.events)
	}
	sent := <-tc.nodes["a"].requests
	if sent.At != time.Second+time.Millisecond {
		t.Fatalf("RequestSent.At = %v, want the first clock reading %v", sent.At, time.Second+time.Millisecond)
	}
}

// TestALeaseRequestCarriesTheRoundsReading is spec lease-renewer-cadence C1 (R4): AskLease takes
// sentAt from its caller, the round's tick reading. The machine is told that reading, the round
// trip is measured from it, and AskLease reads no clock of its own before the request goes out.
func TestALeaseRequestCarriesTheRoundsReading(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	follower := tc.nodes["a"]
	at := follower.clock.BootNow() - 30*time.Millisecond // the tick, read before an earlier part of the round
	before := len(tc.log.snapshot())
	grant, err := follower.control.AskLease(testCtx(t), at)
	if err != nil {
		t.Fatal(err)
	}
	sent := <-follower.requests
	if sent.At != at {
		t.Fatalf("RequestSent.At = %v, want the reading passed in %v", sent.At, at)
	}
	if grant.RTT < grant.At-at {
		t.Fatalf("RTT %v is shorter than the time from the round's reading to the grant (%v)", grant.RTT, grant.At-at)
	}
	events := tc.log.snapshot()[before:]
	for _, event := range events {
		if event == "a:hello" {
			break
		}
		if event == "a:clock" {
			t.Fatalf("event order %v: AskLease read its own clock before the request; sentAt must be the caller's reading", events)
		}
	}
}

// TestRTTDerivesFromTheReadingRequestSentCarries: the round trip is measured
// from the very reading the lease machine was told, not from a second one.
func TestRTTDerivesFromTheReadingRequestSentCarries(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	grant, err := askLease(t, tc.nodes["a"].control)
	if err != nil {
		t.Fatal(err)
	}
	sent := <-tc.nodes["a"].requests
	if grant.RTT != grant.At-sent.At {
		t.Fatalf("RTT = %v but At - RequestSent.At = %v: they must come from the same sentAt", grant.RTT, grant.At-sent.At)
	}
}

func TestTheTickCarriesBothClocks(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	tick := tc.nodes["a"].control.Tick(testCtx(t))
	if tick.At == 0 || tick.Wall == 0 {
		t.Fatalf("Tick = %+v: a zero reading disables the lease's wall-clock cross-check", tick)
	}
	again := tc.nodes["a"].control.Tick(testCtx(t))
	if again.At <= tick.At || again.Wall <= tick.Wall {
		t.Fatalf("second tick %+v does not follow the first %+v on both clocks", again, tick)
	}
}

func TestGrantCarriesTheNodesCommittedEpoch(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	tc.nodes["b"].epoch.Store(6)
	grant, err := askLease(t, tc.nodes["a"].control)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Epoch != 6 || grant.CommittedEpoch != 5 {
		t.Fatalf("grant epoch %d, committed epoch %d; want the grantor's 6 and this node's own 5", grant.Epoch, grant.CommittedEpoch)
	}
}

func TestHigherEpochGrantStopsTheNode(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	follower := tc.nodes["a"].control
	ask := func() machine.LeaseDecision {
		t.Helper()
		grant, err := askLease(t, follower)
		if err != nil {
			t.Fatal(err)
		}
		return follower.Apply(grant, leaseConfig)
	}
	if d := ask(); !d.Valid {
		t.Fatalf("a normal grant did not give a lease: %+v", d)
	}
	tc.nodes["b"].epoch.Store(6)
	d := ask()
	if d.Valid || !d.Behind || !d.StepDown {
		t.Fatalf("a grant naming epoch 6 to a node at 5: %+v; the node must stop serving at once", d)
	}
}

func TestGrantRTTIsTheLargerOfBothMeasurements(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	tc.nodes["a"].clock.setStep(7 * time.Millisecond) // the receiver measures exactly 7ms: sentAt, then at
	for _, tt := range []struct {
		grantor, want time.Duration
	}{{40 * time.Millisecond, 40 * time.Millisecond}, {3 * time.Millisecond, 7 * time.Millisecond}} {
		tc.nodes["b"].rtt.Store(int64(tt.grantor))
		grant, err := askLease(t, tc.nodes["a"].control)
		if err != nil {
			t.Fatal(err)
		}
		if grant.RTT != tt.want {
			t.Fatalf("grantor measured %v, receiver 7ms: RTT = %v, want exactly %v", tt.grantor, grant.RTT, tt.want)
		}
	}
}

func TestLeaseRequestToANonLeaderFailsFast(t *testing.T) {
	tc := newCluster(t, "b", "a", "b", "c")
	tc.leader.Store("c") // a asks c, which is not what c's own view says: make c think b leads
	tc.nodes["c"].control.cfg.Leader = func() (string, bool) { return "b", true }
	start := time.Now()
	_, err := askLease(t, tc.nodes["a"].control)
	var wire *transport.WireError
	if !errors.As(err, &wire) {
		t.Fatalf("AskLease to a non-leader: %v, want the peer's refusal", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the refusal waited for a timeout")
	}
}

func TestSendReportNamesTheObserverAndFirstFailure(t *testing.T) {
	tc := newCluster(t, "b", "a", "b", "c")
	if err := tc.nodes["c"].server.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a := tc.nodes["a"].control
	if err := a.SendReport(ctx, "c"); err == nil {
		t.Fatal("a report for a member that never failed a probe was sent")
	}
	a.Probe(ctx, "c")
	a.Probe(ctx, "c")
	if err := a.SendReport(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	got := <-tc.nodes["b"].reports
	if got.Subject != "c" || got.Observer != "a" || got.At == 0 {
		t.Fatalf("leader saw %+v", got)
	}
	millis := a.firstFailureMillis("c")
	if millis == 0 {
		t.Fatal("no first-failure time recorded")
	}
	a.Probe(ctx, "b") // an answered probe of b changes nothing about c
	if a.firstFailureMillis("c") != millis {
		t.Fatal("a probe of another member moved c's first failure")
	}
}

func TestAnsweredProbeClearsTheFailureRun(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a := tc.nodes["a"].control
	saved := tc.addrs["b"]
	tc.addrs["b"] = "127.0.0.1:1"
	a.Probe(ctx, "b")
	tc.addrs["b"] = saved
	if a.firstFailureMillis("b") == 0 {
		t.Fatal("a failed probe left no first-failure time")
	}
	if r := a.Probe(ctx, "b"); !r.Answered {
		t.Fatalf("probe after recovery: %+v", r)
	}
	if a.firstFailureMillis("b") != 0 {
		t.Fatal("an answered probe did not clear the failure run")
	}
}

func TestNoticeReachesEveryOtherMember(t *testing.T) {
	tc := newCluster(t, "b", "a", "b", "c")
	tc.nodes["b"].control.Notice(7, 99)
	for _, id := range []string{"a", "c"} {
		select {
		case got := <-tc.nodes[id].notices:
			if got != [2]uint64{7, 99} {
				t.Fatalf("%s saw notice %v", id, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s never saw the notice", id)
		}
	}
	select {
	case got := <-tc.nodes["b"].notices:
		t.Fatalf("the leader noticed itself: %v", got)
	default:
	}
}

func TestDeadIsReProposedAfterARestart(t *testing.T) {
	cfg := machine.DetectorConfig{SelfID: "b", Leader: "b", Dead: 10 * time.Minute, ProbeInterval: time.Second,
		Failures: 3, MemberIDs: []string{"a", "b", "c"}, VoterIDs: []string{"a", "b", "c"}, Voter: true}
	state := cluster.ClusterState{Members: []cluster.Member{
		{ID: "a", State: cluster.MemberActive}, {ID: "b", State: cluster.MemberActive}, {ID: "c", State: cluster.MemberDown},
	}}
	start := time.Hour

	var restarted machine.DetectorState // a new process: the stage-2 timer is gone
	for _, ev := range RefeedDown(state, start) {
		restarted.Apply(ev, cfg)
	}
	act := restarted.Apply(machine.ProbeTick{At: start + cfg.Dead}, cfg)
	if len(act.ProposeDead) != 1 || act.ProposeDead[0] != "c" {
		t.Fatalf("after a restart with the re-feed, ProposeDead = %v; the member would never die", act.ProposeDead)
	}

	var bare machine.DetectorState
	if act := bare.Apply(machine.ProbeTick{At: start + 2*cfg.Dead}, cfg); len(act.ProposeDead) != 0 {
		t.Fatalf("a detector with no timer proposed dead: %v", act.ProposeDead)
	}

	// The proposal removed the timer; the commit failed, c is still down in the applied state.
	if again := restarted.Apply(machine.ProbeTick{At: start + cfg.Dead + time.Second}, cfg); len(again.ProposeDead) != 0 {
		t.Fatalf("dead was proposed twice without a re-feed: %v", again.ProposeDead)
	}
	retry := start + cfg.Dead + time.Minute
	for _, ev := range RefeedDown(state, retry) {
		restarted.Apply(ev, cfg)
	}
	if early := restarted.Apply(machine.ProbeTick{At: retry + cfg.Dead - time.Second}, cfg); len(early.ProposeDead) != 0 {
		t.Fatalf("the retry proposed dead early: %v", early.ProposeDead)
	}
	if late := restarted.Apply(machine.ProbeTick{At: retry + cfg.Dead}, cfg); len(late.ProposeDead) != 1 {
		t.Fatalf("the retry after a failed commit never re-proposed dead: %v", late.ProposeDead)
	}
}

func TestRefeedCoversOnlyDownMembers(t *testing.T) {
	state := cluster.ClusterState{Members: []cluster.Member{
		{ID: "a", State: cluster.MemberActive}, {ID: "c", State: cluster.MemberDown}, {ID: "d", State: cluster.MemberDead},
		{ID: "e", State: cluster.MemberJoining}, {ID: "f", State: cluster.MemberDown},
	}}
	got := RefeedDown(state, 5*time.Second)
	want := []machine.DownApplied{{Subject: "c", At: 5 * time.Second}, {Subject: "f", At: 5 * time.Second}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("RefeedDown = %v, want %v", got, want)
	}
}

func TestGrantIsRefusedWhenTheQuorumRoundTookTooLong(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	tc.nodes["b"].verifyTakes.Store(int64(leaseConfig.StepDown + time.Millisecond))
	if _, err := askLease(t, tc.nodes["a"].control); err == nil {
		t.Fatal("a grant was sent although VerifyLeader took longer than step_down_ms")
	}
	tc.nodes["b"].verifyTakes.Store(int64(leaseConfig.StepDown / 2))
	if _, err := askLease(t, tc.nodes["a"].control); err != nil {
		t.Fatalf("a quorum round inside step_down_ms was refused: %v", err)
	}
}

func TestNewRefusesAZeroStepDown(t *testing.T) {
	if _, err := New(Config{SelfID: "a"}); err == nil {
		t.Fatal("New accepted a zero StepDown")
	}
}

func TestAnAddressChangeIsDialled(t *testing.T) {
	tc := newCluster(t, "b", "a", "b")
	a := tc.nodes["a"].control
	if r := a.Probe(testCtx(t), "b"); !r.Answered {
		t.Fatalf("first probe: %+v", r)
	}
	if err := tc.nodes["b"].server.Close(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	moved := transport.ServeChannel(ln, transport.ChannelControl, nil, tc.nodes["b"].control.Handle)
	t.Cleanup(func() { _ = moved.Close() }) // best-effort test cleanup
	tc.addrs["b"] = ln.Addr().String()
	if r := a.Probe(testCtx(t), "b"); !r.Answered {
		t.Fatalf("probe after b moved to %s: %+v; the cached client kept the old address", tc.addrs["b"], r)
	}
}

// TestProbeTravelsOnTheControlChannel reads the frame a probe puts on the
// wire: the control handshake channel, the probe opcode, the PROBE kind.
func TestProbeTravelsOnTheControlChannel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }() // best-effort test cleanup
	seen := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }() // best-effort test cleanup
		hello, err := transport.ReadHandshake(conn)
		if err != nil {
			seen <- "handshake: " + err.Error()
			return
		}
		frame, err := transport.DecodeFrame(conn)
		if err != nil {
			seen <- "frame: " + err.Error()
			return
		}
		env, msg, err := transport.DecodeControl(frame.Payload)
		if err != nil {
			seen <- "payload: " + err.Error()
			return
		}
		seen <- fmt.Sprintf("channel=%d opcode=%s kind=0x%02x", hello.Channel, frame.Opcode, env.Kind)
		reply, err := transport.EncodeControl(transport.Envelope{}, transport.ProbeReply{Nonce: msg.(transport.Probe).Nonce, State: byte(cluster.MemberActive)})
		if err != nil {
			return
		}
		_ = transport.WriteFrame(conn, transport.NewFrameWithID(transport.OpClusterProbeReply, frame.RequestID, reply)) // the test reads seen, not this write
	}()
	tc := newCluster(t, "b", "a")
	tc.addrs["raw"] = ln.Addr().String()
	if r := tc.nodes["a"].control.Probe(testCtx(t), "raw"); !r.Answered {
		t.Fatalf("probe of the raw peer: %+v", r)
	}
	want := fmt.Sprintf("channel=%d opcode=%s kind=0x%02x", transport.HandshakeChannelControl, transport.OpClusterProbe, transport.KindProbe)
	if got := <-seen; got != want {
		t.Fatalf("probe on the wire: %s, want %s", got, want)
	}
}

// TestAFailureReportToADeposedLeaderIsRefusedNotAcknowledged: the leader the reporter has
// cached stepped down; if it acknowledged the report the reporter would think it delivered
// and never send it to the new leader, and the second observer would be lost.
func TestAFailureReportToADeposedLeaderIsRefusedNotAcknowledged(t *testing.T) {
	tc := newCluster(t, "b", "a", "b", "c")
	if err := tc.nodes["c"].server.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	a := tc.nodes["a"].control
	a.Probe(ctx, "c")
	// a still believes b leads; b has been deposed and its own view now names c.
	tc.nodes["b"].leaderView.Store("c")
	if err := a.SendReport(ctx, "c"); err == nil {
		t.Fatal("a report to a node that no longer leads was acknowledged; the reporter must be told to resend to the new leader")
	}
	select {
	case got := <-tc.nodes["b"].reports:
		t.Fatalf("the deposed leader recorded the report %+v", got)
	default:
	}
}
