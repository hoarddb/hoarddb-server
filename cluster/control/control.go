// Package control wires the control channel of conventions section 6.1
// (kinds 0x08 to 0x0D) to the pure machines of cluster/machine: it asks for
// and grants leases, probes members, sends failure reports and cluster
// notices, and hands the machines the readings they need. It lives apart from
// package cluster because cluster/machine imports cluster, so cluster cannot
// import the machine types this package returns.
//
// The control channel is never fenced on the envelope's epoch (membership
// section 7): a probe, a report and a notice are answered whatever epoch the
// sender is at. Only a LEASE_GRANT's own epoch is compared, by the machine.
package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// noticeTimeout bounds one CLUSTER_NOTICE send; a notice is best effort.
const noticeTimeout = 2 * time.Second

// Config is everything a Control reads from the node around it. The function
// fields are read at the moment they are needed, never cached.
type Config struct {
	// SelfID is this node's id.
	SelfID string
	// StepDown is cluster.step_down_ms: a grant is sent only if the grantor's
	// own VerifyLeader succeeded within it. It must be positive.
	StepDown time.Duration
	// Clock is the lease clock (CLOCK_BOOTTIME on Linux).
	Clock bootclock.Clock
	// Wall reads the wall clock for the lease's cross-check; any base works,
	// only differences are used.
	Wall func() time.Duration
	// Committed is this node's committed epoch and state_seq.
	Committed func() (epoch, stateSeq uint64)
	// Term is this node's Raft term.
	Term func() uint64
	// State is this node's own member state, answered in PROBE_REPLY.
	State func() cluster.MemberState
	// VerifyLeader runs the leader's quorum round and returns the round trip
	// it measured. A grant is sent only if it succeeds (conventions section 15).
	VerifyLeader func(ctx context.Context) (time.Duration, error)
	// Hello is this node's handshake for a new connection; the channel is set
	// by the transport.
	Hello func() transport.Handshake
	// Leader is the id of the current Raft leader, if known.
	Leader func() (id string, ok bool)
	// Members lists every member id; Addr resolves one to its control address.
	Members func() []string
	Addr    func(id string) (addr string, ok bool)

	// Dial opens the connection to a member's control address; nil dials plain TCP. A node
	// with pinned TLS supplies the same dialer its other channels use.
	Dial func(ctx context.Context, member, addr string) (net.Conn, error)

	// RequestSent is called with the clock reading taken before a LEASE_REQ
	// is written, so the lease machine can match the grant's nonce.
	RequestSent func(machine.RequestSent)
	// ReportReceived delivers a FAILURE_REPORT to the detector.
	ReportReceived func(machine.ReportReceived)
	// NoticeReceived delivers a CLUSTER_NOTICE; the caller re-reads the
	// committed state when the notice is ahead of it.
	NoticeReceived func(epoch, stateSeq uint64)
	// Relay answers a RELAY_PROPOSAL. from is the pinned, committed identity of the sender;
	// nothing in the request names it. Nil refuses the kind.
	Relay func(ctx context.Context, from transport.Handshake, req transport.RelayProposal) transport.RelayReply
	// Membership answers a MEMBERSHIP_RELAY the same way; nil refuses the kind.
	Membership func(ctx context.Context, from transport.Handshake, req transport.MembershipRelay) transport.MembershipRelayReply
	// Pepper answers a PEPPER_REQUEST (ring user store spec 5.3). from is the pinned, committed
	// identity the channel admitted; the answer re-checks the asker's committed row itself. nil
	// refuses the kind.
	Pepper func(ctx context.Context, from transport.Handshake, req transport.PepperRequest) transport.PepperReply
}

// Control is a node's control channel, client and server side.
type Control struct {
	cfg   Config
	nonce atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	notice sync.WaitGroup

	mu           sync.Mutex
	clients      map[string]memberClient
	firstFailure map[string]uint64
	lease        machine.LeaseState
}

// memberClient is the connection to a member and the address it was made for.
type memberClient struct {
	addr   string
	client *transport.ChannelClient
}

// New returns a Control. Close it to end its connections and goroutines. It
// refuses a zero StepDown: without it the grant bound would be vacuous.
func New(cfg Config) (*Control, error) {
	if cfg.StepDown <= 0 {
		return nil, errors.New("control: StepDown is not set; pass the committed step_down_ms (default 2000 ms) so a grant is bounded by the grantor's own quorum round")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Control{cfg: cfg, ctx: ctx, cancel: cancel,
		clients: map[string]memberClient{}, firstFailure: map[string]uint64{}}, nil
}

// Close cancels in-flight notices, closes every connection and waits for the
// notice goroutines.
func (c *Control) Close() error {
	c.cancel()
	c.notice.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for id, entry := range c.clients {
		errs = append(errs, entry.client.Close())
		delete(c.clients, id)
	}
	return errors.Join(errs...)
}

// client returns the connection to a member, created on first use and
// rebuilt, closing the old one, when the member's address has changed.
func (c *Control) client(id string) (*transport.ChannelClient, error) {
	addr, ok := c.cfg.Addr(id)
	if !ok {
		return nil, fmt.Errorf("member %q has no control address; run 'nodes' to list the members", id)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.clients[id]
	if ok && entry.addr == addr {
		return entry.client, nil
	}
	if ok {
		// Best-effort: the old connection is for an address the member left.
		_ = entry.client.Close()
	}
	client := transport.NewChannelClient(addr, transport.ChannelControl, c.cfg.Hello)
	if c.cfg.Dial != nil {
		client.SetDialer(func(ctx context.Context, addr string) (net.Conn, error) { return c.cfg.Dial(ctx, id, addr) })
	}
	c.clients[id] = memberClient{addr: addr, client: client}
	return client, nil
}

// envelope is the sender's position every message carries.
func (c *Control) envelope() transport.Envelope {
	epoch, stateSeq := c.cfg.Committed()
	return transport.Envelope{Epoch: epoch, StateSeq: stateSeq}
}

func (c *Control) call(ctx context.Context, member string, msg transport.ControlMessage) (transport.ControlMessage, error) {
	reply, _, err := c.callTimed(ctx, member, msg)
	return reply, err
}

// callTimed is call that also reports the wait for the connection's turn.
func (c *Control) callTimed(ctx context.Context, member string, msg transport.ControlMessage) (transport.ControlMessage, transport.TurnWait, error) {
	client, err := c.client(member)
	if err != nil {
		return nil, transport.TurnWait{}, err
	}
	req, err := transport.NewControlMessage(c.envelope(), msg)
	if err != nil {
		return nil, transport.TurnWait{}, fmt.Errorf("encode %T for %s: %w", msg, member, err)
	}
	reply, wait, err := client.CallTimed(ctx, req)
	if err != nil {
		return nil, wait, fmt.Errorf("control call to %s: %w", member, err)
	}
	if reply == nil {
		return nil, wait, nil
	}
	decoded, err := reply.Control()
	if err != nil {
		return nil, wait, fmt.Errorf("control reply from %s: %w", member, err)
	}
	return decoded, wait, nil
}

// AskLease sends LEASE_REQ to the leader and returns the grant as the lease
// machine's event. sentAt is the caller's reading of the lease clock, taken
// before the connection is opened or anything is written: the renewer passes
// its round's tick (lease-renewer-cadence-spec.md R4), so the next tick, one
// heartbeat after it, always finds a request due, and a slow write or wait for
// the connection only shortens the lease. The RTT is the larger of the
// grantor's own measurement and this node's At - sentAt. The caller feeds the
// result to Apply.
func (c *Control) AskLease(ctx context.Context, sentAt time.Duration) (machine.GrantReceived, error) {
	grant, _, err := c.AskLeaseRound(ctx, sentAt)
	return grant, err
}

// LeaseRound is what one lease ask measured besides the grant: the grantor's own round trip and
// the wait for the control connection's turn, with the kind of call that held it.
type LeaseRound struct {
	// Grantor is the round trip the grantor reported for its VerifyLeader; 0 when the ask failed.
	Grantor time.Duration
	// Turn is the wait for the control connection's turn and the call that held it.
	Turn transport.TurnWait
}

// AskLeaseRound is AskLease that also returns what the round measured. The measurement changes
// no lease rule: the grant is the same value AskLease returns.
func (c *Control) AskLeaseRound(ctx context.Context, sentAt time.Duration) (machine.GrantReceived, LeaseRound, error) {
	leader, ok := c.cfg.Leader()
	if !ok {
		return machine.GrantReceived{}, LeaseRound{}, errors.New("no leader is known yet; run 'nodes' to see who is up, the lease request retries at the next heartbeat")
	}
	nonce := c.nonce.Add(1)
	if c.cfg.RequestSent != nil {
		c.cfg.RequestSent(machine.RequestSent{At: sentAt, Nonce: nonce})
	}
	reply, turn, err := c.callTimed(ctx, leader, transport.LeaseReq{Nonce: nonce})
	at, wall := c.cfg.Clock.BootNow(), c.cfg.Wall()
	round := LeaseRound{Turn: turn}
	if err != nil {
		return machine.GrantReceived{}, round, err
	}
	grant, ok := reply.(transport.LeaseGrant)
	if !ok {
		return machine.GrantReceived{}, round, fmt.Errorf("leader %s answered LEASE_REQ with %T, want LEASE_GRANT", leader, reply)
	}
	committed, _ := c.cfg.Committed()
	round.Grantor = time.Duration(grant.RTTMicros) * time.Microsecond
	return machine.GrantReceived{
		At: at, Wall: wall, Nonce: grant.Nonce, Epoch: grant.GrantEpoch, CommittedEpoch: committed, Term: grant.GrantTerm,
		RTT: max(round.Grantor, at-sentAt),
	}, round, nil
}

// Apply feeds one event to this node's lease. The caller writes no field of
// the lease state, with one exception documented as a spec gap: the machine
// compares a grant's term with LeaseState.Term, which U8 says the caller sets
// and conventions section 15 says nobody writes. Control is that caller and
// sets it to this node's own Raft term before every event.
func (c *Control) Apply(ev machine.LeaseEvent, cfg machine.LeaseConfig) machine.LeaseDecision {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lease.Term = c.cfg.Term()
	return c.lease.Apply(ev, cfg)
}

// Probe sends PROBE to subject. The caller's ctx carries cluster.probe_timeout_ms.
// A reply with the wrong nonce or an unknown state byte is not an answer. A
// failure starts the subject's failure run, whose first wall-clock time
// FAILURE_REPORT carries; an answer ends it.
func (c *Control) Probe(ctx context.Context, subject string) machine.ProbeResult {
	startedAt, startWall := c.cfg.Clock.BootNow(), c.cfg.Wall()
	nonce := c.nonce.Add(1)
	reply, err := c.call(ctx, subject, transport.Probe{Nonce: nonce})
	answered := false
	if err == nil {
		if got, ok := reply.(transport.ProbeReply); ok && got.Nonce == nonce {
			_, stateErr := cluster.DecodeMemberState(got.State)
			answered = stateErr == nil
		}
	}
	c.mu.Lock()
	if answered {
		delete(c.firstFailure, subject)
	} else if _, running := c.firstFailure[subject]; !running {
		c.firstFailure[subject] = uint64(startWall / time.Millisecond)
	}
	c.mu.Unlock()
	return machine.ProbeResult{Subject: subject, StartedAt: startedAt, At: c.cfg.Clock.BootNow(), Answered: answered}
}

// firstFailureMillis is the wall-clock millisecond of the subject's first
// failed probe in the current run, or 0 when it has none.
func (c *Control) firstFailureMillis(subject string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.firstFailure[subject]
}

// SendReport sends FAILURE_REPORT for subject to the leader. It is an error
// to report a member that has no failed probe on record. The leader is the
// first observer and keeps its own count, so on the leader it sends nothing.
func (c *Control) SendReport(ctx context.Context, subject string) error {
	first := c.firstFailureMillis(subject)
	if first == 0 {
		return fmt.Errorf("no failed probe of %s is on record, so there is nothing to report", subject)
	}
	leader, ok := c.cfg.Leader()
	if !ok {
		return errors.New("no leader is known yet, so the failure report cannot be delivered")
	}
	if leader == c.cfg.SelfID {
		return nil
	}
	_, err := c.call(ctx, leader, transport.FailureReport{Subject: subject, FirstFailureAtMillis: first})
	return err
}

// Notice tells every other member that the committed state moved to epoch and
// stateSeq. It is best effort and returns at once; a member that misses it
// learns from the next frame it receives.
func (c *Control) Notice(epoch, stateSeq uint64) {
	for _, member := range c.cfg.Members() {
		if member == c.cfg.SelfID {
			continue
		}
		c.notice.Add(1)
		go func() {
			defer c.notice.Done()
			ctx, cancel := context.WithTimeout(c.ctx, noticeTimeout)
			defer cancel()
			// A missed notice is repaired by the next frame's epoch.
			_ = c.callNotice(ctx, member, epoch, stateSeq)
		}()
	}
}

func (c *Control) callNotice(ctx context.Context, member string, epoch, stateSeq uint64) error {
	client, err := c.client(member)
	if err != nil {
		return err
	}
	req, err := transport.NewControlMessage(transport.Envelope{Epoch: epoch, StateSeq: stateSeq}, transport.ClusterNotice{Epoch: epoch, StateSeq: stateSeq})
	if err != nil {
		return err
	}
	if _, err := client.Call(ctx, req); err != nil {
		return fmt.Errorf("cluster notice to %s: %w", member, err)
	}
	return nil
}

// Tick reads both clocks for one renewer tick. The wall reading is what lets
// the lease catch a clock step that a single monotonic source cannot.
func (c *Control) Tick(context.Context) machine.Tick {
	return machine.Tick{At: c.cfg.Clock.BootNow(), Wall: c.cfg.Wall()}
}

// Handle answers one control request. It never compares the envelope's epoch.
func (c *Control) Handle(ctx context.Context, from transport.Handshake, req transport.Message) (*transport.Message, error) {
	msg, err := req.Control()
	if err != nil {
		return nil, err
	}
	var reply transport.ControlMessage
	switch m := msg.(type) {
	case transport.LeaseReq:
		reply, err = c.grant(ctx, m)
	case transport.Probe:
		_, stateSeq := c.cfg.Committed()
		reply = transport.ProbeReply{Nonce: m.Nonce, State: byte(c.cfg.State()), AppliedStateSeq: stateSeq}
	case transport.FailureReport:
		// Only the leader can act on a report. A node that has been deposed answers with an
		// error, not an acknowledgement, so the reporter sends it again to whoever leads now
		// (the detector reports a failure run once).
		if leader, known := c.cfg.Leader(); !known || leader != c.cfg.SelfID {
			return nil, fmt.Errorf("%s is not the leader (%q is); send the failure report there", c.cfg.SelfID, leader)
		}
		if c.cfg.ReportReceived != nil {
			c.cfg.ReportReceived(machine.ReportReceived{Subject: m.Subject, Observer: from.NodeID, At: c.cfg.Clock.BootNow()})
		}
	case transport.ClusterNotice:
		if c.cfg.NoticeReceived != nil {
			c.cfg.NoticeReceived(m.Epoch, m.StateSeq)
		}
	case transport.RelayProposal:
		if c.cfg.Relay == nil {
			return nil, errors.New("this node does not take relayed proposals; send the command to the Raft leader")
		}
		reply = c.cfg.Relay(ctx, from, m)
	case transport.MembershipRelay:
		if c.cfg.Membership == nil {
			return nil, errors.New("this node does not take relayed membership commands; run the statement on the Raft leader")
		}
		reply = c.cfg.Membership(ctx, from, m)
	case transport.PepperRequest:
		if c.cfg.Pepper == nil {
			return nil, errors.New("this node does not hand out the cluster's password key; ask another member")
		}
		reply = c.cfg.Pepper(ctx, from, m)
	default:
		err = fmt.Errorf("%T is a reply, not a request", msg)
	}
	if err != nil || reply == nil {
		return nil, err
	}
	out, err := transport.NewControlMessage(c.envelope(), reply)
	if err != nil {
		return nil, fmt.Errorf("encode %T: %w", reply, err)
	}
	return &out, nil
}

// grant answers LEASE_REQ, only as the leader and only if its own quorum round
// just succeeded, so a minority-side leader cannot keep a lease alive.
func (c *Control) grant(ctx context.Context, req transport.LeaseReq) (transport.ControlMessage, error) {
	leader, ok := c.cfg.Leader()
	if !ok || leader != c.cfg.SelfID {
		return nil, fmt.Errorf("%s is not the leader; ask %q, or run 'nodes' to see who leads", c.cfg.SelfID, leader)
	}
	began := c.cfg.Clock.BootNow()
	rtt, err := c.cfg.VerifyLeader(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s could not confirm it still leads, so it grants no lease: %w", c.cfg.SelfID, err)
	}
	// Conventions section 15: a grant is sent only if VerifyLeader agreed
	// within step_down_ms, measured here on the lease clock.
	rtt = max(rtt, c.cfg.Clock.BootNow()-began)
	if rtt >= c.cfg.StepDown {
		return nil, fmt.Errorf("%s confirmed it leads only after %v, which is not within step_down_ms (%v), so it grants no lease; the next heartbeat asks again",
			c.cfg.SelfID, rtt, c.cfg.StepDown)
	}
	epoch, _ := c.cfg.Committed()
	micros := min(rtt.Microseconds(), math.MaxInt32)
	return transport.LeaseGrant{Nonce: req.Nonce, GrantEpoch: epoch, GrantTerm: c.cfg.Term(), RTTMicros: int32(max(micros, 0))}, nil
}

// RefeedDown returns a DownApplied for every member the applied state shows
// as down, stamped at. The detector proposes dead once and removes its stage-2
// timer, so a restart or a dead commit that failed would otherwise leave the
// member down forever: call this at start, and again after a failed commit.
// The timer restarts at at, which is late and never early.
func RefeedDown(state cluster.ClusterState, at time.Duration) []machine.DownApplied {
	var events []machine.DownApplied
	for _, member := range state.Members {
		if member.State == cluster.MemberDown {
			events = append(events, machine.DownApplied{Subject: member.ID, At: at})
		}
	}
	return events
}
