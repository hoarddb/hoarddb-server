// Package harness runs N cluster nodes in one process for tests: each node has its own
// directory, a real Badger, a real log, a real Raft group and the real peer transport
// (pinned mutual TLS over loopback TCP on 127.0.0.1:0), and the fault layer of faults.go
// sits between TCP and TLS on every dial. It is a non-test package so tests in other
// packages can use it.
//
// It uses the same transport code path as a deployed node, not an in-memory pipe: the only
// difference is the thin connection wrapper that can delay and cut a dialed connection.
// The node clock is the real one (SPEC-GAP U57-6: the nodes read bootclock directly, so a
// clock jump cannot be injected without a clock seam in the node).
package harness

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/bits"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Options configure NewCluster.
type Options struct {
	// FastDetection commits short probe settings (100 ms interval, 2 failures) so a dead
	// node is marked down within about a second; false keeps the committed defaults
	// (1 s interval, 3 failures).
	FastDetection bool
	// NoDetection commits probe_failures 1000 so no member is marked down during the test:
	// a node that is killed and restarted stays an active member. Without it (and without
	// FastDetection) the committed defaults mark a node down after about three seconds.
	NoDetection bool
	// NoProbes commits a one-hour probe interval so the failure detector never probes during
	// the test: nothing but the replicator's own retry can notice that a copy came back.
	NoProbes bool
	// RaftTimeout is the Raft heartbeat and election timeout; 0 takes 150 ms. A test that
	// stalls a peer raises it so the stall is not also a leadership flap.
	RaftTimeout time.Duration
	// WCTimeout is replication.wc_timeout_ms for every node; 0 takes 500 ms.
	WCTimeout time.Duration
	// ClientAddr names the client address a node registers, given its id and its data address
	// (the cluster refuses a client address on any other host); nil registers none.
	ClientAddr func(id, dataAddr string) string
	// Seed is the replication.seed_* bounds of every node; zero fields take the defaults.
	Seed wire.SeedOptions
	// Late is how many extra identities are keyed and pinned at the start but not opened:
	// AddNode opens and joins them one at a time, the way a node is added to a running
	// database.
	Late int
	// Learners is how many of the last nodes are joined as non-voting, inactive members: they
	// are registered and replicate but are neither promoted nor activated.
	Learners int
	// ProposalBudget is how long a cluster command waits for a Raft leader; 0 takes the node's default.
	ProposalBudget time.Duration
	// Voters is cluster.voters on every node (3 or 5): a promotion past it is refused and a join
	// takes a voter slot only while one is free. 0 sets no cap.
	Voters int
	// Witnesses lists the nodes configured with cluster.witness: they say so when asked who
	// they are, and a join registers them as witnesses.
	Witnesses []string
	// LockMembership lists the nodes opened with cluster.allow_membership false.
	LockMembership []string
	// JoinerActivation lets a joining node ask the leader to activate it once its pairs are
	// ready, as a server does. Off by default, so a test that holds a joiner joining keeps it
	// joining.
	JoinerActivation bool
	// HoldSeedReports stops every node's seed reporter from sending SEEDED and RETIRE, so
	// state_seq moves only when the test moves it. A test that builds a command on an exact
	// state_seq, or counts the steps state_seq took, sets it; a test about the reporter
	// itself does not.
	HoldSeedReports bool
	// StateRanges names the nodes that declare a state-level range other than the build's, by
	// node id; SetStateRange changes it for a node's next open. Nil declares the build's range
	// everywhere.
	StateRanges map[string]cluster.StateRange
	// Timings is the profile (P0 or P1) committed on the leader after formation, before
	// NewCluster returns. It is a test bug to set it together with FastDetection,
	// NoDetection or NoProbes.
	Timings map[string]int64
	// CatchUpChunkRecords is how many records one catch-up chunk carries on every node; 0
	// takes the default (1,024). A test lowers it so a copy a few records behind needs
	// several requests to catch up.
	CatchUpChunkRecords int
	// ReclaimKeysPerSecond is storage.reclaim_keys_per_second on every node; 0 takes the
	// default (10,000). A test lowers it so the rate-limited delete of a range takes seconds.
	ReclaimKeysPerSecond int
	// NoDeferredGate opens every node with the deferred-pair gate switched off, so a pair whose
	// boot replay has not finished answers its peers from its low applied position, as before
	// the gate. Only the experiment that shows what the gate removes sets it.
	NoDeferredGate bool
	// CaptureLog replaces the null Raft logger with one the test can read through LogLines, so a
	// test sees the operator lines the nodes write at Info and above (the lease lapse line).
	CaptureLog bool
	// ClusterPepper gives every node a cluster password key file (auth.cluster.pepper beside its
	// data directory, the plaintext form), so it generates, serves and fetches the key as a server
	// does (ring user store spec 5). Off, a node keeps no key.
	ClusterPepper bool
	// AtRest, when set, gives node id the keys its ring store and log are sealed under
	// (wire.NodeConfig.AtRest); nil runs every node in plaintext.
	AtRest func(id string) wire.AtRestKeys
	// BeforePepperFetch, when set, runs before each PEPPER_REQUEST node id sends to member; an
	// error stops that ask. A test holds a joiner without the key with it.
	BeforePepperFetch func(id, member string) error
}

// Cluster is N nodes in one process.
type Cluster struct {
	tb     testing.TB
	root   string
	opts   Options
	faults *Faults

	mu      sync.Mutex
	ids     []string
	members map[string]*member
	errs    []string
	// spare holds the keys of identities that are in the id=fingerprint map but are not
	// cluster members (a decommissioned node, a joiner not yet registered).
	spare map[string]string
	// late are the identities keyed at the start and not yet opened, with their listeners.
	late     []string
	lateLsnr map[string]net.Listener
	// logs holds the nodes' Raft log lines when Options.CaptureLog is set.
	logs logLines
	// heldReplays says HoldBootReplays installed the process-wide hold, which close removes.
	heldReplays bool
}

type member struct {
	id     string
	dir    string
	addr   string
	trust  *peer.Trust
	fp     string
	disk   *diskFault
	acks   *ackFault
	node   *wire.Node
	killed bool
	// slowStep is how long each supervisor step of the node waits before its control calls.
	slowStep atomic.Int64
}

// NewCluster opens n nodes named n1..nN as one cluster of n voters: n1 bootstraps, the
// others join, are promoted and are activated, and the call returns when every node has
// applied the same committed state.
func NewCluster(tb testing.TB, n int, opts Options) *Cluster {
	tb.Helper()
	if len(opts.Timings) > 0 {
		for name, set := range map[string]bool{"FastDetection": opts.FastDetection, "NoDetection": opts.NoDetection, "NoProbes": opts.NoProbes} {
			if set {
				tb.Fatalf("harness: Options.Timings and Options.%s both set probe settings; use one", name)
			}
		}
		MustValidateTimings(tb, opts.Timings)
	}
	if opts.WCTimeout == 0 {
		opts.WCTimeout = 500 * time.Millisecond
	}
	c := &Cluster{tb: tb, root: tb.TempDir(), opts: opts, faults: newFaults(), members: map[string]*member{}, spare: map[string]string{}}
	tb.Cleanup(c.close)
	listeners := map[string]net.Listener{}
	var pins []string
	identities := map[string]string{}
	c.lateLsnr = map[string]net.Listener{}
	for i := 1; i <= n+opts.Late; i++ {
		id := fmt.Sprintf("n%d", i)
		dir := filepath.Join(c.root, id)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			tb.Fatalf("creating %s: %v", dir, err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatalf("listening for %s: %v", id, err)
		}
		_, fp, err := transport.MakeServerTLSConfig(filepath.Join(dir, "keys"))
		if err != nil {
			tb.Fatalf("making the key of %s: %v", id, err)
		}
		listeners[id], identities[id] = listener, fp
		_ = identities
		pins = append(pins, fp)
		if i > n {
			c.late = append(c.late, id)
			c.lateLsnr[id] = listener
		} else {
			c.ids = append(c.ids, id)
		}
		c.members[id] = &member{id: id, dir: dir, addr: listener.Addr().String(), disk: &diskFault{}, acks: &ackFault{}, fp: fp}
		c.faults.register(id, c.members[id].addr)
	}
	for _, id := range append(append([]string(nil), c.ids...), c.late...) {
		serverCfg, _, err := transport.MakeServerTLSConfig(filepath.Join(c.members[id].dir, "keys"))
		if err != nil {
			tb.Fatalf("loading the key of %s: %v", id, err)
		}
		trust, err := peer.NewTrust(serverCfg, pins)
		if err != nil {
			tb.Fatalf("pinning for %s: %v", id, err)
		}
		c.members[id].trust = trust
	}
	for i, id := range c.ids {
		c.open(id, listeners[id], i > 0)
	}
	leader := c.members["n1"].node
	for i, id := range c.ids[1:] {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := leader.JoinRegisterOnly(ctx, id, c.members[id].addr); err != nil {
			cancel()
			tb.Fatalf("harness: joining %s: %v", id, err)
		}
		if i+2 > n-opts.Learners {
			cancel()
			continue
		}
		if err := leader.Promote(ctx, id); err != nil {
			cancel()
			tb.Fatalf("harness: promoting %s: %v", id, err)
		}
		if err := activateForming(ctx, leader, id); err != nil {
			cancel()
			tb.Fatalf("harness: activating %s: %v", id, err)
		}
		cancel()
	}
	if len(opts.Timings) > 0 {
		commitProfile(tb, leader, opts.Timings)
	}
	if opts.NoDetection {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := leader.SetSetting(ctx, "probe_failures", 1000); err != nil {
			cancel()
			tb.Fatalf("harness: setting probe_failures: %v", err)
		}
		cancel()
	}
	if opts.NoProbes {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := leader.SetSetting(ctx, "probe_interval_ms", int64(time.Hour/time.Millisecond)); err != nil {
			cancel()
			tb.Fatalf("harness: setting probe_interval_ms: %v", err)
		}
		cancel()
	}
	if opts.FastDetection {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		for key, millis := range map[string]int64{"probe_interval_ms": 100, "probe_timeout_ms": 150, "probe_failures": 2, "failover_ms": 2000} {
			if err := leader.SetSetting(ctx, key, millis); err != nil {
				cancel()
				tb.Fatalf("harness: setting %s: %v", key, err)
			}
		}
		cancel()
	}
	c.AwaitConverged(30 * time.Second)
	c.AwaitLeases(30 * time.Second)
	return c
}

// AddNode opens the next late identity and registers it with the leader as a joining,
// promoted member. It does not activate it: the caller does, once the node has pulled what
// it will hold (Node.Activate refuses until then). It returns the new node's id.
func (c *Cluster) AddNode() string {
	c.tb.Helper()
	if len(c.late) == 0 {
		c.tb.Fatal("harness: no late identity left; set Options.Late")
	}
	id := c.late[0]
	listener := c.lateLsnr[id]
	c.late = c.late[1:]
	delete(c.lateLsnr, id)
	c.ids = append(c.ids, id)
	c.open(id, listener, true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// A leader change under load is not the test's business: ask the current leader again.
	// So is STATE_MOVED: the seed reporters of the existing nodes commit between the moment the
	// command is built and the moment it is proposed, and the refusal says nothing was changed.
	for _, step := range []struct {
		name string
		run  func(leader *wire.Node) error
	}{
		{"joining", func(leader *wire.Node) error { return leader.JoinRegisterOnly(ctx, id, c.Addr(id)) }},
		{"promoting", func(leader *wire.Node) error { return leader.Promote(ctx, id) }},
	} {
		for {
			err := step.run(c.Node(c.AwaitLeader(30 * time.Second)))
			if err == nil {
				break
			}
			if ctx.Err() != nil || (!strings.Contains(err.Error(), "leader") && !stateMoved(err)) {
				c.tb.Fatalf("harness: %s %s: %v", step.name, id, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return id
}

// OpenJoiner opens the next late identity in join mode and leaves it unjoined: it holds no
// cluster state and serves the identity channel, which is all a leader needs to add it. The
// membership commands (join, promote) then run against it. It returns the new node's id.
func (c *Cluster) OpenJoiner() string {
	c.tb.Helper()
	return c.openLate(true)
}

// OpenBootstrapped opens the next late identity as a cluster of its own: it bootstraps, so it
// holds Raft state and names a cluster that is not this one. It returns the new node's id.
func (c *Cluster) OpenBootstrapped() string {
	c.tb.Helper()
	return c.openLate(false)
}

func (c *Cluster) openLate(join bool) string {
	if len(c.late) == 0 {
		c.tb.Fatal("harness: no late identity left; set Options.Late")
	}
	id := c.late[0]
	listener := c.lateLsnr[id]
	c.late = c.late[1:]
	delete(c.lateLsnr, id)
	c.ids = append(c.ids, id)
	c.open(id, listener, join)
	return id
}

// DataDir is the directory node id keeps its data in (its Badger, log and Raft state), for a
// test that edits the store of a killed node between its kill and its restart.
func (c *Cluster) DataDir(id string) string {
	c.tb.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.members[id]
	if !ok {
		c.tb.Fatalf("harness: no node %q", id)
	}
	return filepath.Join(m.dir, "data")
}

// Membership is the membership commands as node id serves them: the statements an operator
// enters on that member.
func (c *Cluster) Membership(id string) wire.MembershipOps { return wire.NewMembership(c.Node(id)) }

// Addr is the internode address node id listens on.
func (c *Cluster) Addr(id string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.members[id].addr
}

// ActivateWhileSeedingMoves activates id through the leader as soon as the node has pulled every
// pair it will hold, for a joiner whose seeding speed the test does not control. A refusal for
// pairs left to pull, or a leader that changed under the call, is asked again; anything else fails
// the test. The wait fails when the joiner's queued pair count (Node.SeedingPairs) has not changed
// for stall, or when ceiling has passed, instead of after one fixed time: a slow host that keeps
// pulling pairs passes, a seeding that stopped fails at stall.
func (c *Cluster) ActivateWhileSeedingMoves(id string, stall, ceiling time.Duration) {
	c.tb.Helper()
	began := time.Now()
	lastQueued, lastMoved := c.Node(id).SeedingPairs(), began
	for {
		leader := c.Node(c.AwaitLeader(30 * time.Second))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := leader.Activate(ctx, id)
		cancel()
		if err == nil {
			c.tb.Logf("harness: activated %s %s after the first try (stall bound %s, ceiling %s)", id, time.Since(began).Round(time.Millisecond), stall, ceiling)
			return
		}
		if queued := c.Node(id).SeedingPairs(); queued != lastQueued {
			lastQueued, lastMoved = queued, time.Now()
		}
		retryable := errors.Is(err, wire.ErrStillSeeding) || strings.Contains(err.Error(), "lead") || stateMoved(err)
		switch {
		case !retryable:
			c.tb.Fatalf("harness: activating %s: %v", id, err)
		case time.Since(began) > ceiling:
			c.tb.Fatalf("harness: activating %s: still seeding after the %s ceiling: %v", id, ceiling, err)
		case time.Since(lastMoved) > stall:
			c.tb.Fatalf("harness: activating %s: the queued pairs stayed at %d for %s (stall bound %s): %v", id, lastQueued, time.Since(lastMoved).Round(time.Second), stall, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// AwaitWhileSeedingMoves waits for cond as Await does, but bounds the wait by the cluster's seeding
// rather than by one fixed time: it fails when neither the pairs queued for a pull on the running
// nodes (Node.SeedingPairs) nor the SEEDED bits the leader has committed have changed for stall, or
// when ceiling has passed. It is the wait for a joiner or a replaced node to finish seeding (and for
// what follows from it: its bits, its activation) on a host whose speed the test does not control:
// a slow host that keeps pulling passes, a seeding that stopped fails at stall.
func (c *Cluster) AwaitWhileSeedingMoves(what string, stall, ceiling time.Duration, cond func() bool) {
	c.tb.Helper()
	began := time.Now()
	last, lastMoved := c.seedingProgress(), began
	for !cond() {
		if now := c.seedingProgress(); now != last {
			last, lastMoved = now, time.Now()
		}
		switch {
		case time.Since(began) > ceiling:
			c.tb.Fatalf("harness: gave up after the %s ceiling waiting for %s (%d pairs queued, %d bits committed); background errors: %v", ceiling, what, last[0], last[1], c.BackgroundErrors())
		case time.Since(lastMoved) > stall:
			c.tb.Fatalf("harness: gave up waiting for %s: the seeding did not move for %s (stall bound %s; %d pairs queued, %d bits committed); background errors: %v", what, time.Since(lastMoved).Round(time.Second), stall, last[0], last[1], c.BackgroundErrors())
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.tb.Logf("harness: %s after %s (stall bound %s, ceiling %s)", what, time.Since(began).Round(time.Millisecond), stall, ceiling)
}

// AwaitActiveWhileSeedingMoves waits until the leader's committed row of id is active, for a node
// that is activated without the test's help (Options.JoinerActivation, replace-node), bounded as
// AwaitWhileSeedingMoves.
func (c *Cluster) AwaitActiveWhileSeedingMoves(id string, stall, ceiling time.Duration) {
	c.tb.Helper()
	c.AwaitWhileSeedingMoves(id+" to be active", stall, ceiling, func() bool {
		for _, m := range c.Node(c.AwaitLeader(30 * time.Second)).State().Members {
			if m.ID == id {
				return m.State == cluster.MemberActive
			}
		}
		return false
	})
}

// seedingProgress is what AwaitWhileSeedingMoves watches: the pairs queued for a pull on the
// running nodes, and the SEEDED bits the leader has committed over every node and database.
func (c *Cluster) seedingProgress() [2]int {
	var progress [2]int
	for _, id := range c.Running() {
		progress[0] += c.Node(id).SeedingPairs()
	}
	for _, row := range c.Node(c.AwaitLeader(30 * time.Second)).State().SeedBitmaps {
		for _, b := range row.Bits {
			progress[1] += bits.OnesCount8(b)
		}
	}
	return progress
}

// AwaitLeases waits until every running node holds a valid lease and a quorum round fresh
// enough to serve on, so a write sent now is refused neither LEASE_LAPSED nor NO_QUORUM by a
// cluster that has only just formed. A lease alone is not enough: the node refuses with
// NO_QUORUM while its lease is valid once its last confirmed majority round is older than
// the quorum bound (three heartbeats), and a loaded VM can make a round that late.
func (c *Cluster) AwaitLeases(timeout time.Duration) {
	c.tb.Helper()
	c.Await("every running node to hold a valid lease and a fresh quorum round", timeout, func() bool {
		for _, id := range c.Running() {
			if node := c.Node(id); !node.LeaseValid() || node.QuorumLost() {
				return false
			}
		}
		return true
	})
}

// IsTransientRefusal reports whether err is a refusal that passes by itself: LEASE_LAPSED
// or NO_QUORUM. Both are documented as retryable later (clients/handbook/wire.md), and on
// a loaded host either can fire on a healthy cluster, so a test that is not about them
// waits (AwaitLeases) and sends again. It reads the code, never the message text.
func IsTransientRefusal(err error) bool {
	var refusal *cluster.Error
	return errors.As(err, &refusal) && (refusal.Code == cluster.CodeLeaseLapsed || refusal.Code == cluster.CodeNoQuorum)
}

func (c *Cluster) open(id string, listener net.Listener, join bool) {
	c.tb.Helper()
	if err := c.openErr(id, listener, join); err != nil {
		c.tb.Fatalf("harness: opening %s: %v", id, err)
	}
}

// openErr is open that returns the node's refusal instead of failing the test.
func (c *Cluster) openErr(id string, listener net.Listener, join bool) error {
	c.tb.Helper()
	m := c.members[id]
	if listener == nil {
		var err error
		for range 50 {
			if listener, err = net.Listen("tcp", m.addr); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond) // the port is still closing after a kill
		}
		if err != nil {
			return fmt.Errorf("relistening on %s for %s: %w", m.addr, id, err)
		}
	}
	node, err := wire.OpenNode(context.Background(), wire.NodeConfig{
		Dir: filepath.Join(m.dir, "data"), NodeID: id, ClientAddr: c.clientAddrOf(id, m.addr), Voters: c.opts.Voters, Witness: slices.Contains(c.opts.Witnesses, id),
		ReclaimKeysPerSecond: c.opts.ReclaimKeysPerSecond, AtRest: c.atRestOf(id),
		Cluster: &wire.ClusterOptions{
			MembershipLocked: slices.Contains(c.opts.LockMembership, id), HoldActivation: !c.opts.JoinerActivation, HoldSeedReports: c.opts.HoldSeedReports,
			UngatedDeferredPairsTestOnly: c.opts.NoDeferredGate,
			StateRange:                   c.stateRangeOf(id),
			Listener:                     m.trust.Listener(listener), TLS: func(_, addr string) (*tls.Config, error) { return m.trust.ClientConfig(addr) }, Fingerprints: c.fingerprintOf, Dial: c.faults.dialer(id, m.trust), Join: join,
			WCTimeout: c.opts.WCTimeout, RaftTimeout: c.raftTimeout(), RaftLogger: c.raftLogger(),
			WrapLog:  func(log wire.Log) wire.Log { return faultyLog{Log: log, fault: m.disk} },
			WrapLink: func(link wire.Link) wire.Link { return ackFaultLink{Link: link, fault: m.acks} },
			OnError:  c.recordError, Seed: c.opts.Seed, ProposalBudget: c.opts.ProposalBudget, CatchUpChunkRecords: c.opts.CatchUpChunkRecords,
			BeforeSupervisorWork: m.waitSlowStep,
			Pepper:               c.pepperFileOf(id),
			BeforePepperFetch:    c.beforePepperFetchOf(id),
		},
	})
	if err != nil {
		if listener != nil {
			_ = listener.Close() // the refused node never took it; free the port for a later open
		}
		return err
	}
	c.mu.Lock()
	m.node, m.killed = node, false
	c.mu.Unlock()
	return nil
}

// atRestOf is node id's AtRestKeys, or nil when the cluster runs in plaintext. A nil interface,
// not a typed nil, so the node asks for nothing.
func (c *Cluster) atRestOf(id string) wire.AtRestKeys {
	if c.opts.AtRest == nil {
		return nil
	}
	return c.opts.AtRest(id)
}

// PepperFile is node id's cluster password key file; it exists once the node holds the key.
func (c *Cluster) PepperFile(id string) clusterpepper.File {
	return clusterpepper.File{Path: filepath.Join(c.members[id].dir, clusterpepper.FileName)}
}

func (c *Cluster) pepperFileOf(id string) wire.PepperFile {
	if !c.opts.ClusterPepper {
		return nil
	}
	return c.PepperFile(id)
}

func (c *Cluster) beforePepperFetchOf(id string) func(context.Context, string) error {
	if c.opts.BeforePepperFetch == nil {
		return nil
	}
	return func(_ context.Context, member string) error { return c.opts.BeforePepperFetch(id, member) }
}

// SetStateRange makes node id declare r the next time it opens (a Restart, or a node not yet
// opened), standing in for a build at another level.
func (c *Cluster) SetStateRange(id string, r cluster.StateRange) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.opts.StateRanges == nil {
		c.opts.StateRanges = map[string]cluster.StateRange{}
	}
	c.opts.StateRanges[id] = r
}

func (c *Cluster) stateRangeOf(id string) *cluster.StateRange {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.opts.StateRanges[id]; ok {
		return &r
	}
	return nil
}

func (c *Cluster) clientAddrOf(id, dataAddr string) string {
	if c.opts.ClientAddr == nil {
		return ""
	}
	return c.opts.ClientAddr(id, dataAddr)
}

// fingerprintOf is the id=fingerprint map every node is configured with.
func (c *Cluster) fingerprintOf(id string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.members[id]; ok {
		return m.fp, true
	}
	fp, ok := c.spare[id]
	return fp, ok
}

func (c *Cluster) raftTimeout() time.Duration {
	if c.opts.RaftTimeout > 0 {
		return c.opts.RaftTimeout
	}
	return 150 * time.Millisecond
}

func (c *Cluster) recordError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.errs) < 200 {
		c.errs = append(c.errs, err.Error())
	}
}

// BackgroundErrors are the errors nodes reported from background work, for a failing test
// to print.
func (c *Cluster) BackgroundErrors() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.errs...)
}

func (c *Cluster) close() {
	c.mu.Lock()
	var nodes []*wire.Node
	for _, m := range c.members {
		if m.node != nil && !m.killed {
			nodes = append(nodes, m.node)
		}
	}
	c.mu.Unlock()
	for _, n := range nodes {
		if err := n.Close(); err != nil {
			c.tb.Logf("harness: closing a node: %v", err)
		}
	}
	c.mu.Lock()
	held := c.heldReplays
	c.mu.Unlock()
	if held {
		// Every node is closed, so no replay pass can be reading the hold.
		wire.HoldBootReplaysForTest(nil)
	}
	if c.tb.Failed() {
		for _, e := range c.BackgroundErrors() {
			c.tb.Logf("background error: %s", e)
		}
	}
}

// IDs lists the node ids in creation order.
func (c *Cluster) IDs() []string { return append([]string(nil), c.ids...) }

// Node returns the running node id, or fails the test when it is killed.
func (c *Cluster) Node(id string) *wire.Node {
	c.tb.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.members[id]
	if !ok || m.node == nil || m.killed {
		c.tb.Fatalf("harness: node %q is not running; running nodes are %v", id, c.runningLocked())
	}
	return m.node
}

// nodeIfRunning is Node without the Fatalf: a node a schedule kills between a caller's
// Running check and its use is reported as not running, so a client goroutine can record
// the write as refused instead of ending the whole test.
func (c *Cluster) nodeIfRunning(id string) (*wire.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.members[id]
	if !ok || m.node == nil || m.killed {
		return nil, false
	}
	return m.node, true
}

func (c *Cluster) runningLocked() []string {
	var out []string
	for _, id := range c.ids {
		if m := c.members[id]; m.node != nil && !m.killed {
			out = append(out, id)
		}
	}
	return out
}

// Running lists the nodes that are up.
func (c *Cluster) Running() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runningLocked()
}

// Await polls cond every 10 ms until it holds, failing the test with what was awaited.
func (c *Cluster) Await(what string, timeout time.Duration, cond func() bool) {
	c.tb.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.tb.Fatalf("harness: gave up after %s waiting for %s; background errors: %v", timeout, what, c.BackgroundErrors())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// AwaitLeader waits for a leader among the running nodes and returns its id.
func (c *Cluster) AwaitLeader(timeout time.Duration) string {
	c.tb.Helper()
	var leader string
	c.Await("a Raft leader among "+fmt.Sprint(c.Running()), timeout, func() bool {
		for _, id := range c.Running() {
			if c.Node(id).IsLeader() {
				leader = id
				return true
			}
		}
		return false
	})
	return leader
}

// AwaitConverged waits until every running node has applied the same state_seq and epoch.
func (c *Cluster) AwaitConverged(timeout time.Duration) cluster.ClusterState {
	c.tb.Helper()
	var agreed cluster.ClusterState
	c.Await("every running node to apply the same epoch and state_seq", timeout, func() bool {
		var first *cluster.ClusterState
		for _, id := range c.Running() {
			state := c.Node(id).State()
			if state.StateSeq == 0 {
				return false
			}
			if first == nil {
				first = &state
				continue
			}
			if state.Epoch != first.Epoch || state.StateSeq != first.StateSeq {
				return false
			}
		}
		if first != nil {
			agreed = *first
		}
		return first != nil
	})
	return agreed
}

// ---- faults ------------------------------------------------------------------------

// Partition cuts every node of one group from every node of the others, in both
// directions: connections across the cut are severed and new dials refused. A node not
// named is in no group and stays connected to everyone.
func (c *Cluster) Partition(groups ...[]string) {
	for i, group := range groups {
		for _, other := range groups[i+1:] {
			for _, a := range group {
				for _, b := range other {
					c.faults.block(a, b)
				}
			}
		}
	}
}

// Pause cuts one node from all the others while its process keeps running.
func (c *Cluster) Pause(id string) {
	var rest []string
	for _, other := range c.ids {
		if other != id {
			rest = append(rest, other)
		}
	}
	c.Partition([]string{id}, rest)
}

// Stall makes a node stop answering while every connection to it stays open (true), or
// answer again (false): a stopped process, not a closed one.
func (c *Cluster) Stall(id string, stalled bool) { c.faults.Stall(id, stalled) }

// Heal removes every partition.
func (c *Cluster) Heal() { c.faults.Heal() }

// DelayChannel adds d to every write from one node to another on a channel.
func (c *Cluster) DelayChannel(from, to string, ch transport.Channel, d time.Duration) {
	c.faults.DelayChannel(from, to, ch, d)
}

// DropReplies loses the answers on a channel between two nodes while on is true; see Faults.DropReplies.
func (c *Cluster) DropReplies(from, to string, ch transport.Channel, on bool) {
	c.faults.DropReplies(from, to, ch, on)
}

// DropChannel cuts the channel between two nodes and refuses its next dials.
func (c *Cluster) DropChannel(from, to string, ch transport.Channel, dials int) {
	c.faults.DropChannel(from, to, ch, dials)
}

// FailDisk makes a node's log refuse appends (true) or work again (false).
func (c *Cluster) FailDisk(id string, failing bool) { c.members[id].disk.failing.Store(failing) }

// FailAcks makes every acknowledgement a node's copy side tries to send fail (true) or work again
// (false). The attempt is counted either way: AckAttempts says how many were made, so a test can
// see that a record was applied and its ack tried before it kills the node.
func (c *Cluster) FailAcks(id string, failing bool) { c.members[id].acks.failing.Store(failing) }

// AckAttempts is how many acknowledgements the node has tried to send since it was created.
func (c *Cluster) AckAttempts(id string) int64 { return c.members[id].acks.attempts.Load() }

// StreamCounts is how many calls and records a node has sent on the replication stream since it
// was created. A call carries one record or ack on the per-record path and several on the
// batched one, so a test reads Calls below Entries as the batch in use.
type StreamCounts struct {
	// AckCalls and AckEntries are the node's copy-side acknowledgement calls and the acks in them.
	AckCalls, AckEntries int64
	// WriteCalls and WriteEntries are the node's owner-side WRITE calls and the records in them.
	WriteCalls, WriteEntries int64
}

// StreamCounts reports the stream traffic the node has sent, counted in front of its replication link.
func (c *Cluster) StreamCounts(id string) StreamCounts {
	acks := c.members[id].acks
	return StreamCounts{AckCalls: acks.calls.Load(), AckEntries: acks.entries.Load(), WriteCalls: acks.writeCalls.Load(), WriteEntries: acks.writeEntries.Load()}
}

// CatchUpRequests is how many catch-up requests node id has sent to node source, counted in front of its replication link.
func (c *Cluster) CatchUpRequests(id, source string) int64 {
	return c.members[id].acks.catchUpsTo(source).Load()
}

// SlowSupervisor makes every supervisor step of node id wait d before its control calls (0
// removes the delay): a slow activation ask, voter reconcile or disk on a loaded host, while
// the network and the node's quorum rounds stay as fast as ever (BUG-0808).
func (c *Cluster) SlowSupervisor(id string, d time.Duration) { c.members[id].slowStep.Store(int64(d)) }

// waitSlowStep is the node's BeforeSupervisorWork: it waits the configured delay, or until
// the node closes.
func (m *member) waitSlowStep(ctx context.Context) {
	delay := time.Duration(m.slowStep.Load())
	if delay <= 0 {
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// SlowDisk makes every append to a node's log take at least d (0 removes the fault).
func (c *Cluster) SlowDisk(id string, d time.Duration) { c.members[id].disk.slowNanos.Store(int64(d)) }

// Kill stops a node and keeps its directory and address. It is a clean stop of the
// process's work, not a crash of the disk; peers see their connections to it die.
func (c *Cluster) Kill(id string) {
	c.tb.Helper()
	c.mu.Lock()
	m := c.members[id]
	node := m.node
	if node == nil || m.killed {
		c.mu.Unlock()
		c.tb.Fatalf("harness: node %q is not running", id)
	}
	m.killed = true
	c.mu.Unlock()
	if err := node.Close(); err != nil {
		// A TLS close-notify on a connection the kill already tore down is not a failure.
		if !strings.Contains(err.Error(), "closeNotify") {
			c.tb.Fatalf("harness: killing %s: %v", id, err)
		}
	}
	c.faults.sever(func(conn *faultConn) bool { return conn.to == id || conn.from == id })
}

// Restart reopens a killed node on its own directory and address.
func (c *Cluster) Restart(id string) {
	c.tb.Helper()
	c.reopen(id)
	// The reopened node has applied nothing until its leader reaches it: wait for it to hold
	// the cluster's state and a lease, as a client would before sending it a write.
	c.AwaitConverged(30 * time.Second)
	c.AwaitLeases(30 * time.Second)
}

// reopen reopens a killed node without waiting for it to catch up: a test about a node the
// cluster refuses (another build) would wait forever.
func (c *Cluster) reopen(id string) {
	c.tb.Helper()
	c.mu.Lock()
	m := c.members[id]
	if m.node == nil || !m.killed {
		c.mu.Unlock()
		c.tb.Fatalf("harness: node %q is running; kill it before restarting", id)
	}
	c.mu.Unlock()
	c.open(id, nil, true)
}

// TryRestart reopens a killed node on its own directory and returns the node's refusal, if it
// refuses to open, instead of failing the test: for a test about a directory the node must not
// start on. The node stays killed after a refusal, so the directory can be put back and the node
// restarted.
func (c *Cluster) TryRestart(id string) error {
	c.tb.Helper()
	c.mu.Lock()
	m := c.members[id]
	if m.node == nil || !m.killed {
		c.mu.Unlock()
		c.tb.Fatalf("harness: node %q is running; kill it before restarting", id)
	}
	c.mu.Unlock()
	return c.openErr(id, nil, true)
}

// WipeRing kills node id and removes its ring directory (its Badger store, its log and its Raft
// state) while keeping its key, its pin on every member and its address: the start the vouched-cut
// and encryption refusals once advised ("move the ring directory aside, keep keys/"). The node stays
// killed; RestartUnsynced starts it again on the empty directory (BUG-0870).
func (c *Cluster) WipeRing(id string) {
	c.tb.Helper()
	c.Kill(id)
	dir := c.DataDir(id)
	if err := os.RemoveAll(dir); err != nil {
		c.tb.Fatalf("harness: removing the ring directory of %s: %v", id, err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		c.tb.Fatalf("harness: recreating the empty ring directory of %s: %v", id, err)
	}
}

// RestartUnsynced reopens a killed node on its own directory and address and returns at once,
// without waiting for it to hold the cluster's state.
func (c *Cluster) RestartUnsynced(id string) { c.tb.Helper(); c.reopen(id) }

// activateForming activates a member while the cluster forms. With Options.JoinerActivation the
// member asks for its own activation at the same time, and whichever commits second is refused
// STATE_MOVED: that refusal is asked again, and a member another activation already made active is
// done. Anything else is returned.
func activateForming(ctx context.Context, leader *wire.Node, id string) error {
	for {
		err := leader.Activate(ctx, id)
		if err == nil || !stateMoved(err) {
			return err
		}
		for _, m := range leader.State().Members {
			if m.ID == id && m.State == cluster.MemberActive {
				return nil
			}
		}
		if ctx.Err() != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stateMoved says the FSM refused a command because the state moved under it.
func stateMoved(err error) bool {
	var refusal *cluster.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeStateMoved
}

// logLines is a Raft logger's output, kept for a test to read.
type logLines struct {
	mu    sync.Mutex
	lines []string
}

// Write implements io.Writer: hclog hands it one line per call.
func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// raftLogger is the nodes' Raft logger: silent, or captured when Options.CaptureLog is set.
func (c *Cluster) raftLogger() hclog.Logger {
	if !c.opts.CaptureLog {
		return hclog.NewNullLogger()
	}
	// Info, so a test also reads the operator lines a node writes at Info (the cluster password
	// key's creation, hand-out and heal).
	return hclog.New(&hclog.LoggerOptions{Name: "peer", Level: hclog.Info, Output: &c.logs})
}

// LogLines is every line the nodes logged at WARN or above since the cluster opened, when
// Options.CaptureLog is set; nil otherwise.
func (c *Cluster) LogLines() []string {
	c.logs.mu.Lock()
	defer c.logs.mu.Unlock()
	return slices.Clone(c.logs.lines)
}
