package wire

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
	"github.com/hoarddb/hoarddb-server/cluster/control"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ClusterOptions makes a Node a member of a networked cluster: its Link, catch-up, drain,
// forward and replication are the real peer transport instead of NoPeers and NoCopies.
// A Node opened without it is the one-voter node of every earlier build, unchanged.
//
// Cost per node: one internode listener (one descriptor), the peer network's accept
// goroutines (one for the mux, one per served channel, one per inbound connection), one
// supervisor goroutine, the replicator's Run goroutine, and one goroutine per pair that is
// catching up right now. Per peer: at most one outbound connection per channel in use and
// no goroutine. Nothing is per bucket, per database or per document, and a membership
// change touches O(slots x databases) table entries, never O(buckets).
type ClusterOptions struct {
	// Listener is the node's internode listener, TLS-wrapped by the caller when the node
	// uses TLS (peer.Trust.Listener). The node owns it and closes it.
	Listener net.Listener
	// TLS builds the client TLS config for a dial to addr; nil dials plain TCP.
	TLS func(id, addr string) (*tls.Config, error)
	// Dial replaces the dial of every channel, including Raft; the harness's fault layer.
	Dial peer.Dialer
	// Fingerprints is the source of truth for whose key is whose: the fingerprint the
	// member id must authenticate with (the bootstrap seed map `id=FP`). A pin proves a
	// key; this binds it to a member id. A joiner may reach only the Raft channel, and
	// only as the leader that is adding it, until its member row is committed.
	Fingerprints func(id string) (string, bool)
	// StateRange overrides the state-level range the node declares; nil takes the build's.
	// A test sets it to stand in for a build at another level.
	StateRange *cluster.StateRange
	// InsecureTestOnly lets a test run without TLS or key binding. A node refuses to open
	// without TLS and Fingerprints unless it is set.
	InsecureTestOnly bool
	// Join starts the node without bootstrapping: it waits for a leader to Join it.
	Join bool
	// WCTimeout is replication.wc_timeout_ms; 0 takes one second.
	WCTimeout time.Duration
	// RaftTimeout is the Raft heartbeat and election timeout; 0 takes 200 ms.
	RaftTimeout time.Duration
	// RaftLogger receives the Raft library's log; nil logs warnings to stderr.
	RaftLogger hclog.Logger
	// WrapLog, when set, wraps the node's retained log for the owner write path and the
	// replication stream: a test uses it to make appends fail like a full disk.
	WrapLog func(Log) Log
	// WrapLink, when set, wraps the data channel the replicator sends WRITEs, acknowledgements and
	// catch-up requests on: a seam for tests that fail or record a copy's acknowledgement.
	WrapLink func(Link) Link
	// OnError receives errors of background work that has no caller; nil drops them.
	OnError func(error)
	// Seed holds the replication.seed_* and incoming_seeds_max bounds of the seed channel;
	// a zero field takes the config default.
	Seed SeedOptions
	// Limits bound what the internode port holds for callers: connections before routing,
	// per channel connections and requests in flight, idle and frame deadlines, and the
	// Raft channel's connections. A zero field takes the config default.
	Limits PeerLimits
	// ProposalBudget bounds how long a cluster command waits for a Raft leader to take it,
	// across leader changes; 0 takes 30 seconds.
	ProposalBudget time.Duration
	// MembershipLocked is cluster.allow_membership being false: every membership statement is
	// refused here and, when this node leads, on the leader (plan section 6). The zero value
	// allows membership changes, which is the default in cluster mode.
	MembershipLocked bool
	// UngatedDeferredPairsTestOnly switches the deferred-pair gate off: a pair whose boot replay
	// has not finished answers its peers from its low applied position, as it did before the
	// gate. It exists for the harness experiment that shows what the gate removes (deferred-pair
	// gate spec, H1) and is never set by a server.
	UngatedDeferredPairsTestOnly bool
	// HoldActivation stops this node from asking the leader to activate it while it is joining or down.
	// A test that keeps a joiner joining, or a down member down, sets it; a server never does.
	HoldActivation bool
	// HoldSeedReports stops this node's reporter from deriving and sending seed bits; the
	// reseed's one-slot clear still goes out. A test that needs an exact state_seq sets it,
	// because the reporter's SEEDED and RETIRE proposals move state_seq in the background;
	// a server never does.
	HoldSeedReports bool
	// CatchUpChunkRecords bounds the records of one catch-up chunk this node serves; 0 takes
	// the replicator's default (1,024). A test lowers it so a short log spans several
	// chunks; a server never sets it.
	CatchUpChunkRecords int
	// BeforeSupervisorWork, when set, runs in every supervisor step before the step's control
	// calls (the voter reconcile, the activation ask and what follows them). A test makes it
	// wait to stand in for a slow control call or a slow disk on a loaded host (BUG-0808); a
	// server never sets it.
	BeforeSupervisorWork func(ctx context.Context)
	// Pepper is where the node keeps the cluster's password key, auth.cluster.pepper (ring user
	// store spec 5.4). A server always sets it; nil keeps no key (the node runs no fetcher and
	// answers every PEPPER_REQUEST "not held"), which most harness tests use.
	Pepper PepperFile
	// BeforePepperFetch, when set, runs before each PEPPER_REQUEST this node sends; an error stops
	// that ask, as if the member had not answered. A test uses it to hold a joiner without the
	// key; a server never sets it.
	BeforePepperFetch func(ctx context.Context, member string) error
}

// errStarting is what a node answers a peer that reaches it before it finished opening.
var errStarting = errors.New("this node is still opening; retry shortly")

// clusterParts are the pieces a networked node has beyond a one-voter one.
type clusterParts struct {
	opts  ClusterOptions
	net   *peer.Network
	links *peerLinks
	repl  *Replicator
	seed  *SeedSource
	// seeding is set by Node.peers while the library may already be applying the restarted
	// node's log on the FSM goroutine, whose hook reads it: an atomic pointer, nil until set
	// (BUG-0755).
	seeding atomic.Pointer[seedParts]
	// reporter sends this node's seed bits through the relay; startCluster sets it before the
	// node serves, so no frame reaches the reseed hook first.
	reporter *seedReporter
	// gate decides when a retired copy's range may go; startCluster sets it with the reporter.
	gate   *gateDriver
	ctrl   *control.Control
	relays relayLedger
	// pepper holds the cluster's password key; nil when ClusterOptions.Pepper is nil.
	pepper *pepperKeeper
	// users is signalled, without blocking, after every applied command: the server's user-table
	// follower reads the table when woken (ring user store spec 6.3). Capacity 1, so wakes coalesce.
	users chan struct{}
	// membership, membershipSlot, counter, lastActivationAsk and activationAsks belong to the
	// membership commands (membership.go).
	membership        membershipLedger
	membershipSlot    chan struct{}
	counter           membershipCounter
	lastActivationAsk atomic.Int64
	activationAsks    atomic.Int64
	lease             *controlLease
	super             *supervisor
	serving           atomic.Bool
	// admittedAt is the state_seq the member that admitted this node held when it did, recorded
	// only for a node that held no cluster state then (BUG-0877). Such a node applies the
	// cluster's log from the start, and every state below this one is history from before it
	// held anything, in which its row may say active (a replaced node); the seeding driver
	// judges no pair against those states. 0 when the node was not admitted on an empty
	// directory. Memory only: see Node.seedFloor.
	admittedAt atomic.Uint64
	// forwardCatchUpCap overrides forwardCatchUpWait; tests set it before serving a frame, 0 keeps the default.
	forwardCatchUpCap time.Duration
	wake              chan struct{}
	catching          sync.Mutex
	running           map[pairKey]struct{}
	leaseCfg          atomic.Pointer[machine.LeaseConfig]
	// epochs are the committed states at each epoch change not yet fed to the handoff.
	epochMu         sync.Mutex
	epochs          []cluster.ClusterState
	lastQueuedEpoch int64
	lastQueuedDBs   []databaseRFPair
	// rf is the node's Raft seat as the network's goroutines see it: they start before
	// Raft opens, so they read it through this pointer, never through Node.raft.
	rf      atomic.Pointer[cluster.Raft]
	catchWG sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	runWG   sync.WaitGroup
}

// warn writes a one-line operator hint to the Raft logger (stderr at WARN when none is set). It
// is not routed through OnError, which a test treats as a failure and a killed peer would trip.
func (c *clusterParts) warn(message string) {
	logger := c.opts.RaftLogger
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{Name: "peer", Level: hclog.Warn, Output: os.Stderr})
	}
	logger.Warn(message)
}

// notePepper writes a cluster password key line at Info: its creation, a hand-out, a refusal, a
// wrong key moved aside. No line ever carries the key's bytes (clusterpepper prints only its kid).
func (c *clusterParts) notePepper(line string) {
	logger := c.opts.RaftLogger
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{Name: "pepper", Level: hclog.Info, Output: os.Stderr})
	}
	logger.Info(line)
}

func (c *clusterParts) report(err error) {
	// A peer in backoff is the transport working as designed, not news.
	if c.opts.OnError != nil && err != nil && !errors.Is(err, peer.ErrBackoff) {
		c.opts.OnError(err)
	}
}

// onApply is the Raft apply hook. It runs on the FSM goroutine right after each applied
// command, so the state it reads is exactly that command's. It queues the state whenever
// the epoch moved: the handoff must be fed every committed epoch in order, and the
// supervisor, which wakes later, may otherwise see only the latest of several.
func (c *clusterParts) onApply() {
	if parts := c.seeding.Load(); parts != nil {
		parts.signal()
	}
	if rf := c.rf.Load(); rf != nil {
		c.queueState(rf.State())
		if c.pepper != nil {
			c.notePepperKID(rf.Users().PepperKID)
		}
	}
	select {
	case c.users <- struct{}{}:
	default:
	}
	c.signal()
}

// queueState queues state when it is the first of a new epoch or when its database list
// (ids and RFs) differs from the last queued one. The second rule matters when a follower
// applies a create database and the next epoch move before its supervisor runs: without the
// state in between the handoff never learns the database existed at the previous epoch and
// opens its pairs with no barrier (BUG-0747). The comparison allocates only on a change.
func (c *clusterParts) queueState(state cluster.ClusterState) {
	c.epochMu.Lock()
	defer c.epochMu.Unlock()
	if state.Epoch == c.lastQueuedEpoch && sameDatabaseRF(c.lastQueuedDBs, state.Databases) {
		return
	}
	c.lastQueuedEpoch = state.Epoch
	c.lastQueuedDBs = c.lastQueuedDBs[:0]
	for _, db := range state.Databases {
		c.lastQueuedDBs = append(c.lastQueuedDBs, databaseRFPair{id: db.ID, rf: db.RF})
	}
	c.epochs = append(c.epochs, state)
}

// databaseRFPair is a database's id and replication factor, all the handoff reads of it.
type databaseRFPair struct {
	id int64
	rf int32
}

func sameDatabaseRF(known []databaseRFPair, databases []cluster.Database) bool {
	if len(known) != len(databases) {
		return false
	}
	for i, db := range databases {
		if known[i].id != db.ID || known[i].rf != db.RF {
			return false
		}
	}
	return true
}

// takeEpochs returns the queued states in the order they committed and empties the queue.
func (c *clusterParts) takeEpochs() []cluster.ClusterState {
	c.epochMu.Lock()
	defer c.epochMu.Unlock()
	out := c.epochs
	c.epochs = nil
	return out
}

// signal wakes the supervisor without blocking.
func (c *clusterParts) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// peerNet opens the internode network before Raft, because Raft listens on its channel.
func (n *Node) peerNet() error {
	opts := n.cfg.Cluster
	if opts.Listener == nil {
		return errors.New("opening a networked node needs ClusterOptions.Listener: the node's internode listener, wrapped in pinned TLS (peer.Trust.Listener)")
	}
	if !opts.InsecureTestOnly && (opts.TLS == nil || opts.Fingerprints == nil) {
		return errors.Join(errors.New("a networked node needs ClusterOptions.TLS and ClusterOptions.Fingerprints: without pinned TLS and the id=fingerprint map the internode port would trust any caller. Set them from seed_fingerprints"), opts.Listener.Close())
	}
	ctx, cancel := context.WithCancel(context.Background())
	parts := &clusterParts{opts: *opts, wake: make(chan struct{}, 1), users: make(chan struct{}, 1), running: map[pairKey]struct{}{}, ctx: ctx, cancel: cancel, membershipSlot: make(chan struct{}, 1)}
	// A witness keeps no password key (owner ruling, spec 5.3): its file, if one was left from an
	// earlier life as a data node, is not read.
	if opts.Pepper != nil && !n.cfg.Witness {
		keeper, err := newPepperKeeper(opts.Pepper)
		if err != nil {
			cancel()
			return errors.Join(err, opts.Listener.Close())
		}
		keeper.log = parts.notePepper
		parts.pepper = keeper
	}
	n.cl = parts
	network, err := peer.New(peer.Config{
		Self: n.cfg.NodeID, Listener: opts.Listener, TLS: opts.TLS, Dial: opts.Dial,
		IDAt: func(addr string) (string, bool, error) {
			if rf := parts.rf.Load(); rf != nil {
				return rf.IDAt(addr)
			}
			return "", false, nil
		},
		Hello: n.hello,
		Warn:  func(message string) { parts.warn(message) },
		Addr: func(id string) (string, bool) {
			if rf := parts.rf.Load(); rf != nil {
				return rf.MemberAddr(id)
			}
			return "", false
		},
		Admit: func(h transport.Handshake) error {
			if rf := parts.rf.Load(); rf != nil {
				return rf.AdmitPeer(h, true)
			}
			return errStarting
		},
		Data: n.serveData, Control: n.serveControl,
		Identity: n.serveIdentity,
		// The identity channel admits the pinned key alone: a node that is not a member yet
		// is asked who it is, and answers before it holds any cluster state.
		IdentityAdmit: func(h transport.Handshake) error {
			if rf := parts.rf.Load(); rf != nil {
				return rf.AdmitIdentity(h)
			}
			return errStarting
		},
		MuxLimits: opts.Limits.mux(), PeerLimits: opts.Limits.channel(),
		Seed: n.serveSeed, SeedLimits: transport.ChannelLimits{MaxConns: opts.Seed.MaxConns, Idle: opts.Seed.withDefaults().Idle},
		// Half the idle limit this node serves under (the cluster runs one setting), so a pooled
		// connection is redialled before the owner closes it.
		ConnMaxIdle: opts.Limits.withDefaults().Idle / 2,
	})
	if err != nil {
		cancel()
		return errors.Join(err, opts.Listener.Close())
	}
	parts.net = network
	return nil
}

// stateRange is the range this node declares and holds joiners to.
func (n *Node) stateRange() cluster.StateRange {
	if n.cl == nil {
		return cluster.BuildStateRange()
	}
	return declaredRange(&n.cl.opts)
}

// declaredRange is the range a node with these options declares: the override, else the build's.
func declaredRange(opts *ClusterOptions) cluster.StateRange {
	if opts != nil && opts.StateRange != nil {
		return *opts.StateRange
	}
	return cluster.BuildStateRange()
}

// RefusedPeers lists the peers this node's acceptors turned away for a state range that does not
// overlap its own; nil before the node's Raft group is open.
func (n *Node) RefusedPeers() []cluster.RefusedPeer {
	if n.cl == nil {
		return nil
	}
	if rf := n.cl.rf.Load(); rf != nil {
		return rf.RefusedPeers()
	}
	return nil
}

func (n *Node) hello() transport.Handshake {
	if rf := n.cl.rf.Load(); rf != nil {
		return rf.Identity()
	}
	return n.stateRange().Stamp(transport.Handshake{NodeID: n.cfg.NodeID, Incarnation: uint64(n.incarnation)})
}

// peerSet is everything of the node that talks to another node, in one place so wire()
// swaps it as a unit.
type peerSet struct {
	link        HandoffLink
	catchUp     HandoffCatchUp
	drain       HandoffDrain
	reads       ReadForwarder
	writes      WriteForwarder
	replication Replication
	decls       DeclarationReader
	lease       ServeChecker
	slots       SlotStates
	reachable   func(slot uint16, db uint64) bool
	// notReady says why the pair's seed record is not ready ("" when it is); nil on a node with no seed records.
	notReady func(slot uint16, db uint64) string
	// advancer moves a pair's record on a networked node; nil takes the Writer's default.
	advancer PairAdvancer
}

// peers returns the one-voter set (NoPeers, NoCopies, the node's own lease) or, for a
// networked node, the real transport over the same interfaces.
func (n *Node) peers(applier Applier, window machine.WindowConfig) (peerSet, error) {
	local := NewLocalDeclarationReader(n.kv, n.cfg.NodeID, n.ringAdvice())
	if n.cl == nil {
		return peerSet{
			link: NoPeers{}, catchUp: NoPeers{}, drain: NoPeers{}, reads: NoPeers{}, replication: NoCopies{}, decls: local,
			lease: NewServedLease(machine.LeaseConfig{}), slots: SingleVoterSlots{Topology: n.topology},
			reachable: func(uint16, uint64) bool { return false },
		}, nil
	}
	c := n.cl
	c.links = &peerLinks{self: n.cfg.NodeID, net: c.net, hello: n.hello, decl: n.declarationOf}
	var streamLink Link = c.links
	if n.cl.opts.WrapLink != nil {
		streamLink = n.cl.opts.WrapLink(streamLink)
	}
	var err error
	c.repl, err = NewReplicator(ReplicatorConfig{
		Self: n.cfg.NodeID, Committed: func() uint64 { return n.topology.Committed() }, Pairs: n.pairs, Log: n.logFor(), KV: n.kv,
		Applier: applier, Advancer: copyAdvancer{seeding: &c.seeding}, Link: streamLink, Seeds: nodeSeeder{node: n}, Copies: n.copiesOf, Owner: n.ownerOf, CatchUpSource: n.catchUpSource,
		FloorCursor: func(uint16, uint64) uint64 {
			first, err := n.wal.FirstSequence()
			if err != nil {
				return 1
			}
			return max(first, 1)
		},
		SchemaReady: n.copySchemaReady,
		Deferred:    n.pairDeferred,
		Ready: func(slot uint16, db uint64) bool { // c.seeding is built after the replicator
			parts := c.seeding.Load()
			return parts != nil && replicaReady(recordSlots{parts: parts})(slot, db)
		},
		Window:         n.cfg.ReplWindow,
		DedupeWindow:   window,
		Buffer:         n.buffers,
		RequestCatchUp: n.startCatchUp,
		ChunkRecords:   c.opts.CatchUpChunkRecords,
		MarkBehind:     func(epoch uint64) { c.lease.MarkBehind(epoch, false) },
		Behind:         func(committed uint64) bool { _, behind := c.lease.BehindEpoch(committed); return behind },
		OnError:        c.report,
	})
	if err != nil {
		return peerSet{}, fmt.Errorf("building the replicator: %w", err)
	}
	c.links.onAck = c.repl.OnWriteAck
	n.floors.add(c.repl.PinnedFloors)
	parts := n.newSeedParts(applier, window)
	c.seeding.Store(parts)
	view := recordSlots{parts: parts}
	slots := SlotStates(view)
	c.seed = n.newSeedSource(view)
	c.super = newSupervisor(n)
	initial := machine.LeaseConfig{Members: 1}
	c.leaseCfg.Store(&initial)
	ctrl, err := n.newControl()
	if err != nil {
		// New refuses only a zero StepDown, which newControl sets; a failure here is a bug.
		c.report(err)
	}
	c.ctrl = ctrl
	c.lease = &controlLease{ctrl: ctrl, clock: bootclock.New(), cfg: func() machine.LeaseConfig { return *c.leaseCfg.Load() }}
	return peerSet{
		link: c.links, catchUp: c.repl, drain: c.repl, reads: c.links, writes: c.links, replication: c.repl,
		decls: networkDeclarations{self: n.cfg.NodeID, local: local, links: c.links}, lease: c.lease, slots: slots,
		reachable: func(slot uint16, db uint64) bool { return c.super.peerReachable(n.pairs.Snapshot(slot, db).Peer) },
		notReady: func(slot uint16, db uint64) string {
			// The Writer's admission seam (Write and held): a pair the node admits, a record-less
			// one on an active node included, orders; the advancer then creates its record or
			// refuses it against the target set (plan §0.2, pi BL-A).
			if view.Admits(slot, db) {
				return ""
			}
			return notReadyCause(view.State(slot, db))
		},
		advancer: seedAwareAdvancer{recon: parts.recon},
	}, nil
}

// afterServingStarts, when set, runs in startCluster right after the node begins serving: the seam a
// test uses to read the seeding view at that point, because the window between the two
// statements cannot be observed from outside. Nil in production.
var afterServingStarts func(*Node)

// startCluster starts the replicator and the supervisor and opens the node to its peers.
func (n *Node) startCluster() {
	c := n.cl
	parts := c.seeding.Load()
	c.reporter = newSeedReporter(seedReportDeps{
		Self:      n.cfg.NodeID,
		Committed: n.raft.State,
		Record: func(slot uint16, db uint64) (uint8, bool) {
			record, found := parts.recon.Record(slot, db)
			return record.State, found
		},
		Records: parts.recon.Records,
		Send: func(ctx context.Context, kind byte, body []byte) error {
			_, err := n.propose(ctx, kind, body)
			return err
		},
		OnError: c.report,
		Now:     time.Now,
		Hold:    c.opts.HoldSeedReports,
	})
	parts.hooks.BeforeReset = func(ctx context.Context, slot uint16, db uint64) error {
		return c.reporter.Clear(ctx, db, slot)
	}
	c.gate = newGateDriver(gateDeps{
		Self: n.cfg.NodeID, Recon: parts.recon, Committed: n.raft.State, Epoch: n.topology.Committed,
		Ask: c.links.QueryPosition, Delete: parts.puller.resetRange, Queue: parts.puller.Queue,
		Report: c.reporter, OnError: c.report, Deferred: n.own.isDeferred, Diverged: n.raft.IsDiverged,
	})
	// The view is installed before any goroutine below starts and before serving is set: it is
	// built from parts that exist already, so a status seeding call that finds the node serving
	// finds the engine's view, not the empty NoSeeding one, whatever the goroutines have done.
	n.SetSeedingView(newNodeSeedingView(n))
	if n.startsPepperFetcher() {
		c.pepper.ask = c.ctrl.RequestPepper
		c.runWG.Add(1)
		go func() {
			defer c.runWG.Done()
			n.runPepperFetcher(c.ctx)
		}()
	}
	c.runWG.Add(7)
	go func() {
		defer c.runWG.Done()
		c.seed.Run(c.ctx)
	}()
	go func() {
		defer c.runWG.Done()
		c.seeding.Load().puller.Run(c.ctx)
	}()
	go func() {
		defer c.runWG.Done()
		n.runSeedDriver(c.ctx)
	}()
	go func() {
		defer c.runWG.Done()
		c.repl.Run(c.ctx)
	}()
	go func() {
		defer c.runWG.Done()
		c.super.run(c.ctx)
	}()
	go func() {
		defer c.runWG.Done()
		c.reporter.Run(c.ctx)
	}()
	go func() {
		defer c.runWG.Done()
		c.gate.Run(c.ctx)
	}()
	c.serving.Store(true)
	if afterServingStarts != nil {
		afterServingStarts(n)
	}
	c.seeding.Load().signal()
	c.signal()
}

// forwardWrite sends a write this node may not order to the owner, once (write path
// section 1): the hop count is set to one, so the owner refuses NOT_OWNER rather than
// forwarding again.
func (n *Node) forwardWrite(ctx context.Context, req WriteRequest) (WriteAnswer, error) {
	if req.Hop >= 1 {
		return WriteAnswer{}, &cluster.Error{Code: cluster.CodeNotOwner, Message: fmt.Sprintf(
			"this node does not own slot %d of database %d and the write was already forwarded once. Nothing was written. Refresh the topology and send the write to the owner", req.Slot, req.DBID)}
	}
	owner, ok := n.ownerOf(req.Slot, req.DBID)
	if !ok || owner == n.cfg.NodeID {
		return WriteAnswer{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"slot %d of database %d has no owner this node can forward to (every copy is down). Nothing was written. Run 'nodes' to see which members are down", req.Slot, req.DBID)}
	}
	if req.Rewrite != nil || req.ClaimSink != nil {
		// A closure reads and rewrites state only the owner holds, and cannot cross the
		// wire. Running the whole statement at the owner is a new peer message; until
		// then the refusal says where to run it (SPEC-GAP U57-13).
		return WriteAnswer{}, &cluster.Error{Code: cluster.CodeNotOwner, Message: fmt.Sprintf(
			"%s owns the declaration or the document this statement rewrites (slot %d of database %d), and a statement that rewrites it cannot be forwarded. Nothing was written. Connect to %s and run it there", owner, req.Slot, req.DBID, owner)}
	}
	req.Hop = 1
	answer, err := n.writes.ForwardWrite(ctx, owner, req)
	answer.ServedBy = owner
	return answer, err
}

// logFor is the retained log the write path and the stream use: the node's own, or the
// wrapped one a test installed.
func (n *Node) logFor() Log {
	if n.cl != nil && n.cl.opts.WrapLog != nil {
		return n.cl.opts.WrapLog(n.wal)
	}
	return n.wal
}

// declarationOf is the declaration a bucket is read under, from the node's own cache.
func (n *Node) declarationOf(db, bucketID uint64) (machine.Declaration, bool) {
	view := n.schema.View(db, bucketID)
	return view.Decl, view.Present
}

// holdRestarted is PairOwnershipConfig.Hold: the first touch of a pair after a restart asks
// the Handoff to hold it if this node owns it. The replication factor and the epoch come
// from the committed state, which already holds a database created at the current epoch.
//
// It stays undecided until the node's own log is applied and the Handoff was fed the state
// applied now: a hold opened earlier would keep the stop bound of a state the replay or the
// supervisor had not yet passed, and a barrier never changes its bound (BUG-0813).
func (n *Node) holdRestarted(slot uint16, db uint64) (held, decided bool) {
	if !n.raft.StateLoaded() || !n.handoff.SawState(n.raft.StateSeq()) {
		return false, false
	}
	info, epoch, ok := n.topology.DatabaseAtEpoch(db)
	if !ok {
		return false, false
	}
	return n.handoff.RestartHold(slot, db, info.RF, epoch)
}

// undecidedCause is the refusal cause while a restarted node does not yet know which pairs it
// owns: the same condition holdRestarted answers undecided for. "" for a node that did not
// restart or has decided.
func (n *Node) undecidedCause() string {
	if !n.restarted {
		return ""
	}
	return restartUndecidedCause(n.raft.StateLoaded(), n.handoff.SawState(n.raft.StateSeq()))
}

// bootedBefore reports whether a previous process of this node had joined a cluster: the
// Raft directory held state, or the store holds pair records. Either alone is enough, so
// a node that lost its Raft directory but kept its data is still not taken for a new one.
// A node with no cluster has no other owner to wait for, so it never holds.
func (n *Node) bootedBefore() (bool, error) {
	if n.cl == nil {
		return false, nil
	}
	if n.raft.Restarted() {
		return true, nil
	}
	snapshot := n.kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSSeedState)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyRangeEnd(lower)})
	defer iter.Close()
	iter.Seek(lower)
	if err := iter.Err(); err != nil {
		return false, fmt.Errorf("looking for pair records from an earlier boot: %w", err)
	}
	return iter.Valid(), nil
}

// copiesOf lists the other nodes that hold the pair and are not down.
func (n *Node) copiesOf(slot uint16, db uint64) []string {
	info, ok := n.topology.Database(db)
	if !ok {
		return nil
	}
	var out []string
	for _, member := range n.topology.Table().Copies(slot, info.RF) {
		if member.ID != n.cfg.NodeID && member.State != cluster.MemberDown {
			out = append(out, member.ID)
		}
	}
	return out
}

func (n *Node) ownerOf(slot uint16, db uint64) (string, bool) {
	info, ok := n.topology.Database(db)
	if !ok {
		return "", false
	}
	owner, ok := n.topology.Table().Owner(slot, info.RF)
	return owner.ID, ok
}

// catchUpSource chooses where a catch-up reads from: the reachable copy that holds the
// highest position for the pair, asked by POSITION_QUERY, provided it is beyond this
// copy's own. A copy that fell behind its owner picks the owner; the new owner of a pair
// whose old owner died picks the surviving copy that has the most (write path section 8),
// so failover prefers a survivor with the record and the table stays the same on every node.
// Catching up from a node that is behind would be read as a divergent tail, which is why a
// node is asked for its position first and never assumed to be ahead.
//
// Besides the copies, every member that left the copy set and still holds the pair's SEEDED
// bit is asked (Handoff.Holders): an activation adds a copy before that copy holds the data,
// so the copy the activation retired may be the only one with the acknowledged writes, and the
// new owner's target may be its position (BUG-0834). The copies are listed first, so on a tie
// the owner stays the source.
func (n *Node) catchUpSource(slot uint16, db uint64) (string, error) {
	return n.catchUpSourceVia(n.cl.links, slot, db)
}

// positionAsker is the one call catchUpSourceVia makes on the peer links: POSITION_QUERY.
type positionAsker interface {
	QueryPosition(ctx context.Context, peer string, query PositionQuery) (PositionReply, error)
}

// catchUpSourceVia is catchUpSource asking through links, so a unit test can answer the
// queries without a peer network.
func (n *Node) catchUpSourceVia(links positionAsker, slot uint16, db uint64) (string, error) {
	own := n.pairs.Snapshot(slot, db).AppliedPos
	candidates := n.copiesOf(slot, db)
	for _, id := range n.handoff.Holders(slot, db) {
		if !slices.Contains(candidates, id) {
			candidates = append(candidates, id)
		}
	}
	var offers []catchUpOffer
	var silent []error
	for _, id := range candidates {
		ctx, cancel := context.WithTimeout(n.cl.ctx, linkCallTimeout)
		reply, err := links.QueryPosition(ctx, id, PositionQuery{Slot: slot, DB: db})
		cancel()
		if err != nil {
			silent = append(silent, err)
			continue // unreachable: not a source
		}
		offers = append(offers, catchUpOffer{ID: id, Reply: reply})
	}
	if len(silent) > 0 && !n.ownerAnswered(slot, db, offers) {
		// A live copy that did not answer may be ahead of every one that did, with a degraded
		// write only it holds: reaching the best answer would mark the pair current on no evidence
		// and let a new owner open behind it (BUG-0926). A copy that is not the owner reads from
		// the owner, which ordered every write of the epoch, whatever another copy does.
		return "", fmt.Errorf("%d of the copies of slot %d of database %d did not answer, so no catch-up source is taken and the pair is not taken as current; the catch-up asks again: %w",
			len(silent), slot, db, errors.Join(silent...))
	}
	best, found := chooseCatchUpSource(own, offers)
	if found {
		return best, nil
	}
	return "", ErrNoCatchUpSource
}

// ownerAnswered reports whether the pair's owner is another node that answered as a ready copy:
// the one source a copy may catch up from while another copy is silent.
func (n *Node) ownerAnswered(slot uint16, db uint64, offers []catchUpOffer) bool {
	owner, ok := n.ownerOf(slot, db)
	if !ok || owner == n.cfg.NodeID {
		return false
	}
	return slices.ContainsFunc(offers, func(offer catchUpOffer) bool {
		return offer.ID == owner && offer.Reply.State == PositionStateReady
	})
}

// catchUpOffer is one copy's answer to POSITION_QUERY.
type catchUpOffer struct {
	ID    string
	Reply PositionReply
}

// chooseCatchUpSource picks the member with the highest position beyond own whose reply is
// evidence, as handoff.positionOf reads it: ready, or retired (out of the copy set, nothing
// deleted yet, its log still serves a catch-up: BUG-0834). A seeding one reports the cut and
// holds no log for it, and a retiring one has started deleting (T3 review F4b).
func chooseCatchUpSource(own cluster.Position, offers []catchUpOffer) (string, bool) {
	best, bestPos := "", own
	for _, offer := range offers {
		evidence := offer.Reply.State == PositionStateReady || offer.Reply.State == PositionStateRetired
		if evidence && bestPos.Less(offer.Reply.AppliedPos) {
			best, bestPos = offer.ID, offer.Reply.AppliedPos
		}
	}
	return best, best != ""
}

// controlLease is the node's lease for a networked node: the control channel's own lease
// machine, so a grant the renewer accepts is the one the writer checks.
type controlLease struct {
	ctrl     *control.Control
	cfg      func() machine.LeaseConfig
	clock    bootclock.Clock
	quorumAt atomic.Int64
	behind   behindMark
}

// MarkBehind implements BehindMarker.
func (l *controlLease) MarkBehind(epoch uint64, fromClient bool) {
	if fromClient {
		l.behind.MarkClient(epoch, l.clock.BootNow())
		return
	}
	l.behind.MarkMember(epoch, l.clock.BootNow())
}

// BehindEpoch implements BehindReporter.
func (l *controlLease) BehindEpoch(committed uint64) (uint64, bool) {
	return l.behind.Behind(committed, l.clock.BootNow())
}

// confirmEpoch is the word of a lease grantor asked at sentAt that the cluster is at epoch:
// a mark above it that was set before the ask was never real.
func (l *controlLease) confirmEpoch(epoch uint64, sentAt time.Duration) {
	l.behind.Confirm(epoch, sentAt)
}

// quorumAgeHeartbeats is the quorum bound in lease-renewal intervals: a node that has not
// confirmed a voter majority for this many heartbeats (the leader by VerifyLeader, a
// follower by an accepted grant, which the leader gives only after its own quorum round)
// answers NO_QUORUM although its lease may still be valid. Three heartbeats rides out one
// or two lost rounds and closes the step_down window in under half of it at the defaults
// (heartbeat 100 ms, step_down 2000 ms).
const quorumAgeHeartbeats = 3

// QuorumLost implements QuorumReporter.
func (l *controlLease) QuorumLost() bool {
	cfg := l.cfg()
	if cfg.Members <= 1 {
		return false
	}
	at := l.quorumAt.Load()
	if at == 0 {
		return false // no round yet: the lease itself is not valid either
	}
	return l.clock.BootNow()-time.Duration(at) > quorumAgeHeartbeats*cfg.Heartbeat
}

// noteQuorum records that a voter majority was confirmed just now.
func (l *controlLease) noteQuorum() { l.quorumAt.Store(int64(l.clock.BootNow())) }

// Serve implements ServeChecker.
func (l *controlLease) Serve(check machine.ServeCheck) machine.LeaseDecision {
	decision := l.ctrl.Apply(check, l.cfg())
	if _, behind := l.behind.Behind(check.CommittedEpoch, l.clock.BootNow()); behind {
		// conventions section 7: seeing a later epoch stops serving at once, before the
		// FSM applies it. The refusal is the lease's (SPEC-GAP G6).
		decision.Valid, decision.Behind = false, true
	}
	return decision
}

// newRenewer builds the node's lease renewer; the supervisor starts it once Raft and the control
// channel are open.
func (n *Node) newRenewer() *renewer {
	c := n.cl
	return newRenewer(renewerDeps{
		Self: n.cfg.NodeID, Clock: bootclock.New(), Control: c.ctrl, Seat: n.raft, Lease: c.lease,
		Config:    func() machine.LeaseConfig { return *c.leaseCfg.Load() },
		Committed: n.topology.Committed,
		Report:    c.report,
		Warn:      c.warn,
	})
}

// wallClock is the lease's wall reading; only differences are used.
func wallClock() time.Duration { return time.Duration(time.Now().UnixNano()) }

// newControl builds the control channel once Raft is open.
func (n *Node) newControl() (*control.Control, error) {
	return control.New(control.Config{
		SelfID: n.cfg.NodeID, StepDown: 2 * time.Second, Clock: bootclock.New(), Wall: wallClock,
		Committed: func() (uint64, uint64) { return uint64(n.topology.Committed()), uint64(n.raft.StateSeq()) },
		Term:      n.raft.Term,
		State: func() cluster.MemberState {
			for _, m := range n.raft.State().Members {
				if m.ID == n.cfg.NodeID {
					return m.State
				}
			}
			return cluster.MemberJoining
		},
		VerifyLeader: func(context.Context) (time.Duration, error) { return n.raft.VerifyLeader() },
		Hello:        n.hello,
		Leader: func() (string, bool) {
			id := n.raft.Leader()
			return id, id != ""
		},
		Members: n.raft.MemberIDs,
		Addr:    func(id string) (string, bool) { return n.raft.MemberAddr(id) },
		Dial: func(ctx context.Context, member, addr string) (net.Conn, error) {
			return n.cl.net.DialControl(ctx, member, addr)
		},
		RequestSent:    func(ev machine.RequestSent) { n.cl.ctrl.Apply(ev, *n.cl.leaseCfg.Load()) },
		ReportReceived: func(ev machine.ReportReceived) { n.cl.super.onReport(ev) },
		NoticeReceived: func(uint64, uint64) { n.cl.signal() },
		Relay:          n.serveRelay,
		Membership:     n.serveMembershipRelay,
		Pepper:         n.servePepper,
	})
}

// stopCluster ends the supervisor, the replicator, catch-ups and the network.
func (n *Node) stopCluster() error {
	c := n.cl
	if c == nil {
		return nil
	}
	c.serving.Store(false)
	if parts := c.seeding.Load(); parts != nil {
		parts.puller.Close()
	}
	c.cancel()
	c.runWG.Wait()
	c.catchWG.Wait()
	var errs []error
	if c.repl != nil {
		c.repl.Close()
	}
	if c.ctrl != nil {
		errs = append(errs, c.ctrl.Close())
	}
	if c.net != nil {
		errs = append(errs, c.net.Close())
	}
	if c.seed != nil {
		c.seed.Close()
	}
	return errors.Join(errs...)
}

// startCatchUp runs a catch-up of one pair from its owner in the background, once at a
// time per pair: a copy that saw a gap asks for the records it missed. It is a copy's
// catch-up (Replicator.CatchUpAsCopy): it ends when this node becomes the pair's owner, and
// the handoff's own catch-up cancels it and waits for it before it runs.
func (n *Node) startCatchUp(slot uint16, db uint64) {
	c := n.cl
	key := pairKey{slot, db}
	c.catching.Lock()
	if _, busy := c.running[key]; busy || c.ctx.Err() != nil {
		c.catching.Unlock()
		return
	}
	c.running[key] = struct{}{}
	c.catchWG.Add(1)
	c.catching.Unlock()
	go func() {
		defer c.catchWG.Done()
		defer func() {
			c.catching.Lock()
			delete(c.running, key)
			c.catching.Unlock()
		}()
		ctx, cancel := context.WithTimeout(c.ctx, bootTimeout)
		defer cancel()
		// A reset is the designed answer to a catch-up that cannot close, not a failure to report.
		if err := c.repl.CatchUpAsCopy(ctx, slot, db); err != nil && !errors.Is(err, ErrResetAndSeed) && c.ctx.Err() == nil {
			c.report(fmt.Errorf("catch-up of slot %d of database %d: %w", slot, db, err))
		}
	}()
}

// ---- the node's own membership API -------------------------------------------------

// Identity is how the cluster names this node: its id, incarnation, internode address and
// the address drivers connect to, when it has one.
func (n *Node) Identity() cluster.Member {
	member := cluster.Member{ID: n.cfg.NodeID, Incarnation: n.incarnation, DataAddr: n.dataAddr(), Witness: n.cfg.Witness}
	if n.cl != nil {
		member.ClientAddr = n.cfg.ClientAddr
	}
	return member
}

// dataAddr is the address this node tells its peers to dial: the configured Advertise, else
// the address its peer listener is bound to; empty for a one-voter node, which has no peers.
func (n *Node) dataAddr() string {
	if n.cl == nil {
		return ""
	}
	if n.cfg.Advertise != "" {
		return n.cfg.Advertise
	}
	return n.cl.net.Addr().String()
}

// LeaseValid reports whether a keyed request would find this node's lease valid now.
func (n *Node) LeaseValid() bool { return n.live.LeaseValid() }

// QuorumLost reports whether a keyed request would be refused NO_QUORUM now: the lease can
// be valid while the last confirmed voter-majority round is older than the quorum bound.
func (n *Node) QuorumLost() bool {
	reporter, ok := n.lease.(QuorumReporter)
	return ok && reporter.QuorumLost()
}

// IsLeader reports whether this node is the Raft leader now.
func (n *Node) IsLeader() bool { return n.raft.IsLeader() }

// LeaderID is the id of the leader this node knows, or empty.
func (n *Node) LeaderID() string { return n.raft.Leader() }

// AwaitingAdmission reports whether this node opened to join with no Raft state and no member has
// admitted it yet (BUG-0870 guard), so it refuses every peer connection but the identity channel.
func (n *Node) AwaitingAdmission() bool { return n.raft.AwaitingAdmission() }

// State is a copy of the committed cluster state this node has applied.
func (n *Node) State() cluster.ClusterState { return n.raft.State() }

// ClusterID is the id of the cluster whose state this node has applied, read without a copy; zero
// before the node applied its cluster's first command. A token of a cluster account names it.
func (n *Node) ClusterID() [16]byte {
	id, _ := n.raft.PepperIdentity()
	return id
}

// Users is the user table as this node has applied it, read without cloning the rest of the state
// (ring user store spec 6.3, B16). Its slices are shared with the committed state and must never
// be written.
func (n *Node) Users() cluster.UserView { return n.raft.Users() }

// UserTableChanged is signalled after each command this node applies (coalesced: at most one
// wake waits), so a reader of Users learns of a change without polling. It is nil on a node with
// no cluster, whose table never changes.
func (n *Node) UserTableChanged() <-chan struct{} {
	if n.cl == nil {
		return nil
	}
	return n.cl.users
}

// TakeRaftSnapshot writes a Raft snapshot of the applied state now, instead of at the library's
// next interval: a test that scans the snapshot files (the user store's no-pepper scan) needs one.
func (n *Node) TakeRaftSnapshot() error {
	if err := n.raft.TakeSnapshot(); err != nil {
		return fmt.Errorf("taking a Raft snapshot on %s: %w", n.cfg.NodeID, err)
	}
	return nil
}

// Promote makes a joined non-voter a voter.
func (n *Node) Promote(ctx context.Context, id string) error {
	if _, err := n.raft.PromoteWithin(id, n.cfg.Voters); err != nil {
		return fmt.Errorf("promoting %s: %w", id, err)
	}
	n.topology.Refresh()
	return nil
}

// Activate moves a member from joining to active, which ranks it. It is the activation gate
// itself (the seeding check below), the one call the join, the joiner's ask and the harness make. A joiner that still has
// pairs to pull is refused with ErrStillSeeding: ranking it first would make it the owner of
// a slot it holds no data for (seeding section 9). The gate reads the committed SEEDED rows of
// the joiner's placing set, so it makes no round trip to the joiner.
func (n *Node) Activate(ctx context.Context, id string) error {
	if n.cl != nil {
		stateSeq, err := n.seededFor(id)
		if err != nil {
			return err
		}
		// The gate checked the copy sets of one state_seq. The activation is proposed with that
		// base_seq, so the FSM refuses it STATE_MOVED if anything committed since: an epoch
		// change, an RF change (which moves copy sets and not the epoch), or any other command.
		return n.setMemberStateAt(ctx, id, "active", stateSeq)
	}
	return n.setMemberState(ctx, id, "active")
}

// CheckSeeded runs the activation gate alone: it reads the committed SEEDED rows and asks the
// joiner nothing.
func (n *Node) CheckSeeded(_ context.Context, id string) error {
	_, err := n.seededFor(id)
	return err
}

// ActivateAtStateSeq is Activate with the state_seq the caller decided at. It does not rerun the
// gate: the caller ran it at stateSeq (see Activate), and the FSM refuses the proposal
// STATE_MOVED if anything committed since.
func (n *Node) ActivateAtStateSeq(ctx context.Context, id string, stateSeq int64) error {
	return n.setMemberStateAt(ctx, id, "active", stateSeq)
}

// AlterDatabaseRF commits a new copy count for database name. It moves copy sets without
// moving the epoch.
func (n *Node) AlterDatabaseRF(ctx context.Context, name string, rf int) error {
	var dbID int64 = -1
	for _, database := range n.raft.State().Databases {
		if database.Name == name {
			dbID = database.ID
		}
	}
	if dbID < 0 {
		return fmt.Errorf("database %q does not exist. Run 'show databases' to list them", name)
	}
	body := bsoncore.BuildDocument(nil, joinElements(bsoncore.AppendInt64Element(nil, "id", dbID), bsoncore.AppendInt32Element(nil, "rf", int32(rf))))
	if _, err := n.propose(ctx, cluster.CommandAlterDatabase, body); err != nil {
		return fmt.Errorf("setting database %q to %d copies: %w", name, rf, err)
	}
	return nil
}

func (n *Node) setMemberState(ctx context.Context, id, state string) error {
	return n.setMemberStateAt(ctx, id, state, 0)
}

// setMemberStateAt proposes the member state against state_seq stateSeq, or against the
// current one (through the relay, which stamps again after STATE_MOVED) when stateSeq is 0. A
// fixed stateSeq is never stamped again: if the state moved, the FSM's STATE_MOVED is the
// answer.
func (n *Node) setMemberStateAt(ctx context.Context, id, state string, stateSeq int64) error {
	elems := bsoncore.AppendStringElement(bsoncore.AppendStringElement(nil, "id", id), "state", state)
	// The command moves the member only from the state and incarnation this node sees, so a
	// repeat of it after the member moved on is refused instead of undoing the newer change.
	for _, m := range n.raft.State().Members {
		if m.ID == id {
			elems = append(elems, bsoncore.AppendStringElement(nil, "expect_state", m.State.String())...)
			elems = append(elems, bsoncore.AppendInt64Element(nil, "incarnation", m.Incarnation)...)
		}
	}
	body := bsoncore.BuildDocument(nil, elems)
	var err error
	if stateSeq == 0 {
		_, err = n.propose(ctx, cluster.CommandSetMemberState, body)
	} else {
		_, err = n.proposeAtSeq(ctx, cluster.CommandSetMemberState, body, uint64(stateSeq))
	}
	if err != nil {
		return fmt.Errorf("setting %s %s: %w", id, state, err)
	}
	return nil
}

// SetSetting commits one cluster setting (heartbeat_ms, probe_interval_ms, ...). It applies
// only while the setting holds the value this node sees now.
func (n *Node) SetSetting(ctx context.Context, key string, millis int64) error {
	elems := bsoncore.AppendStringElement(nil, "key", key)
	current, _ := n.raft.State().Settings.Value(key)
	if key == "probe_failures" {
		elems = bsoncore.AppendInt32Element(elems, "value", int32(millis))
	} else {
		elems = bsoncore.AppendInt64Element(elems, "value", millis)
	}
	switch now := current.(type) {
	case int32:
		elems = bsoncore.AppendInt32Element(elems, "expect_value", now)
	case int64:
		elems = bsoncore.AppendInt64Element(elems, "expect_value", now)
	case bool:
		elems = bsoncore.AppendBooleanElement(elems, "expect_value", now)
	}
	if _, err := n.propose(ctx, cluster.CommandSetSetting, bsoncore.BuildDocument(nil, elems)); err != nil {
		return fmt.Errorf("setting %s to %d: %w", echoName(key), millis, err)
	}
	return nil
}

// CreateDatabaseRF creates database name with rf copies of every slot.
func (n *Node) CreateDatabaseRF(ctx context.Context, name string, rf int) (uint64, error) {
	return n.CreateDatabaseWithStorage(ctx, name, rf, "")
}

// CreateDatabaseWithStorage creates database name with rf copies of every slot and the
// storage word "disk" or "memory"; "" is disk.
func (n *Node) CreateDatabaseWithStorage(ctx context.Context, name string, rf int, storage string) (uint64, error) {
	if err := databaseNameRefusal(name, n.cfg.StorageNames); err != nil {
		return 0, err
	}
	state := n.topology.Refresh()
	id := state.NextDatabaseID
	body := createDatabaseBody(name, id, rf, storage)
	if _, err := n.propose(ctx, cluster.CommandCreateDatabase, body); err != nil {
		return 0, fmt.Errorf("creating database %q with %d copies: %w", name, rf, err)
	}
	return uint64(id), nil
}

// createDatabaseBody is the create command body. The storage element is written only for a
// non-empty word, so a create that names no storage keeps the body every earlier build wrote.
func createDatabaseBody(name string, id int64, rf int, storage string) []byte {
	parts := [][]byte{
		bsoncore.AppendStringElement(nil, "name", name),
		bsoncore.AppendInt64Element(nil, "id", id),
		bsoncore.AppendInt32Element(nil, "rf", int32(rf)),
		bsoncore.AppendInt64Element(nil, "schema_version", 1),
	}
	if storage != "" {
		parts = append(parts, bsoncore.AppendStringElement(nil, "storage", storage))
	}
	return bsoncore.BuildDocument(nil, joinElements(parts...))
}

func joinElements(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

// LocalDoc is a document as one node's Badger holds it.
type LocalDoc struct {
	Slot  uint16
	EncID []byte
	// Fields are the stored values by field id (type byte then value).
	Fields map[uint16][]byte
}

// LocalDocument reads document id of bucket from this node's own Badger, whatever the
// table says: no gate, no forward. It is how a test proves a copy holds a document.
func (n *Node) LocalDocument(ctx context.Context, db, bucket, id string) (LocalDoc, bool, error) {
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return LocalDoc{}, false, err
	}
	encID, err := encodeID(id)
	if err != nil {
		return LocalDoc{}, false, err
	}
	snapshot := n.kv.NewSnapshot()
	defer snapshot.Discard()
	slot := ref.slot(encID)
	scanned, found, err := readStoredDocument(snapshot, ReadRequest{Slot: slot, DBID: ref.dbID, BucketID: ref.entry.Decl.BucketID, EncID: encID}, n.ringAdvice())
	if err != nil || !found {
		return LocalDoc{}, false, err
	}
	doc := LocalDoc{Slot: slot, EncID: encID, Fields: make(map[uint16][]byte, len(scanned.fields))}
	for _, field := range scanned.fields {
		doc.Fields[field.id] = append([]byte(nil), field.stored...)
	}
	return doc, true, nil
}

func encodeID(id string) ([]byte, error) {
	enc, err := store.EncodeComponent(id)
	if err != nil {
		return nil, fmt.Errorf("encoding the _id_ %q: %w", id, err)
	}
	return enc, nil
}

// PairPosition is the applied position of the pair a document lives in on this node.
func (n *Node) PairPosition(ctx context.Context, db, bucket, id string) (cluster.Position, error) {
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return cluster.Position{}, err
	}
	encID, err := encodeID(id)
	if err != nil {
		return cluster.Position{}, err
	}
	return n.pairs.Snapshot(ref.slot(encID), ref.dbID).AppliedPos, nil
}

// Owner names the node that owns the slot a document lives in, and its copies in rank order.
func (n *Node) Owner(ctx context.Context, db, bucket, id string) (owner string, copies []string, err error) {
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return "", nil, err
	}
	encID, err := encodeID(id)
	if err != nil {
		return "", nil, err
	}
	info, _ := n.topology.Database(ref.dbID)
	slot := ref.slot(encID)
	if m, ok := n.topology.Table().Owner(slot, info.RF); ok {
		owner = m.ID
	}
	for _, m := range n.topology.Table().Copies(slot, info.RF) {
		copies = append(copies, m.ID)
	}
	return owner, copies, nil
}

// PlacementOf is the slot a document id places at in db.bucket, for a caller that must
// pick an id owned by a chosen node.
func PlacementOf(db, bucket, id string) (uint16, error) {
	encID, err := encodeID(id)
	if err != nil {
		return 0, err
	}
	return storage.SlotOf(storage.PlacementKey(db, bucket, encID)), nil
}

// Hello is this node's handshake, as it presents itself to a peer.
func (n *Node) Hello() transport.Handshake { return n.hello() }

// wcTimeout is replication.wc_timeout_ms: the option when set, one second otherwise.
func (n *Node) wcTimeout() time.Duration {
	if n.cl != nil && n.cl.opts.WCTimeout > 0 {
		return n.cl.opts.WCTimeout
	}
	return time.Second
}

// nextIncarnation is this process's incarnation: the wall clock in nanoseconds, but never
// at or below the last one the directory recorded, so a clock stepped back (a VM restore,
// an NTP correction) cannot make peers refuse the restarted node as a stopped process
// speaking for a replaced identity. The high-water mark is written before it is used.
func nextIncarnation(cfg NodeConfig) (int64, error) {
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return 0, fmt.Errorf("creating the node directory %s: %w", cfg.Dir, err)
	}
	path := filepath.Join(cfg.Dir, "incarnation")
	last := int64(0)
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		parsed, perr := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if perr != nil || parsed < 1 {
			// Guessing a value could undo the monotonic guarantee and lock the node out of
			// its cluster, so an unreadable record stops the start.
			return 0, fmt.Errorf("the incarnation record %s holds %q, which is not an incarnation. It is written atomically, so this is a hand edit or a damaged disk. Check the disk, then delete %s once the clock is right (the node takes the clock's value, which must be later than its last run) and start again", path, strings.TrimSpace(string(raw)), path)
		}
		last = parsed
	case !errors.Is(err, os.ErrNotExist):
		return 0, fmt.Errorf("reading the incarnation record %s: %w", path, err)
	}
	next := max(cfg.Now().UnixNano(), last+1)
	if err := storage.WriteFileDurably(path, []byte(strconv.FormatInt(next, 10)+"\n")); err != nil {
		return 0, fmt.Errorf("recording the incarnation in %s: %w", path, err)
	}
	return next, nil
}

// copySchemaReady holds a replicated record until this copy has published the declaration
// version the owner wrote it under, for at most schemaWait. The owner's alter commits in
// Raft and the pairs publish it; a record can arrive between the two, and appending it
// then failing to resolve its store field would fence the pair (U40-7). A refusal is
// retryable: the owner re-sends, or the copy catches up.
func (n *Node) copySchemaReady(db uint64, op cluster.Op) error {
	if op.DeclVersion == 0 || op.BucketID == 0 {
		return nil
	}
	deadline := time.Now().Add(schemaWait)
	for {
		view := n.schema.View(db, op.BucketID)
		if !view.Present {
			ctx, cancel := context.WithTimeout(context.Background(), schemaFetchWait)
			err := n.schema.Refetch(ctx, db, op.BucketID)
			cancel()
			if err != nil {
				// Without the declaration the copy cannot resolve the record's store fields,
				// and appending it would fence the pair when the apply fails (U40-7). The
				// owner retries; the copy answers once it holds the declaration.
				return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
					"this copy does not hold the declaration of bucket %d in database %d yet and could not fetch it (%v). Nothing was applied. The owner retries it; if it keeps failing run 'nodes' to see whether the node that owns the declaration is up", op.BucketID, db, err)}
			}
			view = n.schema.View(db, op.BucketID)
		}
		if _, version := machine.EffectiveTable(view.Decl, view.Published); version >= op.DeclVersion {
			return nil
		}
		if time.Now().After(deadline) {
			return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
				"this copy has published schema version %d of bucket %d in database %d and the record was written under %d. Nothing was applied. The owner retries it; if it keeps failing run 'nodes' to see whether this node is applying cluster changes", viewVersion(view), op.BucketID, db, op.DeclVersion)}
		}
		select {
		case <-n.cl.ctx.Done():
			return n.cl.ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func viewVersion(view SchemaView) int64 {
	_, version := machine.EffectiveTable(view.Decl, view.Published)
	return version
}

// schemaWait bounds how long a copy waits for its own schema publication before it
// refuses a replicated record. It is well under the owner's linkCallTimeout (2 s), or the
// owner's call would time out first and the refusal text would never be seen.
const schemaWait = 800 * time.Millisecond

// schemaFetchWait bounds the declaration fetch a copy makes when it holds none. It is well
// under the owner's linkCallTimeout: a fetch from an owner that has stopped answering must
// give up while the replicated write that asked can still be answered, or the write fails
// with the call and every retry waits the same way. A fetch that fails is a retryable
// refusal: the copy appends nothing without the declaration.
const schemaFetchWait = 300 * time.Millisecond

// SchemaReady is the check a copy makes before it appends a replicated record: nil once the
// copy has published the record's declaration version, a retryable refusal after a bounded
// wait otherwise. It is exported so a test can drive it at the node, not at a stub.
func (n *Node) SchemaReady(db uint64, op cluster.Op) error { return n.copySchemaReady(db, op) }
