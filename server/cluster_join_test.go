package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterJoinCommitsByMajorityAndConverges: an operator joins a node by a
// majority of the current members; the epoch advances to the next value, the
// rest of the cluster converges to it, and the original cluster keeps serving
// (the old "6th node poisons DDL" failure mode does not happen).
func TestClusterJoinCommitsByMajorityAndConverges(t *testing.T) {
	// The member this test joins is fabricated and never runs, so the join
	// never settles and the freeze would refuse the schema changes the test
	// makes after it; the test is about the vote, not the freeze.
	switchJoinFreezeOff(t)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	oc0 := tc.nodes[0].server.core.cluster
	if oc0.Epoch() != InitialEpoch {
		t.Fatalf("node 0 epoch = %d, want %d", oc0.Epoch(), InitialEpoch)
	}

	ja := fabricatedMemberAddr(t)
	newcomer := ring.Node{ID: ja, Addr: ja}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Three members, majority 2. The proposer votes itself and needs one more.
	before := len(oc0.Members())
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	if got := oc0.Epoch(); got != InitialEpoch+1 {
		t.Fatalf("committed epoch = %d, want %d", got, InitialEpoch+1)
	}
	if got := len(oc0.Members()); got != before+1 {
		t.Fatalf("committed members = %d, want %d", got, before+1)
	}

	// The voter that acked is already on the new epoch; the third member
	// converges over a real gossip round trip with the proposer.
	for i := 1; i < 3; i++ {
		ci := tc.nodes[i].server.core.cluster
		peerTo0 := ci.peerFor(mustNode(t, ci, tc.addrs[0]))
		if err := peerTo0.gossip(ctx); err != nil {
			t.Fatalf("gossip from node %d to node 0: %v", i+1, err)
		}
		if ci.Epoch() != InitialEpoch+1 {
			t.Fatalf("node %d did not converge: epoch %d, want %d", i+1, ci.Epoch(), InitialEpoch+1)
		}
	}
	// Every node now agrees, so DDL and writes must work cluster-wide.
	for i := range tc.nodes {
		if tc.nodes[i].server.core.cluster.mismatch.Load() {
			t.Fatalf("node %d has the divergence flag set after a clean join", i+1)
		}
	}
	// DDL after the join is not poisoned: a database create is broadcast to every
	// member (no bucket owner), which is exactly the operation the old
	// "membership disagrees" poison used to break cluster-wide.
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "postjoindb"})

	// A write to a bucket that is still owned by an original (running) member
	// lands. Buckets whose ownership moved to the not-yet-running newcomer are
	// unanswered until the newcomer is up — the expected 1b behaviour — so
	// probe a bucket that stayed on a running node.
	running := map[string]bool{}
	for _, a := range tc.addrs {
		running[a] = true
	}
	probed := false
	for _, name := range []string{testBucket} {
		o, err := oc0.Owner(placementOf(testDatabase, name))
		if err != nil || !running[o.ID] {
			continue
		}
		if frame := tc.frame(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: name, Key: "still-works",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "majority",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("write to %s (owner %s) after join was rejected: %s", name, o.ID, string(frame.Payload))
		}
		probed = true
	}
	if !probed {
		t.Skip("all pre-join buckets moved to the newcomer; nothing left on a running original owner to probe")
	}
}

// TestClusterJoinRejectsInvalidProposals pins the safety edges of the join:
// a node already in the set, a transition already in progress, a proposer
// that is not a member, an epoch jump, and a proposal that drops a member.
func TestClusterJoinRejectsInvalidProposals(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	oc0 := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	newcomer := ring.Node{ID: "n@1", Addr: "n@1"}

	// Already a member: the proposer counts itself in curMembers, so adding it
	// is refused (use an actual member id).
	member0 := mustNode(t, oc0, tc.addrs[0])
	if err := oc0.proposeJoin(ctx, member0); err == nil {
		t.Fatal("adding an existing member was accepted")
	} else if !strings.Contains(err.Error(), ErrAlreadyMember.Error()) {
		t.Fatalf("expected %q, got %q", ErrAlreadyMember, err)
	}

	// A transition in progress rejects the next proposal.
	oc0.transition.Store(true)
	if err := oc0.proposeJoin(ctx, newcomer); err == nil || !strings.Contains(err.Error(), ErrEpochInProgress.Error()) {
		t.Fatalf("expected %q, got %v", ErrEpochInProgress, err)
	}
	oc0.transition.Store(false)

	// The accept phase's safety edges, called directly.
	full := append([]ring.Node{newcomer}, oc0.Members()...)
	joining := Peer{ID: newcomer.ID, Addr: newcomer.Addr}

	// Non-member proposer.
	if _, err := oc0.acceptMembershipChange(acceptRequestOf("stranger@9.9.9.9:1", InitialEpoch+1, full, joining)); err == nil {
		t.Fatal("a non-member proposer was accepted")
	}

	// Epoch jump (proposes +2 instead of +1).
	if _, err := oc0.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+2, full, joining)); err == nil {
		t.Fatal("an epoch-jumping proposal was accepted")
	}

	// Dropping an existing member is not a join.
	dropped := full[1:]
	if _, err := oc0.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, dropped, joining)); err == nil {
		t.Fatal("a proposal that drops an existing member was accepted")
	}
}

// acceptRequestOf is the accept phase of a change to members proposed by
// fromNode, at round 1 of its ballot.
func acceptRequestOf(fromNode string, epoch uint64, members []ring.Node, change Peer) MembershipChangeRequest {
	return MembershipChangeRequest{FromNode: fromNode, Phase: phaseAccept, Epoch: epoch,
		Ballot: ballot{Round: 1, Node: fromNode}, Members: nodesToPeers(members), Change: &change}
}

// TestClusterJoinFencesEmptyOwnedBucket: after a real 4-node join, the joining
// node owns buckets on the new ring it has no data for. A mutation to one must
// be refused with a reason, never accepted into its empty store, and the
// original holders must keep working.
func TestClusterJoinFencesEmptyOwnedBucket(t *testing.T) {
	// The auto-rebalance background sweep would migrate the owned bucket before
	// this test asserts the fence; disable it and drive migration explicitly.
	// This has to be every existing node, not just the proposer: a sweep now
	// runs on every node that adopts the new epoch (a member on the change's
	// commit and reconcileMembership, not just the proposer), and nodes 1/2
	// already hold the bucket the joiner is about to own — their sweep is
	// exactly what would otherwise migrate it before the assertion below.
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()

	const buckets = 8
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("docs%02d", i)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}

	oc0 := tc.nodes[0].server.core.cluster
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}

	// Bring the joining node up against the now-epoch-2 cluster and converge it.
	// AutoRebalance is disabled via opt, before Start() spawns the peer
	// goroutines that read it (see startJoinTarget's doc comment) — setting
	// it afterward raced maybeSweep under -race.
	joiner := startJoinTarget(t, tc, ports, tc.token, func(c *config.Config) {
		c.Server.AutoRebalance = false
	})
	jc := joiner.server.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joining node resync: %v", err)
	}
	if jc.Epoch() != InitialEpoch+1 {
		t.Fatalf("joining node epoch = %d, want %d", jc.Epoch(), InitialEpoch+1)
	}

	// Converge the pre-existing members that were not needed for a majority:
	// a join of 3 asks 1 peer and breaks at 2 votes, so a leftover member stays
	// on the old epoch and a write replicating to it trips the mismatch guard.
	for i := 1; i < 3; i++ {
		ci := tc.nodes[i].server.core.cluster
		if err := ci.peerFor(mustNode(t, ci, tc.addrs[0])).gossip(ctx); err != nil {
			t.Fatalf("gossip from node %d to node 0: %v", i+1, err)
		}
		if ci.Epoch() != InitialEpoch+1 {
			t.Fatalf("pre-existing node %d did not converge: epoch %d, want %d", i+1, ci.Epoch(), InitialEpoch+1)
		}
	}

	// Find a bucket this joining node owns on the new ring but has no store for.
	owned := ""
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("docs%02d", i)
		if o, err := jc.Owner(placementOf(testDatabase, name)); err == nil && o.ID == newcomer.ID {
			owned = name
			break
		}
	}
	if owned == "" {
		t.Skip("joining node owns none of the created buckets; placement did not hand it one")
	}

	// A mutation routed to the joining node for that bucket is fenced.
	payload, _ := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: owned, Key: "k1", Data: map[string]interface{}{"n": 1}})
	frame, err := joiner.client.SendRequest(ctx, transport.OpPut, payload)
	if err != nil {
		t.Fatalf("write to joining node: %v", err)
	}
	// The members hold the bucket, so the owner's answer is the retryable one
	// (BUG-0686 asks them; the sentence names the holders, not "no local copy").
	refused := requireRetryRefusal(t, frame, "a write to an owned-but-empty bucket")
	t.Logf("joining node fenced the write: %s", refused.Message)

	// The original cluster still serves its buckets. Backfill writes onto the
	// joining node's replica positions first (a join re-keys the ring, so an
	// existing bucket can now list the empty joiner as a replica — a majority
	// write would stall on it otherwise), then a normal write must succeed.
	oc0.sweep(ctx)
	wroteOwner := ""
	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("docs%02d", i)
		if o, err := oc0.Owner(placementOf(testDatabase, name)); err == nil && o.ID != newcomer.ID && tc.nodes[0].server.core.bucketStoreOpen(testDatabase, name) {
			wroteOwner = name
			break
		}
	}
	if wroteOwner == "" {
		t.Skip("no non-joining owner for a pre-existing bucket to prove liveness")
	}
	if frame := tc.frame(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: wroteOwner, Key: "still-green",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "majority",
	}); frame.Opcode == transport.OpError {
		t.Fatalf("write to %s (original owner) after join was rejected: %s", wroteOwner, string(frame.Payload))
	}
}

// TestStartJoinTargetPreparedStopsJoinerWhenItsTestReturns is the
// failing-first regression test for a leaked joiner goroutine: startJoinTargetPrepared must
// register a t.Cleanup that stops the joiner *Service it builds, or its
// background reclaimer goroutine (server/reclaim.go:startReclaimer) keeps
// running for the rest of the test binary's life and can race a later,
// unrelated test's global state (the reproduction: the reclaimer's
// orphan tick racing range_raw_test.go's installMapCodecCounter).
//
// The joiner is built inside a subtest so that a registered t.Cleanup fires
// before t.Run returns here — testing.T guarantees cleanups run when the
// test (or subtest) that registered them completes. Without the fix, no
// cleanup is ever registered, so the joiner's context is still live when
// this check runs; with the fix, Stop() has already cancelled it.
func TestStartJoinTargetPreparedStopsJoinerWhenItsTestReturns(t *testing.T) {
	tc := newTestCluster(t, 1, 2)
	defer tc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	oc0 := tc.nodes[0].server.core.cluster
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}

	var joinerCore *Server
	t.Run("leak", func(t *testing.T) {
		joiner := startJoinTarget(t, tc, ports, tc.token)
		joinerCore = joiner.server.core
	})

	if err := joinerCore.ctx.Err(); err == nil {
		t.Fatalf("joiner's context is still live after its creating subtest returned: startJoinTargetPrepared never stopped it, so its reclaimer goroutine (server/reclaim.go:startReclaimer) is still running and can race a later test's global state")
	}
}

// startJoinTarget boots a node at `newcomer` seeded with the current cluster
// plus itself, so it joins on gossip rather than being pre-declared. Any
// opts run on cfg before the node starts — set config from here, not by
// mutating joiner.server.core.config after this returns: Start() already
// has peer-verification goroutines reading it (e.g. maybeSweep reading
// Server.AutoRebalance) by the time this function returns, so a mutation
// afterward is a data race, not just bad style.
func startJoinTarget(t *testing.T, tc *testCluster, ports *testNodePorts, token string, opts ...func(*config.Config)) *testNode {
	t.Helper()
	return startJoinTargetPrepared(t, tc, ports, token, nil, opts...)
}

// startJoinTargetPrepared is startJoinTarget with a hook that runs on the
// built Service after NewService and before Start: the only moment a test can
// put cluster state (e.g. a pending snapshot pull) in place before the
// node's listeners open and its peers start shipping to it. prepare may be
// nil.
func startJoinTargetPrepared(t *testing.T, tc *testCluster, ports *testNodePorts, token string, prepare func(*Service), opts ...func(*config.Config)) *testNode {
	t.Helper()
	newcomer := ports.ringNode()
	dir := t.TempDir()
	if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save joiner token: %v", err)
	}
	// The existing nodes must pin this newcomer before they dial it; the
	// joiner pins the existing fleet from tc.fingerprints.
	trustJoinerFingerprint(t, tc, newcomer.Addr, dir)
	seeds := append([]string{}, tc.addrs...)
	seeds = append(seeds, newcomer.Addr)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = newcomer.Addr
	cfg.Server.NodeID = newcomer.ID
	cfg.Server.ReplicationFactor = tc.rf
	cfg.HashRing.SeedNodes = seeds
	cfg.HashRing.SeedFingerprints = append([]string(nil), tc.fingerprints...)
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Logging.Output = "stderr"
	// As in testCluster.startNode: one host, many nodes — the TCP driver
	// listener would collide on a fixed default port, so each node
	// (including a joiner) gets its own bound driver listener.
	cfg.TCP.Listen = ports.driver.Addr().String()
	for _, opt := range opts {
		opt(cfg)
	}

	srv, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("new joining node: %v", err)
	}
	srv.driverListener = ports.driver
	srv.internodeListener = ports.internode
	// Healthy by default, as in testCluster.startNode (BUG-0553); prepare may
	// pin a red disk.
	setCapacityProbes(t, srv.core, harnessProbes)
	if prepare != nil {
		prepare(srv)
	}
	if err := startWithRetry(srv, 3*time.Second); err != nil {
		t.Fatalf("start joining node: %v", err)
	}
	// Every joiner this helper hands back must be stopped when the test that
	// created it returns, or its reclaimer goroutine (reclaim.go:startReclaimer)
	// outlives the test binary's process life and can race a later, unrelated
	// test's global state.
	t.Cleanup(func() { _ = srv.Stop() })
	if err := srv.core.SeedRootUser(testRootUser, testRootPassword); err != nil {
		t.Logf("seed joining node root user: %v", err)
	}
	client := dialAddr(t, ports.driver.Addr().String())
	return &testNode{cfg: cfg, addr: ports.ringAddr, server: srv, client: client}
}

// dialAddr connects and authenticates a client to an arbitrary node's driver
// address.
func dialAddr(t *testing.T, addr string) *transport.DriverClient {
	t.Helper()
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test driver client"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config for %s: %v", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := transport.NewDriverClient(ctx, addr, tlsCfg)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	payload, err := bson.Marshal(AuthRequest{Username: testRootUser, Password: testRootPassword})
	if err != nil {
		t.Fatalf("marshal auth: %v", err)
	}
	frame, err := client.SendRequest(ctx, transport.OpAuth, payload)
	if err != nil {
		t.Fatalf("auth against %s: %v", addr, err)
	}
	if frame.Opcode != transport.OpAuthOK {
		t.Fatalf("auth against %s rejected with %s", addr, frame.Opcode)
	}
	return client
}

// TestJoinRefusedOnMemtableMismatchText is the design note's §5.4 refusal word
// for word, with the newcomer's memory line (n4 would warn at the cluster's
// size) and without it.
func TestJoinRefusedOnMemtableMismatchText(t *testing.T) {
	n4 := ring.Node{ID: "n4", Addr: "192.0.2.4:4433"}
	got := memtableJoinRefusal(n4, 128, GossipResponse{AppliedMemtableMB: 32, MemoryLimitBytes: limit28GiB}).Error()
	want := "cannot add n4 (192.0.2.4:4433) to the cluster: n4 runs a 32 MiB memtable and this cluster runs 128 MiB. " +
		"Every member must run the same size, because it sets the largest batch a member can apply in one transaction (20 MB at 128 MiB, 5 MB at 32 MiB).\n" +
		"n4's memory limit is 2.8 GiB; at 128 MiB its write bursts can peak near 1.25 GB.\n" +
		"To fix it: give n4 at least 3.5 GiB of memory (from 7.0 GiB a new store chooses 128 MiB by itself), set storage.memtable_mb: 128 in n4's config file, restart n4 (stop it with SIGTERM, not kill -9), then run 'join 192.0.2.4:4433' again."
	if got != want {
		t.Errorf("refusal with the memory line:\n got %s\nwant %s", got, want)
	}

	got = memtableJoinRefusal(n4, 64, GossipResponse{AppliedMemtableMB: 32, MemoryLimitBytes: 16 * gib}).Error()
	want = "cannot add n4 (192.0.2.4:4433) to the cluster: n4 runs a 32 MiB memtable and this cluster runs 64 MiB. " +
		"Every member must run the same size, because it sets the largest batch a member can apply in one transaction (10 MB at 64 MiB, 5 MB at 32 MiB).\n" +
		"To fix it: set storage.memtable_mb: 64 in n4's config file, restart n4 (stop it with SIGTERM, not kill -9), then run 'join 192.0.2.4:4433' again."
	if got != want {
		t.Errorf("refusal without the memory line:\n got %s\nwant %s", got, want)
	}

	got = memtableJoinRefusal(n4, 64, GossipResponse{AppliedMemtableMB: 32, MemoryLimitBytes: 2 * gib}).Error()
	// 3 x 0.72 GB is 2.03 GiB: the figure rounds up, never down to the 2.0 GiB the node already has.
	if !strings.Contains(got, "give n4 at least 2.1 GiB of memory (from 3.5 GiB a new store chooses 64 MiB by itself), set storage.memtable_mb: 64") {
		t.Errorf("a cluster at the automatic 64 MiB does not tell a small newcomer that 3.5 GiB chooses it by itself:\n%s", got)
	}
}

// TestJoinRefusedWithoutMemtableFeature: a newcomer that answers without the
// memtable_mb feature runs a build that cannot say its size, and admitting it
// silently is the divergence the owner's ruling forbids.
func TestJoinRefusedWithoutMemtableFeature(t *testing.T) {
	n4 := ring.Node{ID: "n4", Addr: "192.0.2.4:4433"}
	err := checkNewcomerMemtable(n4, 64, GossipResponse{Success: true, Features: []string{featureBucketRF}}, nil)
	if err == nil || !strings.Contains(err.Error(), "cannot add n4 (192.0.2.4:4433) to the cluster: n4 runs a build that does not report its memtable size, and every member must run the same size. "+
		"Start n4 on this build (") || !strings.Contains(err.Error(), "), with storage.memtable_mb: 64 in its config file, then run 'join 192.0.2.4:4433' again") {
		t.Errorf("a newcomer without the memtable_mb feature: %v, want the refusal that names this build", err)
	}
	if err != nil {
		requireNoOtherBuild(t, err.Error())
	}
	if err := checkNewcomerMemtable(n4, 64, GossipResponse{Success: true, Features: clusterFeatures(), AppliedMemtableMB: 64}, nil); err != nil {
		t.Errorf("a newcomer at the cluster's size was refused: %v", err)
	}
	if err := checkNewcomerMemtable(n4, 64, GossipResponse{}, errors.New("dial 192.0.2.4:4434: connection refused")); err != nil {
		t.Errorf("a newcomer that is not up yet was refused (joining a node before it starts is supported): %v", err)
	}
}

// TestJoinRefusedOnMemtableMismatch: a real newcomer at 32 MiB is refused by a
// cluster at 64 MiB, with the refusal's text, and the membership is unchanged.
func TestJoinRefusedOnMemtableMismatch(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Storage.MemtableMB = 32 })
	newcomer := ring.Node{ID: joiner.cfg.Server.NodeID, Addr: joiner.addr}

	oc0 := tc.nodes[0].server.core.cluster
	before := oc0.Epoch()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := oc0.proposeJoin(ctx, newcomer)
	want := fmt.Sprintf("cannot add %[1]s (%[2]s) to the cluster: %[1]s runs a 32 MiB memtable and this cluster runs 64 MiB. "+
		"Every member must run the same size, because it sets the largest batch a member can apply in one transaction (10 MB at 64 MiB, 5 MB at 32 MiB).\n"+
		"To fix it: set storage.memtable_mb: 64 in %[1]s's config file, restart %[1]s (stop it with SIGTERM, not kill -9), then run 'join %[2]s' again.",
		newcomer.ID, newcomer.Addr)
	if err == nil || err.Error() != want {
		t.Fatalf("joining a 32 MiB newcomer to a 64 MiB cluster:\n got %v\nwant %s", err, want)
	}
	if oc0.Epoch() != before {
		t.Errorf("a refused join moved the epoch from %d to %d", before, oc0.Epoch())
	}
}

func TestJoinAdmittedOnEqualMemtable(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	joiner := startJoinTarget(t, tc, ports, tc.token)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := tc.nodes[0].server.core.cluster.proposeJoin(ctx, ring.Node{ID: joiner.cfg.Server.NodeID, Addr: joiner.addr}); err != nil {
		t.Fatalf("joining a newcomer at the cluster's own 64 MiB: %v", err)
	}
}

// TestPeerMemtableMismatchLogsOncePerChange: members that already disagree
// (started from one template on unequal hosts) never pass through join, so
// every member compares each gossiping peer's size with its own. It logs one
// ERROR per (peer, value) change, not one per gossip round, and status lists
// the disagreement until it is gone.
func TestPeerMemtableMismatchLogsOncePerChange(t *testing.T) {
	var logged bytes.Buffer
	c := &Cluster{
		logger:        slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelError})),
		s:             &Server{memtable: memtableResolution{appliedMB: 128}},
		peerMemtables: make(map[string]int),
	}
	withFeature := clusterFeatures()
	for round := 0; round < 3; round++ {
		c.observePeerMemtable("n2", withFeature, 64)
	}
	if got := strings.Count(logged.String(), "level=ERROR"); got != 1 {
		t.Fatalf("three gossip rounds at one mismatch logged %d ERRORs, want 1:\n%s", got, logged.String())
	}
	for _, want := range []string{"member n2 runs a 64 MiB memtable and this node runs 128 MiB", "set storage.memtable_mb to it in every member's config file"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("ERROR %q does not contain %q", logged.String(), want)
		}
	}
	if differs := c.memtableDisagreements(); len(differs) != 1 || differs[0] != (MemtablePeer{NodeID: "n2", MB: 64}) {
		t.Errorf("disagreements = %v, want n2 at 64", differs)
	}

	c.observePeerMemtable("n2", withFeature, 32)
	if got := strings.Count(logged.String(), "level=ERROR"); got != 2 {
		t.Errorf("a changed mismatch logged %d ERRORs in all, want 2", got)
	}
	c.observePeerMemtable("n2", withFeature, 128)
	c.observePeerMemtable("n3", []string{featureBucketRF}, 0)
	if differs := c.memtableDisagreements(); len(differs) != 0 {
		t.Errorf("disagreements after n2 matched and n3 (an older build) said nothing = %v, want none", differs)
	}
	if got := strings.Count(logged.String(), "level=ERROR"); got != 2 {
		t.Errorf("a match or an older build logged an ERROR: %d in all, want 2", got)
	}
}

// TestGossipRefusalWithABadTokenSaysNothingAboutMemory: a caller without the
// cluster token learns nothing of this node's memory or memtable.
func TestGossipRefusalWithABadTokenSaysNothingAboutMemory(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	payload, err := bson.Marshal(GossipRequest{Token: "not-the-token", NodeID: "stranger", Addr: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for name, body := range map[string][]byte{"a bad token": payload, "an undecodable request": []byte("not bson")} {
		frame, err := handleGossip(context.Background(), transport.NewFrameWithID(transport.OpGossip, [16]byte{1}, body), tc.nodes[0].server.core)
		if err != nil {
			t.Fatalf("%s: handleGossip: %v", name, err)
		}
		var resp GossipResponse
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		if resp.Success || resp.AppliedMemtableMB != 0 || resp.MemoryLimitBytes != 0 {
			t.Errorf("%s: the refusal carries success %v, memtable %d, memory limit %d; an unauthenticated caller must get none of them",
				name, resp.Success, resp.AppliedMemtableMB, resp.MemoryLimitBytes)
		}
	}
}

// TestJoinDoesNotWaitOutAnUnservingNewcomersProbe: a newcomer whose port is
// bound but serves nothing completes the TCP handshake and then says nothing.
// The memtable-size probe is optional, so the join must not wait out the
// deadline the required probes use. The probe is timed on its own: the commit
// round that follows in proposeJoin dials the newcomer too, for up to
// peerDialTimeout, and that wait is not this probe's.
func TestJoinDoesNotWaitOutAnUnservingNewcomersProbe(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	ports := reserveNodePorts(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	started := time.Now()
	if err := clusterOf(tc, 0).admitNewcomerMemtable(ctx, ports.ringNode()); err != nil {
		t.Fatalf("admitNewcomerMemtable: %v", err)
	}
	const probeBudget = 2 * time.Second
	if elapsed := time.Since(started); elapsed >= probeBudget {
		t.Fatalf("the memtable probe took %v against a newcomer that does not serve, want under %v", elapsed, probeBudget)
	}
}
