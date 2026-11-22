package server

import (
	"context"
	"crypto/tls"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// newcomerDatabase is a database name no fixture creates, so a create of it
// that a newcomer answered "created" can only have been decided there.
const newcomerDatabase = "newcomer_db"

// requireNotYetAdded asserts a frame is the refusal of a node that has not
// been added yet: UNAVAILABLE, the text naming the 'join' step, and for a
// mutation the outcome not_applied.
func requireNotYetAdded(t *testing.T, frame *transport.Frame, what string, mutation bool) {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("%s through a node started to join, before 'join': answered %s %s; want UNAVAILABLE naming 'join'",
			what, frame.Opcode, string(frame.Payload))
	}
	var refused ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refused); err != nil {
		t.Fatalf("%s: decoding the refusal: %v", what, err)
	}
	if refused.Code != int(transport.ErrUnavailable) {
		t.Errorf("%s: code %d, want UNAVAILABLE (%d); message %q", what, refused.Code, transport.ErrUnavailable, refused.Message)
	}
	if !strings.Contains(refused.Message, "'join ") {
		t.Errorf("%s: the refusal does not name the 'join' step: %q", what, refused.Message)
	}
	wantOutcome := ""
	if mutation {
		wantOutcome = outcomeNotApplied
	}
	if refused.WriteOutcome != wantOutcome {
		t.Errorf("%s: write outcome %q, want %q", what, refused.WriteOutcome, wantOutcome)
	}
}

// TestANodeStartedToJoinRefusesSchemaChangesBeforeJoin is the first
// criterion: a node started the documented way to join, before 'join' is run,
// answers create database and create bucket UNAVAILABLE, naming the step. It
// answered "created" as the owner of buckets it held no copy of.
func TestANodeStartedToJoinRefusesSchemaChangesBeforeJoin(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	newcomer := startJoinTarget(t, tc, reserveNodePorts(t), tc.token)

	requireNotYetAdded(t, tc.frameOn(newcomer, transport.OpCreateDB, CreateDatabaseRequest{Name: newcomerDatabase}), "create database", true)
	requireNotYetAdded(t, tc.frameOn(newcomer, transport.OpCreateBucket, CreateBucketRequest{
		Database: newcomerDatabase, Name: "orders", Type: BucketTypeHash,
	}), "create bucket", true)
}

// TestANodeStartedToJoinRefusesReadsAndWritesBeforeJoin: its ring is not the
// cluster's, so a read it answered as owner would say "not found" for data the
// cluster holds, and a write would land where no member looks. Requests that
// name no database or bucket are still served.
func TestANodeStartedToJoinRefusesReadsAndWritesBeforeJoin(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}})
	newcomer := startJoinTarget(t, tc, reserveNodePorts(t), tc.token)

	requireNotYetAdded(t, tc.frameOn(newcomer, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "k"}), "get", false)
	requireNotYetAdded(t, tc.frameOn(newcomer, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "k2", Data: map[string]interface{}{"n": 2},
	}), "put", true)

	for _, unscoped := range []struct {
		op      transport.Opcode
		request any
	}{
		{transport.OpListNodes, struct{}{}},
		{transport.OpHealthCheck, HealthRequest{}},
	} {
		if frame := tc.frameOn(newcomer, unscoped.op, unscoped.request); frame.Opcode == transport.OpError {
			t.Errorf("%s through a node waiting for 'join' was refused: %s; a request naming no database or bucket is served",
				unscoped.op, string(frame.Payload))
		}
	}
}

// TestMembersKeepWritingWhileANewcomerWaits is the second criterion: a
// newcomer's view at the members' epoch lists itself, and a member took that
// as a membership mismatch and refused every mutation until the join. A
// non-member's view cannot change a member's placement, so it is ignored.
func TestMembersKeepWritingWhileANewcomerWaits(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	newcomer := startJoinTarget(t, tc, reserveNodePorts(t), tc.token)
	waiting := newcomer.server.core.cluster

	// The newcomer's own view, offered to each member as its gossip offers it.
	for index := range tc.nodes {
		member := clusterOf(tc, index)
		if err := member.reconcileMembership(waiting.self.ID, waiting.memberNodes(), waiting.memberIDs(), waiting.Epoch()); err != nil {
			t.Errorf("node %d answered a newcomer's view with %v; a non-member's view is not a mismatch", index+1, err)
		}
		if member.mismatch.Load() {
			t.Errorf("node %d is in membership mismatch after a newcomer's view", index+1)
		}
	}

	// And over the wire, for as long as the newcomer gossips several times.
	deadline := time.Now().Add(3 * peerRetryInterval)
	for time.Now().Before(deadline) {
		for index := range tc.nodes {
			if clusterOf(tc, index).mismatch.Load() {
				t.Fatalf("node %d went into membership mismatch while a newcomer waited for 'join'", index+1)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	for index := range tc.nodes {
		// Every member writes the same key, so each write after the first replaces it.
		tc.ok(index, transport.OpUpsert, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: "while-waiting", Data: map[string]interface{}{"n": index},
		})
	}
}

// TestAMemberThatDisagreesStillSetsMismatch: a member of this node's epoch that
// sees another member set at the same epoch is a real divergence, and writes
// still fail closed.
func TestAMemberThatDisagreesStillSetsMismatch(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	first := clusterOf(tc, 0)
	second := clusterOf(tc, 1)

	divergent := []Peer{{ID: first.self.ID, Addr: first.self.Addr}, {ID: second.self.ID, Addr: second.self.Addr}}
	err := first.reconcileMembership(second.self.ID, divergent, []string{first.self.ID, second.self.ID}, first.Epoch())
	if !errors.Is(err, ErrMemberMismatch) {
		t.Errorf("a member that sees another member set at the same epoch answered %v, want ErrMemberMismatch", err)
	}
	if !first.mismatch.Load() {
		t.Errorf("a member's disagreement at the same epoch did not set mismatch")
	}
}

// TestANewcomerIgnoresANewerEpochFromANodeOutsideItsSet is BUG-0704's first
// half. A node started to join is on its seed membership, unconfirmed
// (bootstrapped), and a node the cluster removed keeps following later epochs
// and gossips to it at a higher epoch. The newcomer has no placement to protect
// and adopts nothing from a non-member, so the view is noted and the exchange
// answered: it used to set mismatch and refuse requests forwarded to it as
// owner until the join was adopted. A confirmed member given the same view
// still refuses it and sets the flag (TestMembershipRefusesUninvitedProposal).
func TestANewcomerIgnoresANewerEpochFromANodeOutsideItsSet(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	newcomer := startJoinTarget(t, tc, reserveNodePorts(t), tc.token)
	waiting := newcomer.server.core.cluster
	if !waiting.bootstrapped.Load() {
		t.Fatalf("the node started to join is not marked bootstrapped; the test needs a node no member has confirmed")
	}

	const removed = "removed-node"
	if _, listed := waiting.memberIDSet()[removed]; listed {
		t.Fatalf("%s is in the newcomer's member set; it must be outside it", removed)
	}
	view := make([]Peer, 0, len(tc.addrs)+1)
	for index := range tc.nodes {
		self := clusterOf(tc, index).self
		view = append(view, Peer{ID: self.ID, Addr: self.Addr})
	}
	view = append(view, Peer{ID: removed, Addr: removed})
	ids := make([]string, 0, len(view))
	for _, p := range view {
		ids = append(ids, p.ID)
	}

	epochBefore := waiting.Epoch()
	if err := waiting.reconcileMembership(removed, view, ids, epochBefore+3); err != nil {
		t.Errorf("a newcomer answered a newer epoch from a node outside its set with %v; it has no placement to protect", err)
	}
	if waiting.mismatch.Load() {
		t.Errorf("a newcomer set mismatch on a newer epoch from a node outside its set; it refuses forwarded requests while set")
	}
	if got := waiting.Epoch(); got != epochBefore {
		t.Errorf("the newcomer moved from epoch %d to %d on a non-member's view; it adopts nothing from one", epochBefore, got)
	}

	member := clusterOf(tc, 0)
	if member.bootstrapped.Load() {
		t.Fatalf("node 1 of a ready cluster is still bootstrapped; the confirmed-member half needs a confirmed member")
	}
	defer member.mismatch.Store(false)
	if err := member.reconcileMembership(removed, view, ids, member.Epoch()+3); !errors.Is(err, ErrMemberMismatch) {
		t.Errorf("a confirmed member answered a newer epoch from a node outside its set with %v, want ErrMemberMismatch", err)
	}
	if !member.mismatch.Load() {
		t.Errorf("a confirmed member did not set mismatch on a newer epoch from a node outside its set")
	}
}

// TestANewcomerServesOnceTheJoinIsAdopted: adopting the join confirms the
// newcomer's view; the mark is cleared in memory and on disk, and requests are
// routed again.
func TestANewcomerServesOnceTheJoinIsAdopted(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	// A bucket a member still owns after the join: the read below is about
	// routing, not about the copy the joiner has yet to receive, nor about a
	// founder taking the bucket over from another.
	ports := reserveJoinerPlaced(t, func(joiner string) bool {
		_, found := keptByItsFounder(t, tc.addrs, joiner)
		return found
	})
	bucket, _ := keptByItsFounder(t, tc.addrs, ports.ringAddr)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}})
	newcomer := startJoinTarget(t, tc, ports, tc.token)
	joining := newcomer.server.core.cluster

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for joining.bootstrapped.Load() || joining.Epoch() != InitialEpoch+1 {
		if time.Now().After(deadline) {
			t.Fatalf("the newcomer did not adopt the join: epoch %d, bootstrapped %v", joining.Epoch(), joining.bootstrapped.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
	record, found, err := readMembershipRecord(joining.membershipPath())
	if err != nil || !found {
		t.Fatalf("reading the newcomer's membership record: found=%v err=%v", found, err)
	}
	if record.Bootstrapped {
		t.Errorf("the newcomer adopted the join but membership.json still says bootstrapped: a restart would refuse again")
	}
	frame := tc.frameOn(newcomer, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"})
	if frame.Opcode == transport.OpError {
		t.Fatalf("a read through the newcomer after its join was refused: %s", string(frame.Payload))
	}
}

// keptByItsFounder is TestANewcomerServesOnceTheJoinIsAdopted's placement: a
// bucket name of the form kept%03d that the same founder owns on the founders'
// ring and once joiner joins it. A name that moves between two founders is
// answered, while its new owner takes it over, with the designed retryable
// "ownership ... is moving to this node" refusal (BUG-0487), which is not the
// routing the test is about.
func keptByItsFounder(t *testing.T, founders []string, joiner string) (string, bool) {
	t.Helper()
	before := ringWith(t, 3, founders...)
	joined := ringWith(t, 3, append(append([]string(nil), founders...), joiner)...)
	return findBucketName("kept", func(name string) bool {
		owner := ownerOn(joined, name)
		return owner != joiner && owner == ownerOn(before, name)
	})
}

// TestTheNewcomerReadBucketKeepsItsOwner (BUG-0487): on a recorded draw the
// first name the joiner does not own moves from one founder to another, the
// placement whose read was refused "ownership ... is moving" in a gate run.
// The test's placement picks a name its founder keeps. Red where the
// placement is only "not owned by the joiner".
func TestTheNewcomerReadBucketKeepsItsOwner(t *testing.T) {
	founders := []string{"127.0.0.1:40472", "127.0.0.1:45080", "127.0.0.1:38968"}
	joiner := "127.0.0.1:35544"
	before := ringWith(t, 3, founders...)
	joined := ringWith(t, 3, append(append([]string(nil), founders...), joiner)...)
	notTheJoiners := bucketNamed(t, "kept", func(name string) bool { return ownerOn(joined, name) != joiner })
	if ownerOn(before, notTheJoiners) == ownerOn(joined, notTheJoiners) {
		t.Fatalf("the reproducer's draw is not what it needs: %s keeps its owner %s", notTheJoiners, ownerOn(joined, notTheJoiners))
	}
	name, found := keptByItsFounder(t, founders, joiner)
	if !found {
		t.Fatalf("no name of the form kept000 keeps its founder on the draw %v + %s", founders, joiner)
	}
	if was, now := ownerOn(before, name), ownerOn(joined, name); was != now || now == joiner {
		t.Errorf("the placement chose %s, owned by %s before the join and %s after: its read meets a take-over", name, was, now)
	}
}

// TestFoundersOfAStaticClusterConfirmEachOther: the nodes of a new cluster all
// start bootstrapped from one seed list, agree with each other, and confirm
// once every founder has answered and agreed (spec §16, F-A). Each node's mark
// is read directly.
func TestFoundersOfAStaticClusterConfirmEachOther(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	deadline := time.Now().Add(15 * time.Second)
	for index := range tc.nodes {
		founder := clusterOf(tc, index)
		for founder.bootstrapped.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("founder %d is still bootstrapped: every founder has answered and agrees with its view", index+1)
			}
			time.Sleep(20 * time.Millisecond)
		}
		record, found, err := readMembershipRecord(founder.membershipPath())
		if err != nil || !found || record.Bootstrapped {
			t.Errorf("founder %d is confirmed in memory, but its membership record says bootstrapped=%v (found=%v, err=%v)",
				index+1, record.Bootstrapped, found, err)
		}
	}
	tc.ok(1, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(2, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}})
}

// startNewcomerPair starts two nodes from one seed list that names tc's
// members and both of them, as an operator adding two nodes at once would
// write it. The two cannot reach each other yet: neither has pinned the
// other's certificate (trustEachOther).
func startNewcomerPair(t *testing.T, tc *testCluster) (first, second *testNode) {
	t.Helper()
	firstPorts, secondPorts := reserveNodePorts(t), reserveNodePorts(t)
	seeds := append(append([]string{}, tc.addrs...), firstPorts.ringAddr, secondPorts.ringAddr)
	withBothSeeds := func(cfg *config.Config) { cfg.HashRing.SeedNodes = append([]string(nil), seeds...) }
	return startJoinTarget(t, tc, firstPorts, tc.token, withBothSeeds), startJoinTarget(t, tc, secondPorts, tc.token, withBothSeeds)
}

// trustEachOther pins each node's certificate on the other, as
// trustJoinerFingerprint does for a cluster's members.
func trustEachOther(t *testing.T, first, second *testNode) {
	t.Helper()
	pinNodeKey(t, first.server.core.cluster, second.cfg.Storage.DataDir, second.addr)
	pinNodeKey(t, second.server.core.cluster, first.cfg.Storage.DataDir, first.addr)
}

// requireNewcomerPairHeld drives two newcomers in the order the C-2 case
// needs: each hears its members' disagreement first, then the two reach each
// other and agree; after several woken rounds neither is confirmed and both
// still refuse a create database. The order is forced so the members' dispute
// is heard first; TestANewcomerPairThatMeetsFirstJoiningASingleNodeStaysBootstrapped
// covers the order left to the dials.
func requireNewcomerPairHeld(t *testing.T, tc *testCluster, first, second *testNode) {
	t.Helper()
	newcomers := []*testNode{first, second}
	deadline := time.Now().Add(20 * time.Second)
	for _, newcomer := range newcomers {
		for !newcomer.server.core.cluster.viewDisputed.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("newcomer %s never heard its members disagree with its view", newcomer.addr)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	trustEachOther(t, first, second)
	for index, newcomer := range newcomers {
		other := newcomers[1-index].server.core.cluster.self.ID
		waiting := newcomer.server.core.cluster
		// A node that confirmed has forgotten who agreed: that is the failure
		// asserted below, not a pair that never met.
		for !agreedWith(waiting, other) && waiting.bootstrapped.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("newcomer %s never agreed with %s; the test needs the pair to reach each other", newcomer.addr, other)
			}
			waiting.wakeGossip()
			time.Sleep(50 * time.Millisecond)
		}
	}

	for round := 0; round < 3; round++ {
		for _, newcomer := range newcomers {
			newcomer.server.core.cluster.wakeGossip()
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, newcomer := range newcomers {
		if !newcomer.server.core.cluster.bootstrapped.Load() {
			t.Fatalf("newcomer %s confirmed its view with a member disputing it; only 'join' may add it", newcomer.addr)
		}
		requireNotYetAdded(t, tc.frameOn(newcomer, transport.OpCreateDB, CreateDatabaseRequest{Name: newcomerDatabase}),
			"create database through "+newcomer.addr, true)
	}
}

// agreedWith reports whether peerID has agreed with c's view since boot.
func agreedWith(c *Cluster, peerID string) bool {
	c.viewConfirmMu.Lock()
	defer c.viewConfirmMu.Unlock()
	_, agreed := c.viewAgreed[peerID]
	return agreed
}

// TestTwoNewcomersFromOneSeedListStayBootstrapped is review C-2: two nodes
// started together from one seed list naming three members and both of them
// have identical views and agree with each other. They are 2 of 5, and every
// member disputes them: neither may confirm itself.
func TestTwoNewcomersFromOneSeedListStayBootstrapped(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	first, second := startNewcomerPair(t, tc)
	requireNewcomerPairHeld(t, tc, first, second)

	// Then 'join' each, one after the other: each is confirmed by adopting the
	// membership that lists it. Between the two joins the second is shown an
	// epoch that does not list it, which it must not take as its removal
	// (so this half lands with build step 4).
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	via := clusterOf(tc, 0)
	for _, newcomer := range []*testNode{first, second} {
		joining := newcomer.server.core.cluster
		if err := via.proposeJoin(ctx, joining.self); err != nil {
			t.Fatalf("proposeJoin %s: %v", newcomer.addr, err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for joining.bootstrapped.Load() || joining.Epoch() != via.Epoch() {
			if time.Now().After(deadline) {
				t.Fatalf("newcomer %s did not adopt its join: epoch %d (cluster %d), bootstrapped %v, removed %v",
					newcomer.addr, joining.Epoch(), via.Epoch(), joining.bootstrapped.Load(), joining.removed())
			}
			joining.wakeGossip()
			time.Sleep(50 * time.Millisecond)
		}
	}
	for _, newcomer := range []*testNode{first, second} {
		if joined := newcomer.server.core.cluster; joined.removed() || joined.Epoch() != InitialEpoch+2 {
			t.Errorf("newcomer %s after both joins: removed %v, epoch %d, want a member at %d",
				newcomer.addr, joined.removed(), joined.Epoch(), InitialEpoch+2)
		}
	}
}

// TestANewcomerPairJoiningASingleNodeIsHeldByTheDisagreement is review C-2's
// second case: joining a one-node cluster the pair is 2 of 3, a majority, but
// the one member disputes them, and that holds both.
func TestANewcomerPairJoiningASingleNodeIsHeldByTheDisagreement(t *testing.T) {
	tc := newTestCluster(t, 1, 3)
	defer tc.stop()
	first, second := startNewcomerPair(t, tc)
	requireNewcomerPairHeld(t, tc, first, second)
}

// TestANewcomerPairThatMeetsFirstJoiningASingleNodeStaysBootstrapped is the
// same case left to the dials: the second newcomer can reach the first from
// its start. Under the first rule, majority and no dissent *so far*, it
// confirmed whenever the first's agreement arrived before the member's dispute
// (8 runs in 10). Under the coordinator's ruling (spec §16, F-A) a
// bootstrapped peer's agreement does not count, whatever the order.
func TestANewcomerPairThatMeetsFirstJoiningASingleNodeStaysBootstrapped(t *testing.T) {
	tc := newTestCluster(t, 1, 3)
	defer tc.stop()
	firstPorts, secondPorts := reserveNodePorts(t), reserveNodePorts(t)
	seeds := append(append([]string{}, tc.addrs...), firstPorts.ringAddr, secondPorts.ringAddr)
	withBothSeeds := func(cfg *config.Config) { cfg.HashRing.SeedNodes = append([]string(nil), seeds...) }
	first := startJoinTarget(t, tc, firstPorts, tc.token, withBothSeeds)
	var secondDir string
	second := startJoinTargetPrepared(t, tc, secondPorts, tc.token, func(srv *Service) {
		secondDir = srv.core.config.Storage.DataDir
		pinNodeKey(t, srv.core.cluster, first.cfg.Storage.DataDir, first.addr)
		pinNodeKey(t, first.server.core.cluster, secondDir, secondPorts.ringAddr)
	}, withBothSeeds)
	time.Sleep(3 * time.Second)
	for _, newcomer := range []*testNode{first, second} {
		if !newcomer.server.core.cluster.bootstrapped.Load() {
			t.Errorf("newcomer %s confirmed its view while the one member disputes it", newcomer.addr)
		}
	}
}

// TestAStaticClusterConfirmsOnlyOnceEveryFounderHasAnswered: the founders of
// a new cluster are all bootstrapped, so none can confirm another by its
// agreement; they confirm when every member of the view has answered and all
// agree (spec §16, F-A). Two of three agreeing is not enough while the third
// is silent; once it answers, all three confirm.
func TestAStaticClusterConfirmsOnlyOnceEveryFounderHasAnswered(t *testing.T) {
	const silent = 2
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	held := func(inner peerConn) peerConn { return &heldAllConn{peerConn: inner, release: release} }
	tc := startTestClusterTuned(t, 3, 3, nil, func(index int, c *Cluster) {
		// SeedNodes is the fixture's address list, and an address is a
		// node's identity in this fixture.
		silentID := c.s.config.HashRing.SeedNodes[silent]
		for _, seed := range c.s.config.HashRing.SeedNodes {
			if seed != c.self.ID && (index == silent || seed == silentID) {
				wrapPeerConn(c.peerFor(ring.Node{ID: seed, Addr: seed}), held)
			}
		}
	})
	defer tc.stop()

	first, second := clusterOf(tc, 0), clusterOf(tc, 1)
	deadline := time.Now().Add(20 * time.Second)
	// Met, or already confirmed (which forgets who agreed): the failure below.
	met := func(founder, other *Cluster) bool {
		return agreedWith(founder, other.self.ID) || !founder.bootstrapped.Load()
	}
	for !met(first, second) || !met(second, first) {
		if time.Now().After(deadline) {
			t.Fatalf("test setup: founders 1 and 2 never exchanged gossip")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for round := 0; round < 3; round++ {
		first.wakeGossip()
		second.wakeGossip()
		time.Sleep(200 * time.Millisecond)
	}
	for index := range tc.nodes {
		if !clusterOf(tc, index).bootstrapped.Load() {
			t.Fatalf("founder %d confirmed while founder %d has not answered: 2 bootstrapped founders of 3 agreeing is not a confirmation",
				index+1, silent+1)
		}
	}

	close(release)
	released = true
	deadline = time.Now().Add(20 * time.Second)
	for index := range tc.nodes {
		founder := clusterOf(tc, index)
		for founder.bootstrapped.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("founder %d is still bootstrapped after every founder answered and agreed", index+1)
			}
			founder.wakeGossip()
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// pinNodeKey pins, on c, the certificate kept in dir for the node at addr.
func pinNodeKey(t *testing.T, c *Cluster, dir, addr string) {
	t.Helper()
	paths, _, err := transport.LoadOrCreateKeys(filepath.Join(dir, "keys"))
	if err != nil {
		t.Fatalf("load %s's keys: %v", addr, err)
	}
	certificate, err := tls.LoadX509KeyPair(paths.Cert, paths.Key)
	if err != nil {
		t.Fatalf("load %s's certificate: %v", addr, err)
	}
	publicKey, err := transport.ParseEd25519PublicKey(certificate)
	if err != nil {
		t.Fatalf("parse %s's public key: %v", addr, err)
	}
	internodeAddr, err := transport.InternodePeerAddr(addr)
	if err != nil {
		t.Fatalf("derive %s's internode address: %v", addr, err)
	}
	if err := c.knownServers.Trust(internodeAddr, publicKey); err != nil {
		t.Fatalf("pin %s: %v", addr, err)
	}
}

// TestANewcomerWaitingForJoinDoesNotTakeItsMembersForDown is build report
// F-E: a newcomer whose members answer with the member set it is not yet in
// waits for 'join'. That answer is not a failure of the exchange, so it must
// not record the members unreachable — which, after a while, logged "peer has
// been unreachable … run 'remove-node'" about healthy members.
func TestANewcomerWaitingForJoinDoesNotTakeItsMembersForDown(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	newcomer := startJoinTarget(t, tc, reserveNodePorts(t), tc.token)
	waiting := newcomer.server.core.cluster
	deadline := time.Now().Add(20 * time.Second)
	for !waiting.viewDisputed.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("test setup: the newcomer never heard a member answer")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for round := 0; round < 3; round++ {
		waiting.wakeGossip()
		time.Sleep(200 * time.Millisecond)
	}
	for index := range tc.nodes {
		member := clusterOf(tc, index).self
		if since, down := waiting.peerFor(member).downSince(); down {
			t.Errorf("the newcomer recorded member %s unreachable since %s; it answered, with a membership the newcomer is not in yet",
				member.ID, since.Format(time.RFC3339))
		}
	}
	if waiting.mismatch.Load() {
		t.Errorf("the newcomer is in membership mismatch while it waits for 'join'")
	}
}

// removeLastNode stops the last node of a cluster and removes it through node
// 0, then waits until every survivor lists the removal in its history. It
// returns the removed node's ID.
func removeLastNode(t *testing.T, tc *testCluster) string {
	t.Helper()
	last := len(tc.nodes) - 1
	removedID := tc.addrs[last]
	tc.stopNodeKeepingData(last)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clusterOf(tc, 0).proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("removing %s: %v", removedID, err)
	}
	if !eventually(func() bool {
		for index := range last {
			survivor := clusterOf(tc, index)
			if nodeListed(survivor.Members(), removedID) ||
				!slices.ContainsFunc(survivor.removedInHistory(), func(n ring.Node) bool { return n.ID == removedID }) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("the survivors never adopted the removal of %s", removedID)
	}
	return removedID
}

// laterViewFrom builds the member list a removed node that kept following
// epochs would send: the survivors plus a joiner, without the removed node.
func laterViewFrom(tc *testCluster, survivors int) ([]Peer, []string) {
	view := make([]Peer, 0, survivors+1)
	ids := make([]string, 0, survivors+1)
	for index := range survivors {
		self := clusterOf(tc, index).self
		view = append(view, Peer{ID: self.ID, Addr: self.Addr})
		ids = append(ids, self.ID)
	}
	view = append(view, Peer{ID: "a-joiner", Addr: "a-joiner"})
	ids = append(ids, "a-joiner")
	return view, ids
}

// TestAConfirmedMemberIgnoresANewerEpochFromANodeItRemoved is BUG-0707. A
// confirmed member one epoch behind hears from a node it has itself seen
// removed, which keeps following later epochs. That node holds no placement and
// the member knows why it is outside the set, so the view is noted and the
// exchange answered; it used to set mismatch. A stranger in neither the set nor
// the history still sets the flag.
func TestAConfirmedMemberIgnoresANewerEpochFromANodeItRemoved(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	setGossipSkip(t, func(string, string, string) bool { return true })
	removedID := removeLastNode(t, tc)
	member := clusterOf(tc, 0)
	if member.bootstrapped.Load() {
		t.Fatalf("the survivor is bootstrapped; the test needs a confirmed member")
	}
	view, ids := laterViewFrom(tc, 3)

	epochBefore := member.Epoch()
	if err := member.reconcileMembership(removedID, view, ids, epochBefore+1); err != nil {
		t.Errorf("a confirmed member answered a newer epoch from a node it removed with %v; the node holds no placement", err)
	}
	if member.mismatch.Load() {
		t.Errorf("a confirmed member set mismatch on a newer epoch from a node it removed; it refuses direct writes while set")
	}
	if got := member.Epoch(); got != epochBefore {
		t.Errorf("the member moved from epoch %d to %d on a removed node's view; it adopts nothing from one", epochBefore, got)
	}

	defer member.mismatch.Store(false)
	if err := member.reconcileMembership("stranger", view, ids, member.Epoch()+1); !errors.Is(err, ErrMemberMismatch) {
		t.Errorf("a confirmed member answered a newer epoch from a stranger with %v, want ErrMemberMismatch", err)
	}
	if !member.mismatch.Load() {
		t.Errorf("a confirmed member did not set mismatch on a newer epoch from a stranger")
	}
}

// TestADirectWriteToALaggingMemberIsNotRefusedForARemovedNodesGossip is the
// end-to-end form of BUG-0707: after the removed node's newer view reaches a
// lagging member, a write sent straight to it is stored and not refused
// CONFLICT, which a driver never retries. Gossip reconciliation is skipped so
// that agreeing gossip cannot clear the flag under the test.
func TestADirectWriteToALaggingMemberIsNotRefusedForARemovedNodesGossip(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	setGossipSkip(t, func(string, string, string) bool { return true })
	removedID := removeLastNode(t, tc)
	member := clusterOf(tc, 0)
	view, ids := laterViewFrom(tc, 3)
	defer member.mismatch.Store(false)
	// The error is the other test's subject; here only what the client sees.
	if err := member.reconcileMembership(removedID, view, ids, member.Epoch()+1); err != nil {
		t.Logf("the reconcile answered %v", err)
	}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}})
}

// TestARemovedNodeAddedBackIsNoLongerInTheRemovedHistory pins what makes the
// BUG-0707 guard safe for a node removed and then added again under the same
// id: removedInHistory lists only nodes that are not members now, so once a
// member has adopted the add the node is a member again, the guard never
// applies to its gossip, and a view from it at a newer epoch is adopted by the
// ordinary resync. The guard ignores a removed node only while the member has
// not yet seen the add, when the member adopts nothing from a non-member
// either way.
func TestARemovedNodeAddedBackIsNoLongerInTheRemovedHistory(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const back = 2
	removedID := tc.addrs[back]
	base := tc.stopNodeKeepingData(back)
	survivor := clusterOf(tc, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("removing %s: %v", removedID, err)
	}
	tc.bringMembershipTo(t, survivor)
	if !slices.ContainsFunc(survivor.removedInHistory(), func(n ring.Node) bool { return n.ID == removedID }) {
		t.Fatalf("setup: %s is not in the removed history after its removal", removedID)
	}
	if err := survivor.proposeJoin(ctx, ring.Node{ID: removedID, Addr: removedID}); err != nil {
		t.Fatalf("adding %s back: %v", removedID, err)
	}
	tc.bringMembershipTo(t, survivor)
	tc.nodes[back] = tc.startNode(back, base, tc.token)
	tc.bringMembershipTo(t, survivor)

	if !nodeListed(survivor.Members(), removedID) {
		t.Fatalf("setup: %s is not a member after it was added back", removedID)
	}
	if slices.ContainsFunc(survivor.removedInHistory(), func(n ring.Node) bool { return n.ID == removedID }) {
		t.Errorf("%s is a member again but still in the removed history; the guard would ignore its gossip", removedID)
	}
}
