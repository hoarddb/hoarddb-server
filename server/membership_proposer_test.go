package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The proposer and the acceptor wired across real nodes: each test drives one
// rule of the spec's sections 6.2.1 and 6.2.2 through the seams.

// setPhaseHook installs membershipPhaseHook for the test.
func setPhaseHook(t *testing.T, hook func(nodeID, phase, fromNode string, epoch uint64) error) {
	t.Helper()
	membershipPhaseHook.Store(&hook)
	t.Cleanup(func() { membershipPhaseHook.Store(nil) })
}

// setSendHook installs membershipSendHook for the test.
func setSendHook(t *testing.T, hook func(ctx context.Context, fromNode, toNode, phase string) error) {
	t.Helper()
	membershipSendHook.Store(&hook)
	t.Cleanup(func() { membershipSendHook.Store(nil) })
}

// setGossipSkip installs gossipReconcileHook for the test.
func setGossipSkip(t *testing.T, skip func(nodeID, peerID, side string) bool) {
	t.Helper()
	gossipReconcileHook.Store(&skip)
	t.Cleanup(func() { gossipReconcileHook.Store(nil) })
}

// adoptionLog records, through viewAdoptedHook, the member set each node
// adopts at each epoch.
type adoptionLog struct {
	mu       sync.Mutex
	clusters map[string]*Cluster
	adopted  map[uint64]map[string][]string
}

// recordAdoptions starts an adoption log over every node of tc.
func recordAdoptions(t *testing.T, tc *testCluster) *adoptionLog {
	t.Helper()
	log := &adoptionLog{clusters: make(map[string]*Cluster), adopted: make(map[uint64]map[string][]string)}
	for index := range tc.nodes {
		log.clusters[tc.addrs[index]] = clusterOf(tc, index)
	}
	record := func(nodeID string, epoch uint64) {
		_, members := log.clusters[nodeID].epochMembers()
		ids := memberIDsOf(members)
		slices.Sort(ids)
		log.mu.Lock()
		defer log.mu.Unlock()
		if log.adopted[epoch] == nil {
			log.adopted[epoch] = make(map[string][]string)
		}
		log.adopted[epoch][nodeID] = ids
	}
	viewAdoptedHook.Store(&record)
	t.Cleanup(func() { viewAdoptedHook.Store(nil) })
	return log
}

// assertOneSet fails the test unless every node that adopted epoch adopted
// want there.
func (log *adoptionLog) assertOneSet(t *testing.T, epoch uint64, want []string) {
	t.Helper()
	want = slices.Sorted(slices.Values(want))
	log.mu.Lock()
	defer log.mu.Unlock()
	for nodeID, ids := range log.adopted[epoch] {
		if !slices.Equal(ids, want) {
			t.Errorf("%s adopted membership epoch %d with %v; the set chosen there is %v", nodeID, epoch, ids, want)
		}
	}
}

// indexOf is the index in tc of the node with id.
func indexOf(t *testing.T, tc *testCluster, id string) int {
	t.Helper()
	index := slices.Index(tc.addrs, id)
	if index < 0 {
		t.Fatalf("no node %s in the cluster", id)
	}
	return index
}

// commandContext is a context for one operator command.
func commandContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// acceptRefusalDelay holds a seam's refusal of an accept back, so the one
// member that accepts answers first: a phase decided lost by its refusals
// cancels the accepts still in flight, which would otherwise leave no
// acceptance where the test needs one.
const acceptRefusalDelay = 300 * time.Millisecond

// requireAcceptance fails the test setup unless member holds an acceptance.
func requireAcceptance(t *testing.T, member *Cluster) {
	t.Helper()
	if pending := pendingOf(member); pending == nil || pending.Accepted == nil {
		t.Fatalf("test setup: %s holds no acceptance: %+v", member.self.ID, pending)
	}
}

// TestGossipNeverSpreadsAnAcceptedSet is T4: node 4 accepts a set for the next
// epoch, and gossips with node 1 both ways; both stay on the first epoch, and
// node 4 still holds its acceptance. Guard: gossip carries only the adopted
// view, never the pending change.
func TestGossipNeverSpreadsAnAcceptedSet(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	first, stray := clusterOf(tc, 0), clusterOf(tc, 3)
	proposed := slices.DeleteFunc(stray.Members(), func(member ring.Node) bool { return member.ID == tc.addrs[4] })
	if _, err := stray.acceptMembershipChange(acceptRequestOf(tc.addrs[2], InitialEpoch+1, proposed,
		Peer{ID: tc.addrs[4], Addr: tc.addrs[4]})); err != nil {
		t.Fatalf("node 4 accepting node 3's removal of node 5: %v", err)
	}
	ctx := commandContext(t)
	if err := first.peerFor(stray.self).gossip(ctx); err != nil {
		t.Fatalf("node 1 gossiping with node 4: %v", err)
	}
	if err := stray.peerFor(first.self).gossip(ctx); err != nil {
		t.Fatalf("node 4 gossiping with node 1: %v", err)
	}
	if epoch := first.Epoch(); epoch != InitialEpoch {
		t.Errorf("node 1 is at membership epoch %d after gossip with node 4, which only accepted a set for epoch %d", epoch, InitialEpoch+1)
	}
	if epoch := stray.Epoch(); epoch != InitialEpoch {
		t.Errorf("node 4 moved to membership epoch %d on an acceptance", epoch)
	}
	if pendingOf(stray) == nil {
		t.Errorf("node 4 lost its acceptance in the gossip")
	}
}

// TestTwoRacingProposalsCommitOneSet is T2: seven nodes; P1, the lowest id,
// removes one node and P2 another. P1's set is accepted by a majority and P1
// adopts it, and every commit P1 sends is held. P2 then prepares on a majority
// without P1, which holds an acceptance of P1's set, so P2 finishes P1's change
// and says so. Every node ends on one set at epoch 2, with no pending change,
// the removed node included. Guard: the value rule.
func TestTwoRacingProposalsCommitOneSet(t *testing.T) {
	tc := newTestCluster(t, 7, 3)
	defer tc.stop()
	sorted := slices.Sorted(slices.Values(tc.addrs))
	p1ID, p2ID, firstRemoved, secondRemoved := sorted[0], sorted[1], sorted[2], sorted[3]
	p1, p2 := clusterOf(tc, indexOf(t, tc, p1ID)), clusterOf(tc, indexOf(t, tc, p2ID))
	adoptions := recordAdoptions(t, tc)

	commitsHeld := make(chan struct{})
	releaseCommits := make(chan struct{})
	var heldOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseCommits) })
	setSendHook(t, func(ctx context.Context, fromNode, toNode, phase string) error {
		switch {
		case fromNode == p1ID && phase == phaseCommit:
			heldOnce.Do(func() { close(commitsHeld) })
			select {
			case <-releaseCommits:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case fromNode == p2ID && toNode == p1ID && phase == phasePrepare:
			return errors.New("test seam: P2's majority excludes P1")
		}
		return nil
	})

	p1Done := make(chan error, 1)
	go func() { p1Done <- p1.proposeRemoval(commandContext(t), firstRemoved) }()
	select {
	case <-commitsHeld:
	case err := <-p1Done:
		t.Fatalf("test setup: P1's removal ended before its commits: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: P1's removal was never chosen")
	}

	err := p2.proposeRemoval(commandContext(t), secondRemoved)
	releaseOnce.Do(func() { close(releaseCommits) })
	if !errors.Is(err, ErrAnotherMembershipChangeFinished) || !strings.Contains(err.Error(), "removing "+firstRemoved) ||
		!strings.Contains(err.Error(), "removing "+secondRemoved) {
		t.Errorf("P2 answered %v; it must finish P1's change, removing %s, and say its own, removing %s, was not made",
			err, firstRemoved, secondRemoved)
	}
	if err := <-p1Done; err != nil {
		t.Errorf("P1's removal of %s answered %v; its set was chosen", firstRemoved, err)
	}

	chosen := addrsWithout(tc.addrs, firstRemoved)
	if !holdsWithin(30*time.Second, func() bool {
		for index := range tc.nodes {
			if clusterOf(tc, index).Epoch() < InitialEpoch+1 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("not every node reached membership epoch 2")
	}
	adoptions.assertOneSet(t, InitialEpoch+1, chosen)
	for index := range tc.nodes {
		if pending := pendingOf(clusterOf(tc, index)); pending != nil {
			t.Errorf("node %s still holds a pending change once every node is at epoch 2: %+v", tc.addrs[index], pending)
		}
	}
}

// TestAProposerFinishesALostProposersChange is T1's opposite case: node 3's
// removal of node 5 is accepted by node 4 only; node 1 then removes node 2 on a
// majority that includes node 4, so it finishes node 3's change and says so.
// Guard: the value rule.
func TestAProposerFinishesALostProposersChange(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n1, n2, n3, n4, n5 := tc.addrs[0], tc.addrs[1], tc.addrs[2], tc.addrs[3], tc.addrs[4]
	adoptions := recordAdoptions(t, tc)
	// Node 3's change is promised by nodes 1 and 4, so node 1's own record
	// holds node 3's round and its ballot is above it; at accept only node 4
	// takes it.
	setPhaseHook(t, func(nodeID, phase, fromNode string, _ uint64) error {
		if fromNode != n3 {
			return nil
		}
		refusedPrepare := phase == phasePrepare && (nodeID == n2 || nodeID == n5)
		refusedAccept := phase == phaseAccept && nodeID != n4
		if refusedAccept {
			time.Sleep(acceptRefusalDelay)
		}
		if refusedPrepare || refusedAccept {
			return fmt.Errorf("test seam: %s refuses %s's %s", nodeID, fromNode, phase)
		}
		return nil
	})
	if err := clusterOf(tc, 2).proposeRemoval(commandContext(t), n5); !errors.Is(err, ErrMembershipChangeIndeterminate) {
		t.Fatalf("test setup: node 3's removal of node 5 answered %v, want %v", err, ErrMembershipChangeIndeterminate)
	}
	requireAcceptance(t, clusterOf(tc, 3))
	setSendHook(t, func(_ context.Context, fromNode, toNode, phase string) error {
		if fromNode == n1 && phase == phasePrepare && (toNode == n2 || toNode == n3) {
			return errors.New("test seam: node 1's majority is nodes 1, 4 and 5")
		}
		return nil
	})
	err := clusterOf(tc, 0).proposeRemoval(commandContext(t), n2)
	if !errors.Is(err, ErrAnotherMembershipChangeFinished) || !strings.Contains(err.Error(), "removing "+n5) ||
		!strings.Contains(err.Error(), "removing "+n2) {
		t.Errorf("node 1 answered %v; it must finish node 3's removal of node 5 and say its own was not made", err)
	}
	adoptions.assertOneSet(t, InitialEpoch+1, addrsWithout(tc.addrs, n5))
	if epoch := clusterOf(tc, 0).Epoch(); epoch != InitialEpoch+1 {
		t.Errorf("node 1 is at membership epoch %d, want %d", epoch, InitialEpoch+1)
	}
}

// phaseSample is the proposer's membership.json as it stood when the first
// message of one phase left it, or why it could not be read.
type phaseSample struct {
	mu     sync.Mutex
	taken  bool
	record *pendingChange
	err    error
}

// proposalKey marks the context of the proposal a sample belongs to.
type proposalKey struct{}

// sampleProposerRecord installs a send hook that reads proposer's
// membership.json when the first message of phase leaves it, and returns the
// context to propose with. The hook is process-global and node IDs are
// ephemeral ports other tests reuse, so a call counts only when its context
// descends from the returned one: a late goroutine of an earlier test does not.
func sampleProposerRecord(t *testing.T, proposer *Cluster, phase string) (context.Context, *phaseSample) {
	t.Helper()
	sample := &phaseSample{}
	proposalCtx := context.WithValue(commandContext(t), proposalKey{}, sample)
	setSendHook(t, func(ctx context.Context, fromNode, _, sentPhase string) error {
		if ctx.Value(proposalKey{}) != sample || fromNode != proposer.self.ID || sentPhase != phase {
			return nil
		}
		sample.mu.Lock()
		defer sample.mu.Unlock()
		if sample.taken {
			return nil
		}
		sample.taken = true
		sample.record, sample.err = recordedPromise(proposer)
		return nil
	})
	return proposalCtx, sample
}

// recordedPromise reads the pending change a node's membership.json holds. A
// file that cannot be read is an error, not an empty record.
func recordedPromise(c *Cluster) (*pendingChange, error) {
	record, found, err := readMembershipRecord(c.membershipPath())
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", c.membershipPath(), err)
	}
	if !found {
		return nil, fmt.Errorf("%s does not exist", c.membershipPath())
	}
	return record.Pending, nil
}

// first is the record sampled, or nil when no message of the phase left. A
// read error fails the test: it says nothing about what the file held.
func (sample *phaseSample) first(t *testing.T) *pendingChange {
	t.Helper()
	sample.mu.Lock()
	defer sample.mu.Unlock()
	if sample.err != nil {
		t.Fatalf("reading membership.json when the message left: %v", sample.err)
	}
	return sample.record
}

// TestAProposerSelfPromiseIsDurable: the proposer's promise to itself is on
// disk before its first prepare leaves. Guard: the save in
// acceptorStepLocked, which the self-promise goes through.
func TestAProposerSelfPromiseIsDurable(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	proposalCtx, sample := sampleProposerRecord(t, proposer, phasePrepare)
	if err := proposer.proposeJoin(proposalCtx, ringNodeAt(fabricatedMemberAddr(t))); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	record := sample.first(t)
	if record == nil || record.Promised.Node != proposer.self.ID || record.Epoch != InitialEpoch+1 {
		t.Errorf("when the first prepare left, membership.json held %+v; it must hold the proposer's promise to itself", record)
	}
}

// TestAProposerSelfAcceptanceIsDurable: the proposer's acceptance of its own
// offer is on disk before its first accept leaves. Guard: as the test above.
func TestAProposerSelfAcceptanceIsDurable(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	proposalCtx, sample := sampleProposerRecord(t, proposer, phaseAccept)
	if err := proposer.proposeJoin(proposalCtx, ringNodeAt(fabricatedMemberAddr(t))); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	record := sample.first(t)
	if record == nil || record.Accepted == nil || record.Accepted.Node != proposer.self.ID {
		t.Errorf("when the first accept left, membership.json held %+v; it must hold the proposer's own acceptance", record)
	}
}

// TestADurabilitySampleReportsAReadError (BUG-0501): a membership.json that
// cannot be read is an error, never an empty record.
func TestADurabilitySampleReportsAReadError(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	if err := os.WriteFile(proposer.membershipPath(), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("corrupting membership.json: %v", err)
	}
	if record, err := recordedPromise(proposer); err == nil {
		t.Errorf("recordedPromise of a corrupt file returned %+v and no error", record)
	}
}

// TestAProposerThatCannotSaveItsPromiseSendsNoPrepare: the self-promise is
// saved before any prepare leaves, so a failed save ends the command with
// nothing sent. Guard: the refusal acceptorStepLocked returns on a failed
// write, which preparePhase reads as selfRefused.
func TestAProposerThatCannotSaveItsPromiseSendsNoPrepare(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	var sent atomic.Int32
	setSendHook(t, func(_ context.Context, fromNode, _, _ string) error {
		if fromNode == proposer.self.ID {
			sent.Add(1)
		}
		return nil
	})
	failSavesOf(t, proposer)
	err := proposer.proposeJoin(commandContext(t), ringNodeAt(fabricatedMemberAddr(t)))
	if err == nil {
		t.Fatal("proposeJoin succeeded although the proposer could not save its promise")
	}
	// A local write failure is not a disagreement with a peer; the operator
	// is told what failed, the cause and what to check.
	if errors.Is(err, ErrMemberMismatch) {
		t.Errorf("a failed local save is reported as a disagreement with a peer: %v", err)
	}
	for _, want := range []string{"could not save its own promise", proposer.membershipPath(), "injected fsync failure", "free space"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not mention %q", err, want)
		}
	}
	if got := sent.Load(); got != 0 {
		t.Errorf("the proposer sent %d messages after its self-promise failed to save, want none", got)
	}
}

// failSavesOf makes the fsync of one node's membership.json fail, and no other
// node's: the seam is process-wide.
func failSavesOf(t *testing.T, node *Cluster) {
	t.Helper()
	original := syncMembershipFile
	savePath := node.membershipPath()
	syncMembershipFile = func(file *os.File) error {
		if strings.HasPrefix(file.Name(), savePath) {
			return errors.New("injected fsync failure")
		}
		return original(file)
	}
	t.Cleanup(func() { syncMembershipFile = original })
}

// TestADurabilitySampleIgnoresAStrayMessage (BUG-0501): a send-hook call that
// carries the proposer's ID but is not from the proposal under test, such as a
// late goroutine of an earlier test, must not be sampled.
func TestADurabilitySampleIgnoresAStrayMessage(t *testing.T) {
	for _, phase := range []string{phasePrepare, phaseAccept} {
		t.Run(phase, func(t *testing.T) {
			tc := newTestCluster(t, 3, 3)
			defer tc.stop()
			proposer := clusterOf(tc, 0)
			proposalCtx, sample := sampleProposerRecord(t, proposer, phase)
			stray := *membershipSendHook.Load()
			if err := stray(context.Background(), proposer.self.ID, "127.0.0.1:1", phase); err != nil {
				t.Fatalf("stray hook call: %v", err)
			}
			if err := proposer.proposeJoin(proposalCtx, ringNodeAt(fabricatedMemberAddr(t))); err != nil {
				t.Fatalf("proposeJoin: %v", err)
			}
			record := sample.first(t)
			if record == nil || record.Epoch != InitialEpoch+1 {
				t.Errorf("sampled %+v at the first %s; the stray call must not count, so the sample is the proposal's own record", record, phase)
			}
		})
	}
}

// TestAnAdoptedAcceptorRefusesAStaleAccept: a member that adopted epoch 2 is
// asked to accept another set for epoch 2; it answers ahead and records
// nothing. Guard: rule E on accept.
func TestAnAdoptedAcceptorRefusesAStaleAccept(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	founders := clusterOf(tc, 0).Members()
	if err := clusterOf(tc, 0).proposeJoin(commandContext(t), ringNodeAt(fabricatedMemberAddr(t))); err != nil {
		t.Fatalf("test setup: the first join: %v", err)
	}
	member := clusterOf(tc, 1)
	if member.Epoch() != InitialEpoch+1 {
		t.Fatalf("test setup: node 2 is at epoch %d, want %d", member.Epoch(), InitialEpoch+1)
	}
	other := ringNodeAt(fabricatedMemberAddr(t))
	resp, err := member.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, append(founders, other),
		Peer{ID: other.ID, Addr: other.Addr}))
	if err == nil || resp.Answer != string(answerAhead) {
		t.Errorf("a member that adopted epoch %d answered an accept of another set for it with %q (%v); want ahead",
			InitialEpoch+1, resp.Answer, err)
	}
	if pending := pendingOf(member); pending != nil {
		t.Errorf("the stale accept left a pending change: %+v", pending)
	}
}

// TestAnAcceptorRefusesADifferentBase: a prepare made against a committed set
// other than the member's is refused, and not as a promise it could retry
// past. Guard: rule P's base check.
func TestAnAcceptorRefusesADifferentBase(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	member := clusterOf(tc, 1)
	resp, err := member.prepareMembershipChange(MembershipChangeRequest{FromNode: tc.addrs[0], Phase: phasePrepare,
		Epoch: InitialEpoch + 1, Ballot: ballot{Round: 1, Node: tc.addrs[0]},
		BaseMembers: []string{tc.addrs[0], tc.addrs[1], fabricatedMemberAddr(t)}})
	if err == nil || resp.Answer != string(answerRefused) {
		t.Errorf("a prepare against another base set was answered %q (%v); want refused", resp.Answer, err)
	}
	if pending := pendingOf(member); pending != nil {
		t.Errorf("the refused prepare left a pending change: %+v", pending)
	}
}

// TestAnAcceptDoesNotMoveTheView: a member that accepts a set records it and
// acts on nothing: its epoch and members are what they were. Guard: the
// accept handler does not adopt.
func TestAnAcceptDoesNotMoveTheView(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	member := clusterOf(tc, 1)
	epochBefore, membersBefore := member.epochMembers()
	proposed := slices.DeleteFunc(slices.Clone(membersBefore), func(m ring.Node) bool { return m.ID == tc.addrs[2] })
	if _, err := member.acceptMembershipChange(acceptRequestOf(tc.addrs[0], epochBefore+1, proposed,
		Peer{ID: tc.addrs[2], Addr: tc.addrs[2]})); err != nil {
		t.Fatalf("accepting node 1's removal of node 3: %v", err)
	}
	epochAfter, membersAfter := member.epochMembers()
	if epochAfter != epochBefore || !sameMemberIDs(memberIDsOf(membersAfter), memberIDsOf(membersBefore)) {
		t.Errorf("an accept moved node 2 from epoch %d %v to epoch %d %v", epochBefore, memberIDsOf(membersBefore),
			epochAfter, memberIDsOf(membersAfter))
	}
	if pending := pendingOf(member); pending == nil || pending.Accepted == nil {
		t.Errorf("node 2 holds no acceptance after accepting: %+v", pending)
	}
}

// blackhole makes every message to nodeID hang as a dial to a dead host does,
// for one dial timeout, and then count as unreachable.
func blackhole(nodeID string) func(ctx context.Context, fromNode, toNode, phase string) error {
	return func(ctx context.Context, _, toNode, _ string) error {
		if toNode != nodeID {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(peerDialTimeout):
		}
		return fmt.Errorf("test seam: %s does not answer", nodeID)
	}
}

// TestARemovalOfADeadNodeReturnsAtAMajority: removing a dead node from five
// returns in about one dial timeout, not three: prepare and accept return at
// a majority, and only the commit waits for the dead node, within one dial.
// Guard: majority-return in runPhase.
func TestARemovalOfADeadNodeReturnsAtAMajority(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	setSendHook(t, blackhole(tc.addrs[4]))
	started := time.Now()
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4]); err != nil {
		t.Fatalf("removing the dead node 5: %v", err)
	}
	if took := time.Since(started); took >= 5*time.Second {
		t.Errorf("removing a dead node took %s; prepare and accept must return at a majority (want under 5s)", took.Round(time.Millisecond))
	}
}

// TestAPhaseSurvivesAMinorityRefusal: one member refuses the prepare before
// any other answers; the change commits on the others. Guard: a phase fails
// only when a majority can no longer be reached.
func TestAPhaseSurvivesAMinorityRefusal(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n1, n2 := tc.addrs[0], tc.addrs[1]
	setPhaseHook(t, func(nodeID, phase, fromNode string, _ uint64) error {
		if nodeID == n2 && fromNode == n1 && phase == phasePrepare {
			return errors.New("test seam: node 2 refuses the prepare")
		}
		return nil
	})
	setSendHook(t, func(_ context.Context, fromNode, toNode, phase string) error {
		if fromNode == n1 && phase == phasePrepare && toNode != n2 {
			time.Sleep(300 * time.Millisecond)
		}
		return nil
	})
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4]); err != nil {
		t.Errorf("a removal that one member of five refused at prepare answered %v; it must commit on the others", err)
	}
}

// TestALeftoverPromiseCostsOneRetryNotAFailure: node 2 holds a bare promise at
// a round node 1 has not seen, left by a proposer that stopped, and nodes 4
// and 5 cannot be reached, so node 2's promise is needed: node 1's change
// commits on its one retry. Guard: the retry in preparePhase.
func TestALeftoverPromiseCostsOneRetryNotAFailure(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n1, n4, n5 := tc.addrs[0], tc.addrs[3], tc.addrs[4]
	member := clusterOf(tc, 1)
	if _, err := member.prepareMembershipChange(MembershipChangeRequest{FromNode: tc.addrs[2], Phase: phasePrepare,
		Epoch: InitialEpoch + 1, Ballot: ballot{Round: 7, Node: tc.addrs[2]}, BaseMembers: memberIDsOf(member.Members())}); err != nil {
		t.Fatalf("test setup: node 2 promising round 7: %v", err)
	}
	setSendHook(t, func(_ context.Context, fromNode, toNode, _ string) error {
		if fromNode == n1 && (toNode == n4 || toNode == n5) {
			return fmt.Errorf("test seam: %s cannot be reached", toNode)
		}
		return nil
	})
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), n5); err != nil {
		t.Errorf("node 1's removal answered %v; a leftover promise must cost one retry, not the change", err)
	}
}

// TestAnIndeterminateChangeSaysItMayStillComplete: a command whose context
// ends after one accept left says the change may still complete; a change
// every member refused at prepare says nothing was offered. Guard: the two
// texts in runMembershipChange and preparePhase.
func TestAnIndeterminateChangeSaysItMayStillComplete(t *testing.T) {
	t.Run("stopped after an accept left", func(t *testing.T) {
		tc := newTestCluster(t, 5, 3)
		defer tc.stop()
		n1 := tc.addrs[0]
		ctx, cancel := context.WithCancel(commandContext(t))
		var sent atomic.Int32
		setSendHook(t, func(sendCtx context.Context, fromNode, _, phase string) error {
			if fromNode != n1 || phase != phaseAccept || sent.Add(1) == 1 {
				return nil
			}
			cancel()
			<-sendCtx.Done()
			return sendCtx.Err()
		})
		err := clusterOf(tc, 0).proposeRemoval(ctx, tc.addrs[4])
		if !errors.Is(err, ErrMembershipChangeIndeterminate) || !strings.Contains(err.Error(), "usually completes within a minute") {
			t.Errorf("a command stopped after an accept left answered %v; want the indeterminate text", err)
		}
	})
	t.Run("refused at prepare", func(t *testing.T) {
		tc := newTestCluster(t, 5, 3)
		defer tc.stop()
		setPhaseHook(t, func(_, phase, _ string, _ uint64) error {
			if phase == phasePrepare {
				return errors.New("test seam: every member refuses the prepare")
			}
			return nil
		})
		err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4])
		if !errors.Is(err, ErrJoinFailedMajority) || errors.Is(err, ErrMembershipChangeIndeterminate) ||
			!strings.Contains(err.Error(), "nothing was offered and nothing changed") {
			t.Errorf("a change every member refused at prepare answered %v; want the definite text", err)
		}
	})
}

// TestAProposerHearsAheadAndAdopts: node 3 is held on the first epoch while a
// removal commits; once released, its own change hears that the epoch is
// adopted, adopts the set reported, and says it was behind. Guard: 5b.
func TestAProposerHearsAheadAndAdopts(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n3 := tc.addrs[2]
	behind := clusterOf(tc, 2)
	setGossipSkip(t, func(nodeID, peerID, _ string) bool { return nodeID == n3 || peerID == n3 })
	release := holdOnOldEpoch(t, n3, InitialEpoch)
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4]); err != nil {
		t.Fatalf("test setup: removing node 5: %v", err)
	}
	release()
	if epoch := behind.Epoch(); epoch != InitialEpoch {
		t.Fatalf("test setup: node 3 is at epoch %d, want it held on %d", epoch, InitialEpoch)
	}
	err := behind.proposeRemoval(commandContext(t), tc.addrs[3])
	if err == nil || !strings.Contains(err.Error(), "this node was behind") || !strings.Contains(err.Error(), "removing "+tc.addrs[4]) {
		t.Errorf("node 3's change answered %v; want that it was behind, naming the committed removal of node 5", err)
	}
	if epoch := behind.Epoch(); epoch != InitialEpoch+1 {
		t.Errorf("node 3 is at epoch %d after hearing ahead; it must adopt epoch %d", epoch, InitialEpoch+1)
	}
}

// TestAReRunOfACommittedChangeSaysAlreadyCommitted: node 1's removal of node 5
// is accepted only by node 2 and ends indeterminate; another member finishes
// it while node 1 hears nothing; node 1's operator runs the removal again and
// is told it is already made, with success. Guard: the set comparison in 5b.
func TestAReRunOfACommittedChangeSaysAlreadyCommitted(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n1, n2, n5 := tc.addrs[0], tc.addrs[1], tc.addrs[4]
	setGossipSkip(t, func(nodeID, peerID, _ string) bool { return nodeID == n1 || peerID == n1 })
	setPhaseHook(t, func(nodeID, phase, fromNode string, _ uint64) error {
		if fromNode == n1 && phase == phaseAccept && nodeID != n2 {
			time.Sleep(acceptRefusalDelay)
			return fmt.Errorf("test seam: %s refuses node 1's accept", nodeID)
		}
		return nil
	})
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), n5); !errors.Is(err, ErrMembershipChangeIndeterminate) {
		t.Fatalf("test setup: node 1's removal answered %v, want %v", err, ErrMembershipChangeIndeterminate)
	}
	requireAcceptance(t, clusterOf(tc, 1))
	// The finisher promised node 1's ballot, so its own is above it.
	finisherIndex := -1
	for _, index := range []int{2, 3} {
		if pending := pendingOf(clusterOf(tc, index)); pending != nil && pending.Promised.Node == n1 {
			finisherIndex = index
			break
		}
	}
	if finisherIndex < 0 {
		t.Fatal("test setup: neither node 3 nor node 4 promised node 1's ballot")
	}
	finisher := tc.addrs[finisherIndex]
	setSendHook(t, func(_ context.Context, fromNode, toNode, phase string) error {
		if fromNode == finisher && (toNode == n1 || (phase == phasePrepare && toNode == n5)) {
			return fmt.Errorf("test seam: %s does not reach %s", fromNode, toNode)
		}
		return nil
	})
	other := tc.addrs[5-finisherIndex]
	if err := clusterOf(tc, finisherIndex).proposeRemoval(commandContext(t), other); !errors.Is(err, ErrAnotherMembershipChangeFinished) {
		t.Fatalf("test setup: %s's change answered %v; it must finish node 1's removal of node 5", finisher, err)
	}
	if epoch := clusterOf(tc, 0).Epoch(); epoch != InitialEpoch {
		t.Fatalf("test setup: node 1 is at epoch %d, want it still on %d", epoch, InitialEpoch)
	}

	outcome, err := clusterOf(tc, 0).proposeRemovalOutcome(commandContext(t), n5)
	if err != nil || outcome.kind != changeAlreadyCommitted || !strings.Contains(outcome.message, "already committed") {
		t.Errorf("the re-run answered %q %q (%v); want success, already committed", outcome.kind, outcome.message, err)
	}
	if epoch := clusterOf(tc, 0).Epoch(); epoch != InitialEpoch+1 {
		t.Errorf("node 1 is at epoch %d after the re-run; it must adopt epoch %d", epoch, InitialEpoch+1)
	}
}

// TestAnIndeterminateFinishOfAnotherChangeNamesBoth: node 1 finds node 3's
// accepted removal of node 5, offers it, and stops before a majority accepts:
// the text names node 3's change, which may still complete, and node 1's own,
// which was not made. Guard: the template for finishing another change.
func TestAnIndeterminateFinishOfAnotherChangeNamesBoth(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n1, n2, n3, n4, n5 := tc.addrs[0], tc.addrs[1], tc.addrs[2], tc.addrs[3], tc.addrs[4]
	setPhaseHook(t, func(nodeID, phase, fromNode string, _ uint64) error {
		switch {
		case fromNode == n3 && phase == phasePrepare && (nodeID == n2 || nodeID == n5):
		case fromNode == n3 && phase == phaseAccept && nodeID != n4:
			time.Sleep(acceptRefusalDelay)
		case fromNode == n1 && phase == phaseAccept && nodeID != n4:
			time.Sleep(acceptRefusalDelay)
		default:
			return nil
		}
		return fmt.Errorf("test seam: %s refuses %s's %s", nodeID, fromNode, phase)
	})
	if err := clusterOf(tc, 2).proposeRemoval(commandContext(t), n5); !errors.Is(err, ErrMembershipChangeIndeterminate) {
		t.Fatalf("test setup: node 3's removal answered %v, want %v", err, ErrMembershipChangeIndeterminate)
	}
	requireAcceptance(t, clusterOf(tc, 3))
	setSendHook(t, func(_ context.Context, fromNode, toNode, phase string) error {
		if fromNode == n1 && phase == phasePrepare && (toNode == n2 || toNode == n3) {
			return errors.New("test seam: node 1's majority is nodes 1, 4 and 5")
		}
		return nil
	})
	err := clusterOf(tc, 0).proposeRemoval(commandContext(t), n2)
	if !errors.Is(err, ErrMembershipChangeIndeterminate) ||
		!strings.Contains(err.Error(), "another membership change (removing "+n5+")") ||
		!strings.Contains(err.Error(), "Your change (removing "+n2+") was not made") {
		t.Errorf("node 1 answered %v; it must name node 3's change, which may complete, and its own, not made", err)
	}
}

// TestACommitAMemberCannotAdoptDoesNotFailTheCommand: node 3 cannot adopt the
// committed set; it answers the commit not_adopted, the command succeeds, and
// one WARN names node 3. Guard: no commit answer changes the outcome.
func TestACommitAMemberCannotAdoptDoesNotFailTheCommand(t *testing.T) {
	logs := recordClusterLogs(t)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n3 := tc.addrs[2]
	holdOnOldEpoch(t, n3, InitialEpoch)
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4]); err != nil {
		t.Errorf("a removal one member could not adopt answered %v; the set was chosen", err)
	}
	warned := logs.matching(slog.LevelWarn, fmt.Sprintf("member %s could not adopt membership epoch %d", n3, InitialEpoch+1))
	if len(warned) != 1 {
		t.Errorf("%d WARN lines name node 3's failed adoption, want 1", len(warned))
	}
}

// holdPlacementPass makes c's placement pass look unfinished until the
// returned release runs: there is no seam on the pass itself, so this puts a
// channel nobody closes where the running pass's is.
func holdPlacementPass(t *testing.T, c *Cluster) (release func()) {
	t.Helper()
	running := make(chan struct{})
	c.placementMu.Lock()
	c.passRunning = running
	c.placementMu.Unlock()
	var once sync.Once
	release = func() { once.Do(func() { close(running) }) }
	t.Cleanup(release)
	return release
}

// TestACommitAnswersBehindWhileThePlacementPassRuns: node 3's placement pass
// is running; its commit answers behind within placementPassWait, the
// command returns, and node 3 adopts by gossip once the pass ends. Guard: the
// bounded wait in commitMembershipDelivered.
func TestACommitAnswersBehindWhileThePlacementPassRuns(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	member := clusterOf(tc, 2)
	release := holdPlacementPass(t, member)
	started := time.Now()
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4]); err != nil {
		t.Fatalf("removing node 5: %v", err)
	}
	if took := time.Since(started); took > placementPassWait+3*time.Second {
		t.Errorf("the removal took %s with node 3's placement pass running; its commit must answer within %s",
			took.Round(time.Millisecond), placementPassWait)
	}
	if epoch := member.Epoch(); epoch != InitialEpoch {
		t.Errorf("node 3 adopted epoch %d with its placement pass still running", epoch)
	}
	release()
	member.wakeGossip()
	if !holdsWithin(30*time.Second, func() bool { return member.Epoch() == InitialEpoch+1 }) {
		t.Errorf("node 3 never adopted epoch %d by gossip once its placement pass ended", InitialEpoch+1)
	}
}

// TestACommitToANodeTwoEpochsBehindWakesGossip: a commit to a member two
// epochs behind is answered behind, which the proposer counts delivered, and
// the member reaches the epoch by gossip. Guard: the behind answer.
func TestACommitToANodeTwoEpochsBehindWakesGossip(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n3 := tc.addrs[2]
	lagging := clusterOf(tc, 2)
	var skipping atomic.Bool
	skipping.Store(true)
	setGossipSkip(t, func(nodeID, peerID, _ string) bool { return skipping.Load() && (nodeID == n3 || peerID == n3) })
	release := holdOnOldEpoch(t, n3, InitialEpoch)
	proposer := clusterOf(tc, 0)
	if err := proposer.proposeRemoval(commandContext(t), tc.addrs[4]); err != nil {
		t.Fatalf("test setup: the change to epoch 2: %v", err)
	}
	release()
	if err := proposer.proposeRemoval(commandContext(t), tc.addrs[3]); err != nil {
		t.Errorf("the change to epoch 3, with node 3 two epochs behind, answered %v", err)
	}
	_, members := proposer.epochMembers()
	resp, err := lagging.commitMembershipDelivered(MembershipChangeRequest{FromNode: proposer.self.ID, Phase: phaseCommit,
		Epoch: proposer.Epoch(), Members: nodesToPeers(members)})
	if err != nil || resp.Answer != string(answerBehind) {
		t.Errorf("a commit to node 3, two epochs behind, was answered %q (%v); want behind", resp.Answer, err)
	}
	skipping.Store(false)
	lagging.wakeGossip()
	if !holdsWithin(30*time.Second, func() bool { return lagging.Epoch() == proposer.Epoch() }) {
		t.Errorf("node 3 stayed at epoch %d; it must reach %d by gossip", lagging.Epoch(), proposer.Epoch())
	}
}

// oldBinaryConn is a member's connection that answers a membership change as
// a build without the protocol does, with an error frame, and passes every
// other request through.
type oldBinaryConn struct {
	peerConn
}

func (conn oldBinaryConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op != transport.OpMembershipChange {
		return conn.peerConn.SendRequest(ctx, lane, op, payload)
	}
	request := transport.NewFrame(op, payload)
	return errorFrame(request, fmt.Errorf("unsupported opcode %s: this server does not implement it", op)), nil
}

// TestAProposerCountsAnOldMembersErrorFrameAsARefusal: node 3 answers a
// membership change with an error frame, node 2 cannot be reached: the change
// fails without a retry, and the upgrade sentence is logged once for node 3
// across two commands. Guard: the error frame is read as such in askMember.
func TestAProposerCountsAnOldMembersErrorFrameAsARefusal(t *testing.T) {
	logs := recordClusterLogs(t)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	n2, n3 := tc.addrs[1], tc.addrs[2]
	old := proposer.peerFor(ringNodeAt(n3))
	conn, err := old.dial(commandContext(t))
	if err != nil {
		t.Fatalf("test setup: dialing node 3: %v", err)
	}
	old.mu.Lock()
	old.client = oldBinaryConn{peerConn: conn}
	old.mu.Unlock()
	t.Cleanup(func() {
		old.mu.Lock()
		old.client = conn
		old.mu.Unlock()
	})
	var prepares atomic.Int32
	setSendHook(t, func(_ context.Context, fromNode, toNode, phase string) error {
		if fromNode == proposer.self.ID && phase == phasePrepare {
			prepares.Add(1)
		}
		if toNode == n2 {
			return errors.New("test seam: node 2 cannot be reached")
		}
		return nil
	})
	for attempt := 0; attempt < 2; attempt++ {
		if err := proposer.proposeRemoval(commandContext(t), n2); !errors.Is(err, ErrJoinFailedMajority) {
			t.Errorf("command %d answered %v; with node 2 down and node 3 refusing, want no majority", attempt+1, err)
		}
	}
	if sent := prepares.Load(); sent != 4 {
		t.Errorf("two commands sent %d prepares, want 4: an error frame is not a promise a retry gets past", sent)
	}
	if warned := logs.matching(slog.LevelWarn, "member "+n3+" runs a build without"); len(warned) != 1 {
		t.Errorf("%d WARN lines say node 3 runs an older build, want 1", len(warned))
	}
}

// TestTheOldProposeOpcodeIsRefusedWithTheUpgradeMessage: the vote of the
// replaced protocol, sent by an older proposer, is refused with the upgrade
// sentence. Guard: the refusal stub.
func TestTheOldProposeOpcodeIsRefusedWithTheUpgradeMessage(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	proposer := clusterOf(tc, 0)
	conn, err := proposer.peerFor(ringNodeAt(tc.addrs[1])).dial(commandContext(t))
	if err != nil {
		t.Fatalf("dialing node 2: %v", err)
	}
	payload, err := bson.Marshal(ClusterProposeRequest{Token: proposer.localToken(), FromNode: proposer.self.ID,
		Epoch: InitialEpoch + 1, Members: nodesToPeers(proposer.Members())})
	if err != nil {
		t.Fatalf("encoding the vote: %v", err)
	}
	frame, err := conn.SendRequest(commandContext(t), transport.LaneControl, transport.OpClusterPropose, payload)
	if err != nil {
		t.Fatalf("sending the vote: %v", err)
	}
	var resp ClusterProposeResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the answer (%s): %v", frame.Opcode, err)
	}
	if resp.Success || !strings.Contains(resp.Error, "upgrade every member before adding or removing nodes") {
		t.Errorf("the old vote was answered success=%v %q; want a refusal with the upgrade sentence", resp.Success, resp.Error)
	}
	if epoch := clusterOf(tc, 1).Epoch(); epoch != InitialEpoch {
		t.Errorf("the old vote moved node 2 to epoch %d", epoch)
	}
}

// TestARefusedSelfAcceptEndsTheCommandRefused: node 1 is held between its
// prepare and its own accept while node 2 prepares a higher ballot on a
// majority that includes node 1; released, node 1's own acceptor refuses its
// accept, and the command ends refused with no accept sent and its set
// adopted nowhere. Guard: countSelf counts only the proposer's own yes.
func TestARefusedSelfAcceptEndsTheCommandRefused(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	n1, n2 := tc.addrs[0], tc.addrs[1]
	adoptions := recordAdoptions(t, tc)
	setGossipSkip(t, func(nodeID, peerID, _ string) bool { return nodeID == n1 || peerID == n1 })
	atSelfAccept := make(chan struct{})
	release := make(chan struct{})
	var heldOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	setPhaseHook(t, func(nodeID, phase, _ string, _ uint64) error {
		if nodeID == n1 && phase == "self-accept" {
			heldOnce.Do(func() { close(atSelfAccept) })
			<-release
		}
		return nil
	})
	var accepts atomic.Int32
	setSendHook(t, func(_ context.Context, fromNode, toNode, phase string) error {
		if fromNode == n1 && phase == phaseAccept {
			accepts.Add(1)
		}
		if fromNode == n2 && toNode == n1 && phase == phaseCommit {
			return errors.New("test seam: node 2's commit does not reach node 1")
		}
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4]) }()
	select {
	case <-atSelfAccept:
	case err := <-done:
		t.Fatalf("test setup: node 1's change ended before its own accept: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: node 1 never reached its own accept")
	}
	if err := clusterOf(tc, 1).proposeRemoval(commandContext(t), tc.addrs[3]); err != nil {
		t.Fatalf("test setup: node 2's change: %v", err)
	}
	if pending := pendingOf(clusterOf(tc, 0)); pending == nil || pending.Promised.Node != n2 {
		t.Fatalf("test setup: node 1 did not promise node 2's ballot: %+v", pending)
	}
	releaseOnce.Do(func() { close(release) })
	err := <-done
	if err == nil || !strings.Contains(err.Error(), "another member ("+n2+") started a membership change") {
		t.Errorf("node 1 answered %v; want refused, naming node 2", err)
	}
	if sent := accepts.Load(); sent != 0 {
		t.Errorf("node 1 sent %d accepts after its own acceptor refused its offer", sent)
	}
	adoptions.assertOneSet(t, InitialEpoch+1, addrsWithout(tc.addrs, tc.addrs[3]))
}

// TestAChosenChangeWhoseLocalAdoptionFailsIsReportedCommitted: the proposer
// cannot adopt its own chosen set; the commits still go out, every other
// member adopts, and the command says the change is committed. Guard: the
// commits are sent before the local failure is reported.
func TestAChosenChangeWhoseLocalAdoptionFailsIsReportedCommitted(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	holdOnOldEpoch(t, tc.addrs[0], InitialEpoch)
	err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4])
	if !errors.Is(err, ErrMembershipCommittedNotAdoptedHere) ||
		!strings.Contains(err.Error(), fmt.Sprintf("is committed at membership epoch %d", InitialEpoch+1)) {
		t.Errorf("a chosen change the proposer could not adopt answered %v; want that it is committed", err)
	}
	for index := 1; index < 4; index++ {
		if epoch := clusterOf(tc, index).Epoch(); epoch != InitialEpoch+1 {
			t.Errorf("node %d is at epoch %d when the command returns; the commit must reach it", index+1, epoch)
		}
	}
}
