package server

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
)

// Recovery of a lost proposer's change (spec §6.4), across real nodes.

// setRecoveryDelays lowers the recovery's delays for the test.
func setRecoveryDelays(t *testing.T, after, jitter, ceiling time.Duration) {
	t.Helper()
	before := []time.Duration{membershipRecoveryAfter.get(), membershipRecoveryJitter.get(), membershipRecoveryCap.get()}
	membershipRecoveryAfter.set(after)
	membershipRecoveryJitter.set(jitter)
	membershipRecoveryCap.set(ceiling)
	t.Cleanup(func() {
		membershipRecoveryAfter.set(before[0])
		membershipRecoveryJitter.set(before[1])
		membershipRecoveryCap.set(before[2])
	})
}

// holdProposerAtChosen holds node nodeID's own change once its set is chosen
// and before it adopts it or sends any commit: a proposer lost at the worst
// moment. The hold ends with the test, before the cluster stops.
func holdProposerAtChosen(t *testing.T, nodeID string) <-chan struct{} {
	t.Helper()
	held := make(chan struct{})
	release := make(chan struct{})
	var heldOnce, releaseOnce sync.Once
	setPhaseHook(t, func(hookNode, phase, _ string, _ uint64) error {
		if hookNode == nodeID && phase == "chosen" {
			heldOnce.Do(func() { close(held) })
			<-release
		}
		return nil
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	return held
}

// startLostChange starts node 1's removal of node 5 and returns once node 1 is
// held with the set chosen; the command's error arrives on the channel.
func startLostChange(t *testing.T, tc *testCluster) <-chan error {
	t.Helper()
	held := holdProposerAtChosen(t, tc.addrs[0])
	done := make(chan error, 1)
	go func() { done <- clusterOf(tc, 0).proposeRemoval(context.Background(), tc.addrs[4]) }()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("test setup: node 1's removal ended before it was chosen: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: node 1's removal was never chosen")
	}
	return done
}

// waitEveryMemberAt waits until every running node but the skipped one is at
// epoch with members.
func waitEveryMemberAt(t *testing.T, tc *testCluster, epoch uint64, members []string, skip int) {
	t.Helper()
	if !holdsWithin(30*time.Second, func() bool {
		for index, node := range tc.nodes {
			if index == skip || node.server == nil {
				continue
			}
			at, adopted := clusterOf(tc, index).epochMembers()
			if at != epoch || !sameMemberIDs(memberIDsOf(adopted), members) {
				return false
			}
		}
		return true
	}) {
		for index := range tc.nodes {
			if tc.nodes[index].server != nil {
				at, adopted := clusterOf(tc, index).epochMembers()
				t.Logf("node %d: epoch %d %v", index+1, at, memberIDsOf(adopted))
			}
		}
		t.Fatalf("not every member reached epoch %d with %v", epoch, members)
	}
}

// TestALostProposersAcceptedChangeIsFinished: node 1's removal of node 5 is
// chosen and node 1 stops before it adopts it or sends a commit; a member's
// recovery finishes the change, and every member reaches epoch 2 with it.
// Guard: the recovery timer.
func TestALostProposersAcceptedChangeIsFinished(t *testing.T) {
	setRecoveryDelays(t, 500*time.Millisecond, 200*time.Millisecond, 2*time.Second)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	startLostChange(t, tc)
	waitEveryMemberAt(t, tc, InitialEpoch+1, addrsWithout(tc.addrs, tc.addrs[4]), 4)
}

// TestARestartedAcceptorFinishesTheChange: as above, but the members accepted
// with a long delay armed, and one of them restarts: the recovery goroutine
// its Start runs arms itself from the acceptance NewCluster restored. Guard:
// NewCluster records the restored acceptance.
func TestARestartedAcceptorFinishesTheChange(t *testing.T) {
	setRecoveryDelays(t, 10*time.Minute, 0, 10*time.Minute)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	startLostChange(t, tc)
	acceptor := -1
	for _, index := range []int{1, 2, 3} {
		if pending := pendingOf(clusterOf(tc, index)); pending != nil && pending.Accepted != nil {
			acceptor = index
			break
		}
	}
	if acceptor < 0 {
		t.Fatal("test setup: no member holds node 1's acceptance")
	}
	membershipRecoveryAfter.set(500 * time.Millisecond)
	tc.restartNode(acceptor)
	waitEveryMemberAt(t, tc, InitialEpoch+1, addrsWithout(tc.addrs, tc.addrs[4]), 4)
}

// TestNoRecoveryWithoutAnAcceptance: a node armed with no acceptance proposes
// nothing.
func TestNoRecoveryWithoutAnAcceptance(t *testing.T) {
	setRecoveryDelays(t, 100*time.Millisecond, 0, time.Second)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	var prepares atomic.Int32
	setSendHook(t, func(_ context.Context, _, _, phase string) error {
		if phase == phasePrepare {
			prepares.Add(1)
		}
		return nil
	})
	for index := range tc.nodes {
		clusterOf(tc, index).armRecovery()
	}
	time.Sleep(time.Second)
	if sent := prepares.Load(); sent != 0 {
		t.Errorf("nodes with no acceptance sent %d prepares", sent)
	}
	if epoch := clusterOf(tc, 0).Epoch(); epoch != InitialEpoch {
		t.Errorf("the membership moved to epoch %d with no change proposed", epoch)
	}
}

// TestNoRecoveryOnARemovedNode: a removed node is no acceptor, and runs no
// recovery even when a record says it accepted. Guard: the removed checks in
// armRecovery and recoveryProposal.
func TestNoRecoveryOnARemovedNode(t *testing.T) {
	logs := recordClusterLogs(t)
	setRecoveryDelays(t, 100*time.Millisecond, 0, time.Second)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	removed := clusterOf(tc, 4)
	if err := clusterOf(tc, 0).proposeRemoval(commandContext(t), tc.addrs[4]); err != nil {
		t.Fatalf("test setup: removing node 5: %v", err)
	}
	if !holdsWithin(10*time.Second, removed.removed) {
		t.Fatal("test setup: node 5 never learned of its removal")
	}
	epoch, members := removed.epochMembers()
	kept := slices.DeleteFunc(slices.Clone(members), func(member ring.Node) bool { return member.ID == tc.addrs[3] })
	accepted := ballot{Round: 1, Node: tc.addrs[0]}
	setPending(removed, &pendingChange{Epoch: epoch + 1, Promised: accepted, Accepted: &accepted, Members: kept,
		Change: ringNodeAt(tc.addrs[3])})
	removed.armRecovery()
	select {
	case removed.recoveryArm <- time.Now():
	default:
	}
	time.Sleep(time.Second)
	if warned := logs.matching(slog.LevelWarn, "could not finish the membership change"); len(warned) != 0 {
		t.Errorf("the removed node ran a recovery: %v", warned[0].msg)
	}
	if got := clusterOf(tc, 0).Epoch(); got != epoch {
		t.Errorf("the membership moved to epoch %d after a removed node's arm", got)
	}
}

// recoveryGoroutines counts the recovery goroutines running in the test
// binary.
func recoveryGoroutines() int {
	stack := make([]byte, 1<<22)
	stack = stack[:runtime.Stack(stack, true)]
	return strings.Count(string(stack), "(*Cluster).runMembershipRecovery(")
}

// TestRecoveryStopsOnClusterStop: Stop waits for the recovery goroutine,
// and none is left once every node stopped. A recovery held where it ignores
// the node's context keeps Stop waiting, with no bound, and Stop logs that it
// is still waiting (BUG-0680: the node store closes right after Stop).
// Guard: the goroutine is on c.storeWG, which Stop waits for.
func TestRecoveryStopsOnClusterStop(t *testing.T) {
	logs := recordClusterLogs(t)
	setRecoveryDelays(t, 100*time.Millisecond, 0, time.Hour)
	boundBefore := clusterStopSlowAfter.get()
	clusterStopSlowAfter.set(300 * time.Millisecond)
	t.Cleanup(func() { clusterStopSlowAfter.set(boundBefore) })
	tc := newTestCluster(t, 3, 3)
	member := clusterOf(tc, 1)
	inRecovery := make(chan struct{})
	release := make(chan struct{})
	var heldOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	setSendHook(t, func(_ context.Context, fromNode, _, phase string) error {
		if fromNode == member.self.ID && phase == phasePrepare {
			heldOnce.Do(func() { close(inRecovery) })
			<-release
		}
		return nil
	})
	proposed := slices.DeleteFunc(member.Members(), func(node ring.Node) bool { return node.ID == tc.addrs[2] })
	if _, err := member.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, proposed,
		Peer{ID: tc.addrs[2], Addr: tc.addrs[2]})); err != nil {
		t.Fatalf("test setup: accepting: %v", err)
	}
	select {
	case <-inRecovery:
	case <-time.After(10 * time.Second):
		t.Fatal("test setup: node 2's recovery never started")
	}
	stopped := make(chan struct{})
	go func() {
		tc.stopNodeKeepingData(1)
		close(stopped)
	}()
	if !holdsWithin(10*time.Second, func() bool {
		return len(logs.matching(slog.LevelWarn, "replication goroutines that use the node store are still stopping")) > 0
	}) {
		t.Errorf("Stop did not say it was still waiting for the recovery goroutine it holds")
	}
	select {
	case <-stopped:
		t.Error("Stop returned while the recovery goroutine was held")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("node 2 never stopped once the recovery was released")
	}
	tc.stop()
	if !holdsWithin(5*time.Second, func() bool { return recoveryGoroutines() == 0 }) {
		t.Errorf("%d recovery goroutines are still running after every node stopped", recoveryGoroutines())
	}
}

// TestAnAcceptDuringStopDoesNotRace: accepts arrive while the node stops; the
// arm never blocks and nothing races Stop's wait. Run with -race. The race
// detector reports a WaitGroup Add only at a zero counter during a Wait,
// which no schedule here reaches reliably, so the rule it guards is also
// checked in the source: only Cluster.Start starts the recovery goroutine,
// after its startStoreGoroutine. Guard: no handler creates the recovery goroutine.
func TestAnAcceptDuringStopDoesNotRace(t *testing.T) {
	assertOnlyStartRunsRecovery(t)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	member := clusterOf(tc, 1)
	proposed := slices.DeleteFunc(member.Members(), func(node ring.Node) bool { return node.ID == tc.addrs[2] })
	var accepts sync.WaitGroup
	for round := uint64(1); round <= 20; round++ {
		accepts.Add(1)
		go func() {
			defer accepts.Done()
			request := acceptRequestOf(tc.addrs[0], InitialEpoch+1, proposed, Peer{ID: tc.addrs[2], Addr: tc.addrs[2]})
			request.Ballot.Round = round
			_, _ = member.acceptMembershipChange(request)
		}()
	}
	stopped := make(chan struct{})
	go func() {
		tc.stopNodeKeepingData(1)
		close(stopped)
	}()
	accepts.Wait()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("node 2 never stopped while accepts arrived")
	}
}

// TestRecoveryConstantsAreSetWithoutARace: a test lowers the recovery delay
// while a running node's recovery goroutine reads it. Run with -race. Guard:
// the delays are atomic.
func TestRecoveryConstantsAreSetWithoutARace(t *testing.T) {
	setRecoveryDelays(t, time.Hour, 0, time.Hour)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	member := clusterOf(tc, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for round := 0; round < 50; round++ {
			member.armRecovery()
			time.Sleep(time.Millisecond)
		}
	}()
	for round := 0; round < 50; round++ {
		membershipRecoveryAfter.set(time.Duration(round+1) * time.Hour)
		time.Sleep(time.Millisecond)
	}
	<-done
}

// TestRecoveryBacksOffWhenNoMajorityAnswers: with three of five members
// unreachable from it, a member's recovery fails and doubles its delay up to
// the cap, with one WARN per step. Guard: the doubling.
func TestRecoveryBacksOffWhenNoMajorityAnswers(t *testing.T) {
	logs := recordClusterLogs(t)
	setRecoveryDelays(t, 200*time.Millisecond, 0, 800*time.Millisecond)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	recovering := clusterOf(tc, 1)
	setSendHook(t, func(_ context.Context, fromNode, toNode, _ string) error {
		if fromNode == tc.addrs[1] && toNode != tc.addrs[0] {
			return fmt.Errorf("test seam: %s cannot be reached", toNode)
		}
		return nil
	})
	proposed := slices.DeleteFunc(recovering.Members(), func(node ring.Node) bool { return node.ID == tc.addrs[4] })
	if _, err := recovering.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, proposed,
		Peer{ID: tc.addrs[4], Addr: tc.addrs[4]})); err != nil {
		t.Fatalf("test setup: accepting: %v", err)
	}
	want := []string{"400ms", "800ms", "800ms"}
	if !holdsWithin(20*time.Second, func() bool {
		return len(logs.matching(slog.LevelWarn, "could not finish the membership change")) >= len(want)
	}) {
		t.Fatalf("the recovery logged %d backoff WARNs, want at least %d",
			len(logs.matching(slog.LevelWarn, "could not finish the membership change")), len(want))
	}
	warned := logs.matching(slog.LevelWarn, "could not finish the membership change")
	for index, delay := range want {
		if !strings.HasSuffix(warned[index].msg, "tries again in "+delay) {
			t.Errorf("backoff step %d said %q, want it to try again in %s", index+1, warned[index].msg, delay)
		}
	}
}

// TestRecoveryReArmsWhenTheNodeIsProposing: a recovery that finds its node's
// own change running tries again later instead of dropping the acceptance.
// Guard: the re-arm on ErrEpochInProgress.
func TestRecoveryReArmsWhenTheNodeIsProposing(t *testing.T) {
	setRecoveryDelays(t, 300*time.Millisecond, 0, 2*time.Second)
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	recovering := clusterOf(tc, 1)
	recovering.transition.Store(true)
	proposed := slices.DeleteFunc(recovering.Members(), func(node ring.Node) bool { return node.ID == tc.addrs[4] })
	if _, err := recovering.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, proposed,
		Peer{ID: tc.addrs[4], Addr: tc.addrs[4]})); err != nil {
		t.Fatalf("test setup: accepting: %v", err)
	}
	time.Sleep(time.Second)
	if epoch := recovering.Epoch(); epoch != InitialEpoch {
		t.Fatalf("test setup: the recovery ran while the node was proposing: epoch %d", epoch)
	}
	recovering.transition.Store(false)
	waitEveryMemberAt(t, tc, InitialEpoch+1, addrsWithout(tc.addrs, tc.addrs[4]), 4)
}

// assertOnlyStartRunsRecovery fails the test unless the package's one go
// statement of runMembershipRecovery is in Cluster.Start, after a startStoreGoroutine.
func assertOnlyStartRunsRecovery(t *testing.T) {
	t.Helper()
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package's sources: %v", err)
	}
	fileSet := token.NewFileSet()
	var starters []string
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, source, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", source, err)
		}
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			addedBefore := false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if call, isCall := node.(*ast.CallExpr); isCall {
					if selector, isSelector := call.Fun.(*ast.SelectorExpr); isSelector && selector.Sel.Name == "startStoreGoroutine" {
						addedBefore = true
					}
				}
				statement, isGo := node.(*ast.GoStmt)
				if !isGo {
					return true
				}
				if selector, isSelector := statement.Call.Fun.(*ast.SelectorExpr); isSelector && selector.Sel.Name == "runMembershipRecovery" {
					starters = append(starters, fmt.Sprintf("%s (startStoreGoroutine before it: %v)", function.Name.Name, addedBefore))
				}
				return true
			})
		}
	}
	if !slices.Equal(starters, []string{"Start (startStoreGoroutine before it: true)"}) {
		t.Errorf("the recovery goroutine is started by %v; only Cluster.Start may start it, after startStoreGoroutine", starters)
	}
}

// TestStopWaitsForAMembershipAdoptionOnTheRecoveryGoroutine (BUG-0680): the
// recovery goroutine writes the node store while it adopts a change (the
// membership history, then the placement pass), so Stop waits for it before
// Server.Stop closes the store, and does not give up on it at the bound it
// keeps for network-only goroutines. The adoption is held after its history
// write, as a slow disk would hold it, and Stop must still be waiting well
// past the bound.
func TestStopWaitsForAMembershipAdoptionOnTheRecoveryGoroutine(t *testing.T) {
	setRecoveryDelays(t, 100*time.Millisecond, 0, time.Hour)
	boundBefore := clusterStopSlowAfter.get()
	clusterStopSlowAfter.set(300 * time.Millisecond)
	t.Cleanup(func() { clusterStopSlowAfter.set(boundBefore) })
	tc := newTestCluster(t, 3, 3)
	member := clusterOf(tc, 1)
	inAdoption := make(chan struct{})
	release := make(chan struct{})
	var heldOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	hold := func(nodeID string, epoch uint64) {
		if nodeID == member.self.ID && epoch == InitialEpoch+1 {
			heldOnce.Do(func() { close(inAdoption) })
			<-release
		}
	}
	viewAdoptedHook.Store(&hold)
	t.Cleanup(func() { viewAdoptedHook.Store(nil) })
	proposed := slices.DeleteFunc(member.Members(), func(node ring.Node) bool { return node.ID == tc.addrs[2] })
	if _, err := member.acceptMembershipChange(acceptRequestOf(tc.addrs[0], InitialEpoch+1, proposed,
		Peer{ID: tc.addrs[2], Addr: tc.addrs[2]})); err != nil {
		t.Fatalf("test setup: accepting: %v", err)
	}
	select {
	case <-inAdoption:
	case <-time.After(10 * time.Second):
		t.Fatal("test setup: node 2 never adopted the change")
	}
	stopped := make(chan struct{})
	go func() {
		tc.stopNodeKeepingData(1)
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Error("Stop returned while a goroutine was still inside a membership adoption; Server.Stop closes the store right after it")
	case <-time.After(5 * clusterStopSlowAfter.get()):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("node 2 never stopped once the adoption was released")
	}
	tc.stop()
}
