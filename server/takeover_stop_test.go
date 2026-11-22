package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// stopWaitBound is longer than the 3 s a stopping cluster used to wait for its
// goroutines, so a Stop that still lets go at that bound is seen letting go.
const stopWaitBound = 3500 * time.Millisecond

// TestStopWaitsForTakeOverRoundBeforeClosingStore holds a joiner's take-over
// round just before it reads a bucket's copy, stops the joiner, and checks that
// the node store stays open while the round is held, past the old 3 s bound,
// and that Stop returns, with the round gone and no panic, once it is released.
func TestStopWaitsForTakeOverRoundBeforeClosingStore(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	after := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	before := ringWith(t, 2, tc.addrs...)
	bucket := bucketNamed(t, "stopping", func(name string) bool { return ownerOn(after, name) == newcomer.ID })
	oldOwner := nodeIndexByAddr(t, tc.addrs, ownerOn(before, bucket))
	tc.ok(oldOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)

	reached := make(chan struct{})
	gate := make(chan struct{})
	var reachedOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	hold := func(nodeID, database, name string) {
		if nodeID != newcomer.ID || name != bucket {
			return
		}
		reachedOnce.Do(func() { close(reached) })
		<-gate
	}
	takeOverBeforeReadHook.Store(&hold)
	t.Cleanup(func() { takeOverBeforeReadHook.Store(nil) })

	joiner := joinNewcomer(t, tc, oldOwner, ports)
	select {
	case <-reached:
	case <-time.After(takeOverWait):
		release()
		t.Fatalf("setup: the joiner's take-over round never reached the read of %s", bucket)
	}

	var stopReturned atomic.Bool
	stopped := make(chan struct{})
	service := joiner.node.server
	go func() {
		_ = service.Stop()
		stopReturned.Store(true)
		close(stopped)
	}()

	time.Sleep(stopWaitBound)
	if stopReturned.Load() {
		// The round is left held on purpose: releasing it would resume it on
		// the closed store and take the test binary down with it.
		t.Fatal("Stop returned, and closed the node store, while a take-over round still held it")
	}
	release()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("Stop did not return after the take-over round was released")
	}
	joiner.node.server = nil
	joiner.stop()
}

// TestNoGoroutineIsOnABoundedWaitGroupAndStoreWritersStartOnStoreWG is a source
// check of the rule Cluster.Stop relies on (BUG-0680): every goroutine that
// reads or writes the node store or the log is on storeWG, which Stop waits for
// without a bound, because Server.Stop closes the store right after. The
// cluster once had a second group, wg, waited for only 3 s, and the membership
// recovery, the auth-config loop and the peer verifier sat on it although each
// writes the store. wg is gone, and this fails if a group named wg comes back
// (any Add, Done, Wait or Go on a field of that name), or if one of those three
// goroutines is started other than as the body of
// `if c.startStoreGoroutine() { go ... }`, which refuses it once Stop is
// waiting. Guard: put one of them back on a bare go statement and this fails.
func TestNoGoroutineIsOnABoundedWaitGroupAndStoreWritersStartOnStoreWG(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the server sources: %v", err)
	}
	storeWriters := []string{"runMembershipRecovery", "authConfigLoop", "verifyPeer"}
	var bareGroupCalls, unguardedStarts []string
	fileSet := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		guarded := make(map[*ast.GoStmt]bool)
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.IfStmt:
				call, isCall := typed.Cond.(*ast.CallExpr)
				if !isCall {
					return true
				}
				if selector, isSelector := call.Fun.(*ast.SelectorExpr); isSelector && selector.Sel.Name == "startStoreGoroutine" {
					ast.Inspect(typed.Body, func(inner ast.Node) bool {
						if statement, isGo := inner.(*ast.GoStmt); isGo {
							guarded[statement] = true
						}
						return true
					})
				}
			case *ast.CallExpr:
				selector, isSelector := typed.Fun.(*ast.SelectorExpr)
				if !isSelector {
					return true
				}
				if group, isGroup := selector.X.(*ast.SelectorExpr); isGroup && group.Sel.Name == "wg" {
					bareGroupCalls = append(bareGroupCalls, fileSet.Position(typed.Pos()).String()+" wg."+selector.Sel.Name)
				}
			}
			return true
		})
		ast.Inspect(parsed, func(node ast.Node) bool {
			statement, isGo := node.(*ast.GoStmt)
			if !isGo || guarded[statement] {
				return true
			}
			if selector, isSelector := statement.Call.Fun.(*ast.SelectorExpr); isSelector && slices.Contains(storeWriters, selector.Sel.Name) {
				unguardedStarts = append(unguardedStarts, fileSet.Position(statement.Pos()).String()+" go "+selector.Sel.Name)
			}
			return true
		})
	}
	if len(bareGroupCalls) != 0 {
		t.Errorf("a wait group named wg is used again: %v; a goroutine that uses the node store goes on storeWG (startStoreGoroutine)", bareGroupCalls)
	}
	if len(unguardedStarts) != 0 {
		t.Errorf("store writers started outside `if c.startStoreGoroutine() { go ... }`: %v", unguardedStarts)
	}
}

// withPeerAddedAfterTheClose runs stop() on a two-node cluster's second node
// with a peer added at stopAfterPeersClosedHook, the way a membership adoption
// on a goroutine still running adds one. observe, if set, runs at the hook
// first. It returns the added peer.
func withPeerAddedAfterTheClose(t *testing.T, observe func(c *Cluster)) (*Cluster, *peer) {
	t.Helper()
	tc := newTestCluster(t, 2, 2)
	t.Cleanup(tc.stop)
	cluster := clusterOf(tc, 1)
	var added *peer
	hook := func(stopping *Cluster) {
		if stopping != cluster {
			return
		}
		if observe != nil {
			observe(stopping)
		}
		late := ring.Node{ID: "127.0.0.1:1", Addr: "127.0.0.1:1"}
		added = stopping.peerFor(late)
		stopping.ensurePeerRunning(late)
	}
	stopAfterPeersClosedHook.Store(&hook)
	t.Cleanup(func() { stopAfterPeersClosedHook.Store(nil) })

	service := tc.nodes[1].server
	tc.nodes[1].server = nil
	stopped := make(chan struct{})
	go func() {
		_ = service.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		// The peer is closed here so the test's own cleanup is not stuck behind
		// the sender that Stop is waiting for.
		if added != nil {
			added.close()
		}
		<-stopped
		t.Fatal("Stop did not return within 10 s with a peer added after it had closed the peers")
	}
	return cluster, added
}

// TestStopReturnsWithAPeerAddedAfterItClosedThePeers: a membership adoption on
// a goroutine that is still running adds a peer after Stop has taken its list
// of peers to close. Its sender must not hold Stop up. Either of two things
// ends it, each pinned by its own test below: the sender is refused once Stop
// is waiting (TestAStoreGoroutineStartedAfterStopBeganIsRefused), or it leaves
// when the cluster's context is done (TestASenderLeavesWhenTheClusterContextIsDone).
func TestStopReturnsWithAPeerAddedAfterItClosedThePeers(t *testing.T) {
	withPeerAddedAfterTheClose(t, nil)
}

// TestAStoreGoroutineStartedAfterStopBeganIsRefused: from the moment Stop has
// closed the peers, and after it has returned, a goroutine that uses the node
// store is not started, because Server.Stop closes the store right after.
// Guard: remove the storeStopping check in startStoreGoroutine, or set the flag
// after the peers are closed instead of before they are listed.
func TestAStoreGoroutineStartedAfterStopBeganIsRefused(t *testing.T) {
	var startedDuringStop bool
	cluster, _ := withPeerAddedAfterTheClose(t, func(c *Cluster) {
		if startedDuringStop = c.startStoreGoroutine(); startedDuringStop {
			c.storeWG.Done()
		}
	})
	if startedDuringStop {
		t.Error("a store goroutine was started after Stop had begun to close the peers")
	}
	if cluster.startStoreGoroutine() {
		cluster.storeWG.Done()
		t.Error("a store goroutine was started after Stop had returned")
	}
}

// TestASenderLeavesWhenTheClusterContextIsDone: a sender is on storeWG, which
// Stop waits for without a bound, so it must end when the context is done even
// if its peer was never closed. Guard: take the context out of runSender.
func TestASenderLeavesWhenTheClusterContextIsDone(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	cluster := clusterOf(tc, 1)
	late := ring.Node{ID: "127.0.0.1:1", Addr: "127.0.0.1:1"}
	cluster.peerFor(late)
	cluster.ensurePeerRunning(late)

	// The real peers are closed, so the late peer's sender is the only thing
	// left that only the context can end.
	cluster.peersMu.RLock()
	for id, other := range cluster.peers {
		if id != late.ID {
			other.close()
		}
	}
	cluster.peersMu.RUnlock()
	cluster.cancel()
	senders := make(chan struct{})
	go func() {
		cluster.storeWG.Wait()
		close(senders)
	}()
	select {
	case <-senders:
	case <-time.After(10 * time.Second):
		cluster.peerFor(late).close()
		<-senders
		t.Fatal("a sender was still running 10 s after the cluster's context was done")
	}
}
