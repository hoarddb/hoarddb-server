package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0620: a source that accepts the dial and the request and never answers
// held the pending-copy worker, the take-over worker and an operator's migrate
// for as long as the node ran, because nothing put a deadline on a pull's
// requests. Every request a pull makes now has its own deadline
// (pullRequestTimeout); a pull that makes progress is never cut.

// shortenPullTimeouts sets the per-request deadline of a pull's data and done
// requests, and the larger one of its MIGRATE_START, for the test.
func shortenPullTimeouts(t *testing.T, request, start time.Duration) {
	t.Helper()
	previousRequest, previousStart := pullRequestTimeout.get(), pullStartTimeout.get()
	pullRequestTimeout.set(request)
	pullStartTimeout.set(start)
	t.Cleanup(func() {
		pullRequestTimeout.set(previousRequest)
		pullStartTimeout.set(previousStart)
	})
}

// hungSourceConn stands in for a source that accepted the connection and the
// request and never answers: a send of a watched opcode blocks until its
// context ends, as the pool's SendRequest does. Everything else, and every
// send after the first hangs, passes to the real connection.
type hungSourceConn struct {
	peerConn
	ops     map[transport.Opcode]bool
	bucket  string // empty hangs the opcode for every bucket
	hangs   int32  // sends to hang; negative hangs every one
	entered chan struct{}
	mu      sync.Mutex
	taken   int32
}

func newHungSourceConn(bucket string, hangs int32, ops ...transport.Opcode) *hungSourceConn {
	watched := make(map[transport.Opcode]bool, len(ops))
	for _, op := range ops {
		watched[op] = true
	}
	return &hungSourceConn{ops: watched, bucket: bucket, hangs: hangs, entered: make(chan struct{}, 64)}
}

func (h *hungSourceConn) hangsThis(op transport.Opcode, payload []byte) bool {
	if !h.ops[op] {
		return false
	}
	if h.bucket != "" {
		var start MigrateStartRequest
		if err := bson.Unmarshal(payload, &start); err != nil || start.Bucket != h.bucket {
			return false
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.hangs >= 0 && h.taken >= h.hangs {
		return false
	}
	h.taken++
	return true
}

func (h *hungSourceConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if h.hangsThis(op, payload) {
		h.entered <- struct{}{}
		<-ctx.Done()
		return nil, fmt.Errorf("the hung source: %w", ctx.Err())
	}
	return h.peerConn.SendRequest(ctx, lane, op, payload)
}

// slowSourceConn answers every pull request after delay, as a slow but
// healthy source does.
type slowSourceConn struct {
	peerConn
	delay time.Duration
	// only, when set, limits the delay to those opcodes; otherwise all three
	// of a pull's requests are slow.
	only map[transport.Opcode]bool
}

func (s *slowSourceConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	slow := op == transport.OpMigrateStart || op == transport.OpMigrateData || op == transport.OpMigrateDone
	if s.only != nil {
		slow = s.only[op]
	}
	if slow {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.peerConn.SendRequest(ctx, lane, op, payload)
}

// returnsWithin runs pull and reports its error, failing the test if it has
// not returned within limit. The pull's context is the caller's own: a
// long-lived one with no deadline, as the workers pass, so only a deadline the
// pull puts on its requests can end a hung one.
func returnsWithin(t *testing.T, what string, limit time.Duration, pull func(ctx context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pull(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		cancel()
		<-done
		t.Fatalf("%s was still waiting on the hung source after %v; nothing ends a request the source never answers", what, limit)
		return nil
	}
}

// pendingFromOwner is a three-node cluster with auto-rebalance off and one
// bucket pending on member, whose connection to the bucket's owner is wrapped
// by wrap. It returns the member's cluster, the owner as the member sees it
// and the bucket.
func pendingFromOwner(t *testing.T, wrap func(peerConn) peerConn) (*testCluster, *Cluster, ring.Node, string) {
	t.Helper()
	tc, buckets, member, _ := pendingOnAMemberWithAutoRebalanceOff(t, 1, "deadline")
	memberCluster := clusterOf(tc, member)
	owner := mustNode(t, memberCluster, clusterOf(tc, 0).self.ID)
	wrapPeerConn(memberCluster.peerFor(owner), wrap)
	return tc, memberCluster, owner, buckets[0]
}

// TestAHungSourceEndsAMigratePullAtTheDeadline is the operator path:
// migrate's pull (pullBucketPlaced) of a bucket from a source that never
// answers MIGRATE_START ends with an error naming the source and the wait.
func TestAHungSourceEndsAMigratePullAtTheDeadline(t *testing.T) {
	shortenPullTimeouts(t, 500*time.Millisecond, 500*time.Millisecond)
	tc, member, owner, bucket := pendingFromOwner(t, func(inner peerConn) peerConn {
		hung := newHungSourceConn("", -1, transport.OpMigrateStart)
		hung.peerConn = inner
		return hung
	})
	defer tc.stop()

	err := returnsWithin(t, "a migrate pull", 10*time.Second, func(ctx context.Context) error {
		return member.pullBucketPlaced(ctx, testDatabase, bucket, owner, true)
	})
	if err == nil {
		t.Fatal("a pull from a source that never answers reported success")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), owner.ID) {
		t.Errorf("the hung pull ended with %q, want a deadline error that names source %s", err, owner.ID)
	}
}

// TestAHungHolderEndsATakeOverPullAtTheDeadline is the take-over worker's
// path (pullFromHolder), which shares the pull.
func TestAHungHolderEndsATakeOverPullAtTheDeadline(t *testing.T) {
	shortenPullTimeouts(t, 500*time.Millisecond, 500*time.Millisecond)
	tc, member, owner, bucket := pendingFromOwner(t, func(inner peerConn) peerConn {
		hung := newHungSourceConn("", -1, transport.OpMigrateStart)
		hung.peerConn = inner
		return hung
	})
	defer tc.stop()

	err := returnsWithin(t, "a take-over pull", 10*time.Second, func(ctx context.Context) error {
		return member.pullFromHolder(ctx, heldBucket{database: testDatabase, bucket: bucket}, heldCopy{node: owner}, nil)
	})
	if err == nil {
		t.Fatal("a take-over pull from a holder that never answers reported success")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), owner.ID) {
		t.Errorf("the hung take-over pull ended with %q, want a deadline error that names holder %s", err, owner.ID)
	}
}

// TestASlowButProgressingSourceIsNotCutOff is the control: the deadline is on
// each request, not on the pull. The source answers each of the pull's three
// requests after 0.6 of the deadline, so the pull as a whole takes longer than
// the deadline and still lands.
func TestASlowButProgressingSourceIsNotCutOff(t *testing.T) {
	const deadline = time.Second
	shortenPullTimeouts(t, deadline, deadline)
	tc, member, owner, bucket := pendingFromOwner(t, func(inner peerConn) peerConn {
		return &slowSourceConn{peerConn: inner, delay: 600 * time.Millisecond}
	})
	defer tc.stop()

	began := time.Now()
	err := returnsWithin(t, "a slow pull", 30*time.Second, func(ctx context.Context) error {
		return member.pullBucketPlaced(ctx, testDatabase, bucket, owner, true)
	})
	if err != nil {
		t.Fatalf("a slow source that answered every request within the deadline was cut off: %v", err)
	}
	if took := time.Since(began); took <= deadline {
		t.Fatalf("test setup: the pull took %v, no longer than the %v deadline, so it does not show the deadline is per request", took, deadline)
	}
	if !member.s.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the slow pull of %q returned without landing the bucket", bucket)
	}
}

// TestAHungSourceDoesNotHoldThePendingCopyWorker is the reproduction of the
// bug file: the first bucket's owner takes the MIGRATE_START and never answers
// it, and a second bucket pending behind it must still be pulled. The hung
// bucket's attempt ends with the source and the cause in its outcome and is
// tried again on the worker's backoff, when the source answers.
func TestAHungSourceDoesNotHoldThePendingCopyWorker(t *testing.T) {
	shortenPullTimeouts(t, time.Second, time.Second)
	tc, buckets, member := rebalancingPendingOn(t, 2, "hungworker")
	defer tc.stop()
	memberCluster := clusterOf(tc, member)
	owner := mustNode(t, memberCluster, clusterOf(tc, 0).self.ID)
	hung := newHungSourceConn(buckets[0], 1, transport.OpMigrateStart)
	wrapPeerConn(memberCluster.peerFor(owner), func(inner peerConn) peerConn {
		hung.peerConn = inner
		return hung
	})

	raiseToThree(t, tc, buckets[0], member)
	select {
	case <-hung.entered:
	case <-time.After(15 * time.Second):
		t.Fatalf("node %d's worker never asked the owner for %q", member+1, buckets[0])
	}
	raiseToThree(t, tc, buckets[1], member)
	if !holdsWithin(15*time.Second, func() bool { return tc.nodes[member].server.core.bucketStoreOpen(testDatabase, buckets[1]) }) {
		t.Fatalf("node %d's copy of %q waited behind %q, whose owner never answered", member+1, buckets[1], buckets[0])
	}

	key := placementOf(testDatabase, buckets[0])
	pollUntil(t, fmt.Sprintf("node %d's outcome for %q names the hung owner", member+1, buckets[0]), func() bool {
		outcome, pending := memberCluster.pendingCopyOf(key)
		return pending && strings.Contains(outcome.reason, owner.ID) && strings.Contains(outcome.reason, "deadline")
	})
	waitForPendingCopyLanded(t, tc, member, buckets[0])
}

// TestASlowStartWithinItsOwnBoundStillLands: MIGRATE_START is answered only
// after the source has built and hashed the whole snapshot, which for a large
// bucket takes far longer than a chunk, so it has its own, larger bound. Here
// the source answers it after twice the data deadline and inside its own, and
// the pull lands.
func TestASlowStartWithinItsOwnBoundStillLands(t *testing.T) {
	const dataDeadline = 500 * time.Millisecond
	shortenPullTimeouts(t, dataDeadline, 10*time.Second)
	tc, member, owner, bucket := pendingFromOwner(t, func(inner peerConn) peerConn {
		return &slowSourceConn{peerConn: inner, delay: 2 * dataDeadline, only: map[transport.Opcode]bool{transport.OpMigrateStart: true}}
	})
	defer tc.stop()

	if err := returnsWithin(t, "a pull with a slow start", 30*time.Second, func(ctx context.Context) error {
		return member.pullBucketPlaced(ctx, testDatabase, bucket, owner, true)
	}); err != nil {
		t.Fatalf("a source that answered MIGRATE_START after %v, inside its own bound, was cut off at the data deadline: %v", 2*dataDeadline, err)
	}
	if !member.s.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the pull of %q returned without landing the bucket", bucket)
	}
}

// TestAHungStartPastItsOwnBoundIsCutOff: a source that never answers
// MIGRATE_START is cut off at the start bound, not before it and not never,
// and the error names the source and the request.
func TestAHungStartPastItsOwnBoundIsCutOff(t *testing.T) {
	const startBound = time.Second
	shortenPullTimeouts(t, startBound/4, startBound)
	tc, member, owner, bucket := pendingFromOwner(t, func(inner peerConn) peerConn {
		hung := newHungSourceConn("", -1, transport.OpMigrateStart)
		hung.peerConn = inner
		return hung
	})
	defer tc.stop()

	began := time.Now()
	err := returnsWithin(t, "a pull with a hung start", 10*time.Second, func(ctx context.Context) error {
		return member.pullBucketPlaced(ctx, testDatabase, bucket, owner, true)
	})
	if took := time.Since(began); took < startBound {
		t.Errorf("a hung MIGRATE_START was cut off after %v, before its own bound of %v", took, startBound)
	}
	if err == nil || !errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), owner.ID) || !strings.Contains(err.Error(), "MIGRATE_START") {
		t.Errorf("the hung start ended with %v, want a deadline error naming source %s and MIGRATE_START", err, owner.ID)
	}
}
