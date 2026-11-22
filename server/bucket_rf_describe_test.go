package server

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// What `describe bucket` says of a bucket's copies (bucket-replication-rev8-spec
// §12.2; the G4 to G7 build doc step 10): the owner's counts, however far the
// node the client asked is from the owner.

// describeAt asks node index to describe database.bucket and decodes the answer.
func describeAt(t *testing.T, tc *testCluster, index int, database, bucket string) BucketStatsResponse {
	t.Helper()
	frame := tc.ok(index, transport.OpBucketStats, BucketStatsRequest{Database: database, Bucket: bucket})
	var described BucketStatsResponse
	if err := bson.Unmarshal(frame.Payload, &described); err != nil {
		t.Fatalf("decoding node %d's description of %q: %v", index+1, bucket, err)
	}
	return described
}

// recordDescribeForwards records, until the test ends, every node a describe
// is forwarded to.
func recordDescribeForwards(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var targets []string
	hook := func(entry ReplEntry, targetID string) {
		if transport.Opcode(entry.Op) != transport.OpBucketStats {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		targets = append(targets, targetID)
	}
	previous := forwardTargetHook.Swap(&hook)
	t.Cleanup(func() { forwardTargetHook.Store(previous) })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), targets...)
	}
}

// TestDescribeBucketOnANonOwnerReportsTheOwnersCount is T-42: a describe taken
// at a holder is forwarded to the owner and carries the owner's counts; with
// the owner unreachable it is answered from the holder's own copy and says so;
// while the owner takes the bucket over it is answered, not refused; and a
// member that holds a needs-a-copy record adds its own pending copy.
func TestDescribeBucketOnANonOwnerReportsTheOwnersCount(t *testing.T) {
	t.Run("a holder forwards to the owner", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		createBucketAt(tc, 0, testDatabase, testBucket, 0)
		waitForServing(t, tc, testDatabase, testBucket, 3)
		owner := ownerIndexIn(t, tc, testDatabase, testBucket)
		holder := (owner + 1) % 3
		pollUntil(t, "the owner counts three copies in sync", func() bool {
			return describeAt(t, tc, owner, testDatabase, testBucket).InSync == 3
		})
		forwarded := recordDescribeForwards(t)

		described := describeAt(t, tc, holder, testDatabase, testBucket)
		if targets := forwarded(); len(targets) != 1 || targets[0] != tc.addrs[owner] {
			t.Errorf("a describe at holder node %d was forwarded to %v, want only the owner, node %d", holder+1, targets, owner+1)
		}
		if described.State != StateOK || described.InSync != 3 {
			t.Errorf("a describe at holder node %d says %q with %d in sync, want the owner's view: ok, 3", holder+1, described.State, described.InSync)
		}
		if len(described.Nodes) != 3 || described.Nodes[0] != tc.addrs[owner] {
			t.Errorf("a describe at holder node %d names the set %v, want the three nodes, owner %s first", holder+1, described.Nodes, tc.addrs[owner])
		}
		if described.ReplicationFactor != 3 {
			t.Errorf("a describe at holder node %d says factor %d, want 3", holder+1, described.ReplicationFactor)
		}
	})

	t.Run("an unreachable owner is said, not an error", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		createBucketAt(tc, 0, testDatabase, testBucket, 0)
		waitForServing(t, tc, testDatabase, testBucket, 3)
		owner := ownerIndexIn(t, tc, testDatabase, testBucket)
		holder := (owner + 1) % 3
		tc.stopNodeKeepingData(owner)

		described := describeAt(t, tc, holder, testDatabase, testBucket)
		if described.State != StateOwnerUnreachable {
			t.Errorf("a describe at holder node %d with the owner down says %q, want %q", holder+1, described.State, StateOwnerUnreachable)
		}
		if !described.Found || described.Type != BucketTypeHash || described.ReplicationFactor != 3 {
			t.Errorf("a describe at holder node %d with the owner down answers %+v, want the holder's own copy", holder+1, described)
		}
		if described.InSync != 0 {
			t.Errorf("a describe at holder node %d with the owner down counts %d in sync, want none: the count is the owner's", holder+1, described.InSync)
		}
	})

	t.Run("a bucket being taken over is described, not refused", func(t *testing.T) {
		takeOverOffHook.Store(true)
		defer takeOverOffHook.Store(false)
		m, stop := unmarkedMovedBucket(t)
		defer stop()
		// A read is refused, as before: only the describe is carved out.
		assertHandoffRefusal(t, "a count at the taking-over owner", m.tc.frame(m.newOwner, transport.OpCount,
			CountRequest{Database: testDatabase, Bucket: testBucket}))
		c := clusterOf(m.tc, m.newOwner)
		c.setTakeOverStatus(m.key, takeOverStatus{source: mustNode(t, c, m.tc.addrs[m.survivor]), sourceKnown: true})

		described := describeAt(t, m.tc, m.newOwner, testDatabase, testBucket)
		if described.State != StateTakingOver {
			t.Errorf("a describe at the owner that has not taken the bucket over says %q, want %q", described.State, StateTakingOver)
		}
		if described.PreviousOwner != m.tc.addrs[m.survivor] {
			t.Errorf("a describe at the taking-over owner names the previous owner %q, want %q", described.PreviousOwner, m.tc.addrs[m.survivor])
		}
	})

	t.Run("a member with a record adds its pending copy", func(t *testing.T) {
		refusal := errors.New("the source refused the pull for this test")
		injectPendingCopyPulls(t, func(string, string) error { return refusal })
		tc, buckets, member := rebalancingPendingOn(t, 1, "described")
		defer tc.stop()
		bucket := buckets[0]
		core := tc.nodes[member].server.core
		raiseToThree(t, tc, bucket, member)
		pollUntil(t, "the member has tried its copy once", func() bool {
			outcome, pending := core.cluster.pendingCopyOf(placementOf(testDatabase, bucket))
			return pending && !outcome.lastAttempt.IsZero()
		})

		atMember := describeAt(t, tc, member, testDatabase, bucket)
		if !strings.HasPrefix(atMember.PendingCopy, "pending copy here: since ") || !strings.Contains(atMember.PendingCopy, refusal.Error()) {
			t.Errorf("a describe at node %d, which has a record for %q, carries the line %q, want its own pending copy and why", member+1, bucket, atMember.PendingCopy)
		}
		if atMember.State != StateCopying || len(atMember.PendingCopies) != 1 || atMember.PendingCopies[0].Node != tc.addrs[member] {
			t.Errorf("a describe at node %d says %q with pending %v, want the owner's view: copying, node %d", member+1, atMember.State, atMember.PendingCopies, member+1)
		}
		if atOwner := describeAt(t, tc, 0, testDatabase, bucket); atOwner.PendingCopy != "" {
			t.Errorf("a describe at the owner carries the member's line %q, want none: the line is the member's own", atOwner.PendingCopy)
		}
	})
}

// TestDescribeOnAnOwnerWithNoCopyKeepsItsRefusal is SA-15's pin: the carve-out
// is the take-over's alone. An owner still copying its bucket in refuses a
// describe, at the owner and through a holder, with the join fence's retry.
func TestDescribeOnAnOwnerWithNoCopyKeepsItsRefusal(t *testing.T) {
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	done, release := s.holdPull(t, s.hashBucket)
	defer release()

	request := BucketStatsRequest{Database: testDatabase, Bucket: s.hashBucket}
	refusedAwaitingCopy(t, "a describe at the owner", s.tc.frameOn(s.joinerNode, transport.OpBucketStats, request))
	refusedAwaitingCopy(t, "a describe through a holder", s.tc.frame(0, transport.OpBucketStats, request))
	awaitPull(t, done, release)
}

// TestThePendingCopyLineNamesTheOutcome pins the line's four facts: since when
// the record stands, the last attempt and why it did not land, and the next
// retry; and the shorter line before the first attempt.
func TestThePendingCopyLineNamesTheOutcome(t *testing.T) {
	since := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	attempt := since.Add(2 * time.Hour)
	cases := []struct {
		name    string
		outcome pendingCopyOutcome
		want    string
	}{
		{"after an attempt", pendingCopyOutcome{since: since, lastAttempt: attempt, nextRetry: attempt.Add(30 * time.Second), reason: "refused for this node's disk"},
			"pending copy here: since 2026-10-20T09:00:00Z, last attempt 2026-10-20T11:00:00Z: refused for this node's disk, next retry 2026-10-20T11:00:30Z"},
		{"before the first attempt", pendingCopyOutcome{since: since, nextRetry: since},
			"pending copy here: since 2026-10-20T09:00:00Z, not tried yet, next retry 2026-10-20T09:00:00Z"},
	}
	for _, tc := range cases {
		if got := pendingCopyLine(tc.outcome); got != tc.want {
			t.Errorf("%s: the line is %q, want %q", tc.name, got, tc.want)
		}
	}
}
