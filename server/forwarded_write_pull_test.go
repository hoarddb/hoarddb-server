package server

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Regression: a forwarded write's unique pre-check races a pull that is creating
// the bucket on the owner.

// joinerOwningAPulledBucket is a joiner that owns a hash bucket declaring a
// unique index on Email, holds none of it yet, and has not started pulling it.
// The bucket's previous owner holds a document with Email "taken"; its stream
// to the joiner is held, so no replay creates the bucket there, and neither
// node rebalances on its own, so the test's pull is the only one.
type joinerOwningAPulledBucket struct {
	tc     *testCluster
	joiner *Cluster
	bucket string
	// coordinator is a member that is not the joiner, which forwards writes.
	// It owned the bucket before the join: it is the previous owner.
	coordinator string
}

func setUpJoinerOwningAPulledBucket(t *testing.T) joinerOwningAPulledBucket {
	t.Helper()
	return setUpJoinerOwningAPulledBucketWith(t, false)
}

// setUpJoinerOwningAPulledBucketWith is setUpJoinerOwningAPulledBucket;
// knowsPreviousOwner has the joiner adopt its join by gossip, whose answer
// carries the cluster's record of the first epoch (on-demand pull build doc
// step 9), so its take-over knows the bucket's previous owner. Otherwise the
// joiner adopts with no history and knows none.
func setUpJoinerOwningAPulledBucketWith(t *testing.T, knowsPreviousOwner bool) joinerOwningAPulledBucket {
	t.Helper()
	tc := newTestClusterWith(t, 2, 3, func(_ int, cfg *config.Config) { noSweepNoRetention(cfg) })
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	members := append([]ring.Node{}, tc.nodes[0].server.core.cluster.Members()...)
	after := ringOf(t, append(members, ports.ringNode()), 3)
	bucket := ""
	for i := 0; i < 1000 && bucket == ""; i++ {
		name := fmt.Sprintf("prechecked%03d", i)
		if owner, err := after.Owner(placementOf(testDatabase, name).encoded); err == nil && owner.ID == ports.ringAddr {
			bucket = name
		}
	}
	if bucket == "" {
		t.Fatal("setup: no bucket name the joiner would own")
	}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{map[string]interface{}{"field": "Email", "unique": true}}}})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "holder",
		Data: map[string]interface{}{"Email": "taken"}, WriteConcern: "majority"})
	ownerID := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, bucket)[0]
	owner := tc.nodes[nodeIndexByAddr(t, tc.addrs, ownerID)].server.core.cluster

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := owner.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	// Every founder's stream to the joiner is held, not the owner's alone: an
	// entry of another founder's stream (the database's create) that lands
	// late would otherwise move the joiner's log under the test's checks.
	for _, node := range tc.nodes {
		t.Cleanup(holdStreamEntries(node.server.core.cluster.peerFor(ports.ringNode())))
	}
	// Without history the joiner does not know the bucket's previous owner, and
	// a forwarded write meets the join fence's awaiting_copy; one that knows it
	// answers handoff_in_progress, naming it (owner ruling R9 pins both).
	if !knowsPreviousOwner {
		skipGossipOf(t, ports.ringAddr)
	}
	joinerNode := startJoinTarget(t, tc, ports, tc.token, noSweepNoRetention)
	t.Cleanup(func() { _ = joinerNode.client.Close() })
	joiner := joinerNode.server.core.cluster
	if knowsPreviousOwner {
		adoptedByGossip(t, joiner, owner.Epoch())
		if _, recorded := joiner.membersAt(InitialEpoch); !recorded {
			t.Fatalf("setup: the joiner adopted its join without the cluster's record of epoch %d", InitialEpoch)
		}
	} else {
		adoptFrom(t, joiner, owner)
	}
	if !joiner.actsAsOwner(placementOf(testDatabase, bucket)) {
		t.Fatalf("setup: the joiner does not act as the owner of %s", bucket)
	}
	if joiner.s.bucketStoreOpen(testDatabase, bucket) {
		t.Fatalf("setup: the joiner holds %s before any pull", bucket)
	}
	return joinerOwningAPulledBucket{tc: tc, joiner: joiner, bucket: bucket, coordinator: ownerID}
}

// forwardDuplicate sends the joiner a forwarded write, from the coordinator,
// of a document whose Email another document already holds. It takes no
// *testing.T: a test runs it on a goroutine of its own.
func (s joinerOwningAPulledBucket) forwardDuplicate() (ReplPushResponse, error) {
	insert, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "duplicate",
		Data: map[string]interface{}{"Email": "taken"}})
	if err != nil {
		return ReplPushResponse{}, fmt.Errorf("encoding the duplicate: %w", err)
	}
	push, err := bson.Marshal(ReplPushRequest{
		Token:          s.joiner.localToken(),
		FromNode:       s.coordinator,
		ReturnResponse: true,
		Epoch:          s.joiner.Epoch(),
		Entries:        []ReplEntry{{Op: int32(transport.OpPut), Bucket: s.bucket, Payload: insert}},
	})
	if err != nil {
		return ReplPushResponse{}, fmt.Errorf("encoding the forwarded write: %w", err)
	}
	answer, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), s.joiner.s)
	if err != nil {
		return ReplPushResponse{}, fmt.Errorf("forwarding the duplicate: %w", err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(answer.Payload, &resp); err != nil {
		return ReplPushResponse{}, fmt.Errorf("decoding the answer to the forwarded write: %w", err)
	}
	return resp, nil
}

// pull runs the joiner's pull of the bucket and returns its outcome.
func (s joinerOwningAPulledBucket) pull() <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		done <- s.joiner.ensureMigrated(ctx, testDatabase, s.bucket)
	}()
	return done
}

// TestAForwardedWriteDoesNotRaceAPullOfItsBucket: A forwarded write
// reaches the owner through handleReplPush, which, unlike a client's write,
// had no join fence: its unique pre-check read the bucket's type and indexes
// with no lock while the pull's prepareBucketLocked wrote them (the race T-97
// reported), and a write the pre-check let through was logged — and shipped —
// although the owner held no copy to apply it to. The pre-check exists so that
// a duplicate is never logged. A forwarded write to a bucket whose pull is in
// flight is now answered "not yet" as a client's is, and one that arrives
// while the copy is being put in place waits for it, then is pre-checked
// against the whole bucket.
func TestAForwardedWriteDoesNotRaceAPullOfItsBucket(t *testing.T) {
	t.Run("the pull is held before its materialise", func(t *testing.T) {
		s := setUpJoinerOwningAPulledBucket(t)
		held, release := make(chan struct{}), make(chan struct{})
		var once atomic.Bool
		hold := func(string, string) {
			if once.CompareAndSwap(false, true) {
				close(held)
				<-release
			}
		}
		beforeMaterializeHook.Store(&hold)
		defer beforeMaterializeHook.Store(nil)
		done := s.pull()
		select {
		case <-held:
		case err := <-done:
			t.Fatalf("the pull ended before its materialise: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the pull never reached its materialise")
		}

		logged := s.joiner.s.wal.LastSequence()
		resp, err := s.forwardDuplicate()
		if err != nil {
			t.Fatal(err)
		}
		if resp.Success || resp.Refusal != refusalAwaitingCopy {
			t.Errorf("a forwarded write to a bucket whose pull is in flight was answered success=%t refusal=%q error=%q; want %q",
				resp.Success, resp.Refusal, resp.Error, refusalAwaitingCopy)
		}
		if now := s.joiner.s.wal.LastSequence(); now != logged {
			t.Errorf("the joiner's log went from %d to %d for a forwarded write to a bucket it holds no copy of", logged, now)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("the pull, released: %v", err)
		}
	})

	// Owner ruling R9 (2026-10-21): a joiner that knows the bucket's previous
	// owner, with the pull held, refuses the forwarded write naming that owner,
	// stores nothing, and logs nothing.
	t.Run("the pull is held before its materialise, at a joiner that knows its previous owner", func(t *testing.T) {
		s := setUpJoinerOwningAPulledBucketWith(t, true)
		held, release := make(chan struct{}), make(chan struct{})
		var once atomic.Bool
		hold := func(string, string) {
			if once.CompareAndSwap(false, true) {
				close(held)
				<-release
			}
		}
		beforeMaterializeHook.Store(&hold)
		defer beforeMaterializeHook.Store(nil)
		done := s.pull()
		select {
		case <-held:
		case err := <-done:
			t.Fatalf("the pull ended before its materialise: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the pull never reached its materialise")
		}
		if status := s.joiner.takeOverStatusOf(placementOf(testDatabase, s.bucket)); !status.sourceKnown || status.source.ID != s.coordinator {
			t.Fatalf("setup: the joiner's take-over does not name %s as the previous owner: %+v", s.coordinator, status)
		}

		logged := s.joiner.s.wal.LastSequence()
		resp, err := s.forwardDuplicate()
		if err != nil {
			t.Fatal(err)
		}
		if resp.Success || resp.Refusal != refusalHandoffInProgress || !strings.Contains(resp.Error, s.coordinator) {
			t.Errorf("a forwarded write to a bucket whose pull is in flight, at a joiner that knows its previous owner, was answered "+
				"success=%t refusal=%q error=%q; want %q naming %s", resp.Success, resp.Refusal, resp.Error, refusalHandoffInProgress, s.coordinator)
		}
		if resp.WriteOutcome != outcomeNotApplied {
			t.Errorf("the refusal says write_outcome %q, want %q", resp.WriteOutcome, outcomeNotApplied)
		}
		if now := s.joiner.s.wal.LastSequence(); now != logged {
			t.Errorf("the joiner's log went from %d to %d for a forwarded write to a bucket it holds no copy of", logged, now)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("the pull, released: %v", err)
		}
	})

	t.Run("the pull is held during its materialise", func(t *testing.T) {
		s := setUpJoinerOwningAPulledBucket(t)
		held, release := make(chan struct{}), make(chan struct{})
		var once atomic.Bool
		hold := func(uint64) error {
			if once.CompareAndSwap(false, true) {
				close(held)
				<-release
			}
			return nil
		}
		materializeCatalogHook.Store(&hold)
		defer materializeCatalogHook.Store(nil)
		done := s.pull()
		select {
		case <-held:
		case err := <-done:
			t.Fatalf("the pull ended before its catalog commit: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the pull never reached its catalog commit")
		}

		// The bucket's type and indexes are prepared and its copy is not yet
		// served. A write that is let through here is logged at once; one that
		// waits for the copy logs nothing until the pull is released.
		logged := s.joiner.s.wal.LastSequence()
		type answer struct {
			resp ReplPushResponse
			err  error
		}
		answered := make(chan answer, 1)
		go func() {
			resp, err := s.forwardDuplicate()
			answered <- answer{resp, err}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && s.joiner.s.wal.LastSequence() == logged {
			time.Sleep(10 * time.Millisecond)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("the pull, released: %v", err)
		}
		var resp ReplPushResponse
		select {
		case got := <-answered:
			if got.err != nil {
				t.Fatal(got.err)
			}
			resp = got.resp
		case <-time.After(30 * time.Second):
			t.Fatal("the forwarded write was never answered once the pull landed")
		}
		if !resp.Success || len(resp.Responses) != 1 || resp.Responses[0].Opcode != int32(transport.OpError) {
			t.Errorf("a forwarded duplicate sent during the materialise was answered success=%t responses=%d error=%q; "+
				"want it refused by the unique pre-check", resp.Success, len(resp.Responses), resp.Error)
		}
		if now := s.joiner.s.wal.LastSequence(); now != logged {
			t.Errorf("the joiner's log went from %d to %d for a forwarded duplicate: the pre-check, reading a bucket the pull "+
				"had not put in place, let it through", logged, now)
		}
	})
}
