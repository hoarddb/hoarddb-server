package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The alter's refusals (bucket-replication-rev8-spec §4.3; build doc steps
// 2.5 to 2.7). Every refusal is judged before the entry is logged, so a
// refused alter is in no node's log. The CLI's exit class is the fixed mapping
// of the wire code (cli/classify.go: UNAVAILABLE exits ExitUnavailable, every
// other code and a success: false body ExitRefused); the server package
// cannot import cli, so the code is what these tests assert.

// alterRefusalBody is the message of an alter refused with a success: false
// body; an error frame or an accepted alter fails the test.
func alterRefusalBody(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	if frame.Opcode == transport.OpError {
		t.Fatalf("the alter was refused with an error frame, want a success: false body: %s", frame.Payload)
	}
	var answer AlterBucketResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the alter's answer: %v", err)
	}
	if answer.Success {
		t.Fatalf("the alter was accepted, want a refusal: %+v", answer)
	}
	return answer.Message
}

// requireUnavailableNotApplied fails unless frame is an UNAVAILABLE error with
// write_outcome not_applied whose message holds every want.
func requireUnavailableNotApplied(t *testing.T, frame *transport.Frame, want ...string) ErrorResponse {
	t.Helper()
	refused := refusalOf(t, frame)
	if refused.Code != int(transport.ErrUnavailable) {
		t.Errorf("code %d, want UNAVAILABLE (%d): %q", refused.Code, transport.ErrUnavailable, refused.Message)
	}
	if refused.WriteOutcome != outcomeNotApplied {
		t.Errorf("write_outcome %q, want %q: %q", refused.WriteOutcome, outcomeNotApplied, refused.Message)
	}
	for _, text := range want {
		if !strings.Contains(refused.Message, text) {
			t.Errorf("the refusal does not say %q: %q", text, refused.Message)
		}
	}
	return refused
}

// requireNoAlterLogged fails for any alter of bucket in any running node's log.
func requireNoAlterLogged(t *testing.T, tc *testCluster, bucket string) {
	t.Helper()
	for index, node := range tc.nodes {
		if node.server == nil {
			continue
		}
		if logged := walOpCount(t, node.server.core.wal, transport.OpAlterBucket, bucket); logged != 0 {
			t.Errorf("node %d logged %d refused alters of bucket %q", index+1, logged, bucket)
		}
	}
}

// createRemedies are the create's own remedies: an alter refused in them tells
// the user to do something the alter is not.
var createRemedies = []string{"leave replication_factor out", "create the bucket", "or create", "asks for replication_factor"}

// requireAlterWords fails if message is the create's refusal rather than the
// alter's (spec §4.3, B F-4).
func requireAlterWords(t *testing.T, message string, want ...string) {
	t.Helper()
	for _, remedy := range createRemedies {
		if strings.Contains(message, remedy) {
			t.Errorf("the alter's refusal carries the create's remedy %q: %q", remedy, message)
		}
	}
	for _, text := range want {
		if !strings.Contains(message, text) {
			t.Errorf("the alter's refusal does not say %q: %q", text, message)
		}
	}
}

// markStale makes c believe member gossiped without bucket_rf_alter: what an
// older build looks like.
func markStale(c *Cluster, member string) {
	c.featuresMu.Lock()
	defer c.featuresMu.Unlock()
	c.memberFeatures[member] = map[string]bool{featureBucketRF: true, featureMemtableMB: true, featureTypedFields: true, featurePackedVersions: true}
}

// waitForAlterFeature waits until every running node has heard every other
// advertise bucket_rf_alter.
func waitForAlterFeature(t *testing.T, tc *testCluster) {
	t.Helper()
	pollUntil(t, "every node hears every other advertise bucket_rf_alter", func() bool {
		for _, node := range tc.nodes {
			c := node.server.core.cluster
			c.featuresMu.RLock()
			for _, member := range c.Members() {
				if member.ID != c.self.ID && !c.memberFeatures[member.ID][featureBucketRFAlter] {
					c.featuresMu.RUnlock()
					return false
				}
			}
			c.featuresMu.RUnlock()
		}
		return true
	})
}

// TestAnAlterIsRefusedWhileAMemberLacksTheFeature is T-165 (spec §4.3 R4,
// §11.2). The coordinator case: only the coordinator has heard a member gossip
// without bucket_rf_alter, so only its own check can refuse; with the check
// at the owner alone the alter would be logged. The owner case: a new owner
// with an older member refuses before the append. Nothing here claims the
// owner's half protects against an older owner (BUG-0561). The third node is
// stopped first so gossip does not restore what the test made the others
// believe; the switch makes every in-process node advertise the feature.
func TestAnAlterIsRefusedWhileAMemberLacksTheFeature(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	stale := tc.addrs[2]
	stopNode(t, tc, 2)

	t.Run("at the coordinator", func(t *testing.T) {
		markStale(tc.nodes[1].server.core.cluster, stale)
		frame := alterFrame(t, tc, 1, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
		requireUnavailableNotApplied(t, frame, stale, "does not change factors", "Upgrade "+stale)
		requireNoAlterLogged(t, tc, bucket)
	})
	t.Run("at the owner", func(t *testing.T) {
		markStale(tc.nodes[0].server.core.cluster, stale)
		frame := alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
		requireUnavailableNotApplied(t, frame, stale, "does not change factors")
		requireNoAlterLogged(t, tc, bucket)
	})
}

// TestEveryAlterRefusalTravelsInItsWireClass is T-184's rows for R1 to R6, R11
// and R12 (spec §4.3, B F-3, B F-4); R7 and R8 have their own tests. R2 and R3
// are a success: false body in the alter's own words, R4 to R6 and R11
// UNAVAILABLE and not_applied, R1 NOT_FOUND, R12 the plaintext refusal it has
// always been. R10 has no row: G5 removed it (build doc step 5), and no lower
// factor is refused with a body.
func TestEveryAlterRefusalTravelsInItsWireClass(t *testing.T) {

	t.Run("R2 standalone", func(t *testing.T) {
		s := newTestServer(t)
		if created, err := s.CreateDatabase(testDatabase); err != nil || !created.Success {
			t.Fatalf("creating the database: %+v, %v", created, err)
		}
		if created, err := s.CreateBucket(CreateBucketRequest{Database: testDatabase, Name: "Sessions", Type: BucketTypeHash}); err != nil || !created.Success {
			t.Fatalf("creating the bucket: %+v, %v", created, err)
		}
		payload, err := bson.Marshal(AlterBucketRequest{Database: testDatabase, Name: "Sessions", ReplicationFactor: 3})
		if err != nil {
			t.Fatalf("encoding the alter: %v", err)
		}
		frame, err := s.fencedDispatch(context.Background(), transport.NewFrame(transport.OpAlterBucket, payload))
		if err != nil {
			t.Fatalf("dispatching the alter: %v", err)
		}
		requireAlterWords(t, alterRefusalBody(t, frame), `bucket "Sessions" cannot be raised to replication_factor 3`, "running standalone",
			"then run the alter once 'nodes' shows the members")
		if facts, _ := s.peekSettled(testDatabase, "Sessions"); facts.replicationFactor != 0 {
			t.Errorf("the refused alter changed the bucket's factor to %d", facts.replicationFactor)
		}
	})

	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	t.Run("R1 not found", func(t *testing.T) {
		dropped := bucketOwnedBy(t, tc, 0, "dropped")
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: dropped, Type: BucketTypeHash})
		tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: dropped})
		refused := refusalOf(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: dropped, ReplicationFactor: 2}))
		if refused.Code != int(transport.ErrNotFound) {
			t.Errorf("an alter of a dropped bucket: code %d, want NOT_FOUND (%d): %q", refused.Code, transport.ErrNotFound, refused.Message)
		}
		requireNoAlterLogged(t, tc, dropped)
	})
	t.Run("R3 above the member count", func(t *testing.T) {
		message := alterRefusalBody(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 5}))
		requireAlterWords(t, message, "replication_factor 5 is larger than the cluster's 3 members", "its 4th and 5th copies",
			"or use replication_factor 3")
		requireNoAlterLogged(t, tc, bucket)
	})
	t.Run("R4 a member without the feature", func(t *testing.T) {
		owner := tc.nodes[0].server.core.cluster
		markStale(owner, tc.addrs[2])
		defer owner.recordMemberFeatures(tc.addrs[2], clusterFeatures())
		requireUnavailableNotApplied(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}),
			"does not change factors", "Upgrade "+tc.addrs[2])
		requireNoAlterLogged(t, tc, bucket)
	})
	t.Run("R6 the owner has not taken the bucket over", func(t *testing.T) {
		owner := tc.nodes[0].server.core
		rec := owner.lockServedRecord(testDatabase, bucket)
		term := max(rec.term.Load(), 1)
		rec.setLineageLocked(term, false)
		rec.mu.Unlock()
		frame := alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
		requireUnavailableNotApplied(t, frame, "is moving to this node")
		requireNoAlterLogged(t, tc, bucket)
	})
	t.Run("R11 a decrease its survivors do not cover", func(t *testing.T) {
		// The owner is a survivor of every decrease (T-179's case): its copy
		// owes an apply its store failed, so it is not shown to hold what the
		// demoted copies hold, and the decrease to 1 is refused naming it.
		lowered := bucketOwnedBy(t, tc, 0, "lowered")
		createBucketAt(tc, 0, testDatabase, lowered, 3)
		waitForServing(t, tc, testDatabase, lowered, 3)
		owner := tc.nodes[0].server.core
		failNextApplyOn(t, owner.kv, anySeq)
		tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: lowered, Key: "failed", Data: map[string]interface{}{"n": 1}})
		store.ApplyFaultHook.Store(nil)
		putDocument(tc, 0, testDatabase, lowered, "after")
		if ownerCopy, err := owner.cluster.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: lowered}); err != nil || !ownerCopy.Owes {
			t.Fatalf("setup: the owner's copy is %+v (%v), want it owing the apply its store failed", ownerCopy, err)
		}
		requireUnavailableNotApplied(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: lowered, ReplicationFactor: 1}),
			fmt.Sprintf("bucket %q cannot be lowered from replication_factor 3 to 1: %s has not shown that it holds everything", lowered, tc.addrs[0]),
			fmt.Sprintf("Run 'show buckets' on %s and retry when the row reads 3/3.", tc.addrs[0]))
		requireNoAlterLogged(t, tc, lowered)
	})
}

// TestAnAlterIsRefusedAtThePlaintextMemberItWouldPlaceTheBucketOn is T-184's
// R12 row (spec §4.3): on a sealed owner, an increase that would place the
// bucket on a plaintext member is refused before it is logged, judged at the
// factor it asks for. A client's previous_factor must not widen the check: an
// alter to a factor whose set holds no plaintext member is accepted, whatever
// previous_factor the client sent (build doc §2 trap 3).
func TestAnAlterIsRefusedAtThePlaintextMemberItWouldPlaceTheBucketOn(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "on", "off")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	plaintext := tc.addrs[2]
	// The bucket's set at 2 is the two sealed nodes, whichever owns it: at 2
	// its set holds no plaintext member, at 3 it does.
	bucket, ownerID := bucketPlacedOn(t, tc.nodes[0].server.core.cluster, "sealed", tc.addrs[0], tc.addrs[1])
	owner := nodeIndexByAddr(t, tc.addrs, ownerID)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	frame := alterFrame(t, tc, owner, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
	refused := refusalOf(t, frame)
	if refused.WriteOutcome != outcomeNotApplied {
		t.Errorf("the plaintext refusal's write_outcome is %q, want %q", refused.WriteOutcome, outcomeNotApplied)
	}
	assertPlaintextPeerRefusal(t, "an increase onto a plaintext member", refused.Message, plaintext)
	requireNoAlterLogged(t, tc, bucket)

	accepted := alterAccepted(t, alterFrame(t, tc, owner, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2, PreviousFactor: 3}))
	if accepted.ReplicationFactor != 2 {
		t.Errorf("the alter to 2 was answered at %d", accepted.ReplicationFactor)
	}
}

// bucketPlacedOn names a bucket of testDatabase whose set at len(nodes) on
// c's ring is exactly nodes, in any order, and its owner.
func bucketPlacedOn(t *testing.T, c *Cluster, prefix string, nodes ...string) (string, string) {
	t.Helper()
	want := slices.Sorted(slices.Values(nodes))
	for attempt := 0; attempt < 2048; attempt++ {
		name := fmt.Sprintf("%s%03d", prefix, attempt)
		set, err := c.replicasAtFactor(placementOf(testDatabase, name), len(nodes))
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		placed := nodeIDs(set)
		slices.Sort(placed)
		if slices.Equal(placed, want) {
			return name, set[0].ID
		}
	}
	t.Fatalf("no bucket %s000.. is placed on %v", prefix, nodes)
	return "", ""
}

// TestAnAlterIsRefusedWhileANodeJoins is T-161 (spec §4.3 R5, Q5): while a
// join settles, an alter is refused UNAVAILABLE, not_applied, naming the
// joiner, through the joiner and through a member, nothing of it is logged, and
// the refusal says nothing was altered. The client-sent join-freeze table
// (schemaRequests) carries the alter too.
func TestAnAlterIsRefusedWhileANodeJoins(t *testing.T) {
	s := setUpSettlingJoin(t)
	defer s.tc.stop()
	defer s.release()
	before := s.lastSequences()
	joiner := s.ports.ringAddr
	for _, path := range []struct {
		name    string
		cluster *Cluster
		bucket  string
	}{
		{"a member, about a bucket a member owns", clusterOf(s.tc, 1), s.memberOwned[0]},
		{"the joiner, about a bucket it owns", s.joiner.server.core.cluster, s.joinerOwned[0]},
	} {
		t.Run(path.name, func(t *testing.T) {
			answered := sendInBackground(t, path.cluster, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: path.bucket, ReplicationFactor: 2})
			select {
			case answer := <-answered:
				if answer.err != nil {
					t.Fatalf("the alter failed: %v", answer.err)
				}
				requirePausedRefusal(t, answer.frame, "an alter", joiner)
				if refused := refusalOf(t, answer.frame); !strings.Contains(refused.Message, "Nothing was created, dropped, altered or restored") {
					t.Errorf("the freeze's refusal does not say nothing was altered: %q", refused.Message)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the alter was never answered while the node joins")
			}
		})
	}
	s.requireNoSchemaEntrySince(t, before)
}
