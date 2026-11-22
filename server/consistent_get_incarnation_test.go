package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 11.3 (dispatch 11): a consistent get reads each holder's lineage,
// compares only copies of the incarnation the owner serves, refuses while a
// member does not pack versions or a holder answers at another epoch, and
// answers without its owner only when the other copies agree.

// strayInTheSet is a name with two live incarnations at factor 3
// (setUpAStray): the stray's joiner owns it and serves I2, and its set holds
// one founder with I1 and one with I2.
type strayInTheSet struct {
	*strayCase
	owner                     *testNode
	firstHolder, strayHolder  int
	firstIncarnation, strayed uint64
}

func setUpStrayInTheSet(t *testing.T) strayInTheSet {
	t.Helper()
	s := setUpAStray(t, 4, 3, func(layout strayLayout) bool {
		inFirst := 0
		for _, member := range layout.joined[1:] {
			if containsID(layout.first, member) {
				inFirst++
			}
		}
		return inFirst == 1
	})
	set, err := s.stray.cluster().Replicas(placementOf(testDatabase, s.bucket))
	if err != nil {
		t.Fatalf("placing %s: %v", s.bucket, err)
	}
	stray := strayInTheSet{strayCase: s, owner: s.stray.node, strayHolder: s.replicas[0]}
	for _, member := range set[1:] {
		if index := nodeIndexByAddr(t, s.tc.addrs, member.ID); index != stray.strayHolder {
			stray.firstHolder = index
		}
	}
	stray.firstIncarnation = lineageOf(t, s.tc.nodes[stray.firstHolder].server.core, testDatabase, s.bucket).Incarnation
	stray.strayed = lineageOf(t, stray.owner.server.core, testDatabase, s.bucket).Incarnation
	epoch := s.stray.cluster().Epoch()
	pollUntil(t, "every member is at the stray's join", func() bool {
		for _, node := range s.tc.nodes {
			if node.server.core.cluster.Epoch() != epoch {
				return false
			}
		}
		return true
	})
	return stray
}

// logTails is each listed node's log tail.
func logTails(nodes ...*testNode) []uint64 {
	tails := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		tails = append(tails, node.server.core.cluster.walLastSequence())
	}
	return tails
}

// TestConsistentGetComparesOnlyCopiesOfTheOwnersIncarnation: a holder of
// another incarnation than the owner's is left out of the read. Its copy of
// i1 is not the answer and is not repaired from or over; the answer is the
// owner's incarnation's (no i1), and the message names the holder left out
// and both incarnations. Guard: compare across incarnations (the read
// repairs: the owner's log grows).
func TestConsistentGetComparesOnlyCopiesOfTheOwnersIncarnation(t *testing.T) {
	s := setUpStrayInTheSet(t)
	defer s.tc.stop()
	before := logTails(s.owner)
	resp := answeredConsistentGet(t, consistentGetFrame(t, s.tc.nodes[s.firstHolder], testDatabase, s.bucket, "i1"))
	if resp.Found || resp.Repaired {
		t.Errorf("i1, held only by a copy of another incarnation, answered found=%v repaired=%v", resp.Found, resp.Repaired)
	}
	for _, want := range []string{s.tc.addrs[s.firstHolder], fmt.Sprint(s.firstIncarnation), fmt.Sprint(s.strayed)} {
		if !strings.Contains(resp.Message, want) {
			t.Errorf("the message %q does not name %s", resp.Message, want)
		}
	}
	if after := logTails(s.owner); after[0] != before[0] {
		t.Errorf("the owner's log grew from %d to %d during the read: it repaired across incarnations", before[0], after[0])
	}
	requireIncarnationKept(t, s.tc.nodes[s.firstHolder], s.bucket, "i1", "i2", fmt.Sprintf("node %d", s.firstHolder+1))
}

// failingVersionedGetConn fails every versioned read sent on it while active.
type failingVersionedGetConn struct {
	peerConn
	active *atomic.Bool
}

func (f *failingVersionedGetConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpVersionedGet && f.active.Load() {
		return nil, errors.New("a test fails this versioned read")
	}
	return f.peerConn.SendRequest(ctx, lane, op, payload)
}

// failVersionedGets makes from's versioned reads of to fail until the
// returned function is called.
func failVersionedGets(t *testing.T, from *Cluster, to string) (restore func()) {
	t.Helper()
	var active atomic.Bool
	active.Store(true)
	wrapPeerConn(from.peerFor(mustNode(t, from, to)), func(inner peerConn) peerConn {
		return &failingVersionedGetConn{peerConn: inner, active: &active}
	})
	return func() { active.Store(false) }
}

// TestConsistentGetWithoutTheOwnerRefusesTwoIncarnations: the owner cannot be
// read, and the other two holders hold two incarnations: the read is refused,
// UNAVAILABLE, naming the owner and both incarnations, and no holder's log
// grows. Guard: take the first answer's incarnation (the read answers and
// repairs).
func TestConsistentGetWithoutTheOwnerRefusesTwoIncarnations(t *testing.T) {
	s := setUpStrayInTheSet(t)
	defer s.tc.stop()
	coordinator := s.tc.nodes[s.firstHolder]
	defer failVersionedGets(t, coordinator.server.core.cluster, s.owner.addr)()
	before := logTails(s.owner, coordinator, s.tc.nodes[s.strayHolder])
	requireConsistentGetRefused(t, consistentGetFrame(t, coordinator, testDatabase, s.bucket, "i1"), transport.ErrUnavailable,
		s.owner.addr, fmt.Sprint(s.firstIncarnation), fmt.Sprint(s.strayed))
	if after := logTails(s.owner, coordinator, s.tc.nodes[s.strayHolder]); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("the refused read logged a repair: the holders' logs went from %v to %v", before, after)
	}
}

// answerRewritingConn rewrites the versioned answers read through it while
// active.
type answerRewritingConn struct {
	peerConn
	active  *atomic.Bool
	rewrite func(answer bson.D) bson.D
}

func (a *answerRewritingConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	frame, err := a.peerConn.SendRequest(ctx, lane, op, payload)
	if err != nil || op != transport.OpVersionedGet || !a.active.Load() {
		return frame, err
	}
	var answer bson.D
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the answer to rewrite: %w", err)
	}
	rewritten, err := bson.Marshal(a.rewrite(answer))
	if err != nil {
		return nil, fmt.Errorf("encoding the rewritten answer: %w", err)
	}
	copied := *frame
	copied.Payload = rewritten
	return &copied, nil
}

// rewriteAnswers makes from's versioned reads of to come back rewritten until
// the returned function is called.
func rewriteAnswers(t *testing.T, from *Cluster, to string, rewrite func(bson.D) bson.D) (restore func()) {
	t.Helper()
	var active atomic.Bool
	active.Store(true)
	wrapPeerConn(from.peerFor(mustNode(t, from, to)), func(inner peerConn) peerConn {
		return &answerRewritingConn{peerConn: inner, active: &active, rewrite: rewrite}
	})
	return func() { active.Store(false) }
}

// withoutFields is answer with the named fields removed.
func withoutFields(answer bson.D, names ...string) bson.D {
	kept := answer[:0:0]
	for _, element := range answer {
		if !containsID(names, element.Key) {
			kept = append(kept, element)
		}
	}
	return kept
}

// TestConsistentGetIsRefusedWhileAMemberDoesNotPack: (a) the gossip gate — a
// member that gossiped without packed_versions is named in a refusal before
// any holder is read, and once it advertises the read answers; (b) the
// per-read gate — a member whose versioned answer lacks every field an older
// build does not send is named in a refusal that says to upgrade it, and its
// answer's incarnation 0 is never taken as the reference. Guards: no gossip
// gate ((a) answers); no Packs check ((b) is refused only by the epoch check,
// without the upgrade text).
func TestConsistentGetIsRefusedWhileAMemberDoesNotPack(t *testing.T) {
	t.Run("gossip", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		waitForFeatureConvergence(t, tc)
		bucket := bucketOwnedBy(t, tc, 0, "packed")
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of k")
		stale := stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) {
			c.memberFeatures[stale] = map[string]bool{}
		})
		message := requireConsistentGetRefused(t, consistentGetFrame(t, tc.nodes[0], testDatabase, bucket, "k"), transport.ErrUnavailable, stale)
		if !strings.Contains(message, "upgrade") {
			t.Errorf("the refusal %q does not say to upgrade %s", message, stale)
		}
		tc.nodes[0].server.core.cluster.recordMemberFeatures(stale, clusterFeatures())
		if resp := answeredConsistentGet(t, consistentGetFrame(t, tc.nodes[0], testDatabase, bucket, "k")); !resp.Found {
			t.Errorf("once %s advertises the feature, the read of k answered found=%v (%s)", stale, resp.Found, resp.Message)
		}
	})

	t.Run("per read", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of k")
		owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
		older := tc.addrs[replicas[0]]
		defer rewriteAnswers(t, clusterOf(tc, owner), older, func(answer bson.D) bson.D {
			return withoutFields(answer, "incarnation", "term", "through", "epoch", "settled", "owes", "log_short", "packs")
		})()
		message := requireConsistentGetRefused(t, consistentGetFrame(t, tc.nodes[owner], testDatabase, testBucket, "k"), transport.ErrUnavailable, older)
		if !strings.Contains(message, "upgrade") {
			t.Errorf("the refusal %q does not say to upgrade %s", message, older)
		}
	})
}

// TestConsistentGetRefusesAnAnswerFromAnotherEpoch: a holder whose answer
// says it is one membership epoch ahead of the coordinator is named in a
// refusal, UNAVAILABLE, with both epochs, and nothing is repaired; with its
// answers left alone the read answers. Guard: no epoch check (the read
// answers k).
func TestConsistentGetRefusesAnAnswerFromAnotherEpoch(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of k")
	owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
	coordinator := clusterOf(tc, owner)
	ahead := tc.addrs[replicas[0]]
	epoch := coordinator.Epoch()
	restore := rewriteAnswers(t, coordinator, ahead, func(answer bson.D) bson.D {
		return append(withoutFields(answer, "epoch"), bson.E{Key: "epoch", Value: int64(epoch + 1)})
	})
	defer restore()
	before := tc.walSequences()
	requireConsistentGetRefused(t, consistentGetFrame(t, tc.nodes[owner], testDatabase, testBucket, "k"), transport.ErrUnavailable,
		ahead, fmt.Sprintf("epoch %d", epoch+1), fmt.Sprintf("at %d", epoch))
	if after := tc.walSequences(); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("the refused read logged a repair: the logs went from %v to %v", before, after)
	}
	restore()
	if resp := answeredConsistentGet(t, consistentGetFrame(t, tc.nodes[owner], testDatabase, testBucket, "k")); !resp.Found {
		t.Errorf("with every answer at the coordinator's epoch, the read of k answered found=%v (%s)", resp.Found, resp.Message)
	}
}
