package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A write's ownership was read twice around the join fence, so a
// membership adopted between the two reads let a write past the fence of a
// bucket its node holds no copy of.

// joinerWithoutCopies is a joiner J that holds no copy of any bucket: both
// members' streams to it are held, and nothing rebalances. members is the
// membership J adopted (the two members and J).
type joinerWithoutCopies struct {
	tc         *testCluster
	joiner     *Cluster
	joinerNode *testNode
	members    []ring.Node
}

// bucketNamed returns the first name of the form prefix%03d for which want
// holds, or fails the test.
func bucketNamed(t *testing.T, prefix string, want func(name string) bool) string {
	t.Helper()
	if name, found := findBucketName(prefix, want); found {
		return name
	}
	t.Fatalf("setup: no bucket name of the form %s000 satisfies the test's placement", prefix)
	return ""
}

// findBucketName is bucketNamed's search, reporting a miss instead of failing.
// A name's placement is that of its partition (ring.PartitionCount of them),
// and 2000 names of one prefix reach nearly all: a miss means the ring has no
// partition the test wants, which no longer search changes.
func findBucketName(prefix string, want func(name string) bool) (string, bool) {
	for i := 0; i < 2000; i++ {
		if name := fmt.Sprintf("%s%03d", prefix, i); want(name) {
			return name, true
		}
	}
	return "", false
}

func ownerOn(r *ring.Ring, name string) string {
	owner, err := r.Owner(placementOf(testDatabase, name).encoded)
	if err != nil {
		return ""
	}
	return owner.ID
}

// setUpJoinerWithoutCopies starts two members at factor 3, creates the hash
// buckets pick names on the membership the join will make (pick receives the
// members it will have, joiner last), and joins a third node that copies none
// of them.
func setUpJoinerWithoutCopies(t *testing.T, pick func(t *testing.T, joined []ring.Node) []string) joinerWithoutCopies {
	t.Helper()
	tc := newTestClusterWith(t, 2, 3, func(_ int, cfg *config.Config) { noSweepNoRetention(cfg) })
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	joined := append(append([]ring.Node{}, tc.nodes[0].server.core.cluster.Members()...), ports.ringNode())
	buckets := pick(t, joined)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range buckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 2)
	}
	proposer := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	for _, node := range tc.nodes {
		t.Cleanup(holdStreamPushesOnly(node.server.core.cluster.peerFor(ports.ringNode())))
	}
	joinerNode := startJoinTarget(t, tc, ports, tc.token, noSweepNoRetention)
	t.Cleanup(func() { _ = joinerNode.client.Close() })
	joiner := joinerNode.server.core.cluster
	adoptFrom(t, joiner, proposer)
	for _, bucket := range buckets {
		if joiner.s.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("setup: the joiner holds %s before any pull", bucket)
		}
	}
	return joinerWithoutCopies{tc: tc, joiner: joiner, joinerNode: joinerNode, members: joined}
}

// adoptNext moves the joiner, alone, to the next epoch with members: what a
// membership change learned while a write is between two steps does to it.
func (s joinerWithoutCopies) adoptNext(members []ring.Node) error {
	return s.joiner.adoptMembership(s.joiner.Epoch()+1, members)
}

// TestAWriteIsFencedOnTheOwnershipItWasDecidedOn is the first criterion.
// The forwarded half: the joiner owns a bucket it holds no copy of, decides so,
// and adopts a membership in which a new member owns it before the fence runs.
// On the starting tree the fence read the ring again, found another owner, let
// the write through, and the joiner logged it for a bucket it does not hold.
// The client half: the joiner does not own a bucket when the client's write
// passes the fence, and owns it when the write path reads the ring again.
// Both writes must be refused with nothing logged.
func TestAWriteIsFencedOnTheOwnershipItWasDecidedOn(t *testing.T) {
	t.Run("a forwarded write", func(t *testing.T) {
		var newcomer ring.Node
		var bucket string
		s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
			joiner := joined[len(joined)-1].ID
			before := ringOf(t, joined, 3)
			// The newcomer is fabricated, so its address is the test's to
			// choose: one that takes a partition from the joiner.
			decided := func(newcomer string) func(name string) bool {
				after := ringOf(t, append(append([]ring.Node{}, joined...), ring.Node{ID: newcomer, Addr: newcomer}), 3)
				return func(name string) bool { return ownerOn(before, name) == joiner && ownerOn(after, name) == newcomer }
			}
			chosen, err := drawPlaced(func() string { return fabricatedMemberAddr(t) },
				func(addr string) []string { return []string{addr} },
				func(addrs []string) bool { _, found := findBucketName("decided", decided(addrs[0])); return found })
			if err != nil {
				t.Fatal(err)
			}
			newcomer = ring.Node{ID: chosen, Addr: chosen}
			bucket = bucketNamed(t, "decided", decided(chosen))
			return []string{bucket}
		})
		var flipped atomic.Bool
		var flipErr error
		hook := func(_, decided string) {
			if decided == bucket && flipped.CompareAndSwap(false, true) {
				flipErr = s.adoptNext(append(append([]ring.Node{}, s.members...), newcomer))
			}
		}
		afterOwnerDecidedHook.Store(&hook)
		defer afterOwnerDecidedHook.Store(nil)

		logged := s.joiner.s.wal.LastSequence()
		insert := mustMarshalBSON(t, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "fenced", Data: map[string]interface{}{"n": 1}})
		push := mustMarshalBSON(t, ReplPushRequest{
			Token:          s.joiner.localToken(),
			FromNode:       s.members[0].ID,
			ReturnResponse: true,
			Epoch:          s.joiner.Epoch(),
			Entries:        []ReplEntry{{Op: int32(transport.OpPut), Bucket: bucket, Payload: insert}},
		})
		answer, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), s.joiner.s)
		if err != nil {
			t.Fatalf("forwarding the write: %v", err)
		}
		if !flipped.Load() || flipErr != nil {
			t.Fatalf("test setup: the joiner did not adopt the next membership between its owner check and its fence (%t, %v)", flipped.Load(), flipErr)
		}
		var resp ReplPushResponse
		if err := bson.Unmarshal(answer.Payload, &resp); err != nil {
			t.Fatalf("decoding the answer: %v", err)
		}
		if now := s.joiner.s.wal.LastSequence(); now != logged {
			t.Errorf("the joiner's log went from %d to %d for a forwarded write to %s, which it decided it owned and holds no copy of: "+
				"the fence read the ring again and let the write through", logged, now, bucket)
		}
		if resp.Success || resp.Refusal != refusalAwaitingCopy {
			t.Errorf("the forwarded write was answered success=%t refusal=%q error=%q; want the join fence's %q",
				resp.Success, resp.Refusal, resp.Error, refusalAwaitingCopy)
		}
	})

	t.Run("a client's write", func(t *testing.T) {
		var bucket string
		var removed ring.Node
		s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
			joiner := joined[len(joined)-1].ID
			before := ringOf(t, joined, 3)
			for _, candidate := range joined[:len(joined)-1] {
				var rest []ring.Node
				for _, m := range joined {
					if m.ID != candidate.ID {
						rest = append(rest, m)
					}
				}
				after := ringOf(t, rest, 3)
				for i := 0; i < 2000 && bucket == ""; i++ {
					name := fmt.Sprintf("decided%03d", i)
					if ownerOn(before, name) == candidate.ID && ownerOn(after, name) == joiner {
						bucket, removed = name, candidate
					}
				}
				if bucket != "" {
					break
				}
			}
			if bucket == "" {
				t.Fatal("setup: no bucket a member owns that the joiner owns once that member is gone")
			}
			return []string{bucket}
		})
		var rest []ring.Node
		for _, m := range s.members {
			if m.ID != removed.ID {
				rest = append(rest, m)
			}
		}
		var flipped atomic.Bool
		var flipErr error
		hook := func(op transport.Opcode, _, decided string) {
			if op == transport.OpPut && decided == bucket && flipped.CompareAndSwap(false, true) {
				flipErr = s.adoptNext(rest)
			}
		}
		afterJoinFenceHook.Store(&hook)
		defer afterJoinFenceHook.Store(nil)

		logged := s.joiner.s.wal.LastSequence()
		frame := s.tc.frameOn(s.joinerNode, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "fenced",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
		if !flipped.Load() || flipErr != nil {
			t.Fatalf("test setup: the joiner did not adopt the next membership between the fence and the write path (%t, %v)", flipped.Load(), flipErr)
		}
		if now := s.joiner.s.wal.LastSequence(); now != logged {
			t.Errorf("the joiner's log went from %d to %d for a client's write to %s, which it holds no copy of: "+
				"it passed the fence as a non-owner and was applied as the owner", logged, now, bucket)
		}
		if frame.Opcode != transport.OpError {
			t.Errorf("the client's write was answered %s, want a refusal: %s", frame.Opcode, frame.Payload)
		}
	})
}

// streamOnlyHeldConn holds every push of the sender's own stream that carries
// entries, until release is closed, and lets a forwarded request through: a
// joiner that holds nothing still answers the writes forwarded to it.
type streamOnlyHeldConn struct {
	peerConn
	release <-chan struct{}
}

func (h *streamOnlyHeldConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		var push ReplPushRequest
		if err := bson.Unmarshal(payload, &push); err != nil {
			return nil, fmt.Errorf("decoding a held push: %w", err)
		}
		if push.Stream != nil && len(push.Entries) > 0 {
			select {
			case <-h.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return h.peerConn.SendRequest(ctx, lane, op, payload)
}

// holdStreamPushesOnly holds p's stream pushes that carry entries until the
// returned function is called.
func holdStreamPushesOnly(p *peer) (release func()) {
	gate := make(chan struct{})
	wrapPeerConn(p, func(inner peerConn) peerConn { return &streamOnlyHeldConn{peerConn: inner, release: gate} })
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// frameOn sends a request to a node that is not one of tc.nodes (a joiner)
// and returns its answer.
func (tc *testCluster) frameOn(node *testNode, op transport.Opcode, req any) *transport.Frame {
	tc.t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		tc.t.Fatalf("marshal %s: %v", op, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := node.client.SendRequest(ctx, op, payload)
	if err != nil {
		tc.t.Fatalf("send %s to %s: %v", op, node.addr, err)
	}
	return frame
}

// TestAForwardedJoinFenceRefusalSaysWhichNodeAndMatchesTheSentinel is
// The second criterion: a write through a member to a bucket the joiner
// owns and holds no copy of is refused by the joiner with the join fence. At
// the coordinator that refusal is ErrJoinSyncIncomplete (UNAVAILABLE on the
// wire) and names the owner, never "this node", which there is the
// coordinator.
func TestAForwardedJoinFenceRefusalSaysWhichNodeAndMatchesTheSentinel(t *testing.T) {
	var bucket string
	s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
		after := ringOf(t, joined, 3)
		bucket = bucketNamed(t, "awaited", func(name string) bool { return ownerOn(after, name) == joined[len(joined)-1].ID })
		return []string{bucket}
	})
	coordinator := s.tc.nodes[0].server.core.cluster
	payload := mustMarshalBSON(t, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "awaited", Data: map[string]interface{}{"n": 1}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := coordinator.write(ctx, ReplEntry{Op: int32(transport.OpPut), Bucket: bucket, Payload: payload},
		placementOf(testDatabase, bucket), WcOne)
	if !errors.Is(err, ErrJoinSyncIncomplete) {
		t.Errorf("a forwarded write refused by the join fence returned %v at the coordinator; want it to match ErrJoinSyncIncomplete", err)
	}
	if err != nil {
		if strings.Contains(err.Error(), "this node is the ring owner") {
			t.Errorf("the coordinator's error says \"this node is the ring owner\", but the owner is %s: %v", s.joiner.self.ID, err)
		}
		if !strings.Contains(err.Error(), s.joiner.self.ID) {
			t.Errorf("the coordinator's error does not name the owner %s: %v", s.joiner.self.ID, err)
		}
	}

	frame := s.tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "awaited",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Fatalf("the client's write through the coordinator was answered %s: %s", frame.Opcode, frame.Payload)
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable {
		t.Errorf("the client's error code is %d, want UNAVAILABLE (%d): %s", refusal.Code, transport.ErrUnavailable, refusal.Message)
	}
}
