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

// Step 7.4: an error for a write says whether the write was stored (spec
// §19.3.11 P4, §20.12.3).

// errorAnswerOf decodes an error frame, or fails the test when the frame is not
// one.
func errorAnswerOf(t *testing.T, frame *transport.Frame, what string) ErrorResponse {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("%s was answered %s, want an error: %s", what, frame.Opcode, frame.Payload)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding the error for %s: %v", what, err)
	}
	return refusal
}

// requireOutcome checks an error's code and write_outcome.
func requireOutcome(t *testing.T, refusal ErrorResponse, what, outcome string, code transport.ErrorCode) {
	t.Helper()
	if refusal.WriteOutcome != outcome || transport.ErrorCode(refusal.Code) != code {
		t.Errorf("%s was answered code %d, write_outcome %q; want code %d, %q: %s",
			what, refusal.Code, refusal.WriteOutcome, code, outcome, refusal.Message)
	}
}

// testBucketPut is a put of key to the test bucket at concern.
func testBucketPut(key, concern string) InsertRequest {
	return InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key, Data: map[string]interface{}{"key": key}, WriteConcern: concern}
}

// answerDroppingConn sends a forwarded request and then reports its answer
// lost: the owner applied it, and the coordinator never hears so.
type answerDroppingConn struct {
	peerConn
}

func (d *answerDroppingConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	frame, err := d.peerConn.SendRequest(ctx, lane, op, payload)
	if err != nil || op != transport.OpReplPush || lane != transport.LaneRequest {
		return frame, err
	}
	return nil, errors.New("the connection was reset before the answer arrived")
}

// TestAnErrorForAWriteSaysWhetherItWasStored is T-108. Each error a write can
// be answered with through the replicated write path carries write_outcome,
// and the owner's store agrees with it: a write-concern timeout says
// `applied`, with the copies that hold the write, and the owner holds it; a
// forward whose answer is lost says `unknown`; a refusal by a node that does
// not own the bucket, an epoch refusal and the join fence say `not_applied`,
// and nothing was stored. Guard: answer the timeout with a bare error.
func TestAnErrorForAWriteSaysWhetherItWasStored(t *testing.T) {
	t.Run("the write-concern timeout", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		ownerIndex, heldIndex := ownerAndReplica(t, tc)
		otherIndex := 3 - ownerIndex - heldIndex
		ownerCluster := tc.nodes[ownerIndex].server.core.cluster
		// The held replica falls behind the owner's log, so a restarted owner
		// has something to replay to it.
		unfreeze := freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[heldIndex])))
		for i := 0; i < 5; i++ {
			putDocumentAt(tc, ownerIndex, testDatabase, testBucket, fmt.Sprintf("behind-%d", i), "one")
		}
		// After a restart every stream is connecting. The held replica's stays
		// connecting: its replay is held before its first frame, so it is in
		// sync and confirms nothing.
		heldID := tc.addrs[heldIndex]
		holding, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		hold := func(peerID string, _ []ReplEntry) {
			if peerID != heldID {
				return
			}
			once.Do(func() { close(holding) })
			<-release
		}
		beforeReplayShipHook.Store(&hold)
		defer beforeReplayShipHook.Store(nil)
		defer close(release)
		// The freeze is not lifted: it goes with the process the restart stops,
		// so nothing reaches the held replica before the restarted owner
		// replays to it.
		defer unfreeze()
		restarted := tc.restartNode(ownerIndex)
		select {
		case <-holding:
		case <-time.After(30 * time.Second):
			t.Fatal("test setup: the restarted owner never replayed its log to the held replica")
		}
		ownerCluster = restarted.server.core.cluster
		toOther := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[otherIndex]))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := catchUpPeer(ctx, toOther); err != nil {
			t.Fatalf("test setup: %v", err)
		}
		toHeld := ownerCluster.peerFor(mustNode(t, ownerCluster, heldID))
		if state, _ := toHeld.streamStatus(); state != streamConnecting {
			t.Fatalf("test setup: the held replica's stream is %s, want connecting", state)
		}

		refusal := errorAnswerOf(t, tc.frame(ownerIndex, transport.OpPut, testBucketPut("timed-out", "all")), "an all write the held replica cannot confirm")
		requireOutcome(t, refusal, "the write-concern timeout", outcomeApplied, transport.ErrTimeout)
		if refusal.CopiesConfirmed != 2 || refusal.ReplicationFactor != 3 {
			t.Errorf("the timeout was answered copies_confirmed %d, replication_factor %d; want 2 and 3", refusal.CopiesConfirmed, refusal.ReplicationFactor)
		}
		for _, want := range []string{"write concern all was not met in", "stored on the owner", "confirmed by 2 of 3 in-sync copies", "Do not retry a push or an $inc"} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("the timeout's message does not say %q: %s", want, refusal.Message)
			}
		}
		if !holdsKeyLocallyIn(restarted, testBucket, "timed-out") {
			t.Error("the timeout said applied and the owner does not hold the write")
		}
	})

	t.Run("a forward whose answer is lost", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		ownerIndex, coordinatorIndex := ownerAndReplica(t, tc)
		coordinator := tc.nodes[coordinatorIndex].server.core.cluster
		wrapPeerConn(coordinator.peerFor(mustNode(t, coordinator, tc.addrs[ownerIndex])),
			func(inner peerConn) peerConn { return &answerDroppingConn{peerConn: inner} })

		refusal := errorAnswerOf(t, tc.frame(coordinatorIndex, transport.OpPut, testBucketPut("lost", "one")), "a write whose forward lost its answer")
		requireOutcome(t, refusal, "a forward whose answer was lost", outcomeUnknown, transport.ErrUnavailable)
		if !holdsKeyLocallyIn(tc.nodes[ownerIndex], testBucket, "lost") {
			t.Error("test setup: the owner does not hold the write whose answer was dropped")
		}
	})

	t.Run("a refusal by a node that does not own the bucket", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		ownerIndex, replicaIndex := ownerAndReplica(t, tc)
		coordinatorIndex := 3 - ownerIndex - replicaIndex
		coordinator := tc.nodes[coordinatorIndex].server.core.cluster
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := coordinator.forward(ctx, mustNode(t, coordinator, tc.addrs[replicaIndex]),
			ReplEntry{Op: int32(transport.OpPut), Bucket: testBucket, Payload: requestBody(t, testBucketPut("not-owned", "one"))}, WcOne, readForward{})
		refusal := errorAnswerOf(t, errorFrame(transport.NewFrame(transport.OpPut, nil), err), "a forward to a replica")
		requireOutcome(t, refusal, "a not_owner refusal", outcomeNotApplied, transport.ErrUnavailable)
		for index := range tc.nodes {
			if holdsKeyLocallyIn(tc.nodes[index], testBucket, "not-owned") {
				t.Errorf("node %d holds a write refused not_applied", index+1)
			}
		}
	})

	t.Run("an epoch refusal", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		ownerIndex, coordinatorIndex := ownerAndReplica(t, tc)
		ownerID := tc.addrs[ownerIndex]
		// The owner stays on the epoch it has; the coordinator alone moves on.
		refuse := func(self string, _ uint64) error {
			if self == ownerID {
				return errors.New("held on its epoch by the test")
			}
			return nil
		}
		adoptMembershipHook.Store(&refuse)
		defer adoptMembershipHook.Store(nil)
		coordinator := tc.nodes[coordinatorIndex].server.core.cluster
		if err := coordinator.adoptMembership(coordinator.Epoch()+1, append([]ring.Node{}, coordinator.Members()...)); err != nil {
			t.Fatalf("test setup: moving the coordinator to the next epoch: %v", err)
		}

		refusal := errorAnswerOf(t, tc.frame(coordinatorIndex, transport.OpPut, testBucketPut("other-epoch", "one")), "a write forwarded across epochs")
		requireOutcome(t, refusal, "an epoch refusal", outcomeNotApplied, transport.ErrUnavailable)
		if holdsKeyLocallyIn(tc.nodes[ownerIndex], testBucket, "other-epoch") {
			t.Error("the owner holds a write refused not_applied")
		}
	})

	t.Run("the unique pre-check and a handler's refusal", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash,
			Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{map[string]interface{}{"field": "Email", "unique": true}}}})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		ownerIndex := ownerIndexOf(t, tc, testBucket)
		coordinatorIndex := (ownerIndex + 1) % 3
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "holder",
			Data: map[string]interface{}{"Email": "taken", "Name": "ada"}, WriteConcern: "all"})
		for _, via := range []int{ownerIndex, coordinatorIndex} {
			what := fmt.Sprintf("a duplicate through node %d", via+1)
			duplicate := errorAnswerOf(t, tc.frame(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
				Key: fmt.Sprintf("dup-%d", via), Data: map[string]interface{}{"Email": "taken"}, WriteConcern: "all"}), what)
			requireOutcome(t, duplicate, what, outcomeNotApplied, transport.ErrDuplicateKey)

			what = fmt.Sprintf("$inc of a string through node %d", via+1)
			inc := errorAnswerOf(t, tc.frame(via, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: testBucket,
				Key: "holder", Inc: map[string]int64{"Name": 1}}), what)
			if inc.WriteOutcome != outcomeNotApplied {
				t.Errorf("%s was answered code %d, write_outcome %q; want %q: %s", what, inc.Code, inc.WriteOutcome, outcomeNotApplied, inc.Message)
			}
		}
		for index := range tc.nodes {
			if holdsKeyLocallyIn(tc.nodes[index], testBucket, "dup-0") || holdsKeyLocallyIn(tc.nodes[index], testBucket, "dup-1") ||
				holdsKeyLocallyIn(tc.nodes[index], testBucket, "dup-2") {
				t.Errorf("node %d holds a duplicate refused not_applied", index+1)
			}
		}
	})

	// A value of the wrong declared type (typed-fields TF-6, spec §6.1): the
	// entry is logged and shipped, and every copy's apply refuses it, so
	// nothing is stored anywhere and the answer is not_applied.
	t.Run("a type refusal", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		createTypedOn(tc, 0, testBucket, bson.A{bson.M{"name": "Age", "type": "int"}}, "")
		waitForServing(t, tc, testDatabase, testBucket, 3)
		ownerIndex := ownerIndexOf(t, tc, testBucket)
		for _, via := range []int{ownerIndex, (ownerIndex + 1) % 3} {
			what := fmt.Sprintf("a mistyped put through node %d", via+1)
			mistyped := errorAnswerOf(t, tc.frame(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
				Key: fmt.Sprintf("typed-%d", via), Data: map[string]interface{}{"Age": "thirty"}, WriteConcern: "all"}), what)
			requireOutcome(t, mistyped, what, outcomeNotApplied, transport.ErrFieldType)
		}
		for index := range tc.nodes {
			for via := range 3 {
				if holdsKeyLocallyIn(tc.nodes[index], testBucket, fmt.Sprintf("typed-%d", via)) {
					t.Errorf("node %d holds a write refused for its type", index+1)
				}
			}
		}
	})

	// A store failure on the owner's own apply is not a refusal: the entry is
	// logged and shipped, the replicas apply it, and the owner owes it and
	// applies it on replay (owed.go). The client is told the write may be
	// stored, never that retrying is safe. A field-store write answers a store
	// failure with success false, not an error, and is out of scope here.
	t.Run("the owner's store failed its apply", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		ownerIndex := ownerIndexOf(t, tc, testBucket)
		blobs := bucketOwnedBy(t, tc, ownerIndex, "blobs")
		tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: blobs, Type: BucketTypeBlob})
		waitForServing(t, tc, testDatabase, blobs, 3)
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "counter",
			Data: map[string]interface{}{"n": 0}, WriteConcern: "all"})
		coordinatorIndex := (ownerIndex + 1) % 3
		ownerKV := tc.nodes[ownerIndex].server.core.kv

		for _, via := range []int{ownerIndex, coordinatorIndex} {
			failed := failNextApplyOn(t, ownerKV, anySeq)
			what := fmt.Sprintf("$inc with the owner's store failing, through node %d", via+1)
			inc := errorAnswerOf(t, tc.frame(via, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: testBucket,
				Key: "counter", Inc: map[string]int64{"n": 1}}), what)
			if failed.Load() == 0 {
				t.Fatalf("test setup: %s failed no apply", what)
			}
			if inc.WriteOutcome != outcomeUnknown {
				t.Errorf("%s was answered code %d, write_outcome %q; want %q: the entry is logged, shipped and owed: %s",
					what, inc.Code, inc.WriteOutcome, outcomeUnknown, inc.Message)
			}

			failed = failNextApplyOn(t, ownerKV, anySeq)
			what = fmt.Sprintf("a blob write with the owner's store failing, through node %d", via+1)
			blob := errorAnswerOf(t, tc.frame(via, transport.OpBlobWrite, BlobWriteRequest{Database: testDatabase, Bucket: blobs,
				Key: fmt.Sprintf("b%d", via), Data: []byte("bytes")}), what)
			if failed.Load() == 0 {
				t.Fatalf("test setup: %s failed no apply", what)
			}
			if blob.WriteOutcome != outcomeUnknown {
				t.Errorf("%s was answered code %d, write_outcome %q; want %q: %s", what, blob.Code, blob.WriteOutcome, outcomeUnknown, blob.Message)
			}
		}
	})

	// A handler's refusal is only as deterministic as the owner's state. An
	// owner that owes an apply lags its replicas: it refuses a $inc of the
	// string its failed $set left, while the replicas, which applied the $set,
	// apply the $inc. The refusal must not say not_applied.
	t.Run("a refusal by an owner that owes an apply", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		ownerIndex, _ := ownerAndReplica(t, tc)
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k",
			Data: map[string]interface{}{"Name": "s"}, WriteConcern: "all"})
		failed := failNextApplyOn(t, tc.nodes[ownerIndex].server.core.kv, anySeq)
		set := errorAnswerOf(t, tc.frame(ownerIndex, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: testBucket,
			Key: "k", Set: map[string]interface{}{"Name": 5}}), "$set with the owner's store failing")
		if failed.Load() == 0 || set.WriteOutcome != outcomeUnknown {
			t.Fatalf("test setup: the $set was answered write_outcome %q (failed seq %d); want unknown after a store failure", set.WriteOutcome, failed.Load())
		}
		inc := errorAnswerOf(t, tc.frame(ownerIndex, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: testBucket,
			Key: "k", Inc: map[string]int64{"Name": 1}}), "$inc on an owner that owes the $set")
		if inc.WriteOutcome != outcomeUnknown {
			t.Errorf("a $inc refused by an owner that owes an apply was answered code %d, write_outcome %q; want %q: "+
				"its replicas applied the owed $set and apply the $inc: %s", inc.Code, inc.WriteOutcome, outcomeUnknown, inc.Message)
		}
		for index := range tc.nodes {
			if index == ownerIndex {
				continue
			}
			pollUntil(t, fmt.Sprintf("node %d applies the $inc the owner refused", index+1), func() bool {
				found, err := tc.nodes[index].server.core.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "k"})
				if err != nil || !found.Found {
					return false
				}
				name, _ := bson.Raw(found.Data).Lookup("Name").AsInt64OK()
				return name == 6
			})
		}
	})

	t.Run("an unknown write concern", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		ownerIndex, _ := ownerAndReplica(t, tc)
		refusal := errorAnswerOf(t, tc.frame(ownerIndex, transport.OpPut, testBucketPut("majorty", "majorty")), "a write at \"majorty\"")
		requireOutcome(t, refusal, "an unknown write concern", outcomeNotApplied, transport.ErrInvalidArgument)
		if holdsKeyLocallyIn(tc.nodes[ownerIndex], testBucket, "majorty") {
			t.Error("the owner holds a write refused not_applied")
		}
	})

	t.Run("the join fence", func(t *testing.T) {
		var bucket string
		s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
			after := ringOf(t, joined, 3)
			bucket = bucketNamed(t, "fenced", func(name string) bool { return ownerOn(after, name) == joined[len(joined)-1].ID })
			return []string{bucket}
		})
		logged := s.joiner.s.wal.LastSequence()
		request := InsertRequest{Database: testDatabase, Bucket: bucket, Key: "fenced", Data: map[string]interface{}{"n": 1}, WriteConcern: "one"}
		requireOutcome(t, errorAnswerOf(t, s.tc.frameOn(s.joinerNode, transport.OpPut, request), "a write to the joiner"),
			"the join fence at the owner", outcomeNotApplied, transport.ErrUnavailable)
		requireOutcome(t, errorAnswerOf(t, s.tc.frame(0, transport.OpPut, request), "a write forwarded to the joiner"),
			"the join fence through a coordinator", outcomeNotApplied, transport.ErrUnavailable)
		if now := s.joiner.s.wal.LastSequence(); now != logged {
			t.Errorf("the joiner's log went from %d to %d for writes refused not_applied", logged, now)
		}
	})
}
