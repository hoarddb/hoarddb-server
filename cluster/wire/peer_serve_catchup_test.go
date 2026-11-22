package wire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0763 part 1: a forwarded frame carries the state_seq its sender had applied. A node that
// is behind that state_seq may not know a database the sender already created, and used to refuse
// the forwarded write NOT_FOUND with ids in place of names. serveForwardWrite and serveForwardRead
// now wait, bounded, for the node to catch up, and refuse a still-unknown database with a
// retryable UNAVAILABLE that names it. These tests drive the owner's serve functions with an
// envelope whose state_seq is ahead of the node's, with the wait cap shortened.

const catchUpTestCap = 150 * time.Millisecond

// catchUpFrame is a FORWARD_WRITE frame inserting document id into bucket User of database app, with
// the envelope carrying senderSeq as the sender's applied state_seq.
func catchUpFrame(t *testing.T, node *Node, id string, senderSeq int64) (transport.Envelope, []byte) {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	docID, fields, err := ref.documentOp(bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}})
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent(docID)
	if err != nil {
		t.Fatal(err)
	}
	op := ref.template()
	op.Class, op.Fields, op.IDValue, op.EncID = cluster.OpInsert, fields, stringValue(docID), encID
	body, err := EncodeForwardWriteBody(WriteRequest{Op: op, RequestID: [16]byte{0x07, 0x63, 1}, Hop: 1})
	if err != nil {
		t.Fatal(err)
	}
	slot := storage.SlotOf(storage.PlacementKey(u30iDB, "User", encID))
	return transport.Envelope{Slot: slot, DBID: ref.dbID, Epoch: uint64(node.topology.Committed()), StateSeq: uint64(senderSeq)}, body
}

// catchUpStoredRead is a FORWARD_READ frame for the declaration of bucket User, the read a create
// bucket sends first.
func catchUpStoredRead(t *testing.T, node *Node, dbID uint64, senderSeq int64) (transport.Envelope, []byte) {
	t.Helper()
	encID, err := store.EncodeComponent("User")
	if err != nil {
		t.Fatal(err)
	}
	slot := storage.SlotOf(storage.PlacementKey(u30iDB, "_buckets", encID))
	body, err := EncodeForwardReadBody(ForwardReadBody{Stored: true, Req: ReadRequest{BucketID: store.BucketsBucketID, ID: "User", EncID: encID, Hop: 1}})
	if err != nil {
		t.Fatal(err)
	}
	return transport.Envelope{Slot: slot, DBID: dbID, Epoch: uint64(node.topology.Committed()), StateSeq: uint64(senderSeq)}, body
}

// catchUpAdvance commits one more command after delay, moving the node's state_seq by one the way
// the commit a lagging follower is waiting for does.
func catchUpAdvance(t *testing.T, node *Node, delay time.Duration) (done <-chan struct{}) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		time.Sleep(delay)
		if _, err := node.CreateDatabaseRF(context.Background(), "late", 1); err != nil {
			t.Errorf("the commit the forward waits for: %v", err)
		}
	}()
	return finished
}

func catchUpNode(t *testing.T) *Node {
	t.Helper()
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node)
	t.Cleanup(release)
	s0Schema(t, node)
	node.cl.forwardCatchUpCap = catchUpTestCap
	return node
}

func requireNamedUnavailable(t *testing.T, err error, dbID string) {
	t.Helper()
	message := u39Code(t, err, cluster.CodeUnavailable)
	for _, want := range []string{dbID, "run the statement again", "this frame wrote nothing"} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal %q must contain %q", message, want)
		}
	}
	// A create bucket user holds no request id, and the reverse index is written before the
	// declaration, so the statement is not unchanged.
	for _, banned := range []string{"request id", "Nothing was written or changed"} {
		if strings.Contains(message, banned) {
			t.Errorf("the refusal %q must not contain %q", message, banned)
		}
	}
}

// requireNotTheNotKnownYetRefusal fails when err is the refusal for a database this node had not
// applied yet. After a successful wait the owner path judges the frame and may answer anything
// else, so the tests only pin that this refusal is gone.
func requireNotTheNotKnownYetRefusal(t *testing.T, err error) {
	t.Helper()
	var refusal *cluster.Error
	if errors.As(err, &refusal) && strings.Contains(refusal.Message, "is not known on") {
		t.Fatalf("the frame was refused as not known yet although the commit it waits for arrived: %v", err)
	}
}

// TestAForwardedWriteWaitsForThisNodeToApplyTheSendersState: the frame is about a database the
// sender has created and this node has not applied, one command behind; the write is judged once the
// node has applied it, not refused before. [BREAK: no wait; wait on reads only]
func TestAForwardedWriteWaitsForThisNodeToApplyTheSendersState(t *testing.T) {
	node := catchUpNode(t)
	target := node.raft.StateSeq() + 1
	env, body := catchUpFrame(t, node, "lag-1", target)
	env.DBID = uint64(node.topology.Refresh().NextDatabaseID) // the id the awaited create database takes
	node.cl.forwardCatchUpCap = 30 * time.Second              // the commit arrives well inside it
	finished := catchUpAdvance(t, node, 150*time.Millisecond)
	defer func() { <-finished }()

	reply, err := node.serveForwardWrite(context.Background(), env, body)
	if err != nil {
		t.Fatalf("serveForwardWrite: %v", err)
	}
	if applied := node.raft.StateSeq(); applied < target {
		t.Fatalf("the forwarded write was judged at state_seq %d, before this node applied the sender's %d", applied, target)
	}
	_, refusal := DecodeForwardWriteReply(reply.Body)
	requireNotTheNotKnownYetRefusal(t, refusal)
}

// TestAForwardedWriteForAnUnknownDatabaseIsARetryableRefusalThatNamesItAfterTheCap: the sender is
// at a state this node cannot reach; the database is unknown here. The wait is the cap, and the
// refusal is UNAVAILABLE naming the database. [BREAK: wait without the cap]
func TestAForwardedWriteForAnUnknownDatabaseIsARetryableRefusalThatNamesItAfterTheCap(t *testing.T) {
	node := catchUpNode(t)
	env, body := catchUpFrame(t, node, "lag-2", node.raft.StateSeq()+1000)
	env.DBID = 777777
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := time.Now()
	reply, err := node.serveForwardWrite(ctx, env, body)
	waited := time.Since(started)
	if err != nil {
		t.Fatalf("serveForwardWrite returned a transport error instead of a reply: %v", err)
	}
	_, refusal := DecodeForwardWriteReply(reply.Body)
	requireNamedUnavailable(t, refusal, "777777")
	if waited < catchUpTestCap {
		t.Errorf("refused after %v, before the %v cap", waited, catchUpTestCap)
	}
	if waited > 2*time.Second {
		t.Errorf("waited %v: the wait is not bounded by the %v cap", waited, catchUpTestCap)
	}
}

// knownDatabaseServedAtOnce bounds how long a frame for a known database may take with a cap
// of 30 s set: far above any serve, far below the cap, so a wait shows.
const knownDatabaseServedAtOnce = 5 * time.Second

// TestAForwardedWriteForAKnownDatabaseIsServedAtOnceWhenTheSenderIsAhead: a sender ahead on a state
// this node does not reach (a lagging owner) must not park every forwarded write: the database is
// known, so the write is served without waiting. [BREAK: wait before checking the database]
func TestAForwardedWriteForAKnownDatabaseIsServedAtOnceWhenTheSenderIsAhead(t *testing.T) {
	node := catchUpNode(t)
	node.cl.forwardCatchUpCap = 30 * time.Second
	env, body := catchUpFrame(t, node, "lag-3", node.raft.StateSeq()+1000)
	started := time.Now()
	reply, err := node.serveForwardWrite(context.Background(), env, body)
	if err != nil {
		t.Fatalf("serveForwardWrite: %v", err)
	}
	if waited := time.Since(started); waited > knownDatabaseServedAtOnce {
		t.Errorf("a write to a known database waited %v for the sender's state_seq", waited)
	}
	if _, err := DecodeForwardWriteReply(reply.Body); err != nil {
		t.Fatalf("a write to a known database was refused: %v", err)
	}
}

// TestAForwardedReadForAKnownDatabaseIsServedAtOnceWhenTheSenderIsAhead is the read side.
func TestAForwardedReadForAKnownDatabaseIsServedAtOnceWhenTheSenderIsAhead(t *testing.T) {
	node := catchUpNode(t)
	node.cl.forwardCatchUpCap = 30 * time.Second
	ref, err := node.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	env, body := catchUpStoredRead(t, node, ref.dbID, node.raft.StateSeq()+1000)
	started := time.Now()
	reply, err := node.serveForwardRead(context.Background(), env, body)
	if err != nil {
		t.Fatalf("serveForwardRead: %v", err)
	}
	if waited := time.Since(started); waited > knownDatabaseServedAtOnce {
		t.Errorf("a read of a known database waited %v for the sender's state_seq", waited)
	}
	if _, found, err := DecodeForwardReadStored(reply.Body); err != nil || !found {
		t.Fatalf("the declaration read: found %v, err %v", found, err)
	}
}

// TestAForwardedWriteWithNoGapDoesNotWait: the common case is one integer compare.
func TestAForwardedWriteWithNoGapDoesNotWait(t *testing.T) {
	node := catchUpNode(t)
	node.cl.forwardCatchUpCap = 30 * time.Second
	env, body := catchUpFrame(t, node, "lag-4", node.raft.StateSeq())
	started := time.Now()
	reply, err := node.serveForwardWrite(context.Background(), env, body)
	if err != nil {
		t.Fatalf("serveForwardWrite: %v", err)
	}
	if _, err := DecodeForwardWriteReply(reply.Body); err != nil {
		t.Fatalf("the write was refused: %v", err)
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("a frame with no state_seq gap took %v", waited)
	}
}

// TestAForwardedWriteWaitEndsWithTheRequestContext: a cancelled request does not sit out the cap.
func TestAForwardedWriteWaitEndsWithTheRequestContext(t *testing.T) {
	node := catchUpNode(t)
	node.cl.forwardCatchUpCap = 30 * time.Second
	env, body := catchUpFrame(t, node, "lag-5", node.raft.StateSeq()+1000)
	env.DBID = 777777
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	reply, err := node.serveForwardWrite(ctx, env, body)
	if err != nil {
		t.Fatalf("serveForwardWrite: %v", err)
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("waited %v after the request context ended", waited)
	}
	_, refusal := DecodeForwardWriteReply(reply.Body)
	requireNamedUnavailable(t, refusal, "777777")
}

// TestAForwardedReadWaitsForThisNodeToApplyTheSendersState is the read side. [BREAK: wait on writes only]
func TestAForwardedReadWaitsForThisNodeToApplyTheSendersState(t *testing.T) {
	node := catchUpNode(t)
	target := node.raft.StateSeq() + 1
	nextDatabase := uint64(node.topology.Refresh().NextDatabaseID) // the id the awaited create database takes
	env, body := catchUpStoredRead(t, node, nextDatabase, target)
	node.cl.forwardCatchUpCap = 30 * time.Second
	finished := catchUpAdvance(t, node, 150*time.Millisecond)
	defer func() { <-finished }()

	reply, err := node.serveForwardRead(context.Background(), env, body)
	if err != nil {
		t.Fatalf("serveForwardRead: %v", err)
	}
	if applied := node.raft.StateSeq(); applied < target {
		t.Fatalf("the forwarded read was judged at state_seq %d, before this node applied the sender's %d", applied, target)
	}
	_, _, refusal := DecodeForwardReadStored(reply.Body)
	requireNotTheNotKnownYetRefusal(t, refusal)
}

// TestAForwardedReadForAnUnknownDatabaseIsARetryableRefusalThatNamesItAfterTheCap: the declaration
// read of a create bucket answered "not found" for a database this node had not applied yet.
func TestAForwardedReadForAnUnknownDatabaseIsARetryableRefusalThatNamesItAfterTheCap(t *testing.T) {
	node := catchUpNode(t)
	env, body := catchUpStoredRead(t, node, 777777, node.raft.StateSeq()+1000)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	reply, err := node.serveForwardRead(ctx, env, body)
	waited := time.Since(started)
	if err != nil {
		t.Fatalf("serveForwardRead returned a transport error instead of a reply: %v", err)
	}
	_, _, refusal := DecodeForwardReadStored(reply.Body)
	requireNamedUnavailable(t, refusal, "777777")
	if waited < catchUpTestCap || waited > 2*time.Second {
		t.Errorf("waited %v, want the %v cap", waited, catchUpTestCap)
	}
}

// TestAForwardedWriteWaitEndsWhenThisNodeShutsDown: a node that is closing does not hold a forwarded
// frame for the cap; the wait returns and the refusal names the database. [BREAK: no shutdown branch]
func TestAForwardedWriteWaitEndsWhenThisNodeShutsDown(t *testing.T) {
	node := catchUpNode(t)
	node.cl.forwardCatchUpCap = 10 * time.Second
	env, body := catchUpFrame(t, node, "lag-6", node.raft.StateSeq()+1000)
	env.DBID = 777777
	time.AfterFunc(100*time.Millisecond, node.cl.cancel)

	started := time.Now()
	reply, err := node.serveForwardWrite(context.Background(), env, body)
	if err != nil {
		t.Fatalf("serveForwardWrite: %v", err)
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("waited %v after this node began shutting down", waited)
	}
	_, refusal := DecodeForwardWriteReply(reply.Body)
	requireNamedUnavailable(t, refusal, "777777")
}
