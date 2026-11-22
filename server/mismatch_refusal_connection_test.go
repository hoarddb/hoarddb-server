package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAMismatchRefusalKeepsTheConnectionOpen is BUG-0563's server half. A
// node whose membership disagrees with a peer's refuses every mutation with
// errMembershipDiverged. The refusal is an answer, not a failure of the
// connection: the client gets the named refusal with write_outcome
// not_applied, and the next request on the same connection is served.
// handleClientFrame used to return the refusal frame and the Go error
// together, so the transport discarded the frame, answered a generic internal
// error and closed the connection, taking every request in flight on it,
// a pop's answer among them, with it.
func TestAMismatchRefusalKeepsTheConnectionOpen(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)

	client := tc.dial(0)
	defer func() { _ = client.Close() }()
	send := func(op transport.Opcode, req any) (*transport.Frame, error) {
		payload, err := bson.Marshal(req)
		if err != nil {
			t.Fatalf("marshal %s: %v", op, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return client.SendRequest(ctx, op, payload)
	}

	clusterOf(tc, 0).mismatch.Store(true)
	defer clusterOf(tc, 0).mismatch.Store(false)

	refused, err := send(transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "refused", Data: map[string]interface{}{"n": 1},
	})
	if err != nil {
		t.Fatalf("the write refused for a membership disagreement got no answer: %v", err)
	}
	if refused.Opcode != transport.OpError {
		t.Fatalf("the write was answered %s while membership disagrees, want a refusal", refused.Opcode)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(refused.Payload, &refusal); err != nil {
		t.Fatalf("reading the refusal: %v", err)
	}
	if refusal.WriteOutcome != outcomeNotApplied {
		t.Errorf("the refusal says write_outcome %q (code %d: %s), want %q: the client cannot tell the write is safe to retry",
			refusal.WriteOutcome, refusal.Code, refusal.Message, outcomeNotApplied)
	}
	if transport.ErrorCode(refusal.Code) == transport.ErrInternal {
		t.Errorf("the refusal is an internal error (%s), not the named membership refusal", refusal.Message)
	}
	if !strings.Contains(refusal.Message, "nodes history") {
		t.Errorf("the refusal does not say what to run: %s", refusal.Message)
	}

	answer, err := send(transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "refused"})
	if err != nil {
		t.Fatalf("the request after the refusal, on the same connection, was not served: %v; "+
			"the refusal closed the connection", err)
	}
	if answer.RequestID == refused.RequestID {
		t.Fatalf("the read after the refusal was answered with the refusal's request ID")
	}
}

// TestAForwardedReadRefusedForMismatchIsUnavailable is BUG-0704's second half.
// An owner whose membership disagrees with a peer's refuses a request
// forwarded to it, and the refusal is transient: the flag clears when the
// disagreement does. The coordinator's client must read UNAVAILABLE with a
// retry sentence, which a driver retries (driver spec §1.4); the refusal
// carried no kind, so the coordinator answered INTERNAL, which a driver must
// never retry. A forwarded write refused the same way stores nothing.
func TestAForwardedReadRefusedForMismatchIsUnavailable(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 1)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}})

	owner := ownerIndex(t, tc, testBucket)
	coordinator := (owner + 1) % len(tc.nodes)
	ownerCluster := clusterOf(tc, owner)
	// Gossip that agrees would clear the flag under the test; the owner skips
	// its reconciliations while the test holds the flag.
	setGossipSkip(t, func(nodeID, _, _ string) bool { return nodeID == ownerCluster.self.ID })
	ownerCluster.mismatch.Store(true)
	defer ownerCluster.mismatch.Store(false)

	requireRetryableRefusal := func(what string, frame *transport.Frame, wantOutcome string) {
		t.Helper()
		if frame.Opcode != transport.OpError {
			t.Fatalf("%s forwarded to an owner in membership mismatch was answered %s, want a refusal", what, frame.Opcode)
		}
		var refusal ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
			t.Fatalf("%s: reading the refusal: %v", what, err)
		}
		if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable {
			t.Errorf("%s: code %d, want UNAVAILABLE (%d): a driver never retries anything else; message %q",
				what, refusal.Code, transport.ErrUnavailable, refusal.Message)
		}
		if !strings.Contains(refusal.Message, "Retry") {
			t.Errorf("%s: the refusal does not say to retry: %q", what, refusal.Message)
		}
		if refusal.WriteOutcome != wantOutcome {
			t.Errorf("%s: write outcome %q, want %q", what, refusal.WriteOutcome, wantOutcome)
		}
	}

	requireRetryableRefusal("a get", tc.frame(coordinator, transport.OpGet,
		FindRequest{Database: testDatabase, Bucket: testBucket, Key: "k"}), "")
	requireRetryableRefusal("a put", tc.frame(coordinator, transport.OpPut,
		InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "refused", Data: map[string]interface{}{"n": 2}}), outcomeNotApplied)

	ownerCluster.mismatch.Store(false)
	stored := tc.frame(owner, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "refused"})
	if stored.Opcode == transport.OpError {
		t.Fatalf("reading back the refused put on the owner was refused: %s", string(stored.Payload))
	}
	var found FindResponse
	if err := bson.Unmarshal(stored.Payload, &found); err != nil {
		t.Fatalf("reading back the refused put: %v", err)
	}
	if found.Found {
		t.Errorf("the put refused for a membership mismatch was stored on the owner")
	}
}
