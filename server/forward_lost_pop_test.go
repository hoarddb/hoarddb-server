package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// lostForwardConn makes a coordinator's forwarded request lane lose the owner's
// answer, so Cluster.forward takes its ErrForwardLost path. Replication
// shipping on the control lane is untouched: only a forwarded client request
// loses its answer.
type lostForwardConn struct {
	peerConn
}

func (l *lostForwardConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if lane == transport.LaneRequest && op == transport.OpReplPush {
		return nil, errors.New("simulated: the owner's answer never came back")
	}
	return l.peerConn.SendRequest(ctx, lane, op, payload)
}

// TestALostForwardedPopSaysItsEntryMayBeLostAndHowToLook is BUG-0564: a pop
// whose forwarded answer is lost must tell the caller the entry may be gone and
// name the peek that shows what is left, not the read-back-before-retrying
// advice meant for a push or an $inc. A put whose forward is lost keeps that
// advice.
func TestALostForwardedPopSaysItsEntryMayBeLostAndHowToLook(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})

	owner := ownerIndexOf(t, tc, testBucket)
	coordinator := (owner + 1) % len(tc.nodes)
	coordCluster := tc.nodes[coordinator].server.core.cluster

	tc.ok(coordinator, transport.OpFIFOPush, PushRequest{
		Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": 1},
	})

	// Every forward the coordinator sends to the owner now loses its answer.
	wrapPeerConn(coordCluster.peerFor(tc.nodes[owner].server.core.cluster.self),
		func(inner peerConn) peerConn { return &lostForwardConn{peerConn: inner} })

	// The Go error the coordinator gets still wraps the sentinel, so a caller
	// can match it with errors.Is as well as read the wire code.
	entry := ReplEntry{Op: int32(transport.OpFIFOPop), Bucket: testBucket,
		Payload: mustMarshalBSON(t, PopRequest{Database: testDatabase, Bucket: testBucket})}
	if _, err := coordCluster.forward(context.Background(), tc.nodes[owner].server.core.cluster.self, entry, 0, readForward{}); !errors.Is(err, ErrForwardLost) {
		t.Errorf("the forwarded pop's Go error is %v, want it to wrap ErrForwardLost", err)
	}

	pop := tc.frame(coordinator, transport.OpFIFOPop, PopRequest{Database: testDatabase, Bucket: testBucket})
	refused := refusalOf(t, pop)
	if refused.Code != int(transport.ErrUnavailable) {
		t.Errorf("the lost pop is code %d, want UNAVAILABLE (%d): %q", refused.Code, transport.ErrUnavailable, refused.Message)
	}
	if refused.WriteOutcome != outcomeUnknown {
		t.Errorf("the lost pop is write_outcome %q, want %q: %q", refused.WriteOutcome, outcomeUnknown, refused.Message)
	}
	for _, want := range []string{
		"pop",
		"may be lost",
		"Do not pop again",
		"db." + testBucket + ".peek()",
	} {
		if !strings.Contains(refused.Message, want) {
			t.Errorf("the lost pop's message does not say %q: %q", want, refused.Message)
		}
	}
	for _, unwanted := range []string{"$inc", "push"} {
		if strings.Contains(refused.Message, unwanted) {
			t.Errorf("the lost pop's message still says %q: %q", unwanted, refused.Message)
		}
	}

	put := tc.frame(coordinator, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 2},
	})
	refusedPut := refusalOf(t, put)
	if !strings.Contains(refusedPut.Message, "read it back before retrying a push or an $inc") {
		t.Errorf("a lost put's message changed; want the read-back advice: %q", refusedPut.Message)
	}
}

// TestALostForwardedRestoreChunkSaysItsOutcomeIsUnknown is BUG-0631: a restore
// is routed as a forwarded read (it must not travel as one ReplEntry), but the
// owner applies every document a chunk carries, so a lost RESTORE_CHUNK or
// RESTORE_DONE may have changed the bucket. It must carry write_outcome
// unknown and restore-specific advice. A lost FIFO_PUSH keeps unknown and the
// read-back advice, and a lost GET keeps no outcome and the read advice.
func TestALostForwardedRestoreChunkSaysItsOutcomeIsUnknown(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})

	owner := ownerIndexOf(t, tc, testBucket)
	coordinator := (owner + 1) % len(tc.nodes)
	coordCluster := tc.nodes[coordinator].server.core.cluster

	// Every forward the coordinator sends to the owner now loses its answer.
	wrapPeerConn(coordCluster.peerFor(tc.nodes[owner].server.core.cluster.self),
		func(inner peerConn) peerConn { return &lostForwardConn{peerConn: inner} })

	for _, lost := range []struct {
		name string
		op   transport.Opcode
		req  any
	}{
		{"chunk", transport.OpRestoreChunk, RestoreChunkRequest{
			Database: testDatabase, Bucket: testBucket, SessionID: "session-1",
		}},
		{"done", transport.OpRestoreDone, RestoreDoneRequest{
			Database: testDatabase, Bucket: testBucket, SessionID: "session-1",
		}},
	} {
		t.Run(lost.name, func(t *testing.T) {
			refused := refusalOf(t, tc.frame(coordinator, lost.op, lost.req))
			if refused.Code != int(transport.ErrUnavailable) {
				t.Errorf("the lost restore %s is code %d, want UNAVAILABLE (%d): %q", lost.name, refused.Code, transport.ErrUnavailable, refused.Message)
			}
			if refused.WriteOutcome != outcomeUnknown {
				t.Errorf("the lost restore %s is write_outcome %q, want %q: %q", lost.name, refused.WriteOutcome, outcomeUnknown, refused.Message)
			}
			for _, want := range []string{
				strings.ToUpper(lost.name),
				"may have been applied there",
				"merges into what landed",
				"pushed twice",
				"db." + testBucket + ".count()",
			} {
				if !strings.Contains(refused.Message, want) {
					t.Errorf("the lost restore %s's message does not say %q: %q", lost.name, want, refused.Message)
				}
			}
			for _, unwanted := range []string{"$inc", "before retrying a push"} {
				if strings.Contains(refused.Message, unwanted) {
					t.Errorf("the lost restore %s's message still carries the push/$inc advice %q: %q", lost.name, unwanted, refused.Message)
				}
			}
		})
	}

	// The write the restore split must not change: a lost push still says the
	// write may have been applied and keeps the read-back advice.
	push := refusalOf(t, tc.frame(coordinator, transport.OpFIFOPush, PushRequest{
		Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": 1},
	}))
	if push.WriteOutcome != outcomeUnknown {
		t.Errorf("the lost push is write_outcome %q, want %q: %q", push.WriteOutcome, outcomeUnknown, push.Message)
	}
	if !strings.Contains(push.Message, "read it back before retrying a push or an $inc") {
		t.Errorf("the lost push lost its read-back advice: %q", push.Message)
	}

	// And the read half: a lost GET stores nothing, so it carries no outcome
	// and the read advice.
	get := refusalOf(t, tc.frame(coordinator, transport.OpGet, FindRequest{
		Database: testDatabase, Bucket: testBucket, Key: "k",
	}))
	if get.WriteOutcome != "" {
		t.Errorf("the lost GET is write_outcome %q, want none: %q", get.WriteOutcome, get.Message)
	}
	if !strings.Contains(get.Message, "nothing changed") {
		t.Errorf("the lost GET lost the read advice: %q", get.Message)
	}
}
