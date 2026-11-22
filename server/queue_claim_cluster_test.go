package server

import (
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The lease verbs through a cluster member (claim/ack build step 10): the
// door's refusals, the owner's feature gate and the dead verbs' routing. The
// owner's stamps are step 13's (claim_owner_stamp_test.go).

// leaseRefusalRows are requests the door can refuse, each with the text it must
// carry.
func leaseRefusalRows(bucket string) []struct {
	name    string
	op      transport.Opcode
	request map[string]any
	want    string
} {
	base := func(extra map[string]any) map[string]any {
		request := map[string]any{"database": testDatabase, "bucket": bucket}
		for name, value := range extra {
			request[name] = value
		}
		return request
	}
	lease := strings.Repeat("0", 40)
	return []struct {
		name    string
		op      transport.Opcode
		request map[string]any
		want    string
	}{
		{"claim limit above the maximum", transport.OpQueueClaim, base(map[string]any{"limit": 500}), "500 is above the claim maximum of 100"},
		{"claim limit negative", transport.OpQueueClaim, base(map[string]any{"limit": -2}), "claim({limit: N}) takes a whole number"},
		{"claim lease above the maximum", transport.OpQueueClaim, base(map[string]any{"lease_ms": 7_200_000}), "above the lease maximum of 1h"},
		{"claim lease below the minimum", transport.OpQueueClaim, base(map[string]any{"lease_ms": 10}), "below the lease minimum of 1s"},
		{"ack with a malformed lease id", transport.OpQueueAck, base(map[string]any{"lease_id": "nope"}), "is not a lease id"},
		{"release with a malformed lease id", transport.OpQueueRelease, base(map[string]any{"lease_id": strings.Repeat("g", 40)}), "is not a lease id"},
		{"extend with no length", transport.OpQueueExtend, base(map[string]any{"lease_id": lease}), "extend needs a new length"},
		{"ack with 101 item ids", transport.OpQueueAck, base(map[string]any{"lease_id": lease, "item_ids": make([]string, 101)}), "ack names 101 item ids and takes at most 100"},
	}
}

// TestClaimAndLeaseAreRefusedAtTheDoorOnACluster: a member refuses what the
// bounds forbid before anything is logged: INVALID_ARGUMENT, not_applied, and no
// node's log moves.
func TestClaimAndLeaseAreRefusedAtTheDoorOnACluster(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 3, 0)
	for _, refused := range leaseRefusalRows(subject.bucket) {
		before := tc.quiescedSequences(t)
		refusal := errorAnswerOf(t, tc.frame(member, refused.op, refused.request), refused.name)
		requireOutcome(t, refusal, refused.name, outcomeNotApplied, transport.ErrInvalidArgument)
		if !strings.Contains(refusal.Message, refused.want) {
			t.Errorf("%s: message %q lacks %q", refused.name, refusal.Message, refused.want)
		}
		if after := tc.quiescedSequences(t); !slices.Equal(after, before) {
			t.Errorf("%s moved the nodes' logs from %v to %v; what the door can refuse must never be logged", refused.name, before, after)
		}
	}
}

// TestItemIDThatIsADocumentIDIsRefusedThroughAMember: the item id's form depends
// on the bucket's type, which a coordinator may not hold, so the owner's handler
// refuses it, with the text that says these are claim's handles.
func TestItemIDThatIsADocumentIDIsRefusedThroughAMember(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 1, 0)
	request := map[string]any{"database": testDatabase, "bucket": subject.bucket, "lease_id": strings.Repeat("0", 40), "item_ids": []string{"job-7"}}
	refusal := errorAnswerOf(t, tc.frame(member, transport.OpQueueAck, request), "an ack by document id")
	requireOutcome(t, refusal, "an ack by document id", outcomeNotApplied, transport.ErrInvalidArgument)
	for _, want := range []string{`item id "job-7" is not a handle claim returned`, "not the documents' _id_ values"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("message %q lacks %q", refusal.Message, want)
		}
	}
}

// TestAClaimThroughAMemberIsAppliedAlikeOnEveryCopy: a claim with no stamps,
// sent through a member, was refused as malformed on every copy until the
// owner stamped it (step 13). Now the owner stamps it, every copy applies it
// alike, no copy is reported diverged, and no apply is owed.
func TestAClaimThroughAMemberIsAppliedAlikeOnEveryCopy(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 3, 0)
	divergencesBefore := divergencesCounted(t)

	request := map[string]any{"database": testDatabase, "bucket": subject.bucket, "limit": 2, "writeconcern": "all"}
	if answer := claimAnswerOf(t, tc.frame(member, transport.OpQueueClaim, request), "a claim through a member"); len(answer.Items) != 2 {
		t.Fatalf("a claim({limit: 2}) through a member took %d entries, want 2", len(answer.Items))
	}
	subject.requireIdenticalCopies(t, tc, "after a claim through a member")
	if got := divergencesCounted(t) - divergencesBefore; got != 0 {
		t.Errorf("the claim moved the replica divergence counter by %v", got)
	}
	for index, node := range tc.nodes {
		if owed := node.server.core.owed.count(); owed != 0 {
			t.Errorf("node %d owes %d applies after a claim every copy applied", index+1, owed)
		}
	}
}

// TestLeaseVerbsAreRefusedWhileAMemberLacksQueueClaim: an owner refuses each of
// the four before its log while a member that has gossiped does not advertise
// queue_claim (claim-ack-spec 10.2), and nothing is logged.
func TestLeaseVerbsAreRefusedWhileAMemberLacksQueueClaim(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "gated")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	stale := stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) {
		c.memberFeatures[stale] = map[string]bool{featureBucketRF: true, featureMemtableMB: true, featureTypedFields: true}
	})
	lease := strings.Repeat("0", 40)
	for op, request := range map[transport.Opcode]map[string]any{
		transport.OpQueueClaim:   {"database": testDatabase, "bucket": bucket},
		transport.OpQueueAck:     {"database": testDatabase, "bucket": bucket, "lease_id": lease},
		transport.OpQueueRelease: {"database": testDatabase, "bucket": bucket, "lease_id": lease},
		transport.OpQueueExtend:  {"database": testDatabase, "bucket": bucket, "lease_id": lease, "lease_ms": 5000},
	} {
		logged := tc.nodes[0].server.core.wal.LastSequence()
		refusal := errorAnswerOf(t, tc.frame(0, op, request), op.String())
		requireOutcome(t, refusal, op.String(), outcomeNotApplied, transport.ErrUnavailable)
		for _, want := range []string{op.String(), `supports "queue_claim"`, `member "` + stale + `"`, "Nothing was changed."} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("%s: the refusal %q does not say %q", op, refusal.Message, want)
			}
		}
		if now := tc.nodes[0].server.core.wal.LastSequence(); now != logged {
			t.Errorf("%s moved the owner's log from %d to %d while a member lacks queue_claim", op, logged, now)
		}
	}
}

// TestDeadOptionsAreRefusedAtTheDoorOnACluster: neither or both of dead_id and
// all, a dead_id that is not hex and a list limit out of bounds are refused by a
// member before anything is logged, and a DEAD_RESTORE a client sends is refused
// before routing and never logged.
func TestDeadOptionsAreRefusedAtTheDoorOnACluster(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	base := func(extra map[string]any) map[string]any {
		request := map[string]any{"database": testDatabase, "bucket": subject.bucket}
		for name, value := range extra {
			request[name] = value
		}
		return request
	}
	rows := []struct {
		name    string
		op      transport.Opcode
		request map[string]any
		code    transport.ErrorCode
		want    string
	}{
		{"requeue of neither", transport.OpDeadRequeue, base(nil), transport.ErrInvalidArgument, "names one dead entry or all of them, not both and not neither"},
		{"requeue of both", transport.OpDeadRequeue, base(map[string]any{"dead_id": "00", "all": true}), transport.ErrInvalidArgument, "not both and not neither"},
		{"discard of a dead_id that is not hex", transport.OpDeadDiscard, base(map[string]any{"dead_id": "zz"}), transport.ErrInvalidArgument, "is not a dead id"},
		{"dead list above the limit", transport.OpDeadList, base(map[string]any{"limit": 5000}), transport.ErrInvalidArgument, "above the dead list maximum of 1000"},
		{"a client DEAD_RESTORE", transport.OpDeadRestore, base(map[string]any{"data": map[string]any{"n": 1}, "deliveries": 1, "reason": "collided"}), transport.ErrPermissionDenied, "restore-only: DEAD_RESTORE is sent by a restore"},
	}
	for _, refused := range rows {
		before := tc.quiescedSequences(t)
		refusal := errorAnswerOf(t, tc.frame(member, refused.op, refused.request), refused.name)
		if transport.ErrorCode(refusal.Code) != refused.code || !strings.Contains(refusal.Message, refused.want) {
			t.Errorf("%s: code %d, message %q; want code %d and %q", refused.name, refusal.Code, refusal.Message, refused.code, refused.want)
		}
		if refused.op != transport.OpDeadList {
			requireOutcome(t, refusal, refused.name, outcomeNotApplied, refused.code)
		}
		if after := tc.quiescedSequences(t); !slices.Equal(after, before) {
			t.Errorf("%s moved the nodes' logs from %v to %v; it must never be logged", refused.name, before, after)
		}
	}
}

// TestDeadListThroughAMemberIsAnsweredByTheOwner: a list sent to a member that
// is not the owner is forwarded to it and answers in band, with nothing logged.
func TestDeadListThroughAMemberIsAnsweredByTheOwner(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	before := tc.quiescedSequences(t)
	answer := tc.ok(member, transport.OpDeadList, map[string]any{"database": testDatabase, "bucket": subject.bucket})
	if answer.Opcode != transport.OpDeadList {
		t.Fatalf("a list was answered %s", answer.Opcode)
	}
	if after := tc.quiescedSequences(t); !slices.Equal(after, before) {
		t.Errorf("a list moved the logs from %v to %v", before, after)
	}
}
