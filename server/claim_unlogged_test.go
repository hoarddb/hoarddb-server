package server

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Claim/ack build step 14: the owner answers an empty claim without logging
// it (claim-ack-spec 5.3), and DEAD_LIST is served by the owner.

// counted is an unlabelled counter's value.
func counted(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("reading a counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

// requireUnloggedEmptyClaim sends a claim through node via and requires an
// empty answer naming noun, carrying pending, with no write-outcome member,
// and no node's log moved.
func requireUnloggedEmptyClaim(t *testing.T, tc *testCluster, subject clusterPopSubject, via int, noun string, pending int64, what string) {
	t.Helper()
	before := tc.quiescedSequences(t)
	frame := tc.frame(via, transport.OpQueueClaim, subject.claimRequest(2, 60_000))
	answer := claimAnswerOf(t, frame, what)
	if answer.Found || answer.Message != noun+" is empty" || answer.Pending != pending {
		t.Errorf("%s answered found %v, message %q, pending %d; want found false, %q, pending %d",
			what, answer.Found, answer.Message, answer.Pending, noun+" is empty", pending)
	}
	for _, member := range []string{"write_outcome", "copies_confirmed", "replication_factor"} {
		if _, err := bson.Raw(frame.Payload).LookupErr(member); err == nil {
			t.Errorf("%s answered with %s: nothing was written, so nothing is confirmed: %s", what, member, bson.Raw(frame.Payload))
		}
	}
	if after := tc.quiescedSequences(t); !slices.Equal(after, before) {
		t.Errorf("%s moved the nodes' logs from %v to %v; an empty claim is answered without logging", what, before, after)
	}
}

// requireLoggedClaim sends a claim through node via and requires it to take
// want entries and to move every node's log by exactly one entry.
func requireLoggedClaim(t *testing.T, tc *testCluster, subject clusterPopSubject, via, want int, what string) ClaimResponse {
	t.Helper()
	before := tc.quiescedSequences(t)
	answer := claimAnswerOf(t, tc.frame(via, transport.OpQueueClaim, subject.claimRequest(2, 60_000)), what)
	if !answer.Found || len(answer.Items) != want {
		t.Errorf("%s took %d entries (found %v, %q), want %d", what, len(answer.Items), answer.Found, answer.Message, want)
	}
	subject.settle(t, tc, before)
	after := tc.quiescedSequences(t)
	for index := range tc.nodes {
		if moved := after[index] - before[index]; moved != 1 {
			t.Errorf("%s moved node %d's log by %d entries, want 1", what, index+1, moved)
		}
	}
	return answer
}

// TestEmptyClaimLogsNothing (CA-15): through the owner and through a member,
// no node's log moves for an empty claim in either idle state (no lease; every
// entry claimed under a live lease), and the answer carries no write outcome.
// A claim after an acknowledged push takes it; a claim once a lease has ended,
// and one after a lease granted while the owner's clock was hours ahead, are
// logged and take the returned entries; an empty hash bucket answers claim's
// type refusal. Guards: remove the branch; narrow it to Leases == 0; drop the
// forward-jump guard's seek; drop the type check.
func TestEmptyClaimLogsNothing(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	for _, kind := range leaseSubjects {
		subject := kind.createOn(t, tc, owner)
		noun := "queue"
		if kind.bucketType == BucketTypeHeap {
			noun = "heap"
		}
		for _, via := range []int{owner, member} {
			requireUnloggedEmptyClaim(t, tc, subject, via, noun, 0, fmt.Sprintf("a %s claim with no lease through node %d", subject.kind, via+1))
		}
		subject.push(t, tc, owner, 1, 2, 0)
		requireLoggedClaim(t, tc, subject, member, 2, subject.kind+" claim after an acknowledged push")
		for _, via := range []int{owner, member} {
			requireUnloggedEmptyClaim(t, tc, subject, via, noun, 2, fmt.Sprintf("a %s claim while every entry is under a live lease, through node %d", subject.kind, via+1))
		}

		clocks[owner].advance(61 * time.Second)
		returned := requireLoggedClaim(t, tc, subject, member, 2, subject.kind+" claim once the lease has ended")
		leaseAnswerOf(t, tc.frame(owner, transport.OpQueueAck, subject.leaseRequest(returned.LeaseID, nil)), "the ack of the returned entries")

		subject.push(t, tc, owner, 3, 1, 0)
		clocks[owner].advance(2 * time.Hour)
		requireLoggedClaim(t, tc, subject, owner, 1, subject.kind+" claim while the owner's clock is two hours ahead")
		clocks[owner].advance(-2 * time.Hour)
		requireLoggedClaim(t, tc, subject, member, 1, subject.kind+" claim after the owner's clock is corrected")
		subject.requireIdenticalCopies(t, tc, subject.kind+" after the empty claims")
	}

	hash := bucketOwnedBy(t, tc, owner, "claimhash")
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: hash, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, hash, len(tc.nodes))
	refused := tc.frame(member, transport.OpQueueClaim, bson.M{"database": testDatabase, "bucket": hash})
	refusal := decodeErrorFrame(t, refused, "a claim of a hash bucket")
	if refusal.Code != 4 || !strings.Contains(refusal.Message, "claim is not defined for hash buckets") {
		t.Errorf("a claim of an empty hash bucket answered %+v; want claim's type refusal as INVALID_ARGUMENT, not an empty queue", refusal)
	}
}

// TestEmptyClaimIsCountedOnceOnTheOwner: an unlogged empty claim moves
// hoarddb_queue_empty_claims_unlogged_total by one, through the owner and
// through a member. The three nodes share one process, so the counter is the
// sum of theirs: a claim counted on two nodes moves it by two. Guard: count at
// the coordinator as well.
func TestEmptyClaimIsCountedOnceOnTheOwner(t *testing.T) {
	tc, _ := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner = 0
	subject := fifoPopSubject.createOn(t, tc, owner)
	for via := range tc.nodes {
		unlogged, claims := counted(t, queueEmptyClaimsUnlogged), counted(t, queueClaims)
		requireUnloggedEmptyClaim(t, tc, subject, via, "queue", 0, fmt.Sprintf("an empty claim through node %d", via+1))
		if moved := counted(t, queueEmptyClaimsUnlogged) - unlogged; moved != 1 {
			t.Errorf("an empty claim through node %d moved the unlogged counter by %v, want 1", via+1, moved)
		}
		if moved := counted(t, queueClaims) - claims; moved != 1 {
			t.Errorf("an empty claim through node %d moved the claims counter by %v, want 1", via+1, moved)
		}
	}
}

// TestDeadListIsServedByTheOwner (CA-24): a DEAD_LIST sent to a replica that
// has not received the owner's latest writes is forwarded to the owner and
// lists what the owner holds. Guard: drop DEAD_LIST from isPeekOpcode, so the
// replica answers from its own copy.
func TestDeadListIsServedByTheOwner(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, lagging = 0, 2
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 1, 0)
	release := holdShipsTo(tc.addrs[lagging])
	defer release()
	for round := 0; round <= store.MaxDeliveries+1; round++ {
		request := subject.claimRequest(1, 1_000)
		request["writeconcern"] = "majority"
		claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, request), fmt.Sprintf("claim %d", round+1))
		clocks[owner].advance(2 * time.Second)
	}
	if dead := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket).Dead; dead != 1 {
		t.Fatalf("setup: the owner holds %d dead entries, want 1", dead)
	}
	if dead := bucketStateOf(t, tc.nodes[lagging].server.core, testDatabase, subject.bucket).Dead; dead != 0 {
		t.Fatalf("setup: the held replica already holds %d dead entries", dead)
	}
	var listed DeadListResponse
	frame := tc.ok(lagging, transport.OpDeadList, bson.M{"database": testDatabase, "bucket": subject.bucket})
	if err := bson.Unmarshal(frame.Payload, &listed); err != nil {
		t.Fatalf("decoding the dead list: %v", err)
	}
	if !listed.Found || len(listed.Items) != 1 {
		t.Errorf("a dead list sent to the lagging replica answered %+v; want the owner's one dead entry", listed)
	}
}

// TestClaimRefusedWhileAMemberLacksTheFeature (claim-ack-spec 10.2, 9.5): while
// a member that has gossiped does not advertise queue_claim, a claim of an
// empty bucket and of a bucket with entries, through the owner and through a
// member, is refused with the text that names the member and the feature, and
// nothing is logged or applied: the unlogged answer is not a way round the
// gate. Guard: drop the claim's row in writeOpcodeFeatures.
func TestClaimRefusedWhileAMemberLacksTheFeature(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	empty := bucketOwnedBy(t, tc, 0, "gatedempty")
	full := bucketOwnedBy(t, tc, 0, "gatedfull")
	for _, bucket := range []string{empty, full} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	}
	tc.ok(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: full, Data: map[string]interface{}{"n": 1}})
	stale := stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) {
		c.memberFeatures[stale] = map[string]bool{featureBucketRF: true, featureMemtableMB: true, featureTypedFields: true}
	})
	for _, bucket := range []string{empty, full} {
		for _, via := range []int{0, 1} {
			what := fmt.Sprintf("a claim of %s through node %d", bucket, via+1)
			logged := []uint64{tc.nodes[0].server.core.wal.LastSequence(), tc.nodes[1].server.core.wal.LastSequence()}
			refusal := errorAnswerOf(t, tc.frame(via, transport.OpQueueClaim, bson.M{"database": testDatabase, "bucket": bucket}), what)
			requireOutcome(t, refusal, what, outcomeNotApplied, transport.ErrUnavailable)
			for _, want := range []string{"QUEUE_CLAIM", `supports "queue_claim"`, `member "` + stale + `"`, "Nothing was changed."} {
				if !strings.Contains(refusal.Message, want) {
					t.Errorf("%s: the refusal %q does not say %q", what, refusal.Message, want)
				}
			}
			for index, before := range logged {
				if now := tc.nodes[index].server.core.wal.LastSequence(); now != before {
					t.Errorf("%s moved node %d's log from %d to %d while a member lacks queue_claim", what, index+1, before, now)
				}
			}
		}
	}
	if st := bucketStateOf(t, tc.nodes[0].server.core, testDatabase, full); st.Entries != 1 || st.Pending != 0 {
		t.Errorf("after the refused claims the owner's bucket holds %d entries and %d claimed, want 1 and 0", st.Entries, st.Pending)
	}
}

// TestAnOlderOwnerRefusesAForwardedClaimBeforeItsLog pins BUG-0561 for the new
// opcode: an owner whose build does not implement QUEUE_CLAIM (0x34) refuses a
// forwarded one not_applied, and neither logs nor queues it for a replica.
func TestAnOlderOwnerRefusesAForwardedClaimBeforeItsLog(t *testing.T) {
	tc, owner, coordinator := forwardedOpcodeCluster(t)
	defer tc.stop()
	ownerCore := tc.nodes[owner].server.core
	tc.quiescedSequences(t)
	logged := ownerCore.wal.LastSequence()

	restore := withholdOpcode(transport.OpQueueClaim)
	resp := forwardToOwner(t, tc, owner, coordinator, transport.OpQueueClaim, bson.M{"database": testDatabase, "bucket": testBucket})
	restore()

	if resp.Success || resp.WriteOutcome != outcomeNotApplied || !strings.Contains(resp.Error, "0x34") {
		t.Errorf("an owner without QUEUE_CLAIM answered success=%t write_outcome=%q error=%q; want a not_applied refusal naming 0x34",
			resp.Success, resp.WriteOutcome, resp.Error)
	}
	if now := ownerCore.wal.LastSequence(); now != logged {
		t.Errorf("the owner's log went from %d to %d for a claim it does not implement", logged, now)
	}
	if queuedPast(ownerCore.cluster, logged+1) {
		t.Errorf("the owner queued an entry past sequence %d for a replica", logged)
	}
}
