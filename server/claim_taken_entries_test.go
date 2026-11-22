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

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Claim/ack build step 15: BUG-0548's rule for claim (claim-ack-spec 5.4,
// CA-16). A claim the owner applied is answered with its lease and entries
// although its write fell short, in claim's own words: the entries come back
// when the lease ends, and claiming again is safe, the reverse of pop's advice.

// popsAdvice is the phrase of pop's texts a claim's must never carry.
var popsAdvice = []string{"do not pop again", "Do not pop again"}

// requireClaimsWords fails unless message carries want and none of pop's
// advice.
func requireClaimsWords(t *testing.T, what, message, want string) {
	t.Helper()
	if !strings.Contains(message, want) {
		t.Errorf("%s: the message does not say %q: %s", what, want, message)
	}
	for _, advice := range popsAdvice {
		if strings.Contains(message, advice) {
			t.Errorf("%s: the message gives pop's advice %q to a claim: %s", what, advice, message)
		}
	}
}

// requireLeaseAnswered fails unless answer is a claim's answer holding its
// lease and at least one entry, write_outcome applied, with confirmed of
// factor copies.
func requireLeaseAnswered(t *testing.T, what string, answer *transport.Frame, confirmed, factor int32) string {
	t.Helper()
	if answer.Opcode == transport.OpError {
		t.Fatalf("%s was answered an error, and the lease and entries it took are lost with it: %s", what, bson.Raw(answer.Payload))
	}
	reply := bson.Raw(answer.Payload)
	lease, _ := reply.Lookup("lease_id").StringValueOK()
	if items, _ := reply.Lookup("items").ArrayOK(); lease == "" || len(items) == 0 {
		t.Errorf("%s was answered without its lease or entries: %s", what, reply)
	}
	gotConfirmed, gotFactor, _, _ := copiesConfirmedOf(answer.Payload)
	outcome, _ := reply.Lookup("write_outcome").StringValueOK()
	if gotConfirmed != confirmed || gotFactor != factor || outcome != outcomeApplied {
		t.Errorf("%s was answered copies_confirmed %d, replication_factor %d, write_outcome %q; want %d, %d and %q",
			what, gotConfirmed, gotFactor, outcome, confirmed, factor, outcomeApplied)
	}
	message, _ := reply.Lookup("message").StringValueOK()
	return message
}

// TestClaimWhoseWriteConcernTimesOutStillAnswersItsLease is BUG-0548's path 2
// for claim: a replica in sync that confirms nothing holds a claim at concern
// all past the timeout, after its owner logged, applied and shipped it. The
// claim is answered with its lease and entries, two copies of three, applied,
// in claim's words, through the owner and through a member. Guards: claim left
// out of answersWithTakenEntries; claim given pop's text.
func TestClaimWhoseWriteConcernTimesOutStillAnswersItsLease(t *testing.T) {
	tc, _ := leaseCluster(t, nil, nil, func(_ int, c *Cluster) { c.wcTimeout = 2 * time.Second })
	defer tc.stop()
	const owner, held, coordinator = 0, 1, 2
	subjects := make([]clusterPopSubject, 0, len(leaseSubjects))
	for _, kind := range leaseSubjects {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 2, 0)
		subjects = append(subjects, subject)
	}
	defer holdShipsTo(tc.addrs[held])()

	for _, subject := range subjects {
		for _, via := range []int{owner, coordinator} {
			what := fmt.Sprintf("a %s claim at concern all through node %d", subject.kind, via+1)
			message := requireLeaseAnswered(t, what, tc.frame(via, transport.OpQueueClaim, subject.claimRequest(1, 60_000)), 2, 3)
			requireClaimsWords(t, what, message, "return to the queue when the lease ends")
		}
		if pending := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket).Pending; pending != 2 {
			t.Errorf("after two claims the owner holds %d %s entries under a lease, want 2", pending, subject.kind)
		}
	}
}

// TestClaimWhoseOwnershipMovesMidWriteStillAnswersItsLease is BUG-0548's path
// 3 for claim: the bucket's owner changes while a claim is in flight, after
// the owner applied it. The claim is answered with its lease and entry, one
// copy of three, applied, in claim's words. Guards: as above.
func TestClaimWhoseOwnershipMovesMidWriteStillAnswersItsLease(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()
	sc := findCededWriteScenario(t, tc)
	tc.ok(sc.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	target := popTarget{kind: "fifo claim", bucket: sc.moved, op: transport.OpFIFOPop}
	createPopTarget(t, tc, sc.owner, target)
	ownerCluster := tc.nodes[sc.owner].server.core.cluster
	replicaCluster := tc.nodes[sc.replica].server.core.cluster

	reached := make(chan struct{})
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID != ownerCluster.self.ID || op != transport.OpQueueClaim || bucket != sc.moved {
			return
		}
		held := false
		holdOnce.Do(func() { held = true })
		if held {
			close(reached)
			<-release
		}
	}
	afterOwnerViewHook.Store(&hold)
	defer afterOwnerViewHook.Store(nil)
	defer releaseOnce.Do(func() { close(release) })

	payload := mustMarshalBSON(t, bson.M{"database": testDatabase, "bucket": sc.moved, "writeconcern": "one"})
	type claimAnswer struct {
		frame *transport.Frame
		err   error
	}
	answered := make(chan claimAnswer, 1)
	client := tc.dial(sc.owner)
	defer func() { _ = client.Close() }()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		frame, err := client.SendRequest(ctx, transport.OpQueueClaim, payload)
		answered <- claimAnswer{frame: frame, err: err}
	}()
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the claim never reached its owner's fence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, sc.newcomer); err != nil {
		t.Fatalf("join %s: %v", sc.newcomer.ID, err)
	}
	pollUntil(t, "the replica adopts the join", func() bool { return replicaCluster.Epoch() == ownerCluster.Epoch() })
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.moved)); err != nil || owner.ID != sc.newcomer.ID {
		t.Fatalf("setup: %s is owned by %v (%v) after the join, not by the newcomer", sc.moved, owner, err)
	}

	releaseOnce.Do(func() { close(release) })
	var answer claimAnswer
	select {
	case answer = <-answered:
	case <-time.After(30 * time.Second):
		t.Fatal("the held claim was never answered")
	}
	if answer.err != nil {
		t.Fatalf("the held claim's answer did not arrive: %v", answer.err)
	}
	what := "a fifo claim whose bucket moved while it was in flight"
	message := requireLeaseAnswered(t, what, answer.frame, 1, 3)
	requireClaimsWords(t, what, message, "return to the queue when the lease ends")
}

// TestClaimWhoseSyncFailsStillAnswersItsLease is BUG-0548's path 1 for claim,
// on a node that logs nothing: a claim that moved an entry under a lease and
// whose sync then failed is answered with its lease and entry, applied, in
// claim's text 4; a claim that wrote (it dead-lettered) and took nothing is
// answered claim's text 3, an error. Guards: claim left out of the single-node
// branch's outer guard; claim given pop's text.
func TestClaimWhoseSyncFailsStillAnswersItsLease(t *testing.T) {
	s, kv := newCountingServer(t)
	clock := withLeaseClock(s)
	handler := NewHandler(s)
	createDurabilityBucket(t, s, "dying", BucketTypeFIFO)
	handlerSend(t, handler, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "jobs", Data: map[string]interface{}{"n": "job-first"}})
	handlerSend(t, handler, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "dying", Data: map[string]interface{}{"n": "dies"}})

	kv.failWith = errors.New("input/output error")
	answer, err := handler(t.Context(), transport.NewFrame(transport.OpQueueClaim, mustMarshalBSON(t, ClaimRequest{Database: "app", Bucket: "jobs"})))
	kv.failWith = nil
	if err != nil {
		t.Fatalf("claim: handler: %v", err)
	}
	if answer.Opcode == transport.OpError || poppedMarker(answer) != "job-first" {
		t.Fatalf("a claim whose sync failed was answered %s without its entry, which is under a lease nobody holds: %.400s", answer.Opcode, answer.Payload)
	}
	reply := bson.Raw(answer.Payload)
	if outcome, _ := reply.Lookup("write_outcome").StringValueOK(); outcome != outcomeApplied {
		t.Errorf("a claim whose sync failed carries write_outcome %q, want %q", outcome, outcomeApplied)
	}
	if lease, _ := reply.Lookup("lease_id").StringValueOK(); lease == "" {
		t.Errorf("a claim whose sync failed was answered without its lease: %s", reply)
	}
	message, _ := reply.Lookup("message").StringValueOK()
	requireClaimsWords(t, "the answered claim whose sync failed", message, "the lease may not exist")
	for _, want := range []string{"claimed from app.jobs, but", "input/output error", "Check the node's disk"} {
		requireClaimsWords(t, "the answered claim whose sync failed", message, want)
	}

	for claims := 0; claims < store.MaxDeliveries; claims++ {
		handlerSend(t, handler, transport.OpQueueClaim, ClaimRequest{Database: "app", Bucket: "dying", LeaseMs: 1_000})
		clock.advance(2 * time.Second)
	}
	kv.failWith = errors.New("input/output error")
	answer, err = handler(t.Context(), transport.NewFrame(transport.OpQueueClaim, mustMarshalBSON(t, ClaimRequest{Database: "app", Bucket: "dying"})))
	kv.failWith = nil
	if err != nil {
		t.Fatalf("claim: handler: %v", err)
	}
	if dead := bucketStateOf(t, s, "app", "dying").Dead; dead != 1 {
		t.Fatalf("setup: the claim after %d deliveries left %d dead entries, want 1", store.MaxDeliveries, dead)
	}
	refused := refusalOf(t, answer)
	if refused.WriteOutcome != outcomeUnknown {
		t.Errorf("a claim that took nothing and whose sync failed is write_outcome %q, want %q", refused.WriteOutcome, outcomeUnknown)
	}
	for _, want := range []string{"the claim on app.dying", "input/output error", "claim again", "Check the node's disk"} {
		requireClaimsWords(t, "the unanswered claim whose sync failed", refused.Message, want)
	}
}

// TestALostForwardedClaimAdvisesClaimingAgain is BUG-0564 for claim: a claim
// whose forwarded answer is lost tells the caller its entries return when the
// lease ends and to claim again, never pop's "Do not pop again" nor the
// read-back advice of a push. Guard: route claim through pop's advice.
func TestALostForwardedClaimAdvisesClaimingAgain(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	owner := ownerIndexOf(t, tc, testBucket)
	coordinator := (owner + 1) % len(tc.nodes)
	coordCluster := tc.nodes[coordinator].server.core.cluster
	tc.ok(coordinator, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": 1}})

	wrapPeerConn(coordCluster.peerFor(tc.nodes[owner].server.core.cluster.self),
		func(inner peerConn) peerConn { return &lostForwardConn{peerConn: inner} })

	refused := refusalOf(t, tc.frame(coordinator, transport.OpQueueClaim, ClaimRequest{Database: testDatabase, Bucket: testBucket}))
	if refused.Code != int(transport.ErrUnavailable) || refused.WriteOutcome != outcomeUnknown {
		t.Errorf("the lost claim is code %d, write_outcome %q; want UNAVAILABLE and %q: %q", refused.Code, refused.WriteOutcome, outcomeUnknown, refused.Message)
	}
	for _, want := range []string{"when that lease ends", "Claim again: db." + testBucket + ".claim()"} {
		requireClaimsWords(t, "the lost claim", refused.Message, want)
	}
	for _, unwanted := range []string{"$inc", "read it back", "peek"} {
		if strings.Contains(refused.Message, unwanted) {
			t.Errorf("the lost claim's message says %q: %q", unwanted, refused.Message)
		}
	}
}

// TestAClaimThatReturnedLeasesAndTookNothingKeepsItsTimeout: a claim whose
// return step wrote an expired 1 MiB lease back and whose budget then had no
// room to take it answers found false with returned 1. It holds no entry, so a
// write concern it does not meet is answered TIMEOUT, applied, as any write's
// is. Guard: answer a claim that returned entries as one holding them.
func TestAClaimThatReturnedLeasesAndTookNothingKeepsItsTimeout(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, nil, func(_ int, c *Cluster) { c.wcTimeout = 2 * time.Second })
	defer tc.stop()
	const owner, held, coordinator = 0, 1, 2
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 2, 1<<20)
	claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), "the claim whose lease ends")
	clocks[owner].advance(2 * time.Second)
	defer holdShipsTo(tc.addrs[held])()

	what := "a claim at concern all that returned a lease and took nothing"
	refusal := errorAnswerOf(t, tc.frame(coordinator, transport.OpQueueClaim, subject.claimRequest(1, 60_000)), what)
	requireOutcome(t, refusal, what, outcomeApplied, transport.ErrTimeout)
	if st := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket); st.Leases != 0 || st.Entries != 2 {
		t.Fatalf("setup: after the claim the owner holds %d leases and %d entries, want 0 and 2: the claim took something", st.Leases, st.Entries)
	}
}
