package server

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Claim/ack build step 13: on a cluster the owner stamps lease_now (its own
// clock), lease_nonce and the result ceiling on a lease verb or a pop before
// the entry is logged, overwriting whatever a client sent, so every copy
// decodes the same values and composes the same lease.

// leaseSubjects are the three bucket kinds that issue leases.
var leaseSubjects = []clusterPopSubject{fifoPopSubject, lifoPopSubject, heapPopSubject}

// leaseClusterBase is the owner's lease clock at the start of every row.
const leaseClusterBase = int64(1_800_000_000_000)

// leaseCluster is ceilingCluster with a lease clock per node that only the test
// moves, each at leaseClusterBase plus its offset. The clocks are set before a
// node starts, and again on a restart.
func leaseCluster(t *testing.T, ceilings map[int]int, offsets []time.Duration, tune func(int, *Cluster)) (*testCluster, []*leaseClockAt) {
	t.Helper()
	return leaseClusterOf(t, 3, func(index int, cfg *config.Config) {
		if ceiling, set := ceilings[index]; set {
			setQueryCeiling(cfg, ceiling)
		}
	}, offsets, tune)
}

// leaseClusterOf is count nodes at factor 3, each configured by configure,
// with its own lease clock as leaseCluster gives it, and the test database.
func leaseClusterOf(t *testing.T, count int, configure func(int, *config.Config), offsets []time.Duration, tune func(int, *Cluster)) (*testCluster, []*leaseClockAt) {
	t.Helper()
	clocks := make([]*leaseClockAt, count)
	for index := range clocks {
		clocks[index] = &leaseClockAt{}
		offset := time.Duration(0)
		if index < len(offsets) {
			offset = offsets[index]
		}
		clocks[index].millis.Store(leaseClusterBase + offset.Milliseconds())
	}
	tc := newTestClusterTuned(t, count, 3, configure, func(index int, c *Cluster) {
		c.s.leaseClock = clocks[index].now
		if tune != nil {
			tune(index, c)
		}
	})
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	return tc, clocks
}

// skewedByMinutes puts node 2's clock seven minutes ahead of the owner's (node
// 1) and node 3's seven minutes behind.
var skewedByMinutes = []time.Duration{0, 7 * time.Minute, -7 * time.Minute}

// claimRequest is a claim of limit entries of the subject under a lease of
// leaseMs, at concern all.
func (s clusterPopSubject) claimRequest(limit int, leaseMs int64) bson.M {
	return bson.M{"database": testDatabase, "bucket": s.bucket, "limit": limit, "lease_ms": leaseMs, "writeconcern": "all"}
}

// leaseRequest is an ack, release or extend of lease on the subject.
func (s clusterPopSubject) leaseRequest(lease string, extra bson.M) bson.M {
	request := bson.M{"database": testDatabase, "bucket": s.bucket, "lease_id": lease, "writeconcern": "all"}
	for name, value := range extra {
		request[name] = value
	}
	return request
}

// claimAnswerOf decodes a claim's answer, failing on an error frame.
func claimAnswerOf(t *testing.T, frame *transport.Frame, what string) ClaimResponse {
	t.Helper()
	if frame.Opcode == transport.OpError {
		t.Fatalf("%s was refused: %s", what, bson.Raw(frame.Payload))
	}
	var answer ClaimResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("%s: decoding the answer: %v", what, err)
	}
	return answer
}

// leaseAnswerOf decodes an ack's, release's or extend's answer.
func leaseAnswerOf(t *testing.T, frame *transport.Frame, what string) LeaseResponse {
	t.Helper()
	if frame.Opcode == transport.OpError {
		t.Fatalf("%s was refused: %s", what, bson.Raw(frame.Payload))
	}
	var answer LeaseResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("%s: decoding the answer: %v", what, err)
	}
	return answer
}

// leaseBytesOf is a lease id a claim answered, decoded.
func leaseBytesOf(t *testing.T, lease string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(lease)
	if err != nil || len(raw) != store.LeaseIDBytes {
		t.Fatalf("lease id %q is not %d bytes of hex", lease, store.LeaseIDBytes)
	}
	return raw
}

// leaseDeadlineOf is the deadline, in ms, a lease id carries (bytes 4 to 12).
func leaseDeadlineOf(t *testing.T, lease string) int64 {
	t.Helper()
	return int64(binary.BigEndian.Uint64(leaseBytesOf(t, lease)[4:12]))
}

// leaseNonceOf is the nonce a lease id carries (bytes 12 to 20).
func leaseNonceOf(t *testing.T, lease string) []byte {
	t.Helper()
	return leaseBytesOf(t, lease)[12:]
}

// loggedOf is the payload of every entry of op on the subject's bucket that
// the node at index logged above sequence after.
func (s clusterPopSubject) loggedOf(t *testing.T, tc *testCluster, index int, after uint64, op transport.Opcode) []bson.Raw {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(after + 1)
	if err != nil {
		t.Fatalf("node %d: reading its log above %d: %v", index+1, after, err)
	}
	var payloads []bson.Raw
	for _, entry := range entries {
		if entry.Bucket == s.bucket && transport.Opcode(entry.Op) == op {
			payloads = append(payloads, bson.Raw(entry.Data))
		}
	}
	return payloads
}

// membersNamed is every member of payload whose name is name in any letter
// case: the decoder matches each, so each is a value an applier could read.
func membersNamed(t *testing.T, payload bson.Raw, name string) []bson.RawValue {
	t.Helper()
	elements, err := payload.Elements()
	if err != nil {
		t.Fatalf("reading a logged payload: %v", err)
	}
	var values []bson.RawValue
	for _, element := range elements {
		if strings.EqualFold(element.Key(), name) {
			values = append(values, element.Value())
		}
	}
	return values
}

// stampedLeaseNow is the one lease_now a logged payload carries, failing when
// it carries none, several, or one that is not an integer.
func stampedLeaseNow(t *testing.T, payload bson.Raw, what string) int64 {
	t.Helper()
	values := membersNamed(t, payload, "lease_now")
	if len(values) != 1 {
		t.Fatalf("%s was logged with %d lease_now members, want exactly one: %s", what, len(values), payload)
	}
	now, ok := values[0].AsInt64OK()
	if !ok {
		t.Fatalf("%s was logged with lease_now %v, not an integer", what, values[0])
	}
	return now
}

// stampedNonce is the one lease_nonce a logged claim carries.
func stampedNonce(t *testing.T, payload bson.Raw, what string) []byte {
	t.Helper()
	values := membersNamed(t, payload, "lease_nonce")
	if len(values) != 1 {
		t.Fatalf("%s was logged with %d lease_nonce members, want exactly one: %s", what, len(values), payload)
	}
	_, nonce, ok := values[0].BinaryOK()
	if !ok || len(nonce) != 8 {
		t.Fatalf("%s was logged with lease_nonce %v, want 8 bytes of binary", what, values[0])
	}
	return nonce
}

// copyOn is everything of the subject's bucket the node at index holds that
// must be the same on every copy (bucketCopyOf).
func (s clusterPopSubject) copyOn(t *testing.T, tc *testCluster, index int) string {
	t.Helper()
	return bucketCopyOf(t, tc.nodes[index].server.core, s.bucket)
}

// bucketCopyOf is everything of bucket that server holds that must be the
// same on every copy: each key of its range but the state document, with its
// value, and the state's queue and delivery fields. The state's applied_seq
// and lineage are each node's own.
func bucketCopyOf(t *testing.T, server *Server, bucket string) string {
	t.Helper()
	handle, err := server.resolveBucket(testDatabase, bucket)
	if err != nil {
		t.Fatalf("%s no longer serves %s: %v", server.config.Server.NodeID, bucket, err)
	}
	id := handle.id.Load()
	prefix := storage.Scope(handle.slot, handle.databaseID, id)
	snap := server.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.Scope(handle.slot, handle.databaseID, id+1)})
	defer it.Close()
	var held strings.Builder
	for it.Seek(nil); it.Valid(); it.Next() {
		key := it.Key()[len(prefix):]
		if key[0] == storage.NSState {
			continue
		}
		stored, err := it.Value()
		if err != nil {
			t.Fatalf("%s: reading %x of %s: %v", server.config.Server.NodeID, key, bucket, err)
		}
		fmt.Fprintf(&held, "%x=%s\n", key, canonicalValue(stored))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("%s: walking %s: %v", server.config.Server.NodeID, bucket, err)
	}
	st := bucketStateOf(t, server, testDatabase, bucket)
	fmt.Fprintf(&held, "head=%d tail=%d next=%d entries=%d bytes=%d incarnation=%d leases=%d pending=%d pending_bytes=%d returned=%d dead=%d dead_bytes=%d dead_seq=%d lease_clock=%d resume_at=%d resume_below=%d\n",
		st.Head, st.Tail, st.Next, st.Entries, st.LogicalBytes, st.Incarnation, st.Leases, st.Pending, st.PendingBytes,
		st.Returned, st.Dead, st.DeadBytes, st.DeadSeq, st.LeaseClock, st.ResumeAt, st.ResumeBelow)
	return held.String()
}

// canonicalValue is a stored value with any document in it written with its
// members in name order: each node encodes a pushed document from the map the
// request decoded into, so two copies of one entry may order its members
// differently. A value is a document, or one byte and a document (a claimed
// entry), or neither (a lease header or a count), which is compared as it is.
func canonicalValue(stored []byte) string {
	for _, offset := range []int{0, 1} {
		if len(stored) < offset+5 || int(binary.LittleEndian.Uint32(stored[offset:])) != len(stored)-offset {
			continue
		}
		var document bson.M
		if bson.Unmarshal(stored[offset:], &document) == nil {
			return fmt.Sprintf("%x%v", stored[:offset], document)
		}
	}
	return fmt.Sprintf("%x", stored)
}

// requireIdenticalCopies waits up to 20 s for every node's copy of the subject
// to equal the first node's, key for key and field for field, and fails naming
// the first difference when they never do.
func (s clusterPopSubject) requireIdenticalCopies(t *testing.T, tc *testCluster, when string) string {
	t.Helper()
	every := make([]int, len(tc.nodes))
	for index := range every {
		every[index] = index
	}
	return s.requireIdenticalCopiesOn(t, tc, every, when)
}

// requireIdenticalCopiesOn is requireIdenticalCopies over the nodes at indices.
func (s clusterPopSubject) requireIdenticalCopiesOn(t *testing.T, tc *testCluster, indices []int, when string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		copies := make([]string, len(indices))
		identical := true
		for at, index := range indices {
			copies[at] = s.copyOn(t, tc, index)
			identical = identical && copies[at] == copies[0]
		}
		if identical {
			return copies[0]
		}
		if time.Now().After(deadline) {
			for at := 1; at < len(copies); at++ {
				if copies[at] != copies[0] {
					t.Fatalf("%s: node %d's copy of %s differs from node %d's:\nnode %d:\n%s\nnode %d:\n%s",
						when, indices[at]+1, s.bucket, indices[0]+1, indices[0]+1, copies[0], indices[at]+1, copies[at])
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// settle waits until every node has logged and applied the write that moved
// the logs past before.
func (s clusterPopSubject) settle(t *testing.T, tc *testCluster, before []uint64) {
	t.Helper()
	tc.waitEveryNodeLogged(t, before)
	tc.waitAppliedPast(t, s.bucket, before)
}

// TestClaimIsOneWALEntry: a claim({limit: 2}) through the owner, and again
// through a member that forwards it, moves every node's log by exactly one
// entry, and every copy holds the same two entries fewer. Until step 13 a
// logged claim carried no lease_now and was refused as malformed. Guard: the
// claim is logged as two entries.
func TestClaimIsOneWALEntry(t *testing.T) {
	tc, _ := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	for _, kind := range leaseSubjects {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 6, 0)
		for round, via := range []int{owner, member} {
			what := fmt.Sprintf("a %s claim({limit: 2}) through node %d", subject.kind, via+1)
			before := tc.quiescedSequences(t)
			if answer := claimAnswerOf(t, tc.frame(via, transport.OpQueueClaim, subject.claimRequest(2, 30_000)), what); len(answer.Items) != 2 {
				t.Errorf("%s answered %d items, want 2", what, len(answer.Items))
			}
			subject.settle(t, tc, before)
			after := tc.quiescedSequences(t)
			for index := range tc.nodes {
				if moved := after[index] - before[index]; moved != 1 {
					t.Errorf("%s moved node %d's log by %d entries, want exactly 1", what, index+1, moved)
				}
			}
			if held, want := subject.requireCopiesAgree(t, tc, what), 6-2*(round+1); len(held) != want {
				t.Fatalf("after %s every copy holds %d claimable entries, want %d", what, len(held), want)
			}
		}
		subject.requireIdenticalCopies(t, tc, subject.kind+" after two claims")
	}
}

// requireClaimStampedByOwner sends a claim through node via while every node's
// clock differs from the owner's by minutes, and requires the answer's lease
// to end at the owner's clock plus the lease, every node to log the claim with
// the owner's lease_now, one 8-byte lease_nonce that is the one inside the
// lease id, and the owner's result ceiling, and every copy to end identical.
func requireClaimStampedByOwner(t *testing.T, tc *testCluster, clocks []*leaseClockAt, subject clusterPopSubject, owner, via int) {
	t.Helper()
	what := fmt.Sprintf("a %s claim through node %d", subject.kind, via+1)
	ownerNow := clocks[owner].now().UnixMilli()
	before := tc.quiescedSequences(t)
	answer := claimAnswerOf(t, tc.frame(via, transport.OpQueueClaim, subject.claimRequest(2, 30_000)), what)
	if len(answer.Items) != 2 {
		t.Fatalf("%s answered %d items, want 2", what, len(answer.Items))
	}
	if got, want := answer.LeaseDeadline.UnixMilli(), ownerNow+30_000; got != want || leaseDeadlineOf(t, answer.LeaseID) != want {
		t.Errorf("%s answered lease_deadline %d and a lease id ending at %d; want the owner's clock plus 30 s = %d",
			what, got, leaseDeadlineOf(t, answer.LeaseID), want)
	}
	subject.settle(t, tc, before)
	leaseNonce := leaseNonceOf(t, answer.LeaseID)
	for index := range tc.nodes {
		logged := subject.loggedOf(t, tc, index, before[index], transport.OpQueueClaim)
		if len(logged) != 1 {
			t.Fatalf("node %d logged %d claims of %s, want 1", index+1, len(logged), subject.bucket)
		}
		where := fmt.Sprintf("%s, as node %d logged it", what, index+1)
		if now := stampedLeaseNow(t, logged[0], where); now != ownerNow {
			t.Errorf("%s carries lease_now %d, want the owner's clock %d (node %d's is %d)", where, now, ownerNow, via+1, clocks[via].now().UnixMilli())
		}
		if nonce := stampedNonce(t, logged[0], where); !slices.Equal(nonce, leaseNonce) {
			t.Errorf("%s carries lease_nonce %x, and the lease id it answered %s holds %x", where, nonce, answer.LeaseID, leaseNonce)
		}
		if ceilings := stampedCeilings(t, logged[0]); len(ceilings) != 1 {
			t.Errorf("%s carries result_ceiling %v, want the owner's alone", where, ceilings)
		}
	}
	subject.requireIdenticalCopies(t, tc, what)
}

// TestClaimStampsLeaseAndNowAtTheOwner: a claim sent to its owner is logged
// with the owner's clock and a nonce the owner drew, and every copy composes
// the same lease. Guards: the stamp is not written (the copies refuse a claim
// with no lease_now); the coordinator stamps with its own clock.
func TestClaimStampsLeaseAndNowAtTheOwner(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, skewedByMinutes, nil)
	defer tc.stop()
	const owner = 0
	for _, kind := range leaseSubjects {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 4, 0)
		requireClaimStampedByOwner(t, tc, clocks, subject, owner, owner)
	}
}

// TestForwardedClaimIsStampedByTheOwner (CA-18): a claim sent to a member that
// does not own the bucket, whose clock is minutes ahead or behind, reaches the
// owner through the replication push and is stamped there. Guard: stamp at the
// coordinator (the door), with the coordinator's clock.
func TestForwardedClaimIsStampedByTheOwner(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, skewedByMinutes, nil)
	defer tc.stop()
	const owner = 0
	for _, kind := range leaseSubjects {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 4, 0)
		for _, via := range []int{1, 2} {
			requireClaimStampedByOwner(t, tc, clocks, subject, owner, via)
		}
	}
}

// TestSkewedCoordinatorsDoNotMoveLeaseDeadlines: three nodes whose clocks
// differ by minutes. A claim through each ends its lease at the owner's clock
// plus the lease; an ack through the node seven minutes ahead, sent before the
// owner's deadline, acknowledges; an extend through the node seven minutes
// behind ends at the owner's clock plus the new length; and an ack after the
// owner's deadline, through the node behind, answers expired. Guard: stamp
// lease_now with the coordinator's clock.
func TestSkewedCoordinatorsDoNotMoveLeaseDeadlines(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, skewedByMinutes, nil)
	defer tc.stop()
	const owner, ahead, behind = 0, 1, 2
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 6, 0)

	leases := make([]string, 3)
	for via := range tc.nodes {
		what := fmt.Sprintf("a claim through node %d", via+1)
		answer := claimAnswerOf(t, tc.frame(via, transport.OpQueueClaim, subject.claimRequest(1, 10_000)), what)
		if got, want := leaseDeadlineOf(t, answer.LeaseID), clocks[owner].now().UnixMilli()+10_000; got != want {
			t.Errorf("%s ends its lease at %d, want the owner's clock plus 10 s = %d (a skew of %d ms)", what, got, want, got-want)
		}
		leases[via] = answer.LeaseID
	}

	clocks[owner].advance(5 * time.Second)
	acked := leaseAnswerOf(t, tc.frame(ahead, transport.OpQueueAck, subject.leaseRequest(leases[0], nil)), "an ack through the node ahead")
	if !acked.Success || acked.LeaseState != "acked" {
		t.Errorf("an ack through the node seven minutes ahead, 5 s into a 10 s lease by the owner's clock, answered %+v; want acked", acked)
	}
	extended := leaseAnswerOf(t, tc.frame(behind, transport.OpQueueExtend, subject.leaseRequest(leases[1], bson.M{"lease_ms": 60_000})), "an extend through the node behind")
	if want := clocks[owner].now().UnixMilli() + 60_000; !extended.Success || extended.LeaseDeadline.UnixMilli() != want {
		t.Errorf("an extend through the node seven minutes behind answered %+v; want success and a deadline of the owner's clock plus 60 s = %d", extended, want)
	}
	clocks[owner].advance(10 * time.Second)
	late := leaseAnswerOf(t, tc.frame(behind, transport.OpQueueAck, subject.leaseRequest(leases[2], nil)), "a late ack through the node behind")
	if late.Success || late.LeaseState != "expired" {
		t.Errorf("an ack through the node seven minutes behind, 15 s into a 10 s lease by the owner's clock, answered %+v; want expired", late)
	}
	subject.requireIdenticalCopies(t, tc, "after the skewed claims")
}

// TestClaimCeilingIsStampedAtTheOwner is pop-n's three-node ceiling test for
// claim: the owner and node 3 run a 1 MiB result ceiling, node 2 4 MiB. A claim
// of eleven 400 KiB entries through node 2 stops short at the owner's ceiling
// and takes two on every copy, though node 2's own ceiling and the delivery
// budget would let it take three; the next, through node 3, takes two more;
// every logged claim carries the owner's ceiling alone; no copy is reported
// diverged. The owner's ceiling is the small one because a claim of 400 KiB
// entries stops at the delivery budget (about 2.4 MiB) before any ceiling above
// it. Guard: the handler reads its own node's ceiling.
func TestClaimCeilingIsStampedAtTheOwner(t *testing.T) {
	const owner, large, small = 0, 1, 2
	tc, _ := leaseCluster(t, map[int]int{owner: 1 << 20, large: 4 << 20, small: 1 << 20}, nil, nil)
	defer tc.stop()
	divergedBefore := divergencesCounted(t)
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 8, popPad)
	loggedBefore := tc.quiescedSequences(t)

	for _, row := range []struct{ via, left int }{{large, 6}, {small, 4}} {
		what := fmt.Sprintf("a claim({limit: 11}) of 400 KiB entries through node %d", row.via+1)
		before := tc.quiescedSequences(t)
		if answer := claimAnswerOf(t, tc.frame(row.via, transport.OpQueueClaim, subject.claimRequest(11, 60_000)), what); len(answer.Items) != 2 {
			t.Fatalf("%s took %d entries, want 2: it stops at the owner's 1 MiB", what, len(answer.Items))
		}
		subject.settle(t, tc, before)
		if held := subject.requireCopiesAgree(t, tc, what); len(held) != row.left {
			t.Fatalf("after %s every copy holds %d claimable entries, want %d", what, len(held), row.left)
		}
	}
	subject.requireIdenticalCopies(t, tc, "after the claims at the owner's ceiling")
	for index := range tc.nodes {
		for _, payload := range subject.loggedOf(t, tc, index, loggedBefore[index], transport.OpQueueClaim) {
			if ceilings := stampedCeilings(t, payload); !slices.Equal(ceilings, []int64{1 << 20}) {
				t.Errorf("node %d logged a claim carrying result_ceiling %v, want the owner's [%d] alone", index+1, ceilings, 1<<20)
			}
		}
	}
	if moved := divergencesCounted(t) - divergedBefore; moved != 0 {
		t.Errorf("%v copies were reported diverged; a claim judged by its owner's stamp is decided alike on every copy", moved)
	}
}

// TestClaimReplicasEndIdentical: claims, an extend, a partial ack, an expiry
// and its return by a pop, a release and a claim of what it returned, sent
// through different members while the replicas' clocks are minutes off, leave
// every copy with the same keys and the same delivery state. Guard: a lease
// verb reads its node's clock on a logged entry.
func TestClaimReplicasEndIdentical(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, skewedByMinutes, nil)
	defer tc.stop()
	const owner = 0
	for _, kind := range leaseSubjects {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 8, 0)
		claimAnswerOf(t, tc.frame(1, transport.OpQueueClaim, subject.claimRequest(2, 1_000)), "a short claim")
		long := claimAnswerOf(t, tc.frame(2, transport.OpQueueClaim, subject.claimRequest(2, 60_000)), "a long claim")
		leaseAnswerOf(t, tc.frame(0, transport.OpQueueExtend, subject.leaseRequest(long.LeaseID, bson.M{"lease_ms": 120_000})), "an extend")
		leaseAnswerOf(t, tc.frame(1, transport.OpQueueAck, subject.leaseRequest(long.LeaseID, bson.M{"item_ids": long.ItemIDs[:1]})), "a partial ack")
		clocks[owner].advance(2 * time.Second)
		popped := poppedNumbers(t, tc.frame(2, subject.popOp(), subject.pop(1)), "a pop after the short lease ended")
		if len(popped) != 1 {
			t.Fatalf("a %s pop after the short lease ended took %v", subject.kind, popped)
		}
		released := claimAnswerOf(t, tc.frame(0, transport.OpQueueClaim, subject.claimRequest(3, 30_000)), "a claim to release")
		leaseAnswerOf(t, tc.frame(1, transport.OpQueueRelease, subject.leaseRequest(released.LeaseID, nil)), "a release")
		claimAnswerOf(t, tc.frame(2, transport.OpQueueClaim, subject.claimRequest(2, 30_000)), "a claim of the released entries")
		held := subject.requireIdenticalCopies(t, tc, subject.kind+" after claims, expiry, return and release")
		if !strings.Contains(held, "leases=") {
			t.Fatalf("the %s copy has no state: %s", subject.kind, held)
		}
	}
}

// TestDeadLetteringIsIdenticalOnEveryCopy: an entry claimed and left to expire
// until its returns pass store.MaxDeliveries is dead-lettered on every copy
// alike, with the same dead key, the same dead_seq and the same state, though
// the replicas' clocks are minutes off. Guard: a lease verb reads its node's
// clock on a logged entry.
func TestDeadLetteringIsIdenticalOnEveryCopy(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, skewedByMinutes, nil)
	defer tc.stop()
	const owner = 0
	for _, kind := range []clusterPopSubject{fifoPopSubject, heapPopSubject} {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 2, 0)
		for round := 0; round <= store.MaxDeliveries; round++ {
			claimAnswerOf(t, tc.frame(round%3, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), fmt.Sprintf("claim %d", round+1))
			clocks[owner].advance(2 * time.Second)
		}
		claimAnswerOf(t, tc.frame(1, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), "the claim after the last expiry")
		subject.requireIdenticalCopies(t, tc, subject.kind+" after dead-lettering")
		if dead := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket).Dead; dead == 0 {
			t.Fatalf("setup: the %s entry claimed %d times was never dead-lettered", subject.kind, store.MaxDeliveries+1)
		}
	}
}

// TestClaimIgnoresTheCallersCancellation is the M-1 row for claim: the caller
// goes away after its claim is logged and before its owner applies it. Every
// copy claims the same three entries and the owner's outcome is applied.
// Guard: hand newQueryWalk the request's context in Server.ClaimCtx.
func TestClaimIgnoresTheCallersCancellation(t *testing.T) {
	tc, _ := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner = 0
	ownerCluster := tc.nodes[owner].server.core.cluster
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 5, 0)

	ctx, cancel := context.WithCancel(context.Background())
	var cancelOnce sync.Once
	gone := func(nodeID string, repl ReplEntry) {
		if nodeID == ownerCluster.self.ID && repl.Bucket == subject.bucket && transport.Opcode(repl.Op) == transport.OpQueueClaim {
			cancelOnce.Do(cancel)
		}
	}
	outcomes := make(chan int32, 1)
	enqueued := func(entry ReplEntry) {
		if entry.Bucket == subject.bucket && transport.Opcode(entry.Op) == transport.OpQueueClaim {
			select {
			case outcomes <- entry.OwnerOutcome:
			default:
			}
		}
	}
	before := tc.quiescedSequences(t)
	beforeLocalApplyHook.Store(&gone)
	replicaEnqueueHook.Store(&enqueued)
	// The caller is gone, so its answer is not asserted.
	_, _ = ownerCluster.handleClientFrame(ctx, transport.NewFrame(transport.OpQueueClaim, mustMarshalBSON(t, subject.claimRequest(3, 30_000))))
	beforeLocalApplyHook.Store(nil)
	replicaEnqueueHook.Store(nil)
	cancel()
	tc.waitAppliedPast(t, subject.bucket, before)

	select {
	case outcome := <-outcomes:
		if outcome != ownerOutcomeApplied {
			t.Errorf("the owner's outcome for a claim whose caller had gone is %d, want applied (%d)", outcome, ownerOutcomeApplied)
		}
	default:
		t.Fatalf("setup: the claim was never routed to the replicas")
	}
	if held := subject.requireCopiesAgree(t, tc, "a claim whose caller had gone"); len(held) != 2 {
		t.Fatalf("every copy holds %d claimable entries after a claim of three whose caller had gone, want 2", len(held))
	}
	subject.requireIdenticalCopies(t, tc, "a claim whose caller had gone")
}

// TestClientLeaseStampsAreOverwrittenOnACluster is the cluster half of
// TestClientLeaseStampsAreOverwritten (D4 verification finding S1): a client's
// lease_now, Lease_Now and an 8-byte lease_nonce sent through a member are
// replaced by the owner's, so the lease ends at the owner's clock and its id
// holds the owner's nonce; and a pop whose client names a lease_now far in the
// future returns no lease that has not ended by the owner's clock. Guard: the
// owner keeps the client's stamps.
func TestClientLeaseStampsAreOverwrittenOnACluster(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	clientNonce := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	farFuture := leaseClusterBase + int64(365*24*time.Hour/time.Millisecond)
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 4, 0)

	for _, name := range []string{"lease_now", "Lease_Now"} {
		what := "a claim naming " + name + " and lease_nonce"
		before := tc.quiescedSequences(t)
		request := subject.claimRequest(1, 30_000)
		request[name] = farFuture
		request["lease_nonce"] = clientNonce
		answer := claimAnswerOf(t, tc.frame(member, transport.OpQueueClaim, request), what)
		if nonce := leaseNonceOf(t, answer.LeaseID); slices.Equal(nonce, clientNonce) {
			t.Errorf("%s answered lease id %s, which holds the client's nonce", what, answer.LeaseID)
		}
		if got, want := leaseDeadlineOf(t, answer.LeaseID), clocks[owner].now().UnixMilli()+30_000; got != want {
			t.Errorf("%s ends its lease at %d, want the owner's clock plus 30 s = %d", what, got, want)
		}
		subject.settle(t, tc, before)
		for index := range tc.nodes {
			for _, payload := range subject.loggedOf(t, tc, index, before[index], transport.OpQueueClaim) {
				where := fmt.Sprintf("%s, as node %d logged it", what, index+1)
				if now := stampedLeaseNow(t, payload, where); now != clocks[owner].now().UnixMilli() {
					t.Errorf("%s carries lease_now %d, want the owner's %d", where, now, clocks[owner].now().UnixMilli())
				}
				if nonce := stampedNonce(t, payload, where); slices.Equal(nonce, clientNonce) {
					t.Errorf("%s carries the client's lease_nonce", where)
				}
			}
		}
	}

	pop := subject.pop(5)
	pop["lease_now"] = farFuture
	before := tc.quiescedSequences(t)
	popped := poppedNumbers(t, tc.frame(member, subject.popOp(), pop), "a pop naming a far-future lease_now")
	if !slices.Equal(popped, []int{3, 4}) {
		t.Errorf("a pop naming a lease_now a year ahead took %v, want the two unclaimed entries [3 4]: no lease has ended by the owner's clock", popped)
	}
	subject.settle(t, tc, before)
	for index := range tc.nodes {
		for _, payload := range subject.loggedOf(t, tc, index, before[index], subject.popOp()) {
			if now := stampedLeaseNow(t, payload, fmt.Sprintf("the pop, as node %d logged it", index+1)); now != clocks[owner].now().UnixMilli() {
				t.Errorf("node %d logged the pop with lease_now %d, want the owner's %d", index+1, now, clocks[owner].now().UnixMilli())
			}
		}
	}
	if pending := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket).Pending; pending != 2 {
		t.Errorf("after the pop the owner holds %d claimed entries, want the 2 still under lease", pending)
	}
	subject.requireIdenticalCopies(t, tc, "after the client's stamps")
}

// TestPopLogsItsLeaseNowAndEveryReplicaReturnsTheSameLeases: a pop through a
// member on a bucket whose lease has ended by the owner's clock is logged with
// the owner's lease_now, returns the expired lease's entries first, and every
// copy returns and takes the same ones, though the replicas' clocks are
// minutes off. Guard: the owner skips the stamp for pops.
func TestPopLogsItsLeaseNowAndEveryReplicaReturnsTheSameLeases(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, skewedByMinutes, nil)
	defer tc.stop()
	const owner, member = 0, 2
	for _, kind := range leaseSubjects {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 5, 0)
		claimed := claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(2, 1_000)), "a short claim")
		claimedNumbers := peekedNumbers(t, claimed.Items)
		clocks[owner].advance(2 * time.Second)

		before := tc.quiescedSequences(t)
		popped := poppedNumbers(t, tc.frame(member, subject.popOp(), subject.pop(2)), "a pop after the lease ended")
		if !slices.Equal(popped, claimedNumbers) {
			t.Errorf("a %s pop after the lease ended took %v, want the returned entries %v first", subject.kind, popped, claimedNumbers)
		}
		subject.settle(t, tc, before)
		for index := range tc.nodes {
			logged := subject.loggedOf(t, tc, index, before[index], subject.popOp())
			if len(logged) != 1 {
				t.Fatalf("node %d logged %d pops of %s, want 1", index+1, len(logged), subject.bucket)
			}
			if now := stampedLeaseNow(t, logged[0], fmt.Sprintf("the %s pop, as node %d logged it", subject.kind, index+1)); now != clocks[owner].now().UnixMilli() {
				t.Errorf("node %d logged the %s pop with lease_now %d, want the owner's %d", index+1, subject.kind, now, clocks[owner].now().UnixMilli())
			}
		}
		subject.requireIdenticalCopies(t, tc, subject.kind+" after a pop that returned a lease")
	}
}

// TestStampAtOwnerGivesAFieldPopTheCeilingOnly pins D-15: a store field has no
// leases, so a field pop's logged payload carries the owner's result_ceiling
// and no lease_now. Guard: stamp lease_now on a field pop.
func TestStampAtOwnerGivesAFieldPopTheCeilingOnly(t *testing.T) {
	tc, _ := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fieldPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 2, 0)
	before := tc.quiescedSequences(t)
	poppedNumbers(t, tc.frame(member, subject.popOp(), subject.pop(1)), "a field pop")
	subject.settle(t, tc, before)
	for index := range tc.nodes {
		logged := subject.loggedOf(t, tc, index, before[index], transport.OpFieldPop)
		if len(logged) != 1 {
			t.Fatalf("node %d logged %d field pops, want 1", index+1, len(logged))
		}
		if ceilings := stampedCeilings(t, logged[0]); len(ceilings) != 1 {
			t.Errorf("node %d logged a field pop with result_ceiling %v, want the owner's alone", index+1, ceilings)
		}
		if leaseNow := membersNamed(t, logged[0], "lease_now"); len(leaseNow) != 0 {
			t.Errorf("node %d logged a field pop carrying lease_now %v; a store field has no leases", index+1, leaseNow)
		}
	}
}
