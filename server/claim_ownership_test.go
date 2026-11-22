package server

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Claim/ack build step 16: ownership changes, snapshots, replay and the budget
// on unequal members (claim-ack-spec 4.4, 6.2 to 6.5, 11.1). The delivery keys
// travel because they are keys of the bucket's range, and the ten delivery
// fields because they are fields of store.BucketState.

// deadLetterOne claims the head entry of subject store.MaxDeliveries times,
// each lease ending before the next claim by the owner's clock, so the claim
// after these returns it past its last delivery and dead-letters it.
func deadLetterOne(t *testing.T, tc *testCluster, clocks []*leaseClockAt, owner int, subject clusterPopSubject) {
	t.Helper()
	for claims := 0; claims < store.MaxDeliveries; claims++ {
		claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), fmt.Sprintf("delivery %d", claims+1))
		clocks[owner].advance(2 * time.Second)
	}
}

// answerLostConn delivers a coordinator's forwarded request to the owner and
// then loses the owner's answer, so the write is applied there and
// Cluster.forward takes its ErrForwardLost path. Shipping on the control lane
// is untouched.
type answerLostConn struct {
	peerConn
}

func (l *answerLostConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	answer, err := l.peerConn.SendRequest(ctx, lane, op, payload)
	if lane == transport.LaneRequest && op == transport.OpReplPush {
		return nil, fmt.Errorf("simulated: the owner applied the request and its answer never came back (it was %v, %v)", answer != nil, err)
	}
	return answer, err
}

// TestLostClaimAnswerLosesNothing: a claim forwarded to the owner, applied
// there, whose answer never reaches the client, loses no entry. When its lease
// ends, the entry is delivered again, with deliveries 2. Guard: drop the fault
// that loses the answer (the first claim is then answered).
func TestLostClaimAnswerLosesNothing(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, coordinator = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 1, 0)
	coordCluster := clusterOf(tc, coordinator)
	wrapPeerConn(coordCluster.peerFor(clusterOf(tc, owner).self),
		func(inner peerConn) peerConn { return &answerLostConn{peerConn: inner} })

	lost := refusalOf(t, tc.frame(coordinator, transport.OpQueueClaim, subject.claimRequest(1, 30_000)))
	if lost.WriteOutcome != outcomeUnknown {
		t.Errorf("the lost claim is write_outcome %q, want %q: %s", lost.WriteOutcome, outcomeUnknown, lost.Message)
	}
	if pending := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket).Pending; pending != 1 {
		t.Fatalf("setup: the owner holds %d claimed entries after the lost claim, want 1: it was applied", pending)
	}
	clocks[owner].advance(31 * time.Second)
	again := claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 30_000)), "the claim after the lost lease ended")
	if len(again.Items) != 1 || len(again.Deliveries) != 1 || again.Deliveries[0] != 2 {
		t.Errorf("the claim after the lost lease ended took %d entries with deliveries %v; want the entry again, delivered a second time", len(again.Items), again.Deliveries)
	}
	subject.requireIdenticalCopies(t, tc, "after the redelivery")
}

// ownerLossOf is the owner-loss construction of TestOwnerMoveSeeds... for one
// fifo bucket, on a four-node lease cluster: the bucket's owner, and the node
// that owns it once the owner is removed, which held no copy before.
func ownerLossOf(t *testing.T) (loss *ownerLoss, clocks []*leaseClockAt, subject clusterPopSubject, newOwner int) {
	t.Helper()
	tc, clocks := leaseClusterOf(t, 4, nil, nil, nil)
	loss = pickOwnerLoss(t, tc, 1)
	subject = clusterPopSubject{kind: "lost", bucketType: BucketTypeFIFO, bucket: loss.toNothing[0]}
	tc.ok(loss.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: subject.bucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, subject.bucket, 3)
	return loss, clocks, subject, loss.newOwner[subject.bucket]
}

// holders is the index of every node that holds bucket before the owner is
// lost: its replica set on the ring of all four.
func (l *ownerLoss) holders(t *testing.T, bucket string) []int {
	t.Helper()
	set, err := ringWith(t, 3, l.tc.addrs...).Replicas(placementOf(testDatabase, bucket).encoded, 3)
	if err != nil {
		t.Fatalf("placing %s: %v", bucket, err)
	}
	indices := make([]int, 0, len(set))
	for _, node := range set {
		indices = append(indices, nodeIndexByAddr(t, l.tc.addrs, node.ID))
	}
	return indices
}

// TestTakeOverKeepsLeasesAndDead (CA-19): the owner of a bucket with a dead
// entry, a lease that has ended and one that has not is lost and removed; the
// node that becomes the owner held no copy and takes the bucket over from a
// replica. It then holds exactly what its predecessor held, key for key and
// field for field, and its first claim, once its clock is past the ended
// lease, returns that lease's entry first. Guard: reset Leases in
// Server.materializeRange.
func TestTakeOverKeepsLeasesAndDead(t *testing.T) {
	loss, clocks, subject, newOwner := ownerLossOf(t)
	tc, owner := loss.tc, loss.owner
	defer tc.stop()
	subject.push(t, tc, owner, 1, 5, 0)
	deadLetterOne(t, tc, clocks, owner, subject)
	ended := claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), "the claim that dead-letters and leases")
	claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(2, 60_000)), "the live lease")
	before := subject.requireIdenticalCopiesOn(t, tc, loss.holders(t, subject.bucket), "before the owner is lost")
	if st := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket); st.Dead != 1 || st.Leases != 2 || st.Pending != 3 {
		t.Fatalf("setup: the owner holds dead %d, leases %d, pending %d; want 1, 2, 3", st.Dead, st.Leases, st.Pending)
	}

	ownerNow := clocks[owner].millis.Load()
	tc.stopNodeKeepingData(owner)
	loss.removeTheStoppedOwner(t)
	newCore := tc.nodes[newOwner].server.core
	waitTakenOver(t, newCore, testDatabase, subject.bucket)
	if after := bucketCopyOf(t, newCore, subject.bucket); after != before {
		t.Fatalf("the new owner, node %d, holds a copy that differs from its predecessor's:\nbefore:\n%s\nafter:\n%s", newOwner+1, before, after)
	}

	clocks[newOwner].millis.Store(ownerNow + 2_000)
	first := claimAnswerOf(t, tc.frame(newOwner, transport.OpQueueClaim, subject.claimRequest(5, 60_000)), "the new owner's first claim")
	if first.Returned != 1 || len(first.Items) == 0 || markerNumber(t, first.Items[0]) != markerNumber(t, ended.Items[0]) {
		t.Errorf("the new owner's first claim returned %d and took %v; want the ended lease's entry %v returned and taken first",
			first.Returned, peekedNumbers(t, first.Items), peekedNumbers(t, ended.Items))
	}
}

// markerNumber is an entry's n.
func markerNumber(t *testing.T, entry bson.Raw) int {
	t.Helper()
	return peekedNumbers(t, []bson.Raw{entry})[0]
}

// stopWithoutShipping stops the node at index while every ship to a peer is
// held, so whatever it logged and has not shipped is never shipped: the held
// senders are let go only once the node's context is cancelled, and their
// ship then fails at once.
func stopWithoutShipping(t *testing.T, tc *testCluster, index int, held chan struct{}) {
	t.Helper()
	stopping := clusterOf(tc, index)
	server := tc.nodes[index].server
	_ = tc.nodes[index].client.Close()
	tc.nodes[index].client = nil
	stopped := make(chan error, 1)
	go func() { stopped <- server.Stop() }()
	pollUntil(t, fmt.Sprintf("node %d's context is cancelled", index+1), func() bool { return stopping.ctx.Err() != nil })
	beforeShipHook.Store(nil)
	close(held)
	if err := <-stopped; err != nil {
		t.Fatalf("stopping node %d: %v", index+1, err)
	}
	tc.nodes[index].server = nil
}

// TestAckOfAClaimTheNewOwnerNeverReceivedIsUnknown (CA-19, D-16): a claim the
// owner applied and answered at concern one is lost with its owner before it
// reached any replica. On the new owner the ack of its lease answers unknown,
// and the entry is available, delivered with a count that does not include the
// lost claim. Guard: answer an ack whose lease header is absent as acked.
func TestAckOfAClaimTheNewOwnerNeverReceivedIsUnknown(t *testing.T) {
	loss, _, subject, newOwner := ownerLossOf(t)
	tc, owner := loss.tc, loss.owner
	defer tc.stop()
	subject.push(t, tc, owner, 1, 1, 0)
	subject.requireIdenticalCopiesOn(t, tc, loss.holders(t, subject.bucket), "before the claim")

	held := make(chan struct{})
	hold := func(string) func() {
		<-held
		return func() {}
	}
	beforeShipHook.Store(&hold)
	request := subject.claimRequest(1, 60_000)
	request["writeconcern"] = "one"
	lost := claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, request), "the claim lost with its owner")
	stopWithoutShipping(t, tc, owner, held)
	loss.removeTheStoppedOwner(t)
	newCore := tc.nodes[newOwner].server.core
	waitTakenOver(t, newCore, testDatabase, subject.bucket)

	acked := leaseAnswerOf(t, tc.frame(newOwner, transport.OpQueueAck, subject.leaseRequest(lost.LeaseID, nil)), "the ack of the lost claim")
	if acked.Success || acked.LeaseState != string(store.LeaseUnknown) {
		t.Errorf("the ack of a claim the new owner never received answered %+v; want unknown", acked)
	}
	again := claimAnswerOf(t, tc.frame(newOwner, transport.OpQueueClaim, subject.claimRequest(1, 60_000)), "the claim on the new owner")
	if len(again.Items) != 1 || len(again.Deliveries) != 0 {
		t.Errorf("the new owner's claim took %d entries with deliveries %v; want the entry, delivered for the first time as far as any copy knows", len(again.Items), again.Deliveries)
	}
}

// TestAckOfALeaseOfAnotherBucketIsUnknown is the recreated row (CA-21): a
// bucket dropped and created again under its name has a new incarnation, so a
// lease id of the old one answers unknown, even when a header under that id
// is planted in the new range. Guard: skip the tag check in the store's lease
// lookup.
func TestAckOfALeaseOfAnotherBucketIsUnknown(t *testing.T) {
	s, _ := leaseServer(t)
	old := claimOf(t, s, "fifo", 1, 30_000)
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "fifo"}); err != nil || !resp.Success {
		t.Fatalf("drop: %+v, %v", resp, err)
	}
	createTestBucket(t, s, "app", "fifo", BucketTypeFIFO)
	pushPeekEntries(t, s, "fifo", 2, "")
	current := claimOf(t, s, "fifo", 1, 30_000)

	handle, err := s.resolveBucket("app", "fifo")
	if err != nil {
		t.Fatalf("resolving the recreated bucket: %v", err)
	}
	slot, databaseID, id := handle.slot, handle.databaseID, handle.id.Load()
	headerOf := func(lease string) []byte {
		return storage.Key(slot, databaseID, id, storage.NSDelivery, []byte{'h'}, leaseBytesOf(t, lease))
	}
	snap := s.kv.NewSnapshot()
	header, err := snap.Get(headerOf(current.LeaseID))
	snap.Discard()
	if err != nil {
		t.Fatalf("setup: reading the live lease's header: %v", err)
	}
	batch := s.kv.NewBatch()
	if err := batch.Set(headerOf(old.LeaseID), header); err != nil {
		t.Fatalf("planting: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("planting: %v", err)
	}

	answer, err := s.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: "fifo", LeaseID: old.LeaseID})
	if err != nil || answer.Success || answer.LeaseState != string(store.LeaseUnknown) {
		t.Errorf("an ack of the dropped incarnation's lease answered %+v, %v; want unknown", answer, err)
	}
	if pending := bucketStateOf(t, s, "app", "fifo").Pending; pending != 1 {
		t.Errorf("after the unknown ack the recreated bucket holds %d claimed entries, want the live lease's 1", pending)
	}
}

// requireDeliveryFieldsKept fails unless every one of the ten delivery fields
// of target equals source's, naming each that does not.
func requireDeliveryFieldsKept(t *testing.T, source, target store.BucketState) {
	t.Helper()
	fields := []struct {
		name             string
		source, received int64
	}{
		{"leases", source.Leases, target.Leases},
		{"pending", source.Pending, target.Pending},
		{"pending_bytes", source.PendingBytes, target.PendingBytes},
		{"returned", source.Returned, target.Returned},
		{"dead", source.Dead, target.Dead},
		{"dead_bytes", source.DeadBytes, target.DeadBytes},
		{"dead_seq", int64(source.DeadSeq), int64(target.DeadSeq)},
		{"lease_clock", source.LeaseClock, target.LeaseClock},
		{"resume_at", int64(source.ResumeAt), int64(target.ResumeAt)},
		{"resume_below", source.ResumeBelow, target.ResumeBelow},
	}
	for _, field := range fields {
		if field.received != field.source {
			t.Errorf("%s is %d on the target, %d on the source", field.name, field.received, field.source)
		}
	}
}

// materializedCopy lands a snapshot of bucket on source in a scratch server,
// as a migration or a take-over lands it (Server.materializeRange), and
// returns the scratch server.
func materializedCopy(t *testing.T, source *Server, bucket string) *Server {
	t.Helper()
	snapshot, err := source.cluster.snapshotBucket(testDatabase, bucket, "")
	if err != nil {
		t.Fatalf("snapshotting %s: %v", bucket, err)
	}
	scratch := newReplayTestServer(t, filepath.Join(t.TempDir(), "scratch"), quietLogger())
	t.Cleanup(func() { closeReplayTestServer(t, scratch) })
	var stream []byte
	for _, chunk := range snapshot.chunks {
		stream = append(stream, chunk...)
	}
	pulled := migratedBucket{database: testDatabase, name: bucket, storeType: snapshot.storeType, generation: snapshot.generation}
	if err := scratch.materializeRange(pulled, stream, 0); err != nil {
		t.Fatalf("materialising %s: %v", bucket, err)
	}
	return scratch
}

// TestMigrationCarriesLeaseAndDeadState (CA-20): a fifo and a heap bucket with
// a dead entry, a live lease and entries returned by a release are snapshotted
// on a replica and landed elsewhere: every key of the range, the delivery keys
// among them, and the delivery fields arrive as they were. Guard: reset one
// delivery field in Server.materializeRange.
func TestMigrationCarriesLeaseAndDeadState(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, replica = 0, 1
	for _, kind := range []clusterPopSubject{fifoPopSubject, heapPopSubject} {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 6, 0)
		deadLetterOne(t, tc, clocks, owner, subject)
		claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 60_000)), "the claim that dead-letters")
		released := claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(2, 60_000)), "the claim to release")
		leaseAnswerOf(t, tc.frame(owner, transport.OpQueueRelease, subject.leaseRequest(released.LeaseID, nil)), "the release")
		subject.requireIdenticalCopies(t, tc, subject.kind+" before the snapshot")

		source := tc.nodes[replica].server.core
		landed := materializedCopy(t, source, subject.bucket)
		st := bucketStateOf(t, source, testDatabase, subject.bucket)
		if st.Leases == 0 || st.Pending == 0 || st.PendingBytes == 0 || st.Returned == 0 || st.Dead == 0 || st.DeadBytes == 0 || st.LeaseClock == 0 {
			t.Fatalf("setup: the %s source state is not what the row needs: %+v", subject.kind, st)
		}
		if kind.bucketType == BucketTypeHeap && st.DeadSeq == 0 {
			t.Fatalf("setup: the heap source has no dead_seq: %+v", st)
		}
		requireDeliveryFieldsKept(t, st, bucketStateOf(t, landed, testDatabase, subject.bucket))
		if want, got := bucketCopyOf(t, source, subject.bucket), bucketCopyOf(t, landed, subject.bucket); got != want {
			t.Errorf("the landed %s copy differs from its source:\nsource:\n%s\nlanded:\n%s", subject.kind, want, got)
		}
	}
}

// TestMaterializeKeepsDeliveryState (CA-20, F-14): a bucket whose state has
// every one of the ten delivery fields non-zero is snapshotted and landed, and
// each field arrives as it was. The state is written directly, so no field is
// zero for lack of a scenario that sets it. Guard: reset any one of them in
// Server.materializeRange.
func TestMaterializeKeepsDeliveryState(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	core := tc.nodes[0].server.core
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "jobs", Type: BucketTypeLIFO})
	handle, err := core.resolveBucket(testDatabase, "jobs")
	if err != nil {
		t.Fatalf("resolving jobs: %v", err)
	}
	slot, databaseID, id := handle.slot, handle.databaseID, handle.id.Load()
	snap := core.kv.NewSnapshot()
	st, err := store.LoadBucketState(snap, slot, databaseID, id)
	snap.Discard()
	if err != nil {
		t.Fatalf("loading the state: %v", err)
	}
	st.Leases, st.Pending, st.PendingBytes, st.Returned = 3, 7, 7_001, 2
	st.Dead, st.DeadBytes, st.DeadSeq, st.LeaseClock = 4, 4_004, 9, leaseClusterBase
	st.ResumeAt, st.ResumeBelow = 11, 5
	batch := core.kv.NewBatch()
	if err := st.Put(batch, slot, databaseID, id); err != nil {
		t.Fatalf("writing the state: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("writing the state: %v", err)
	}
	landed := materializedCopy(t, core, "jobs")
	requireDeliveryFieldsKept(t, st, bucketStateOf(t, landed, testDatabase, "jobs"))
}

// TestReturnBudgetIsTheSameOnUnequalMemtables (claim-ack-spec 4.4, F-15): the
// owner runs a 32 MiB memtable and its replicas 64 and 128 MiB. Twelve leases
// of 400 KiB entries end, and one pop returns as many as the delivery budget
// at the floor memtable admits: every copy stops at the same lease. Guard:
// size the budget from the applying node's own batch bound.
func TestReturnBudgetIsTheSameOnUnequalMemtables(t *testing.T) {
	memtables := []int{32, 64, 128}
	tc, clocks := leaseClusterOf(t, 3, func(index int, cfg *config.Config) {
		cfg.Storage.MemtableMB = memtables[index]
	}, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 12, popPad)
	for lease := 0; lease < 12; lease++ {
		claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), fmt.Sprintf("lease %d", lease+1))
	}
	clocks[owner].advance(2 * time.Second)
	before := tc.quiescedSequences(t)
	poppedNumbers(t, tc.frame(member, subject.popOp(), subject.pop(1)), "the pop that returns the ended leases")
	subject.settle(t, tc, before)
	subject.requireIdenticalCopies(t, tc, "after the return on unequal memtables")
	if leases := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket).Leases; leases == 0 || leases == 12 {
		t.Fatalf("setup: %d leases are left after the pop; the budget must have stopped the return part-way", leases)
	}
}

// TestReplayAfterCrashKeepsLeasesAndDead (F-18): the owner logs the claim that
// dead-letters an entry and leases two more, and its apply fails before the
// commit; the replicas apply it. The owner restarts and replays it, judged by
// the time and nonce it stamped, and ends byte for byte with its replicas.
// Guards: skip the fault, so there is nothing to replay; word the store's
// error before Server.noteApplyOutcome sees it, so the owner owes nothing.
func TestReplayAfterCrashKeepsLeasesAndDead(t *testing.T) {
	for _, kind := range []clusterPopSubject{fifoPopSubject, heapPopSubject} {
		t.Run(kind.kind, func(t *testing.T) {
			tc, clocks := leaseCluster(t, nil, nil, nil)
			defer tc.stop()
			const owner = 0
			subject := kind.createOn(t, tc, owner)
			subject.push(t, tc, owner, 1, 3, 0)
			deadLetterOne(t, tc, clocks, owner, subject)

			claimSeq := tc.quiescedSequences(t)[owner] + 1
			failed := failNextApplyOn(t, tc.nodes[owner].server.core.kv, func(seq uint64) bool { return seq == claimSeq })
			request := subject.claimRequest(2, 60_000)
			request["writeconcern"] = "majority"
			// The owner's apply failed, so the answer is not the claim's.
			tc.frame(owner, transport.OpQueueClaim, request)
			if failed.Load() != claimSeq {
				t.Fatalf("setup: the owner's apply at sequence %d never reached its store", claimSeq)
			}
			store.ApplyFaultHook.Store(nil)
			for _, replica := range []int{1, 2} {
				pollUntil(t, fmt.Sprintf("replica %d dead-letters the entry", replica+1), func() bool {
					return bucketStateOf(t, tc.nodes[replica].server.core, testDatabase, subject.bucket).Dead == 1
				})
			}
			if dead := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, subject.bucket).Dead; dead != 0 {
				t.Fatalf("setup: the owner dead-lettered before its restart (dead %d); its apply was meant to fail", dead)
			}
			tc.restartNode(owner)
			tc.waitReady()
			held := subject.requireIdenticalCopies(t, tc, "after the owner's replay")
			if !strings.Contains(held, "leases=1 pending=2") || !strings.Contains(held, "dead=1") {
				t.Errorf("after the replay every copy holds:\n%s\nwant one lease of two entries and one dead entry", held)
			}
		})
	}
}

// deliveryCounters are the delivery counters, read together.
type deliveryCounters map[string]prometheus.Counter

func (counters deliveryCounters) read(t *testing.T) map[string]float64 {
	t.Helper()
	values := make(map[string]float64, len(counters))
	for name, counter := range counters {
		values[name] = counted(t, counter)
	}
	return values
}

// TestDeliveryMetricsCountOnTheOwnerOnly (claim-ack-spec 11.1): through a
// member of three nodes, each counter moves by exactly what the request did,
// once, though every copy applies it; and the owner's replay of its own entry
// after a restart moves none. The nodes share one process, so a counter is
// the sum of the three nodes'. Guard: count on lineageFrom(ctx).originated
// alone (the replay row goes red).
func TestDeliveryMetricsCountOnTheOwnerOnly(t *testing.T) {
	tc, clocks := leaseCluster(t, nil, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	counters := deliveryCounters{
		"claims":           queueClaims,
		"claimed_entries":  queueClaimedEntries,
		"returned":         queueEntriesReturned,
		"dead_lettered":    queueEntriesDeadLettered.WithLabelValues(store.DeadReasonMaxDeliveries),
		"acked":            queueLeaseSettlements.WithLabelValues("ack", string(store.LeaseAcked)),
		"extended":         queueLeaseSettlements.WithLabelValues("extend", string(store.LeaseExtended)),
		"released":         queueLeaseSettlements.WithLabelValues("release", string(store.LeaseReleased)),
		"requeued":         queueDeadRequeued,
		"discarded":        queueDeadDiscarded,
		"unlogged_empties": queueEmptyClaimsUnlogged,
	}
	requireMoved := func(what string, before map[string]float64, want map[string]float64) {
		t.Helper()
		after := counters.read(t)
		for name := range counters {
			if moved := after[name] - before[name]; moved != want[name] {
				t.Errorf("%s moved %s by %v, want %v", what, name, moved, want[name])
			}
		}
	}

	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 2, 0)
	before := counters.read(t)
	claimed := claimAnswerOf(t, tc.frame(member, transport.OpQueueClaim, subject.claimRequest(2, 60_000)), "the claim")
	requireMoved("a claim of two", before, map[string]float64{"claims": 1, "claimed_entries": 2})
	before = counters.read(t)
	leaseAnswerOf(t, tc.frame(member, transport.OpQueueExtend, subject.leaseRequest(claimed.LeaseID, bson.M{"lease_ms": 90_000})), "the extend")
	leaseAnswerOf(t, tc.frame(member, transport.OpQueueAck, subject.leaseRequest(claimed.LeaseID, bson.M{"item_ids": claimed.ItemIDs[:1]})), "the ack")
	leaseAnswerOf(t, tc.frame(member, transport.OpQueueRelease, subject.leaseRequest(claimed.LeaseID, nil)), "the release")
	// The claim took two; the ack settled one, so the release writes the other
	// back, and the counter's help says released entries are counted.
	requireMoved("an extend, an ack and a release", before, map[string]float64{"extended": 1, "acked": 1, "released": 1, "returned": 1})

	dying := []clusterPopSubject{
		{kind: "dyingone", bucketType: BucketTypeFIFO}, {kind: "dyingtwo", bucketType: BucketTypeFIFO},
	}
	for index := range dying {
		dying[index] = dying[index].createOn(t, tc, owner)
		dying[index].push(t, tc, owner, 1, 1, 0)
		before = counters.read(t)
		deadLetterOne(t, tc, clocks, owner, dying[index])
		claimAnswerOf(t, tc.frame(member, transport.OpQueueClaim, dying[index].claimRequest(1, 1_000)), "the claim that dead-letters")
		requireMoved("five deliveries and the claim that dead-letters", before, map[string]float64{
			"claims": float64(store.MaxDeliveries + 1), "claimed_entries": float64(store.MaxDeliveries),
			"returned": float64(store.MaxDeliveries - 1), "dead_lettered": 1, "unlogged_empties": 0,
		})
	}
	before = counters.read(t)
	tc.ok(member, transport.OpDeadRequeue, bson.M{"database": testDatabase, "bucket": dying[0].bucket, "all": true})
	tc.ok(member, transport.OpDeadDiscard, bson.M{"database": testDatabase, "bucket": dying[1].bucket, "all": true})
	requireMoved("a requeue and a discard", before, map[string]float64{"requeued": 1, "discarded": 1})

	// The replay row: the owner's apply of a claim fails, so it owes it, and
	// replays it from its own log after a restart.
	subject.push(t, tc, owner, 3, 1, 0)
	claimSeq := tc.quiescedSequences(t)[owner] + 1
	failed := failNextApplyOn(t, tc.nodes[owner].server.core.kv, func(seq uint64) bool { return seq == claimSeq })
	request := subject.claimRequest(1, 60_000)
	request["writeconcern"] = "majority"
	tc.frame(owner, transport.OpQueueClaim, request)
	if failed.Load() != claimSeq {
		t.Fatalf("setup: the owner's apply at sequence %d never reached its store", claimSeq)
	}
	store.ApplyFaultHook.Store(nil)
	for _, replica := range []int{1, 2} {
		pollUntil(t, fmt.Sprintf("replica %d applies the claim", replica+1), func() bool {
			return bucketStateOf(t, tc.nodes[replica].server.core, testDatabase, subject.bucket).Pending == 1
		})
	}
	before = counters.read(t)
	tc.restartNode(owner)
	tc.waitReady()
	subject.requireIdenticalCopies(t, tc, "after the owner's replay")
	requireMoved("the owner's replay of its own claim", before, map[string]float64{})
}
