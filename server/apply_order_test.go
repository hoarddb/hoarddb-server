package server

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 7 of the shared-storage build: same-bucket applies happen in sequence
// order, and nothing is held across the WAL append.

// applyOrderCluster is a two-node cluster at RF=1 with testBucket created,
// and the index of the node that owns it. Two nodes because a one-node ring
// runs standalone, which has no owner write path.
func applyOrderCluster(t *testing.T) (*testCluster, int) {
	t.Helper()
	tc := newTestCluster(t, 2, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner of %q: %v", testBucket, err)
	}
	return tc, nodeIndexByAddr(t, tc.addrs, owner.ID)
}

// sendPut writes key to testBucket through node index on its own connection.
func sendPut(t *testing.T, tc *testCluster, index int, key string) *transport.Frame {
	t.Helper()
	client := tc.dial(index)
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := client.SendRequest(ctx, transport.OpPut, mustMarshalPut(t, testBucket, key))
	if err != nil {
		t.Errorf("put %q: %v", key, err)
		return nil
	}
	return frame
}

// TestSameBucketAppliesFollowSequenceOrder is the acceptance premise:
// a writer delayed between its append and its apply is not overtaken by a
// later-sequenced writer of the same bucket.
func TestSameBucketAppliesFollowSequenceOrder(t *testing.T) {
	tc, ownerIndex := applyOrderCluster(t)
	defer tc.stop()
	wal := tc.nodes[ownerIndex].server.core.wal

	var (
		orderMu sync.Mutex
		applied []uint64
	)
	var armed atomic.Bool
	armed.Store(true)
	firstSeq := make(chan uint64, 1)
	releaseFirst := make(chan struct{})
	hook := func(_ string, entry ReplEntry) {
		if entry.Bucket != testBucket || transport.Opcode(entry.Op) != transport.OpPut {
			return
		}
		if armed.CompareAndSwap(true, false) {
			firstSeq <- entry.Seq
			<-releaseFirst
		}
		orderMu.Lock()
		applied = append(applied, entry.Seq)
		orderMu.Unlock()
	}
	beforeLocalApplyHook.Store(&hook)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })

	var writers sync.WaitGroup
	writers.Add(2)
	go func() { defer writers.Done(); sendPut(t, tc, ownerIndex, "first") }()
	var lowSeq uint64
	select {
	case lowSeq = <-firstSeq:
	case <-time.After(15 * time.Second):
		t.Fatal("the first writer never reached its apply")
	}
	go func() { defer writers.Done(); sendPut(t, tc, ownerIndex, "second") }()
	deadline := time.Now().Add(10 * time.Second)
	for wal.LastSequence() <= lowSeq {
		if time.Now().After(deadline) {
			t.Fatal("the second writer never appended")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Give the second writer every chance to overtake.
	time.Sleep(200 * time.Millisecond)
	close(releaseFirst)
	writers.Wait()

	orderMu.Lock()
	defer orderMu.Unlock()
	if len(applied) != 2 {
		t.Fatalf("recorded %d applies, want 2: %v", len(applied), applied)
	}
	if !sort.SliceIsSorted(applied, func(i, j int) bool { return applied[i] < applied[j] }) {
		t.Fatalf("same-bucket applies ran in the order %v, not sequence order: a later write overtook an earlier one", applied)
	}
}

// TestLowerSequenceSurvivesAFailedApplyBelowALaterOne is the
// acceptance test in its rev 5.2 form. Ordering covers a crash: a later writer
// cannot apply while an earlier one's ticket is open. It does not cover an
// apply that returns an error, because the ticket closes on that path too and
// the next writer advances applied_seq past the failure. Spec §4.4 rule 5
// records the failure in the next writer's batch, and replay applies it
// beneath the mark. The two keys differ on purpose: that is the case where a
// beneath-the-mark apply is correct (the same-key case is SA-27's test).
func TestLowerSequenceSurvivesAFailedApplyBelowALaterOne(t *testing.T) {
	tc, ownerIndex := applyOrderCluster(t)
	defer tc.stop()
	ownerCore := tc.nodes[ownerIndex].server.core

	var armed atomic.Bool
	armed.Store(true)
	heldSeq := make(chan uint64, 1)
	release := make(chan struct{})
	hold := func(_ string, entry ReplEntry) {
		if entry.Bucket != testBucket || transport.Opcode(entry.Op) != transport.OpPut {
			return
		}
		if armed.CompareAndSwap(true, false) {
			heldSeq <- entry.Seq
			<-release
		}
	}
	beforeLocalApplyHook.Store(&hold)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })

	var writers sync.WaitGroup
	writers.Add(2)
	go func() { defer writers.Done(); sendPut(t, tc, ownerIndex, "a") }()
	var seqA uint64
	select {
	case seqA = <-heldSeq:
	case <-time.After(15 * time.Second):
		t.Fatal("writer A never reached its apply")
	}
	failed := failNextApplyOn(t, ownerCore.kv, func(seq uint64) bool { return seq == seqA })
	go func() { defer writers.Done(); sendPut(t, tc, ownerIndex, "b") }()
	waitForSequence(t, ownerCore.wal, seqA)
	seqB := ownerCore.wal.LastSequence()
	close(release)
	writers.Wait()
	if failed.Load() != seqA {
		t.Fatalf("A's commit (seq %d) was not failed; the test built no failure", seqA)
	}

	st := bucketStateOf(t, ownerCore, testDatabase, testBucket)
	if !reflect.DeepEqual(st.FailedSeqs, []uint64{seqA}) {
		t.Fatalf("before the restart failed_seqs = %v, want [%d]: B's batch did not record A", st.FailedSeqs, seqA)
	}
	if st.AppliedSeq != seqB {
		t.Fatalf("before the restart applied_seq = %d, want B's %d", st.AppliedSeq, seqB)
	}
	if found, _ := tc.readFromNode(ownerIndex, "a"); found {
		t.Fatal("a is readable before the restart; its apply was meant to fail")
	}

	store.ApplyFaultHook.Store(nil)
	restarted := tc.restartNode(ownerIndex).server.core
	for _, key := range []string{"a", "b"} {
		if found, msg := tc.readFromNode(ownerIndex, key); !found {
			t.Fatalf("after replay %q is missing (%s): a failed apply below a later one was lost", key, msg)
		}
	}
	if st := bucketStateOf(t, restarted, testDatabase, testBucket); len(st.FailedSeqs) != 0 {
		t.Fatalf("after replay failed_seqs = %v, want none", st.FailedSeqs)
	}
}

// TestApplyOrderHoldsNothingAcrossTheAppend: ordering costs no lock across the
// WAL append, so group commit is intact. A writer whose turn has come is held
// before its apply; a second writer of the same bucket still appends.
func TestApplyOrderHoldsNothingAcrossTheAppend(t *testing.T) {
	tc, ownerIndex := applyOrderCluster(t)
	defer tc.stop()
	wal := tc.nodes[ownerIndex].server.core.wal

	var armed atomic.Bool
	armed.Store(true)
	heldSeq := make(chan uint64, 1)
	release := make(chan struct{})
	hook := func(_ string, entry ReplEntry) {
		if entry.Bucket != testBucket || transport.Opcode(entry.Op) != transport.OpPut {
			return
		}
		if armed.CompareAndSwap(true, false) {
			heldSeq <- entry.Seq
			<-release
		}
	}
	beforeLocalApplyHook.Store(&hook)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })

	var writers sync.WaitGroup
	writers.Add(2)
	go func() { defer writers.Done(); sendPut(t, tc, ownerIndex, "held") }()
	var seqA uint64
	select {
	case seqA = <-heldSeq:
	case <-time.After(15 * time.Second):
		t.Fatal("the first writer never reached its apply")
	}
	go func() { defer writers.Done(); sendPut(t, tc, ownerIndex, "appends") }()

	appended := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if wal.LastSequence() > seqA {
			appended = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	writers.Wait()
	if !appended {
		t.Fatalf("while a writer (seq %d) was held before its apply, a second writer of the same bucket could not append: something is held across the WAL append", seqA)
	}
}

// BenchmarkSameBucketWritersOrdered measures 64 concurrent writers of one
// bucket through the owner's write path (Cluster.applyAsOwner, which is where
// the ordering lives), reporting p50 and p99 per write: the cost of ordering
// same-bucket applies that the fix owes. It calls the
// write path directly rather than through 64 client connections, which the
// per-address connection cap (64, one already taken by the harness) refuses.
func BenchmarkSameBucketWritersOrdered(b *testing.B) {
	tc := newTestCluster(b, 2, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		b.Fatalf("owner: %v", err)
	}
	ownerCluster := tc.nodes[nodeIndexByAddr(b, tc.addrs, owner.ID)].server.core.cluster
	payload, err := bson.Marshal(putRequest(testBucket, "k"))
	if err != nil {
		b.Fatalf("marshal put: %v", err)
	}

	const writers = 64
	latencies := make([][]time.Duration, writers)
	var next atomic.Int64
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for next.Add(1) <= int64(b.N) {
				began := time.Now()
				_, err := ownerCluster.applyAsOwner(context.Background(), ReplEntry{
					Op: int32(transport.OpPut), Bucket: testBucket, Key: []byte("k"), Payload: payload,
				}, placementOf(testDatabase, testBucket), WcOne)
				if err != nil {
					b.Errorf("write failed: %v", err)
					return
				}
				latencies[w] = append(latencies[w], time.Since(began))
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()
	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	if len(all) == 0 {
		return
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	b.ReportMetric(float64(all[len(all)/2].Microseconds()), "p50-µs")
	b.ReportMetric(float64(all[len(all)*99/100].Microseconds()), "p99-µs")
}

// TestReplicaCreateFloorIsItsOwnSequence is SA-17 on the live path: a replica
// stamps the create floor with the sequence its own log gave the create, not
// the owner's. The two are made to differ by an extra entry in the replica's
// log, so a replica that used the owner's sequence would be caught.
func TestReplicaCreateFloorIsItsOwnSequence(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	replica := tc.nodes[1-ownerIndex].server.core
	if _, err := replica.cluster.appendWAL(ReplEntry{
		Op: int32(transport.OpPut), Bucket: "elsewhere", Payload: mustMarshalPut(t, "elsewhere", "k"),
	}, storage.WALOrigin{Kind: storage.WALOriginOwn}); err != nil {
		t.Fatalf("shifting the replica's log: %v", err)
	}

	// RF=2 writes at w=majority by default: both copies, so the replica has
	// applied the create when the owner answers.
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: testBucket, Type: BucketTypeHash,
	})
	ownerSeq := loggedSequence(t, tc.nodes[ownerIndex].server.core, transport.OpCreateBucket, testBucket)
	localSeq := loggedSequence(t, replica, transport.OpCreateBucket, testBucket)
	if localSeq == ownerSeq {
		t.Fatalf("the replica logged the create at %d, the owner's sequence too; the test cannot tell them apart", localSeq)
	}
	if floor := bucketStateOf(t, replica, testDatabase, testBucket).AppliedSeq; floor != localSeq {
		t.Fatalf("the replica's create floor is %d, want its own log's sequence %d (the owner's is %d)", floor, localSeq, ownerSeq)
	}
}

// TestApplyTurnFollowsTheAssignedSequence: two writes can open their tickets
// before either appends, with one floor, and the one whose append lands second
// gets the higher sequence. Its turn must wait for the other's apply, not come
// because the two floors were equal when it opened.
func TestApplyTurnFollowsTheAssignedSequence(t *testing.T) {
	var entry fenceEntry
	turnCame := func(ticket *applyTicket) bool {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		return entry.waitApplyTurn(ctx, ticket) == nil
	}
	// An earlier write, seq 10, still applying while the two open at floor 11.
	earlier := entry.openApply(func() uint64 { return 9 })
	entry.assignApply(earlier, 10)
	lastSeq := func() uint64 { return 10 }
	first := entry.openApply(lastSeq)
	second := entry.openApply(lastSeq)
	// Its apply ends while neither of the two has appended: both floors are 11.
	entry.closeApply(earlier)

	entry.assignApply(first, 12)
	if turnCame(first) {
		t.Fatal("a write at seq 12 got its turn while another write of the bucket could still be seq 11")
	}
	entry.assignApply(second, 11)
	if !turnCame(second) {
		t.Fatal("the write at seq 11 did not get its turn with nothing below it")
	}
	if turnCame(first) {
		t.Fatal("the write at seq 12 got its turn before seq 11 applied")
	}
	entry.closeApply(second)
	if !turnCame(first) {
		t.Fatal("the write at seq 12 did not get its turn once seq 11 applied")
	}
	entry.closeApply(first)
}
