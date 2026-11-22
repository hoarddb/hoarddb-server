package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A heap pop on a cluster of mixed memtable sizes (pop-n D3's blocker). The
// pop is one logged write whose batch is its deletes, so the owner at 64 MiB
// can commit a pop that the 32 MiB replica cannot. That replica must stall on
// it, as it does on a batch put, and never acknowledge it as applied while it
// keeps the entries its owner removed.

const (
	// longIDHeapEntries is a whole pop({limit: 100}).
	longIDHeapEntries = 100
	// longIDBytes is each entry's id. The id is part of a heap key, so a pop
	// of every entry deletes about 6.4 MB of keys: over a 32 MiB member's
	// 5 MB atomic batch, under a 64 MiB member's.
	longIDBytes = 64_000
)

// pushLongIDHeap creates a heap bucket owned by node 1 in database app and
// pushes longIDHeapEntries entries to it, each under a longIDBytes id. Every
// push alone fits the 32 MiB member. Each push takes the bucket's default
// concern: asking for all three copies on every push made the helper fail when
// a loaded host missed the 5 s write concern for one of them, and what these
// tests need is that every node holds the entries, which the poll below waits
// for (BUG-0599).
func pushLongIDHeap(t *testing.T, tc *testCluster) string {
	t.Helper()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHeap})
	for n := range longIDHeapEntries {
		id := fmt.Sprintf("%03d", n) + strings.Repeat("i", longIDBytes-3)
		tc.ok(0, transport.OpHeapPush, PushRequest{Database: "app", Bucket: bucket,
			Data: map[string]interface{}{"_id_": id, "n": int32(n)}, Priority: int64(n)})
	}
	held := make([]int64, len(tc.nodes))
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		allHeld := true
		for index := range tc.nodes {
			held[index] = tc.bucketCountOn(index, "app", bucket)
			if held[index] != longIDHeapEntries {
				allHeld = false
			}
		}
		if allHeld {
			return bucket
		}
	}
	t.Fatalf("setup: node counts %v after 60 s; every node must hold all %d heap entries", held, longIDHeapEntries)
	return bucket
}

// popWholeLongIDHeap pops every entry of the bucket through its owner at
// concern all and returns the answer.
func popWholeLongIDHeap(tc *testCluster, bucket string) *transport.Frame {
	return tc.frame(0, transport.OpHeapPop, bson.M{"database": "app", "bucket": bucket, "limit": longIDHeapEntries, "writeconcern": "all"})
}

// TestASmallerReplicaStallsOnAHeapPopItsOwnerCommitted: the 64 MiB owner
// removes 100 heap entries in one pop that the 32 MiB replica cannot commit.
// The client must not be told 3 of 3 copies hold the removal, the replica's
// stream must stall with a reason that tells the operator to resize it, and
// once resized and restarted it must remove what its owner removed.
func TestASmallerReplicaStallsOnAHeapPopItsOwnerCommitted(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	bucket := pushLongIDHeap(t, tc)

	answer := popWholeLongIDHeap(tc, bucket)
	ownerSeq := tc.nodes[0].server.core.cluster.walLastSequence()
	if held := tc.bucketCountOn(0, "app", bucket); held != 0 {
		t.Fatalf("setup: the 64 MiB owner holds %d entries after pop({limit: %d}); want it to have removed every one", held, longIDHeapEntries)
	}
	if confirmed, factor, _, _ := copiesConfirmedOf(answer.Payload); confirmed >= factor {
		t.Errorf("pop({limit: %d}) answered copies_confirmed %d of %d while the 32 MiB replica holds %d of the entries its owner removed: %.300s",
			longIDHeapEntries, confirmed, factor, tc.bucketCountOn(2, "app", bucket), bson.Raw(answer.Payload))
	}

	reason := tc.stallStaysOnSmallReplica(t, bucket, longIDHeapEntries, ownerSeq, "after the pop")
	for _, want := range []string{"this member runs a 32 MiB memtable", "pop", "Set storage.memtable_mb", "SIGTERM"} {
		if !strings.Contains(reason, want) {
			t.Errorf("the owner's stall reason for the 32 MiB replica %q does not contain %q", reason, want)
		}
	}
	if strings.Contains(reason, "do not resize") {
		t.Errorf("the stall reason tells the operator not to resize a member whose memtable cannot commit the pop: %q", reason)
	}

	tc.configure = func(index int, cfg *config.Config) {
		if index == 2 {
			cfg.Storage.MemtableMB = 64
		}
	}
	tc.restartNode(2)
	held := int64(-1)
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if held = tc.bucketCountOn(2, "app", bucket); held == 0 {
			break
		}
	}
	if held != 0 {
		t.Fatalf("node 3 resized to 64 MiB and restarted holds %d entries its owner's pop removed", held)
	}
}

// TestASmallerReplicaReplayingAHeapPopItCouldNotCommitStillStalls is the
// replay side, BUG-0248's crash for a pop: the 32 MiB replica logs the
// owner's pop, its apply refuses it, and the node dies before the durable
// record of that refusal commits. Restarted unresized, its replay meets the
// pop it logged and must recognise the refusal there too: the owner's
// sequence stays unapplied, and the stream stalls again with the entries
// still held.
func TestASmallerReplicaReplayingAHeapPopItCouldNotCommitStillStalls(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	bucket := pushLongIDHeap(t, tc)

	small := tc.nodes[2].server.core.cluster
	var crashed atomic.Bool
	hook := func(c *Cluster, _ string, _ uint64) error {
		if c != small {
			return nil
		}
		crashed.Store(true)
		return errors.New("injected crash before the size refusal record commits")
	}
	sizeRefusalRecordFaultHook.Store(&hook)
	defer sizeRefusalRecordFaultHook.Store(nil)

	popWholeLongIDHeap(tc, bucket)
	ownerSeq := tc.nodes[0].server.core.cluster.walLastSequence()
	for deadline := time.Now().Add(15 * time.Second); !crashed.Load(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the 32 MiB replica never reached a size refusal record for the owner's pop at sequence %d: it took the pop as applied (marked applied: %v)",
				ownerSeq, !small.shouldApply(tc.addrs[0], ownerSeq))
		}
	}
	smallDir := tc.stopNodeKeepingData(2)
	sizeRefusalRecordFaultHook.Store(nil)

	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	if !tc.nodes[2].server.core.cluster.shouldApply(tc.addrs[0], ownerSeq) {
		t.Fatalf("the 32 MiB replica, restarted unresized after a crash before its refusal record, replayed the owner's pop at sequence %d and marked it applied while it holds %d of the entries the owner removed",
			ownerSeq, tc.bucketCountOn(2, "app", bucket))
	}
	tc.stallStaysOnSmallReplica(t, bucket, longIDHeapEntries, ownerSeq, "after the replica crashed before its refusal record and restarted unresized")
}

// TestBatchTooLargeReasonForAPop: a pop's batch is the deletes of the entries
// it removes, which its small request does not show, and a restart at the
// same size refuses the same deletes again. The stall reason must name the
// pop and tell the operator to resize the member, never that it already runs
// the size the batch fits.
func TestBatchTooLargeReasonForAPop(t *testing.T) {
	popEntry := func(op transport.Opcode, request any) ReplEntry {
		payload, err := bson.Marshal(request)
		if err != nil {
			t.Fatalf("encoding a pop: %v", err)
		}
		return ReplEntry{Seq: 7, Op: int32(op), Bucket: "b", Payload: payload}
	}
	tests := []struct {
		name  string
		entry ReplEntry
		want  []string
	}{
		{name: "heap pop of 100", entry: popEntry(transport.OpHeapPop, PopRequest{Database: "app", Bucket: "b", Limit: 100}),
			want: []string{"pop of up to 100 entries"}},
		{name: "fifo pop with no limit", entry: popEntry(transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "b"}),
			want: []string{"pop of up to 1 entry"}},
		{name: "field pop of 40", entry: popEntry(transport.OpFieldPop, FieldPopRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "b", Key: "p1", Field: "Comments"}, Limit: 40}),
			want: []string{"pop of up to 40 entries"}},
	}
	for _, row := range tests {
		t.Run(row.name, func(t *testing.T) {
			reason := batchTooLargeReason(32, row.entry)
			for _, want := range append(row.want, "runs a 32 MiB memtable", "memtable", "Set storage.memtable_mb", "SIGTERM") {
				if !strings.Contains(reason, want) {
					t.Errorf("reason %q does not contain %q", reason, want)
				}
			}
			for _, unwanted := range []string{"do not resize", "0.0 MB", "from its own log"} {
				if strings.Contains(reason, unwanted) {
					t.Errorf("reason %q contains %q", reason, unwanted)
				}
			}
		})
	}
}

// TestBatchTooLargeReasonForACascade: a delete and a TTL expiry commit the
// document and every element and index entry beneath it, none of which the
// request shows, so the reason cannot size the batch and must not print a
// size. The only fix is a larger memtable: a restart at the same size refuses
// the same cascade again from the log, which is how a member once stayed
// stalled on advice to "restart, do not resize" (pop-n D3 verification).
func TestBatchTooLargeReasonForACascade(t *testing.T) {
	cascadeEntry := func(op transport.Opcode, request any) ReplEntry {
		payload, err := bson.Marshal(request)
		if err != nil {
			t.Fatalf("encoding a delete: %v", err)
		}
		return ReplEntry{Seq: 7, Op: int32(op), Bucket: "b", Payload: payload}
	}
	expiry := bson.M{"database": "app", "bucket": "b", "key": "k1",
		"expire_if": bson.M{"field": "CreatedAt", "date_before": primitive.NewDateTimeFromTime(time.Now()), "seconds_before": time.Now().Unix()}}
	tests := []struct {
		name  string
		entry ReplEntry
		want  []string
	}{
		{name: "client delete", entry: cascadeEntry(transport.OpDelete, bson.M{"database": "app", "bucket": "b", "key": "k1"}),
			want: []string{"owner's delete at sequence 7"}},
		{name: "ttl expiry", entry: cascadeEntry(transport.OpDelete, expiry),
			want: []string{"owner's TTL expiry at sequence 7"}},
	}
	for _, row := range tests {
		t.Run(row.name, func(t *testing.T) {
			reason := batchTooLargeReason(32, row.entry)
			for _, want := range append(row.want, `bucket "b"`, "runs a 32 MiB memtable", "Set storage.memtable_mb", "SIGTERM") {
				if !strings.Contains(reason, want) {
					t.Errorf("reason %q does not contain %q", reason, want)
				}
			}
			for _, unwanted := range []string{"do not resize", "0.0 MB", "from its own log", "k1"} {
				if strings.Contains(reason, unwanted) {
					t.Errorf("reason %q contains %q", reason, unwanted)
				}
			}
		})
	}
}

// TestABatchTooLargePopIsRecognisedByItsTypeNotItsWording pins how a replica
// knows its store refused an entry as too large: by the store's typed error,
// which the apply's outcome carries, and not by the answer's text. A pop's
// answer words the refusal for a client and does not carry the store's
// sentence; the refusal must be recognised all the same, so a later rewording
// of any handler's message cannot turn a refusal into an acknowledgement.
func TestABatchTooLargePopIsRecognisedByItsTypeNotItsWording(t *testing.T) {
	s := newTestServerFloorMemtable(t)
	createTestBucket(t, s, "app", "heap", BucketTypeHeap)
	for n := range longIDHeapEntries {
		id := fmt.Sprintf("%03d", n) + strings.Repeat("i", longIDBytes-3)
		if _, err := s.Push(PushRequest{Database: "app", Bucket: "heap", Data: map[string]interface{}{"_id_": id}, Priority: int64(n)}); err != nil {
			t.Fatalf("setup: pushing entry %d: %v", n, err)
		}
	}

	ctx, outcome := withApplyOutcome(context.Background())
	answer, err := s.PopCtx(ctx, PopRequest{Database: "app", Bucket: "heap", Limit: longIDHeapEntries})
	if err != nil {
		t.Fatalf("pop({limit: %d}) at the 32 MiB floor: %v", longIDHeapEntries, err)
	}
	if answer.Found {
		t.Fatalf("setup: pop({limit: %d}) at the 32 MiB floor removed %d entries; it must not fit one batch", longIDHeapEntries, len(answer.Items))
	}
	if !outcome.tooLarge {
		t.Fatalf("the store refused the pop as too large and the apply's outcome does not say so; answer %q", answer.Message)
	}
	for _, ownerOutcome := range []int32{ownerOutcomeApplied, ownerOutcomeOwed} {
		if !ownerCommittedWhatThisNodeCannot(ReplEntry{Op: int32(transport.OpHeapPop), OwnerOutcome: ownerOutcome}, outcome) {
			t.Errorf("a replica whose store refused, as too large, a pop its owner reports %d is not recognised; answer %q", ownerOutcome, answer.Message)
		}
	}
	if ownerCommittedWhatThisNodeCannot(ReplEntry{Op: int32(transport.OpHeapPop), OwnerOutcome: ownerOutcomeUnknown}, outcome) {
		t.Error("a pop its owner did not commit is recognised as one the owner committed and this node cannot")
	}
	if ownerCommittedWhatThisNodeCannot(ReplEntry{Op: int32(transport.OpHeapPop), OwnerOutcome: ownerOutcomeApplied}, &applyOutcome{}) {
		t.Error("an apply with no too-large refusal is recognised as one")
	}
}

// TestASmallerReplicaReplayingAFieldPopItCouldNotCommitIsNoDivergence is the
// replay judgement of a size refusal whose answer does not carry the store's
// sentence (the merge of BUG-0581's replay judgement with pop-n). A field
// pop's refusal is answered success false with a pop's own advice, so a
// replay that judged it by the answer's wording took it for a refusal of an
// entry its owner applied for a reason not shared: a divergence, recorded,
// and the copy re-seeded, where the live apply stalls on the memtable. The
// 32 MiB replica logs the owner's field pop of 100 long-id elements, its
// apply refuses it as too large, and it dies before its size refusal record
// commits. Restarted unresized, its replay must judge the pop as the live
// apply did, from the apply's outcome: no divergence, the owner's sequence
// unapplied, and the stream stalled on the memtable again.
func TestASmallerReplicaReplayingAFieldPopItCouldNotCommitIsNoDivergence(t *testing.T) {
	tc := memtableMixedCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash,
		Options: storesOption(StoreDeclaration{Name: "Ranked", Engine: "heapStore"})})
	tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "p1", Data: map[string]interface{}{"title": "p1"}, WriteConcern: "all"})
	for n := range longIDHeapEntries {
		id := fmt.Sprintf("%03d", n) + strings.Repeat("i", longIDBytes-3)
		tc.ok(0, transport.OpFieldPush, FieldRequest{Database: "app", Bucket: bucket, Key: "p1", Field: "Ranked", Priority: int64(n),
			Data: map[string]interface{}{"_id_": id, "n": int32(n)}, WriteConcern: "all"})
	}

	small := tc.nodes[2].server.core.cluster
	var crashed atomic.Bool
	hook := func(c *Cluster, _ string, _ uint64) error {
		if c != small {
			return nil
		}
		crashed.Store(true)
		return errors.New("injected crash before the size refusal record commits")
	}
	sizeRefusalRecordFaultHook.Store(&hook)
	defer sizeRefusalRecordFaultHook.Store(nil)

	tc.frame(0, transport.OpFieldPop, FieldPopRequest{FieldRequest: FieldRequest{Database: "app", Bucket: bucket, Key: "p1", Field: "Ranked", WriteConcern: "all"},
		Limit: longIDHeapEntries})
	ownerSeq := owner.walLastSequence()
	for deadline := time.Now().Add(15 * time.Second); !crashed.Load(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("setup: the 32 MiB replica never reached a size refusal record for the owner's field pop at sequence %d (marked applied: %v)",
				ownerSeq, !small.shouldApply(tc.addrs[0], ownerSeq))
		}
	}
	smallDir := tc.stopNodeKeepingData(2)
	sizeRefusalRecordFaultHook.Store(nil)

	countedBefore := divergencesCounted(t)
	tc.nodes[2] = tc.startNode(2, smallDir, tc.token)
	restarted := tc.nodes[2].server.core.cluster
	// The count, not only the record: a recorded divergence wakes the
	// re-seed, which can settle it before the record is read.
	if counted := divergencesCounted(t) - countedBefore; counted != 0 {
		t.Fatalf("the 32 MiB replica, restarted unresized, counted %v divergences replaying the owner's field pop it cannot commit; it is a size refusal", counted)
	}
	if record, recorded := restarted.divergenceFrom(tc.addrs[0]); recorded {
		t.Fatalf("the 32 MiB replica, restarted unresized, judged the owner's field pop it cannot commit a divergence (%+v), not a size refusal", record)
	}
	if !restarted.shouldApply(tc.addrs[0], ownerSeq) {
		t.Fatalf("the 32 MiB replica, restarted unresized, replayed the owner's field pop at sequence %d and marked it applied", ownerSeq)
	}
	tc.stallStaysOnSmallReplica(t, bucket, 1, ownerSeq, "after the replica crashed before its refusal record and restarted unresized")
}
