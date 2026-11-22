package server

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// rawDocumentKey is the store key a document lives at, so a test can aim the
// fault store's one-shot read fault at exactly the read a handler makes.
func rawDocumentKey(t *testing.T, s *Server, database, bucket, key string) []byte {
	t.Helper()
	b := lookupBucket(s, database, bucket)
	if b == nil {
		t.Fatalf("%s/%s is not served", database, bucket)
	}
	if b.bucketType == BucketTypeBTree {
		encoded, err := store.EncodeComponent(key)
		if err != nil {
			t.Fatalf("encoding the btree key %q: %v", key, err)
		}
		return storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSDocuments, encoded)
	}
	return storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSDocuments, []byte(key))
}

func updateRequestPayload(t *testing.T, key string) []byte {
	t.Helper()
	payload, err := bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Key: key,
		Set: map[string]interface{}{"n": 2}})
	if err != nil {
		t.Fatalf("marshal the update of %q: %v", key, err)
	}
	return payload
}

func multiUpdateRequestPayload(t *testing.T, bucket string) []byte {
	t.Helper()
	payload, err := bson.Marshal(UpdateRequest{Database: "app", Bucket: bucket, Multi: true,
		Set: map[string]interface{}{"n": 2}})
	if err != nil {
		t.Fatalf("marshal the multi-update of %s: %v", bucket, err)
	}
	return payload
}

func deleteRequestPayload(t *testing.T, key string) []byte {
	t.Helper()
	payload, err := bson.Marshal(DeleteRequest{Database: "app", Bucket: "held", Key: key})
	if err != nil {
		t.Fatalf("marshal the delete of %q: %v", key, err)
	}
	return payload
}

// applyOwnerPut pushes an applied put of key to bucket and checks it lands, so
// the handler under test has a document to read.
func applyOwnerPut(t *testing.T, c *Cluster, bucket, key string) {
	t.Helper()
	put := ReplEntry{Seq: 1, Op: int32(transport.OpPut), Bucket: bucket, Key: []byte(key),
		Payload: putPayload(t, "app", bucket, key), OwnerOutcome: ownerOutcomeApplied}
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{put}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("setup: the put was answered success=%t applied_through=%d error=%q",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
}

// assertReadFailureLeavesTheEntry is one owner entry this replica's store read
// fails once while applying: no divergence is recorded, the push is not
// answered Refusal diverged, and the entry is left unapplied. The owner's
// redelivery then applies it once the read works.
func assertReadFailureLeavesTheEntry(t *testing.T, kv *kvfault.Store, c *Cluster, faultKey []byte, entry ReplEntry) {
	t.Helper()
	kv.FailNextReadOf(faultKey)
	resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{entry})
	if _, diverged := c.divergenceFrom(frameSyncSource); diverged {
		t.Fatalf("the replica recorded a divergence from its owner after its store read failed: %+v", resp)
	}
	if resp.Refusal == refusalDiverged {
		t.Fatalf("the store read failure was answered Refusal %q: %+v", resp.Refusal, resp)
	}
	if resp.Success || resp.AppliedThrough >= entry.Seq {
		t.Fatalf("the store read failure was answered success=%t applied_through=%d error=%q; want the entry left unapplied",
			resp.Success, resp.AppliedThrough, resp.Error)
	}

	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{entry}); !resp.Success || resp.AppliedThrough != entry.Seq {
		t.Fatalf("after the read recovered the owner's redelivery was answered success=%t applied_through=%d error=%q",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	if _, diverged := c.divergenceFrom(frameSyncSource); diverged {
		t.Fatal("a divergence from the owner is recorded after the redelivery applied")
	}
}

// TestAReplicaStoreReadFailureInAHandlerIsNotADivergence is BUG-0583: a store
// read fault in the read an update or delete handler makes before its write is
// a store failure, not a disagreement with the owner. The entry is left
// unapplied for the owner to re-send, and the redelivery applies once the
// store reads again.
func TestAReplicaStoreReadFailureInAHandlerIsNotADivergence(t *testing.T) {
	const key = "k"

	handlers := []struct {
		name  string
		entry func(t *testing.T) ReplEntry
	}{
		{"update", func(t *testing.T) ReplEntry {
			return ReplEntry{Seq: 2, Op: int32(transport.OpUpdate), Bucket: "held", Key: []byte(key),
				Payload: updateRequestPayload(t, key), OwnerOutcome: ownerOutcomeApplied}
		}},
		{"update-multiple-hash", func(t *testing.T) ReplEntry {
			return ReplEntry{Seq: 2, Op: int32(transport.OpUpdate), Bucket: "held",
				Payload: multiUpdateRequestPayload(t, "held"), OwnerOutcome: ownerOutcomeApplied}
		}},
		{"delete", func(t *testing.T) ReplEntry {
			return ReplEntry{Seq: 2, Op: int32(transport.OpDelete), Bucket: "held", Key: []byte(key),
				Payload: deleteRequestPayload(t, key), OwnerOutcome: ownerOutcomeApplied}
		}},
	}
	for _, handler := range handlers {
		t.Run(handler.name, func(t *testing.T) {
			s, kv, c, _ := newReplicaOnFaultStore(t)
			streamOf(t, c, frameSyncSource)
			applyOwnerPut(t, c, "held", key)
			assertReadFailureLeavesTheEntry(t, kv, c, rawDocumentKey(t, s, "app", "held", key), handler.entry(t))
		})
	}

	t.Run("update-multiple-btree", func(t *testing.T) {
		s, kv, c, _ := newReplicaOnFaultStore(t)
		streamOf(t, c, frameSyncSource)
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "btree", Type: BucketTypeBTree}); err != nil || !resp.Success {
			t.Fatalf("setup: creating the btree bucket: %+v, %v", resp, err)
		}
		applyOwnerPut(t, c, "btree", key)
		entry := ReplEntry{Seq: 2, Op: int32(transport.OpUpdate), Bucket: "btree",
			Payload: multiUpdateRequestPayload(t, "btree"), OwnerOutcome: ownerOutcomeApplied}
		assertReadFailureLeavesTheEntry(t, kv, c, rawDocumentKey(t, s, "app", "btree", key), entry)
	})
}

// ownerReadFailureCase is one client mutation whose owner-side pre-write read
// the test fails: a single-document update, a multi-document update over a
// hash walk or a btree range, and a delete.
type ownerReadFailureCase struct {
	name       string
	bucketType BucketType
	op         transport.Opcode
	multi      bool
}

var ownerReadFailureCases = []ownerReadFailureCase{
	{name: "update", bucketType: BucketTypeHash, op: transport.OpUpdate},
	{name: "update-multiple-hash", bucketType: BucketTypeHash, op: transport.OpUpdate, multi: true},
	{name: "update-multiple-btree", bucketType: BucketTypeBTree, op: transport.OpUpdate, multi: true},
	{name: "delete", bucketType: BucketTypeHash, op: transport.OpDelete},
}

// entry is the owner-path entry for the case's mutation of key.
func (tt ownerReadFailureCase) entry(t *testing.T, key string) ReplEntry {
	t.Helper()
	entry := ReplEntry{Op: int32(tt.op), Bucket: "held", Key: []byte(key)}
	var err error
	switch {
	case tt.op == transport.OpDelete:
		entry.Payload, err = bson.Marshal(DeleteRequest{Database: "app", Bucket: "held", Key: key})
	case tt.multi:
		entry.Key = nil
		entry.Payload, err = bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Multi: true,
			Set: map[string]interface{}{"n": 2}})
	default:
		entry.Payload, err = bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Key: key,
			Set: map[string]interface{}{"n": 2}})
	}
	if err != nil {
		t.Fatalf("marshalling the %s of %q: %v", tt.name, key, err)
	}
	return entry
}

// ownerPutEntry is a client put of key into the bucket "held".
func ownerPutEntry(t *testing.T, key string) ReplEntry {
	t.Helper()
	return ReplEntry{Op: int32(transport.OpPut), Bucket: "held", Key: []byte(key), Payload: putPayload(t, "app", "held", key)}
}

// newFaultNodeWithBucket is a one-member cluster over the fault store holding
// the bucket "held" of bucketType.
func newFaultNodeWithBucket(t *testing.T, bucketType BucketType) (*Server, *kvfault.Store, *Cluster, string) {
	t.Helper()
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "held", Type: bucketType}); err != nil || !resp.Success {
		t.Fatalf("creating the bucket: %+v, %v", resp, err)
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	t.Cleanup(c.cancel)
	return s, kv, c, dataDir
}

// restartOwner closes the node's log and reopens the node on the same store
// and log, replaying the log as a start does.
func restartOwner(t *testing.T, s *Server, kv *kvfault.Store, dataDir string) *Server {
	t.Helper()
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the store: %v", err)
	}
	if err := s.wal.Close(); err != nil {
		t.Fatalf("closing the WAL: %v", err)
	}
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the restart: %v", err)
	}
	w, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("reopening the WAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	restarted.wal = w
	if err := restarted.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the WAL: %v", err)
	}
	return restarted
}

// TestAnOwnersFailedPreWriteReadIsRetriedOnTheOwner is BUG-0584: the owner
// logs a client update or delete and ships it, and its handler's read of the
// document fails before store.Apply runs. The sequence must reach the bucket's
// failed_seqs, so a restart's replay retries the write on the owner after a
// later write has moved applied_seq past it. A single update or delete then
// leaves the owner with the document its replica holds. A multi-document
// update has no key and never commutes with a later write (SA-27): it is
// recorded, and replay reports it loudly instead of applying it.
func TestAnOwnersFailedPreWriteReadIsRetriedOnTheOwner(t *testing.T) {
	const key = "k"
	for _, tt := range ownerReadFailureCases {
		t.Run(tt.name, func(t *testing.T) {
			owner, kv, ownerCluster, dataDir := newFaultNodeWithBucket(t, tt.bucketType)
			replica, _, replicaCluster, _ := newFaultNodeWithBucket(t, tt.bucketType)

			seed := ownerPutEntry(t, key)
			if resp, err := asOwner(ownerCluster, seed); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("seeding the document: %+v, %v", resp, err)
			}
			seedSeq := owner.wal.LastSequence()

			kv.FailNextReadOf(rawDocumentKey(t, owner, "app", "held", key))
			write := tt.entry(t, key)
			resp, _ := asOwner(ownerCluster, write)
			message, _ := bson.Raw(resp.Payload).Lookup("message").StringValueOK()
			if !strings.Contains(message, "injected read failure") {
				t.Fatalf("the %s did not fail on the armed owner-side read: response %q", tt.name, message)
			}
			failedSeq := owner.wal.LastSequence()
			if !lookupBucket(owner, "app", "held").owedApplies().Owes() {
				t.Fatalf("after the failed read the bucket's carry owes nothing: sequence %d would not be recorded by the next apply", failedSeq)
			}

			later := ownerPutEntry(t, "later")
			if resp, err := asOwner(ownerCluster, later); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("the later write: %+v, %v", resp, err)
			}
			laterSeq := owner.wal.LastSequence()
			if state := bucketStateOf(t, owner, "app", "held"); !reflect.DeepEqual(state.FailedSeqs, []uint64{failedSeq}) {
				t.Fatalf("after the later write at %d failed_seqs = %v, want [%d]: replay would skip the failed read's entry for good",
					laterSeq, state.FailedSeqs, failedSeq)
			}

			// The replica applies what the owner shipped: its read works.
			seed.Seq, write.Seq, later.Seq = seedSeq, failedSeq, laterSeq
			seed.OwnerOutcome, write.OwnerOutcome, later.OwnerOutcome = ownerOutcomeApplied, ownerOutcomeOwed, ownerOutcomeApplied
			if push := pushToReplica(t, replicaCluster, frameSyncSource, []ReplEntry{seed, write, later}); !push.Success || push.AppliedThrough != laterSeq {
				t.Fatalf("the replica applied through %d (success %t, %q), want %d", push.AppliedThrough, push.Success, push.Error, laterSeq)
			}

			notReplayedBefore := notReplayedCount(t)
			restarted := restartOwner(t, owner, kv, dataDir)

			if tt.multi {
				if moved := notReplayedCount(t) - notReplayedBefore; moved != 1 {
					t.Fatalf("hoarddb_apply_failures_not_replayed_total moved by %v after the restart, want 1: a multi-update beneath the mark must be reported, not dropped", moved)
				}
				if state := bucketStateOf(t, restarted, "app", "held"); len(state.FailedSeqs) != 0 {
					t.Fatalf("after the restart failed_seqs = %v, want none: the loud path settles the record", state.FailedSeqs)
				}
				return
			}
			if got, want := documentOf(t, restarted, "app", "held", key), documentOf(t, replica, "app", "held", key); !reflect.DeepEqual(got, want) {
				t.Fatalf("after the restart the owner holds %v at %q and its replica holds %v: replay never retried the write", got, key, want)
			}
		})
	}
}

// TestAReplayedCarriedReadFailureThatFindsNothingIsSettled: a failed read is
// carried into failed_seqs, and a later write then removes what the entry
// would have changed. Replay of the entry finds nothing to write, so no apply
// consumes its sequence unless the handler settles it. Left in failed_seqs it
// outlives the restart and ends in a false "lost write" ERROR at the next one.
// Only failed_seqs is load-bearing here; the registry and carry assertions
// check that nothing else is left behind.
func TestAReplayedCarriedReadFailureThatFindsNothingIsSettled(t *testing.T) {
	const key = "k"
	for _, tt := range ownerReadFailureCases {
		t.Run(tt.name, func(t *testing.T) {
			owner, kv, ownerCluster, dataDir := newFaultNodeWithBucket(t, tt.bucketType)
			if resp, err := asOwner(ownerCluster, ownerPutEntry(t, key)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("seeding the document: %+v, %v", resp, err)
			}
			failedSeq := failOwnerRead(t, tt, owner, kv, ownerCluster, key)

			removal := ownerReadFailureCase{name: "delete", bucketType: tt.bucketType, op: transport.OpDelete}
			if resp, err := asOwner(ownerCluster, removal.entry(t, key)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("the later write that removes the document: %+v, %v", resp, err)
			}
			if state := bucketStateOf(t, owner, "app", "held"); !reflect.DeepEqual(state.FailedSeqs, []uint64{failedSeq}) {
				t.Fatalf("test setup: failed_seqs = %v, want [%d]", state.FailedSeqs, failedSeq)
			}

			restarted := restartOwner(t, owner, kv, dataDir)
			if state := bucketStateOf(t, restarted, "app", "held"); len(state.FailedSeqs) != 0 {
				t.Fatalf("after the restart failed_seqs = %v, want none: the replay found nothing to write and never consumed sequence %d", state.FailedSeqs, failedSeq)
			}
			if n := restarted.owed.count(); n != 0 {
				t.Fatalf("after the restart the owed registry holds %d sequences, want none: it holds WAL retention for good", n)
			}
			if restarted.bucketOwesApplies("app", "held") {
				t.Fatal("after the restart the bucket still reports it owes an apply")
			}
		})
	}
}

// TestAReplayedMultiUpdateThatNoLongerMatchesIsReportedNotSettledQuietly: a
// filtered multi-update whose read failed is replayed after a later field
// update moved its document out of the filter. It matches nothing now, yet its
// replicas applied it while the document matched, so the owner's copy differs
// from theirs. A multi-update is never treated as commuting (SA-27): replay
// settles the record and reports it, as it does for one that still matches. An
// update of one document that is gone stays quiet, because an update never
// changes whether a document exists.
func TestAReplayedMultiUpdateThatNoLongerMatchesIsReportedNotSettledQuietly(t *testing.T) {
	const key = "k"
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			owner, kv, ownerCluster, dataDir := newFaultNodeWithBucket(t, bucketType)
			if resp, err := asOwner(ownerCluster, ownerPutEntry(t, key)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("seeding the document: %+v, %v", resp, err)
			}
			filtered, err := bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Multi: true,
				Filter: map[string]interface{}{"n": 1}, Set: map[string]interface{}{"a": 2}})
			if err != nil {
				t.Fatalf("marshalling the filtered multi-update: %v", err)
			}
			kv.FailNextReadOf(rawDocumentKey(t, owner, "app", "held", key))
			resp, _ := asOwner(ownerCluster, ReplEntry{Op: int32(transport.OpUpdate), Bucket: "held", Payload: filtered})
			if !strings.Contains(string(resp.Payload), "injected read failure") {
				t.Fatalf("the multi-update did not fail on the armed read: %s", resp.Payload)
			}
			failedSeq := owner.wal.LastSequence()

			moved := ownerReadFailureCase{name: "update", bucketType: bucketType, op: transport.OpUpdate}.entry(t, key)
			moved.Payload, err = bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Key: key,
				Set: map[string]interface{}{"n": 3}})
			if err != nil {
				t.Fatalf("marshalling the update that leaves the filter: %v", err)
			}
			if resp, err := asOwner(ownerCluster, moved); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("the later update: %+v, %v", resp, err)
			}
			if state := bucketStateOf(t, owner, "app", "held"); !reflect.DeepEqual(state.FailedSeqs, []uint64{failedSeq}) {
				t.Fatalf("test setup: failed_seqs = %v, want [%d]", state.FailedSeqs, failedSeq)
			}

			notReplayedBefore := notReplayedCount(t)
			restarted := restartOwner(t, owner, kv, dataDir)
			if state := bucketStateOf(t, restarted, "app", "held"); len(state.FailedSeqs) != 0 {
				t.Fatalf("after the restart failed_seqs = %v, want none", state.FailedSeqs)
			}
			if movedBy := notReplayedCount(t) - notReplayedBefore; movedBy != 1 {
				t.Fatalf("hoarddb_apply_failures_not_replayed_total moved by %v, want 1: the multi-update that matched on its replicas was settled without a word", movedBy)
			}
		})
	}
}

// TestReplayedRecordedFailureRefusedByHandlerIsConsumed is BUG-0586: a write
// the owner's store failed is recorded in failed_seqs and retried by replay.
// A later put made the field a string, so at replay the update handler
// refuses the $inc before store.Apply runs, and nothing consumed the
// sequence: it stayed owed until the log's floor passed it and a misattributed
// "restore from a replica" ERROR followed. The sequence is consumed at replay
// and reported through SA-27 there, because the owner cannot tell a write a
// later one superseded from one its replicas applied.
func TestReplayedRecordedFailureRefusedByHandlerIsConsumed(t *testing.T) {
	const key = "k"
	increment, err := bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Key: key, Inc: map[string]int64{"n": 1}})
	if err != nil {
		t.Fatalf("marshalling the increment: %v", err)
	}
	makeString, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "held", Key: key,
		Data: map[string]interface{}{"n": "text"}})
	if err != nil {
		t.Fatalf("marshalling the put that makes n a string: %v", err)
	}
	cases := []struct {
		name string
		// fail arms the fault that fails the owner's first apply of the
		// increment and returns the text its error carries.
		fail func(t *testing.T, owner *Server, kv *kvfault.Store) string
	}{
		{"read-failure", func(t *testing.T, owner *Server, kv *kvfault.Store) string {
			kv.FailNextReadOf(rawDocumentKey(t, owner, "app", "held", key))
			return "injected read failure"
		}},
		{"apply-failure", func(t *testing.T, owner *Server, kv *kvfault.Store) string {
			failNextApplyOn(t, kv, anySeq)
			return errInjectedStoreFailure.Error()
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			owner, kv, c, dataDir := newFaultNodeWithBucket(t, BucketTypeHash)
			if resp, err := asOwner(c, ownerPutEntry(t, key)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("seeding the document: %+v, %v", resp, err)
			}
			wantInError := tt.fail(t, owner, kv)
			resp, _ := asOwner(c, ReplEntry{Op: int32(transport.OpUpdate), Bucket: "held", Key: []byte(key), Payload: increment})
			if !strings.Contains(string(resp.Payload), wantInError) {
				t.Fatalf("the increment did not fail on the armed fault: %s", resp.Payload)
			}
			failedSeq := owner.wal.LastSequence()

			if resp, err := asOwner(c, ReplEntry{Op: int32(transport.OpUpsert), Bucket: "held", Key: []byte(key), Payload: makeString}); err != nil ||
				transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("the put that makes n a string: %+v, %v", resp, err)
			}
			if state := bucketStateOf(t, owner, "app", "held"); !reflect.DeepEqual(state.FailedSeqs, []uint64{failedSeq}) {
				t.Fatalf("test setup: failed_seqs = %v, want [%d]", state.FailedSeqs, failedSeq)
			}

			notReplayedBefore := notReplayedCount(t)
			restarted := restartOwner(t, owner, kv, dataDir)

			if state := bucketStateOf(t, restarted, "app", "held"); len(state.FailedSeqs) != 0 {
				t.Fatalf("after the restart failed_seqs = %v, want none: the handler refused sequence %d before the store and nothing consumed it", state.FailedSeqs, failedSeq)
			}
			if n := restarted.owed.count(); n != 0 {
				t.Fatalf("after the restart the owed registry holds %d sequences, want none", n)
			}
			if restarted.bucketOwesApplies("app", "held") {
				t.Fatal("after the restart the bucket still reports it owes an apply")
			}
			if moved := notReplayedCount(t) - notReplayedBefore; moved != 1 {
				t.Fatalf("hoarddb_apply_failures_not_replayed_total moved by %v, want 1: the refused-at-replay write must be reported at replay", moved)
			}
		})
	}
}

// TestReplayedRecordedFailureThatFailsAgainStaysOwed guards the other side of
// BUG-0586's fix: a retry the store fails again reached the store, so it is
// not a refusal. It stays in failed_seqs for the next restart and is not
// reported as a divergence.
func TestReplayedRecordedFailureThatFailsAgainStaysOwed(t *testing.T) {
	const key = "k"
	owner, kv, c, dataDir := newFaultNodeWithBucket(t, BucketTypeHash)
	if resp, err := asOwner(c, ownerPutEntry(t, key)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
		t.Fatalf("seeding the document: %+v, %v", resp, err)
	}
	failNextApplyOn(t, kv, anySeq)
	resp, _ := asOwner(c, ownerReadFailureCase{name: "update", bucketType: BucketTypeHash, op: transport.OpUpdate}.entry(t, key))
	if !strings.Contains(string(resp.Payload), errInjectedStoreFailure.Error()) {
		t.Fatalf("the update did not fail on the armed fault: %s", resp.Payload)
	}
	failedSeq := owner.wal.LastSequence()
	if resp, err := asOwner(c, ownerPutEntry(t, "later")); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
		t.Fatalf("the later write: %+v, %v", resp, err)
	}

	failNextApplyOn(t, kv, func(seq uint64) bool { return seq == failedSeq })
	notReplayedBefore := notReplayedCount(t)
	restarted := restartOwner(t, owner, kv, dataDir)

	if state := bucketStateOf(t, restarted, "app", "held"); !reflect.DeepEqual(state.FailedSeqs, []uint64{failedSeq}) {
		t.Fatalf("after a retry the store failed again failed_seqs = %v, want [%d]: it must stay owed", state.FailedSeqs, failedSeq)
	}
	if moved := notReplayedCount(t) - notReplayedBefore; moved != 0 {
		t.Fatalf("hoarddb_apply_failures_not_replayed_total moved by %v, want 0: a store failure is not a refusal", moved)
	}
}

// TestReplayedRecordedFailureRefusedByTheFieldTypesIsConsumed: the declared
// field types refuse before the store too, and replayWALOnce counts such a
// refusal as a constraint's answer and moves on. A recorded failure of the
// entry was then never consumed (BUG-0586). The $inc passes the int32 check
// when it runs live and fails in the store; a later put leaves n so close to
// the bound that the replayed $inc is refused with FIELD_TYPE.
func TestReplayedRecordedFailureRefusedByTheFieldTypesIsConsumed(t *testing.T) {
	const key = "k"
	dataDir := t.TempDir()
	kv := kvfault.New()
	owner := newKVFaultServer(dataDir, kv)
	options := map[string]interface{}{"fields": bson.A{bson.M{"name": "n", "type": "int32"}}}
	if resp, err := owner.CreateBucket(CreateBucketRequest{Database: "app", Name: "held", Type: BucketTypeHash, Options: options}); err != nil || !resp.Success {
		t.Fatalf("creating the bucket: %+v, %v", resp, err)
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, owner, dataDir)
	t.Cleanup(c.cancel)

	put := func(n int32) ReplEntry {
		payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "held", Key: key, Data: map[string]interface{}{"n": n}})
		if err != nil {
			t.Fatalf("marshalling the put: %v", err)
		}
		// UPSERT: the same key is written again below.
		return ReplEntry{Op: int32(transport.OpUpsert), Bucket: "held", Key: []byte(key), Payload: payload}
	}
	if resp, err := asOwner(c, put(1)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
		t.Fatalf("seeding the document: %+v %s, %v", resp, resp.Payload, err)
	}
	increment, err := bson.Marshal(UpdateRequest{Database: "app", Bucket: "held", Key: key, Inc: map[string]int64{"n": 2147483000}})
	if err != nil {
		t.Fatalf("marshalling the increment: %v", err)
	}
	failNextApplyOn(t, kv, anySeq)
	resp, _ := asOwner(c, ReplEntry{Op: int32(transport.OpUpdate), Bucket: "held", Key: []byte(key), Payload: increment})
	if !strings.Contains(string(resp.Payload), errInjectedStoreFailure.Error()) {
		t.Fatalf("the increment did not fail on the armed fault: %s", resp.Payload)
	}
	failedSeq := owner.wal.LastSequence()
	if resp, err := asOwner(c, put(1000)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
		t.Fatalf("the later put: %+v %s, %v", resp, resp.Payload, err)
	}
	if state := bucketStateOf(t, owner, "app", "held"); !reflect.DeepEqual(state.FailedSeqs, []uint64{failedSeq}) {
		t.Fatalf("test setup: failed_seqs = %v, want [%d]", state.FailedSeqs, failedSeq)
	}

	notReplayedBefore := notReplayedCount(t)
	restarted := restartOwner(t, owner, kv, dataDir)
	if state := bucketStateOf(t, restarted, "app", "held"); len(state.FailedSeqs) != 0 {
		t.Fatalf("after the restart failed_seqs = %v, want none: the field types refused sequence %d before the store", state.FailedSeqs, failedSeq)
	}
	if moved := notReplayedCount(t) - notReplayedBefore; moved != 1 {
		t.Fatalf("hoarddb_apply_failures_not_replayed_total moved by %v, want 1", moved)
	}
}
