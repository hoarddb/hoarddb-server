package server

import (
	"bytes"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// failOwnerRead arms the fault store's one-shot read fault on key's document,
// sends the case's mutation of it as the owner and returns the log sequence the
// entry took.
func failOwnerRead(t *testing.T, tt ownerReadFailureCase, s *Server, kv *kvfault.Store, c *Cluster, key string) uint64 {
	t.Helper()
	kv.FailNextReadOf(rawDocumentKey(t, s, "app", "held", key))
	resp, _ := asOwner(c, tt.entry(t, key))
	if !strings.Contains(string(resp.Payload), "injected read failure") {
		t.Fatalf("the %s did not fail on the armed owner-side read: %s", tt.name, resp.Payload)
	}
	return s.wal.LastSequence()
}

// TestAFailedPreWriteReadIsOwedOnlyWhileTheBucketCarriesIt is BUG-0583's
// rework as BUG-0584 changed it. A client update or delete whose owner-side
// pre-write read fails is a store failure. The owed registry holds its
// sequence, and so WAL retention, exactly while the bucket's carry or its
// failed_seqs owes it: the sequence is backed, so the log is held until a
// restart's replay retries it, and nothing is held once replay has settled it.
// BUG-0583 had removed an entry that nothing backed; that stays removed.
func TestAFailedPreWriteReadIsOwedOnlyWhileTheBucketCarriesIt(t *testing.T) {
	const key = "k"
	for _, tt := range ownerReadFailureCases {
		t.Run(tt.name, func(t *testing.T) {
			s, kv, c, dataDir := newFaultNodeWithBucket(t, tt.bucketType)
			if resp, err := asOwner(c, ownerPutEntry(t, key)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("seeding the document the failed read looks for: %+v, %v", resp, err)
			}

			failedSeq := failOwnerRead(t, tt, s, kv, c, key)
			if !lookupBucket(s, "app", "held").owedApplies().Owes() {
				t.Fatalf("the failed read at %d left the bucket's carry empty: nothing backs an owed entry", failedSeq)
			}
			if n := s.owed.count(); n != 1 {
				t.Fatalf("after the failed read the owed registry holds %d sequences, want 1 (%d): retention would prune the only copy before the retry", n, failedSeq)
			}
			if bound := c.retentionBound(); bound >= failedSeq {
				t.Fatalf("the retention bound is %d with the owed sequence %d unretried", bound, failedSeq)
			}

			if err := putThrough(c, asOwner, "held", "later", "v2"); err != nil {
				t.Fatalf("the later write: %v", err)
			}
			if state := bucketStateOf(t, s, "app", "held"); !reflect.DeepEqual(state.FailedSeqs, []uint64{failedSeq}) {
				t.Fatalf("after the later write failed_seqs = %v, want [%d]", state.FailedSeqs, failedSeq)
			}
			if bound := c.retentionBound(); bound >= failedSeq {
				t.Fatalf("the retention bound is %d once failed_seqs holds %d: the log must stay until replay retries it", bound, failedSeq)
			}

			restarted := restartOwner(t, s, kv, dataDir)
			if n := restarted.owed.count(); n != 0 {
				t.Fatalf("after replay settled the sequence the owed registry holds %d, want none", n)
			}
			if state := bucketStateOf(t, restarted, "app", "held"); len(state.FailedSeqs) != 0 {
				t.Fatalf("after replay failed_seqs = %v, want none", state.FailedSeqs)
			}
		})
	}
}

// TestAFailedPreWriteReadLogsTheStoreKey: the client's message leaves out the
// internal storage key of a failed read, and an operator diagnosing it needs
// it, so the owner logs it (logKeyReadFailure). BUG-0583's early return in
// deleteStoredDocument lost the line for a delete.
func TestAFailedPreWriteReadLogsTheStoreKey(t *testing.T) {
	const key = "k"
	for _, tt := range ownerReadFailureCases {
		t.Run(tt.name, func(t *testing.T) {
			s, kv, c, _ := newFaultNodeWithBucket(t, tt.bucketType)
			if resp, err := asOwner(c, ownerPutEntry(t, key)); err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
				t.Fatalf("seeding the document the failed read looks for: %+v, %v", resp, err)
			}
			var logged bytes.Buffer
			s.logger = slog.New(slog.NewTextHandler(&logged, nil))

			failOwnerRead(t, tt, s, kv, c, key)
			if !strings.Contains(logged.String(), "a node-store operation failed at a key") {
				t.Fatalf("the failed %s read logged no store key; log: %q", tt.name, logged.String())
			}
		})
	}
}
