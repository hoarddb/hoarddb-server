package server

import (
	"strings"
	"testing"
)

// TestCatalogTableUnlinkReportsARecordMissingFromItsChain is BUG-0359 part
// two: unlink takes the record out of its hash chain before it marks the
// record unlinked. A record the chain cannot reach is an invariant breach, and
// marking it unlinked anyway would finish the drop of a record the index
// cannot name — the breach becomes invisible and the record's slot is freed
// while the chain still has whatever is left of it.
func TestCatalogTableUnlinkReportsARecordMissingFromItsChain(t *testing.T) {
	table := newCatalogTableWithHasher(func(string) uint64 { return 7 })
	first := claimNamed(t, table, "first")
	second := claimNamed(t, table, "second")

	// Cut first out of the chain behind the table's back, as a corrupted or
	// mis-linked index would: the head now reaches second alone.
	table.indexSet(7, second.self+1)
	second.next = 0
	first.next = 0
	if table.find("first") != nil {
		t.Fatal("the chain still reaches the record the test cut out; the test proves nothing")
	}
	linked := table.linked

	err := table.unlink(first)
	if err == nil {
		t.Fatal("unlink of a record its chain cannot reach returned nil; the breach is silent")
	}
	if !strings.Contains(err.Error(), "chain") {
		t.Fatalf("the error from a record missing from its chain does not name the chain: %v", err)
	}
	if first.state != recLinked {
		t.Fatalf("a record that could not be unlinked is in state %d, want recLinked (%d)", first.state, recLinked)
	}
	if table.linked != linked {
		t.Fatalf("a failed unlink changed the linked count from %d to %d", linked, table.linked)
	}
	if first.next != 0 {
		t.Fatalf("a failed unlink rewrote the record's chain link to %d", first.next)
	}
	if got := table.find("second"); got != second {
		t.Fatalf("after the refused unlink find(second) returned %v, want %v", got, second)
	}
}

// TestADropWhoseRecordCannotBeUnlinkedSaysTheDropIsDone is BUG-0634. The
// refusal sits after the drop's batch has committed (commitDropBatch, then
// window.recordCommitted), so the catalog no longer holds the bucket, and a
// restart rebuilds the registry from that catalog. The answer used to end
// "restart the node and retry the drop"; after the restart the retry answers
// not-found, because there is nothing left to drop. It must say the drop is
// complete and that no retry is needed.
//
// The breach is forced from dropCommitted, the last phase before the unlink: a
// record taken out of the linked state, as a mis-linked index or a torn drop
// would leave it. A broken chain, the breach the test above builds, cannot
// reach this branch through bucketTable.unlink: find's guard answers false,
// nil once the chain stops naming the record, and every record find still
// reaches is one chainRemove still reaches. unlink's other error lands on this
// same branch with this same wording.
func TestADropWhoseRecordCannotBeUnlinkedSaysTheDropIsDone(t *testing.T) {
	s := newTestServer(t)
	const database, name = "app", "unlinkable"
	createTestBucket(t, s, database, name, BucketTypeHash)

	forced := false
	hook := func(phase dropPhase, atDatabase, atBucket string) {
		if phase != dropCommitted || atDatabase != database || atBucket != name {
			return
		}
		db := s.serverDataAccessor().getDB(atDatabase)
		db.mu.Lock()
		defer db.mu.Unlock()
		// The test file is outside TestNoCodeReachesIntoTheBucketMap's sweep
		// (it skips _test.go), which is the only place the registry is taken
		// out of its bucketTable.
		table := db.table.records
		rec := table.find(atBucket)
		if rec == nil {
			t.Errorf("setup: the index no longer names %q before the test forced the breach", atBucket)
			return
		}
		rec.state = recUnlinked
		forced = true
	}
	dropPhaseHook.Store(&hook)
	t.Cleanup(func() { dropPhaseHook.Store(nil) })

	resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: name})
	if err != nil {
		t.Fatalf("dropping %s/%s: %v", database, name, err)
	}
	if !forced {
		t.Fatal("setup: the drop never reached its commit, so the test forced nothing")
	}
	if resp.Success {
		t.Fatalf("the drop answered success though its record could not be unlinked: %+v", resp)
	}
	if strings.Contains(resp.Message, "retry the drop") {
		t.Fatalf("the refusal still tells the user to retry a drop that has already committed: %q", resp.Message)
	}
	for _, want := range []string{"already complete", "restart", "no retry is needed", "show buckets in " + database} {
		if !strings.Contains(resp.Message, want) {
			t.Fatalf("the refusal does not say %q: %q", want, resp.Message)
		}
	}
	if _, catalogued, err := s.readCatalogEntry(database, name); err != nil || catalogued {
		t.Fatalf("the refusal's drop is not complete: the catalog still holds %s/%s (catalogued %v, %v)", database, name, catalogued, err)
	}
}
