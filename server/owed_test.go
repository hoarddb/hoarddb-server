package server

import (
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// TestALargeCommitListPastTheThresholdWarnsOnce is BUG-0291's acceptance test.
// A bucket's large_commits list is rewritten by every write and is pruned only
// below the log's first sequence, so while a peer still holds the log back the
// list grows by one per large batch and nothing tells the operator. Past the
// threshold it warns once per start, naming the bucket, its length and the one
// fact the node knows — the local log still starts below the list — and points
// the operator at the members and at the warning that names the holder, as the
// sibling failed_seqs warning does.
func TestALargeCommitListPastTheThresholdWarnsOnce(t *testing.T) {
	s, _, c, _ := newFaultNodeWithBucket(t, BucketTypeHash)
	logger, captured := newLogCapture()
	s.logger = logger

	const below = 2 * failedSeqsWarnAt
	seedLargeCommits(t, s, "app", "held", below)

	if err := putThrough(c, asOwner, "held", "k", "v"); err != nil {
		t.Fatalf("the write over a large_commits list of %d: %v", below, err)
	}
	warns := captured.messages(slog.LevelWarn, "large commits", "held")
	if len(warns) != 1 {
		t.Fatalf("a large_commits list of %d wrote %d WARNs, want exactly one: %q", below, len(warns), warns)
	}
	if !strings.Contains(warns[0], strconv.Itoa(below)) {
		t.Fatalf("the WARN does not name the list's length %d: %q", below, warns[0])
	}
	// The node does not know which peer holds the log, only that the local log
	// still starts below the list. The WARN must say that and no more, then
	// name the HQL verb that lists the members and the log warning that names
	// the holder (BUG-0291 rework).
	for _, want := range []string{"nodes", "WAL retention on this node is held"} {
		if !strings.Contains(warns[0], want) {
			t.Fatalf("the WARN does not point the operator at %q: %q", want, warns[0])
		}
	}

	if err := putThrough(c, asOwner, "held", "k2", "v2"); err != nil {
		t.Fatalf("the second write: %v", err)
	}
	if got := captured.count(slog.LevelWarn, "large commits", "held"); got != 1 {
		t.Fatalf("the second write made %d large_commits WARNs, want the one already logged: %q",
			got, captured.messages(slog.LevelWarn))
	}
}

// seedLargeCommits writes n large-commit sequences into the bucket's state
// document, above any sequence the test's log hands out, so settleCarry keeps
// them. It builds the long list BUG-0291 warns about without a lagging peer.
func seedLargeCommits(t *testing.T, s *Server, database, bucket string, n int) {
	t.Helper()
	b := lookupBucket(s, database, bucket)
	if b == nil {
		t.Fatalf("bucket %s/%s is not served", database, bucket)
	}
	b.rec.mu.RLock()
	slot, databaseID, id := b.slot, b.databaseID, b.id.Load()
	b.rec.mu.RUnlock()
	snap := s.kv.NewSnapshot()
	st, err := store.LoadBucketState(snap, slot, databaseID, id)
	snap.Discard()
	if err != nil {
		t.Fatalf("loading the state of %s/%s: %v", database, bucket, err)
	}
	st.LargeCommits = make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		st.LargeCommits = append(st.LargeCommits, 1<<40+uint64(i))
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := st.Put(batch, slot, databaseID, id); err != nil {
		t.Fatalf("writing the seeded state of %s/%s: %v", database, bucket, err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the seeded state of %s/%s: %v", database, bucket, err)
	}
}
