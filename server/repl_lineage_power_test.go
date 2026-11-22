package server

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A replica passes an entry over by the receiver's rule (lineage_rule.go) in
// a state-only batch it does not sync: the copy already holds the entry, or it
// is of a term below the copy's. A power cut can take that batch while the
// entry stays in the log above the mark. Replay must judge the entry as the
// live apply did and pass it over again, never apply it (BUG-0588).

// secondSource is a node other than frameSyncSource that ships the queue's
// entries in the same term, as a ceded owner finishing its term's delivery
// does: its per-source position is its own, so an entry the copy already
// holds from frameSyncSource is logged and reaches the store's lineage rule.
const secondSource = "second-source"

// TestAPowerCutThatLosesALineagePassOversBatchDoesNotReApplyTheEntry: the copy
// holds push 1 from its owner, durably. A second source ships the same entry;
// the live apply passes it over as already held. The power then takes that
// pass-over's batch, and replay must not apply the second copy.
func TestAPowerCutThatLosesALineagePassOversBatchDoesNotReApplyTheEntry(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a")}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("setup: the owner's push was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the store: %v", err)
	}
	loggedBefore := s.wal.LastSequence()
	if resp := pushToReplica(t, c, secondSource, []ReplEntry{queuePush(t, 1, "a")}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("setup: the second source's copy of push 1 was answered success=%t applied_through=%d error=%q",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	if logged := s.wal.LastSequence() - loggedBefore; logged != 1 {
		t.Fatalf("setup: the second source's copy put %d entries in the log, want 1: it never reached the store's lineage rule", logged)
	}
	if order := appQueueOrder(t, s); !slices.Equal(order, []string{"a"}) {
		t.Fatalf("setup: the live apply did not pass the second copy over as already held: the queue is %v", order)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a"}) {
		t.Fatalf("after a power cut and the replay the queue is %v, want [a]: replay applied an entry the live apply passed over as already held", order)
	}
}

// TestAPowerCutThatLosesAnOlderTermPassOverDoesNotApplyTheEntry: the copy
// accepts a newer term between the push's admission and its apply, so the
// store passes the push over as of an older term. The term change is synced;
// the pass-over's batch is not, and the power takes it. Replay must not apply
// the push.
func TestAPowerCutThatLosesAnOlderTermPassOverDoesNotApplyTheEntry(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	var acceptOnce sync.Once
	var acceptErr error
	acceptNewerTerm := func(admitting *Cluster, entry ReplEntry) {
		if admitting != c || entry.Bucket != "queue" {
			return
		}
		// As acceptTerm does: the copy follows the new term from its base,
		// and replay applies nothing logged before it.
		acceptOnce.Do(func() {
			last := c.walLastSequence()
			acceptErr = s.rewriteLineageIf("app", "queue", func(st *store.BucketState) bool {
				st.Term, st.Through, st.Opened = st.Term+1, 0, false
				st.AppliedSeq, st.FailedSeqs = max(st.AppliedSeq, last), nil
				return true
			})
		})
	}
	afterReplicaAdmittedHook.Store(&acceptNewerTerm)
	defer afterReplicaAdmittedHook.Store(nil)

	olderBefore := c.entriesOfOlderTerm.Load()
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{queuePush(t, 1, "a")}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("setup: the push was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	afterReplicaAdmittedHook.Store(nil)
	if acceptErr != nil {
		t.Fatalf("setup: accepting the newer term: %v", acceptErr)
	}
	if got := c.entriesOfOlderTerm.Load() - olderBefore; got != 1 {
		t.Fatalf("setup: %d entries were passed over as of an older term, want 1", got)
	}
	if order := appQueueOrder(t, s); len(order) != 0 {
		t.Fatalf("setup: the live apply did not pass the push over: the queue is %v", order)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if order := appQueueOrder(t, restarted.s); len(order) != 0 {
		t.Fatalf("after a power cut and the replay the queue is %v, want it empty: replay applied an entry of an older term the live apply passed over", order)
	}
}

// TestReplayJudgesOnlyAnEntryThatCarriesItsTerm: the replay of another node's
// bucket entry carries the term its log record holds, with the position judged
// unless the source is clamped. An entry an earlier build logged (kind 2)
// carries no term and replays as that build replayed it, judged for nothing;
// so do this node's own entries and a create.
func TestReplayJudgesOnlyAnEntryThatCarriesItsTerm(t *testing.T) {
	s, _, c, _ := newReplicaOnFaultStore(t)
	const clampedSource = "clamped-source"
	c.appliedMu.Lock()
	c.recordedStream[clampedSource] = recordedStream{clamped: true}
	c.appliedMu.Unlock()
	push := queuePush(t, 9, "a")
	replicaPush := func(source string, notLogged bool) storage.WALEntry {
		return storage.WALEntry{Seq: 3, Op: uint8(push.Op), Bucket: "queue", Data: push.Payload,
			Origin: storage.WALOrigin{Kind: storage.WALOriginReplica, Source: source, Seq: 9, Term: 5, OutcomeNotLogged: notLogged}}
	}
	createPayload, err := bson.Marshal(CreateBucketRequest{Database: "app", Name: "queue", Type: BucketTypeFIFO})
	if err != nil {
		t.Fatalf("encoding a create: %v", err)
	}
	cases := []struct {
		name      string
		entry     storage.WALEntry
		wantJudge bool
		wantPos   bool
	}{
		{name: "another node's entry", entry: replicaPush(frameSyncSource, false), wantJudge: true, wantPos: true},
		{name: "another node's entry from a clamped source", entry: replicaPush(clampedSource, false), wantJudge: true},
		{name: "an entry an earlier build logged (kind 2)", entry: replicaPush(frameSyncSource, true)},
		{name: "this node's own entry", entry: storage.WALEntry{Seq: 3, Op: uint8(push.Op), Bucket: "queue", Data: push.Payload,
			Origin: storage.WALOrigin{Kind: storage.WALOriginOwn}}},
		{name: "another node's create", entry: storage.WALEntry{Seq: 3, Op: uint8(transport.OpCreateBucket), Bucket: "queue", Data: createPayload,
			Origin: storage.WALOrigin{Kind: storage.WALOriginReplica, Source: frameSyncSource, Seq: 9, Term: 5}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			judged, attached := entryTermFrom(s.withReplayedEntryTerm(context.Background(), tc.entry))
			if attached != tc.wantJudge {
				t.Fatalf("the replay is judged by lineage: %v, want %v", attached, tc.wantJudge)
			}
			if !attached {
				return
			}
			if judged.term != 5 || judged.checkPosition != tc.wantPos {
				t.Errorf("the replay is judged at term %d with the position %v, want term 5 with the position %v", judged.term, judged.checkPosition, tc.wantPos)
			}
		})
	}
}

// TestAPowerCutAfterATermAheadStopKeepsTheEntryForItsReSend: the store refuses
// a push of a term above its copy's and records the stop in the synced batch
// that consumes it. A power cut must leave that as it was: replay holds the
// stopped entry back rather than judge it again, and the owner's re-send
// applies.
func TestAPowerCutAfterATermAheadStopKeepsTheEntryForItsReSend(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	term, opened, err := s.bucketLineage("app", "queue")
	if err != nil {
		t.Fatalf("reading app/queue's lineage: %v", err)
	}
	rec := s.lockServedRecord("app", "queue")
	if rec == nil {
		t.Fatal("setup: app/queue is not served")
	}
	rec.setLineageLocked(term+1, opened)
	rec.mu.Unlock()
	ahead := queuePush(t, 1, "a")
	ahead.Term = term + 1
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{ahead}); resp.Success || !strings.Contains(resp.Error, "term") {
		t.Fatalf("setup: the push was answered success=%t error=%q; want it refused as term-ahead", resp.Success, resp.Error)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if order := appQueueOrder(t, restarted.s); len(order) != 0 {
		t.Fatalf("after a power cut and the replay the queue is %v, want it empty: the stopped push was applied", order)
	}
	if resp := pushToReplica(t, restarted, frameSyncSource, []ReplEntry{queuePush(t, 1, "a")}); !resp.Success || resp.AppliedThrough != 1 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a"}) {
		t.Fatalf("after the power cut and the owner's re-send the queue is %v, want [a]", order)
	}
}
