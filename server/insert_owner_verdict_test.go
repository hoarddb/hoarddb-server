package server

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Spec §6.5 items 2 and 3 (build doc BD-4): an insert is decided once, at its
// owner. A replica applies what the owner applied and skips what the owner
// refused, live and when it replays its log after a restart, whatever its own
// copy holds. Each test drives a replica over the fault store with the frames
// its owner would ship (newReplicaOnFaultStore), so the owner's verdict on
// each entry is exactly the one under test.

const heldIDLine = "the replica held an _id_ its owner did not"

// verdictEntry is the owner's entry at sequence seq: op of key into
// app/held, with the owner's outcome.
func verdictEntry(t *testing.T, seq uint64, op transport.Opcode, key string, n int32, outcome int32) ReplEntry {
	t.Helper()
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "held", Key: key, Data: map[string]interface{}{"n": n}})
	if err != nil {
		t.Fatalf("encoding the %s of %s: %v", op, key, err)
	}
	return ReplEntry{Seq: seq, Op: int32(op), Bucket: "held", Key: []byte(key), Payload: payload, OwnerOutcome: outcome}
}

// recordVerdictLogs points the replica's cluster and server loggers at a
// recorder.
func recordVerdictLogs(c *Cluster) *recordedLog {
	recorder := &recordedLog{}
	logger := slog.New(recorder)
	c.logger = logger
	c.s.logger = logger
	return recorder
}

// heldN is n of key in app/held on s, or "absent".
func heldN(t *testing.T, s *Server, key string) string {
	t.Helper()
	return phase1Field(t, s, "held", key, "n")
}

func requireApplied(t *testing.T, resp ReplPushResponse, through uint64, what string) {
	t.Helper()
	if !resp.Success || resp.AppliedThrough != through {
		t.Fatalf("%s: answered success=%t applied_through=%d error=%q; want success through %d",
			what, resp.Success, resp.AppliedThrough, resp.Error, through)
	}
}

// TestReplicaAppliesOwnersInsertOutcome is T-05. A replica that already holds
// "x" applies its owner's applied insert of "x": no refusal, the owner's
// document replaces the held one, the difference is said once at ERROR and
// counted, and the stream from the owner is not held. A replica receiving an
// insert its owner refused skips it, whatever its own copy holds (here it
// does not hold the id, so deciding again would store it), and its mark moves
// past it; that is no disagreement and no divergence.
func TestReplicaAppliesOwnersInsertOutcome(t *testing.T) {
	s, _, c, _ := newReplicaOnFaultStore(t)
	requireApplied(t, pushToReplica(t, c, frameSyncSource, []ReplEntry{
		verdictEntry(t, 1, transport.OpUpsert, "x", 1, ownerOutcomeApplied)}), 1, "the setup upsert of x")
	recorder := recordVerdictLogs(c)
	divergences := divergencesCounted(t)
	const disagreementCounter = "hoarddb_replica_constraint_disagreements_total"
	disagreements := gatheredCounter(t, disagreementCounter)

	requireApplied(t, pushToReplica(t, c, frameSyncSource, []ReplEntry{
		verdictEntry(t, 2, transport.OpPut, "x", 2, ownerOutcomeApplied)}), 2, "the owner's applied insert of the held x")
	if got := heldN(t, s, "x"); got != "2" {
		t.Errorf("after the owner's applied insert the replica holds x with n %s; want the owner's document, n 2", got)
	}
	if moved := divergencesCounted(t) - divergences; moved != 1 {
		t.Errorf("hoarddb_replica_divergences_total moved by %v; want 1 for the held _id_", moved)
	}
	if said := recorder.matching(slog.LevelError, heldIDLine); len(said) != 1 {
		t.Errorf("%d ERROR lines say %q; want 1: %+v", len(said), heldIDLine, recorder.matching(slog.LevelError, ""))
	}
	requireApplied(t, pushToReplica(t, c, frameSyncSource, []ReplEntry{
		verdictEntry(t, 3, transport.OpPut, "after", 3, ownerOutcomeApplied)}), 3, "the next entry of the stream")

	divergences = divergencesCounted(t)
	errorsBefore := len(recorder.matching(slog.LevelError, ""))
	requireApplied(t, pushToReplica(t, c, frameSyncSource, []ReplEntry{
		verdictEntry(t, 4, transport.OpPut, "y", 4, ownerOutcomeRefusedByConstraint)}), 4, "the owner-refused insert of y")
	if got := heldN(t, s, "y"); got != "absent" {
		t.Errorf("the replica stored y (n %s), an insert its owner refused", got)
	}
	if mark, logged := bucketStateOf(t, s, "app", "held").AppliedSeq, s.wal.LastSequence(); mark != logged {
		t.Errorf("the replica's mark is %d after the skipped insert it logged at %d; the skip must be consumed", mark, logged)
	}
	if moved := divergencesCounted(t) - divergences; moved != 0 {
		t.Errorf("the skipped insert moved hoarddb_replica_divergences_total by %v", moved)
	}
	if moved := gatheredCounter(t, disagreementCounter) - disagreements; moved != 0 {
		t.Errorf("the skipped insert moved %s by %v", disagreementCounter, moved)
	}
	if loud := recorder.matching(slog.LevelError, ""); len(loud) != errorsBefore {
		t.Errorf("the skipped insert logged at ERROR: %+v", loud[errorsBefore:])
	}
}

// replayedLine is the last "replayed write-ahead log" summary s logged.
func replayedLine(t *testing.T, recorder *recordedLog) map[string]string {
	t.Helper()
	lines := recorder.matching(slog.LevelInfo, "replayed write-ahead log")
	if len(lines) == 0 {
		t.Fatal("the restarted replica logged no replay summary")
	}
	return lines[len(lines)-1].attrs
}

// TestRestartedReplicaDoesNotRedecideInsert is T-05c: T-05's two cases met
// again by the replica's replay after a power cut took the store's applies
// of them. The owner's applied insert of the held x is put over it again,
// said at most once and counted at most once; the owner-refused insert of y
// is skipped again. Neither is a failure, and the stream goes on.
func TestRestartedReplicaDoesNotRedecideInsert(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	requireApplied(t, pushToReplica(t, c, frameSyncSource, []ReplEntry{
		verdictEntry(t, 1, transport.OpUpsert, "x", 1, ownerOutcomeApplied)}), 1, "the setup upsert of x")
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the setup: %v", err)
	}
	requireApplied(t, pushToReplica(t, c, frameSyncSource, []ReplEntry{
		verdictEntry(t, 2, transport.OpPut, "x", 2, ownerOutcomeApplied),
		verdictEntry(t, 3, transport.OpPut, "y", 3, ownerOutcomeRefusedByConstraint)}), 3, "the two inserts")

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if got := heldN(t, restarted.s, "x"); got != "1" {
		t.Fatalf("setup: after the power cut x has n %s; want the synced n 1, so replay meets both inserts", got)
	}
	recorder := recordVerdictLogs(restarted)
	divergences := divergencesCounted(t)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}

	if got := heldN(t, restarted.s, "x"); got != "2" {
		t.Errorf("after replay x has n %s; want the owner's document, n 2", got)
	}
	if got := heldN(t, restarted.s, "y"); got != "absent" {
		t.Errorf("replay stored y (n %s), an insert its owner refused", got)
	}
	if said := recorder.matching(slog.LevelError, heldIDLine); len(said) > 1 {
		t.Errorf("%d ERROR lines say %q; want at most 1", len(said), heldIDLine)
	}
	if moved := divergencesCounted(t) - divergences; moved > 1 {
		t.Errorf("replay moved hoarddb_replica_divergences_total by %v; want at most 1", moved)
	}
	for _, line := range recorder.matching(slog.LevelError, "") {
		if !strings.Contains(line.msg, heldIDLine) {
			t.Errorf("replay logged an ERROR other than the held _id_: %q", line.msg)
		}
	}
	if summary := replayedLine(t, recorder); summary["skipped"] != "0" {
		t.Errorf("replay reported %v; want skipped=0: neither insert is a failure", summary)
	}
	if st := bucketStateOf(t, restarted.s, "app", "held"); len(st.FailedSeqs) != 0 {
		t.Errorf("replay recorded failed_seqs %v", st.FailedSeqs)
	}
	requireApplied(t, pushToReplica(t, restarted, frameSyncSource, []ReplEntry{
		verdictEntry(t, 4, transport.OpPut, "after", 4, ownerOutcomeApplied)}), 4, "the stream after the restart")
}

// TestReplayedSkippedLastEntryIsReskipped is T-05d: an owner-refused insert is
// the last entry of the replica's log. Replay never moves the mark (spec §6.5
// item 3), so every restart meets the entry again, and each time it is
// skipped as its owner refused it: no divergence, no lost-write ERROR, the
// document absent and nothing in failed_seqs.
func TestReplayedSkippedLastEntryIsReskipped(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	requireApplied(t, pushToReplica(t, c, frameSyncSource, []ReplEntry{
		verdictEntry(t, 1, transport.OpPut, "y", 1, ownerOutcomeRefusedByConstraint)}), 1, "the owner-refused insert of y")

	killed := s
	for restart := 1; restart <= 2; restart++ {
		restarted := killedAfterPowerLoss(t, dataDir, kv, killed)
		recorder := recordVerdictLogs(restarted)
		divergences := divergencesCounted(t)
		if err := restarted.s.replayWAL(context.Background()); err != nil {
			t.Fatalf("restart %d: replaying the log: %v", restart, err)
		}
		if got := heldN(t, restarted.s, "y"); got != "absent" {
			t.Errorf("restart %d: replay stored y (n %s), an insert its owner refused", restart, got)
		}
		if moved := divergencesCounted(t) - divergences; moved != 0 {
			t.Errorf("restart %d: replay moved hoarddb_replica_divergences_total by %v", restart, moved)
		}
		if loud := recorder.matching(slog.LevelError, ""); len(loud) != 0 {
			t.Errorf("restart %d: replay logged at ERROR: %+v", restart, loud)
		}
		if summary := replayedLine(t, recorder); summary["skipped"] != "0" || summary["rejected_by_constraint"] != "1" {
			t.Errorf("restart %d: replay reported %v; want the entry met again and skipped as refused (rejected_by_constraint=1, skipped=0)",
				restart, summary)
		}
		if st := bucketStateOf(t, restarted.s, "app", "held"); len(st.FailedSeqs) != 0 {
			t.Errorf("restart %d: replay recorded failed_seqs %v", restart, st.FailedSeqs)
		}
		killed = restarted.s
	}
}
