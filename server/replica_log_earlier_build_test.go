package server

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// A node upgraded from a build before BUG-0581 holds replica entries whose
// origin block is kind 2: no owner outcome, no term. A retention pass keeps
// the segment that holds its bound, so practically every replica of a running
// cluster has some. The owner decided on 2026-10-16 that they are read as
// before: replay does not judge them by an outcome they do not carry, so they
// apply, count and leave positions exactly as on the build that wrote them,
// and only an entry logged after the upgrade gets the crash fix.

// rewriteAsAnEarlierBuildWrote rewrites, in the plain log under walDir, the
// origin block of every replica entry at a local sequence up to through into
// the layout of a build before BUG-0581, kind(1) len(source)(1) source seq(8),
// dropping the owner's outcome and the term, and re-checksums the entry. It
// returns how many it rewrote. A segment in format 2 keeps its header and is
// re-checksummed over its generation and the body, as the log reads it.
func rewriteAsAnEarlierBuildWrote(t *testing.T, walDir string, through uint64) int {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(walDir, "wal_*.log"))
	if err != nil {
		t.Fatalf("listing the log's segments: %v", err)
	}
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	rewritten := 0
	for _, segment := range segments {
		raw, err := os.ReadFile(segment)
		if err != nil {
			t.Fatalf("reading %s: %v", segment, err)
		}
		// A segment in format 2 starts with a 24-byte header (flag 0x01 at
		// byte 6) whose generation, bytes 16 to 24, each entry's CRC covers
		// before its body. The header stays; only the entries are rewritten.
		start, gen := 0, []byte(nil)
		if len(raw) >= 24 && raw[0] == 0x00 && string(raw[1:4]) == "HDB" && raw[6]&0x01 != 0 {
			start, gen = 24, raw[16:24]
		}
		earlier := append(make([]byte, 0, len(raw)), raw[:start]...)
		for pos := start; pos < len(raw); {
			bodyLen := int(uint32(binary.BigEndian.Uint64(raw[pos:])))
			body := raw[pos+8 : pos+8+bodyLen]
			pos += 8 + bodyLen
			if old, ok := bodyAsAnEarlierBuildWrote(body, through); ok {
				body = old
				rewritten++
			}
			checksum := crc32.Update(crc32.Update(0, castagnoli, gen), castagnoli, body)
			header := uint64(uint32(len(body))) | uint64(checksum)<<32
			earlier = binary.BigEndian.AppendUint64(earlier, header)
			earlier = append(earlier, body...)
		}
		if err := os.WriteFile(segment, earlier, 0o600); err != nil {
			t.Fatalf("writing %s back: %v", segment, err)
		}
	}
	return rewritten
}

// bodyAsAnEarlierBuildWrote is body, a plain entry's body, with a kind 3
// replica block in kind 2's layout, or false when body is not a replica entry
// at a sequence up to through.
func bodyAsAnEarlierBuildWrote(body []byte, through uint64) ([]byte, bool) {
	if binary.BigEndian.Uint64(body) > through {
		return nil, false
	}
	at := 8 + 1
	bucketWord := binary.BigEndian.Uint32(body[at:])
	at += 4 + int(bucketWord&^(1<<31))
	at += 4 + int(binary.BigEndian.Uint32(body[at:]))
	if bucketWord>>31 == 0 || body[at] != byte(storage.WALOriginReplica) {
		return nil, false
	}
	seqEnd := at + 2 + int(body[at+1]) + 8
	_, termWidth := binary.Uvarint(body[seqEnd+1:])
	old := append([]byte{}, body[:at]...)
	old = append(old, 2)
	old = append(old, body[at+1:seqEnd]...)
	return append(old, body[seqEnd+1+termWidth:]...), true
}

// crashedLeavingAnEarlierBuildsLog is crashedKeepingTheStore with the log's
// replica entries up to local sequence through rewritten as an earlier build
// wrote them, as an upgrade after the crash finds the log.
func crashedLeavingAnEarlierBuildsLog(t *testing.T, dataDir string, kv *kvfault.Store, s *Server, through uint64) *Cluster {
	t.Helper()
	orphanVoidFaultHook.Store(nil)
	divergenceRecordFaultHook.Store(nil)
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the store as a process crash keeps it: %v", err)
	}
	kv.PowerLoss()
	if err := s.wal.Close(); err != nil {
		t.Fatalf("closing the log: %v", err)
	}
	if rewritten := rewriteAsAnEarlierBuildWrote(t, filepath.Join(dataDir, "wal"), through); rewritten == 0 {
		t.Fatal("setup: the log holds no replica entry to rewrite")
	}
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the crash: %v", err)
	}
	cluster := retentionTestCluster(t, restarted, dataDir)
	t.Cleanup(cluster.cancel)
	if err := cluster.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the earlier build's log: %v", err)
	}
	return cluster
}

// requireAsBeforeTheFix requires restarted to hold what a build before
// BUG-0581 held after the same crash: no divergence, the source counted
// through seq, and the queue as want.
func requireAsBeforeTheFix(t *testing.T, restarted *Cluster, seq uint64, want []string) {
	t.Helper()
	requireNoDivergenceAfterRestart(t, restarted, seq)
	if position := appliedSnapshot(restarted)[frameSyncSource]; position != seq {
		t.Fatalf("restarted, the replica stands at %d for %s, want %d as before the fix", position, frameSyncSource, seq)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, want) {
		t.Fatalf("restarted, the replica holds the queue as %v, want %v as before the fix", order, want)
	}
}

// TestAnEarlierBuildsHandlerRefusalReplaysAsBeforeTheFix: the Reproduction's
// crash (an update its owner applied, refused by the handler, and a push),
// with the log as a build before BUG-0581 wrote it. The upgraded node opens
// it and replays it as that build did: the refusal counted applied, nothing
// recorded, the push applied.
func TestAnEarlierBuildsHandlerRefusalReplaysAsBeforeTheFix(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	after := ownerApplied(queuePush(t, 2, "a"))
	failTheStopsRecords(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{updateOfMissing(t, 1, ownerOutcomeApplied), after}); resp.Success || resp.AppliedThrough != 0 {
		t.Fatalf("setup: the frame was answered success=%t applied_through=%d error=%q; want the update refused as diverged",
			resp.Success, resp.AppliedThrough, resp.Error)
	}

	restarted := crashedLeavingAnEarlierBuildsLog(t, dataDir, kv, s, math.MaxUint64)
	requireAsBeforeTheFix(t, restarted, 2, []string{"a"})
}

// TestAnEarlierBuildsOtherIncarnationCreateReplaysAsBeforeTheFix: a create of
// another incarnation of app/held, refused, and the crash before its record,
// with the log as a build before BUG-0581 wrote it. That build's replay
// judged nothing, so the create counts applied and nothing is recorded: the
// judgement of another incarnation reads no outcome, so it is the entry's
// kind, not its outcome, that keeps it from being judged.
func TestAnEarlierBuildsOtherIncarnationCreateReplaysAsBeforeTheFix(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	frame, live := createOfAnotherIncarnationOfHeld(t, s)
	failTheStopsRecords(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, frame); resp.Success || resp.AppliedThrough != 0 {
		t.Fatalf("setup: the create was answered success=%t applied_through=%d error=%q; want it refused as another incarnation",
			resp.Success, resp.AppliedThrough, resp.Error)
	}

	restarted := crashedLeavingAnEarlierBuildsLog(t, dataDir, kv, s, math.MaxUint64)
	requireAsBeforeTheFix(t, restarted, 1, []string{})
	if _, held, err := restarted.s.openBucketIdentity("app", "held"); err != nil || held != live {
		t.Fatalf("restarted, app/held is incarnation %d (%v), want the %d it held", held, err, live)
	}
}

// TestALogOfAnEarlierBuildAndThisOneReplaysEachAsItsBuildDoes: the
// Reproduction's crash on a build before BUG-0581, restarted on this build,
// then the same crash again on this build. The second restart replays a log
// that holds both kinds: the earlier build's refusal still counts applied with
// its push, and this build's refusal is recorded, not counted, and stalls the
// owner's re-send.
func TestALogOfAnEarlierBuildAndThisOneReplaysEachAsItsBuildDoes(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	failTheStopsRecords(t, c)
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{updateOfMissing(t, 1, ownerOutcomeApplied), ownerApplied(queuePush(t, 2, "a"))}); resp.Success {
		t.Fatalf("setup: the earlier build's frame was answered success: %+v", resp)
	}
	upgraded := crashedLeavingAnEarlierBuildsLog(t, dataDir, kv, s, math.MaxUint64)
	requireAsBeforeTheFix(t, upgraded, 2, []string{"a"})

	frame := []ReplEntry{updateOfMissing(t, 3, ownerOutcomeApplied), ownerApplied(queuePush(t, 4, "b"))}
	failTheStopsRecords(t, upgraded)
	if resp := pushToReplica(t, upgraded, frameSyncSource, frame); resp.Success || !strings.Contains(resp.Error, "sequence 3") {
		t.Fatalf("setup: this build's frame was answered success=%t applied_through=%d error=%q; want the update refused as diverged",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
	restarted := crashedKeepingTheStore(t, dataDir, kv, upgraded.s)

	if restarted.shouldApply(frameSyncSource, 2) {
		t.Fatalf("restarted, the replica no longer counts the earlier build's entries through 2 applied (position %d)",
			appliedSnapshot(restarted)[frameSyncSource])
	}
	if !restarted.shouldApply(frameSyncSource, 3) {
		t.Fatalf("restarted, the replica counts this build's refused entry at 3 applied (position %d): its owner would never send it again",
			appliedSnapshot(restarted)[frameSyncSource])
	}
	if record, recorded := restarted.divergenceFrom(frameSyncSource); !recorded || record.Seq != 3 || record.Reason != refusalDiverged {
		t.Fatalf("restarted, the divergence from %s is %+v (recorded %t); want one at sequence 3, reason %q", frameSyncSource, record, recorded, refusalDiverged)
	}
	if order := appQueueOrder(t, restarted.s); !slices.Equal(order, []string{"a"}) {
		t.Fatalf("restarted, the replica holds the queue as %v, want [a]: the earlier build's push, and nothing past this build's refusal", order)
	}
	if resp := pushToReplica(t, restarted, frameSyncSource, frame); resp.Success || resp.AppliedThrough >= 3 {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q; want it refused at the entry at 3",
			resp.Success, resp.AppliedThrough, resp.Error)
	}
}
