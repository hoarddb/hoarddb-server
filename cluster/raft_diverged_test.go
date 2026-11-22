package cluster

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
)

// divergedEntry wraps raw command bytes as the committed entry at index.
func divergedEntry(index uint64, raw []byte) *hraft.Log {
	return &hraft.Log{Index: index, Term: 3, Type: hraft.LogCommand, Data: raw}
}

// divergedFSM is an FSM holding the test cluster's state, for the pieces that need no Raft.
func divergedFSM(t *testing.T) *clusterFSM {
	t.Helper()
	return &clusterFSM{state: fsmCluster(t)}
}

func divergedRaw(t *testing.T, cmd Command) []byte {
	t.Helper()
	raw, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestAnAheadStampSetsTheFlagFromTheRawHeaderBeforeDecode: Apply reads the stamp from bytes 2 to
// 10 before decoding, so an entry from a build whose command version this build refuses still
// sets the flag with the ahead stamp as its reason (the decode error that follows would set it
// too, with a reason that hides the cause). Break: compare after DecodeCommand (the unsupported-version entry then never reaches
// the comparison and the flag stays clear).
func TestAnAheadStampSetsTheFlagFromTheRawHeaderBeforeDecode(t *testing.T) {
	f := divergedFSM(t)
	ahead := uint64(f.state.StateSeq) + 5
	raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: ahead, Body: fsmCreateDatabaseBody("app", 1, 1)})
	raw[0] = CommandVersion + 1 // a version this build refuses: DecodeCommand refuses it before ApplyCommand
	if got := f.Apply(divergedEntry(11, raw)).(applyOutcome); got.err == nil {
		t.Fatal("the entry was applied")
	}
	got := f.diverged.Load()
	if got == nil {
		t.Fatal("an entry stamped above the state, from a version this build refuses, did not set the flag")
	}
	if !strings.Contains(got.Reason, "stamped at state_seq") {
		t.Fatalf("the record's reason is %q, want the ahead stamp named, not the decode error that would have followed", got.Reason)
	}
	if got.Index != 11 || got.Term != 3 || got.BaseSeq != ahead || got.StateSeq != f.state.StateSeq {
		t.Fatalf("the record is %+v, want index 11, term 3, base %d, state %d", *got, ahead, f.state.StateSeq)
	}
}

// TestEveryRefusalButStateMovedSetsTheFlag is the classification of section 3, trigger (a):
// a committed entry the node refuses for any reason but STATE_MOVED sets the flag, and the
// flag is set once (the first refusal is the one recorded).
func TestEveryRefusalButStateMovedSetsTheFlag(t *testing.T) {
	seq := func(f *clusterFSM) uint64 { return uint64(f.state.StateSeq) }
	good := fsmCreateDatabaseBody("orders", 2, 1)
	cases := []struct {
		name string
		raw  func(t *testing.T, f *clusterFSM) []byte
		want bool
	}{
		{"an unread field", func(t *testing.T, f *clusterFSM) []byte {
			return divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f),
				Body: fsmDoc(fsmStr("name", "x"), fsmI64("id", 9), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storagex", "memory"))})
		}, true},
		{"an unknown kind", func(t *testing.T, f *clusterFSM) []byte {
			raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f), Body: good})
			raw[1] = 0x1F
			return raw
		}, true},
		{"an unknown version", func(t *testing.T, f *clusterFSM) []byte {
			raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f), Body: good})
			raw[0] = 0x7F
			return raw
		}, true},
		{"an entry shorter than the header", func(t *testing.T, f *clusterFSM) []byte { return []byte{1, 5, 0} }, true},
		{"NOT_FOUND", func(t *testing.T, f *clusterFSM) []byte {
			return divergedRaw(t, Command{Version: CommandVersion, Kind: CommandDropDatabase, BaseSeq: seq(f), Body: fsmDoc(fsmI64("id", 4242))})
		}, true},
		{"STATE_MOVED", func(t *testing.T, f *clusterFSM) []byte {
			return divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f) - 1, Body: good})
		}, false},
		{"an accepted command", func(t *testing.T, f *clusterFSM) []byte {
			return divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f), Body: good})
		}, false},
	}
	for _, tc := range cases {
		f := divergedFSM(t)
		out := f.Apply(divergedEntry(20, tc.raw(t, f))).(applyOutcome)
		if (f.diverged.Load() != nil) != tc.want {
			t.Errorf("%s (outcome error %v): flag set = %v, want %v", tc.name, out.err, f.diverged.Load() != nil, tc.want)
		}
	}
	// CONFLICT: the same name created twice, the second at the right stamp.
	f := divergedFSM(t)
	first := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f), Body: good})
	if out := f.Apply(divergedEntry(20, first)).(applyOutcome); out.err != nil || f.diverged.Load() != nil {
		t.Fatalf("the first create: %v, flag %v", out.err, f.diverged.Load())
	}
	again := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: seq(f), Body: fsmCreateDatabaseBody("orders", 3, 1)})
	out := f.Apply(divergedEntry(21, again)).(applyOutcome)
	var refusal *Error
	if !errors.As(out.err, &refusal) || refusal.Code != CodeConflict {
		t.Fatalf("the second create gave %v, want CONFLICT", out.err)
	}
	if got := f.diverged.Load(); got == nil || got.Index != 21 {
		t.Fatalf("CONFLICT did not set the flag at index 21: %v", got)
	}
	f.Apply(divergedEntry(22, []byte{0}))
	if f.diverged.Load().Index != 21 {
		t.Fatal("a later refusal replaced the first record")
	}
}

// TestAFlaggedNodeRefusesToSnapshotTheFrozenState: Snapshot is refused once the flag is set, so
// the frozen state is never written and the refused entry is never compacted away. Break: let
// Snapshot encode the state.
func TestAFlaggedNodeRefusesToSnapshotTheFrozenState(t *testing.T) {
	f := divergedFSM(t)
	if snap, err := f.Snapshot(); err != nil {
		t.Fatalf("an unflagged node must snapshot: %v", err)
	} else {
		snap.Release()
	}
	f.Apply(divergedEntry(30, []byte{0}))
	snap, err := f.Snapshot()
	if err == nil {
		snap.Release()
		t.Fatal("a flagged node snapshotted its frozen state")
	}
	if !strings.Contains(err.Error(), "not snapshotting") || !strings.Contains(err.Error(), "disagrees with the cluster's") {
		t.Fatalf("the refusal does not say why: %v", err)
	}
}

// TestARefusedEntryFollowedBy200EntriesStampedAboveItSurvivesASnapshotAndARestart is round-5 B1,
// from the probe: a refused entry, 200 entries stamped above it (each STATE_MOVED before this
// change), a snapshot request and a restart. The snapshot is refused, so the refused entry stays in the
// log; the restart replays it and sets the flag before LogApplied can turn true. Break two: let
// Snapshot encode the frozen state (the snapshot exists, the refused entry is compacted, the
// restart finds LogApplied true with a frozen state and no flag). The split of an ahead stamp
// (break one) is TestAnAheadStampSetsTheFlagWhenOnlyTheAheadEntriesRemain.
func TestARefusedEntryFollowedBy200EntriesStampedAboveItSurvivesASnapshotAndARestart(t *testing.T) {
	dir := t.TempDir()
	r := openOrFail(t, quickRaft(t, dir, "n1"))
	createDatabase(t, r, "app", 1)
	frozen := r.State().StateSeq
	bad := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen),
		Body: fsmDoc(fsmStr("name", "x"), fsmI64("id", 9), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storagex", "memory"))})
	future := r.raft.Apply(bad, time.Second) // a raw append: the dry run is bypassed, as a foreign build's leader would
	if err := future.Error(); err != nil {
		t.Fatal(err)
	}
	badIndex := future.Index()
	for i := range 200 {
		raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen) + 1 + uint64(i), Body: fsmCreateDatabaseBody("d", int64(100+i), 1)})
		if err := r.raft.Apply(raw, time.Second).Error(); err != nil {
			t.Fatal(err)
		}
	}
	if !r.IsDiverged() {
		t.Fatal("the refused entry did not set the flag")
	}
	if err := r.TakeSnapshot(); err == nil {
		t.Fatal("a flagged node took a snapshot of its frozen state")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	noRaftGoroutines(t)

	again := openOrFail(t, quickRaft(t, dir, "n1"))
	var kept hraft.Log
	if err := again.store.GetLog(badIndex, &kept); err != nil {
		t.Fatalf("the refused entry is gone from the log after the restart: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !again.IsDiverged() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !again.IsDiverged() {
		t.Fatalf("the restart replayed the log (state_seq %d) and never set the flag again", again.State().StateSeq)
	}
	time.Sleep(200 * time.Millisecond)
	if again.LogApplied() {
		t.Fatalf("LogApplied is true on a flagged node at the frozen state_seq %d", again.State().StateSeq)
	}
	if got := again.State().StateSeq; got != frozen {
		t.Fatalf("the state moved from %d to %d after a refused entry", frozen, got)
	}
}

// TestAnAheadStampSetsTheFlagWhenOnlyTheAheadEntriesRemain is break one of round-5 B1: the
// refused entry is gone (a pre-spec build snapshotted the frozen state and compacted it), so the
// log holds only entries stamped above the restored state. The ahead split is then the only
// thing that sets the flag. Break: return STATE_MOVED for an ahead stamp.
func TestAnAheadStampSetsTheFlagWhenOnlyTheAheadEntriesRemain(t *testing.T) {
	f := divergedFSM(t)
	frozen := f.state.StateSeq
	for i := range 3 {
		raw := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(frozen) + 1 + uint64(i), Body: fsmCreateDatabaseBody("d", int64(100+i), 1)})
		f.Apply(divergedEntry(uint64(40+i), raw))
	}
	got := f.diverged.Load()
	if got == nil || got.Index != 40 {
		t.Fatalf("the first ahead entry did not set the flag: %v", got)
	}
	if f.state.StateSeq != frozen {
		t.Fatal("the frozen state moved")
	}
}

// TestLogAppliedIsFalseWhileDiverged: a node that has applied everything its log held reports
// LogApplied false once it records a divergence, so nothing keyed on it treats a frozen state as
// loaded. Break: report true.
func TestLogAppliedIsFalseWhileDiverged(t *testing.T) {
	r := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	createDatabase(t, r, "app", 1)
	if !r.LogApplied() {
		t.Fatal("a healthy node that applied its log reports it not applied")
	}
	bad := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: unreadFieldBody("x", 2)})
	if err := r.AppendWithoutDryRun(bad); err != nil {
		t.Fatal(err)
	}
	if r.LogApplied() {
		t.Fatal("LogApplied is true on a diverged node")
	}
}

// lockedBuffer is a log sink the library's goroutines and the test can share.
type lockedBuffer struct {
	mu   sync.Mutex
	text strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.text.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.text.String()
}

// TestTheFirstDivergenceIsLoggedOnce: a node that records a divergence says so in its log, once,
// naming the entry, the cause and what the node now refuses, so an operator reading the log of a
// node that stopped answering finds the reason. A second refused entry adds no line. Break:
// do not log in noteDivergence (no line), or log on every refused entry (two lines).
func TestTheFirstDivergenceIsLoggedOnce(t *testing.T) {
	var sink lockedBuffer
	cfg := quickRaft(t, t.TempDir(), "n1")
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: &sink})
	r := openOrFail(t, cfg)
	createDatabase(t, r, "app", 1)
	for i := range 2 {
		bad := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: unreadFieldBody("x", int64(2+i))})
		if err := r.AppendWithoutDryRun(bad); err != nil {
			t.Fatal(err)
		}
	}
	record, flagged := r.Divergence()
	if !flagged {
		t.Fatal("the refused entry did not set the flag")
	}
	var lines []string
	for _, line := range strings.Split(sink.String(), "\n") {
		if strings.Contains(line, "the node that proposed committed entry") {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("the divergence was logged %d times, want once:\n%s", len(lines), sink.String())
	}
	for _, want := range []string{fmt.Sprintf("entry %d", record.Index), "does not read"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("the log line does not say %q: %s", want, lines[0])
		}
	}
}

// TestTheDivergenceCausesAreNamed: the log line names an ahead stamp and a decode error as such,
// not only a refusal code. Break: pass the same cause word for each.
func TestTheDivergenceCausesAreNamed(t *testing.T) {
	cases := map[string]func(f *clusterFSM) []byte{
		"ahead stamp": func(f *clusterFSM) []byte {
			return divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(f.state.StateSeq) + 5, Body: fsmCreateDatabaseBody("d", 50, 1)})
		},
		"decode error": func(f *clusterFSM) []byte { return []byte{255, 1} },
	}
	for want, raw := range cases {
		var sink lockedBuffer
		f := divergedFSM(t)
		f.logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: &sink})
		f.Apply(divergedEntry(7, raw(f)))
		if !strings.Contains(sink.String(), want) || !strings.Contains(sink.String(), "entry 7") {
			t.Errorf("%s: the log line is %q", want, sink.String())
		}
	}
}
