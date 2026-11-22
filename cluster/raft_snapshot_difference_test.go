package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// forgedSnapshot writes a snapshot file the way EncodeSnapshot does, with the header version and
// flags and the body given, and a checksum that matches them: a file another build could have
// written, so only its content, never its length or checksum, says it is foreign.
func forgedSnapshot(t *testing.T, st ClusterState, version, flags byte, body []byte) []byte {
	t.Helper()
	raw := make([]byte, snapshotHeaderBytes, snapshotHeaderBytes+len(body)+snapshotCRCBytes)
	copy(raw, snapshotMagic)
	raw[4], raw[5] = version, flags
	binary.BigEndian.PutUint64(raw[8:], uint64(st.Epoch))
	binary.BigEndian.PutUint64(raw[16:], uint64(st.StateSeq))
	binary.BigEndian.PutUint32(raw[24:], uint32(len(body)))
	raw = append(raw, body...)
	return binary.BigEndian.AppendUint32(raw, crc32.Checksum(raw, snapshotCRCTable))
}

// differentSnapshots are the four ways another build's snapshot differs, each with the words
// text 2 must say for it.
func differentSnapshots(t *testing.T, st ClusterState) map[string][2]any {
	t.Helper()
	body, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	withField := append([]byte{}, body[:len(body)-1]...)
	withField = bsoncore.AppendInt32Element(withField, "zz_new_field", 1)
	withField = append(withField, 0)
	binary.LittleEndian.PutUint32(withField, uint32(len(withField)))
	withValue := bytes.Replace(body, []byte("active"), []byte("frozen"), 1)
	if bytes.Equal(withValue, body) {
		t.Fatal("the state holds no active member to give an unknown value")
	}
	return map[string][2]any{
		"unknown field": {forgedSnapshot(t, st, snapshotVersion, snapshotFlagBSON, withField), `has the field "zz_new_field", which this build does not read`},
		"version":       {forgedSnapshot(t, st, snapshotVersion+1, snapshotFlagBSON, body), "is version 2 and this build reads version 1"},
		"flags":         {forgedSnapshot(t, st, snapshotVersion, snapshotFlagBSON|0x02, body), "sets flags this build does not define"},
		"unknown value": {forgedSnapshot(t, st, snapshotVersion, snapshotFlagBSON, withValue), `holds the value "frozen" for member state, which this build does not accept`},
	}
}

var textTwoTail = []string{
	"it was written by another build, or this build cannot read its own snapshot",
	"Start this node on that build",
	"keep its data directory untouched and replace the node",
	"'replace-node <node> --with <node>@<addr>'",
}

// writeSnapshot adds a snapshot file to dir's snapshot store as the library would, newer than
// anything the node wrote.
func writeSnapshot(t *testing.T, dir string, raw []byte) {
	t.Helper()
	store, err := hraft.NewFileSnapshotStoreWithLogger(snapshotStoreDir(dir), raftSnapshotsRetained, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := store.Create(1, 1_000_000, 1_000, hraft.Configuration{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

// directoryWithACompatibleSnapshot is a closed node directory holding one snapshot this build
// reads, and the state it holds.
func directoryWithACompatibleSnapshot(t *testing.T) (string, ClusterState) {
	t.Helper()
	dir := t.TempDir()
	first := openOrFail(t, quickRaft(t, dir, "n1"))
	createDatabase(t, first, "app", 1)
	createDatabase(t, first, "shop", 2)
	if err := first.TakeSnapshot(); err != nil {
		t.Fatal(err)
	}
	st := first.State()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, st
}

// TestAnIncompatibleSnapshotAtStartNamesTheFix (test 14): a directory whose NEWEST snapshot is
// foreign and which also holds an older compatible one is refused at open with text 2, one case per
// variant, and the library is never started on it (today it falls back to the older snapshot
// and replays the log into a state the cluster has moved on from, or panics in NewRaft). A
// snapshot whose own checksum is damaged, and one whose file fails the library's Open CRC, are
// damage and are left to the library (round-5 m2). Break: classify only a version difference,
// or call NewRaft first.
func TestAnIncompatibleSnapshotAtStartNamesTheFix(t *testing.T) {
	_, st := directoryWithACompatibleSnapshot(t)
	for name, c := range differentSnapshots(t, st) {
		t.Run(name, func(t *testing.T) {
			dir, _ := directoryWithACompatibleSnapshot(t)
			writeSnapshot(t, dir, c[0].([]byte))
			var sink lockedBuffer
			cfg := quickRaft(t, dir, "n1")
			cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Debug, Output: &sink})
			r, err := OpenRaft(cfg)
			if err == nil {
				_ = r.Close()
				t.Fatal("OpenRaft started on a directory whose newest snapshot this build cannot read")
			}
			_ = cfg.Listener.Close()
			var foreign *ForeignSnapshotError
			if !errors.As(err, &foreign) {
				t.Errorf("the refusal is not a *ForeignSnapshotError, so main cannot exit %d for it: %T %v", DivergedExitCode, err, err)
			}
			for _, want := range append([]string{c[1].(string)}, textTwoTail...) {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("text 2 does not say %q: %v", want, err)
				}
			}
			for _, library := range []string{"failed to load", "failed to get log", "restoring snapshot", "starting restore"} {
				if strings.Contains(sink.String(), library) {
					t.Errorf("the library ran (%q in its log) before the refusal:\n%s", library, sink.String())
				}
			}
		})
	}
	t.Run("a damaged checksum is left to the library", func(t *testing.T) {
		dir, st := directoryWithACompatibleSnapshot(t)
		raw, err := EncodeSnapshot(st)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 0xFF
		writeSnapshot(t, dir, raw)
		if r, err := OpenRaft(quickRaft(t, dir, "n1")); err != nil {
			t.Fatalf("a damaged snapshot was refused as a build difference: %v", err)
		} else {
			_ = r.Close()
		}
	})
	t.Run("an Open CRC error is damage", func(t *testing.T) {
		dir, st := directoryWithACompatibleSnapshot(t)
		raw, err := EncodeSnapshot(st)
		if err != nil {
			t.Fatal(err)
		}
		writeSnapshot(t, dir, raw)
		newest, _ := filepath.Glob(filepath.Join(snapshotDir(dir), "1000-1000000-*", "state.bin"))
		if len(newest) != 1 {
			t.Fatalf("found %v", newest)
		}
		damaged, err := os.ReadFile(newest[0])
		if err != nil {
			t.Fatal(err)
		}
		damaged[10] ^= 0xFF
		if err := os.WriteFile(newest[0], damaged, 0o600); err != nil {
			t.Fatal(err)
		}
		if r, err := OpenRaft(quickRaft(t, dir, "n1")); err != nil {
			t.Fatalf("a file that fails the library's CRC was refused as a build difference: %v", err)
		} else {
			_ = r.Close()
		}
	})
}

// TestALeaderSentSnapshotThisBuildCannotDecodeStopsTheNode (test 14, second half): Restore of a
// foreign snapshot records a divergence like Apply does, closes Diverged and logs text 2, so
// the leader's retries do not leave the node frozen at its old state; a damaged one is only
// refused. Break: leave Restore as it was (the error alone, no record).
func TestALeaderSentSnapshotThisBuildCannotDecodeStopsTheNode(t *testing.T) {
	f := divergedFSM(t)
	for name, c := range differentSnapshots(t, f.state) {
		t.Run(name, func(t *testing.T) {
			var sink lockedBuffer
			f := divergedFSM(t)
			f.logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: &sink})
			if err := f.Restore(io.NopCloser(bytes.NewReader(c[0].([]byte)))); err == nil {
				t.Fatal("Restore accepted a snapshot this build cannot read")
			}
			if f.diverged.Load() == nil {
				t.Fatal("Restore recorded no divergence")
			}
			if !closedWithin(f.divergedChan(), time.Second) {
				t.Fatal("Diverged was not closed")
			}
			for _, want := range append([]string{"the snapshot the leader sent", c[1].(string)}, textTwoTail...) {
				if !strings.Contains(sink.String(), want) {
					t.Errorf("text 2 does not say %q:\n%s", want, sink.String())
				}
			}
		})
	}
	damaged := divergedFSM(t)
	raw, err := EncodeSnapshot(damaged.state)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := damaged.Restore(io.NopCloser(bytes.NewReader(raw))); err == nil {
		t.Fatal("Restore accepted a damaged snapshot")
	}
	if damaged.diverged.Load() != nil {
		t.Fatal("a damaged snapshot was recorded as a build difference")
	}
}

// TestTheSnapshotRefusalsNoLongerSayTheNodeReplaysItsLog: a damaged snapshot is not replayed
// from the log by the library, it falls back to an older snapshot, so no refusal says the node
// "replays its log". Break: restore the old wording.
func TestTheSnapshotRefusalsNoLongerSayTheNodeReplaysItsLog(t *testing.T) {
	for name, raw := range map[string][]byte{
		"short":     {1, 2, 3},
		"not ours":  append([]byte("NOPE"), make([]byte, 40)...),
		"checksums": func() []byte { r, _ := EncodeSnapshot(ClusterState{ClusterVersion: 1}); r[len(r)-1] ^= 1; return r }(),
	} {
		_, err := DecodeSnapshot(raw)
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if strings.Contains(err.Error(), "replays its log") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
