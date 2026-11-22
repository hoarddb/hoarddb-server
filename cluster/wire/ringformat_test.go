package wire

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
)

// ringFormat1Fixture is a ring directory written by the build before the FORMAT marker
// existed (storage/testdata/ring-format-1.generator.txt says how): badger/ and wal/ with
// content, no raft/, no incarnation, no FORMAT.
const ringFormat1Fixture = "../../storage/testdata/ring-format-1"

// copyRingFixture copies the fixture into a fresh temp directory and returns it.
func copyRingFixture(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ring")
	if err := filepath.WalkDir(ringFormat1Fixture, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(ringFormat1Fixture, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dir, rel), 0o700)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, rel), raw, 0o600)
	}); err != nil {
		t.Fatalf("copying the fixture: %v", err)
	}
	return dir
}

// treeDigest is every file under dir: path, size and content hash, so a refusal can be
// shown to have left the tree byte-identical.
func treeDigest(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			out[rel+"/"] = "dir"
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		out[rel] = fmt.Sprintf("%x:%d", sum, len(raw))
		return nil
	}); err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	return out
}

// wantRingFormatRefusal is the whole R3 text for a single node: the directory, the one format
// this release reads, what the open found, that nothing was touched, and the fix.
func wantRingFormatRefusal(dir, found string) string {
	return "the ring data directory " + dir + " is not in the format this release reads, which is format " +
		strconv.Itoa(ringFormat) + " (" + found + "). Nothing in the directory was read, changed or deleted. " +
		singleNodeEmptyRingAdviceFor(dir)
}

func openExpectingRefusal(t *testing.T, dir string) *RingFormatRefusal {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, NodeConfig{Dir: dir})
	if err == nil {
		_ = node.Close()
		t.Fatalf("OpenNode(%s) opened a directory of another format; it must be refused", dir)
	}
	var refusal *RingFormatRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("OpenNode(%s) = %v, want a *RingFormatRefusal wrapped in it", dir, err)
	}
	return refusal
}

func TestARingDirectoryOfFormatOneIsRefusedAtOpen(t *testing.T) {
	dir := copyRingFixture(t)
	before := treeDigest(t, dir)
	refusal := openExpectingRefusal(t, dir)
	if want := wantRingFormatRefusal(dir, "no FORMAT file and a store is present"); refusal.Error() != want {
		t.Errorf("the refusal is\n  %q\nwant\n  %q", refusal.Error(), want)
	}
	after := treeDigest(t, dir)
	if len(before) != len(after) {
		t.Fatalf("the refused directory holds %d entries, had %d: the refusal changed it", len(after), len(before))
	}
	for path, digest := range before {
		if after[path] != digest {
			t.Errorf("%s changed under the refusal", path)
		}
	}
	for _, mustNotExist := range []string{"FORMAT", "FORMAT.tmp", "incarnation", "raft"} {
		if _, err := os.Stat(filepath.Join(dir, mustNotExist)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s exists after the refusal (stat: %v); the refusal must create nothing", mustNotExist, err)
		}
	}
}

func TestAFreshDirectoryGetsTheMarker(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(dir string)
	}{
		{"absent", func(string) {}},
		{"empty", func(dir string) { mustMkdir(t, dir) }},
		{"only raft and incarnation", func(dir string) {
			mustMkdir(t, filepath.Join(dir, "raft"))
			mustWrite(t, filepath.Join(dir, "incarnation"), "1\n")
		}},
		{"empty badger and wal", func(dir string) {
			mustMkdir(t, filepath.Join(dir, "badger"))
			mustMkdir(t, filepath.Join(dir, "wal"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ring")
			tc.setup(dir)
			node := u30iOpen(t, dir)
			raw, err := os.ReadFile(filepath.Join(dir, ringFormatFile))
			if err != nil {
				t.Fatalf("reading the marker: %v", err)
			}
			if string(raw) != ringFormatLine {
				t.Fatalf("marker = %q, want %q", raw, ringFormatLine)
			}
			if _, err := os.Stat(filepath.Join(dir, ringFormatFile+".tmp")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the marker's temp file is still there (stat: %v)", err)
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTheMarkerIsWrittenBeforeTheFirstWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	fired := false
	beforeStoreOpen = func(got string) {
		fired = true
		if got != dir {
			t.Errorf("the hook saw %s, want %s", got, dir)
		}
		raw, err := os.ReadFile(filepath.Join(dir, ringFormatFile))
		if err != nil {
			t.Errorf("FORMAT is not on disk when the store is about to open: %v", err)
			return
		}
		if string(raw) != ringFormatLine {
			t.Errorf("FORMAT holds %q before the store opens, want %q", raw, ringFormatLine)
		}
		for _, part := range []string{"badger", "wal"} {
			if entries, err := os.ReadDir(filepath.Join(dir, part)); err == nil && len(entries) > 0 {
				t.Errorf("%s/ already holds %d entries before the hook: the store was written before the marker", part, len(entries))
			}
		}
	}
	t.Cleanup(func() { beforeStoreOpen = nil })
	node := u30iOpen(t, dir)
	if !fired {
		t.Fatal("the hook before the store open never fired")
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAMarkerNamingAnotherFormatIsRefusedNamingWhatItRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, dir)
	next := strconv.Itoa(ringFormat + 1)
	mustWrite(t, filepath.Join(dir, ringFormatFile), "hoarddb ring format "+next+"\n")
	before := treeDigest(t, dir)
	refusal := openExpectingRefusal(t, dir)
	if !strings.Contains(refusal.Error(), wantRingFormatRefusal(dir, "FORMAT says: hoarddb ring format "+next)) {
		t.Fatalf("refusal %q does not name the format it read", refusal.Error())
	}
	if after := treeDigest(t, dir); len(after) != len(before) || after[ringFormatFile] != before[ringFormatFile] {
		t.Fatalf("the refusal changed the directory (before %v, after %v); a marker is never fixed", before, after)
	}
}

func TestAnUnparsableMarkerIsRefused(t *testing.T) {
	for _, line := range []string{"", "garbage\n", "hoarddb ring format two\n", "hoarddb ring format 2 extra\n", "hoarddb ring format 2\nhoarddb ring format 2\n", "hoarddb ring format 02\n", "hoarddb ring format +2\n", "hoarddb ring format 2", "hoarddb ring format  2\n"} {
		t.Run(strings.TrimSpace(line), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ring")
			mustMkdir(t, dir)
			mustWrite(t, filepath.Join(dir, ringFormatFile), line)
			before := treeDigest(t, dir)
			refusal := openExpectingRefusal(t, dir)
			want := wantRingFormatRefusal(dir, "FORMAT says: "+strings.TrimSpace(line))
			if refusal.Error() != want {
				t.Fatalf("refusal %q\n  lacks %q", refusal.Error(), want)
			}
			if after := treeDigest(t, dir); after[ringFormatFile] != before[ringFormatFile] {
				t.Fatal("the refusal rewrote the marker; a marker is never fixed")
			}
		})
	}
}

func TestAMarkedDirectoryReopensAndTheMarkerIsNotRewritten(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	first := u30iOpen(t, dir)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, ringFormatFile)
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(marker, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	second := u30iOpen(t, dir)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(marker)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(stamp) {
		t.Fatalf("the marker was rewritten on reopen (mtime %v, want %v)", info.ModTime(), stamp)
	}
}

func TestACrashAfterTheMarkerAndBeforeTheStoreReopensCleanly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, dir)
	mustWrite(t, filepath.Join(dir, ringFormatFile), ringFormatLine)
	node := u30iOpen(t, dir)
	u30iSchema(t, node)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestTheMarkerHasOneWriter is data-safety proof (c): the only writer of the marker is
// storage.WriteFileDurably (temp file, fsync, rename, fsync of the directory).
func TestTheMarkerHasOneWriter(t *testing.T) {
	raw, err := os.ReadFile("ringformat.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"os.WriteFile", "os.Create", "os.OpenFile"} {
		if strings.Contains(string(raw), banned) {
			t.Errorf("ringformat.go calls %s; the marker is written by storage.WriteFileDurably only", banned)
		}
	}
	if !strings.Contains(string(raw), "storage.WriteFileDurably") {
		t.Error("ringformat.go does not call storage.WriteFileDurably")
	}
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAVersionOneRecordStopsReplayLoudly is the fixture's second case (F4, R1): a FORMAT
// written by hand says 2 over a directory whose log holds version-1 records, so the marker
// passes and the first record stops the open with R1's text; nothing is applied, and the
// Badger half holds exactly the keys it held.
// The replay's refusal names the version it found and the one this release reads, names no other
// build, and ends in the single node's advice (store-vouched-cut review R-1, R-5). A member's open fails at its peer
// listener before the replay with this test's empty cluster options, so the member's text is
// TestAReplayRefusalGetsTheAdviceForWhatTheNodeIs's.
func TestAVersionOneRecordStopsReplayLoudly(t *testing.T) {
	const member = false
	dir := copyRingFixture(t)
	mustWrite(t, filepath.Join(dir, ringFormatFile), ringFormatLine)
	// The fixture's store predates the sync marker, and a store with data files and no marker is
	// refused before the log is read (S-4). A marker vouching every version lets the open reach
	// the replay this test is about, and the cut then discards nothing.
	writeVouchingMarker(t, filepath.Join(dir, "badger"), math.MaxUint64>>1)
	before := badgerKeyDigest(t, filepath.Join(dir, "badger"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, nodeConfigFor(t, dir, member))
	if err == nil {
		_ = node.Close()
		t.Fatal("a directory of version-1 records opened")
	}
	for _, want := range []string{
		"retained-log record at sequence 1: version 0x01 is not the 0x02 this release reads. Nothing was applied: ",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("OpenNode error %q\n  lacks %q", err, want)
		}
	}
	requireNoOtherBuild(t, err.Error())
	requireRingAdvice(t, err.Error(), dir, member, singleNodeUnreadLogAdviceFor(dir))
	if after := badgerKeyDigest(t, filepath.Join(dir, "badger")); after != before {
		t.Fatalf("the Badger half changed under the refused replay:\n before %s\n after  %s", before, after)
	}
}

// badgerKeyDigest is every key and value hash of a Badger directory, opened read-only for
// the purpose and closed again.
func badgerKeyDigest(t *testing.T, dir string) string {
	t.Helper()
	kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	iter := snapshot.NewIterator(storage.IterOptions{})
	defer iter.Close()
	sum := sha256.New()
	n := 0
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		sum.Write(iter.Key())
		sum.Write(mustValue(t, iter))
		n++
	}
	return fmt.Sprintf("%d keys %x", n, sum.Sum(nil))
}

// A file where badger/ or wal/ should be is still a store the marker never described: the
// refusal is R3, not a listing error.
func TestAFileNamedBadgerOrWalIsRefusedAsFormatOne(t *testing.T) {
	for _, name := range []string{"badger", "wal"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ring")
			mustMkdir(t, dir)
			mustWrite(t, filepath.Join(dir, name), "not a directory")
			refusal := openExpectingRefusal(t, dir)
			if !strings.Contains(refusal.Error(), "no FORMAT file and a store is present") {
				t.Fatalf("refusal %q does not give the R3 text", refusal.Error())
			}
		})
	}
}

// TestADirectoryOfTheBuildBeforeStorageOnTheDatabaseRowIsRefusedNamingTheFix: that build wrote
// marker 2 and a Raft snapshot whose database rows carry no storage element, and reading it
// failed deep in Raft with no text naming the fix. The marker now says 3, so the open refuses
// it first, names the directory and the fix, and leaves the directory as it found it.
func TestADirectoryOfTheBuildBeforeStorageOnTheDatabaseRowIsRefusedNamingTheFix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(dir, "badger"))
	mustMkdir(t, filepath.Join(dir, "raft"))
	mustWrite(t, filepath.Join(dir, "badger", "000001.vlog"), "an older store")
	mustWrite(t, filepath.Join(dir, "raft", "snapshots-placeholder"), "a snapshot without the storage element")
	mustWrite(t, filepath.Join(dir, ringFormatFile), "hoarddb ring format 2\n")
	before := treeDigest(t, dir)
	refusal := openExpectingRefusal(t, dir)
	if want := wantRingFormatRefusal(dir, "FORMAT says: hoarddb ring format 2"); refusal.Error() != want {
		t.Errorf("the refusal is\n  %q\nwant\n  %q", refusal.Error(), want)
	}
	after := treeDigest(t, dir)
	if len(after) != len(before) {
		t.Fatalf("the refused directory holds %d entries, had %d", len(after), len(before))
	}
	for path, digest := range before {
		if after[path] != digest {
			t.Errorf("%s changed under the refusal", path)
		}
	}
}

// TestADirectoryOfTheBuildBeforeTheUpdateTrailerIsRefusedNamingTheFix: directories marked format 3
// exist both with and without the trailer byte on update and delete record bodies, so the number 3
// was ambiguous and the marker was bumped to 4, which always has it. This build's replay refuses a
// body without it ("trailer needs 1 bytes"), so the open refuses a format 3 directory first,
// naming the directory and the fix, and leaves it as it found it.
func TestADirectoryOfTheBuildBeforeTheUpdateTrailerIsRefusedNamingTheFix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(dir, "wal"))
	mustWrite(t, filepath.Join(dir, "wal", "000001.seg"), "an update record body without the trailer byte")
	mustWrite(t, filepath.Join(dir, ringFormatFile), "hoarddb ring format 3\n")
	before := treeDigest(t, dir)
	refusal := openExpectingRefusal(t, dir)
	if want := wantRingFormatRefusal(dir, "FORMAT says: hoarddb ring format 3"); refusal.Error() != want {
		t.Errorf("the refusal is\n  %q\nwant\n  %q", refusal.Error(), want)
	}
	if after := treeDigest(t, dir); len(after) != len(before) || after["wal/000001.seg"] != before["wal/000001.seg"] {
		t.Fatalf("the refusal changed the directory")
	}
}

// TestADirectoryOfTheBuildBeforeTheClusterVersionIsRefusedNamingTheFix: format 4 is the last format
// written before the cluster version existed (StateLevel 2). The level moved to 3 with this format,
// so a directory at the older level is refused here, before any state is loaded, with the
// fresh-directory text.
func TestADirectoryOfTheBuildBeforeTheClusterVersionIsRefusedNamingTheFix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(dir, "raft"))
	mustWrite(t, filepath.Join(dir, "raft", "snapshots-placeholder"), "a snapshot without the cluster version field")
	mustWrite(t, filepath.Join(dir, ringFormatFile), "hoarddb ring format 4\n")
	before := treeDigest(t, dir)
	refusal := openExpectingRefusal(t, dir)
	if want := wantRingFormatRefusal(dir, "FORMAT says: hoarddb ring format 4"); refusal.Error() != want {
		t.Errorf("the refusal is\n  %q\nwant\n  %q", refusal.Error(), want)
	}
	if after := treeDigest(t, dir); len(after) != len(before) || after[ringFormatFile] != before[ringFormatFile] {
		t.Fatalf("the refusal changed the directory (before %v, after %v)", before, after)
	}
}

// TestTheRingFormatLineFollowsTheRingFormat keeps the two spellings of the number from drifting.
func TestTheRingFormatLineFollowsTheRingFormat(t *testing.T) {
	if want := fmt.Sprintf("%s%d\n", ringFormatPrefix, ringFormat); ringFormatLine != want {
		t.Fatalf("ringFormatLine is %q, ringFormat is %d: want %q", ringFormatLine, ringFormat, want)
	}
}
