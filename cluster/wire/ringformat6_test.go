package wire

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestFormat5DirectoryIsRefused is the prefix bloom spec's T13, as later bumps moved it. Format 6
// tables carry the prefix hashes and the rule bits in every table; a format 5 directory's tables
// carry neither. Format 7 is the vouched cut: its directory holds the VOUCHED marker, and a build
// of `next` (format 6) never advances it. Format 8 is the ring user store (spec 8): a state level
// bump brings a format bump, so a directory whose ring/raft holds no user table is refused before
// the raft state is read. Format 9 is the first release (1.1): nothing on disk changed from 8, the
// number moved so that a directory of a development build is refused by the marker. A directory of
// an earlier format, the format 6 of a `next` build and the format 7 of the cut included, is refused
// at open, with nothing in it read past the marker,
// changed or deleted, in the text that names the directory and the fix. A fresh directory is
// marked with this build's format (ringFormat) before the store opens.
// Against: leaving ringFormat at the previous format.
func TestFormat5DirectoryIsRefused(t *testing.T) {
	marked := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(marked, "badger"))
	mustMkdir(t, filepath.Join(marked, "wal"))
	mustWrite(t, filepath.Join(marked, "badger", "000001.sst"), "a table without prefix hashes")
	mustWrite(t, filepath.Join(marked, "wal", "000001.seg"), "a log of the earlier build")
	mustWrite(t, filepath.Join(marked, ringFormatFile), "hoarddb ring format 5\n")

	markedByNext := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(markedByNext, "badger"))
	mustMkdir(t, filepath.Join(markedByNext, "wal"))
	mustWrite(t, filepath.Join(markedByNext, "badger", "000001.sst"), "a table of a build of next")
	mustWrite(t, filepath.Join(markedByNext, "wal", "000001.seg"), "a log of a build of next")
	mustWrite(t, filepath.Join(markedByNext, ringFormatFile), "hoarddb ring format 6\n")

	previous := strconv.Itoa(ringFormat - 1)
	markedByPrevious := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(markedByPrevious, "badger"))
	mustMkdir(t, filepath.Join(markedByPrevious, "wal"))
	mustWrite(t, filepath.Join(markedByPrevious, "badger", "000001.sst"), "a table of the previous format")
	mustWrite(t, filepath.Join(markedByPrevious, "wal", "000001.seg"), "a log of the previous format")
	mustWrite(t, filepath.Join(markedByPrevious, ringFormatFile), "hoarddb ring format "+previous+"\n")

	unmarked := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(unmarked, "badger"))
	mustWrite(t, filepath.Join(unmarked, "badger", "000001.sst"), "a table of the build before the marker")

	for _, tc := range []struct {
		name, dir, found string
	}{
		{"marked 5", marked, "FORMAT says: hoarddb ring format 5"},
		{"marked 6, a build of next's", markedByNext, "FORMAT says: hoarddb ring format 6"},
		{"marked with the previous format", markedByPrevious, "FORMAT says: hoarddb ring format " + previous},
		{"a store and no marker", unmarked, "no FORMAT file and a store is present"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := treeDigest(t, tc.dir)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			node, err := OpenNode(ctx, NodeConfig{Dir: tc.dir})
			if err == nil {
				_ = node.Close()
				t.Fatalf("OpenNode(%s) opened a directory of an earlier format", tc.dir)
			}
			if want := "refusing the ring directory: " + wantRingFormatRefusal(tc.dir, tc.found); err.Error() != want {
				t.Errorf("the refusal is\n  %q\nwant\n  %q", err.Error(), want)
			}
			after := treeDigest(t, tc.dir)
			if len(after) != len(before) {
				t.Fatalf("the refused directory holds %d entries, had %d: the refusal changed it", len(after), len(before))
			}
			for path, digest := range before {
				if after[path] != digest {
					t.Errorf("%s changed under the refusal", path)
				}
			}
		})
	}

	// A fresh directory: the marker names this build's format and is on disk before the store is
	// opened.
	fresh := filepath.Join(t.TempDir(), "ring")
	var markerAtStoreOpen string
	beforeStoreOpen = func(dir string) {
		raw, err := os.ReadFile(filepath.Join(dir, ringFormatFile))
		if err != nil {
			t.Errorf("reading the marker before the store opens: %v", err)
		}
		markerAtStoreOpen = string(raw)
	}
	t.Cleanup(func() { beforeStoreOpen = nil })
	node := u30iOpen(t, fresh)
	if markerAtStoreOpen != ringFormatLine {
		t.Fatalf("when the store opened the marker read %q, want %q", markerAtStoreOpen, ringFormatLine)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
}
