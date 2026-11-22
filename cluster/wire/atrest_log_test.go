package wire

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
)

// atRestLogDocs is how many documents the sealed log run writes: a few hundred log entries.
const atRestLogDocs = 120

// sealedLogKeys are the fixed keys the sealed log run opens its node with.
func sealedLogKeys() *countingAtRest {
	return &countingAtRest{storeKey: bytes.Repeat([]byte{0x3C}, 32), logKeys: fixedLogKeys{key: bytes.Repeat([]byte{0x2D}, 32)}}
}

// readWholeLogThroughTheCursor reads the node's log from its first sequence to its last through
// the cursor the ring's replay and catch-up read with (cluster.LogReader), and fails unless every
// sequence in between comes back once, in order.
func readWholeLogThroughTheCursor(t *testing.T, node *Node) int {
	t.Helper()
	first, err := node.wal.FirstSequence()
	if err != nil {
		t.Fatalf("reading the log's first sequence: %v", err)
	}
	last := node.wal.LastSequence()
	if last < first {
		t.Fatalf("SCENARIO NOT REACHED: the log holds no entry (first %d, last %d)", first, last)
	}
	cursor, err := node.wal.OpenCursor(first)
	if err != nil {
		t.Fatalf("opening a cursor at %d: %v", first, err)
	}
	want := first
	for want <= last {
		entries, next, err := node.wal.ReadBatch(cursor, last, 64<<10)
		if err != nil {
			t.Fatalf("reading the sealed log through the cursor at %d: %v", want, err)
		}
		if len(entries) == 0 {
			t.Fatalf("the cursor stopped at %d of %d", want, last)
		}
		for _, entry := range entries {
			if entry.Seq != want {
				t.Fatalf("the cursor read sequence %d, want %d", entry.Seq, want)
			}
			want++
		}
		cursor = next
	}
	return int(last - first + 1)
}

// requireSealedSegments fails unless every segment of the log under ringDir has a sealed header.
func requireSealedSegments(t *testing.T, ringDir string) {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(ringDir, "wal", "wal_*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("no log segment under %s (%v)", filepath.Join(ringDir, "wal"), err)
	}
	for _, segment := range segments {
		header := make([]byte, 24)
		file, err := os.Open(segment)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := file.ReadAt(header, 0)
		if closeErr := file.Close(); readErr != nil || closeErr != nil {
			t.Fatalf("reading %s: %v %v", segment, readErr, closeErr)
		}
		if !storage.IsSealedWALSegmentHeader(header) {
			t.Fatalf("%s is not sealed", segment)
		}
	}
}

// TestASealedPreallocatedRingLogReadsBackThroughTheCursor: [REGRESSION RUN, no guard: the sealed
// log format and the preallocated segments are unchanged by encryption at rest, and a plaintext
// log passes the same reads] a single ring node with its keys and preallocated log segments
// takes documents, whose records the write path appends unsynced and syncs as a group; the whole
// log reads back through the cursor while the node runs and again after a restart, every segment
// is sealed, and every document is read back after the restart (ring encryption at rest spec
// §9.1 step 5).
func TestASealedPreallocatedRingLogReadsBackThroughTheCursor(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	keys := sealedLogKeys()
	node, err := OpenNode(ctx, NodeConfig{Dir: dir, AtRest: keys, WALPreallocate: true})
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	if !node.wal.Stats().Preallocated {
		t.Fatal("SCENARIO NOT REACHED: the node's log is not preallocated")
	}
	u30iSchema(t, node)
	for i := range atRestLogDocs {
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(fmt.Sprintf("u%03d", i), "Ada", int32(i))); err != nil {
			t.Fatalf("inserting u%03d: %v", i, err)
		}
	}
	running := readWholeLogThroughTheCursor(t, node)
	if err := node.Close(); err != nil {
		t.Fatalf("closing the node: %v", err)
	}
	requireSealedSegments(t, dir)

	reopened, err := OpenNode(ctx, NodeConfig{Dir: dir, AtRest: sealedLogKeys(), WALPreallocate: true})
	if err != nil {
		t.Fatalf("reopening the sealed node: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("closing the reopened node: %v", err)
		}
	})
	if again := readWholeLogThroughTheCursor(t, reopened); again != running {
		t.Fatalf("the reopened log reads %d entries through the cursor, the running one read %d", again, running)
	}
	for i := range atRestLogDocs {
		if _, err := reopened.Get(ctx, u30iDB, "User", fmt.Sprintf("u%03d", i)); err != nil {
			t.Fatalf("u%03d is not back after the restart: %v", i, err)
		}
	}
}
