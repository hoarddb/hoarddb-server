package wire

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The ring directory's format marker (storage-format spec §5.1): one line in
// <data_dir>/ring/FORMAT, written durably before the first Badger or log write and read
// first at every open. A directory that holds a store and no marker is format 1 by
// definition. The first release (1.1) is format 9, and it reads no other.
const (
	ringFormat       = 9
	ringFormatFile   = "FORMAT"
	ringFormatPrefix = "hoarddb ring format "
	ringFormatLine   = ringFormatPrefix + "9\n"
)

// RingFormatRefusal is why OpenNode refused a ring directory of another format (R3). Nothing
// in the directory was read past the marker, changed or deleted. No other build was released,
// so the text names none: it says which format this release reads and what the open found,
// then the fix for the node's kind. A single node starts on an empty ring directory; a cluster
// member is given the member advice and is never told to move its directory aside (BUG-0870).
type RingFormatRefusal struct {
	// Dir is the ring directory.
	Dir string
	// Format is the number the directory is in; empty when the marker does not parse.
	Format string
	// Found is how the format was learned: no marker beside a store, or the marker's line.
	Found string
	// Member is whether the node is configured as a cluster member (NodeConfig.Cluster set).
	Member bool
}

// Error implements error with spec §5.4's R3 text.
func (r *RingFormatRefusal) Error() string {
	advice := singleNodeEmptyRingAdvice(r.Dir)
	if r.Member {
		advice = ClusterMemberRingAdvice(r.Dir)
	}
	return fmt.Sprintf("the ring data directory %s is not in the format this release reads, which is format %d (%s). "+
		"Nothing in the directory was read, changed or deleted. %s",
		r.Dir, ringFormat, r.Found, advice)
}

// beforeStoreOpen, when set, runs right before the node opens Badger: the seam the marker
// test uses to see that FORMAT is on disk before the first store write. Nil in production.
var beforeStoreOpen func(dir string)

// ensureRingFormat is the open's first act: it reads the marker and refuses a directory of another format
// or an unreadable one, and writes the marker into a fresh one. The table is spec §5.1: no
// marker beside a badger/ or wal/ with content is format 1; no marker and no store is fresh
// (a raft/ or an incarnation record alone is fresh too, since this slice leaves the Raft
// state's format alone); the marker's own line decides the rest, and a marker that says
// anything else is refused naming what it read, never rewritten.
func ensureRingFormat(dir string) error {
	marker := filepath.Join(dir, ringFormatFile)
	raw, err := os.ReadFile(marker)
	switch {
	case err == nil:
		return checkRingMarker(dir, string(raw))
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("reading the ring format marker %s: %w", marker, err)
	}
	for _, part := range []string{"badger", "wal"} {
		populated, err := directoryHasEntries(filepath.Join(dir, part))
		if err != nil {
			return err
		}
		if populated {
			return &RingFormatRefusal{Dir: dir, Format: "1", Found: "no FORMAT file and a store is present"}
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating the ring directory %s: %w", dir, err)
	}
	if err := storage.WriteFileDurably(marker, []byte(ringFormatLine)); err != nil {
		return fmt.Errorf("writing the ring format marker %s: %w", marker, err)
	}
	return nil
}

// checkRingMarker judges the marker's content: exactly the canonical line of this build's
// format proceeds; another canonical number is refused naming it, and anything else (a
// leading zero or sign, a missing newline, extra text) is refused as unknown, naming what
// was read.
func checkRingMarker(dir, content string) error {
	if content == ringFormatLine {
		return nil
	}
	found := "FORMAT says: " + strings.TrimSpace(content)
	line, hasNewline := strings.CutSuffix(content, "\n")
	digits, hasPrefix := strings.CutPrefix(line, ringFormatPrefix)
	number, err := strconv.Atoi(digits)
	if !hasNewline || !hasPrefix || err != nil || number < 1 || strconv.Itoa(number) != digits {
		return &RingFormatRefusal{Dir: dir, Format: "", Found: found}
	}
	return &RingFormatRefusal{Dir: dir, Format: digits, Found: found}
}

// directoryHasEntries reports whether path holds a store: a directory with at least one
// entry, or a file where the directory should be (still not a fresh ring). A path that does
// not exist has none.
func directoryHasEntries(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("looking at %s to learn the ring format: %w", path, err)
	}
	if !info.IsDir() {
		return true, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, fmt.Errorf("listing %s to learn the ring format: %w", path, err)
	}
	return len(entries) > 0, nil
}
