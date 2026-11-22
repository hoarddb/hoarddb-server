// Package legacyqueue is the retired per-bucket segment store, kept here as a
// bench fixture. It measures the retired layout: before the shared-storage
// build, every fifo and lifo bucket was a directory of segment files opened by
// NewSegmentStore, and the VER-19 queue figures (queueopen, queueforest,
// opencost) were produced by driving exactly this code. Deleting it would
// delete the method behind those figures, so it lives on outside storage/,
// where nothing in the server can reach it. It is not the server's queue:
// that is store/queue.go, over the node's shared store.
package legacyqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// maxEntryLen is the largest entry length the store accepts: one entry plus
// its 8-byte length prefix must fit inside a single segment. Writes are
// validated against this (see ensureEntryFits) and reads use the *same*
// bound. A stricter read bound silently discards entries the store already
// accepted — which is how 10,000 1 MiB documents were written, counted, and
// then never returned by any pop or peek (2026-09-16 edge run).
func (ss *SegmentStore) maxEntryLen() uint64 {
	if ss.segmentSize <= 8 {
		return 0
	}
	return uint64(ss.segmentSize) - 8
}

// tombstoneFlag marks a deleted entry in its 8-byte length prefix: the length
// itself is preserved so a walk can still skip over the entry's bytes, and
// compaction is what reclaims the space.
const tombstoneFlag = uint64(1) << 63

// entryLength splits a raw length prefix into the entry's length and whether
// the entry was tombstoned.
func entryLength(raw uint64) (uint64, bool) {
	return raw &^ tombstoneFlag, raw&tombstoneFlag != 0
}

// minSegmentSize is the smallest segment the store accepts: a segment has to
// hold at least one entry header plus a little data.
const minSegmentSize = 64

// ensureEntryFits rotates to a fresh segment when the entry would not fit in
// the current one, so every entry is stored wholly inside a single segment:
// each segment then starts on an entry boundary and no entry is ever split
// across files. The head, tail and compaction walks all depend on that.
func (ss *SegmentStore) ensureEntryFits(n int) error {
	if int64(n)+8 > ss.segmentSize {
		return fmt.Errorf("entry of %d bytes exceeds the %d-byte segment size for %s buckets: split the document into smaller entries or store it in a blob bucket", n, ss.segmentSize, ss.storeType)
	}
	if err := ss.rotateAwayFromDivergentTail(); err != nil {
		return err
	}
	if ss.currentOff+8+int64(n) <= ss.segmentSize {
		return nil
	}
	if err := ss.closeCurrentSegment(); err != nil {
		return err
	}
	return ss.openNewSegment()
}

// rotateAwayFromDivergentTail is LIFO-only, and only does anything while a
// dump pin is held. popLIFO, while pinned, rewinds ss.currentOff without
// truncating the segment file, so a pinned dump cursor can still read a
// just-popped entry's bytes by offset (see popLIFO's truncate-defer). If a
// Push then reused that offset, writeBytes' ordinary WriteAt would
// physically overwrite those still-promised bytes: the dump would silently
// read the new document in place of the one it captured, with no error and
// no digest mismatch — the hazard flagged in task-1 review round 1.
//
// So: whenever pinned and the current segment's logical write cursor sits
// below its actual on-disk size (the signature of a deferred truncate),
// roll to a brand-new, empty segment instead of writing into that reclaimed
// space, and record the old segment's true logical size in
// divergentTailLogicalSize so moveBackwardSegment can use it later instead
// of trusting the file's on-disk size, which still includes the phantom
// tail.
func (ss *SegmentStore) rotateAwayFromDivergentTail() error {
	if ss.storeType != "lifo" || !ss.pinned() || ss.currentSeg == nil {
		return nil
	}
	info, err := ss.currentSeg.Stat()
	if err != nil {
		return fmt.Errorf("stat current segment while checking for a pinned divergent tail: %w", err)
	}
	if ss.currentOff >= info.Size() {
		return nil
	}

	slog.Info("segment rotated away from a pinned divergent tail", "segment_dir", ss.segmentDir, "store_type", ss.storeType, "segment_index", ss.tailIdx, "logical_size", ss.currentOff, "on_disk_size", info.Size())
	ss.divergentTailLogicalSize[ss.tailIdx] = ss.currentOff
	if err := ss.closeCurrentSegment(); err != nil {
		return err
	}
	return ss.openNewSegment()
}

// finalizeDeferredDivergence reclaims every segment still recorded in
// divergentTailLogicalSize, and is called once — from Unpin, when the last
// outstanding pin releases (task-1 review round 2, Important finding: left
// unbounded, one entry accumulates per pinned dump for the life of the
// process, since moveBackwardSegment only clears an entry if it happens to
// revisit that exact index later, which is not guaranteed). With no pin
// left, nothing can still need a phantom region's bytes preserved: a
// logical size of 0 (removeCurrentSegment's whole-segment case) means the
// entire segment is dead, so its file is removed outright; any other
// recorded size (rotateAwayFromDivergentTail's partial-tail case) means the
// file still holds live data up to that offset, so it is truncated down to
// it — finally performing the truncate popLIFO deferred, rather than
// leaving it deferred forever.
//
// An entry is only dropped once its file operation actually succeeds, so a
// transient failure (logged, not returned — Unpin has no error return, and
// a caller cannot act on this failure differently than any other) leaves
// the entry in place for the next attempt rather than silently exposing the
// phantom bytes it was protecting. Callers must hold ss.mu for writing.
func (ss *SegmentStore) finalizeDeferredDivergence() {
	for segIdx, logicalSize := range ss.divergentTailLogicalSize {
		// Truncate in both cases — a fully-phantom segment (logicalSize 0,
		// from removeCurrentSegment) included — rather than os.Remove for
		// the zero case: moveBackwardSegment, peekLIFO and peekNLIFO all
		// assume every index they step backward into still has a file on
		// disk, the same invariant that held before this store ever
		// deferred anything. Truncating to 0 reclaims the same disk space
		// os.Remove would, without breaking that assumption or requiring
		// three separate backward-scan call sites to learn to treat a
		// missing file as "empty, keep going" instead of "stop here."
		path := segmentPath(ss.segmentDir, segIdx, ss.storeType)
		if err := truncateSegmentFile(path, logicalSize); err != nil {
			slog.Error("finalizing a deferred segment after the last pin released", "segment_dir", ss.segmentDir, "store_type", ss.storeType, "segment_index", segIdx, "logical_size", logicalSize, "error", err)
			continue
		}
		delete(ss.divergentTailLogicalSize, segIdx)
	}
}

// truncateSegmentFile truncates the segment file at path down to size. A
// path that no longer exists is not an error: another finalization path
// (compact's own unconditional removal of every segment in its old range)
// may have already removed it.
func truncateSegmentFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("opening segment %s to finalize a deferred truncate: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("truncating segment %s to %d bytes: %w", path, size, err)
	}
	return nil
}

// SegmentStore implements a FIFO or LIFO segment-based append-only store.
// Data is written to fixed-size segment files on disk. FIFO reads from
// the head forward, LIFO reads from the tail backward.
type SegmentStore struct {
	mu          sync.RWMutex
	segmentDir  string
	segmentSize int64
	storeType   string // "fifo" or "lifo"
	currentSeg  *os.File
	currentOff  int64
	headSeg     *os.File
	headOff     int64
	tailOff     int64
	segCount    int
	entryCount  int64
	// tombstoneCount tracks tombstones written since the last compaction. It
	// is a compaction trigger hint only — it can overstate the real number
	// because consumed segments take their tombstones with them — so the
	// exact count is confirmed before a rewrite.
	tombstoneCount int64
	// tombstoneCountSeeded records whether tombstoneCount has been reconciled
	// with what is on disk since this store was opened. A reopened store starts
	// false and the first positional delete pays one walk to seed it; push, pop
	// and peek never read the counter, so a queue that is only pushed and popped
	// never pays for it at all. Open was O(entries) before this existed.
	tombstoneCountSeeded bool
	headIdx              int
	tailIdx              int

	// dirty records whether a mutation (Push, PushBatch, Pop, DeleteAtPosition,
	// DeleteAtID, compact) has happened since this store was opened or last
	// saved. Close consults it so a read-only session (Peek, PeekN, Walk,
	// DocCount, IterateDocuments, Length) costs nothing to close: no fsync,
	// no state.bson rewrite. It starts false on every open/recover
	// and is cleared by a successful Close, never by saveState itself — the
	// mutating callers that call saveState directly (Push on rotation,
	// PushBatch, Pop, tombstoneAt, compact) already persist unconditionally,
	// so leaving it true between them and the next Close costs nothing extra
	// and keeps the crash-safety ordering: a close that follows a mutation
	// still syncs and rewrites state before it returns.
	dirty bool

	// syncCount counts every fsync this store issues on a segment file,
	// through syncSegment. It exists so a test can observe an fsync
	// happening at all: /proc/self/io's rchar/wchar/syscr/syscw never move
	// for an fsync, because fsync carries no data and never touches the
	// kernel's read/write accounting path (confirmed directly:
	// 100 consecutive f.Sync() calls moved wchar/syscw by exactly 0). A
	// production build pays one int64 increment per fsync it was already
	// going to do, guarded by the same ss.mu every caller already holds.
	syncCount int64

	// openCount counts every time this store opens a segment file to read
	// an entry, through openSegmentForRead. Like syncCount, it exists
	// because the cost it counts — open, stat, close per entry
	// stepped over — moves no bytes, so /proc/self/io cannot see it either;
	// see openSegmentForRead's own comment. Unlike syncCount, it is its own
	// atomic counter, independent of ss.mu, for the same reason pinCount is:
	// IterateDocuments/DocCount deliberately call readEntryAt (and so this)
	// without ss.mu held (capturedRange's own comment), so two concurrent
	// dump walks over the same store can increment it at once.
	openCount int64

	// lastEntryStart/lastEntryLen cache where the newest live entry of the tail
	// segment begins, so a LIFO peek — and a LIFO pop — is one
	// read instead of a scan. It cannot be derived from tailOff: entries are
	// written as [8-byte length][data], so the length prefix of the last entry
	// is not adjacent to the end of the segment. Push sets it; popLIFO uses it
	// when valid (skipping the scan) and cannot repopulate it from that fast
	// path alone (the entry layout has no backward pointer to what came before
	// the one just popped — see popLIFO's own comment), so it clears the cache
	// on that path; when popLIFO instead falls back to a scan, it recovers the
	// position of the entry immediately before the one it pops "for free" from
	// that same scan and leaves the cache valid for the next pop.
	// moveBackwardSegment clears it (a different tail segment). Persisted in
	// state.bson so a reopened store need not rescan on its first
	// pop either — see the state struct's LastEntryStart/LastEntryLen/
	// LastEntryValid fields, which recover()/saveState() round-trip these
	// three through exactly the same unconditional writes Pop already forces.
	lastEntryStart int64
	lastEntryLen   uint64
	lastEntryValid bool

	// pinCount is the compaction pin (see Pin/Unpin): the number of
	// in-flight dumps currently reading a captured range from this store.
	// It is its own atomic counter, independent of mu, because the
	// functions that consult it (maybeCompact, compact, removeCurrentSegment,
	// advanceHeadSegment, popLIFO) are called with mu already held for
	// writing — re-acquiring mu to check it would deadlock.
	pinCount int64

	// divergentTailLogicalSize records, per segment index, the true logical
	// tail size of a LIFO segment that was rotated away from while pinned
	// and diverging from its on-disk size (see ensureEntryFits and
	// popLIFO's truncate-defer). Only ever populated and consumed while
	// ss.mu is held for writing, so — unlike pinCount — it needs no atomic
	// access of its own.
	//
	// The hazard this closes: popLIFO, while pinned, rewinds ss.currentOff
	// without truncating the file (so a pinned dump cursor can still read
	// the popped entry's bytes by offset). If a Push then reused that
	// offset via the ordinary WriteAt in writeBytes, it would physically
	// overwrite those still-promised bytes — the dump would silently read
	// the new document instead of the one it captured, with no error and
	// no digest mismatch. ensureEntryFits instead rotates LIFO to a fresh
	// segment whenever pinned and the write cursor sits below the current
	// segment's actual size, and records here what that segment's true
	// logical size was — because moveBackwardSegment, if it ever backs
	// into that segment later, must use that recorded size instead of the
	// file's on-disk size (which still includes the phantom, un-truncated
	// tail), or it would reopen the segment believing those already-popped
	// bytes are live again (a double-pop).
	//
	// removeCurrentSegment adds a second kind of entry, keyed the same way
	// but always valued 0: a segment moveBackwardSegment backed away from
	// after draining it fully via pops is entirely phantom, not just its
	// tail, and needs the same protection against every other reader that
	// trusts a historical segment's on-disk size (segmentLogicalSize,
	// readEntryAtLive).
	//
	// An entry here is transient by design, not permanent bookkeeping:
	// finalizeDeferredDivergence (called from Unpin once the last pin
	// releases) truncates or removes the segment it describes and deletes
	// the entry, and compact's own reset unconditionally clears the whole
	// map, because compact deletes every old segment file regardless —
	// both exist so this map cannot either (a) collide with a segment
	// index reused after compact's numbering reset, or (b) grow for the
	// life of a process that keeps pinning and unpinning without ever
	// compacting (task-1 review round 2, Critical and Important findings).
	divergentTailLogicalSize map[int]int64
}

// Pin marks a captured read range of this store as in use by a live dump,
// refusing compaction and segment removal until every matching Unpin call
// returns (see maybeCompact, compact, removeCurrentSegment,
// advanceHeadSegment and popLIFO, which all consult it). Pin/Unpin nest: a
// second, independent dump of the same bucket holds its own increment, and
// compaction stays refused until both release theirs.
//
// This is the compaction pin only. It protects the bytes a dump cursor
// captured from being rewritten or deleted out from under it; it does not
// fence pushes or pops, which is deliberate (spec dump-restore-spec.md
// §7.2) — fencing the queue for a dump's whole duration is exactly the cost
// a point-in-time cursor exists to avoid. A caller drives Pin/Unpin around
// the span it needs protected (a later task adds the ceiling that
// force-releases a pin whose dump never returned — not this store's job).
func (ss *SegmentStore) Pin() {
	n := atomic.AddInt64(&ss.pinCount, 1)
	slog.Info("segment compaction pinned", "segment_dir", ss.segmentDir, "store_type", ss.storeType, "pin_count", n)
}

// Unpin releases one Pin. Calling it more times than Pin was called is a
// caller bug — it would let compaction resume while a dump still believes
// its range is protected — so it is refused and logged rather than let the
// counter go negative and silently under-protect every future pin.
func (ss *SegmentStore) Unpin() {
	n := atomic.AddInt64(&ss.pinCount, -1)
	if n < 0 {
		atomic.AddInt64(&ss.pinCount, 1)
		slog.Error("segment compaction Unpin called without a matching Pin", "segment_dir", ss.segmentDir, "store_type", ss.storeType)
		return
	}
	if n == 0 {
		// The last outstanding pin just released, as observed by this
		// lock-free atomic read: no dump cursor can still need a deferred
		// segment's phantom bytes (a caller is expected to finish reading
		// before it unpins — see Pin's doc comment), so this looks like the
		// first safe moment to actually reclaim them rather than leave
		// divergentTailLogicalSize growing for the life of the process
		// (task-1 review round 2, Important finding).
		//
		// But Pin is deliberately lock-free too, so a brand-new, unrelated
		// Pin can land in the window between this read and ss.mu.Lock()
		// below — pinCount 0->1 from a second dump that has already started
		// its own captured-range walk by the time this goroutine finally
		// acquires the lock (task-1 review round 3, Critical finding: a
		// TOCTOU, not a data race — every access here is properly atomic or
		// mutex-guarded, but the *decision* was made before the lock that
		// makes it safe to act on was ever held). Re-check under the lock,
		// immediately before sweeping: if pinCount is no longer zero, some
		// other dump is now relying on divergentTailLogicalSize entries
		// this call has no way to distinguish from ones it would be safe to
		// reclaim, so it does nothing and leaves the sweep to whichever
		// future Unpin next observes a genuine zero under the lock.
		ss.mu.Lock()
		if atomic.LoadInt64(&ss.pinCount) == 0 {
			ss.finalizeDeferredDivergence()
		}
		ss.mu.Unlock()
	}
	slog.Info("segment compaction unpinned", "segment_dir", ss.segmentDir, "store_type", ss.storeType, "pin_count", n)
}

// pinned reports whether at least one dump currently holds the compaction
// pin on this store.
func (ss *SegmentStore) pinned() bool {
	return atomic.LoadInt64(&ss.pinCount) > 0
}

// Pinned is pinned's exported form, for a caller outside this package that
// needs to assert a compaction pin was actually released (not just that the
// dump session that held it is gone) — e.g. server package tests proving a
// dump session's release path really called Unpin rather than merely
// forgetting about the pin (whole-branch review, Important #3).
func (ss *SegmentStore) Pinned() bool {
	return ss.pinned()
}

// Tombstone compaction policy, from docs/Arcitecture.md: rewrite the store
// once more than a quarter of its entries are tombstones.
const (
	compactionTombstoneNum = 1
	compactionTombstoneDen = 4
)

// state represents the persistent state of a SegmentStore, serialized as
// BSON. TombstoneCount/TombstoneCountSeeded were added after the first format: a
// state.bson written before they existed decodes with both left at their BSON
// zero value (0, false), which is exactly "unreconciled" — the same starting
// point recover() already gives a brand-new store — so an old-format file
// needs no migration step, just the ordinary lazy reseed tombstoneAt already
// performs on that value.
// LastEntryStart/LastEntryLen/LastEntryValid were added after the first format: a
// state.bson written before they existed decodes them at their BSON zero
// value (0, 0, false) — LastEntryValid false is exactly "the cache cannot be
// trusted," the same condition a fresh, never-cached LIFO store already
// starts in, so an old-format file needs no migration step, just the
// ordinary scan popLIFO/peekLIFO already fall back to on a cache miss.
type state struct {
	HeadIdx              int    `bson:"head_idx"`
	HeadOff              int64  `bson:"head_off"`
	TailIdx              int    `bson:"tail_idx"`
	TailOff              int64  `bson:"tail_off"`
	EntryCount           int64  `bson:"entry_count"`
	TombstoneCount       int64  `bson:"tombstone_count"`
	TombstoneCountSeeded bool   `bson:"tombstone_count_seeded"`
	LastEntryStart       int64  `bson:"last_entry_start"`
	LastEntryLen         uint64 `bson:"last_entry_len"`
	LastEntryValid       bool   `bson:"last_entry_valid"`
}

// segmentPath returns the full path for a segment file with zero-padded index.
func segmentPath(dir string, idx int, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("seg_%05d.%s", idx, ext))
}

// stateFilePath returns the path to the state file.
func stateFilePath(dir string) string {
	return filepath.Join(dir, "state.bson")
}

// isSegmentFile checks if a filename matches the segment file pattern.
func isSegmentFile(name string, ext string) bool {
	prefix := "seg_"
	suffix := "." + ext
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	mid := name[len(prefix) : len(name)-len(suffix)]
	for _, c := range mid {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(mid) > 0
}

// parseSegmentIndex extracts the numeric index from a segment filename.
func parseSegmentIndex(name string) (int, error) {
	rest := strings.TrimPrefix(name, "seg_")
	dotIdx := strings.LastIndex(rest, ".")
	if dotIdx < 0 {
		return 0, fmt.Errorf("invalid segment filename: %s", name)
	}
	numStr := rest[:dotIdx]
	var idx int
	for _, c := range numStr {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid segment index in %s", name)
		}
		idx = idx*10 + int(c-'0')
	}
	return idx, nil
}

// bsonMarshalState serializes a state struct to BSON bytes.
func bsonMarshalState(s *state) []byte {
	data, _ := bson.Marshal(s)
	return data
}

// bsonUnmarshalState deserializes BSON bytes into a state struct.
func bsonUnmarshalState(data []byte, s *state) error {
	return bson.Unmarshal(data, s)
}

// NewSegmentStore creates a new segment store. dir is the directory for
// segment files, segmentSize is the maximum size per segment in bytes,
// and storeType must be "fifo" or "lifo".
func NewSegmentStore(dir string, segmentSize int64, storeType string) (*SegmentStore, error) {
	if storeType != "fifo" && storeType != "lifo" {
		return nil, fmt.Errorf("invalid store type %q: must be fifo or lifo", storeType)
	}
	if segmentSize < minSegmentSize {
		return nil, fmt.Errorf("segment size must be at least %d bytes, got %d", minSegmentSize, segmentSize)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating segment directory: %w", err)
	}

	ss := &SegmentStore{
		segmentDir:               dir,
		segmentSize:              segmentSize,
		storeType:                storeType,
		divergentTailLogicalSize: make(map[int]int64),
	}

	if err := ss.recover(); err != nil {
		return nil, fmt.Errorf("recovering segment store: %w", err)
	}

	return ss, nil
}

// recover scans the directory for existing segment files and state, validating
// and restoring the store to a consistent state.
func (ss *SegmentStore) recover() error {
	ext := ss.storeType
	entries, err := os.ReadDir(ss.segmentDir)
	if err != nil {
		return fmt.Errorf("reading segment directory: %w", err)
	}

	type segInfo struct {
		name  string
		index int
	}
	var segments []segInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if isSegmentFile(e.Name(), ext) {
			idx, err := parseSegmentIndex(e.Name())
			if err != nil {
				continue
			}
			segments = append(segments, segInfo{name: e.Name(), index: idx})
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].index < segments[j].index
	})

	// Try to read state file
	sfPath := stateFilePath(ss.segmentDir)
	var st state
	if data, readErr := os.ReadFile(sfPath); readErr == nil {
		if unmarshalErr := bsonUnmarshalState(data, &st); unmarshalErr == nil {
			ss.headIdx = st.HeadIdx
			ss.headOff = st.HeadOff
			ss.tailIdx = st.TailIdx
			ss.tailOff = st.TailOff
			ss.entryCount = st.EntryCount
			ss.segCount = len(segments)
			// A state file written before these fields existed decodes them
			// as their BSON zero value (0, false) — already "unreconciled",
			// the same state a brand-new ss starts in — so an old-format
			// file needs no special case: tombstoneAt's existing lazy seed
			// reruns exactly once, on this store's first
			// positional delete, same as it always has.
			ss.tombstoneCount = st.TombstoneCount
			ss.tombstoneCountSeeded = st.TombstoneCountSeeded
			// Same backward-compatibility note as tombstoneCount above: an
			// old-format file decodes LastEntryValid as false, which is
			// already "cannot be trusted," the state a fresh store starts
			// in — so popLIFO/peekLIFO's existing scan-on-miss path is the
			// migration case, not a special one.
			ss.lastEntryStart = st.LastEntryStart
			ss.lastEntryLen = st.LastEntryLen
			ss.lastEntryValid = st.LastEntryValid
		}
	}

	if len(segments) == 0 {
		return ss.openNewSegment()
	}

	// Validate tail segment — truncate if truncated mid-entry
	tailSegName := segments[len(segments)-1].name
	tailIdx := segments[len(segments)-1].index
	tailPath := filepath.Join(ss.segmentDir, tailSegName)
	tailFile, err := os.OpenFile(tailPath, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("opening tail segment %s: %w", tailSegName, err)
	}

	tailInfo, err := tailFile.Stat()
	if err != nil {
		_ = tailFile.Close()
		return fmt.Errorf("stat tail segment: %w", err)
	}

	// For LIFO, validate tail segment by reading backward
	if ss.storeType == "lifo" && tailInfo.Size() > 0 {
		tOff := tailInfo.Size()
		for tOff > 0 {
			if tOff < 8 {
				// Truncate to 0 — partial length prefix
				if err := tailFile.Truncate(0); err != nil {
					_ = tailFile.Close()
					return fmt.Errorf("truncating partial length prefix: %w", err)
				}
				tOff = 0
				break
			}
			buf := make([]byte, 8)
			n, readErr := tailFile.ReadAt(buf, tOff-8)
			if readErr != nil || n != 8 {
				tOff -= 8
				continue
			}
			length := binary.BigEndian.Uint64(buf)
			entryEnd := tOff - 8 + int64(length)
			if entryEnd > tailInfo.Size() {
				tOff -= 8
				continue
			}
			break
		}
		if err := tailFile.Truncate(tOff); err != nil {
			_ = tailFile.Close()
			return fmt.Errorf("truncating tail segment to valid boundary: %w", err)
		}
		tailInfo, _ = tailFile.Stat()
	}

	// For FIFO, open head segment
	if ss.storeType == "fifo" {
		headName := segments[0].name
		headPath := filepath.Join(ss.segmentDir, headName)
		headFile, openErr := os.OpenFile(headPath, os.O_RDWR, 0o644)
		if openErr != nil {
			_ = tailFile.Close()
			return fmt.Errorf("opening head segment %s: %w", headName, openErr)
		}
		ss.headSeg = headFile
		if ss.headIdx == 0 && ss.headOff == 0 {
			ss.headIdx = segments[0].index
		}
	}

	ss.currentSeg = tailFile
	ss.currentOff = tailInfo.Size()
	ss.tailIdx = tailIdx
	ss.tailOff = tailInfo.Size()
	ss.segCount = len(segments)

	return nil
}

// openNewSegment creates and opens a new empty segment file.
func (ss *SegmentStore) openNewSegment() error {
	idx := 0
	if ss.segCount > 0 {
		idx = ss.tailIdx + 1
	}
	// A LIFO tail can move backward (moveBackwardSegment, draining segments
	// one pop at a time) and then forward again past the same index — and
	// while a dump pin defers removeCurrentSegment's delete, the file at
	// that index is still sitting on disk. O_TRUNC below would silently
	// destroy it, which is exactly the bug task-1 review round 1 found
	// (there under ensureEntryFits's divergence check; this is the same
	// hazard one layer down, in the index-assignment itself). Skip forward
	// past any index that already has a file, pinned or not — a store that
	// somehow already has a stray file at the computed index should never
	// silently overwrite it either.
	for {
		path := segmentPath(ss.segmentDir, idx, ss.storeType)
		if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
			break
		}
		idx++
	}
	path := segmentPath(ss.segmentDir, idx, ss.storeType)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("creating segment %s: %w", path, err)
	}

	ss.currentSeg = f
	ss.currentOff = 0
	ss.tailIdx = idx
	ss.tailOff = 0
	ss.segCount++

	if ss.storeType == "fifo" && ss.headSeg == nil {
		ss.headSeg = f
		ss.headIdx = idx
		ss.headOff = 0
	}

	return nil
}

// saveState atomically writes the current state to disk.
func (ss *SegmentStore) saveState() error {
	st := state{
		HeadIdx:              ss.headIdx,
		HeadOff:              ss.headOff,
		TailIdx:              ss.tailIdx,
		TailOff:              ss.tailOff,
		EntryCount:           ss.entryCount,
		TombstoneCount:       ss.tombstoneCount,
		TombstoneCountSeeded: ss.tombstoneCountSeeded,
		LastEntryStart:       ss.lastEntryStart,
		LastEntryLen:         ss.lastEntryLen,
		LastEntryValid:       ss.lastEntryValid,
	}
	data := bsonMarshalState(&st)
	tmpPath := stateFilePath(ss.segmentDir) + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("writing state temp file: %w", err)
	}
	return os.Rename(tmpPath, stateFilePath(ss.segmentDir))
}

// syncSegment fsyncs f and records the call in ss.syncCount (see that
// field's comment). Every fsync this store issues for a segment's own
// durability goes through here rather than calling f.Sync() directly, so
// the counter reflects every one of them regardless of call site.
func (ss *SegmentStore) syncSegment(f *os.File) error {
	ss.syncCount++
	return f.Sync()
}

// writeEntry writes a length-prefixed entry to the current segment,
// handling cross-segment spanning for oversized entries.
func (ss *SegmentStore) writeEntry(data []byte) error {
	if err := ss.ensureEntryFits(len(data)); err != nil {
		return err
	}

	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, uint64(len(data)))

	if err := ss.writeBytes(header); err != nil {
		return fmt.Errorf("writing entry header: %w", err)
	}
	if err := ss.writeBytes(data); err != nil {
		return fmt.Errorf("writing entry data: %w", err)
	}

	if err := ss.syncSegment(ss.currentSeg); err != nil {
		return fmt.Errorf("fsyncing segment: %w", err)
	}

	ss.entryCount++
	return nil
}

// writeBytes writes raw bytes to the current segment.
func (ss *SegmentStore) writeBytes(data []byte) error {
	// WriteAt, not Write: the kernel file offset is stale after a Truncate
	// (LIFO pop) or after re-opening a previous segment, and would silently
	// write at the wrong position.
	n, err := ss.currentSeg.WriteAt(data, ss.currentOff)
	if err != nil {
		return fmt.Errorf("writing to segment: %w", err)
	}
	ss.currentOff += int64(n)
	return nil
}

// closeCurrentSegment closes the current write segment, syncing it first
// when the store is dirty (a segment no mutation has touched since
// it was opened needs nothing flushed — see the ss.dirty field comment).
// Every call site that reaches here through an active write (ensureEntryFits'
// mid-write rotation, rotateAwayFromDivergentTail) runs after that write's
// caller has already set ss.dirty, so the sync those rotations rely on is
// unaffected. For FIFO stores, if headSeg points to the same file, it is
// re-opened for reading so that popFIFO can continue reading entries from it.
func (ss *SegmentStore) closeCurrentSegment() error {
	if ss.currentSeg != nil {
		if ss.dirty {
			if err := ss.syncSegment(ss.currentSeg); err != nil {
				return fmt.Errorf("fsyncing before close: %w", err)
			}
		}
		// For FIFO stores, headSeg may reference the same file. Re-open it
		// for reading before closing the write handle.
		if ss.storeType == "fifo" && ss.headSeg == ss.currentSeg {
			headPath := segmentPath(ss.segmentDir, ss.headIdx, ss.storeType)
			f, err := os.Open(headPath)
			if err != nil {
				_ = ss.currentSeg.Close()
				return fmt.Errorf("re-opening head segment for reading: %w", err)
			}
			ss.headSeg = f
		}
		return ss.currentSeg.Close()
	}
	return nil
}

// Push appends data to the tail of the store. Creates a new segment
// if the current segment is full. Returns the offset of the entry.
func (ss *SegmentStore) Push(data []byte) (int64, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.dirty = true

	if ss.currentSeg == nil {
		if err := ss.openNewSegment(); err != nil {
			return 0, err
		}
	}

	idx := ss.tailIdx
	off := ss.currentOff

	if err := ss.writeEntry(data); err != nil {
		return 0, err
	}

	ss.tailOff = ss.currentOff
	// The entry just written is now the newest live one, so a LIFO peek can be
	// answered without scanning (see lastEntryStart).
	ss.lastEntryStart, ss.lastEntryLen, ss.lastEntryValid = off, uint64(len(data)), true

	if idx != ss.tailIdx {
		if err := ss.saveState(); err != nil {
			return 0, err
		}
	}

	return off, nil
}

// writeEntryNoSync writes a length-prefixed entry to the current segment,
// handling cross-segment spanning for oversized entries. Unlike writeEntry,
// it does NOT fsync — the caller is responsible for batching the fsync.
func (ss *SegmentStore) writeEntryNoSync(data []byte) error {
	if err := ss.ensureEntryFits(len(data)); err != nil {
		return err
	}

	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, uint64(len(data)))

	if err := ss.writeBytes(header); err != nil {
		return fmt.Errorf("writing entry header: %w", err)
	}
	if err := ss.writeBytes(data); err != nil {
		return fmt.Errorf("writing entry data: %w", err)
	}

	ss.entryCount++
	return nil
}

// PushBatch appends multiple entries to the tail of the store in a single
// lock acquisition. All entries are written in sequence with a single fsync
// at the end, which is significantly faster than repeated Push calls for
// bulk inserts. Returns the offsets of each entry.
func (ss *SegmentStore) PushBatch(items [][]byte) ([]int64, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.dirty = true

	if ss.currentSeg == nil {
		if err := ss.openNewSegment(); err != nil {
			return nil, err
		}
	}

	offsets := make([]int64, 0, len(items))
	for _, data := range items {
		off := ss.currentOff
		if err := ss.writeEntryNoSync(data); err != nil {
			return nil, err
		}
		offsets = append(offsets, off)
	}

	// Single fsync after the entire batch.
	if err := ss.syncSegment(ss.currentSeg); err != nil {
		return nil, fmt.Errorf("fsyncing batch: %w", err)
	}

	ss.tailOff = ss.currentOff
	if err := ss.saveState(); err != nil {
		return nil, err
	}

	return offsets, nil
}

// Pop removes and returns the next entry. For FIFO, reads from head forward.
// For LIFO, reads from tail backward.
func (ss *SegmentStore) Pop() ([]byte, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.storeType == "lifo" {
		return ss.popLIFO()
	}
	return ss.popFIFO()
}

// popFIFO reads the next entry from the head and advances the head pointer.
// Tombstoned entries (zero length prefix) are silently skipped.
func (ss *SegmentStore) popFIFO() ([]byte, error) {
	if ss.entryCount == 0 || ss.headSeg == nil {
		return nil, io.EOF
	}
	// A pop always advances ss.headOff, even for a call that only skips
	// tombstoned or corrupt entries before hitting EOF — see the loop below —
	// so mark dirty here rather than only on a successful return.
	ss.dirty = true

	for {
		// Read 8-byte length prefix
		header := make([]byte, 8)
		n, err := ss.headSeg.ReadAt(header, ss.headOff)
		if n > 0 && n < 8 {
			return nil, fmt.Errorf("segment %d is corrupt at offset %d: entry header crosses the segment boundary", ss.headIdx, ss.headOff)
		}
		if err != nil || n < 8 {
			// Segment exhausted, move to next
			if advErr := ss.advanceHeadSegment(); advErr != nil {
				return nil, advErr
			}
			if ss.headSeg == nil {
				return nil, io.EOF
			}
			continue
		}

		length, tombstoned := entryLength(binary.BigEndian.Uint64(header))

		// Sanity check: reject corrupted length prefixes. The bound is the
		// same one writes enforce, so an entry this store accepted is never
		// mistaken for corruption here.
		if length > ss.maxEntryLen() {
			// Corrupted length — skip this entry and continue.
			ss.headOff += 8
			continue
		}

		if tombstoned {
			// The data is still on disk; skip the whole tombstoned region.
			ss.headOff += 8 + int64(length)
			continue
		}

		ss.headOff += 8

		// Read entry data, potentially spanning segments
		data, readErr := ss.readEntryDataFIFO(length)
		if readErr != nil {
			return nil, readErr
		}

		ss.entryCount--

		if saveErr := ss.saveState(); saveErr != nil {
			return nil, saveErr
		}

		return data, nil
	}
}

// readEntryDataFIFO reads entry data of the given length from the FIFO head,
// advancing through segments as needed.
func (ss *SegmentStore) readEntryDataFIFO(length uint64) ([]byte, error) {
	data := make([]byte, 0, length)
	remaining := length

	for remaining > 0 {
		if ss.headSeg == nil {
			return nil, fmt.Errorf("unexpected end of segments while reading entry")
		}

		info, err := ss.headSeg.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat segment: %w", err)
		}

		available := info.Size() - ss.headOff
		if available <= 0 {
			if advErr := ss.advanceHeadSegment(); advErr != nil {
				return nil, advErr
			}
			continue
		}

		toRead := remaining
		if toRead > uint64(available) {
			toRead = uint64(available)
		}

		chunk := make([]byte, toRead)
		n, readErr := ss.headSeg.ReadAt(chunk, ss.headOff)
		if readErr != nil && readErr != io.EOF {
			return nil, fmt.Errorf("reading entry data: %w", readErr)
		}
		data = append(data, chunk[:n]...)
		ss.headOff += int64(n)
		remaining -= uint64(n)

		if remaining > 0 {
			if advErr := ss.advanceHeadSegment(); advErr != nil {
				return nil, advErr
			}
		}
	}

	return data, nil
}

// advanceHeadSegment advances the FIFO head to the next segment.
//
// Deleting the consumed segment here is FIFO's half of the same hazard
// removeCurrentSegment guards for LIFO: a dump cursor's captured range can
// still include this segment even after popFIFO has advanced past it in
// the live store, so the delete is deferred while a dump pin is held (see
// Pin/Unpin) exactly the way removeCurrentSegment defers its own.
func (ss *SegmentStore) advanceHeadSegment() error {
	if ss.headSeg != nil {
		_ = ss.headSeg.Close()
		ss.headSeg = nil
	}

	oldPath := segmentPath(ss.segmentDir, ss.headIdx, ss.storeType)
	if ss.pinned() {
		slog.Info("segment removal deferred: compaction pin held", "segment_dir", ss.segmentDir, "store_type", ss.storeType, "segment_index", ss.headIdx)
	} else {
		_ = os.Remove(oldPath)
	}

	ss.headIdx++
	ss.headOff = 0

	if ss.headIdx > ss.tailIdx {
		return nil
	}

	path := segmentPath(ss.segmentDir, ss.headIdx, ss.storeType)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("opening head segment %s: %w", path, err)
	}
	ss.headSeg = f
	return nil
}

// popLIFO reads the last entry from the tail and truncates the segment.
// Tombstoned entries (zero length prefix) are silently skipped.
func (ss *SegmentStore) popLIFO() ([]byte, error) {
	if ss.entryCount == 0 || ss.currentSeg == nil {
		return nil, io.EOF
	}
	// A pop can move the tail backward across segments (moveBackwardSegment)
	// even when the tail segment itself holds nothing poppable, so mark dirty
	// here rather than only on a successful return.
	ss.dirty = true

	// Fast path: the cache — set by Push, left behind by the scan
	// below, or loaded from a persisted reopen — already names the newest
	// live entry. Trust it after the same validation cachedLastEntry uses,
	// rather than rescanning from offset 0 to find the same position again.
	// The entry layout has no backward pointer to what came before it (see
	// peekLIFO's comment on why a backward read is impossible), so this path
	// cannot repopulate the cache for the *next* pop on its own — it is
	// invalidated below and the next pop scans once unless a push
	// repopulates it first.
	if start, length, ok := ss.validatedCachePosition(); ok {
		data := make([]byte, length)
		if _, err := ss.currentSeg.ReadAt(data, start+8); err != nil {
			return nil, fmt.Errorf("reading entry data: %w", err)
		}
		return ss.finishPopLIFO(start, data, 0, 0, false)
	}

	// Cache miss: scan the tail segment once, with the 8-byte header buffer
	// hoisted out of the loop so the scan allocates once rather than once
	// per entry (the other half of the tail-position cache). While scanning, remember not
	// only the last live entry (the one about to be popped) but the live
	// entry immediately before it: that is exactly the position the newest
	// live entry moves to once this pop removes the last one, known "for
	// free" from a scan this pop already has to pay for. Caching it there
	// keeps every following pop on the fast path above instead of scanning
	// again, until a segment boundary is crossed (moveBackwardSegment, which
	// clears the cache — see its own comment on why a stale position across
	// segments is unsafe to trust).
	header := make([]byte, 8)
	offset := int64(0)
	var lastStart, prevStart int64
	var lastLen, prevLen uint64
	found, havePrev := false, false

	for offset+8 <= ss.currentOff {
		n, err := ss.currentSeg.ReadAt(header, offset)
		if err != nil || n < 8 {
			break
		}
		length, tombstoned := entryLength(binary.BigEndian.Uint64(header))
		if tombstoned {
			// The data is still on disk; skip the whole tombstoned region.
			offset += 8 + int64(length)
			continue
		}
		// Sanity check: reject corrupted length prefixes (same bound as writes).
		if length > ss.maxEntryLen() {
			offset += 8
			continue
		}
		if offset+8+int64(length) > ss.currentOff {
			break
		}
		if found {
			prevStart, prevLen, havePrev = lastStart, lastLen, true
		}
		lastStart, lastLen = offset, length
		offset += 8 + int64(length)
		found = true
	}

	if !found {
		// The tail segment holds nothing poppable: drop it and continue in the
		// previous one, so a LIFO store drains across segment boundaries.
		if ss.entryCount > 0 && ss.tailIdx > ss.headIdx {
			if err := ss.moveBackwardSegment(); err != nil {
				return nil, err
			}
			if ss.currentSeg != nil {
				return ss.popLIFO()
			}
		}
		return nil, io.EOF
	}

	data := make([]byte, lastLen)
	if _, err := ss.currentSeg.ReadAt(data, lastStart+8); err != nil {
		return nil, fmt.Errorf("reading entry data: %w", err)
	}

	return ss.finishPopLIFO(lastStart, data, prevStart, prevLen, havePrev)
}

// finishPopLIFO truncates the tail segment down to start — deleting the
// entry that begins there and everything after it — and leaves the cache
// pointing at (nextStart, nextLen) if nextValid, or invalidated otherwise.
// Shared tail for popLIFO's fast (cache-hit) and slow (scan) paths.
func (ss *SegmentStore) finishPopLIFO(start int64, data []byte, nextStart int64, nextLen uint64, nextValid bool) ([]byte, error) {
	if ss.pinned() {
		// A live dump captured (headIdx, headOff, tailIdx, tailOff) before
		// this pop ran and reads by file offset via readEntryAt, not
		// through ss.tailOff/ss.currentOff — so truncating now would
		// destroy bytes that captured range still promises. Skip the
		// physical truncate; every *live* read (Peek, Pop, a later
		// IterateDocuments) already stops seeing this entry the moment the
		// pointers below move, which is what removeCurrentSegment and
		// advanceHeadSegment rely on too when they defer their own delete.
		slog.Info("segment truncate deferred: compaction pin held", "segment_dir", ss.segmentDir, "store_type", ss.storeType, "segment_index", ss.tailIdx)
	} else if err := ss.currentSeg.Truncate(start); err != nil {
		return nil, fmt.Errorf("truncating segment: %w", err)
	}
	ss.tailOff = start
	ss.currentOff = start
	ss.entryCount--
	ss.lastEntryStart, ss.lastEntryLen, ss.lastEntryValid = nextStart, nextLen, nextValid

	if err := ss.saveState(); err != nil {
		return nil, err
	}

	return data, nil
}

// moveBackwardSegment moves the LIFO tail to the previous segment.
func (ss *SegmentStore) moveBackwardSegment() error {
	if err := ss.removeCurrentSegment(); err != nil {
		return err
	}

	// The cache names a position in the segment being moved away from; the
	// struct comment on lastEntryValid used to claim this call cleared it,
	// but it never did (queue-layout-spike-plan-rev5-review.md R5-04). That
	// was harmless while only peekLIFO ever consulted the cache, because it
	// re-validates the header at the cached offset before trusting it — but
	// popLIFO now consults it too, and a stale offset read
	// against a *different* segment's bytes is a data-dependent guard, not
	// an invariant: it is astronomically unlikely to pass validation by
	// coincidence, but "astronomically unlikely" is not a bound this store
	// accepts for returning the wrong entry from a pop. Clear it here,
	// unconditionally, so every future pop scans fresh in the new segment.
	ss.lastEntryValid = false

	ss.tailIdx--
	if ss.tailIdx < ss.headIdx {
		ss.tailOff = 0
		ss.currentOff = 0
		return nil
	}

	// This index's file is guaranteed to exist: removeCurrentSegment only
	// ever acted on the segment being moved away from above, never this
	// one, and finalizeDeferredDivergence (the only other thing that
	// reclaims a divergent segment) truncates rather than removes it
	// precisely so this assumption keeps holding once a dump pin releases.
	path := segmentPath(ss.segmentDir, ss.tailIdx, ss.storeType)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("opening previous segment %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat previous segment %s: %w", path, err)
	}

	ss.currentSeg = f
	logicalSize := info.Size()
	if recorded, ok := ss.divergentTailLogicalSize[ss.tailIdx]; ok {
		// This segment was rotated away from while pinned and diverging
		// (see rotateAwayFromDivergentTail): its on-disk size still
		// includes a phantom tail past its true logical end, so trust the
		// size recorded at rotation time instead of info.Size() — using
		// the on-disk size here would resurrect already-popped entries as
		// live again (a double-pop).
		logicalSize = recorded
		delete(ss.divergentTailLogicalSize, ss.tailIdx)
	}
	ss.tailOff = logicalSize
	// currentOff is the tail cursor: popLIFO and peekLIFO scan up to it, and
	// writeBytes (WriteAt) appends from it.
	ss.currentOff = logicalSize
	return nil
}

// removeCurrentSegment closes and removes the current tail segment.
func (ss *SegmentStore) removeCurrentSegment() error {
	if ss.currentSeg != nil {
		if err := ss.currentSeg.Close(); err != nil {
			return fmt.Errorf("closing segment: %w", err)
		}
		ss.currentSeg = nil
	}

	path := segmentPath(ss.segmentDir, ss.tailIdx, ss.storeType)
	if ss.pinned() {
		// A live dump's captured range may still include bytes in this
		// segment (readEntryAt opens it fresh by path on every call, not
		// through a held file descriptor). Leave the file in place, and
		// record it as entirely logical-size-0: this segment reached here
		// by draining fully via pops (every live entry it had was already
		// popped, or moveBackwardSegment would not have been called), so
		// every byte in it is phantom from the live store's perspective —
		// the same rule rotateAwayFromDivergentTail applies to a partial
		// tail, generalized to the whole segment. Without this, a later
		// compact/countTombstonesLocked/DeleteAtPosition/DeleteAtID/Peek/
		// PeekN that ever reaches this index again (the tail can rotate
		// forward past it — see openNewSegment's index-reuse guard) would
		// resurrect or miscount it. The pinned dump's own captured-range
		// walk is unaffected: it reads via readEntryAt directly, which
		// does not consult this map.
		ss.divergentTailLogicalSize[ss.tailIdx] = 0
		slog.Info("segment removal deferred: compaction pin held", "segment_dir", ss.segmentDir, "store_type", ss.storeType, "segment_index", ss.tailIdx)
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing segment %s: %w", path, err)
	}
	return nil
}

// Peek returns the next entry without removing it.
func (ss *SegmentStore) Peek() ([]byte, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if ss.storeType == "lifo" {
		return ss.peekLIFO()
	}
	return ss.peekFIFO()
}

// peekFIFO reads the next entry from head without advancing.
// Tombstoned entries (zero length prefix) are silently skipped.
func (ss *SegmentStore) peekFIFO() ([]byte, error) {
	if ss.entryCount == 0 || ss.headSeg == nil {
		return nil, io.EOF
	}

	// Walk forward from the head, skipping tombstones. A segmentWalker holds
	// one segment file open across the whole walk instead of reopening it
	// per entry stepped over — safe here because peekFIFO holds
	// ss.mu (via Peek's RLock) for the walk's entire duration, so no
	// concurrent pop or compaction can invalidate the handle mid-walk.
	w := ss.newSegmentWalker()
	defer w.close()

	segIdx, off := ss.headIdx, ss.headOff
	for segIdx <= ss.tailIdx {
		data, tombstone, nextSeg, nextOff, err := w.readEntryAt(segIdx, off)
		if errors.Is(err, io.EOF) {
			segIdx, off = nextSeg, nextOff
			continue
		}
		if err != nil {
			return nil, err
		}
		if !tombstone {
			return data, nil
		}
		segIdx, off = nextSeg, nextOff
	}

	return nil, io.EOF
}

// peekLIFO returns the most recently pushed entry without consuming it.
// Tombstoned entries are skipped.
//
// It scans forward, because entries are written as [8-byte length][data]: the
// last entry's length prefix is therefore not adjacent to the end of the
// segment and cannot be found by reading backwards from tailOff. The previous
// implementation read a header at tailOff-8, which is only correct for a
// [data][length] layout that no writer produces — so a LIFO bucket reported
// itself empty however many entries it held, while popLIFO (which scans
// forward) worked. For the same reason peek never deletes a segment the way
// popLIFO's moveBackwardSegment does: peeking must not change the store.
func (ss *SegmentStore) peekLIFO() ([]byte, error) {
	if ss.entryCount == 0 || ss.currentSeg == nil {
		return nil, io.EOF
	}

	// The common case: the newest entry is the last one written, and its
	// position is cached, so this is a single read.
	if data, ok := ss.cachedLastEntry(); ok {
		return data, nil
	}

	// Cache miss — first peek after a reopen, or the newest entry was popped or
	// tombstoned. Scan the tail segment once and remember where the newest live
	// entry starts, so the next peek is a read again.
	if data, start, length, ok := scanLastEntry(ss.currentSeg, ss.currentOff, ss.maxEntryLen()); ok {
		ss.lastEntryStart, ss.lastEntryLen, ss.lastEntryValid = start, length, true
		return data, nil
	}

	// Nothing live in the tail segment: look back through the earlier ones,
	// read-only, without deleting anything.
	for idx := ss.tailIdx - 1; idx >= ss.headIdx; idx-- {
		f, err := os.Open(segmentPath(ss.segmentDir, idx, ss.storeType))
		if err != nil {
			return nil, io.EOF
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, io.EOF
		}
		// segmentLogicalSize, not info.Size() directly: idx may have been
		// rotated away from while a dump pin held it diverging (see
		// rotateAwayFromDivergentTail), in which case its on-disk size
		// still includes a phantom tail of already-popped bytes a live
		// Peek must never hand back.
		data, _, _, ok := scanLastEntry(f, ss.segmentLogicalSize(idx, info.Size()), ss.maxEntryLen())
		_ = f.Close()
		if ok {
			return data, nil
		}
	}

	return nil, io.EOF
}

// cachedLastEntry reads the entry at the cached position, verifying that the
// position still describes a live entry of the expected length. It reports
// false when the cache cannot be trusted, which sends the caller to a scan.
func (ss *SegmentStore) cachedLastEntry() ([]byte, bool) {
	start, length, ok := ss.validatedCachePosition()
	if !ok {
		return nil, false
	}
	data := make([]byte, length)
	if _, err := ss.currentSeg.ReadAt(data, start+8); err != nil {
		return nil, false
	}
	return data, true
}

// validatedCachePosition reports the cached last-entry position after
// confirming the header still there describes a live entry of the expected
// length — the check cachedLastEntry uses, split out so popLIFO's fast path
// can reuse it without reading the entry's data twice.
func (ss *SegmentStore) validatedCachePosition() (start int64, length uint64, ok bool) {
	if !ss.lastEntryValid || ss.lastEntryStart < 0 || ss.lastEntryStart+8 > ss.currentOff {
		return 0, 0, false
	}

	header := make([]byte, 8)
	if n, err := ss.currentSeg.ReadAt(header, ss.lastEntryStart); err != nil || n < 8 {
		return 0, 0, false
	}
	length, tombstoned := entryLength(binary.BigEndian.Uint64(header))
	if tombstoned || length != ss.lastEntryLen || ss.lastEntryStart+8+int64(length) > ss.currentOff {
		return 0, 0, false
	}
	return ss.lastEntryStart, length, true
}

// scanLastEntry returns the last live entry in [0, limit) of an open segment,
// along with its position and length, reporting false when the range holds no
// live entry. It is the read-only counterpart of the forward scan in popLIFO:
// same entry layout, same corruption guards, no mutation.
func scanLastEntry(f *os.File, limit int64, maxEntryLen uint64) ([]byte, int64, uint64, bool) {
	var (
		headerAt int64
		dataLen  uint64
		found    bool
	)
	for offset := int64(0); offset+8 <= limit; {
		header := make([]byte, 8)
		n, err := f.ReadAt(header, offset)
		if err != nil || n < 8 {
			break
		}
		length, tombstoned := entryLength(binary.BigEndian.Uint64(header))
		if tombstoned {
			offset += 8 + int64(length)
			continue
		}
		if length > maxEntryLen || offset+8+int64(length) > limit {
			break
		}
		headerAt, dataLen, found = offset, length, true
		offset += 8 + int64(length)
	}
	if !found {
		return nil, 0, 0, false
	}

	data := make([]byte, dataLen)
	if _, err := f.ReadAt(data, headerAt+8); err != nil {
		return nil, 0, 0, false
	}
	return data, headerAt, dataLen, true
}

// PeekN returns the next N entries without removing them.
func (ss *SegmentStore) PeekN(n int) ([][]byte, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if ss.storeType == "lifo" {
		return ss.peekNLIFO(n)
	}
	return ss.peekNFIFO(n)
}

// peekNFIFO peeks at the next N entries from head, skipping tombstones.
func (ss *SegmentStore) peekNFIFO(n int) ([][]byte, error) {
	var results [][]byte
	scanSegIdx := ss.headIdx
	scanOff := ss.headOff

	for len(results) < n && scanSegIdx <= ss.tailIdx {
		path := segmentPath(ss.segmentDir, scanSegIdx, ss.storeType)
		f, err := os.Open(path)
		if err != nil {
			return results, nil
		}

		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return results, nil
		}
		segSize := info.Size()

		for scanOff+8 <= segSize {
			header := make([]byte, 8)
			_, readErr := f.ReadAt(header, scanOff)
			if readErr != nil {
				_ = f.Close()
				return results, nil
			}

			length := binary.BigEndian.Uint64(header)

			if length == 0 {
				// Tombstone — skip
				scanOff += 8
				continue
			}

			// Read data (may span segments)
			data := make([]byte, length)
			dataRead := 0
			readSeg := scanSegIdx
			readOff := scanOff + 8

			for dataRead < int(length) {
				readPath := segmentPath(ss.segmentDir, readSeg, ss.storeType)
				rf, rErr := os.Open(readPath)
				if rErr != nil {
					_ = f.Close()
					return results, nil
				}
				rInfo, rErr := rf.Stat()
				if rErr != nil {
					_ = rf.Close()
					_ = f.Close()
					return results, nil
				}
				available := rInfo.Size() - readOff
				if available <= 0 {
					_ = rf.Close()
					readSeg++
					readOff = 0
					continue
				}
				toRead := int(length) - dataRead
				if toRead > int(available) {
					toRead = int(available)
				}
				nn, rErr := rf.ReadAt(data[dataRead:dataRead+toRead], readOff)
				_ = rf.Close()
				if rErr != nil && !errors.Is(rErr, io.EOF) {
					_ = f.Close()
					return results, nil
				}
				dataRead += nn
				readOff += int64(nn)
				if readOff >= rInfo.Size() {
					readSeg++
					readOff = 0
				}
			}

			results = append(results, data)
			scanOff += 8 + int64(length)
			if scanOff > segSize {
				overflow := scanOff - segSize
				scanSegIdx++
				scanOff = overflow
				_ = f.Close()
				break
			}
		}

		if scanOff+8 > segSize {
			scanSegIdx++
			scanOff = 0
		}
		_ = f.Close()
	}

	return results, nil
}

// peekNLIFO peeks at the last N entries from tail, skipping tombstones.
func (ss *SegmentStore) peekNLIFO(n int) ([][]byte, error) {
	var results [][]byte
	tOff := ss.tailOff
	tIdx := ss.tailIdx

	for len(results) < n {
		if tOff < 8 {
			tIdx--
			if tIdx < ss.headIdx {
				return results, nil
			}
			path := segmentPath(ss.segmentDir, tIdx, ss.storeType)
			f, err := os.Open(path)
			if err != nil {
				return results, nil
			}
			info, err := f.Stat()
			if err != nil {
				_ = f.Close()
				return results, nil
			}
			// segmentLogicalSize, not info.Size() directly: see peekLIFO's
			// identical guard — tIdx may have been rotated away from while
			// a dump pin held it diverging, and its on-disk size still
			// includes a phantom tail of already-popped bytes.
			tOff = ss.segmentLogicalSize(tIdx, info.Size())
			_ = f.Close()
			continue
		}

		path := segmentPath(ss.segmentDir, tIdx, ss.storeType)
		f, err := os.Open(path)
		if err != nil {
			return results, nil
		}

		header := make([]byte, 8)
		_, readErr := f.ReadAt(header, tOff-8)
		if readErr != nil {
			_ = f.Close()
			tOff -= 8
			continue
		}

		length, tombstoned := entryLength(binary.BigEndian.Uint64(header))

		if tombstoned {
			// Skip the whole tombstoned region.
			_ = f.Close()
			tOff -= 8 + int64(length)
			continue
		}

		dataStart := tOff - 8 - int64(length)
		if dataStart < 0 {
			_ = f.Close()
			tOff -= 8
			continue
		}

		data := make([]byte, length)
		_, readErr = f.ReadAt(data, dataStart)
		_ = f.Close()
		if readErr != nil {
			tOff = dataStart
			continue
		}

		results = append(results, data)
		tOff = dataStart
	}

	return results, nil
}

// Length returns the total number of entries in the store.
func (ss *SegmentStore) Length() int {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return int(ss.entryCount)
}

// Close closes all open segment files and, only when something changed since
// the store was opened, syncs and saves state. A store that was
// only read (Peek, PeekN, Walk, DocCount, IterateDocuments, Length) closes
// with neither an fsync nor a state.bson rewrite, because neither has
// anything to make durable. A store a mutation touched still gets both,
// in the same order as before: segment fsync, then the state rewrite.
func (ss *SegmentStore) Close() error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if err := ss.closeCurrentSegment(); err != nil {
		return err
	}
	if ss.headSeg != nil && ss.headSeg != ss.currentSeg {
		_ = ss.headSeg.Close()
		ss.headSeg = nil
	}

	if !ss.dirty {
		return nil
	}
	if err := ss.saveState(); err != nil {
		return err
	}
	ss.dirty = false
	return nil
}

// readEntryAt reads a single entry starting at the given segment/offset
// position. Returns the entry data, whether it was a tombstone (length 0),
// and the next scan position.
func (ss *SegmentStore) readEntryAt(segIdx int, offset int64) (data []byte, isTombstone bool, nextSegIdx int, nextOffset int64, err error) {
	f, err := ss.openSegmentForRead(segIdx)
	if err != nil {
		return nil, false, 0, 0, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("stat segment: %w", err)
	}

	return ss.decodeEntryAt(f, segIdx, offset, info.Size())
}

// openSegmentForRead opens the segment file at segIdx for reading and
// records the open in ss.openCount (see that field's comment). This is the
// exact per-call open the walker avoids: every walk that steps through more
// than one entry via readEntryAt used to pay it once per entry stepped over,
// worst on the case the FIFO layout is supposed to be good at — a head
// trimmed by deletes. segmentWalker amortizes it across a whole walk by
// calling this only on a segment-boundary crossing instead of on every call.
func (ss *SegmentStore) openSegmentForRead(segIdx int) (*os.File, error) {
	// Atomic, not ss.mu-guarded: IterateDocuments/DocCount deliberately call
	// readEntryAt (and so this) without ss.mu held, so two concurrent dump
	// walks over the same store can both be incrementing this at once (found
	// by go test -race on hoardDB/server's
	// TestDumpStartPublishesSessionOnlyAfterFullyWired while verifying this
	// fix — see openCount's own field comment).
	atomic.AddInt64(&ss.openCount, 1)
	path := segmentPath(ss.segmentDir, segIdx, ss.storeType)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening segment: %w", err)
	}
	return f, nil
}

// decodeEntryAt reads and decodes the entry at offset within an already-open
// segIdx of the given size, shared by readEntryAt (which opens f itself, for
// a single call) and segmentWalker.readEntryAt (which holds f open across a
// whole walk). Splitting this out is what lets the walker reuse the decode
// logic without also reusing readEntryAt's own per-call open.
func (ss *SegmentStore) decodeEntryAt(f *os.File, segIdx int, offset int64, segSize int64) (data []byte, isTombstone bool, nextSegIdx int, nextOffset int64, err error) {
	if offset >= segSize {
		return nil, false, segIdx + 1, 0, io.EOF
	}
	if offset+8 > segSize {
		// A prefix starting inside the last 7 bytes of a segment can only come
		// from a build that split headers across the boundary (fixed in
		// writeEntry). Refuse loudly rather than reinterpreting data bytes as
		// a length prefix.
		return nil, false, 0, 0, fmt.Errorf("segment %d: entry header at offset %d crosses the segment boundary; data written before the 2026-09 format fix must be rebuilt", segIdx, offset)
	}

	header := make([]byte, 8)
	if _, err := f.ReadAt(header, offset); err != nil {
		return nil, false, 0, 0, fmt.Errorf("reading length prefix: %w", err)
	}

	length, tombstoned := entryLength(binary.BigEndian.Uint64(header))
	if length > ss.maxEntryLen() {
		return nil, false, 0, 0, fmt.Errorf("segment %d: implausible entry length %d at offset %d", segIdx, length, offset)
	}

	// Calculate next position after this entry
	totalAdvance := offset + 8 + int64(length)
	if totalAdvance <= segSize {
		nextSegIdx = segIdx
		nextOffset = totalAdvance
	} else {
		nextSegIdx = segIdx + 1
		nextOffset = totalAdvance - segSize
	}

	if tombstoned {
		return nil, true, nextSegIdx, nextOffset, nil
	}

	data, err = ss.readEntryDataSpanning(segIdx, offset+8, length)
	if err != nil {
		return nil, false, 0, 0, err
	}
	return data, false, nextSegIdx, nextOffset, nil
}

// readEntryDataSpanning reads length bytes of entry data starting at
// (segIdx, offset), opening each segment it touches fresh and closing it
// immediately after. Data spanning more than one segment only happens for
// an entry that was written straddling a rotation boundary, which is rare
// enough that amortizing this open is not worth the complexity the
// entry-to-entry walk (segmentWalker) is fixing — that defect is
// about walking many *entries*, not about one oversized entry's own data.
func (ss *SegmentStore) readEntryDataSpanning(segIdx int, offset int64, length uint64) ([]byte, error) {
	data := make([]byte, 0, length)
	dataRead := uint64(0)
	readSeg := segIdx
	readOff := offset

	for dataRead < length {
		readPath := segmentPath(ss.segmentDir, readSeg, ss.storeType)
		rf, rErr := os.Open(readPath)
		if rErr != nil {
			return nil, fmt.Errorf("opening segment for data: %w", rErr)
		}
		rInfo, rErr := rf.Stat()
		if rErr != nil {
			_ = rf.Close()
			return nil, fmt.Errorf("stat segment for data: %w", rErr)
		}
		available := rInfo.Size() - readOff
		if available <= 0 {
			_ = rf.Close()
			readSeg++
			readOff = 0
			continue
		}
		toRead := length - dataRead
		if toRead > uint64(available) {
			toRead = uint64(available)
		}
		chunk := make([]byte, toRead)
		n, rErr := rf.ReadAt(chunk, readOff)
		_ = rf.Close()
		if rErr != nil && !errors.Is(rErr, io.EOF) {
			return nil, fmt.Errorf("reading entry data: %w", rErr)
		}
		data = append(data, chunk[:n]...)
		dataRead += uint64(n)
		readOff += int64(n)
		if readOff >= rInfo.Size() {
			readSeg++
			readOff = 0
		}
	}

	return data, nil
}

// segmentWalker amortizes the per-call segment open readEntryAt otherwise
// pays on every step of a multi-entry walk: it holds one segment
// file open across consecutive readEntryAt calls and opens (then holds) a
// new one only when a call crosses into a segment index it is not already
// holding. peekFIFO, DeleteAtPosition, countTombstonesLocked and compact's
// collection loop each walk many entries under a single ss.mu acquisition
// and are the fixed call sites; a walker is created fresh at the top of each
// of those calls and closed before it returns, so nothing here is cached
// across separate lock acquisitions the way the LIFO tail-position cache
// was — there is no cross-operation invalidation hazard to get
// wrong, because the walker never survives past the single walk it was
// built for.
//
// IterateDocuments and DocCount deliberately stay off it: their walk's
// duration is controlled by an external caller (a dump) that can run
// arbitrarily long without ss.mu held the whole time (capturedRange's own
// comment), so a concurrent pop or compaction can remove or truncate a
// segment mid-walk. readEntryAt's fresh open-per-call is what keeps those
// two walks correct instead of reading through a handle to a file that may
// no longer exist or may have shrunk (queue-layout-spike-plan-rev5-review.md
// R5-08 finding 3).
type segmentWalker struct {
	ss     *SegmentStore
	segIdx int
	f      *os.File
	size   int64
}

// newSegmentWalker returns a walker with no segment open yet.
func (ss *SegmentStore) newSegmentWalker() *segmentWalker {
	return &segmentWalker{ss: ss, segIdx: -1}
}

// close releases whatever segment handle the walker is holding. Callers
// must defer this once per walk.
func (w *segmentWalker) close() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
		w.segIdx = -1
	}
}

// open returns the walker's handle and cached size for segIdx, reusing them
// when the walker is already positioned there and opening (then holding) a
// fresh handle otherwise.
func (w *segmentWalker) open(segIdx int) (*os.File, int64, error) {
	if w.f != nil && w.segIdx == segIdx {
		return w.f, w.size, nil
	}
	w.close()
	f, err := w.ss.openSegmentForRead(segIdx)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("stat segment: %w", err)
	}
	w.f, w.segIdx, w.size = f, segIdx, info.Size()
	return w.f, w.size, nil
}

// readEntryAt is readEntryAt, reusing the walker's held segment handle
// across calls instead of opening, stat-ing and closing the segment file on
// every one.
func (w *segmentWalker) readEntryAt(segIdx int, offset int64) (data []byte, isTombstone bool, nextSegIdx int, nextOffset int64, err error) {
	f, segSize, err := w.open(segIdx)
	if err != nil {
		return nil, false, 0, 0, err
	}
	return w.ss.decodeEntryAt(f, segIdx, offset, segSize)
}

// readEntryAtLive is segmentWalker.readEntryAt with the same
// divergentTailLogicalSize clamp readEntryAtLive applies — see that
// function's comment for why the live-store walkers that trust a historical
// segment's logical size need it.
func (w *segmentWalker) readEntryAtLive(segIdx int, offset int64) (data []byte, isTombstone bool, nextSegIdx int, nextOffset int64, err error) {
	if logicalSize, ok := w.ss.divergentTailLogicalSize[segIdx]; ok && offset >= logicalSize {
		return nil, false, segIdx + 1, 0, io.EOF
	}
	return w.readEntryAt(segIdx, offset)
}

// segmentLogicalSize returns the size a *live-store* reader should treat
// segIdx as having. Ordinarily that is just onDiskSize (the segment's
// actual file size), but a segment that was rotated away from while a dump
// pin held it diverging (see rotateAwayFromDivergentTail) has a physical
// tail of already-popped bytes past its true logical end — bytes a pinned
// dump's own captured-range walk still needs to read verbatim, but that
// every *other* reader of this store must treat as if they were never
// written, or it would resurrect (compact), miscount (DeleteAtPosition), or
// hand back (Peek/PeekN) an entry the live store already popped. Callers
// must hold ss.mu (read or write) — divergentTailLogicalSize is only ever
// mutated with it held for writing.
func (ss *SegmentStore) segmentLogicalSize(segIdx int, onDiskSize int64) int64 {
	if logicalSize, ok := ss.divergentTailLogicalSize[segIdx]; ok {
		return logicalSize
	}
	return onDiskSize
}

// readEntryAtLive is readEntryAt with segmentLogicalSize applied: once
// offset reaches or passes segIdx's logical end, it reports io.EOF (the
// same signal readEntryAt gives at a segment's real end) instead of
// decoding a phantom entry as live. Every *live-store* walk that trusts a
// historical segment's size — compact, countTombstonesLocked,
// DeleteAtPosition, DeleteAtID — goes through this rather than readEntryAt
// directly, for every segment strictly before the walk's own (tailIdx,
// tailOff) boundary: those four callers bound their own loop at that exact
// position (a segPos, the same type IterateDocuments/DocCount use for their
// captured range), so the *current* tail segment's own trailing bytes past
// ss.tailOff — live or pinned-and-deferred — are never read by them at all,
// pinned or not, with no divergentTailLogicalSize entry required. This
// function's clamp is what protects the segments *behind* that boundary:
// ones a past rotateAwayFromDivergentTail or removeCurrentSegment call
// walked away from, whose own on-disk size still includes bytes no longer
// live. A pinned dump's own captured-range cursor (IterateDocuments/
// DocCount) deliberately calls readEntryAt directly instead: it must read
// straight through that same phantom region, since that is exactly the
// entry it captured before the pop that created it. Callers must hold
// ss.mu for writing, same as readEntryAt itself requires from them.
func (ss *SegmentStore) readEntryAtLive(segIdx int, offset int64) (data []byte, isTombstone bool, nextSegIdx int, nextOffset int64, err error) {
	if logicalSize, ok := ss.divergentTailLogicalSize[segIdx]; ok && offset >= logicalSize {
		return nil, false, segIdx + 1, 0, io.EOF
	}
	return ss.readEntryAt(segIdx, offset)
}

// DeleteAtPosition removes the entry at the given 0-indexed position by
// tombstoning it (see tombstoneAt). The position counts only live entries
// starting from the head, and returns an error if it is out of range.
// tombstoneAt marks the entry that starts at the given position as deleted by
// setting the tombstone flag in its length prefix, keeping the length so later
// walks can skip the entry's bytes. Callers must hold ss.mu for writing.
func (ss *SegmentStore) tombstoneAt(segIdx int, off int64) error {
	ss.dirty = true

	// Reconcile the hint with disk once per open, before this tombstone is
	// written: a walk taken afterwards would count the new tombstone and the
	// increment below would then count it twice. countTombstonesLocked, not
	// countTombstones — the caller already holds ss.mu for writing, and
	// sync.RWMutex is not reentrant.
	if !ss.tombstoneCountSeeded {
		ss.tombstoneCount = int64(ss.countTombstonesLocked())
		ss.tombstoneCountSeeded = true
	}

	path := segmentPath(ss.segmentDir, segIdx, ss.storeType)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("opening segment %s for tombstone: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	header := make([]byte, 8)
	if _, err := f.ReadAt(header, off); err != nil {
		return fmt.Errorf("reading length prefix for tombstone: %w", err)
	}
	raw := binary.BigEndian.Uint64(header)
	if raw&tombstoneFlag != 0 {
		return fmt.Errorf("entry at segment %d offset %d is already tombstoned", segIdx, off)
	}
	binary.BigEndian.PutUint64(header, raw|tombstoneFlag)
	if _, err := f.WriteAt(header, off); err != nil {
		return fmt.Errorf("writing tombstone: %w", err)
	}
	if err := ss.syncSegment(f); err != nil {
		return fmt.Errorf("fsyncing tombstone: %w", err)
	}

	ss.entryCount--
	ss.tombstoneCount++
	if err := ss.saveState(); err != nil {
		return err
	}
	return ss.maybeCompact()
}

// Walk calls visit for each live entry in the store's natural scan order:
// oldest first for a fifo store, newest first for a lifo store. Tombstoned
// entries are skipped. It is read-only — unlike DeleteAtPosition it tombstones
// nothing, so a scan cannot consume the queue it is reading. visit must not
// call back into the store, because the store lock is held for the walk.
//
// FIFO streams; LIFO collects the head-to-tail walk and reverses it, because
// entries are written [length][data] and there is no backward reader to stream
// from.
// ponytail: that buffers one slice of the store's live entries for LIFO; stream
// it only if a large-LIFO scan proves the memory matters.
func (ss *SegmentStore) Walk(visit func(entry []byte) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if ss.entryCount == 0 {
		return nil
	}

	if ss.storeType != "lifo" {
		return ss.walkForward(visit)
	}

	var entries [][]byte
	if err := ss.walkForward(func(entry []byte) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		return err
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if err := visit(entries[i]); err != nil {
			return err
		}
	}
	return nil
}

// walkForward calls visit for each live entry from head to tail, skipping
// tombstones. The caller must hold ss.mu.
func (ss *SegmentStore) walkForward(visit func(entry []byte) error) error {
	segIdx, off := ss.headIdx, ss.headOff
	for segIdx <= ss.tailIdx {
		data, tombstone, nextSeg, nextOff, err := ss.readEntryAt(segIdx, off)
		if errors.Is(err, io.EOF) {
			segIdx, off = nextSeg, nextOff
			continue
		}
		if err != nil {
			return err
		}
		if !tombstone {
			if err := visit(data); err != nil {
				return err
			}
		}
		segIdx, off = nextSeg, nextOff
	}
	return nil
}

func (ss *SegmentStore) DeleteAtPosition(pos int) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if pos < 0 || pos >= int(ss.entryCount) {
		return fmt.Errorf("position %d out of range [0, %d)", pos, ss.entryCount)
	}

	// One segment handle held across the whole positional walk instead of
	// reopened per entry stepped over: safe here because
	// DeleteAtPosition holds ss.mu for writing across the entire walk, so
	// nothing else can touch these segment files concurrently.
	w := ss.newSegmentWalker()
	defer w.close()

	walkPos := segPos{ss.headIdx, ss.headOff}
	tail := segPos{ss.tailIdx, ss.tailOff}
	count := 0
	for walkPos.before(tail) {
		_, tombstone, nextSeg, nextOff, err := w.readEntryAtLive(walkPos.seg, walkPos.off)
		if errors.Is(err, io.EOF) {
			walkPos = segPos{nextSeg, nextOff}
			continue
		}
		if err != nil {
			return err
		}
		if !tombstone {
			if count == pos {
				return ss.tombstoneAt(walkPos.seg, walkPos.off)
			}
			count++
		}
		walkPos = segPos{nextSeg, nextOff}
	}

	return fmt.Errorf("position %d not found during scan", pos)
}

// DeleteAtID removes the entry whose _id_ field matches entryID by
// tombstoning it (zeroing the 8-byte length prefix). Scans all entries
// from head to tail, unmarshalling each to check the _id_ field.
// Returns an error if no matching entry is found.
func (ss *SegmentStore) DeleteAtID(entryID string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	walkPos := segPos{ss.headIdx, ss.headOff}
	tail := segPos{ss.tailIdx, ss.tailOff}
	for walkPos.before(tail) {
		data, tombstone, nextSeg, nextOff, err := ss.readEntryAtLive(walkPos.seg, walkPos.off)
		if errors.Is(err, io.EOF) {
			walkPos = segPos{nextSeg, nextOff}
			continue
		}
		if err != nil {
			return err
		}
		if !tombstone {
			var doc map[string]interface{}
			if uErr := bson.Unmarshal(data, &doc); uErr == nil {
				if id, ok := doc["_id_"]; ok {
					if idStr, ok := id.(string); ok && idStr == entryID {
						return ss.tombstoneAt(walkPos.seg, walkPos.off)
					}
				}
			}
		}
		walkPos = segPos{nextSeg, nextOff}
	}

	return fmt.Errorf("entry with ID %q not found", entryID)
}

// countTombstonesLocked counts the tombstoned entries across all segments
// from head to tail. Callers must already hold ss.mu (read or write).
func (ss *SegmentStore) countTombstonesLocked() int {
	count := 0

	// One segment handle held across the whole walk instead of reopened per
	// entry stepped over: safe here because every caller of this
	// method already holds ss.mu, read or write, for the walk's duration.
	w := ss.newSegmentWalker()
	defer w.close()

	walkPos := segPos{ss.headIdx, ss.headOff}
	tail := segPos{ss.tailIdx, ss.tailOff}
	for walkPos.before(tail) {
		_, tombstone, nextSeg, nextOff, err := w.readEntryAtLive(walkPos.seg, walkPos.off)
		if errors.Is(err, io.EOF) {
			walkPos = segPos{nextSeg, nextOff}
			continue
		}
		if err != nil {
			// A damaged segment cannot be walked any further; report what was
			// counted rather than spinning on the same offset.
			return count
		}
		if tombstone {
			count++
		}
		walkPos = segPos{nextSeg, nextOff}
	}

	return count
}

// maybeCompact rewrites the store when the tombstone ratio exceeds the
// compaction policy threshold. The counter is a hint, so the exact tombstone
// count is confirmed before paying for a rewrite. Callers must hold ss.mu for
// writing, because compaction rewrites every segment.
func (ss *SegmentStore) maybeCompact() error {
	if ss.pinned() {
		// A live dump has captured a read range and is walking it by file
		// offset; compaction would rewrite every segment out from under
		// that walk. Deferred, not lost: the next write after Unpin
		// re-evaluates the same tombstone ratio.
		return nil
	}

	tombstones := ss.tombstoneCount
	total := ss.entryCount + tombstones
	if tombstones == 0 || tombstones*compactionTombstoneDen <= total {
		return nil
	}

	exact := int64(ss.countTombstonesLocked())
	if exact == 0 || exact*compactionTombstoneDen <= ss.entryCount+exact {
		return nil
	}

	if err := ss.compact(); err != nil {
		return fmt.Errorf("compacting segments: %w", err)
	}
	// A rewrite drops every tombstone, so zero is exact rather than a hint.
	// The flag is already true on the only path that reaches here — tombstoneAt
	// seeds before it writes — so this keeps the two assignments together
	// rather than relying on that: whoever sets the counter states whether it
	// is reconciled.
	ss.tombstoneCount = 0
	ss.tombstoneCountSeeded = true

	return nil
}

// collectLiveEntries reads every non-tombstoned entry from head to tail,
// used by compact to gather what a rewrite must preserve. Split out from
// compact so the walker it uses is scoped to exactly this
// read-only pass and closed before compact goes on to close and remove the
// segments those entries were just read from. Callers must hold ss.mu for
// writing, the same requirement compact itself has.
func (ss *SegmentStore) collectLiveEntries() ([][]byte, error) {
	w := ss.newSegmentWalker()
	defer w.close()

	var entries [][]byte
	walkPos := segPos{ss.headIdx, ss.headOff}
	tail := segPos{ss.tailIdx, ss.tailOff}
	for walkPos.before(tail) {
		data, tombstone, nextSeg, nextOff, err := w.readEntryAtLive(walkPos.seg, walkPos.off)
		if errors.Is(err, io.EOF) {
			walkPos = segPos{nextSeg, nextOff}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading entry during compaction: %w", err)
		}
		if !tombstone {
			entries = append(entries, data)
		}
		walkPos = segPos{nextSeg, nextOff}
	}
	return entries, nil
}

// compact rewrites all segments removing tombstoned entries. Reads every
// valid entry from head to tail, writes them to fresh segments, then
// deletes the old segments and updates state. Called by maybeCompact once
// the tombstone ratio exceeds 25%. Callers must hold ss.mu for writing.
func (ss *SegmentStore) compact() error {
	if ss.pinned() {
		// maybeCompact already refuses to call compact while pinned; this
		// is the same guard repeated at the point that actually rewrites
		// segments, so a future direct call to compact cannot bypass it.
		return fmt.Errorf("compacting segments: refused while a dump pin is held for %s", ss.segmentDir)
	}

	// Collect all valid entries. One segment handle held across the whole
	// collection walk instead of reopened per entry stepped over:
	// safe here because compact holds ss.mu for writing across its entire
	// body, and closing the walker below happens before this function goes
	// on to close and remove the very segments it just read.
	entries, err := ss.collectLiveEntries()
	if err != nil {
		return err
	}

	// Close and remove old segments
	if ss.headSeg != nil {
		_ = ss.headSeg.Close()
		ss.headSeg = nil
	}
	if ss.currentSeg != nil {
		_ = ss.currentSeg.Close()
		ss.currentSeg = nil
	}

	oldHead := ss.headIdx
	oldTail := ss.tailIdx
	for i := oldHead; i <= oldTail; i++ {
		oldPath := segmentPath(ss.segmentDir, i, ss.storeType)
		_ = os.Remove(oldPath)
	}

	// Reset state
	ss.headIdx = 0
	ss.headOff = 0
	ss.tailIdx = 0
	ss.tailOff = 0
	ss.segCount = 0
	ss.entryCount = 0
	// The cache names a position in a segment compact is about to delete;
	// every offset is meaningless once the rewrite starts. Same hazard, same
	// fix, as moveBackwardSegment's clear above (the tail-position cache): the next
	// pop scans fresh in the rewritten segment rather than trusting a stale
	// offset that now falls inside different bytes.
	ss.lastEntryStart = 0
	ss.lastEntryLen = 0
	ss.lastEntryValid = false
	// Every old index just had its file removed above, unconditionally,
	// regardless of any divergentTailLogicalSize entry for it — the
	// collection loop already read this store correctly, using whatever
	// entries were current at the time (readEntryAtLive), so anything
	// still recorded now describes segment indices that are about to be
	// reused by the fresh numbering below and would otherwise collide
	// (task-1 review round 2, Critical finding). Unpin normally clears
	// these already once the last pin releases, but Pin is lock-free and
	// compact always runs under ss.mu, so a pin that reached zero and a
	// compact that observed !pinned() can race for ss.mu — this is the
	// second, unconditional half of that fix, not redundant with it.
	ss.divergentTailLogicalSize = make(map[int]int64)

	// Create fresh segment and write valid entries. writeEntryNoSync plus one
	// Sync after the loop is the same amortized-durability pattern PushBatch
	// already uses in this file — one fsync for the whole rewrite instead of
	// one per surviving entry. The rewrite is still synced before
	// saveState below records it, and — as it already did before this fix —
	// before compact returns and the caller (maybeCompact, under the same
	// write-lock acquisition as the delete that triggered it) can act on the
	// result; only the per-entry fsyncs during the loop are removed.
	if err := ss.openNewSegment(); err != nil {
		return fmt.Errorf("creating new segment for compaction: %w", err)
	}

	for _, entry := range entries {
		if err := ss.writeEntryNoSync(entry); err != nil {
			return fmt.Errorf("writing entry during compaction: %w", err)
		}
	}
	if err := ss.syncSegment(ss.currentSeg); err != nil {
		return fmt.Errorf("fsyncing rewritten segment: %w", err)
	}

	ss.tailOff = ss.currentOff

	return ss.saveState()
}

// segPos identifies a read position in the segment log as a segment index
// and a byte offset inside it — the unit both the head/tail cursor and a
// dump walk advance in.
type segPos struct {
	seg int
	off int64
}

// before reports whether p comes strictly earlier than other in segment
// log order.
func (p segPos) before(other segPos) bool {
	if p.seg != other.seg {
		return p.seg < other.seg
	}
	return p.off < other.off
}

// capturedRange snapshots the store's current head and tail positions under
// a read lock. It is the "capture under RLock" step a dump cursor and
// DocCount both start with (dump-restore-spec.md §7.2): everything the walk
// below then does reads by file offset via readEntryAt, which does not
// touch ss.mu, so the lock is held only for this snapshot, not for the
// walk's whole duration.
func (ss *SegmentStore) capturedRange() (head, tail segPos) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return segPos{ss.headIdx, ss.headOff}, segPos{ss.tailIdx, ss.tailOff}
}

// DocCount reports how many live (non-tombstoned) entries lie between this
// store's head and the tail captured at the start of the call, read by
// walking storage directly — never from ss.entryCount, the counter this
// store persists and restores as-is on recover() without ever re-deriving
// it from the segment files themselves, which is exactly the class of bug
// HashStore.Recount and BTreeStore.Recount exist to correct for those two
// engines (see store/enumerate.go's Enumerator doc comment).
//
// The count is taken under this store's own compaction pin so a concurrent
// compaction cannot rewrite segments out from under the scan; the pin is
// held only for the scan's own bounded duration, not exposed to the caller,
// because DocCount (unlike IterateDocuments) controls its own duration.
func (ss *SegmentStore) DocCount() (int64, error) {
	ss.Pin()
	defer ss.Unpin()

	head, tail := ss.capturedRange()
	var n int64
	pos := head
	for pos.before(tail) {
		_, tombstone, nextSeg, nextOff, err := ss.readEntryAt(pos.seg, pos.off)
		if errors.Is(err, io.EOF) {
			pos = segPos{nextSeg, nextOff}
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("counting entries at segment %d offset %d: %w", pos.seg, pos.off, err)
		}
		if !tombstone {
			n++
		}
		pos = segPos{nextSeg, nextOff}
	}
	return n, nil
}

// IterateDocuments walks every live (non-tombstoned) entry between this
// store's head and the tail captured at the start of the call, and invokes
// fn once per entry with a nil key — segment-backed queues carry no
// document key of their own, only an opaque payload framed
// [8-byte length][data] (see the package doc comment on maxEntryLen) — and
// the entry's raw bytes, unmodified.
//
// Order depends on storeType: FIFO order is push order (oldest first),
// which is what the forward walk naturally produces. LIFO order is pop
// order (newest first, dump-restore-spec.md §7.2 rule 3) — a restore that
// replayed a LIFO dump in storage order would reverse the stack it is
// supposed to reproduce — so the LIFO walk collects live positions forward
// first and then reads and emits them in reverse.
//
// IterateDocuments does not itself hold the compaction pin: its duration is
// controlled by fn, which the caller supplies and may take arbitrarily
// long, so the caller — a future dump session — pins around the call to
// IterateDocuments (see Pin's doc comment), not this method around itself.
// Without an external pin, a concurrent compaction or a LIFO pop that
// crosses a segment boundary can remove or shrink a segment this walk has
// not yet reached, which surfaces as an error from readEntryAt rather than
// a silently incomplete dump.
func (ss *SegmentStore) IterateDocuments(fn func(key, value []byte) error) error {
	head, tail := ss.capturedRange()

	if ss.storeType == "lifo" {
		return ss.iterateLIFO(head, tail, fn)
	}
	return ss.iterateFIFO(head, tail, fn)
}

// iterateFIFO walks [head, tail) once, forward, emitting each live entry as
// it is read — push order, which is FIFO pop order.
func (ss *SegmentStore) iterateFIFO(head, tail segPos, fn func(key, value []byte) error) error {
	pos := head
	for pos.before(tail) {
		data, tombstone, nextSeg, nextOff, err := ss.readEntryAt(pos.seg, pos.off)
		if errors.Is(err, io.EOF) {
			pos = segPos{nextSeg, nextOff}
			continue
		}
		if err != nil {
			return fmt.Errorf("reading entry at segment %d offset %d: %w", pos.seg, pos.off, err)
		}
		next := segPos{nextSeg, nextOff}
		if !tombstone {
			if err := fn(nil, data); err != nil {
				return fmt.Errorf("iterating entry at segment %d offset %d: %w", pos.seg, pos.off, err)
			}
		}
		pos = next
	}
	return nil
}

// iterateLIFO walks [head, tail) forward once to collect the position of
// every live entry, then reads and emits them in reverse — newest push
// first, which is LIFO pop order (dump-restore-spec.md §7.2 rule 3).
// Positions, not data, are what the forward pass collects: holding every
// entry's bytes in memory at once would size the walk to the bucket, where
// holding only their positions sizes it to the entry count instead.
func (ss *SegmentStore) iterateLIFO(head, tail segPos, fn func(key, value []byte) error) error {
	var live []segPos
	pos := head
	for pos.before(tail) {
		_, tombstone, nextSeg, nextOff, err := ss.readEntryAt(pos.seg, pos.off)
		if errors.Is(err, io.EOF) {
			pos = segPos{nextSeg, nextOff}
			continue
		}
		if err != nil {
			return fmt.Errorf("scanning entry at segment %d offset %d: %w", pos.seg, pos.off, err)
		}
		if !tombstone {
			live = append(live, pos)
		}
		pos = segPos{nextSeg, nextOff}
	}

	for i := len(live) - 1; i >= 0; i-- {
		data, tombstone, _, _, err := ss.readEntryAt(live[i].seg, live[i].off)
		if err != nil {
			return fmt.Errorf("re-reading entry at segment %d offset %d: %w", live[i].seg, live[i].off, err)
		}
		if tombstone {
			// The first pass already recorded this position as live; a
			// concurrent DeleteAtPosition/DeleteAtID tombstoned it between
			// the two passes (LIFO's own Pop never sets the tombstone bit —
			// see popLIFO). Skip it here the same way the first pass would
			// have, rather than emit a deleted element.
			continue
		}
		if err := fn(nil, data); err != nil {
			return fmt.Errorf("iterating entry at segment %d offset %d: %w", live[i].seg, live[i].off, err)
		}
	}
	return nil
}
