package storage

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// ErrBelowHorizon is returned when the sequence a reader asks for has been
// pruned from the log. The text carries the first sequence still retained.
var ErrBelowHorizon = errors.New("the log no longer holds that sequence")

// belowHorizon builds the error for a pruned sequence, naming what is left.
func belowHorizon(wanted, firstRetained uint64) error {
	return fmt.Errorf("%w: sequence %d was asked for and the earliest retained entry is %d",
		ErrBelowHorizon, wanted, firstRetained)
}

// logGap is "the sequence wanted is not the next entry". It carries the run of
// sequences the log skipped so ReadBatch can tell a permanent hole in the
// retained range from a segment pruned under a racing read. A gap still matches
// ErrBelowHorizon — that is what a prune race means — until ReadBatch has seen
// the same gap twice without the first retained sequence moving.
type logGap struct {
	wanted uint64 // the first missing sequence
	next   uint64 // the first sequence present after the gap
}

// Error keeps the horizon wording a caller expects while the read decides
// whether the gap heals.
func (g *logGap) Error() string {
	return belowHorizon(g.wanted, g.next).Error()
}

// Is makes errors.Is(g, ErrBelowHorizon) true, so a caller that only cares
// that the sequence is gone keeps seeing the horizon until ReadBatch proves the
// gap cannot heal.
func (g *logGap) Is(target error) bool {
	return target == ErrBelowHorizon
}

// holeInLog names a run of missing sequences. It deliberately does not wrap
// ErrBelowHorizon: the sequence was never pruned by retention, so the horizon
// remedy points at the wrong cause. The advice is the node's (SetDirectoryAdvice) or a single
// node's: restore the segment, never empty the directory.
func holeInLog(dir string, wanted, next uint64) error {
	return fmt.Errorf("the log has a hole: sequences %d..%d are missing; it cannot repair a peer. %s",
		wanted, next-1, adviceForDirectory(dir, "Restore the segment from a backup, or keep this data directory"))
}

// WALCursor is a position in the log: the segment, the byte offset of the next
// entry in it, and the sequence the reader wants next. It is a value; a reader
// keeps the one ReadBatch returned and passes it back. The zero cursor is not a
// valid position: get one from OpenCursor.
type WALCursor struct {
	segmentIndex int
	offset       int64
	nextSeq      uint64
	// lastSeq is the sequence of the entry the cursor last passed, 0 when it
	// has passed none. A v2 entry after it must rise above it (spec §3.3 rule
	// 5), across segments and across ReadBatch calls: a call that started
	// from nothing would skip a lower entry as one below the sequence wanted.
	lastSeq uint64
}

// NextSeq is the log sequence the cursor reads next.
func (c WALCursor) NextSeq() uint64 { return c.nextSeq }

// SyncedSequence is the highest sequence known to be durable on disk. What is
// on disk when the log is opened counts as synced. A replay that ships entries
// must stop here and not at LastSequence: an entry that is written and not yet
// fsynced can still be lost to a power cut, and a different write then takes its
// sequence number.
func (w *WAL) SyncedSequence() uint64 {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	return w.syncedSeq
}

// SetBeforeSyncHook installs a function that runs after an entry has been
// written and before the append waits for its fsync; nil removes it. It is a
// test seam, exported only because tests in other packages hold a log open and
// need an entry in the written-and-not-synced state: production code must not
// call it. It is safe to call while appends run (an append sees the old hook or
// the new one, never a torn value).
func (w *WAL) SetBeforeSyncHook(hook func(seq uint64)) {
	if hook == nil {
		w.beforeSyncHook.Store(nil)
		return
	}
	w.beforeSyncHook.Store(&hook)
}

// SetFsyncHook installs a function that runs once per group-commit fsync of the
// log, in the goroutine about to call it, before the call; nil removes it. One
// call is one fsync whatever number of appends it covers, so a test counts the
// log's fsyncs with it, or sleeps in it to stand in for a slow disk. It is a
// test seam, exported for tests in other packages: production code must not
// call it.
func (w *WAL) SetFsyncHook(hook func()) {
	if hook == nil {
		w.fsyncHook.Store(nil)
		return
	}
	w.fsyncHook.Store(&hook)
}

// SetMaxSegmentBytes lowers the size at which the log rotates to a new segment.
// It exists so a test in another package can rotate and prune a log with
// kilobytes instead of writing 64 MB; nothing else calls it.
func (w *WAL) SetMaxSegmentBytes(size int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.maxFileSize = size
}

// readSnapshot is what a reader needs from the append lock, taken once.
type readSnapshot struct {
	segments []segmentRef
	// activeIndex is the segment still being appended to, and activeLimit the
	// number of its bytes that were complete entries when the snapshot was taken.
	activeIndex int
	activeLimit int64
	// skips are the runs of sequences the log passed over (WAL.FastForward):
	// the next entry past one is not a hole.
	skips []WALSkip
}

// snapshotForRead takes w.mu only for the length of a directory listing. An
// append writes a whole entry under the same lock and only then moves the write
// offset, so the offset read here is always on an entry boundary; a reader that
// never looks past it never sees half an entry, nor a preallocated segment's
// zero tail, and needs no lock while it decodes.
func (w *WAL) snapshotForRead() readSnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	return readSnapshot{
		segments:    w.listSegments(),
		activeIndex: w.activeIndex,
		activeLimit: w.entryLimitLocked(w.activeIndex),
		skips:       w.skips,
	}
}

// limitFor is the number of bytes of a segment a reader may look at.
func (s readSnapshot) limitFor(segmentIndex int) int64 {
	if segmentIndex == s.activeIndex {
		return s.activeLimit
	}
	return -1
}

// OpenCursor positions a cursor at the first entry with a sequence at or above
// from. It returns ErrBelowHorizon when that entry has been pruned. A sequence
// past the tail is a valid position to wait at.
func (w *WAL) OpenCursor(from uint64) (WALCursor, error) {
	// A sequence the log passed over holds nothing: the read starts at the
	// first one after it, which no retention can have pruned before from.
	from = w.pastSkip(max(from, 1))
	var lastErr error
	for attempt := 0; attempt < reseatAttempts; attempt++ {
		cursor, err := w.openCursorOnce(from)
		if err == nil {
			return cursor, nil
		}
		if retry, settled := w.stillInLog(err, from); !retry {
			return WALCursor{}, settled
		}
		lastErr = err
	}
	return WALCursor{}, fmt.Errorf("opening a cursor at sequence %d: the log's segments were removed faster than they could be opened: %w", from, lastErr)
}

// stillInLog decides what a failed open or read means. A pruned segment, or an
// entry that is not the one asked for, is only a retry if the wanted sequence is
// still at or above the first retained one: the log moved between the check and
// the read and looking again will find it. If the sequence is below the first
// retained one it has really gone, and the answer is the horizon. Any other
// error is settled as it is.
func (w *WAL) stillInLog(err error, wanted uint64) (retry bool, settled error) {
	if !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, ErrBelowHorizon) {
		return false, err
	}
	first, firstErr := w.FirstSequence()
	if firstErr != nil {
		return false, errors.Join(err, firstErr)
	}
	if w.pastSkip(wanted) < first {
		return false, belowHorizon(wanted, first)
	}
	return true, nil
}

// pastSkip is seq, or the first sequence after the skip it lies in.
func (w *WAL) pastSkip(seq uint64) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return pastSkip(w.skips, seq)
}

// reseatAttempts bounds how often a read starts over after a segment it listed
// was pruned before it could open it. Each retry means pruning made progress, so
// more than a few in a row is a log being emptied, not a race.
const reseatAttempts = 4

func (w *WAL) openCursorOnce(from uint64) (WALCursor, error) {
	first, err := w.FirstSequence()
	if err != nil {
		return WALCursor{}, fmt.Errorf("reading the first retained sequence: %w", err)
	}
	if from < first {
		return WALCursor{}, belowHorizon(from, first)
	}
	if w.openCursorHook != nil {
		w.openCursorHook()
	}
	snapshot := w.snapshotForRead()
	if len(snapshot.segments) == 0 {
		return WALCursor{}, fmt.Errorf("the WAL directory %s holds no segment", w.dir)
	}

	chosen, err := w.segmentHolding(snapshot, from)
	if err != nil {
		return WALCursor{}, err
	}
	view, err := w.openSegmentView(chosen, snapshot.limitFor(chosen.index))
	if err != nil {
		return WALCursor{}, w.horizonOrError(err, from)
	}
	defer view.close()

	offset := view.start
	var lastSeq uint64
	for offset < view.limit {
		frame, err := view.frame(offset)
		if err != nil {
			return WALCursor{}, err
		}
		if frame.seq >= from {
			// Sequences are contiguous, so the first entry at or above from that
			// is not from itself means from was pruned after the check above and
			// the segment chosen is a later one. Handing out a cursor here would
			// make the reader skip everything in between.
			if frame.seq > from {
				return WALCursor{}, belowHorizon(from, frame.seq)
			}
			break
		}
		lastSeq = frame.seq
		offset += frame.size
	}
	return WALCursor{segmentIndex: chosen.index, offset: offset, nextSeq: from, lastSeq: lastSeq}, nil
}

// segmentHolding returns the last segment whose first entry is at or below
// from. Sequences ascend across segments, so that is where from is, or where it
// would be appended. A log whose segments are all empty is served by its last.
func (w *WAL) segmentHolding(snapshot readSnapshot, from uint64) (segmentRef, error) {
	segments := snapshot.segments
	for i := len(segments) - 1; i >= 0; i-- {
		firstInSegment, found, err := w.firstSequenceOf(segments[i], snapshot.limitFor(segments[i].index))
		if err != nil {
			return segmentRef{}, w.horizonOrError(err, from)
		}
		if found && firstInSegment <= from {
			return segments[i], nil
		}
	}
	return segments[len(segments)-1], nil
}

// horizonOrError classifies "the file is gone". A segment removed between
// listing it and opening it was pruned; if the sequence wanted is below the
// first one still retained, that is ErrBelowHorizon. If the sequence is still in
// the log (the segment that went held only entries the reader is past, as when a
// cursor sits at the end of a segment that is then pruned after a rotation), it
// is not a horizon and err is returned unchanged, still matching fs.ErrNotExist,
// so the caller can re-open its position.
func (w *WAL) horizonOrError(err error, wanted uint64) error {
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	first, firstErr := w.FirstSequence()
	if firstErr != nil {
		return errors.Join(err, firstErr)
	}
	if w.pastSkip(wanted) >= first {
		return err
	}
	return belowHorizon(wanted, first)
}

// ReadBatch returns the entries from the cursor with a sequence at most upTo,
// holding at most maxBytes of log (an entry that is bigger than the bound is
// returned alone rather than never), and the cursor after them. It takes the
// append lock only to snapshot the segment list and the active segment's
// length, and decodes outside it, so a reader never delays a writer.
//
// The cursor advances by the bytes consumed and moves to the next segment when
// this one is exhausted, so a rotation or a Truncate between calls cannot make
// it skip or repeat an entry. A segment that Truncate removes under an open read
// stays readable through its descriptor. One removed before it is opened is
// ErrBelowHorizon only if the sequence the cursor wants went with it; when that
// sequence is still in the log the cursor is re-opened at it and the read goes on.
func (w *WAL) ReadBatch(cur WALCursor, upTo uint64, maxBytes int) ([]WALEntry, WALCursor, error) {
	original := cur
	var lastErr error
	var gap *logGap
	var gapFirst uint64
	for attempt := 0; attempt < reseatAttempts; attempt++ {
		batch, next, err := w.readBatchOnce(original, upTo, maxBytes)
		if err == nil {
			return batch, next, nil
		}
		// The same gap after a reseat with the first retained sequence where it
		// was is a hole in the log, not a pruner that is too fast: reseating
		// cannot move past it and waiting cannot repair it.
		var seen *logGap
		if errors.As(err, &seen) {
			first, firstErr := w.FirstSequence()
			if firstErr != nil {
				return nil, original, errors.Join(err, firstErr)
			}
			if gap != nil && seen.wanted == gap.wanted && seen.next == gap.next && first == gapFirst {
				return nil, original, fmt.Errorf("reading from sequence %d: %w", original.nextSeq, holeInLog(w.dir, seen.wanted, seen.next))
			}
			gap, gapFirst = seen, first
		}
		if retry, settled := w.stillInLog(err, original.nextSeq); !retry {
			return nil, original, settled
		}
		lastErr = err
		reseated, reseatErr := w.OpenCursor(original.nextSeq)
		if reseatErr != nil {
			return nil, original, reseatErr
		}
		original = reseated
	}
	return nil, original, fmt.Errorf("reading from sequence %d: the log's segments were removed faster than they could be read: %w", original.nextSeq, lastErr)
}

func (w *WAL) readBatchOnce(cur WALCursor, upTo uint64, maxBytes int) ([]WALEntry, WALCursor, error) {
	snapshot := w.snapshotForRead()
	if w.readSnapshotHook != nil {
		w.readSnapshotHook()
	}
	position := 0
	for position < len(snapshot.segments) && snapshot.segments[position].index < cur.segmentIndex {
		position++
	}
	if position == len(snapshot.segments) || snapshot.segments[position].index != cur.segmentIndex {
		return nil, cur, w.horizonOrError(fmt.Errorf("WAL segment %d: %w", cur.segmentIndex, fs.ErrNotExist), cur.nextSeq)
	}

	var batch []WALEntry
	batchBytes := 0
	for {
		segment := snapshot.segments[position]
		view, err := w.openSegmentView(segment, snapshot.limitFor(segment.index))
		if err != nil {
			return nil, cur, w.horizonOrError(err, cur.nextSeq)
		}
		view.skips = snapshot.skips
		stopped, err := w.drainSegment(view, &cur, &batch, &batchBytes, upTo, maxBytes)
		view.close()
		if err != nil {
			return nil, cur, err
		}
		if stopped || position+1 >= len(snapshot.segments) {
			return batch, cur, nil
		}
		position++
		// The last sequence read goes with the cursor, so an empty segment
		// between two does not reset rule 5.
		cur = WALCursor{segmentIndex: snapshot.segments[position].index, offset: 0, nextSeq: cur.nextSeq, lastSeq: cur.lastSeq}
	}
}

// drainSegment reads entries from cur.offset to the end of the view, appending
// to batch and advancing cur. It reports whether a bound (upTo or maxBytes)
// stopped it before the end of the segment.
func (w *WAL) drainSegment(view *walSegmentView, cur *WALCursor, batch *[]WALEntry, batchBytes *int, upTo uint64, maxBytes int) (bool, error) {
	cur.offset = max(cur.offset, view.start)
	for cur.offset < view.limit {
		if w.readDecodeHook != nil {
			w.readDecodeHook()
		}
		entry, size, err := view.readEntry(cur.offset)
		if err != nil {
			return false, err
		}
		if view.format.v2 && cur.lastSeq != 0 && entry.Seq <= cur.lastSeq {
			return false, fmt.Errorf("WAL segment %s is damaged at offset %d: its entry's sequence %d does not rise above %d, the one before it",
				view.name, cur.offset, entry.Seq, cur.lastSeq)
		}
		// lastSeq moves only with the offset: an entry a bound stops at below
		// is read again by the next call and must not be compared to itself.
		if entry.Seq < cur.nextSeq {
			cur.offset += size
			cur.lastSeq = entry.Seq
			continue
		}
		// The next entry must be the one wanted, or the first after a skip the
		// wanted one lies in. A later one means the wanted entry is gone (its
		// segment was pruned between the snapshot and this read), and returning
		// the later one would skip the entries in between.
		if entry.Seq > cur.nextSeq && entry.Seq == pastSkip(view.skips, cur.nextSeq) {
			cur.nextSeq = entry.Seq
		}
		if entry.Seq > cur.nextSeq {
			return false, &logGap{wanted: cur.nextSeq, next: entry.Seq}
		}
		if entry.Seq > upTo {
			return true, nil
		}
		if len(*batch) > 0 && *batchBytes+int(size) > maxBytes {
			return true, nil
		}
		*batch = append(*batch, entry)
		*batchBytes += int(size)
		cur.offset += size
		cur.nextSeq = entry.Seq + 1
		cur.lastSeq = entry.Seq
	}
	return false, nil
}

// walSegmentView reads the entries of one open segment without the append lock.
type walSegmentView struct {
	file   *os.File
	name   string
	index  int
	start  int64 // offset of the first entry: past the file header, if any
	limit  int64 // one past the last byte a reader may look at
	format walSegmentFormat
	aead   cipher.AEAD
	// skips are the log's skips as the read's snapshot saw them; set by
	// readBatchOnce, which is the only reader that checks the next sequence.
	skips []WALSkip
}

// openSegmentView opens a segment for reading. limit is the snapshotted length
// of the active segment, or -1 to read the whole file, which is right for a
// segment that has been rotated out and will not grow.
func (w *WAL) openSegmentView(seg segmentRef, limit int64) (*walSegmentView, error) {
	file, err := os.Open(filepath.Join(w.dir, seg.name))
	if err != nil {
		return nil, fmt.Errorf("opening WAL segment %s to read it: %w", seg.name, err)
	}
	view := &walSegmentView{file: file, name: seg.name, index: seg.index}
	info, err := file.Stat()
	if err != nil {
		view.close()
		return nil, fmt.Errorf("stat WAL segment %s: %w", seg.name, err)
	}
	view.limit = info.Size()
	if limit >= 0 && limit < view.limit {
		view.limit = limit
	}
	format, err := readSegmentFormat(file, view.limit, seg.name)
	if err != nil {
		view.close()
		return nil, err
	}
	view.format = format
	view.start = format.entryStart()
	if format.sealed {
		if err := view.adoptSealed(w); err != nil {
			view.close()
			return nil, err
		}
	}
	if format.v2 && view.limit >= view.start+8 {
		// A zero first word means the segment holds no entry, whatever its
		// size (spec §3.3): the reader sees an empty segment, not damage.
		word := make([]byte, 8)
		if _, err := file.ReadAt(word, view.start); err != nil {
			view.close()
			return nil, fmt.Errorf("reading the first entry of WAL segment %s: %w", seg.name, err)
		}
		if binary.BigEndian.Uint64(word) == 0 {
			view.limit = view.start
		}
	}
	return view, nil
}

func (v *walSegmentView) adoptSealed(w *WAL) error {
	if w.keys == nil {
		return fmt.Errorf("WAL segment %s is sealed and this node was given no key for it: start the node with the encryption keystore that wrote it", v.name)
	}
	aead, err := walEntryAEAD(w.keys, v.format.epochID, v.index)
	if err != nil {
		return fmt.Errorf("WAL segment %s: %w", v.name, err)
	}
	v.aead = aead
	return nil
}

func (v *walSegmentView) close() {
	_ = v.file.Close()
}

// walFrame is what the 16 bytes at the start of an entry say: its sequence, and
// how long it is. In a plain entry those are the length word and the first eight
// bytes of the body; in a sealed one the length word and the clear sequence.
type walFrame struct {
	seq         uint64
	bodyLen     int
	size        int64
	checksum    uint32
	checksummed bool
}

func (v *walSegmentView) frame(offset int64) (walFrame, error) {
	framing := make([]byte, 8+walSeqFieldLen)
	if v.limit-offset < int64(len(framing)) {
		return walFrame{}, fmt.Errorf("WAL segment %s ends inside an entry header at offset %d", v.name, offset)
	}
	if _, err := v.file.ReadAt(framing, offset); err != nil {
		return walFrame{}, fmt.Errorf("reading the entry header at offset %d of WAL segment %s: %w", offset, v.name, err)
	}
	word := binary.BigEndian.Uint64(framing[:8])
	seq := binary.BigEndian.Uint64(framing[8:])
	if v.format.sealed {
		onDiskLen := int64(uint32(word))
		if onDiskLen < walSealedMinOnDisk || onDiskLen > int64(walSealedMaxOnDisk) || onDiskLen > v.limit-offset-int64(len(framing)) {
			return walFrame{}, fmt.Errorf("WAL segment %s has an implausible entry length %d at offset %d", v.name, onDiskLen, offset)
		}
		return walFrame{seq: seq, bodyLen: int(onDiskLen), size: int64(len(framing)) + onDiskLen, checksum: uint32(word >> 32), checksummed: true}, nil
	}
	bodyLen, checksum, _ := decodeWALHeader(word)
	checksummed := v.format.checksumVerified(checksum)
	if bodyLen < walMinEntrySize || bodyLen > walMaxEntrySize || int64(bodyLen) > v.limit-offset-8 {
		return walFrame{}, fmt.Errorf("WAL segment %s has an implausible entry length %d at offset %d", v.name, bodyLen, offset)
	}
	return walFrame{seq: seq, bodyLen: bodyLen, size: 8 + int64(bodyLen), checksum: checksum, checksummed: checksummed}, nil
}

// readEntry decodes the entry at offset and returns it with its length on disk.
// Damage is an error here and never a torn tail: the reader looks only at bytes
// that were complete entries when it started, so a failed checksum is the log
// being damaged and not a write in progress.
func (v *walSegmentView) readEntry(offset int64) (WALEntry, int64, error) {
	frame, err := v.frame(offset)
	if err != nil {
		return WALEntry{}, 0, err
	}
	if v.format.sealed {
		return v.readSealedEntry(offset, frame)
	}
	body := make([]byte, frame.bodyLen)
	if _, err := v.file.ReadAt(body, offset+8); err != nil {
		return WALEntry{}, 0, fmt.Errorf("reading the entry at offset %d of WAL segment %s: %w", offset, v.name, err)
	}
	if frame.checksummed && v.format.checksum(nil, body) != frame.checksum {
		return WALEntry{}, 0, fmt.Errorf("WAL segment %s: the entry at offset %d failed its checksum", v.name, offset)
	}
	entry, err := parseWALEntry(body[8:], frame.seq)
	if err != nil {
		return WALEntry{}, 0, fmt.Errorf("WAL segment %s: the entry at offset %d (sequence %d) cannot be read: %w", v.name, offset, frame.seq, err)
	}
	return entry, frame.size, nil
}

func (v *walSegmentView) readSealedEntry(offset int64, frame walFrame) (WALEntry, int64, error) {
	onDisk := make([]byte, frame.bodyLen)
	if _, err := v.file.ReadAt(onDisk, offset+8+walSeqFieldLen); err != nil {
		return WALEntry{}, 0, fmt.Errorf("reading the entry at offset %d of WAL segment %s: %w", offset, v.name, err)
	}
	if v.format.checksum(binary.BigEndian.AppendUint64(nil, frame.seq), onDisk) != frame.checksum {
		return WALEntry{}, 0, fmt.Errorf("WAL segment %s: the entry at offset %d failed its checksum", v.name, offset)
	}
	body, err := v.aead.Open(nil, onDisk[:walNonceLen], onDisk[walNonceLen:], walEntryAAD(v.format.header, frame.seq, offset))
	if err != nil {
		return WALEntry{}, 0, fmt.Errorf(
			"WAL segment %s is corrupt: the entry at offset %d (sequence %d, epoch %d) passed its checksum but could not be decrypted: %w",
			v.name, offset, frame.seq, v.format.epochID, err)
	}
	entry, err := parseWALEntry(body, frame.seq)
	if err != nil {
		return WALEntry{}, 0, fmt.Errorf("WAL segment %s: the entry at offset %d (sequence %d) decrypted but cannot be read: %w", v.name, offset, frame.seq, err)
	}
	return entry, frame.size, nil
}
