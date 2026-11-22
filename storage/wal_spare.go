package storage

import (
	"bytes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// A preallocating log keeps one spare: the next segment, born ahead of the
// rotation that needs it as a whole segment of written zeros behind a v2
// header, so the active segment's size never changes and its group commit is a
// data-only flush (spec §2.1, §4.1). One background filler per log (per node,
// never per bucket) makes it; a rotation takes it under mu.

// spareState is where the spare is (spec §2.1). It is guarded by spareMu.
type spareState int

const (
	// spareIdle: no wal_spare.ready, no fill running.
	spareIdle spareState = iota
	// spareFilling: exactly one fill owns wal_spare.tmp.
	spareFilling
	// spareReady: wal_spare.ready exists and nothing touches it until a
	// rotation or the open takes it, under mu.
	spareReady
)

// spareFill is one fill of the spare. done is closed when the fill ends, after
// err holds its result, so a rotation that waited on done reads err safely.
type spareFill struct {
	done chan struct{}
	err  error
}

// walSpareRetryInterval is how long the filler waits after a failed fill
// before it tries again: a full disk costs one attempt per interval, not a
// loop. It is the table's fillRetry in production.
const walSpareRetryInterval = 10 * time.Second

// walFillWarnInterval is how often a fill that keeps failing is logged again:
// the first failure is a WARN at once, and later ones are counted into one
// WARN an hour, not one per retry (server/reclaim.go's reclaimWarnInterval).
const walFillWarnInterval = time.Hour

// fillWarning is what the log has said about failing fills, guarded by
// spareMu: whether the last fill failed, when the last WARN was written, and
// how many failures since then it has not reported.
type fillWarning struct {
	failing  bool
	warnedAt time.Time
	unwarned int
}

// walSpareChunk is the size of each zero write of a fill, each followed by a
// data-only flush, so a fill never holds more than this of dirty page cache.
const walSpareChunk = 1 << 20

// walSpareCheckLen is how many bytes after a spare's header its validation
// reads: a spare that is not zero there was not filled by this log.
const walSpareCheckLen = 4 << 10

// errFillCancelled ends a fill that Close stopped.
var errFillCancelled = errors.New("the write-ahead log closed while its next segment was being filled")

// errSpareUnusable marks a ready spare that failed validation and was deleted.
var errSpareUnusable = errors.New("the ready spare segment is not usable")

// beginFill claims the spare for one fill, from idle only: the background
// filler and a rotation's synchronous fill both claim it here, so two fills
// never exist and none writes into a file a rotation has taken (owner D11).
func (w *WAL) beginFill() (*spareFill, bool) {
	w.spareMu.Lock()
	defer w.spareMu.Unlock()
	if w.spareState != spareIdle {
		return nil, false
	}
	fill := &spareFill{done: make(chan struct{})}
	w.spareState, w.spareCurrent = spareFilling, fill
	return fill, true
}

// endFill records how fill ended and wakes whatever waits on it: the spare is
// ready after a fill that succeeded and idle after one that failed, whose file
// fillSpare already deleted.
func (w *WAL) endFill(fill *spareFill, err error) {
	w.spareMu.Lock()
	fill.err = err
	w.spareState, w.spareCurrent = spareReady, nil
	if err != nil {
		w.spareState = spareIdle
	}
	w.spareMu.Unlock()
	close(fill.done)
}

// setSpareIdle records that the ready spare is gone: taken by a rotation or
// the open, or deleted as unusable.
func (w *WAL) setSpareIdle() {
	w.spareMu.Lock()
	defer w.spareMu.Unlock()
	w.spareState = spareIdle
}

// fillSpare runs one fill with the table sys (B1 to B7) and ends with a
// wal_spare.ready, or with nothing: a fill that fails deletes its
// wal_spare.tmp before it returns. That delete is load-bearing, not tidiness:
// ext4 keeps the blocks a failed fallocate managed to allocate (NR-1), so until
// the file goes the space a full disk needs back stays taken. stop cancels the
// fill between chunks; nil never does. The outcome is logged and counted
// (noteFill).
func (w *WAL) fillSpare(sys *walSyscalls, stop <-chan struct{}) error {
	fillStage(sys, "begin")
	started := time.Now()
	tmpPath := filepath.Join(w.dir, walSpareTmpName)
	err := w.writeSpare(sys, tmpPath, stop)
	if err != nil {
		if removeErr := sys.remove(tmpPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("removing %s after the failed fill, which still holds its blocks: %w", walSpareTmpName, removeErr))
		}
	}
	w.counters.zeroFillNanos.Add(int64(time.Since(started)))
	w.noteFill(sys.now(), err)
	return err
}

// noteFill logs and counts how a fill ended (spec §4.3): the first failure is
// a WARN; later ones are counted into a WARN at most once an hour, so a full
// disk the filler retries every 10 s is not a WARN every 10 s; the first fill
// that succeeds after a failure is one INFO. A fill Close cancelled is neither.
func (w *WAL) noteFill(now time.Time, err error) {
	if errors.Is(err, errFillCancelled) {
		return
	}
	if err != nil {
		w.counters.fillFailures.Add(1)
	}
	w.spareMu.Lock()
	warning := &w.fillWarning
	var warn, recovered bool
	var unwarned int
	switch {
	case err == nil:
		recovered = warning.failing
		*warning = fillWarning{}
	case !warning.failing || now.Sub(warning.warnedAt) >= walFillWarnInterval:
		warn, unwarned = true, warning.unwarned+1
		*warning = fillWarning{failing: true, warnedAt: now}
	default:
		warning.unwarned++
	}
	w.spareMu.Unlock()

	switch {
	case warn:
		// The log is not poisoned: the active segment is untouched, and a
		// rotation that finds no spare fills one itself or refuses.
		slog.Warn("could not prepare the next write-ahead log segment; retrying every 10 s",
			"dir", w.dir, "failures_since_last_warning", unwarned, "error", err)
	case recovered:
		slog.Info("prepared the next write-ahead log segment again after failing to", "dir", w.dir)
	}
}

// fillStage reports a fill's progress to a test's hook; production has none.
func fillStage(sys *walSyscalls, stage string) {
	if sys.fillStage != nil {
		sys.fillStage(stage)
	}
}

// writeSpare creates wal_spare.tmp, fills it and names it wal_spare.ready.
func (w *WAL) writeSpare(sys *walSyscalls, tmpPath string, stop <-chan struct{}) error {
	// B1: a leftover is a fill a crash or a failure interrupted.
	if err := sys.remove(tmpPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing a leftover %s before filling the next WAL segment: %w", walSpareTmpName, err)
	}
	spare, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s for the next WAL segment: %w", walSpareTmpName, err)
	}
	fillStage(sys, "created")
	if err := w.fillOpenSpare(sys, spare, stop); err != nil {
		return errors.Join(err, spare.Close())
	}
	if err := spare.Close(); err != nil {
		return fmt.Errorf("closing the filled %s: %w", walSpareTmpName, err)
	}

	// B7: named ready only now, after its fsync, so a ready spare is always
	// a whole one; the directory fsync makes the name survive a power cut.
	readyPath := filepath.Join(w.dir, walSpareReadyName)
	if err := sys.rename(tmpPath, readyPath); err != nil {
		return fmt.Errorf("naming the filled %s %s: %w", walSpareTmpName, walSpareReadyName, err)
	}
	if err := sys.syncDir(w.dir); err != nil {
		err = fmt.Errorf("making the name of %s durable: %w", walSpareReadyName, err)
		// The spare state goes back to idle, and a ready file the state does
		// not know of would refuse the next fill's rename.
		if removeErr := sys.remove(readyPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("removing %s: %w", walSpareReadyName, removeErr))
		}
		return err
	}
	fillStage(sys, "ready")
	return nil
}

// fillOpenSpare is B2 to B6 on the open wal_spare.tmp.
func (w *WAL) fillOpenSpare(sys *walSyscalls, spare *os.File, stop <-chan struct{}) error {
	// B2: never take the filesystem to its last bytes, which the store needs:
	// a fill starts only with room for two segments.
	need := 2 * w.spareBytes
	free, err := sys.statfsFree(w.dir)
	if err != nil {
		return fmt.Errorf("measuring the free space in %s before filling the next WAL segment: %w", w.dir, err)
	}
	if free < uint64(need) {
		return fmt.Errorf("%d bytes are free in %s and filling the next %d-byte WAL segment needs %d, room for two: %w",
			free, w.dir, w.spareBytes, need, syscall.ENOSPC)
	}
	// B3
	if err := sys.fallocate(spare, w.spareBytes); err != nil {
		return fmt.Errorf("reserving the next WAL segment: %w", err)
	}
	fillStage(sys, "allocated")
	// B4: this run's key mode and epoch; a later run validates them at use.
	var epochID uint64
	if w.keys != nil {
		epochID = w.keys.CurrentEpoch()
	}
	format := newWALv2Format(w.keys != nil, epochID)
	if _, err := sys.writeAt(spare, format.header, 0); err != nil {
		return fmt.Errorf("writing the header of the next WAL segment: %w", err)
	}
	// B5: zeros, written and flushed a chunk at a time, chunk-aligned. Written
	// rather than left to fallocate: a write into an unwritten extent converts
	// it, and that conversion would be paid by the group commit's flush.
	zeros := make([]byte, min(walSpareChunk, w.spareBytes))
	for offset := int64(walFileHeaderLen); offset < w.spareBytes; {
		select {
		case <-stop:
			return errFillCancelled
		default:
		}
		n := min(walSpareChunk-offset%walSpareChunk, w.spareBytes-offset)
		if _, err := sys.writeAt(spare, zeros[:n], offset); err != nil {
			return fmt.Errorf("writing zeros at offset %d of the next WAL segment: %w", offset, err)
		}
		w.counters.zeroFillBytes.Add(uint64(n))
		if err := sys.fdatasync(spare); err != nil {
			return fmt.Errorf("flushing zeros at offset %d of the next WAL segment: %w", offset, err)
		}
		offset += n
		fillStage(sys, "chunk")
	}
	// B6: the size and extents must be durable before fdatasync is the only
	// flush the segment gets.
	if err := sys.fsync(spare); err != nil {
		return fmt.Errorf("fsyncing the filled next WAL segment: %w", err)
	}
	return nil
}

// startFiller starts the background filler and asks it for a spare. It runs
// with its own copy of the table: it reads it without mu, and tests replace
// entries of w.sys after the open.
func (w *WAL) startFiller() {
	w.fillerStop = make(chan struct{})
	w.fillerWake = make(chan struct{}, 1)
	sys := w.sys
	w.fillerDone.Add(1)
	go w.runFiller(&sys)
	w.wakeFiller()
}

// wakeFiller asks the filler for a spare. It never blocks: a wake already
// pending covers this one, and before the filler starts there is no channel.
func (w *WAL) wakeFiller() {
	select {
	case w.fillerWake <- struct{}{}:
	default:
	}
}

// runFiller is the filler: it fills the spare whenever it is woken and the
// spare is idle, and after a failed fill again every sys.fillRetry. It never
// takes mu, so a rotation can wait for it with mu held and Close can stop it
// with mu held. It exits when Close closes fillerStop.
func (w *WAL) runFiller(sys *walSyscalls) {
	defer w.fillerDone.Done()
	retry := time.NewTimer(sys.fillRetry)
	retry.Stop()
	defer retry.Stop()
	for {
		select {
		case <-w.fillerStop:
			return
		case <-w.fillerWake:
		case <-retry.C:
		}
		fill, claimed := w.beginFill()
		if !claimed {
			continue
		}
		err := w.fillSpare(sys, w.fillerStop)
		w.endFill(fill, err)
		switch {
		case errors.Is(err, errFillCancelled):
			return
		case err != nil:
			retry.Reset(sys.fillRetry)
		}
	}
}

// stopFiller cancels a fill in flight, which deletes its wal_spare.tmp, and
// waits for the filler to exit. A log that does not preallocate has none.
func (w *WAL) stopFiller() {
	if w.fillerStop == nil {
		return
	}
	w.fillerStopOnce.Do(func() { close(w.fillerStop) })
	w.fillerDone.Wait()
}

// prepareSpareSegment is a preallocating log's R0 for segment idx, under mu:
// it takes the ready spare; waits, with mu held, for a fill in flight and
// takes what it made (D11); or, when the spare is idle, fills one itself under
// the single-fill rule. A spare that fails validation is deleted and the
// rotation goes on as if none were ready, filling one itself, once. waited is
// whether it held mu for a fill, its own or the filler's.
func (w *WAL) prepareSpareSegment(idx int) (seg bornSegment, waited bool, err error) {
	filled := false
	for {
		w.spareMu.Lock()
		state, fill := w.spareState, w.spareCurrent
		w.spareMu.Unlock()
		switch state {
		case spareFilling:
			// spareMu is released: the filler takes it to end the fill.
			<-fill.done
			waited = true
			if fill.err != nil {
				return bornSegment{}, waited, fmt.Errorf("preparing the next WAL segment: %w", fill.err)
			}
		case spareIdle:
			own, claimed := w.beginFill()
			if !claimed {
				// The filler claimed it between the read and here.
				continue
			}
			waited = true
			err := w.fillSpare(&w.sys, nil)
			w.endFill(own, err)
			if err != nil {
				return bornSegment{}, waited, fmt.Errorf("preparing the next WAL segment: %w", err)
			}
			filled = true
		case spareReady:
			seg, err := w.takeReadySpare(idx)
			if err == nil {
				return seg, waited, nil
			}
			// takeReadySpare deleted the spare.
			w.setSpareIdle()
			if !errors.Is(err, errSpareUnusable) || filled {
				return bornSegment{}, waited, err
			}
			slog.Warn("deleted an unusable spare write-ahead log segment; preparing a new one", "dir", w.dir, "reason", err)
			filled = true
		}
	}
}

// takeReadySpare opens wal_spare.ready as segment idx: it validates it,
// re-heads it when its key mode or epoch is not this run's, derives the
// segment's file key and returns it open under the segment's name, still
// named wal_spare.ready on disk until the rotation renames it (R3). On any
// failure it deletes the spare, so the state can go back to idle: an unusable
// one is reported with errSpareUnusable; a failed re-head fails R0.
func (w *WAL) takeReadySpare(idx int) (bornSegment, error) {
	readyPath := filepath.Join(w.dir, walSpareReadyName)
	seg, err := w.openReadySpare(readyPath, idx)
	if err != nil {
		if removeErr := w.sys.remove(readyPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("removing %s: %w", walSpareReadyName, removeErr))
		}
		return bornSegment{}, err
	}
	return seg, nil
}

// openReadySpare is takeReadySpare without the delete; it closes the spare on
// any failure.
func (w *WAL) openReadySpare(readyPath string, idx int) (bornSegment, error) {
	spare, err := os.OpenFile(readyPath, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return bornSegment{}, fmt.Errorf("%w: %s is missing", errSpareUnusable, walSpareReadyName)
	}
	if err != nil {
		return bornSegment{}, fmt.Errorf("opening %s: %w", walSpareReadyName, err)
	}
	format, err := w.validateSpare(spare)
	if err == nil && w.spareIsStale(format) {
		format, err = w.reheadSpare(spare)
	}
	var aead cipher.AEAD
	if err == nil && format.sealed {
		aead, err = walEntryAEAD(w.keys, format.epochID, idx)
	}
	if err != nil {
		return bornSegment{}, errors.Join(err, spare.Close())
	}
	named, err := fileUnderName(spare, segmentFilePath(w.dir, idx))
	if err != nil {
		return bornSegment{}, err
	}
	return bornSegment{file: named, index: idx, format: format, aead: aead, bornAs: walSpareReadyName, preallocated: true}, nil
}

// validateSpare checks the spare in f is what this log's fill makes (spec
// §2.1 "Validation at use"): exactly a segment long, a v2 header, and zeros in
// the 4 KiB after it. A spare that is not is errSpareUnusable; an I/O error
// reading it is that error.
func (w *WAL) validateSpare(f *os.File) (walSegmentFormat, error) {
	info, err := f.Stat()
	if err != nil {
		return walSegmentFormat{}, fmt.Errorf("stat %s: %w", walSpareReadyName, err)
	}
	if info.Size() != w.spareBytes {
		return walSegmentFormat{}, fmt.Errorf("%w: it is %d bytes and a segment is %d", errSpareUnusable, info.Size(), w.spareBytes)
	}
	head := make([]byte, min(walFileHeaderLen+walSpareCheckLen, w.spareBytes))
	if _, err := f.ReadAt(head, 0); err != nil {
		return walSegmentFormat{}, fmt.Errorf("reading the start of %s: %w", walSpareReadyName, err)
	}
	format, err := parseWALv2Header(head)
	if err != nil {
		return walSegmentFormat{}, fmt.Errorf("%w: %w", errSpareUnusable, err)
	}
	if i := bytes.IndexFunc(head[walFileHeaderLen:], func(r rune) bool { return r != 0 }); i >= 0 {
		return walSegmentFormat{}, fmt.Errorf("%w: a non-zero byte at offset %d, where its zeros start", errSpareUnusable, walFileHeaderLen+i)
	}
	return format, nil
}

// spareIsStale reports whether a valid spare was filled in another key mode
// or epoch than this run's: a spare survives Close, and a restart can add or
// remove the key source or move to a new epoch.
func (w *WAL) spareIsStale(format walSegmentFormat) bool {
	if format.sealed != (w.keys != nil) {
		return true
	}
	return format.sealed && format.epochID != w.keys.CurrentEpoch()
}

// reheadSpare writes a fresh header for this run's key mode and epoch, with a
// new generation, over a stale spare's, and flushes it: an overwrite of
// written blocks, so a data-only flush.
func (w *WAL) reheadSpare(f *os.File) (walSegmentFormat, error) {
	var epochID uint64
	if w.keys != nil {
		epochID = w.keys.CurrentEpoch()
	}
	format := newWALv2Format(w.keys != nil, epochID)
	if _, err := w.sys.writeAt(f, format.header, 0); err != nil {
		return walSegmentFormat{}, fmt.Errorf("re-heading the stale %s: %w", walSpareReadyName, err)
	}
	if err := w.sys.fdatasync(f); err != nil {
		return walSegmentFormat{}, fmt.Errorf("flushing the re-headed %s: %w", walSpareReadyName, err)
	}
	return format, nil
}
