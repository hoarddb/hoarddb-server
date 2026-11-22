package cli

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dumpCounters is what the dump's summary record says the file holds beyond its own
// record count. The caller owns these four figures, because only the code that reads
// the ring knows a database, a bucket, an entity or an element; the writer counts its
// records itself.
type dumpCounters struct {
	Databases int64
	Buckets   int64
	Entities  int64
	Elements  int64
}

// dumpWriter writes one dump file of format version 1
// (the ring cli parity build spec section 3.1). It streams: at most one encoded
// record is held (maxDumpRecordBytes, 31 MiB today), plus the sha256 state of about
// 100 bytes and the two paths. It keeps no list of records, entities or buckets, so
// its memory does not grow with the dump.
//
// The file never has its final name until it is complete: the writer creates
// <final>.partial, and Close writes the summary and the footer, syncs, and only then
// renames it. A caller that fails or is interrupted calls Abort, which removes the
// partial and never touches a final file.
type dumpWriter struct {
	file        *os.File
	partialPath string
	finalPath   string
	digest      hash.Hash
	byteCount   int64
	recordCount int64
	completed   bool
	discarded   bool
}

// openDumpWriter creates <dir>/<name>.partial, writes the magic and returns a writer
// for it. An existing <dir>/<name> is refused and left untouched (rule 10: a dump
// never overwrites a file). A symbolic link at the partial path is refused by
// createPrivateFile, and the mode is 0600 whatever the umask says.
func openDumpWriter(dir, name string) (*dumpWriter, error) {
	finalPath := filepath.Join(dir, name)
	if _, err := os.Lstat(finalPath); err == nil {
		return nil, fmt.Errorf("File %s already exists. Pick another --out, or remove it", name) //nolint:staticcheck // ST1005: the refusal text is fixed by section 3.1 of the build spec
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("checking whether %s exists: %w", finalPath, err)
	}

	partialPath := finalPath + ".partial"
	file, err := createPrivateFile(partialPath)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", partialPath, err)
	}
	writer := &dumpWriter{
		file:        file,
		partialPath: partialPath,
		finalPath:   finalPath,
		digest:      sha256.New(),
	}
	// The checksum covers the magic as well: it is the first byte of the range the
	// footer's bytes member measures.
	if err := writer.writeCovered([]byte(dumpMagic)); err != nil {
		if discardErr := writer.Abort(); discardErr != nil {
			return nil, errors.Join(fmt.Errorf("writing the dump magic: %w", err), discardErr)
		}
		return nil, fmt.Errorf("writing the dump magic: %w", err)
	}
	return writer, nil
}

// WriteRecord appends one record to the partial file and to the running checksum. A
// record longer than maxDumpRecordBytes is refused before anything is written, so a
// file the reader would refuse is never produced. The writer counts every record it
// writes; the summary's records member is that count before the summary itself.
func (w *dumpWriter) WriteRecord(record any) error {
	if w.completed || w.discarded || w.file == nil {
		return fmt.Errorf("writing a dump record after the writer was closed, aborted or failed")
	}
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding a %T dump record: %w", record, err)
	}
	if len(encoded) > maxDumpRecordBytes {
		return fmt.Errorf("a %T dump record is %d bytes and a dump record may hold at most %d; it was not written", record, len(encoded), maxDumpRecordBytes)
	}
	if err := w.writeCovered(encoded); err != nil {
		return fmt.Errorf("writing dump record %d: %w", w.recordCount+1, err)
	}
	w.recordCount++
	return nil
}

// Close completes the file: it writes the summary and the footer, syncs the file,
// renames the partial into place and syncs the directory. The summary is covered by
// the checksum; the footer is not, because it carries the checksum. A caller that
// gets an error must call Abort.
func (w *dumpWriter) Close(counters dumpCounters) error {
	if w.completed {
		return fmt.Errorf("the dump writer for %s is already closed", w.finalPath)
	}
	if w.discarded {
		return fmt.Errorf("the dump writer for %s was aborted", w.finalPath)
	}
	if w.file == nil {
		return fmt.Errorf("the dump writer for %s cannot be completed after a failed Close; call Abort", w.finalPath)
	}

	summary, err := bson.Marshal(&dumpSummaryRecord{
		T:         dumpKindSummary,
		Records:   w.recordCount,
		Databases: counters.Databases,
		Buckets:   counters.Buckets,
		Entities:  counters.Entities,
		Elements:  counters.Elements,
	})
	if err != nil {
		return fmt.Errorf("encoding the dump summary: %w", err)
	}
	if err := w.writeCovered(summary); err != nil {
		return fmt.Errorf("writing the dump summary: %w", err)
	}

	footer, err := bson.Marshal(&dumpFooter{
		T:      dumpKindFooter,
		Bytes:  w.byteCount,
		SHA256: w.digest.Sum(nil),
	})
	if err != nil {
		return fmt.Errorf("encoding the dump footer: %w", err)
	}
	if _, err := w.file.Write(footer); err != nil {
		return fmt.Errorf("writing the dump footer to %s: %w", w.partialPath, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("syncing %s: %w", w.partialPath, err)
	}
	if err := w.file.Close(); err != nil {
		w.file = nil
		return fmt.Errorf("closing %s: %w", w.partialPath, err)
	}
	w.file = nil

	if err := os.Rename(w.partialPath, w.finalPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", w.partialPath, w.finalPath, err)
	}
	// Past the rename the file is complete, so Abort must not remove it, whatever
	// the directory sync below returns.
	w.completed = true
	if err := syncDumpDirectory(filepath.Dir(w.finalPath)); err != nil {
		return fmt.Errorf("making the rename of %s durable: %w", w.finalPath, err)
	}
	return nil
}

// Abort closes and removes the partial file and does nothing else. It is safe to call
// after a failed Close and more than once, and it never touches a file with the final
// name: a completed dump's file stays, and a refused open leaves the existing final
// file alone.
func (w *dumpWriter) Abort() error {
	if w.completed || w.discarded {
		return nil
	}
	w.discarded = true

	var closeErr error
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			closeErr = fmt.Errorf("closing %s: %w", w.partialPath, err)
		}
		w.file = nil
	}
	removeErr := os.Remove(w.partialPath)
	if errors.Is(removeErr, fs.ErrNotExist) {
		removeErr = nil
	} else if removeErr != nil {
		removeErr = fmt.Errorf("removing %s: %w", w.partialPath, removeErr)
	}
	return errors.Join(closeErr, removeErr)
}

// writeCovered adds record to the checksum and then to the file, so a footer written
// later covers exactly the bytes before it. A failed write leaves the writer unusable
// and the caller aborts: there is no path that continues from a partial record.
func (w *dumpWriter) writeCovered(record []byte) error {
	if _, err := w.digest.Write(record); err != nil {
		return fmt.Errorf("adding %d bytes to the dump checksum: %w", len(record), err)
	}
	if _, err := w.file.Write(record); err != nil {
		return fmt.Errorf("writing %d bytes to %s: %w", len(record), w.partialPath, err)
	}
	w.byteCount += int64(len(record))
	return nil
}

// syncDumpDirectory makes the rename durable. Some filesystems refuse fsync on a
// directory (EINVAL); the rename is atomic there regardless, so that one error is not
// a failure of the dump.
func syncDumpDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("opening %s to flush the rename: %w", directory, err)
	}
	syncErr := handle.Sync()
	closeErr := handle.Close()
	if syncErr != nil && !errors.Is(syncErr, syscall.EINVAL) {
		return errors.Join(fmt.Errorf("flushing the rename in %s: %w", directory, syncErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("closing %s after flushing the rename: %w", directory, closeErr)
	}
	return nil
}
