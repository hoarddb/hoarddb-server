package cli

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dumpCopyChunkBytes is how much of a buffered entity moves into the stream at once. The
// copy has to be chunked, not read whole, or a spilled entity would be loaded back into
// memory and the spill would have bought nothing.
const dumpCopyChunkBytes = 64 << 10

// dumpRecordSink is what one entity's records are written to: the dump stream itself
// (*dumpWriter), or a buffer that holds them in memory or in a spill file until the
// entity's turn in page order (section 3.3).
type dumpRecordSink interface {
	WriteRecord(record any) error
}

// dumpSpillBytes is the in-memory limit of one entity buffer. It is a variable only so a
// test can raise it, which keeps the test off the disk; nothing at run time changes it.
var dumpSpillBytes = 1 << 20

// dumpSpiller creates and counts the spill files of one dump run. Both counters are
// atomic because the parallel readers of slice Db3 spill from several goroutines. The
// file names carry ".spill." because section 3.5 tells the operator to delete
// `.spill.*` after a run that was killed.
type dumpSpiller struct {
	directory string
	finalName string
	number    atomic.Uint64
	live      atomic.Int64
	peak      atomic.Int64
}

// newDumpSpiller returns a spiller of <directory>/<finalName>.spill.<n>. finalName is the
// dump's own file name, so a spill file sits next to it and an operator cleaning up after
// a killed run sees both names in one directory.
func newDumpSpiller(directory, finalName string) *dumpSpiller {
	return &dumpSpiller{directory: directory, finalName: finalName}
}

// create opens one spill file, private to its owner like the dump itself, and raises the
// live and peak counts. The caller removes it with remove.
func (s *dumpSpiller) create() (*os.File, string, error) {
	number := s.number.Add(1)
	path := filepath.Join(s.directory, fmt.Sprintf("%s.spill.%d", s.finalName, number))
	file, err := createPrivateFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("creating the spill file %s: %w", path, err)
	}
	live := s.live.Add(1)
	for {
		peak := s.peak.Load()
		if live <= peak || s.peak.CompareAndSwap(peak, live) {
			break
		}
	}
	return file, path, nil
}

// remove deletes one spill file and lowers the live count. The count follows the disk, so
// a removal that fails leaves both the file and the count in place and reports why.
func (s *dumpSpiller) remove(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("removing the spill file %s: %w", path, err)
	}
	s.live.Add(-1)
	return nil
}

// dumpEntityBuffer holds one entity's records encoded exactly as the stream would carry
// them: in memory until they would pass dumpSpillBytes, then in one spill file
// (section 3.3). A buffer under the limit never touches the disk.
//
// counts is the caller's, not the buffer's: only the code that reads the ring knows what
// the entity adds to the dump's entity and element totals, and it adds them only when the
// buffered entity is copied.
type dumpEntityBuffer struct {
	spiller *dumpSpiller
	records int64
	counts  dumpCounters

	memory      []byte
	file        *os.File
	path        string
	spillWriter *bufio.Writer
}

// newDumpEntityBuffer returns an empty buffer. Nothing is allocated for the entity until
// its first record arrives.
func newDumpEntityBuffer(spiller *dumpSpiller) *dumpEntityBuffer {
	return &dumpEntityBuffer{spiller: spiller}
}

// WriteRecord encodes one record and appends it to the buffer, spilling to a file first
// when the records would pass dumpSpillBytes. A record longer than maxDumpRecordBytes is
// refused with the same text as dumpWriter.WriteRecord, because the file the buffer feeds
// is the same file.
func (b *dumpEntityBuffer) WriteRecord(record any) error {
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding a %T dump record: %w", record, err)
	}
	if len(encoded) > maxDumpRecordBytes {
		return fmt.Errorf("a %T dump record is %d bytes and a dump record may hold at most %d; it was not written", record, len(encoded), maxDumpRecordBytes)
	}
	if b.file == nil && len(b.memory)+len(encoded) > dumpSpillBytes {
		// The caller's error path closes the buffer, which removes the file and the
		// memory, so a failure here leaves nothing behind.
		if err := b.spill(); err != nil {
			return err
		}
	}
	if b.file != nil {
		if _, err := b.spillWriter.Write(encoded); err != nil {
			return fmt.Errorf("writing dump record %d of a buffered entity to %s: %w", b.records+1, b.path, err)
		}
	} else {
		b.memory = append(b.memory, encoded...)
	}
	b.records++
	return nil
}

// spill moves the records held in memory into a new spill file, after which every record
// goes there. Memory is released only once the file holds the bytes, so a failed spill
// never loses a record the stream has not seen.
func (b *dumpEntityBuffer) spill() error {
	file, path, err := b.spiller.create()
	if err != nil {
		return err
	}
	b.file = file
	b.path = path
	b.spillWriter = bufio.NewWriterSize(file, dumpCopyChunkBytes)
	if _, err := b.spillWriter.Write(b.memory); err != nil {
		return fmt.Errorf("moving %d bytes of a buffered entity into %s: %w", len(b.memory), path, err)
	}
	b.memory = nil
	return nil
}

// copyTo writes the buffered entity into the stream, in record order, and counts its
// records. The stream and its checksum are appended to and never rewound, so an entity
// copied from a spill file reaches the dump exactly as if it had been written straight
// through. The spill file is removed whatever the copy returns: a failed copy leaves the
// writer unusable, and the run that owns it is over.
func (b *dumpEntityBuffer) copyTo(writer *dumpWriter) error {
	var copyErr error
	if b.file == nil {
		copyErr = writer.writeBuffered(bytes.NewReader(b.memory), b.records)
	} else {
		copyErr = b.copySpillTo(writer)
	}
	b.memory = nil
	closeErr := b.close()
	if copyErr != nil {
		return errors.Join(fmt.Errorf("copying %d buffered records: %w", b.records, copyErr), closeErr)
	}
	return closeErr
}

// copySpillTo flushes, closes and reopens the spill file and copies its bytes into the
// stream. The file is reopened because createPrivateFile opens it for writing only.
func (b *dumpEntityBuffer) copySpillTo(writer *dumpWriter) error {
	if err := b.spillWriter.Flush(); err != nil {
		return fmt.Errorf("flushing %s: %w", b.path, err)
	}
	if err := b.file.Close(); err != nil {
		b.file = nil
		return fmt.Errorf("closing %s: %w", b.path, err)
	}
	b.file = nil
	b.spillWriter = nil

	reader, err := os.Open(b.path)
	if err != nil {
		return fmt.Errorf("reopening %s to copy it into the dump: %w", b.path, err)
	}
	copyErr := writer.writeBuffered(reader, b.records)
	closeErr := reader.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("closing %s after copying it: %w", b.path, closeErr)
	}
	return errors.Join(copyErr, closeErr)
}

// close drops the buffered records and removes the spill file. It is safe to call more
// than once and after copyTo, so a caller that does not copy an entity (an error, an
// interrupt) and then unwinds through a deferred close leaves no spill file behind.
func (b *dumpEntityBuffer) close() error {
	b.memory = nil
	b.spillWriter = nil
	var closeErr error
	if b.file != nil {
		if err := b.file.Close(); err != nil {
			closeErr = fmt.Errorf("closing the spill file %s: %w", b.path, err)
		}
		b.file = nil
	}
	if b.path == "" {
		return closeErr
	}
	path := b.path
	b.path = ""
	return errors.Join(closeErr, b.spiller.remove(path))
}

// writeBuffered appends the bytes of a buffered entity to the partial file and to the
// running checksum, and adds its records to the writer's count only once every byte is
// written. Section 3.3 forbids rewinding the stream or the checksum, so the copy is an
// ordinary covered write of the same bytes WriteRecord would have written. A failed
// writeCovered leaves the writer unusable, exactly as for WriteRecord.
func (w *dumpWriter) writeBuffered(source io.Reader, records int64) error {
	if w.completed || w.discarded || w.file == nil {
		return fmt.Errorf("writing a buffered dump entity after the writer was closed, aborted or failed")
	}
	chunk := make([]byte, dumpCopyChunkBytes)
	for {
		read, err := source.Read(chunk)
		if read > 0 {
			if writeErr := w.writeCovered(chunk[:read]); writeErr != nil {
				return fmt.Errorf("writing a buffered dump entity: %w", writeErr)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading a buffered dump entity back: %w", err)
		}
	}
	w.recordCount += records
	return nil
}
