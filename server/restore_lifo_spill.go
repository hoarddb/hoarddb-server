package server

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
)

// A LIFO restore receives its records newest first, the order the bucket pops
// (store/delivery_dump.go:QueueStore.IterateDump), and must push them oldest
// first to pop the same way again, so the first push is the stream's last
// record. Holding the stream in memory until RESTORE_DONE let a large bucket
// exhaust the node's memory (BUG-0674). The records go to a spill file in the
// data directory instead, framed so it reads from its end, and the finish
// pushes them back in reverse. Memory is one write buffer and one record.
//
// Each record is sealed with AES-256-GCM under a key made for the spill and
// held only in memory: a node with encryption at rest never writes a document
// in plaintext, and a spill a crash leaves behind cannot be read by anyone.

// lifoSpillPattern names a spill file in the data directory. No database or
// bucket name reaches the filesystem, and the leading dot keeps it apart from
// the node's own directories (server/blob_dir.go:nodeOwnedDirNames).
const lifoSpillPattern = ".restore-lifo-*.spill"

// lifoSpillWriteBufferBytes is the spill's write buffer: what a LIFO restore
// holds in memory between disk writes, whatever the size of the bucket.
const lifoSpillWriteBufferBytes = 64 << 10

// lifoSpillTrailerBytes is the length written after each sealed record, which
// is what lets the file be read from its end.
const lifoSpillTrailerBytes = 4

// lifoSpill is one LIFO restore's records on disk, in arrival order.
type lifoSpill struct {
	file    *os.File
	writer  *bufio.Writer
	sealer  cipher.AEAD
	records uint64
	// size is the bytes appended, buffered ones included.
	size int64
	// peakResident is the most memory, in bytes, the spill has held at once:
	// the write buffer and the record being sealed, or the record being read.
	peakResident int64
}

// newLIFOSpill creates an empty spill file in dataDir under a fresh key.
func newLIFOSpill(dataDir string) (*lifoSpill, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("making the LIFO restore's spill key: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("making the LIFO restore's spill cipher: %w", err)
	}
	sealer, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("making the LIFO restore's spill cipher: %w", err)
	}
	file, err := os.CreateTemp(dataDir, lifoSpillPattern)
	if err != nil {
		return nil, fmt.Errorf("creating the LIFO restore's spill file in %s: %w", dataDir, err)
	}
	return &lifoSpill{file: file, writer: bufio.NewWriterSize(file, lifoSpillWriteBufferBytes), sealer: sealer}, nil
}

// nonce is record index's nonce, never stored: the key is never used for
// another file, so an index is never sealed twice under it.
func (spill *lifoSpill) nonce(index uint64) []byte {
	nonce := make([]byte, spill.sealer.NonceSize())
	binary.BigEndian.PutUint64(nonce[len(nonce)-8:], index)
	return nonce
}

// append seals record and writes it, then its sealed length.
func (spill *lifoSpill) append(record []byte) error {
	sealed := spill.sealer.Seal(nil, spill.nonce(spill.records), record, nil)
	if len(sealed) > math.MaxUint32 {
		return fmt.Errorf("a LIFO record of %d bytes is too large to spill", len(record))
	}
	spill.notePeak(int64(spill.writer.Size() + len(sealed)))
	var trailer [lifoSpillTrailerBytes]byte
	binary.LittleEndian.PutUint32(trailer[:], uint32(len(sealed)))
	if _, err := spill.writer.Write(sealed); err != nil {
		return fmt.Errorf("writing LIFO record %d to %s: %w", spill.records, spill.file.Name(), err)
	}
	if _, err := spill.writer.Write(trailer[:]); err != nil {
		return fmt.Errorf("writing LIFO record %d to %s: %w", spill.records, spill.file.Name(), err)
	}
	spill.records++
	spill.size += int64(len(sealed) + lifoSpillTrailerBytes)
	return nil
}

func (spill *lifoSpill) notePeak(resident int64) {
	spill.peakResident = max(spill.peakResident, resident)
}

// replay hands push every record, newest appended first. index is the
// record's position in arrival order, counting from 0.
func (spill *lifoSpill) replay(push func(index uint64, record []byte) error) error {
	if err := spill.writer.Flush(); err != nil {
		return fmt.Errorf("flushing %s: %w", spill.file.Name(), err)
	}
	var trailer [lifoSpillTrailerBytes]byte
	var sealed []byte
	offset := spill.size
	for index := spill.records; index > 0; {
		index--
		if offset < lifoSpillTrailerBytes {
			return fmt.Errorf("%s ends before LIFO record %d: the spill file was changed under the restore", spill.file.Name(), index)
		}
		if _, err := spill.file.ReadAt(trailer[:], offset-lifoSpillTrailerBytes); err != nil {
			return fmt.Errorf("reading LIFO record %d from %s: %w", index, spill.file.Name(), err)
		}
		sealedLen := int64(binary.LittleEndian.Uint32(trailer[:]))
		start := offset - lifoSpillTrailerBytes - sealedLen
		if start < 0 {
			return fmt.Errorf("%s names a %d-byte LIFO record %d where only %d bytes precede it: the spill file was changed under the restore",
				spill.file.Name(), sealedLen, index, offset-lifoSpillTrailerBytes)
		}
		if int64(cap(sealed)) < sealedLen {
			sealed = make([]byte, sealedLen)
		}
		sealed = sealed[:sealedLen]
		if _, err := spill.file.ReadAt(sealed, start); err != nil {
			return fmt.Errorf("reading LIFO record %d from %s: %w", index, spill.file.Name(), err)
		}
		record, err := spill.sealer.Open(sealed[:0], spill.nonce(index), sealed, nil)
		if err != nil {
			return fmt.Errorf("opening LIFO record %d from %s: the spill file was changed under the restore: %w", index, spill.file.Name(), err)
		}
		spill.notePeak(int64(cap(sealed)))
		if err := push(index, record); err != nil {
			return err
		}
		offset = start
	}
	return nil
}

// discard closes the spill file and removes it.
func (spill *lifoSpill) discard() error {
	closeErr := spill.file.Close()
	removeErr := os.Remove(spill.file.Name())
	if removeErr != nil {
		return fmt.Errorf("removing the LIFO restore's spill file %s: %w", spill.file.Name(), removeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("closing the LIFO restore's spill file %s: %w", spill.file.Name(), closeErr)
	}
	return nil
}

// removeLeftoverLIFOSpills removes the spill files of LIFO restores a stopped
// node never finished. Their key died with the process, so nothing in them can
// be read or resumed.
func removeLeftoverLIFOSpills(dataDir string, logger *slog.Logger) error {
	leftovers, err := filepath.Glob(filepath.Join(dataDir, lifoSpillPattern))
	if err != nil {
		return fmt.Errorf("listing leftover LIFO restore spill files in %s: %w", dataDir, err)
	}
	for _, path := range leftovers {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("removing %s, the spill file of a LIFO restore the node stopped during: %w. Remove it by hand: rm %s", path, err, path)
		}
		logger.Warn("removed the spill file of a LIFO restore that did not finish before the node stopped; its bucket holds none or only part of that dump, so run the restore again with --drop",
			"path", path)
	}
	return nil
}
