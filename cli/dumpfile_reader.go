package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// dumpRecordMinBytes is the shortest BSON document: a length prefix and the terminator.
// A record shorter than this cannot hold its t member, so a length prefix under it is
// damage and is refused before a buffer is grown for it.
const dumpRecordMinBytes = 5

// errBadDumpMagic is under the refusal of a file that is not a dump of this layout
// generation. The text is the one section 3.1 fixes, because an operator who picked the
// wrong file needs the fix, not the comparison.
var errBadDumpMagic = errors.New("This is not a hoarddb dump: it does not start with HDBDUMP1. Check the file name, or take a new dump with 'hoarddb dump'.") //nolint:staticcheck // ST1005: the refusal text is fixed by section 3.1 of the build spec

// errBadRecordLength is under the refusal of a record whose length prefix is outside the
// range the format allows. It is decided before any buffer for the record is allocated,
// so a damaged prefix cannot reserve two gigabytes, and it carries the record's offset.
var errBadRecordLength = errors.New("a dump record declares a length outside the range a record may hold")

// errRecordHasNoKind is under the refusal of a record whose first member is not its kind
// word. A record that cannot name its kind is not one this version can count or decode,
// so the caller stops rather than guessing a kind for it.
var errRecordHasNoKind = errors.New("a dump record's first member is not the string \"t\"")

// dumpReader streams a dump file's records from an io.Reader
// (the ring cli parity build spec section 3.1). It holds one record at a time and
// one buffer that grows to the largest record it has read, plus the sha256 state of about
// 100 bytes, so its memory does not grow with the file.
//
// The record a call to Next returns is a slice of that buffer and is valid only until the
// next call to Next: the next record overwrites it. A caller that must keep a record
// copies it.
type dumpReader struct {
	source    io.Reader
	digest    hash.Hash
	buffer    []byte
	offset    int64
	magicRead bool
}

// newDumpReader returns a reader of the dump records in source. It reads nothing yet: the
// first Next reads and checks the magic, so a caller can build a reader and refuse the
// file with the error that call returns.
func newDumpReader(source io.Reader) *dumpReader {
	return &dumpReader{source: source, digest: sha256.New()}
}

// Next returns the next record's kind word and its bytes, one record at a time. It reads
// the magic before the first record and checks it; a stream that does not start with the
// magic gives errBadDumpMagic, and a stream that ends inside the magic gives an error
// wrapping io.ErrUnexpectedEOF.
//
// A length prefix under dumpRecordMinBytes or over maxDumpRecordBytes gives
// errBadRecordLength before a buffer for that record is allocated. A stream that ends
// exactly at a record boundary gives io.EOF; one that ends inside a length prefix or
// inside a record gives an error wrapping io.ErrUnexpectedEOF, so a caller can tell a
// cut-off file from a clean end.
//
// Next does not decode the record: the caller runs decodeRecord on the bytes, so that it
// can remember a kind it does not know and read on to the end of the file, the way
// section 3.1 requires. The returned bytes are valid only until the next call to Next.
func (r *dumpReader) Next() (string, bson.Raw, error) {
	if !r.magicRead {
		if err := r.readMagic(); err != nil {
			return "", nil, err
		}
	}

	recordOffset := r.offset
	var prefix [4]byte
	prefixRead, readErr := io.ReadFull(r.source, prefix[:])
	if prefixRead > 0 {
		if err := r.consume(prefix[:prefixRead]); err != nil {
			return "", nil, err
		}
	}
	if readErr != nil {
		if errors.Is(readErr, io.EOF) {
			// Zero bytes of a length prefix: the file ends at a record boundary.
			return "", nil, io.EOF
		}
		return "", nil, fmt.Errorf("a dump record at offset %d has only %d of the 4 bytes of its length prefix and then the file ends: %w", recordOffset, prefixRead, io.ErrUnexpectedEOF)
	}

	length := int(int32(binary.LittleEndian.Uint32(prefix[:])))
	if length < dumpRecordMinBytes || length > maxDumpRecordBytes {
		return "", nil, fmt.Errorf("%w: a dump record at offset %d declares %d bytes and a dump record holds between %d and %d", errBadRecordLength, recordOffset, length, dumpRecordMinBytes, maxDumpRecordBytes)
	}
	if cap(r.buffer) < length {
		r.buffer = make([]byte, length)
	}
	r.buffer = r.buffer[:length]
	copy(r.buffer, prefix[:])
	bodyRead, bodyErr := io.ReadFull(r.source, r.buffer[4:])
	if bodyErr != nil {
		return "", nil, fmt.Errorf("a dump record at offset %d declares %d bytes and the file ends after %d of them: %w", recordOffset, length, prefixRead+bodyRead, io.ErrUnexpectedEOF)
	}
	if err := r.consume(r.buffer[4:]); err != nil {
		return "", nil, err
	}

	record := bson.Raw(r.buffer)
	kind, err := readRecordKind(record)
	if err != nil {
		return "", nil, fmt.Errorf("the dump record at offset %d cannot be read: %w", recordOffset, err)
	}
	return kind, record, nil
}

// Offset returns the number of bytes this reader has consumed, counting the magic from
// its first byte. It is where the record a later Next returns starts.
func (r *dumpReader) Offset() int64 {
	return r.offset
}

// Sum returns the sha256 of the bytes this reader has consumed so far. The verifier takes
// it right after the summary record, where it must equal the footer's sha256 (section
// 3.1: the checksum covers the magic through the end of the summary).
func (r *dumpReader) Sum() [32]byte {
	var sum [32]byte
	copy(sum[:], r.digest.Sum(nil))
	return sum
}

// readMagic reads and checks the magic. A stream shorter than the magic is a cut-off
// file, not a wrong one, so it carries io.ErrUnexpectedEOF rather than errBadDumpMagic:
// the verifier reports a cut-off file with its own text.
func (r *dumpReader) readMagic() error {
	var magic [len(dumpMagic)]byte
	magicRead, readErr := io.ReadFull(r.source, magic[:])
	if magicRead > 0 {
		if err := r.consume(magic[:magicRead]); err != nil {
			return err
		}
	}
	if readErr != nil {
		return fmt.Errorf("a dump file holds %d bytes and its magic is %d bytes long: %w", magicRead, len(dumpMagic), io.ErrUnexpectedEOF)
	}
	if string(magic[:]) != dumpMagic {
		return fmt.Errorf("reading a dump file: %w", errBadDumpMagic)
	}
	r.magicRead = true
	return nil
}

// consume adds chunk to the checksum and to the offset, so the checksum covers exactly
// the bytes the reader has taken from the stream.
func (r *dumpReader) consume(chunk []byte) error {
	if _, err := r.digest.Write(chunk); err != nil {
		return fmt.Errorf("adding %d bytes to the dump checksum: %w", len(chunk), err)
	}
	r.offset += int64(len(chunk))
	return nil
}

// readRecordKind returns the t member of a record, read by hand from the bytes because
// Next runs before the strict decode and must not depend on the record being decodable.
// The first member must be a string named t: anything else is damage, not a record of an
// unknown kind, and the caller reports it as such.
func readRecordKind(record bson.Raw) (string, error) {
	position := 4
	if len(record) <= position {
		return "", fmt.Errorf("%w: the record is %d bytes and holds no members", errRecordHasNoKind, len(record))
	}
	if record[position] != 0x02 { // the BSON type of a string
		return "", fmt.Errorf("%w: its first member is of BSON type 0x%02x", errRecordHasNoKind, record[position])
	}
	position++
	keyEnd := bytes.IndexByte(record[position:], 0x00)
	if keyEnd < 0 {
		return "", fmt.Errorf("%w: its first member's key is not terminated", errRecordHasNoKind)
	}
	if key := string(record[position : position+keyEnd]); key != dumpTypeMember {
		return "", fmt.Errorf("%w: its first member is named %q", errRecordHasNoKind, key)
	}
	position += keyEnd + 1
	if len(record)-position < 4 {
		return "", fmt.Errorf("%w: its first member ends before its string length", errRecordHasNoKind)
	}
	stringBytes := int(int32(binary.LittleEndian.Uint32(record[position:])))
	position += 4
	if stringBytes < 1 || len(record)-position < stringBytes {
		return "", fmt.Errorf("%w: its first member's string holds %d bytes in a record with %d left", errRecordHasNoKind, stringBytes, len(record)-position)
	}
	word := record[position : position+stringBytes]
	if word[len(word)-1] != 0x00 {
		return "", fmt.Errorf("%w: its first member's string is not terminated", errRecordHasNoKind)
	}
	return string(word[:len(word)-1]), nil
}
