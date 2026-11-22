package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// Segment format 2 (spec §3.1). Every segment created from this build on has a
// 24-byte header, plain or sealed, laid out like a v1 sealed header: magic,
// version 0x02 and purpose 0x04 unchanged, flags, a reserved zero, the epoch (0
// on a plain segment) and a random per-segment generation.
//
// The flags byte is what makes a build from before format 2 refuse a v2 log
// rather than truncate it: bytes 0 to 5 are the v1 sealed signature, so that
// build parses the header with sealed.ParseFileHeader, which refuses any flags
// other than 0x00 (spec §5). A new version or purpose byte would have made it
// read the header as a plain entry, fail the CRC and cut the segment to 0.
const (
	// walV2FlagFormat2 is set on every v2 segment.
	walV2FlagFormat2 byte = 0x01
	// walV2FlagSealed is set when the segment's entries are sealed.
	walV2FlagSealed byte = 0x02
)

// walGeneration draws a segment's generation. It is a variable so the tests
// that freeze v2 bytes (the plain fixture, the origin vectors) can fix it; a
// generation is drawn when a segment is born, which happens inside the open,
// before a test could replace the returned WAL's syscall table.
var walGeneration = randomWALGeneration

// randomWALGeneration returns 8 random bytes, never zero. The generation
// guards nothing reachable without recycling (owner D2), but a zero one would
// read as "no generation" to a later build that recycles. crypto/rand.Read
// cannot fail since Go 1.24 (it aborts the process instead), and go.mod is
// above that, so there is no failure to return.
func randomWALGeneration() uint64 {
	var drawn [8]byte
	for {
		_, _ = rand.Read(drawn[:])
		if gen := binary.BigEndian.Uint64(drawn[:]); gen != 0 {
			return gen
		}
	}
}

// walSegmentFormat is how one segment frames its entries.
type walSegmentFormat struct {
	// v2 is true for a segment with a format 2 header.
	v2 bool
	// sealed is true when the entries are sealed: every v1 segment with a
	// header, and a v2 one with walV2FlagSealed.
	sealed bool
	// header is the 24 header bytes, nil for a v1 plain segment. On a sealed
	// segment it is the first 24 bytes of every entry's AAD.
	header  []byte
	epochID uint64
	// gen is the v2 generation, covered by every entry's CRC; 0 on v1.
	gen uint64
}

// entryStart is the offset of a segment's first entry.
func (sf walSegmentFormat) entryStart() int64 {
	if sf.header == nil {
		return 0
	}
	return walFileHeaderLen
}

// framingLen is the clear framing in front of an entry's checksummed bytes:
// the length+CRC word, plus the clear sequence on a sealed segment.
func (sf walSegmentFormat) framingLen() int64 {
	if sf.sealed {
		return 8 + walSeqFieldLen
	}
	return 8
}

// checksum is the CRC32C an entry's word carries. A v1 entry's covers its
// body (plain) or its on-disk bytes (sealed). A v2 entry's also covers the
// generation, and on a sealed segment the clear sequence, so a stale entry from
// another generation or a torn sequence fails the CRC and reads as the end of
// the log, rather than passing it and failing the AEAD, which is reported as
// corruption (spec §3.2). seqField is nil on a plain segment, whose body
// carries the sequence.
func (sf walSegmentFormat) checksum(seqField, payload []byte) uint32 {
	if !sf.v2 {
		return crc32.Checksum(payload, crc32cTable)
	}
	var gen [8]byte
	binary.BigEndian.PutUint64(gen[:], sf.gen)
	sum := crc32.Update(0, crc32cTable, gen[:])
	sum = crc32.Update(sum, crc32cTable, seqField)
	return crc32.Update(sum, crc32cTable, payload)
}

// checksumVerified reports whether a stored checksum must match. Only a v1
// plain entry may carry none: a zero high half was the "no checksum" of the
// build before the checksum existed. In v2 a computed CRC of 0 is verified like
// any other.
func (sf walSegmentFormat) checksumVerified(stored uint32) bool {
	return sf.v2 || sf.sealed || stored != 0
}

// frameWord is the 8-byte word in front of an entry: the length in the low
// half, the checksum in the high half.
func frameWord(length int, checksum uint32) []byte {
	return binary.BigEndian.AppendUint64(make([]byte, 0, 8), uint64(uint32(length))|uint64(checksum)<<32)
}

// newWALv2Format is the format of a segment about to be born in the given key
// mode, with its header and a fresh generation. epochID is 0 for a plain
// segment.
func newWALv2Format(sealedEntries bool, epochID uint64) walSegmentFormat {
	flags := walV2FlagFormat2
	if sealedEntries {
		flags |= walV2FlagSealed
	}
	gen := walGeneration()
	header := make([]byte, 0, walFileHeaderLen)
	header = binary.BigEndian.AppendUint32(header, sealed.Magic)
	header = append(header, sealed.FileVersion, sealed.PurposeWALEntry, flags, 0x00)
	header = binary.BigEndian.AppendUint64(header, epochID)
	header = binary.BigEndian.AppendUint64(header, gen)
	return walSegmentFormat{v2: true, sealed: sealedEntries, header: header, epochID: epochID, gen: gen}
}

// parseWALv2Header checks a v2 header. It is the WAL's own parser:
// sealed.ParseFileHeader stays strict, because its refusal of non-zero flags is
// what stops an old build (spec §3.1).
func parseWALv2Header(header []byte) (walSegmentFormat, error) {
	if len(header) < walFileHeaderLen {
		return walSegmentFormat{}, fmt.Errorf("a segment format 2 header is %d bytes, this has %d", walFileHeaderLen, len(header))
	}
	if magic := binary.BigEndian.Uint32(header[0:4]); magic != sealed.Magic {
		return walSegmentFormat{}, fmt.Errorf("bad magic %08x at offset 0", magic)
	}
	if header[4] != sealed.FileVersion || header[5] != sealed.PurposeWALEntry {
		return walSegmentFormat{}, fmt.Errorf("version %02x and purpose %02x at offsets 4 and 5, want %02x and %02x",
			header[4], header[5], sealed.FileVersion, sealed.PurposeWALEntry)
	}
	flags := header[6]
	if flags&walV2FlagFormat2 == 0 || flags&^(walV2FlagFormat2|walV2FlagSealed) != 0 {
		return walSegmentFormat{}, fmt.Errorf("flags %02x at offset 6 are not segment format 2 (01, or 03 when sealed)", flags)
	}
	if header[7] != 0 {
		return walSegmentFormat{}, fmt.Errorf("reserved byte %02x at offset 7 must be 00", header[7])
	}
	gen := binary.BigEndian.Uint64(header[16:24])
	if gen == 0 {
		return walSegmentFormat{}, fmt.Errorf("the generation at offset 16 is zero")
	}
	return walSegmentFormat{
		v2:      true,
		sealed:  flags&walV2FlagSealed != 0,
		header:  append([]byte(nil), header[:walFileHeaderLen]...),
		epochID: binary.BigEndian.Uint64(header[8:16]),
		gen:     gen,
	}, nil
}

// readSegmentFormat decides how the segment in f, of the given size, frames its
// entries, from its first bytes and never from the key source: no header is v1
// plain; a header that sealed.ParseFileHeader accepts is v1 sealed; flag 0x01
// is v2. A header that is neither is an error naming the segment, never a
// guess.
func readSegmentFormat(f io.ReaderAt, size int64, name string) (walSegmentFormat, error) {
	if size < walFileHeaderLen {
		return walSegmentFormat{}, nil
	}
	header := make([]byte, walFileHeaderLen)
	if _, err := f.ReadAt(header, 0); err != nil {
		return walSegmentFormat{}, fmt.Errorf("reading the header of WAL segment %s: %w", name, err)
	}
	if !isFileSealedWAL(header) {
		return walSegmentFormat{}, nil
	}
	if header[6]&walV2FlagFormat2 != 0 {
		format, err := parseWALv2Header(header)
		if err != nil {
			return walSegmentFormat{}, fmt.Errorf("WAL segment %s: parsing its segment format 2 header: %w", name, err)
		}
		return format, nil
	}
	parsed, err := sealed.ParseFileHeader(header)
	if err != nil {
		return walSegmentFormat{}, fmt.Errorf("WAL segment %s: %w", name, err)
	}
	return walSegmentFormat{sealed: true, header: header, epochID: parsed.EpochID}, nil
}

// IsSealedWALSegmentHeader reports whether header, the first bytes of a WAL
// segment file, says the segment's entries are sealed: a v1 sealed header, or a
// v2 one with the sealed flag. A plain v2 segment has the same 24-byte layout
// and is not sealed; a v1 plain segment has no header. The server's
// sealed-material check asks this rather than sealed.ParseFileHeader, which
// refuses every v2 header (spec §5).
func IsSealedWALSegmentHeader(header []byte) bool {
	format, err := readSegmentFormat(bytes.NewReader(header), int64(len(header)), "")
	return err == nil && format.sealed
}

// IsPlainWALSegmentHeader reports whether header, the first bytes of a WAL
// segment file, is a segment format 2 header without the sealed flag: a segment
// whose entries are written in the clear. A v1 plain segment has no header and
// a file shorter than a header says nothing, so neither is reported; the ring
// log, which never wrote v1, is judged by this.
func IsPlainWALSegmentHeader(header []byte) bool {
	format, err := readSegmentFormat(bytes.NewReader(header), int64(len(header)), "")
	return err == nil && format.v2 && !format.sealed
}
