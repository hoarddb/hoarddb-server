package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// The seed channel codec of conventions section 6.1 (kinds 0x20 to 0x27).
// Every message is one frame payload: the 30-byte envelope, then the body.
// There is no SEED_DEDUPE (0x28): a dedupe entry is an ordinary key of the
// pair's range, so a snapshot already carries it.

const (
	// Kinds of the seed channel. 0x28 is deliberately absent.
	KindSeedRegister byte = 0x20 // KindSeedRegister is SEED_REGISTER.
	KindSeedAccept   byte = 0x21 // KindSeedAccept is SEED_ACCEPT.
	KindSeedRedirect byte = 0x22 // KindSeedRedirect is SEED_REDIRECT.
	KindSeedRefused  byte = 0x23 // KindSeedRefused is SEED_REFUSED.
	KindSeedBusy     byte = 0x24 // KindSeedBusy is SEED_BUSY.
	KindSeedChunk    byte = 0x25 // KindSeedChunk is SEED_CHUNK.
	KindSeedAck      byte = 0x26 // KindSeedAck is SEED_ACK.
	KindSeedRestart  byte = 0x27 // KindSeedRestart is SEED_RESTART.
)

const (
	seedIDLen       = 16
	seedPositionLen = 16
	// seedMaxKeyBytes mirrors storage.MaxKeyBytes; a test fails if the two
	// drift apart. A chunk entry never carries a longer key.
	seedMaxKeyBytes = 65000
	// seedMinEntryBytes is a one-byte key length, a one-byte key, a one-byte
	// value length: the least an entry can occupy, used to bound entryCount
	// before anything is allocated.
	seedMinEntryBytes = 3
	// seedMaxChunkEntries bounds the entries of one chunk. The entry slice costs 32 bytes per
	// entry, and a 32 MiB frame of 3-byte entries would otherwise make the decoder allocate
	// about 358 MB before reading one. It is far above any seed_chunk_entries (default 1024).
	seedMaxChunkEntries = 1 << 16
	// seedMaxTracked bounds the seed_ids a ChunkSequence or CreditWindow
	// remembers, so a peer cannot grow them without limit.
	seedMaxTracked = 1024
)

// Reasons of SEED_REFUSED and SEED_RESTART (conventions section 6.1).
const (
	seedRefusedReasonMax byte = 5
	seedRestartReasonMax byte = 4
)

// ErrSeedMalformed is the refusal for a seed message that cannot be decoded:
// short, trailing bytes, a length past its frame, a bad CRC. Its text is the
// INVALID_ARGUMENT code of the cluster package, which this package cannot
// import; the cluster boundary codes it. It is never relayed to a client.
var ErrSeedMalformed = errors.New("INVALID_ARGUMENT")

// ErrSeedChunkGap is the refusal for a SEED_CHUNK whose index is not the next
// one of its seed_id. The pull answers it by restarting the attempt on a fresh
// cut. Its text is the same INVALID_ARGUMENT code as ErrSeedMalformed.
var ErrSeedChunkGap = errors.New("INVALID_ARGUMENT")

// ErrSeedAckRegressed is the refusal for a SEED_ACK whose applied position is
// below the one already acknowledged for its seed_id. It is a peer violation
// and only that. Its text is the same INVALID_ARGUMENT code as ErrSeedMalformed.
var ErrSeedAckRegressed = errors.New("INVALID_ARGUMENT")

// ErrSeedUnknown is the refusal for a SEED_ACK whose seed_id was never opened
// by a SEED_ACCEPT on this window, or was forgotten since. The peer may have
// raced a finished seed, so it is not a regression. Its text is the same
// INVALID_ARGUMENT code as ErrSeedMalformed.
var ErrSeedUnknown = errors.New("INVALID_ARGUMENT")

// ErrSeedLimit is the refusal for a new seed_id once this node already tracks
// seedMaxTracked seeds. It is a local limit on this node's memory, not a peer
// violation: the peer did nothing wrong, and retrying after a finished seed is
// forgotten or restarted succeeds. Its text is the same INVALID_ARGUMENT code
// as ErrSeedMalformed.
var ErrSeedLimit = errors.New("INVALID_ARGUMENT")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// SeedPosition is a log position, [epoch:8][seq:8] on the wire.
type SeedPosition struct{ Epoch, Seq uint64 }

// before reports whether p orders strictly below other.
func (p SeedPosition) before(other SeedPosition) bool {
	return p.Epoch < other.Epoch || (p.Epoch == other.Epoch && p.Seq < other.Seq)
}

// SeedRegister is SEED_REGISTER. Mask bits 1 to 7 must be zero, but a set bit
// is the source's SEED_REFUSED reason 5, so the decoder passes the byte on.
type SeedRegister struct {
	Header     Envelope
	SeedID     [seedIDLen]byte
	MyPosition SeedPosition
	Mask       byte
	Cursor     []byte
}

// SeedAccept is SEED_ACCEPT. WindowBytes is the initial credit.
type SeedAccept struct {
	Header      Envelope
	SeedID      [seedIDLen]byte
	Cut         SeedPosition
	WindowBytes uint32
	Resume      bool
}

// SeedRedirect is SEED_REDIRECT to another source.
type SeedRedirect struct {
	Header Envelope
	SeedID [seedIDLen]byte
	Source string
}

// SeedRefused is SEED_REFUSED; Reason is 1 disk, 2 database dropped, 3 not a
// holder, 4 not current, 5 mask conflict.
type SeedRefused struct {
	Header Envelope
	SeedID [seedIDLen]byte
	Reason byte
	Detail string
}

// SeedBusy is SEED_BUSY.
type SeedBusy struct {
	Header       Envelope
	SeedID       [seedIDLen]byte
	RetryAfterMs uint32
}

// SeedEntry is one key and value of a chunk. After a decode both alias the
// payload that was decoded, so the caller must not reuse that buffer.
type SeedEntry struct{ Key, Value []byte }

// SeedChunk is SEED_CHUNK. CRC is CRC32C over the encoded entries only. The
// encoder computes it and ignores the field; the decoder verifies it and fills
// it in. Final is the envelope's flag bit 1: a stream that ends without it is
// incomplete.
type SeedChunk struct {
	Header  Envelope
	SeedID  [seedIDLen]byte
	Index   uint64
	Entries []SeedEntry
	CRC     uint32
	Final   bool
}

// SeedAck is SEED_ACK. CreditBytes is the receiver's absolute current free
// window, never a delta and never a running total, so a lost ack costs nothing:
// the next one restates the window.
type SeedAck struct {
	Header      Envelope
	SeedID      [seedIDLen]byte
	Applied     SeedPosition
	CreditBytes uint32
}

// SeedRestart is SEED_RESTART; Reason is 1 pin gone, 2 log floor passed, 3
// epoch changed, 4 range reset.
type SeedRestart struct {
	Header Envelope
	SeedID [seedIDLen]byte
	Reason byte
}

// seedStart returns the envelope for a message of the given kind with room for
// bodyLen more bytes. The caller's Header supplies epoch, state_seq, slot and
// database; version and kind are the codec's.
func seedStart(header Envelope, kind byte, flags uint16, bodyLen int) []byte {
	header.Version = EnvelopeVersion
	header.Kind = kind
	header.Flags = flags
	return append(make([]byte, 0, EnvelopeSize+bodyLen), EncodeEnvelope(header)...)
}

// seedOpen decodes the envelope of a seed payload and returns the body. The
// kind must be wanted and a seed-channel kind.
func seedOpen(payload []byte, wanted byte) (Envelope, []byte, error) {
	if len(payload) > MaxFrameLength {
		return Envelope{}, nil, fmt.Errorf("%w: seed message of %d bytes exceeds the %d byte frame ceiling", ErrSeedMalformed, len(payload), MaxFrameLength)
	}
	header, err := DecodeEnvelope(payload)
	if err != nil {
		return Envelope{}, nil, err
	}
	if err := CheckKind(header.Kind, ChannelSeed); err != nil {
		return Envelope{}, nil, err
	}
	if header.Kind != wanted {
		return Envelope{}, nil, fmt.Errorf("%w: expected seed kind 0x%02x, got 0x%02x", ErrSeedMalformed, wanted, header.Kind)
	}
	return header, payload[EnvelopeSize:], nil
}

// seedReader walks a body; the first short read sticks as err.
type seedReader struct {
	rest []byte
	name string
	err  error
}

func (r *seedReader) take(width int, field string) []byte {
	if r.err != nil {
		return nil
	}
	if len(r.rest) < width {
		r.err = fmt.Errorf("%w: %s needs %d bytes for %s, %d left", ErrSeedMalformed, r.name, width, field, len(r.rest))
		return nil
	}
	taken := r.rest[:width:width]
	r.rest = r.rest[width:]
	return taken
}

func (r *seedReader) seedID() (id [seedIDLen]byte) {
	copy(id[:], r.take(seedIDLen, "seed id"))
	return id
}

func (r *seedReader) position(field string) (pos SeedPosition) {
	raw := r.take(seedPositionLen, field)
	if raw == nil {
		return pos
	}
	pos.Epoch = binary.BigEndian.Uint64(raw[0:8])
	pos.Seq = binary.BigEndian.Uint64(raw[8:16])
	return pos
}

func (r *seedReader) byteField(field string) byte {
	if raw := r.take(1, field); raw != nil {
		return raw[0]
	}
	return 0
}

func (r *seedReader) u16(field string) int {
	if raw := r.take(2, field); raw != nil {
		return int(binary.BigEndian.Uint16(raw))
	}
	return 0
}

func (r *seedReader) u32(field string) uint32 {
	if raw := r.take(4, field); raw != nil {
		return binary.BigEndian.Uint32(raw)
	}
	return 0
}

func (r *seedReader) u64(field string) uint64 {
	if raw := r.take(8, field); raw != nil {
		return binary.BigEndian.Uint64(raw)
	}
	return 0
}

// finish refuses trailing bytes.
func (r *seedReader) finish() error {
	if r.err != nil {
		return r.err
	}
	if len(r.rest) != 0 {
		return fmt.Errorf("%w: %s has %d trailing bytes", ErrSeedMalformed, r.name, len(r.rest))
	}
	return nil
}

// noFlags refuses an envelope flag on a message that defines none.
func noFlags(header Envelope, name string) error {
	if header.Flags != 0 {
		return fmt.Errorf("%w: %s sets envelope flags 0x%04x, it defines none", ErrSeedMalformed, name, header.Flags)
	}
	return nil
}

func appendSeedPosition(buf []byte, pos SeedPosition) []byte {
	buf = binary.BigEndian.AppendUint64(buf, pos.Epoch)
	return binary.BigEndian.AppendUint64(buf, pos.Seq)
}

// EncodeSeedRegister returns the SEED_REGISTER payload. It refuses a cursor
// longer than a key can be.
func EncodeSeedRegister(msg SeedRegister) ([]byte, error) {
	if len(msg.Cursor) > seedMaxKeyBytes {
		return nil, fmt.Errorf("%w: SEED_REGISTER cursor is %d bytes, a key is at most %d", ErrSeedMalformed, len(msg.Cursor), seedMaxKeyBytes)
	}
	buf := seedStart(msg.Header, KindSeedRegister, 0, seedIDLen+seedPositionLen+3+len(msg.Cursor))
	buf = append(buf, msg.SeedID[:]...)
	buf = appendSeedPosition(buf, msg.MyPosition)
	buf = append(buf, msg.Mask)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(msg.Cursor)))
	return append(buf, msg.Cursor...), nil
}

// DecodeSeedRegister reads a SEED_REGISTER payload.
func DecodeSeedRegister(payload []byte) (SeedRegister, error) {
	header, body, err := seedOpen(payload, KindSeedRegister)
	if err != nil {
		return SeedRegister{}, err
	}
	if err := noFlags(header, "SEED_REGISTER"); err != nil {
		return SeedRegister{}, err
	}
	reader := &seedReader{rest: body, name: "SEED_REGISTER"}
	msg := SeedRegister{Header: header}
	msg.SeedID = reader.seedID()
	msg.MyPosition = reader.position("myPosition")
	msg.Mask = reader.byteField("mask")
	cursorLen := reader.u16("cursorLen")
	if cursorLen > seedMaxKeyBytes {
		return SeedRegister{}, fmt.Errorf("%w: SEED_REGISTER cursor is %d bytes, a key is at most %d", ErrSeedMalformed, cursorLen, seedMaxKeyBytes)
	}
	msg.Cursor = reader.take(cursorLen, "cursor")
	if err := reader.finish(); err != nil {
		return SeedRegister{}, err
	}
	return msg, nil
}

// EncodeSeedAccept returns the SEED_ACCEPT payload.
func EncodeSeedAccept(msg SeedAccept) []byte {
	buf := seedStart(msg.Header, KindSeedAccept, 0, seedIDLen+seedPositionLen+5)
	buf = append(buf, msg.SeedID[:]...)
	buf = appendSeedPosition(buf, msg.Cut)
	buf = binary.BigEndian.AppendUint32(buf, msg.WindowBytes)
	if msg.Resume {
		return append(buf, 1)
	}
	return append(buf, 0)
}

// DecodeSeedAccept reads a SEED_ACCEPT payload; resume must be 0 or 1.
func DecodeSeedAccept(payload []byte) (SeedAccept, error) {
	header, body, err := seedOpen(payload, KindSeedAccept)
	if err != nil {
		return SeedAccept{}, err
	}
	if err := noFlags(header, "SEED_ACCEPT"); err != nil {
		return SeedAccept{}, err
	}
	reader := &seedReader{rest: body, name: "SEED_ACCEPT"}
	msg := SeedAccept{Header: header}
	msg.SeedID = reader.seedID()
	msg.Cut = reader.position("cutPosition")
	msg.WindowBytes = reader.u32("windowBytes")
	resume := reader.byteField("resume")
	if err := reader.finish(); err != nil {
		return SeedAccept{}, err
	}
	if resume > 1 {
		return SeedAccept{}, fmt.Errorf("%w: SEED_ACCEPT resume byte is %d, it is 0 or 1", ErrSeedMalformed, resume)
	}
	msg.Resume = resume == 1
	return msg, nil
}

// EncodeSeedRedirect returns the SEED_REDIRECT payload. The source is a node
// id of 1 to 255 bytes.
func EncodeSeedRedirect(msg SeedRedirect) ([]byte, error) {
	if len(msg.Source) == 0 || len(msg.Source) > 255 {
		return nil, fmt.Errorf("%w: SEED_REDIRECT source is %d bytes, a node id is 1 to 255", ErrSeedMalformed, len(msg.Source))
	}
	buf := seedStart(msg.Header, KindSeedRedirect, 0, seedIDLen+1+len(msg.Source))
	buf = append(buf, msg.SeedID[:]...)
	buf = append(buf, byte(len(msg.Source)))
	return append(buf, msg.Source...), nil
}

// DecodeSeedRedirect reads a SEED_REDIRECT payload.
func DecodeSeedRedirect(payload []byte) (SeedRedirect, error) {
	header, body, err := seedOpen(payload, KindSeedRedirect)
	if err != nil {
		return SeedRedirect{}, err
	}
	if err := noFlags(header, "SEED_REDIRECT"); err != nil {
		return SeedRedirect{}, err
	}
	reader := &seedReader{rest: body, name: "SEED_REDIRECT"}
	msg := SeedRedirect{Header: header}
	msg.SeedID = reader.seedID()
	sourceLen := int(reader.byteField("sourceLen"))
	source := reader.take(sourceLen, "source")
	if err := reader.finish(); err != nil {
		return SeedRedirect{}, err
	}
	if sourceLen == 0 {
		return SeedRedirect{}, fmt.Errorf("%w: SEED_REDIRECT names no source", ErrSeedMalformed)
	}
	msg.Source = string(source)
	return msg, nil
}

// EncodeSeedRefused returns the SEED_REFUSED payload. The reason is 1 to 5.
func EncodeSeedRefused(msg SeedRefused) ([]byte, error) {
	if msg.Reason == 0 || msg.Reason > seedRefusedReasonMax {
		return nil, fmt.Errorf("%w: SEED_REFUSED reason %d, it is 1 to %d", ErrSeedMalformed, msg.Reason, seedRefusedReasonMax)
	}
	if len(msg.Detail) > 0xFFFF {
		return nil, fmt.Errorf("%w: SEED_REFUSED detail is %d bytes, at most 65535", ErrSeedMalformed, len(msg.Detail))
	}
	buf := seedStart(msg.Header, KindSeedRefused, 0, seedIDLen+3+len(msg.Detail))
	buf = append(buf, msg.SeedID[:]...)
	buf = append(buf, msg.Reason)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(msg.Detail)))
	return append(buf, msg.Detail...), nil
}

// DecodeSeedRefused reads a SEED_REFUSED payload.
func DecodeSeedRefused(payload []byte) (SeedRefused, error) {
	header, body, err := seedOpen(payload, KindSeedRefused)
	if err != nil {
		return SeedRefused{}, err
	}
	if err := noFlags(header, "SEED_REFUSED"); err != nil {
		return SeedRefused{}, err
	}
	reader := &seedReader{rest: body, name: "SEED_REFUSED"}
	msg := SeedRefused{Header: header}
	msg.SeedID = reader.seedID()
	msg.Reason = reader.byteField("reason")
	detail := reader.take(reader.u16("detailLen"), "detail")
	if err := reader.finish(); err != nil {
		return SeedRefused{}, err
	}
	if msg.Reason == 0 || msg.Reason > seedRefusedReasonMax {
		return SeedRefused{}, fmt.Errorf("%w: SEED_REFUSED reason %d, it is 1 to %d", ErrSeedMalformed, msg.Reason, seedRefusedReasonMax)
	}
	msg.Detail = string(detail)
	return msg, nil
}

// EncodeSeedBusy returns the SEED_BUSY payload.
func EncodeSeedBusy(msg SeedBusy) []byte {
	buf := seedStart(msg.Header, KindSeedBusy, 0, seedIDLen+4)
	buf = append(buf, msg.SeedID[:]...)
	return binary.BigEndian.AppendUint32(buf, msg.RetryAfterMs)
}

// DecodeSeedBusy reads a SEED_BUSY payload.
func DecodeSeedBusy(payload []byte) (SeedBusy, error) {
	header, body, err := seedOpen(payload, KindSeedBusy)
	if err != nil {
		return SeedBusy{}, err
	}
	if err := noFlags(header, "SEED_BUSY"); err != nil {
		return SeedBusy{}, err
	}
	reader := &seedReader{rest: body, name: "SEED_BUSY"}
	msg := SeedBusy{Header: header}
	msg.SeedID = reader.seedID()
	msg.RetryAfterMs = reader.u32("retryAfterMs")
	if err := reader.finish(); err != nil {
		return SeedBusy{}, err
	}
	return msg, nil
}

// EncodeSeedAck returns the SEED_ACK payload.
func EncodeSeedAck(msg SeedAck) []byte {
	buf := seedStart(msg.Header, KindSeedAck, 0, seedIDLen+seedPositionLen+4)
	buf = append(buf, msg.SeedID[:]...)
	buf = appendSeedPosition(buf, msg.Applied)
	return binary.BigEndian.AppendUint32(buf, msg.CreditBytes)
}

// DecodeSeedAck reads a SEED_ACK payload.
func DecodeSeedAck(payload []byte) (SeedAck, error) {
	header, body, err := seedOpen(payload, KindSeedAck)
	if err != nil {
		return SeedAck{}, err
	}
	if err := noFlags(header, "SEED_ACK"); err != nil {
		return SeedAck{}, err
	}
	reader := &seedReader{rest: body, name: "SEED_ACK"}
	msg := SeedAck{Header: header}
	msg.SeedID = reader.seedID()
	msg.Applied = reader.position("appliedPosition")
	msg.CreditBytes = reader.u32("creditBytes")
	if err := reader.finish(); err != nil {
		return SeedAck{}, err
	}
	return msg, nil
}

// EncodeSeedRestart returns the SEED_RESTART payload. The reason is 1 to 4.
func EncodeSeedRestart(msg SeedRestart) ([]byte, error) {
	if msg.Reason == 0 || msg.Reason > seedRestartReasonMax {
		return nil, fmt.Errorf("%w: SEED_RESTART reason %d, it is 1 to %d", ErrSeedMalformed, msg.Reason, seedRestartReasonMax)
	}
	buf := seedStart(msg.Header, KindSeedRestart, 0, seedIDLen+1)
	buf = append(buf, msg.SeedID[:]...)
	return append(buf, msg.Reason), nil
}

// DecodeSeedRestart reads a SEED_RESTART payload.
func DecodeSeedRestart(payload []byte) (SeedRestart, error) {
	header, body, err := seedOpen(payload, KindSeedRestart)
	if err != nil {
		return SeedRestart{}, err
	}
	if err := noFlags(header, "SEED_RESTART"); err != nil {
		return SeedRestart{}, err
	}
	reader := &seedReader{rest: body, name: "SEED_RESTART"}
	msg := SeedRestart{Header: header}
	msg.SeedID = reader.seedID()
	msg.Reason = reader.byteField("reason")
	if err := reader.finish(); err != nil {
		return SeedRestart{}, err
	}
	if msg.Reason == 0 || msg.Reason > seedRestartReasonMax {
		return SeedRestart{}, fmt.Errorf("%w: SEED_RESTART reason %d, it is 1 to %d", ErrSeedMalformed, msg.Reason, seedRestartReasonMax)
	}
	return msg, nil
}

// EncodeSeedChunk returns the SEED_CHUNK payload: seed id, chunk index, entry
// count, CRC32C over the entry bytes only, then the entries, each
// [uvarint klen][key][uvarint vlen][value]. An entry is never split, so a
// chunk may exceed the nominal size; the only ceiling is the frame's, which
// bounds the opcode and request ID together with the payload.
func EncodeSeedChunk(chunk SeedChunk) ([]byte, error) {
	if len(chunk.Entries) > seedMaxChunkEntries {
		return nil, fmt.Errorf("%w: SEED_CHUNK has %d entries, at most %d", ErrSeedMalformed, len(chunk.Entries), seedMaxChunkEntries)
	}
	entryBytes := 0
	for index, entry := range chunk.Entries {
		if len(entry.Key) == 0 || len(entry.Key) > seedMaxKeyBytes {
			return nil, fmt.Errorf("%w: SEED_CHUNK entry %d has a %d byte key, a key is 1 to %d", ErrSeedMalformed, index, len(entry.Key), seedMaxKeyBytes)
		}
		entryBytes += uvarintLen(uint64(len(entry.Key))) + len(entry.Key) + uvarintLen(uint64(len(entry.Value))) + len(entry.Value)
	}
	bodyLen := seedIDLen + 16 + entryBytes
	// The frame writer refuses opcode + request ID + payload over MaxFrameLength,
	// so the payload ceiling is smaller than MaxFrameLength by those bytes.
	payloadCeiling := MaxFrameLength - 1 - RequestIDLen
	if EnvelopeSize+bodyLen > payloadCeiling {
		return nil, fmt.Errorf("%w: SEED_CHUNK of %d bytes exceeds the %d byte frame ceiling; one entry is too large to seed", ErrSeedMalformed, EnvelopeSize+bodyLen, payloadCeiling)
	}
	var flags uint16
	if chunk.Final {
		flags = FlagLastOfChunk
	}
	buf := seedStart(chunk.Header, KindSeedChunk, flags, bodyLen)
	buf = append(buf, chunk.SeedID[:]...)
	buf = binary.BigEndian.AppendUint64(buf, chunk.Index)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(chunk.Entries)))
	crcAt := len(buf)
	buf = append(buf, 0, 0, 0, 0)
	entriesAt := len(buf)
	for _, entry := range chunk.Entries {
		buf = binary.AppendUvarint(buf, uint64(len(entry.Key)))
		buf = append(buf, entry.Key...)
		buf = binary.AppendUvarint(buf, uint64(len(entry.Value)))
		buf = append(buf, entry.Value...)
	}
	binary.BigEndian.PutUint32(buf[crcAt:], crc32.Checksum(buf[entriesAt:], castagnoli))
	return buf, nil
}

// DecodeSeedChunk reads a SEED_CHUNK payload. The CRC is checked before any
// entry is parsed, the entry count is bounded by the bytes that remain before
// the entry slice is allocated, and every length is canonical, so a frame that
// decodes re-encodes to the same bytes. Keys and values alias payload.
func DecodeSeedChunk(payload []byte) (SeedChunk, error) {
	header, body, err := seedOpen(payload, KindSeedChunk)
	if err != nil {
		return SeedChunk{}, err
	}
	if header.Flags&^FlagLastOfChunk != 0 {
		return SeedChunk{}, fmt.Errorf("%w: SEED_CHUNK sets envelope flags 0x%04x, only the final-chunk bit is defined", ErrSeedMalformed, header.Flags)
	}
	reader := &seedReader{rest: body, name: "SEED_CHUNK"}
	chunk := SeedChunk{Header: header, Final: header.Flags&FlagLastOfChunk != 0}
	chunk.SeedID = reader.seedID()
	chunk.Index = reader.u64("chunkIndex")
	count := reader.u32("entryCount")
	chunk.CRC = reader.u32("crc32c")
	if reader.err != nil {
		return SeedChunk{}, reader.err
	}
	entryBytes := reader.rest
	if got := crc32.Checksum(entryBytes, castagnoli); got != chunk.CRC {
		return SeedChunk{}, fmt.Errorf("%w: SEED_CHUNK %d of seed %x has crc32c %08x over its entries, the header says %08x", ErrSeedMalformed, chunk.Index, chunk.SeedID, got, chunk.CRC)
	}
	if count > seedMaxChunkEntries {
		return SeedChunk{}, fmt.Errorf("%w: SEED_CHUNK claims %d entries, at most %d", ErrSeedMalformed, count, seedMaxChunkEntries)
	}
	if uint64(count) > uint64(len(entryBytes))/seedMinEntryBytes {
		return SeedChunk{}, fmt.Errorf("%w: SEED_CHUNK claims %d entries in %d bytes", ErrSeedMalformed, count, len(entryBytes))
	}
	chunk.Entries = make([]SeedEntry, 0, min(count, seedMaxChunkEntries))
	for index := uint32(0); index < count; index++ {
		entry, remaining, err := readSeedEntry(entryBytes)
		if err != nil {
			return SeedChunk{}, fmt.Errorf("SEED_CHUNK entry %d: %w", index, err)
		}
		chunk.Entries = append(chunk.Entries, entry)
		entryBytes = remaining
	}
	if len(entryBytes) != 0 {
		return SeedChunk{}, fmt.Errorf("%w: SEED_CHUNK has %d bytes after its %d entries", ErrSeedMalformed, len(entryBytes), count)
	}
	return chunk, nil
}

// readSeedEntry parses one entry from the front of raw.
func readSeedEntry(raw []byte) (SeedEntry, []byte, error) {
	keyLen, raw, err := readCanonicalUvarint(raw, "key length")
	if err != nil {
		return SeedEntry{}, nil, err
	}
	if keyLen == 0 || keyLen > seedMaxKeyBytes || keyLen > uint64(len(raw)) {
		return SeedEntry{}, nil, fmt.Errorf("%w: key length %d with %d bytes left, a key is 1 to %d", ErrSeedMalformed, keyLen, len(raw), seedMaxKeyBytes)
	}
	key := raw[:keyLen:keyLen]
	raw = raw[keyLen:]
	valueLen, raw, err := readCanonicalUvarint(raw, "value length")
	if err != nil {
		return SeedEntry{}, nil, err
	}
	if valueLen > uint64(len(raw)) {
		return SeedEntry{}, nil, fmt.Errorf("%w: value length %d with %d bytes left", ErrSeedMalformed, valueLen, len(raw))
	}
	return SeedEntry{Key: key, Value: raw[:valueLen:valueLen]}, raw[valueLen:], nil
}

// readCanonicalUvarint reads a uvarint and refuses an over-long encoding, so
// every accepted frame has exactly one byte form.
func readCanonicalUvarint(raw []byte, field string) (uint64, []byte, error) {
	number, width := binary.Uvarint(raw)
	if width <= 0 || width != uvarintLen(number) {
		return 0, nil, fmt.Errorf("%w: %s is not a canonical uvarint", ErrSeedMalformed, field)
	}
	return number, raw[width:], nil
}

func uvarintLen(number uint64) int {
	width := 1
	for number >= 0x80 {
		number >>= 7
		width++
	}
	return width
}

// ChunkSequence checks that SEED_CHUNK indices are contiguous per seed_id,
// which the seeding machine cannot see: machine.ChunkStored carries no index,
// so a skipped middle chunk would otherwise leave the slot ready with a hole.
// A stream starts at index 0 (a resume restarts at 0 on a fresh cut, so call
// Restart first). It is for one goroutine at a time.
type ChunkSequence struct {
	streams map[[seedIDLen]byte]*chunkStream
}

type chunkStream struct {
	next uint64
	done bool
}

// NewChunkSequence returns an empty tracker.
func NewChunkSequence() *ChunkSequence {
	return &ChunkSequence{streams: make(map[[seedIDLen]byte]*chunkStream)}
}

// Accept admits a decoded chunk if its index is the next of its seed_id. A gap,
// a repeat, a chunk after the final one, or a first chunk above 0 is refused
// with ErrSeedChunkGap naming the seed and the index that was expected; the
// pull answers by restarting the attempt on a fresh cut. A new seed past the
// seedMaxTracked cap is refused with ErrSeedLimit, which is local and not the
// peer's fault.
func (s *ChunkSequence) Accept(chunk SeedChunk) error {
	stream := s.streams[chunk.SeedID]
	if stream == nil {
		if len(s.streams) >= seedMaxTracked {
			return fmt.Errorf("%w: %d seeds are already tracked; Restart the finished ones", ErrSeedLimit, len(s.streams))
		}
		stream = &chunkStream{}
	}
	if stream.done {
		return fmt.Errorf("%w: seed %x sent chunk %d after its final chunk; restart the attempt on a fresh cut", ErrSeedChunkGap, chunk.SeedID, chunk.Index)
	}
	if chunk.Index != stream.next {
		return fmt.Errorf("%w: seed %x sent chunk %d but chunk %d is missing or repeated; restart the attempt on a fresh cut", ErrSeedChunkGap, chunk.SeedID, chunk.Index, stream.next)
	}
	stream.next++
	stream.done = chunk.Final
	s.streams[chunk.SeedID] = stream
	return nil
}

// Finished reports whether the final chunk of the seed_id has been accepted.
func (s *ChunkSequence) Finished(seedID [seedIDLen]byte) bool {
	stream := s.streams[seedID]
	return stream != nil && stream.done
}

// Restart forgets a seed_id so its next stream starts again at index 0.
func (s *ChunkSequence) Restart(seedID [seedIDLen]byte) { delete(s.streams, seedID) }

// CreditWindow is the source's view of the receiver's credit. The window is
// absolute: every SEED_ACK replaces it, nothing accumulates, and an ack lost
// on the wire is repaired by the next one. The applied position an ack reports
// never moves backwards. It is for one goroutine at a time.
type CreditWindow struct {
	seeds map[[seedIDLen]byte]*creditState
}

type creditState struct {
	applied SeedPosition
	window  uint32
}

// NewCreditWindow returns an empty tracker.
func NewCreditWindow() *CreditWindow {
	return &CreditWindow{seeds: make(map[[seedIDLen]byte]*creditState)}
}

// Open starts a seed's accounting from its SEED_ACCEPT: the initial window and
// the cut as the first applied position. A new seed past the seedMaxTracked cap
// is refused with ErrSeedLimit, which is local and not the peer's fault.
func (c *CreditWindow) Open(accept SeedAccept) error {
	if _, known := c.seeds[accept.SeedID]; !known && len(c.seeds) >= seedMaxTracked {
		return fmt.Errorf("%w: %d seeds are already tracked; Forget the finished ones", ErrSeedLimit, len(c.seeds))
	}
	c.seeds[accept.SeedID] = &creditState{applied: accept.Cut, window: accept.WindowBytes}
	return nil
}

// Observe applies an ack: the window becomes the ack's creditBytes, replacing
// what was there. An ack for an unopened seed, or one whose applied position is
// below the last acknowledged, is refused with ErrSeedAckRegressed. An ack for
// a seed that was never opened, or was forgotten, is refused with ErrSeedUnknown.
func (c *CreditWindow) Observe(ack SeedAck) error {
	state := c.seeds[ack.SeedID]
	if state == nil {
		return fmt.Errorf("%w: SEED_ACK for seed %x, which was never accepted", ErrSeedUnknown, ack.SeedID)
	}
	if ack.Applied.before(state.applied) {
		return fmt.Errorf("%w: seed %x acknowledged %d:%d after %d:%d", ErrSeedAckRegressed, ack.SeedID,
			ack.Applied.Epoch, ack.Applied.Seq, state.applied.Epoch, state.applied.Seq)
	}
	state.applied = ack.Applied
	state.window = ack.CreditBytes
	return nil
}

// Window returns the current absolute window of a seed.
func (c *CreditWindow) Window(seedID [seedIDLen]byte) (uint32, bool) {
	state := c.seeds[seedID]
	if state == nil {
		return 0, false
	}
	return state.window, true
}

// Forget drops a seed's accounting.
func (c *CreditWindow) Forget(seedID [seedIDLen]byte) { delete(c.seeds, seedID) }
