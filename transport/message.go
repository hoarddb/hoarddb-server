package transport

import (
	"encoding/binary"
	"fmt"
	"github.com/hoarddb/hoarddb-server/ids"
	"io"
)

// FrameHeader is the fixed-size header of every wire frame.
//
//	+--------+----------------+--------+------------------+------------------+
//	|Ver(1B) | Length (4 bytes)| Opcode (1 byte)| Request ID (16B)| Payload (N bytes)|
//	+--------+----------------+--------+------------------+------------------+
//
// Length = len(Opcode) + len(RequestID) + len(Payload).
// Length does NOT include Ver or Length fields themselves.
// Length is little-endian to match BSON's native byte order.
const FrameHeaderSize = 1 + 4 + 1 + RequestIDLen // 22 bytes

// MaxFrameLength bounds the frame body (opcode + requestID + payload) that
// DecodeFrame will allocate for. Length is a wire-supplied uint32, so without
// this an attacker (or a corrupt peer) can claim a multi-gigabyte length and
// force a huge allocation before a single body byte has been read — this
// check runs before any auth or 0-RTT gating. 32 MiB comfortably covers the
// largest legitimate frame today (8 MiB migration chunks, see
// server/migrate.go's snapshotChunkSize) with headroom for BSON overhead.
const MaxFrameLength = 32 << 20 // 32 MiB

// PreAuthFrameLength bounds the frame body a connection may send before its
// AUTH has succeeded. AUTH and PING, the only requests served before login,
// are a few hundred bytes; a resumption token is under 2 KiB.
const PreAuthFrameLength = 64 << 10 // 64 KiB

// frameBodyReadStep is the first buffer DecodeFrame allocates for a body
// larger than it. The buffer then doubles as bytes arrive, so the memory a
// frame holds is at most twice what its sender has delivered, whatever its
// length word claims.
const frameBodyReadStep = 64 << 10

// DumpChunkSize is the nominal size of one OpDumpChunk response
// (the dump-restore-spec record §16.1): a chunk boundary never splits a
// document, so a single document between this size and MaxFrameLength can
// make one chunk larger than the nominal target — that is expected, not a
// bug. 4 MiB leaves ample headroom under the 32 MiB frame cap after BSON and
// frame header overhead.
const DumpChunkSize = 4 << 20 // 4 MiB

// Frame represents a decoded wire frame.
type Frame struct {
	Version   uint8
	Opcode    Opcode
	RequestID [RequestIDLen]byte
	Payload   []byte

	// onWritten runs once the frame's write has returned, whatever its
	// outcome: it is how a query's admission slot is held through the write
	// of its result (query-pipelining spec §3.1, OC-2). Encode never sees
	// it; only ServeFrame fires it.
	onWritten func()

	// credentialUnchecked is set by an AUTH handler on a refusal it made
	// before looking at any credential. It never goes on the wire; it tells
	// the per-IP ledger the refusal is not a failed login (BUG-0237).
	credentialUnchecked bool
}

// MarkCredentialUnchecked records, on an AUTH_FAIL response, that the handler
// refused the frame without checking any credential, so the per-IP ledger must
// not count it as a failed login. It is not encoded: the wire is unchanged.
func (f *Frame) MarkCredentialUnchecked() { f.credentialUnchecked = true }

// CredentialUnchecked reports whether the handler marked this response with
// MarkCredentialUnchecked.
func (f *Frame) CredentialUnchecked() bool { return f.credentialUnchecked }

// AddWrittenHook adds hook to what runs once the frame's write returns. A
// hook already set still runs, before hook.
func (f *Frame) AddWrittenHook(hook func()) {
	if f.onWritten == nil {
		f.onWritten = hook
		return
	}
	previous := f.onWritten
	f.onWritten = func() {
		previous()
		hook()
	}
}

// TakeWrittenHook returns the frame's written hook, or nil, and clears it: the
// caller now owns firing it, on this frame's write or on another carrier's.
func (f *Frame) TakeWrittenHook() func() {
	hook := f.onWritten
	f.onWritten = nil
	return hook
}

// NewFrame creates a frame with a random request ID and the given opcode/payload.
func NewFrame(op Opcode, payload []byte) *Frame {
	return &Frame{
		Version:   ProtocolVersion,
		Opcode:    op,
		RequestID: ids.NewRequestID(),
		Payload:   payload,
	}
}

// NewFrameWithID creates a frame with an explicit request ID.
func NewFrameWithID(op Opcode, requestID [RequestIDLen]byte, payload []byte) *Frame {
	return &Frame{
		Version:   ProtocolVersion,
		Opcode:    op,
		RequestID: requestID,
		Payload:   payload,
	}
}

// Encode serializes the frame into wire format bytes.
//
// Wire format:
//
//	Ver:    1 byte  (currently 0x01)
//	Length: 4 bytes little-endian (opcode + requestID + payload)
//	Opcode: 1 byte
//	Req ID: 16 bytes
//	Payload: N bytes
//
// Length is little-endian to match BSON's native byte order, avoiding
// unnecessary byte-swapping on every message.
func (f *Frame) Encode() []byte {
	payloadLen := len(f.Payload)
	length := 1 + RequestIDLen + payloadLen // opcode + requestID + payload
	buf := make([]byte, FrameHeaderSize+payloadLen)

	// Version
	buf[0] = f.Version

	// Length (little-endian uint32 — matches BSON byte order)
	binary.LittleEndian.PutUint32(buf[1:5], uint32(length))

	// Opcode
	buf[5] = byte(f.Opcode)

	// Request ID
	copy(buf[6:6+RequestIDLen], f.RequestID[:])

	// Payload
	copy(buf[FrameHeaderSize:], f.Payload)

	return buf
}

// FrameTooLargeError is DecodeFrame's refusal of a frame whose length word is
// over MaxFrameLength. The reader is left positioned after the opcode and the
// request ID, with BodyLength-(1+RequestIDLen) payload bytes unread, so a
// server can answer the request by its ID and then discard those bytes.
type FrameTooLargeError struct {
	// BodyLength is the length word: opcode, request ID and payload.
	BodyLength uint32
	Opcode     Opcode
	RequestID  [RequestIDLen]byte
	// Limit is the bound the length word exceeded: MaxFrameLength, or
	// PreAuthFrameLength for a frame read before login.
	Limit uint32
}

// Error names the cap and the remedy, in the words a client sees.
func (e *FrameTooLargeError) Error() string {
	if e.Limit < MaxFrameLength {
		return preAuthRequestTooLargeMessage(e.Opcode, int64(e.BodyLength))
	}
	return requestTooLargeMessage(e.Opcode, int64(e.BodyLength))
}

// UnreadBodyBytes is how much of the oversize frame's payload is still in the
// reader.
func (e *FrameTooLargeError) UnreadBodyBytes() int64 {
	return int64(e.BodyLength) - 1 - RequestIDLen
}

// requestTooLargeMessage is the one sentence for a request over the frame cap,
// whether the client refuses it before sending or the server answers it
// : the size, the cap, and what to do instead.
func requestTooLargeMessage(op Opcode, bodyBytes int64) string {
	return fmt.Sprintf("this %s request is %d bytes, over the %d MiB frame limit, so it was not processed. "+
		"Split it into smaller requests (fewer documents per batch; 'hoarddb ingest' sizes batches for you), "+
		"or store a large value as a blob. See the limits page of the user docs.", op, bodyBytes, MaxFrameLength>>20)
}

// preAuthRequestTooLargeMessage answers a frame over PreAuthFrameLength sent
// before login: the size, the limit, and the order a client must follow.
func preAuthRequestTooLargeMessage(op Opcode, bodyBytes int64) string {
	return fmt.Sprintf("this %s request is %d bytes, over the %d KiB limit for a connection that has not logged in, "+
		"so it was not processed and the connection is being closed. Send AUTH first and wait for AUTH_OK "+
		"before any other request; after login the limit is %d MiB.", op, bodyBytes, PreAuthFrameLength>>10, MaxFrameLength>>20)
}

// DecodeFrame reads a single frame from the reader. It reads exactly the
// header, then reads Length bytes of body (opcode + requestID + payload).
func DecodeFrame(r io.Reader) (*Frame, error) {
	return DecodeFrameWithin(r, MaxFrameLength)
}

// DecodeFrameWithin is DecodeFrame with a tighter bound on the body: a length
// word over limit is refused with a *FrameTooLargeError naming limit, exactly
// as one over MaxFrameLength is. A limit above MaxFrameLength is lowered to
// it. The server reads with PreAuthFrameLength until a connection has logged
// in.
func DecodeFrameWithin(r io.Reader, limit uint32) (*Frame, error) {
	limit = min(limit, MaxFrameLength)
	// Read version + length (5 bytes).
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("read frame header: %w", err)
	}

	ver := hdr[0]
	length := binary.LittleEndian.Uint32(hdr[1:5])

	if ver != ProtocolVersion {
		return nil, &WireError{
			Code:    ErrUnsupportedVersion,
			Message: fmt.Sprintf("protocol version %d not supported (expected %d)", ver, ProtocolVersion),
		}
	}

	if length < 1+RequestIDLen {
		return nil, &WireError{
			Code:    ErrInvalidArgument,
			Message: fmt.Sprintf("frame length %d too short (minimum %d for opcode + request ID)", length, 1+RequestIDLen),
		}
	}

	if length > limit {
		// The opcode and request ID come before the payload, so a request that
		// is merely too large can still be answered by its ID. Nothing is
		// allocated for the body, which is left unread for the caller to
		// discard or to abandon. A frame over the absolute cap is named by
		// that cap even before login, so its answer stays the one a client
		// can act on by splitting the request.
		var identity [1 + RequestIDLen]byte
		if _, err := io.ReadFull(r, identity[:]); err != nil {
			return nil, fmt.Errorf("read oversize frame identity: %w", err)
		}
		bound := limit
		if length > MaxFrameLength {
			bound = MaxFrameLength
		}
		oversize := &FrameTooLargeError{BodyLength: length, Opcode: Opcode(identity[0]), Limit: bound}
		copy(oversize.RequestID[:], identity[1:])
		return nil, oversize
	}

	// Read body: opcode (1) + requestID (16) + payload (N).
	body, err := readFrameBody(r, int(length))
	if err != nil {
		return nil, fmt.Errorf("read frame body: %w", err)
	}

	f := &Frame{
		Version: ver,
		Opcode:  Opcode(body[0]),
	}
	copy(f.RequestID[:], body[1:1+RequestIDLen])
	f.Payload = body[1+RequestIDLen:]

	return f, nil
}

// readFrameBody reads exactly length bytes. A body up to frameBodyReadStep is
// read into one allocation; a larger one into a buffer that doubles as it
// fills, so a sender that stops short has made the reader hold at most twice
// what it sent. The copies on growth add up to less than the body itself.
func readFrameBody(r io.Reader, length int) ([]byte, error) {
	if length <= frameBodyReadStep {
		body := make([]byte, length)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		return body, nil
	}
	body := make([]byte, 0, frameBodyReadStep)
	for len(body) < length {
		if len(body) == cap(body) {
			grown := make([]byte, len(body), min(2*cap(body), length))
			copy(grown, body)
			body = grown
		}
		n, err := io.ReadFull(r, body[len(body):cap(body)])
		body = body[:len(body)+n]
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

// RequestIDString returns the request ID as a hex string for logging.
func (f *Frame) RequestIDString() string {
	return fmt.Sprintf("%x", f.RequestID)
}

// SizeBytes returns the total wire size of this frame in bytes.
func (f *Frame) SizeBytes() int {
	return FrameHeaderSize + len(f.Payload)
}
