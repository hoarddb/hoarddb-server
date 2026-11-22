package wire

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// maxBarrierBitmap is the longest barrier bitmap: 8192 slots, one bit each, the
// size of a seed bitmap (conv section 13). Bit s of byte s/8 is slot s, least
// significant bit first, the order machine.Bit freezes.
const maxBarrierBitmap = 1024

// barrierSlots is the slot count a bitmap can name.
const barrierSlots = maxBarrierBitmap * 8

// positionSize is the encoded size of a cluster.Position.
const positionSize = 16

// HandoffReq is HANDOFF_REQ (conv section 6.1, kind 0x05):
// [fromEpoch:8][toEpoch:8][bitmapLen:2][slotBitmap], integers big-endian.
// The spec fixes the field order and widths but not the byte order; every other
// integer of the node protocol is big-endian, so these are.
type HandoffReq struct {
	FromEpoch, ToEpoch uint64
	// Bitmap names the slots of one database that moved from the sender to the receiver.
	Bitmap []byte
}

// HandoffStopped is HANDOFF_STOPPED (kind 0x14): [toEpoch:8][bitmapLen:2][slotBitmap].
// The old owner has stopped ordering and serving reads for the named pairs.
type HandoffStopped struct {
	ToEpoch uint64
	Bitmap  []byte
}

// HandoffReady is HANDOFF_READY (kind 0x06):
// [toEpoch:8][bitmapLen:2][slotBitmap][appliedPos:16].
type HandoffReady struct {
	ToEpoch uint64
	Bitmap  []byte
	// AppliedPos is the least applied position over the named pairs (build SPEC-GAP G2).
	AppliedPos cluster.Position
}

// PositionQuery is POSITION_QUERY (kind 0x12): [slot:2][db:8].
type PositionQuery struct {
	Slot uint16
	DB   uint64
}

// PositionReply is POSITION_REPLY (kind 0x13):
// [slot:2][db:8][state:1][appliedPos:16][appliedCursor:8]. State is conv section 12's
// byte: 1 seeding, 2 ready, 3 retired, 4 retiring.
type PositionReply struct {
	Slot          uint16
	DB            uint64
	State         uint8
	AppliedPos    cluster.Position
	AppliedCursor uint64
}

// The slot-record states of conv section 12 that a POSITION_REPLY carries.
const (
	// PositionStateSeeding is a copy still being seeded.
	PositionStateSeeding uint8 = 1
	// PositionStateReady is a copy that serves and counts for a concern.
	PositionStateReady uint8 = 2
	// PositionStateRetired is a slot out of the copy set, nothing deleted yet.
	PositionStateRetired uint8 = 3
	// PositionStateRetiring is a slot whose range delete has started.
	PositionStateRetiring uint8 = 4
)

// checkBitmap refuses a bitmap that is empty (the message would cover no pair) or
// longer than 8192 bits.
func checkBitmap(length int) error {
	if length == 0 || length > maxBarrierBitmap {
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"a barrier slot bitmap is 1 to %d bytes, got %d. The message was dropped; a node that sends this has a bug, so report it with its version", maxBarrierBitmap, length)}
	}
	return nil
}

// cutBitmap reads [bitmapLen:2][slotBitmap] at the start of body and returns the
// bitmap (a copy) and the rest. The length is checked against the body before
// anything is allocated.
func cutBitmap(body []byte) (bitmap, rest []byte, err error) {
	if len(body) < 2 {
		return nil, nil, shortBody("bitmap length", 2, len(body))
	}
	length := int(binary.BigEndian.Uint16(body))
	body = body[2:]
	if err := checkBitmap(length); err != nil {
		return nil, nil, err
	}
	if len(body) < length {
		return nil, nil, shortBody("slot bitmap", length, len(body))
	}
	return append([]byte(nil), body[:length]...), body[length:], nil
}

func shortBody(what string, want, got int) error {
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
		"a node message is too short for its %s: it needs %d more bytes and has %d. The message was dropped", what, want, got)}
}

func trailingBytes(kind string, extra int) error {
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
		"a %s has %d bytes after its last field. The message was dropped", kind, extra)}
}

func appendBitmap(dst, bitmap []byte) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(bitmap)))
	return append(dst, bitmap...)
}

// EncodeHandoffReq returns the body of a HANDOFF_REQ.
func EncodeHandoffReq(msg HandoffReq) ([]byte, error) {
	if err := checkBitmap(len(msg.Bitmap)); err != nil {
		return nil, err
	}
	out := binary.BigEndian.AppendUint64(make([]byte, 0, 18+len(msg.Bitmap)), msg.FromEpoch)
	out = binary.BigEndian.AppendUint64(out, msg.ToEpoch)
	return appendBitmap(out, msg.Bitmap), nil
}

// DecodeHandoffReq reads a HANDOFF_REQ body.
func DecodeHandoffReq(body []byte) (HandoffReq, error) {
	if len(body) < 16 {
		return HandoffReq{}, shortBody("HANDOFF_REQ epochs", 16, len(body))
	}
	msg := HandoffReq{FromEpoch: binary.BigEndian.Uint64(body), ToEpoch: binary.BigEndian.Uint64(body[8:])}
	bitmap, rest, err := cutBitmap(body[16:])
	if err != nil {
		return HandoffReq{}, fmt.Errorf("reading HANDOFF_REQ: %w", err)
	}
	if len(rest) != 0 {
		return HandoffReq{}, trailingBytes("HANDOFF_REQ", len(rest))
	}
	msg.Bitmap = bitmap
	return msg, nil
}

// EncodeHandoffStopped returns the body of a HANDOFF_STOPPED.
func EncodeHandoffStopped(msg HandoffStopped) ([]byte, error) {
	if err := checkBitmap(len(msg.Bitmap)); err != nil {
		return nil, err
	}
	out := binary.BigEndian.AppendUint64(make([]byte, 0, 10+len(msg.Bitmap)), msg.ToEpoch)
	return appendBitmap(out, msg.Bitmap), nil
}

// DecodeHandoffStopped reads a HANDOFF_STOPPED body.
func DecodeHandoffStopped(body []byte) (HandoffStopped, error) {
	if len(body) < 8 {
		return HandoffStopped{}, shortBody("HANDOFF_STOPPED epoch", 8, len(body))
	}
	bitmap, rest, err := cutBitmap(body[8:])
	if err != nil {
		return HandoffStopped{}, fmt.Errorf("reading HANDOFF_STOPPED: %w", err)
	}
	if len(rest) != 0 {
		return HandoffStopped{}, trailingBytes("HANDOFF_STOPPED", len(rest))
	}
	return HandoffStopped{ToEpoch: binary.BigEndian.Uint64(body), Bitmap: bitmap}, nil
}

// EncodeHandoffReady returns the body of a HANDOFF_READY.
func EncodeHandoffReady(msg HandoffReady) ([]byte, error) {
	if err := checkBitmap(len(msg.Bitmap)); err != nil {
		return nil, err
	}
	out := binary.BigEndian.AppendUint64(make([]byte, 0, 10+len(msg.Bitmap)+positionSize), msg.ToEpoch)
	out = appendBitmap(out, msg.Bitmap)
	return cluster.AppendPosition(out, msg.AppliedPos), nil
}

// DecodeHandoffReady reads a HANDOFF_READY body.
func DecodeHandoffReady(body []byte) (HandoffReady, error) {
	if len(body) < 8 {
		return HandoffReady{}, shortBody("HANDOFF_READY epoch", 8, len(body))
	}
	bitmap, rest, err := cutBitmap(body[8:])
	if err != nil {
		return HandoffReady{}, fmt.Errorf("reading HANDOFF_READY: %w", err)
	}
	if len(rest) != positionSize {
		if len(rest) < positionSize {
			return HandoffReady{}, shortBody("HANDOFF_READY applied position", positionSize, len(rest))
		}
		return HandoffReady{}, trailingBytes("HANDOFF_READY", len(rest)-positionSize)
	}
	applied, err := cluster.DecodePosition(rest)
	if err != nil {
		return HandoffReady{}, fmt.Errorf("reading HANDOFF_READY: %w", err)
	}
	return HandoffReady{ToEpoch: binary.BigEndian.Uint64(body), Bitmap: bitmap, AppliedPos: applied}, nil
}

func checkSlotNumber(slot uint16) error {
	if int(slot) >= barrierSlots {
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"slot %d is above the last slot %d (slots run 0 to %d). The message was dropped", slot, barrierSlots-1, barrierSlots-1)}
	}
	return nil
}

// EncodePositionQuery returns the body of a POSITION_QUERY.
func EncodePositionQuery(msg PositionQuery) ([]byte, error) {
	if err := checkSlotNumber(msg.Slot); err != nil {
		return nil, err
	}
	out := binary.BigEndian.AppendUint16(make([]byte, 0, 10), msg.Slot)
	return binary.BigEndian.AppendUint64(out, msg.DB), nil
}

// DecodePositionQuery reads a POSITION_QUERY body.
func DecodePositionQuery(body []byte) (PositionQuery, error) {
	if len(body) != 10 {
		return PositionQuery{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"a POSITION_QUERY is exactly 10 bytes and this one has %d. The message was dropped", len(body))}
	}
	msg := PositionQuery{Slot: binary.BigEndian.Uint16(body), DB: binary.BigEndian.Uint64(body[2:])}
	if err := checkSlotNumber(msg.Slot); err != nil {
		return PositionQuery{}, err
	}
	return msg, nil
}

func checkPositionState(state uint8) error {
	if state < PositionStateSeeding || state > PositionStateRetiring {
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"a POSITION_REPLY state is 1 seeding, 2 ready, 3 retired or 4 retiring and this one is %d. The reply was dropped", state)}
	}
	return nil
}

// EncodePositionReply returns the body of a POSITION_REPLY.
func EncodePositionReply(msg PositionReply) ([]byte, error) {
	if err := checkSlotNumber(msg.Slot); err != nil {
		return nil, err
	}
	if err := checkPositionState(msg.State); err != nil {
		return nil, err
	}
	out := binary.BigEndian.AppendUint16(make([]byte, 0, 35), msg.Slot)
	out = binary.BigEndian.AppendUint64(out, msg.DB)
	out = append(out, msg.State)
	out = cluster.AppendPosition(out, msg.AppliedPos)
	return binary.BigEndian.AppendUint64(out, msg.AppliedCursor), nil
}

// DecodePositionReply reads a POSITION_REPLY body.
func DecodePositionReply(body []byte) (PositionReply, error) {
	if len(body) != 35 {
		return PositionReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"a POSITION_REPLY is exactly 35 bytes and this one has %d. The reply was dropped", len(body))}
	}
	applied, err := cluster.DecodePosition(body[11:27])
	if err != nil {
		return PositionReply{}, fmt.Errorf("reading POSITION_REPLY: %w", err)
	}
	msg := PositionReply{
		Slot: binary.BigEndian.Uint16(body), DB: binary.BigEndian.Uint64(body[2:]), State: body[10],
		AppliedPos: applied, AppliedCursor: binary.BigEndian.Uint64(body[27:]),
	}
	if err := checkSlotNumber(msg.Slot); err != nil {
		return PositionReply{}, err
	}
	if err := checkPositionState(msg.State); err != nil {
		return PositionReply{}, err
	}
	return msg, nil
}

// SlotsOfBitmap lists the slots a barrier bitmap names, lowest first.
func SlotsOfBitmap(bitmap []byte) []uint16 {
	var slots []uint16
	for index, octet := range bitmap {
		for octet != 0 {
			bit := bits.TrailingZeros8(octet)
			slots = append(slots, uint16(index*8+bit))
			octet &^= 1 << bit
		}
	}
	return slots
}

// BitmapOfSlots builds the shortest bitmap that names exactly slots, or nil when
// slots is empty or names a slot above 8191.
func BitmapOfSlots(slots []uint16) []byte {
	var highest = -1
	for _, slot := range slots {
		if int(slot) >= barrierSlots {
			return nil
		}
		highest = max(highest, int(slot))
	}
	if highest < 0 {
		return nil
	}
	bitmap := make([]byte, highest/8+1)
	for _, slot := range slots {
		bitmap[slot/8] |= 1 << (slot % 8)
	}
	return bitmap
}
