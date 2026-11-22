package transport

import (
	"encoding/binary"
	"fmt"
)

// EnvelopeSize is the length of the node-to-node envelope that opens every
// node payload (conventions section 6).
const EnvelopeSize = 30

// EnvelopeVersion is the version byte every node-to-node envelope carries. It moves with
// the record format: a peer of another build is refused at its first frame, before any body
// is decoded, because nodes of different builds are never mixed (storage-format spec §1.5).
const EnvelopeVersion byte = 0x02

const (
	// FlagAckRequested and FlagLastOfChunk are the only envelope flag bits
	// defined; every other bit must be zero.
	FlagAckRequested uint16 = 1 << 0
	FlagLastOfChunk  uint16 = 1 << 1
	envelopeFlagMask        = FlagAckRequested | FlagLastOfChunk
)

// Envelope is the decoded 30-byte header at payload offset 0 of every
// node-to-node message. The frame header stays the transport's.
type Envelope struct {
	Version, Kind   byte
	Epoch, StateSeq uint64
	Flags, Slot     uint16
	DBID            uint64
}

// EncodeEnvelope returns the 30 envelope bytes, big-endian, for the caller to
// put in front of the message body.
func EncodeEnvelope(env Envelope) []byte {
	raw := make([]byte, EnvelopeSize)
	raw[0] = env.Version
	raw[1] = env.Kind
	binary.BigEndian.PutUint64(raw[2:10], env.Epoch)
	binary.BigEndian.PutUint64(raw[10:18], env.StateSeq)
	binary.BigEndian.PutUint16(raw[18:20], env.Flags)
	binary.BigEndian.PutUint16(raw[20:22], env.Slot)
	binary.BigEndian.PutUint64(raw[22:30], env.DBID)
	return raw
}

// DecodeEnvelope reads the envelope at the start of a payload; the body is
// payload[EnvelopeSize:]. It refuses a short payload, a version other than
// EnvelopeVersion (naming both and the fix), an unknown kind (naming the number)
// and any reserved flag bit.
func DecodeEnvelope(payload []byte) (Envelope, error) {
	if len(payload) < EnvelopeSize {
		return Envelope{}, fmt.Errorf("%w: envelope needs %d bytes, payload has %d", ErrInvalidKind, EnvelopeSize, len(payload))
	}
	env := Envelope{
		Version:  payload[0],
		Kind:     payload[1],
		Epoch:    binary.BigEndian.Uint64(payload[2:10]),
		StateSeq: binary.BigEndian.Uint64(payload[10:18]),
		Flags:    binary.BigEndian.Uint16(payload[18:20]),
		Slot:     binary.BigEndian.Uint16(payload[20:22]),
		DBID:     binary.BigEndian.Uint64(payload[22:30]),
	}
	if env.Version != EnvelopeVersion {
		return Envelope{}, fmt.Errorf("%w: this node speaks envelope version %d and the sender speaks %d: nodes of different builds are never mixed; "+
			"run the same build on every node, each on its own data directory: an empty data directory loses that node's data and, "+
			"on a cluster member, can lose committed cluster state", ErrInvalidKind, EnvelopeVersion, env.Version)
	}
	if _, ok := KindChannel(env.Kind); !ok {
		return Envelope{}, unknownKindError(env.Kind)
	}
	if env.Flags&^envelopeFlagMask != 0 {
		return Envelope{}, fmt.Errorf("%w: envelope flags 0x%04x set reserved bits", ErrInvalidKind, env.Flags)
	}
	return env, nil
}
