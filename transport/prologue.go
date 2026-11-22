package transport

import (
	"encoding/binary"
	"fmt"
)

// PrologueSize is the length of the client prologue at payload offset 0 of a
// frame a client sends to a node (conventions section 6).
const PrologueSize = 20

// Prologue is the client's routing hint: its table's epoch and state_seq, both
// zero for "route it for me", and the write-concern deadline, zero for none.
type Prologue struct {
	Epoch, StateSeq uint64
	DeadlineMs      uint32
}

// EncodePrologue returns the 20 prologue bytes, big-endian.
func EncodePrologue(pro Prologue) []byte {
	raw := make([]byte, PrologueSize)
	binary.BigEndian.PutUint64(raw[0:8], pro.Epoch)
	binary.BigEndian.PutUint64(raw[8:16], pro.StateSeq)
	binary.BigEndian.PutUint32(raw[16:20], pro.DeadlineMs)
	return raw
}

// DecodePrologue reads the prologue at the start of a client payload; the BSON
// body is payload[PrologueSize:].
func DecodePrologue(payload []byte) (Prologue, error) {
	if len(payload) < PrologueSize {
		return Prologue{}, fmt.Errorf("client prologue needs %d bytes (epoch, state_seq, deadline_ms), payload has %d", PrologueSize, len(payload))
	}
	return Prologue{
		Epoch:      binary.BigEndian.Uint64(payload[0:8]),
		StateSeq:   binary.BigEndian.Uint64(payload[8:16]),
		DeadlineMs: binary.BigEndian.Uint32(payload[16:20]),
	}, nil
}
