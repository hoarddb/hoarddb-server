package machine

import (
	"fmt"
	stdbits "math/bits"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// BitmapBytes is the size of a seed bitmap: 8192 slots, one bit each. It is
// the body of SEEDED and RETIRE and the bits of a SeedBitmap (conventions §13).
const BitmapBytes = 1024

// slotsPerBitmap is the number of slots a bitmap of BitmapBytes holds.
const slotsPerBitmap = BitmapBytes * 8

// Validate reports whether bits is exactly BitmapBytes long. The wire decode of
// SEEDED and RETIRE bodies calls it before any bit is read. A body of any other
// length is INVALID_ARGUMENT, because a short or long body is a broken sender,
// not a bitmap with fewer or more slots.
func Validate(bits []byte) error {
	if len(bits) != BitmapBytes {
		return &cluster.Error{
			Code: cluster.CodeInvalidArgument,
			Message: fmt.Sprintf("seed bitmap is %d bytes, want exactly %d (8192 slots, one bit each)",
				len(bits), BitmapBytes),
		}
	}
	return nil
}

// checkSlot refuses a slot above the last one. The bitmap holds slots 0 to
// 8191, and a slot above that would name a bit in the next bitmap.
func checkSlot(slot uint16) error {
	if slot >= slotsPerBitmap {
		return &cluster.Error{
			Code: cluster.CodeInvalidArgument,
			Message: fmt.Sprintf("slot %d is above the last seed slot %d (slots run 0 to %d)",
				slot, slotsPerBitmap-1, slotsPerBitmap-1),
		}
	}
	return nil
}

// Bit reports whether slot is set. Slot s is bit s%8 of byte s/8, least
// significant bit first. The order is frozen: it is the shape stored in
// ClusterState and sent on the wire.
func Bit(bits []byte, slot uint16) (bool, error) {
	if err := Validate(bits); err != nil {
		return false, err
	}
	if err := checkSlot(slot); err != nil {
		return false, err
	}
	return bits[slot/8]&(1<<(slot%8)) != 0, nil
}

// Set marks slot as ready. Setting a slot twice is the same as setting it once,
// which is what lets a replayed proposal converge. A refused call leaves bits
// unchanged.
func Set(bits []byte, slot uint16) error {
	if err := Validate(bits); err != nil {
		return err
	}
	if err := checkSlot(slot); err != nil {
		return err
	}
	bits[slot/8] |= 1 << (slot % 8)
	return nil
}

// OR merges src into dst. SEEDED uses it so that a lost, duplicated or replayed
// batch leaves the same bitmap: OR is commutative, associative and idempotent.
// Both lengths are checked before dst is written, so a refused call changes
// nothing.
func OR(dst, src []byte) error {
	if err := Validate(dst); err != nil {
		return err
	}
	if err := Validate(src); err != nil {
		return fmt.Errorf("OR source: %w", err)
	}
	for i := range dst {
		dst[i] |= src[i]
	}
	return nil
}

// Clear removes the slots named in src from dst and leaves every other bit as
// it was. RETIRE uses it, and a bit already 0 stays 0, so a repeated RETIRE
// changes nothing. Both lengths are checked before dst is written.
func Clear(dst, src []byte) error {
	if err := Validate(dst); err != nil {
		return err
	}
	if err := Validate(src); err != nil {
		return fmt.Errorf("Clear source: %w", err)
	}
	for i := range dst {
		dst[i] &^= src[i]
	}
	return nil
}

// Count returns the number of set slots in bits.
func Count(bits []byte) (int, error) {
	if err := Validate(bits); err != nil {
		return 0, err
	}
	n := 0
	for _, b := range bits {
		n += stdbits.OnesCount8(b)
	}
	return n, nil
}
