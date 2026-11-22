package machine

import (
	"bytes"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// bitmapSlots is the number of slots a seed bitmap holds: 1024 bytes of 8 bits.
const bitmapSlots = BitmapBytes * 8

// bitmapSetSlot sets slot in bits and fails the test on any refusal. The tests
// that are not about refusals use it so their own assertions stay readable.
func bitmapSetSlot(t *testing.T, bits []byte, slot uint16) {
	t.Helper()
	if err := Set(bits, slot); err != nil {
		t.Fatalf("Set(slot %d): %v", slot, err)
	}
}

// bitmapOrInto merges src into dst and fails the test on any refusal.
func bitmapOrInto(t *testing.T, dst, src []byte) {
	t.Helper()
	if err := OR(dst, src); err != nil {
		t.Fatalf("OR: %v", err)
	}
}

// bitmapClearFrom clears the bits named in src from dst and fails the test on
// any refusal.
func bitmapClearFrom(t *testing.T, dst, src []byte) {
	t.Helper()
	if err := Clear(dst, src); err != nil {
		t.Fatalf("Clear: %v", err)
	}
}

// bitmapCountOf returns Count of bits and fails the test on any refusal.
func bitmapCountOf(t *testing.T, bits []byte) int {
	t.Helper()
	n, err := Count(bits)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	return n
}

// bitmapBitOf returns Bit for slot and fails the test on any refusal.
func bitmapBitOf(t *testing.T, bits []byte, slot uint16) bool {
	t.Helper()
	ok, err := Bit(bits, slot)
	if err != nil {
		t.Fatalf("Bit(slot %d): %v", slot, err)
	}
	return ok
}

// TestBitmapSeedingReportsAreBatchedBitmaps proposes one single-slot entry per
// slot and checks that the OR of all of them is the full bitmap: every byte 0xff
// and 8192 bits set.
func TestBitmapSeedingReportsAreBatchedBitmaps(t *testing.T) {
	acc := make([]byte, BitmapBytes)
	for slot := uint16(0); slot < bitmapSlots; slot++ {
		one := make([]byte, BitmapBytes)
		bitmapSetSlot(t, one, slot)
		bitmapOrInto(t, acc, one)
	}
	if got := bitmapCountOf(t, acc); got != bitmapSlots {
		t.Fatalf("Count after one entry per slot = %d, want %d", got, bitmapSlots)
	}
	for i, b := range acc {
		if b != 0xff {
			t.Fatalf("byte %d = %#02x after one entry per slot, want 0xff", i, b)
		}
	}
}

// TestBitmapOrIsIdempotentUnderReplay merges two entries, then replays both, the
// second one out of order. An assignment in place of OR loses slot 0 on the
// second merge and fails here.
func TestBitmapOrIsIdempotentUnderReplay(t *testing.T) {
	first := make([]byte, BitmapBytes)
	bitmapSetSlot(t, first, 0)
	second := make([]byte, BitmapBytes)
	bitmapSetSlot(t, second, 5)

	dst := make([]byte, BitmapBytes)
	bitmapOrInto(t, dst, first)
	bitmapOrInto(t, dst, second)
	bitmapOrInto(t, dst, second)
	bitmapOrInto(t, dst, first)

	// Slot 0 is bit 0 of byte 0 (0x01); slot 5 is bit 5 of byte 0 (0x20).
	if dst[0] != 0x21 {
		t.Fatalf("byte 0 after replay = %#02x, want 0x21", dst[0])
	}
	if got := bitmapCountOf(t, dst); got != 2 {
		t.Fatalf("Count after replay = %d, want 2", got)
	}
}

// TestBitmapOrIsCommutativeAndCountsTheUnion merges the same two entries in both
// orders. The union is {3, 4, 100, 8191}: four bits.
func TestBitmapOrIsCommutativeAndCountsTheUnion(t *testing.T) {
	x := make([]byte, BitmapBytes)
	bitmapSetSlot(t, x, 3)
	bitmapSetSlot(t, x, 100)
	bitmapSetSlot(t, x, 8191)
	y := make([]byte, BitmapBytes)
	bitmapSetSlot(t, y, 3)
	bitmapSetSlot(t, y, 4)

	a := make([]byte, BitmapBytes)
	bitmapOrInto(t, a, x)
	bitmapOrInto(t, a, y)

	b := make([]byte, BitmapBytes)
	bitmapOrInto(t, b, y)
	bitmapOrInto(t, b, x)

	if !bytes.Equal(a, b) {
		t.Fatalf("OR(x then y) != OR(y then x)")
	}
	if got := bitmapCountOf(t, a); got != 4 {
		t.Fatalf("Count of the union = %d, want 4", got)
	}
}

// TestBitmapClearIsIdempotentAndNamedBitsOnly clears {1, 5000} from {0, 1, 2}.
// Slot 5000 was never set, so clearing it must be a no-op. Only slot 1 goes; a
// second clear changes nothing.
func TestBitmapClearIsIdempotentAndNamedBitsOnly(t *testing.T) {
	dst := make([]byte, BitmapBytes)
	bitmapSetSlot(t, dst, 0)
	bitmapSetSlot(t, dst, 1)
	bitmapSetSlot(t, dst, 2)
	named := make([]byte, BitmapBytes)
	bitmapSetSlot(t, named, 1)
	bitmapSetSlot(t, named, 5000)

	bitmapClearFrom(t, dst, named)
	once := append([]byte(nil), dst...)
	bitmapClearFrom(t, dst, named)

	if !bytes.Equal(dst, once) {
		t.Fatalf("second Clear changed the bitmap")
	}
	if bitmapBitOf(t, dst, 1) {
		t.Fatalf("slot 1 is still set after Clear")
	}
	if !bitmapBitOf(t, dst, 0) || !bitmapBitOf(t, dst, 2) {
		t.Fatalf("Clear removed a bit it was not named for")
	}
	if got := bitmapCountOf(t, dst); got != 2 {
		t.Fatalf("Count after Clear = %d, want 2", got)
	}
}

// TestBitmapSetIsIdempotent sets slot 9 twice and expects one bit.
func TestBitmapSetIsIdempotent(t *testing.T) {
	bits := make([]byte, BitmapBytes)
	bitmapSetSlot(t, bits, 9)
	bitmapSetSlot(t, bits, 9)
	if got := bitmapCountOf(t, bits); got != 1 {
		t.Fatalf("Count after setting slot 9 twice = %d, want 1", got)
	}
}

// TestBitmapBitOrderIsFrozen checks Set against bitmaps written out by hand from
// the frozen order: slot s is bit s%8 of byte s/8, least significant bit first.
// Each expected body lists only its non-zero bytes, by index.
func TestBitmapBitOrderIsFrozen(t *testing.T) {
	vectors := []struct {
		name  string
		slots []uint16
		want  map[int]byte
	}{
		// Slot 0 is the least significant bit of byte 0.
		{"slot 0 only", []uint16{0}, map[int]byte{0: 0x01}},
		// Slot 7 is the most significant bit of byte 0.
		{"slot 7 only", []uint16{7}, map[int]byte{0: 0x80}},
		// Slot 8 is the least significant bit of byte 1.
		{"slot 8 only", []uint16{8}, map[int]byte{1: 0x01}},
		// Slots 1 and 9 are bit 1 of byte 0 and bit 1 of byte 1.
		{"slots 1 and 9", []uint16{1, 9}, map[int]byte{0: 0x02, 1: 0x02}},
		// Slot 8191 is bit 7 of byte 1023, the last bit of the body.
		{"slot 8191 only", []uint16{8191}, map[int]byte{1023: 0x80}},
		// Slots 0 and 8191 are the two ends of the body.
		{"slots 0 and 8191", []uint16{0, 8191}, map[int]byte{0: 0x01, 1023: 0x80}},
	}
	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			want := make([]byte, BitmapBytes)
			for i, b := range v.want {
				want[i] = b
			}
			got := make([]byte, BitmapBytes)
			for _, slot := range v.slots {
				bitmapSetSlot(t, got, slot)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("bitmap for %v differs from the hand-computed body", v.slots)
			}
			for _, slot := range v.slots {
				if !bitmapBitOf(t, want, slot) {
					t.Fatalf("Bit(want, %d) = false, want true", slot)
				}
			}
		})
	}
}

// bitmapAssertInvalidArgument runs call and fails unless it panicked not at all
// and returned an error that carries INVALID_ARGUMENT through errors.As.
func bitmapAssertInvalidArgument(t *testing.T, call string, fn func() error) {
	t.Helper()
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s panicked on an input it must refuse: %v", call, r)
			}
		}()
		err = fn()
	}()
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("%s error = %v, want INVALID_ARGUMENT", call, err)
	}
}

// bitmapFilledBody returns n bytes of 0xa5, so an unchanged body is easy to
// compare against a copy. A negative n returns nil, the nil slice case.
func bitmapFilledBody(n int) []byte {
	if n < 0 {
		return nil
	}
	body := make([]byte, n)
	for i := range body {
		body[i] = 0xa5
	}
	return body
}

// TestBitmapLengthIsExact feeds every helper a body whose length is not exactly
// BitmapBytes. The lengths include 8192, the bit count mistaken for the byte
// count, and its neighbours. Each call must return INVALID_ARGUMENT without
// panicking, and no helper may change the bitmap it was given.
func TestBitmapLengthIsExact(t *testing.T) {
	bad := []struct {
		name string
		n    int
	}{
		{"zero", 0}, {"one", 1}, {"1023", 1023}, {"1025", 1025}, {"2048", 2048},
		{"4096", 4096}, {"8191", 8191}, {"8192", 8192}, {"8193", 8193}, {"nil", -1},
	}
	for _, b := range bad {
		t.Run(b.name, func(t *testing.T) {
			short := bitmapFilledBody(b.n)
			good := make([]byte, BitmapBytes)
			bitmapSetSlot(t, good, 5)

			bitmapAssertInvalidArgument(t, "Validate", func() error { return Validate(short) })
			bitmapAssertInvalidArgument(t, "Set", func() error { return Set(short, 0) })
			bitmapAssertInvalidArgument(t, "Bit", func() error { _, err := Bit(short, 0); return err })
			bitmapAssertInvalidArgument(t, "Count", func() error { _, err := Count(short); return err })

			// A bad dst must not be written to.
			dst := bitmapFilledBody(b.n)
			want := bitmapFilledBody(b.n)
			bitmapAssertInvalidArgument(t, "OR(bad dst)", func() error { return OR(dst, good) })
			if !bytes.Equal(dst, want) {
				t.Fatalf("OR changed a refused dst")
			}
			bitmapAssertInvalidArgument(t, "Clear(bad dst)", func() error { return Clear(dst, good) })
			if !bytes.Equal(dst, want) {
				t.Fatalf("Clear changed a refused dst")
			}

			// A bad src must be refused, and a good dst must be left alone.
			goodBefore := append([]byte(nil), good...)
			bitmapAssertInvalidArgument(t, "OR(bad src)", func() error { return OR(good, short) })
			if !bytes.Equal(good, goodBefore) {
				t.Fatalf("OR changed dst when src was refused")
			}
			bitmapAssertInvalidArgument(t, "Clear(bad src)", func() error { return Clear(good, short) })
			if !bytes.Equal(good, goodBefore) {
				t.Fatalf("Clear changed dst when src was refused")
			}
		})
	}
}

// TestBitmapValidateAcceptsExactlyTheBody checks that the one correct length
// passes Validate.
func TestBitmapValidateAcceptsExactlyTheBody(t *testing.T) {
	body := make([]byte, BitmapBytes)
	if err := Validate(body); err != nil {
		t.Fatalf("Validate(1024 bytes) = %v, want nil", err)
	}
}

// TestBitmapSlotAboveLastIsRefused: slot 8191 is the last slot and is accepted;
// a slot above it is INVALID_ARGUMENT, and the bitmap is left as it was.
func TestBitmapSlotAboveLastIsRefused(t *testing.T) {
	bits := make([]byte, BitmapBytes)
	bitmapSetSlot(t, bits, 8191)
	if !bitmapBitOf(t, bits, 8191) {
		t.Fatalf("slot 8191 was not set")
	}
	before := append([]byte(nil), bits...)
	for _, slot := range []uint16{8192, 65535} {
		bitmapAssertInvalidArgument(t, "Set above the last slot", func() error { return Set(bits, slot) })
		bitmapAssertInvalidArgument(t, "Bit above the last slot", func() error { _, err := Bit(bits, slot); return err })
		if !bytes.Equal(bits, before) {
			t.Fatalf("slot %d changed the bitmap before being refused", slot)
		}
	}
}
