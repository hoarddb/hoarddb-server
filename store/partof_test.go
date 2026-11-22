package store

import (
	"errors"
	"testing"
)

// TestPartOfIsStableAndUniform pins the part a document goes to: the top
// log2(K) bits of xxhash64 over the encoded id (document placement spread MVP
// note, B11). The vector is the permanent one: a change re-parts every
// document of every spread collection. Hashing the raw bytes instead of the
// encoded form gives the "raw" column and fails the vector.
func TestPartOfIsStableAndUniform(t *testing.T) {
	vector := []struct {
		id   interface{}
		part int
	}{
		{"jane", 207},
		{"a\x00b", 144},
		{"", 63},
		{"User-1", 229},
		{"ada", 43},
		{"7", 202},
		{int32(7), 235},
		{int64(7), 235},
	}
	for _, row := range vector {
		encoded, err := EncodeComponent(row.id)
		if err != nil {
			t.Fatalf("EncodeComponent(%#v): %v", row.id, err)
		}
		got, err := PartOf(encoded, 256)
		if err != nil {
			t.Fatalf("PartOf(%#v, 256): %v", row.id, err)
		}
		if got != row.part {
			t.Errorf("PartOf(%#v, 256) = %d, want %d (the pinned vector)", row.id, got, row.part)
		}
	}

	// The string "7" and the number 7 are different ids and part apart.
	if vector[5].part == vector[6].part {
		t.Fatal("the vector is broken: \"7\" and 7 should differ")
	}

	// Uniform: 25,600 ids over 256 parts, every part used, none far from the mean of 100.
	counts := make([]int, 256)
	for i := 0; i < 25600; i++ {
		encoded, err := EncodeComponent("user-" + string(rune('a'+i%26)) + itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		part, err := PartOf(encoded, 256)
		if err != nil {
			t.Fatal(err)
		}
		counts[part]++
	}
	for part, n := range counts {
		if n < 50 || n > 170 {
			t.Errorf("part %d holds %d of 25,600 ids; want within 50..170 of a mean of 100", part, n)
		}
	}
}

// TestPartOfReadsTheFactorItIsGiven holds that K is an argument: a smaller K
// is the top bits of the same hash, and a K that is not a power of two (or is
// out of range) is refused instead of giving a skewed part.
func TestPartOfReadsTheFactorItIsGiven(t *testing.T) {
	encoded, err := EncodeComponent("jane")
	if err != nil {
		t.Fatal(err)
	}
	wide, _ := PartOf(encoded, 256)
	narrow, err := PartOf(encoded, 16)
	if err != nil {
		t.Fatal(err)
	}
	if narrow != wide>>4 {
		t.Errorf("PartOf(jane, 16) = %d, want the top 4 bits of %d = %d", narrow, wide, wide>>4)
	}
	for _, bad := range []int{0, 1, 3, 255, 257, -256, 1 << 17} {
		if _, err := PartOf(encoded, bad); !errors.Is(err, ErrBadPartCount) {
			t.Errorf("PartOf(jane, %d) error = %v, want ErrBadPartCount", bad, err)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for ; n > 0; n /= 10 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
	}
	return string(digits)
}
