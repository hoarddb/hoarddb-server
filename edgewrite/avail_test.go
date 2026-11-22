package main

import "testing"

// TestAvailFromRefusesANegativeBavail pins the FreeBSD reading Linux cannot
// produce: Bavail is int64 there and goes negative when the reserved blocks are
// gone. availBytes feeds the guard that stops the fill before it takes the
// filesystem to zero; a raw conversion would wrap to about 16 EiB and the guard
// would never fire.
func TestAvailFromRefusesANegativeBavail(t *testing.T) {
	cases := []struct {
		name                  string
		blocks, bavail, bsize int64
		want                  uint64
	}{
		{"ten blocks of 4 KiB", 1000, 10, 4096, 40960},
		{"no blocks free", 1000, 0, 4096, 0},
		{"negative avail", 1000, -1, 4096, 0},
		{"no block size", 1000, 10, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := availFrom(tc.bavail, tc.bsize); got != tc.want {
				t.Fatalf("availFrom(%d, %d) = %d, want %d", tc.bavail, tc.bsize, got, tc.want)
			}
		})
	}
}
