package wire

import "testing"

// TestUsedPctClampsAnExhaustedFilesystem pins the two readings FreeBSD can
// produce and Linux cannot: Bavail is int64 there, so it goes negative when the
// reserved blocks are gone, and a raw conversion to uint64 would wrap to about
// 1.8e19 free blocks and turn the seed source's disk high-water refusal into a
// report of an empty disk.
func TestUsedPctClampsAnExhaustedFilesystem(t *testing.T) {
	cases := []struct {
		name           string
		blocks, bavail uint64
		want           int
	}{
		{"half free", 100, 50, 50},
		{"full", 100, 0, 100},
		{"empty", 100, 100, 0},
		// A reading where more is free than exists is not a filesystem: it is
		// what a raw conversion of freebsd's negative Bavail produces. It must
		// read as full, because the seed source pauses above its high-water
		// mark and a copy must never seed into a disk it cannot measure.
		{"more free than total", 100, 200, 100},
		{"wrapped negative bavail", 100, ^uint64(0), 100},
		{"no blocks", 0, 0, 0},
		{"one block free of three", 3, 1, 66},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := usedPct(tc.blocks, tc.bavail); got != tc.want {
				t.Fatalf("usedPct(%d blocks, %d available) = %d%%, want %d%%", tc.blocks, tc.bavail, got, tc.want)
			}
		})
	}
}
