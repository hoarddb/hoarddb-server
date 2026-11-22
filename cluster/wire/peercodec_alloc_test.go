package wire

import (
	"runtime"
	"testing"
)

// TestAStoredReadThatClaimsManyFieldsAllocatesNothingForThem: the declared count is
// checked against the bytes that remain, but a map must not be sized from it before the
// entries are read; a refused body costs the refusal, not megabytes.
func TestAStoredReadThatClaimsManyFieldsAllocatesNothingForThem(t *testing.T) {
	const claimed = 60000
	body := []byte{forwardAnswered, 1, byte(claimed >> 8), byte(claimed & 0xFF)}
	body = append(body, make([]byte, claimed*6)...) // every field id 0: refused at the second entry
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 20 {
		if _, _, err := DecodeForwardReadStored(body); err == nil {
			t.Fatal("a body whose fields do not ascend was accepted")
		}
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / 20; per > 64<<10 {
		t.Fatalf("refusing the body allocated %d bytes per call, want under 64 KiB", per)
	}
}
