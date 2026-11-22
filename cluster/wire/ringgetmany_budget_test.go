package wire

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestGetManyDocumentLargerThanTheBudgetDoesNotFillTheFrame: one document whose answer is larger
// than the whole reply budget fits no frame and is refused alone; it keeps no room, so it must not
// make the keys after it answer "the reply was already full".
func TestGetManyDocumentLargerThanTheBudgetDoesNotFillTheFrame(t *testing.T) {
	const keys = 64
	budget := int64(64 << 10)
	world, _ := getManyPaddedWorld(t, budget, func(id string) int {
		if id == "k000" {
			return 100 << 10
		}
		return 100
	}, func(id string) time.Duration {
		if id == "k000" {
			return 0
		}
		return 20 * time.Millisecond
	})
	ids := make([]string, keys)
	for i := range ids {
		ids[i] = fmt.Sprintf("k%03d", i)
	}
	reply := world.serve(getManyReadsOf(t, world.near, ids...)...)
	if a := reply.Answers[0]; a.Error == nil || !strings.Contains(a.Error.Message, "larger than") {
		t.Fatalf("key 0 (100 KiB, budget 64 KiB) was not refused as larger than the budget: %+v", a)
	}
	for i, answer := range reply.Answers[1:] {
		if answer.Error != nil {
			t.Fatalf("key %d of small documents answered %q while the reply held %d of %d bytes", i+1, answer.Error.Message, getManyKeptBytes(t, reply), budget)
		}
	}
}
