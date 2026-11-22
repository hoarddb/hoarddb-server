package wire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestExpandCapsTheDistinctFarReadsOfOneCall: [GUARD memory is O(page): above maxExpandReads
// distinct far documents the call is refused unread, with the number and the fix].
func TestExpandCapsTheDistinctFarReadsOfOneCall(t *testing.T) {
	world, _ := ownerSplitWorld(t, 1, 0, func(string) error { return nil })
	specs := []RingExpandSpec{{Field: "Author", Bucket: "User"}}
	rows := func(n int) [][]string {
		ids := make([][]string, n)
		for i := range ids {
			ids[i] = []string{fmt.Sprintf("far%d", i)}
		}
		return ids
	}
	if _, _, _, _, err := world.server.expandRows(context.Background(), world.near, specs, rows(maxExpandReads), false); err != nil {
		t.Fatalf("exactly %d distinct documents were refused: %v", maxExpandReads, err)
	}
	reads := world.server.node.ExpandStats().Reads.Load()
	_, _, _, _, err := world.server.expandRows(context.Background(), world.near, specs, rows(maxExpandReads+1), false)
	text := refusalText(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(text, fmt.Sprint(maxExpandReads+1)) || !strings.Contains(text, "smaller limit") {
		t.Fatalf("the refusal does not give the number and the fix: %s", text)
	}
	if got := world.server.node.ExpandStats().Reads.Load(); got != reads {
		t.Fatalf("the refused call read %d documents", got-reads)
	}
}
