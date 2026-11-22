package ringclient

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestNearestResultOfRefusesAReplyWithoutAScorePerElement: the codec refuses such a reply, and the client does not
// rely on that alone, since the CLI indexes Scores by element.
func TestNearestResultOfRefusesAReplyWithoutAScorePerElement(t *testing.T) {
	zero := int32(0)
	reply := wire.RingReply{Elements: [][]byte{{5, 0, 0, 0, 0}, {5, 0, 0, 0, 0}}, Scanned: &zero, Skipped: &zero}
	_, err := nearestResultOf("Friends", reply)
	if err == nil || !strings.Contains(err.Error(), "the server answered a nearest without a score per element") {
		t.Fatalf("error = %v, want the missing-score refusal", err)
	}
}
