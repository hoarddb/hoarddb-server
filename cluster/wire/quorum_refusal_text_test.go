package wire

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// unfilled finds a placeholder the refusal path left in a text: <name>, as cluster/text.go writes them.
var unfilled = regexp.MustCompile(`<[a-z_0-9]+>`)

func quorumRefusalText(t *testing.T, err error) string {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeNoQuorum {
		t.Fatalf("want a NO_QUORUM refusal, got %v", err)
	}
	return coded.Message
}

func requireQuorumText(t *testing.T, text string) {
	t.Helper()
	if found := unfilled.FindString(text); found != "" {
		t.Errorf("the refusal still holds the placeholder %s: %s", found, text)
	}
	if !strings.Contains(text, "3 voters") {
		t.Errorf("the refusal does not say how many voters the cluster has: %s", text)
	}
	// The node knows only that its last confirmed round is old, not how many answered, so the
	// text must not print a count or a stand-in for one.
	for _, invented := range []string{"answered)", "too few"} {
		if strings.Contains(text, invented) {
			t.Errorf("the refusal prints %q for a figure the node does not have: %s", invented, text)
		}
	}
}

// TestAWriteRefusedForALostQuorumPrintsNoPlaceholder: the write path filled the voter count and
// stood in "too few" for the answered count it does not have.
func TestAWriteRefusedForALostQuorumPrintsNoPlaceholder(t *testing.T) {
	fx := newFixture(t)
	fx.topo.setMembers(3)
	fx.lease.quorumLost.Store(true)
	_, err := fx.write(testSlot, "p1")
	requireQuorumText(t, quorumRefusalText(t, err))
}

// TestAReadRefusedForALostQuorumPrintsNoPlaceholder is the quorum half of the read-gate refusal:
// the read path filled neither figure and printed "<v> voters (<n> answered)" as written.
func TestAReadRefusedForALostQuorumPrintsNoPlaceholder(t *testing.T) {
	world := newU36World(t)
	world.fx.topo.setMembers(3)
	world.fx.mustWrite(world.slot, "p1")
	world.fx.lease.quorumLost.Store(true)
	_, err := world.get("p1")
	requireQuorumText(t, quorumRefusalText(t, err))
}

// TestADroppingBucketRefusalPrintsNoSweepCount: the text promised "<n> of 8192 slots swept", and no
// path that raises it has the count (a dropping bucket on the read and write paths left the
// placeholder, and alter bucket printed "0 of 8192"), so the sentence does not name one.
func TestADroppingBucketRefusalPrintsNoSweepCount(t *testing.T) {
	world := newU36World(t)
	world.fx.topo.dbRow.Name = "app"
	world.fx.schema.name = "users"
	texts := map[string]string{
		"the shared text": cluster.CodeBucketDropping.Text(cluster.CondBucketDropping, map[string]string{"bucket": "users", "db": "app"}),
	}
	var coded *cluster.Error
	if errors.As(world.gate.refusal(cluster.CodeBucketDropping, ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket}), &coded) {
		texts["a read"] = coded.Message
	}
	if errors.As(world.fx.w.refusal(cluster.CodeBucketDropping, world.fx.request(world.slot, "p1"), cluster.Position{}, nil), &coded) {
		texts["a write"] = coded.Message
	}
	if len(texts) != 3 {
		t.Fatalf("collected %d texts, want 3", len(texts))
	}
	for path, text := range texts {
		if found := unfilled.FindString(text); found != "" {
			t.Errorf("%s: the dropping-bucket refusal prints the placeholder %s: %s", path, found, text)
		}
		if strings.Contains(text, "8192 slots swept") {
			t.Errorf("%s: the dropping-bucket refusal prints a sweep count nobody has: %s", path, text)
		}
	}
}
