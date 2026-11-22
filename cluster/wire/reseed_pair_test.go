package wire

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestReseedPairRefusesAPairWithNoRecord: ReseedPair is the call an operator or a test makes by
// hand, so a pair this node holds no record of is an error that names the pair and the command
// that shows what the node holds, not a silent no-op. A pair with a record still reseeds and
// returns nil. [BREAK: return nil for a pair with no record in ReseedPair]
func TestReseedPairRefusesAPairWithNoRecord(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	const slot, db = uint16(5), uint64(3)
	if state := node.SlotRecordState(slot, db); state != machine.SlotAbsent {
		t.Fatalf("the fresh node's record for slot %d of database %d is %v, want none", slot, db, state)
	}
	err := node.ReseedPair(slot, db)
	if err == nil {
		t.Fatal("ReseedPair of a pair with no record returned nil, want an error naming the pair")
	}
	for _, want := range []string{"slot 5", "database 3", "reseed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not contain %q", err, want)
		}
	}

	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	s0Insert(t, node, "u1")
	held := s0SlotOf(t, "u1")
	if err := node.ReseedPair(held, dbID); err != nil {
		t.Fatalf("ReseedPair of a pair with a record returned %v, want nil", err)
	}
	if state := node.SlotRecordState(held, dbID); state != machine.SlotSeeding {
		t.Fatalf("after ReseedPair the record is %v, want seeding", state)
	}
}
