package wire

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestCreateDatabaseRefusalOnOneVoterReportsTheCommittedCounts: [GUARD a factor above what
// one voter can hold is still refused RF_UNSUPPORTED, and both the text and the detail carry
// the voter and data-member counts of the committed state].
func TestCreateDatabaseRefusalOnOneVoterReportsTheCommittedCounts(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	two := int32(2)
	refusal := u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "ha", RF: &two}), cluster.CodeRFUnsupported)
	state := env.node.State()
	voters, dataMembers := len(state.Voters), len(cluster.BuildTable(state).Members())
	if voters != 1 || dataMembers != 1 {
		t.Fatalf("the test node has %d voters and %d data members, want one of each", voters, dataMembers)
	}
	if refusal.Detail == nil {
		t.Fatalf("RF_UNSUPPORTED carried no detail: %s", refusal.Message)
	}
	if detail := *refusal.Detail; detail.Rf != 2 || detail.Voters != voters || detail.DataMembers != dataMembers {
		t.Errorf("detail %+v, want rf 2, %d voters and %d data members", detail, voters, dataMembers)
	}
	for _, want := range []string{"at least 3 voters", fmt.Sprintf("this cluster has %d", voters), "join", "create database ha"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("the refusal %q does not mention %q", refusal.Message, want)
		}
	}
	if _, ok := env.node.topology.DatabaseID("ha"); ok {
		t.Error("a refused create_database committed the database")
	}
}
