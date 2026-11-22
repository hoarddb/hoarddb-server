package harness_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// rfCreateAllowAll admits every admin command: these tests are about the replication factor,
// not grants.
type rfCreateAllowAll struct{}

func (rfCreateAllowAll) Authorize(context.Context, wire.RingAccess) error { return nil }

func rfCreateHandler(t *testing.T, c *harness.Cluster, id string) transport.RequestHandler {
	t.Helper()
	handler, err := wire.NewRingHandler(c.Node(id), func(context.Context, *transport.Frame) (*transport.Frame, error) {
		t.Error("an admin request was handed to the next handler")
		return nil, nil
	}, rfCreateAllowAll{})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

// rfCreateAsk sends one create_database through the node's RING_ADMIN handler.
func rfCreateAsk(t *testing.T, handler transport.RequestHandler, name string, rf int32) *transport.Frame {
	t.Helper()
	payload, err := wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: name, RF: &rf})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := handler(proofCtx(t), transport.NewFrameWithID(transport.OpRingAdmin, [16]byte{0xD2}, payload))
	if err != nil {
		t.Fatalf("create_database %q: the handler returned an error: %v", name, err)
	}
	return frame
}

// rfCreateRefusal decodes the OpError frame an admin request was answered with.
func rfCreateRefusal(t *testing.T, frame *transport.Frame) wire.RingError {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("answered %s %x, want an error", frame.Opcode, frame.Payload)
	}
	refusal, err := wire.DecodeRingError(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return refusal
}

// rfCreateRow finds database name in a committed state.
func rfCreateRow(state cluster.ClusterState, name string) (cluster.Database, bool) {
	for _, row := range state.Databases {
		if row.Name == name {
			return row, true
		}
	}
	return cluster.Database{}, false
}

// TestCreateDatabaseRF2ThroughTheAdminWireOnThreeVoters: [GUARD create_database with
// replication_factor 2 on a cluster of three voters succeeds, answers rf 2 and commits it].
func TestCreateDatabaseRF2ThroughTheAdminWireOnThreeVoters(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, NoProbes: true})
	handler := rfCreateHandler(t, c, c.AwaitLeader(10*time.Second))
	frame := rfCreateAsk(t, handler, "app", 2)
	if frame.Opcode != transport.OpRingReply {
		refusal := rfCreateRefusal(t, frame)
		t.Fatalf("create_database with replication_factor 2 on three voters was refused %s: %s", refusal.Code, refusal.Message)
	}
	reply, err := wire.DecodeRingAdminReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if reply.RF != 2 || reply.DatabaseID == 0 {
		t.Fatalf("create_database answered %+v, want a database id and rf 2", reply)
	}
	c.AwaitConverged(10 * time.Second)
	for _, id := range c.Running() {
		row, ok := rfCreateRow(c.Node(id).State(), "app")
		if !ok || row.RF != 2 {
			t.Errorf("node %s committed database app as %+v (found %v), want rf 2", id, row, ok)
		}
	}
}

// TestCreateDatabaseRF2OnTwoVotersIsRefused: [GUARD a factor above 1 needs at least three
// voters: two voters and two data members cannot hold rf 2, the refusal names the two
// voters, and the refused create commits no database].
func TestCreateDatabaseRF2OnTwoVotersIsRefused(t *testing.T) {
	c := harness.NewCluster(t, 2, harness.Options{NoDetection: true, NoProbes: true})
	leader := c.AwaitLeader(10 * time.Second)
	handler := rfCreateHandler(t, c, leader)
	refusal := rfCreateRefusal(t, rfCreateAsk(t, handler, "app", 2))
	if refusal.Code != cluster.CodeRFUnsupported {
		t.Fatalf("create_database with replication_factor 2 on two voters answered %s: %s, want RF_UNSUPPORTED", refusal.Code, refusal.Message)
	}
	if refusal.Detail == nil {
		t.Fatalf("RF_UNSUPPORTED carried no detail: %s", refusal.Message)
	}
	if detail := *refusal.Detail; detail.Rf != 2 || detail.Voters != 2 || detail.DataMembers != 2 {
		t.Errorf("detail %+v, want rf 2, 2 voters and 2 data members", detail)
	}
	for _, want := range []string{"at least 3 voters", "this cluster has 2", "join"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("the refusal %q does not mention %q", refusal.Message, want)
		}
	}
	if row, ok := rfCreateRow(c.Node(leader).State(), "app"); ok {
		t.Errorf("a refused create_database committed database app as %+v", row)
	}
}

// TestCreateDatabaseRefusalOnThreeVotersNamesTheRealCounts: [GUARD a factor the three voters
// cannot hold is refused with the voters and data members the committed state lists].
func TestCreateDatabaseRefusalOnThreeVotersNamesTheRealCounts(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, NoProbes: true})
	handler := rfCreateHandler(t, c, c.AwaitLeader(10*time.Second))
	refusal := rfCreateRefusal(t, rfCreateAsk(t, handler, "app", 4))
	if refusal.Code != cluster.CodeRFUnsupported {
		t.Fatalf("create_database with 4 copies answered %s: %s, want RF_UNSUPPORTED", refusal.Code, refusal.Message)
	}
	if refusal.Detail == nil {
		t.Fatalf("RF_UNSUPPORTED carried no detail: %s", refusal.Message)
	}
	if detail := *refusal.Detail; detail.Rf != 4 || detail.Voters != 3 || detail.DataMembers != 3 {
		t.Errorf("detail %+v, want rf 4, 3 voters and 3 data members", detail)
	}
	for _, want := range []string{"at least 4 data members", "this cluster has 3", "join", "create database app"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("the refusal %q does not mention %q", refusal.Message, want)
		}
	}
}
