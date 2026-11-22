package wire

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func u50Send(t *testing.T, c *u66Conn, req RingTopologyRequest) *transport.Frame {
	t.Helper()
	body, err := EncodeRingTopology(req)
	if err != nil {
		t.Fatal(err)
	}
	return c.send(transport.OpRingTopology, u66ID(), body)
}

func u50Decode(t *testing.T, frame *transport.Frame) RingTopologyReply {
	t.Helper()
	if frame.Opcode != transport.OpRingReply {
		t.Fatalf("answered %s %x, want RING_REPLY", frame.Opcode, frame.Payload)
	}
	reply, err := DecodeRingTopologyReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

// TestU50ASingleNodeAnswersOneMemberAndATableNamingItForEverySlot: [GUARD the reply is the node's
// committed view: its epoch and state_seq, the one ranked member, every slot owned by it].
func TestU50ASingleNodeAnswersOneMemberAndATableNamingItForEverySlot(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	reply := u50Decode(t, u50Send(t, c, RingTopologyRequest{}))
	view := env.node.topology.view.Load()
	if reply.Unchanged || reply.Epoch != int64(view.epoch) || reply.StateSeq != view.seq || reply.Epoch == 0 {
		t.Fatalf("reply %d/%d unchanged %v, node is at %d/%d", reply.Epoch, reply.StateSeq, reply.Unchanged, view.epoch, view.seq)
	}
	members := reply.Table.Members()
	if len(members) != 1 || members[0].ID != env.node.cfg.NodeID || members[0].State != cluster.MemberActive {
		t.Fatalf("members %+v, want only %s active", members, env.node.cfg.NodeID)
	}
	for slot := range uint16(8192) {
		owner, ok := reply.Table.Owner(slot, 1)
		if !ok || owner.ID != members[0].ID {
			t.Fatalf("slot %d is owned by %q (%v), want the one member", slot, owner.ID, ok)
		}
	}
	if len(reply.Databases) == 0 || reply.Databases[0].Name != u30iDB || reply.Databases[0].RF != 1 || reply.Databases[0].ID < 1 {
		t.Fatalf("databases %+v", reply.Databases)
	}
}

// TestU50AMatchingViewIsUnchangedAndEpochZeroIsNever: [GUARD unchanged needs a non-zero epoch and both
// numbers equal, and then no blob is sent].
func TestU50AMatchingViewIsUnchangedAndEpochZeroIsNever(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	first := u50Decode(t, u50Send(t, c, RingTopologyRequest{}))
	same := u50Decode(t, u50Send(t, c, RingTopologyRequest{Epoch: first.Epoch, StateSeq: first.StateSeq}))
	if !same.Unchanged || same.Table != nil || same.Databases != nil || same.Epoch != first.Epoch || same.StateSeq != first.StateSeq {
		t.Fatalf("a matching view answered %+v", same)
	}
	for _, req := range []RingTopologyRequest{{Epoch: first.Epoch, StateSeq: first.StateSeq - 1}, {Epoch: first.Epoch + 1, StateSeq: first.StateSeq}, {Epoch: 0, StateSeq: first.StateSeq}} {
		if got := u50Decode(t, u50Send(t, c, req)); got.Unchanged || got.Table == nil {
			t.Errorf("request %+v answered unchanged", req)
		}
	}
	if _, err := env.node.CreateDatabase(context.Background(), "later"); err != nil {
		t.Fatal(err)
	}
	moved := u50Decode(t, u50Send(t, c, RingTopologyRequest{Epoch: first.Epoch, StateSeq: first.StateSeq}))
	if moved.Unchanged || len(moved.Databases) != len(first.Databases)+1 || moved.StateSeq <= first.StateSeq {
		t.Fatalf("after a create the view %d/%d answered %+v", first.Epoch, first.StateSeq, moved)
	}
}

// TestU50AnySessionGetsMembersAndTableAndOnlyItsDatabases: [GUARD the databases are filtered by the session's
// roles while the members and the table go to every authenticated session].
func TestU50AnySessionGetsMembersAndTableAndOnlyItsDatabases(t *testing.T) {
	app, _, gate := u70Start(t)
	for _, tc := range []struct {
		user, grants string
		want         []string
	}{
		{"nobody", "", nil},
		{"reader", "read@vault", []string{"vault"}},
		{"both", "read@vault,write@" + u30iDB, []string{u30iDB, "vault"}},
		{"admin", "write@*", []string{u30iDB, "vault"}},
	} {
		c := u70Login(t, app, tc.user, tc.grants)
		reply := u50Decode(t, u50Send(t, c, RingTopologyRequest{}))
		if reply.Table == nil || len(reply.Table.Members()) != 1 {
			t.Errorf("%s: no members or table in %+v", tc.user, reply)
		}
		var got []string
		for _, database := range reply.Databases {
			got = append(got, database.Name)
		}
		if len(got) != len(tc.want) {
			t.Errorf("%s: databases %v, want %v", tc.user, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: databases %v, want %v", tc.user, got, tc.want)
			}
		}
	}
	if gate.askedCount() == 0 {
		t.Fatal("the handler never asked the authorizer")
	}
}

// TestU50ASessionTheAuthorizerRefusesIsDeniedAndLearnsNothing: [GUARD the class-only ask comes first and a
// refusal is PERMISSION_DENIED with no table].
func TestU50ASessionTheAuthorizerRefusesIsDeniedAndLearnsNothing(t *testing.T) {
	app, _, gate := u70Start(t)
	c := u70Login(t, app, "nobody", "")
	gate.denyTopology = true
	refusal := u66Refusal(t, u50Send(t, c, RingTopologyRequest{}), cluster.CodePermissionDenied)
	if refusal.Detail != nil {
		t.Errorf("the denial carries a detail %+v", refusal.Detail)
	}
}

// TestU50ABadRequestIsRefusedWithTheFormat: [GUARD a request that does not re-encode is INVALID_ARGUMENT].
func TestU50ABadRequestIsRefusedWithTheFormat(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	refusal := u66Refusal(t, c.send(transport.OpRingTopology, u66ID(), []byte{1, 2, 3}), cluster.CodeInvalidArgument)
	if refusal.Message == "" {
		t.Error("the refusal has no text")
	}
}

// TestU50EpochZeroIsNeverUnchanged: [GUARD a fresh cluster at epoch 0 and state_seq 0 does not answer
// unchanged to a driver that holds nothing and sends (0, 0)].
func TestU50EpochZeroIsNeverUnchanged(t *testing.T) {
	env := u66Start(t)
	held := env.node.topology.view.Load()
	fresh := *held
	fresh.epoch, fresh.seq = 0, 0
	env.node.topology.view.Store(&fresh)
	c := env.login(t)
	if reply := u50Decode(t, u50Send(t, c, RingTopologyRequest{})); reply.Unchanged || reply.Table == nil {
		t.Fatalf("a request that holds nothing was answered %+v", reply)
	}
}
