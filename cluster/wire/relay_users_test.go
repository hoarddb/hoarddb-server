package wire

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// usersRaftTest is a one-node Raft leader whose state holds three members (n1 the leader, data2 a
// joining data member, wit a witness, the last two registered as rows only) and a user table
// bootstrapped with root.
type usersRaftTest struct {
	t    *testing.T
	raft *cluster.Raft
	node *Node
	next byte
}

func newUsersRaftTest(t *testing.T) *usersRaftTest {
	t.Helper()
	raft := openDivergedRaft(t, t.TempDir())
	ut := &usersRaftTest{t: t, raft: raft, node: bareNode(raft)}
	for _, m := range []cluster.Member{{ID: "data2", Incarnation: 1, DataAddr: "data2:1"}, {ID: "wit", Incarnation: 1, DataAddr: "wit:1", Witness: true}} {
		ut.propose(cluster.CommandRegisterMember, cluster.RegisterMemberBody(m, make([]byte, 1024), cluster.BuildStateRange()))
	}
	ut.propose(cluster.CommandUserBootstrap, cluster.UserBootstrapBody{Request: ut.request(), Name: "root", Credential: ut.credential(), CreatedMs: 1}.Encode())
	return ut
}

func (ut *usersRaftTest) request() [cluster.UserRequestIDLen]byte {
	ut.next++
	var id [cluster.UserRequestIDLen]byte
	id[0], id[1] = 0xCD, ut.next
	return id
}

func (ut *usersRaftTest) credential() cluster.UserCredential {
	ut.next++
	salt := bytes.Repeat([]byte{0x11}, authrules.MinSaltLen)
	salt[0] = ut.next
	hash := bytes.Repeat([]byte{0x22}, authrules.Argon2HashLen)
	return cluster.UserCredential{Salt: salt, Hash: hash, Memory: authrules.MinArgon2MemoryKiB, Iterations: authrules.MinArgon2Iterations, Parallelism: 4, PepperKID: "0a1b2c3d"}
}

func (ut *usersRaftTest) propose(kind byte, body []byte) {
	ut.t.Helper()
	if _, err := ut.raft.Propose(cluster.Command{Version: cluster.CommandVersion, Kind: kind, BaseSeq: uint64(ut.raft.StateSeq()), Body: body}); err != nil {
		ut.t.Fatalf("kind 0x%02x: %v", kind, err)
	}
}

func (ut *usersRaftTest) row(name string) cluster.UserRow {
	ut.t.Helper()
	row, ok := ut.raft.Users().User(name)
	if !ok {
		ut.t.Fatalf("no user %q", name)
	}
	return row
}

func (ut *usersRaftTest) createBody(name string) cluster.UserCreateBody {
	return cluster.UserCreateBody{Request: ut.request(), By: "root", BySeq: ut.row("root").CreatedSeq, Name: name, ExpectDropSeq: ut.raft.Users().DropSeq, Credential: ut.credential(), Enabled: true, CreatedMs: 2}
}

func TestEveryUserKindIsRelayable(t *testing.T) {
	for _, kind := range []byte{cluster.CommandUserBootstrap, cluster.CommandUserCreate, cluster.CommandUserAlter, cluster.CommandUserDrop, cluster.CommandUserGrant, cluster.CommandUserRevoke} {
		if !relayable(kind) {
			t.Errorf("user kind 0x%02x is not relayable", kind)
		}
	}
	if relayable(0x17) {
		t.Error("0x17, which is no kind, is relayable")
	}
}

// TestUserKindsAreRelayedOnlyFromNonWitnessMembers (spec 6.1 step 4): the leader admits a relayed
// user command from a committed data member, joining included, and refuses it from a witness and
// from an id that is no member, before anything is appended. Break: drop the witness check in
// relayAuthority; red.
func TestUserKindsAreRelayedOnlyFromNonWitnessMembers(t *testing.T) {
	ut := newUsersRaftTest(t)
	ctx := context.Background()
	for _, sender := range []string{"wit", "stranger"} {
		req := transport.RelayProposal{Kind: cluster.CommandUserCreate, Body: ut.createBody("from-" + sender).Encode()}
		err := ut.node.relayAuthority(ctx, sender, req)
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodePermissionDenied {
			t.Fatalf("a user command relayed by %s: %v, want PERMISSION_DENIED", sender, err)
		}
	}
	before := ut.raft.StateSeq()
	reply := ut.node.serveRelay(ctx, transport.Handshake{NodeID: "wit"}, transport.RelayProposal{RequestID: [transport.RelayRequestIDLen]byte{9}, Kind: cluster.CommandUserCreate, Body: ut.createBody("w2").Encode()})
	if reply.Status != transport.RelayRefused || ut.raft.StateSeq() != before {
		t.Fatalf("serveRelay from the witness answered %+v and moved state_seq %d -> %d", reply, before, ut.raft.StateSeq())
	}
	for i, sender := range []string{"data2", "n1"} {
		reply := ut.node.serveRelay(ctx, transport.Handshake{NodeID: sender}, transport.RelayProposal{RequestID: [transport.RelayRequestIDLen]byte{byte(10 + i)}, Kind: cluster.CommandUserCreate, Body: ut.createBody("from-" + sender).Encode()})
		if reply.Status != transport.RelayApplied {
			t.Fatalf("a user command relayed by data member %s: %+v, want applied", sender, reply)
		}
	}
}

// TestCommittedEffectReportsAUserCommandDoneFromTheRequestList (spec 6.1 step 6, B1, B5, B14): a
// proposer that lost track of an attempt reads it as done when its request id is in the list,
// even after the row was dropped (a create) and for USER_BOOTSTRAP, and never for a request the
// list does not hold. Break: read the effect from the row instead of the list; red.
func TestCommittedEffectReportsAUserCommandDoneFromTheRequestList(t *testing.T) {
	ut := newUsersRaftTest(t)
	bootstrapRequest := ut.raft.Users().Requests[0].ID
	create := ut.createBody("ada")
	ut.propose(cluster.CommandUserCreate, create.Encode())
	createdAt := ut.raft.StateSeq()
	ut.propose(cluster.CommandUserDrop, cluster.UserDropBody{Request: ut.request(), By: "root", BySeq: ut.row("root").CreatedSeq, Name: "ada", ExpectSeq: ut.row("ada").ChangedSeq}.Encode())
	result, done := ut.node.committedEffect(cluster.CommandUserCreate, create.Encode())
	if !done || result.StateSeq != createdAt {
		t.Fatalf("committedEffect of the dropped account's create = %+v, %v; want done at state_seq %d", result, done, createdAt)
	}
	bootstrap := cluster.UserBootstrapBody{Request: bootstrapRequest, Name: "root", Credential: ut.credential()}
	if _, done := ut.node.committedEffect(cluster.CommandUserBootstrap, bootstrap.Encode()); !done {
		t.Fatal("a node whose USER_BOOTSTRAP committed does not see it as done")
	}
	if _, done := ut.node.committedEffect(cluster.CommandUserCreate, ut.createBody("never").Encode()); done {
		t.Fatal("a create that never committed shows an effect")
	}
	if _, done := ut.node.committedEffect(cluster.CommandUserCreate, bsonDoc(bsonStr("name", "x"))); done {
		t.Fatal("a body with no request id shows an effect")
	}
}

// TestUserKindsAreRelayedOnlyFromLiveMembers (review R2, spec 6.1 step 4 and the pepper rule of
// 5.3): who asked (by, by_seq) is the sender's claim, so the sender must be a member the cluster
// still counts: joining, active or down. A dead or leaving member, and a member removed while its
// control connection stays open (serveRelay does not re-admit), is refused before anything is
// appended. Break: drop the state check; red.
func TestUserKindsAreRelayedOnlyFromLiveMembers(t *testing.T) {
	ut := newUsersRaftTest(t)
	ctx := context.Background()
	setState := func(state string) {
		ut.propose(cluster.CommandSetMemberState, bsonDoc(bsonStr("id", "data2"), bsonStr("state", state)))
	}
	relay := func(name string) error {
		return ut.node.relayAuthority(ctx, "data2", transport.RelayProposal{Kind: cluster.CommandUserCreate, Body: ut.createBody(name).Encode()})
	}
	for _, state := range []string{"active", "down"} {
		setState(state)
		if err := relay("ok-" + state); err != nil {
			t.Fatalf("data2 %s: %v, want admitted", state, err)
		}
	}
	for _, state := range []string{"leaving", "dead"} {
		setState(state)
		err := relay("from-" + state)
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodePermissionDenied || !strings.Contains(refusal.Message, state) {
			t.Fatalf("data2 %s relaying USER_CREATE: %v, want PERMISSION_DENIED naming the state", state, err)
		}
	}
	ut.propose(cluster.CommandRemoveMember, bsonDoc(bsonStr("id", "data2")))
	before := ut.raft.StateSeq()
	reply := ut.node.serveRelay(ctx, transport.Handshake{NodeID: "data2"}, transport.RelayProposal{RequestID: [transport.RelayRequestIDLen]byte{0x42}, Kind: cluster.CommandUserCreate, Body: ut.createBody("removed").Encode()})
	if reply.Status != transport.RelayRefused || ut.raft.StateSeq() != before {
		t.Fatalf("a removed member's open connection relayed USER_CREATE: %+v (state_seq %d -> %d)", reply, before, ut.raft.StateSeq())
	}
}

// TestTheFollowerReadsUsersWithoutCloningTheState (spec 6.3, B16): the user table a node's
// follower reads after every commit comes through a narrow accessor that copies nothing, not
// the whole-state clone of State(). Break: read through Raft.State(); red (allocations).
func TestTheFollowerReadsUsersWithoutCloningTheState(t *testing.T) {
	ut := newUsersRaftTest(t)
	ut.propose(cluster.CommandUserCreate, ut.createBody("ada").Encode())
	var view cluster.UserView
	allocs := testing.AllocsPerRun(100, func() { view = ut.node.Users() })
	if allocs != 0 {
		t.Fatalf("reading the user table allocates %.0f times per read", allocs)
	}
	if _, ok := view.User("ada"); !ok || view.StateSeq != ut.raft.StateSeq() {
		t.Fatalf("the accessor's view: %+v", view)
	}
	if ut.node.UserTableChanged() != nil {
		t.Fatal("a node with no cluster has a user-table wake")
	}
}
