package cluster

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/hoarddb/hoarddb-server/authrules"
)

// TestACreateBuiltBeforeALaterDropIsRefused: the create-side fence (owner ruling 2026-11-20, R3)
// refuses a create whose proposer read the users before a drop committed, even a drop of another
// name; read again, the same create applies. Break: drop the expect_drop_seq check; red.
func TestACreateBuiltBeforeALaterDropIsRefused(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "bob")
	stale := fx.createBody("root", "ada")
	fx.apply(CommandUserDrop, fx.dropBody("root", "bob").Encode())
	wantMention(t, fx.refuse(CommandUserCreate, stale.Encode()), CodeConflict, "dropped since this node read the users")
	fresh := stale
	fresh.Request, fresh.ExpectDropSeq = fx.req(), fx.st.UserDropSeq
	fx.apply(CommandUserCreate, fresh.Encode())
	if !fx.has("ada") {
		t.Fatal("a create read after the drop did not apply")
	}
}

// TestOnlyADropMovesTheDropSeq: user_drop_seq is the state_seq of the last committed USER_DROP;
// every other kind, user or not, leaves it. Break: move it on a revoke; red.
func TestOnlyADropMovesTheDropSeq(t *testing.T) {
	fx := newUserFixture(t)
	if fx.st.UserDropSeq != 0 {
		t.Fatalf("a state with no drop has user_drop_seq %d", fx.st.UserDropSeq)
	}
	fx.create("root", "ada", readerApp)
	for _, c := range []struct {
		kind byte
		body []byte
	}{
		{CommandUserGrant, fx.roleBody("root", "ada", writerApp).Encode()},
		{CommandUserCreate, fx.createBody("root", "bob").Encode()},
		{CommandSetSetting, fsmDoc(fsmStr("key", "heartbeat_ms"), fsmI64("value", 150))},
	} {
		fx.apply(c.kind, c.body)
		if fx.st.UserDropSeq != 0 {
			t.Fatalf("kind 0x%02x moved user_drop_seq to %d", c.kind, fx.st.UserDropSeq)
		}
	}
	fx.apply(CommandUserRevoke, fx.roleBody("root", "ada", writerApp).Encode())
	off := false
	disable := fx.alterBody("root", "ada")
	disable.Enabled = &off
	fx.apply(CommandUserAlter, disable.Encode())
	if fx.st.UserDropSeq != 0 {
		t.Fatalf("a revoke or an alter moved user_drop_seq to %d", fx.st.UserDropSeq)
	}
	fx.apply(CommandUserDrop, fx.dropBody("root", "ada").Encode())
	if fx.st.UserDropSeq != fx.st.StateSeq {
		t.Fatalf("after a drop user_drop_seq is %d, want the drop's state_seq %d", fx.st.UserDropSeq, fx.st.StateSeq)
	}
}

// TestRetriesNeverResurrectAnAccountOnAnyReplica is the reviewer's differential probe over the
// new field: three replicas (one restored from its own encoded state now and then) apply a random
// mix of user commands and retries of old bodies, from the last few up to anything earlier, and
// must answer each command alike, end byte-equal, and never apply a create whose request id was
// applied before (that is a resurrection, at whatever distance).
func TestRetriesNeverResurrectAnAccountOnAnyReplica(t *testing.T) {
	names := []string{"ada", "bob", "cy", "dee", "mgr"}
	grants := []authrules.Grant{readerApp, writerApp, managerAll, clusterAll, rootAll}
	for seed := int64(1); seed <= 4; seed++ {
		r := rand.New(rand.NewSource(seed))
		fx := newUserFixture(t)
		a, b, c := fx.st, fx.st, fx.st
		type sent struct {
			kind byte
			body []byte
		}
		var history []sent
		created := map[[UserRequestIDLen]byte]bool{}
		for step := 0; step < 3000; step++ {
			fx.st = a
			name := names[r.Intn(len(names))]
			expect := int64(0)
			if fx.has(name) {
				expect = fx.row(name).ChangedSeq
			}
			by, bySeq := "root", fx.seqOf("root")
			var cmd sent
			switch k := r.Intn(10); {
			case len(history) > 0 && k == 0:
				cmd = history[r.Intn(len(history))]
			case len(history) > 0 && k == 1:
				cmd = history[len(history)-1-r.Intn(min(len(history), 20))]
			case k < 4:
				cmd = sent{CommandUserCreate, UserCreateBody{Request: fx.req(), By: by, BySeq: bySeq, Name: name, ExpectDropSeq: a.UserDropSeq, Roles: []authrules.Grant{grants[r.Intn(len(grants))]}, Credential: fx.cred(), Enabled: true}.Encode()}
			case k < 6:
				cmd = sent{CommandUserDrop, UserDropBody{Request: fx.req(), By: by, BySeq: bySeq, Name: name, ExpectSeq: expect}.Encode()}
			case k < 8:
				cmd = sent{CommandUserGrant, UserRoleBody{Request: fx.req(), By: by, BySeq: bySeq, Name: name, ExpectSeq: expect, Grant: grants[r.Intn(len(grants))]}.Encode()}
			default:
				cmd = sent{CommandUserRevoke, UserRoleBody{Request: fx.req(), By: by, BySeq: bySeq, Name: name, ExpectSeq: expect, Grant: grants[r.Intn(len(grants))]}.Encode()}
			}
			history = append(history, cmd)
			var answers [3]string
			for i, st := range []*ClusterState{&a, &b, &c} {
				_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: cmd.kind, BaseSeq: uint64(st.StateSeq), Body: cmd.body})
				answers[i] = fmt.Sprint(err)
			}
			if answers[0] != answers[1] || answers[0] != answers[2] {
				t.Fatalf("seed %d step %d: replicas answered %q", seed, step, answers)
			}
			if cmd.kind == CommandUserCreate && answers[0] == "<nil>" {
				request, _ := UserCommandRequest(cmd.body)
				if created[request] {
					t.Fatalf("seed %d step %d: a create applied a second time under request %x", seed, step, request)
				}
				created[request] = true
			}
			if r.Intn(50) == 0 {
				restored, err := DecodeClusterState(fsmEncodedTB(t, b))
				if err != nil {
					t.Fatal(err)
				}
				b = restored
			}
		}
		if !bytes.Equal(fsmEncodedTB(t, a), fsmEncodedTB(t, b)) || !bytes.Equal(fsmEncodedTB(t, a), fsmEncodedTB(t, c)) {
			t.Fatalf("seed %d: the replicas ended in different states", seed)
		}
	}
}

// TestACreateBuiltBeforeTwoDropsIsRefused: the fence is an exact match, not a tolerance. A create
// read before two consecutive drops is refused, as is one read between them. Break: accept a fence
// one drop behind; red.
func TestACreateBuiltBeforeTwoDropsIsRefused(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "bob")
	fx.create("root", "cy")
	beforeBoth := fx.createBody("root", "ada")
	fx.apply(CommandUserDrop, fx.dropBody("root", "bob").Encode())
	between := fx.createBody("root", "dee")
	fx.apply(CommandUserDrop, fx.dropBody("root", "cy").Encode())
	wantMention(t, fx.refuse(CommandUserCreate, beforeBoth.Encode()), CodeConflict, "dropped since this node read the users")
	wantMention(t, fx.refuse(CommandUserCreate, between.Encode()), CodeConflict, "dropped since this node read the users")
}

// TestNameExistsIsJudgedBeforeTheDropFence pins the order the R3 note states: a create of a name
// that exists is answered "already exists" even when a drop committed since it was read, so a
// retry of a create that applied, after an unrelated drop, tells the person the account is there.
// Break: judge the drop fence before the name; red.
func TestNameExistsIsJudgedBeforeTheDropFence(t *testing.T) {
	fx := newUserFixture(t)
	fx.create("root", "bob")
	create := fx.createBody("root", "ada")
	fx.apply(CommandUserCreate, create.Encode())
	fx.apply(CommandUserDrop, fx.dropBody("root", "bob").Encode())
	fx.filler(authrules.RequestListSize)
	err := fx.refuse(CommandUserCreate, create.Encode())
	wantMention(t, err, CodeUserExists, "already exists")
}
