package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The tests of the U1 review's findings (F1 to F4, the ceiling and the cluster composition).

// TestRingCreateIsFencedByADropThroughTheLegacyPath (F1): a drop made through the legacy user
// opcodes (AuthManager.DropUser, what OP_USER_DROP runs) moves the drop fence too, so a create
// carrying a fence read before it is refused and the dropped account does not come back.
func TestRingCreateIsFencedByADropThroughTheLegacyPath(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	stale := u1DropSeq(t, root)
	u1Do(t, root, u1Create(t, "carol", "carol-password-1", stale))
	if err := qs.core.auth.DropUser("carol"); err != nil {
		t.Fatal(err)
	}
	if fresh := u1DropSeq(t, root); fresh <= stale {
		t.Fatalf("a legacy drop left the fence at %d (read at %d)", fresh, stale)
	}
	refusal := ringAdminRefusal(t, u1Frame(t, root, u1Create(t, "carol", "carol-password-1", stale)), cluster.CodeConflict)
	if !strings.Contains(refusal.Message, "a user was dropped since the users were read") {
		t.Errorf("a create behind a legacy drop answered %q", refusal.Message)
	}
	if _, ok := qs.core.auth.UserInfo("carol"); ok {
		t.Fatal("the account dropped through the legacy path came back")
	}
}

// TestRingChangeUnderAZeroRequestIDIsRefused (F2): an all-zero frame id cannot tell a retry from
// another change, so every user change sent under it is refused, naming the fix, and nothing is
// changed; the reads need no id.
func TestRingChangeUnderAZeroRequestIDIsRefused(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	fence := u1DropSeq(t, root)
	for _, name := range []string{"a1", "a2"} {
		refusal := ringAdminRefusal(t, u1SendWithID(t, root, [16]byte{}, u1Create(t, name, "a-password-1", fence)), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "request id") || !strings.Contains(refusal.Message, "Nothing was changed") {
			t.Errorf("a create under a zero id answered %q", refusal.Message)
		}
		if _, ok := qs.core.auth.UserInfo(name); ok {
			t.Errorf("%s was created under a zero id", name)
		}
	}
	if frame := u1SendWithID(t, root, [16]byte{}, wire.RingAdminRequest{Cmd: wire.AdminShowUsers}); frame.Opcode != transport.OpRingReply {
		t.Errorf("show_users under a zero id answered %s", frame.Opcode)
	}
}

// TestRingDedupeIsBoundToTheCommandAndTheName (F2): a reused frame id answers "already applied"
// only for a retry of the same command on the same account; another command or another name
// under that id is applied.
func TestRingDedupeIsBoundToTheCommandAndTheName(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	id := [16]byte{0xB2, 0xB2, 7}
	fence := u1DropSeq(t, root)
	for _, req := range []wire.RingAdminRequest{
		u1Create(t, "a1", "a1-password-1", fence),
		u1Create(t, "a2", "a2-password-1", fence),
		{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "a1", Grant: wire.RingUserGrant{Role: roleReader, DB: ringComposeDB}}},
	} {
		if frame := u1SendWithID(t, root, id, req); frame.Opcode != transport.OpRingReply {
			t.Fatalf("%s %s answered %s %q", req.Cmd, req.User.Name, frame.Opcode, frame.Payload)
		}
	}
	if _, ok := qs.core.auth.UserInfo("a2"); !ok {
		t.Error("a create of another name under a reused id was answered as a retry and not applied")
	}
	if info, _ := qs.core.auth.UserInfo("a1"); len(info.Roles) != 1 {
		t.Errorf("a grant under the id of the create was not applied: %+v", info.Roles)
	}
}

// TestRingRetryRacingItsOriginalAnswersSuccess (F3): a retry that arrives while its original is
// still being applied waits for it and answers success, never "exists" or "not found".
func TestRingRetryRacingItsOriginalAnswersSuccess(t *testing.T) {
	qs, _, _ := ringComposeServer(t)
	addr := qs.tcpListener.Addr().String()
	sessions := [2]*tcpServerClient{dialTCPServer(t, addr), dialTCPServer(t, addr)}
	for _, session := range sessions {
		session.login()
	}
	fence := u1DropSeq(t, sessions[0])
	for round := range 12 {
		name := fmt.Sprintf("racer%02d", round)
		create := u1Create(t, name, "racer-password-1", fence)
		drop := wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: name}}
		for step, req := range []wire.RingAdminRequest{create, drop} {
			id := [16]byte{0xC3, byte(round), byte(step), 1}
			var frames [2]*transport.Frame
			var wait sync.WaitGroup
			for i := range sessions {
				wait.Add(1)
				go func() {
					defer wait.Done()
					frames[i] = u1SendWithIDQuiet(sessions[i], id, req)
				}()
			}
			wait.Wait()
			for i, frame := range frames {
				if frame == nil || frame.Opcode != transport.OpRingReply {
					t.Fatalf("round %d, %s, session %d: a retry racing its original answered %v", round, req.Cmd, i, u1Describe(frame))
				}
			}
		}
		fence = u1DropSeq(t, sessions[0])
	}
}

// u1SendWithIDQuiet is u1SendWithID for a goroutine: it returns nil instead of failing the test.
func u1SendWithIDQuiet(c *tcpServerClient, id [16]byte, req wire.RingAdminRequest) *transport.Frame {
	body, err := wire.EncodeRingAdmin(req)
	if err != nil {
		return nil
	}
	if _, err := c.conn.Write(transport.NewFrameWithID(transport.OpRingAdmin, id, body).Encode()); err != nil {
		return nil
	}
	frame, err := transport.DecodeFrame(c.br)
	if err != nil {
		return nil
	}
	return frame
}

func u1Describe(frame *transport.Frame) string {
	if frame == nil {
		return "nothing"
	}
	if frame.Opcode == transport.OpError {
		if refusal, err := wire.DecodeRingError(frame.Payload); err == nil {
			return fmt.Sprintf("%s %q", refusal.Code, refusal.Message)
		}
	}
	return frame.Opcode.String()
}

// TestARoleGrantBeyondTheCapIsRefusedWhenMade (F4): an account holds at most wire.MaxUserGrants
// grants; the grant, the role replacement or the create that would pass it is refused when it
// is made, with the fix, and show_users and describe_user keep answering.
func TestARoleGrantBeyondTheCapIsRefusedWhenMade(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	u1Do(t, root, u1Create(t, "many", "many-password-1", u1DropSeq(t, root)))
	for i := range wire.MaxUserGrants {
		u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "many", Grant: wire.RingUserGrant{Role: roleReader, DB: fmt.Sprintf("db%03d", i)}}})
	}
	refusal := ringAdminRefusal(t, u1Frame(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole,
		User: &wire.RingUserRequest{Name: "many", Grant: wire.RingUserGrant{Role: roleReader, DB: "one-too-many"}}}), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, fmt.Sprint(wire.MaxUserGrants)) || !strings.Contains(refusal.Message, "revoke role") {
		t.Errorf("the grant past the cap answered %q", refusal.Message)
	}
	if listed := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowUsers}); len(listed.Users.Users) != 2 {
		t.Errorf("show_users lists %d users", len(listed.Users.Users))
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "many"}})
	tooMany := make([]Grant, wire.MaxUserGrants+1)
	for i := range tooMany {
		tooMany[i] = Grant{Role: roleReader, DB: fmt.Sprintf("other%03d", i)}
	}
	if _, err := qs.core.auth.AlterUser(AlterSpec{Username: "many", Version: qs.core.auth.Version(), Grants: &tooMany}); err == nil {
		t.Error("a role replacement past the cap was applied")
	}
	material, err := NewCredential("cap-password-1", DefaultArgon2Params())
	if err != nil {
		t.Fatal(err)
	}
	if err := qs.core.auth.CreateUser("capped", material, tooMany, true); err == nil {
		t.Error("a create past the cap was applied")
	}
}

// TestRingChangesRespectTheGrantorCeiling: a user-manager on every database who is not root may
// not change root, give root, or give a role it does not hold, through any of the commands.
func TestRingChangesRespectTheGrantorCeiling(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	rootName := qs.core.config.Server.RootUser
	manager := ringAdminUser(t, qs, "manager", Grant{Role: roleUserManager, DB: "*"})
	ringAdminUser(t, qs, "target")
	password, err := NewRingCredential("a-new-password-1")
	if err != nil {
		t.Fatal(err)
	}
	off := false
	rootGrant := wire.RingUserGrant{Role: roleRoot, DB: "*"}
	rootOnly := []wire.RingUserGrant{rootGrant}
	cases := []wire.RingAdminRequest{
		{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: rootName, Credential: password}},
		{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: rootName, Enabled: &off}},
		{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "target", Roles: &rootOnly}},
		{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: rootName}},
		{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: rootName, Grant: rootGrant}},
		{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "manager", Grant: rootGrant}},
		{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "target", Grant: wire.RingUserGrant{Role: roleReader, DB: ringComposeDB}}},
		u1Create(t, "eve", "eve-password-1", u1DropSeq(t, root), rootGrant),
	}
	for _, req := range cases {
		refusal := ringAdminRefusal(t, u1Frame(t, manager, req), cluster.CodePermissionDenied)
		if !strings.Contains(refusal.Message, "permission denied") {
			t.Errorf("%s %s: %q", req.Cmd, req.User.Name, refusal.Message)
		}
	}
	if info, _ := qs.core.auth.UserInfo("target"); len(info.Roles) != 0 {
		t.Errorf("target gained %+v", info.Roles)
	}
}

// TestRingUserRefusalsClipTheName: a refusal of the binding repeats at most 64 bytes of the
// account name it was sent.
func TestRingUserRefusalsClipTheName(t *testing.T) {
	_, _, root := ringComposeServer(t)
	long := strings.Repeat("n", 1000)
	for _, req := range []wire.RingAdminRequest{
		{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: long}},
		{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: long}},
	} {
		refusal := ringAdminRefusal(t, u1Frame(t, root, req), cluster.CodeNotFound)
		if len(refusal.Message) > 400 {
			t.Errorf("%s of a 1000-byte name answered %d bytes", req.Cmd, len(refusal.Message))
		}
	}
}
