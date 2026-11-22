package server

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u1Create is a create_user request for name with password, fenced at dropSeq.
func u1Create(t *testing.T, name, password string, dropSeq int64, grants ...wire.RingUserGrant) wire.RingAdminRequest {
	t.Helper()
	credential, err := NewRingCredential(password)
	if err != nil {
		t.Fatal(err)
	}
	if grants == nil {
		grants = []wire.RingUserGrant{}
	}
	return wire.RingAdminRequest{Cmd: wire.AdminCreateUser, User: &wire.RingUserRequest{Name: name, ExpectDropSeq: dropSeq, Roles: &grants, Credential: credential}}
}

// u1DropSeq reads the drop fence the way a client must: from show_users.
func u1DropSeq(t *testing.T, c *tcpServerClient) int64 {
	t.Helper()
	return u1Do(t, c, wire.RingAdminRequest{Cmd: wire.AdminShowUsers}).Users.DropSeq
}

// u1Next numbers the frames these tests send: ringSend derives its id from the payload's length,
// so two changes of the same length would share an id and the second would be a dedupe retry.
var u1Next atomic.Uint64

// u1Frame sends one RING_ADMIN command under a fresh request id and returns the answer.
func u1Frame(t *testing.T, c *tcpServerClient, req wire.RingAdminRequest) *transport.Frame {
	t.Helper()
	var id [16]byte
	binary.BigEndian.PutUint64(id[:8], 0x5531)
	binary.BigEndian.PutUint64(id[8:], u1Next.Add(1))
	return u1SendWithID(t, c, id, req)
}

// u1Do sends one RING_ADMIN command under a fresh request id and returns its reply.
func u1Do(t *testing.T, c *tcpServerClient, req wire.RingAdminRequest) wire.RingAdminReply {
	t.Helper()
	frame := u1Frame(t, c, req)
	if frame.Opcode != transport.OpRingReply {
		t.Fatalf("%s was answered %s %q, want RING_REPLY", req.Cmd, frame.Opcode, frame.Payload)
	}
	reply, err := wire.DecodeRingAdminReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

// u1SendWithID sends one RING_ADMIN command under a chosen frame request id.
func u1SendWithID(t *testing.T, c *tcpServerClient, id [16]byte, req wire.RingAdminRequest) *transport.Frame {
	t.Helper()
	body, err := wire.EncodeRingAdmin(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := c.conn.Write(transport.NewFrameWithID(transport.OpRingAdmin, id, body).Encode()); err != nil {
		t.Fatal(err)
	}
	frame, err := transport.DecodeFrame(c.br)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func u1Login(t *testing.T, qs *Service, name, password string) *tcpServerClient {
	t.Helper()
	c := dialTCPServer(t, qs.tcpListener.Addr().String())
	c.loginAs(name, password)
	return c
}

// TestRingUserCommandsChangeTheAuthManager: on a zero-config node a user made over the ring logs
// in, gains and loses a grant, and is gone after a drop; the list and describe reads agree.
func TestRingUserCommandsChangeTheAuthManager(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	created := u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root), wire.RingUserGrant{Role: roleReader, DB: ringComposeDB}))
	if created.Users.Name != "ada" || created.Users.Revision < 1 || !strings.Contains(created.Users.Result, "ada") {
		t.Fatalf("create reply %+v", created.Users)
	}
	u1Login(t, qs, "ada", "ada-password-1")
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: roleWriter, DB: ringComposeDB}}})
	described := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}).Users.User
	if len(described.Roles) != 2 || !described.Enabled || described.CreatedMs <= 0 {
		t.Fatalf("describe after grant %+v", described)
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: roleWriter, DB: ringComposeDB}}})
	// A revoke of a grant not held succeeds and changes nothing (ring-user-store.md 4.2, B12).
	before := qs.core.auth.Version()
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: roleWriter, DB: ringComposeDB}}})
	if qs.core.auth.Version() != before {
		t.Errorf("a revoke of a grant not held moved the store from %d to %d", before, qs.core.auth.Version())
	}
	newPassword, err := NewRingCredential("ada-password-2")
	if err != nil {
		t.Fatal(err)
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Credential: newPassword}})
	u1Login(t, qs, "ada", "ada-password-2")
	listed := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowUsers}).Users
	if len(listed.Users) != 2 || listed.Users[0].Name != "ada" || listed.Revision != int64(qs.core.auth.Version()) {
		t.Fatalf("show_users %+v", listed)
	}
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}})
	if _, ok := qs.core.auth.UserInfo("ada"); ok {
		t.Fatal("ada is still in the user store after drop_user")
	}
	refusal := ringAdminRefusal(t, u1Frame(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}), cluster.CodeNotFound)
	if !strings.Contains(refusal.Message, "'show users'") {
		t.Errorf("describe of a dropped user: %q", refusal.Message)
	}
	refusal = ringAdminRefusal(t, u1Frame(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "adaa"}}), cluster.CodeNotFound)
	if refusal.Message != `drop user "adaa" was not applied: there is no user "adaa". Run 'show users' to list the users.` {
		t.Errorf("drop of an absent name: %q", refusal.Message)
	}
}

// TestRingCreateUserNeedsUserAdminOnTheServer: the seven user classes need user-manager (or root)
// on every database; a grant on one database, a reader and a cluster-manager are denied with the
// text that names the grant to give.
func TestRingCreateUserNeedsUserAdminOnTheServer(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	for _, session := range []*tcpServerClient{
		ringAdminUser(t, qs, "reader1", Grant{Role: roleReader, DB: "*"}),
		ringAdminUser(t, qs, "scoped", Grant{Role: roleUserManager, DB: ringComposeDB}),
		ringAdminUser(t, qs, "clusterman", Grant{Role: roleClusterManager, DB: "*"}),
	} {
		for _, req := range []wire.RingAdminRequest{
			u1Create(t, "eve", "eve-password-1", u1DropSeq(t, root)),
			{Cmd: wire.AdminShowUsers},
			{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "root"}},
			{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "reader1"}},
		} {
			refusal := ringAdminRefusal(t, u1Frame(t, session, req), cluster.CodePermissionDenied)
			if !strings.Contains(refusal.Message, "'grant role user-manager@* to <user>'") {
				t.Errorf("%s: denial %q does not name the grant", req.Cmd, refusal.Message)
			}
		}
	}
	if _, ok := qs.core.auth.UserInfo("eve"); ok {
		t.Fatal("a denied create made the account")
	}
	manager := ringAdminUser(t, qs, "manager", Grant{Role: roleUserManager, DB: "*"})
	u1Do(t, manager, u1Create(t, "eve", "eve-password-1", u1DropSeq(t, manager)))
}

// TestRingDropUserRefusesTheLastAdminAndNamesTheFix: the bootstrap root is the only account that
// can manage users, so dropping or disabling it is refused CONFLICT with the fix.
func TestRingDropUserRefusesTheLastAdminAndNamesTheFix(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	name := qs.core.config.Server.RootUser
	off := false
	for _, req := range []wire.RingAdminRequest{
		{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: name}},
		{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: name, Enabled: &off}},
	} {
		refusal := ringAdminRefusal(t, u1Frame(t, root, req), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "last enabled account that can manage users") || !strings.Contains(refusal.Message, "'grant role user-manager@* to ops'") {
			t.Errorf("%s: %q", req.Cmd, refusal.Message)
		}
	}
	if _, ok := qs.core.auth.UserInfo(name); !ok {
		t.Fatal("the last administrator was dropped")
	}
}

// TestRingGrantRespectsTheGrantorCeiling: a user-manager@* who is not root cannot grant root.
func TestRingGrantRespectsTheGrantorCeiling(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	ringAdminUser(t, qs, "target")
	manager := ringAdminUser(t, qs, "manager", Grant{Role: roleUserManager, DB: "*"})
	ringAdminRefusal(t, u1Frame(t, manager, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "target", Grant: wire.RingUserGrant{Role: roleRoot, DB: "*"}}}), cluster.CodePermissionDenied)
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "target", Grant: wire.RingUserGrant{Role: roleRoot, DB: "*"}}})
}

// TestRingUserRequestIDIsDeduped: the zero-config binding remembers an applied change by the
// frame's 16-byte request id; a retry under it is answered success and applied once, even with
// another body (a driver retry draws a new salt; ring-user-store.md 4.3, B12).
func TestRingUserRequestIDIsDeduped(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	id := [16]byte{0xD1, 0xD2, 1}
	fence := u1DropSeq(t, root)
	first := u1SendWithID(t, root, id, u1Create(t, "ada", "ada-password-1", fence))
	if first.Opcode != transport.OpRingReply {
		t.Fatalf("first create answered %s %q", first.Opcode, first.Payload)
	}
	version := qs.core.auth.Version()
	again := u1SendWithID(t, root, id, u1Create(t, "ada", "ada-password-2", fence))
	if again.Opcode != transport.OpRingReply {
		t.Fatalf("the retry answered %s %q, want success", again.Opcode, again.Payload)
	}
	reply, err := wire.DecodeRingAdminReply(again.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reply.Users.Result, "already applied") || qs.core.auth.Version() != version {
		t.Errorf("the retry reply %+v moved the store from %d to %d", reply.Users, version, qs.core.auth.Version())
	}
	u1Login(t, qs, "ada", "ada-password-1")
	other := u1SendWithID(t, root, [16]byte{0xD1, 0xD2, 2}, u1Create(t, "ada", "ada-password-2", fence))
	ringAdminRefusal(t, other, cluster.CodeUserExists)
}

// TestRingCreateIsFencedByTheLastDrop: a create carrying a drop fence read before a later drop is
// refused, whatever the name; one carrying the current fence is applied; a create of a name that
// exists answers "exists" before the fence is judged.
func TestRingCreateIsFencedByTheLastDrop(t *testing.T) {
	_, _, root := ringComposeServer(t)
	stale := u1DropSeq(t, root)
	u1Do(t, root, u1Create(t, "carol", "carol-password-1", stale))
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "carol"}})
	refusal := ringAdminRefusal(t, u1Frame(t, root, u1Create(t, "bob", "bob-password-1", stale)), cluster.CodeConflict)
	if !strings.Contains(refusal.Message, "a user was dropped since the users were read") || !strings.Contains(refusal.Message, "'user describe bob'") {
		t.Errorf("stale fence: %q", refusal.Message)
	}
	fresh := u1DropSeq(t, root)
	if fresh <= stale {
		t.Fatalf("the drop fence did not rise: %d then %d", stale, fresh)
	}
	u1Do(t, root, u1Create(t, "bob", "bob-password-1", fresh))
	refusal = ringAdminRefusal(t, u1Frame(t, root, u1Create(t, "bob", "bob-password-2", stale)), cluster.CodeUserExists)
	if want := cluster.CodeUserExists.Text(cluster.CondUserExists, map[string]string{"user": "bob"}); refusal.Message != want {
		t.Errorf("an existing name with a stale fence answered %q, want %q", refusal.Message, want)
	}
}

// TestRingChangeWithAStaleRevisionIsRefused: expect_seq that is not the current revision refuses
// alter, drop, grant and revoke with the re-read text; the current one is applied.
func TestRingChangeWithAStaleRevisionIsRefused(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root)))
	stale := int64(qs.core.auth.Version()) - 1
	off := false
	grant := wire.RingUserGrant{Role: roleReader, DB: ringComposeDB}
	for _, req := range []wire.RingAdminRequest{
		{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &stale, Enabled: &off}},
		{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &stale, Grant: grant}},
		{Cmd: wire.AdminRevokeRole, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &stale, Grant: grant}},
		{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &stale}},
	} {
		refusal := ringAdminRefusal(t, u1Frame(t, root, req), cluster.CodeConflict)
		if !strings.Contains(refusal.Message, "the user store changed since this user was read") || !strings.Contains(refusal.Message, "'user describe ada'") {
			t.Errorf("%s: %q", req.Cmd, refusal.Message)
		}
	}
	current := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}).Users.User.Revision
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", ExpectSeq: &current, Grant: grant}})
}

// TestRingShowUsersNeverReturnsAHash: no user reply carries a salt, a hash or a key id, in any
// encoding.
func TestRingShowUsersNeverReturnsAHash(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	req := u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root))
	u1Do(t, root, req)
	record, ok := qs.core.auth.recordForTest("ada")
	if !ok {
		t.Fatal("ada was not created")
	}
	stored, err := base64.StdEncoding.DecodeString(record.Credentials.Hash)
	if err != nil {
		t.Fatal(err)
	}
	secrets := [][]byte{req.User.Credential.Salt, req.User.Credential.Hash, stored, []byte(record.Credentials.Hash), []byte(record.Credentials.Salt),
		[]byte("salt"), []byte("hash"), []byte("pepper")}
	for _, read := range []wire.RingAdminRequest{{Cmd: wire.AdminShowUsers}, {Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "ada"}}} {
		frame := u1Frame(t, root, read)
		for _, secret := range secrets {
			if bytes.Contains(frame.Payload, secret) {
				t.Errorf("%s reply carries %q", read.Cmd, secret)
			}
		}
	}
}

// u1Closed reports whether the node closed the connection: a read with nothing sent ends in an
// error other than the deadline. A session refused at its next request but left open is not closed.
func u1Closed(c *tcpServerClient) bool {
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := c.br.ReadByte()
	if err == nil {
		return false
	}
	var timeout net.Error
	return !errors.As(err, &timeout) || !timeout.Timeout()
}

// TestDisablingAUserOverTheRingClosesItsSessions: a disable, a new password and a drop each end
// the account's open sessions on the node (the limitations page of the user docs); a role change does not.
func TestDisablingAUserOverTheRingClosesItsSessions(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root), wire.RingUserGrant{Role: roleReader, DB: ringComposeDB}))
	session := u1Login(t, qs, "ada", "ada-password-1")
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: roleWriter, DB: ringComposeDB}}})
	if !session.served() {
		t.Fatal("a grant ended the session")
	}
	off := false
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Enabled: &off}})
	if !u1Closed(session) {
		t.Fatal("the connection of a disabled account was not closed by the node")
	}
	on := true
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Enabled: &on}})
	session = u1Login(t, qs, "ada", "ada-password-1")
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}})
	if !u1Closed(session) {
		t.Fatal("the connection of a dropped account was not closed by the node")
	}
}

// TestAUserChangeInTheInstantAfterItsLoginClosesTheConnection: the account is disabled, given a
// new password or dropped after the node answered its login and before the node registered the
// session, so the change's own close finds nothing to close. The node must still close that
// connection by itself: the client sends nothing more, and "these changes close the user's
// sessions" (the user docs) has no window in which it is false.
func TestAUserChangeInTheInstantAfterItsLoginClosesTheConnection(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	u1ChangeInsideTheLoginWindow(t, qs, root, qs.tcpListener.SetAfterAuthOKTestHook)
}

// TestAUserChangeJustBeforeItsSessionIsRegisteredClosesTheConnection: the same changes, placed
// inside the registration itself: after anything the node checked ahead of it and before the
// session is recorded. A node that looked at the login's standing before registering has already
// looked, and the change's close finds nothing, so only a look after the registration closes it.
func TestAUserChangeJustBeforeItsSessionIsRegisteredClosesTheConnection(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	u1ChangeInsideTheLoginWindow(t, qs, root, qs.tcpListener.SetBeforeRegistrationTestHook)
}

// u1ChangeInsideTheLoginWindow logs ada in, holds the login's dispatch at the seam setHook
// installs until a disable, a new password or a drop has been answered, and requires the node to
// close the connection with nothing sent on it.
func u1ChangeInsideTheLoginWindow(t *testing.T, qs *Service, root *tcpServerClient, setHook func(func())) {
	t.Helper()
	u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root), wire.RingUserGrant{Role: roleReader, DB: ringComposeDB}))
	off, on := false, true
	newPassword, err := NewRingCredential("ada-password-2")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		password string
		change   wire.RingAdminRequest
		undo     *wire.RingAdminRequest
	}{
		{"disable", "ada-password-1",
			wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Enabled: &off}},
			&wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Enabled: &on}}},
		{"new password", "ada-password-1",
			wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Credential: newPassword}}, nil},
		{"drop", "ada-password-2",
			wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}}, nil},
	}
	for _, c := range cases {
		// The login's dispatch waits at the seam until the change has been answered.
		inWindow := make(chan struct{})
		changed := make(chan struct{})
		var once, release sync.Once
		answered := func() { release.Do(func() { close(changed) }) }
		t.Cleanup(answered)
		setHook(func() {
			once.Do(func() {
				close(inWindow)
				<-changed
			})
		})
		session := u1Login(t, qs, "ada", c.password)
		select {
		case <-inWindow:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: the login never reached the seam after its AUTH_OK", c.name)
		}
		u1Do(t, root, c.change)
		answered()
		if !u1Closed(session) {
			t.Fatalf("%s: a connection whose login was answered just before the change was left open by the node", c.name)
		}
		setHook(nil)
		if c.undo != nil {
			u1Do(t, root, *c.undo)
		}
	}
}

// TestARingUserChangeIsAudited: each change is one audit line naming the account, who made it
// and, for an alter, what changed.
func TestARingUserChangeIsAudited(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) {
		cfg.Audit.Enabled = true
		cfg.Audit.Output = auditPath
	})
	root := dialTCPServer(t, addr)
	root.login()
	u1Do(t, root, u1Create(t, "ada", "ada-password-1", u1DropSeq(t, root)))
	off := false
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: &wire.RingUserRequest{Name: "ada", Enabled: &off}})
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminGrantRole, User: &wire.RingUserRequest{Name: "ada", Grant: wire.RingUserGrant{Role: roleReader, DB: "*"}}})
	u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: "ada"}})
	want := []string{`"event":"user.create"`, `"result":"changed: enabled"`, `"result":"grant reader@*"`, `"event":"user.drop"`}
	deadline := time.Now().Add(10 * time.Second)
	for {
		recorded, err := os.ReadFile(auditPath)
		if err != nil {
			t.Fatal(err)
		}
		missing := 0
		for _, line := range want {
			if !strings.Contains(string(recorded), line) {
				missing++
			}
		}
		if missing == 0 && strings.Count(string(recorded), `"target":"user:ada"`) == 4 {
			_ = qs
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the audit log lacks the four user lines: %s", recorded)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestEveryRingUserRefusalExitsAsItsRetryRuleSays: exit 8 tells a wrapper the outcome may change on
// a retry, so only a refusal that rerunning the statement can clear may carry a code that exits 8.
// A rerun reads its fences again, so a stale fence (a drop since the read, a stale revision) and a
// salt collision (a rerun draws a new salt) clear; a name that exists, the last administrator and
// the user limit never do.
func TestEveryRingUserRefusalExitsAsItsRetryRuleSays(t *testing.T) {
	cases := []struct {
		err  error
		code cluster.Code
	}{
		{ErrUserExists, cluster.CodeUserExists},
		{ErrLastAdmin, cluster.CodeInvalidArgument},
		{ErrTooManyUsers, cluster.CodeInvalidArgument},
		{ErrUserDroppedSince, cluster.CodeConflict},
		{ErrUserVersion, cluster.CodeConflict},
		{ErrSaltReused, cluster.CodeConflict},
		{ErrUserNotFound, cluster.CodeNotFound},
		{ErrPermissionDenied, cluster.CodePermissionDenied},
		{ErrUnknownRole, cluster.CodeInvalidArgument},
		{ErrTooManyGrants, cluster.CodeInvalidArgument},
		{ErrCredentialInvalid, cluster.CodeInvalidArgument},
		{ErrUsernameInvalid, cluster.CodeInvalidArgument},
	}
	retryClears := map[cluster.Code]bool{cluster.CodeConflict: true}
	for _, c := range cases {
		var coded *cluster.Error
		if !errors.As(ringUserRefusal(fmt.Errorf("create user \"ada\": %w", c.err)), &coded) || coded.Code != c.code {
			t.Errorf("%v: refused %v, want %s", c.err, coded, c.code)
			continue
		}
		if wantExit := map[bool]int{true: 8, false: 2}[retryClears[c.code]]; coded.Code.Exit() != wantExit {
			t.Errorf("%v: %s exits %d, want %d", c.err, coded.Code, coded.Code.Exit(), wantExit)
		}
	}
}

// TestTheZeroConfigStaleRevisionTextIsTrueWhenAnotherUserChanged: on a node without a cluster the
// revision is the store's, so a change to another user makes a read stale; the text must not
// claim this user changed.
func TestTheZeroConfigStaleRevisionTextIsTrueWhenAnotherUserChanged(t *testing.T) {
	_, _, root := ringComposeServer(t)
	u1Do(t, root, u1Create(t, "race2", "race2-password-1", u1DropSeq(t, root)))
	read := u1Do(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: "race2"}}).Users.User.Revision
	u1Do(t, root, u1Create(t, "race3", "race3-password-1", u1DropSeq(t, root)))
	off := false
	refusal := ringAdminRefusal(t, u1Frame(t, root, wire.RingAdminRequest{Cmd: wire.AdminAlterUser,
		User: &wire.RingUserRequest{Name: "race2", ExpectSeq: &read, Enabled: &off}}), cluster.CodeConflict)
	if strings.Contains(refusal.Message, "the user changed") || !strings.Contains(refusal.Message, "the user store changed since this user was read") {
		t.Fatalf("stale revision after a change to another user: %q", refusal.Message)
	}
}
