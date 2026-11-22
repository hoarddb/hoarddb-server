package wire

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u1Admin is a fake UserAdmin that records each call with the request id it was given.
type u1Admin struct {
	mu       sync.Mutex
	calls    []string
	requests [][16]byte
	err      error
}

func (a *u1Admin) record(call string, request [16]byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, call)
	a.requests = append(a.requests, request)
	return a.err
}

func (a *u1Admin) change(call string, request [16]byte) (UserOutcome, error) {
	if err := a.record(call, request); err != nil {
		return UserOutcome{}, err
	}
	return UserOutcome{Revision: 21, Result: "done " + call}, nil
}

func (a *u1Admin) CreateUser(_ context.Context, request [16]byte, user RingUserRequest) (UserOutcome, error) {
	return a.change("create_user "+user.Name, request)
}
func (a *u1Admin) AlterUser(_ context.Context, request [16]byte, user RingUserRequest) (UserOutcome, error) {
	return a.change("alter_user "+user.Name, request)
}
func (a *u1Admin) DropUser(_ context.Context, request [16]byte, user RingUserRequest) (UserOutcome, error) {
	return a.change("drop_user "+user.Name, request)
}
func (a *u1Admin) GrantRole(_ context.Context, request [16]byte, user RingUserRequest) (UserOutcome, error) {
	return a.change("grant_role "+user.Name+" "+user.Grant.Role+"@"+user.Grant.DB, request)
}
func (a *u1Admin) RevokeRole(_ context.Context, request [16]byte, user RingUserRequest) (UserOutcome, error) {
	return a.change("revoke_role "+user.Name+" "+user.Grant.Role+"@"+user.Grant.DB, request)
}
func (a *u1Admin) ShowUsers(context.Context) (RingAdminUsers, error) {
	if err := a.record("show_users", [16]byte{}); err != nil {
		return RingAdminUsers{}, err
	}
	return RingAdminUsers{Revision: 5, DropSeq: 2, Users: []RingUserRow{u1Row()}}, nil
}
func (a *u1Admin) DescribeUser(_ context.Context, name string) (RingAdminUsers, error) {
	if err := a.record("describe_user "+name, [16]byte{}); err != nil {
		return RingAdminUsers{}, err
	}
	row := u1Row()
	return RingAdminUsers{DropSeq: 2, User: &row}, nil
}

// u1Serve answers one RING_ADMIN frame sent under id.
func u1Serve(t *testing.T, gate RingAuthorizer, id [16]byte, request RingAdminRequest, options ...RingOption) *transport.Frame {
	t.Helper()
	node, _ := u63Fresh(t)
	handler, err := NewRingHandler(node, u66AuthNext, gate, options...)
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRingAdmin(request)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := handler(context.Background(), transport.NewFrameWithID(transport.OpRingAdmin, id, body))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

var u1Classes = map[string]RingClass{AdminCreateUser: RingClassCreateUser, AdminAlterUser: RingClassAlterUser, AdminDropUser: RingClassDropUser,
	AdminGrantRole: RingClassGrantRole, AdminRevokeRole: RingClassRevokeRole, AdminShowUsers: RingClassShowUsers, AdminDescribeUser: RingClassDescribeUser}

// TestRingUserCommandsAskOnceClassOnlyAndPassTheFrameRequestID: [GUARD each user command is
// authorized once as its own class with no database, then reaches the UserAdmin method with the
// frame's own 16-byte request id (ring-user-store.md 4.3), and the reply carries the outcome].
func TestRingUserCommandsAskOnceClassOnlyAndPassTheFrameRequestID(t *testing.T) {
	for _, req := range u1Requests() {
		gate, admin := &c6Gate{}, &u1Admin{}
		id := [16]byte{0x0F, 0x55, byte(len(admin.calls))}
		reply := u75Reply(t, u1Serve(t, gate, id, req, WithUserAdmin(admin)))
		if len(gate.asked) != 1 || gate.asked[0] != (RingAccess{Class: u1Classes[req.Cmd]}) {
			t.Errorf("%s: asked %+v, want one class-only ask of 0x%02x", req.Cmd, gate.asked, u1Classes[req.Cmd])
		}
		if len(admin.calls) != 1 || !strings.HasPrefix(admin.calls[0], req.Cmd) {
			t.Fatalf("%s: the admin saw %q", req.Cmd, admin.calls)
		}
		if reply.Users == nil {
			t.Fatalf("%s: reply %+v", req.Cmd, reply)
		}
		switch req.Cmd {
		case AdminShowUsers:
			if reply.Users.DropSeq != 2 || len(reply.Users.Users) != 1 {
				t.Errorf("show_users reply %+v", reply.Users)
			}
		case AdminDescribeUser:
			if reply.Users.User == nil || reply.Users.DropSeq != 2 {
				t.Errorf("describe_user reply %+v", reply.Users)
			}
		default:
			if admin.requests[0] != id {
				t.Errorf("%s: the admin was given request id %x, want the frame's %x", req.Cmd, admin.requests[0], id)
			}
			if reply.Users.Name != req.User.Name || reply.Users.Revision != 21 || reply.Users.Result != "done "+admin.calls[0] {
				t.Errorf("%s: reply %+v", req.Cmd, reply.Users)
			}
		}
	}
}

// TestRingCreateUserNeedsUserAdmin: [GUARD a session the authorizer refuses gets one
// PERMISSION_DENIED text for all seven commands, naming the grant to give and the statement that
// gives it, and the UserAdmin is never called].
func TestRingCreateUserNeedsUserAdmin(t *testing.T) {
	const want = "this session cannot manage users: the user holds no user-manager grant on all databases. Nothing was done. " +
		"Ask an administrator to run 'grant role user-manager@* to <user>' (a grant on one database is not enough) if the user should have access"
	for _, req := range u1Requests() {
		admin := &u1Admin{}
		refusal := u66Refusal(t, c6Serve(t, &c6Gate{deny: true}, req, WithUserAdmin(admin)), cluster.CodePermissionDenied)
		if refusal.Message != want {
			t.Errorf("%s: refusal %q", req.Cmd, refusal.Message)
		}
		if len(admin.calls) != 0 {
			t.Errorf("%s: a denied session reached the admin: %q", req.Cmd, admin.calls)
		}
	}
}

// TestRingUserAdminDefaultRefuses: [GUARD a handler built without WithUserAdmin authorizes the
// command, then refuses it with ErrUserAdminUnavailable, for all seven].
func TestRingUserAdminDefaultRefuses(t *testing.T) {
	for _, req := range u1Requests() {
		gate := &c6Gate{}
		refusal := u66Refusal(t, c6Serve(t, gate, req), cluster.CodeInvalidArgument)
		if refusal.Message != ErrUserAdminUnavailable.Error() {
			t.Errorf("%s: refusal %q", req.Cmd, refusal.Message)
		}
		if len(gate.asked) != 1 {
			t.Errorf("%s: NoUserAdmin answered before the session was authorized (%d asks)", req.Cmd, len(gate.asked))
		}
	}
	var none NoUserAdmin
	if _, err := none.CreateUser(context.Background(), [16]byte{}, RingUserRequest{}); !errors.Is(err, ErrUserAdminUnavailable) {
		t.Errorf("NoUserAdmin.CreateUser returned %v", err)
	}
	if _, err := none.ShowUsers(context.Background()); !errors.Is(err, ErrUserAdminUnavailable) {
		t.Errorf("NoUserAdmin.ShowUsers returned %v", err)
	}
}

// TestRingUserAdminRefusalsReachTheCallerUnchanged: [GUARD a typed refusal of the UserAdmin is what
// the session sees, code and text].
func TestRingUserAdminRefusalsReachTheCallerUnchanged(t *testing.T) {
	text := `drop user "adaa" was not applied: there is no user "adaa". Run 'show users' to list the users.`
	admin := &u1Admin{err: &cluster.Error{Code: cluster.CodeNotFound, Message: text}}
	refusal := u66Refusal(t, c6Serve(t, u70AllowAll{}, RingAdminRequest{Cmd: AdminDropUser, User: &RingUserRequest{Name: "adaa"}}, WithUserAdmin(admin)), cluster.CodeNotFound)
	if refusal.Message != text {
		t.Errorf("refusal %q", refusal.Message)
	}
}

// TestAPeerSessionNeverReachesTheUserAdmin: [GUARD the server's authorizer refuses an internode
// session, so a user command from a member never reaches the UserAdmin].
func TestAPeerSessionNeverReachesTheUserAdmin(t *testing.T) {
	admin := &u1Admin{}
	u66Refusal(t, c6Serve(t, c6PeerGate{}, RingAdminRequest{Cmd: AdminShowUsers}, WithUserAdmin(admin)), cluster.CodePermissionDenied)
	if len(admin.calls) != 0 {
		t.Errorf("a peer session reached the admin: %q", admin.calls)
	}
}
