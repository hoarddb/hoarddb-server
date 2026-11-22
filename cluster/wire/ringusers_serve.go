package wire

import (
	"context"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the server end of the seven user commands: one class-only authorization ask, then
// the UserAdmin the server composed the handler with. What a node does with a user change is not
// decided here (server/ring_users.go binds it to the node's own users).

// UserOutcome is what a UserAdmin change reports on success: the account's revision after it and
// one printable line saying what was done.
type UserOutcome struct {
	Revision int64
	Result   string
}

// UserAdmin is what the server end of the user commands calls once the session is authorized.
// request is the RING frame's own 16-byte request id (ring-user-store.md 4.3): a change retried
// under it while it is remembered answers success and is applied once. A method returns a
// *cluster.Error, which the session sees unchanged, for a refusal it can explain; any other error
// becomes UNAVAILABLE with its text. An implementation must:
//
//   - check a present expect_seq against the account's revision, and take its own when absent;
//   - never fill create_user's expect_drop_seq from its own table: the client carries it, so a
//     retry carries the first attempt's value;
//   - apply the grantor's ceiling and the last-administrator guard;
//   - answer a revoke of a grant not held, and a grant already held, with success and no change;
//   - answer a drop of an absent name NOT_FOUND naming 'show users';
//   - never return a salt, a hash or a key id from ShowUsers or DescribeUser.
type UserAdmin interface {
	// CreateUser creates user.Name with user.Credential and *user.Roles, fenced by user.ExpectDropSeq.
	CreateUser(ctx context.Context, request [transport.RequestIDLen]byte, user RingUserRequest) (UserOutcome, error)
	// AlterUser changes the members of user that are set: Credential, Enabled, Roles.
	AlterUser(ctx context.Context, request [transport.RequestIDLen]byte, user RingUserRequest) (UserOutcome, error)
	// DropUser removes user.Name.
	DropUser(ctx context.Context, request [transport.RequestIDLen]byte, user RingUserRequest) (UserOutcome, error)
	// GrantRole adds user.Grant to user.Name.
	GrantRole(ctx context.Context, request [transport.RequestIDLen]byte, user RingUserRequest) (UserOutcome, error)
	// RevokeRole removes user.Grant from user.Name.
	RevokeRole(ctx context.Context, request [transport.RequestIDLen]byte, user RingUserRequest) (UserOutcome, error)
	// ShowUsers returns every account (Users non-nil), the store's revision and user_drop_seq.
	ShowUsers(ctx context.Context) (RingAdminUsers, error)
	// DescribeUser returns one account (User) and user_drop_seq; an absent name is NOT_FOUND.
	DescribeUser(ctx context.Context, name string) (RingAdminUsers, error)
}

// ErrUserAdminUnavailable is what NoUserAdmin refuses every call with: INVALID_ARGUMENT (exit 2).
var ErrUserAdminUnavailable error = &cluster.Error{Code: cluster.CodeInvalidArgument,
	Message: "user administration is not served by this ring handler: it was built without a user store. Nothing was changed. Send the request to a node started by hoarddb-server"}

// NoUserAdmin is the UserAdmin of a handler built without WithUserAdmin: it refuses every call
// with ErrUserAdminUnavailable and does nothing else.
type NoUserAdmin struct{}

// CreateUser refuses with ErrUserAdminUnavailable.
func (NoUserAdmin) CreateUser(context.Context, [transport.RequestIDLen]byte, RingUserRequest) (UserOutcome, error) {
	return UserOutcome{}, ErrUserAdminUnavailable
}

// AlterUser refuses with ErrUserAdminUnavailable.
func (NoUserAdmin) AlterUser(context.Context, [transport.RequestIDLen]byte, RingUserRequest) (UserOutcome, error) {
	return UserOutcome{}, ErrUserAdminUnavailable
}

// DropUser refuses with ErrUserAdminUnavailable.
func (NoUserAdmin) DropUser(context.Context, [transport.RequestIDLen]byte, RingUserRequest) (UserOutcome, error) {
	return UserOutcome{}, ErrUserAdminUnavailable
}

// GrantRole refuses with ErrUserAdminUnavailable.
func (NoUserAdmin) GrantRole(context.Context, [transport.RequestIDLen]byte, RingUserRequest) (UserOutcome, error) {
	return UserOutcome{}, ErrUserAdminUnavailable
}

// RevokeRole refuses with ErrUserAdminUnavailable.
func (NoUserAdmin) RevokeRole(context.Context, [transport.RequestIDLen]byte, RingUserRequest) (UserOutcome, error) {
	return UserOutcome{}, ErrUserAdminUnavailable
}

// ShowUsers refuses with ErrUserAdminUnavailable.
func (NoUserAdmin) ShowUsers(context.Context) (RingAdminUsers, error) {
	return RingAdminUsers{}, ErrUserAdminUnavailable
}

// DescribeUser refuses with ErrUserAdminUnavailable.
func (NoUserAdmin) DescribeUser(context.Context, string) (RingAdminUsers, error) {
	return RingAdminUsers{}, ErrUserAdminUnavailable
}

// WithUserAdmin supplies the node-side behaviour of the user commands. Without it a handler
// serves NoUserAdmin.
func WithUserAdmin(admin UserAdmin) RingOption {
	return func(s *ringServer) {
		if admin != nil {
			s.users = admin
		}
	}
}

// ErrUsersDenied is the PERMISSION_DENIED answer for a user command: one text for every command
// and every cause, naming the grant to give.
var ErrUsersDenied error = &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot manage users: the user holds no user-manager grant on all databases. Nothing was done. " +
	"Ask an administrator to run 'grant role user-manager@* to <user>' (a grant on one database is not enough) if the user should have access"}

// ErrUserChangeNeedsRequestID refuses a user change sent under an all-zero frame request id: the
// server remembers applied changes by that id, so a zero id would make one change look like a
// retry of another. INVALID_ARGUMENT (exit 2).
var ErrUserChangeNeedsRequestID error = &cluster.Error{Code: cluster.CodeInvalidArgument,
	Message: "the user change was not applied: its request id is all zeros, so a retry could not be told from another change. Nothing was changed. " +
		"Send every user change under its own random 16-byte request id (ringclient draws one per call; pass WithRequestID only to retry a change whose answer was lost)"}

// serveUsers authorizes the command's class once, with no database, before it looks at anything
// else, then calls the UserAdmin with the frame's request id.
func (s *ringServer) serveUsers(ctx context.Context, request [transport.RequestIDLen]byte, req RingAdminRequest) (RingAdminReply, error) {
	class, _ := UserClassOf(req.Cmd)
	if err := s.authz.Authorize(ctx, RingAccess{Class: class}); err != nil {
		return RingAdminReply{}, ErrUsersDenied
	}
	if request == ([transport.RequestIDLen]byte{}) && req.Cmd != AdminShowUsers && req.Cmd != AdminDescribeUser {
		return RingAdminReply{}, ErrUserChangeNeedsRequestID
	}
	var listed RingAdminUsers
	var outcome UserOutcome
	var err error
	switch req.Cmd {
	case AdminShowUsers:
		listed, err = s.users.ShowUsers(ctx)
	case AdminDescribeUser:
		listed, err = s.users.DescribeUser(ctx, req.User.Name)
	case AdminCreateUser:
		outcome, err = s.users.CreateUser(ctx, request, *req.User)
	case AdminAlterUser:
		outcome, err = s.users.AlterUser(ctx, request, *req.User)
	case AdminDropUser:
		outcome, err = s.users.DropUser(ctx, request, *req.User)
	case AdminGrantRole:
		outcome, err = s.users.GrantRole(ctx, request, *req.User)
	default:
		outcome, err = s.users.RevokeRole(ctx, request, *req.User)
	}
	if err != nil {
		return RingAdminReply{}, err
	}
	if req.Cmd == AdminShowUsers || req.Cmd == AdminDescribeUser {
		return RingAdminReply{Cmd: req.Cmd, Users: &listed}, nil
	}
	return RingAdminReply{Cmd: req.Cmd, Users: &RingAdminUsers{Name: req.User.Name, Revision: outcome.Revision, Result: outcome.Result}}, nil
}
