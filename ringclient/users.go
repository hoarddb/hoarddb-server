package ringclient

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// The work factors a credential is derived with: the server's defaults (Argon2id, 64 MiB, three
// passes, four lanes, a 16-byte salt, a 32-byte hash), inside the window every server accepts.
const (
	credentialMemoryKiB   = 64 * 1024
	credentialIterations  = 3
	credentialParallelism = 4
	credentialSaltLen     = 16
	credentialHashLen     = 32
)

// UserGrant is one role on one database; DB "*" is every database.
type UserGrant struct {
	Role, DB string
}

// UserChange is what AlterUser changes: any of a new password, the enabled flag and the whole
// role list (an empty, non-nil list removes every grant). ExpectSeq, when set, is the revision the
// caller last read; the server refuses the change CONFLICT when the account has changed since.
type UserChange struct {
	Password  *string
	Enabled   *bool
	Roles     *[]UserGrant
	ExpectSeq *int64
}

// UserResult is the answer to a user change: the account's revision after it and the server's line.
type UserResult struct {
	Revision int64
	Result   string
}

// UserInfo is an account as the server reports it, never with credential material.
type UserInfo struct {
	Name    string
	Roles   []UserGrant
	Enabled bool
	// Epoch moves when the password changes or the account is disabled.
	Epoch           int64
	Created         time.Time
	PasswordChanged time.Time
	// Revision is what a later change of this account passes as ExpectSeq.
	Revision int64
}

// UserList is every account, the user store's revision and its drop fence.
type UserList struct {
	Users    []UserInfo
	Revision int64
	// DropSeq is what the next CreateUser passes as expectDropSeq.
	DropSeq int64
}

// UserDescription is one account and the store's drop fence.
type UserDescription struct {
	User    UserInfo
	DropSeq int64
}

// deriveCredential derives the credential of password with a fresh random salt. Only the result
// leaves the process.
func deriveCredential(password string) (*wire.RingUserCredential, error) {
	if password == "" {
		return nil, errors.New("ringclient: the password is empty. Pass the password the account will log in with")
	}
	salt := make([]byte, credentialSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("ringclient: drawing a salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt, credentialIterations, credentialMemoryKiB, credentialParallelism, credentialHashLen)
	return &wire.RingUserCredential{Algo: cluster.UserAlgo, Salt: salt, Hash: hash, Memory: credentialMemoryKiB,
		Iterations: credentialIterations, Parallelism: credentialParallelism}, nil
}

func wireGrants(grants []UserGrant) []wire.RingUserGrant {
	out := make([]wire.RingUserGrant, len(grants))
	for i, grant := range grants {
		out[i] = wire.RingUserGrant(grant)
	}
	return out
}

func userInfoOf(row wire.RingUserRow) UserInfo {
	roles := make([]UserGrant, len(row.Roles))
	for i, grant := range row.Roles {
		roles[i] = UserGrant(grant)
	}
	return UserInfo{Name: row.Name, Roles: roles, Enabled: row.Enabled, Epoch: row.Epoch, Created: time.UnixMilli(row.CreatedMs).UTC(),
		PasswordChanged: time.UnixMilli(row.PasswordChangedMs).UTC(), Revision: row.Revision}
}

// userAdmin sends one user command under the caller's request id, or a fresh one.
func (c *Client) userAdmin(ctx context.Context, request wire.RingAdminRequest, options []CallOption) (wire.RingAdminUsers, error) {
	config := newCallConfig(options)
	id := config.requestID
	if !config.hasRequestID {
		id = c.NextRequestID()
	}
	reply, err := c.adminWithID(ctx, request, id)
	if err != nil {
		return wire.RingAdminUsers{}, err
	}
	if reply.Users == nil {
		return wire.RingAdminUsers{}, fmt.Errorf("ringclient: the server answered %s without its user members. Check that the server is a hoarddb server of this version", request.Cmd)
	}
	return *reply.Users, nil
}

func (c *Client) userChange(ctx context.Context, request wire.RingAdminRequest, options []CallOption) (UserResult, error) {
	users, err := c.userAdmin(ctx, request, options)
	if err != nil {
		return UserResult{}, err
	}
	return UserResult{Revision: users.Revision, Result: users.Result}, nil
}

// CreateUser creates account name with password and roles. The password is derived into an
// Argon2id credential here and never sent. expectDropSeq is the DropSeq of the last ShowUsers or
// DescribeUser: the server refuses the create CONFLICT when a user was dropped since, so a retry
// (pass the same WithRequestID and the same expectDropSeq) never brings back a dropped account.
func (c *Client) CreateUser(ctx context.Context, name, password string, roles []UserGrant, expectDropSeq int64, options ...CallOption) (UserResult, error) {
	credential, err := deriveCredential(password)
	if err != nil {
		return UserResult{}, err
	}
	grants := wireGrants(roles)
	return c.userChange(ctx, wire.RingAdminRequest{Cmd: wire.AdminCreateUser,
		User: &wire.RingUserRequest{Name: name, ExpectDropSeq: expectDropSeq, Roles: &grants, Credential: credential}}, options)
}

// AlterUser changes account name as change says; a new password is derived here and never sent.
func (c *Client) AlterUser(ctx context.Context, name string, change UserChange, options ...CallOption) (UserResult, error) {
	user := &wire.RingUserRequest{Name: name, ExpectSeq: change.ExpectSeq, Enabled: change.Enabled}
	if change.Password != nil {
		credential, err := deriveCredential(*change.Password)
		if err != nil {
			return UserResult{}, err
		}
		user.Credential = credential
	}
	if change.Roles != nil {
		grants := wireGrants(*change.Roles)
		user.Roles = &grants
	}
	return c.userChange(ctx, wire.RingAdminRequest{Cmd: wire.AdminAlterUser, User: user}, options)
}

// DropUser removes account name. expectSeq, when not nil, is the revision the caller last read.
// An absent name is a NOT_FOUND *Error.
func (c *Client) DropUser(ctx context.Context, name string, expectSeq *int64, options ...CallOption) (UserResult, error) {
	return c.userChange(ctx, wire.RingAdminRequest{Cmd: wire.AdminDropUser, User: &wire.RingUserRequest{Name: name, ExpectSeq: expectSeq}}, options)
}

// GrantRole adds grant to account name; a grant already held succeeds and changes nothing.
func (c *Client) GrantRole(ctx context.Context, name string, grant UserGrant, expectSeq *int64, options ...CallOption) (UserResult, error) {
	return c.userChange(ctx, wire.RingAdminRequest{Cmd: wire.AdminGrantRole,
		User: &wire.RingUserRequest{Name: name, ExpectSeq: expectSeq, Grant: wire.RingUserGrant(grant)}}, options)
}

// RevokeRole removes grant from account name; a grant not held succeeds and changes nothing.
func (c *Client) RevokeRole(ctx context.Context, name string, grant UserGrant, expectSeq *int64, options ...CallOption) (UserResult, error) {
	return c.userChange(ctx, wire.RingAdminRequest{Cmd: wire.AdminRevokeRole,
		User: &wire.RingUserRequest{Name: name, ExpectSeq: expectSeq, Grant: wire.RingUserGrant(grant)}}, options)
}

// ShowUsers lists every account with the store's revision and drop fence.
func (c *Client) ShowUsers(ctx context.Context) (UserList, error) {
	users, err := c.userAdmin(ctx, wire.RingAdminRequest{Cmd: wire.AdminShowUsers}, nil)
	if err != nil {
		return UserList{}, err
	}
	list := UserList{Users: make([]UserInfo, len(users.Users)), Revision: users.Revision, DropSeq: users.DropSeq}
	for i, row := range users.Users {
		list.Users[i] = userInfoOf(row)
	}
	return list, nil
}

// DescribeUser returns account name and the store's drop fence; an absent name is a NOT_FOUND *Error.
func (c *Client) DescribeUser(ctx context.Context, name string) (UserDescription, error) {
	users, err := c.userAdmin(ctx, wire.RingAdminRequest{Cmd: wire.AdminDescribeUser, User: &wire.RingUserRequest{Name: name}}, nil)
	if err != nil {
		return UserDescription{}, err
	}
	if users.User == nil {
		return UserDescription{}, fmt.Errorf("ringclient: the server answered describe_user without the user. Check that the server is a hoarddb server of this version")
	}
	return UserDescription{User: userInfoOf(*users.User), DropSeq: users.DropSeq}, nil
}
