package server

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// This file binds the ring user commands (cluster/wire/ringusers.go) to this node's AuthManager:
// the zero-config binding of legacy-deletion-build.md U1.3. It is the legacy user handlers'
// work (users_handlers.go) on the RING_ADMIN request: the same AuthManager methods with the
// grantor ceiling and the last-administrator guard, the same per-node dedupe window keyed on the
// frame's 16-byte request id, the same session closes and audit lines. In cluster mode users
// belong to the cluster's replicated state, served by ring_users_replicated.go.

// ringUsers is the zero-config binding. mu makes a revision check and the change it guards one
// step for ring requests; a change through the legacy user opcodes is not under it (they leave
// with the legacy engine, legacy-deletion-build.md D2c). The drop fence is the AuthManager's
// own (AuthManager.DropSeq), moved by every drop whichever path made it, and judged under the
// store's lock (CreateUserWith).
type ringUsers struct {
	server *Server
	mu     sync.Mutex
}

// ringRequestKey is the dedupe key of a ring user change: the frame's request id, bound to the
// command and the account, so a reused id answers "already applied" only for a retry of the same
// change (a retry may carry another body, ring-user-store.md 4.3 B12, but never another command
// or account). It is apart from the legacy string ids.
func ringRequestKey(request [16]byte, cmd, name string) string {
	return "ring:" + hex.EncodeToString(request[:]) + ":" + cmd + ":" + name
}

// ringCredentialMaterial is the AuthManager form of a wire credential: base64 text and the
// lengths the legacy parameters carry.
func ringCredentialMaterial(c *wire.RingUserCredential) CredentialMaterial {
	return CredentialMaterial{
		Algo: c.Algo,
		Salt: base64.StdEncoding.EncodeToString(c.Salt),
		Hash: base64.StdEncoding.EncodeToString(c.Hash),
		Params: Argon2Params{Memory: uint32(c.Memory), Iterations: uint32(c.Iterations), Parallelism: uint8(c.Parallelism),
			SaltLen: len(c.Salt), HashLen: len(c.Hash)},
	}
}

// NewRingCredential derives the wire credential of a password on the client, with a fresh salt
// and the default work factors: only the returned material is ever sent.
func NewRingCredential(password string) (*wire.RingUserCredential, error) {
	return ringCredential(password, DefaultArgon2Params())
}

// ringCredential is NewRingCredential at the work factor params.
func ringCredential(password string, params Argon2Params) (*wire.RingUserCredential, error) {
	material, err := NewCredential(password, params)
	if err != nil {
		return nil, err
	}
	salt, err := base64.StdEncoding.DecodeString(material.Salt)
	if err != nil {
		return nil, fmt.Errorf("derive credential: decode the salt just drawn: %w", err)
	}
	hash, err := base64.StdEncoding.DecodeString(material.Hash)
	if err != nil {
		return nil, fmt.Errorf("derive credential: decode the hash just derived: %w", err)
	}
	return &wire.RingUserCredential{Algo: material.Algo, Salt: salt, Hash: hash, Memory: int64(material.Params.Memory),
		Iterations: int64(material.Params.Iterations), Parallelism: int32(material.Params.Parallelism)}, nil
}

func ringGrants(grants []wire.RingUserGrant) []Grant {
	out := make([]Grant, len(grants))
	for i, grant := range grants {
		out[i] = Grant{Role: grant.Role, DB: grant.DB}
	}
	return out
}

// ringUserRefusal gives an AuthManager error the ring code ring-user-store.md 4.2 gives the same
// refusal in the cluster, so both bindings answer one command alike (rule 7); the text is the
// AuthManager's. An error it does not know stays as it is (UNAVAILABLE with its text).
func ringUserRefusal(err error) error {
	var coded *cluster.Error
	if errors.As(err, &coded) {
		return err
	}
	code := cluster.Code(0)
	switch {
	case errors.Is(err, ErrUserNotFound):
		code = cluster.CodeNotFound
	case errors.Is(err, ErrPermissionDenied):
		code = cluster.CodePermissionDenied
	case errors.Is(err, ErrUserExists):
		code = cluster.CodeUserExists
	case errors.Is(err, ErrUserDroppedSince), errors.Is(err, ErrSaltReused), errors.Is(err, ErrUserVersion):
		// Exit 8: rerunning the statement reads its fences and draws a salt again, so a retry can succeed.
		code = cluster.CodeConflict
	case errors.Is(err, ErrLastAdmin), errors.Is(err, ErrTooManyUsers),
		errors.Is(err, ErrUnknownRole), errors.Is(err, ErrTooManyGrants), errors.Is(err, ErrCredentialInvalid), errors.Is(err, ErrUsernameRequired), errors.Is(err, ErrReservedUsername), errors.Is(err, ErrUsernameInvalid):
		// Exit 2: a retry is refused the same way until something else changes.
		code = cluster.CodeInvalidArgument
	default:
		return err
	}
	return &cluster.Error{Code: code, Message: err.Error()}
}

// staleRevision is the refusal of a change whose expect_seq is not the current revision. The
// revision here is the store's, so the text says the store changed, not this user.
func staleRevision(statement, name string, read, now uint64) error {
	return &cluster.Error{Code: cluster.CodeConflict, Message: fmt.Sprintf(
		"%s was not applied: the user store changed since this user was read (revision %d, now %d). Run 'user describe %s' and repeat the change against what it shows.",
		statement, read, now, clipBare(name))}
}

// checkRevisionLocked refuses a change whose expect_seq names another revision; absent, the
// binding takes its own (ring-user-store.md 4.3). The caller holds r.mu.
func (r *ringUsers) checkRevisionLocked(statement string, user wire.RingUserRequest) error {
	now := r.server.auth.Version()
	if user.ExpectSeq != nil && uint64(*user.ExpectSeq) != now {
		return staleRevision(statement, user.Name, uint64(*user.ExpectSeq), now)
	}
	return nil
}

// applied answers a retry of a change this node already made: success, applied once.
// The caller holds r.mu, so a retry that races its original waits for it and then finds it applied.
func (r *ringUsers) applied(request [16]byte, cmd, name, statement string) (wire.UserOutcome, bool) {
	if !r.server.authRequestAlreadyApplied(ringRequestKey(request, cmd, name)) {
		return wire.UserOutcome{}, false
	}
	return wire.UserOutcome{Revision: int64(r.server.auth.Version()), Result: statement + ": already applied (a retry of an earlier request)"}, true
}

// done records an applied change: the dedupe entry, the audit line, and the reply.
func (r *ringUsers) done(ctx context.Context, request [16]byte, cmd, event, name, result, line string) wire.UserOutcome {
	r.server.markAuthRequest(ringRequestKey(request, cmd, name))
	r.server.audit.Log(AuditEntry{Event: event, User: sessionUser(ctx), Target: "user:" + name, Result: result})
	return wire.UserOutcome{Revision: int64(r.server.auth.Version()), Result: line}
}

// closeSessions ends the account's open sessions on this node.
func (r *ringUsers) closeSessions(name, why string) {
	if closed := r.server.closeUserSessions(name); closed > 0 {
		r.server.logger.Info(why, "username", name, "closed", closed)
	}
}

// CreateUser creates the account. The drop fence is judged after the name: a create of a name that
// exists answers "exists" whatever its fence.
func (r *ringUsers) CreateUser(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	statement := "create user " + clipQuoted(user.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome, ok := r.applied(request, wire.AdminCreateUser, user.Name, statement); ok {
		return outcome, nil
	}
	fence := user.ExpectDropSeq
	spec := CreateSpec{Grantor: requestGrantor(ctx), Username: user.Name, Material: ringCredentialMaterial(user.Credential),
		Grants: ringGrants(*user.Roles), Enabled: true, ExpectDropSeq: &fence}
	if err := r.server.auth.CreateUserWith(spec); err != nil {
		if errors.Is(err, ErrUserExists) {
			return wire.UserOutcome{}, &cluster.Error{Code: cluster.CodeUserExists,
				Message: cluster.CodeUserExists.Text(cluster.CondUserExists, map[string]string{"user": clipBare(user.Name)})}
		}
		return wire.UserOutcome{}, ringUserRefusal(err)
	}
	return r.done(ctx, request, wire.AdminCreateUser, EventUserCreate, user.Name, "ok", "user "+clipQuoted(user.Name)+" created"), nil
}

// AlterUser changes the credential, the enabled flag or the whole role list. A disable or a new
// password ends the account's sessions on this node; a role change reaches a session at its next
// refresh.
func (r *ringUsers) AlterUser(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	statement := "alter user " + clipQuoted(user.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome, ok := r.applied(request, wire.AdminAlterUser, user.Name, statement); ok {
		return outcome, nil
	}
	if err := r.checkRevisionLocked(statement, user); err != nil {
		return wire.UserOutcome{}, err
	}
	spec := AlterSpec{Grantor: requestGrantor(ctx), Username: user.Name, Version: r.server.auth.Version(), Enabled: user.Enabled}
	var changed []string
	if user.Roles != nil {
		grants := ringGrants(*user.Roles)
		spec.Grants = &grants
		changed = append(changed, "roles")
	}
	if user.Enabled != nil {
		changed = append(changed, "enabled")
	}
	if user.Credential != nil {
		material := ringCredentialMaterial(user.Credential)
		spec.Credential = &material
		changed = append(changed, "credentials")
	}
	if _, err := r.server.auth.AlterUser(spec); err != nil {
		return wire.UserOutcome{}, ringUserRefusal(err)
	}
	if (user.Enabled != nil && !*user.Enabled) || user.Credential != nil {
		r.closeSessions(user.Name, "closed sessions for a user whose access was revoked")
	}
	return r.done(ctx, request, wire.AdminAlterUser, EventUserAlter, user.Name, "changed: "+strings.Join(changed, ","), fmt.Sprintf("user %s altered (%s)", clipQuoted(user.Name), strings.Join(changed, ", "))), nil
}

// DropUser removes the account and ends its sessions on this node. A name that is absent is
// NOT_FOUND, never success: a typo is not silent.
func (r *ringUsers) DropUser(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	statement := "drop user " + clipQuoted(user.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome, ok := r.applied(request, wire.AdminDropUser, user.Name, statement); ok {
		return outcome, nil
	}
	if err := r.checkRevisionLocked(statement, user); err != nil {
		return wire.UserOutcome{}, err
	}
	if err := r.server.auth.DropUserAs(requestGrantor(ctx), user.Name); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return wire.UserOutcome{}, &cluster.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf(
				"%s was not applied: there is no user %s. Run 'show users' to list the users.", statement, clipQuoted(user.Name))}
		}
		return wire.UserOutcome{}, ringUserRefusal(err)
	}
	r.closeSessions(user.Name, "closed sessions for dropped user")
	return r.done(ctx, request, wire.AdminDropUser, EventUserDrop, user.Name, "ok", "user "+clipQuoted(user.Name)+" dropped"), nil
}

// GrantRole adds one grant; a grant already held succeeds and changes nothing.
func (r *ringUsers) GrantRole(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	grant := Grant{Role: user.Grant.Role, DB: user.Grant.DB}
	statement := fmt.Sprintf("grant role %s@%s to %s", clipBare(grant.Role), clipBare(grant.DB), clipQuoted(user.Name))
	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome, ok := r.applied(request, wire.AdminGrantRole, user.Name, statement); ok {
		return outcome, nil
	}
	if err := r.checkRevisionLocked(statement, user); err != nil {
		return wire.UserOutcome{}, err
	}
	if _, err := r.server.auth.GrantRoleAs(requestGrantor(ctx), user.Name, grant); err != nil {
		return wire.UserOutcome{}, ringUserRefusal(err)
	}
	return r.done(ctx, request, wire.AdminGrantRole, EventUserAlter, user.Name, "grant "+grant.Role+"@"+grant.DB, fmt.Sprintf("granted %s@%s to %s", clipBare(grant.Role), clipBare(grant.DB), clipQuoted(user.Name))), nil
}

// RevokeRole removes one grant; a grant not held succeeds and changes nothing.
func (r *ringUsers) RevokeRole(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	grant := Grant{Role: user.Grant.Role, DB: user.Grant.DB}
	statement := fmt.Sprintf("revoke role %s@%s from %s", clipBare(grant.Role), clipBare(grant.DB), clipQuoted(user.Name))
	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome, ok := r.applied(request, wire.AdminRevokeRole, user.Name, statement); ok {
		return outcome, nil
	}
	if err := r.checkRevisionLocked(statement, user); err != nil {
		return wire.UserOutcome{}, err
	}
	if _, err := r.server.auth.RevokeRoleAs(requestGrantor(ctx), user.Name, grant); err != nil {
		return wire.UserOutcome{}, ringUserRefusal(err)
	}
	return r.done(ctx, request, wire.AdminRevokeRole, EventUserAlter, user.Name, "revoke "+grant.Role+"@"+grant.DB, fmt.Sprintf("revoked %s@%s from %s", clipBare(grant.Role), clipBare(grant.DB), clipQuoted(user.Name))), nil
}

// unixMs is t in Unix milliseconds, 0 for an unset time.
func unixMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// ringUserRow is an account as the wire reports it: never a salt, hash or key id. Every row's
// revision is the store's: the per-node store keeps one version, not one per account.
func ringUserRow(info UserInfo, revision int64) wire.RingUserRow {
	roles := make([]wire.RingUserGrant, len(info.Roles))
	for i, role := range info.Roles {
		roles[i] = wire.RingUserGrant{Role: role.Role, DB: role.DB}
	}
	return wire.RingUserRow{Name: info.Username, Roles: roles, Enabled: info.Enabled, Epoch: int64(info.Epoch),
		CreatedMs: unixMs(info.CreatedAt), PasswordChangedMs: unixMs(info.PasswordChangedAt), Revision: revision}
}

// ShowUsers lists every account, the store's revision and the drop fence.
func (r *ringUsers) ShowUsers(context.Context) (wire.RingAdminUsers, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	revision := int64(r.server.auth.Version())
	infos := r.server.auth.UserInfos()
	rows := make([]wire.RingUserRow, len(infos))
	for i, info := range infos {
		rows[i] = ringUserRow(info, revision)
	}
	return wire.RingAdminUsers{Revision: revision, DropSeq: r.server.auth.DropSeq(), Users: rows}, nil
}

// DescribeUser returns one account and the drop fence; an absent name is NOT_FOUND.
func (r *ringUsers) DescribeUser(_ context.Context, name string) (wire.RingAdminUsers, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.server.auth.UserInfo(name)
	if !ok {
		return wire.RingAdminUsers{}, &cluster.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf("there is no user %s. Run 'show users' to list the users.", clipQuoted(name))}
	}
	row := ringUserRow(info, int64(r.server.auth.Version()))
	return wire.RingAdminUsers{DropSeq: r.server.auth.DropSeq(), User: &row}, nil
}
