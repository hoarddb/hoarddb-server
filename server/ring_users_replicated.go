package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The replicated binding of the ring user commands (ring user store spec 6.1, 6.2, 4.3; steps
// M2a.3 and M2a.5): on a cluster member each change becomes one of the cluster's user commands
// (USER_CREATE, USER_ALTER, USER_DROP, USER_GRANT, USER_REVOKE), proposed through the ring node
// and applied by every member to the same table. The binding:
//
//   - answers a request id the cluster remembers (user_requests) as done, before anything else and
//     whatever the drop sequence says (R3-F2), so a client retry is applied once;
//   - refuses at once, with the case's text, when this node holds no cluster password key;
//   - peppers the client's hash once with the cluster key, inside the window every member accepts
//     and this node's own clamp;
//   - takes create_user's expect_drop_seq from the client and never from its own table (R3), and
//     fills expect_seq from its table only when the client sent none;
//   - checks the session's frozen grants against the change (the grantor ceiling), names the
//     session's account by its created_seq (B17), and keeps a row within the grants a reply can
//     carry;
//   - proposes under a 20 s deadline, below a default client's 30 s, and answers "nothing changed"
//     only for a decided ending and "may or may not have been applied" after any undecided one;
//   - answers once this node's follower has installed the commit, so a login right after works.
//
// Rule 11: nothing per bucket or store; per command one O(users) table read (shared, not copied)
// and one cluster commit; no goroutine.

// userProposalDeadline bounds one user change's proposal (spec 6.1 step 3, B4): below the client's
// default request timeout, so a default client reads this server's text, not its own deadline.
const userProposalDeadline = 20 * time.Second

// expectSeqWait bounds how long a change carrying a client's expect_seq waits for this node to
// have applied that state_seq, so the row it checks is the row the fence names.
const expectSeqWait = 5 * time.Second

// rememberedSettle bounds how long a decided "already applied" refusal waits for this node to show
// the request in its list before it is answered as done.
const rememberedSettle = 2 * time.Second

// clusterUserNode is the cluster as the binding sees it: the ring node.
type clusterUserNode interface {
	Users() cluster.UserView
	PasswordKey() (clusterpepper.Key, string, error)
	ProposeUserCommand(ctx context.Context, kind byte, body []byte) (cluster.Result, error)
}

// installWaiter is the follower's watermark.
type installWaiter interface {
	awaitInstalled(ctx context.Context, seq int64) error
}

// ringUsersReplicated is the binding of a cluster member.
type ringUsersReplicated struct {
	server    *Server
	node      clusterUserNode
	installed installWaiter
	deadline  time.Duration
	now       func() time.Time
}

// newRingUsersReplicated is the binding; deadline is userProposalDeadline in production and a
// test's budget otherwise (Service.userDeadline).
func newRingUsersReplicated(s *Server, node clusterUserNode, installed installWaiter, deadline time.Duration) *ringUsersReplicated {
	return &ringUsersReplicated{server: s, node: node, installed: installed, deadline: deadline, now: time.Now}
}

// userActor is the session's account as a user command names it.
type userActor struct {
	by      string
	bySeq   int64
	grantor *Grantor
}

// sessionActor is the account the session logged in as. A session whose account is not a row of
// the cluster's table (no created_seq) cannot name itself to the cluster.
func sessionActor(ctx context.Context) (userActor, error) {
	if sess, ok := transport.SessionFromContext(ctx); ok && sess != nil {
		if identity, ok := sess.Identity(); ok && identity.Incarnation > 0 {
			return userActor{by: identity.Username, bySeq: identity.Incarnation,
				grantor: &Grantor{Username: identity.Username, Grants: identity.Grants}}, nil
		}
	}
	return userActor{}, &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session's account is not one of the cluster's users, so it cannot change them. Nothing was changed. " +
		"Log in again as an account of the cluster that holds user-manager on every database"}
}

// userChange is one change: what the binding needs besides the command's body.
type userChange struct {
	request   [16]byte
	kind      byte
	statement string
	name      string
	event     string
	audit     string
	line      string
	// body builds the command from the table as this node has applied it.
	body func(view cluster.UserView, actor userActor, key clusterpepper.Key, kid string) ([]byte, error)
}

// apply runs one change through the cluster.
func (r *ringUsersReplicated) apply(ctx context.Context, change userChange) (wire.UserOutcome, error) {
	if seq, ok := r.node.Users().RequestApplied(change.request); ok {
		return r.remembered(ctx, change, seq), nil
	}
	key, kid, err := r.node.PasswordKey()
	if err != nil {
		return wire.UserOutcome{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf("%s was not applied: %v", change.statement, err)}
	}
	actor, err := sessionActor(ctx)
	if err != nil {
		return wire.UserOutcome{}, err
	}
	body, err := change.body(r.node.Users(), actor, key, kid)
	if err != nil {
		return wire.UserOutcome{}, err
	}
	proposal, cancel := context.WithTimeout(ctx, r.deadline)
	defer cancel()
	result, err := r.node.ProposeUserCommand(proposal, change.kind, body)
	if err != nil {
		return r.refusal(proposal, change, err)
	}
	if err := r.installed.awaitInstalled(proposal, result.StateSeq); err != nil {
		return wire.UserOutcome{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"%s was applied by the cluster at state_seq %d, but this node has not shown it yet. Do not run it again: run 'user describe %s' in a moment",
			change.statement, result.StateSeq, clipBare(change.name))}
	}
	r.server.audit.Log(AuditEntry{Event: change.event, User: sessionUser(ctx), Target: "user:" + change.name, Result: change.audit})
	return wire.UserOutcome{Revision: r.revision(change.name, result.StateSeq), Result: change.line}, nil
}

// remembered answers a change whose request id the cluster applied already: done, once this node
// shows it (bounded).
func (r *ringUsersReplicated) remembered(ctx context.Context, change userChange, seq int64) wire.UserOutcome {
	wait, cancel := context.WithTimeout(ctx, r.deadline)
	defer cancel()
	if err := r.installed.awaitInstalled(wait, seq); err != nil {
		r.server.logger.Warn("a remembered user change is not installed on this node yet", "statement", change.statement, "state_seq", seq, "error", err)
	}
	return wire.UserOutcome{Revision: r.revision(change.name, seq), Result: change.statement + ": already applied (a retry of an earlier request)"}
}

// revision is the account's changed_seq, or seq when the account is gone (a drop).
func (r *ringUsersReplicated) revision(name string, seq int64) int64 {
	if row, ok := r.node.Users().User(name); ok {
		return row.ChangedSeq
	}
	return seq
}

// refusal turns a proposal's error into the session's answer (spec 6.2, B3, B5).
func (r *ringUsersReplicated) refusal(ctx context.Context, change userChange, err error) (wire.UserOutcome, error) {
	seconds := int(r.deadline / time.Second)
	var undecided *wire.UndecidedError
	if errors.As(err, &undecided) {
		return wire.UserOutcome{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"%s may or may not have been applied: this node sent it, but no answer came back within %d seconds. "+
				"Run 'nodes' to see which members are down; when a majority is up, run 'user describe %s' to see whether the change was made before running the statement again.",
			change.statement, seconds, clipBare(change.name))}
	}
	var decided *cluster.Error
	if errors.As(err, &decided) && decided.Code != cluster.CodeUnavailable {
		// A repeat the leader remembers and this node has not applied yet is the first attempt's
		// success, not a conflict.
		if decided.Code == cluster.CodeConflict {
			if seq, ok := r.settleRemembered(ctx, change.request); ok {
				return r.remembered(ctx, change, seq), nil
			}
		}
		return wire.UserOutcome{}, decided
	}
	r.server.logger.Warn("a user change found no leader", "statement", change.statement, "error", err)
	return wire.UserOutcome{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"%s was not applied: no leader of the cluster answered within %d seconds, so nothing changed. Run 'nodes' to see which members are down, and run the statement again when a majority is up.",
		change.statement, seconds)}
}

// settleRemembered reads this node's request list for a short while.
func (r *ringUsersReplicated) settleRemembered(ctx context.Context, request [16]byte) (int64, bool) {
	ctx, cancel := context.WithTimeout(ctx, rememberedSettle)
	defer cancel()
	for {
		if seq, ok := r.node.Users().RequestApplied(request); ok {
			return seq, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// peppered checks a client credential against the window every member accepts and this node's
// clamp, and peppers its hash once with the cluster key (spec 4.2, 5.1, F6).
func (r *ringUsersReplicated) peppered(statement string, c *wire.RingUserCredential, key clusterpepper.Key, kid string) (cluster.UserCredential, error) {
	if outside := authrules.CredentialWindowViolation(c.Memory, c.Iterations, int64(c.Parallelism), len(c.Salt), len(c.Hash)); outside != "" {
		return cluster.UserCredential{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"%s was refused: the credential is outside what every member of a cluster accepts: %s. Derive it again with ringclient or a current driver", statement, outside)}
	}
	if err := validateCredentialClamp(ringCredentialMaterial(c), r.server.auth.argon2); err != nil {
		return cluster.UserCredential{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"%s was refused: %v. Derive it again with ringclient or a current driver", statement, err)}
	}
	return cluster.UserCredential{Salt: slices.Clone(c.Salt), Hash: key.Hash(c.Hash), Memory: c.Memory, Iterations: c.Iterations,
		Parallelism: c.Parallelism, PepperKID: kid}, nil
}

// target is the row a change names as this node has applied it, and the fence the command carries:
// the client's expect_seq, waited for here so the row is the one it names, or the row's own.
func (r *ringUsersReplicated) target(ctx context.Context, view cluster.UserView, name string, expect *int64) (cluster.UserRow, bool, int64) {
	if expect == nil {
		row, ok := view.User(name)
		return row, ok, row.ChangedSeq
	}
	if view.StateSeq < *expect {
		wait, cancel := context.WithTimeout(ctx, expectSeqWait)
		defer cancel()
		if err := r.installed.awaitInstalled(wait, *expect); err == nil {
			view = r.node.Users()
		}
	}
	row, ok := view.User(name)
	return row, ok, *expect
}

func toRuleGrantList(grants []wire.RingUserGrant) []authrules.Grant {
	out := make([]authrules.Grant, len(grants))
	for i, grant := range grants {
		out[i] = authrules.Grant{Role: grant.Role, DB: grant.DB}
	}
	return out
}

func rowGrants(row cluster.UserRow) []Grant {
	out := make([]Grant, len(row.Roles))
	for i, grant := range row.Roles {
		out[i] = Grant{Role: grant.Role, DB: grant.DB}
	}
	return out
}

// CreateUser proposes USER_CREATE with the client's drop fence.
func (r *ringUsersReplicated) CreateUser(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	statement := "create user " + clipQuoted(user.Name)
	return r.apply(ctx, userChange{request: request, kind: cluster.CommandUserCreate, statement: statement, name: user.Name,
		event: EventUserCreate, audit: "ok", line: "user " + clipQuoted(user.Name) + " created",
		body: func(_ cluster.UserView, actor userActor, key clusterpepper.Key, kid string) ([]byte, error) {
			given := ringGrants(*user.Roles)
			if err := actor.grantor.checkCeiling(statement, user.Name, nil, given); err != nil {
				return nil, ringUserRefusal(err)
			}
			cred, err := r.peppered(statement, user.Credential, key, kid)
			if err != nil {
				return nil, err
			}
			// expect_drop_seq is the client's, never this node's table: a retry carries its
			// first attempt's value, which predates any drop since (R3).
			return cluster.UserCreateBody{Request: request, By: actor.by, BySeq: actor.bySeq, Name: user.Name, ExpectDropSeq: user.ExpectDropSeq,
				Roles: toRuleGrantList(*user.Roles), Credential: cred, Enabled: true, CreatedMs: r.now().UnixMilli()}.Encode(), nil
		}})
}

// AlterUser proposes USER_ALTER.
func (r *ringUsersReplicated) AlterUser(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	statement := "alter user " + clipQuoted(user.Name)
	var changed []string
	if user.Roles != nil {
		changed = append(changed, "roles")
	}
	if user.Enabled != nil {
		changed = append(changed, "enabled")
	}
	if user.Credential != nil {
		changed = append(changed, "credentials")
	}
	return r.apply(ctx, userChange{request: request, kind: cluster.CommandUserAlter, statement: statement, name: user.Name,
		event: EventUserAlter, audit: "changed: " + strings.Join(changed, ","), line: fmt.Sprintf("user %s altered (%s)", clipQuoted(user.Name), strings.Join(changed, ", ")),
		body: func(view cluster.UserView, actor userActor, key clusterpepper.Key, kid string) ([]byte, error) {
			row, _, fence := r.target(ctx, view, user.Name, user.ExpectSeq)
			body := cluster.UserAlterBody{Request: request, By: actor.by, BySeq: actor.bySeq, Name: user.Name, ExpectSeq: fence, Enabled: user.Enabled}
			var given []Grant
			if user.Roles != nil {
				given = ringGrants(*user.Roles)
				roles := toRuleGrantList(*user.Roles)
				body.Roles = &roles
			}
			if err := actor.grantor.checkCeiling(statement, user.Name, rowGrants(row), given); err != nil {
				return nil, ringUserRefusal(err)
			}
			if user.Credential != nil {
				cred, err := r.peppered(statement, user.Credential, key, kid)
				if err != nil {
					return nil, err
				}
				body.Credential, body.PasswordChangedMs = &cred, r.now().UnixMilli()
			}
			return body.Encode(), nil
		}})
}

// DropUser proposes USER_DROP. A name the cluster does not hold is refused NOT_FOUND by the leader.
func (r *ringUsersReplicated) DropUser(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	statement := "drop user " + clipQuoted(user.Name)
	return r.apply(ctx, userChange{request: request, kind: cluster.CommandUserDrop, statement: statement, name: user.Name,
		event: EventUserDrop, audit: "ok", line: "user " + clipQuoted(user.Name) + " dropped",
		body: func(view cluster.UserView, actor userActor, _ clusterpepper.Key, _ string) ([]byte, error) {
			row, _, fence := r.target(ctx, view, user.Name, user.ExpectSeq)
			if err := actor.grantor.checkCeiling(statement, user.Name, rowGrants(row), nil); err != nil {
				return nil, ringUserRefusal(err)
			}
			return cluster.UserDropBody{Request: request, By: actor.by, BySeq: actor.bySeq, Name: user.Name, ExpectSeq: fence}.Encode(), nil
		}})
}

// GrantRole proposes USER_GRANT; a grant already held succeeds and changes nothing.
func (r *ringUsersReplicated) GrantRole(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	return r.role(ctx, request, user, true)
}

// RevokeRole proposes USER_REVOKE; a grant not held succeeds and changes nothing.
func (r *ringUsersReplicated) RevokeRole(ctx context.Context, request [16]byte, user wire.RingUserRequest) (wire.UserOutcome, error) {
	return r.role(ctx, request, user, false)
}

func (r *ringUsersReplicated) role(ctx context.Context, request [16]byte, user wire.RingUserRequest, grant bool) (wire.UserOutcome, error) {
	change := Grant{Role: user.Grant.Role, DB: user.Grant.DB}
	kind, event := cluster.CommandUserRevoke, EventUserAlter
	statement := fmt.Sprintf("revoke role %s@%s from %s", clipBare(change.Role), clipBare(change.DB), clipQuoted(user.Name))
	line := fmt.Sprintf("revoked %s@%s from %s", clipBare(change.Role), clipBare(change.DB), clipQuoted(user.Name))
	audit := "revoke " + change.Role + "@" + change.DB
	if grant {
		kind = cluster.CommandUserGrant
		statement = fmt.Sprintf("grant role %s@%s to %s", clipBare(change.Role), clipBare(change.DB), clipQuoted(user.Name))
		line = fmt.Sprintf("granted %s@%s to %s", clipBare(change.Role), clipBare(change.DB), clipQuoted(user.Name))
		audit = "grant " + change.Role + "@" + change.DB
	}
	return r.apply(ctx, userChange{request: request, kind: kind, statement: statement, name: user.Name, event: event, audit: audit, line: line,
		body: func(view cluster.UserView, actor userActor, _ clusterpepper.Key, _ string) ([]byte, error) {
			row, ok, fence := r.target(ctx, view, user.Name, user.ExpectSeq)
			var given []Grant
			if grant {
				given = []Grant{change}
			}
			if err := actor.grantor.checkCeiling(statement, user.Name, rowGrants(row), given); err != nil {
				return nil, ringUserRefusal(err)
			}
			// The row the fence names is the row the command changes, so counting its grants
			// here keeps every row within what a reply carries (U1 carried item a).
			held := slices.Contains(row.Roles, authrules.Grant{Role: change.Role, DB: change.DB})
			if grant && ok && row.ChangedSeq == fence && !held {
				if err := checkGrantCount(statement, user.Name, len(row.Roles)+1); err != nil {
					return nil, ringUserRefusal(err)
				}
			}
			return cluster.UserRoleBody{Request: request, By: actor.by, BySeq: actor.bySeq, Name: user.Name, ExpectSeq: fence,
				Grant: authrules.Grant{Role: change.Role, DB: change.DB}}.Encode(), nil
		}})
}

// clusterUserRow is a row as the wire reports it: never a salt, hash or key id; its revision is
// the row's own changed_seq, what expect_seq means in a cluster (spec 4.2).
func clusterUserRow(row cluster.UserRow) wire.RingUserRow {
	roles := make([]wire.RingUserGrant, len(row.Roles))
	for i, grant := range row.Roles {
		roles[i] = wire.RingUserGrant{Role: grant.Role, DB: grant.DB}
	}
	return wire.RingUserRow{Name: row.Name, Roles: roles, Enabled: row.Enabled, Epoch: row.Epoch, CreatedMs: row.CreatedMs,
		PasswordChangedMs: row.PasswordChangedMs, Revision: row.ChangedSeq}
}

// ShowUsers lists the cluster's table as this node has applied it, with its state_seq as the
// revision and user_drop_seq as the fence the next create carries.
func (r *ringUsersReplicated) ShowUsers(context.Context) (wire.RingAdminUsers, error) {
	view := r.node.Users()
	rows := make([]wire.RingUserRow, len(view.Users))
	for i, row := range view.Users {
		rows[i] = clusterUserRow(row)
	}
	return wire.RingAdminUsers{Revision: view.StateSeq, DropSeq: view.DropSeq, Users: rows}, nil
}

// DescribeUser returns one row and the drop fence; an absent name is NOT_FOUND.
func (r *ringUsersReplicated) DescribeUser(_ context.Context, name string) (wire.RingAdminUsers, error) {
	view := r.node.Users()
	row, ok := view.User(name)
	if !ok {
		return wire.RingAdminUsers{}, &cluster.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf("there is no user %s. Run 'show users' to list the users.", clipQuoted(name))}
	}
	described := clusterUserRow(row)
	return wire.RingAdminUsers{DropSeq: view.DropSeq, User: &described}, nil
}
