package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringGrant reads a role the grammar accepted: <role>@<db>, or a bare <role>, which is every database.
func ringGrant(text string) ringclient.UserGrant {
	role, database, found := strings.Cut(text, "@")
	if !found {
		return ringclient.UserGrant{Role: role, DB: "*"}
	}
	return ringclient.UserGrant{Role: role, DB: database}
}

// ringGrants reads the roles list of create user and alter user roles; an empty list is a
// non-nil empty slice, which alter user takes as "remove every grant".
func ringGrants(st Statement) []ringclient.UserGrant {
	grants := []ringclient.UserGrant{}
	roles, _ := st.Arg("roles")
	for _, text := range strings.Split(roles.Text, ",") {
		if text = strings.TrimSpace(text); text != "" {
			grants = append(grants, ringGrant(text))
		}
	}
	return grants
}

// userStatement runs a user or role statement on the session's own connection. Every refusal
// is the server's text and code (ringFailure); the CLI adds only its own checks of the password.
// Each run draws a fresh request id, and nothing here retries: a statement run again reads its
// fences again (ring-user-store.md 4.3, "The CLI does not retry").
func (s *RingSession) userStatement(ctx context.Context, st Statement) (RingOutput, error) {
	var users RingUsers = s.api
	name := ringText(st, "user")
	switch st.Verb {
	case "create user":
		return s.createUser(ctx, users, st, name)
	case "alter user", "alter user roles":
		return s.alterUser(ctx, users, st, name)
	case "drop user":
		if _, err := users.DropUser(ctx, name, nil); err != nil {
			return RingOutput{}, err
		}
		return ringLines(fmt.Sprintf("user %q dropped", name)), nil
	case "grant role":
		grant := ringGrant(ringText(st, "role"))
		result, err := users.GrantRole(ctx, name, grant, nil)
		if err != nil {
			return RingOutput{}, err
		}
		return ringLines(fmt.Sprintf("granted %s@%s to %q (revision %d)", grant.Role, grant.DB, name, result.Revision)), nil
	case "revoke role":
		grant := ringGrant(ringText(st, "role"))
		result, err := users.RevokeRole(ctx, name, grant, nil)
		if err != nil {
			return RingOutput{}, err
		}
		return ringLines(fmt.Sprintf("revoked %s@%s from %q (revision %d)", grant.Role, grant.DB, name, result.Revision)), nil
	case "user describe":
		described, err := users.DescribeUser(ctx, name)
		if err != nil {
			return RingOutput{}, err
		}
		return ringDescribeUser(described.User), nil
	}
	// show users and user list.
	list, err := users.ShowUsers(ctx)
	if err != nil {
		return RingOutput{}, err
	}
	out := ringLines(fmt.Sprintf("USERS (revision %d)", list.Revision), "---")
	if len(list.Users) == 0 {
		out.Lines = append(out.Lines, "(no users)")
	}
	for _, user := range list.Users {
		status := "enabled"
		if !user.Enabled {
			status = "disabled"
		}
		out.Lines = append(out.Lines, fmt.Sprintf("%s: %s, roles [%s], epoch %d", user.Name, status, ringRoleList(user.Roles, "(none)"), user.Epoch))
	}
	return out, nil
}

// createUser takes the password first, so a piped run consumes its password lines whatever the
// server answers, then reads the drop fence and creates with it: a user dropped since the read
// makes the server refuse the create rather than bring the account back.
func (s *RingSession) createUser(ctx context.Context, users RingUsers, st Statement, name string) (RingOutput, error) {
	password, err := s.newUserPassword(users, name)
	if err != nil {
		return RingOutput{}, err
	}
	list, err := users.ShowUsers(ctx)
	if err != nil {
		return RingOutput{}, err
	}
	result, err := users.CreateUser(ctx, name, password, ringGrants(st), list.DropSeq)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(fmt.Sprintf("user %q created (revision %d)", name, result.Revision)), nil
}

// alterUser reads the account first: an absent name is refused before a password is asked, and
// the change carries the revision read, so a change made in between is refused, not overwritten.
func (s *RingSession) alterUser(ctx context.Context, users RingUsers, st Statement, name string) (RingOutput, error) {
	described, err := users.DescribeUser(ctx, name)
	if err != nil {
		return RingOutput{}, err
	}
	change := ringclient.UserChange{ExpectSeq: &described.User.Revision}
	switch action := ringText(st, "action"); {
	case st.Verb == "alter user roles":
		grants := ringGrants(st)
		change.Roles = &grants
	case action == "password":
		password, err := s.newUserPassword(users, name)
		if err != nil {
			return RingOutput{}, err
		}
		change.Password = &password
	default:
		enabled := action == "enable"
		change.Enabled = &enabled
	}
	result, err := users.AlterUser(ctx, name, change)
	if err != nil {
		return RingOutput{}, err
	}
	return ringLines(fmt.Sprintf("user %q altered (revision %d)", name, result.Revision)), nil
}

// ringRoleList renders grants as role@db, sorted, or empty when there are none.
func ringRoleList(grants []ringclient.UserGrant, empty string) string {
	roles := make([]string, 0, len(grants))
	for _, grant := range grants {
		roles = append(roles, grant.Role+"@"+grant.DB)
	}
	sort.Strings(roles)
	if len(roles) == 0 {
		return empty
	}
	return strings.Join(roles, ", ")
}

// ringDescribeUser renders one account as v1.0 did, never with credential material.
func ringDescribeUser(user ringclient.UserInfo) RingOutput {
	status := "enabled"
	if !user.Enabled {
		status = "disabled"
	}
	const stamp = "2006-01-02T15:04:05Z07:00"
	return ringLines(
		"user:  "+user.Name,
		"status: "+status,
		"roles:  ["+ringRoleList(user.Roles, "")+"]",
		fmt.Sprintf("epoch:  %d", user.Epoch),
		"created: "+user.Created.Format(stamp),
		"password changed: "+user.PasswordChanged.Format(stamp))
}
