package server

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/authrules"
)

// The built-in roles, named by authrules, which the cluster's appliers share: the server keeps
// no role table of its own (ring user store spec 4.2, F10). Their names are the privilege names
// Action.String reports, so an error that says what is missing is also what to grant.
const (
	roleRoot            = authrules.RoleRoot
	roleReader          = authrules.RoleReader
	roleWriter          = authrules.RoleWriter
	roleDBManager       = authrules.RoleDBManager
	roleUserManager     = authrules.RoleUserManager
	roleClusterManager  = authrules.RoleClusterManager
	roleBackupOperator  = authrules.RoleBackupOperator
	roleRestoreOperator = authrules.RoleRestoreOperator
)

// errUnknownRole names the valid roles, and the role's name when the one given
// is a name this release maps to a role (authrules.RenamedRole), because a
// mistyped role stored as it is would silently grant nothing.
func errUnknownRole(role string) error {
	hint := ""
	if current, renamed := authrules.RenamedRole(role); renamed {
		hint = fmt.Sprintf(": the role is called %q", current)
	}
	return fmt.Errorf("%w %q%s. The valid roles are %s", ErrUnknownRole, role, hint, strings.Join(authrules.RoleNames(), ", "))
}

// checkGrants refuses a grant naming an unknown role and returns the grants
// with repeats dropped.
func checkGrants(grants []Grant) ([]Grant, error) {
	for _, grant := range grants {
		if !authrules.KnownRole(grant.Role) {
			return nil, errUnknownRole(grant.Role)
		}
	}
	return dedupeGrants(grants), nil
}

// dedupeGrants drops repeats of an identical grant, keeping the first. A
// second copy would outlive a revoke of the first.
func dedupeGrants(grants []Grant) []Grant {
	seen := make(map[Grant]struct{}, len(grants))
	unique := grants[:0:0]
	for _, grant := range grants {
		if _, dup := seen[grant]; dup {
			continue
		}
		seen[grant] = struct{}{}
		unique = append(unique, grant)
	}
	return unique
}

// errRetiredRoles refuses a users config that holds a retired role name. Such
// a config must not load: its administrators would silently lose their
// authority. The error names each old role and its replacement; the caller adds
// the recovery that fits where the config came from.
func (c *usersConfig) errRetiredRoles() error {
	found := make(map[string]string)
	for _, record := range c.Users {
		for _, grant := range record.Roles {
			if current, renamed := authrules.RenamedRole(grant.Role); renamed {
				found[grant.Role] = current
			}
		}
	}
	if len(found) == 0 {
		return nil
	}
	retired := make([]string, 0, len(found))
	for old := range found {
		retired = append(retired, old)
	}
	sort.Strings(retired)
	renames := make([]string, 0, len(retired))
	for _, old := range retired {
		renames = append(renames, fmt.Sprintf("%q is called %q", old, found[old]))
	}
	return fmt.Errorf("it uses role names this release does not have: %s", strings.Join(renames, ", "))
}

// errRetiredRolesInFile is the load-time refusal. users.json is sealed, so the
// operator cannot edit it by hand, and the only way back is to start without it.
func errRetiredRolesInFile(path string, retired error) error {
	return fmt.Errorf("%s cannot be loaded: %w. "+
		"To recover: stop the server, move %s aside, and start it again with HOARDDB_ROOT_PASSWORD set, which seeds the root user. "+
		"This drops every other account; recreate them with the role names this release uses",
		path, retired, path)
}

// errRetiredRolesInPush is the refusal of a pushed config. The names come from
// a node on a different build, so nothing on this node needs editing.
func errRetiredRolesInPush(retired error) error {
	return fmt.Errorf("refusing pushed auth config: %w. "+
		"The node that pushed it runs a different build: stop every node of the cluster and start them all on the same release", retired)
}
