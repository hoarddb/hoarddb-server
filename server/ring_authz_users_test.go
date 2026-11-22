package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u1UserClasses maps each user class to its cmd.
var u1UserClasses = map[wire.RingClass]string{
	wire.RingClassCreateUser: wire.AdminCreateUser, wire.RingClassAlterUser: wire.AdminAlterUser, wire.RingClassDropUser: wire.AdminDropUser,
	wire.RingClassGrantRole: wire.AdminGrantRole, wire.RingClassRevokeRole: wire.AdminRevokeRole, wire.RingClassShowUsers: wire.AdminShowUsers,
	wire.RingClassDescribeUser: wire.AdminDescribeUser,
}

// u1CmdOf is the cmd of a user class, "" for any other class.
func u1CmdOf(class wire.RingClass) string { return u1UserClasses[class] }

// TestRingUserClassesNeedUnscopedUserManager: [GUARD the seven user classes are allowed to
// user-manager@* and root@* and to nobody else (a grant on one database is denied, and so are
// cluster-manager, db-manager, writer, reader, backup, no grants, no session and an internode
// peer), whether the ask names a database or not; a denial names the user-manager role it lacks;
// each class stands for the legacy opcode opRules gives that action].
func TestRingUserClassesNeedUnscopedUserManager(t *testing.T) {
	authz := newTestServer(t).RingAuthorizer()
	cases := []struct {
		name  string
		ctx   context.Context
		allow bool
	}{
		{"user-manager", u70Ctx(transport.ScopedGrant{Role: roleUserManager, DB: "*"}), true},
		{"root", u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "*"}), true},
		{"user-manager on one database", u70Ctx(transport.ScopedGrant{Role: roleUserManager, DB: "app"}), false},
		{"root on one database", u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "app"}), false},
		{"cluster-manager", u70Ctx(transport.ScopedGrant{Role: roleClusterManager, DB: "*"}), false},
		{"db-manager", u70Ctx(transport.ScopedGrant{Role: roleDBManager, DB: "*"}), false},
		{"writer", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "*"}), false},
		{"reader", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "*"}), false},
		{"backup-operator", u70Ctx(transport.ScopedGrant{Role: roleBackupOperator, DB: "*"}), false},
		{"no grants", u70Ctx(), false},
		{"no session", context.Background(), false},
	}
	if len(u1UserClasses) != 7 {
		t.Fatalf("the test maps %d user classes", len(u1UserClasses))
	}
	listed := map[wire.RingClass]bool{}
	for _, class := range wire.RingClasses() {
		listed[class] = true
	}
	for class, cmd := range u1UserClasses {
		if !listed[class] {
			t.Errorf("class 0x%02x (%s) is not listed by wire.RingClasses", class, cmd)
		}
		if got, ok := wire.UserClassOf(cmd); !ok || got != class {
			t.Errorf("%s is authorized as 0x%02x, want 0x%02x", cmd, got, class)
		}
		for _, tc := range cases {
			for _, database := range []string{"", "app"} {
				err := authz.Authorize(tc.ctx, wire.RingAccess{Database: database, Class: class})
				if (err == nil) != tc.allow {
					t.Errorf("%s, %s, database %q: Authorize returned %v, allow=%v", cmd, tc.name, database, err, tc.allow)
				}
			}
		}
		peer := &transport.Session{Peer: true}
		peer.SetIdentity(transport.Identity{Username: NodeUser, Grants: []transport.ScopedGrant{{Role: roleRoot, DB: "*"}}})
		if err := authz.Authorize(transport.WithSession(context.Background(), peer), wire.RingAccess{Class: class}); err == nil {
			t.Errorf("%s: a session on the internode listener was authorized", cmd)
		}
		err := authz.Authorize(u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "*"}), wire.RingAccess{Class: class})
		if err == nil || !strings.Contains(err.Error(), ActionUserAdmin.String()) {
			t.Errorf("%s: the denial %v does not name the action it lacks", cmd, err)
		}
		if _, ok := opRules[transport.Opcode(class)]; ok {
			t.Errorf("class 0x%02x is also a transport opcode with an authorization rule", class)
		}
	}
}

// TestRingUserClassesAreDefaultDenied: [GUARD a user class is refused by the database-scoped path,
// and when the legacy rule it stands for stops being a user-manager rule with no database the
// class is refused for every user, root included].
func TestRingUserClassesAreDefaultDenied(t *testing.T) {
	for class := range u1UserClasses {
		if action, err := ringAction(class); err == nil {
			t.Errorf("ringAction gave class 0x%02x the database action %s", class, action)
		}
	}
	root := u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "*"})
	saved := opRules[transport.OpUserCreate]
	opRules[transport.OpUserCreate] = OpRule{Action: ActionRead, Scope: ScopeDatabase}
	err := newTestServer(t).RingAuthorizer().Authorize(root, wire.RingAccess{Class: wire.RingClassCreateUser})
	opRules[transport.OpUserCreate] = saved
	if err == nil {
		t.Errorf("create_user was allowed to root after %s stopped being a user-manager rule", transport.OpUserCreate)
	}
}
