package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The built-in roles take names that stand on their own. Nothing has been
// released under the earlier names, so they are not aliases: they are refused
// like any unknown role, and the refusal says what the role is called now.

// oldRoleNames maps each name used by earlier development builds to its
// replacement. It is the test's own copy of the owner-approved mapping.
var oldRoleNames = map[string]string{
	"read":         "reader",
	"readWrite":    "writer",
	"dbAdmin":      "db-manager",
	"userAdmin":    "user-manager",
	"clusterAdmin": "cluster-manager",
	"backup":       "backup-operator",
	"restore":      "restore-operator",
}

var allActions = []Action{
	ActionRead, ActionWrite, ActionDBAdmin, ActionUserAdmin,
	ActionClusterAdmin, ActionBackup, ActionRestore,
}

// roleGrants is the privilege table. backup-operator keeps backup's
// read-any-database behaviour (spec 3.1).
var roleGrants = map[string][]Action{
	"root":             allActions,
	"reader":           {ActionRead},
	"writer":           {ActionRead, ActionWrite},
	"db-manager":       {ActionRead, ActionWrite, ActionDBAdmin},
	"user-manager":     {ActionUserAdmin},
	"cluster-manager":  {ActionClusterAdmin},
	"backup-operator":  {ActionBackup, ActionRead},
	"restore-operator": {ActionRestore},
}

var validRoleList = []string{"root", "reader", "writer", "db-manager", "user-manager", "cluster-manager", "backup-operator", "restore-operator"}

func actionIn(action Action, set []Action) bool {
	for _, member := range set {
		if member == action {
			return true
		}
	}
	return false
}

// TestRoleToActionTable pins which actions each role grants, and that an
// earlier name grants nothing.
func TestRoleToActionTable(t *testing.T) {
	for role, granted := range roleGrants {
		for _, action := range allActions {
			want := actionIn(action, granted)
			if got := roleSatisfies(role, action); got != want {
				t.Errorf("roleSatisfies(%q, %s) = %v, want %v", role, action, got, want)
			}
		}
	}
	for oldName := range oldRoleNames {
		for _, action := range allActions {
			if roleSatisfies(oldName, action) {
				t.Errorf("the retired name %q satisfied %s", oldName, action)
			}
		}
	}
}

// TestActionStringUsesRoleNames: the privilege names in errors and audit
// entries are the role names that grant them.
func TestActionStringUsesRoleNames(t *testing.T) {
	want := map[Action]string{
		ActionRead: "reader", ActionWrite: "writer", ActionDBAdmin: "db-manager",
		ActionUserAdmin: "user-manager", ActionClusterAdmin: "cluster-manager",
		ActionBackup: "backup-operator", ActionRestore: "restore-operator",
	}
	for action, name := range want {
		if got := action.String(); got != name {
			t.Errorf("Action(%d).String() = %q, want %q", action, got, name)
		}
	}
}

// TestNewRoleNamesAreEnforced drives the gate with scoped grants.
func TestNewRoleNamesAreEnforced(t *testing.T) {
	allow := func(role string, action Action, db string) bool {
		return identityAllows(ident(transport.ScopedGrant{Role: role, DB: "app"}), action, db)
	}
	if !allow("writer", ActionWrite, "app") {
		t.Error("writer@app was refused a write to app")
	}
	if allow("writer", ActionWrite, "logs") {
		t.Error("writer@app was allowed a write to logs")
	}
	if allow("reader", ActionWrite, "app") {
		t.Error("reader@app was allowed a write")
	}
	if allow("db-manager", ActionUserAdmin, "") {
		t.Error("db-manager was allowed to manage users")
	}
	if !allow("backup-operator", ActionRead, "app") {
		t.Error("backup-operator lost backup's read access")
	}
}

func roleTestMaterial(name string) CredentialMaterial {
	return CredentialMaterial{Algo: "argon2id", Salt: testSalt(name), Hash: testHash(name), Params: defaultArgon2Params()}
}

func grantsOf(t *testing.T, am *AuthManager, username string) []UserRole {
	t.Helper()
	info, ok := am.UserInfo(username)
	if !ok {
		t.Fatalf("user %q not found", username)
	}
	return info.Roles
}

// requireRenameHint fails unless err names the refused role, says what the
// role is called, and lists the valid roles.
func requireRenameHint(t *testing.T, where string, err error, oldName, newName string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: %q was accepted", where, oldName)
		return
	}
	wants := []string{`"` + oldName + `"`, `the role is called "` + newName + `"`}
	wants = append(wants, validRoleList...)
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: refusal %q does not contain %q", where, err, want)
		}
	}
}

// TestRetiredRoleNamesAreRefusedWithTheHint covers create, alter, grant and
// revoke: nothing is stored, and the refusal names the replacement.
func TestRetiredRoleNamesAreRefusedWithTheHint(t *testing.T) {
	for oldName, newName := range oldRoleNames {
		t.Run(oldName, func(t *testing.T) {
			am := newTestAuthManager(t, discardLogger())
			seedUser(t, am, "root")
			createErr := am.CreateUser("made", roleTestMaterial("made"), []Grant{{Role: oldName, DB: "app"}}, true)
			requireRenameHint(t, "create", createErr, oldName, newName)

			if err := am.CreateUser("alice", roleTestMaterial("alice"), []Grant{{Role: newName, DB: "app"}}, true); err != nil {
				t.Fatalf("CreateUser alice: %v", err)
			}
			_, grantErr := am.GrantRole("alice", Grant{Role: oldName, DB: "app"})
			requireRenameHint(t, "grant", grantErr, oldName, newName)

			replacement := []Grant{{Role: oldName, DB: "logs"}}
			_, alterErr := am.AlterUser(AlterSpec{Username: "alice", Grants: &replacement, Version: am.Version()})
			requireRenameHint(t, "alter", alterErr, oldName, newName)

			_, revokeErr := am.RevokeRole("alice", Grant{Role: oldName, DB: "app"})
			requireRenameHint(t, "revoke", revokeErr, oldName, newName)

			if _, ok := am.UserInfo("made"); ok {
				t.Error("a refused create left the user behind")
			}
			got := grantsOf(t, am, "alice")
			if len(got) != 1 || got[0].Role != newName {
				t.Errorf("alice holds %v after refused changes, want only %s@app", got, newName)
			}
		})
	}
}

// TestUnknownRoleIsRefusedWithTheValidNames: free text used to be stored and
// then silently granted nothing.
func TestUnknownRoleIsRefusedWithTheValidNames(t *testing.T) {
	am := newTestAuthManager(t, discardLogger())
	seedUser(t, am, "root")
	if err := am.CreateUser("alice", roleTestMaterial("alice"), nil, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	refusals := map[string]error{}
	_, grantErr := am.GrantRole("alice", Grant{Role: "superuser", DB: "app"})
	refusals["grant"] = grantErr
	refusals["create"] = am.CreateUser("bob", roleTestMaterial("bob"), []Grant{{Role: "superuser", DB: "app"}}, true)
	bad := []Grant{{Role: "superuser", DB: "app"}}
	_, alterErr := am.AlterUser(AlterSpec{Username: "alice", Grants: &bad, Version: am.Version()})
	refusals["alter"] = alterErr

	for verb, err := range refusals {
		if err == nil {
			t.Errorf("%s with an unknown role was accepted", verb)
			continue
		}
		for _, valid := range validRoleList {
			if !strings.Contains(err.Error(), valid) {
				t.Errorf("%s refusal %q does not list %q", verb, err, valid)
			}
		}
		if strings.Contains(err.Error(), "is called") {
			t.Errorf("%s refusal %q gives an unknown role another name", verb, err)
		}
	}
	if len(grantsOf(t, am, "alice")) != 0 {
		t.Error("alice holds a role after refused changes")
	}
}

// oldNamesUsersFile writes a users.json from an earlier development build:
// root plus one user per retired role name.
func oldNamesUsersFile(t *testing.T, withRoot bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), UsersFileName)
	cfg := validUsersConfig()
	var users []UserRecord
	if withRoot {
		users = append(users, rootRecord("root"))
	}
	for oldName := range oldRoleNames {
		record := rootRecord("u-" + oldName)
		record.Credentials.Salt = testSalt(oldName)
		record.Roles = []Grant{{Role: oldName, DB: "app"}}
		users = append(users, record)
	}
	cfg.Users = users
	// Save validates, so the fixture is written raw.
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// TestOldNameUsersFileIsRefusedAtLoad: the file is not loaded, not rewritten,
// and the message names every retired role with its replacement.
func TestOldNameUsersFileIsRefusedAtLoad(t *testing.T) {
	for _, withRoot := range []bool{true, false} {
		path := oldNamesUsersFile(t, withRoot)
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		am := newTestAuthManager(t, discardLogger())
		_, _, loadErr := am.LoadUsers(path, nil, nil)
		if loadErr == nil {
			t.Fatalf("withRoot=%v: a users.json holding retired role names was loaded", withRoot)
		}
		for oldName, newName := range oldRoleNames {
			for _, want := range []string{`"` + oldName + `"`, `"` + newName + `"`} {
				if !strings.Contains(loadErr.Error(), want) {
					t.Errorf("withRoot=%v: load error %q does not contain %s", withRoot, loadErr, want)
				}
			}
		}
		for _, want := range []string{path + " cannot be loaded: it uses role names this release does not have: ",
			"HOARDDB_ROOT_PASSWORD", "move " + path + " aside", "drops every other account", "recreate them with the role names this release uses"} {
			if !strings.Contains(loadErr.Error(), want) {
				t.Errorf("withRoot=%v: load error %q does not give the recovery %q", withRoot, loadErr, want)
			}
		}
		requireNoOtherBuild(t, loadErr.Error())
		if strings.Contains(loadErr.Error(), "Rename them") {
			t.Errorf("withRoot=%v: load error %q tells the operator to edit a sealed file", withRoot, loadErr)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(before, after) {
			t.Errorf("withRoot=%v: the refused file was rewritten", withRoot)
		}
	}
}

// TestPushedOldNameConfigIsRefused: a push carrying a retired name is refused
// whole, like any config that fails validation.
func TestPushedOldNameConfigIsRefused(t *testing.T) {
	am := newTestAuthManager(t, discardLogger())
	seedUser(t, am, "root")
	pushed := validUsersConfig()
	pushed.Term, pushed.Version = 9, 9
	operator := rootRecord("operator")
	operator.Credentials.Salt = testSalt("operator")
	operator.Roles = []Grant{{Role: "dbAdmin", DB: "app"}}
	pushed.Users = append(pushed.Users, operator)
	err := am.ApplyConfig(pushed)
	if err == nil {
		t.Fatal("a push holding a retired role name was applied")
	}
	if want := `refusing pushed auth config: it uses role names this release does not have: "dbAdmin" is called "db-manager". ` +
		"The node that pushed it runs a different build: stop every node of the cluster and start them all on the same release"; err.Error() != want {
		t.Errorf("the push refusal is not the whole text:\n got: %s\nwant: %s", err, want)
	}
	requireNoOtherBuild(t, err.Error())
	for _, unwanted := range []string{"HOARDDB_ROOT_PASSWORD", "users.json", "recreate"} {
		if strings.Contains(err.Error(), unwanted) {
			t.Errorf("push refusal %q sends the operator to local recovery (%s)", err, unwanted)
		}
	}
	if _, ok := am.UserInfo("operator"); ok {
		t.Error("a refused push installed a user")
	}
}

// TestDenialNamesTheNewRoleInTheFix: the refusal's fix command uses a name
// the operator can type.
func TestDenialNamesTheNewRoleInTheFix(t *testing.T) {
	s := userTestServer(t)
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "alice", Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	ctx := transport.WithSession(context.Background(), sess)
	frame := transport.NewFrame(transport.OpPut, mustMarshalBSON(t, map[string]string{"database": "app", "bucket": "sessions"}))

	err := s.authorize(ctx, frame)
	if err == nil {
		t.Fatal("a reader was allowed to put")
	}
	for _, want := range []string{`lacks "writer"`, "grant role writer@app to alice"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
}

// TestTokenWithARetiredNameGrantsNothing: a claim spelled with an earlier name
// no longer matches a record's grant.
func TestTokenWithARetiredNameGrantsNothing(t *testing.T) {
	record := &UserRecord{Username: "alice", Roles: []Grant{{Role: "writer", DB: "app"}}}
	if grants := sessionGrants(record, []string{"readWrite"}, []string{"app"}); len(grants) != 0 {
		t.Fatalf("sessionGrants with a retired-name claim = %v, want none", grants)
	}
}

// TestDuplicateGrantsCollapseAndOneRevokeRemovesThem: a repeated grant must
// not outlive a revoke.
func TestDuplicateGrantsCollapseAndOneRevokeRemovesThem(t *testing.T) {
	am := newTestAuthManager(t, discardLogger())
	seedUser(t, am, "root")
	pair := []Grant{{Role: "writer", DB: "app"}, {Role: "writer", DB: "app"}}
	if err := am.CreateUser("alice", roleTestMaterial("alice"), pair, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if got := grantsOf(t, am, "alice"); len(got) != 1 {
		t.Fatalf("create stored %v, want one writer@app", got)
	}
	replacement := append([]Grant(nil), pair...)
	if _, err := am.AlterUser(AlterSpec{Username: "alice", Grants: &replacement, Version: am.Version()}); err != nil {
		t.Fatalf("AlterUser: %v", err)
	}
	if got := grantsOf(t, am, "alice"); len(got) != 1 {
		t.Fatalf("alter stored %v, want one writer@app", got)
	}
	if _, err := am.RevokeRole("alice", Grant{Role: "writer", DB: "app"}); err != nil {
		t.Fatalf("RevokeRole: %v", err)
	}
	if got := grantsOf(t, am, "alice"); len(got) != 0 {
		t.Fatalf("alice still holds %v after one revoke", got)
	}
}

// TestRevokeRemovesEveryMatchingGrant: a store that already holds identical
// duplicates is cleared by a single revoke.
func TestRevokeRemovesEveryMatchingGrant(t *testing.T) {
	am := newTestAuthManager(t, discardLogger())
	seedUser(t, am, "root")
	if err := am.CreateUser("alice", roleTestMaterial("alice"), nil, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	am.mu.Lock()
	am.users["alice"].Roles = []Grant{{Role: "writer", DB: "app"}, {Role: "reader", DB: "app"}, {Role: "writer", DB: "app"}}
	am.mu.Unlock()
	if _, err := am.RevokeRole("alice", Grant{Role: "writer", DB: "app"}); err != nil {
		t.Fatalf("RevokeRole: %v", err)
	}
	got := grantsOf(t, am, "alice")
	if len(got) != 1 || got[0].Role != "reader" {
		t.Fatalf("after revoke alice holds %v, want only reader@app", got)
	}
}

// requireNoOtherBuild fails when a text sends the operator to a build other than this one. No
// other build was released (owner ruling 2026-11-21), so a refusal names what it found and the fix
// that works for this release.
func requireNoOtherBuild(t *testing.T, text string) {
	t.Helper()
	for _, banned := range []string{"earlier build", "earlier development build", "another build", "previous release", "the build that wrote",
		"earlier version", "older version", "previous version", "older build", "earlier hoardDB", "earlier engine"} {
		if strings.Contains(text, banned) {
			t.Errorf("the text says %q, and no other build was released: %s", banned, text)
		}
	}
}
