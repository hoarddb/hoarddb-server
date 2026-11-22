package server

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

func u78Names(reply wire.RingAdminReply) []string {
	names := make([]string, len(reply.Databases))
	for i, database := range reply.Databases {
		names[i] = database.Name
	}
	return names
}

// u78Fixture is a running server with databases app, vault and zeta, and app holding the
// buckets User, Order and Ticket. The returned connection is the bootstrap root.
func u78Fixture(t *testing.T) (*Service, *tcpServerClient) {
	t.Helper()
	qs, _, root := ringComposeServer(t)
	for _, name := range []string{"vault", "zeta"} {
		ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: name})
	}
	for _, name := range []string{"Order", "Ticket"} {
		ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminCreateBucket, Database: ringComposeDB, Name: name, Fields: []wire.RingAdminField{}})
	}
	return qs, root
}

// TestU78ShowDatabasesListsOnlyWhatTheSessionMayUse: root and a wildcard reader see every
// database with its id and replication factor; a reader of one database sees that one; a
// session with no grants gets an empty list and no error, as the legacy show databases gives.
func TestU78ShowDatabasesListsOnlyWhatTheSessionMayUse(t *testing.T) {
	qs, root := u78Fixture(t)
	all := ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases})
	if want := []string{ringComposeDB, "vault", "zeta"}; !slices.Equal(u78Names(all), want) || all.More {
		t.Fatalf("root sees %v more=%v, want %v", u78Names(all), all.More, want)
	}
	for _, database := range all.Databases {
		if database.DatabaseID < 1 || database.RF != 1 {
			t.Errorf("%+v: want an id and rf 1", database)
		}
	}
	star := ringAdminUser(t, qs, "star", Grant{Role: roleReader, DB: "*"})
	if got := u78Names(ringAdmin(t, star, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases})); len(got) != 3 {
		t.Errorf("a wildcard reader sees %v", got)
	}
	one := ringAdminUser(t, qs, "one", Grant{Role: roleReader, DB: ringComposeDB})
	if got := u78Names(ringAdmin(t, one, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases})); !slices.Equal(got, []string{ringComposeDB}) {
		t.Errorf("a reader of %s sees %v", ringComposeDB, got)
	}
	nobody := ringAdminUser(t, qs, "nobody")
	if got := ringAdmin(t, nobody, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases}); len(got.Databases) != 0 || got.More || got.Databases == nil {
		t.Errorf("a session with no grants sees %+v, want an empty list", got)
	}
	// Legacy databasesVisibleTo lists a database for any role granted on it.
	manager := ringAdminUser(t, qs, "usermanager", Grant{Role: roleUserManager, DB: "vault"})
	if got := u78Names(ringAdmin(t, manager, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases})); !slices.Equal(got, []string{"vault"}) {
		t.Errorf("a user-manager on vault sees %v, as the legacy listing shows it", got)
	}
}

// TestU78ShowDatabasesPagesAndACursorCannotReachWhatIsHidden: a reader of app and zeta pages
// over a hidden vault between them, and no cursor, however forged, lists vault.
func TestU78ShowDatabasesPagesAndACursorCannotReachWhatIsHidden(t *testing.T) {
	qs, root := u78Fixture(t)
	reader := ringAdminUser(t, qs, "pager", Grant{Role: roleReader, DB: ringComposeDB}, Grant{Role: roleReader, DB: "zeta"})
	one := int32(1)
	first := ringAdmin(t, reader, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases, Limit: &one})
	if !slices.Equal(u78Names(first), []string{ringComposeDB}) || !first.More || string(first.Cursor) != ringComposeDB {
		t.Fatalf("first page %v more=%v cursor=%q", u78Names(first), first.More, first.Cursor)
	}
	second := ringAdmin(t, reader, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases, Limit: &one, Cursor: first.Cursor})
	if !slices.Equal(u78Names(second), []string{"zeta"}) || second.More || len(second.Cursor) != 0 {
		t.Fatalf("second page %v more=%v cursor=%q", u78Names(second), second.More, second.Cursor)
	}
	for _, forged := range []string{"", "a", "vault", "vaul", "vault\x00", "zzzz", strings.Repeat("v", 1000)} {
		req := wire.RingAdminRequest{Cmd: wire.AdminShowDatabases}
		if forged != "" {
			req.Cursor = []byte(forged)
		}
		for _, name := range u78Names(ringAdmin(t, reader, req)) {
			if name == "vault" {
				t.Errorf("cursor %q listed vault to a session with no grant on it", forged)
			}
		}
	}
	only := ringAdminUser(t, qs, "onlyapp", Grant{Role: roleReader, DB: ringComposeDB})
	after := ringAdmin(t, only, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases, Cursor: []byte("vault")})
	if len(after.Databases) != 0 || after.More {
		t.Errorf("a cursor past every visible database gave %+v", after)
	}
	three := int32(2)
	page := ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases, Limit: &three})
	if len(page.Databases) != 2 || !page.More {
		t.Errorf("root page of 2 gave %v more=%v", u78Names(page), page.More)
	}
	tooMany := int32(10001)
	refusal := ringAdminRefusal(t, ringAdminFrame(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases, Limit: &tooMany}), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "limit") || !strings.Contains(refusal.Message, "10000") {
		t.Errorf("limit refusal: %s", refusal.Message)
	}
}

// TestU78DescribeDatabaseIsDeniedUniformlyAndPagesItsBuckets: a session with no grants and a
// reader of another database get one denial text for a database that exists and for one that
// does not; a reader of app gets the id, rf and a paged list of buckets.
func TestU78DescribeDatabaseIsDeniedUniformlyAndPagesItsBuckets(t *testing.T) {
	qs, root := u78Fixture(t)
	reader := ringAdminUser(t, qs, "describer", Grant{Role: roleReader, DB: ringComposeDB})
	nobody := ringAdminUser(t, qs, "nobody")
	denied := func(c *tcpServerClient, database string) string {
		frame := ringAdminFrame(t, c, wire.RingAdminRequest{Cmd: wire.AdminDescribeDatabase, Database: database})
		return ringAdminRefusal(t, frame, cluster.CodePermissionDenied).Message
	}
	forbidden, missing, none := denied(reader, "vault"), denied(reader, "ghost"), denied(nobody, ringComposeDB)
	if forbidden != missing || forbidden != none {
		t.Errorf("denial texts differ:\n%q\n%q\n%q", forbidden, missing, none)
	}
	for _, name := range []string{"vault", "ghost", ringComposeDB} {
		if strings.Contains(forbidden, name) {
			t.Errorf("the denial names %q: %s", name, forbidden)
		}
	}

	listed := ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases})
	var appRow wire.RingAdminDatabase
	for _, row := range listed.Databases {
		if row.Name == ringComposeDB {
			appRow = row
		}
	}
	two := int32(2)
	first := ringAdmin(t, reader, wire.RingAdminRequest{Cmd: wire.AdminDescribeDatabase, Database: ringComposeDB, Limit: &two})
	if first.Cmd != wire.AdminDescribeDatabase || first.Name != ringComposeDB || first.DatabaseID != appRow.DatabaseID || first.RF != appRow.RF {
		t.Fatalf("describe_database %+v, want name %s id %d rf %d", first, ringComposeDB, appRow.DatabaseID, appRow.RF)
	}
	if len(first.Buckets) != 2 || !first.More || len(first.Cursor) == 0 {
		t.Fatalf("first page of buckets: %+v", first)
	}
	second := ringAdmin(t, reader, wire.RingAdminRequest{Cmd: wire.AdminDescribeDatabase, Database: ringComposeDB, Limit: &two, Cursor: first.Cursor})
	var names []string
	for _, bucket := range append(first.Buckets, second.Buckets...) {
		names = append(names, bucket.Name)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"Order", "Ticket", "User"}) || second.More {
		t.Fatalf("pages gave %v, second %+v", names, second)
	}
	// A cursor issued for another database's buckets is refused with the fix named.
	other := ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeDatabase, Database: "zeta", Limit: &two})
	if len(other.Buckets) != 0 || other.Buckets == nil {
		t.Errorf("zeta has buckets %+v", other.Buckets)
	}
	forged := ringAdminRefusal(t, ringAdminFrame(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeDatabase, Database: "zeta", Cursor: first.Cursor}), cluster.CodeInvalidArgument)
	if !strings.Contains(forged.Message, "describe_database") {
		t.Errorf("cursor refusal does not name the command: %s", forged.Message)
	}
	if text := ringAdminRefusal(t, ringAdminFrame(t, root, wire.RingAdminRequest{Cmd: wire.AdminDescribeDatabase, Database: "ghost"}), cluster.CodeNotFound).Message; !strings.Contains(text, "show databases") {
		t.Errorf("a missing database for root: %s", text)
	}
}

// TestU78ShowDatabasesWorkAndAllocationAreBoundedByThePage: asking for 1 of many databases
// returns 1, so a reply never grows with the databases the session cannot see.
func TestU78ShowDatabasesWorkAndAllocationAreBoundedByThePage(t *testing.T) {
	qs, root := u78Fixture(t)
	for i := range 40 {
		ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: fmt.Sprintf("db%02d", i)})
	}
	one := int32(1)
	page := ringAdmin(t, root, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases, Limit: &one})
	if len(page.Databases) != 1 || !page.More {
		t.Fatalf("page of 1 gave %v more=%v", u78Names(page), page.More)
	}
	nobody := ringAdminUser(t, qs, "nobody")
	if got := ringAdmin(t, nobody, wire.RingAdminRequest{Cmd: wire.AdminShowDatabases}); len(got.Databases) != 0 {
		t.Fatalf("a session with no grants sees %v among 43 databases", u78Names(got))
	}
}

// TestU78EveryNewClassIsRuledByItsLegacyOpcode: describe_database needs what LIST_BUCKETS
// needs (reader on the database); show_databases is ruled by LIST_DATABASES, which carries no
// action, so the filter, not a denial, is its rule.
func TestU78EveryNewClassIsRuledByItsLegacyOpcode(t *testing.T) {
	action, err := ringAction(wire.RingClassDescribeDatabase)
	if err != nil || action != opRules[transport.OpListBuckets].Action {
		t.Fatalf("describe_database needs %v, %v; LIST_BUCKETS needs %v", action, err, opRules[transport.OpListBuckets].Action)
	}
	if ringLegacyOpcode[wire.RingClassShowDatabases] != transport.OpListDBs {
		t.Fatalf("show_databases stands for %v", ringLegacyOpcode[wire.RingClassShowDatabases])
	}
	if rule := opRules[transport.OpListDBs]; rule.Action != ActionNone || rule.Scope != ScopeNone {
		t.Fatalf("LIST_DATABASES is now ruled %+v: show_databases must follow it", rule)
	}
	authz := newTestServer(t).RingAuthorizer()
	cases := []struct {
		name   string
		grants []transport.ScopedGrant
		db     string
		allow  bool
	}{
		{"no grants", nil, "app", false},
		{"reader of app, app", []transport.ScopedGrant{{Role: roleReader, DB: "app"}}, "app", true},
		{"reader of app, vault", []transport.ScopedGrant{{Role: roleReader, DB: "app"}}, "vault", false},
		{"wildcard, vault", []transport.ScopedGrant{{Role: roleReader, DB: "*"}}, "vault", true},
		{"any role counts, as in the legacy listing", []transport.ScopedGrant{{Role: roleUserManager, DB: "vault"}}, "vault", true},
	}
	for _, tc := range cases {
		err := authz.Authorize(u70Ctx(tc.grants...), wire.RingAccess{Database: tc.db, Class: wire.RingClassShowDatabases})
		if (err == nil) != tc.allow {
			t.Errorf("%s: %v, allow=%v", tc.name, err, tc.allow)
		}
	}
}
