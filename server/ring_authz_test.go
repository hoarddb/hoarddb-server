package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u70Ctx is a context carrying a session that holds grants.
func u70Ctx(grants ...transport.ScopedGrant) context.Context {
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "u70", Grants: grants})
	return transport.WithSession(context.Background(), sess)
}

// u70Legacy is the legacy opcodes each ring class stands for, from the spec's
// class table: a ring class needs what the legacy opcodes doing the same thing need.
var u70Legacy = map[wire.RingClass][]transport.Opcode{
	cluster.OpInsert: {transport.OpPut},
	cluster.OpUpsert: {transport.OpUpsert},
	cluster.OpUpdate: {transport.OpUpdate},
	cluster.OpDelete: {transport.OpDelete},
	cluster.OpStoreVerb: {transport.OpFIFOPush, transport.OpFIFOPop, transport.OpLIFOPush, transport.OpLIFOPop,
		transport.OpHeapPush, transport.OpHeapPop, transport.OpFieldPush, transport.OpFieldPop, transport.OpFieldPut,
		transport.OpFieldDelete, transport.OpBlobWrite, transport.OpBTreeInsert},
	cluster.OpClaim:          {transport.OpQueueClaim},
	cluster.OpAck:            {transport.OpQueueAck},
	cluster.OpRelease:        {transport.OpQueueRelease},
	cluster.OpExtend:         {transport.OpQueueExtend},
	wire.RingClassGet:        {transport.OpGet},
	wire.RingClassGetFields:  {transport.OpGet},
	wire.RingClassPeek:       {transport.OpFIFOPeek, transport.OpFieldPeek},
	wire.RingClassStoreList:  {transport.OpFieldList},
	wire.RingClassStoreCount: {transport.OpFieldLen},
	wire.RingClassNearest:    {transport.OpFieldList},
	// the aggregate read needs the list grant: its by forms list every group value
	wire.RingClassAggregate: {transport.OpFieldList},
	// A lease read is a peek at what a claim holds: it needs what a queue peek needs.
	wire.RingClassLeaseGet: {transport.OpFIFOPeek, transport.OpFieldPeek},
	// A keyed read of one element is the legacy read of a field element by key.
	wire.RingClassStoreGet: {transport.OpFieldGet},
	wire.RingClassList:     {transport.OpSearch, transport.OpScan},
	wire.RingClassCount:    {transport.OpCount},
	// RING_ADMIN: each command needs what the legacy opcode doing the same thing needs.
	wire.RingClassCreateDatabase: {transport.OpCreateDB},
	wire.RingClassCreateBucket:   {transport.OpCreateBucket},
	// alter_bucket is bucket DDL, ruled as the legacy alter bucket is.
	wire.RingClassAlterBucket: {transport.OpAlterBucket},
	// drop_bucket is bucket DDL, ruled as the legacy drop bucket is.
	wire.RingClassDropBucket:     {transport.OpDeleteBucket},
	wire.RingClassDescribeBucket: {transport.OpBucketInfo},
	wire.RingClassShowBuckets:    {transport.OpListBuckets},
	// describe_database lists a database's buckets, as LIST_BUCKETS does; show_databases is
	// LIST_DATABASES, which carries no action and filters by grant (TestU78).
	wire.RingClassDescribeDatabase: {transport.OpListBuckets},
	wire.RingClassShowDatabases:    {transport.OpListDBs},
	// RING_TOPOLOGY: the legacy list of databases, which needs no action and shows a name to a session holding any role on it.
	wire.RingClassTopology: {transport.OpListDBs},
	// RING_ADMIN describe_node: the legacy health request, which any authenticated user may send.
	wire.RingClassDescribeNode: {transport.OpHealthCheck},
	// Membership is a cluster operation, not a database one: adding a node (join, promote,
	// replace_node) is the legacy cluster add and removing one (leave, remove_node) the legacy
	// cluster remove, both cluster-manager.
	wire.RingClassJoin:        {transport.OpClusterAdd},
	wire.RingClassPromote:     {transport.OpClusterAdd},
	wire.RingClassLeave:       {transport.OpClusterRemove},
	wire.RingClassRemoveNode:  {transport.OpClusterRemove},
	wire.RingClassReplaceNode: {transport.OpClusterAdd},
	// Seeding is a cluster operation, not a database one: the status is the node listing and
	// the two actions are the legacy migrate (pull a bucket to this node), all cluster-manager.
	wire.RingClassSeedingStatus: {transport.OpListNodes},
	wire.RingClassSeedingRetry:  {transport.OpMigrate},
	wire.RingClassSeedingForget: {transport.OpMigrate},
	wire.RingClassCreateUser:    {transport.OpUserCreate},
	wire.RingClassAlterUser:     {transport.OpUserAlter},
	wire.RingClassDropUser:      {transport.OpUserDrop},
	wire.RingClassGrantRole:     {transport.OpUserGrant},
	wire.RingClassRevokeRole:    {transport.OpUserRevoke},
	wire.RingClassShowUsers:     {transport.OpUserList},
	wire.RingClassDescribeUser:  {transport.OpUserGet},
}

func TestU70EveryRingClassNeedsWhatItsLegacyOpcodesNeed(t *testing.T) {
	if len(u70Legacy) != len(wire.RingClasses()) {
		t.Fatalf("the test maps %d classes, the wire has %d", len(u70Legacy), len(wire.RingClasses()))
	}
	for _, class := range wire.RingClasses() {
		legacy, ok := u70Legacy[class]
		if !ok {
			t.Fatalf("class 0x%02x has no legacy opcode in the test", class)
		}
		if class == wire.RingClassShowDatabases {
			// Ruled by the grant filter, not by an action: TestU78EveryNewClassIsRuledByItsLegacyOpcode.
			if rule := opRules[legacy[0]]; rule.Action != ActionNone || rule.Scope != ScopeNone {
				t.Errorf("show_databases stands for %s, now ruled %+v", legacy[0], rule)
			}
			continue
		}
		if class == wire.RingClassTopology {
			if rule := opRules[transport.OpListDBs]; rule.Scope != ScopeNone || rule.Action != ActionNone {
				t.Errorf("topology stands for the list of databases, which now needs %s (scope %d): the topology authorization must follow it", rule.Action, rule.Scope)
			}
			continue
		}
		if class == wire.RingClassDescribeNode {
			if ringLegacyOpcode[class] != transport.OpHealthCheck {
				t.Errorf("describe_node stands for %v, want the health request", ringLegacyOpcode[class])
			}
			if rule := opRules[transport.OpHealthCheck]; rule.Scope != ScopeNone || rule.Action != ActionNone {
				t.Errorf("describe_node stands for the health request, which now needs %s (scope %d): its authorization must follow it", rule.Action, rule.Scope)
			}
			continue
		}
		if c6IsMembershipClass(class) || class == wire.RingClassSeedingStatus || class == wire.RingClassSeedingRetry || class == wire.RingClassSeedingForget {
			for _, op := range legacy {
				if rule := opRules[op]; rule.Action != ActionClusterAdmin || rule.Scope != ScopeNone {
					t.Errorf("class 0x%02x stands for %s, now ruled %+v: the unscoped cluster authorization must follow it", class, op, rule)
				}
				if ringLegacyOpcode[class] != op {
					t.Errorf("class 0x%02x stands for %v in the server, %v in the test", class, ringLegacyOpcode[class], op)
				}
			}
			continue
		}
		if _, users := wire.UserClassOf(u1CmdOf(class)); users {
			for _, op := range legacy {
				if rule := opRules[op]; rule.Action != ActionUserAdmin || rule.Scope != ScopeNone {
					t.Errorf("class 0x%02x stands for %s, now ruled %+v: the unscoped user authorization must follow it", class, op, rule)
				}
				if ringLegacyOpcode[class] != op {
					t.Errorf("class 0x%02x stands for %v in the server, %v in the test", class, ringLegacyOpcode[class], op)
				}
			}
			continue
		}
		action, err := ringAction(class)
		if err != nil {
			t.Fatalf("class 0x%02x: %v", class, err)
		}
		// The permission outcome is the same for two opcodes with one rule (peek and list are both a read on
		// the database), so the table must also say which opcode the server maps the class to.
		if len(legacy) == 1 && ringLegacyOpcode[class] != legacy[0] {
			t.Errorf("class 0x%02x stands for %v in the server and %v in the test", class, ringLegacyOpcode[class], legacy[0])
		}
		for _, op := range legacy {
			rule := opRules[op]
			if rule.Scope != ScopeDatabase || rule.Action != action {
				t.Errorf("class 0x%02x needs %s but %s needs %s (scope %d)", class, action, op, rule.Action, rule.Scope)
			}
		}
	}
}

func TestU70TheRealAuthorizerFollowsTheGrantTable(t *testing.T) {
	authz := newTestServer(t).RingAuthorizer()
	cases := []struct {
		name   string
		ctx    context.Context
		access wire.RingAccess
		allow  bool
	}{
		{"no session", context.Background(), wire.RingAccess{Database: "app", Class: wire.RingClassGet}, false},
		{"no grants, get", u70Ctx(), wire.RingAccess{Database: "app", Class: wire.RingClassGet}, false},
		{"no grants, anywhere", u70Ctx(), wire.RingAccess{Class: wire.RingClassGet}, false},
		{"no grants, lease_get", u70Ctx(), wire.RingAccess{Database: "app", Class: wire.RingClassLeaseGet}, false},
		{"no grants, store_get", u70Ctx(), wire.RingAccess{Database: "app", Class: wire.RingClassStoreGet}, false},
		{"no grants, get_fields", u70Ctx(), wire.RingAccess{Database: "app", Class: wire.RingClassGetFields}, false},
		{"reader reads fields of a document", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "app", Class: wire.RingClassGetFields}, true},
		{"reader reads fields only on its database", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "vault", Class: wire.RingClassGetFields}, false},
		{"reader reads one element by key", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "app", Class: wire.RingClassStoreGet}, true},
		{"reader reads one element only on its database", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "vault", Class: wire.RingClassStoreGet}, false},
		{"no grants, lease_get anywhere", u70Ctx(), wire.RingAccess{Class: wire.RingClassLeaseGet}, false},
		{"reader reads a lease", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "app", Class: wire.RingClassLeaseGet}, true},
		{"reader reads a lease only on its database", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "vault", Class: wire.RingClassLeaseGet}, false},
		{"reader reads", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "app", Class: wire.RingClassPeek}, true},
		{"reader does not write", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "app", Class: cluster.OpInsert}, false},
		{"reader does not write anywhere", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Class: cluster.OpStoreVerb}, false},
		{"writer writes", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "app"}), wire.RingAccess{Database: "app", Class: cluster.OpClaim}, true},
		{"writer on another database", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "app"}), wire.RingAccess{Database: "vault", Class: cluster.OpDelete}, false},
		{"writer holds it somewhere", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "app"}), wire.RingAccess{Class: cluster.OpDelete}, true},
		{"wildcard grant", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "*"}), wire.RingAccess{Database: "vault", Class: cluster.OpAck}, true},
		{"a role of another kind", u70Ctx(transport.ScopedGrant{Role: roleUserManager, DB: "*"}), wire.RingAccess{Database: "app", Class: wire.RingClassGet}, false},
		{"unknown class", u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "*"}), wire.RingAccess{Database: "app", Class: 0x55}, false},
	}
	for _, tc := range cases {
		err := authz.Authorize(tc.ctx, tc.access)
		if (err == nil) != tc.allow {
			t.Errorf("%s: Authorize returned %v, allow=%v", tc.name, err, tc.allow)
		}
	}
}

func TestU70APeerSessionIsNeverAuthorizedForARingClass(t *testing.T) {
	sess := &transport.Session{Peer: true}
	sess.SetIdentity(transport.Identity{Username: NodeUser, Grants: []transport.ScopedGrant{{Role: roleRoot, DB: "*"}}})
	ctx := transport.WithSession(context.Background(), sess)
	authz := newTestServer(t).RingAuthorizer()
	for _, class := range wire.RingClasses() {
		if err := authz.Authorize(ctx, wire.RingAccess{Database: "app", Class: class}); err == nil {
			t.Errorf("class 0x%02x: a session on the internode listener was authorized", class)
		}
	}
}

func TestU70TheEmptyDatabaseAskIsAuditedAsNoGrantOnAnyDatabase(t *testing.T) {
	err := newTestServer(t).RingAuthorizer().Authorize(u70Ctx(), wire.RingAccess{Class: wire.RingClassGet})
	if err == nil || !strings.Contains(err.Error(), "no grant on any database") || strings.Contains(err.Error(), `"*"`) {
		t.Fatalf("the refusal reads %v, want it to say the user has no grant on any database", err)
	}
}

// TestU50AnyAuthenticatedSessionMayAskTheTopologyAndSeesItsDatabases: [GUARD the class-only ask needs an
// identity and nothing else, the database ask needs a role on that database as the legacy list does,
// and a session with no identity or on the internode listener gets neither].
func TestU50AnyAuthenticatedSessionMayAskTheTopologyAndSeesItsDatabases(t *testing.T) {
	authz := newTestServer(t).RingAuthorizer()
	peer := &transport.Session{Peer: true}
	peer.SetIdentity(transport.Identity{Username: NodeUser, Grants: []transport.ScopedGrant{{Role: roleRoot, DB: "*"}}})
	anonymous := transport.WithSession(context.Background(), &transport.Session{})
	cases := []struct {
		name   string
		ctx    context.Context
		access wire.RingAccess
		allow  bool
	}{
		{"no session", context.Background(), wire.RingAccess{Class: wire.RingClassTopology}, false},
		{"a session with no identity", anonymous, wire.RingAccess{Class: wire.RingClassTopology}, false},
		{"an internode session", transport.WithSession(context.Background(), peer), wire.RingAccess{Class: wire.RingClassTopology}, false},
		{"no grants, class only", u70Ctx(), wire.RingAccess{Class: wire.RingClassTopology}, true},
		{"no grants, a database", u70Ctx(), wire.RingAccess{Database: "app", Class: wire.RingClassTopology}, false},
		{"reader of app sees app", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "app", Class: wire.RingClassTopology}, true},
		{"reader of app does not see vault", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), wire.RingAccess{Database: "vault", Class: wire.RingClassTopology}, false},
		{"writer of vault sees vault", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "vault"}), wire.RingAccess{Database: "vault", Class: wire.RingClassTopology}, true},
		{"a star grant sees every database", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "*"}), wire.RingAccess{Database: "vault", Class: wire.RingClassTopology}, true},
	}
	for _, tc := range cases {
		err := authz.Authorize(tc.ctx, tc.access)
		if (err == nil) != tc.allow {
			t.Errorf("%s: Authorize = %v, want allow=%v", tc.name, err, tc.allow)
		}
	}
}

// TestU50TopologyAndShowDatabasesSeeTheSameDatabases: [GUARD both classes answer the same for the same grants, so
// show_databases and TOPOLOGY can never disagree on which databases a session sees].
func TestU50TopologyAndShowDatabasesSeeTheSameDatabases(t *testing.T) {
	authz := newTestServer(t).RingAuthorizer()
	grantSets := map[string][]transport.ScopedGrant{
		"no grants":          nil,
		"reader@app":         {{Role: roleReader, DB: "app"}},
		"writer@*":           {{Role: roleWriter, DB: "*"}},
		"user-manager@vault": {{Role: roleUserManager, DB: "vault"}},
	}
	for name, grants := range grantSets {
		ctx := u70Ctx(grants...)
		for _, database := range []string{"app", "vault", "other"} {
			topology := authz.Authorize(ctx, wire.RingAccess{Database: database, Class: wire.RingClassTopology}) == nil
			shown := authz.Authorize(ctx, wire.RingAccess{Database: database, Class: wire.RingClassShowDatabases}) == nil
			if topology != shown {
				t.Errorf("%s, %s: TOPOLOGY says %v and show_databases says %v", name, database, topology, shown)
			}
		}
	}
}

func c6IsMembershipClass(class wire.RingClass) bool {
	return class >= wire.RingClassJoin && class <= wire.RingClassReplaceNode
}

// TestC6MembershipClassesNeedTheClusterManagerRole: [GUARD the five classes are allowed to
// cluster-manager@* and root@* and to nobody else (a grant on one database is denied) (reader, writer, db-manager,
// user-manager, backup, no grants, no session, internode peer), whether the ask names a
// database or not, and a denial names the cluster-manager role it lacks].
func TestC6MembershipClassesNeedTheClusterManagerRole(t *testing.T) {
	authz := newTestServer(t).RingAuthorizer()
	cases := []struct {
		name  string
		ctx   context.Context
		allow bool
	}{
		{"cluster-manager", u70Ctx(transport.ScopedGrant{Role: roleClusterManager, DB: "*"}), true},
		// Owner ruling 2026-11-01: a node-wide command needs the unscoped grant.
		{"cluster-manager on one database", u70Ctx(transport.ScopedGrant{Role: roleClusterManager, DB: "app"}), false},
		{"root on one database", u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "app"}), false},
		{"cluster-manager on one database and a reader elsewhere", u70Ctx(transport.ScopedGrant{Role: roleClusterManager, DB: "app"}, transport.ScopedGrant{Role: roleReader, DB: "*"}), false},
		{"root", u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "*"}), true},
		{"db-manager", u70Ctx(transport.ScopedGrant{Role: roleDBManager, DB: "*"}), false},
		{"writer", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "*"}), false},
		{"reader", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "*"}), false},
		{"user-manager", u70Ctx(transport.ScopedGrant{Role: roleUserManager, DB: "*"}), false},
		{"backup-operator", u70Ctx(transport.ScopedGrant{Role: roleBackupOperator, DB: "*"}), false},
		{"no grants", u70Ctx(), false},
		{"no session", context.Background(), false},
	}
	for _, class := range []wire.RingClass{wire.RingClassJoin, wire.RingClassPromote, wire.RingClassLeave, wire.RingClassRemoveNode, wire.RingClassReplaceNode} {
		for _, tc := range cases {
			for _, database := range []string{"", "app"} {
				err := authz.Authorize(tc.ctx, wire.RingAccess{Database: database, Class: class})
				if (err == nil) != tc.allow {
					t.Errorf("class 0x%02x, %s, database %q: Authorize returned %v, allow=%v", class, tc.name, database, err, tc.allow)
				}
			}
		}
		if op, ok := ringMembershipOpcode(class); !ok || op != ringLegacyOpcode[class] {
			t.Errorf("class 0x%02x is authorized as %v, and the table says it stands for %v", class, op, ringLegacyOpcode[class])
		}
		peer := &transport.Session{Peer: true}
		peer.SetIdentity(transport.Identity{Username: NodeUser, Grants: []transport.ScopedGrant{{Role: roleRoot, DB: "*"}}})
		if err := authz.Authorize(transport.WithSession(context.Background(), peer), wire.RingAccess{Class: class}); err == nil {
			t.Errorf("class 0x%02x: a session on the internode listener was authorized", class)
		}
		err := authz.Authorize(u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "*"}), wire.RingAccess{Class: class})
		if err == nil || !strings.Contains(err.Error(), roleClusterManager) {
			t.Errorf("class 0x%02x: the denial %v does not name the cluster-manager role it lacks", class, err)
		}
	}
}

// TestC6MembershipClassesAreDefaultDenied: [GUARD a membership class is refused by the
// database-scoped path (ringAction), and when the legacy rule it stands for stops being a
// cluster-manager rule with no database the class is refused for every user, root included].
func TestC6MembershipClassesAreDefaultDenied(t *testing.T) {
	for _, class := range []wire.RingClass{wire.RingClassJoin, wire.RingClassPromote, wire.RingClassLeave, wire.RingClassRemoveNode, wire.RingClassReplaceNode} {
		if action, err := ringAction(class); err == nil {
			t.Errorf("ringAction gave class 0x%02x the database action %s", class, action)
		}
	}
	root := u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "*"})
	saved := opRules[transport.OpClusterAdd]
	opRules[transport.OpClusterAdd] = OpRule{Action: ActionRead, Scope: ScopeDatabase}
	err := newTestServer(t).RingAuthorizer().Authorize(root, wire.RingAccess{Class: wire.RingClassJoin})
	opRules[transport.OpClusterAdd] = saved
	if err == nil {
		t.Errorf("join was allowed to root after %s stopped being a cluster-manager rule", transport.OpClusterAdd)
	}
}

// TestC6MembershipClassBytesAreNotOpcodes: [GUARD a class byte read in an audit line never
// reads as a transport opcode: none of the five has a rule in opRules].
func TestC6MembershipClassBytesAreNotOpcodes(t *testing.T) {
	for class := wire.RingClassJoin; class <= wire.RingClassReplaceNode; class++ {
		if _, ok := opRules[transport.Opcode(class)]; ok {
			t.Errorf("class 0x%02x is also a transport opcode with an authorization rule", class)
		}
	}
}

// TestT6SeedingClassesNeedTheClusterManagerRole: [GUARD status, retry and forget are allowed
// to cluster-manager and root on any database and to nobody else (reader, writer, db-manager,
// user-manager, backup, no grants, no session, internode peer), whether the ask names a
// database or not, and a denial is audited as a missing cluster-manager grant].
func TestT6SeedingClassesNeedTheClusterManagerRole(t *testing.T) {
	authz := newTestServer(t).RingAuthorizer()
	classes := []wire.RingClass{wire.RingClassSeedingStatus, wire.RingClassSeedingRetry, wire.RingClassSeedingForget}
	cases := []struct {
		name  string
		ctx   context.Context
		allow bool
	}{
		{"cluster-manager", u70Ctx(transport.ScopedGrant{Role: roleClusterManager, DB: "*"}), true},
		// Cluster roles are cluster-wide: status prints other databases' names and slots, so a grant scoped to one database is not enough.
		{"cluster-manager on one database", u70Ctx(transport.ScopedGrant{Role: roleClusterManager, DB: "app"}), false},
		{"cluster-manager on one database and another", u70Ctx(transport.ScopedGrant{Role: roleClusterManager, DB: "app"}, transport.ScopedGrant{Role: roleReader, DB: "*"}), false},
		{"root", u70Ctx(transport.ScopedGrant{Role: roleRoot, DB: "*"}), true},
		{"db-manager", u70Ctx(transport.ScopedGrant{Role: roleDBManager, DB: "*"}), false},
		{"writer", u70Ctx(transport.ScopedGrant{Role: roleWriter, DB: "*"}), false},
		{"reader", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "*"}), false},
		{"user-manager", u70Ctx(transport.ScopedGrant{Role: roleUserManager, DB: "*"}), false},
		{"backup-operator", u70Ctx(transport.ScopedGrant{Role: roleBackupOperator, DB: "*"}), false},
		{"no grants", u70Ctx(), false},
		{"no session", context.Background(), false},
	}
	for _, class := range classes {
		for _, tc := range cases {
			for _, database := range []string{"", "app"} {
				err := authz.Authorize(tc.ctx, wire.RingAccess{Database: database, Class: class})
				if (err == nil) != tc.allow {
					t.Errorf("class 0x%02x, %s, database %q: Authorize returned %v, allow=%v", class, tc.name, database, err, tc.allow)
				}
			}
		}
		peer := &transport.Session{Peer: true}
		peer.SetIdentity(transport.Identity{Username: NodeUser, Grants: []transport.ScopedGrant{{Role: roleRoot, DB: "*"}}})
		if err := authz.Authorize(transport.WithSession(context.Background(), peer), wire.RingAccess{Class: class}); err == nil {
			t.Errorf("class 0x%02x: a session on the internode listener was authorized", class)
		}
	}
	err := authz.Authorize(u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "*"}), wire.RingAccess{Class: wire.RingClassSeedingRetry})
	if err == nil || !strings.Contains(err.Error(), roleClusterManager) {
		t.Errorf("the denial %v does not name the cluster-manager role it lacks", err)
	}
}

// TestARingAggregateReadNeedsTheListGrant: an aggregate read is served under the list grant, not the peek grant,
// because its by forms list every group value of a store. Without the mapping the class maps to no opcode.
func TestARingAggregateReadNeedsTheListGrant(t *testing.T) {
	if got := ringLegacyOpcode[wire.RingClassAggregate]; got != transport.OpFieldList {
		t.Fatalf("an aggregate read maps to %v, want OpFieldList", got)
	}
	if ringLegacyOpcode[wire.RingClassAggregate] == transport.OpFieldPeek {
		t.Fatal("an aggregate read needs the list grant; a peek grant must not be enough")
	}
}
