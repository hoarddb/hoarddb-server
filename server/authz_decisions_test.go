package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0098 removes two costs from the authorization gate: the payload decode it
// shared with nothing, and the identity copy every authorized request paid.
// Both live on the path that decides allow or deny, so this file comes first:
// it pins today's decisions for every opcode in the table and every role, from
// expectations written here by hand rather than from identityAllows, and the
// optimisation that follows must keep it green.

// gateRoles is every role the model names, plus one it does not: a retired or
// invented role name must grant nothing.
var gateRoles = []string{
	roleRoot, roleReader, roleWriter, roleDBManager, roleUserManager,
	roleClusterManager, roleBackupOperator, roleRestoreOperator,
	"a-role-that-does-not-exist",
}

// gateAllowedRoles is the permission model as a table an independent reader can
// check: which role satisfies which action. It is deliberately not derived from
// roleSatisfies, which is the code under guard.
var gateAllowedRoles = map[Action]map[string]bool{
	ActionRead:         {roleRoot: true, roleReader: true, roleWriter: true, roleDBManager: true, roleBackupOperator: true},
	ActionWrite:        {roleRoot: true, roleWriter: true, roleDBManager: true},
	ActionDBAdmin:      {roleRoot: true, roleDBManager: true},
	ActionUserAdmin:    {roleRoot: true, roleUserManager: true},
	ActionClusterAdmin: {roleRoot: true, roleClusterManager: true},
	ActionBackup:       {roleRoot: true, roleBackupOperator: true},
	ActionRestore:      {roleRoot: true, roleRestoreOperator: true},
}

// gateFrame is a frame the gate can read a scope from, naming database and
// bucket "app" in the encoding the opcode's handler decodes.
func gateFrame(t *testing.T, op transport.Opcode) *transport.Frame {
	t.Helper()
	if op == transport.OpGetFast {
		return transport.NewFrame(op, mustGetFastPayload(t, "app", "app", "k1"))
	}
	return transport.NewFrame(op, mustMarshalBSON(t, map[string]interface{}{
		"database": "app", "bucket": "app", "name": "app", "key": "k1",
	}))
}

// gateSession is a session authenticated as username holding exactly grants.
func gateSession(username string, grants ...transport.ScopedGrant) *transport.Session {
	identity := ident(grants...)
	identity.Username = username
	sess := &transport.Session{}
	sess.SetIdentity(identity)
	return sess
}

// gateAuthorize applies the gate to frame on a session, if any.
func gateAuthorize(s *Server, sess *transport.Session, frame *transport.Frame) error {
	ctx := context.Background()
	if sess != nil {
		ctx = transport.WithSession(ctx, sess)
	}
	return s.authorize(ctx, frame)
}

// TestAuthorizeDecisionsPerOpcodeAndRole pins allow/deny for every opcode the
// table rules, against every role, a grant in the frame's database, a grant
// elsewhere, a global grant, no identity, and no session at all.
func TestAuthorizeDecisionsPerOpcodeAndRole(t *testing.T) {
	s := userTestServer(t)
	seenActions := map[Action]bool{}

	for op, rule := range opRules {
		seenActions[rule.Action] = true
		t.Run(op.String(), func(t *testing.T) {
			frame := gateFrame(t, op)

			if rule.Action == ActionNone {
				// No privilege beyond AUTH: allowed with any identity, an
				// empty one, or none at all.
				if err := gateAuthorize(s, gateSession("nobody"), frame); err != nil {
					t.Errorf("%s is ActionNone but was refused for an authenticated session: %v", op, err)
				}
				if err := gateAuthorize(s, nil, frame); err != nil {
					t.Errorf("%s is ActionNone but was refused with no session: %v", op, err)
				}
				return
			}

			for _, role := range gateRoles {
				allowed := gateAllowedRoles[rule.Action][role]
				if _, unscoped := unscopedOpcodes[op]; unscoped {
					// Owner ruling 2026-11-01: a node-wide command needs the unscoped grant.
					allowed = false
				}
				err := gateAuthorize(s, gateSession("alice", transport.ScopedGrant{Role: role, DB: "app"}), frame)
				if allowed && err != nil {
					t.Errorf("role %q satisfies %s but %s was refused: %v", role, rule.Action, op, err)
				}
				if !allowed && err == nil {
					t.Errorf("role %q does not satisfy %s but %s was allowed", role, rule.Action, op)
				}
			}

			// A global grant satisfies the action on any database.
			privileged := roleThatSatisfies(rule.Action)
			if err := gateAuthorize(s, gateSession("alice", transport.ScopedGrant{Role: privileged, DB: "*"}), frame); err != nil {
				t.Errorf("%s@* must allow %s: %v", privileged, op, err)
			}

			if _, unscoped := unscopedOpcodes[op]; unscoped {
				// Cluster roles are cluster-wide (owner ruling 2026-11-01): the legacy cluster add
				// and remove are node-wide and a grant in a named database does not satisfy them.
				if err := gateAuthorize(s, gateSession("alice", transport.ScopedGrant{Role: privileged, DB: "elsewhere"}), frame); err == nil {
					t.Errorf("%s is node-wide but a grant in \"elsewhere\" was allowed", op)
				}
			} else if rule.Scope == ScopeNone {
				// Any other ScopeNone rule checks no resource, so a grant in a named
				// database satisfies it too (databaseMatches treats "" as covered).
				if err := gateAuthorize(s, gateSession("alice", transport.ScopedGrant{Role: privileged, DB: "elsewhere"}), frame); err != nil {
					t.Errorf("%s is ScopeNone, so a grant in any database must allow it: %v", op, err)
				}
			} else {
				// A scoped rule must not be satisfied by a grant elsewhere.
				if err := gateAuthorize(s, gateSession("alice", transport.ScopedGrant{Role: privileged, DB: "elsewhere"}), frame); err == nil {
					t.Errorf("%s is scoped to %q but a grant in \"elsewhere\" was allowed", op, rule.Scope)
				}
			}

			if err := gateAuthorize(s, gateSession("anonymous"), frame); err == nil {
				t.Errorf("%s requires %s but a session with no grants was allowed", op, rule.Action)
			}
			if err := gateAuthorize(s, nil, frame); err != nil {
				t.Errorf("%s was refused with no session in the context: a direct handler call is not a client frame (the transport refuses anonymous frames before this gate)", op)
			}
		})
	}

	// "Every opcode class" is a claim about the table; fail if one disappears.
	for _, action := range []Action{ActionNone, ActionRead, ActionWrite, ActionDBAdmin,
		ActionUserAdmin, ActionClusterAdmin, ActionBackup, ActionRestore} {
		if !seenActions[action] {
			t.Errorf("opRules no longer rules any opcode with %s; this table was written against all eight", action)
		}
	}
}

// roleThatSatisfies names one role that satisfies action, from the same
// independent table, and fails when none does.
func roleThatSatisfies(action Action) string {
	for _, role := range gateRoles {
		if gateAllowedRoles[action][role] {
			return role
		}
	}
	return ""
}

// TestAuthorizeDecisionsBySessionShape pins the shapes that bypass or change
// the role matrix: a cluster member, an internode session that is not one, an
// opcode with no rule at all, and an unreadable payload.
func TestAuthorizeDecisionsBySessionShape(t *testing.T) {
	s := userTestServer(t)
	frame := gateFrame(t, transport.OpPut)

	member := gateSession(NodeUser)
	member.Peer = true
	// A member passes the gate for OpMigrate only; it forwards client
	// writes inside OpReplPush, which is dispatched above the gate.
	migrate := gateFrame(t, transport.OpMigrate)
	if err := gateAuthorize(s, member, migrate); err != nil {
		t.Errorf("a cluster member's OpMigrate was refused: %v", err)
	}
	if err := gateAuthorize(s, member, frame); !errors.Is(err, ErrPermissionDenied) {
		t.Errorf("a cluster member's OpPut: got %v, want ErrPermissionDenied", err)
	}

	// On the internode listener and authenticated as anything but the node
	// identity, a privileged opcode is refused whatever the account holds.
	notMember := gateSession("alice", transport.ScopedGrant{Role: roleRoot, DB: "*"})
	notMember.Peer = true
	if err := gateAuthorize(s, notMember, frame); err == nil {
		t.Error("an internode session that is not a cluster member was allowed a write")
	}
	// ActionNone opcodes are how such a session authenticates at all.
	ping := transport.NewFrame(transport.OpPing, nil)
	if err := gateAuthorize(s, notMember, ping); err != nil {
		t.Errorf("an internode session was refused PING, which is ActionNone: %v", err)
	}

	root := gateSession("alice", transport.ScopedGrant{Role: roleRoot, DB: "*"})
	unruled := transport.NewFrame(transport.Opcode(0xFE), nil)
	if err := gateAuthorize(s, root, unruled); err == nil {
		t.Error("an opcode with no rule was allowed; the table is default deny")
	}

	undecodable := transport.NewFrame(transport.OpPut, []byte{1, 2, 3})
	if err := gateAuthorize(s, root, undecodable); err == nil {
		t.Error("a payload the gate cannot read was allowed even for root; it must be refused, not checked as unscoped")
	}

	unscoped := transport.NewFrame(transport.OpPut, mustMarshalBSON(t, map[string]interface{}{"key": "k1"}))
	if err := gateAuthorize(s, root, unscoped); err == nil {
		t.Error("a scoped frame that names no database was allowed even for root")
	}
}

// gateShapeRow is one decision row for a session shape the opcode/role table
// cannot express: how the session came to hold an identity, which listener it
// arrived on, or how it authenticated. want is a fragment of the denial.
type gateShapeRow struct {
	name    string
	session func(t *testing.T) *transport.Session
	frame   *transport.Frame
	allowed bool
	want    string
}

// TestAuthorizeDecisionsBySessionLifecycle pins the shapes that are about a
// session rather than about a role: an authenticated connection that never set
// an identity, the node identity on a client connection, a peer connection
// with no identity, a resumed token session, and an identity replaced
// mid-session. Every row records today's decision.
func TestAuthorizeDecisionsBySessionLifecycle(t *testing.T) {
	s := userTestServer(t)
	put := gateFrame(t, transport.OpPut)
	get := gateFrame(t, transport.OpGet)
	migrate := gateFrame(t, transport.OpMigrate)
	ping := transport.NewFrame(transport.OpPing, nil)

	// resumedSession is a session that authenticated (by token when resumed is
	// true, by password otherwise) holding exactly grants.
	resumedSession := func(resumed bool, grants ...transport.ScopedGrant) func(*testing.T) *transport.Session {
		return func(t *testing.T) *transport.Session {
			identity := ident(grants...)
			identity.Username = "alice"
			identity.Resumed = resumed
			sess := &transport.Session{}
			sess.SetIdentity(identity)
			return sess
		}
	}

	// narrowedMidSession is a root session whose identity is replaced by a
	// reader's before the gate sees it. The pre-narrow decision is asserted in
	// the setup, so a gate that ignored the replacement would still fail the
	// post-narrow row.
	narrowedMidSession := func(t *testing.T) *transport.Session {
		sess := gateSession("alice", transport.ScopedGrant{Role: roleRoot, DB: "*"})
		if err := gateAuthorize(s, sess, put); err != nil {
			t.Fatalf("a root session was refused PUT before it was narrowed: %v", err)
		}
		narrowed := ident(transport.ScopedGrant{Role: roleReader, DB: "app"})
		narrowed.Username = "alice"
		sess.SetIdentity(narrowed)
		return sess
	}

	rows := []gateShapeRow{
		{
			name: "an authenticated session that never set an identity",
			session: func(t *testing.T) *transport.Session {
				sess := &transport.Session{}
				sess.MarkAuthenticated()
				return sess
			},
			frame: put,
			want:  "no authenticated identity",
		},
		{
			name:    "the node identity on a client connection is not a member: MIGRATE",
			session: func(t *testing.T) *transport.Session { return gateSession(NodeUser) },
			frame:   migrate,
			want:    `lacks "cluster-manager"`,
		},
		{
			name:    "the node identity on a client connection is not a member: PUT",
			session: func(t *testing.T) *transport.Session { return gateSession(NodeUser) },
			frame:   put,
			want:    `lacks "writer"`,
		},
		{
			name:    "a peer connection with no identity is not a member: PUT",
			session: func(t *testing.T) *transport.Session { return &transport.Session{Peer: true} },
			frame:   put,
			want:    "cluster members only",
		},
		{
			name:    "a peer connection with no identity is not a member: MIGRATE",
			session: func(t *testing.T) *transport.Session { return &transport.Session{Peer: true} },
			frame:   migrate,
			want:    "cluster members only",
		},
		{
			name:    "a peer connection with no identity still reaches an ActionNone opcode",
			session: func(t *testing.T) *transport.Session { return &transport.Session{Peer: true} },
			frame:   ping,
			allowed: true,
		},
		{
			name:    "a resumed reader is refused a write, as a password reader is",
			session: resumedSession(true, transport.ScopedGrant{Role: roleReader, DB: "app"}),
			frame:   put,
			want:    `lacks "writer"`,
		},
		{
			name:    "a resumed root is allowed a write, as a password root is",
			session: resumedSession(true, transport.ScopedGrant{Role: roleRoot, DB: "*"}),
			frame:   put,
			allowed: true,
		},
		{
			name:    "an identity narrowed mid-session is followed at once: the write is refused",
			session: narrowedMidSession,
			frame:   put,
			want:    `lacks "writer"`,
		},
		{
			name:    "an identity narrowed mid-session is followed at once: the read still passes",
			session: narrowedMidSession,
			frame:   get,
			allowed: true,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			err := gateAuthorize(s, row.session(t), row.frame)
			if row.allowed {
				if err != nil {
					t.Fatalf("%s was refused: %v", row.frame.Opcode, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("%s was allowed; today the gate refuses it", row.frame.Opcode)
			}
			if !errors.Is(err, ErrPermissionDenied) {
				t.Fatalf("%s: got %v, want ErrPermissionDenied", row.frame.Opcode, err)
			}
			if !strings.Contains(err.Error(), row.want) {
				t.Fatalf("%s denial = %q, want it to mention %q", row.frame.Opcode, err, row.want)
			}
		})
	}

	// Resumed is audit information; the gate reads Grants, never Resumed. A
	// token session and a password session holding the same grants must make
	// the same decision on every row, compared here so a rule that trusted a
	// resumed session cannot pass by denying only some of them.
	grantSets := [][]transport.ScopedGrant{
		{{Role: roleReader, DB: "app"}},
		{{Role: roleWriter, DB: "app"}},
		{{Role: roleReader, DB: "logs"}, {Role: roleWriter, DB: "app"}},
		{{Role: roleDBManager, DB: "app"}},
		{{Role: roleRoot, DB: "*"}},
	}
	checks := []struct {
		name  string
		frame *transport.Frame
	}{
		{"read", get},
		{"write", put},
		{"cluster admin", migrate},
		{"ping", ping},
	}
	for _, grants := range grantSets {
		for _, check := range checks {
			passwordErr := gateAuthorize(s, resumedSession(false, grants...)(t), check.frame)
			resumedErr := gateAuthorize(s, resumedSession(true, grants...)(t), check.frame)
			if (passwordErr == nil) != (resumedErr == nil) {
				t.Errorf("%s with grants %v: the password session got %v but the resumed session got %v; a token session carries the same grants and must make the same decision",
					check.name, grants, passwordErr, resumedErr)
			}
		}
	}
}

// TestRuleResourceUsesTheFieldItsScopeNames pins which payload field becomes
// the checked resource: data opcodes name a database, database DDL names one in
// "name", and a bucket-scoped rule (none is tabled today) names the bucket.
func TestRuleResourceUsesTheFieldItsScopeNames(t *testing.T) {
	data := transport.NewFrame(transport.OpPut, mustMarshalBSON(t, map[string]interface{}{
		"database": "app", "bucket": "users", "name": "ignored",
	}))
	ddl := transport.NewFrame(transport.OpCreateDB, mustMarshalBSON(t, map[string]interface{}{
		"database": "ignored", "bucket": "ignored", "name": "logs",
	}))

	cases := []struct {
		name     string
		frame    *transport.Frame
		rule     OpRule
		resource string
	}{
		{"unscoped rule reads nothing", data, OpRule{ActionNone, ScopeNone}, ""},
		{"data opcode is its database", data, OpRule{ActionRead, ScopeDatabase}, "app"},
		{"database DDL is its name", ddl, OpRule{ActionDBAdmin, ScopeDatabase}, "logs"},
		{"bucket scope is the bucket", data, OpRule{ActionRead, ScopeBucket}, "users"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resource, err := ruleResource(tc.frame, tc.rule)
			if err != nil {
				t.Fatalf("ruleResource: %v", err)
			}
			if resource != tc.resource {
				t.Fatalf("resource = %q, want %q", resource, tc.resource)
			}
		})
	}

	getFast := gateFrame(t, transport.OpGetFast)
	if resource, err := ruleResource(getFast, OpRule{ActionRead, ScopeDatabase}); err != nil || resource != "app" {
		t.Fatalf("GET_FAST's scope = %q, %v; want \"app\" from its own wire layout", resource, err)
	}
}

// TestFrameScopeReadsLikeTheSharedMetaDecoder is BUG-0098 part 1's safety net.
// The gate used to decode the payload with its own bson.Unmarshal; it now goes
// through the same decoder the write path uses, which returns the error the gate
// needs to refuse an unreadable payload (BUG-0113). The two readings must agree
// on whether a payload can be read and on the scope it names, including
// payloads whose routing fields have the wrong type and payloads that are not
// BSON at all.
func TestFrameScopeReadsLikeTheSharedMetaDecoder(t *testing.T) {
	cases := []struct {
		name    string
		op      transport.Opcode
		payload []byte
	}{
		{"a data request", transport.OpPut, mustMarshalBSON(t, map[string]interface{}{
			"database": "app", "bucket": "users", "key": "k1", "write_concern": "majority",
		})},
		{"a bucket DDL request names its bucket", transport.OpCreateBucket, mustMarshalBSON(t, map[string]interface{}{
			"database": "app", "name": "users", "type": "hash",
		})},
		{"a database DDL request names its database", transport.OpCreateDB, mustMarshalBSON(t, map[string]interface{}{
			"name": "logs",
		})},
		{"duplicate keys: the last wins", transport.OpPut, wireRequest(
			wireString("database", "app"), wireString("database", "logs"))},
		{"an unknown nested field is skipped", transport.OpGet, mustMarshalBSON(t, map[string]interface{}{
			"database": "app", "bucket": "users", "options": map[string]interface{}{"a": 1},
		})},
		{"a routing field of the wrong type", transport.OpPut, mustMarshalBSON(t, map[string]interface{}{
			"database": "app", "bucket": "users", "write_concern": int32(5),
		})},
		{"a nested document where a bucket belongs", transport.OpGet, mustMarshalBSON(t, map[string]interface{}{
			"database": "app", "bucket": map[string]interface{}{"a": 1},
		})},
		{"bytes that are not BSON", transport.OpPut, []byte{1, 2, 3}},
		{"an empty payload", transport.OpPut, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame := transport.NewFrame(tc.op, tc.payload)
			meta, metaErr := decodeFrameMetaErr("test", tc.op, tc.payload)
			bucket, database, scopeErr := frameScope(frame)

			if (metaErr != nil) != (scopeErr != nil) {
				t.Fatalf("the shared decoder returns error %v but the gate returns %v for the same payload", metaErr, scopeErr)
			}
			if scopeErr != nil {
				return
			}
			wantBucket, wantDatabase := meta.scope(tc.op)
			if bucket != wantBucket || database != wantDatabase {
				t.Fatalf("the gate reads bucket %q in database %q; the shared decoder reads bucket %q in database %q",
					bucket, database, wantBucket, wantDatabase)
			}
		})
	}
}
