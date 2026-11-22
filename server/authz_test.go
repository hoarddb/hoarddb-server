package server

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// mustMarshalBSON encodes a request for a gate test.
func mustMarshalBSON(t *testing.T, value interface{}) []byte {
	t.Helper()
	data, err := bson.Marshal(value)
	if err != nil {
		t.Fatalf("marshal BSON: %v", err)
	}
	return data
}

// TestEveryOpcodeHasARule is the mechanical guard the lesson
// demands: a rule table a test cannot notice is not a rule table. It parses
// the transport constants and fails when one is added without a rule.
func TestEveryOpcodeHasARule(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "../transport/opcodes.go", nil, 0)
	if err != nil {
		t.Fatalf("parse transport/opcodes.go: %v", err)
	}

	found := make(map[string]uint8)
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		ident, ok := spec.Type.(*ast.Ident)
		if !ok || ident.Name != "Opcode" {
			return true
		}
		for i, name := range spec.Names {
			if i >= len(spec.Values) {
				continue
			}
			lit, ok := spec.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.INT {
				continue
			}
			value, err := strconv.ParseUint(lit.Value, 0, 8)
			if err != nil {
				t.Fatalf("parse opcode value for %s: %v", name.Name, err)
			}
			found[name.Name] = uint8(value)
		}
		return true
	})
	if len(found) < 40 {
		t.Fatalf("parsed only %d opcode constants; the parser or the file layout changed", len(found))
	}
	for name, value := range found {
		if _, ok := opRules[transport.Opcode(value)]; !ok {
			t.Errorf("opcode %s (0x%02X) has no authorization rule; add it to opRules", name, value)
		}
	}
}

// TestGateRefusesUnruledOpcode proves default deny: an opcode absent from the
// table is refused rather than silently allowed.
func TestGateRefusesUnruledOpcode(t *testing.T) {
	s := userTestServer(t)
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "root", Roles: []string{"root"}, Databases: []string{"*"}, Grants: []transport.ScopedGrant{{Role: "root", DB: "*"}}})
	ctx := transport.WithSession(context.Background(), sess)

	err := s.authorize(ctx, transport.NewFrame(transport.Opcode(0xFE), nil))
	if err == nil {
		t.Fatal("an unruled opcode was allowed, want refusal")
	}
	if !strings.Contains(err.Error(), "no authorization rule") {
		t.Fatalf("error %q does not say a rule is missing", err)
	}
}

// TestAuthorizationRoleOutcomes covers the spec 8.3 cases at the predicate.
// Every identity carries scoped grants because the gate no longer reads the
// flat role/database lists.
func TestAuthorizationRoleOutcomes(t *testing.T) {
	cases := []struct {
		name     string
		identity transport.Identity
		action   Action
		resource string
		allowed  bool
	}{
		{"read user can get", ident(transport.ScopedGrant{Role: "reader", DB: "app"}), ActionRead, "app", true},
		{"read user cannot write", ident(transport.ScopedGrant{Role: "reader", DB: "app"}), ActionWrite, "app", false},
		{"writer user writes its db", ident(transport.ScopedGrant{Role: "writer", DB: "app"}), ActionWrite, "app", true},
		{"writer user cannot touch another db", ident(transport.ScopedGrant{Role: "writer", DB: "app"}), ActionWrite, "logs", false},
		{"db-manager cannot create users", ident(transport.ScopedGrant{Role: "db-manager", DB: "*"}), ActionUserAdmin, "", false},
		{"user-manager cannot add nodes", ident(transport.ScopedGrant{Role: "user-manager", DB: "*"}), ActionClusterAdmin, "", false},
		{"cluster-manager cannot read without read", ident(transport.ScopedGrant{Role: "cluster-manager", DB: "*"}), ActionRead, "app", false},
		{"root can do anything", ident(transport.ScopedGrant{Role: "root", DB: "*"}), ActionClusterAdmin, "", true},
		{"global role covers every db", ident(transport.ScopedGrant{Role: "writer", DB: "*"}), ActionWrite, "anything", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := identityAllows(tc.identity, tc.action, tc.resource); got != tc.allowed {
				t.Fatalf("identityAllows(%v, %s, %s) = %v, want %v",
					tc.identity.Grants, tc.action, tc.resource, got, tc.allowed)
			}
		})
	}
}

// ident builds an identity whose flat lists are derived from its scoped
// grants, the way AUTH builds one.
func ident(grants ...transport.ScopedGrant) transport.Identity {
	identity := transport.Identity{Grants: grants}
	seenRoles := map[string]struct{}{}
	seenDBs := map[string]struct{}{}
	for _, grant := range grants {
		if _, ok := seenRoles[grant.Role]; !ok {
			seenRoles[grant.Role] = struct{}{}
			identity.Roles = append(identity.Roles, grant.Role)
		}
		if _, ok := seenDBs[grant.DB]; !ok {
			seenDBs[grant.DB] = struct{}{}
			identity.Databases = append(identity.Databases, grant.DB)
		}
	}
	return identity
}

// TestGrantPairsDoNotCrossDatabases is the regression test: a role held
// in one database must not satisfy another. It fails on 473c791, where the
// gate matched the flat lists independently.
func TestGrantPairsDoNotCrossDatabases(t *testing.T) {
	// alice holds write on app and read on logs.
	alice := ident(
		transport.ScopedGrant{Role: "writer", DB: "app"},
		transport.ScopedGrant{Role: "reader", DB: "logs"},
	)
	if !identityAllows(alice, ActionWrite, "app") {
		t.Fatal("write to app must be allowed")
	}
	if identityAllows(alice, ActionWrite, "logs") {
		t.Fatal("write to logs must be refused: writer@app does not scope to logs")
	}
	if !identityAllows(alice, ActionRead, "logs") {
		t.Fatal("read on logs must be allowed")
	}
	if identityAllows(alice, ActionRead, "other") {
		t.Fatal("no grant on other must be refused")
	}

	// db-manager in one database must not grant bucket admin in another.
	admin := ident(
		transport.ScopedGrant{Role: "db-manager", DB: "app"},
		transport.ScopedGrant{Role: "reader", DB: "logs"},
	)
	if identityAllows(admin, ActionDBAdmin, "logs") {
		t.Fatal("db-manager@app satisfied ActionDBAdmin on logs")
	}
	if !identityAllows(admin, ActionDBAdmin, "app") {
		t.Fatal("db-manager@app must satisfy ActionDBAdmin on app")
	}
}

// TestEmptyGrantsFailClosed is acceptance criterion 5: a session identity
// with no scoped grants refuses a read/write action rather than falling back
// to the flat lists.
func TestEmptyGrantsFailClosed(t *testing.T) {
	// Deliberately populated flat lists with no scoped grants: the old gate
	// would allow these, and that fallback is how that defect comes back.
	identity := transport.Identity{Roles: []string{"root"}, Databases: []string{"*"}}
	if identityAllows(identity, ActionRead, "app") {
		t.Fatal("flat role/database lists satisfied the gate with no scoped grants")
	}
	if identityAllows(identity, ActionWrite, "app") {
		t.Fatal("flat role/database lists satisfied a write with no scoped grants")
	}
}

// TestSessionGrantsRestorePairing checks the AUTH-time reconstruction from
// the record, including the narrowing a token refresh must preserve.
func TestSessionGrantsRestorePairing(t *testing.T) {
	record := &UserRecord{
		Username: "alice",
		Roles: []Grant{
			{Role: "writer", DB: "app"},
			{Role: "reader", DB: "logs"},
		},
	}

	grants := sessionGrants(record, []string{"writer", "reader"}, []string{"app", "logs"})
	want := map[transport.ScopedGrant]bool{
		{Role: "writer", DB: "app"}:  true,
		{Role: "reader", DB: "logs"}: true,
	}
	if len(grants) != len(want) {
		t.Fatalf("grants = %v, want the two record pairs", grants)
	}
	for _, grant := range grants {
		if !want[grant] {
			t.Fatalf("unexpected grant %v", grant)
		}
	}

	// A refresh that presented only app must not carry the logs read grant.
	narrowed := sessionGrants(record, []string{"writer", "read"}, []string{"app"})
	for _, grant := range narrowed {
		if grant.DB == "logs" {
			t.Fatalf("a narrowed token regained a logs grant: %v", narrowed)
		}
	}

	// A global record grant is bounded by what the token presented.
	globalRecord := &UserRecord{Roles: []Grant{{Role: "reader", DB: "*"}}}
	bounded := sessionGrants(globalRecord, []string{"reader"}, []string{"app"})
	if len(bounded) != 1 || bounded[0] != (transport.ScopedGrant{Role: "reader", DB: "app"}) {
		t.Fatalf("global grant bounded by presented = %v, want reader@app", bounded)
	}
}

// TestGateDoesNotRunOnWALReplay is the test that catches the gate in the
// wrong function. s.dispatch is the replay path; a mutating frame must apply
// there even when the context carries an unprivileged session, because a
// gate in dispatch would refuse replay after a restart and the node would
// never come back.
func TestGateDoesNotRunOnWALReplay(t *testing.T) {
	s := userTestServer(t)
	frame := transport.NewFrame(transport.OpCreateDB, mustMarshalBSON(t, &CreateDatabaseRequest{Name: "replayed"}))

	// The unprivileged session is what makes the test discriminating: if the
	// gate were in dispatch, this frame would be refused.
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "reader", Roles: []string{"reader"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	ctx := transport.WithSession(context.Background(), sess)

	resp, err := s.dispatch(ctx, frame)
	if err != nil {
		t.Fatalf("dispatch on the replay path returned an error: %v", err)
	}
	if resp.Opcode != transport.OpCreateDB {
		t.Fatalf("replayed create opcode = %s, want CREATE_DB (the gate must not run in dispatch)", resp.Opcode)
	}
}

// TestDriverAndMemberSessionsBothRefuseRawCreateDB pins both refusals this
// test asserts: a driver session without db-manager has a raw CREATE_DB
// refused, and so does a cluster member that sends the same client opcode raw.
// The internode exemption is drawn by session and opcode, not by whether the
// write was forwarded: a member sends no client opcode at all, and a forwarded
// frame travels inside OpReplPush. That forwarded writes still work
// is a separate path, proved end to end by
// TestForwardedClientFramesStillWorkAfterTheInternodeFix.
func TestDriverAndMemberSessionsBothRefuseRawCreateDB(t *testing.T) {
	s := userTestServer(t)
	handler := NewHandler(s)

	// Driver session without db-manager: refused.
	driver := &transport.Session{}
	driver.SetIdentity(transport.Identity{Username: "reader", Roles: []string{"reader"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	driverCtx := transport.WithSession(context.Background(), driver)
	driverResp, err := handler(driverCtx, transport.NewFrame(transport.OpCreateDB, mustMarshalBSON(t, &CreateDatabaseRequest{Name: "driver"})))
	if err != nil {
		t.Fatalf("driver handler: %v", err)
	}
	if driverResp.Opcode != transport.OpError {
		t.Fatalf("driver create opcode = %s, want ERROR (authorization refused)", driverResp.Opcode)
	}

	// Peer session: a member sends no raw client opcode (a forwarded frame
	// travels inside OpReplPush), so the same create is refused.
	peer := &transport.Session{Peer: true}
	peer.SetIdentity(transport.Identity{Username: NodeUser})
	peerCtx := transport.WithSession(context.Background(), peer)
	peerResp, err := handler(peerCtx, transport.NewFrame(transport.OpCreateDB, mustMarshalBSON(t, &CreateDatabaseRequest{Name: "forwarded"})))
	if err != nil {
		t.Fatalf("peer handler: %v", err)
	}
	if peerResp.Opcode != transport.OpError {
		t.Fatalf("peer create opcode = %s, want ERROR (a member does not send raw client opcodes)", peerResp.Opcode)
	}
}

// TestDenialAuditBatchesRepeats asserts one full line per interval plus a
// suppressed count on the next, so a retry loop cannot flood the log.
func TestDenialAuditBatchesRepeats(t *testing.T) {
	s := userTestServer(t)
	path := t.TempDir() + "/audit.jsonl"
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	s.audit = al

	for i := 0; i < 5; i++ {
		s.recordDenial("alice", ActionWrite, "app", transport.OpPut, []transport.ScopedGrant{{Role: "reader", DB: "app"}})
	}
	// Force the interval open and record once more; the suppressed count
	// should now be reported.
	s.denials.mu.Lock()
	key := denialKey{User: "alice", Action: ActionWrite, Resource: "app"}
	state := s.denials.entries[key]
	state.LastFull = time.Now().Add(-2 * authzDenialInterval)
	s.denials.entries[key] = state
	s.denials.mu.Unlock()
	s.recordDenial("alice", ActionWrite, "app", transport.OpPut, []transport.ScopedGrant{{Role: "reader", DB: "app"}})

	al.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	denials := 0
	sawSuppressed := false
	for _, line := range lines {
		if strings.Contains(line, `"event":"authz.denied"`) {
			denials++
		}
		if strings.Contains(line, "4 identical denials suppressed") {
			sawSuppressed = true
		}
	}
	if denials != 2 {
		t.Fatalf("audit had %d authz.denied lines, want 2 (one per interval)", denials)
	}
	if !sawSuppressed {
		t.Fatal("the second full line does not report the suppressed repeats")
	}
}

// TestDenialMessageNamesTheFix asserts the form a 3am reader needs: the user,
// the missing action, the resource, and the command that fixes it.
func TestDenialMessageNamesTheFix(t *testing.T) {
	s := userTestServer(t)
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "alice", Roles: []string{"writer"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "writer", DB: "app"}}})
	ctx := transport.WithSession(context.Background(), sess)
	frame := transport.NewFrame(transport.OpDeleteBucket, mustMarshalBSON(t, &DeleteBucketRequest{Database: "app", Name: "sessions"}))

	err := s.authorize(ctx, frame)
	if err == nil {
		t.Fatal("writer user was allowed to drop a bucket")
	}
	for _, want := range []string{"alice", "db-manager", "app", "grant role db-manager@app to alice"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
}

// TestAuthorizationRefusalAnswersPermissionDenied checks that a request
// the user is not granted is PERMISSION_DENIED on the wire, not INTERNAL, with
// the message that names the grant unchanged. The same holds for the internode
// listener's refusal of a session that is not a cluster member.
func TestAuthorizationRefusalAnswersPermissionDenied(t *testing.T) {
	s := userTestServer(t)
	handler := NewHandler(s)

	reader := &transport.Session{}
	reader.SetIdentity(transport.Identity{Username: "reader", Roles: []string{"reader"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	nonMember := &transport.Session{Peer: true}
	nonMember.SetIdentity(transport.Identity{Username: "someone"})

	cases := []struct {
		name    string
		session *transport.Session
		frame   *transport.Frame
		message string
	}{
		{"a read outside the grant", reader, transport.NewFrame(transport.OpBucketStats, mustMarshalBSON(t, &BucketStatsRequest{Database: "other", Bucket: "b"})), "grant role reader@other to reader"},
		{"an internode session that is not a member", nonMember, transport.NewFrame(transport.OpBucketStats, mustMarshalBSON(t, &BucketStatsRequest{Database: "app", Bucket: "b"})), "internode listener serves cluster members only"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			answer, err := handler(transport.WithSession(context.Background(), tc.session), tc.frame)
			if err != nil {
				t.Fatalf("handler returned a Go error: %v", err)
			}
			if answer.Opcode != transport.OpError {
				t.Fatalf("answer opcode = %s, want ERROR", answer.Opcode)
			}
			if got, want := decodeErrorCode(t, answer.Payload), int(transport.ErrPermissionDenied); got != want {
				t.Errorf("error code = %d, want %d (PERMISSION_DENIED)", got, want)
			}
			if message := decodeErrorMessage(t, answer.Payload); !strings.Contains(message, tc.message) {
				t.Errorf("message %q lost %q", message, tc.message)
			}
		})
	}
}
