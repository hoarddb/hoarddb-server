package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAlterPasswordBumpsEpoch is the password-change half of step 7: the
// online change goes through the same derivation and clamp as creation, and
// the existing epoch bump is what refuses an outstanding token at refresh
// (spec 6.1, 8.8).
func TestAlterPasswordBumpsEpoch(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	token, _ := mintToken(t, am, "alice")

	newCredential, err := NewCredential("a-new-longer-password", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	if _, err := am.AlterUser(AlterSpec{Username: "alice", Grants: &[]Grant{{Role: "root", DB: "*"}}, Credential: &newCredential, Version: am.Version()}); err != nil {
		t.Fatalf("AlterUser with credentials: %v", err)
	}

	if ok, _ := am.VerifyUser("alice", "correct-horse-battery"); ok {
		t.Fatal("the old password still authenticates after a password change")
	}
	if ok, err := am.VerifyUser("alice", "a-new-longer-password"); err != nil || !ok {
		t.Fatalf("the new password does not authenticate: %v, %v", ok, err)
	}

	claims, err := am.ValidateToken(token)
	if err != nil {
		t.Fatalf("the outstanding token signature should still parse: %v", err)
	}
	if err := am.VerifyClaims(claims); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("VerifyClaims after a password change = %v, want ErrTokenInvalid (epoch bumped)", err)
	}
}

// TestAlterPasswordRejectsOutOfClamp is the reuse check: the online password
// change runs the create path's clamp, not a second one.
func TestAlterPasswordRejectsOutOfClamp(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	bad := CredentialMaterial{
		Algo:   "argon2id",
		Salt:   testSalt("salt"),
		Hash:   "aGFzaA==",
		Params: Argon2Params{Memory: 8, Iterations: 1, Parallelism: 1, SaltLen: 16, HashLen: 32},
	}
	if _, err := am.AlterUser(AlterSpec{Username: "alice", Grants: &[]Grant{{Role: "root", DB: "*"}}, Credential: &bad, Version: am.Version()}); err == nil {
		t.Fatal("an out-of-clamp credential was accepted by the online password change")
	}
}

// TestDisableClosesSessionsAndRoleChangeDoesNot is the §8.8 call-site rule:
// disable closes the user's sessions, a role change does not.
func TestDisableClosesSessionsAndRoleChangeDoesNot(t *testing.T) {
	s := userTestServer(t)
	if err := s.auth.CreateUser("alice", CredentialMaterial{
		Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("alice"), Params: defaultArgon2Params(),
	}, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	var closedUser string
	closeCalls := 0
	s.SetUserSessionCloser(func(username string) int {
		closedUser = username
		closeCalls++
		return 2
	})

	// A role change keeps next-AUTH semantics: no close.
	roleReq, _ := bson.Marshal(&UserAlterRequest{
		RequestID: "roles", Username: "alice", Roles: &[]UserRole{{Role: "writer", DB: "app"}}, Version: s.auth.Version(),
	})
	if _, err := handleUserAlter(context.Background(), transport.NewFrame(transport.OpUserAlter, roleReq), s); err != nil {
		t.Fatalf("role alter: %v", err)
	}
	if closeCalls != 0 {
		t.Fatalf("a role change closed %d session(s); it must not", closeCalls)
	}

	disabled := false
	disableReq, _ := bson.Marshal(&UserAlterRequest{
		RequestID: "disable", Username: "alice", Roles: &[]UserRole{{Role: "writer", DB: "app"}}, Enabled: &disabled, Version: s.auth.Version(),
	})
	if _, err := handleUserAlter(context.Background(), transport.NewFrame(transport.OpUserAlter, disableReq), s); err != nil {
		t.Fatalf("disable alter: %v", err)
	}
	if closeCalls != 1 || closedUser != "alice" {
		t.Fatalf("disable closed %d session(s) for %q, want 1 for alice", closeCalls, closedUser)
	}
}

// TestDropClosesSessions is the other §8.8 call site.
func TestDropClosesSessions(t *testing.T) {
	s := userTestServer(t)
	if err := s.auth.CreateUser("alice", CredentialMaterial{
		Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("alice"), Params: defaultArgon2Params(),
	}, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	var closedUser string
	s.SetUserSessionCloser(func(username string) int {
		closedUser = username
		return 1
	})

	dropReq, _ := bson.Marshal(&UserDropRequest{RequestID: "drop", Username: "alice"})
	if _, err := handleUserDrop(context.Background(), transport.NewFrame(transport.OpUserDrop, dropReq), s); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if closedUser != "alice" {
		t.Fatalf("drop closed sessions for %q, want alice", closedUser)
	}
}

// TestRefusalNamesScopedRoles checks the refusal line spec 4 rule 5 promises.
func TestRefusalNamesScopedRoles(t *testing.T) {
	s := userTestServer(t)
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{
		Username:  "alice",
		Roles:     []string{"writer", "read"},
		Databases: []string{"app", "logs"},
		Grants: []transport.ScopedGrant{
			{Role: "writer", DB: "app"},
			{Role: "reader", DB: "logs"},
		},
	})
	ctx := transport.WithSession(context.Background(), sess)
	frame := transport.NewFrame(transport.OpDeleteBucket, mustMarshalBSON(t, &DeleteBucketRequest{Database: "app", Name: "sessions"}))

	err := s.authorize(ctx, frame)
	if err == nil {
		t.Fatal("writer@app was allowed to drop a bucket")
	}
	if !strings.Contains(err.Error(), "alice's roles: reader@logs, writer@app") {
		t.Fatalf("refusal %q does not print the scoped roles", err)
	}
}

// TestDropClosesLiveSessionOnTCP is the §8.8 end-to-end: a second connection
// authenticated as the user is closed by a drop, and its next frame fails
// rather than being served from a stale identity.
func TestDropClosesLiveSessionOnTCP(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	qs, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := qs.SeedRootUser("admin", "test123"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = qs.Stop() })
	addr := qs.tcpListener.Addr().String()

	root := dialTCPServer(t, addr)
	root.login()

	credential, err := NewCredential("alice-password-123", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	var created UserMutationResponse
	root.call(transport.OpUserCreate, &UserCreateRequest{
		RequestID:  fmt.Sprintf("create-%d", time.Now().UnixNano()),
		Username:   "alice",
		Credential: UserCredential{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params},
		Roles:      []UserRole{{Role: "reader", DB: "app"}},
	}, &created)

	alice := dialTCPServer(t, addr)
	var aliceAuth AuthResponse
	alice.call(transport.OpAuth, &LoginRequest{Username: "alice", Password: "alice-password-123"}, &aliceAuth)

	// The session works before the drop (list databases needs no privilege).
	var dbs ListDatabasesResponse
	alice.call(transport.OpListDBs, &ListDatabasesRequest{}, &dbs)

	var dropped UserMutationResponse
	root.call(transport.OpUserDrop, &UserDropRequest{RequestID: fmt.Sprintf("drop-%d", time.Now().UnixNano()), Username: "alice"}, &dropped)

	// Her next frame must fail: the listener closed her connection.
	payload, _ := bson.Marshal(&ListDatabasesRequest{})
	frame := transport.NewFrame(transport.OpListDBs, payload)
	_ = alice.conn.SetDeadline(time.Now().Add(5 * time.Second))
	writeErr := error(nil)
	if _, err := alice.conn.Write(frame.Encode()); err == nil {
		if _, err := transport.DecodeFrame(alice.br); err == nil {
			t.Fatal("the dropped user's next frame was served; the session was not closed")
		} else {
			writeErr = err
		}
	} else {
		writeErr = err
	}
	if writeErr == nil {
		t.Fatal("expected the closed connection to fail the next frame")
	}
}

// TestAlterCredentialsOnlyKeepsRoles is the §12.4 defect in one test: an
// absent roles field means unchanged, so a password change must not strip the
// user's grants.
func TestAlterCredentialsOnlyKeepsRoles(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	before, _ := am.UserInfo("alice")

	newCredential, err := NewCredential("a-new-longer-password", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	if _, err := am.AlterUser(AlterSpec{Username: "alice", Credential: &newCredential, Version: am.Version()}); err != nil {
		t.Fatalf("AlterUser: %v", err)
	}
	after, _ := am.UserInfo("alice")
	if len(after.Roles) != len(before.Roles) {
		t.Fatalf("roles changed from %v to %v; a credentials-only alter must leave them alone", before.Roles, after.Roles)
	}
	for i := range before.Roles {
		if before.Roles[i] != after.Roles[i] {
			t.Fatalf("roles changed from %v to %v", before.Roles, after.Roles)
		}
	}
	if after.Enabled != before.Enabled {
		t.Fatal("a credentials-only alter changed the enabled flag")
	}
}

// TestAlterEmptyRolesClears is the other half of presence: an explicitly empty
// roles list replaces the set with nothing.
func TestAlterEmptyRolesClears(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "admin")
	credential, err := NewCredential("bob-password-123", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	if err := am.CreateUser("bob", credential, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	empty := []Grant{}
	if _, err := am.AlterUser(AlterSpec{Username: "bob", Grants: &empty, Version: am.Version()}); err != nil {
		t.Fatalf("AlterUser: %v", err)
	}
	after, _ := am.UserInfo("bob")
	if len(after.Roles) != 0 {
		t.Fatalf("roles = %v, want cleared", after.Roles)
	}
}

// TestAlterRoleChangeDoesNotBumpEpoch is §12.4 item 4: a routine role change
// reaches a live session at its next refresh; it must not revoke the token and
// force a password re-entry.
func TestAlterRoleChangeDoesNotBumpEpoch(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "admin")
	credential, err := NewCredential("bob-password-123", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	if err := am.CreateUser("bob", credential, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	bobToken, _ := mintToken(t, am, "bob")

	grants := []Grant{{Role: "writer", DB: "app"}}
	if _, err := am.AlterUser(AlterSpec{Username: "bob", Grants: &grants, Version: am.Version()}); err != nil {
		t.Fatalf("AlterUser: %v", err)
	}
	claims, err := am.ValidateToken(bobToken)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if err := am.VerifyClaims(claims); err != nil {
		t.Fatalf("a role change revoked the token: %v; a role change must not bump the epoch", err)
	}
}

// TestAlterDisableBumpsEpoch is the other side of item 4: disable is an
// immediacy operation and does bump.
func TestAlterDisableBumpsEpoch(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "admin")
	credential, err := NewCredential("bob-password-123", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	if err := am.CreateUser("bob", credential, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	bobToken, _ := mintToken(t, am, "bob")

	disabled := false
	if _, err := am.AlterUser(AlterSpec{Username: "bob", Enabled: &disabled, Version: am.Version()}); err != nil {
		t.Fatalf("AlterUser: %v", err)
	}
	claims, err := am.ValidateToken(bobToken)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if err := am.VerifyClaims(claims); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("VerifyClaims after a disable = %v, want ErrTokenInvalid (epoch bumped)", err)
	}
}

// TestAlterEmptyRolesRefusesLastAdmin: an explicitly empty roles array is a
// replacement, and the last-admin guard must refuse it when it would leave no
// enabled administrator. It must not be mistaken for "unchanged".
func TestAlterEmptyRolesRefusesLastAdmin(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "admin") // the only enabled administrator
	empty := []Grant{}
	if _, err := am.AlterUser(AlterSpec{Username: "admin", Grants: &empty, Version: am.Version()}); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("AlterUser with an explicit empty roles list = %v, want ErrLastAdmin", err)
	}
	// And the record is untouched.
	after, _ := am.UserInfo("admin")
	if len(after.Roles) == 0 {
		t.Fatal("the refused alter changed the record")
	}
}

// TestAlterAuditAndEchoNameChangedFields covers spec 12.4 item 2 and 6.1 item
// 3: the audit line names the changed fields, the response echoes them, and a
// password-only alter names credentials and not roles.
func TestAlterAuditAndEchoNameChangedFields(t *testing.T) {
	s := userTestServer(t)
	if err := s.auth.CreateUser("alice", CredentialMaterial{
		Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("alice"), Params: defaultArgon2Params(),
	}, []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	path := t.TempDir() + "/audit.jsonl"
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	s.audit = al

	credential, err := NewCredential("a-new-longer-password", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	req := &UserAlterRequest{
		RequestID: "pw-only", Username: "alice", Version: s.auth.Version(),
		Credentials: &UserCredential{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params},
	}
	payload, _ := bson.Marshal(req)
	resp, err := handleUserAlter(context.Background(), transport.NewFrame(transport.OpUserAlter, payload), s)
	if err != nil {
		t.Fatalf("handleUserAlter: %v", err)
	}
	var mutation UserMutationResponse
	if err := bson.Unmarshal(resp.Payload, &mutation); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(mutation.Changed) != 1 || mutation.Changed[0] != "credentials" {
		t.Fatalf("response Changed = %v, want [credentials]", mutation.Changed)
	}
	al.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if !strings.Contains(string(data), `"changed: credentials"`) {
		t.Fatalf("audit does not name the changed field: %s", data)
	}
	if strings.Contains(string(data), "changed: credentials,roles") || strings.Contains(string(data), "changed: roles") {
		t.Fatalf("a password-only alter named roles: %s", data)
	}
}

// TestAlterEchoNamesEveryRequestedField checks roles+enabled are both echoed.
func TestAlterEchoNamesEveryRequestedField(t *testing.T) {
	s := userTestServer(t)
	if err := s.auth.CreateUser("admin", CredentialMaterial{
		Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("admin"), Params: defaultArgon2Params(),
	}, []Grant{{Role: "root", DB: "*"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	roles := []UserRole{{Role: "root", DB: "*"}}
	enabled := true
	req := &UserAlterRequest{RequestID: "roles-enabled", Username: "admin", Version: s.auth.Version(), Roles: &roles, Enabled: &enabled}
	payload, _ := bson.Marshal(req)
	resp, err := handleUserAlter(context.Background(), transport.NewFrame(transport.OpUserAlter, payload), s)
	if err != nil {
		t.Fatalf("handleUserAlter: %v", err)
	}
	var mutation UserMutationResponse
	_ = bson.Unmarshal(resp.Payload, &mutation)
	if strings.Join(mutation.Changed, ",") != "roles,enabled" {
		t.Fatalf("response Changed = %v, want [roles enabled]", mutation.Changed)
	}
}

// TestUserAlterRequestPresence is the wire half of item 1: absent roles and
// enabled must decode as nil (unchanged) while an explicitly empty roles array
// must decode as present-and-empty (replace with nothing). If these two become
// indistinguishable on the wire, the server cannot honour the distinction.
func TestUserAlterRequestPresence(t *testing.T) {
	absentPayload, err := bson.Marshal(&UserAlterRequest{RequestID: "absent", Username: "alice", Version: 1})
	if err != nil {
		t.Fatalf("marshal absent: %v", err)
	}
	var absent UserAlterRequest
	if err := bson.Unmarshal(absentPayload, &absent); err != nil {
		t.Fatalf("unmarshal absent: %v", err)
	}
	if absent.Roles != nil {
		t.Fatal("absent roles decoded non-nil; it must mean unchanged")
	}
	if absent.Enabled != nil {
		t.Fatal("absent enabled decoded non-nil; it must mean unchanged")
	}

	empty := []UserRole{}
	emptyPayload, err := bson.Marshal(&UserAlterRequest{RequestID: "empty", Username: "alice", Version: 1, Roles: &empty})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	var present UserAlterRequest
	if err := bson.Unmarshal(emptyPayload, &present); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if present.Roles == nil {
		t.Fatal("an explicitly empty roles array decoded nil; absent and empty must differ")
	}
	if len(*present.Roles) != 0 {
		t.Fatalf("explicitly empty roles decoded %v, want empty", *present.Roles)
	}
}
