package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"

	"golang.org/x/crypto/argon2"
)

func userTestServer(t *testing.T) *Server {
	t.Helper()
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: 12 * time.Hour, Skew: time.Minute, Keyring: keyring,
	}, discardLogger())
	return &Server{
		config:      &config.Config{Server: config.ServerConfig{NodeID: "node-a", Listen: "127.0.0.1:1"}, Storage: config.StorageConfig{DataDir: t.TempDir()}},
		logger:      discardLogger(),
		auth:        am,
		authReqSeen: make(map[string]time.Time),
		dataStore:   &serverData{dbs: make(map[string]*inMemoryDB)},
		kv:          kvfault.New(),
	}
}

func testCredential(t *testing.T, password string) UserCredential {
	t.Helper()
	params := defaultArgon2Params()
	salt := make([]byte, params.SaltLen)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("salt: %v", err)
	}
	hash := argon2.IDKey([]byte(password), salt, params.Iterations, params.Memory, params.Parallelism, uint32(params.HashLen))
	return UserCredential{
		Algo:   "argon2id",
		Salt:   base64.StdEncoding.EncodeToString(salt),
		Hash:   base64.StdEncoding.EncodeToString(hash),
		Params: params,
	}
}

// testHash returns a valid base64 32-byte value for tests that only exercise
// the store logic, not a real Argon2 derivation.
func testHash(seed string) string {
	b := make([]byte, 32)
	copy(b, seed)
	return base64.StdEncoding.EncodeToString(b)
}

func createFrame(t *testing.T, requestID, username, password string, roles []UserRole) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(&UserCreateRequest{
		RequestID: requestID, Username: username, Credential: testCredential(t, password), Roles: roles,
	})
	if err != nil {
		t.Fatalf("marshal create: %v", err)
	}
	return transport.NewFrame(transport.OpUserCreate, payload)
}

func TestUserCreateAndListWire(t *testing.T) {
	s := userTestServer(t)
	frame := createFrame(t, "req-1", "alice", "correct-horse-battery", []UserRole{{Role: "writer", DB: "app"}})

	resp, err := handleUserCreate(context.Background(), frame, s)
	if err != nil {
		t.Fatalf("handleUserCreate: %v", err)
	}
	if resp.Opcode != transport.OpUserCreate {
		t.Fatalf("create opcode = %s, want USER_CREATE", resp.Opcode)
	}

	listResp, err := handleUserList(context.Background(), transport.NewFrame(transport.OpUserList, nil), s)
	if err != nil {
		t.Fatalf("handleUserList: %v", err)
	}
	var list UserListResponse
	if err := bson.Unmarshal(listResp.Payload, &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Users) != 1 || list.Users[0].Username != "alice" {
		t.Fatalf("list = %+v, want alice", list.Users)
	}
	// The wire shape has no hash or salt fields at all; assert on the bytes.
	if strings.Contains(string(listResp.Payload), "hash") || strings.Contains(string(listResp.Payload), "salt") {
		t.Fatalf("user list payload contains credential material: %s", listResp.Payload)
	}
}

// TestCreateRequestHasNoPlaintextField documents the creation contract: the
// wire type has no password field, so the plaintext cannot be sent (spec 8.7).
func TestCreateRequestHasNoPlaintextField(t *testing.T) {
	typ := reflect.TypeOf(UserCreateRequest{})
	for i := 0; i < typ.NumField(); i++ {
		name := strings.ToLower(typ.Field(i).Name)
		if name == "password" || name == "plaintext" {
			t.Fatalf("UserCreateRequest exposes a plaintext field %q", typ.Field(i).Name)
		}
	}
	frame := createFrame(t, "req-1", "alice", "correct-horse-battery", nil)
	if strings.Contains(string(frame.Payload), "correct-horse-battery") {
		t.Fatal("the marshalled creation frame contains the plaintext password")
	}
}

func TestUserCreateReplayIsNoOp(t *testing.T) {
	s := userTestServer(t)
	frame := createFrame(t, "same-id", "alice", "correct-horse-battery", nil)
	if _, err := handleUserCreate(context.Background(), frame, s); err != nil {
		t.Fatalf("first create: %v", err)
	}
	second, err := handleUserCreate(context.Background(), frame, s)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if second.Opcode != transport.OpUserCreate {
		t.Fatalf("replayed create opcode = %s, want USER_CREATE (deduped, not an error)", second.Opcode)
	}
	if got := len(s.auth.UserInfos()); got != 1 {
		t.Fatalf("users = %d after a replay, want 1", got)
	}
}

func TestLastAdminGuardUnderConcurrentDrops(t *testing.T) {
	s := userTestServer(t)
	for _, name := range []string{"alice", "bob"} {
		if err := s.auth.CreateUser(name, CredentialMaterial{
			Algo: "argon2id", Salt: testSalt(name), Hash: testHash(name), Params: defaultArgon2Params(),
		}, []Grant{{Role: "user-manager", DB: "*"}}, true); err != nil {
			t.Fatalf("CreateUser(%s): %v", name, err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	names := []string{"alice", "bob"}
	for i, name := range names {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			errs[i] = s.auth.DropUser(name)
		}(i, name)
	}
	wg.Wait()

	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrLastAdmin) {
			t.Fatalf("concurrent drop error = %v, want ErrLastAdmin", err)
		}
	}
	if successes != 1 {
		t.Fatalf("%d concurrent drops succeeded, want exactly 1", successes)
	}
	if got := len(s.auth.UserInfos()); got != 1 {
		t.Fatalf("users = %d after the drops, want 1 administrator left", got)
	}
}

func TestDropLastAdminIsRefusedSequentially(t *testing.T) {
	s := userTestServer(t)
	if err := s.auth.CreateUser("solo", CredentialMaterial{
		Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("hash"), Params: defaultArgon2Params(),
	}, []Grant{{Role: "user-manager", DB: "*"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := s.auth.DropUser("solo"); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("DropUser(last admin) = %v, want ErrLastAdmin", err)
	}
	if _, err := s.auth.AlterUser(AlterSpec{Username: "solo", Grants: &[]Grant{{Role: "reader", DB: "app"}}, Version: s.auth.Version()}); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("AlterUser stripping last admin = %v, want ErrLastAdmin", err)
	}
}

func TestAlterVersionMismatchIsRefused(t *testing.T) {
	s := userTestServer(t)
	if err := s.auth.CreateUser("alice", CredentialMaterial{
		Algo: "argon2id", Salt: testSalt("salt"), Hash: testHash("hash"), Params: defaultArgon2Params(),
	}, []Grant{{Role: "user-manager", DB: "*"}}, true); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	stale := s.auth.Version() - 1
	if _, err := s.auth.AlterUser(AlterSpec{Username: "alice", Grants: &[]Grant{{Role: "reader", DB: "app"}}, Version: stale}); !errors.Is(err, ErrUserVersion) {
		t.Fatalf("AlterUser with a stale version = %v, want ErrUserVersion", err)
	}
}

func TestCreateUserRefusesReusedSalt(t *testing.T) {
	s := userTestServer(t)
	material := CredentialMaterial{Algo: "argon2id", Salt: testSalt("same"), Hash: testHash("alice"), Params: defaultArgon2Params()}
	if err := s.auth.CreateUser("alice", material, nil, true); err != nil {
		t.Fatalf("first CreateUser: %v", err)
	}
	second := material
	second.Hash = testHash("bob")
	if err := s.auth.CreateUser("bob", second, nil, true); !errors.Is(err, ErrSaltReused) {
		t.Fatalf("CreateUser with a reused salt = %v, want ErrSaltReused", err)
	}
}

func TestCreateDatabaseReservesSystem(t *testing.T) {
	s := userTestServer(t)
	s.config.Storage.DataDir = t.TempDir()
	resp, err := s.CreateDatabase("_system")
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp.Success {
		t.Fatal("create database _system succeeded, want refusal")
	}
	if !strings.Contains(resp.Message, "reserved") {
		t.Fatalf("refusal %q does not say the name is reserved", resp.Message)
	}
}

// TestCreatedUserAuthenticates is the check that a client-derived credential
// and the server's peppered verification agree. A mismatch here breaks every
// created user and nothing else notices (user-store-build.md §8).
func TestCreatedUserAuthenticates(t *testing.T) {
	s := userTestServer(t)
	frame := createFrame(t, "req-auth", "alice", "correct-horse-battery", []UserRole{{Role: "reader", DB: "app"}})
	if _, err := handleUserCreate(context.Background(), frame, s); err != nil {
		t.Fatalf("handleUserCreate: %v", err)
	}
	if ok, err := s.auth.VerifyUser("alice", "correct-horse-battery"); err != nil || !ok {
		t.Fatalf("created user does not authenticate: %v, %v", ok, err)
	}
	if ok, _ := s.auth.VerifyUser("alice", "the-wrong-password"); ok {
		t.Fatal("a wrong password authenticated for a created user")
	}
}

// userAdminRefusal sends frame to its handler and returns the code and message
// of the ERROR frame it answers with. A refusal that came back as a success, or
// as a Go error (which resets the stream), fails the test.
func userAdminRefusal(t *testing.T, s *Server, handler func(context.Context, *transport.Frame, *Server) (*transport.Frame, error), frame *transport.Frame) (transport.ErrorCode, string) {
	t.Helper()
	answer, err := handler(context.Background(), frame, s)
	if err != nil {
		t.Fatalf("handler returned a Go error, which resets the stream: %v", err)
	}
	if answer.Opcode != transport.OpError {
		t.Fatalf("answer opcode = %s, want ERROR", answer.Opcode)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
		t.Fatalf("decoding error response: %v", err)
	}
	return transport.ErrorCode(refusal.Code), refusal.Message
}

func marshalUserRequest(t *testing.T, opcode transport.Opcode, request any) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("marshal %s: %v", opcode, err)
	}
	return transport.NewFrame(opcode, payload)
}

// TestUserAdminRefusalCodes is BUG-0622: every refusal of a user-administration
// command, driven through its handler, carries the code the owner ruled
// (2026-10-19) and a message that names the user or role. Before the fix every
// row except the mapped UNAVAILABLE ones answered INTERNAL.
func TestUserAdminRefusalCodes(t *testing.T) {
	goodMaterial := func(seed string) CredentialMaterial {
		return CredentialMaterial{Algo: "argon2id", Salt: testSalt(seed), Hash: testHash(seed), Params: defaultArgon2Params()}
	}
	// seeded has "root-admin" (the only administrator) and "ada" (a plain
	// reader), then runs one request.
	seeded := func(t *testing.T) *Server {
		s := userTestServer(t)
		admin := []Grant{{Role: "user-manager", DB: "*"}}
		if err := s.auth.CreateUser("root-admin", goodMaterial("root-admin"), admin, true); err != nil {
			t.Fatalf("seed root-admin: %v", err)
		}
		if err := s.auth.CreateUser("ada", goodMaterial("ada"), []Grant{{Role: "reader", DB: "app"}}, true); err != nil {
			t.Fatalf("seed ada: %v", err)
		}
		return s
	}
	credential := func(seed string) UserCredential {
		m := goodMaterial(seed)
		return UserCredential{Algo: m.Algo, Salt: m.Salt, Hash: m.Hash, Params: m.Params}
	}
	badAlgo := credential("fresh")
	badAlgo.Algo = "md5"
	notBase64 := credential("fresh")
	notBase64.Hash = "***not base64***"

	cases := []struct {
		name        string
		handler     func(context.Context, *transport.Frame, *Server) (*transport.Frame, error)
		prepare     func(*testing.T, *Server)
		frame       func(*testing.T, *Server) *transport.Frame
		want        transport.ErrorCode
		wantMessage []string
	}{
		{"duplicate user", handleUserCreate, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Username: "ada", Credential: credential("dup")})
		}, transport.ErrAlreadyExists, []string{`"ada"`, "already exists", "alter user"}},
		{"unknown role on create", handleUserCreate, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Username: "bob", Credential: credential("bob"), Roles: []UserRole{{Role: "nope", DB: "app"}}})
		}, transport.ErrInvalidArgument, []string{`"nope"`, "valid roles"}},
		{"retired role on grant", handleUserGrant, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserGrant, &UserGrantRequest{Username: "ada", Role: UserRole{Role: "readWrite", DB: "app"}})
		}, transport.ErrInvalidArgument, []string{`"readWrite"`, `"writer"`}},
		{"retired role on revoke", handleUserRevoke, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserRevoke, &UserRevokeRequest{Username: "ada", Role: UserRole{Role: "read", DB: "app"}})
		}, transport.ErrInvalidArgument, []string{`"read"`, `"reader"`}},
		{"credential algo", handleUserCreate, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Username: "bob", Credential: badAlgo})
		}, transport.ErrInvalidArgument, []string{`"bob"`, `"md5"`, "argon2id"}},
		{"credential not base64", handleUserCreate, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Username: "bob", Credential: notBase64})
		}, transport.ErrInvalidArgument, []string{`"bob"`, "base64"}},
		{"credential on alter", handleUserAlter, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserAlter, &UserAlterRequest{Username: "ada", Version: s.auth.Version(), Credentials: &badAlgo})
		}, transport.ErrInvalidArgument, []string{`"ada"`, `"md5"`}},
		{"reserved username", handleUserCreate, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Username: NodeUser, Credential: credential("node")})
		}, transport.ErrInvalidArgument, []string{"reserved", "choose another"}},
		{"empty username", handleUserCreate, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Credential: credential("empty")})
		}, transport.ErrInvalidArgument, []string{"username is required", "create user"}},
		{"reused salt on create", handleUserCreate, nil, func(t *testing.T, s *Server) *transport.Frame {
			reused := credential("ada")
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Username: "bob", Credential: reused})
		}, transport.ErrInvalidArgument, []string{`"bob"`, "salt", "fresh"}},
		{"reused salt on alter", handleUserAlter, nil, func(t *testing.T, s *Server) *transport.Frame {
			reused := credential("root-admin")
			return marshalUserRequest(t, transport.OpUserAlter, &UserAlterRequest{Username: "ada", Version: s.auth.Version(), Credentials: &reused})
		}, transport.ErrInvalidArgument, []string{`"ada"`, "salt", "fresh"}},
		{"user limit", handleUserCreate, func(t *testing.T, s *Server) {
			s.auth.mu.Lock()
			defer s.auth.mu.Unlock()
			for len(s.auth.users) < maxUsers {
				name := fmt.Sprintf("filler-%d", len(s.auth.users))
				s.auth.users[name] = &UserRecord{Username: name}
			}
		}, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserCreate, &UserCreateRequest{Username: "bob", Credential: credential("bob")})
		}, transport.ErrInvalidArgument, []string{`"bob"`, "user limit", "drop user"}},
		{"stale version", handleUserAlter, nil, func(t *testing.T, s *Server) *transport.Frame {
			disabled := false
			return marshalUserRequest(t, transport.OpUserAlter, &UserAlterRequest{Username: "ada", Version: s.auth.Version() + 1, Enabled: &disabled})
		}, transport.ErrConflict, []string{`"ada"`, "version", "retry"}},
		{"alter unknown user", handleUserAlter, nil, func(t *testing.T, s *Server) *transport.Frame {
			disabled := false
			return marshalUserRequest(t, transport.OpUserAlter, &UserAlterRequest{Username: "ghost", Version: s.auth.Version(), Enabled: &disabled})
		}, transport.ErrNotFound, []string{`"ghost"`, "user list"}},
		{"drop unknown user", handleUserDrop, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserDrop, &UserDropRequest{Username: "ghost"})
		}, transport.ErrNotFound, []string{`"ghost"`, "user list"}},
		{"grant to unknown user", handleUserGrant, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserGrant, &UserGrantRequest{Username: "ghost", Role: UserRole{Role: "reader", DB: "app"}})
		}, transport.ErrNotFound, []string{`"ghost"`, "user list"}},
		{"revoke from unknown user", handleUserRevoke, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserRevoke, &UserRevokeRequest{Username: "ghost", Role: UserRole{Role: "reader", DB: "app"}})
		}, transport.ErrNotFound, []string{`"ghost"`, "user list"}},
		{"drop last administrator", handleUserDrop, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserDrop, &UserDropRequest{Username: "root-admin"})
		}, transport.ErrInvalidArgument, []string{`"root-admin"`, "the last enabled account that can manage users", "'grant role user-manager@* to "}},
		{"alter strips last administrator", handleUserAlter, nil, func(t *testing.T, s *Server) *transport.Frame {
			disabled := false
			return marshalUserRequest(t, transport.OpUserAlter, &UserAlterRequest{Username: "root-admin", Version: s.auth.Version(), Enabled: &disabled})
		}, transport.ErrInvalidArgument, []string{`"root-admin"`, "the last enabled account that can manage users"}},
		{"revoke last administrator role", handleUserRevoke, nil, func(t *testing.T, s *Server) *transport.Frame {
			return marshalUserRequest(t, transport.OpUserRevoke, &UserRevokeRequest{Username: "root-admin", Role: UserRole{Role: "user-manager", DB: "*"}})
		}, transport.ErrInvalidArgument, []string{`"root-admin"`, "the last enabled account that can manage users"}},
		{"undecodable create", handleUserCreate, nil, func(*testing.T, *Server) *transport.Frame {
			return transport.NewFrame(transport.OpUserCreate, []byte{0xff, 0x01, 0x02})
		}, transport.ErrInvalidArgument, []string{"user create", "BSON"}},
		{"undecodable alter", handleUserAlter, nil, func(*testing.T, *Server) *transport.Frame {
			return transport.NewFrame(transport.OpUserAlter, []byte{0xff, 0x01, 0x02})
		}, transport.ErrInvalidArgument, []string{"user alter", "BSON"}},
		{"undecodable drop", handleUserDrop, nil, func(*testing.T, *Server) *transport.Frame {
			return transport.NewFrame(transport.OpUserDrop, []byte{0xff, 0x01, 0x02})
		}, transport.ErrInvalidArgument, []string{"user drop", "BSON"}},
		{"undecodable grant", handleUserGrant, nil, func(*testing.T, *Server) *transport.Frame {
			return transport.NewFrame(transport.OpUserGrant, []byte{0xff, 0x01, 0x02})
		}, transport.ErrInvalidArgument, []string{"user grant", "BSON"}},
		{"undecodable revoke", handleUserRevoke, nil, func(*testing.T, *Server) *transport.Frame {
			return transport.NewFrame(transport.OpUserRevoke, []byte{0xff, 0x01, 0x02})
		}, transport.ErrInvalidArgument, []string{"user revoke", "BSON"}},
		{"undecodable get", handleUserGet, nil, func(*testing.T, *Server) *transport.Frame {
			return transport.NewFrame(transport.OpUserGet, []byte{0xff, 0x01, 0x02})
		}, transport.ErrInvalidArgument, []string{"user get", "BSON"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := seeded(t)
			if tc.prepare != nil {
				tc.prepare(t, s)
			}
			code, message := userAdminRefusal(t, s, tc.handler, tc.frame(t, s))
			if code != tc.want {
				t.Errorf("code = %s, want %s; message: %s", code, tc.want, message)
			}
			for _, fragment := range tc.wantMessage {
				if !strings.Contains(message, fragment) {
					t.Errorf("message %q does not contain %q", message, fragment)
				}
			}
		})
	}
}

// TestUserAdminReadinessRefusalsAreUnavailable: not-master and no-quorum are
// retryable, so a driver must not classify them with INTERNAL (BUG-0622).
func TestUserAdminReadinessRefusalsAreUnavailable(t *testing.T) {
	for _, sentinel := range []error{ErrNotMaster, ErrNoQuorum} {
		wrapped := fmt.Errorf("%w\n       this node: node-b (not the master)", sentinel)
		if got := codeFor(wrapped); got != transport.ErrUnavailable {
			t.Errorf("codeFor(%v) = %s, want UNAVAILABLE", sentinel, got)
		}
	}
}

// TestUserGetUnknownAnswersFoundFalse: an unknown user is a successful USER_GET
// with found false, the answer the CLI's alter and describe branches read
// (BUG-0622). It used to be an ERROR frame, so found was never false.
func TestUserGetUnknownAnswersFoundFalse(t *testing.T) {
	s := userTestServer(t)
	answer, err := handleUserGet(context.Background(), marshalUserRequest(t, transport.OpUserGet, &UserGetRequest{Username: "ghost"}), s)
	if err != nil {
		t.Fatalf("handleUserGet: %v", err)
	}
	if answer.Opcode != transport.OpUserGet {
		t.Fatalf("answer opcode = %s, want USER_GET (a success frame); payload %q", answer.Opcode, answer.Payload)
	}
	var got UserGetResponse
	if err := bson.Unmarshal(answer.Payload, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Found {
		t.Errorf("Found = true for an unknown user")
	}
	if !strings.Contains(got.Message, `"ghost"`) || !strings.Contains(got.Message, "user list") {
		t.Errorf("Message = %q, want it to name the user and 'user list'", got.Message)
	}
}

// testSalt returns standard base64 of a 16-byte salt unique to seed, the
// shape the server now requires (BUG-0629): decoded length equals salt_len.
func testSalt(seed string) string {
	salt := make([]byte, defaultArgon2SaltLen)
	copy(salt, seed)
	return base64.StdEncoding.EncodeToString(salt)
}

// TestNotMasterRefusalNamesTheMastersDriverAddress is BUG-0709: the address a
// not-master refusal tells the operator to run the command against, and the
// one 'master' and 'user list' print, must be one a client can connect to --
// the master's driver listener -- not its ring address, where nothing
// answers a client.
func TestNotMasterRefusalNamesTheMastersDriverAddress(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	masterIndex := tc.masterIndex(t)
	followerIndex := 1 - masterIndex
	wantDriver := tc.driverAddrs[masterIndex]
	ringAddr := tc.addrs[masterIndex]
	if wantDriver == ringAddr {
		t.Fatalf("the harness gave node %d one address for ring and driver (%s); the test needs them to differ", masterIndex, ringAddr)
	}

	// The master's driver address reaches the follower with gossip, which runs
	// on a background loop: wait for it rather than assume the first request
	// after readiness has it.
	var listed UserListResponse
	deadline := time.Now().Add(10 * time.Second)
	for {
		frame := tc.ok(followerIndex, transport.OpUserList, &UserListRequest{})
		if err := bson.Unmarshal(frame.Payload, &listed); err != nil {
			t.Fatalf("decode user list: %v", err)
		}
		if listed.MasterAddr == wantDriver || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if listed.MasterAddr != wantDriver {
		t.Errorf("user list on the follower: MasterAddr = %q, want the master's driver address %q (its ring address is %q)",
			listed.MasterAddr, wantDriver, ringAddr)
	}

	// The acceptance criterion: a client that connects to the printed address
	// authenticates, and the node it reaches is the master.
	printed := tc.dialAddress(listed.MasterAddr)
	defer func() { _ = printed.Close() }()
	reached, err := printed.SendRequest(context.Background(), transport.OpUserList, mustMarshal(t, &UserListRequest{}))
	if err != nil || reached.Opcode == transport.OpError {
		t.Errorf("a client at the printed address %s could not list users: %v", listed.MasterAddr, err)
	}

	refusal := tc.frame(followerIndex, transport.OpUserCreate, clusterCreateRequest(t, "bug0709", "probe"))
	if refusal.Opcode != transport.OpError {
		t.Fatalf("create user on the follower answered %s, want an ERROR refusal", refusal.Opcode)
	}
	var decoded ErrorResponse
	if err := bson.Unmarshal(refusal.Payload, &decoded); err != nil {
		t.Fatalf("decode refusal: %v", err)
	}
	if decoded.WriteOutcome != "" {
		t.Errorf("refusal carries write_outcome %q: a not-master refusal is fail-closed before any apply and says nothing about a write", decoded.WriteOutcome)
	}
	message := errorMessageOf(t, refusal)
	for _, want := range []string{
		"at " + wantDriver,
		"run the same command against " + wantDriver,
	} {
		if !strings.Contains(message, want) {
			t.Errorf("refusal does not contain %q:\n%s", want, message)
		}
	}
	if strings.Contains(message, "at "+ringAddr) || strings.Contains(message, "against "+ringAddr) {
		t.Errorf("refusal sends the operator to the master's ring address %s, which no client can connect to:\n%s", ringAddr, message)
	}
}

// TestDriverAddressReplacesAWildcardHostWithTheRingHost: a node bound to every
// interface is reached on the host its peers already dial it by, with its own
// driver port, and an IPv6 host keeps its brackets (BUG-0709).
func TestDriverAddressReplacesAWildcardHostWithTheRingHost(t *testing.T) {
	cases := []struct {
		name, tcpListen, ringAddr, want string
	}{
		{"specific host is kept", "192.0.2.7:7433", "192.0.2.4:4433", "192.0.2.7:7433"},
		{"IPv4 wildcard takes the ring host", "0.0.0.0:7433", "192.0.2.4:4433", "192.0.2.4:7433"},
		{"no host takes the ring host", ":9000", "192.0.2.4:4433", "192.0.2.4:9000"},
		{"IPv6 wildcard takes the ring host", "[::]:7433", "192.0.2.4:4433", "192.0.2.4:7433"},
		{"IPv6 ring host keeps its brackets", "0.0.0.0:7433", "[2001:db8::4]:4433", "[2001:db8::4]:7433"},
		{"IPv6 listen is kept", "[2001:db8::7]:7433", "192.0.2.4:4433", "[2001:db8::7]:7433"},
		{"wildcard on a loopback ring is loopback", "0.0.0.0:7433", "127.0.0.1:4433", "127.0.0.1:7433"},
		{"unset listen has no address", "", "192.0.2.4:4433", ""},
		{"listen without a port has no address", "192.0.2.7", "192.0.2.4:4433", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{config: &config.Config{TCP: config.TCPConfig{Listen: tc.tcpListen}}}
			if got := s.driverAddress(tc.ringAddr); got != tc.want {
				t.Errorf("driverAddress(tcp.listen %q, ring %q) = %q, want %q", tc.tcpListen, tc.ringAddr, got, tc.want)
			}
		})
	}
}

// TestStandaloneMasterIsReportedOnItsDriverAddress: a node with no cluster is
// its own master, and the address it reports is its driver listener, not its
// ring address (BUG-0709).
func TestStandaloneMasterIsReportedOnItsDriverAddress(t *testing.T) {
	s := userTestServer(t)
	s.config.Server.Listen = "0.0.0.0:4433"
	s.config.TCP.Listen = "0.0.0.0:7433"
	answer, err := handleUserList(context.Background(), transport.NewFrame(transport.OpUserList, nil), s)
	if err != nil {
		t.Fatalf("handleUserList: %v", err)
	}
	var listed UserListResponse
	if err := bson.Unmarshal(answer.Payload, &listed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if want := "127.0.0.1:7433"; listed.MasterAddr != want {
		t.Errorf("MasterAddr = %q, want %q", listed.MasterAddr, want)
	}
}

// TestNotMasterRefusalSaysWhenTheMastersDriverAddressIsUnknown: until the
// master's own word on where clients reach it arrives (a restart, or an older
// peer that never sends it), the refusal still fails closed and says so; it
// never falls back to the ring address, and it names the way forward.
func TestNotMasterRefusalSaysWhenTheMastersDriverAddressIsUnknown(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	masterIndex := tc.masterIndex(t)
	followerIndex := 1 - masterIndex
	// The master stops so no gossip can bring its address back mid-test.
	tc.stopNodeKeepingData(masterIndex)
	follower := tc.nodes[followerIndex].server.core
	follower.cluster.peerDrivers.record(tc.addrs[masterIndex], "")

	refusal := tc.frame(followerIndex, transport.OpUserCreate, clusterCreateRequest(t, "bug0709-unknown", "probe"))
	if refusal.Opcode != transport.OpError {
		t.Fatalf("create user on the follower answered %s, want an ERROR refusal", refusal.Opcode)
	}
	message := errorMessageOf(t, refusal)
	for _, want := range []string{"not the master", "has not reached this node yet", "retry in a few seconds", "tcp.listen"} {
		if !strings.Contains(message, want) {
			t.Errorf("refusal does not contain %q:\n%s", want, message)
		}
	}
	if strings.Contains(message, "at "+tc.addrs[masterIndex]) || strings.Contains(message, "against "+tc.addrs[masterIndex]) {
		t.Errorf("refusal falls back to the ring address:\n%s", message)
	}
	if got := follower.auth.UserInfos(); len(got) != 1 {
		// The root user seeded at start is the only one.
		t.Errorf("users after the refusal = %d, want 1 (nothing was applied)", len(got))
	}
}

// TestDriverAddressOfARemoteMaster: a loopback tcp.listen on a member whose
// ring host is not loopback is the default configuration on a real
// multi-machine cluster. From another machine that address dials the asking
// node itself, so it is not offered as a place to run the command; the note
// names the fix (BUG-0709 verifier finding).
func TestDriverAddressOfARemoteMaster(t *testing.T) {
	cases := []struct {
		name, driver, ringAddr string
		wantAddress            string
		wantNote               []string
	}{
		{"reachable driver", "192.0.2.4:7433", "192.0.2.4:4433", "192.0.2.4:7433", nil},
		{"one-machine cluster on loopback", "127.0.0.1:7433", "127.0.0.1:4433", "127.0.0.1:7433", nil},
		{"loopback listen, non-loopback ring", "127.0.0.1:7433", "192.0.2.4:4433", "",
			[]string{"loopback", "127.0.0.1:7433", "set tcp.listen", "0.0.0.0:7433"}},
		{"IPv6 loopback listen, non-loopback ring", "[::1]:7433", "192.0.2.4:4433", "",
			[]string{"loopback", "set tcp.listen"}},
		{"localhost listen, non-loopback ring", "localhost:7433", "192.0.2.4:4433", "",
			[]string{"loopback", "set tcp.listen"}},
		{"nothing received", "", "192.0.2.4:4433", "",
			[]string{"has not reached this node yet"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			address, note := clientAddressOfRemote(tc.driver, tc.ringAddr)
			if address != tc.wantAddress {
				t.Errorf("address = %q, want %q", address, tc.wantAddress)
			}
			for _, want := range tc.wantNote {
				if !strings.Contains(note, want) {
					t.Errorf("note %q does not contain %q", note, want)
				}
			}
			if len(tc.wantNote) == 0 && note != "" {
				t.Errorf("note = %q, want none", note)
			}
		})
	}
}

// TestNotMasterDirectionsDoNotPrintAWithheldAddress: when the master's address
// is withheld the refusal says why and what to change, and names no address to
// run the command against.
func TestNotMasterDirectionsDoNotPrintAWithheldAddress(t *testing.T) {
	address, note := clientAddressOfRemote("127.0.0.1:7433", "192.0.2.4:4433")
	got := notMasterDirections(ring.Node{ID: "node-a", Addr: "192.0.2.4:4433"}, address, note)
	for _, want := range []string{"node-a", "loopback only", "set tcp.listen to 0.0.0.0:7433", "use 'master'"} {
		if !strings.Contains(got, want) {
			t.Errorf("directions do not contain %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "against 127.0.0.1") || strings.Contains(got, " at 127.0.0.1") {
		t.Errorf("directions offer the loopback address as a place to run the command:\n%s", got)
	}
}

// TestMasterClientAddressTrustsItsOwnListener: when the master is this node,
// its tcp.listen is reported as configured even on loopback with a ring host
// elsewhere. A client reading it is already connected to this node, so the
// loopback warning kept for a remote master would be noise here.
func TestMasterClientAddressTrustsItsOwnListener(t *testing.T) {
	members := ring.New(1)
	self := ring.Node{ID: "node-a", Addr: "192.0.2.4:4433"}
	for _, member := range []ring.Node{self, {ID: "node-b", Addr: "192.0.2.5:4433"}} {
		if err := members.AddNode(member); err != nil {
			t.Fatalf("AddNode %s: %v", member.ID, err)
		}
	}
	s := &Server{
		config: &config.Config{TCP: config.TCPConfig{Listen: "127.0.0.1:7433"}},
		cluster: &Cluster{
			self:        self,
			r:           members,
			peerDrivers: newPeerDriverTable(),
		},
	}

	address, note := s.masterClientAddress(self)
	if address != "127.0.0.1:7433" || note != "" {
		t.Errorf("masterClientAddress(self) = (%q, %q), want (127.0.0.1:7433, no note)", address, note)
	}

	// The same listener on the other member is withheld: the asker is not on that machine.
	s.cluster.peerDrivers.record("node-b", "127.0.0.1:7433")
	address, note = s.masterClientAddress(ring.Node{ID: "node-b", Addr: "192.0.2.5:4433"})
	if address != "" || !strings.Contains(note, "loopback") {
		t.Errorf("masterClientAddress(remote) = (%q, %q), want no address and a loopback note", address, note)
	}
}
