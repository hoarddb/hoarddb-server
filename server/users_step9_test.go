package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// clusterAuthAs dials a node and authenticates as a named user, returning the
// AUTH failure as an error rather than fataling so a poll loop can retry.
func clusterAuthAs(t *testing.T, addr, user, pass string) error {
	t.Helper()
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test driver client"}}, addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := transport.NewDriverClient(ctx, addr, tlsCfg)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	payload, err := bson.Marshal(AuthRequest{Username: user, Password: pass})
	if err != nil {
		return err
	}
	frame, err := client.SendRequest(ctx, transport.OpAuth, payload)
	if err != nil {
		return err
	}
	if frame.Opcode != transport.OpAuthOK {
		var fail AuthFailResponse
		_ = bson.Unmarshal(frame.Payload, &fail)
		return fmt.Errorf("%s on %s: %s (%s)", user, addr, frame.Opcode, fail.Message)
	}
	return nil
}

// TestUserCreatedOnMasterAuthenticatesOnEveryNode is the §8.4 acceptance: the
// config replicates to every member, not to rf replicas.
func TestUserCreatedOnMasterAuthenticatesOnEveryNode(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	masterIndex := clusterMasterIndex(t, tc)

	credential, err := NewCredential("replicated-password-123", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	frame := tc.frame(masterIndex, transport.OpUserCreate, &UserCreateRequest{
		RequestID:  "step9-create",
		Username:   "alice",
		Credential: UserCredential{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params},
		Roles:      []UserRole{{Role: "reader", DB: "app"}},
	})
	if frame.Opcode == transport.OpError {
		t.Fatalf("create user on the master was refused: %s", frame.Payload)
	}

	deadline := time.Now().Add(30 * time.Second)
	for _, addr := range tc.driverAddrs {
		for {
			err := clusterAuthAs(t, addr, "alice", "replicated-password-123")
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("alice did not authenticate on %s within the retry window: %v", addr, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// TestStaleFollowerRefusesRefresh is the §5.3.5 half the first draft left
// open: the staleness bound must cover token refreshes, not only fresh
// password logins.
func TestStaleFollowerRefusesRefresh(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	token, _ := mintToken(t, am, "alice")
	s := &Server{config: &config.Config{Auth: config.AuthConfig{AuthConfigMaxStalenessSeconds: 1}}, logger: discardLogger()}
	am.SetFollower(true)
	am.mu.Lock()
	am.lastSync = time.Now().Add(-2 * time.Second)
	am.mu.Unlock()

	req := AuthRequest{Token: token}
	resp, err := handleTokenAuth(transport.NewFrame(transport.OpAuth, mustMarshalBSON(t, &req)), req, am, s, nil, discardLogger())
	if err != nil {
		t.Fatalf("handleTokenAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthFail {
		t.Fatalf("stale refresh opcode = %s, want AUTH_FAIL", resp.Opcode)
	}
	var fail AuthFailResponse
	_ = bson.Unmarshal(resp.Payload, &fail)
	if !strings.Contains(fail.Message, "behind the cluster master") {
		t.Fatalf("stale refusal %q does not name the staleness bound", fail.Message)
	}
}

// TestStaleFollowerRecoversAfterSync asserts the bound is a bound, not a
// permanent lockout: a push that touches the sync clock restores refreshes.
func TestStaleFollowerRecoversAfterSync(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	token, _ := mintToken(t, am, "alice")
	s := &Server{config: &config.Config{Auth: config.AuthConfig{AuthConfigMaxStalenessSeconds: 1}}, logger: discardLogger()}
	am.SetFollower(true)
	am.mu.Lock()
	am.lastSync = time.Now().Add(-2 * time.Second)
	am.mu.Unlock()
	am.TouchConfigSync()

	req := AuthRequest{Token: token}
	resp, err := handleTokenAuth(transport.NewFrame(transport.OpAuth, mustMarshalBSON(t, &req)), req, am, s, nil, discardLogger())
	if err != nil {
		t.Fatalf("handleTokenAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthOK {
		t.Fatalf("refresh after a sync = %s, want AUTH_OK", resp.Opcode)
	}
}

// TestMemberRefusesLogicallyWrongConfig is the §8.4 guard that stops one bad
// config from bricking every node: the member refuses it and keeps serving
// from its current config.
func TestMemberRefusesLogicallyWrongConfig(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "root")
	before := len(am.UserInfos())

	bad := &usersConfig{FormatVersion: usersFormatVersion, Term: 99, Version: 99, Master: "m"}
	if err := am.ApplyConfig(bad); err == nil {
		t.Fatal("ApplyConfig accepted an empty user list")
	}
	if got := len(am.UserInfos()); got != before {
		t.Fatalf("the refused config changed the store (%d users, want %d)", got, before)
	}
}

// TestAuthConfigVersionGate checks the fence rules the member applies.
func TestAuthConfigVersionGate(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "root")
	cfg := &usersConfig{FormatVersion: usersFormatVersion, Term: 2, Version: 5, Master: "m", Users: []UserRecord{rootRecord("root")}}
	if err := am.ApplyConfig(cfg); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	if am.NewerThan(2, 5) {
		t.Fatal("an equal version was reported newer")
	}
	if !am.NewerThan(2, 4) {
		t.Fatal("a higher version within the term was not newer")
	}
	if !am.NewerThan(1, 999) {
		t.Fatal("a higher term must win regardless of version")
	}
	if am.NewerThan(3, 0) {
		t.Fatal("a lower term must lose regardless of version")
	}
	// Re-applying the same config is an idempotent no-op.
	if err := am.ApplyConfig(cfg); err != nil {
		t.Fatalf("idempotent ApplyConfig: %v", err)
	}
}

// TestConfigBlockedRefusesAuth is the config-less bootstrap block: a
// clustered node with no users.json refuses AUTH until a config arrives.
func TestConfigBlockedRefusesAuth(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "root")
	s := &Server{config: config.DefaultConfig(), logger: discardLogger()}
	am.SetConfigBlocked(true)

	req := AuthRequest{Username: "root", Password: "correct-horse-battery"}
	resp, err := handleAuth(context.Background(), transport.NewFrame(transport.OpAuth, mustMarshalBSON(t, &req)), am, s, discardLogger())
	if err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthFail {
		t.Fatalf("AUTH on a config-less node = %s, want AUTH_FAIL", resp.Opcode)
	}
	var fail AuthFailResponse
	_ = bson.Unmarshal(resp.Payload, &fail)
	if !strings.Contains(fail.Message, "no auth config yet") {
		t.Fatalf("blocked refusal %q does not explain the wait", fail.Message)
	}

	am.SetConfigBlocked(false)
	resp, err = handleAuth(context.Background(), transport.NewFrame(transport.OpAuth, mustMarshalBSON(t, &req)), am, s, discardLogger())
	if err != nil {
		t.Fatalf("handleAuth after unblock: %v", err)
	}
	if resp.Opcode != transport.OpAuthOK {
		t.Fatalf("AUTH after the block cleared = %s, want AUTH_OK", resp.Opcode)
	}
}

// TestPushedConfigTermFence exercises the handler branch the pure NewerThan
// rule test cannot reach (build doc 16.2): a member refuses a push whose term
// is older than its own membership epoch, even if the version is higher.
func TestPushedConfigTermFence(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	s := tc.nodes[0].server.core
	if epoch := s.cluster.Epoch(); epoch == 0 {
		t.Fatalf("cluster epoch = 0; the fence test needs a newer membership view")
	}
	req := AuthConfigPushRequest{
		Token:   s.cluster.localToken(),
		Term:    0, // older than any elected membership epoch
		Version: 99,
		Master:  "old-node",
	}
	payload, err := bson.Marshal(&req)
	if err != nil {
		t.Fatalf("marshal push: %v", err)
	}
	resp, err := handleAuthConfigPush(context.Background(), transport.NewFrame(transport.OpAuthConfigPush, payload), s)
	if err != nil {
		t.Fatalf("handleAuthConfigPush: %v", err)
	}
	var pushResp AuthConfigPushResponse
	if err := bson.Unmarshal(resp.Payload, &pushResp); err != nil {
		t.Fatalf("decode push response: %v", err)
	}
	if !strings.Contains(pushResp.Error, "older than this node's membership epoch") {
		t.Fatalf("older-term push response = %+v, want a term-fence refusal", pushResp)
	}
	if pushResp.Ack {
		t.Fatal("an older-term push was acked")
	}
}
