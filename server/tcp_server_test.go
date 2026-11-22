package server

import (
	"bufio"
	"crypto/tls"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// tcpServerClient is a minimal framed TLS client for driving the TCP
// listener in tests: dial, AUTH, then request/response calls.
type tcpServerClient struct {
	t    *testing.T
	conn *tls.Conn
	br   *bufio.Reader
}

func dialTCPServer(t *testing.T, addr string) *tcpServerClient {
	t.Helper()
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test driver client"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	conn, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &tcpServerClient{t: t, conn: conn, br: bufio.NewReader(conn)}
}

// call sends one frame and decodes the response, failing the test on any
// server OpError so a protocol problem is loud.
func (c *tcpServerClient) call(op transport.Opcode, req, resp interface{}) *transport.Frame {
	c.t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		c.t.Fatalf("marshal %s request: %v", op, err)
	}
	_ = c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	frame := transport.NewFrame(op, payload)
	if _, err := c.conn.Write(frame.Encode()); err != nil {
		c.t.Fatalf("write %s: %v", op, err)
	}
	f, err := transport.DecodeFrame(c.br)
	if err != nil {
		c.t.Fatalf("read response to %s: %v", op, err)
	}
	if f.Opcode == transport.OpError {
		var errResp ErrorResponse
		_ = bson.Unmarshal(f.Payload, &errResp)
		c.t.Fatalf("server error on %s: code=%d message=%q", op, errResp.Code, errResp.Message)
	}
	if resp != nil && len(f.Payload) > 0 {
		if err := bson.Unmarshal(f.Payload, resp); err != nil {
			c.t.Fatalf("unmarshal %s response: %v", op, err)
		}
	}
	return f
}

func (c *tcpServerClient) login() AuthResponse {
	c.t.Helper()
	var authResp AuthResponse
	resp := c.call(transport.OpAuth, &LoginRequest{Username: "admin", Password: "test123"}, &authResp)
	if resp.Opcode != transport.OpAuthOK {
		c.t.Fatalf("login opcode = %s, want AUTH_OK", resp.Opcode)
	}
	return authResp
}

// newTCPEnabledServer builds a Service with the TCP driver listener on
// an ephemeral port, seeded with the bench's root credentials.
func newTCPEnabledServer(t *testing.T, mutate func(*config.Config)) (*Service, string) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	if mutate != nil {
		mutate(cfg)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	qs, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	// A cluster member's administrator comes from the root task, as cmd/server arms it.
	seed := qs.SeedRootUser
	if cfg.Cluster.Enabled {
		seed = func(user, password string) error {
			return qs.BootstrapUsersFrom(user, password, true, "HOARDDB_ROOT_PASSWORD")
		}
	}
	if err := seed("admin", "test123"); err != nil {
		t.Fatalf("seeding the root user: %v", err)
	}
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = qs.Stop() })
	addr := qs.tcpListener.Addr().String()
	if addr == "" {
		t.Fatal("TCP listener has no bound address after Start")
	}
	if cfg.Cluster.Enabled {
		awaitClusterRoot(t, qs, "admin")
	}
	return qs, addr
}

// awaitClusterRoot waits until the root task of a cluster member has created name and this node
// installed it.
func awaitClusterRoot(t *testing.T, qs *Service, name string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, ok := qs.core.auth.UserInfo(name); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the root task did not create %q within 30 s", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTCPServerAuthPutGetBatch drives the full TCP listener path against a
// real server: AUTH (with the capability round-trip from spec 3.1), create
// database and bucket, PUT, GET, BATCH_PUT, GET again.
func TestTCPServerAuthPutGetBatch(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, nil)
	_ = qs

	c := dialTCPServer(t, addr)
	authResp := c.login()

	if authResp.ProtocolVersion != uint32(transport.ProtocolMajor) {
		t.Fatalf("negotiated protocol_version = %d, want %d", authResp.ProtocolVersion, transport.ProtocolMajor)
	}
	foundCapability := false
	for _, cap := range authResp.Capabilities {
		if cap == "pipelining" {
			foundCapability = true
		}
	}
	if !foundCapability {
		t.Fatalf("server advertised capabilities %v without pipelining", authResp.Capabilities)
	}
	if authResp.ServerVersion == "" {
		t.Fatal("server_version not advertised at AUTH")
	}

	c.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "testdb"}, nil)
	c.call(transport.OpCreateBucket, &CreateBucketRequest{
		Database: "testdb", Name: "User", Type: BucketTypeHash,
	}, nil)

	c.call(transport.OpPut, &InsertRequest{
		Database: "testdb", Bucket: "User", Key: "alice",
		Data: map[string]interface{}{"email": "alice@test.com", "age": 34},
	}, nil)

	var found FindResponse
	c.call(transport.OpGet, &FindRequest{Database: "testdb", Bucket: "User", Key: "alice"}, &found)
	if !found.Found {
		t.Fatal("GET missed the inserted document")
	}
	var doc map[string]interface{}
	if err := bson.Unmarshal(found.Data, &doc); err != nil {
		t.Fatalf("decode GET document: %v", err)
	}
	if doc["email"] != "alice@test.com" {
		t.Fatalf("GET returned %v, want the inserted document", doc)
	}

	c.call(transport.OpBatchPut, &BatchInsertRequest{
		Database: "testdb", Bucket: "User",
		Data: []map[string]interface{}{
			{"_id_": "bob", "email": "bob@test.com"},
			{"_id_": "carol", "email": "carol@test.com"},
		},
	}, nil)

	for _, key := range []string{"bob", "carol"} {
		var got FindResponse
		c.call(transport.OpGet, &FindRequest{Database: "testdb", Bucket: "User", Key: key}, &got)
		if !got.Found {
			t.Fatalf("GET missed batch-inserted key %q", key)
		}
	}
}

// TestTCPServerRestartPersists proves the data written over TCP survives a
// full server restart on the same data directory.
func TestTCPServerRestartPersists(t *testing.T) {
	dataDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	qs1, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := qs1.SeedRootUser("admin", "test123"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := qs1.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	addr1 := qs1.tcpListener.Addr().String()
	c1 := dialTCPServer(t, addr1)
	c1.login()
	c1.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "testdb"}, nil)
	c1.call(transport.OpCreateBucket, &CreateBucketRequest{Database: "testdb", Name: "User", Type: BucketTypeHash}, nil)
	c1.call(transport.OpPut, &InsertRequest{
		Database: "testdb", Bucket: "User", Key: "persist",
		Data: map[string]interface{}{"msg": "survives"},
	}, nil)
	if err := qs1.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}

	// Restart on the same data dir: catalog reattaches, TLS keys reuse, and
	// the entry written over TCP is still there. Root credentials bootstrap
	// idempotently on every boot, exactly as cmd/server/main.go does.
	qs2, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("restart NewService: %v", err)
	}
	if err := qs2.BootstrapUsers("admin", "test123", true); err != nil {
		t.Fatalf("restart BootstrapUsers: %v", err)
	}
	if err := qs2.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	if err := qs2.Start(); err != nil {
		t.Fatalf("restart Start: %v", err)
	}
	t.Cleanup(func() { _ = qs2.Stop() })
	addr2 := qs2.tcpListener.Addr().String()

	c2 := dialTCPServer(t, addr2)
	c2.login()
	var found FindResponse
	c2.call(transport.OpGet, &FindRequest{Database: "testdb", Bucket: "User", Key: "persist"}, &found)
	if !found.Found {
		t.Fatal("GET missed the pre-restart document")
	}
	var doc map[string]interface{}
	if err := bson.Unmarshal(found.Data, &doc); err != nil {
		t.Fatalf("decode GET document: %v", err)
	}
	if doc["msg"] != "survives" {
		t.Fatalf("GET returned %v after restart, want the pre-restart document", doc)
	}
}

// TestTCPServerMaxConnsNamedRefusal verifies HOARDDB_MAX_CONNS is real: a
// connection beyond the cap receives a named OpError (OVERLOADED) naming
// the cap, not an opaque close (spec 5.6).
func TestTCPServerMaxConnsNamedRefusal(t *testing.T) {
	var addr string
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) {
		cfg.Server.MaxConns = 2
	})
	_ = qs

	// Two connections fill the budget (the slots are held for the
	// connections' lifetimes — no AUTH needed, the cap is at accept-time).
	first := dialTCPServer(t, addr)
	defer func() { _ = first.conn.Close() }()
	second := dialTCPServer(t, addr)
	defer func() { _ = second.conn.Close() }()

	// The third is refused: the server still completes the TLS handshake,
	// then writes one OpError naming the cap, then closes.
	third := dialTCPServer(t, addr)
	_ = third.conn.SetDeadline(time.Now().Add(10 * time.Second))
	f, err := transport.DecodeFrame(third.br)
	if err != nil {
		t.Fatalf("read refusal: %v", err)
	}
	if f.Opcode != transport.OpError {
		t.Fatalf("refusal opcode = %s, want OpError", f.Opcode)
	}
	var errResp ErrorResponse
	if err := bson.Unmarshal(f.Payload, &errResp); err != nil {
		t.Fatalf("unmarshal refusal: %v", err)
	}
	if transport.ErrorCode(errResp.Code) != transport.ErrOverloaded {
		t.Fatalf("refusal code = %d, want OVERLOADED (%d)", errResp.Code, transport.ErrOverloaded)
	}
	if !strings.Contains(errResp.Message, "HOARDDB_MAX_CONNS") {
		t.Fatalf("refusal %q does not name the enforced cap", errResp.Message)
	}
	if !strings.Contains(errResp.Message, "retry shortly") {
		t.Fatalf("refusal %q lacks retry guidance", errResp.Message)
	}

	// A released slot admits a fresh connection again.
	_ = first.conn.Close()
	time.Sleep(100 * time.Millisecond)
	fresh := dialTCPServer(t, addr)
	_ = fresh.conn.SetDeadline(time.Now().Add(10 * time.Second))
	// The fresh connection is admitted; it must be able to AUTH, proving
	// the slot was freed and the refusal was not permanent.
	resp := fresh.call(transport.OpAuth, &LoginRequest{Username: "admin", Password: "test123"}, nil)
	if resp.Opcode != transport.OpAuthOK {
		t.Fatalf("post-release auth opcode = %s, want AUTH_OK", resp.Opcode)
	}
}

// TestTCPServerNodeToNodeRefused guards spec 7 over the real server: a
// node-to-node opcode (REPL_PUSH) over TCP is refused by name.
func TestTCPServerNodeToNodeRefused(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	c := dialTCPServer(t, addr)
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))

	frame := transport.NewFrame(transport.OpReplPush, []byte("{}"))
	if _, err := c.conn.Write(frame.Encode()); err != nil {
		t.Fatalf("write repl push: %v", err)
	}
	f, err := transport.DecodeFrame(c.br)
	if err != nil {
		t.Fatalf("read refusal: %v", err)
	}
	if f.Opcode != transport.OpError {
		t.Fatalf("refusal opcode = %s, want OpError", f.Opcode)
	}
	var errResp ErrorResponse
	_ = bson.Unmarshal(f.Payload, &errResp)
	if !strings.Contains(errResp.Message, "REPL_PUSH") {
		t.Fatalf("refusal %q does not name the refused opcode", errResp.Message)
	}
}
