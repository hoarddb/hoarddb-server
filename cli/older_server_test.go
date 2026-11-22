package cli

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// What a real server built from the parent of the BUCKET_INFO merge (f6e448b9)
// answers to OpBucketInfo (0xD9), captured from that binary through this CLI.
// Its authorization layer rejects an opcode it has no rule for before the
// dispatcher is reached, so the dispatcher's "unsupported opcode" never
// appears: the first CLI that keyed off that phrase failed every get.
const (
	olderServerDefaultDeny  = `no authorization rule is declared for opcode 0xD9 "UNKNOWN"; refusing the frame (default deny)`
	dispatcherUnsupportedOp = `unsupported opcode 0xD9 (request id 1): this server does not implement it`
)

// TestAnOlderServersWordsAreRecognisedExactly pins which BUCKET_INFO errors
// mean "this server does not know the op", and which must not: a real
// permission error or a missing bucket is not an old server.
func TestAnOlderServersWordsAreRecognisedExactly(t *testing.T) {
	rows := []struct {
		name string
		err  error
		want bool
	}{
		{"the older server's default deny", &ServerError{Code: transport.ErrInternal, Message: olderServerDefaultDeny}, true},
		{"the dispatcher's unsupported opcode", &ServerError{Code: transport.ErrInternal, Message: dispatcherUnsupportedOp}, true},
		{"wrapped as send wraps it", fmt.Errorf("inspecting: %w", &ServerError{Code: transport.ErrInternal, Message: olderServerDefaultDeny}), true},
		{"a permission error", &ServerError{Code: transport.ErrPermissionDenied, Message: `permission denied: user "bob" has no read grant on app`}, false},
		{"a permission error naming the opcode", &ServerError{Code: transport.ErrPermissionDenied, Message: `permission denied for opcode 0xD9`}, false},
		{"a missing bucket", &ServerError{Code: transport.ErrNotFound, Message: `bucket not found: bucket "Ghost" not found in database "app"`}, false},
		{"the default deny of another opcode", &ServerError{Code: transport.ErrInternal, Message: `no authorization rule is declared for opcode 0x20 "UNKNOWN"; refusing the frame (default deny)`}, false},
		{"an unsupported other opcode", &ServerError{Code: transport.ErrInternal, Message: `unsupported opcode 0x7F (request id 1): this server does not implement it`}, false},
		{"a timeout", errors.New("context deadline exceeded"), false},
		{"no error", nil, false},
	}
	for _, row := range rows {
		if got := isUnsupportedOpcode(row.err); got != row.want {
			t.Errorf("%s: isUnsupportedOpcode = %v, want %v", row.name, got, row.want)
		}
	}
}

// TestAnOlderServerThatDeniesTheOpcodeStillServesEveryLookup: the real wording,
// through a stub, for every path that looks a declaration up. Each asks
// BUCKET_STATS, as the CLI did before BUCKET_INFO, and works.
func TestAnOlderServerThatDeniesTheOpcodeStillServesEveryLookup(t *testing.T) {
	script := declarationScript(declaredBucket{bucketType: server.BucketTypeHeap,
		stores: []server.StoreStats{{Name: "Photos", Engine: "blob"}, {Name: "Jobs", Engine: "heap"}}})
	script[transport.OpBucketInfo] = errorFrame(t, transport.ErrInternal, olderServerDefaultDeny)
	stub, c, _ := newDeclarationStub(t, script)

	for _, statement := range []string{
		`db.Users.push({priority: 3, n: 1})`,
		`db.Users["u1"].Photos.put("a", {data: "x"})`,
		`db.Users["u1"].Jobs.push({priority: 1.5})`,
	} {
		for range 3 {
			err := c.handle(statement)
			if statement == `db.Users["u1"].Jobs.push({priority: 1.5})` {
				if err == nil {
					t.Fatalf("%s: a fraction on a heap field was accepted", statement)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s on an older server: %v", statement, err)
			}
		}
	}
	if got := stub.sent(transport.OpBucketStats); got != 9 {
		t.Errorf("9 lookups on an older server sent %d BUCKET_STATS frames, want 9", got)
	}
	if got := stub.sent(transport.OpBucketInfo); got != 1 {
		t.Errorf("9 lookups on an older server sent %d BUCKET_INFO frames, want 1: the refusal is remembered for the bucket", got)
	}
}

// TestAPermissionErrorOnBucketInfoIsNotAnOlderServer: the fallback must not turn
// a refusal into a second, different question.
func TestAPermissionErrorOnBucketInfoIsNotAnOlderServer(t *testing.T) {
	script := declarationScript(declaredBucket{bucketType: server.BucketTypeHash})
	script[transport.OpBucketInfo] = errorFrame(t, transport.ErrPermissionDenied, `permission denied: user "bob" has no read grant on app`)
	stub, c, _ := newDeclarationStub(t, script)

	err := c.handle(`db.Users.get("u1")`)
	if err == nil || err.Error() != `inspecting bucket "Users": server error (3): permission denied: user "bob" has no read grant on app` {
		t.Fatalf("get returned %v, want the permission error as it was", err)
	}
	if got := stub.sent(transport.OpBucketStats); got != 0 {
		t.Errorf("a permission error sent %d BUCKET_STATS frames, want 0", got)
	}
}

// startOlderServer runs the binary named by HOARDDB_OLD_SERVER_BIN (a server
// built from the parent of f6e448b9, which does not know BUCKET_INFO) and
// returns a connected CLI. Without the variable the test is skipped:
//
//	git worktree add --detach /path/old f6e448b9^
//	(cd /path/old/hoardDB && go build -o /path/server-old ./cmd/server)
//	HOARDDB_OLD_SERVER_BIN=/path/server-old go test -run TestTheCLIAgainstAnOlderServer ./cli/
func startOlderServer(t *testing.T) *CLI {
	t.Helper()
	binary := os.Getenv("HOARDDB_OLD_SERVER_BIN")
	if binary == "" {
		t.Skip("HOARDDB_OLD_SERVER_BIN is not set: no older server to run against")
	}
	driverAddress, httpAddress, internodeAddress := freeLoopbackAddress(t), freeLoopbackAddress(t), freeLoopbackAddress(t)
	process := exec.Command(binary)
	process.Env = append(os.Environ(),
		"HOARDDB_ROOT_USER=admin", "HOARDDB_ROOT_PASSWORD=older-server-password",
		"HOARDDB_DATA_DIR="+t.TempDir(),
		"HOARDDB_TCP_LISTEN="+driverAddress, "HOARDDB_LISTEN="+httpAddress, "HOARDDB_TCP_INTERNODE_LISTEN="+internodeAddress)
	if err := process.Start(); err != nil {
		t.Fatalf("starting %s: %v", binary, err)
	}
	t.Cleanup(func() {
		_ = process.Process.Kill()
		_ = process.Wait()
	})

	var out bytes.Buffer
	c := NewCLI(WithAddress(driverAddress), WithInsecureTLS(true), WithCredentials("admin", "older-server-password"), WithTerminal(&out), WithTheme("plain"))
	deadline := time.Now().Add(20 * time.Second)
	for {
		err := c.Connect()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the older server never accepted a connection: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Cleanup(c.Close)
	return c
}

// freeLoopbackAddress is a loopback address nothing listens on.
func freeLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("releasing the port: %v", err)
	}
	return address
}

// TestTheCLIAgainstAnOlderServer is the case the stubs could not model: a real
// server whose authorization layer refuses BUCKET_INFO in its own words. Every
// statement that looks a declaration up must work exactly as on main, and the
// display, turned on, must stay off with no error line.
func TestTheCLIAgainstAnOlderServer(t *testing.T) {
	c := startOlderServer(t)
	for _, statement := range []string{
		`create database app`, `use app`,
		`create bucket Users { type: hash }`,
		`create bucket Files { type: blob }`,
		`create bucket Tasks { type: heap }`,
		`create bucket Post { type: hash, Photos blobStore, Jobs heapStore, Notes fifoStore }`,
		`db.Users.insert({"_id_": "u1", "n": 1})`,
		`db.Files.put({"_id_": "f1", "data": "hi"})`,
		`db.Post.insert({"_id_": "p1", "n": 1})`,
	} {
		if err := c.handle(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	c.handler.SetDocumentDecoration(true)
	var chatter strings.Builder
	c.handler.SetChatter(&chatter)

	run := func(statement string) string {
		t.Helper()
		var printed bytes.Buffer
		c.handler.terminal = &printed
		cmd, err := ParseCommand(statement)
		if err != nil {
			t.Fatalf("parsing %s: %v", statement, err)
		}
		cmd.Database = "app"
		if err := c.handler.Handle(cmd); err != nil {
			t.Fatalf("%s on an older server: %v", statement, err)
		}
		return printed.String()
	}
	for range 7 {
		if got := strings.TrimSpace(run(`db.Users.get("u1")`)); got != `{"n": 1, "_id_": "u1"}` && got != `{"_id_": "u1", "n": 1}` {
			t.Fatalf("get printed %q", got)
		}
		if got := run(`db.Files.get("f1")`); !strings.Contains(got, "hi") {
			t.Fatalf("a blob get printed %q", got)
		}
		if got := run(`db.Post.get("p1")`); strings.Contains(got, "Store") {
			t.Fatalf("the display decorated a document on an older server: %q", got)
		}
	}
	for range 5 {
		run(`db.Tasks.push({priority: 3, n: 1})`)
		run(`db.Post["p1"].Photos.put("a", {data: "x"})`)
		run(`db.Post["p1"].Notes.push({priority: 1.5, n: 1})`)
	}
	if err := c.handle(`db.Post["p1"].Jobs.push({priority: 1.5, n: 1})`); err == nil {
		t.Error("a fractional priority on a heap field was accepted on an older server")
	}
	if chatter.Len() != 0 {
		t.Errorf("an older server made the CLI say %q", chatter.String())
	}
}
