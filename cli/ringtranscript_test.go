package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The ring REPL transcripts (U77) pin the look of ring mode the way
// TestREPLTranscriptUnchanged pins the v1.0 look: each row runs statements
// against a real in-process server and records the prompt, the statement and
// what it printed. Goldens live in testdata/ringgolden/ and are rewritten only
// with HOARDDB_UPDATE_GOLDENS=1, like the legacy ones. They are kept apart from
// testdata/golden/ because TestGoldenFilesHaveOwners owns that directory.

const ringGoldenDir = "testdata/ringgolden"

type ringTranscriptRow struct {
	slug string
	// setup runs before the recorded statements and is not recorded.
	setup []string
	// seed writes documents the statement grammar cannot spell (a datetime).
	seed       func(t *testing.T, ctx context.Context, client *ringclient.Client)
	statements []string
}

var ringTranscriptRows = []ringTranscriptRow{
	{slug: "schema", statements: []string{
		"show databases", "create database app", "create database shop { replication_factor: 1 }", "show databases", "show databases limit 1",
		"use app", "show buckets", "describe database app",
		"create bucket User { Name string, Age int, Joined datetime, Purchases fifoStore, Friends hashStore }",
		"show buckets", "describe database app", "describe database app limit 1", "describe bucket User", "describe bucket Nope", "use nope", "show buckets in nope",
		"describe database nope",
	}},
	{slug: "users", setup: []string{"create database app"}, statements: []string{
		"show users", "create user alice roles [reader@app]", "grant role writer@app to alice", "show users", "revoke role reader@app from alice",
		"alter user alice disable", "alter user alice enable", "alter user alice roles [db-manager@app, reader@*]", "user list",
		"drop user alice", "drop user alice", "grant role writer@app to alice", "create user alice roles reader@app", "user describe alice",
	}},
	{slug: "alter", setup: []string{"create database app", "use app", "create bucket User { Name string, Age int, Purchases fifoStore }"}, statements: []string{
		"alter bucket User { add Email string, drop Age }", "describe bucket User", "alter bucket User { add Email string }",
		"alter bucket User { add Orders fifoStore }", "alter bucket User { drop Email, add Email string }", "alter bucket User { add Name int }",
		"alter bucket User { drop Nmae }", "alter bucket Nope { add A string }", "alter bucket User { drop Age }", "alter bucket User { add Age int }",
		"describe bucket User", "alter bucket User { }",
	}},
	{slug: "describe-long-names", setup: []string{"create database app", "use app"}, statements: []string{
		"create bucket Profile { Name string, FollowerCount int, FollowingCount int, Posts fifoStore }", "describe bucket Profile",
	}},
	{slug: "documents", setup: []string{
		"create database app", "use app", "create bucket User { Name string, Age int, Joined datetime, Purchases fifoStore, Friends hashStore }",
	}, seed: ringSeedAda, statements: []string{
		`db.User["ada"].get()`, `db.User["nobody"].get()`, "db.User.list()", "db.User.count()",
		`db.User["ada"].Purchases.push({Item: "coffee", Qty: 2})`, `db.User["ada"].Purchases.push({Item: "tea", Qty: 1})`,
		`db.User["ada"].Friends.put("grace", {since: 2024})`, `db.User["ada"].get()`,
		`db.User["ada"].Purchases.peek()`, `db.User["ada"].Purchases.peek({limit: 2})`, `db.User["ada"].Purchases.list({limit: 10})`,
		`db.User["ada"].Purchases.pop()`, `db.User["ada"].Purchases.count()`, `db.User["ada"].Purchases.pop()`, `db.User["ada"].Purchases.pop()`,
	}},
	{slug: "documents-pretty", setup: []string{
		"create database app", "use app", "create bucket User { Name string, Age int, Joined datetime, Purchases fifoStore }",
	}, seed: ringSeedAda, statements: []string{
		"set pretty on", `db.User["ada"].get()`, "db.User.list()",
		`db.User["ada"].Purchases.push({Item: "coffee", Qty: 2})`, `db.User["ada"].Purchases.peek()`, "set pretty off", `db.User["ada"].get()`,
	}},
	{slug: "documents-extjson", setup: []string{
		"create database app", "use app", "create bucket User { Name string, Age int, Joined datetime, Purchases fifoStore }",
	}, seed: ringSeedAda, statements: []string{
		"set extjson on", `db.User["ada"].get()`, "set pretty on", "db.User.list()", "set extjson off", `db.User["ada"].get()`,
	}},
	{slug: "set", statements: []string{
		"set", "set pretty", "set pretty on", "set pretty", "set extjson", "set extjson on", "set pager", "set pager off", "set pager on",
		"set pretty maybe", "set colour on",
	}},
	{slug: "help", statements: []string{
		"help", "help all", "help create", "help db", "help set", "help nothing",
	}},
	{slug: "seeding", setup: []string{"create database app"}, statements: []string{
		"status seeding", "status seeding app", "status seeding app 5", "status seeding nobody", "seeding retry app 5", "seeding retry app 0x0a4e", "seeding forget app 5",
		"seeding retry app", "seeding retry app 9000", "seeding forget", "status seeding app 5 6", "help seeding", "help status",
	}},
	{slug: "cluster", statements: []string{"nodes", "status"}},
	{slug: "refusals", setup: []string{"create database app", "use app"}, statements: []string{
		"promote node-b", "leave node-b", "alter database app { replication_factor: 2 }", "drop database app",
		"frobnicate", "db.User.find()", "create bucket", `db.User["ada"].get(`,
	}},
}

func ringSeedAda(t *testing.T, ctx context.Context, client *ringclient.Client) {
	t.Helper()
	joined := time.Date(2024, 8, 24, 4, 57, 21, 295_000_000, time.UTC)
	_, err := client.Bucket("app", "User").Insert(ctx, "ada", ringclient.Doc{"Name": "Ada", "Age": int64(36), "Joined": joined})
	if err != nil {
		t.Fatalf("seeding ada: %v", err)
	}
}

var (
	ringBucketID = regexp.MustCompile(`\b[0-9a-f]{16}\b`)
	ringLoopback = regexp.MustCompile(`127\.0\.0\.1:[0-9]+`)
	ringLeaseID  = regexp.MustCompile(`\b[0-9a-f]{32}\b`)
	ringCursor   = regexp.MustCompile(`(cursor:? "?)[0-9a-f]{20,}`)
)

func ringNormalise(transcript, addr string) string {
	transcript = strings.ReplaceAll(transcript, addr, "<ADDR>")
	transcript = ringLoopback.ReplaceAllString(transcript, "<ADDR>")
	transcript = ringLeaseID.ReplaceAllString(transcript, "<LEASE-ID>")
	transcript = ringBucketID.ReplaceAllString(transcript, "<BUCKET-ID.....>")
	transcript = ringCursor.ReplaceAllString(transcript, "${1}<CURSOR>")
	return transcript
}

// ringRecord runs statements as the interactive session does and returns the
// transcript: the prompt and the statement, then what it printed, or
// 'Error: <text>' for a failure.
func ringRecord(ctx context.Context, session *RingSession, statements []string) string {
	var out strings.Builder
	for _, statement := range statements {
		out.WriteString(session.Prompt() + statement + "\n")
		output, err := session.Execute(ctx, statement)
		for _, line := range output.Lines {
			out.WriteString(line + "\n")
		}
		if err != nil {
			out.WriteString("Error: " + terminalSafeText(err.Error()) + "\n")
		}
	}
	return out.String()
}

// TestRingREPLTranscript pins what ring mode prints, route by route.
func TestRingREPLTranscript(t *testing.T) {
	for _, row := range ringTranscriptRows {
		t.Run(row.slug, func(t *testing.T) {
			addr := u52LiveServer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			t.Cleanup(cancel)
			client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			passwords := filepath.Join(t.TempDir(), "new.pw")
			if err := os.WriteFile(passwords, []byte("alice-s3cret-pw\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			session := NewRingSession(NewRingAPI(client), WithRingPasswordSource(passwords, false, io.Discard))
			for _, statement := range row.setup {
				if _, err := session.Execute(ctx, statement); err != nil {
					t.Fatalf("setup %q: %v", statement, err)
				}
			}
			if row.seed != nil {
				row.seed(t, ctx, client)
			}
			assertRingGolden(t, row.slug, ringNormalise(ringRecord(ctx, session, row.statements), addr))
		})
	}
}

// ringPiped runs a piped ring session through the whole binary path.
func ringPiped(t *testing.T, addr, script string, flags ...string) (status int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	args := append([]string{"-insecure", "-address", addr, "-user", "root", "-password=live-test-password"}, flags...)
	status = RunMain(Invocation{Args: args, Stdin: strings.NewReader(script), Stdout: &out, Stderr: &errOut,
		Getenv: func(string) string { return "" }})
	return status, ringNormalise(out.String(), addr), errOut.String()
}

// TestRingPipedLooksLikeV1 pins the start of a piped session: the same notes
// the legacy session prints, the same two greeting lines, the same prompt-free
// output, 'CLI error:' on stderr.
func TestRingPipedLooksLikeV1(t *testing.T) {
	addr := u52LiveServer(t)
	status, out, errOut := ringPiped(t, addr, "create database app\nuse app\ncreate bucket User { Name string }\nshow buckets\ndb.User[\"x\"].get()\nshow buckets\n")
	if status != 2 {
		t.Fatalf("status %d, want 2 (the owner refused the read)", status)
	}
	assertRingGolden(t, "piped-session", "--- stdout ---\n"+out+"--- stderr ---\n"+ringNormalise(errOut, addr))
}

// TestRingFlagsSetTheOutput: -pretty and -extjson mean what they mean in v1.0.
func TestRingFlagsSetTheOutput(t *testing.T) {
	addr := u52LiveServer(t)
	script := "create database app\nuse app\ncreate bucket User { Name string }\ndb.User.insert({_id_: \"ada\", Name: \"Ada\"})\ndb.User[\"ada\"].get()\n"
	_, plain, _ := ringPiped(t, addr, script)
	if !strings.Contains(plain, "{\"Name\": \"Ada\", \"_id_\": \"ada\"}\n") {
		t.Errorf("default output is not one line with sorted keys:\n%s", plain)
	}
	_, pretty, _ := ringPiped(t, addr, "use app\ndb.User[\"ada\"].get()\n", "-pretty")
	if !strings.Contains(pretty, "{\n  \"Name\": \"Ada\",\n  \"_id_\": \"ada\"\n}\n") {
		t.Errorf("-pretty did not indent:\n%s", pretty)
	}
}

// assertRingGolden compares transcript with testdata/ringgolden/<slug>.golden,
// or rewrites the file when HOARDDB_UPDATE_GOLDENS=1.
func assertRingGolden(t *testing.T, slug, transcript string) {
	t.Helper()
	path := filepath.Join(ringGoldenDir, slug+".golden")
	if os.Getenv(goldenUpdateEnv) == "1" {
		if err := os.MkdirAll(ringGoldenDir, 0o755); err != nil {
			t.Fatalf("creating %s: %v", ringGoldenDir, err)
		}
		if err := os.WriteFile(path, []byte(transcript), 0o644); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v\nrecord it with %s=1 go test ./cli/ -run TestRing", path, err, goldenUpdateEnv)
	}
	if transcript != string(want) {
		t.Errorf("%s changed.\n--- golden ---\n%s\n--- now ---\n%s", path, want, transcript)
	}
}

// TestRingRefusalsAtThePrompt pins what a prompt session prints for a failed statement: the problem on the
// first line, the fix on its own. The piped form of the same failures is refusals.golden.
func TestRingRefusalsAtThePrompt(t *testing.T) {
	addr := u52LiveServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	session := NewRingSession(NewRingAPI(client))
	for _, statement := range []string{"create database app", "use app"} {
		if _, err := session.Execute(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	var out strings.Builder
	for _, statement := range []string{"promote node-b", "alter database app { replication_factor: 2 }", "use shop", "describe bucket Nope",
		"frobnicate", "creat database app", "db.User.find()", "create bucket", `db.User["ada"].get(`, "show buckets limit", `db.User["ghost"].get()`,
		`db.User.insert({_id_: "a"`, "help fifo"} {
		out.WriteString(session.Prompt() + statement + "\n")
		if _, err := session.Execute(ctx, statement); err != nil {
			out.WriteString(strings.Join(ringErrorLines(err), "\n") + "\n")
		}
	}
	assertRingGolden(t, "refusals-prompt", ringNormalise(out.String(), addr))
}
