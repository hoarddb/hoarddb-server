package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// runRingDumpForTest runs the verb the way main does, with its own streams.
func runRingDumpForTest(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	code = runRingDump(ctx, args, ringDumpEnv{stdout: &out, stderr: &errOut})
	return code, out.String(), errOut.String()
}

// dumpOnHost runs the verb against the test host at addr, with the connection flags
// ringIngestLogin returns (the root login newRingTestHost seeded).
func dumpOnHost(t *testing.T, addr string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runRingDumpForTest(t, append(append([]string{}, args...), ringIngestLogin(addr)...)...)
}

// dumpTestBucket is the one bucket dumpTestDatabase creates: a run that writes no bucket
// is refused as an empty dump (section 5), so a database with nothing in it cannot show
// what a dump of a database looks like.
const dumpTestBucket = "Seed"

// dumpTestDatabase creates the database a dump needs to get past the zero-databases
// refusal, with one bucket in it to get past the empty-dump refusal.
func dumpTestDatabase(t *testing.T, client *ringclient.Client, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, name, 1); err != nil {
		t.Fatalf("creating database %s on the test host: %v", name, err)
	}
	if _, err := client.CreateBucket(ctx, name, dumpTestBucket, []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
	}); err != nil {
		t.Fatalf("creating bucket %s.%s on the test host: %v", name, dumpTestBucket, err)
	}
}

// readDumpRecords reads a written dump back with the format F reader and returns every
// record it holds, decoded.
func readDumpRecords(t *testing.T, path string) []dumpRecord {
	t.Helper()
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the dump %s back: %v", path, err)
	}
	reader := newDumpReader(bytes.NewReader(fileBytes))
	var records []dumpRecord
	for {
		kind, raw, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return records
		}
		if err != nil {
			t.Fatalf("reading record %d of %s: %v", len(records)+1, path, err)
		}
		record, err := decodeRecord(raw)
		if err != nil {
			t.Fatalf("record %d of %s does not decode: %v", len(records)+1, path, err)
		}
		if record.kindWord() != kind {
			t.Fatalf("record %d of %s reads as kind %q and decodes to %q", len(records)+1, path, kind, record.kindWord())
		}
		records = append(records, record)
	}
}

// requireNoDumpFile asserts that a refused run left nothing behind: no final file and no
// .partial for it.
func requireNoDumpFile(t *testing.T, path string) {
	t.Helper()
	for _, candidate := range []string{path, path + ".partial"} {
		if _, err := os.Lstat(candidate); err == nil {
			t.Errorf("a refused dump left %s behind; a refusal must write no file", candidate)
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("checking whether %s exists: %v", candidate, err)
		}
	}
}

// TestDumpFlagErrorsNameTheFix pins the flag rules of step Da1: a bucket is scoped by
// exactly one database, a database the session cannot see is a refusal that names what
// exists, and neither writes a file (AGENTS.md rule 3, section 3.1).
func TestDumpFlagErrorsNameTheFix(t *testing.T) {
	addr, _ := newRingTestHost(t)
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "never.hoarddump")
	tests := []struct {
		reason   string
		args     []string
		wantExit int
		wantText string
		// wantHelp is true for a usage error: every flag problem ends with the line that
		// names the help.
		wantHelp bool
	}{
		{
			reason:   "a bucket without a database",
			args:     []string{"--bucket", "users", "--out", outPath},
			wantExit: ExitUsage,
			wantText: "--bucket needs exactly one --db. Example: hoarddb dump --db app --bucket users",
			wantHelp: true,
		},
		{
			reason:   "a bucket with two databases",
			args:     []string{"--db", "app", "--db", "other", "--bucket", "users", "--out", outPath},
			wantExit: ExitUsage,
			wantText: "--bucket needs exactly one --db. Example: hoarddb dump --db app --bucket users",
			wantHelp: true,
		},
		{
			reason:   "a database the session cannot see",
			args:     []string{"--db", "nope", "--out", outPath},
			wantExit: ExitRefused,
			wantText: `Database "nope" not found. Run 'show databases' to list what exists.`,
		},
		{
			reason:   "a positional argument",
			args:     []string{"app", "--out", outPath},
			wantExit: ExitUsage,
			wantText: "dump takes no positional arguments",
			wantHelp: true,
		},
	}
	for _, test := range tests {
		t.Run(test.reason, func(t *testing.T) {
			code, stdout, stderr := dumpOnHost(t, addr, test.args...)
			if code != test.wantExit {
				t.Fatalf("%s: exit %d, want %d\nstdout: %s\nstderr: %s", test.reason, code, test.wantExit, stdout, stderr)
			}
			if !strings.Contains(stderr, test.wantText) {
				t.Errorf("%s: the refusal does not say %q:\nstderr: %s", test.reason, test.wantText, stderr)
			}
			if test.wantHelp && !strings.Contains(stderr, "Run 'hoarddb dump --help' for the flags and examples.") {
				t.Errorf("%s: the refusal does not name the help:\nstderr: %s", test.reason, stderr)
			}
			requireNoDumpFile(t, outPath)
		})
	}
}

// TestDumpOfARingWithNoDatabasesIsAnErrorNotAnEmptyFile: a ring that holds no database the
// session may see is refused with the text of section 3.2 item 1, and no empty file is
// written (review F-15).
func TestDumpOfARingWithNoDatabasesIsAnErrorNotAnEmptyFile(t *testing.T) {
	addr, _ := newRingTestHost(t)
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "empty.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--out", outPath)
	if code != ExitRefused {
		t.Fatalf("dumping a ring with no databases: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitRefused, stdout, stderr)
	}
	want := `The ring on this server holds no databases that user "root" may see. Check the user's permissions, or create one with 'create database <name>'.`
	if !strings.Contains(stderr, want) {
		t.Errorf("the refusal does not say %q:\nstderr: %s", want, stderr)
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("listing %s: %v", outDir, err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
		}
		t.Errorf("a refused dump left %v in %s; it must write no file and no .partial", names, outDir)
	}
}

// TestDumpHeaderRecordsTheUser: the first record says which user's view the file holds, so
// a non-root dump is a subset by construction and says so (section 3.2 item 1).
func TestDumpHeaderRecordsTheUser(t *testing.T) {
	addr, client := newRingTestHost(t)
	dumpTestDatabase(t, client, "app")
	outPath := filepath.Join(t.TempDir(), "app.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping one database: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	records := readDumpRecords(t, outPath)
	if len(records) == 0 {
		t.Fatalf("%s holds no record", outPath)
	}
	header, isHeader := records[0].(*dumpHeader)
	if !isHeader {
		t.Fatalf("the first record of %s is a %T, and the first record must be the header", outPath, records[0])
	}
	if header.User != "root" {
		t.Errorf("the header's user is %q, want %q (the session that ran the dump)", header.User, "root")
	}
	if header.Scope != "db:app" {
		t.Errorf("the header's scope is %q, want %q for --db app", header.Scope, "db:app")
	}
	if header.V != dumpFormatVersion {
		t.Errorf("the header's v is %d, want the format version %d", header.V, dumpFormatVersion)
	}
	if header.Tool == "" {
		t.Error("the header's tool is empty; it must say what wrote the file")
	}
}

// TestDumpHeaderSaysNotPointInTime: every dump writes the one consistency word of format
// version 1, and the file passes the whole-file check restore --verify runs (section 3.1,
// D14).
func TestDumpHeaderSaysNotPointInTime(t *testing.T) {
	addr, client := newRingTestHost(t)
	dumpTestDatabase(t, client, "app")
	outPath := filepath.Join(t.TempDir(), "app.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	header, isHeader := readDumpRecords(t, outPath)[0].(*dumpHeader)
	if !isHeader {
		t.Fatalf("the first record of %s is not the header", outPath)
	}
	if header.Consistency != dumpConsistencyWord {
		t.Errorf("the header's consistency is %q, want %q", header.Consistency, dumpConsistencyWord)
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Consistency != dumpConsistencyWord {
		t.Errorf("the verification reads consistency %q, want %q", verification.Consistency, dumpConsistencyWord)
	}
}

// TestDumpPrintsTheNotPointInTimeStatementAtStartAndEnd: the statement goes to stderr
// before the first progress line, and the end line that repeats it is the last line of the
// run (section 3.1, "What the tool prints").
func TestDumpPrintsTheNotPointInTimeStatementAtStartAndEnd(t *testing.T) {
	addr, client := newRingTestHost(t)
	dumpTestDatabase(t, client, "app")
	outPath := filepath.Join(t.TempDir(), "app.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	if stdout != "" {
		t.Errorf("a text run wrote to stdout: %q (progress and the report go to stderr)", stdout)
	}
	start := strings.Index(stderr, dumpStartStatement)
	if start < 0 {
		t.Fatalf("the start of the output does not carry the not-point-in-time statement %q:\nstderr: %s", dumpStartStatement, stderr)
	}
	for _, want := range []string{
		`Dumped what user "root" may see. Databases that user may not see are not in this file.`,
	} {
		at := strings.Index(stderr, want)
		if at < 0 {
			t.Fatalf("the output does not carry %q:\nstderr: %s", want, stderr)
		}
		if at < start {
			t.Errorf("%q came before the start statement; the statement goes first", want)
		}
	}
	done := strings.Index(stderr, "Done: ")
	if done < 0 {
		t.Fatalf("the output has no end line (Done: <path>, <entities> entities, <elements> elements):\nstderr: %s", stderr)
	}
	if done < start {
		t.Errorf("the end line came before the start statement:\nstderr: %s", stderr)
	}
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	if last := lines[len(lines)-1]; !strings.HasPrefix(last, "Done: ") {
		t.Errorf("the last line of the run is %q, and the end line must be last:\nstderr: %s", last, stderr)
	}
	if !strings.Contains(stderr, outPath) {
		t.Errorf("the end line does not name the file %s:\nstderr: %s", outPath, stderr)
	}
}

// TestDumpHelpSaysNotPointInTime: the help text carries the statement and the queue
// sentence, so a reader who never runs a dump is told what the file is not (section 3.1,
// rev 7.1).
func TestDumpHelpSaysNotPointInTime(t *testing.T) {
	code, stdout, stderr := runRingDumpForTest(t, "--help")
	if code != ExitOK {
		t.Fatalf("dump --help: exit %d, want %d\nstderr: %s", code, ExitOK, stderr)
	}
	if stderr != "" {
		t.Errorf("dump --help wrote to stderr: %q (the help is a report and goes to stdout)", stderr)
	}
	for _, want := range []string{
		"Usage: hoarddb dump",
		dumpStartStatement,
		"Queue messages that are claimed or dead-lettered are not included in this dump.",
		// The flags as fs.PrintDefaults prints them, one dash.
		"-out string",
		"-db value",
		"-bucket string",
		"-json",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("dump --help does not say %q:\nstdout: %s", want, stdout)
		}
	}
	for _, banned := range []string{"MongoDB", "MySQL", "mongodump", "mysqldump"} {
		if strings.Contains(stdout, banned) {
			t.Errorf("dump --help names %q; the tool's own output never names another database product:\nstdout: %s", banned, stdout)
		}
	}
}
