package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/version"
)

// Golden transcripts (GAP-24 Part 1). Each file under testdata/golden/ is what
// the REPL prints today for one route, recorded against a real in-process
// server, so a later refactor that changes user-visible text fails a test.
//
// A golden is rewritten only when HOARDDB_UPDATE_GOLDENS=1 is set, so a plain
// `go test` can never edit one to make itself pass. Review the diff of every
// rewrite: a change outside the routes the build doc lists for Part 6 means a
// print site moved that should not have.

const (
	goldenDir       = "testdata/golden"
	goldenUpdateEnv = "HOARDDB_UPDATE_GOLDENS"

	// goldenPassword is the root password of every golden server. It is a test
	// credential that must never reach a golden file
	// (TestGoldensContainNoTestCredentials greps for it).
	goldenPassword = "golden-e2e-password"

	// goldenUserPassword is the password `create user` and `alter user ...
	// password` read from a file in the rows. It must not reach a golden either.
	goldenUserPassword = "golden-user-password-1"

	// goldenDiskUsedPct is the disk usage every golden server reports. The
	// probe is pinned because the data directory is t.TempDir() on the test
	// host's filesystem, and the status disk row and Healthy must not follow it.
	goldenDiskUsedPct = 1
)

// goldenSession is one in-process server and a CLI connected to it, with
// everything the CLI prints captured in out.
type goldenSession struct {
	cli     *CLI
	out     *bytes.Buffer
	addr    string
	dataDir string
	// outDir is a scratch directory for dump and restore statements; rows
	// name it as {OUT} and the normaliser prints it back as <OUT-DIR>.
	outDir string
	svc    *server.Service
}

// newGoldenSession starts a real server (TCP+TLS, on disk) and returns a CLI
// whose terminal and handler terminal are one buffer. It repeats
// newDumpTestServer's start-up rather than wrapping it because the
// normaliser needs the server's data directory and address, which
// newDumpTestServer does not return.
func newGoldenSession(t *testing.T, extra ...CLIOption) *goldenSession {
	t.Helper()
	return newGoldenSessionWith(t, nil, extra...)
}

// newGoldenSessionWith is newGoldenSession with the server's configuration
// edited before it starts, so a row can state a threshold instead of
// inheriting the harness default.
func newGoldenSessionWith(t *testing.T, edit func(*config.Config), extra ...CLIOption) *goldenSession {
	t.Helper()

	// The server's real statfs reads the test host's filesystem. Pin it, so
	// the status disk row's ratio, state and Healthy are the same on a full
	// host as on an empty one (BUG-0483).
	restoreDisk := server.PinDiskUsageForTesting(goldenDiskUsedPct)
	t.Cleanup(restoreDisk)

	svc, cfg := startLoopbackService(t, goldenPassword, edit)

	out := &bytes.Buffer{}
	options := append([]CLIOption{
		WithAddress(cfg.TCP.Listen),
		WithInsecureTLS(true),
		WithCredentials("admin", goldenPassword),
		WithTerminal(out),
		WithTheme("plain"),
	}, extra...)
	c := NewCLI(options...)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect to %s: %v", cfg.TCP.Listen, err)
	}
	t.Cleanup(c.Close)
	// User statements read the new password from a 0600 file: without a source
	// the CLI prompts on the process's own stdin, which a test cannot answer.
	passwordFile := filepath.Join(t.TempDir(), "new-user-password")
	if err := os.WriteFile(passwordFile, []byte(goldenUserPassword+"\n"), 0o600); err != nil {
		t.Fatalf("writing the new-user password file: %v", err)
	}
	c.handler.SetPasswordSource(passwordFile, false)
	out.Reset()
	return &goldenSession{cli: c, out: out, addr: cfg.TCP.Listen, dataDir: cfg.Storage.DataDir, outDir: t.TempDir(), svc: svc}
}

// runStatements feeds each line to c.handle and returns what the session
// printed. Every statement is echoed as "> <line>" so the golden reads as a
// transcript. A returned error is appended as "Error: <err>", the line
// CLI.Run prints for it; `exit` prints "Goodbye." and stops, as Run does.
func runStatements(t *testing.T, c *CLI, out *bytes.Buffer, lines ...string) string {
	t.Helper()
	var transcript strings.Builder
	var held heldHandles
	for _, line := range lines {
		out.Reset()
		transcript.WriteString("> " + line + "\n")
		err := c.handle(held.expand(line))
		held.observe(out.String())
		if errors.Is(err, ErrExit) {
			transcript.WriteString(out.String() + "Goodbye.\n")
			break
		}
		transcript.WriteString(out.String())
		if err != nil {
			fmt.Fprintf(&transcript, "Error: %v\n", err)
		}
	}
	return transcript.String()
}

// heldHandles are the lease id and item ids the last claim printed, and the
// dead ids the last dead() listed, so a golden row can acknowledge what it
// claimed and requeue what died. A real id differs on every run and cannot be
// written into a row, so a statement names it as {LEASE}, {ITEM1}, {ITEM2} or
// {DEAD1}, {DEAD2} and so on; the transcript keeps the placeholder in the echoed
// statement and the normaliser hides the real values in the output.
type heldHandles struct {
	leaseID string
	itemIDs []string
	deadIDs []string
}

// deadIDPattern is the id dead() prints on each line.
var deadIDPattern = regexp.MustCompile(`"dead_id": "([0-9a-f]+)"`)

// leaseLinePattern is the line claim prints after its entries.
var leaseLinePattern = regexp.MustCompile(`(?m)^lease_id ([0-9a-f]{40}) for \S+; item_ids \[([^\]]*)\]`)

// observe remembers the handles in output when it holds a claim's lease line.
func (h *heldHandles) observe(output string) {
	if listed := deadIDPattern.FindAllStringSubmatch(output, -1); listed != nil {
		h.deadIDs = h.deadIDs[:0]
		for _, parts := range listed {
			h.deadIDs = append(h.deadIDs, parts[1])
		}
	}
	parts := leaseLinePattern.FindStringSubmatch(output)
	if parts == nil {
		return
	}
	h.leaseID = parts[1]
	h.itemIDs = h.itemIDs[:0]
	for _, quoted := range strings.Split(parts[2], ", ") {
		h.itemIDs = append(h.itemIDs, strings.Trim(quoted, `"`))
	}
}

// expand puts the held handles in place of the placeholders in a statement.
func (h *heldHandles) expand(line string) string {
	if !strings.Contains(line, "{LEASE}") && !strings.Contains(line, "{ITEM") && !strings.Contains(line, "{DEAD") {
		return line
	}
	line = strings.ReplaceAll(line, "{LEASE}", h.leaseID)
	for index, itemID := range h.itemIDs {
		line = strings.ReplaceAll(line, "{ITEM"+strconv.Itoa(index+1)+"}", itemID)
	}
	for index, deadID := range h.deadIDs {
		line = strings.ReplaceAll(line, "{DEAD"+strconv.Itoa(index+1)+"}", deadID)
	}
	return line
}

// runQuiet runs setup statements whose output the test does not record, and
// fails the test if one of them errors.
func runQuiet(t *testing.T, s *goldenSession, lines ...string) {
	t.Helper()
	for _, line := range lines {
		s.out.Reset()
		if err := s.cli.handle(line); err != nil {
			t.Fatalf("fixture statement %q failed: %v", line, err)
		}
	}
	s.out.Reset()
}

// goldenFixture is the state every route row starts from: database app, one
// bucket of each type, a nested bucket with two field stores, and one
// document (or element) in each so the reads have something to print.
var goldenFixture = []string{
	"create database app",
	"use app",
	"create bucket Users { type: hash }",
	"create bucket Events { type: btree }",
	"create bucket Jobs { type: fifo }",
	"create bucket Tasks { type: heap }",
	"create bucket Files { type: blob }",
	"create bucket Post { type: hash, Comments fifoStore, Tags hashStore }",
	"create bucket Emails { type: hash, placement: whole, Email string unique }",
	`db.Users.insert({"_id_": "u1", "n": 1})`,
	`db.Users.insert({"_id_": "u2", "n": 9})`,
	`db.Events.insert({"_id_": "e5", "kind": "click"})`,
	`db.Jobs.push({"task": "x"})`,
	`db.Tasks.push({"task": "t"})`,
	`db.Files.put({"_id_": "f1", "data": "hi"})`,
	`db.Post.insert({"_id_": "p1", "title": "hello"})`,
	`db.Post["p1"].Comments.push({"by": "ada"})`,
	`db.Post["p1"].Tags.put("t1", {"n": 1})`,
	`db.Emails.insert({"_id_": "e1", "Email": "a@example.com"})`,
}

// newFixtureSession is a golden session with goldenFixture applied.
func newFixtureSession(t *testing.T, extra ...CLIOption) *goldenSession {
	t.Helper()
	s := newGoldenSession(t, extra...)
	runQuiet(t, s, goldenFixture...)
	return s
}

// The normaliser replaces exactly the values that differ between runs, in a
// fixed order, and nothing else. Every substitution is listed here so an
// over-eager rule cannot hide a regression:
//
//  1. the server's temp data directory              -> <DATA-DIR>
//     and the dump/restore scratch directory        -> <OUT-DIR>
//  2. the server's listen address 127.0.0.1:<port>  -> <ADDR>
//     2a. the sha256 digest a dump prints for a bucket -> sha256:<DIGEST>
//     (buckets whose documents carry a generated id hash differently on
//     every run; the "sha256:" prefix is kept so the format is still pinned)
//  3. generated ids (UUIDs: auto _id_, queue and blob ids) -> <ID>
//  4. cluster token values after "Cluster Token: ", "Old token: " and
//     "New token: " (64 hex digits)                 -> <TOKEN>
//  5. "Uptime:" durations                           -> <UPTIME>
//  6. the capacity ratio of each resource in `status`
//     (host disk usage, descriptors, mappings, memory) -> <RATIO>
//     (the disk reading itself is pinned by the server probe seam,
//     server.PinDiskUsageForTesting, so the disk row's state and the rest of
//     the transcript are the same whatever the host filesystem holds)
//  7. RFC 3339 timestamps, and the token file's
//     "2006-01-02 15:04:05 MST" timestamps          -> <TIMESTAMP>
//  8. the build's version.Version, Commit and BuildDate
//     (only in the forms the CLI prints them)       -> <VERSION>, <COMMIT>, <BUILD-DATE>
//     8a. a lease id (40 hex digits), a quoted item or dead id (16 or more hex
//     digits) and a UTC timestamp a lease verb prints -> <LEASE-ID>, "<HANDLE>", <TIMESTAMP>
//     (a row names the real ones as {LEASE} and {ITEM1}; see heldHandles)
//  9. in `status`, the memtable a fresh store chose from the test host's memory,
//     that memory and the batch bound it gives      -> <MEMTABLE>, <MEMORY>, <BATCH>
//     (only the "chosen at first start" form; the cli package's servers size
//     themselves from the host they run on)
//
// Nothing else is touched. In particular the byte sizes of `describe bucket`
// are not normalised: if they turn out to vary between runs the golden test
// fails, and the row is then changed rather than hidden behind a blanket rule.
var (
	dumpDigest       = regexp.MustCompile(`sha256:[0-9a-f]{64}`)
	generatedID      = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	clusterTokenLine = regexp.MustCompile(`((?:Cluster Token|Old token|New token): )[0-9a-f]{64}`)
	uptimeValue      = regexp.MustCompile(`(?m)^(\s*Uptime:\s+).*$`)
	capacityRatio    = regexp.MustCompile(`(?m)^(\s+(?:fds|mappings|memory|disk)\s+)\d+\.\d+`)
	userTimestamp    = regexp.MustCompile(`(?m)^((?:created|password changed): ).*$`)
	hostMemtable     = regexp.MustCompile(`(?m)^(Memtable:\s+)\d+ MiB  chosen at first start from \d+\.\d+ GiB memory; largest atomic batch \d+ MB$`)
	tokenTimestamp   = regexp.MustCompile(`(?m)^((?:Created|Expires):\s+)\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [A-Z]{2,5}$`)
	leaseIDRun       = regexp.MustCompile(`\b[0-9a-f]{40}\b`)
	quotedHandle     = regexp.MustCompile(`"[0-9a-f]{16,}"`)
	leaseDeadline    = regexp.MustCompile(`((?:until |"dead_at": ")\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`)
	shortLeaseID     = regexp.MustCompile(`(lease )[0-9a-f]{4}…[0-9a-f]{2}\b`)
)

// normaliser carries the per-session values it replaces.
type normaliser struct {
	dataDir string
	outDir  string
	addr    string
	// deadAddr is an address nothing listens on, when a row dials one; it is
	// a free port picked per run, so it prints as <DEAD-ADDR>.
	deadAddr string
}

func (s *goldenSession) normaliser() *normaliser {
	return &normaliser{dataDir: s.dataDir, outDir: s.outDir, addr: s.addr}
}

func (n *normaliser) apply(transcript string) string {
	transcript = strings.ReplaceAll(transcript, n.dataDir, "<DATA-DIR>")
	transcript = strings.ReplaceAll(transcript, n.outDir, "<OUT-DIR>")
	transcript = strings.ReplaceAll(transcript, n.addr, "<ADDR>")
	if n.deadAddr != "" {
		transcript = strings.ReplaceAll(transcript, n.deadAddr, "<DEAD-ADDR>")
	}
	transcript = dumpDigest.ReplaceAllString(transcript, "sha256:<DIGEST>")
	transcript = generatedID.ReplaceAllString(transcript, "<ID>")
	transcript = clusterTokenLine.ReplaceAllString(transcript, "${1}<TOKEN>")
	transcript = uptimeValue.ReplaceAllString(transcript, "${1}<UPTIME>")
	transcript = capacityRatio.ReplaceAllString(transcript, "${1}<RATIO>")
	transcript = userTimestamp.ReplaceAllString(transcript, "${1}<TIMESTAMP>")
	transcript = tokenTimestamp.ReplaceAllString(transcript, "${1}<TIMESTAMP>")
	transcript = leaseIDRun.ReplaceAllString(transcript, "<LEASE-ID>")
	transcript = quotedHandle.ReplaceAllString(transcript, `"<HANDLE>"`)
	transcript = leaseDeadline.ReplaceAllStringFunc(transcript, func(match string) string {
		return match[:strings.IndexAny(match, "0123456789")] + "<TIMESTAMP>"
	})
	transcript = shortLeaseID.ReplaceAllString(transcript, "${1}<LEASE-ID>")
	transcript = hostMemtable.ReplaceAllString(transcript, "${1}<MEMTABLE> MiB  chosen at first start from <MEMORY> memory; largest atomic batch <BATCH> MB")
	transcript = strings.ReplaceAll(transcript, "version="+version.Version+" commit="+version.Commit+" build_date="+version.BuildDate,
		"version=<VERSION> commit=<COMMIT> build_date=<BUILD-DATE>")
	transcript = strings.ReplaceAll(transcript, "hoardDB CLI "+version.Version, "hoardDB CLI <VERSION>")
	return transcript
}

// assertGolden compares transcript with testdata/golden/<slug>.golden, or
// rewrites the file when HOARDDB_UPDATE_GOLDENS=1.
func assertGolden(t *testing.T, slug, transcript string) {
	t.Helper()
	path := filepath.Join(goldenDir, slug+".golden")
	if os.Getenv(goldenUpdateEnv) == "1" {
		if err := os.MkdirAll(goldenDir, 0o755); err != nil {
			t.Fatalf("creating %s: %v", goldenDir, err)
		}
		if err := os.WriteFile(path, []byte(transcript), 0o644); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v\nrecord it with %s=1 go test ./cli/ -run TestREPLTranscriptUnchanged", path, err, goldenUpdateEnv)
	}
	if got := transcript; got != string(want) {
		t.Errorf("%s changed.\n--- golden ---\n%s\n--- now ---\n%s", path, want, got)
	}
}

// extraGoldenSlugs are the goldens that are not REPL rows (the verbs'
// summaries and exit codes), so the orphan check below knows them.
var extraGoldenSlugs = []string{"verb-dump", "verb-restore", "piped-stdin-stops-at-failure", "piped-stdin-completes", "piped-stdin-ends-without-exit"}

// TestGoldenFilesHaveOwners fails on a golden nothing records any more: a
// route whose row was deleted leaves a stale file that would keep passing
// review while pinning nothing.
func TestGoldenFilesHaveOwners(t *testing.T) {
	owned := map[string]bool{}
	for _, row := range goldenRows {
		owned[row.slug+".golden"] = true
	}
	for _, slug := range extraGoldenSlugs {
		owned[slug+".golden"] = true
	}
	entries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatalf("reading %s: %v", goldenDir, err)
	}
	for _, entry := range entries {
		if !owned[entry.Name()] {
			t.Errorf("%s/%s has no row or verb test recording it; delete it or restore the row", goldenDir, entry.Name())
		}
	}
	for name := range owned {
		if _, err := os.Stat(filepath.Join(goldenDir, name)); err != nil {
			t.Errorf("%s/%s is recorded by a test but missing on disk: %v", goldenDir, name, err)
		}
	}
}

// hexRun is a 64-digit hex string: the shape of a cluster token, a bearer
// token or a fingerprint digest. None may appear in a golden.
var hexRun = regexp.MustCompile(`[0-9a-f]{64}`)

// TestGoldensContainNoTestCredentials greps every golden for the credentials
// the golden servers use, and for anything shaped like a token, because a
// golden is committed and read by people who never ran the test. The dump
// digests are the one place a 64-digit hex string is legitimate, and the
// normaliser has already replaced them with sha256:<DIGEST>.
func TestGoldensContainNoTestCredentials(t *testing.T) {
	forbidden := []string{
		goldenPassword, goldenUserPassword, "dump-e2e-password", "ingest-e2e-password", "wrong-password-golden",
	}
	entries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatalf("reading %s: %v", goldenDir, err)
	}
	if len(entries) == 0 {
		t.Fatalf("%s is empty: the guard would pass vacuously", goldenDir)
	}
	for _, entry := range entries {
		content, err := os.ReadFile(filepath.Join(goldenDir, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		for _, secret := range forbidden {
			if strings.Contains(string(content), secret) {
				t.Errorf("%s contains the test credential %q", entry.Name(), secret)
			}
		}
		if match := hexRun.Find(content); match != nil {
			t.Errorf("%s contains a 64-digit hex string (%s...), the shape of a token or key", entry.Name(), match[:8])
		}
	}
}
