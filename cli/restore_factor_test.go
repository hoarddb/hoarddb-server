package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

const factorTestPassword = "factor-test-password"

// newFactorTestServer starts a standalone server and returns a CLI connected
// to it and the flags that point a verb at it.
func newFactorTestServer(t *testing.T) (*CLI, []string) {
	t.Helper()
	_, cfg := startLoopbackService(t, factorTestPassword, nil)
	c := NewCLI(WithAddress(cfg.TCP.Listen), WithInsecureTLS(true), WithCredentials("admin", factorTestPassword), WithTerminal(io.Discard))
	if err := c.Connect(); err != nil {
		t.Fatalf("connect to %s: %v", cfg.TCP.Listen, err)
	}
	t.Cleanup(c.Close)
	return c, []string{"--address", cfg.TCP.Listen, "--insecure", "--user", "admin", "--password", factorTestPassword}
}

// readMetadataAsWritten is a dumped bucket's metadata.json as the generic
// object a reader of the file sees.
func readMetadataAsWritten(t *testing.T, dir, database, bucket string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, database, bucket+".metadata.json"))
	if err != nil {
		t.Fatalf("reading the metadata of %s/%s: %v", database, bucket, err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("parsing the metadata of %s/%s: %v", database, bucket, err)
	}
	return fields
}

// writeMetadataAs writes fields as database/bucket's metadata.json.
func writeMetadataAs(t *testing.T, dir, database, bucket string, fields map[string]any) {
	t.Helper()
	if err := writeJSONFile(filepath.Join(dir, database, bucket+".metadata.json"), fields); err != nil {
		t.Fatalf("writing the metadata of %s/%s: %v", database, bucket, err)
	}
}

// TestDumpWritesTheDeclaredFactorToMetadata: a bucket's metadata.json records
// the replication factor it declared, and nothing for one that inherits
// (BUG-0497).
func TestDumpWritesTheDeclaredFactorToMetadata(t *testing.T) {
	c, _ := newFactorTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	for name, factor := range map[string]int{"declared": 1, "inherits": 0} {
		var created server.CreateBucketResponse
		if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
			Database: "app", Name: name, Type: server.BucketTypeHash, ReplicationFactor: factor,
		}, &created); err != nil || !created.Success {
			t.Fatalf("create bucket %s: %v %s", name, err, created.Message)
		}
	}
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", "", outDir, io.Discard, nil); err != nil || len(summary.failures) != 0 {
		t.Fatalf("dump: %v %v", err, summary)
	}
	if got := readMetadataAsWritten(t, outDir, "app", "declared")["replication_factor"]; got != float64(1) {
		t.Errorf("declared.metadata.json records replication_factor %v, want 1", got)
	}
	if got, present := readMetadataAsWritten(t, outDir, "app", "inherits")["replication_factor"]; present {
		t.Errorf("inherits.metadata.json records replication_factor %v; a bucket that declared none records no field", got)
	}
}

// TestRestoreSummaryCountsBucketsRestoredWithoutTheirFactor is T-59
// (bucket-replication spec §18.9; BUG-0497). Twelve buckets whose dump records
// a factor this standalone server cannot grant are restored at its default.
// The text summary ends with one count, ten names and "and 2 more", on
// stderr; --json carries all twelve; the exit code is the one a clean restore
// has, because every document was restored.
func TestRestoreSummaryCountsBucketsRestoredWithoutTheirFactor(t *testing.T) {
	c, connection := newFactorTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	const buckets = 12
	for n := range buckets {
		if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{
			Database: "app", Name: fmt.Sprintf("b%02d", n), Type: server.BucketTypeHash,
		}, nil); err != nil {
			t.Fatalf("create bucket b%02d: %v", n, err)
		}
	}
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", "", outDir, io.Discard, nil); err != nil || len(summary.failures) != 0 {
		t.Fatalf("dump: %v %v", err, summary)
	}
	// What a dump of a cluster's buckets at replication_factor 3 holds, aimed
	// at new names on this standalone server.
	want := make([]string, 0, buckets)
	for n := range buckets {
		source, target := fmt.Sprintf("b%02d", n), fmt.Sprintf("r%02d", n)
		renameDumpedBucket(t, outDir, "app", source, target)
		fields := readMetadataAsWritten(t, outDir, "app", target)
		fields["replication_factor"] = 3
		writeMetadataAs(t, outDir, "app", target, fields)
		want = append(want, "app/"+target)
	}

	code, _, stderr := captureProcessStreams(t, func() int {
		return RunRestoreCommand(append(append([]string{}, connection...), "--dir", outDir))
	})
	if code != ExitOK {
		t.Fatalf("restore exited %d, want %d: every document was restored\n%s", code, ExitOK, stderr)
	}
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	heading := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "restore: 12 bucket(s) were restored WITHOUT the replication factor their dump records") {
			heading = i
		}
	}
	if heading < 0 {
		t.Fatalf("stderr does not count the twelve buckets restored without their factor:\n%s", stderr)
	}
	block := strings.Join(lines[heading:], "\n")
	if len(lines)-heading > 4 {
		t.Errorf("the factor block is not the last thing restore prints; %d line(s) follow its heading:\n%s", len(lines)-heading-1, block)
	}
	for n := range buckets {
		named := strings.Contains(block, fmt.Sprintf("app.r%02d (3)", n))
		if n < 10 && !named {
			t.Errorf("the block does not name app.r%02d (3):\n%s", n, block)
		}
		if n >= 10 && named {
			t.Errorf("the block names app.r%02d, past the first ten:\n%s", n, block)
		}
	}
	if !strings.Contains(block, "and 2 more") {
		t.Errorf("the block does not say \"and 2 more\":\n%s", block)
	}

	code, stdout, stderr := captureProcessStreams(t, func() int {
		return RunRestoreCommand(append(append([]string{}, connection...), "--json", "--drop", "--dir", outDir))
	})
	if code != ExitOK {
		t.Fatalf("restore --json --drop exited %d, want %d\n%s", code, ExitOK, stderr)
	}
	var printed map[string]any
	if err := json.Unmarshal([]byte(stdout), &printed); err != nil {
		t.Fatalf("restore --json printed %q: %v", stdout, err)
	}
	listed, _ := printed["factor_not_restored"].([]any)
	if len(listed) != buckets {
		t.Fatalf("restore --json lists %d bucket(s) in factor_not_restored, want all %d: %s", len(listed), buckets, stdout)
	}
	for i, name := range listed {
		if name != want[i] {
			t.Errorf("factor_not_restored[%d] = %v, want %s", i, name, want[i])
		}
	}
}

// TestRestoreDryRunSummaryDescribesTheFactorInTheConditional: a dry run
// creates and drops nothing, so its factor blocks say what a restore would
// do, under the dry run's own prefix, and never that a bucket was restored
// or lowered (BUG-0497 verification).
func TestRestoreDryRunSummaryDescribesTheFactorInTheConditional(t *testing.T) {
	summary := newRestoreSummary()
	summary.dryRun = true
	summary.factorNotRestored = []factorNote{{database: "app", bucket: "events", dumped: 3}}
	summary.factorKept = []factorNote{{database: "app", bucket: "orders", dumped: 3, bucketed: 1}}
	summary.factorLowered = []factorNote{{database: "app", bucket: "audit", bucketed: 3}}
	var printed strings.Builder
	printRestoreSummary(&printed, summary)
	text := printed.String()
	for _, past := range []string{"were restored", "kept their own", "had their replication factor LOWERED"} {
		if strings.Contains(text, past) {
			t.Errorf("a dry run's summary says %q, as if it had happened:\n%s", past, text)
		}
	}
	for _, conditional := range []string{
		"restore --dry-run: 1 bucket(s) would be restored without the replication factor their dump records",
		"restore --dry-run: 1 bucket(s) would keep their own replication factor; the dump records another",
		"restore --dry-run: 1 bucket(s) would have their replication factor LOWERED by --drop",
	} {
		if !strings.Contains(text, conditional) {
			t.Errorf("a dry run's summary does not say %q:\n%s", conditional, text)
		}
	}
}
