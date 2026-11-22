package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestDryRunPrintsDeclarationAndCost: a dry run says how many documents it read and would
// write, the declaration it would create one field per line, and what a declaration costs the
// ring, and it creates nothing.
func TestDryRunPrintsDeclarationAndCost(t *testing.T) {
	addr, _ := newRingTestHost(t)
	path := writeBSONFile(t, "users.bson",
		bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}, {Key: "age", Value: int32(3)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "name", Value: "Bo"}, {Key: "age", Value: "unknown"}},
	)
	code, stdout, stderr := ingestOnHost(t, addr, path, "--into", "app.users", "--dry-run")
	if code != ExitOK {
		t.Fatalf("exit %d:\n%s", code, stderr)
	}
	for _, want := range []string{
		"2 read, would write 2",
		"would create database app",
		"would create bucket app.users with 2 fields:",
		"    name string",
		"    age any",
		`Field "age" holds int and string values, so it is declared any.`,
		"8 keys and 436 bytes plus about 26 bytes per field",
		"computed, not measured",
		"nothing was written",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the dry-run report does not say %q:\n%s", want, stderr)
		}
	}
	if stdout != "" {
		t.Errorf("a dry run without --json wrote to stdout: %q", stdout)
	}
}

// TestJSONReportHasDocumentsSkippedAndDeclaration: --json prints one object on stdout with the
// counts, the declaration and the skipped documents by position, and never their content.
func TestJSONReportHasDocumentsSkippedAndDeclaration(t *testing.T) {
	addr, _ := newRingTestHost(t)
	path := writeTextFile(t, "users.jsonl", "{\"_id\":\"a\",\"name\":\"Ann\",\"age\":3}\nnot json at all\n{\"_id\":\"c\",\"name\":\"Cy\",\"age\":5}\n")
	code, stdout, stderr := ingestOnHost(t, addr, path, "--into", "app.users", "--on-error", "skip", "--json")
	if code != ExitPartial {
		t.Fatalf("exit %d, want %d:\n%s", code, ExitPartial, stderr)
	}
	if strings.Count(strings.TrimSpace(stdout), "\n") != 0 {
		t.Fatalf("stdout should be exactly one JSON object, got:\n%s", stdout)
	}
	var report struct {
		OK               bool   `json:"ok"`
		Partial          bool   `json:"partial"`
		DryRun           bool   `json:"dry_run"`
		Input            string `json:"input"`
		Database         string `json:"database"`
		Bucket           string `json:"bucket"`
		DocumentsRead    int64  `json:"documents_read"`
		DocumentsWritten int64  `json:"documents_written"`
		DocumentsSkipped int64  `json:"documents_skipped"`
		CreatedDatabase  bool   `json:"created_database"`
		CreatedBucket    bool   `json:"created_bucket"`
		Declaration      []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"declaration"`
		Errors []struct {
			Document int64  `json:"document"`
			Reason   string `json:"reason"`
		} `json:"errors"`
		Warnings       []string `json:"warnings"`
		StoppedBecause string   `json:"stopped_because"`
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("stdout is not one JSON object: %v\n%s", err, stdout)
	}
	if report.OK || !report.Partial || report.DryRun {
		t.Errorf("ok %v, partial %v, dry_run %v; want false, true, false for a run that skipped a document", report.OK, report.Partial, report.DryRun)
	}
	if report.Database != "app" || report.Bucket != "users" || report.Input != "users.jsonl" {
		t.Errorf("target %s.%s from %s, want app.users from users.jsonl", report.Database, report.Bucket, report.Input)
	}
	if report.DocumentsRead != 3 || report.DocumentsWritten != 2 || report.DocumentsSkipped != 1 {
		t.Errorf("read %d, written %d, skipped %d; want 3, 2, 1", report.DocumentsRead, report.DocumentsWritten, report.DocumentsSkipped)
	}
	if !report.CreatedDatabase || !report.CreatedBucket {
		t.Errorf("created_database %v and created_bucket %v, want both true", report.CreatedDatabase, report.CreatedBucket)
	}
	if len(report.Declaration) != 2 || report.Declaration[0].Name != "name" || report.Declaration[0].Type != "string" || report.Declaration[1].Name != "age" || report.Declaration[1].Type != "int" {
		t.Errorf("declaration %+v, want name string and age int", report.Declaration)
	}
	if len(report.Errors) != 1 || report.Errors[0].Document != 2 {
		t.Errorf("errors %+v, want one, for document 2", report.Errors)
	}
	if strings.Contains(stdout, "not json at all") {
		t.Errorf("the report quotes the content of a skipped document: %s", stdout)
	}
	if report.Warnings == nil {
		t.Errorf("warnings is null, want an empty list so a script can iterate it")
	}
}

// TestJSONReportOfARefusalSaysWhyItStopped: a refusal before anything is written still prints
// the object, so a script reading stdout always gets one.
func TestJSONReportOfARefusalSaysWhyItStopped(t *testing.T) {
	addr, _ := newRingTestHost(t)
	path := writeBSONFile(t, "users.bson", bson.D{{Key: "_id", Value: "a"}})
	code, stdout, _ := ingestOnHost(t, addr, path, "--into", "app.absent", "--no-create", "--json")
	if code != ExitRefused {
		t.Fatalf("exit %d, want %d", code, ExitRefused)
	}
	var report struct {
		OK             bool   `json:"ok"`
		StoppedBecause string `json:"stopped_because"`
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("stdout is not one JSON object: %v\n%s", err, stdout)
	}
	if report.OK || !strings.Contains(report.StoppedBecause, "Database \"app\" not found") {
		t.Errorf("report %+v, want ok false and the refusal's words in stopped_because", report)
	}
}

// TestJSONReportOfAnEarlyRefusalNamesTheFileLikeTheOtherPaths: input is the file's base name and
// format is the one its name or --format gives, as in the reports of a run that got further.
func TestJSONReportOfAnEarlyRefusalNamesTheFileLikeTheOtherPaths(t *testing.T) {
	path := writeBSONFile(t, "users.bson", bson.D{{Key: "_id", Value: "a"}})
	_, stdout, _ := runRingIngestForTest(t, path, "--json")
	var report struct {
		Input  string `json:"input"`
		Format string `json:"format"`
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("stdout is not one JSON object: %v\n%s", err, stdout)
	}
	if report.Input != "users.bson" || report.Format != "bson" {
		t.Errorf("input %q and format %q, want users.bson and bson", report.Input, report.Format)
	}
}

// TestDryRunOnAnExistingBucketReportsItsDeclaration: the report names the fields the bucket
// has, not an empty list, and the text says the bucket exists.
func TestDryRunOnAnExistingBucketReportsItsDeclaration(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "users", []ringclient.FieldDecl{{Name: "name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	path := writeBSONFile(t, "users.bson", bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}})
	code, stdout, stderr := ingestOnHost(t, addr, path, "--into", "app.users", "--dry-run", "--json")
	if code != ExitOK {
		t.Fatalf("exit %d:\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "bucket app.users exists and keeps its declaration") || !strings.Contains(stderr, "name string") {
		t.Errorf("the dry run should say the bucket exists and show its fields:\n%s", stderr)
	}
	if !strings.Contains(stdout, `"declaration":[{"name":"name","type":"string"}]`) {
		t.Errorf("the report's declaration should be the existing one:\n%s", stdout)
	}
}
