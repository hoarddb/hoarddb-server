package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestDumpOutputVerifies: the file a ring dump writes is a file `restore --verify` accepts,
// through the command-line route main uses, and the counts the verifier reports are the
// dump's own. The bucket holds a scalar and four store types so the file carries every
// record kind the summary counts.
func TestDumpOutputVerifies(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatalf("creating app on the test host: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Purchases", Type: "fifoStore"},
		{Name: "Photos", Type: "lifoStore"},
		{Name: "Friends", Type: "hashStore"},
		{Name: "Recipes", Type: "btreeStore"},
	}); err != nil {
		t.Fatalf("creating app.User on the test host: %v", err)
	}
	user := client.Bucket("app", "User")
	if _, err := user.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"}); err != nil {
		t.Fatalf("inserting ada into app.User: %v", err)
	}
	for _, product := range []string{"tea", "cake", "jam"} {
		if _, err := user.Push(ctx, "ada", "Purchases", ringclient.Doc{"Product": product}); err != nil {
			t.Fatalf("pushing %q into ada's Purchases: %v", product, err)
		}
	}
	for _, photo := range []string{"beach", "sunset"} {
		if _, err := user.Push(ctx, "ada", "Photos", ringclient.Doc{"Title": photo}); err != nil {
			t.Fatalf("pushing %q into ada's Photos: %v", photo, err)
		}
	}
	for _, friend := range []string{"zoe", "mia"} {
		if _, err := user.Put(ctx, "ada", "Friends", friend, ringclient.Doc{"Name": friend}); err != nil {
			t.Fatalf("putting %q into ada's Friends: %v", friend, err)
		}
	}
	for _, recipe := range []string{"pear", "fig"} {
		if _, err := user.Put(ctx, "ada", "Recipes", recipe, ringclient.Doc{"Title": recipe}); err != nil {
			t.Fatalf("putting %q into ada's Recipes: %v", recipe, err)
		}
	}
	outPath := filepath.Join(t.TempDir(), "app.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	var summary *dumpSummaryRecord
	for _, record := range readDumpRecords(t, outPath) {
		if candidate, isSummary := record.(*dumpSummaryRecord); isSummary {
			summary = candidate
		}
	}
	if summary == nil {
		t.Fatalf("the dump %s holds no summary record", outPath)
	}
	wantDone := fmt.Sprintf("Done: %s, %d entities, %d elements.", outPath, summary.Entities, summary.Elements)
	if !strings.Contains(stderr, wantDone) {
		t.Errorf("the dump's end line does not carry its own counts; want it to hold %q:\nstderr: %s", wantDone, stderr)
	}
	if summary.Databases != 1 || summary.Buckets != 1 || summary.Entities != 1 || summary.Elements != 9 {
		t.Fatalf("the dump counted databases %d, buckets %d, entities %d, elements %d; want 1, 1, 1, 9",
			summary.Databases, summary.Buckets, summary.Entities, summary.Elements)
	}

	verifyCode, verifyStdout, verifyStderr := captureProcessStreams(t, func() int {
		return verbRunners["restore"]([]string{outPath, "--verify"})
	})
	if verifyCode != ExitOK {
		t.Fatalf("restore --verify exited %d, want %d\nstdout: %s\nstderr: %s", verifyCode, ExitOK, verifyStdout, verifyStderr)
	}
	lines := strings.Split(strings.TrimRight(verifyStdout, "\n"), "\n")
	if len(lines) == 0 || lines[0] != restoreConsistencyStatement {
		t.Fatalf("restore --verify printed %q first, want the dump's consistency statement %q", verifyStdout, restoreConsistencyStatement)
	}
	wantVerified := fmt.Sprintf("Verified %s: databases %d, buckets %d, entities %d, elements %d",
		outPath, summary.Databases, summary.Buckets, summary.Entities, summary.Elements)
	if last := lines[len(lines)-1]; last != wantVerified {
		t.Errorf("restore --verify printed %q; the dump's own counts give %q", last, wantVerified)
	}
}
