package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// writeDumpDirWithStores lays out a dump directory whose buckets declare
// store fields, the way writeDumpDir lays out one that does not.
func writeDumpDirWithStores(t *testing.T, bucketStores map[string][]server.DumpStore) string {
	t.Helper()
	dir := t.TempDir()
	manifestRaw, err := json.Marshal(topLevelManifest{FormatVersion: dumpManifestFormatVersionCLI, Databases: []string{"app"}, Buckets: len(bucketStores)})
	if err != nil {
		t.Fatalf("marshalling manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifestRaw, 0o644); err != nil {
		t.Fatalf("writing manifest.json: %v", err)
	}
	dbDir := filepath.Join(dir, "app")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dbDir, err)
	}
	for bucket, stores := range bucketStores {
		metaRaw, err := json.Marshal(bucketMetadataFile{
			FormatVersion: dumpManifestFormatVersionCLI, Database: "app", Bucket: bucket, StoreType: "hash", Stores: stores,
		})
		if err != nil {
			t.Fatalf("marshalling metadata for %s: %v", bucket, err)
		}
		if err := os.WriteFile(filepath.Join(dbDir, bucket+".metadata.json"), metaRaw, 0o644); err != nil {
			t.Fatalf("writing metadata for %s: %v", bucket, err)
		}
	}
	return dir
}

// TestBlobBucketsMissingPayloadsNamesBlobFields checks the warning half:
// a hash bucket that declares a blobStore field loses that field's elements
// on restore, so the scan the dump and restore warnings are built from must
// flag it, name the field, and leave buckets whose fields restore alone.
func TestBlobBucketsMissingPayloadsNamesBlobFields(t *testing.T) {
	dir := writeDumpDirWithStores(t, map[string][]server.DumpStore{
		"Post":    {{Name: "Photos", Engine: "blob"}, {Name: "Tags", Engine: "hash"}},
		"Thread":  {{Name: "Comments", Engine: "fifo"}, {Name: "Flags", Engine: "hash", Parent: "Comments"}},
		"Gallery": {{Name: "Albums", Engine: "btree"}, {Name: "Scans", Engine: "blob", Parent: "Albums"}, {Name: "Covers", Engine: "blob"}},
	})
	got, err := blobBucketsMissingPayloads(dir)
	if err != nil {
		t.Fatalf("blobBucketsMissingPayloads(%q): %v", dir, err)
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{"app/Post", "Photos", "app/Gallery", "Covers", "Scans"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the scan does not mention %q.\ngot: %v", want, got)
		}
	}
	for _, unwanted := range []string{"Thread", "Tags", "Comments"} {
		if strings.Contains(joined, unwanted) {
			t.Errorf("the scan flagged %q, whose fields restore.\ngot: %v", unwanted, got)
		}
	}
}

// seedBlobFieldBucket creates app.Post with a blob field and a hash field,
// two elements in each, on a live server.
func seedBlobFieldBucket(t *testing.T, c *CLI) {
	t.Helper()
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	var created server.CreateBucketResponse
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{
			map[string]interface{}{"name": "Photos", "engine": "blobStore"},
			map[string]interface{}{"name": "Tags", "engine": "hashStore"},
		},
	}}, &created)
	if !created.Success {
		t.Fatalf("creating Post: %s", created.Message)
	}
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)
	for _, element := range []string{"one", "two"} {
		mustSend(t, c, transport.OpFieldPut, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Photos", Element: element, Content: []byte("bytes " + element), Data: map[string]interface{}{"name": element}}, nil)
		mustSend(t, c, transport.OpFieldPut, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: element, Data: map[string]interface{}{"n": 1}}, nil)
	}
}

// TestRestoreOfABlobFieldWarnsAndCountsTheSkippedElements drives a real dump
// and restore of a bucket with a blob field: the warning must come before the
// restore touches anything, and the closing summary must say how many
// elements were left behind, not only the per-bucket progress line.
func TestRestoreOfABlobFieldWarnsAndCountsTheSkippedElements(t *testing.T) {
	source := newDumpTestServer(t)
	seedBlobFieldBucket(t, source)
	outDir := filepath.Join(t.TempDir(), "dump")
	var dumpProgress bytes.Buffer
	dumped, err := runDumpBuckets(source.handler, "app", "Post", outDir, &dumpProgress, nil)
	if err != nil || dumped.incomplete {
		t.Fatalf("dumping Post: %+v, %v", dumped, err)
	}
	if !strings.Contains(dumpProgress.String(), "Photos") || !summaryHasWarningNaming(dumped.warnings, "Photos") {
		t.Errorf("dump did not warn about the blob field Photos.\nprogress: %s\nwarnings: %v", dumpProgress.String(), dumped.warnings)
	}

	target := newDumpTestServer(t)
	var progress bytes.Buffer
	restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, &progress, nil)
	if err != nil || len(restored.failures) != 0 {
		t.Fatalf("restoring Post: %+v, %v", restored, err)
	}
	if !summaryHasWarningNaming(restored.warnings, "Photos") {
		t.Errorf("restore did not warn about the blob field Photos.\nwarnings: %v", restored.warnings)
	}
	var closing bytes.Buffer
	printRestoreSummary(&closing, restored)
	if !strings.Contains(closing.String(), "2 store-field element(s) skipped") {
		t.Errorf("the closing summary does not report the 2 skipped blob-field elements.\ngot: %s", closing.String())
	}
	resultJSON, err := json.Marshal(restored.result(restored.exitCode()))
	if err != nil {
		t.Fatalf("marshalling the --json result: %v", err)
	}
	if !strings.Contains(string(resultJSON), `"elements_skipped":2`) {
		t.Errorf("the --json result does not carry elements_skipped:2.\ngot: %s", resultJSON)
	}
}

// TestRestoreThatSkippedDataExitsPartial pins the owner's ruling:
// a restore that left documents or store-field elements behind is partial,
// not clean, for blob buckets and blob fields alike.
func TestRestoreThatSkippedDataExitsPartial(t *testing.T) {
	for _, tc := range []struct {
		name    string
		summary *restoreSummary
		want    int
	}{
		{name: "nothing skipped", summary: &restoreSummary{buckets: 1}, want: ExitOK},
		{name: "blob bucket documents skipped", summary: &restoreSummary{buckets: 1, documentsSkipped: 3}, want: ExitPartial},
		{name: "blob field elements skipped", summary: &restoreSummary{buckets: 1, elementsSkipped: 2}, want: ExitPartial},
		{name: "interrupted still wins", summary: &restoreSummary{buckets: 1, elementsSkipped: 2, interrupted: true}, want: ExitInterrupted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.summary.exitCode(); got != tc.want {
				t.Fatalf("exitCode() = %d, want %d", got, tc.want)
			}
		})
	}
}
