package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeDumpDir lays out a dump directory the way runDumpBuckets writes one:
// manifest.json at the root and <database>/<bucket>.metadata.json per bucket.
// The .bson files are deliberately absent — every assertion here is about what
// the metadata says was captured, which is the only thing the blob-payload
// warning reads.
func writeDumpDir(t *testing.T, storeTypes map[string]string) string {
	t.Helper()
	dir := t.TempDir()

	manifest := topLevelManifest{
		FormatVersion: dumpManifestFormatVersionCLI,
		Databases:     []string{"app"},
		Buckets:       len(storeTypes),
	}
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshalling manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifestRaw, 0o644); err != nil {
		t.Fatalf("writing manifest.json: %v", err)
	}

	for scope, storeType := range storeTypes {
		database, bucket, ok := strings.Cut(scope, "/")
		if !ok {
			t.Fatalf("scope %q is not database/bucket", scope)
		}
		dbDir := filepath.Join(dir, database)
		if err := os.MkdirAll(dbDir, 0o755); err != nil {
			t.Fatalf("creating %s: %v", dbDir, err)
		}
		metaRaw, err := json.Marshal(bucketMetadataFile{
			FormatVersion: dumpManifestFormatVersionCLI,
			Database:      database,
			Bucket:        bucket,
			StoreType:     storeType,
			BlobsIncluded: false,
		})
		if err != nil {
			t.Fatalf("marshalling metadata for %s: %v", scope, err)
		}
		if err := os.WriteFile(filepath.Join(dbDir, bucket+".metadata.json"), metaRaw, 0o644); err != nil {
			t.Fatalf("writing metadata for %s: %v", scope, err)
		}
	}
	return dir
}

// TestBlobBucketsMissingPayloadsNamesEveryBlobBucket is GAP-28's first half:
// the scan that the warning is built from must find every blob bucket in a
// dump and nothing else, so the warning can name them rather than saying
// "some buckets".
func TestBlobBucketsMissingPayloadsNamesEveryBlobBucket(t *testing.T) {
	for _, tc := range []struct {
		name       string
		storeTypes map[string]string
		want       []string
	}{
		{
			name:       "no blob bucket",
			storeTypes: map[string]string{"app/users": "hash", "app/events": "fifo"},
			want:       nil,
		},
		{
			name:       "one blob bucket among others",
			storeTypes: map[string]string{"app/users": "hash", "app/files": "blob"},
			want:       []string{"app/files"},
		},
		{
			name:       "every blob bucket is named, sorted",
			storeTypes: map[string]string{"app/uploads": "blob", "app/files": "blob", "app/users": "btree"},
			want:       []string{"app/files", "app/uploads"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeDumpDir(t, tc.storeTypes)
			got, err := blobBucketsMissingPayloads(dir)
			if err != nil {
				t.Fatalf("blobBucketsMissingPayloads(%q): %v", dir, err)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("blob buckets = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestBlobPayloadNoteNamesEachBucketOrSaysNothing pins the warning text
// itself: it must name each bucket (an operator who is told "some buckets
// lost data" has to go and find out which), and it must be empty when there
// is nothing to warn about, so a dump with no blob bucket stays quiet.
func TestBlobPayloadNoteNamesEachBucketOrSaysNothing(t *testing.T) {
	if note := blobPayloadNote(blobConsequenceDump, nil); note != "" {
		t.Errorf("a dump with no blob bucket produced a warning: %s", note)
	}

	note := blobPayloadNote(blobConsequenceDump, []string{"app/files", "app/uploads"})
	for _, want := range []string{"app/files", "app/uploads", "payload"} {
		if !strings.Contains(note, want) {
			t.Errorf("the blob warning does not mention %q.\ngot: %s", want, note)
		}
	}
}

// TestRunRestoreBucketsWarnsAboutBlobPayloads is GAP-28's restore half, driven
// through runRestoreBuckets itself rather than through the helper: a restore
// must say, before it touches anything, which buckets it is about to recreate
// empty. The handler has no connection, so every bucket then fails — that is
// deliberate, because the warning has to be emitted from the pre-flight path
// and not as a side effect of a successful transfer.
func TestRunRestoreBucketsWarnsAboutBlobPayloads(t *testing.T) {
	var terminal bytes.Buffer
	h := NewCommandHandler(&terminal, false)

	withBlob := writeDumpDir(t, map[string]string{"app/users": "hash", "app/files": "blob"})
	var progress bytes.Buffer
	summary, err := runRestoreBuckets(h, restoreCLIOptions{Dir: withBlob}, &progress, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets on a dump with a blob bucket: %v", err)
	}
	if !strings.Contains(progress.String(), "app/files") {
		t.Errorf("restoring a dump with a blob bucket did not name it on the progress stream.\ngot: %s", progress.String())
	}
	if !summaryHasWarningNaming(summary.warnings, "app/files") {
		t.Errorf("the blob warning is not in summary.warnings, so --json would not carry it.\ngot: %v", summary.warnings)
	}
	if summaryHasWarningNaming(summary.warnings, "app/users") {
		t.Errorf("a non-blob bucket was named in the blob warning.\ngot: %v", summary.warnings)
	}

	withoutBlob := writeDumpDir(t, map[string]string{"app/users": "hash"})
	progress.Reset()
	quiet, err := runRestoreBuckets(h, restoreCLIOptions{Dir: withoutBlob}, &progress, nil)
	if err != nil {
		t.Fatalf("runRestoreBuckets on a dump with no blob bucket: %v", err)
	}
	for _, note := range quiet.warnings {
		if strings.Contains(note, "payload") {
			t.Errorf("a dump with no blob bucket still warned about payloads: %s", note)
		}
	}
}

func summaryHasWarningNaming(warnings []string, bucket string) bool {
	for _, note := range warnings {
		if strings.Contains(note, "payload") && strings.Contains(note, bucket) {
			return true
		}
	}
	return false
}

// TestBlobPayloadNoteDoesNotPromiseKeysAndMetadataOnRestore pins BUG-0494: the
// restore clause must not say a restored blob bucket comes back with its keys
// and metadata, because restore discards every one of those documents
// (server/restore.go:applyRestoreDocument). The restored bucket is empty, and
// the note must say where the keys and metadata remain.
func TestBlobPayloadNoteDoesNotPromiseKeysAndMetadataOnRestore(t *testing.T) {
	note := blobPayloadNote(blobConsequenceRestore, []string{"app/files"})
	if strings.Contains(note, "with their keys and metadata") {
		t.Errorf("the restore warning promises keys and metadata come back, but restore writes neither.\ngot: %s", note)
	}
	if !strings.Contains(note, "empty") {
		t.Errorf("the restore warning does not say the restored blob bucket comes back empty.\ngot: %s", note)
	}
}
