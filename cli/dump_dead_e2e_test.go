package cli

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dump format 3 through the real CLI and a real server: the dead entries of a
// queue are kept dead across a dump and a restore, in a file of their own
// (claim/ack build step 17).

// seedJobsWithDeadEntries makes a fifo "Jobs" of entries 1..total in which the
// first dead entries have been claimed and released until they were
// dead-lettered, and then claims one more and leaves it pending.
func seedJobsWithDeadEntries(t *testing.T, c *CLI, total, dead int) {
	t.Helper()
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	var created server.CreateBucketResponse
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Jobs", Type: server.BucketTypeFIFO}, &created)
	if !created.Success {
		t.Fatalf("creating Jobs: %s", created.Message)
	}
	for n := 1; n <= total; n++ {
		mustSend(t, c, transport.OpFIFOPush, &server.PushRequest{Database: "app", Bucket: "Jobs", Data: map[string]interface{}{"n": int32(n)}}, nil)
	}
	for cycle := 0; len(deadItems(t, c)) < dead; cycle++ {
		if cycle > dead*store.MaxDeliveries {
			t.Fatalf("setup: %d claim and release cycles left %d dead entries, want %d", cycle, len(deadItems(t, c)), dead)
		}
		var claimed server.ClaimResponse
		mustSend(t, c, transport.OpQueueClaim, &server.ClaimRequest{Database: "app", Bucket: "Jobs", Limit: 1, LeaseMs: 60_000}, &claimed)
		mustSend(t, c, transport.OpQueueRelease, &server.LeaseRequest{Database: "app", Bucket: "Jobs", LeaseID: claimed.LeaseID}, nil)
	}
	var pending server.ClaimResponse
	mustSend(t, c, transport.OpQueueClaim, &server.ClaimRequest{Database: "app", Bucket: "Jobs", Limit: 1, LeaseMs: 3_600_000}, &pending)
	if !pending.Found {
		t.Fatalf("setup: the last claim found nothing")
	}
}

func deadItems(t *testing.T, c *CLI) []server.DeadItem {
	t.Helper()
	var listed server.DeadListResponse
	mustSend(t, c, transport.OpDeadList, &server.DeadListRequest{Database: "app", Bucket: "Jobs", Limit: 100}, &listed)
	return listed.Items
}

// liveJobs pops Jobs empty and returns the n of each entry, in pop order.
func liveJobs(t *testing.T, c *CLI) []int {
	t.Helper()
	var numbers []int
	for {
		var popped server.PopResponse
		mustSend(t, c, transport.OpFIFOPop, &server.PopRequest{Database: "app", Bucket: "Jobs"}, &popped)
		if !popped.Found {
			return numbers
		}
		numbers = append(numbers, int(popped.Items[0].Lookup("n").Int32()))
	}
}

// requireNothingRestored fails when target holds an entry of Jobs; a bucket the
// refused restore never created is the usual, and good, answer.
func requireNothingRestored(t *testing.T, target *CLI) {
	t.Helper()
	var popped server.PopResponse
	if err := target.handler.send(transport.OpFIFOPop, &server.PopRequest{Database: "app", Bucket: "Jobs"}, &popped); err == nil && popped.Found {
		t.Fatalf("the refused restore left a live entry: %v", popped.Items[0])
	}
}

func dumpJobs(t *testing.T, c *CLI) string {
	t.Helper()
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", "Jobs", outDir, io.Discard, nil); err != nil || summary.incomplete {
		t.Fatalf("dumping Jobs: %+v, %v", summary, err)
	}
	return outDir
}

func readJobsMetadata(t *testing.T, outDir string) bucketMetadataFile {
	t.Helper()
	var meta bucketMetadataFile
	readJSONFile(t, filepath.Join(outDir, "app", "Jobs.metadata.json"), &meta)
	return meta
}

// TestDumpAndRestoreKeepDeadEntriesDead: through the CLI, a queue's dead
// entries land in <bucket>.dead.bson, their count and digest in metadata.json,
// and come back dead with the deliveries they died with. The documents file
// holds the live and pending entries and nothing else.
func TestDumpAndRestoreKeepDeadEntriesDead(t *testing.T) {
	source := newDumpTestServer(t)
	seedJobsWithDeadEntries(t, source, 5, 2)
	wantDead := deadItems(t, source)
	outDir := dumpJobs(t, source)

	meta := readJobsMetadata(t, outDir)
	if meta.FormatVersion != 3 || meta.Dead != 2 || meta.DeadDigest == "" || meta.DeadBytes == 0 || meta.Documents != 3 {
		t.Fatalf("the metadata reads format %d, %d dead (%d bytes, digest %q), %d documents; want 3, 2 dead and 3 documents (one of them pending)",
			meta.FormatVersion, meta.Dead, meta.DeadBytes, meta.DeadDigest, meta.Documents)
	}
	deadRaw, err := os.ReadFile(filepath.Join(outDir, "app", "Jobs.dead.bson"))
	if err != nil {
		t.Fatalf("reading the dead file: %v", err)
	}
	if first := bson.Raw(splitBSONDocumentsForTest(t, deadRaw)[0]); first.Lookup("reason").StringValue() != store.DeadReasonMaxDeliveries {
		t.Fatalf("the first record of the dead file is %v, want a dead entry that died of max_deliveries", first)
	}
	documentsRaw, err := os.ReadFile(filepath.Join(outDir, "app", "Jobs.bson"))
	if err != nil {
		t.Fatalf("reading the documents file: %v", err)
	}
	for _, document := range splitBSONDocumentsForTest(t, documentsRaw) {
		if _, isDeadRecord := bson.Raw(document).Lookup("deliveries").Int32OK(); isDeadRecord {
			t.Fatalf("the documents file holds a dead record: %v", bson.Raw(document))
		}
	}

	target := newDumpTestServer(t)
	restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil || len(restored.failures) != 0 {
		t.Fatalf("restoring Jobs: %+v, %v", restored, err)
	}
	gotDead := deadItems(t, target)
	if len(gotDead) != len(wantDead) {
		t.Fatalf("the restored bucket lists %d dead entries, want %d", len(gotDead), len(wantDead))
	}
	for i := range wantDead {
		if !slices.Equal(gotDead[i].Data, wantDead[i].Data) || gotDead[i].Deliveries != store.MaxDeliveries || gotDead[i].Reason != wantDead[i].Reason {
			t.Errorf("dead entry %d came back as %+v, want %+v", i, gotDead[i], wantDead[i])
		}
	}
	// The claimed entry 3 comes back as an ordinary entry, ahead of 4 and 5; the
	// two dead ones (1 and 2) must not come back as entries at all.
	if got, want := liveJobs(t, target), []int{3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("the restored bucket pops %v, want %v: no dead entry may come back alive", got, want)
	}
}

// TestRestoreRefusesADeadRecordWrittenIntoTheDocumentsFile is the guard of the
// test above: a dead record added to the documents file by hand no longer
// matches the file's digest or count, so the restore refuses the bucket and no
// dead wrapper becomes a live document.
func TestRestoreRefusesADeadRecordWrittenIntoTheDocumentsFile(t *testing.T) {
	source := newDumpTestServer(t)
	seedJobsWithDeadEntries(t, source, 5, 2)
	outDir := dumpJobs(t, source)
	deadRaw, err := os.ReadFile(filepath.Join(outDir, "app", "Jobs.dead.bson"))
	if err != nil {
		t.Fatalf("reading the dead file: %v", err)
	}
	documentsPath := filepath.Join(outDir, "app", "Jobs.bson")
	documents, err := os.OpenFile(documentsPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("opening the documents file: %v", err)
	}
	if _, err := documents.Write(splitBSONDocumentsForTest(t, deadRaw)[0]); err != nil {
		t.Fatalf("appending the dead record: %v", err)
	}
	if err := documents.Close(); err != nil {
		t.Fatalf("closing the documents file: %v", err)
	}

	target := newDumpTestServer(t)
	restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil {
		t.Fatalf("restoring: %v", err)
	}
	if len(restored.failures) != 1 || !strings.Contains(restored.failures[0].Error, "digest mismatch") {
		t.Fatalf("the restore reported %+v; want Jobs refused for a digest mismatch", restored.failures)
	}
	requireNothingRestored(t, target)
}

// TestRestoreRefusesAMissingOrChangedDeadFile: a bucket whose metadata records
// dead entries is never restored without them.
func TestRestoreRefusesAMissingOrChangedDeadFile(t *testing.T) {
	source := newDumpTestServer(t)
	seedJobsWithDeadEntries(t, source, 5, 2)
	for name, damage := range map[string]func(path string) error{
		"missing": os.Remove,
		"changed": func(path string) error { return os.WriteFile(path, []byte{5, 0, 0, 0, 0}, 0o644) },
	} {
		t.Run(name, func(t *testing.T) {
			outDir := dumpJobs(t, source)
			if err := damage(filepath.Join(outDir, "app", "Jobs.dead.bson")); err != nil {
				t.Fatalf("damaging the dead file: %v", err)
			}
			target := newDumpTestServer(t)
			restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
			if err != nil || len(restored.failures) != 1 || !strings.Contains(restored.failures[0].Error, "Jobs.dead.bson") {
				t.Fatalf("restoring with the dead file %s = %+v, %v; want Jobs refused naming Jobs.dead.bson", name, restored, err)
			}
			requireNothingRestored(t, target)
		})
	}
}

// TestDumpOfABucketWithNoDeadEntryWritesNoDeadFile (rule 11): nothing dead,
// nothing written.
func TestDumpOfABucketWithNoDeadEntryWritesNoDeadFile(t *testing.T) {
	source := newDumpTestServer(t)
	seedJobsWithDeadEntries(t, source, 3, 0)
	outDir := dumpJobs(t, source)
	if _, err := os.Stat(filepath.Join(outDir, "app", "Jobs.dead.bson")); !os.IsNotExist(err) {
		t.Fatalf("a bucket with no dead entry wrote a dead file (stat: %v)", err)
	}
	if meta := readJobsMetadata(t, outDir); meta.Dead != 0 || meta.DeadDigest != "" || meta.DeadBytes != 0 {
		t.Fatalf("the metadata records dead %d, digest %q, %d bytes; want none", meta.Dead, meta.DeadDigest, meta.DeadBytes)
	}
}

// rewriteFormat sets every format_version in a Jobs dump directory.
func rewriteFormat(t *testing.T, outDir string, version int) {
	t.Helper()
	for _, name := range []string{"manifest.json", filepath.Join("app", "Jobs.metadata.json")} {
		path := filepath.Join(outDir, name)
		var fields map[string]json.RawMessage
		readJSONFile(t, path, &fields)
		fields["format_version"] = json.RawMessage(strconv.Itoa(version))
		if err := writeJSONFile(path, fields); err != nil {
			t.Fatalf("rewriting %s: %v", path, err)
		}
	}
}

// TestDumpFormat2StillRestores: a format-2 dump restores, a format-3 dump is
// refused by a restorer that accepts only 2 (a test-local older set), and any
// other version is refused.
func TestDumpFormat2StillRestores(t *testing.T) {
	older := func(version int) bool { return version == 2 }
	if older(dumpManifestFormatVersionCLI) {
		t.Fatalf("the CLI writes format %d, which a restorer that knows only 2 accepts: it would restore the dead file's records as documents", dumpManifestFormatVersionCLI)
	}
	source := newDumpTestServer(t)
	seedJobsWithDeadEntries(t, source, 3, 0)
	for version, accepted := range map[int]bool{1: false, 2: true, 3: true, 4: false} {
		outDir := dumpJobs(t, source)
		rewriteFormat(t, outDir, version)
		target := newDumpTestServer(t)
		restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
		if accepted && (err != nil || len(restored.failures) != 0 || len(liveJobs(t, target)) != 3) {
			t.Errorf("format_version %d: %+v, %v; want it restored with 3 entries", version, restored, err)
		}
		if !accepted && (err == nil || !strings.Contains(err.Error(), "format_versions 2 and 3")) {
			t.Errorf("format_version %d = %v; want a refusal naming the versions this build restores", version, err)
		}
	}
}
