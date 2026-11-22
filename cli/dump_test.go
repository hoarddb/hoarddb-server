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

// TestDumpPositionalScope pins the positional/--db/--bucket reconciliation
// rules (dump-restore-spec.md §6): each may be given once, either way, and a
// bucket requires a database.
func TestDumpPositionalScope(t *testing.T) {
	cases := []struct {
		name         string
		positional   []string
		dbFlag       string
		bucketFlag   string
		wantDatabase string
		wantBucket   string
		wantErr      bool
	}{
		{name: "nothing", wantErr: false},
		{name: "database only positional", positional: []string{"app"}, wantDatabase: "app"},
		{name: "database and bucket positional", positional: []string{"app", "users"}, wantDatabase: "app", wantBucket: "users"},
		{name: "database via flag", dbFlag: "app", wantDatabase: "app"},
		{name: "bucket via flag requires database via flag", dbFlag: "app", bucketFlag: "users", wantDatabase: "app", wantBucket: "users"},
		{name: "bucket without database is refused", bucketFlag: "users", wantErr: true},
		{name: "database given both ways is refused", positional: []string{"app"}, dbFlag: "app", wantErr: true},
		{name: "bucket given both ways is refused", positional: []string{"app", "users"}, bucketFlag: "users", wantErr: true},
		{name: "too many positional args is refused", positional: []string{"a", "b", "c"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			database, bucket, err := dumpPositionalScope(tc.positional, tc.dbFlag, tc.bucketFlag)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("dumpPositionalScope(%v, %q, %q) = (%q, %q, nil), want an error",
						tc.positional, tc.dbFlag, tc.bucketFlag, database, bucket)
				}
				return
			}
			if err != nil {
				t.Fatalf("dumpPositionalScope(%v, %q, %q): unexpected error: %v", tc.positional, tc.dbFlag, tc.bucketFlag, err)
			}
			if database != tc.wantDatabase || bucket != tc.wantBucket {
				t.Fatalf("dumpPositionalScope(%v, %q, %q) = (%q, %q), want (%q, %q)",
					tc.positional, tc.dbFlag, tc.bucketFlag, database, bucket, tc.wantDatabase, tc.wantBucket)
			}
		})
	}
}

// TestCheckOutDirRefusesForeignNonEmptyDirectory pins dump-restore-spec.md
// §6/§13: a non-empty --out that is not itself a previous dump (no
// manifest.json) is refused rather than silently co-mingled into.
func TestCheckOutDirRefusesForeignNonEmptyDirectory(t *testing.T) {
	t.Run("missing directory is created", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "fresh")
		if err := checkOutDir(dir); err != nil {
			t.Fatalf("checkOutDir on a missing directory: %v", err)
		}
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Fatalf("checkOutDir did not create %s", dir)
		}
	})

	t.Run("empty directory is accepted", func(t *testing.T) {
		dir := t.TempDir()
		if err := checkOutDir(dir); err != nil {
			t.Fatalf("checkOutDir on an empty directory: %v", err)
		}
	})

	t.Run("non-empty foreign directory is refused", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "some_other_file.txt"), []byte("not a dump"), 0o644); err != nil {
			t.Fatalf("seeding a foreign file: %v", err)
		}
		if err := checkOutDir(dir); err == nil {
			t.Fatal("checkOutDir accepted a non-empty directory with no manifest.json")
		}
	})

	t.Run("a previous dump directory is accepted", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(`{"format_version":1}`), 0o644); err != nil {
			t.Fatalf("seeding manifest.json: %v", err)
		}
		if err := checkOutDir(dir); err != nil {
			t.Fatalf("checkOutDir refused a directory that is itself a previous dump: %v", err)
		}
	})
}

// TestDumpSummaryExitCode pins the exit-code mapping (dump-restore-spec.md
// §6): success, refused-outright, partial, and interrupted are distinct.
func TestDumpSummaryExitCode(t *testing.T) {
	cases := []struct {
		name    string
		summary *dumpSummary
		want    int
	}{
		{name: "clean success", summary: &dumpSummary{buckets: 2}, want: ExitOK},
		{
			name:    "nothing succeeded, something refused",
			summary: &dumpSummary{buckets: 0, failures: []dumpFailure{{Database: "app", Bucket: "a", Error: "refused"}}},
			want:    ExitRefused,
		},
		{
			name: "partial: some succeeded, some failed",
			summary: &dumpSummary{buckets: 1, failures: []dumpFailure{
				{Database: "app", Bucket: "b", Error: "boom"},
			}},
			want: ExitPartial,
		},
		{name: "interrupted wins regardless", summary: &dumpSummary{buckets: 1, interrupted: true}, want: ExitInterrupted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.summary.exitCode(); got != tc.want {
				t.Fatalf("exitCode() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestBucketMetadataFileRoundTrips proves the metadata.json shape
// (dump-restore-spec.md §4) survives a JSON round trip with the field names
// the spec's own example uses.
func TestBucketMetadataFileRoundTrips(t *testing.T) {
	meta := bucketMetadataFile{
		FormatVersion:  1,
		HoardDBVersion: "0.1.0",
		ProtocolMajor:  1,
		CreatedAt:      "2026-09-18T09:12:44Z",
		SourceNode:     "node-a",
		Database:       "app",
		Bucket:         "users",
		StoreType:      "hash",
		Indexes:        []string{"Email"},
		Documents:      4213,
		Bytes:          1832491,
		Digest:         "sha256:deadbeef",
		BlobsIncluded:  false,
	}
	path := filepath.Join(t.TempDir(), "users.metadata.json")
	if err := writeJSONFile(path, meta); err != nil {
		t.Fatalf("writeJSONFile: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading back %s: %v", path, err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, field := range []string{"format_version", "hoarddb_version", "protocol_major", "created_at",
		"source_node", "database", "bucket", "store_type", "indexes", "documents", "bytes", "digest", "blobs_included"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("metadata.json is missing field %q: %v", field, decoded)
		}
	}
}

// TestDumpOnAClusterPrintsNoPartialCoverageWarning is BUG-0689: the listings
// answer for the whole cluster and a dump forwards each bucket's start to its
// owner (BUG-0468), so a scopeless dump through any node holds every bucket in
// the cluster. The CLI must not tell the operator it covers only the connected
// node's buckets, because that sentence sends them to dump each node in turn.
func TestDumpOnAClusterPrintsNoPartialCoverageWarning(t *testing.T) {
	script := map[transport.Opcode]interface{}{
		transport.OpListNodes: server.NodeListResponse{Nodes: []*server.NodeInfo{
			{NodeID: "n1", Status: "active"},
			{NodeID: "n2", Status: "active"},
			{NodeID: "n3", Status: "active"},
		}},
		transport.OpListDBs: server.ListDatabasesResponse{
			Databases: []server.DatabaseInfo{{Name: "cert", BucketCount: 1}},
		},
		transport.OpListBuckets: server.ListBucketsResponse{
			Found: true, Database: "cert", Buckets: []server.BucketInfo{{Name: "held-elsewhere"}},
		},
	}
	stub, c := newStubServer(t, script, &bytes.Buffer{})

	targets, err := resolveDumpTargets(c.handler, "", "")
	if err != nil {
		t.Fatalf("resolveDumpTargets on a cluster: %v", err)
	}
	if len(targets) != 1 || targets[0].Database != "cert" || targets[0].Bucket != "held-elsewhere" {
		t.Fatalf("resolveDumpTargets on a cluster = %+v, want the one bucket the cluster listing named", targets)
	}
	// The partial-coverage warning was produced by asking for the node list; a
	// cluster-wide listing needs no such question, so none may be sent.
	if asked := stub.sent(transport.OpListNodes); asked != 0 {
		t.Errorf("resolving a cluster dump's targets sent LIST_NODES %d time(s); the listings are cluster-wide, so there is nothing to count nodes for", asked)
	}
}

// TestVerbUsageStatesEachDefaultOnce: the --out and --dir help strings spelled
// their default in prose, and the flag package appends its own
// `(default "./dump")`, so the usage text read "(default ./dump) (default
// "./dump")". A default belongs to the flag package to print, once.
func TestVerbUsageStatesEachDefaultOnce(t *testing.T) {
	verbs := map[string]func([]string) int{"dump": RunDumpCommand, "restore": RunRestoreCommand}
	for name, run := range verbs {
		t.Run(name, func(t *testing.T) {
			code, _, stderr := captureProcessStreams(t, func() int { return run([]string{"--bogus"}) })
			if code != ExitUsage {
				t.Fatalf("%s --bogus exited %d, want %d", name, code, ExitUsage)
			}
			for _, line := range strings.Split(stderr, "\n") {
				if strings.Count(line, "(default") > 1 {
					t.Errorf("a usage line states a default twice: %q", line)
				}
			}
		})
	}
}
