package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPackRestoreChunksNeverSplitsADocument proves the chunking discipline
// OpRestoreChunk's server-side parser depends on: every chunk is a
// concatenation of whole documents, and a single document larger than the
// nominal chunk size still goes out as one chunk rather than being split.
func TestPackRestoreChunksNeverSplitsADocument(t *testing.T) {
	small := []byte("abc")
	medium := []byte("defgh")
	big := make([]byte, 20)
	for i := range big {
		big[i] = byte('x')
	}

	docs := [][]byte{small, medium, big, small}
	chunks := packRestoreChunks(docs, 10)

	var reassembled []byte
	for _, c := range chunks {
		reassembled = append(reassembled, c...)
	}
	var want []byte
	for _, d := range docs {
		want = append(want, d...)
	}
	if string(reassembled) != string(want) {
		t.Fatalf("packRestoreChunks lost or reordered bytes: got %q, want %q", reassembled, want)
	}

	// No chunk may contain a partial document: splitting each chunk back
	// into documents (using the same 4-byte-would-be-length rule is not
	// meaningful for these plain test byte strings, so instead assert that
	// each of the four original slices appears intact, at a single
	// contiguous offset, inside exactly one chunk).
	for _, doc := range docs {
		found := false
		for _, c := range chunks {
			if containsAt(c, doc) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("document %q does not appear intact in any chunk: %v", doc, chunks)
		}
	}

	// The oversized document must not have been merged with its neighbours
	// into a chunk exceeding chunkSize by more than the oversized document
	// itself can explain.
	for _, c := range chunks {
		if len(c) > 10 && len(c) != len(big) && len(c) != len(big)+len(small) {
			t.Fatalf("unexpected chunk length %d for chunk %q", len(c), c)
		}
	}
}

func containsAt(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// TestSplitDumpFileIntoDocuments proves the BSON length-prefix framing is
// parsed correctly, and that a truncated trailing document is refused
// rather than silently dropped.
func TestSplitDumpFileIntoDocuments(t *testing.T) {
	doc := func(body string) []byte {
		total := 4 + len(body) + 1 // length prefix + body + trailing null, matching real BSON's own byte 0 terminator
		b := make([]byte, 0, total)
		n := int32(total)
		b = append(b, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
		b = append(b, body...)
		b = append(b, 0)
		return b
	}

	d1, d2 := doc("one"), doc("two")
	concatenated := append(append([]byte{}, d1...), d2...)

	docs, err := splitDumpFileIntoDocuments(concatenated)
	if err != nil {
		t.Fatalf("splitDumpFileIntoDocuments: %v", err)
	}
	if len(docs) != 2 || string(docs[0]) != string(d1) || string(docs[1]) != string(d2) {
		t.Fatalf("splitDumpFileIntoDocuments = %v, want [%q %q]", docs, d1, d2)
	}

	truncated := concatenated[:len(concatenated)-1]
	if _, err := splitDumpFileIntoDocuments(truncated); err == nil {
		t.Fatal("a truncated document was not refused")
	}
}

// TestValidateRestoreReadPathRefusesHostileNames pins dump-restore-spec.md
// §9.1/§12.12 on the CLI's read side: "../", an absolute path, a NUL byte,
// and a name that canonicalises outside --dir are all refused.
func TestValidateRestoreReadPathRefusesHostileNames(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name     string
		database string
		bucket   string
		wantOK   bool
	}{
		{"ordinary", "app", "users", true},
		{"dotdot_database", "../escaped", "b", false},
		{"dotdot_bucket", "app", "../escaped", false},
		{"absolute_database", "/etc/passwd", "b", false},
		{"absolute_bucket", "app", "/etc/passwd", false},
		{"nul_database", "app\x00evil", "b", false},
		{"nul_bucket", "app", "b\x00evil", false},
		{"empty_database", "", "b", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRestoreReadPath(dir, tc.database, tc.bucket)
			if tc.wantOK && err != nil {
				t.Fatalf("validateRestoreReadPath(%q, %q) = %v, want nil", tc.database, tc.bucket, err)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("validateRestoreReadPath(%q, %q) = nil, want a refusal", tc.database, tc.bucket)
			}
		})
	}
}

// TestResolveRestoreTargetsSortedAndFiltered proves the directory walk
// finds every <database>/<bucket>.metadata.json, ignores everything else
// (the .bson files, manifest.json, users.json), and returns them sorted.
func TestResolveRestoreTargetsSortedAndFiltered(t *testing.T) {
	dir := t.TempDir()
	mustWrite := func(path string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	mustWrite(filepath.Join(dir, "manifest.json"))
	mustWrite(filepath.Join(dir, "users.json"))
	mustWrite(filepath.Join(dir, "zdb", "zebra.metadata.json"))
	mustWrite(filepath.Join(dir, "zdb", "zebra.bson"))
	mustWrite(filepath.Join(dir, "adb", "apple.metadata.json"))
	mustWrite(filepath.Join(dir, "adb", "apple.bson"))

	targets, err := resolveRestoreTargets(dir)
	if err != nil {
		t.Fatalf("resolveRestoreTargets: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("resolveRestoreTargets = %v, want 2 targets", targets)
	}
	if targets[0].Database != "adb" || targets[0].Bucket != "apple" {
		t.Fatalf("targets[0] = %+v, want adb/apple (sorted first)", targets[0])
	}
	if targets[1].Database != "zdb" || targets[1].Bucket != "zebra" {
		t.Fatalf("targets[1] = %+v, want zdb/zebra", targets[1])
	}
}

// TestRestoreSummaryExitCode mirrors TestDumpSummaryExitCode for restore's
// own summary type.
func TestRestoreSummaryExitCode(t *testing.T) {
	cases := []struct {
		name    string
		summary *restoreSummary
		want    int
	}{
		{name: "clean success", summary: &restoreSummary{buckets: 2}, want: ExitOK},
		{
			name:    "nothing succeeded",
			summary: &restoreSummary{buckets: 0, failures: []dumpFailure{{Database: "app", Bucket: "a", Error: "refused"}}},
			want:    ExitRefused,
		},
		{
			name: "partial",
			summary: &restoreSummary{buckets: 1, failures: []dumpFailure{
				{Database: "app", Bucket: "b", Error: "boom"},
			}},
			want: ExitPartial,
		},
		{name: "interrupted wins regardless", summary: &restoreSummary{buckets: 1, interrupted: true}, want: ExitInterrupted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.summary.exitCode(); got != tc.want {
				t.Fatalf("exitCode() = %d, want %d", got, tc.want)
			}
		})
	}
}
