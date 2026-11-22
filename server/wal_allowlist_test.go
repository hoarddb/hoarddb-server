package server

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/internal/repofile"
	"github.com/hoarddb/hoarddb-server/transport"
)

// classifyHelper is the acceptance run's allowlist as one piece of shell code:
// scripts/encryption-acceptance.sh sources it, and this test drives the same
// classify() through sh, so the spec table and the script cannot disagree about
// a path without this test failing. See the helper's header.
const classifyHelperPath = "scripts/encryption-acceptance-classify.sh"

// specWALPaths reads the "what is not sealed" table of the at-rest encryption
// spec (section 1.2) and returns the file names it lists under wal/. The spec is
// the allowlist's source: a file the WAL directory grows has to be added there,
// and this test is what notices when it is not.
func specWALPaths(t *testing.T) map[string]bool {
	t.Helper()
	// The spec is private-only; the published tree skips this test.
	const specPath = "docs/specs/at-rest-encryption-spec.md"
	spec := repofile.ReadPrivate(t, specPath)
	section := string(spec)
	start := strings.Index(section, "### 1.2 What is *not* sealed")
	if start < 0 {
		t.Fatalf("%s has no section 1.2 table", specPath)
	}
	section = section[start:]
	if end := strings.Index(section[4:], "\n### "); end >= 0 {
		section = section[:end+4]
	}
	listed := map[string]bool{}
	for _, match := range regexp.MustCompile("`wal/([^`]+)`").FindAllStringSubmatch(section, -1) {
		listed[match[1]] = true
	}
	return listed
}

// classifyRunner drives the acceptance script's own classify() through sh by
// sourcing the helper the script sources. See classifyHelperPath.
type classifyRunner struct {
	shell  string
	helper string
}

// class runs classify() for one path relative to dir; isDir picks the
// directory branch.
func (r classifyRunner) class(t *testing.T, dir, rel string, isDir bool) string {
	t.Helper()
	kind := "f"
	if isDir {
		kind = "d"
	}
	out, err := exec.Command(r.shell, "-c", `. "$1" || exit 90; sealed_dir=$2; classify "$3" "$4"`, "sh", r.helper, dir, rel, kind).Output()
	if err != nil {
		t.Fatalf("driving classify(%q, %q) through %s: %v", rel, kind, r.helper, err)
	}
	return strings.TrimSpace(string(out))
}

// TestASealedClusterNodeWritesOnlyWALFilesTheSpecLists runs the path the one-node
// acceptance script never reaches: a sealed node in a cluster appending to its
// write-ahead log. Every file in wal/ afterwards must be a segment or be named
// in the spec's list of what is not sealed, and the spec's own classify() must
// agree with it: a name in the spec alone, or a file the test lists but the
// script does not, fails here rather than only in a full acceptance run.
func TestASealedClusterNodeWritesOnlyWALFilesTheSpecLists(t *testing.T) {
	listed := specWALPaths(t)
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("sh is not on PATH, so %s cannot run: %v", classifyHelperPath, err)
	}
	helper, err := repofile.Find(classifyHelperPath)
	if err != nil {
		t.Skipf("%s is not in this tree, so the allowlist cannot be driven: %v", classifyHelperPath, err)
	}
	runner := classifyRunner{shell: shell, helper: helper}
	// Every name the spec lists must be one classify() knows, whether or not
	// this run wrote it. Without this a name added to the table alone would be
	// UNMATCHED the day some node writes it, and only the acceptance run would
	// say so.
	for name := range listed {
		if class := runner.class(t, t.TempDir(), "wal/"+name, false); class == "UNMATCHED" {
			t.Errorf("section 1.2 of docs/specs/at-rest-encryption-spec.md lists wal/%s, but classify() in %s does not know it: add the path there", name, classifyHelperPath)
		}
	}

	tc := newTestClusterWith(t, 3, 3, func(index int, cfg *config.Config) {
		cfg.Encryption.Keystore = "keyfile"
		cfg.Encryption.KeyFile = makeKeyFile(t, t.TempDir(), byte(0x50+index))
		cfg.Encryption.Mode = "require"
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for _, key := range []string{"a", "b", "c"} {
		if frame := tc.writeConcerned(0, key, "all"); frame.Opcode == transport.OpError {
			t.Fatalf("write %q: %s", key, frame.Payload)
		}
	}

	segment := regexp.MustCompile(`^wal_[0-9]+\.log$`)
	sawSegment := false
	for index, node := range tc.nodes {
		walDir := filepath.Join(node.cfg.Storage.DataDir, "wal")
		entries, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("reading node %d's wal directory: %v", index+1, err)
		}
		for _, entry := range entries {
			rel := "wal/" + entry.Name()
			switch {
			case segment.MatchString(entry.Name()):
				sawSegment = true
			case !listed[entry.Name()]:
				t.Errorf("node %d wrote wal/%s, which section 1.2 of docs/specs/at-rest-encryption-spec.md does not list: add it there and to classify() in %s", index+1, entry.Name(), classifyHelperPath)
			}
			class := runner.class(t, node.cfg.Storage.DataDir, rel, entry.IsDir())
			switch {
			case class == "UNMATCHED":
				t.Errorf("node %d wrote wal/%s, which classify() in %s does not know: add the path there", index+1, entry.Name(), classifyHelperPath)
			case strings.HasSuffix(class, "-VIOLATED") || strings.HasSuffix(class, "-violated"):
				t.Errorf("node %d's wal/%s is %s: the acceptance run would fail this directory", index+1, entry.Name(), class)
			}
		}
		// A clustered node's credential files are sealed (section 1.2), and
		// classify() holds the acceptance run to that.
		for _, rel := range []string{"cluster.token", "keys/server.key"} {
			if class := runner.class(t, node.cfg.Storage.DataDir, rel, false); !strings.HasPrefix(class, "sealed:") || strings.HasSuffix(class, "-VIOLATED") {
				t.Errorf("node %d's %s is %s, want sealed", index+1, rel, class)
			}
		}
		// The origin-format marker's emptiness is a rule the spec states and
		// classify() only enforces inside an acceptance run; a cheap direct
		// check here means a marker that gains content fails on every go test.
		if markerInfo, statErr := os.Stat(filepath.Join(walDir, originFormatMarker)); statErr == nil && markerInfo.Size() != 0 {
			t.Errorf("node %d's wal/%s marker holds %d bytes; the spec says it is an empty marker", index+1, originFormatMarker, markerInfo.Size())
		}
	}
	if !sawSegment {
		t.Fatal("no node had a WAL segment, so the run did not reach the write path")
	}
}
