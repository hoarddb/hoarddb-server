package cli

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var printedIngestCommand = regexp.MustCompile(`hoarddb ingest (\S+) --into (\S+)`)

// TestPlanDirectoryRefusalsNameCommandsRingIngestAccepts: `hoarddb plan <dir>` reads a directory
// with the same code the legacy directory ingest used, and every refusal and note it can print
// for a directory that suggests a command must suggest one ring ingest accepts: one file, a
// target of the form <database>.<bucket>, none of the removed flags.
func TestPlanDirectoryRefusalsNameCommandsRingIngestAccepts(t *testing.T) {
	write := func(directory, name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	withSubdirectory := t.TempDir()
	if err := os.Mkdir(filepath.Join(withSubdirectory, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	empty := t.TempDir()
	onlyGzip := t.TempDir()
	write(onlyGzip, "users.bson.gz", "x")
	two := t.TempDir()
	write(two, "a.bson", "")
	write(two, "b.bson", "")
	dotted := t.TempDir()
	write(dotted, "my.users.bson", "")
	view := t.TempDir()
	write(view, "v.bson", "")
	write(view, "v.metadata.json", `{"type":"view","indexes":[]}`)

	cases := []struct {
		name       string
		directory  string
		maxBuckets int
	}{
		{"a subdirectory", withSubdirectory, 1000},
		{"an empty directory", empty, 1000},
		{"only compressed files", onlyGzip, 1000},
		{"more files than the limit", two, 1},
		{"a dotted name", dotted, 1000},
		{"a view", view, 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var texts []string
			plan, err := resolveDirectory(directoryRequest{Input: tc.directory, MaxBuckets: tc.maxBuckets})
			if err != nil {
				texts = append(texts, err.Error())
			}
			if plan != nil {
				texts = append(texts, plan.Notes...)
				for _, file := range plan.Files {
					texts = append(texts, file.Refusal)
				}
			}
			joined := strings.Join(texts, "\n")
			if strings.TrimSpace(joined) == "" {
				t.Fatalf("%s printed nothing; the case does not reach a refusal", tc.name)
			}
			for _, removed := range []string{"--into-db", "--allow-dots", "--max-buckets", "--into ."} {
				if strings.Contains(joined, removed) {
					t.Errorf("%s: the text still carries %q:\n%s", tc.name, removed, joined)
				}
			}
			commands := printedIngestCommand.FindAllStringSubmatch(joined, -1)
			if len(commands) == 0 && !strings.Contains(tc.name, "compressed") {
				t.Fatalf("%s names no ingest command:\n%s", tc.name, joined)
			}
			for _, command := range commands {
				file := strings.NewReplacer("<file>", "users", "<bucket>", "Users").Replace(command[1])
				into := strings.NewReplacer("<bucket>", "Users").Replace(strings.TrimRight(command[2], ".,)"))
				flags, err := parseRingIngestArgs([]string{file, "--into", into}, ringIngestEnv{stdout: io.Discard, stderr: io.Discard})
				if err == nil {
					_, _, err = flags.validate()
				}
				if err != nil {
					t.Errorf("%s: %q is not a command ring ingest accepts: %v", tc.name, command[0], err)
				}
			}
		})
	}
}

// TestTooManyFilesRefusalStatesNoBucketCountClaim: the refusal of a directory with more files than
// the plan reads gives the reason and the way out. It names no number of buckets a node holds,
// because the server enforces none (owner, 2026-11-21).
func TestTooManyFilesRefusalStatesNoBucketCountClaim(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"a.bson", "b.bson"} {
		if err := os.WriteFile(filepath.Join(directory, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_, err := resolveDirectory(directoryRequest{Input: directory, MaxBuckets: 1})
	if err == nil {
		t.Fatal("two files over a limit of one were accepted")
	}
	refusal := err.Error()
	for _, claim := range []string{"thousands", "published", "A hoardDB node"} {
		if strings.Contains(refusal, claim) {
			t.Errorf("the refusal carries %q:\n%s", claim, refusal)
		}
	}
	for _, wanted := range []string{"2 .bson files", "at most 1", "hoarddb ingest " + directory + "/<file>.bson --into "} {
		if !strings.Contains(refusal, wanted) {
			t.Errorf("the refusal lacks %q:\n%s", wanted, refusal)
		}
	}
}
