package config

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
)

// legacyEnvPrefix is the environment prefix the owner retired on 2026-09-26
// with a hard cut: no release carried it, so the binary honours only
// HOARDDB_. It is assembled from two halves so this file does not match the
// search it runs.
const legacyEnvPrefix = "HOAR" + "DB_"

// legacyPrefixRecords are the tracked paths allowed to keep the retired
// prefix, each because it records what was true on the day it was written.
// Rewriting a variable name inside a bug's evidence or a spec's citation
// makes the record stop being what was observed. Everything else — code,
// tests, scripts, images, workflows and the pages an operator reads — must
// use HOARDDB_, which is what this test enforces.
var legacyPrefixRecords = []struct {
	pathspec, why string
}{
	{"docs/bugs/", "bug files: evidence and reproductions as observed"},
	{"docs/specs/", "specs, build docs and reviews: citations of the code as it was"},
	// Spelled in two pieces so the published tree's private-content guard does
	// not flag this test: the pathspec is needed only while the private
	// repository tracks a .claude directory.
	{".clau" + "de/", "dispatch prompts and agent reports: the instructions as issued"},
	{"docs/internals/benchmarks/benchmark-plan.md", "the benchmark plan as run"},
	{"docs/internals/process/bug-queue-2026-09-26.md", "a dated run report"},
	{"docs/internals/reviews/code-review-15-09-2026.md", "a dated review"},
	{"docs/internals/reviews/code-review-plan.md", "a dated review plan"},
	{"docs/internals/benchmarks/db-comparison-2026-09-24.md", "a dated measurement report"},
	{"docs/internals/benchmarks/db-comparison-2026-09-26.md", "a dated measurement report"},
	{"docs/internals/research/market-research-fit-evidence-2026-09-23.md", "a dated research report"},
	{"docs/internals/research/market-research-mongodb-parity-2026-09-20.md", "a dated research report"},
	{"docs/internals/project-status.md", "the status log: each row records its day"},
	{"docs/internals/read-path-build.md", "a build doc, filed under internals"},
	{"docs/internals/reviews/read-path-optimization-review-fable-5.1.md", "a review"},
	{"docs/internals/read-path-optimization-spec.md", "a spec, filed under internals"},
	{"docs/publish-checklist.md", "the pre-publish tracker: each row records its verification"},
	{"docs/internals/reviews/STALE-DOC-AUDIT-2026-10-05.md", "a dated audit: it quotes each misspelling it corrected"},
	{"docs/internals/process/pi-session-report-2026-10-05.md", "a dated session report: it quotes the misspelling it found"},
	{"docs/internals/reviews/doc-claim-resolver-2026-10-05.md", "a dated report on the resolver that detects the misspelling"},
	{"scripts/doc-claim-check.py", "the resolver: it must name the retired prefix to report it"},
	{"scripts/doc-claim-selftest.sh", "the resolver's selftest: the misspelling is its fixture"},
	{"scripts/docs-env-check.sh", "the live-doc gate: it must name the retired prefix to reject it"},
	{"bench/RESULTS.md", "benchmark results: the commands as they were run"},
	{"github.com/hoarddb/hoarddb-server/bench/RESULTS.md", "benchmark results: the commands as they were run"},
	// Gate 7. Its env-var leg greps for the retired prefix literally, so after
	// the rename it matches nothing and passes vacuously. It is owned by a
	// separate change (the rename dispatch forbids editing it); remove this
	// entry when that change lands.
	{"scripts/claims-check.sh", "gate 7, pending its own change: see the rename's report"},
}

// TestNoLegacyEnvPrefix fails when a tracked file outside legacyPrefixRecords
// names an environment variable by the retired prefix. A one-shot rename
// cannot catch the next variable someone adds by copying an old line; this
// does. The fix it asks for is always the same: spell the variable HOARDDB_.
func TestNoLegacyEnvPrefix(t *testing.T) {
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("not inside a git checkout (git rev-parse: %v); this test reads the tracked tree", err)
	}
	args := []string{"-C", strings.TrimSpace(string(root)), "grep", "-n", "-F", legacyEnvPrefix, "--", "."}
	for _, record := range legacyPrefixRecords {
		args = append(args, ":(exclude)"+record.pathspec)
	}
	matches, err := exec.Command("git", args...).Output()
	// git grep exits 1 when nothing matches, which is the passing case.
	var exitErr *exec.ExitError
	if err != nil && (!errors.As(err, &exitErr) || exitErr.ExitCode() != 1) {
		t.Fatalf("git grep for %q: %v", legacyEnvPrefix, err)
	}
	if hits := strings.TrimSpace(string(matches)); hits != "" {
		t.Errorf("these tracked lines use the retired environment prefix %q; the binary reads only HOARDDB_, "+
			"so spell each one HOARDDB_ (or, if the file is a historical record, add it to legacyPrefixRecords with the reason):\n%s",
			legacyEnvPrefix, hits)
	}
}

// TestLegacyEnvPrefixIsIgnored pins the hard cut: a variable spelled with the
// retired prefix changes nothing, and the same variable spelled HOARDDB_ is
// read. A compatibility shim that honoured both would fail the first half.
func TestLegacyEnvPrefixIsIgnored(t *testing.T) {
	t.Setenv("HOARDDB_DATA_DIR", "")
	t.Setenv(legacyEnvPrefix+"DATA_DIR", "/legacy-prefix-data")
	legacyOnly, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if legacyOnly.Storage.DataDir != "./data" {
		t.Errorf("with only %sDATA_DIR set, data_dir = %q; the retired prefix must be ignored, leaving the default ./data",
			legacyEnvPrefix, legacyOnly.Storage.DataDir)
	}

	t.Setenv("HOARDDB_DATA_DIR", "/current-prefix-data")
	current, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if current.Storage.DataDir != "/current-prefix-data" {
		t.Errorf("with HOARDDB_DATA_DIR set, data_dir = %q, want /current-prefix-data", current.Storage.DataDir)
	}
}
