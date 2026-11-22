package cluster

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoProductionFileCallsAppendWithoutDryRun: Raft.AppendWithoutDryRun commits an entry with no
// dry run, which is exactly what a leader of another build does and what this build must never
// do. It is exported only because the wire package's tests need it (a _test.go helper in this
// package is invisible to them), so the rule is enforced here: no non-test file in the module
// may mention it except its definition. Break: call it from any production file.
func TestNoProductionFileCallsAppendWithoutDryRun(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	definition := filepath.Join(root, "cluster", "raft.go")
	seen := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "vendor") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == definition {
			return nil
		}
		seen++
		text, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(text), "AppendWithoutDryRun") {
			t.Errorf("%s mentions AppendWithoutDryRun: production code must append through ProposeContext, which dry-runs", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen < 50 {
		t.Fatalf("the scan read only %d production files: the module root %s is wrong", seen, root)
	}
}
