package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The eight characterisation tests GAP-24 Part 1 recorded here each pinned a
// behaviour that was wrong and that a later part fixed; all of them have
// flipped into the real tests (Parts 6 and 7), so this file keeps only the two
// helpers that run the built binary.

// builtCLI compiles cmd/cli into a temp dir and returns the binary's path, for
// the tests that pin the process's own behaviour (its exit code) rather than
// RunMain's. It skips when the toolchain is unavailable.
func builtCLI(t *testing.T) string {
	t.Helper()
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("the go toolchain is not on PATH; cannot build cmd/cli for this test")
	}
	binary := filepath.Join(t.TempDir(), "hoarddb")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, goTool, "build", "-o", binary, "./cmd/cli")
	build.Dir = ".." // the module root: cli/ is one level below it
	if output, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build cmd/cli (%v):\n%s", err, output)
	}
	return binary
}

// runBinary runs the CLI binary with stdin as given and returns its exit code
// and both streams.
func runBinary(t *testing.T, binary, stdin string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	command := exec.Command(binary, args...)
	command.Stdin = strings.NewReader(stdin)
	command.Stdout, command.Stderr = &outBuf, &errBuf
	// A clean environment: HOARDDB_* variables in the developer's shell must
	// not change which server or user the binary picks.
	command.Env = []string{"HOME=" + t.TempDir(), "PATH=" + os.Getenv("PATH")}
	command.Dir = t.TempDir()
	err := command.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		code = 0
	case errors.As(err, &exitErr):
		code = exitErr.ExitCode()
	default:
		t.Fatalf("running %s: %v", binary, err)
	}
	return code, outBuf.String(), errBuf.String()
}
