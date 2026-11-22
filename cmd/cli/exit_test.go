package main

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
)

// TestMainExitsWithRunMainsCode builds the real binary, because the only thing
// this package adds to cli.RunMain is os.Exit, and a test that calls RunMain
// cannot tell whether main forwards its return value. Before the fix a bad flag
// exited 2, the number a refused dump uses, and a positional argument was
// ignored and exited 0.
func TestMainExitsWithRunMainsCode(t *testing.T) {
	dir := t.TempDir()
	if out, err := exec.Command("go", "build", "-C", "../..", "-o", dir+"/hcli", "./cmd/cli").CombinedOutput(); err != nil {
		t.Fatalf("build cli: %v\n%s", err, out)
	}

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
	}{
		{"an unknown flag", []string{"-bogus"}, 1, "flag provided but not defined: -bogus"},
		{"a positional argument", []string{"show databases"}, 1, "takes no positional arguments"},
		{"a flag before a verb", []string{"--address", "127.0.0.1:1", "dump"}, 1, "a verb must come first"},
		{"help", []string{"-h"}, 0, "Examples:"},
		{"version", []string{"-v"}, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command(dir+"/hcli", tc.args...)
			command.Dir = dir
			command.Stdin = strings.NewReader("")
			var stderr strings.Builder
			command.Stderr = &stderr
			code := 0
			if err := command.Run(); err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("running the CLI: %v", err)
				}
				code = exitErr.ExitCode()
			}
			if code != tc.wantCode {
				t.Errorf("hoarddb %q exited %d, want %d\n%s", tc.args, code, tc.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Errorf("stderr lacks %q:\n%s", tc.wantStderr, stderr.String())
			}
		})
	}
}
