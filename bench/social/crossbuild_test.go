package main

import (
	"os"
	"os/exec"
	"testing"
)

// TestBenchSocialBuildsForDarwin guards BUG-0722: the probe once called
// syscall.Fdatasync, which exists on Linux only, so gate 11's cross-platform
// vet failed while every Linux test passed. A cross-build is the cheapest
// check that sees it; it is skipped under -short because a cold build cache
// compiles the whole dependency tree.
func TestBenchSocialBuildsForDarwin(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-build compiles the dependency tree on a cold cache")
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go tool not on PATH: %v", err)
	}
	for _, goarch := range []string{"arm64", "amd64"} {
		t.Run("darwin_"+goarch, func(t *testing.T) {
			build := exec.Command(goTool, "build", "-o", os.DevNull, ".")
			build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=darwin", "GOARCH="+goarch)
			output, err := build.CombinedOutput()
			if err != nil {
				t.Fatalf("GOOS=darwin GOARCH=%s go build ./bench/social failed: %v\n%s", goarch, err, output)
			}
		})
	}
}
