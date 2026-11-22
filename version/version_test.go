package version

import (
	"strings"
	"testing"
)

// TestVersionStringHasEveryField pins the String() shape: version, commit and
// build date are all named, and the version defaults to devel on a plain
// build (ldflags replaces it).
func TestVersionStringHasEveryField(t *testing.T) {
	line := String()
	for _, field := range []string{"version=", "commit=", "build_date="} {
		if !strings.Contains(line, field) {
			t.Fatalf("String() = %q, missing %q", line, field)
		}
	}
	if Version != "devel" || !strings.HasPrefix(line, "version=devel") {
		t.Fatalf("plain-build String() = %q; it must report devel", line)
	}

	// With build metadata injected (the ldflags case), the value shows.
	Version, Commit, BuildDate = "v1.2.3", "abc123", "2026-09-19T00:00:00Z"
	defer func() { Version, Commit, BuildDate = "devel", "", "" }()
	line = String()
	if !strings.Contains(line, "v1.2.3") || !strings.Contains(line, "abc123") || !strings.Contains(line, "2026-09-19T00:00:00Z") {
		t.Fatalf("injected String() = %q, missing the injected fields", line)
	}
}
