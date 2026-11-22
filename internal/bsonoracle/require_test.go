package bsonoracle

import (
	"encoding/json"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	// The blank import is what keeps the library requirement in go.mod before
	// any code uses the library (build doc step 1c).
	_ "github.com/hoarddb/hoarddb-bson/bson"
)

const libraryModulePath = "github.com/hoarddb/hoarddb-bson"

// releaseTag matches vX.Y.Z and nothing else: a pseudo-version
// (v0.0.0-20261024120000-abcdef123456) or a pre-release is not a tag the
// coordinator made (build doc §0.2).
var releaseTag = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

// goModJSON is the part of `go mod edit -json` this test reads.
type goModJSON struct {
	Require []struct {
		Path    string
		Version string
	}
	Replace []struct {
		Old struct{ Path string }
		New struct{ Path, Version string }
	}
}

// isLibraryModule reports whether modulePath is the library or one of its
// nested modules (oracle).
func isLibraryModule(modulePath string) bool {
	return modulePath == libraryModulePath || strings.HasPrefix(modulePath, libraryModulePath+"/")
}

// TestLibraryIsRequiredAtATag fails when go.mod pins hoarddb-bson, or its
// oracle module, at anything but a release tag, or replaces it: a replace or
// a pseudo-version builds here and nowhere else (build doc §0 rule 8).
func TestLibraryIsRequiredAtATag(t *testing.T) {
	editOutput, err := exec.Command("go", "mod", "edit", "-json").Output()
	if err != nil {
		t.Fatalf("reading go.mod with go mod edit -json: %v", err)
	}
	var parsedGoMod goModJSON
	if err := json.Unmarshal(editOutput, &parsedGoMod); err != nil {
		t.Fatalf("parsing go mod edit -json: %v", err)
	}
	libraryRequired := false
	for _, requirement := range parsedGoMod.Require {
		if !isLibraryModule(requirement.Path) {
			continue
		}
		if requirement.Path == libraryModulePath {
			libraryRequired = true
		}
		if !releaseTag.MatchString(requirement.Version) {
			t.Errorf("go.mod requires %s %s, which is not a vX.Y.Z tag. Pin a tag the coordinator pushed: GOFLAGS=-mod=mod go get %s@vX.Y.Z", requirement.Path, requirement.Version, requirement.Path)
		}
	}
	if !libraryRequired {
		t.Errorf("go.mod does not require %s. Run: GOFLAGS=-mod=mod go get %s@vX.Y.Z", libraryModulePath, libraryModulePath)
	}
	for _, replacement := range parsedGoMod.Replace {
		if isLibraryModule(replacement.Old.Path) {
			t.Errorf("go.mod replaces %s with %s %s. A library replace is never committed (build doc §0 rule 8); use a go.work inside hoardDB/ for untagged library work", replacement.Old.Path, replacement.New.Path, replacement.New.Version)
		}
	}
}
