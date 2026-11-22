package bsonoracle

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The library keeps copies of the files hoardDB owns and it must agree with
// (scripts/sync-bson-library.sh makes them): the vectors, the lint-tool pin
// and the Go version. A library CI cannot read clients/, which stays private,
// so this is the check that a copy has not drifted from its source
// (bson-codec-spec §9.1.4, build doc step 1d).
const (
	clientsTestdataPath = "../../../clients/testdata"
	lintToolSourcePath  = "../../../scripts/lint-tool.sh"
	privateTreeMarker   = "../../../scripts/ci.sh"
	// syncedFromVersion is the first library tag that carries testdata/vectors.
	syncedFromVersion = "v0.2.0"
)

// libraryModule is `go list -m` for the library: where it is on disk, and
// which version resolved ("" when a go.work or a replace points at a
// directory).
func libraryModule(t *testing.T) (directory, version string) {
	t.Helper()
	listed, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}\t{{.Version}}", libraryModulePath).Output()
	if err != nil {
		t.Fatalf("finding the pinned library with go list -m: %v", err)
	}
	directory, version, _ = strings.Cut(strings.TrimSpace(string(listed)), "\t")
	return directory, version
}

// versionBefore reports whether release tag a sorts before release tag b.
// Both are vMAJOR.MINOR.PATCH.
func versionBefore(a, b string) bool {
	numbers := func(tag string) [3]int {
		var parts [3]int
		for position, text := range strings.SplitN(strings.TrimPrefix(tag, "v"), ".", 3) {
			parts[position], _ = strconv.Atoi(text)
		}
		return parts
	}
	left, right := numbers(a), numbers(b)
	for position := range left {
		if left[position] != right[position] {
			return left[position] < right[position]
		}
	}
	return false
}

func hashOfFile(t *testing.T, path string) string {
	t.Helper()
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	fileHash := sha256.Sum256(fileBytes)
	return hex.EncodeToString(fileHash[:])
}

// filesUnder lists the regular files under root as slash paths relative to it.
func filesUnder(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		found = append(found, filepath.ToSlash(relative))
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walking %s: %v", root, walkErr)
	}
	sort.Strings(found)
	return found
}

// serverGoVersion is the go directive of hoardDB/go.mod.
func serverGoVersion(t *testing.T) string {
	t.Helper()
	moduleText, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	goLine := regexp.MustCompile(`^go\s+([0-9]+\.[0-9]+(?:\.[0-9]+)?)\s*$`)
	scanner := bufio.NewScanner(strings.NewReader(string(moduleText)))
	for scanner.Scan() {
		if match := goLine.FindStringSubmatch(scanner.Text()); match != nil {
			return match[1]
		}
	}
	t.Fatal("go.mod has no go directive")
	return ""
}

func TestLibraryCopiesMatchTheirSources(t *testing.T) {
	if _, err := os.Stat(clientsTestdataPath); errors.Is(err, fs.ErrNotExist) {
		if _, markerErr := os.Stat(privateTreeMarker); errors.Is(markerErr, fs.ErrNotExist) {
			t.Skipf("published server tree: clients/ is not part of it, so there are no sources at %s to compare the library's copies with", clientsTestdataPath)
		}
		t.Fatalf("%s is missing from the private tree", clientsTestdataPath)
	}

	libraryDir, libraryVersion := libraryModule(t)
	copiesDir := filepath.Join(libraryDir, "testdata", "vectors")
	if _, err := os.Stat(copiesDir); errors.Is(err, fs.ErrNotExist) {
		if libraryVersion != "" && versionBefore(libraryVersion, syncedFromVersion) {
			t.Skipf("the pinned library %s predates %s, the first tag with testdata/vectors; the comparison starts when go.mod requires %s", libraryVersion, syncedFromVersion, syncedFromVersion)
		}
		t.Fatalf("the pinned library (%s %s) has no testdata/vectors at %s. Run scripts/sync-bson-library.sh against a library checkout and tag it", libraryModulePath, libraryVersion, copiesDir)
	}

	// The vectors the sync script copies (bson-vectors/v1.json and every file of
	// driver-vectors) are in the copy, byte for byte, and the copy holds nothing else but SHA256SUMS.
	expected := map[string]string{} // path relative to testdata/vectors -> source path
	expected["bson-vectors/v1.json"] = filepath.Join(clientsTestdataPath, "bson-vectors", "v1.json")
	for _, name := range filesUnder(t, filepath.Join(clientsTestdataPath, "driver-vectors")) {
		expected["driver-vectors/"+name] = filepath.Join(clientsTestdataPath, "driver-vectors", filepath.FromSlash(name))
	}
	for _, copied := range filesUnder(t, copiesDir) {
		if copied == "SHA256SUMS" {
			continue
		}
		if _, known := expected[copied]; !known {
			t.Errorf("the library's testdata/vectors/%s has no source under clients/testdata: remove it from the library, or sync again", copied)
		}
	}
	for copied, source := range expected {
		copyPath := filepath.Join(copiesDir, filepath.FromSlash(copied))
		if _, err := os.Stat(copyPath); err != nil {
			t.Errorf("the library lacks testdata/vectors/%s (source %s): %v", copied, source, err)
			continue
		}
		if hashOfFile(t, copyPath) != hashOfFile(t, source) {
			t.Errorf("the library's testdata/vectors/%s differs from %s. A vector is never edited to pass a build: run scripts/sync-bson-library.sh and tag the library", copied, source)
		}
	}

	// The lint-tool pin and the Go version.
	if hashOfFile(t, filepath.Join(libraryDir, "scripts", "lint-tool.sh")) != hashOfFile(t, lintToolSourcePath) {
		t.Errorf("the library's scripts/lint-tool.sh differs from %s: run scripts/sync-bson-library.sh", lintToolSourcePath)
	}
	libraryGoVersion, err := os.ReadFile(filepath.Join(libraryDir, ".server-go-version"))
	if err != nil {
		t.Fatalf("reading the library's .server-go-version: %v", err)
	}
	if got, want := strings.TrimSpace(string(libraryGoVersion)), serverGoVersion(t); got != want {
		t.Errorf("the library's .server-go-version is %s, go.mod's go directive is %s: run scripts/sync-bson-library.sh", got, want)
	}
}
