package cli

import (
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/repofile"
)

// storeTypesPagePath is the "store-types" user-docs page in the private repository
// (docs/store-types.md in the published tree); repofile finds it in either.
const storeTypesPagePath = "docs/user/store-types.md"

// storeTypeSections maps a "## " heading of the page to the bucket type its
// Operations block is written for.
var storeTypeSections = []struct{ heading, bucketType string }{
	{"FIFO", "fifo"},
	{"LIFO", "lifo"},
	{"Hash", "hash"},
	{"B+Tree", "btree"},
	{"Heap", "heap"},
	{"Blob", "blob"},
}

// operationsBlock returns the statements of the first ```text fence that
// follows "### Operations" inside the page section whose "## " heading starts
// with heading: its non-blank lines that are not "--" comments.
func operationsBlock(t *testing.T, page, heading string) []string {
	t.Helper()
	var statements []string
	inSection, inOperations, inFence := false, false, false
	for _, line := range strings.Split(page, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "## "):
			inSection = strings.HasPrefix(strings.TrimPrefix(line, "## "), heading)
			inOperations = false
		case inSection && strings.HasPrefix(line, "### "):
			inOperations = strings.HasPrefix(line, "### Operations")
		case inOperations && strings.HasPrefix(trimmed, "```text"):
			inFence = true
		case inOperations && inFence && strings.HasPrefix(trimmed, "```"):
			return statements
		case inFence && trimmed != "" && !strings.HasPrefix(trimmed, "--"):
			statements = append(statements, trimmed)
		}
	}
	return statements
}

// promptedStatements returns every statement the page shows as typed at the
// prompt ("[h]oardDB [PhotoJournal]> db.User.insert(...)"), in page order, for
// the sessions in database db. A statement the page shows refused (its next
// line starts "CLI error:") is left out.
func promptedStatements(page, db string) []string {
	statements, _ := promptedSplit(page, db)
	return statements
}

func promptedSplit(page, db string) (accepted, refused []string) {
	prompt := regexp.MustCompile(`^\[h\]oardDB \[([^\]]+)\]> (.+)$`)
	lines := strings.Split(page, "\n")
	for i, line := range lines {
		match := prompt.FindStringSubmatch(line)
		if match == nil || match[1] != db {
			continue
		}
		if i+1 < len(lines) && strings.HasPrefix(lines[i+1], "CLI error:") {
			refused = append(refused, match[2])
			continue
		}
		accepted = append(accepted, match[2])
	}
	return accepted, refused
}

// schemaStatements returns the statements of the page's PhotoJournal schema
// block: the ```text fence under "Example: PhotoJournal schema", one statement a line.
func schemaStatements(t *testing.T, page string) []string {
	t.Helper()
	_, afterHeading, found := strings.Cut(page, "### Example: PhotoJournal schema")
	if !found {
		t.Fatal("the page has no \"### Example: PhotoJournal schema\" section")
	}
	_, fence, found := strings.Cut(afterHeading, "```text\n")
	if !found {
		t.Fatal("the PhotoJournal section has no ```text block")
	}
	block, _, _ := strings.Cut(fence, "```")
	var statements []string
	for _, line := range strings.Split(block, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			statements = append(statements, trimmed)
		}
	}
	return statements
}

// TestStoreTypesPageExamplesRun: the page's examples are what a developer
// copies first, so each one runs in a ring session against a real in-process
// server and must be accepted: every store type's Operations block, then the
// PhotoJournal schema and the session shown after it.
func TestStoreTypesPageExamplesRun(t *testing.T) {
	page := string(repofile.Read(t, storeTypesPagePath))

	for _, section := range storeTypeSections {
		if section.heading == "Heap" || section.heading == "Blob" {
			continue // declared and read only: the page shows no Operations block for them
		}
		t.Run(section.heading, func(t *testing.T) {
			statements := operationsBlock(t, page, section.heading)
			if len(statements) == 0 {
				t.Fatalf("the %s section has no fenced Operations block", section.heading)
			}
			session, ctx := u52LiveSession(t)
			for _, line := range append([]string{"create database app", "use app"}, statements...) {
				if _, err := session.Execute(ctx, line); err != nil {
					t.Errorf("%s\n  failed: %v", line, err)
				}
			}
		})
	}

	t.Run("PhotoJournal", func(t *testing.T) {
		session, ctx := u52LiveSession(t)
		schema := schemaStatements(t, page)
		script := schema
		script = append(script, promptedStatements(page, "PhotoJournal")...)
		for _, line := range script {
			if _, err := session.Execute(ctx, line); err != nil {
				t.Errorf("%s\n  failed: %v", line, err)
			}
		}
	})
}

// TestStoreTypesPageDescribesTheSharedLayout is the other half of the shared-layout check:
// every bucket is a key range in the node's one store, so no sentence on the
// page may give a bucket a storage directory, instance or segment files of its
// own.
func TestStoreTypesPageDescribesTheSharedLayout(t *testing.T) {
	page := string(repofile.Read(t, storeTypesPagePath))
	stale := []string{
		"instance per bucket",
		"/data/MyApp",
		"seg_0",
		"segment file",
		"badger/",
		".ldb",
		"state.bson",
		"An in-memory max-heap",
		"Rebuilt from BadgerDB on startup",
	}
	for _, phrase := range stale {
		if strings.Contains(page, phrase) {
			t.Errorf("the page still says %q, which describes the per-bucket layout", phrase)
		}
	}
}

// ringPageStatements returns the statements a ring-mode page shows in its
// fences, in page order: the ones typed at a prompt ("[h]oardDB [media]> db...")
// and the bare db., create, alter and use lines of a fence without prompts.
func ringPageStatements(page string) []string {
	prompt := regexp.MustCompile(`^\[h\]oardDB(?: \[[^\]]+\])?> (.+)$`)
	bare := regexp.MustCompile(`^(db\.|create |alter |use )`)
	var statements []string
	inFence := false
	for _, line := range strings.Split(page, "\n") {
		if strings.HasPrefix(line, "```") {
			inFence = !inFence
			continue
		}
		if !inFence {
			continue
		}
		if match := prompt.FindStringSubmatch(line); match != nil {
			statements = append(statements, match[1])
		} else if bare.MatchString(line) && !strings.ContainsAny(line, "<>") && !strings.Contains(line, "…") {
			statements = append(statements, line)
		}
	}
	return statements
}

// TestRingPageExamplesRun runs the statements of the large-files and indexes
// pages, in page order, in a ring session against a real in-process server.
func TestRingPageExamplesRun(t *testing.T) {
	for _, name := range []string{"indexes.md", "large-files.md"} {
		t.Run(name, func(t *testing.T) {
			page := string(repofile.Read(t, "docs/user/"+name))
			statements := ringPageStatements(page)
			if len(statements) < 3 {
				t.Fatalf("%s shows only %d runnable statements: the scan stopped matching", name, len(statements))
			}
			session, ctx := u52LiveSession(t)
			if _, err := session.Execute(ctx, "create database app"); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Execute(ctx, "use app"); err != nil {
				t.Fatal(err)
			}
			for _, line := range statements {
				if strings.HasPrefix(line, "create database ") || line == "use media" {
					continue // the page's own database; the test works in app
				}
				if _, err := session.Execute(ctx, line); err != nil {
					t.Errorf("%s\n  failed: %v", line, err)
				}
			}
		})
	}
}
