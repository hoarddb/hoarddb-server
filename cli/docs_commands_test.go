package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/repofile"
)

// BUG-0155: the configuration page of the user docs told readers about a "Built-in
// `hoarddb health` command". The CLI has no such verb: an unknown first word is
// a positional argument that is ignored (BUG-0139), so a HEALTHCHECK built from
// that sentence runs a session instead of giving a verdict. The only commands
// the CLI dispatches by name are the keys of verbRunners, so a user page that
// shows `hoarddb <word>` for any other word is naming a command that does not
// exist.

// documentedCommand matches `hoarddb <word>` in code: inside a fenced block (at
// the start of a line, optionally after a "$ " prompt) or inside an inline code
// span. The word must start with a letter, so flags (`hoarddb -insecure`),
// placeholders and the `hoarddb-server` binary (the hyphen is not whitespace)
// are not commands. Prose outside code, such as "hoarddb store types" in a case
// study, is not matched: the pre-fix sentence used an inline span, so spans
// count.
var (
	fencedCommand = regexp.MustCompile(`^\s*(?:\$\s+)?hoarddb\s+([A-Za-z][A-Za-z0-9_-]*)`)
	inlineCommand = regexp.MustCompile("`hoarddb\\s+([A-Za-z][A-Za-z0-9_-]*)[^`]*`")
)

type documentedWord struct {
	page string
	line int
	word string
}

// commandWordsIn returns every word shown after `hoarddb` in code on the page.
func commandWordsIn(page, markdown string) []documentedWord {
	var found []documentedWord
	inFence := false
	for index, line := range strings.Split(markdown, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			if match := fencedCommand.FindStringSubmatch(line); match != nil {
				found = append(found, documentedWord{page, index + 1, match[1]})
			}
			continue
		}
		for _, match := range inlineCommand.FindAllStringSubmatch(line, -1) {
			found = append(found, documentedWord{page, index + 1, match[1]})
		}
	}
	return found
}

// TestUserPagesNameOnlyCLICommandsThatParse is BUG-0155's acceptance test: every
// `hoarddb <word>` a page of the user docs shows is a verb the CLI dispatches.
func TestUserPagesNameOnlyCLICommandsThatParse(t *testing.T) {
	userDocs, err := repofile.Find("docs/user")
	if err != nil {
		t.Fatalf("locating the user pages: %v", err)
	}
	pages, err := filepath.Glob(filepath.Join(userDocs, "*.md"))
	if err != nil || len(pages) == 0 {
		t.Fatalf("listing the user pages under %s: %v (found %d)", userDocs, err, len(pages))
	}
	sort.Strings(pages)

	shownVerbs := map[string]bool{}
	for _, page := range pages {
		markdown, err := os.ReadFile(page)
		if err != nil {
			t.Fatalf("reading %s: %v", page, err)
		}
		for _, shown := range commandWordsIn(filepath.Base(page), string(markdown)) {
			if _, isVerb := verbRunners[shown.word]; isVerb {
				shownVerbs[shown.word] = true
				continue
			}
			t.Errorf("%s:%d shows `hoarddb %s`, which is not a CLI command; the commands are %s",
				shown.page, shown.line, shown.word, verbList())
		}
	}
	// A scan that found nothing proves nothing: the pages do show these.
	for verb := range verbRunners {
		if !shownVerbs[verb] {
			t.Errorf("no user page shows `hoarddb %s`; the scan is not reading the pages it should", verb)
		}
	}
}

// TestTheCommandScanCatchesTheBUG0155Sentence feeds the scan the sentence the
// page carried, in both code forms, and a line it must leave alone.
func TestTheCommandScanCatchesTheBUG0155Sentence(t *testing.T) {
	markdown := "- **Health check:** Built-in `hoarddb health` command\n" +
		"```\n$ hoarddb health\nhoarddb-server -v\nhoarddb -insecure\n```\n" +
		"hoarddb store types, as prose\n"
	var words []string
	for _, shown := range commandWordsIn("configuration.md", markdown) {
		words = append(words, shown.word)
	}
	if len(words) != 2 || words[0] != "health" || words[1] != "health" {
		t.Fatalf("scan found %v, want [health health]", words)
	}
}

func verbList() string {
	verbs := make([]string, 0, len(verbRunners))
	for verb := range verbRunners {
		verbs = append(verbs, verb)
	}
	sort.Strings(verbs)
	return strings.Join(verbs, ", ")
}
