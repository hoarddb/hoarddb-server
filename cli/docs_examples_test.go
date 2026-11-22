package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/internal/repofile"
)

// The page-example runner: a user page shows a command and what it prints, and a test runs the
// command against a real in-process ring server and compares the first lines of what came back
// with the lines the page shows, so a page cannot drift from what the command does.
//
// A page marks an example with a fenced block whose info string is "sh", under a heading. In the
// block a line that starts with "$ " is a command; the lines after it, up to the next "$ " line,
// are the first lines of the output. A command that starts with "hoarddb <verb>" runs that verb
// (pageVerbs); any other command is a statement typed at the ring prompt. The shell-only words
// the server does not need are dropped before a verb runs: --address, --user, --password and
// --insecure (and the single-dash forms), because the test host's own login is added.
// Two shapes are normalised on both sides, because they vary between runs: the elapsed time and
// the rate at the end of a summary line, which a page writes as "<time>" and "<rate>".

// pageHost is what a page's examples run against: a ring test host, a session on it, and a
// directory holding the input files the page names.
type pageHost struct {
	addr    string
	session *RingSession
	ctx     context.Context
	dir     string
	// files maps a file name the page uses to its content; each is written under dir.
	files map[string][]byte
}

// newPageHost starts the ring test host and writes the page's input files.
func newPageHost(t *testing.T, files map[string][]byte) pageHost {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	addr, client := newRingTestHost(t)
	host := pageHost{addr: addr, session: NewRingSession(NewRingAPI(client)), ctx: ctx, dir: t.TempDir(), files: files}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(host.dir, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return host
}

// pageVerbs are the verbs a page example may run. Each takes the command's arguments with the
// host's login added and returns what the verb printed, standard output first.
var pageVerbs = map[string]func(t *testing.T, host pageHost, args []string) string{
	"ingest": func(t *testing.T, host pageHost, args []string) string {
		_, stdout, stderr := ingestOnHost(t, host.addr, args...)
		return stdout + stderr
	},
	"dump": func(t *testing.T, host pageHost, args []string) string {
		_, stdout, stderr := dumpOnHost(t, host.addr, args...)
		return stdout + stderr
	},
	// With --verify the verb reads the file and contacts no server; without it the verb
	// restores into the host's ring, which is how a page shows a refusal and a restore under a
	// new name. The verb writes to the process's own streams, which captureProcessStreams
	// redirects, when it is only checking.
	"restore": func(t *testing.T, host pageHost, args []string) string {
		if slices.Contains(args, "--verify") {
			_, stdout, stderr := captureProcessStreams(t, func() int {
				return verbRunners["restore"](args)
			})
			return stdout + stderr
		}
		_, stdout, stderr := restoreOnHost(t, host.addr, args...)
		return stdout + stderr
	},
}

// pageExample is one command of a page and the first lines it must print.
type pageExample struct {
	line    int
	command string
	want    []string
}

// pageExamples returns the examples of the "sh" blocks under heading, in page order. A heading
// is the text after the leading hashes; its section ends at the next heading of the same or a
// higher level.
func pageExamples(page, heading string) ([]pageExample, error) {
	var examples []pageExample
	level, inSection, inBlock := 0, false, false
	found := false
	for index, line := range strings.Split(page, "\n") {
		trimmed := strings.TrimSpace(line)
		if !inBlock && strings.HasPrefix(trimmed, "#") {
			hashes := len(trimmed) - len(strings.TrimLeft(trimmed, "#"))
			text := strings.TrimSpace(trimmed[hashes:])
			switch {
			case text == heading:
				level, inSection, found = hashes, true, true
			case inSection && hashes <= level:
				inSection = false
			}
			continue
		}
		if !inSection {
			continue
		}
		if strings.HasPrefix(trimmed, "```") {
			if inBlock {
				inBlock = false
			} else {
				inBlock = strings.TrimSpace(strings.TrimPrefix(trimmed, "```")) == "sh"
			}
			continue
		}
		if !inBlock {
			continue
		}
		if command, isCommand := strings.CutPrefix(line, "$ "); isCommand {
			examples = append(examples, pageExample{line: index + 1, command: strings.TrimSpace(command)})
			continue
		}
		if len(examples) == 0 {
			return nil, fmt.Errorf("line %d of the page is output with no command above it", index+1)
		}
		last := &examples[len(examples)-1]
		last.want = append(last.want, line)
	}
	if !found {
		return nil, fmt.Errorf("the page has no heading %q", heading)
	}
	if len(examples) == 0 {
		return nil, fmt.Errorf("the section %q has no sh block with a command", heading)
	}
	for index := range examples {
		for len(examples[index].want) > 0 && strings.TrimSpace(examples[index].want[len(examples[index].want)-1]) == "" {
			examples[index].want = examples[index].want[:len(examples[index].want)-1]
		}
	}
	return examples, nil
}

var pageVariableTail = regexp.MustCompile(` in (?:<time>|[0-9.]+(?:ms|µs|s)|[0-9]+m[0-9]{2}s|[0-9]+h[0-9]{2}m)(?: \((?:<rate>|[0-9,]+) docs/s\))?`)

// normalisePageLine rewrites what varies between runs to a fixed word.
func normalisePageLine(line string) string {
	return strings.TrimRight(pageVariableTail.ReplaceAllString(line, " in <time>"), " ")
}

// splitCommand splits a command line into words, honouring single and double quotes.
func splitCommand(command string) []string {
	var words []string
	var current strings.Builder
	quote := rune(0)
	started := false
	for _, character := range command {
		switch {
		case quote != 0:
			if character == quote {
				quote = 0
			} else {
				current.WriteRune(character)
			}
		case character == '\'' || character == '"':
			quote, started = character, true
		case character == ' ' || character == '\t':
			if started || current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
				started = false
			}
		default:
			current.WriteRune(character)
		}
	}
	if started || current.Len() > 0 {
		words = append(words, current.String())
	}
	return words
}

// verbArguments drops the connection words the host supplies and points every input file and
// every output file at the host's directory.
func (host pageHost) verbArguments(words []string) []string {
	var arguments []string
	for index := 0; index < len(words); index++ {
		word := words[index]
		name := strings.TrimLeft(word, "-")
		switch {
		case word == "--insecure" || word == "-insecure":
			continue
		case strings.HasPrefix(word, "-") && (name == "address" || name == "user" || name == "password"):
			index++ // its value
			continue
		case strings.HasPrefix(word, "-") && (name == "rejects" || name == "out") && index+1 < len(words):
			arguments = append(arguments, word, filepath.Join(host.dir, words[index+1]))
			index++
			continue
		}
		if _, isFile := host.files[word]; isFile {
			word = filepath.Join(host.dir, word)
		}
		// A dump file is written by one example and read by a later one, so both name it
		// by the name the page shows and the runner keeps it in the host's directory.
		if strings.HasSuffix(word, ".hoarddump") {
			word = filepath.Join(host.dir, word)
		}
		arguments = append(arguments, word)
	}
	return arguments
}

// outputOf runs one example's command and returns what it printed.
func (host pageHost) outputOf(t *testing.T, command string) string {
	t.Helper()
	words := splitCommand(command)
	if len(words) >= 2 && words[0] == "hoarddb" {
		run, known := pageVerbs[words[1]]
		if !known {
			return fmt.Sprintf("the runner has no verb %q", words[1])
		}
		return host.stripDir(run(t, host, host.verbArguments(words[2:])))
	}
	output, err := host.session.Execute(host.ctx, command)
	text := strings.Join(output.Lines, "\n")
	if err != nil {
		text += "\n" + err.Error()
	}
	return host.stripDir(text)
}

// stripDir removes the host's directory from a line, because a page writes the file name it
// passed (`app.hoarddump`) and the verb prints the path it wrote. Like the time words, the
// directory is a run detail the page cannot show.
func (host pageHost) stripDir(text string) string {
	return strings.ReplaceAll(text, host.dir+string(filepath.Separator), "")
}

// checkPageExamples runs the examples under heading and returns one text per line that differs.
func checkPageExamples(t *testing.T, page, heading string, host pageHost) []string {
	t.Helper()
	examples, err := pageExamples(page, heading)
	if err != nil {
		return []string{err.Error()}
	}
	var mismatches []string
	for _, example := range examples {
		var got []string
		for _, line := range strings.Split(host.outputOf(t, example.command), "\n") {
			if strings.HasPrefix(line, "WARNING: TLS certificate verification is disabled") {
				continue // the test host's own trust flag, which the page's command does not show
			}
			got = append(got, normalisePageLine(line))
		}
		for position, want := range example.want {
			want = normalisePageLine(want)
			switch {
			case position >= len(got):
				mismatches = append(mismatches, fmt.Sprintf("line %d, %q: the page shows %q as output line %d and the command printed only %d lines:\n%s",
					example.line, example.command, want, position+1, len(got), strings.Join(got, "\n")))
			case got[position] != want:
				mismatches = append(mismatches, fmt.Sprintf("line %d, %q: output line %d is %q and the page shows %q",
					example.line, example.command, position+1, got[position], want))
			}
		}
	}
	return mismatches
}

// runPageExamples runs the examples of the page's section and fails the test for each line that
// differs from what the page shows.
func runPageExamples(t *testing.T, page, heading string, host pageHost) {
	t.Helper()
	for _, mismatch := range checkPageExamples(t, page, heading, host) {
		t.Error(mismatch)
	}
}

// usersFixtureFiles is a small BSON file of three users, for the pages that load one.
func usersFixtureFiles(t *testing.T) map[string][]byte {
	t.Helper()
	return map[string][]byte{"users.bson": bsonFileOf(t,
		bson.D{{Key: "_id", Value: "ada"}, {Key: "name", Value: "Ada"}},
		bson.D{{Key: "_id", Value: "grace"}, {Key: "name", Value: "Grace"}},
		bson.D{{Key: "_id", Value: "alan"}, {Key: "name", Value: "Alan"}},
	)}
}

const runnerSelfTestPage = "# A page\n\n## Load it\n\n```sh\n" +
	"$ hoarddb ingest users.bson --into app.users --address 127.0.0.1:7433 --insecure\n" +
	"ingest: users.bson → app.users — 3 read, 3 written in <time>\n" +
	"  created database app\n" +
	"$ show buckets in app\n" +
	"```\n\n## Elsewhere\n\n```sh\n$ not run\n```\n"

// TestPageExampleRunnerRunsTheExamplesOfOneSection: the runner runs the verb and a statement
// against the ring and finds the page's lines in the output.
func TestPageExampleRunnerRunsTheExamplesOfOneSection(t *testing.T) {
	host := newPageHost(t, usersFixtureFiles(t))
	runPageExamples(t, runnerSelfTestPage, "Load it", host)
	if _, err := host.session.Execute(host.ctx, "use app"); err != nil {
		t.Fatalf("the example did not load the bucket: %v", err)
	}
}

// TestPageExampleRunnerFailsWhenALineOfThePageDrifts: a word changed in the page's output is
// reported, so the page cannot say what the command no longer does.
func TestPageExampleRunnerFailsWhenALineOfThePageDrifts(t *testing.T) {
	host := newPageHost(t, usersFixtureFiles(t))
	drifted := strings.Replace(runnerSelfTestPage, "3 written", "4 written", 1)
	mismatches := checkPageExamples(t, drifted, "Load it", host)
	if len(mismatches) != 1 || !strings.Contains(mismatches[0], "4 written") {
		t.Fatalf("a drifted line gave %q, want one mismatch naming it", mismatches)
	}
}

// TestIngestPageExamplesRun: the first example of the ingest page runs against a ring and
// prints the lines the page shows, so the page cannot drift from the verb it documents.
func TestIngestPageExamplesRun(t *testing.T) {
	page := string(repofile.Read(t, "docs/user/ingest.md"))
	host := newPageHost(t, usersFixtureFiles(t))
	runPageExamples(t, page, "Load your first file", host)
}

// TestIngestPageExamplesFailWhenALineIsEdited: an output line of the page that no longer
// matches what the command prints is reported, which is what makes the runner a gate.
func TestIngestPageExamplesFailWhenALineIsEdited(t *testing.T) {
	page := string(repofile.Read(t, "docs/user/ingest.md"))
	if !strings.Contains(page, "3 read") {
		t.Fatalf("the page's first example does not show the %q of its summary line, so this test cannot edit it", "3 read")
	}
	edited := strings.Replace(page, "3 read", "4 read", 1)
	host := newPageHost(t, usersFixtureFiles(t))
	if mismatches := checkPageExamples(t, edited, "Load your first file", host); len(mismatches) == 0 {
		t.Fatal("a page line changed to 4 read was not reported as a mismatch")
	}
}

// TestBackupRestorePageExamplesRun: the commands of the backup page's "Take a dump" and
// "Restore a dump" sections run against a ring whose app.users holds three documents, and print the lines
// the page shows. The restore example reads the file the dump example wrote, so the page
// cannot show a file check that never ran against that dump.
func TestBackupRestorePageExamplesRun(t *testing.T) {
	page := string(repofile.Read(t, "docs/user/backup-restore.md"))
	host := newPageHost(t, nil)
	for _, statement := range []string{
		"create database app",
		"use app",
		"create bucket users { name string }",
		`db.users.insert({_id_: "ada", name: "Ada"})`,
		`db.users.insert({_id_: "grace", name: "Grace"})`,
		`db.users.insert({_id_: "alan", name: "Alan"})`,
	} {
		if _, err := host.session.Execute(host.ctx, statement); err != nil {
			t.Fatalf("seeding the page's host with %q: %v", statement, err)
		}
	}
	runPageExamples(t, page, "Take a dump", host)
	// The restore half reads the file the dump example just wrote: the first example is the
	// refusal a ring that already holds the database gives, the second restores it under
	// another name.
	runPageExamples(t, page, "Restore a dump", host)
}

// TestPlanPageLoadLineRuns: the load line the plan page shows is one command a developer
// can paste, and it prints the lines the page shows.
func TestPlanPageLoadLineRuns(t *testing.T) {
	page := string(repofile.Read(t, "docs/user/plan.md"))
	host := newPageHost(t, usersFixtureFiles(t))
	runPageExamples(t, page, "Run what you picked", host)
}

// TestPageExampleRunnerRefusesAPageItCannotReadExamplesFrom: a missing heading and a section
// with no command are failures, not a pass over nothing.
func TestPageExampleRunnerRefusesAPageItCannotReadExamplesFrom(t *testing.T) {
	for _, tc := range []struct{ page, heading, want string }{
		{runnerSelfTestPage, "Not there", `no heading "Not there"`},
		{"## Empty\n\ntext only\n", "Empty", "no sh block with a command"},
	} {
		if _, err := pageExamples(tc.page, tc.heading); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("pageExamples(%q) = %v, want an error containing %q", tc.heading, err, tc.want)
		}
	}
}
