package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/hoarddb/hoarddb-server/internal/repofile"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runPlanForTest(t *testing.T, stdin string, terminal bool, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := runPlan(args, planEnv{stdin: strings.NewReader(stdin), stdout: &stdout, stderr: &stderr, stdinIsTerminal: terminal})
	return code, stdout.String(), stderr.String()
}

// TestPlanNoArgumentsIsUsage: exit 1, synopsis and example on stderr, stdout empty.
func TestPlanNoArgumentsIsUsage(t *testing.T) {
	code, stdout, stderr := runPlanForTest(t, "", false)
	if code != ExitUsage || stdout != "" || !strings.Contains(stderr, "Examples:") || !strings.Contains(stderr, "hoarddb plan model.json --json") {
		t.Fatalf("code %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

// TestPlanJSONStdoutIsOnlyJSON: stdout is one JSON value; chatter stays on stderr.
func TestPlanJSONStdoutIsOnlyJSON(t *testing.T) {
	code, stdout, _ := runPlanForTest(t, "", false, "testdata/plan/docs-blog.model.json", "--trace", "testdata/plan/docs-blog.trace.jsonl", "--json", "--out", "-")
	if code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("stdout is not one JSON object: %v\n%s", err, stdout)
	}
	for _, key := range []string{"inputs", "scorecard", "reductions", "suggestions", "wont_port", "cost", "hql"} {
		if _, ok := doc[key]; !ok {
			t.Errorf("missing key %s", key)
		}
	}
}

// TestPlanJSONRefusalIsOnlyJSON: a refusal under --json is {"error":{message,fix}} on stdout.
func TestPlanJSONRefusalIsOnlyJSON(t *testing.T) {
	code, stdout, stderr := runPlanForTest(t, "", false, "--json", "no-such-file.bson")
	if code != ExitUsage || stderr == "" {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	var doc struct{ Error struct{ Message, Fix string } }
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil || doc.Error.Message == "" {
		t.Fatalf("stdout %q: %v", stdout, err)
	}
}

// TestPlanRefusesToOverwriteAForeignFile: --out never replaces a file the advisor did not write.
func TestPlanRefusesToOverwriteAForeignFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "notes.md")
	if err := os.WriteFile(out, []byte("my notes\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"testdata/plan/docs-blog.model.json", "--out", out}
	code, _, stderr := runPlanForTest(t, "", false, args...)
	if code != ExitUsage || !strings.Contains(stderr, "--force") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if data, _ := os.ReadFile(out); string(data) != "my notes\n" {
		t.Fatalf("the foreign file changed: %q", data)
	}
	if code, _, _ := runPlanForTest(t, "", false, append(args, "--force")...); code != ExitOK {
		t.Fatalf("--force exit %d", code)
	}
	if code, _, _ := runPlanForTest(t, "", false, args...); code != ExitOK {
		t.Fatalf("a plan file is replaced without --force: exit %d", code)
	}
}

// TestPlanAsksOnATerminalAndSavesAnswers: the second run, from the saved file, asks nothing.
func TestPlanAsksOnATerminalAndSavesAnswers(t *testing.T) {
	dir := t.TempDir()
	saved := filepath.Join(dir, "answers.jsonl")
	base := []string{"testdata/plan/docs-blog.model.json", "--trace", "testdata/plan/docs-blog.trace.jsonl", "--out", "-"}
	code, stdout, stderr := runPlanForTest(t, "2\n", true, append(base, "--save-answers", saved)...)
	if code != ExitOK || !strings.Contains(stderr, "store field of Post, or its own bucket?") || strings.Contains(stdout, "open") {
		t.Fatalf("exit %d\nstderr %s\nstdout %s", code, stderr, stdout)
	}
	data, _ := os.ReadFile(saved)
	if !strings.Contains(string(data), `"answer":"bucket"`) {
		t.Fatalf("saved answers %q", data)
	}
	code, again, stderr := runPlanForTest(t, "", false, append(base, "--answers", saved)...)
	if code != ExitOK || again != stdout || strings.Contains(stderr, "?") {
		t.Fatalf("rerun differs or asked (exit %d)", code)
	}
}

// TestPlanNoInputLeavesChoicesOpenOnATerminal: --no-input never prompts.
func TestPlanNoInputLeavesChoicesOpenOnATerminal(t *testing.T) {
	code, stdout, stderr := runPlanForTest(t, "", true, "testdata/plan/docs-blog.model.json", "--trace", "testdata/plan/docs-blog.trace.jsonl", "--no-input", "--out", "-")
	if code != ExitOK || strings.Contains(stderr, "?") || !strings.Contains(stdout, "choice is open") {
		t.Fatalf("exit %d\n%s\n%s", code, stderr, stdout)
	}
}

// TestPlanFromIsPlannedNotSilentlyIgnored: --from is refused with the fix.
func TestPlanFromIsPlannedNotSilentlyIgnored(t *testing.T) {
	code, _, stderr := runPlanForTest(t, "", false, "--from", "app")
	if code != ExitUsage || !strings.Contains(stderr, "not available yet") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}

// TestPlanThresholdsPrints: --thresholds lists the constants and exits 0.
func TestPlanThresholdsPrints(t *testing.T) {
	code, stdout, _ := runPlanForTest(t, "", false, "--thresholds")
	if code != ExitOK || !strings.Contains(stdout, "BucketDeclBytes = 436") {
		t.Fatalf("exit %d: %s", code, stdout)
	}
}

// TestPlanUserPageQuotesTheRealOutput pins the user page to the command: the
// model, the trace, the question and the whole plan on the page are what the
// command prints for them today.
func TestPlanUserPageQuotesTheRealOutput(t *testing.T) {
	root, err := repofile.Find("docs/user/plan.md")
	if err != nil {
		t.Fatalf("locating the page: %v", err)
	}
	page, err := os.ReadFile(root)
	if err != nil {
		t.Fatal(err)
	}
	read := func(name string) string {
		data, err := os.ReadFile("testdata/plan/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimRight(string(data), "\n")
	}
	_, plan, _ := runPlanForTest(t, "", false, "testdata/plan/docs-blog.model.json", "--trace", "testdata/plan/docs-blog.trace.jsonl", "--no-input", "--out", "-")
	outPath := filepath.Join(t.TempDir(), "hoarddb-plan.md")
	_, _, summary := runPlanForTest(t, "", false, "testdata/plan/docs-blog.model.json", "--trace", "testdata/plan/docs-blog.trace.jsonl", "--no-input", "--out", outPath)
	summary = strings.TrimSpace(strings.Replace(summary, outPath, "hoarddb-plan.md", 1))
	_, _, question := runPlanForTest(t, "2\n", true, "testdata/plan/docs-blog.model.json", "--trace", "testdata/plan/docs-blog.trace.jsonl", "--out", "-")
	for name, want := range map[string]string{
		"the model":                           read("docs-blog.model.json"),
		"the trace":                           read("docs-blog.trace.jsonl"),
		"the plan":                            strings.TrimRight(plan, "\n"),
		"the question":                        strings.TrimRight(question, "\n"),
		"the summary line the command prints": summary,
	} {
		if !strings.Contains(string(page), want) {
			t.Errorf("docs/user/plan.md does not quote %s as the command prints it today:\n%s", name, want)
		}
	}
}

// TestPlanSingleJSONDumpIsADump (M2): one file named users.json whose lines are
// documents is a JSON Lines dump, not a broken model.
func TestPlanSingleJSONDumpIsADump(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.json")
	if err := os.WriteFile(path, []byte("{\"_id\":\"u1\",\"name\":\"Ada\"}\n{\"_id\":\"u2\",\"name\":\"Bo\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runPlanForTest(t, "", false, path, "--out", "-")
	if code != ExitOK || !strings.Contains(stdout, "users:") {
		t.Fatalf("exit %d\nstderr %s", code, stderr)
	}
}

// TestPlanHelpIsNotARefusal (M3): -h and --help print the usage with the flags and exit 0.
func TestPlanHelpIsNotARefusal(t *testing.T) {
	for _, flagName := range []string{"-h", "--help"} {
		code, stdout, stderr := runPlanForTest(t, "", false, flagName)
		if code != ExitOK || !strings.Contains(stdout, "Examples:") || !strings.Contains(stdout, "--save-answers") || stderr != "" {
			t.Fatalf("%s: exit %d, stdout %q, stderr %q", flagName, code, stdout, stderr)
		}
	}
}

// TestHoarddbHelpListsPlan (M3): the top-level -h names the verb with an example.
func TestHoarddbHelpListsPlan(t *testing.T) {
	var out bytes.Buffer
	fs := flag.NewFlagSet("hoarddb", flag.ContinueOnError)
	fs.SetOutput(&out)
	printUsage(fs)
	if !strings.Contains(out.String(), "dump|restore|ingest|plan") || !strings.Contains(out.String(), "hoarddb plan ") {
		t.Fatalf("usage lacks plan:\n%s", out.String())
	}
}

// TestPlanJSONRefusalForEveryJSONSpelling (m1).
func TestPlanJSONRefusalForEveryJSONSpelling(t *testing.T) {
	for _, spelling := range []string{"--json", "--json=true", "-json"} {
		_, stdout, _ := runPlanForTest(t, "", false, spelling, "--sample", "x", "no-such-file.bson")
		if !strings.HasPrefix(stdout, `{"error"`) {
			t.Fatalf("%s: stdout %q is not the JSON refusal", spelling, stdout)
		}
	}
}

// TestPlanSaveAnswersDoesNotOverwriteAForeignFile (m3).
func TestPlanSaveAnswersDoesNotOverwriteAForeignFile(t *testing.T) {
	foreign := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(foreign, []byte("keep me\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"testdata/plan/docs-blog.model.json", "--trace", "testdata/plan/docs-blog.trace.jsonl", "--out", "-", "--save-answers", foreign}
	code, _, stderr := runPlanForTest(t, "2\n", true, args...)
	if code != ExitUsage || !strings.Contains(stderr, "--force") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if data, _ := os.ReadFile(foreign); string(data) != "keep me\n" {
		t.Fatalf("the foreign file changed: %q", data)
	}
}
