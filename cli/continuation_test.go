package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/termio"
)

// scriptedReader hands readStatement one canned line per call and records the
// prompt each call was given, standing in for the terminal line editor.
type scriptedReader struct {
	steps   []scriptedStep
	prompts []string
}

type scriptedStep struct {
	line string
	err  error
}

func (r *scriptedReader) read(prompt string) (string, error) {
	r.prompts = append(r.prompts, prompt)
	if len(r.steps) == 0 {
		return "", termio.ErrEOF
	}
	step := r.steps[0]
	r.steps = r.steps[1:]
	return step.line, step.err
}

func TestReadStatementJoinsLinesUntilBalanced(t *testing.T) {
	cli := &CLI{}
	reader := &scriptedReader{steps: []scriptedStep{
		{line: "create bucket ML {"}, {line: "  Caption string,"}, {line: "  Photos blobStore"}, {line: "};"},
	}}
	lineNumber := 0
	statement, historyEntry, err := cli.readStatement(reader.read, &lineNumber)
	if err != nil {
		t.Fatalf("readStatement: %v", err)
	}
	if want := "create bucket ML {\n  Caption string,\n  Photos blobStore\n};"; statement != want {
		t.Errorf("statement = %q, want %q", statement, want)
	}
	if want := "create bucket ML { Caption string, Photos blobStore };"; historyEntry != want {
		t.Errorf("history entry = %q, want the one-line form %q", historyEntry, want)
	}
	wantPrompts := []string{"hoardDB> ", continuationPrompt, continuationPrompt, continuationPrompt}
	if strings.Join(reader.prompts, "|") != strings.Join(wantPrompts, "|") {
		t.Errorf("prompts = %q, want %q", reader.prompts, wantPrompts)
	}
}

func TestReadStatementCtrlCAbandonsThePendingStatement(t *testing.T) {
	cli := &CLI{}
	reader := &scriptedReader{steps: []scriptedStep{
		{line: "create bucket ML {"}, {err: termio.ErrInterrupt},
		{line: "show buckets"},
	}}
	lineNumber := 0
	if _, _, err := cli.readStatement(reader.read, &lineNumber); !errors.Is(err, termio.ErrInterrupt) {
		t.Fatalf("readStatement after Ctrl-C = %v, want ErrInterrupt", err)
	}
	statement, _, err := cli.readStatement(reader.read, &lineNumber)
	if err != nil || statement != "show buckets" {
		t.Fatalf("the statement after Ctrl-C = %q, %v; want %q with the abandoned half gone", statement, err, "show buckets")
	}
	if got := reader.prompts[2]; got != "hoardDB> " {
		t.Errorf("prompt after Ctrl-C = %q, want the main prompt", got)
	}
}

// TestReadStatementCommentBraceDoesNotContinue: a `--` comment that holds an
// unbalanced brace or bracket is not an open statement, so the next line is
// read as its own statement rather than swallowed looking for the close.
func TestReadStatementCommentBraceDoesNotContinue(t *testing.T) {
	for _, line := range []string{"show buckets -- {", "show buckets -- ["} {
		cli := &CLI{}
		reader := &scriptedReader{steps: []scriptedStep{{line: line}, {line: "show buckets"}}}
		lineNumber := 0
		statement, _, err := cli.readStatement(reader.read, &lineNumber)
		if err != nil {
			t.Fatalf("readStatement(%q): %v", line, err)
		}
		if statement != line {
			t.Errorf("statement = %q, want %q", statement, line)
		}
		second, _, err := cli.readStatement(reader.read, &lineNumber)
		if err != nil || second != "show buckets" {
			t.Errorf("statement after %q = %q, %v; want %q, nil", line, second, err, "show buckets")
		}
	}
}

// TestUnfinishedStatementQuotesTheFirstLineAndCountsTheLinesNotRun: the error
// at end of input must name the line it began on, quote that line so it can be
// found, and say how many later lines were read and never run.
func TestUnfinishedStatementQuotesTheFirstLineAndCountsTheLinesNotRun(t *testing.T) {
	cli := &CLI{}
	reader := &scriptedReader{steps: []scriptedStep{
		{line: "create bucket ML {"},
		{line: "  Caption string,"},
		{line: "  Photos blobStore"},
	}}
	lineNumber := 0
	_, _, err := cli.readStatement(reader.read, &lineNumber)
	if err == nil {
		t.Fatal("end of input inside a statement returned no error")
	}
	for _, want := range []string{`"{"`, "line 1", `("create bucket ML {")`, "2 later lines were read and not run", `Close it with "}"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func TestReadStatementEndOfInputNamesTheOpenCharacterAndLine(t *testing.T) {
	cli := &CLI{}
	reader := &scriptedReader{steps: []scriptedStep{{line: ""}, {line: "show buckets"}, {line: "db.X.find([1,"}, {line: "2"}}}
	lineNumber := 0
	if _, _, err := cli.readStatement(reader.read, &lineNumber); err != nil {
		t.Fatalf("first statement: %v", err)
	}
	_, _, err := cli.readStatement(reader.read, &lineNumber)
	if err == nil {
		t.Fatal("end of input inside a statement returned no error")
	}
	for _, want := range []string{`"["`, "line 3", `"]"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func TestOpenDelimiter(t *testing.T) {
	cases := []struct {
		text     string
		wantOpen byte
	}{
		{"show buckets", 0},
		{"create bucket A { type: hash }", 0},
		{"create bucket A {", '{'},
		{"db.X.find([1, {a: 2}", '['},
		{`db.X.insert({"t": "a { in a string"})`, 0},
		{`db.X.insert({"t": "a } in a string"`, '{'},
		{"help it's", 0},
		{"create bucket A { note: 'x\ny' }", 0},
		// A `--` comment runs to the end of its line, so a brace or bracket
		// in it is text and neither opens nor closes a continuation.
		{"show buckets -- {", 0},
		{"show buckets -- [", 0},
		{"create bucket A { -- }", '{'},
		{"create bucket A { -- {\n}", 0},
		{"create bucket A { -- }\n", '{'},
		// A `--` inside a quoted string is text, not a comment.
		{`db.X.insert({"t": "a--{", "u": 1})`, 0},
		// A quote left open at the end of a line cannot close on a later
		// line (a string holds no newline), so the statement ends there and
		// the parser reports the unterminated string.
		{`db.X.insert({"a": "x})`, 0},
	}
	for _, tc := range cases {
		got, open := openDelimiter(tc.text)
		if got != tc.wantOpen || open != (tc.wantOpen != 0) {
			t.Errorf("openDelimiter(%q) = %q, %v; want %q", tc.text, got, open, tc.wantOpen)
		}
	}
}
