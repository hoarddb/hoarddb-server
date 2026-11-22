package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/termio"
)

// continuationPrompt is shown while a statement is still open. It is plain
// rather than themed: the themed prompt carries the database name, which would
// make the second line of a statement look like the start of a new one.
const continuationPrompt = "...> "

// openDelimiter reports the outermost brace or bracket still open at the end of
// text. A brace inside a quoted string is text, not structure, and a `--`
// comment runs to the end of its line, so neither counts.
//
// It reads quotes the way splitTopLevel in declare.go does (' or ", no escape
// sequences), so a statement this function calls complete is one the body
// splitter will not refuse as unclosed. A quote left open at depth zero is not
// a continuation: a lone apostrophe in a one-line statement was accepted before
// statements could span lines, and stays accepted.
func openDelimiter(text string) (byte, bool) {
	var open []byte
	var quote byte
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '-' && i+1 < len(text) && text[i+1] == '-':
			// A `--` comment runs to the end of its line. The loop's i++
			// steps past the newline that ends it, or past the input.
			if nl := strings.IndexByte(text[i:], '\n'); nl >= 0 {
				i += nl
			} else {
				i = len(text)
			}
		case c == '{' || c == '[':
			open = append(open, c)
		case (c == '}' || c == ']') && len(open) > 0:
			open = open[:len(open)-1]
		}
	}
	// A quote left open at the end of a line cannot be closed by a later
	// line: a string holds no newline. End the statement here so the parser
	// reports the unterminated string at once, not at end of input.
	if quote != 0 && len(open) > 0 {
		return 0, false
	}
	if len(open) == 0 {
		return 0, false
	}
	return open[0], true
}

// readStatement reads input lines until they form one complete statement: the
// braces and brackets opened so far are closed. read fetches one line for the
// prompt it is given. It returns the statement with its lines joined by
// newlines, and a one-line form for the editor's history, because a recalled
// multi-line entry would break the single-line redraw.
//
// Blank lines between statements are skipped. Ctrl-C on any line surfaces as
// termio.ErrInterrupt from read and abandons the pending statement. End of
// input with a statement open is an error naming the open character and the
// line the statement began on; lineNumber counts lines read across calls.
func (c *CLI) readStatement(read func(prompt string) (string, error), lineNumber *int) (statement, historyEntry string, err error) {
	var lines []string
	startLine := 0
	for {
		prompt := c.prompt()
		if len(lines) > 0 {
			prompt = continuationPrompt
		}
		raw, err := read(prompt)
		if err != nil {
			if len(lines) > 0 && errors.Is(err, termio.ErrEOF) {
				opener, _ := openDelimiter(strings.Join(lines, "\n"))
				return "", "", unfinishedStatement(opener, startLine, lines)
			}
			return "", "", err
		}
		*lineNumber++
		if len(lines) == 0 {
			if strings.TrimSpace(raw) == "" {
				continue
			}
			startLine = *lineNumber
		}
		lines = append(lines, raw)

		joined := strings.Join(lines, "\n")
		if _, stillOpen := openDelimiter(joined); !stillOpen {
			trimmed := make([]string, len(lines))
			for i, line := range lines {
				trimmed[i] = strings.TrimSpace(line)
			}
			return strings.TrimSpace(joined), strings.Join(trimmed, " "), nil
		}
	}
}

// unfinishedStatement is the error for input that ends inside a statement. It
// is a parse error, so a script sees PARSE_ERROR and exit 1 like any statement
// the CLI refuses to send. It names the line the statement began on, quotes
// that line so it can be found, and counts the later lines read and not run.
func unfinishedStatement(opener byte, startLine int, lines []string) error {
	closer := "}"
	if opener == '[' {
		closer = "]"
	}
	return fmt.Errorf("parse: %w", &UsageError{
		Message: fmt.Sprintf("the statement that starts at line %d (%s) is unfinished: its %q is never closed. "+
			"%s. Close it with %q, or write the statement on one line",
			startLine, quoteStatementLine(lines[0]), string(opener), laterLinesNotRun(len(lines)-1), closer),
		Parse: true,
	})
}

// quoteStatementLine quotes a statement's first line for an error: trimmed, and
// cut off with an ellipsis past a length that keeps the message readable.
func quoteStatementLine(line string) string {
	const statementLineLimit = 60
	runes := []rune(strings.TrimSpace(line))
	if len(runes) > statementLineLimit {
		return fmt.Sprintf("%q", string(runes[:statementLineLimit])+"...")
	}
	return fmt.Sprintf("%q", string(runes))
}

// laterLinesNotRun reports the lines a statement read after its first line and
// never ran, singular or plural.
func laterLinesNotRun(n int) string {
	if n == 1 {
		return "1 later line was read and not run"
	}
	return fmt.Sprintf("%d later lines were read and not run", n)
}
