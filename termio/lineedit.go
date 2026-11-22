package termio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"
)

// ErrInterrupt is returned when the user presses Ctrl-C at a prompt. The caller
// decides what that means: the REPL abandons the line and keeps the session, a
// scripted caller treats it as a failure.
var ErrInterrupt = errors.New("interrupt")

// ErrEOF is returned when the user presses Ctrl-D on an empty line, or when the
// input stream ends.
var ErrEOF = errors.New("eof")

// maxHistory bounds the in-session history so a long session cannot grow
// without limit.
const maxHistory = 1000

// Completer returns the completion candidates for the word ending at pos in
// line, with the rune offset where that word starts. Candidates must already be
// filtered by the typed prefix; the editor does not filter again.
type Completer func(line string, pos int) (start int, candidates []string)

// LineEditor reads lines with in-session history, cursor editing and Tab
// completion. One editor is reused for the life of a REPL: the history and the
// history cursor live here, not in the caller.
type LineEditor struct {
	w         io.Writer
	completer Completer
	history   []string
	plain     bool
}

// NewLineEditor returns an editor writing its prompts and redraws to w. A nil
// writer means os.Stdout; a nil completer disables Tab completion.
func NewLineEditor(w io.Writer, completer Completer) *LineEditor {
	if w == nil {
		w = os.Stdout
	}
	return &LineEditor{w: w, completer: completer}
}

// SetPlain makes the editor read each line as a cooked line, the way it reads a
// pipe: the terminal's own line discipline echoes and edits, and nothing but the
// prompt is written. It is for a terminal that cannot take the editor's cursor
// escapes (TERM=dumb or unset) and for output that is not a terminal, where an
// erase-to-end-of-line would land in a file. History and completion are given up.
func (e *LineEditor) SetPlain(plain bool) { e.plain = plain }

// History returns the session's history, oldest first.
func (e *LineEditor) History() []string { return e.history }

// AddHistory appends a line, ignoring an empty one and a repeat of the entry
// immediately before it: pressing Up twice should reach the previous *command*,
// not the same one again.
func (e *LineEditor) AddHistory(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	if n := len(e.history); n > 0 && e.history[n-1] == line {
		return
	}
	e.history = append(e.history, line)
	if len(e.history) > maxHistory {
		e.history = e.history[len(e.history)-maxHistory:]
	}
}

// ReadLine writes prompt and returns one edited line. On a terminal it reads in
// raw mode, so Up/Down walk the history, the arrow keys and Ctrl-A/E move the
// cursor, and Tab completes. When stdin is not a terminal — a pipe or a file —
// it reads a plain line with no editing, which is what keeps `printf … | hoardDB`
// working as a script.
func (e *LineEditor) ReadLine(prompt string) (string, error) {
	fd := int(os.Stdin.Fd())
	if e.plain || !isTerminal(fd) {
		return e.readLinePlain(prompt)
	}
	return readLineRaw(fd, e.w, prompt, e)
}

// readLinePlain reads one line from the shared reader, for a pipe or a file.
func (e *LineEditor) readLinePlain(prompt string) (string, error) {
	_, _ = fmt.Fprint(e.w, prompt)
	line, err := stdin().ReadString('\n')
	if err != nil && line == "" {
		if errors.Is(err, io.EOF) {
			return "", ErrEOF
		}
		return "", fmt.Errorf("read line: %w", err)
	}
	return trimTrailingNewline(line), nil
}

// trimTrailingNewline strips the line ending a terminal or a pipe appended. It
// handles CRLF as well as LF, so a line piped from a file edited on another
// platform does not carry a stray carriage return into the parser.
func trimTrailingNewline(line string) string {
	return strings.TrimRight(line, "\r\n")
}

// applyCompletion performs one Tab press: the word ending at pos is replaced by
// the only candidate, or by the candidates' common prefix when that is longer
// than what was typed. When the candidates are ambiguous and share nothing new,
// they are returned for the caller to display.
//
// This is the whole completion decision, kept free of terminal state so it can
// be tested without a tty.
func applyCompletion(line []rune, pos int, completer Completer) (newLine []rune, newPos int, display []string) {
	if completer == nil {
		return line, pos, nil
	}
	start, candidates := completer(string(line), pos)
	if len(candidates) == 0 || start < 0 || start > pos || pos > len(line) {
		return line, pos, nil
	}
	if len(candidates) == 1 {
		return splice(line, start, pos, candidates[0]), start + utf8.RuneCountInString(candidates[0]), nil
	}
	prefix := commonPrefix(candidates)
	if utf8.RuneCountInString(prefix) > pos-start {
		return splice(line, start, pos, prefix), start + utf8.RuneCountInString(prefix), nil
	}
	return line, pos, candidates
}

// splice replaces line[start:pos] with text.
func splice(line []rune, start, pos int, text string) []rune {
	out := make([]rune, 0, len(line)+len(text))
	out = append(out, line[:start]...)
	out = append(out, []rune(text)...)
	out = append(out, line[pos:]...)
	return out
}

// commonPrefix returns the longest prefix shared by every candidate, trimmed to
// a rune boundary so a partial UTF-8 sequence can never be inserted.
func commonPrefix(values []string) string {
	if len(values) == 0 {
		return ""
	}
	prefix := values[0]
	for _, v := range values[1:] {
		for !strings.HasPrefix(v, prefix) {
			prefix = prefix[:len(prefix)-1]
			if prefix == "" {
				return ""
			}
		}
	}
	for prefix != "" && !utf8.ValidString(prefix) {
		prefix = prefix[:len(prefix)-1]
	}
	return prefix
}
