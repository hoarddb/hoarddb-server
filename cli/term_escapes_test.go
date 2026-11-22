package cli

import (
	"bytes"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// swapStdout points the process's standard output at f for one test: the OSC 11
// background query is written to os.Stdout itself, not to the session's
// terminal, so a test that wants to see it has to capture that.
func swapStdout(t *testing.T, f *os.File) {
	t.Helper()
	previous := os.Stdout
	os.Stdout = f
	t.Cleanup(func() { os.Stdout = previous })
}

// escapeRun is one interactive session driven from a pseudo-terminal, and what
// it wrote to its output.
type escapeRun struct {
	// term is TERM; unsetTerm removes it instead.
	term      string
	unsetTerm bool
	noColor   bool
	// redirected sends the session's output to a buffer, the way
	// `hoardDB > file` does while stdin stays a terminal.
	redirected bool
}

// run types a statement and exit into the session and returns everything the
// session wrote to its output, including the OSC 11 query on os.Stdout.
func (r escapeRun) run(t *testing.T) string {
	t.Helper()
	if r.unsetTerm {
		t.Setenv("TERM", "")
		if err := os.Unsetenv("TERM"); err != nil {
			t.Fatalf("unsetting TERM: %v", err)
		}
	} else {
		t.Setenv("TERM", r.term)
	}
	if r.noColor {
		t.Setenv("NO_COLOR", "1")
	} else {
		t.Setenv("NO_COLOR", "")
	}
	t.Setenv("HOARDDB_THEME", "")
	t.Setenv("HOARDDB_BACKGROUND", "")

	inMaster, inSlave := openTestPty(t)
	setPtySize(t, inSlave, 24, 80)

	var (
		terminal   io.Writer
		outMaster  *os.File
		redirected bytes.Buffer
	)
	if r.redirected {
		terminal = &redirected
	} else {
		var outSlave *os.File
		outMaster, outSlave = openTestPty(t)
		setPtySize(t, outSlave, 24, 80)
		terminal = outSlave
		swapStdout(t, outSlave)
	}

	script := bannerScript()
	script[transport.OpListDBs] = server.ListDatabasesResponse{}
	_, c := newStubServer(t, script, terminal)
	// The stub builds its session with the plain theme; the theme under test is
	// the one the environment picks.
	c.theme = resolveTheme("", c.stdoutIsTTY)
	c.reader = inSlave
	swapStdin(t, inSlave)
	// Typed after the background query's 50 ms budget has run out, as a user's
	// keystrokes arrive after the greeting: a query still running would drain
	// them as its reply, and this test would hang instead of failing.
	typed := time.AfterFunc(300*time.Millisecond, func() {
		_, _ = inMaster.WriteString("show databases\nexit\n")
	})
	t.Cleanup(func() { typed.Stop() })

	if err := c.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.redirected {
		return redirected.String()
	}
	return ptyOutput(t, outMaster)
}

var (
	anyEscape     = regexp.MustCompile("\x1b")
	colourEscapes = regexp.MustCompile("\x1b\\[[0-9;]*m|\x1b\\]")
)

// TestNoEscapeSequencesWhereTheTerminalCannotTakeThem pins the promise the docs
// make: when TERM is dumb or unset, or the output is not a terminal, the CLI
// writes no escape sequence at all — no colour, no background query, and no
// erase-to-end-of-line from the line editor. With NO_COLOR set a terminal that
// can take cursor movement still gets it, but no colour and no query.
//
// The last case is the control: a capable terminal does get colour, so a
// session that wrote nothing because the harness was wrong would fail here.
func TestNoEscapeSequencesWhereTheTerminalCannotTakeThem(t *testing.T) {
	tests := []struct {
		name      string
		run       escapeRun
		forbidden *regexp.Regexp
	}{
		{"TERM=dumb", escapeRun{term: "dumb"}, anyEscape},
		{"TERM unset", escapeRun{unsetTerm: true}, anyEscape},
		{"stdout redirected", escapeRun{term: "xterm-256color", redirected: true}, anyEscape},
		{"NO_COLOR set", escapeRun{term: "xterm-256color", noColor: true}, colourEscapes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.run.run(t)
			if !strings.Contains(out, "show databases") && !strings.Contains(out, "hoardDB") {
				t.Fatalf("the session printed nothing recognisable, so this proves nothing: %q", out)
			}
			if found := tc.forbidden.FindAllString(out, -1); len(found) > 0 {
				t.Fatalf("escape sequences %q reached the output:\n%q", found, out)
			}
		})
	}

	t.Run("a capable terminal still gets colour", func(t *testing.T) {
		out := escapeRun{term: "xterm-256color"}.run(t)
		if !colourEscapes.MatchString(out) {
			t.Fatalf("a colour terminal got no colour, so the cases above prove nothing:\n%q", out)
		}
	})
}
