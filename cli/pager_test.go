package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// scriptKeys is a pager's key source with the keys already pressed.
type scriptKeys struct {
	keys []string
}

func (s *scriptKeys) Read() (string, error) {
	if len(s.keys) == 0 {
		return "", io.EOF
	}
	key := s.keys[0]
	s.keys = s.keys[1:]
	return key, nil
}

// numberedReport is a report of n lines, each saying which line it is, so a test
// can tell which screen it is looking at.
func numberedReport(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "line %02d\n", i)
	}
	return b.String()
}

// screens splits a pager's output into the frames it drew. The clear-screen
// escape is what separates them, because every frame starts by clearing.
func screens(t *testing.T, out string) []string {
	t.Helper()
	if !strings.HasPrefix(out, altScreenOn) {
		t.Fatalf("the pager did not enter the alternate screen; output was %q", out)
	}
	if !strings.HasSuffix(out, altScreenOff) {
		t.Fatalf("the pager left the alternate screen on; output ended %q", tail(out, 40))
	}
	frames := strings.Split(strings.TrimSuffix(strings.TrimPrefix(out, altScreenOn), altScreenOff), clearScreen)
	return frames[1:] // element 0 is what came between the two escapes
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// TestPagerShowsAScreenAtATime is the behaviour the feature exists for: a report
// longer than the screen arrives a screen at a time, and one key advances it.
func TestPagerShowsAScreenAtATime(t *testing.T) {
	var out bytes.Buffer
	p := &pager{out: &out, keys: &scriptKeys{keys: []string{" ", "q"}}, rows: 6}

	if err := p.show(numberedReport(20)); err != nil {
		t.Fatalf("show: %v", err)
	}
	frames := screens(t, out.String())
	if len(frames) != 2 {
		t.Fatalf("drew %d screens, want 2 (space then q)", len(frames))
	}
	// rows is 6, so one screen is five lines and a prompt.
	if !strings.Contains(frames[0], "line 01") || !strings.Contains(frames[0], "line 05") {
		t.Fatalf("the first screen does not hold lines 1 to 5:\n%q", frames[0])
	}
	if strings.Contains(frames[0], "line 06") {
		t.Fatalf("the first screen runs past its five lines:\n%s", frames[0])
	}
	if !strings.Contains(frames[0], "-- More -- (0%)") {
		t.Fatalf("the first screen has no prompt saying where it is:\n%s", frames[0])
	}
	if strings.Contains(frames[0], "(END)") {
		t.Fatalf("the first screen claims to be the end:\n%s", frames[0])
	}
	if !strings.Contains(frames[1], "line 06") || !strings.Contains(frames[1], "line 10") {
		t.Fatalf("the second screen does not hold the next five lines:\n%s", frames[1])
	}

	// The keys are described on screen: a pager whose keys nobody can guess is a
	// pager nobody uses.
	for _, key := range []string{"space next page", "enter next line", "b back", "q quit"} {
		if !strings.Contains(frames[0], key) {
			t.Fatalf("the prompt does not mention %q:\n%s", key, frames[0])
		}
	}
}

// TestPagerEndAndQuit covers the two ends of a report: G jumps to the last
// screen and says it is the end, and q leaves without drawing another.
func TestPagerEndAndQuit(t *testing.T) {
	var out bytes.Buffer
	p := &pager{out: &out, keys: &scriptKeys{keys: []string{"G", "q"}}, rows: 4}

	if err := p.show(numberedReport(10)); err != nil {
		t.Fatalf("show: %v", err)
	}
	frames := screens(t, out.String())
	if len(frames) != 2 {
		t.Fatalf("drew %d screens, want 2 (G then q)", len(frames))
	}
	last := frames[1]
	if !strings.Contains(last, "line 08") || !strings.Contains(last, "line 10") {
		t.Fatalf("G did not land on the last screen (3 lines: 8, 9, 10 of 10):\n%q", last)
	}
	if !strings.Contains(last, "(END)") {
		t.Fatalf("the last screen does not say it is the end:\n%s", last)
	}
}

// TestPagerArrowsScrollInsteadOfQuitting is the guard for the defect that made
// an arrow key quit the pager: Down arrives as ESC, '[', 'B', and treating its
// first byte as the quit key stopped the pager and left "[B" for the next
// prompt. Down scrolls one line and the pager keeps going; Up comes back.
func TestPagerArrowsScrollInsteadOfQuitting(t *testing.T) {
	var out bytes.Buffer
	p := &pager{out: &out, keys: &scriptKeys{keys: []string{"\x1b[B", "\x1b[A", "q"}}, rows: 6}

	if err := p.show(numberedReport(20)); err != nil {
		t.Fatalf("show: %v", err)
	}
	frames := screens(t, out.String())
	if len(frames) != 3 {
		t.Fatalf("drew %d screens, want 3 (Down, Up, q): an arrow key quit the pager", len(frames))
	}
	// rows is 6, so one screen is five lines. Down moved the top from line 1 to
	// line 2.
	if !strings.Contains(frames[1], "line 02") || strings.Contains(frames[1], "line 01") {
		t.Fatalf("Down did not scroll one line:\n%s", frames[1])
	}
	if got := frames[2]; got != frames[0] {
		t.Fatalf("Up did not come back to the first screen:\n%q\nwant\n%q", got, frames[0])
	}
}

// TestPagerPageKeysMoveAWholeScreen covers Page Down and Page Up, which arrive
// as longer sequences (ESC '[' '6' '~').
func TestPagerPageKeysMoveAWholeScreen(t *testing.T) {
	var out bytes.Buffer
	p := &pager{out: &out, keys: &scriptKeys{keys: []string{"\x1b[6~", "q"}}, rows: 6}

	if err := p.show(numberedReport(20)); err != nil {
		t.Fatalf("show: %v", err)
	}
	frames := screens(t, out.String())
	if len(frames) != 2 {
		t.Fatalf("drew %d screens, want 2 (Page Down then q)", len(frames))
	}
	if !strings.Contains(frames[1], "line 06") || strings.Contains(frames[1], "line 01") {
		t.Fatalf("Page Down did not move a whole screen:\n%s", frames[1])
	}
}

// TestPagerLoneEscapeQuits is the other half of the same rule: a real Escape
// key — no bytes behind it — still leaves the pager, so the sequence handling
// did not cost the user the exit key.
func TestPagerLoneEscapeQuits(t *testing.T) {
	var out bytes.Buffer
	p := &pager{out: &out, keys: &scriptKeys{keys: []string{"\x1b"}}, rows: 4}

	if err := p.show(numberedReport(10)); err != nil {
		t.Fatalf("show: %v", err)
	}
	if frames := screens(t, out.String()); len(frames) != 1 {
		t.Fatalf("a lone Escape drew %d screens, want 1: it should have quit", len(frames))
	}
}

// TestPagerIgnoresAnUnboundSequence: a key the pager does not bind draws the
// screen again rather than being taken for a quit or left unconsumed.
func TestPagerIgnoresAnUnboundSequence(t *testing.T) {
	var out bytes.Buffer
	p := &pager{out: &out, keys: &scriptKeys{keys: []string{"\x1b[Z", "q"}}, rows: 6}

	if err := p.show(numberedReport(20)); err != nil {
		t.Fatalf("show: %v", err)
	}
	frames := screens(t, out.String())
	if len(frames) != 2 {
		t.Fatalf("drew %d screens, want 2 (an unbound key redraws, then q)", len(frames))
	}
	if frames[1] != frames[0] {
		t.Fatalf("an unbound sequence moved the screen:\n%q\nwant\n%q", frames[1], frames[0])
	}
}

// syncBuffer is a writer a test can read while the pager's own goroutine writes
// to it: the signal watcher writes from a second goroutine, and a plain
// bytes.Buffer would be a data race under -race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// blockingKeys is a key source that waits until the test hands it a key, so a
// test can observe the pager while it is still on screen.
type blockingKeys struct{ keys chan string }

func (b *blockingKeys) Read() (string, error) {
	key, ok := <-b.keys
	if !ok {
		return "", io.EOF
	}
	return key, nil
}

// TestPagerRestoresTheTerminalWhenAStopSignalArrives is the guard for a process
// stopped from outside while the pager owns the terminal: without restoring,
// the shell that gets the process back is left with echo off and no text on
// screen. The pager must leave the alternate screen, put the terminal's
// attributes back, and then let the signal take its course.
func TestPagerRestoresTheTerminalWhenAStopSignalArrives(t *testing.T) {
	var out syncBuffer
	keys := &blockingKeys{keys: make(chan string, 1)}
	signals := make(chan os.Signal, 1)
	restored := make(chan struct{}, 1)
	raised := make(chan os.Signal, 1)
	p := &pager{
		out: &out, keys: keys, rows: 4, signals: signals,
		restore: func() error { restored <- struct{}{}; return nil },
		raise:   func(sig os.Signal) { raised <- sig },
	}

	finished := make(chan error, 1)
	go func() { finished <- p.show(numberedReport(10)) }()

	// Wait until the pager is on screen and waiting for a key.
	waitFor(t, func() bool { return strings.Contains(out.String(), altScreenOn) })

	signals <- syscall.SIGTERM
	select {
	case got := <-raised:
		if got != syscall.SIGTERM {
			t.Fatalf("the pager handed on %v, want the SIGTERM it was sent", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pager ignored a stop signal")
	}
	if text := out.String(); !strings.HasSuffix(text, altScreenOff) {
		t.Fatalf("the pager did not leave the alternate screen; output ended %q", tail(text, 40))
	}
	select {
	case <-restored:
	default:
		t.Fatal("the pager left the terminal in raw mode: it was not restored")
	}

	// Let the pager leave normally so its goroutine does not outlive the test.
	keys.keys <- "q"
	if err := <-finished; err != nil {
		t.Fatalf("show: %v", err)
	}
}

// waitFor polls until cond holds, and fails the test if it never does.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the condition never became true")
}

// TestPagerNeverPagesWhatFits: taking the screen away to show less than a screen
// holds is worse than not paging at all, and it would also lose the session's own
// lines above the report.
func TestPagerNeverPagesWhatFits(t *testing.T) {
	var out bytes.Buffer
	keys := &scriptKeys{keys: []string{"q"}}
	p := &pager{out: &out, keys: keys, rows: 10}

	if err := p.show(numberedReport(6)); err != nil {
		t.Fatalf("show: %v", err)
	}
	if got := out.String(); got != numberedReport(6) {
		t.Fatalf("short output was not printed as-is:\n%q", got)
	}
	if len(keys.keys) != 1 {
		t.Fatalf("the pager read a key for output it printed in full")
	}
}

// TestPagerSurvivesKeysRunningOut: a pager that swallowed a report because its
// input ended would be worse than no pager, so the whole report is left behind.
func TestPagerSurvivesKeysRunningOut(t *testing.T) {
	var out bytes.Buffer
	p := &pager{out: &out, keys: &scriptKeys{}, rows: 4} // no keys at all

	if err := p.show(numberedReport(10)); err != nil {
		t.Fatalf("show: %v (the report must still be printed)", err)
	}
	got := out.String()
	if !strings.HasSuffix(got, numberedReport(10)) {
		t.Fatalf("the report was not printed in full after the keys ran out:\n%q", got)
	}
	if !strings.Contains(got, altScreenOff) {
		t.Fatalf("the terminal was left on the alternate screen:\n%q", got)
	}
}

// TestPagerReportsAWriteFailure: an error must reach the caller, which prints the
// report instead. The pager is the only thing that knows it failed.
func TestPagerReportsAWriteFailure(t *testing.T) {
	p := &pager{out: failingWriter{}, keys: &scriptKeys{keys: []string{"q"}}, rows: 4}
	if err := p.show(numberedReport(10)); err == nil {
		t.Fatal("a failed write went unreported")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("terminal went away") }

// TestSessionOutputHoldsAndReleases pins the mechanism paging rests on: while a
// hold is in progress the output goes to the hold and not to the terminal, and
// releasing it hands the text back and puts the terminal back in line.
func TestSessionOutputHoldsAndReleases(t *testing.T) {
	var terminal bytes.Buffer
	out := &sessionOutput{terminal: &terminal}

	_, _ = io.WriteString(out, "before\n")
	release := out.hold()
	_, _ = io.WriteString(out, "held\n")
	if got := terminal.String(); got != "before\n" {
		t.Fatalf("held output reached the terminal: %q", got)
	}
	if held := release(); held != "held\n" {
		t.Fatalf("hold returned %q, want %q", held, "held\n")
	}
	_, _ = io.WriteString(out, "after\n")
	if got := terminal.String(); got != "before\nafter\n" {
		t.Fatalf("output after the hold did not reach the terminal: %q", got)
	}
}

// TestNoPagerPrintsEverything is the guarantee the rest of the CLI and its
// scripts depend on: with --no-pager, a report is printed in full, with no
// screen-taking and no key reading.
//
// The session here is one that could otherwise page — both ends are a real
// terminal of a known height — so what stops it is the setting itself. A
// session whose input is a string reader would refuse for the wrong reason and
// the guard would go untested.
func TestNoPagerPrintsEverything(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	master, slave := openTestPty(t)
	setPtySize(t, slave, 24, 80)

	c := NewCLI(WithTerminal(slave), WithNoPager())
	c.reader = slave
	if rows, paging := c.pageable(&Cmd{Method: "help"}); paging {
		t.Fatalf("--no-pager still held output for paging (rows=%d)", rows)
	}

	// The same session with paging left on pages, which is the other half of
	// the guard: the setting is what decides.
	without := NewCLI(WithTerminal(slave))
	without.reader = slave
	rows, paging := without.pageable(&Cmd{Method: "help"})
	if !paging {
		t.Fatalf("the same session with paging on did not page (rows=%d): %v", rows, ptyOutput(t, master))
	}
}

// TestPipedSessionIsNeverPaged guards the other half of the same decision: a
// session reading its statements from a pipe has no one to press a key, whatever
// the terminal end looks like. The reader is a real pipe — an *os.File, so it
// gets past the "is this a file" questions — which leaves the guard itself as
// the only thing that can refuse it.
func TestPipedSessionIsNeverPaged(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	master, slave := openTestPty(t)
	setPtySize(t, slave, 24, 80)

	statements, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("making the pipe: %v", err)
	}
	defer func() { _ = statements.Close() }()
	defer func() { _ = writer.Close() }()

	c := NewCLI(WithTerminal(slave))
	c.reader = statements
	if rows, paging := c.pageable(&Cmd{Method: "help"}); paging {
		t.Fatalf("a session reading from a pipe held output for paging (rows=%d)", rows)
	}

	// The same session with a terminal for input does page.
	interactive := NewCLI(WithTerminal(slave))
	interactive.reader = slave
	if _, paging := interactive.pageable(&Cmd{Method: "help"}); !paging {
		t.Fatalf("the same session with a terminal for input did not page: %v", ptyOutput(t, master))
	}
}

// TestPagingDegradesToPlainOutputWhenKeysCannotBeRead: an input that is a
// character device but not a terminal (`hoardDB < /dev/null`) is judged
// pageable, and the pager then cannot take it for keystrokes. The report must
// still arrive, in full and with no control sequences — the same degradation a
// platform without a pager gives, and it must not be an error.
func TestPagingDegradesToPlainOutputWhenKeysCannotBeRead(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	master, slave := openTestPty(t)
	setPtySize(t, slave, 4, 80)

	notATerminal, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("opening %s: %v", os.DevNull, err)
	}
	defer func() { _ = notATerminal.Close() }()

	c := NewCLI(WithTerminal(slave), WithTheme("plain"))
	c.reader = notATerminal
	if err := c.runCommand(&Cmd{Type: "global", Method: "theme"}); err != nil {
		t.Fatalf("runCommand(theme) = %v, want the report printed and no error", err)
	}
	out := ptyOutput(t, master)
	if strings.ContainsRune(out, 0x1b) {
		t.Fatalf("the report was paged onto a terminal that cannot read keys: %q", out)
	}
	if !strings.Contains(out, "prompt:") {
		t.Fatalf("the report was not printed at all: %q", out)
	}
}

// TestDumbTerminalIsNeverPaged is the guard for defect (c): a terminal that
// says it cannot take control sequences — TERM=dumb, or no TERM at all — must
// not be given the alternate screen and the cursor-home escapes the pager
// writes. They are not control there, they are noise in whatever reads the
// output.
func TestDumbTerminalIsNeverPaged(t *testing.T) {
	for _, term := range []string{"dumb", ""} {
		t.Run("TERM="+term, func(t *testing.T) {
			t.Setenv("TERM", term)
			master, slave := openTestPty(t)
			setPtySize(t, slave, 4, 80)

			c := NewCLI(WithTerminal(slave), WithTheme("plain"))
			c.reader = slave
			if rows, paging := c.pageable(&Cmd{Method: "theme"}); paging {
				t.Fatalf("a terminal that cannot take the escapes was paged (rows=%d)", rows)
			}
			// `theme` prints five lines; the terminal is four rows tall, so a
			// pager that ran would take the screen.
			if err := c.runCommand(&Cmd{Type: "global", Method: "theme"}); err != nil {
				t.Fatalf("runCommand(theme): %v", err)
			}
			out := ptyOutput(t, master)
			if strings.ContainsRune(out, 0x1b) {
				t.Fatalf("a terminal that cannot take escapes was sent control sequences: %q", out)
			}
			if !strings.Contains(out, "prompt:") {
				t.Fatalf("the report was not printed at all: %q", out)
			}
		})
	}
}

// TestNoPagingWhenTheTerminalReportsNoSize is the platform case: macOS today,
// and any terminal that will not say how tall it is, has no paging to give — a
// pager with no height cannot decide what fits. Both are the same refusal, and
// both must print the report in full with no control characters and no error.
func TestNoPagingWhenTheTerminalReportsNoSize(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	_, inSlave := openTestPty(t)
	setPtySize(t, inSlave, 24, 80)

	// A regular file answers no window-size ioctl, which is the "no height"
	// case; the report goes into it, so the test can read back what was shown.
	output, err := os.CreateTemp(t.TempDir(), "terminal-*")
	if err != nil {
		t.Fatalf("making the output file: %v", err)
	}
	defer func() { _ = output.Close() }()

	c := NewCLI(WithTerminal(output), WithTheme("plain"))
	c.reader = inSlave
	if rows, paging := c.pageable(&Cmd{Method: "theme"}); paging {
		t.Fatalf("a terminal with no height was paged (rows=%d)", rows)
	}
	// `theme` prints five lines, so a pager that ran would have taken a screen.
	if err := c.runCommand(&Cmd{Type: "global", Method: "theme"}); err != nil {
		t.Fatalf("runCommand(theme) = %v, want the report printed in full and no error", err)
	}
	shown, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatalf("reading the report back: %v", err)
	}
	if strings.ContainsRune(string(shown), 0x1b) {
		t.Fatalf("a terminal with no height was sent control sequences: %q", string(shown))
	}
	if !strings.Contains(string(shown), "prompt:") {
		t.Fatalf("the report was not printed in full: %q", string(shown))
	}
}

// TestPagingNeedsACapableTerminal is the control for the test above: the same
// four-row terminal with a TERM that can take the escapes does page, so the
// gate is what changed and not something else about the session.
func TestPagingNeedsACapableTerminal(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	master, slave := openTestPty(t)
	setPtySize(t, slave, 24, 80)

	c := NewCLI(WithTerminal(slave))
	c.reader = slave
	if rows, paging := c.pageable(&Cmd{Method: "help"}); !paging {
		t.Fatalf("a capable terminal was not paged (rows=%d): %v", rows, ptyOutput(t, master))
	}
}

// TestStreamingVerbsAreNeverPaged: a command whose output is work being done
// rather than a report about it must not be held, because holding it shows the
// user nothing until the work finishes, which reads as a hang.
func TestStreamingVerbsAreNeverPaged(t *testing.T) {
	for _, method := range []string{"ingest", "dump", "restore"} {
		if !streamsProgress(&Cmd{Method: method}) {
			t.Errorf("%s streams progress and must not be held for paging", method)
		}
	}
	for _, method := range []string{"help", "status", "describe", "show", "nodes"} {
		if streamsProgress(&Cmd{Method: method}) {
			t.Errorf("%s is a report and can be paged", method)
		}
	}
}
