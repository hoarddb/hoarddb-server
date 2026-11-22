package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-server/termio"
)

// The pager shows a report one screen at a time, the way more(1) does, because
// some of them do not fit: `help` is 159 lines on a default install, `status`
// and a `describe` of a wide bucket run past a screen, and `nodes history` grows
// with the cluster's age. It is built in rather than handed to $PAGER: hoardDB
// ships as one binary, the CLI already owns a raw-mode terminal (termio), and a
// child process would have to be given the terminal and then taken back from it
// — which is how a CLI ends up leaving a shell without echo.
//
// The screen is the alternate one, so quitting restores exactly what the user
// had: the report never mixes into the scrollback, and "back" can redraw a
// previous screen instead of pretending to scroll a terminal that has already
// forgotten those lines.

const (
	altScreenOn  = "\x1b[?1049h"
	altScreenOff = "\x1b[?1049l"
	clearScreen  = "\x1b[2J\x1b[H"
)

// The keys that arrive as an escape sequence. termio.Keys hands these over
// whole (see Termio's escapeTimeout), so the pager compares the whole sequence
// and never the single ESC byte they all begin with.
const (
	escKey   = "\x1b"    // a lone Escape: quit
	upLine   = "\x1b[A"  // Up: one line back
	downLine = "\x1b[B"  // Down: one line on
	upPage   = "\x1b[5~" // Page Up: a screen back
	downPage = "\x1b[6~" // Page Down: a screen on
	homeKey  = "\x1b[H"  // Home: the top
	endKey   = "\x1b[F"  // End: the bottom
)

// keySource is one keystroke at a time. termio.Keys is a terminal; a test
// scripts it.
type keySource interface {
	Read() (string, error)
}

// pager draws held output a screen at a time.
type pager struct {
	out  io.Writer
	keys keySource
	rows int // the screen's height; the last row carries the prompt

	// signals carries the stop signals to answer while the pager owns the
	// terminal. A process stopped from outside — SIGTERM, SIGHUP or an external
	// SIGINT — would otherwise end with the terminal still in raw mode on the
	// alternate screen, and the shell that gets it back has no echo and no
	// text. nil when nothing is watching, which is how a test drives show.
	signals <-chan os.Signal
	// restore puts the terminal's own attributes back. nil when there is
	// nothing to restore.
	restore func() error
	// raise hands the signal on once the terminal is back; nil means terminate.
	raise func(os.Signal)
}

// page shows text, paging it when it does not fit. It is called only for a
// session that can page (see pageable), and it reports the first error it hit.
//
// A failure never costs the user a report: whatever went wrong, the text is
// printed in full afterwards.
func (c *CLI) page(text string, rows int) error {
	return pageText(c.terminal, c.reader, text, rows)
}

// pageText is page for any terminal and input: ring mode pages through it too.
func pageText(terminal io.Writer, input io.Reader, text string, rows int) error {
	c := &CLI{terminal: terminal, reader: input}
	if fitsOnScreen(text, rows) {
		// Short enough to print where it is, so the terminal is not touched at
		// all — no alternate screen and no raw mode for a report that fits.
		_, err := io.WriteString(c.terminal, text)
		return err
	}
	keys, err := c.newKeys()
	if err != nil {
		// No keys means no paging; the report still has to be printed.
		_, werr := io.WriteString(c.terminal, text)
		if werr != nil {
			return werr
		}
		return nil
	}
	defer func() { _ = keys.Close() }()

	p := &pager{out: c.terminal, keys: keys, rows: rows}

	// Paging is the only time the CLI holds the terminal this way, so the stop
	// signals are caught for exactly as long as it does: a stop while the pager
	// is up restores the terminal, and a stop at any other moment keeps its
	// usual disposition.
	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
	defer signal.Stop(stopSignals)
	p.signals = stopSignals
	p.restore = keys.Close

	if err := p.show(text); err != nil {
		// The pager failed part-way: leave the screen, then print the whole
		// report so nothing is truncated and nothing is lost.
		_, _ = io.WriteString(c.terminal, altScreenOff)
		if _, werr := io.WriteString(c.terminal, text); werr != nil {
			return fmt.Errorf("paging: %v; showing the report instead: %w", err, werr)
		}
		return err
	}
	return nil
}

// watchStop answers a stop signal while paging, and stops when the pager is
// done with the terminal.
func (p *pager) watchStop(stop <-chan struct{}) {
	select {
	case sig, ok := <-p.signals:
		if !ok {
			return
		}
		p.onStop(sig)
	case <-stop:
	}
}

// onStop puts the terminal back before the process goes: leave the alternate
// screen, restore the attributes, then hand the signal on. The order matters —
// the escape has to reach a terminal that is still usable — and the signal is
// handed on rather than swallowed so the caller still ends the way it was
// asked to.
func (p *pager) onStop(sig os.Signal) {
	_, _ = io.WriteString(p.out, altScreenOff)
	if p.restore != nil {
		_ = p.restore()
	}
	if p.raise != nil {
		p.raise(sig)
		return
	}
	terminate(sig)
}

// terminate replays a stop signal with its default disposition, so the process
// dies of the signal it was sent — the shell sees 128+n, and a supervisor can
// tell a stop from a failure — and does not come back to a terminal it never
// restored.
func terminate(sig os.Signal) {
	signal.Reset(sig)
	exitStatus := 1
	if number, ok := sig.(syscall.Signal); ok {
		exitStatus = 128 + int(number)
	}
	if proc, err := os.FindProcess(os.Getpid()); err == nil {
		_ = proc.Signal(sig)
	}
	// A signal a process sends itself is delivered asynchronously, so exiting at
	// once would win the race and end with an ordinary status. Wait for it; the
	// exit is only for a signal that was ignored or could not be sent, which
	// still has to end the process the way the shell would have reported it.
	time.Sleep(time.Second)
	os.Exit(exitStatus)
}

// fitsOnScreen reports whether a report is at most one screen of content: the
// screen's rows less the row the prompt needs. A report that fits is printed in
// place — taking the screen away to show less than a screen holds would be worse
// than not paging, and it would also hide the session's own lines above it.
func fitsOnScreen(text string, rows int) bool {
	return len(reportLines(text)) <= rows-1
}

// newKeys takes the terminal for single keystrokes. The session's own reader is
// the key source: it is the device the user is typing on, and on anything but a
// terminal it cannot be read a key at a time at all.
func (c *CLI) newKeys() (*termio.Keys, error) {
	f, ok := c.reader.(*os.File)
	if !ok {
		return nil, errors.New("input is not a terminal")
	}
	return termio.NewKeys(f)
}

// pageable reports the screen height and whether this command's output should be
// held and paged. Five things must hold: the user did not turn paging off, both
// ends of the session are terminals (a pipe has no one to press a key and no
// height to fit), the terminal can take the pager's escapes at all, the command
// is one whose output is a report, and the output end reports its size.
//
// The input end is asked one question — is it a terminal — and answered by
// interactive(), the same predicate the session uses to decide whether it has a
// line editor. Asking it a second time for its *size* used to sit here too, and
// it refused the same sessions one check later, which made the piped-session
// guard untestable: every reader that reached it had already been refused. A
// char device that is not a terminal (`hoardDB < /dev/null`) passes
// interactive() and is caught by page() instead, which cannot take it for keys
// and prints the report in full.
func (c *CLI) pageable(cmd *Cmd) (rows int, ok bool) {
	if c.noPager || !c.interactive() {
		return 0, false
	}
	if !terminalCanTakeEscapes() {
		return 0, false
	}
	if streamsProgress(cmd) {
		return 0, false
	}
	return screenRows(c.terminal)
}

// screenRows is the height of the terminal out, and whether it is tall enough
// to page on.
func screenRows(terminal io.Writer) (rows int, ok bool) {
	out, isFile := terminal.(*os.File)
	if !isFile {
		return 0, false
	}
	_, rows, ok = termio.Size(out)
	if !ok || rows < 4 {
		// A screen this short cannot hold content and a prompt; paging it would
		// hide more than it shows.
		return 0, false
	}
	return rows, true
}

// terminalCanTakeEscapes reports whether the terminal can be given the pager's
// control sequences. TERM=dumb is a terminal saying it cannot; an unset TERM
// means nothing is known about it, and guessing wrong leaves the alternate
// screen and the cursor-home escape in whatever is reading the output — a log
// file, a captured transcript, a program that parses it.
func terminalCanTakeEscapes() bool {
	term := os.Getenv("TERM")
	return term != "" && term != "dumb"
}

// streamsProgress reports whether a command's output is work being done rather
// than a report about it. Holding a stream to count its lines would show the
// user nothing until the work finished — which is indistinguishable from a hang.
func streamsProgress(cmd *Cmd) bool {
	switch cmd.Method {
	case "ingest", "dump", "restore":
		return true
	}
	return false
}

// show pages text. Output that fits is written straight through: taking the
// screen away to show less than a screen holds is worse than not paging.
func (p *pager) show(text string) error {
	if fitsOnScreen(text, p.rows) {
		_, err := io.WriteString(p.out, text)
		return err
	}
	if p.signals != nil {
		// A stop signal has to be able to restore the terminal while this
		// screen is up, and the watcher has to go when paging does.
		stop := make(chan struct{})
		defer close(stop)
		go p.watchStop(stop)
	}
	lines := reportLines(text)
	height := p.rows - 1 // the last row is the prompt

	if _, err := io.WriteString(p.out, altScreenOn); err != nil {
		return err
	}
	err := p.cycle(lines, height)
	if _, werr := io.WriteString(p.out, altScreenOff); werr != nil && err == nil {
		err = werr
	}
	if errors.Is(err, io.EOF) {
		// The key source ended under us — input closed. Leaving the whole
		// report behind is the only honest thing left to do: a screen the user
		// cannot advance is worse than no pager.
		_, werr := io.WriteString(p.out, text)
		return werr
	}
	return err
}

// cycle draws screens until the user quits or the key source ends. It returns
// io.EOF when the keys ran out, which the caller answers by printing everything.
func (p *pager) cycle(lines []string, height int) error {
	last := 0
	for {
		if _, err := io.WriteString(p.out, p.frame(lines, last, height)); err != nil {
			return err
		}
		key, err := p.keys.Read()
		if err != nil {
			return err
		}
		switch key {
		case "q", "\x03", "\x04", escKey:
			// q, Ctrl-C, Ctrl-D and ESC all leave: there is nothing here a user
			// should have to remember the exit key for.
			return nil
		case " ", "f", "\x06", downPage:
			// space, f, Ctrl-F, Page Down
			last += height
		case "\r", "\n", "j", "\x0e", downLine:
			// enter, j, Ctrl-N, Down
			last++
		case "b", "\x02", upPage:
			// b, Ctrl-B, Page Up
			last -= height
		case "k", "\x10", upLine:
			// k, Ctrl-P, Up
			last--
		case "g", homeKey:
			last = 0
		case "G", endKey:
			last = len(lines)
		default:
			// An unknown key redraws, which is also how a terminal that was
			// resized under us recovers. The arrow, Page, Home and End sequences
			// are named above, so a key that arrives as one is never a quit: only
			// a lone ESC is, which is why it is compared against escKey and not
			// matched by prefix.
		}
		if last > len(lines)-height {
			last = len(lines) - height
		}
		if last < 0 {
			last = 0
		}
	}
}

// frame renders one screen: the lines from top, then the prompt on the last row.
func (p *pager) frame(lines []string, top, height int) string {
	end := top + height
	if end > len(lines) {
		end = len(lines)
	}
	var b strings.Builder
	b.WriteString(clearScreen)
	for _, line := range lines[top:end] {
		b.WriteString(line)
		b.WriteString("\r\n")
	}
	b.WriteString(p.prompt(top+height >= len(lines), top, len(lines)))
	return b.String()
}

// prompt is the last line of every frame. It says where the user is and what the
// keys do, because a pager whose keys nobody can guess is a pager nobody uses.
func (p *pager) prompt(atEnd bool, top, total int) string {
	position := fmt.Sprintf("%d%%", top*100/total)
	if atEnd {
		position = "END"
	}
	return fmt.Sprintf("-- More -- (%s)   space next page   enter next line   b back   g top   G end   q quit", position)
}

// reportLines splits a report into the lines to show, dropping the one trailing
// newline that separates output from the prompt rather than paging a blank line.
func reportLines(text string) []string {
	return strings.Split(strings.TrimRight(text, "\n"), "\n")
}
