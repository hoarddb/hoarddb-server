//go:build linux

package termio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

// readLineRaw reads one line in raw mode, driving the editor's history and
// completion. The terminal's original attributes are restored on every exit
// path, including a read error, so a failed read cannot leave the user's shell
// without echo.
func readLineRaw(fd int, w io.Writer, prompt string, e *LineEditor) (string, error) {
	original, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return "", fmt.Errorf("read terminal attributes: %w", err)
	}
	raw := *original
	// The canonical line discipline and its echo must both go: the editor draws
	// every character itself, and a kernel echo would double each one.
	raw.Lflag &^= unix.ICANON | unix.ECHO
	// Keep ISIG off too, so Ctrl-C arrives as a byte the editor can turn into
	// ErrInterrupt rather than a signal that kills the process mid-line.
	raw.Lflag &^= unix.ISIG
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &raw); err != nil {
		return "", fmt.Errorf("enter raw mode: %w", err)
	}
	defer func() {
		// Best effort: the terminal must be usable even if the read failed.
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, original)
	}()

	_, _ = fmt.Fprint(w, prompt)

	var (
		line []rune
		pos  int
		buf  = make([]byte, 0, 64)
	)
	// redraw reprints the line after a cursor move or an edit. It is deliberately
	// a full redraw rather than a diff: a REPL line is short, and a wrong diff
	// corrupts the display.
	redraw := func() {
		_, _ = fmt.Fprintf(w, "\r\x1b[K%s%s", prompt, string(line))
		if back := len(line) - pos; back > 0 {
			_, _ = fmt.Fprintf(w, "\x1b[%dD", back)
		}
	}

	historyAt := len(e.history)
	var saved string
	for {
		var b [1]byte
		n, err := os.Stdin.Read(b[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				_, _ = fmt.Fprint(w, "\r\n")
				return "", ErrEOF
			}
			return "", fmt.Errorf("read input: %w", err)
		}
		if n == 0 {
			continue
		}

		switch b[0] {
		case '\r', '\n':
			_, _ = fmt.Fprint(w, "\r\n")
			return string(line), nil

		case 0x03: // Ctrl-C
			_, _ = fmt.Fprint(w, "^C\r\n")
			return "", ErrInterrupt

		case 0x04: // Ctrl-D: EOF only on an empty line, otherwise delete forward
			if len(line) == 0 {
				_, _ = fmt.Fprint(w, "\r\n")
				return "", ErrEOF
			}
			if pos < len(line) {
				line = append(line[:pos], line[pos+1:]...)
				redraw()
			}

		case 0x01: // Ctrl-A: start of line
			pos = 0
			redraw()

		case 0x05: // Ctrl-E: end of line
			pos = len(line)
			redraw()

		case 0x0B: // Ctrl-K: delete to end of line
			line = line[:pos]
			redraw()

		case 0x15: // Ctrl-U: delete to start of line
			line = append([]rune(nil), line[pos:]...)
			pos = 0
			redraw()

		case 0x7F, 0x08: // Backspace
			if pos > 0 {
				line = append(line[:pos-1], line[pos:]...)
				pos--
				redraw()
			}

		case '\t':
			newLine, newPos, display := applyCompletion(line, pos, e.completer)
			line, pos = newLine, newPos
			if len(display) > 0 {
				_, _ = fmt.Fprintf(w, "\r\n%s\r\n", strings.Join(display, "  "))
			}
			redraw()

		case 0x1B: // Escape sequence: an arrow or Home/End key
			seq, ok := readEscape(os.Stdin)
			if !ok {
				continue
			}
			switch seq {
			case "[A": // Up
				if historyAt > 0 {
					if historyAt == len(e.history) {
						saved = string(line)
					}
					historyAt--
					line = []rune(e.history[historyAt])
					pos = len(line)
					redraw()
				}
			case "[B": // Down
				if historyAt < len(e.history) {
					historyAt++
					if historyAt == len(e.history) {
						line = []rune(saved)
					} else {
						line = []rune(e.history[historyAt])
					}
					pos = len(line)
					redraw()
				}
			case "[C": // Right
				if pos < len(line) {
					pos++
					redraw()
				}
			case "[D": // Left
				if pos > 0 {
					pos--
					redraw()
				}
			case "[H", "[1~": // Home
				pos = 0
				redraw()
			case "[F", "[4~": // End
				pos = len(line)
				redraw()
			case "[3~": // Delete
				if pos < len(line) {
					line = append(line[:pos], line[pos+1:]...)
					redraw()
				}
			}

		default:
			// A printable byte, or the first byte of a multi-byte rune. bufio is
			// not used here because raw mode hands over exactly the bytes typed.
			buf = append(buf[:0], b[0])
			if b[0] >= utf8.RuneSelf {
				for !utf8.FullRune(buf) {
					var more [1]byte
					n, err := os.Stdin.Read(more[:])
					if err != nil || n == 0 {
						break
					}
					buf = append(buf, more[0])
				}
			}
			r, _ := utf8.DecodeRune(buf)
			if r == utf8.RuneError && len(buf) == 1 {
				continue
			}
			if r < 0x20 {
				continue
			}
			line = splice(line, pos, pos, string(r))
			pos++
			redraw()
		}
	}
}

// readEscape reads the rest of an escape sequence after ESC. It returns the
// sequence without the ESC and reports whether one was read; a lone ESC is
// discarded rather than treated as a key.
func readEscape(r io.Reader) (string, bool) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return "", false
	}
	if first[0] != '[' && first[0] != 'O' {
		return "", false
	}
	seq := []byte{first[0]}
	for {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return "", false
		}
		seq = append(seq, b[0])
		// A CSI sequence ends at the first byte in @..~ — enough for the arrow,
		// Home, End and Delete keys, which are the ones this editor handles.
		if b[0] >= '@' && b[0] <= '~' {
			return string(seq), true
		}
		if len(seq) > 8 {
			return "", false
		}
	}
}
