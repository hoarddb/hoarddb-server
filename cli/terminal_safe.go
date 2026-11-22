package cli

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// A server decides the names it lists and the words of its messages, and the
// CLI writes both to a terminal. A name holding an escape sequence would
// otherwise reach the terminal as a command: a title change, a clipboard
// write, a screen clear. Three layers keep that from happening, so one missed
// call site is not enough to reopen it:
//
//   - terminalSafe and terminalSafeText at the place a server's name or
//     message is printed;
//   - the Error methods of the CLI's error types, which carry server text;
//   - neutralizeControls inside the printf and notef every statement's output
//     goes through, which leaves only the colour sequences the theme itself
//     writes.

// terminalSafe returns a name unchanged when every character in it is
// printable, and its quoted, escaped form otherwise, so the text shows what
// the name holds and a terminal never acts on it.
func terminalSafe(name string) string {
	for _, character := range name {
		if !unicode.IsPrint(character) {
			return strconv.Quote(name)
		}
	}
	return name
}

// terminalSafeText escapes the characters of a server's message that a
// terminal would act on. A message may span lines, so newline and tab stay.
func terminalSafeText(message string) string {
	return escapeControls(message, false)
}

// neutralizeControls is terminalSafeText for text the CLI composed itself,
// which may carry the theme's own colour sequences: ESC [ digits and
// semicolons m passes, every other control character is escaped.
func neutralizeControls(text string) string {
	return escapeControls(text, true)
}

func escapeControls(text string, keepColour bool) string {
	if !strings.ContainsFunc(text, actsOnTerminal) {
		return text
	}
	var escaped strings.Builder
	escaped.Grow(len(text) + 16)
	for index := 0; index < len(text); {
		if keepColour && text[index] == 0x1b {
			if length := colourSequenceLength(text[index:]); length > 0 {
				escaped.WriteString(text[index : index+length])
				index += length
				continue
			}
		}
		character, width := utf8.DecodeRuneInString(text[index:])
		if actsOnTerminal(character) {
			escaped.WriteString(escapeRune(character))
		} else {
			escaped.WriteString(text[index : index+width])
		}
		index += width
	}
	return escaped.String()
}

// actsOnTerminal is true for a character a terminal acts on instead of
// showing: the C0 and C1 controls and DEL (carriage return among them), the
// bidirectional overrides that reorder what is shown, and the line and
// paragraph separators. Newline and tab are layout and are not included.
func actsOnTerminal(character rune) bool {
	switch {
	case character == '\n' || character == '\t':
		return false
	case unicode.IsControl(character):
		return true
	case character == ' ' || character == ' ':
		return true
	}
	return unicode.Is(unicode.Bidi_Control, character)
}

func escapeRune(character rune) string {
	if character < 0x100 {
		return fmt.Sprintf(`\x%02x`, character)
	}
	return fmt.Sprintf(`\u%04x`, character)
}

// colourSequenceLength is the length of a leading ESC [ <digits and ;> m, and
// 0 when text does not start with one.
func colourSequenceLength(text string) int {
	if len(text) < 3 || text[0] != 0x1b || text[1] != '[' {
		return 0
	}
	for index := 2; index < len(text); index++ {
		switch character := text[index]; {
		case character == 'm':
			return index + 1
		case character >= '0' && character <= '9', character == ';':
		default:
			return 0
		}
	}
	return 0
}

// inertWriter escapes every character a terminal would act on in what is
// written through it. dump and restore print database and bucket names from a
// server's listing or from a dump directory and never write colour, so nothing
// is let through; newline and tab stay.
type inertWriter struct {
	destination io.Writer
}

// Write escapes p and writes it, reporting the length of p so a caller's
// byte count is not thrown off by the escapes it never sees.
func (w inertWriter) Write(p []byte) (int, error) {
	if _, err := io.WriteString(w.destination, terminalSafeText(string(p))); err != nil {
		return 0, err
	}
	return len(p), nil
}

// jsonEscapeControls rewrites, as \uXXXX, each character in rendered JSON that
// a terminal would act on, so the line is still the same JSON value.
func jsonEscapeControls(line string) string {
	if !strings.ContainsFunc(line, actsOnTerminal) {
		return line
	}
	var escaped strings.Builder
	for _, character := range line {
		if actsOnTerminal(character) {
			fmt.Fprintf(&escaped, `\u%04x`, character)
		} else {
			escaped.WriteRune(character)
		}
	}
	return escaped.String()
}
