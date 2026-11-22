package cli

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ringTokKind is the lexical class of a token.
type ringTokKind int

const (
	// tokWord is a run of letters, digits, '_', '-' and '$': a keyword, a name, a number or a hex cursor.
	tokWord ringTokKind = iota
	// tokString is a double-quoted string; text holds the unquoted content.
	tokString
	// tokPunct is one of . : @ [ ] ( ) { } , ; < > *.
	tokPunct
)

// ringTok is one lexed token with its byte range in the line.
type ringTok struct {
	kind       ringTokKind
	text       string
	start, end int
}

const ringPunct = ".:@[](){},;<>*"

func ringWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '$'
}

// ringCharPos converts a byte offset to the 1-based character position errors print.
func ringCharPos(line string, offset int) int {
	if offset > len(line) {
		offset = len(line)
	}
	return utf8.RuneCountInString(line[:offset]) + 1
}

// ringLex splits a statement into tokens. On a lexical error it returns the
// tokens read so far together with a *ParseError, so completion can still use
// the text before the error.
func ringLex(line string) ([]ringTok, error) {
	var toks []ringTok
	for i := 0; i < len(line); {
		r, size := utf8.DecodeRuneInString(line[i:])
		switch {
		case unicode.IsSpace(r):
			i += size
		case ringWordRune(r):
			j := i
			for j < len(line) {
				next, nextSize := utf8.DecodeRuneInString(line[j:])
				if !ringWordRune(next) {
					break
				}
				j += nextSize
			}
			toks = append(toks, ringTok{kind: tokWord, text: line[i:j], start: i, end: j})
			i = j
		case r < utf8.RuneSelf && strings.ContainsRune(ringPunct, r):
			toks = append(toks, ringTok{kind: tokPunct, text: string(r), start: i, end: i + 1})
			i++
		case r == '"':
			end := ringStringEnd(line, i)
			if end < 0 {
				return toks, &ParseError{Pos: ringCharPos(line, i), Message: fmt.Sprintf(
					"unterminated string at character %d: add the closing double quote, as in \"ada\"", ringCharPos(line, i))}
			}
			text, err := strconv.Unquote(line[i:end])
			if err != nil {
				return toks, &ParseError{Pos: ringCharPos(line, i), Message: fmt.Sprintf(
					"the string at character %d has an escape the grammar does not know: use \\\" for a quote, \\\\ for a backslash, \\n for a newline", ringCharPos(line, i))}
			}
			toks = append(toks, ringTok{kind: tokString, text: text, start: i, end: end})
			i = end
		default:
			hint := ""
			if r == '\'' {
				hint = " (strings use double quotes, as in \"ada\")"
			}
			return toks, &ParseError{Pos: ringCharPos(line, i), Message: fmt.Sprintf(
				"character %q at position %d is not part of a statement%s. Run 'help' to list the statements", r, ringCharPos(line, i), hint)}
		}
	}
	return toks, nil
}

// ringStringEnd returns the byte offset after the closing quote of the string
// that opens at start, or -1 when the line ends first.
func ringStringEnd(line string, start int) int {
	for i := start + 1; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return -1
}
