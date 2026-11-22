//go:build !linux

package termio

import (
	"fmt"
	"io"
)

// readLineRaw is unreachable on a platform without a termios implementation —
// isTerminal returns false there, so ReadLine takes the plain path — but the
// symbol must exist for the shared code to compile.
func readLineRaw(_ int, w io.Writer, prompt string, e *LineEditor) (string, error) {
	return "", fmt.Errorf("line editing needs a terminal: %w", ErrEOF)
}
