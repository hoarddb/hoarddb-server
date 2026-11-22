//go:build !linux

package termio

import (
	"os"
	"testing"
)

// openTestPty has no meaning where there is no termios to drive: every test
// that needs a terminal skips here rather than pretending a file is one.
func openTestPty(t *testing.T) (master, slave *os.File) {
	t.Helper()
	t.Skip("this platform has no termios, so there is no terminal to test against")
	return nil, nil
}
