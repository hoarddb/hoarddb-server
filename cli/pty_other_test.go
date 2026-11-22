//go:build !linux

package cli

import (
	"os"
	"testing"
)

// openTestPty has no meaning where the CLI has no terminal support at all: the
// tests that need one skip here rather than pretending a file is a terminal.
func openTestPty(t *testing.T) (master, slave *os.File) {
	t.Helper()
	t.Skip("this platform has no terminal support to test against")
	return nil, nil
}

// setPtySize has no terminal to size where openTestPty has none.
func setPtySize(t *testing.T, f *os.File, rows, cols int) {
	t.Helper()
	t.Skip("this platform has no terminal support to test against")
}

// ptyOutput has nothing to drain where openTestPty has no terminal.
func ptyOutput(t *testing.T, master *os.File) string {
	t.Helper()
	t.Skip("this platform has no terminal support to test against")
	return ""
}

// feedPty has nothing to type into where openTestPty has no terminal.
func feedPty(t *testing.T, master *os.File, keys string) {
	t.Helper()
	t.Skip("this platform has no terminal support to test against")
}
