//go:build !linux

package termio

import "os"

// Size reports no size, because this platform has no termios implementation
// (the same reason isTerminal is always false here). Callers treat that as
// "do not page": a pager with no height cannot decide what fits.
func Size(f *os.File) (cols, rows int, ok bool) {
	return 0, 0, false
}
