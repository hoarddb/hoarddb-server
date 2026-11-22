//go:build linux

package termio

import (
	"os"

	"golang.org/x/sys/unix"
)

// Size reports the terminal's width and height in character cells. ok is false
// when f is not a terminal, or when the terminal has no size to report — the
// answer a pager needs before it pages anything, because a pipe has no height
// and neither has a terminal that has not been told its own.
func Size(f *os.File) (cols, rows int, ok bool) {
	ws, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ)
	if err != nil || ws.Col == 0 || ws.Row == 0 {
		return 0, 0, false
	}
	return int(ws.Col), int(ws.Row), true
}
