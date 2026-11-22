//go:build freebsd

package cli

import "golang.org/x/sys/unix"

// See termios_linux.go: the BSD platforms name the termios ioctls TIOCGETA and
// TIOCSETA rather than Linux's TCGETS and TCSETS, with the same semantics.
const (
	termiosGetRequest = unix.TIOCGETA
	termiosSetRequest = unix.TIOCSETA
)
