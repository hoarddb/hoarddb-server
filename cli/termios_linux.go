//go:build linux

package cli

import "golang.org/x/sys/unix"

// The termios get/set ioctl request numbers differ per platform and must
// resolve at compile time, so they are selected by build tag rather than by
// runtime.GOOS. Linux names them TCGETS/TCSETS; darwin names the same pair
// TIOCGETA/TIOCSETA (termios_darwin.go). Everything else queryBackground
// touches — IoctlGetTermios, IoctlSetTermios, Poll, ICANON, ECHO — is spelled
// the same on both.
const (
	termiosGetRequest = unix.TCGETS
	termiosSetRequest = unix.TCSETS
)
