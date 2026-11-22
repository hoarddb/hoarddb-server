//go:build darwin

package cli

import "golang.org/x/sys/unix"

// See termios_linux.go: darwin's termios ioctls carry the BSD names, with the
// same semantics as the Linux TCGETS/TCSETS pair.
const (
	termiosGetRequest = unix.TIOCGETA
	termiosSetRequest = unix.TIOCSETA
)
