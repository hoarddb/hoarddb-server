//go:build unix

package server

import "syscall"

// UmaskChange is what RestrictUmask found and left.
type UmaskChange struct {
	// Previous is the process umask before the call; Applied is the one in
	// force after it.
	Previous, Applied int
}

// RestrictUmask makes every file and directory the process creates from here
// on owner-only, by adding 0077 to the umask.
//
// Badger creates its *.vlog, *.mem, DISCARD and LOCK files at 0666, and the
// node's own files at 0644/0755 in places, so under the common umask 0002 they
// landed readable by the group. The store directory is 0700 and already
// shields them; this is hygiene for the case where that directory's mode is
// ever loosened, not a response to an exposure.
//
// It only adds bits: an operator who already runs under 0277 keeps it. The
// umask is process-global, which is the cost: a child process the server
// starts (the encryption.key_command) inherits it, and a backup agent that
// reads the data directory through group membership can no longer do so.
func RestrictUmask() UmaskChange {
	previous := syscall.Umask(0o077)
	applied := previous | 0o077
	syscall.Umask(applied)
	return UmaskChange{Previous: previous, Applied: applied}
}
