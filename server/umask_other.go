//go:build !unix

package server

// UmaskChange is what RestrictUmask found and left.
type UmaskChange struct {
	// Previous is the process umask before the call; Applied is the one in
	// force after it.
	Previous, Applied int
}

// RestrictUmask does nothing where the platform has no umask.
func RestrictUmask() UmaskChange { return UmaskChange{} }
