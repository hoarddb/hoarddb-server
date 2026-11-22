package cli

import (
	"crypto/sha256"
	"hash"
	"syscall"
)

// setUmaskForTest sets the process umask and returns the previous one, so a
// test can show a mode is chosen by the code and not inherited from the shell.
func setUmaskForTest(mask int) int { return syscall.Umask(mask) }

func newTestHasher() hash.Hash { return sha256.New() }
