//go:build !unix

package transport

import (
	"fmt"
	"io/fs"
	"os"
)

// The known_servers store needs flock(2) and POSIX ownership to keep its
// safety and saving rules (trust.md sections 4.3 and 4.4). Without them it
// refuses, rather than offer a check that only looks like one.

func knownServersUnsupported(path string) error {
	return fmt.Errorf("the known_servers trust store %s cannot be used on this platform: it needs flock(2) and POSIX file ownership, which only unix builds have; pin the server's fingerprint instead", path)
}

func openNoFollow(path string, _ int, _ os.FileMode) (*os.File, error) {
	return nil, knownServersUnsupported(path)
}

func lockExclusive(file *os.File) error {
	return knownServersUnsupported(file.Name())
}

func trustPathFacts(path string, _ fs.FileInfo) (TrustPathFacts, error) {
	return TrustPathFacts{}, knownServersUnsupported(path)
}
