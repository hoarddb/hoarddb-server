//go:build linux

package termio

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// isTerminal reports whether fd is a terminal.
func isTerminal(fd int) bool {
	_, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	return err == nil
}

// readPasswordNoEcho clears ECHO on the terminal, reads a line, and restores
// the previous attributes even on error.
func readPasswordNoEcho(fd int) (string, error) {
	original, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return "", fmt.Errorf("read terminal attributes: %w", err)
	}
	noEcho := *original
	noEcho.Lflag &^= unix.ECHO
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &noEcho); err != nil {
		return "", fmt.Errorf("disable terminal echo: %w", err)
	}
	defer func() {
		// Best effort: the terminal must be usable even if the read failed.
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, original)
	}()

	line, err := stdin().ReadString('\n')
	fmt.Fprintln(os.Stderr)
	if err != nil && line == "" {
		return "", fmt.Errorf("read password: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}
