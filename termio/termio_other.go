//go:build !linux

package termio

import (
	"fmt"
	"strings"
)

// isTerminal is always false on platforms without a termios implementation,
// so ReadPassword falls back to a plain line read.
func isTerminal(fd int) bool { return false }

// readPasswordNoEcho is unreachable because isTerminal returns false, but the
// symbol must exist for the shared ReadPassword to compile.
func readPasswordNoEcho(fd int) (string, error) {
	line, err := stdin().ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("read password: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}
