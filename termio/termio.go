// Package termio reads secrets from a terminal without echoing them. It
// exists so the CLI and cmd/server prompt for passwords the same way and
// neither links a new module: the no-echo path uses golang.org/x/sys, which
// the server module already depends on.
package termio

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

// sharedStdin is the single buffered reader over standard input, so a second
// prompt does not lose a line the first read buffered ahead (bufio over-reads
// by design).
var sharedStdin struct {
	mu     sync.Mutex
	file   *os.File
	reader *bufio.Reader
}

// stdin returns the shared reader. It is rebuilt when os.Stdin has been
// replaced — which only a test that drives a session from a pseudo-terminal
// does — because a reader bound once at start-up would go on reading the file
// the process began with.
func stdin() *bufio.Reader {
	sharedStdin.mu.Lock()
	defer sharedStdin.mu.Unlock()
	if sharedStdin.reader == nil || sharedStdin.file != os.Stdin {
		sharedStdin.file = os.Stdin
		sharedStdin.reader = bufio.NewReader(os.Stdin)
	}
	return sharedStdin.reader
}

// ReadPasswordTwice prompts twice and returns the value only when both
// entries match and the value is non-empty.
func ReadPasswordTwice(prompt, repeat string) (string, error) {
	first, err := ReadPassword(prompt)
	if err != nil {
		return "", err
	}
	second, err := ReadPassword(repeat)
	if err != nil {
		return "", err
	}
	if first != second {
		return "", fmt.Errorf("the two passwords did not match; nothing was changed")
	}
	if first == "" {
		return "", fmt.Errorf("the password is empty; nothing was changed")
	}
	return first, nil
}

// StdinIsTerminal reports whether standard input is a terminal, so that a
// prompt has someone to answer it.
func StdinIsTerminal() bool {
	return isTerminal(int(os.Stdin.Fd()))
}

// StderrIsTerminal reports whether standard error is a terminal. A secret
// printed anywhere else ends up in whatever records the output: a service
// manager's journal, a container log, a file.
func StderrIsTerminal() bool {
	return isTerminal(int(os.Stderr.Fd()))
}

// ReadPassword writes prompt to stderr and reads one line without echoing it
// when stdin is a terminal. When stdin is not a terminal (piped or
// redirected) it reads a line as-is — the only option, and echo does not
// apply.
func ReadPassword(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)
	fd := int(os.Stdin.Fd())
	if !isTerminal(fd) {
		line, err := stdin().ReadString('\n')
		if err != nil && line == "" {
			return "", fmt.Errorf("read password: %w", err)
		}
		fmt.Fprintln(os.Stderr)
		return strings.TrimRight(line, "\r\n"), nil
	}
	return readPasswordNoEcho(fd)
}
