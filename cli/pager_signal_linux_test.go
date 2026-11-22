//go:build linux

package cli

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// pagerChildEnv marks the test binary as the pager's child process. The tests
// below need a process that really owns a pseudo-terminal and really receives
// a signal from outside: feeding the pager's own channel (as
// TestPagerRestoresTheTerminalWhenAStopSignalArrives does) cannot show that the
// signals are subscribed to, nor how the process ends.
const pagerChildEnv = "HOARDDB_TEST_PAGER_CHILD"

// TestPagerChildProcess is not a test: it is the body of the child process the
// signal tests start. It pages a long report on the pseudo-terminal it was
// handed as fd 3 and waits for a key that never comes.
func TestPagerChildProcess(t *testing.T) {
	if os.Getenv(pagerChildEnv) == "" {
		t.Skip("child body of the pager signal tests")
	}
	slave := os.NewFile(3, "pty-slave")
	c := NewCLI(WithTerminal(slave))
	c.reader = slave
	if err := c.page(numberedReport(60), 10); err != nil {
		_, _ = os.Stderr.WriteString("page: " + err.Error() + "\n")
		os.Exit(2)
	}
}

// pagerChild is a started child process paging on a pseudo-terminal.
type pagerChild struct {
	cmd    *exec.Cmd
	master *os.File
	output string
}

// startPagerChild starts the child and returns once its pager is on screen.
func startPagerChild(t *testing.T) *pagerChild {
	t.Helper()
	master, slave := openTestPty(t)
	setPtySize(t, slave, 10, 80)

	// env --default-signal=HUP: a test run started under nohup, or by a
	// supervisor that ignores SIGHUP, passes the ignore on to its children, and
	// a signal that is ignored at exec cannot be put back by the child (Go
	// keeps an inherited ignore across signal.Reset), so the hangup case
	// would end with exit status 129 instead of death by SIGHUP. The test is
	// about the pager's own handling, so the child starts from the default.
	cmd := exec.Command("env", "--default-signal=HUP", os.Args[0], "-test.run=^TestPagerChildProcess$")
	cmd.Env = append(os.Environ(), pagerChildEnv+"=1", "TERM=xterm-256color")
	cmd.ExtraFiles = []*os.File{slave}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the pager child: %v", err)
	}
	child := &pagerChild{cmd: cmd, master: master}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	child.readUntil(t, altScreenOn)
	return child
}

// readUntil reads the terminal until marker has appeared, or fails the test.
func (c *pagerChild) readUntil(t *testing.T, marker string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(c.output, marker) {
		if time.Now().After(deadline) {
			t.Fatalf("the child never wrote %q; it wrote %q", marker, c.output)
		}
		c.output += ptyOutput(t, c.master)
	}
}

// waitStatus waits for the child to end and returns how.
func (c *pagerChild) waitStatus(t *testing.T) syscall.WaitStatus {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	select {
	case err := <-done:
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Fatalf("waiting for the pager child: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pager child ignored the signal")
	}
	return c.cmd.ProcessState.Sys().(syscall.WaitStatus)
}

var stopSignals = []syscall.Signal{syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT}

// TestPagerDiesBySignalItWasSent: a process stopped by SIGTERM must end as
// killed by SIGTERM, so the shell reports 128+15 and a supervisor can tell a
// stop from a failure. Ending with a plain exit status 1 reads as an error.
func TestPagerDiesBySignalItWasSent(t *testing.T) {
	for _, sig := range stopSignals {
		t.Run(sig.String(), func(t *testing.T) {
			child := startPagerChild(t)
			if err := child.cmd.Process.Signal(sig); err != nil {
				t.Fatalf("sending %v: %v", sig, err)
			}
			status := child.waitStatus(t)
			if !status.Signaled() || status.Signal() != sig {
				t.Fatalf("the pager child ended with %v, want death by %v", status, sig)
			}
		})
	}
}

// TestPagerRestoresTheTerminalForARealSignal sends a real signal from outside
// and checks the terminal afterwards. It is the guard for the pager subscribing
// to the stop signals at all: without that, the signal takes its default course
// and the terminal stays on the alternate screen in raw mode.
func TestPagerRestoresTheTerminalForARealSignal(t *testing.T) {
	for _, sig := range stopSignals {
		t.Run(sig.String(), func(t *testing.T) {
			child := startPagerChild(t)
			if err := child.cmd.Process.Signal(sig); err != nil {
				t.Fatalf("sending %v: %v", sig, err)
			}
			child.waitStatus(t)
			child.output += ptyOutput(t, child.master)

			if !strings.HasSuffix(child.output, altScreenOff) {
				t.Errorf("the pager did not leave the alternate screen; output ended %q", tail(child.output, 40))
			}
			attributes, err := unix.IoctlGetTermios(int(child.master.Fd()), unix.TCGETS)
			if err != nil {
				t.Fatalf("reading the terminal's attributes: %v", err)
			}
			if attributes.Lflag&(unix.ICANON|unix.ECHO) != unix.ICANON|unix.ECHO {
				t.Errorf("the terminal was left in raw mode (lflag %#x)", attributes.Lflag)
			}
		})
	}
}
