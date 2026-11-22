//go:build linux

package cli

import (
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// openTestPty opens a pseudo-terminal and returns its master and slave. The
// slave is a real terminal, which is what several tests need: paging and the
// line editor both ask a file whether it is a terminal and how tall it is, and
// a buffer answers neither question. The test writes keys to the master end.
func openTestPty(t *testing.T) (master, slave *os.File) {
	t.Helper()
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no pseudo-terminal on this host: %v", err)
	}
	if err := unix.IoctlSetPointerInt(int(m.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		_ = m.Close()
		t.Skipf("unlocking the pseudo-terminal: %v", err)
	}
	number, err := unix.IoctlGetInt(int(m.Fd()), unix.TIOCGPTN)
	if err != nil {
		_ = m.Close()
		t.Skipf("naming the pseudo-terminal: %v", err)
	}
	s, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR, 0)
	if err != nil {
		_ = m.Close()
		t.Skipf("opening the pseudo-terminal's slave: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
		_ = m.Close()
	})
	return m, s
}

// setPtySize gives a pseudo-terminal a window size, so a test of the pager does
// not depend on the host's own screen.
func setPtySize(t *testing.T, f *os.File, rows, cols int) {
	t.Helper()
	if err := unix.IoctlSetWinsize(int(f.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: uint16(rows), Col: uint16(cols)}); err != nil {
		t.Fatalf("setting the pseudo-terminal's size: %v", err)
	}
}

// ptyOutput drains what the terminal's slave wrote, stopping once nothing more
// arrives. It is how a test reads the session's own output back.
func ptyOutput(t *testing.T, master *os.File) string {
	t.Helper()
	var out []byte
	poll := []unix.PollFd{{Fd: int32(master.Fd()), Events: unix.POLLIN}}
	for {
		ready, err := unix.Poll(poll, 200)
		if err != nil || ready == 0 {
			break
		}
		buf := make([]byte, 4096)
		read, _ := master.Read(buf)
		if read == 0 {
			break
		}
		out = append(out, buf[:read]...)
	}
	return string(out)
}

// feedPty types keys into the terminal's master end, as a user would.
func feedPty(t *testing.T, master *os.File, keys string) {
	t.Helper()
	if _, err := master.WriteString(keys); err != nil {
		t.Fatalf("writing to the pseudo-terminal: %v", err)
	}
	// The kernel hands the bytes to the slave end asynchronously; a short wait
	// keeps a test from racing the reader it just started.
	time.Sleep(20 * time.Millisecond)
}
