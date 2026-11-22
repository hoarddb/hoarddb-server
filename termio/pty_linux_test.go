//go:build linux

package termio

import (
	"fmt"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// openTestPty opens a pseudo-terminal and returns its master and slave. The
// slave is a real terminal: it answers every termios ioctl the code under test
// makes, which a buffer cannot, so a test can exercise raw mode, key reads and
// the window size for real. Keys are delivered by writing to the master.
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
