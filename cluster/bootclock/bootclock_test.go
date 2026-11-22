package bootclock

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
)

// errDiskFull is what the failing writer returns: a stderr that cannot take the warning.
var errDiskFull = errors.New("disk full")

// failingWriter stands in for a stderr that refuses every write.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errDiskFull }

// TestNonLinuxWarnsOnceAndRefusesNothing runs on every platform. A darwin start writes one warning and carry on:
// WarnOnce returns the write error the first time, so the node-start code can log it, and nil on the second call,
// which is once-only even when the first write failed. A linux start writes nothing and returns nil.
func TestNonLinuxWarnsOnceAndRefusesNothing(t *testing.T) {
	warnOnce = sync.Once{}

	err := WarnOnce(failingWriter{}, "darwin")
	if !errors.Is(err, errDiskFull) {
		t.Fatalf("WarnOnce on a failing writer returned %v, want the write error %v", err, errDiskFull)
	}
	if !strings.Contains(err.Error(), "writing the boot clock warning") {
		t.Fatalf("WarnOnce error %q does not say which write failed", err)
	}
	var second bytes.Buffer
	if err := WarnOnce(&second, "darwin"); err != nil {
		t.Fatalf("second WarnOnce returned %v, want nil: the warning is once-only", err)
	}
	if second.Len() != 0 {
		t.Fatalf("second WarnOnce wrote %q, want nothing", second.String())
	}

	warnOnce = sync.Once{}
	var darwin bytes.Buffer
	if err := WarnOnce(&darwin, "darwin"); err != nil {
		t.Fatalf("WarnOnce on darwin returned %v, want nil", err)
	}
	if err := WarnOnce(&darwin, "darwin"); err != nil {
		t.Fatalf("second WarnOnce on darwin returned %v, want nil", err)
	}
	if lines := strings.Count(darwin.String(), "\n"); lines != 1 {
		t.Fatalf("two WarnOnce calls on darwin wrote %d lines, want exactly 1: %q", lines, darwin.String())
	}
	for _, want := range []string{"darwin", "suspend"} {
		if !strings.Contains(darwin.String(), want) {
			t.Fatalf("warning %q does not name %q", darwin.String(), want)
		}
	}

	warnOnce = sync.Once{}
	var linux bytes.Buffer
	if err := WarnOnce(&linux, "linux"); err != nil {
		t.Fatalf("WarnOnce on linux returned %v, want nil", err)
	}
	if linux.Len() != 0 {
		t.Fatalf("WarnOnce on linux wrote %q, want nothing", linux.String())
	}
}
