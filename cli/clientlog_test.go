package cli

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestClientLoggerDropsOnlyTheDuplicate is the guard for the narrowing: the
// client's library layer logged the insecure-trust fact a second time, in a
// shape a user cannot act on, and it was silenced along with everything else —
// including a fingerprint the client could not record, which is a real warning.
// The handler must drop the duplicate and forward the rest.
func TestClientLoggerDropsOnlyTheDuplicate(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(clientLogHandler(&out, false))

	logger.Warn(insecureTrustRecord, "host", "127.0.0.1:7433")
	if strings.Contains(out.String(), "insecure trust policy") {
		t.Fatalf("the duplicate insecure record was printed: %q", out.String())
	}

	logger.Warn("failed to record learned peer fingerprint", "host", "n1", "error", errors.New("read-only file system"))
	if !strings.Contains(out.String(), "failed to record learned peer fingerprint") {
		t.Fatalf("a real client-side warning was dropped: %q", out.String())
	}
	if !strings.Contains(out.String(), "read-only file system") {
		t.Fatalf("the warning lost its reason: %q", out.String())
	}

	logger.Debug("a connection detail")
	if strings.Contains(out.String(), "a connection detail") {
		t.Fatalf("a debug record was printed without -debug: %q", out.String())
	}
}

// TestClientLoggerKeepsTheDuplicateUnderDebug: -debug is the one place the
// structured record is worth having, so the narrowing must not take it away
// there.
func TestClientLoggerKeepsTheDuplicateUnderDebug(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(clientLogHandler(&out, true))

	logger.Warn(insecureTrustRecord, "host", "127.0.0.1:7433")
	if !strings.Contains(out.String(), "insecure trust policy") {
		t.Fatalf("with -debug the record was dropped: %q", out.String())
	}
	logger.Debug("a connection detail")
	if !strings.Contains(out.String(), "a connection detail") {
		t.Fatalf("with -debug a debug record was dropped: %q", out.String())
	}
}

// TestInstallClientLoggerRoutesWarningsToStderr is the wiring rather than the
// handler: installClientLogger must put the narrowed stream on the process's
// default logger, so a Warn from transport/ lands on stderr and the duplicate
// does not.
func TestInstallClientLoggerRoutesWarningsToStderr(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	captured, err := os.CreateTemp(t.TempDir(), "stderr-*")
	if err != nil {
		t.Fatalf("capturing stderr: %v", err)
	}
	defer func() { _ = captured.Close() }()
	realStderr := os.Stderr
	os.Stderr = captured
	t.Cleanup(func() { os.Stderr = realStderr })

	installClientLogger(false)
	slog.Warn("failed to record learned peer fingerprint", "host", "n1")
	slog.Warn(insecureTrustRecord, "host", "n1")

	os.Stderr = realStderr
	if _, err := captured.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewinding the capture: %v", err)
	}
	data, err := io.ReadAll(captured)
	if err != nil {
		t.Fatalf("reading the capture: %v", err)
	}
	if !strings.Contains(string(data), "learned peer fingerprint") {
		t.Fatalf("installClientLogger did not forward a client warning: %q", string(data))
	}
	if strings.Contains(string(data), "insecure trust policy") {
		t.Fatalf("installClientLogger forwarded the duplicate: %q", string(data))
	}
}
