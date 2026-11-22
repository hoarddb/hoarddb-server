package cli

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestLegacyReplDumpSaysItIsAnotherFormat: the REPL `dump` statement writes the legacy
// engine's format, which `hoarddb restore` on the ring cannot read, and it says so after
// its summary (section 5 of ring-cli-parity-build.md).
func TestLegacyReplDumpSaysItIsAnotherFormat(t *testing.T) {
	const note = `This is the file of the dump statement. 'hoarddb restore' reads only files from 'hoarddb dump'; they are different formats.`
	session := newFixtureSession(t)
	transcript := runStatements(t, session.cli, session.out, "dump app --out "+filepath.Join(session.outDir, "note"))
	if !strings.Contains(transcript, note) {
		t.Fatalf("the REPL dump statement does not say it writes another format:\n%s", transcript)
	}
}
