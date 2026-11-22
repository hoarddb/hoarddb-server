package server

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestAbbreviatedCutsOnACharacterBoundary: a lease id, item id or dead id the
// client sent is echoed cut to 64 bytes, and the cut never splits a character.
// Split, the half character is escaped by %q as \xc3 in the refusal, or sent
// as invalid UTF-8 by any caller that does not quote it. Guard: cut at the
// byte limit in abbreviated (D4 verification finding S2).
func TestAbbreviatedCutsOnACharacterBoundary(t *testing.T) {
	text := strings.Repeat("a", 63) + "é" + strings.Repeat("b", 10)
	cut := abbreviated(text, 64)
	if !utf8.ValidString(cut) || cut != strings.Repeat("a", 63)+"…" {
		t.Errorf("abbreviated(%q, 64) = %q; want the 63 whole characters before the cut and an ellipsis", text, cut)
	}
	if kept := abbreviated("é", 1); kept != "…" {
		t.Errorf("abbreviated of a two-byte character to one byte = %q; want the ellipsis alone", kept)
	}
	for what, err := range map[string]error{
		"lease id": func() error { _, err := leaseIDOf(text, "jobs"); return err }(),
		"item id":  func() error { _, err := itemIDsOf([]string{text}, BucketTypeFIFO, "jobs"); return err }(),
		"dead id":  func() error { _, err := deadIDOf(text, "jobs"); return err }(),
	} {
		if err == nil {
			t.Fatalf("a %s of %q was accepted", what, text)
		}
		if strings.Contains(err.Error(), `\x`) || !utf8.ValidString(err.Error()) {
			t.Errorf("the %s refusal echoes half a character: %s", what, err)
		}
	}
}
