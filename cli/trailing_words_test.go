package cli

import (
	"strings"
	"testing"
)

// TestNoArgumentStatementsRefuseTrailingWords: the keyword arms returned a
// command without looking at the words after it, so `use app extra` switched
// database and the extra word was taken for nothing.
func TestNoArgumentStatementsRefuseTrailingWords(t *testing.T) {
	refused := []struct {
		line      string
		extraWord string
	}{
		{"use app extra", "extra"},
		{"status extra", "extra"},
		{"version now", "now"},
		{"exit now", "now"},
		{"quit now", "now"},
		{"nodes now", "now"},
		{"nodes history now", "now"},
		{"encryption on", "on"},
		{"master now", "now"},
		{"theme dark", "dark"},
	}
	for _, tc := range refused {
		t.Run(tc.line, func(t *testing.T) {
			cmd, err := ParseCommand(tc.line)
			if err == nil {
				t.Fatalf("ParseCommand(%q) = %+v, want a usage error", tc.line, cmd)
			}
			if !strings.Contains(err.Error(), tc.extraWord) {
				t.Errorf("error %q does not name the extra word %q", err, tc.extraWord)
			}
		})
	}
}

// TestNoArgumentStatementsStillParseTheirValidForms guards the other side.
func TestNoArgumentStatementsStillParseTheirValidForms(t *testing.T) {
	for _, line := range []string{
		"use app", "use app;", "status", "version", "exit", "quit", "nodes", "nodes history",
		"encryption", "master", "theme", "help", "help fields",
	} {
		t.Run(line, func(t *testing.T) {
			if _, err := ParseCommand(line); err != nil {
				t.Errorf("ParseCommand(%q): %v", line, err)
			}
		})
	}
}
