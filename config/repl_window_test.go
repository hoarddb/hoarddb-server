package config

import (
	"strings"
	"testing"
)

// TestReplWindowIsRefusedOutsideItsRange (BUG-0745): replication.repl_window now sizes the owner's
// window and the copy's queue, so a typo is refused at start with the key and the range.
// [GUARD: drop the range check]
func TestReplWindowIsRefusedOutsideItsRange(t *testing.T) {
	cfg := DefaultConfig()
	for _, ok := range []int{1, 64, MaxReplWindow} {
		cfg.Replication.ReplWindow = ok
		if err := cfg.ValidateReplWindow(); err != nil {
			t.Fatalf("window %d refused: %v", ok, err)
		}
	}
	for _, bad := range []int{0, -1, MaxReplWindow + 1, 1_000_000} {
		cfg.Replication.ReplWindow = bad
		err := cfg.ValidateReplWindow()
		if err == nil || !strings.Contains(err.Error(), "replication.repl_window") || !strings.Contains(err.Error(), "4096") {
			t.Fatalf("window %d: %v, want a refusal naming the key and the range", bad, err)
		}
	}
}
