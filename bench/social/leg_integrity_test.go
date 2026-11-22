package main

import (
	"context"
	"strings"
	"testing"
)

// TestLegFailsWhenTheLaunchContradictsTheLevel: a leg claiming L-A against a node
// launched with the unsafe switch compares unlike things, so it fails and the
// record says why (spec 2, integrity failure 1).
func TestLegFailsWhenTheLaunchContradictsTheLevel(t *testing.T) {
	cfg := mixTestConfig("rh", "social_integrity")
	cfg.Level = "L-A"
	cfg.HoardEnv = writeTestFile(t, "launch.env", "HOARDDB_UNSAFE_ACK_BEFORE_SYNC=true\n")
	cfg.HoardLog = writeTestFile(t, "server.log", unsafeWarningLine)
	record, err := runMixLeg(context.Background(), cfg, tinySpec, fakeHoardEngine(cfg))
	if err == nil || !strings.Contains(err.Error(), "integrity failure") {
		t.Fatalf("runMixLeg error = %v; want a P6 integrity failure", err)
	}
	if len(record.Integrity) != 1 || !strings.Contains(record.Integrity[0], "L-A") {
		t.Errorf("record integrity failures = %v; want the L-A contradiction", record.Integrity)
	}
}
