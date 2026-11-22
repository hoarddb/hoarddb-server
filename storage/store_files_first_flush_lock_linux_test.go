//go:build linux

package storage

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestTheFirstFlushRepairDoesNotTouchAStoreAnotherProcessHolds defends the
// directory lock: Badger flocks the store directory only inside its open, so
// the repair takes the same lock, and refuses while another process holds it.
func TestTheFirstFlushRepairDoesNotTouchAStoreAnotherProcessHolds(t *testing.T) {
	dir, tableName := writeFirstFlushCrashStore(t, nil)
	holder, err := os.Open(dir)
	if err != nil {
		t.Fatalf("opening the directory: %v", err)
	}
	defer func() { _ = holder.Close() }()
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("locking the directory: %v", err)
	}

	_, openErr := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
	if openErr == nil || !strings.Contains(openErr.Error(), "is in use by another process; stop it first") {
		t.Fatalf("want the in-use refusal, got %v", openErr)
	}
	if _, err := os.Stat(filepath.Join(dir, tableName)); err != nil {
		t.Errorf("the repair moved the table of a store another process holds: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, tableName+".first-flush-crash")); err == nil {
		t.Error("a quarantine file was made")
	}
}
