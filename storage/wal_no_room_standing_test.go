package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// failSpareHeader fails the off mode's 24-byte birth while full is set.
func failSpareHeader(full *atomic.Bool) func(sys *walSyscalls) {
	return func(sys *walSyscalls) {
		real := sys.writeAt
		sys.writeAt = func(f *os.File, b []byte, off int64) (int, error) {
			if full.Load() && filepath.Base(f.Name()) == walSpareTmpName {
				return 0, fmt.Errorf("injected header write: %w", syscall.ENOSPC)
			}
			return real(f, b, off)
		}
	}
}

// An open with no segment and no write after it still refuses past 10 s,
// and keeps state.bson until a segment is named.
func TestWALOpenWithoutASegmentRefusesUntilAWrite(t *testing.T) {
	var full atomic.Bool
	full.Store(true)
	clock := newWALTestClock()
	withOpenSyscalls(t, func(sys *walSyscalls) {
		failSpareHeader(&full)(sys)
		sys.now = clock.read
	})
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, walStateFileName), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeWAL(t, w)
	clock.advance(11 * time.Second)
	if err := w.WriteRefusal(); err == nil {
		t.Fatalf("WriteRefusal cleared 11 s after an open with no segment, with no write in between")
	}
	if _, err := os.Stat(filepath.Join(dir, walStateFileName)); err != nil {
		t.Fatalf("an open with no segment removed the state file: %v", err)
	}
	full.Store(false)
	clock.advance(time.Second)
	if _, err := appendRotationEntry(w, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, walStateFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the activation kept the state file: %v", err)
	}
}

// The activation's failure after R0 poisons.
func TestWALActivationFailureAfterR0Poisons(t *testing.T) {
	var full, failDir atomic.Bool
	full.Store(true)
	clock := newWALTestClock()
	withOpenSyscalls(t, func(sys *walSyscalls) {
		failSpareHeader(&full)(sys)
		sys.now = clock.read
		real := sys.syncDir
		sys.syncDir = func(d string) error {
			if failDir.Load() {
				return fmt.Errorf("injected dir fsync: %w", syscall.EIO)
			}
			return real(d)
		}
	})
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	full.Store(false)
	failDir.Store(true)
	clock.advance(11 * time.Second)
	if _, err := appendRotationEntry(w, 0); err == nil {
		t.Fatal("append whose activation's dir fsync failed succeeded")
	}
	if err := w.poisonedErr(); err == nil {
		t.Fatal("a failed dir fsync at activation did not poison the log")
	}
}

// A failed rename or dir fsync at open still fails the open: the segment may have a name a crash can lose (build doc E20).
func TestWALOpenNamingFailureFailsTheOpen(t *testing.T) {
	for _, which := range []string{"rename", "syncDir"} {
		t.Run(which, func(t *testing.T) {
			withOpenSyscalls(t, func(sys *walSyscalls) {
				if which == "rename" {
					sys.rename = func(_, to string) error {
						return fmt.Errorf("injected rename to %s: %w", to, syscall.EIO)
					}
				} else {
					sys.syncDir = func(string) error { return fmt.Errorf("injected dir fsync: %w", syscall.EIO) }
				}
			})
			w, err := OpenWAL(t.TempDir(), nil)
			if err == nil {
				_ = w.Close()
				t.Fatalf("an open whose %s failed after R0 succeeded", which)
			}
		})
	}
}
