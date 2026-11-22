package main

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("entropy gone") }

func frozenGenerator(at time.Time) *idGenerator {
	return newIDGenerator(func() time.Time { return at }, bytes.NewReader(bytes.Repeat([]byte{0xA5}, 1<<22)))
}

func TestIDShapeIsAUUIDv7(t *testing.T) {
	id, err := frozenGenerator(time.UnixMilli(1_700_000_000_123)).next()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		t.Fatalf("id %q is not 8-4-4-4-12", id)
	}
	if id[14] != '7' {
		t.Fatalf("version nibble %q in %q, want 7", id[14], id)
	}
	if !strings.ContainsRune("89ab", rune(id[19])) {
		t.Fatalf("variant nibble %q in %q, want one of 8 9 a b", id[19], id)
	}
	stamp, err := strconv.ParseInt(id[:8]+id[9:13], 16, 64)
	if err != nil || stamp != 1_700_000_000_123 {
		t.Fatalf("timestamp prefix of %q is %d (%v), want 1700000000123", id, stamp, err)
	}
}

func TestIDsFromOneInstantAreUniqueAndAscending(t *testing.T) {
	gen := frozenGenerator(time.UnixMilli(1_700_000_000_000))
	seen := make(map[string]bool, 20000)
	previous := ""
	for i := 0; i < 20000; i++ {
		id, err := gen.next()
		if err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("id %q issued twice", id)
		}
		if id <= previous {
			t.Fatalf("id %q does not sort after %q", id, previous)
		}
		seen[id], previous = true, id
	}
}

func TestIDsDoNotGoBackwardsWhenTheClockDoes(t *testing.T) {
	now := time.UnixMilli(1_700_000_005_000)
	gen := newIDGenerator(func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{1}, 4096)))
	first, _ := gen.next()
	now = now.Add(-3 * time.Second)
	second, err := gen.next()
	if err != nil || second <= first {
		t.Fatalf("second %q (%v) does not sort after first %q", second, err, first)
	}
}

func TestIDGeneratorReportsEntropyFailure(t *testing.T) {
	gen := newIDGenerator(time.Now, failingReader{})
	if _, err := gen.next(); err == nil || !strings.Contains(err.Error(), "entropy gone") {
		t.Fatalf("err %v, want the reader's failure wrapped", err)
	}
}
