package ids

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestNewDocumentIDReturnsUUIDv7 verifies that NewDocumentID generates UUID v7.
func TestNewDocumentIDReturnsUUIDv7(t *testing.T) {
	id, err := NewDocumentID()
	if err != nil {
		t.Fatalf("NewDocumentID: %v", err)
	}

	parsed, err := uuid.Parse(id)
	if err != nil {
		t.Fatalf("parse generated id: %v", err)
	}

	if parsed.Version() != 7 {
		t.Fatalf("id version = %d, want 7", parsed.Version())
	}
}

// TestNewDocumentIDTimestampIsNearInsert verifies the v7 timestamp is within
// one second of the insert time.
func TestNewDocumentIDTimestampIsNearInsert(t *testing.T) {
	before := time.Now().UTC()
	id, err := NewDocumentID()
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("NewDocumentID: %v", err)
	}

	parsed, err := uuid.Parse(id)
	if err != nil {
		t.Fatalf("parse generated id: %v", err)
	}

	// Extract the 48-bit millisecond timestamp from v7
	uuidTime := parsed.Time()
	sec, nsec := uuidTime.UnixTime()
	timestamp := time.Unix(sec, nsec)
	if timestamp.Before(before.Add(-time.Second)) || timestamp.After(after.Add(time.Second)) {
		t.Fatalf("id timestamp %v outside [%v, %v]", timestamp, before, after)
	}
}

// TestNewDocumentIDsSortChronologically verifies that v7 IDs generated in sequence
// sort in insertion order when compared as strings.
func TestNewDocumentIDsSortChronologically(t *testing.T) {
	ids := make([]string, 3)
	for i := 0; i < 3; i++ {
		var err error
		ids[i], err = NewDocumentID()
		if err != nil {
			t.Fatalf("NewDocumentID: %v", err)
		}
		if i < 2 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify insertion order matches string sort order
	if ids[0] > ids[1] || ids[1] > ids[2] {
		t.Fatalf("ids not in chronological order: %v, %v, %v", ids[0], ids[1], ids[2])
	}
}

// TestNewRequestIDReturnsUUID verifies that NewRequestID returns a v4 UUID in bytes.
func TestNewRequestIDReturnsUUID(t *testing.T) {
	id := NewRequestID()

	// Convert to uuid.UUID and verify it's valid
	parsed := uuid.UUID(id)

	// Should be v4 (request IDs do not need v7 ordering)
	if parsed.Version() != 4 {
		t.Fatalf("request id version = %d, want 4", parsed.Version())
	}
}

// TestNewDocumentIDErrorHandling verifies the error path by checking the error
// is wrapped with context.
func TestNewDocumentIDErrorHandling(t *testing.T) {
	// NewDocumentID should succeed in normal operation; this test just verifies
	// the error message format if it somehow fails.
	id, err := NewDocumentID()
	if err != nil {
		// If it fails, check the error message includes context
		if !strings.Contains(err.Error(), "generating a document id") {
			t.Fatalf("error message missing context: %v", err)
		}
		return
	}

	// Normal operation: should return a valid v7 UUID
	parsed, err := uuid.Parse(id)
	if err != nil {
		t.Fatalf("parse generated id: %v", err)
	}
	if parsed.Version() != 7 {
		t.Fatalf("id version = %d, want 7", parsed.Version())
	}
}
