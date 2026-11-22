package ids

import (
	"fmt"

	"github.com/google/uuid"
)

// NewDocumentID generates a UUID v7 for a stored document key or _id_.
// It wraps uuid.NewV7() with error context. Unlike uuid.New(), which panics,
// this returns an error so every call site can handle it gracefully.
func NewDocumentID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generating a document id: %w", err)
	}
	return id.String(), nil
}

// NewRequestID generates an ID for transport frames or restore sessions.
// These are not data-visible, so they do not need v7 semantics. They use v4,
// which is always available and free of ordering constraints that would be unused.
func NewRequestID() [16]byte {
	return uuid.New()
}
