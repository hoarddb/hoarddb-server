package sealed

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// FileKey derives a file-specific key from a parent key, purpose, and identity.
// It implements the key derivation pattern from §2.5 of the spec.
// parent must be exactly 32 bytes.
//
// The identity is appended to the info string after a fixed prefix and the
// purpose, so two identities derive the same key only if they are the same
// string. A per-bucket identity such as "db/bucket/gen" is unambiguous because
// server/restore.go:validRestoreNameComponent refuses a database or bucket
// name that is empty, contains a NUL, is absolute, contains "/" or "\", or
// is "." or "..". A node-scope identity carries no such name: a blob payload's
// is store.PayloadFileName, <slot:3 hex>-<bucket id:16 hex>-<blob id>, which
// has no "/" in it by construction. If either validator or that format is ever
// relaxed, this encoding must change with it.
func FileKey(parent []byte, purpose byte, identity string) ([]byte, error) {
	if len(parent) != 32 {
		return nil, fmt.Errorf("deriving file key: parent key must be 32 bytes, got %d", len(parent))
	}

	info := fmt.Sprintf("github.com/hoarddb/hoarddb-server/v2/%02x/%s", purpose, identity)
	derived, err := hkdf.Key(sha256.New, parent, nil, info, 32)
	if err != nil {
		return nil, fmt.Errorf("deriving file key: %w", err)
	}
	return derived, nil
}

// Kid returns the key ID: the first 8 hex characters of sha256(key).
// This matches the construction in server/authkey.go:newSigningKey.
func Kid(key []byte) string {
	hash := sha256.Sum256(key)
	return hex.EncodeToString(hash[:])[:8]
}
