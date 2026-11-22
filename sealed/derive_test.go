package sealed

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestDeriveMatchesPublishedVectors(t *testing.T) {
	// Frozen derivation vectors to catch inadvertent changes to the info string.
	// These are derived from this implementation and must never change.
	vectors := []struct {
		purpose  byte
		identity string
		parentHx string
		wantHx   string
	}{
		{
			// NodeMaster wrap: 0x02, "node-master/1"
			purpose:  0x02,
			identity: "node-master/1",
			parentHx: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			wantHx:   "82725ae5be5c88944249fdcb2a6d336483adc9ad1a858093a275e91e388c7ff1",
		},
		{
			// Users json: 0x01, "users/1"
			purpose:  0x01,
			identity: "users/1",
			parentHx: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			wantHx:   "f4394dbc20670d3f1471f762d8b83b8f0768a7e7cf064e7e29b45dc4c7797fef",
		},
	}

	for _, v := range vectors {
		t.Run(hex.EncodeToString([]byte{v.purpose})+":"+v.identity, func(t *testing.T) {
			parentBytes, err := hex.DecodeString(v.parentHx)
			if err != nil {
				t.Fatalf("decoding parent: %v", err)
			}

			derived, err := FileKey(parentBytes, v.purpose, v.identity)
			if err != nil {
				t.Fatalf("FileKey failed: %v", err)
			}

			got := hex.EncodeToString(derived)
			if got != v.wantHx {
				t.Errorf("FileKey mismatch for %02x/%s\n  got:  %s\n  want: %s", v.purpose, v.identity, got, v.wantHx)
			}
		})
	}
}

func TestKidMatchesSigningKeyConstruction(t *testing.T) {
	// Kid must match server/authkey.go:newSigningKey's construction:
	// hex.EncodeToString(sha256.Sum256(keyBytes)[:])[:8]
	// This verifies the correspondence between sealed.Kid() and the auth key ID,
	// frozen to catch accidental changes to the kid computation.
	vectors := []struct {
		keyHx   string
		wantKid string
	}{
		// Test vector 1
		{"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20", "ae216c2e"},
		// Test vector 2
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "af961376"},
		// Test vector 3 (all zeros)
		{"0000000000000000000000000000000000000000000000000000000000000000", "66687aad"},
	}

	for i, v := range vectors {
		t.Run(fmt.Sprintf("vector_%d", i), func(t *testing.T) {
			keyBytes, err := hex.DecodeString(v.keyHx)
			if err != nil {
				t.Fatalf("decoding key: %v", err)
			}

			kid := Kid(keyBytes)
			if kid != v.wantKid {
				t.Errorf("Kid mismatch: got %s, want %s", kid, v.wantKid)
			}

			// Verify it has the right format (8 ASCII hex chars)
			if len(kid) != 8 {
				t.Errorf("Kid length: got %d, want 8", len(kid))
			}

			// Verify it matches the published construction
			hash := sha256.Sum256(keyBytes)
			expectedFromConstruction := hex.EncodeToString(hash[:])[:8]
			if kid != expectedFromConstruction {
				t.Errorf("Kid does not match construction: got %s, want %s", kid, expectedFromConstruction)
			}
		})
	}
}
