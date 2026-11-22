package server

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// A keys/store.wrapped written by createNodeStoreKey at af29f2f, before the
// purpose constant was renamed, under encryptionTestNodeMaster, and the data
// key it wraps. Frozen: a store sealed before the rename must still open, so
// neither the purpose byte nor the identity may move. Cross-checked by
// unwrapping it with an independent HKDF + AES-GCM implementation.
const (
	preRenameWrappedStoreKey = "0048444202036165323136633265000000000000000187ad726b48eee06ff45998b2b38b6ce802dcc45071ed85e7d2f53722dcae7f42c5fb7f6ff0094ec833ac24709d7b4e08f0823c11a3f6480872bf3926"
	preRenameStoreDataKey    = "7bcdf2ce19b0ba1ea3944a7842be892513b55846ee64b00c2c8da3481d90e0cc"
)

// TestStoreKeyWrappedBeforeTheRenameStillOpens: the node store's data key is
// sealed under purpose 0x03, now named for what it is (spec rev 4 §2.3; §0.6
// item 4). The rename moves no byte: the frozen store key unwraps through
// nodeStoreDataKey to the frozen data key.
func TestStoreKeyWrappedBeforeTheRenameStillOpens(t *testing.T) {
	if sealed.PurposeStoreKey != 0x03 {
		t.Fatalf("PurposeStoreKey is %#02x; the store key has always been sealed under 0x03", sealed.PurposeStoreKey)
	}
	wrapped, err := hex.DecodeString(preRenameWrappedStoreKey)
	if err != nil {
		t.Fatalf("decoding the frozen store key: %v", err)
	}
	wantDataKey, err := hex.DecodeString(preRenameStoreDataKey)
	if err != nil {
		t.Fatalf("decoding the frozen data key: %v", err)
	}
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "keys"), 0o700); err != nil {
		t.Fatalf("creating keys/: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "keys", nodeStoreKeyFileName), wrapped, 0o600); err != nil {
		t.Fatalf("writing the frozen store key: %v", err)
	}

	dataKey, err := nodeStoreDataKey(encryptionTestConfig(dataDir, "on", "keyfile"), encryptionTestNodeMaster())
	if err != nil {
		t.Fatalf("a store key wrapped before the rename no longer opens: %v", err)
	}
	if !bytes.Equal(dataKey, wantDataKey) {
		t.Fatalf("the frozen store key unwrapped to %x, want %x", dataKey, wantDataKey)
	}
}

// TestNodeStoreDataKeyDiagnosesMissingKeyOnSealedStore: a sealed
// node store (its Badger KEYREGISTRY rejects an empty key) whose
// keys/store.wrapped is missing must be refused with a diagnosis naming the
// truth — sealed, key file missing, restore from backup/escrow — never
// "not encrypted", which sends the operator toward encryption.mode off, a
// setting that cannot open a sealed KEYREGISTRY.
func TestNodeStoreDataKeyDiagnosesMissingKeyOnSealedStore(t *testing.T) {
	dataDir := t.TempDir()
	cfg := encryptionTestConfig(dataDir, "on", "keyfile")
	nm := encryptionTestNodeMaster()

	// Create a genuinely sealed store: openNodeStore on a fresh data dir with
	// mode=on writes keys/store.wrapped and opens Badger with the unwrapped
	// data key, which seals the KEYREGISTRY under it.
	kv, _, err := openNodeStore(cfg, nm)
	if err != nil {
		t.Fatalf("creating the sealed store: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the sealed store: %v", err)
	}

	keyPath := filepath.Join(dataDir, "keys", nodeStoreKeyFileName)
	if err := os.Remove(keyPath); err != nil {
		t.Fatalf("removing %s to simulate a lost key file: %v", keyPath, err)
	}

	_, err = nodeStoreDataKey(cfg, nm)
	if err == nil {
		t.Fatalf("nodeStoreDataKey on a sealed store with a missing key file: got nil error, want a key-missing refusal")
	}
	if strings.Contains(err.Error(), "not encrypted") {
		t.Fatalf("nodeStoreDataKey misreported a sealed store with a missing key file as \"not encrypted\": %v", err)
	}
	if !strings.Contains(err.Error(), "sealed") {
		t.Fatalf("nodeStoreDataKey's refusal does not say the store is sealed: %v", err)
	}
	if !strings.Contains(err.Error(), "keys/store.wrapped") {
		t.Fatalf("nodeStoreDataKey's refusal does not name the missing key file: %v", err)
	}
}

// TestNodeStoreDataKeyKeepsNotEncryptedForGenuinelyUnsealedStore is the
// other half: a store that was created with encryption.mode=off (no
// KEYREGISTRY ever sealed under a key) and is then started with mode=on must
// still get today's "not encrypted" refusal — the mislabel is fixed by
// splitting the two cases, not by softening this one.
func TestNodeStoreDataKeyKeepsNotEncryptedForGenuinelyUnsealedStore(t *testing.T) {
	dataDir := t.TempDir()
	offCfg := encryptionTestConfig(dataDir, "off", "")

	kv, _, err := openNodeStore(offCfg, nil)
	if err != nil {
		t.Fatalf("creating the unsealed store: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the unsealed store: %v", err)
	}

	onCfg := encryptionTestConfig(dataDir, "on", "keyfile")
	_, err = nodeStoreDataKey(onCfg, encryptionTestNodeMaster())
	if err == nil {
		t.Fatalf("nodeStoreDataKey on a genuinely unsealed store started with mode on: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "not encrypted") {
		t.Fatalf("nodeStoreDataKey's refusal for a genuinely unsealed store changed away from \"not encrypted\": %v", err)
	}
}
