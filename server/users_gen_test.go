package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// usersGenTestNodeMaster is a node master whose kid is its real one, so a
// sealed users.json opens under it.
func usersGenTestNodeMaster() *NodeMaster {
	nodeMasterBytes := make([]byte, 32)
	for i := range nodeMasterBytes {
		nodeMasterBytes[i] = byte(0x30 + i)
	}
	return &NodeMaster{bytes: nodeMasterBytes, kid: sealed.Kid(nodeMasterBytes)}
}

// usersGenOnDisk is the envelope generation in a sealed users.json's header.
func usersGenOnDisk(t *testing.T, path string) uint64 {
	t.Helper()
	envelope, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if len(envelope) < sealed.EnvelopeHeaderLen || envelope[4] != sealed.Version || envelope[5] != sealed.PurposeUsers {
		t.Fatalf("%s is not a sealed users envelope", path)
	}
	return binary.BigEndian.Uint64(envelope[14:22])
}

// TestUsersGenIncrementsAcrossSaves: every save derives a fresh key by writing
// the generation of the file it replaces plus one (spec §2.3), so no two saves
// seal under the same derived key. Each file still opens under the node master.
func TestUsersGenIncrementsAcrossSaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), UsersFileName)
	cfg := validUsersConfig()
	nodeMaster := usersGenTestNodeMaster()
	for wantGen := uint64(1); wantGen <= 3; wantGen++ {
		cfg.Version = 10 + wantGen
		if err := cfg.Save(path, usersSealing{NodeMaster: nodeMaster, Mode: "on"}); err != nil {
			t.Fatalf("save %d: %v", wantGen, err)
		}
		if gen := usersGenOnDisk(t, path); gen != wantGen {
			t.Fatalf("save %d wrote gen %d, want %d: saves are reusing one derived key", wantGen, gen, wantGen)
		}
		loaded, err := LoadUsersConfig(path, nil, nodeMaster)
		if err != nil {
			t.Fatalf("the gen-%d file does not open under the node master: %v", wantGen, err)
		}
		if loaded.Version != cfg.Version {
			t.Fatalf("the gen-%d file holds version %d, want %d", wantGen, loaded.Version, cfg.Version)
		}
	}
}

// TestUsersGenStartsAtOneOverAnUnsealedFile: a predecessor with no generation
// (none at all, plaintext JSON, or the legacy {0x00, 0x01} envelope) is
// replaced at gen 1.
func TestUsersGenStartsAtOneOverAnUnsealedFile(t *testing.T) {
	cfg := validUsersConfig()
	plaintext, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	legacyKey := bytes.Repeat([]byte{0x5A}, 32)
	legacy, err := encryptConfig(plaintext, legacyKey, "legacy01")
	if err != nil {
		t.Fatalf("encryptConfig: %v", err)
	}
	predecessors := []struct {
		name     string
		contents []byte
	}{
		{"no file", nil},
		{"plaintext", plaintext},
		{"legacy sealed", legacy},
	}
	for _, predecessor := range predecessors {
		t.Run(predecessor.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), UsersFileName)
			if predecessor.contents != nil {
				if err := os.WriteFile(path, predecessor.contents, 0o600); err != nil {
					t.Fatalf("writing the predecessor: %v", err)
				}
			}
			if err := cfg.Save(path, usersSealing{NodeMaster: usersGenTestNodeMaster(), Mode: "on"}); err != nil {
				t.Fatalf("Save: %v", err)
			}
			if gen := usersGenOnDisk(t, path); gen != 1 {
				t.Fatalf("the first sealed save over %s wrote gen %d, want 1", predecessor.name, gen)
			}
		})
	}
}

// TestUsersGenAtMaximumRefuses: a header already at the largest generation
// can only have been edited. The save refuses, naming the file, rather than
// wrapping to 0, and leaves the file as it was.
func TestUsersGenAtMaximumRefuses(t *testing.T) {
	path := filepath.Join(t.TempDir(), UsersFileName)
	cfg := validUsersConfig()
	nodeMaster := usersGenTestNodeMaster()
	if err := cfg.Save(path, usersSealing{NodeMaster: nodeMaster, Mode: "on"}); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	envelope, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	binary.BigEndian.PutUint64(envelope[14:22], ^uint64(0))
	if err := os.WriteFile(path, envelope, 0o600); err != nil {
		t.Fatalf("writing the edited header: %v", err)
	}

	cfg.Version++
	err = cfg.Save(path, usersSealing{NodeMaster: nodeMaster, Mode: "on"})
	if err == nil {
		t.Fatal("a save over a header at the largest generation succeeded; the next generation wraps to 0")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("the refusal does not name %s: %v", path, err)
	}
	onDisk, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("reading %s after the refusal: %v", path, readErr)
	}
	if !bytes.Equal(onDisk, envelope) {
		t.Error("the refused save changed the file on disk")
	}
}
