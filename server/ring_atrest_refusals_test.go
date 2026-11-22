package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// ringKEKFile writes kek to a key file outside any data directory and returns its path.
func ringKEKFile(t *testing.T, kek []byte) string {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyPath, kek, 0o600); err != nil {
		t.Fatalf("writing the test key file: %v", err)
	}
	return keyPath
}

// ringKEKEdit configures a node to use a key file holding kek, in mode.
func ringKEKEdit(t *testing.T, kek []byte, mode string) func(*config.Config) {
	t.Helper()
	keyPath := ringKEKFile(t, kek)
	return func(cfg *config.Config) {
		cfg.Encryption.Keystore = "keyfile"
		cfg.Encryption.KeyFile = keyPath
		cfg.Encryption.Mode = mode
	}
}

// ringStartAt runs the full start path, NewService then Start, over dataDir and returns the
// running service or the error either step gave. A failed start is closed again.
func ringStartAt(t *testing.T, dataDir string, edit func(*config.Config)) (*Service, error) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	if edit != nil {
		edit(cfg)
	}
	qs, err := NewService(cfg, discardLogger())
	if err != nil {
		return nil, err
	}
	if err := qs.Start(); err != nil {
		if stopErr := qs.Stop(); stopErr != nil {
			t.Logf("stopping the failed start: %v", stopErr)
		}
		return nil, err
	}
	return qs, nil
}

// ringRunOnce starts a node over dataDir, lets it create its ring, and stops it.
func ringRunOnce(t *testing.T, dataDir string, edit func(*config.Config)) {
	t.Helper()
	qs, err := ringStartAt(t, dataDir, edit)
	if err != nil {
		t.Fatalf("the first start: %v", err)
	}
	if err := qs.Stop(); err != nil {
		t.Fatalf("stopping the first start: %v", err)
	}
}

// ringTreeSums is every file under dir by its path and content hash, and every directory by
// its path: the ring directory "unchanged" means equal sums.
func ringTreeSums(t *testing.T, dir string) map[string]string {
	t.Helper()
	sums := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			sums[rel+"/"] = "dir"
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(content)
		sums[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatalf("summing %s: %v", dir, err)
	}
	return sums
}

// requireRingUnchanged fails when dir's sums differ from before.
func requireRingUnchanged(t *testing.T, dir string, before map[string]string) {
	t.Helper()
	after := ringTreeSums(t, dir)
	for path, sum := range before {
		if after[path] != sum {
			t.Errorf("the refused start changed %s in the ring directory", path)
		}
	}
	for path := range after {
		if _, ok := before[path]; !ok {
			t.Errorf("the refused start created %s in the ring directory", path)
		}
	}
}

// requireRefusalSays fails unless err is a refusal carrying every piece of want.
func requireRefusalSays(t *testing.T, err error, want ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("the start was not refused")
	}
	for _, piece := range want {
		if !strings.Contains(err.Error(), piece) {
			t.Errorf("the refusal does not say %q:\n%v", piece, err)
		}
	}
	requireNoOtherBuild(t, err.Error())
}

// removeRingSegments deletes the ring log's segments, so a fixture's only evidence is the store.
func removeRingSegments(t *testing.T, dataDir string) {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(dataDir, ringDirName, "wal", walSegmentGlob))
	if err != nil {
		t.Fatal(err)
	}
	for _, segment := range segments {
		if err := os.Remove(segment); err != nil {
			t.Fatal(err)
		}
	}
}

var (
	ringFirstKEK  = bytes.Repeat([]byte{0x11}, 32)
	ringSecondKEK = bytes.Repeat([]byte{0x22}, 32)
)

// TestARingNodeStartedWithTheWrongKeyIsRefusedNamingBothKids: R-1 [GUARD a node started with a
// key that does not open its node master names both key ids and the setting; red when R-1's
// branch keeps the old text]. Ring encryption at rest spec §5.4, §9.1 step 3.
func TestARingNodeStartedWithTheWrongKeyIsRefusedNamingBothKids(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	ringDir := filepath.Join(dataDir, ringDirName)
	before := ringTreeSums(t, ringDir)
	_, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringSecondKEK, "on"))
	requireRefusalSays(t, err,
		"opening the node master: the configured key (kid "+sealed.Kid(ringSecondKEK)+", keystore keyfile) does not open "+filepath.Join(dataDir, "keys", NodeMasterFileName),
		"which was sealed under a key with kid "+sealed.Kid(ringFirstKEK)+".",
		"Point the keystore at the key this node was first started with (encryption.key_file, HOARDDB_ENCRYPTION_KEY_FILE, for the keyfile keystore; encryption.key_command or the systemd credential hoarddb-dek otherwise), or restore that key from your escrow copy.",
		"A new key cannot open data sealed under the old one, so nothing in the ring directory was created or changed")
	requireRingUnchanged(t, ringDir, before)
}

// TestASealedRingStartedWithEncryptionOffGetsTheRingText: R-3 [GUARD a sealed ring started with
// encryption off is refused with the ring's text; red when loadRingAtRest runs after
// refuseSealedStoreWithEncryptionOff, whose legacy text then fires]. Spec §5.4, §5.5, §9.1 step 3.
func TestASealedRingStartedWithEncryptionOffGetsTheRingText(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	ringDir := filepath.Join(dataDir, ringDirName)
	before := ringTreeSums(t, ringDir)
	_, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "off"))
	requireRefusalSays(t, err,
		"the ring store at "+ringDir+" is encrypted (found: ring/keys/store.wrapped",
		`but encryption.mode is "off". A node's encryption is fixed when its ring store is created and is not switched in place.`,
		"Start this node with encryption.mode on (HOARDDB_ENCRYPTION_MODE=on) and the keystore that sealed it.",
		"To run this node in plaintext instead, stop it and move "+ringDir+" aside (keep it: it is the only copy of this node's data), and also the node store this build keeps beside it, "+
			filepath.Join(dataDir, nodeStoreDirName)+" and "+filepath.Join(dataDir, "keys", nodeStoreKeyFileName)+", keep encryption.keystore set, and start it with encryption.mode off.",
		"A cluster member comes back empty and is not reseeded while its cluster holds a database, and every node of a cluster must use the same setting")
	requireRingUnchanged(t, ringDir, before)
}

// r4Text is R-4's text after the evidence, for a node started in mode.
func r4Text(dataDir, mode string) []string {
	ringDir := filepath.Join(dataDir, ringDirName)
	return []string{
		`but encryption.mode is "` + mode + `". A node's encryption is fixed when its ring store is created and is not switched in place.`,
		"Start this node with encryption.mode off (HOARDDB_ENCRYPTION_MODE=off).",
		"To run this node encrypted instead, stop it and move " + ringDir + " aside (keep it: it is the only copy of this node's data), and also the node store this build keeps beside it, " +
			filepath.Join(dataDir, nodeStoreDirName) + ", then start it with encryption.mode on and a keystore.",
		"A cluster member comes back empty and is not reseeded while its cluster holds a database, and every node of a cluster must use the same setting",
	}
}

// TestAPlaintextRingStartedWithEncryptionOnIsRefused: R-4, store half [GUARD a plaintext ring
// store started with encryption on is refused with the ring's text; red when R-4's store half is
// deleted: Badger then fails with its raw error]. The fixture's log segments are removed so the
// store is the only evidence. Spec §5.4, §9.1 step 3.
func TestAPlaintextRingStartedWithEncryptionOnIsRefused(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, nil)
	removeRingSegments(t, dataDir)
	ringDir := filepath.Join(dataDir, ringDirName)
	before := ringTreeSums(t, ringDir)
	_, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	requireRefusalSays(t, err, append([]string{"the ring store at " + ringDir + " is not encrypted (found: ring/badger holds a store created without a key)"}, r4Text(dataDir, "on")...)...)
	requireRingUnchanged(t, ringDir, before)
}

// TestAPlaintextRingSegmentStartedWithEncryptionOnIsRefused: R-4, log half [GUARD a plaintext
// segment in the ring log of a sealed node is refused even with wal.wrapped present; red when
// R-4's log half is deleted: the keyed open then starts a sealed segment beside it, a mixed log].
// Spec §5.4, §9.1 step 3.
func TestAPlaintextRingSegmentStartedWithEncryptionOnIsRefused(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	removeRingSegments(t, dataDir)
	plainLog, err := storage.OpenWALWithOptions(filepath.Join(dataDir, ringDirName, "wal"), nil, storage.WALOptions{})
	if err != nil {
		t.Fatalf("writing the plaintext segment: %v", err)
	}
	if err := plainLog.Close(); err != nil {
		t.Fatal(err)
	}
	ringDir := filepath.Join(dataDir, ringDirName)
	if _, err := os.Stat(filepath.Join(ringDir, ringKeysDirName, ringWALKeyFileName)); err != nil {
		t.Fatalf("the fixture has no ring/keys/wal.wrapped: %v", err)
	}
	before := ringTreeSums(t, ringDir)
	_, err = ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	requireRefusalSays(t, err, append([]string{"the ring store at " + ringDir + " is not encrypted (found: ring/wal/wal_00001.log is not sealed)"}, r4Text(dataDir, "on")...)...)
	requireRingUnchanged(t, ringDir, before)
}

// TestAKeyFileBesidePlaintextStoreIsRefusedWithTheRingText: R-9 [GUARD a ring key file that
// unwraps, beside a store created without a key, is refused with R-4's text naming the key file;
// red when R-9 is deleted: Badger then refuses with its raw key-mismatch error]. Spec §5.4, §9.1
// step 3.
func TestAKeyFileBesidePlaintextStoreIsRefusedWithTheRingText(t *testing.T) {
	dataDir := t.TempDir()
	// Mode off with a keystore: the node has a node master, and its ring is plaintext.
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "off"))
	removeRingSegments(t, dataDir)
	keysCfg := config.DefaultConfig()
	keysCfg.Storage.DataDir = dataDir
	ringKEKEdit(t, ringFirstKEK, "on")(keysCfg)
	keys, err := OpenNodeKeys(keysCfg)
	if err != nil {
		t.Fatalf("opening the node master: %v", err)
	}
	wrapped, err := sealed.Seal(sealed.PurposeStoreKey, keys.nodeMaster.Bytes(), ringStoreKeyIdentity, 1, bytes.Repeat([]byte{0x5C}, 32))
	if err != nil {
		t.Fatal(err)
	}
	ringDir := filepath.Join(dataDir, ringDirName)
	if err := os.MkdirAll(filepath.Join(ringDir, ringKeysDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ringDir, ringKeysDirName, ringStoreKeyFileName), wrapped, 0o600); err != nil {
		t.Fatal(err)
	}
	before := ringTreeSums(t, ringDir)
	_, err = ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	requireRefusalSays(t, err, append([]string{"the ring store at " + ringDir + " is not encrypted (found: a ring key file beside a store that was created without it)"}, r4Text(dataDir, "on")...)...)
	requireRingUnchanged(t, ringDir, before)
}

// TestLostRingKeysAreReportedInOneText: R-5 [GUARD a sealed ring whose ring/keys/ is gone is
// refused naming both missing files in one text; red when R-5 checks one file]. Spec §5.4, §9.1
// step 3.
func TestLostRingKeysAreReportedInOneText(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	ringDir := filepath.Join(dataDir, ringDirName)
	if err := os.RemoveAll(filepath.Join(ringDir, ringKeysDirName)); err != nil {
		t.Fatal(err)
	}
	before := ringTreeSums(t, ringDir)
	_, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	keysDir := filepath.Join(ringDir, ringKeysDirName)
	requireRefusalSays(t, err,
		"the ring data at "+ringDir+" is sealed, but "+filepath.Join(keysDir, ringStoreKeyFileName)+" and "+filepath.Join(keysDir, ringWALKeyFileName)+" are missing.",
		"encryption.mode off cannot open sealed data, and a new key cannot either, so none was created and nothing was written.",
		"Restore ring/keys/ from a backup of this data directory: these keys exist nowhere else, and the node-master escrow does not cover them.",
		"Without a backup this node's ring data cannot be read: stop it, move "+ringDir+" aside (keep it) and start it with an empty ring; a cluster member is not reseeded while its cluster holds a database")
	requireRingUnchanged(t, ringDir, before)
}

// TestADamagedRingKeyFileIsRefusedBeforeAnythingIsWritten: R-8 [GUARD a ring key file the node
// master does not open is refused in the load, with the ring directory unchanged; red when the
// unwrap moves into Node.open: incarnation changes]. Spec §5.4, §9.1 step 3.
func TestADamagedRingKeyFileIsRefusedBeforeAnythingIsWritten(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	ringDir := filepath.Join(dataDir, ringDirName)
	keyPath := filepath.Join(ringDir, ringKeysDirName, ringStoreKeyFileName)
	wrapped, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	wrapped[len(wrapped)-1] ^= 0xFF
	if err := os.WriteFile(keyPath, wrapped, 0o600); err != nil {
		t.Fatal(err)
	}
	before := ringTreeSums(t, ringDir)
	_, err = ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	requireRefusalSays(t, err,
		"unwrapping the ring key "+keyPath+" under node master kid ",
		"The node master opens but the ring key does not, so the file is damaged or belongs to another data directory.",
		"Restore ring/keys/ from a backup of this data directory; nothing in the ring directory was created or changed")
	requireRingUnchanged(t, ringDir, before)
}

// moveAside renames each of paths (relative to dataDir) into a directory outside it.
func moveAside(t *testing.T, dataDir string, paths ...string) {
	t.Helper()
	aside := t.TempDir()
	for _, rel := range paths {
		target := filepath.Join(aside, filepath.Base(rel))
		if err := os.Rename(filepath.Join(dataDir, rel), target); err != nil {
			t.Fatalf("moving %s aside: %v", rel, err)
		}
	}
}

// TestTheModeSwitchWithTheEarlierStoreMovedAsideStarts: [GUARD the interim mode switch the texts
// give works: with ring/, store/ and keys/store.wrapped moved aside and the keystore kept, a
// sealed node starts with encryption off; red when a check still sees the moved files]. Spec
// §5.5, §9.1 step 3.
func TestTheModeSwitchWithTheEarlierStoreMovedAsideStarts(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	moveAside(t, dataDir, ringDirName, nodeStoreDirName, filepath.Join("keys", nodeStoreKeyFileName))
	qs, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "off"))
	if err != nil {
		t.Fatalf("the switch to encryption off with the stores moved aside was refused: %v", err)
	}
	if err := qs.Stop(); err != nil {
		t.Fatal(err)
	}
}

// TestTheEarlierStoresMismatchTextGivesTheRingAdvice: [GUARD with only ring/ moved aside the
// earlier store's refusal fires, and its advice is the ring-mode move, not a dump; red when
// storeModeMismatch keeps the dump advice]. Spec §5.5, §9.1 step 3.
func TestTheEarlierStoresMismatchTextGivesTheRingAdvice(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	moveAside(t, dataDir, ringDirName)
	_, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "off"))
	requireRefusalSays(t, err,
		"the node store at "+filepath.Join(dataDir, nodeStoreDirName)+" is encrypted",
		"move "+filepath.Join(dataDir, nodeStoreDirName)+" and "+filepath.Join(dataDir, "keys", nodeStoreKeyFileName)+" aside together with "+filepath.Join(dataDir, ringDirName)+", then start again")
	if err != nil && strings.Contains(err.Error(), "hoarddb dump") {
		t.Errorf("the refusal still gives the dump advice, which does not fit ring mode:\n%v", err)
	}
}

// TestAnUnreadableRingKeyRegistryIsRefusedAsUnreadable: review F-2 [GUARD a sealed ring whose
// ring/badger/KEYREGISTRY cannot be read is refused naming the file, the error and the fix, not
// with R-4's false "not encrypted, start with encryption.mode off"; red when the evidence read
// treats a read error as "not sealed" (storeIsSealed)]. Skipped as root, which reads a mode-000
// file anyway.
func TestAnUnreadableRingKeyRegistryIsRefusedAsUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: root reads a mode-000 file, so the file cannot be made unreadable")
	}
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	ringDir := filepath.Join(dataDir, ringDirName)
	registry := filepath.Join(ringDir, "badger", "KEYREGISTRY")
	before := ringTreeSums(t, ringDir)
	if err := os.Chmod(registry, 0); err != nil {
		t.Fatal(err)
	}
	_, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	if chmodErr := os.Chmod(registry, 0o600); chmodErr != nil {
		t.Fatal(chmodErr)
	}
	requireRefusalSays(t, err,
		"cannot read "+registry+" to tell whether the ring store at "+ringDir+" is encrypted:",
		"permission denied",
		"Nothing was opened or written. Make "+ringDir+" and everything in it readable and writable by the user this server runs as",
		"then start again")
	if err != nil && strings.Contains(err.Error(), "is not encrypted") {
		t.Errorf("an unreadable key registry is reported as not encrypted:\n%v", err)
	}
	requireRingUnchanged(t, ringDir, before)
}

// asClusterMember makes a start a configured cluster member (cluster.enabled) on top of edit.
func asClusterMember(edit func(*config.Config)) func(*config.Config) {
	return func(cfg *config.Config) {
		edit(cfg)
		cfg.Cluster.Enabled = true
		cfg.Server.NodeID = "node-a"
	}
}

// requireMemberAdvice fails unless err gives a cluster member's advice, wire.ClusterMemberRingAdvice
// spelled out: do not move ring/ aside while the cluster runs, stop the node, keep ring/, start it
// on an empty ring directory with the same id, keys and address and run replace-node; and never the
// single-node advice to move ring/ aside, a restore from a backup, or a bug number.
func requireMemberAdvice(t *testing.T, err error, dataDir string) {
	t.Helper()
	ringDir := filepath.Join(dataDir, ringDirName)
	requireRefusalSays(t, err,
		"This node is configured as a member of a cluster (cluster.enabled), so do not move "+ringDir+" aside while the cluster runs: that can lose committed cluster state.",
		"Stop this node, keep "+ringDir+", start it again on an empty ring directory with the same node id, keys and address, and run 'replace-node <node> --with <node>@<addr>' on any member")
	if err == nil {
		return
	}
	// R-5's own sentence about the key files (they never change after creation, so a copy of
	// them is not a rolled-back voter) is not member advice; it is left out of the ban.
	text := strings.Replace(err.Error(), "Restore ring/keys/ from a backup of this data directory: these keys exist nowhere else", "", 1)
	// A restored voter is a rolled-back voter (owner ruling 2026-11-19), so the member advice
	// never recommends restoring the node from a backup.
	for _, banned := range []string{ringDir + " aside (keep it", "together with " + ringDir, "start it with an empty ring", "BUG-",
		"Restore this node from its own data", "a backup of this data directory"} {
		if strings.Contains(text, banned) {
			t.Errorf("a cluster member's refusal says %q, the single-node advice that can lose committed cluster state:\n%v", banned, err)
		}
	}
}

// TestAClusterMemberIsNotToldToMoveItsRingAside: [GUARD for a node configured as a cluster member
// (cluster.enabled) the refusals that tell a single node to move ring/ aside (R-3, R-4, R-5,
// require's structural check, and the earlier store's mismatch) say not to on a live cluster, and
// give the member's recovery, replace-node, because a member restarted with an empty ring and no
// replace rejoins as a voter with no Raft state and can lose committed cluster state; red when
// the texts ignore cluster membership]. The single-node texts are guarded by the tests above.
func TestAClusterMemberIsNotToldToMoveItsRingAside(t *testing.T) {
	t.Run("R-3 sealed ring, mode off", func(t *testing.T) {
		dataDir := t.TempDir()
		ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
		_, err := ringStartAt(t, dataDir, asClusterMember(ringKEKEdit(t, ringFirstKEK, "off")))
		requireRefusalSays(t, err, "is encrypted (found: ring/keys/store.wrapped", "Start this node with encryption.mode on (HOARDDB_ENCRYPTION_MODE=on) and the keystore that sealed it.", "Every node of a cluster must use the same setting")
		requireMemberAdvice(t, err, dataDir)
	})
	t.Run("R-4 plaintext store, mode on", func(t *testing.T) {
		dataDir := t.TempDir()
		ringRunOnce(t, dataDir, nil)
		removeRingSegments(t, dataDir)
		_, err := ringStartAt(t, dataDir, asClusterMember(ringKEKEdit(t, ringFirstKEK, "on")))
		requireRefusalSays(t, err, "is not encrypted (found: ring/badger holds a store created without a key)", "Start this node with encryption.mode off (HOARDDB_ENCRYPTION_MODE=off).", "Every node of a cluster must use the same setting")
		requireMemberAdvice(t, err, dataDir)
	})
	t.Run("R-5 lost ring keys", func(t *testing.T) {
		dataDir := t.TempDir()
		ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
		if err := os.RemoveAll(filepath.Join(dataDir, ringDirName, ringKeysDirName)); err != nil {
			t.Fatal(err)
		}
		_, err := ringStartAt(t, dataDir, asClusterMember(ringKEKEdit(t, ringFirstKEK, "on")))
		requireRefusalSays(t, err, "are missing.", "Restore ring/keys/ from a backup of this data directory: these keys exist nowhere else, and the node-master escrow does not cover them.")
		requireMemberAdvice(t, err, dataDir)
	})
	t.Run("require structural check", func(t *testing.T) {
		dataDir := t.TempDir()
		ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "require"))
		appendManifestTableWithKeyIDZero(t, filepath.Join(dataDir, ringDirName, "badger"))
		_, err := ringStartAt(t, dataDir, asClusterMember(ringKEKEdit(t, ringFirstKEK, "require")))
		requireRefusalSays(t, err, "is not sealed: table 999999 has KeyID == 0", "set encryption.mode to on (HOARDDB_ENCRYPTION_MODE=on)")
		requireMemberAdvice(t, err, dataDir)
	})
	t.Run("the earlier store's mismatch", func(t *testing.T) {
		dataDir := t.TempDir()
		ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
		moveAside(t, dataDir, ringDirName)
		_, err := ringStartAt(t, dataDir, asClusterMember(ringKEKEdit(t, ringFirstKEK, "off")))
		// The ring holds no evidence of its mode here, so the mismatch is the earlier engine's store
		// alone: a member is told to move that store and its key aside and to leave ring/ in place,
		// not the ring recovery, which would not clear this refusal (integration review F-2 (b)).
		storeDir, ringDir := filepath.Join(dataDir, nodeStoreDirName), filepath.Join(dataDir, ringDirName)
		requireRefusalSays(t, err, "the node store at "+storeDir+" is encrypted", "Start this node with encryption.mode on",
			"This node is configured as a member of a cluster (cluster.enabled): leave "+ringDir+" in place.",
			"To keep encryption.mode \"off\", stop this node and move "+storeDir+" and "+filepath.Join(dataDir, "keys", nodeStoreKeyFileName)+
				" aside (keep them: the ring does not read them), then start it again")
		if err != nil && (strings.Contains(err.Error(), "start it again on an empty ring directory") || strings.Contains(err.Error(), "aside together with "+ringDir)) {
			t.Errorf("a member's earlier-store mismatch gives advice about ring/, which does not clear it:\n%v", err)
		}
	})
	t.Run("an unreadable ring key registry", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("running as root: root reads a mode-000 file, so the file cannot be made unreadable")
		}
		dataDir := t.TempDir()
		ringRunOnce(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
		registry := filepath.Join(dataDir, ringDirName, "badger", "KEYREGISTRY")
		if err := os.Chmod(registry, 0); err != nil {
			t.Fatal(err)
		}
		_, err := ringStartAt(t, dataDir, asClusterMember(ringKEKEdit(t, ringFirstKEK, "on")))
		if chmodErr := os.Chmod(registry, 0o600); chmodErr != nil {
			t.Fatal(chmodErr)
		}
		requireRefusalSays(t, err, "cannot read "+registry+" to tell whether the ring store", "permission denied")
		requireMemberAdvice(t, err, dataDir)
	})
}

// TestAPlaintextRingStoreWithoutItsManifestIsRefusedBeforeAKeyIsCreated: R-4 [GUARD a plaintext
// ring store whose MANIFEST is gone but whose table files remain counts as a plaintext store, so a
// start with encryption on is refused in the load, before Node.open asks for a store key; red when
// readRingEvidence reads only MANIFEST: the load then sees no store, ringAtRest.StoreKey writes
// ring/keys/store.wrapped, and only then does the store's guard refuse the missing MANIFEST, leaving
// a key file beside a plaintext store] (integration review F-1). Spec §5.3, §5.4.
func TestAPlaintextRingStoreWithoutItsManifestIsRefusedBeforeAKeyIsCreated(t *testing.T) {
	dataDir := t.TempDir()
	ringRunOnce(t, dataDir, nil)
	removeRingSegments(t, dataDir)
	ringDir := filepath.Join(dataDir, ringDirName)
	storeDir := filepath.Join(ringDir, ringStoreDirName)
	// A start this small leaves its writes in the memtable log; a table file stands for one a flush
	// wrote. The store's guard refuses the missing MANIFEST before it reads the table.
	if err := os.WriteFile(filepath.Join(storeDir, "000001.sst"), bytes.Repeat([]byte{0x5C}, 64), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(storeDir, "MANIFEST")); err != nil {
		t.Fatal(err)
	}
	// The earlier engine's plaintext store would answer first (storeModeMismatch); moved aside, the
	// ring's own evidence is all that stands between the start and Node.open.
	if err := os.Rename(filepath.Join(dataDir, nodeStoreDirName), filepath.Join(t.TempDir(), nodeStoreDirName)); err != nil {
		t.Fatal(err)
	}
	before := ringTreeSums(t, ringDir)
	_, err := ringStartAt(t, dataDir, ringKEKEdit(t, ringFirstKEK, "on"))
	requireRefusalSays(t, err, append([]string{"the ring store at " + ringDir + " is not encrypted (found: ring/badger holds a store created without a key)"}, r4Text(dataDir, "on")...)...)
	requireRingUnchanged(t, ringDir, before)
	if _, statErr := os.Stat(filepath.Join(ringDir, ringKeysDirName, ringStoreKeyFileName)); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("the refused start left ring/keys/%s beside a plaintext store (stat: %v)", ringStoreKeyFileName, statErr)
	}
}
