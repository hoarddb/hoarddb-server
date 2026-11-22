package server

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/transport"
)

// testRingNodeMaster is a node master the ring key tests seal under.
func testRingNodeMaster(fill byte) *NodeMaster {
	master := bytes.Repeat([]byte{fill}, 32)
	return &NodeMaster{bytes: master, kid: sealed.Kid(master)}
}

// recordRingKeyCreation replaces the link and sync seams with recorders that still do the work,
// and returns the recorded events in order.
func recordRingKeyCreation(t *testing.T) *[]string {
	t.Helper()
	var events []string
	link, syncDir := ringKeyLink, ringKeySyncDir
	ringKeyLink = func(oldname, newname string) error {
		events = append(events, "link "+newname)
		return link(oldname, newname)
	}
	ringKeySyncDir = func(dir string) error {
		events = append(events, "sync "+dir)
		return syncDir(dir)
	}
	t.Cleanup(func() { ringKeyLink, ringKeySyncDir = link, syncDir })
	return &events
}

// TestTheRingStoreKeyIsLinkedThenItsDirectorySynced: [GUARD creating ring/keys/store.wrapped
// syncs ring/ after making keys/, links the key into place and then syncs ring/keys/; red when
// either sync is deleted]. Ring encryption at rest spec §5.3, §9.1 step 2.
func TestTheRingStoreKeyIsLinkedThenItsDirectorySynced(t *testing.T) {
	ringDir := filepath.Join(t.TempDir(), ringDirName)
	if err := os.MkdirAll(ringDir, 0o700); err != nil {
		t.Fatal(err)
	}
	events := recordRingKeyCreation(t)
	keys := &ringAtRest{nodeMaster: testRingNodeMaster(0x31), ringDir: ringDir}
	if _, err := keys.StoreKey(filepath.Join(ringDir, "badger")); err != nil {
		t.Fatalf("StoreKey: %v", err)
	}
	keysDir := filepath.Join(ringDir, ringKeysDirName)
	want := []string{"sync " + ringDir, "link " + filepath.Join(keysDir, ringStoreKeyFileName), "sync " + keysDir}
	if len(*events) != len(want) {
		t.Fatalf("creating the ring store key did %q, want %q", *events, want)
	}
	for i := range want {
		if (*events)[i] != want[i] {
			t.Fatalf("creating the ring store key did %q, want %q", *events, want)
		}
	}
	entries, err := os.ReadDir(keysDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != ringStoreKeyFileName {
		t.Fatalf("%s holds %v after the create, want only %s (the temporary name removed)", keysDir, entries, ringStoreKeyFileName)
	}
}

// TestASecondCreatorOfTheRingStoreKeyUsesTheFirstKey: [GUARD a key file another process created
// between the load and the create is used, never replaced; red when the link is replaced by a
// rename, which replaces the first process's key while that process writes under it]. Spec §5.3
// (second review R2-6), §9.1 step 2.
func TestASecondCreatorOfTheRingStoreKeyUsesTheFirstKey(t *testing.T) {
	ringDir := filepath.Join(t.TempDir(), ringDirName)
	if err := os.MkdirAll(ringDir, 0o700); err != nil {
		t.Fatal(err)
	}
	master := testRingNodeMaster(0x32)
	firstKey := bytes.Repeat([]byte{0xF1}, 32)
	firstWrapped, err := sealed.Seal(sealed.PurposeStoreKey, master.Bytes(), ringStoreKeyIdentity, 1, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	beforeRingKeyLink = func(finalPath string) {
		if err := os.WriteFile(finalPath, firstWrapped, 0o600); err != nil {
			t.Errorf("playing the first creator: %v", err)
		}
	}
	t.Cleanup(func() { beforeRingKeyLink = nil })
	keys := &ringAtRest{nodeMaster: master, ringDir: ringDir}
	got, err := keys.StoreKey(filepath.Join(ringDir, "badger"))
	if err != nil {
		t.Fatalf("StoreKey: %v", err)
	}
	if !bytes.Equal(got, firstKey) {
		t.Fatal("the second creator returned its own key, not the key the first creator linked")
	}
	onDisk, err := os.ReadFile(filepath.Join(ringDir, ringKeysDirName, ringStoreKeyFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, firstWrapped) {
		t.Fatal("the second creator replaced the first creator's key file")
	}
}

// TestARefusedRingFormatCreatesNoKeyFile: [GUARD a ring directory the format check refuses gets
// no key file; red when key creation moves into the load, which runs before the format check].
// Spec §5.3, §9.1 step 2.
func TestARefusedRingFormatCreatesNoKeyFile(t *testing.T) {
	dataDir := t.TempDir()
	ringDir := filepath.Join(dataDir, ringDirName)
	if err := os.MkdirAll(ringDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ringDir, "FORMAT"), []byte("hoarddb ring format 99\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	seal := sealEdit(t, "on")
	qs := newCapacityService(t, func(cfg *config.Config) {
		seal(cfg)
		cfg.Storage.DataDir = dataDir
		cfg.TCP.Listen = "127.0.0.1:0"
		cfg.Server.Listen = "127.0.0.1:0"
	}, nil)
	stopService(t, qs)
	if err := qs.Start(); err == nil {
		t.Fatal("a ring directory at format 99 started")
	}
	if _, err := os.Stat(filepath.Join(ringDir, ringKeysDirName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused ring directory holds %s (stat: %v)", filepath.Join(ringDir, ringKeysDirName), err)
	}
}

// startSealedRingService starts a single node with encryption on over dataDir.
func startSealedRingService(t *testing.T, dataDir string, seal func(*config.Config)) *Service {
	t.Helper()
	qs := newCapacityService(t, func(cfg *config.Config) {
		seal(cfg)
		cfg.Storage.DataDir = dataDir
		cfg.TCP.Listen = "127.0.0.1:0"
		cfg.Server.Listen = "127.0.0.1:0"
	}, nil)
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return qs
}

// TestTheRingStoreKeyIsSealedUnderARingIdentity: [GUARD ring/keys/store.wrapped is sealed under
// the identity ring-store, so it never opens as the legacy keys/store.wrapped; red when the
// identity is store]. Spec §5.1, §9.1 step 2.
func TestTheRingStoreKeyIsSealedUnderARingIdentity(t *testing.T) {
	dataDir := t.TempDir()
	qs := startSealedRingService(t, dataDir, sealEdit(t, "on"))
	master := qs.core.nodeMaster.Bytes()
	if err := qs.Stop(); err != nil {
		t.Fatal(err)
	}
	wrapped, err := os.ReadFile(filepath.Join(dataDir, ringDirName, ringKeysDirName, ringStoreKeyFileName))
	if err != nil {
		t.Fatalf("the sealed ring node wrote no store key: %v", err)
	}
	if _, err := openWrappedStoreKey(master, wrapped, ringStoreKeyIdentity); err != nil {
		t.Fatalf("the ring store key does not open under %q: %v", ringStoreKeyIdentity, err)
	}
	if _, err := openWrappedStoreKey(master, wrapped, nodeStoreKeyIdentity); err == nil {
		t.Fatalf("the ring store key opens under the legacy identity %q", nodeStoreKeyIdentity)
	}
	registry, err := os.ReadFile(filepath.Join(dataDir, ringDirName, ringKeysDirName, ringWALKeyFileName))
	if err != nil {
		t.Fatalf("the sealed ring node wrote no log key registry: %v", err)
	}
	if _, _, err := openWALEpochRegistry(master, registry, ringWALKeyIdentity); err != nil {
		t.Fatalf("the ring log registry does not open under %q: %v", ringWALKeyIdentity, err)
	}
	if _, _, err := openWALEpochRegistry(master, registry, legacyWALRegistryIdentity); err == nil {
		t.Fatalf("the ring log registry opens under the legacy identity %q", legacyWALRegistryIdentity)
	}
}

// TestAClusterNodeWithEncryptionOnStarts: [GUARD a cluster node with encryption on opens a sealed
// ring node; red while validateClusterMode refuses encryption with cluster mode]. Spec §9.1
// step 2.
func TestAClusterNodeWithEncryptionOnStarts(t *testing.T) {
	listen := ringClusterPorts(t)
	seal := sealEdit(t, "on")
	qs := ringClusterService(t, listen, nil, seal)
	ringClusterStart(t, qs)
	if qs.ringNode == nil {
		t.Fatal("the cluster node opened no ring node")
	}
	if _, err := os.Stat(filepath.Join(qs.config.Storage.DataDir, ringDirName, ringKeysDirName, ringStoreKeyFileName)); err != nil {
		t.Fatalf("the cluster node's ring store has no key file: %v", err)
	}
}

// TestASingleRingNodeWithEncryptionOnServesTheRingOpcodes: [GUARD a single node with encryption
// on serves the ring opcodes from a sealed ring node; red while ringSealedRefusal keeps the ring
// node closed and the legacy handler answers]. Spec §9.1 step 2.
func TestASingleRingNodeWithEncryptionOnServesTheRingOpcodes(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, sealEdit(t, "on"))
	if qs.ringNode == nil {
		t.Fatal("a node with encryption on opened no ring node")
	}
	client := dialTCPServer(t, addr)
	client.login()
	frame := ringAdminFrame(t, client, wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: ringComposeDB})
	if frame.Opcode != transport.OpRingReply {
		t.Fatalf("RING_ADMIN create database was answered %s %q, want RING_REPLY", frame.Opcode, frame.Payload)
	}
	if _, err := os.Stat(filepath.Join(qs.config.Storage.DataDir, ringDirName, ringKeysDirName, ringStoreKeyFileName)); err != nil {
		t.Fatalf("the ring store has no key file: %v", err)
	}
}

// plantLeftoverRingKeyTemp leaves in <dataDir>/ring/keys what a crash between the temporary
// write and the link leaves: a temporary file beside where the key would go.
func plantLeftoverRingKeyTemp(t *testing.T, dataDir string) string {
	t.Helper()
	keysDir := filepath.Join(dataDir, ringDirName, ringKeysDirName)
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatal(err)
	}
	leftover := filepath.Join(keysDir, ringStoreKeyFileName+ringKeyTempFilePrefix+"123456")
	if err := os.WriteFile(leftover, []byte("a wrapped key that was never linked"), 0o600); err != nil {
		t.Fatal(err)
	}
	return leftover
}

// TestALeftoverTemporaryKeyFileIsIgnored: review F-4 [GUARD a temporary key file a crash left in
// ring/keys is ignored, not swept: it is never read as a key, never counts as evidence that the
// ring is sealed, and is left as it was; red when the evidence counts any file named after a key
// (a leftover beside a plaintext ring then gets R-3)]. Ignored rather than swept because a sweep
// could delete the temporary file of a second process about to link it.
func TestALeftoverTemporaryKeyFileIsIgnored(t *testing.T) {
	plainDir := t.TempDir()
	leftover := plantLeftoverRingKeyTemp(t, plainDir)
	qs, err := ringStartAt(t, plainDir, ringKEKEdit(t, ringFirstKEK, "off"))
	if err != nil {
		t.Fatalf("a plaintext node with a leftover temporary key file was refused: %v", err)
	}
	if err := qs.Stop(); err != nil {
		t.Fatal(err)
	}

	sealedDir := t.TempDir()
	sealedLeftover := plantLeftoverRingKeyTemp(t, sealedDir)
	ringRunOnce(t, sealedDir, ringKEKEdit(t, ringFirstKEK, "on"))
	ringRunOnce(t, sealedDir, ringKEKEdit(t, ringFirstKEK, "on"))
	if _, err := os.Stat(filepath.Join(sealedDir, ringDirName, ringKeysDirName, ringStoreKeyFileName)); err != nil {
		t.Fatalf("the sealed node created no store key beside the leftover: %v", err)
	}
	for _, path := range []string{leftover, sealedLeftover} {
		content, err := os.ReadFile(path)
		if err != nil || string(content) != "a wrapped key that was never linked" {
			t.Fatalf("the leftover %s was changed or removed (%q, %v)", path, content, err)
		}
	}
}

// TestAFilesystemWithoutHardLinksNamesTheFix: review F-5 [GUARD when the filesystem refuses the
// link that creates a ring key, the refusal says hard links are needed and to put data_dir on a
// local filesystem; red when the link error is returned with no fix]. The link seam returns the
// errors such filesystems give.
func TestAFilesystemWithoutHardLinksNamesTheFix(t *testing.T) {
	for _, refusal := range []syscall.Errno{syscall.EPERM, syscall.EXDEV, syscall.ENOTSUP, syscall.ENOSYS} {
		t.Run(refusal.Error(), func(t *testing.T) {
			ringDir := filepath.Join(t.TempDir(), ringDirName)
			if err := os.MkdirAll(ringDir, 0o700); err != nil {
				t.Fatal(err)
			}
			link := ringKeyLink
			ringKeyLink = func(oldname, newname string) error {
				return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: refusal}
			}
			t.Cleanup(func() { ringKeyLink = link })
			keys := &ringAtRest{nodeMaster: testRingNodeMaster(0x33), ringDir: ringDir}
			_, err := keys.StoreKey(filepath.Join(ringDir, "badger"))
			requireRefusalSays(t, err,
				refusal.Error(),
				"the filesystem under "+ringDir+" refused the hard link that creating a ring key needs",
				"put data_dir on a local filesystem that supports hard links (ext4, xfs, btrfs, zfs) and start again; this build has no fallback")
			if entries, readErr := os.ReadDir(filepath.Join(ringDir, ringKeysDirName)); readErr != nil || len(entries) != 0 {
				t.Errorf("the refused create left %v in ring/keys (%v)", entries, readErr)
			}
		})
	}
}
