package server

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

func makeKeyFile(t *testing.T, keydir string, seed byte) string {
	keyfile := filepath.Join(keydir, "kek")
	rawKey := make([]byte, 32)
	for i := range rawKey {
		rawKey[i] = seed + byte(i)
	}
	if err := os.WriteFile(keyfile, rawKey, 0o600); err != nil {
		t.Fatalf("writing key file: %v", err)
	}
	return keyfile
}

func TestFirstStartGeneratesAndWrapsNodeMaster(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}

	nm, err := LoadOrCreateNodeMaster(datadir, ks)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	if !nm.Created() {
		t.Error("Created should be true")
	}
	if len(nm.Bytes()) != 32 {
		t.Errorf("length: got %d, want 32", len(nm.Bytes()))
	}

	wrappedPath := filepath.Join(datadir, "keys", "node-master.key.wrapped")
	info, _ := os.Stat(wrappedPath)
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("perm: got %04o, want 0600", perm)
	}
}

func TestNodeMasterNeverWrittenUnwrapped(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	nm, err := LoadOrCreateNodeMaster(datadir, ks)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	wrappedPath := filepath.Join(datadir, "keys", "node-master.key.wrapped")
	wrappedData, err := os.ReadFile(wrappedPath)
	if err != nil {
		t.Fatalf("reading the wrapped node master: %v", err)
	}
	// An empty or short file would make the absence check below pass without
	// looking at anything: the envelope must at least be longer than the key.
	if len(wrappedData) <= len(nm.Bytes()) {
		t.Fatalf("wrapped node master is %d bytes, no longer than the %d-byte key it wraps", len(wrappedData), len(nm.Bytes()))
	}

	// A random key has no compression to hide it; every 8-byte window is
	// checked because absence of the whole key is the weakest form of the claim.
	if window := anyMarkerWindow(wrappedData, nm.Bytes()); window >= 0 {
		t.Errorf("node master bytes %d..%d appear in the wrapped file: plaintext found", window, window+markerScanWindow)
	}
}

func TestSecondStartUnwrapsTheSameBytes(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, _ := OpenKeystore(cfg)
	nm1, _ := LoadOrCreateNodeMaster(datadir, ks)
	nm2, _ := LoadOrCreateNodeMaster(datadir, ks)

	if !bytes.Equal(nm1.Bytes(), nm2.Bytes()) {
		t.Error("bytes mismatch")
	}
	if nm2.Created() {
		t.Error("created should be false")
	}
}

func TestExportRefusesDestinationInsideDataDir(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, _ := OpenKeystore(cfg)
	if _, err := LoadOrCreateNodeMaster(datadir, ks); err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	err := ExportNodeMaster(cfg, filepath.Join(datadir, "nm.key"))
	if err == nil {
		t.Error("should refuse")
	}
}

func TestExportWritesZeroSixHundred(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, _ := OpenKeystore(cfg)
	if _, err := LoadOrCreateNodeMaster(datadir, ks); err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	exportPath := filepath.Join(exportdir, "nm.key")
	if err := ExportNodeMaster(cfg, exportPath); err != nil {
		t.Fatalf("ExportNodeMaster: %v", err)
	}

	info, _ := os.Stat(exportPath)
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("perm: got %04o", perm)
	}
}

func TestImportRefusedOverLiveWrappedFile(t *testing.T) {
	datadir := t.TempDir()
	keydir1 := t.TempDir()
	keydir2 := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir1, 0)

	ks1, _ := OpenKeystore(cfg)
	if _, err := LoadOrCreateNodeMaster(datadir, ks1); err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	exportPath := filepath.Join(exportdir, "nm.key")
	if err := ExportNodeMaster(cfg, exportPath); err != nil {
		t.Fatalf("ExportNodeMaster: %v", err)
	}

	cfg.Encryption.KeyFile = makeKeyFile(t, keydir2, 255)
	err := ImportNodeMaster(cfg, exportPath, false)
	if err == nil {
		t.Error("should refuse without force")
	}

	err = ImportNodeMaster(cfg, exportPath, true)
	if err != nil {
		t.Fatalf("import with force: %v", err)
	}
}

func TestWrongKEKRefusesAndNamesBothKids(t *testing.T) {
	datadir := t.TempDir()
	keydir1 := t.TempDir()
	keydir2 := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir1, 0)

	ks1, _ := OpenKeystore(cfg)
	if _, err := LoadOrCreateNodeMaster(datadir, ks1); err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	cfg.Encryption.KeyFile = makeKeyFile(t, keydir2, 255)
	ks2, _ := OpenKeystore(cfg)
	_, err := LoadOrCreateNodeMaster(datadir, ks2)
	if err == nil {
		t.Error("should fail with wrong key")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("kid")) {
		t.Errorf("error should name kid: %v", err)
	}
}

func TestKeystoreOffCreatesNoKeyFiles(t *testing.T) {
	datadir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "off"
	cfg.Encryption.Mode = "off"

	ks, _ := OpenKeystore(cfg)
	nm, _ := LoadOrCreateNodeMaster(datadir, ks)

	if ks != nil || nm != nil {
		t.Error("should be nil")
	}

	keysDir := filepath.Join(datadir, "keys")
	if _, err := os.Stat(keysDir); !os.IsNotExist(err) {
		t.Error("keys dir should not exist")
	}
}

// TestExportNeverCreatesANodeMaster covers the second defect: exporting
// against a data directory with no wrapped node master must refuse, not
// silently mint one (the old code called LoadOrCreateNodeMaster, which
// generates on a missing file).
func TestExportNeverCreatesANodeMaster(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	// No LoadOrCreateNodeMaster call: this data directory has never had a
	// node master created in it.
	exportPath := filepath.Join(exportdir, "nm.key")
	err := ExportNodeMaster(cfg, exportPath)
	if err == nil {
		t.Fatal("ExportNodeMaster should refuse when no node master exists")
	}
	if !strings.Contains(err.Error(), datadir) {
		t.Errorf("error should name the data directory %q: %v", datadir, err)
	}

	wrappedPath := filepath.Join(datadir, "keys", NodeMasterFileName)
	if _, statErr := os.Stat(wrappedPath); !os.IsNotExist(statErr) {
		t.Errorf("ExportNodeMaster created a node master at %s", wrappedPath)
	}
	if _, statErr := os.Stat(exportPath); !os.IsNotExist(statErr) {
		t.Errorf("ExportNodeMaster wrote an export file at %s despite refusing", exportPath)
	}
}

func TestEscrowRehearsal(t *testing.T) {
	datadir := t.TempDir()
	keydir1 := t.TempDir()
	keydir2 := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir1, 0)

	ks1, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore 1: %v", err)
	}

	nm1, err := LoadOrCreateNodeMaster(datadir, ks1)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	initialKid := nm1.Kid()

	exportPath := filepath.Join(exportdir, "nm.key")
	err = ExportNodeMaster(cfg, exportPath)
	if err != nil {
		t.Fatalf("ExportNodeMaster: %v", err)
	}

	cfg.Encryption.KeyFile = makeKeyFile(t, keydir2, 255)

	err = ImportNodeMaster(cfg, exportPath, true)
	if err != nil {
		t.Fatalf("ImportNodeMaster: %v", err)
	}

	ks2, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore 2: %v", err)
	}

	nm2, err := LoadOrCreateNodeMaster(datadir, ks2)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster after import: %v", err)
	}

	if !bytes.Equal(nm1.Bytes(), nm2.Bytes()) {
		t.Error("node master bytes should match after escrow")
	}

	if nm1.Kid() != nm2.Kid() {
		t.Error("node master kid should match after escrow")
	}

	if nm1.Kid() != initialKid {
		t.Error("node master kid changed unexpectedly")
	}
}

// TestDamagedNodeMasterIsNotReportedAsAWrongKeystore: A node
// master that does not open under the configured KEK used to get one message
// for two failures, with sealed.Open's error dropped: "verify the keystore".
// Under the right KEK (the two kids are equal) the file is damaged, and that
// advice sends the operator to the wrong place. Each case must name itself,
// and the cause must stay reachable through errors.Unwrap.
func TestDamagedNodeMasterIsNotReportedAsAWrongKeystore(t *testing.T) {
	keystoreFor := func(t *testing.T, datadir string, seed byte) *Keystore {
		t.Helper()
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = datadir
		cfg.Encryption.Keystore = "keyfile"
		cfg.Encryption.Mode = "on"
		cfg.Encryption.KeyFile = makeKeyFile(t, t.TempDir(), seed)
		ks, err := OpenKeystore(cfg)
		if err != nil {
			t.Fatalf("OpenKeystore: %v", err)
		}
		return ks
	}

	for _, tt := range []struct {
		name       string
		damage     bool
		reopenSeed byte
		want       []string
		banned     []string
	}{
		{
			name: "a damaged file under the right KEK", damage: true, reopenSeed: 0,
			want:   []string{"damaged", NodeMasterFileName, "-import-key"},
			banned: []string{"verify the keystore"},
		},
		{
			name: "the wrong KEK", reopenSeed: 0x40,
			want: []string{"the configured key (kid", "keystore keyfile", "which was sealed under a key with kid", "encryption.key_file"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			datadir := t.TempDir()
			if _, err := LoadOrCreateNodeMaster(datadir, keystoreFor(t, datadir, 0)); err != nil {
				t.Fatalf("creating the node master: %v", err)
			}
			wrappedPath := filepath.Join(datadir, "keys", NodeMasterFileName)
			if tt.damage {
				wrapped, err := os.ReadFile(wrappedPath)
				if err != nil {
					t.Fatalf("reading the wrapped node master: %v", err)
				}
				wrapped[len(wrapped)-1] ^= 0xFF
				if err := os.WriteFile(wrappedPath, wrapped, 0o600); err != nil {
					t.Fatalf("damaging the wrapped node master: %v", err)
				}
			}

			_, err := LoadOrCreateNodeMaster(datadir, keystoreFor(t, datadir, tt.reopenSeed))
			if err == nil {
				t.Fatal("a node master that cannot open was opened")
			}
			if errors.Unwrap(err) == nil {
				t.Errorf("the refusal does not wrap sealed.Open's error: %v", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not say %q: %v", want, err)
				}
			}
			for _, banned := range tt.banned {
				if strings.Contains(err.Error(), banned) {
					t.Errorf("the refusal says %q, which sends the operator to the wrong place: %v", banned, err)
				}
			}
		})
	}
}

// TestExportRefusesAnExistingDestination: an export writes the root key, so it
// never rewrites a file that is already there: an operator re-running the
// command could otherwise overwrite the only copy of another key, and a file
// left from an earlier export would keep whichever mode it has.
func TestExportRefusesAnExistingDestination(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	if _, err := LoadOrCreateNodeMaster(datadir, ks); err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	exportPath := filepath.Join(exportdir, "nm.key")
	previous := []byte("an earlier export that must survive")
	if err := os.WriteFile(exportPath, previous, 0o644); err != nil {
		t.Fatalf("write the existing destination: %v", err)
	}

	err = ExportNodeMaster(cfg, exportPath)
	if err == nil {
		t.Fatal("ExportNodeMaster wrote over an existing destination file")
	}
	for _, want := range []string{exportPath, "new path", "remove"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}

	after, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("read the destination after the refusal: %v", err)
	}
	if !bytes.Equal(after, previous) {
		t.Errorf("the destination changed despite the refusal:\nbefore: %s\nafter:  %s", previous, after)
	}
	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatalf("stat the destination: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o644 {
		t.Errorf("the refused destination's mode changed to %04o, want it left at 0644", perm)
	}
}

// TestExportRefusesASymbolicLinkDestination: an export never writes through a
// link, so a link planted in a writable directory cannot place the root key
// somewhere else.
func TestExportRefusesASymbolicLinkDestination(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	if _, err := LoadOrCreateNodeMaster(datadir, ks); err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	target := filepath.Join(exportdir, "elsewhere.key")
	linkPath := filepath.Join(exportdir, "nm.key")
	if err := os.Symlink(target, linkPath); err != nil {
		t.Skipf("this platform cannot create a symbolic link: %v", err)
	}

	err = ExportNodeMaster(cfg, linkPath)
	if err == nil {
		t.Fatal("ExportNodeMaster accepted a symbolic link as the destination")
	}
	for _, want := range []string{linkPath, "symbolic link", "rm "} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}

	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the link's target %s was written: %v", target, err)
	}
	info, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("lstat the destination: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Errorf("%s is no longer a symbolic link after the refusal", linkPath)
	}
}

// TestExportRefusesASymbolicLinkToAnExistingFile: a link to a file that is
// already there is refused as well, and the file it points at is not touched.
func TestExportRefusesASymbolicLinkToAnExistingFile(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	if _, err := LoadOrCreateNodeMaster(datadir, ks); err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	target := filepath.Join(exportdir, "other.txt")
	previous := []byte("a file the link points at")
	if err := os.WriteFile(target, previous, 0o644); err != nil {
		t.Fatalf("write the link target: %v", err)
	}
	linkPath := filepath.Join(exportdir, "nm.key")
	if err := os.Symlink(target, linkPath); err != nil {
		t.Skipf("this platform cannot create a symbolic link: %v", err)
	}

	if err := ExportNodeMaster(cfg, linkPath); err == nil {
		t.Fatal("ExportNodeMaster wrote through a symbolic link")
	}
	after, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read the link target: %v", err)
	}
	if !bytes.Equal(after, previous) {
		t.Errorf("the link target changed: %q", after)
	}
}

// TestExportCreatesAPrivateFile: a new export holds the node master and is
// readable by its owner only, whatever the process umask is.
func TestExportCreatesAPrivateFile(t *testing.T) {
	datadir := t.TempDir()
	keydir := t.TempDir()
	exportdir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = datadir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, keydir, 0)

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	masterKey, err := LoadOrCreateNodeMaster(datadir, ks)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	exportPath := filepath.Join(exportdir, "nm.key")
	if err := ExportNodeMaster(cfg, exportPath); err != nil {
		t.Fatalf("ExportNodeMaster: %v", err)
	}
	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatalf("stat the export: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("the export's mode is %04o, want 0600", perm)
	}
	exported, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("read the export: %v", err)
	}
	if !bytes.Equal(exported, masterKey.Bytes()) {
		t.Error("the export does not hold the node master")
	}
}
