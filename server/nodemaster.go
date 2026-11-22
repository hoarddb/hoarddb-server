package server

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

const NodeMasterFileName = "node-master.key.wrapped"

// errNoNodeMaster distinguishes "nothing on disk yet" from a read or unwrap
// failure, so ExportNodeMaster can refuse without creating one while
// LoadOrCreateNodeMaster still generates on a genuine first start.
var errNoNodeMaster = errors.New("no wrapped node master on disk")

// NodeMaster is the 32-byte root key for all node-level encryption.
type NodeMaster struct {
	bytes   []byte
	kid     string
	created bool
}

// Bytes returns the 32-byte node master key.
func (nm *NodeMaster) Bytes() []byte {
	return nm.bytes
}

// Kid returns the key ID.
func (nm *NodeMaster) Kid() string {
	return nm.kid
}

// Created reports whether this is a fresh node master (first start).
func (nm *NodeMaster) Created() bool {
	return nm.created
}

// loadNodeMaster reads and unwraps an existing wrapped node master, returning
// errNoNodeMaster when the file is not there. It never writes anything --
// LoadOrCreateNodeMaster generates on a genuine first start; ExportNodeMaster
// must not, so both call this instead of duplicating the unwrap.
func loadNodeMaster(dataDir string, ks *Keystore) (*NodeMaster, error) {
	keyPath := filepath.Join(dataDir, "keys", NodeMasterFileName)

	data, err := os.ReadFile(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errNoNodeMaster
		}
		return nil, fmt.Errorf("reading node master: %w", err)
	}

	plaintext, _, err := sealed.Open(sealed.PurposeNodeMaster, ks.KEK(), func(g uint64) string {
		return fmt.Sprintf("node-master/%d", g)
	}, data)
	if err != nil {
		// Equal kids mean the configured KEK is the one the file was wrapped
		// under, so the keystore is right and the file is not: telling that
		// operator to check the keystore sends them to the wrong place.
		if diskKid := getHeaderKid(data); diskKid == ks.Kid() {
			return nil, fmt.Errorf("opening node master: %s is damaged or was altered — the configured key matches it (kid %s) but the file does not authenticate. "+
				"Restore keys/%s from a backup of this data directory, or import the escrowed node master with 'hoarddb-server -import-key <file> -force' while the server is stopped: %w",
				keyPath, diskKid, NodeMasterFileName, err)
		}
		return nil, wrongKeyError(ks, keyPath, getHeaderKid(data), err)
	}

	if len(plaintext) != 32 {
		return nil, fmt.Errorf("node master key is %d bytes, want 32", len(plaintext))
	}

	return &NodeMaster{
		bytes:   plaintext,
		kid:     sealed.Kid(plaintext),
		created: false,
	}, nil
}

// wrongKeyError is R-1 of the ring encryption at rest spec (§5.4): the configured key is not
// the one the node master was sealed under. It names both key ids, the setting to point at the
// first key, and that the refusal changed nothing in the ring directory.
func wrongKeyError(ks *Keystore, keyPath, kidOnDisk string, cause error) error {
	return fmt.Errorf("opening the node master: the configured key (kid %s, keystore %s) does not open %s, "+
		"which was sealed under a key with kid %s. Point the keystore at the key this node was first started with "+
		"(encryption.key_file, HOARDDB_ENCRYPTION_KEY_FILE, for the keyfile keystore; encryption.key_command or the systemd credential hoarddb-dek otherwise), "+
		"or restore that key from your escrow copy. A new key cannot open data sealed under the old one, so nothing in the ring directory was created or changed: %w",
		ks.Kid(), ks.Source(), keyPath, kidOnDisk, cause)
}

// writeNodeMasterFile is the shared durable write the wrapped node master is
// written through. A test replaces it to record the writes.
var writeNodeMasterFile = storage.WriteFileDurably

// LoadOrCreateNodeMaster loads the node master key or generates one on first start.
func LoadOrCreateNodeMaster(dataDir string, ks *Keystore) (*NodeMaster, error) {
	if ks == nil {
		// keystore=off: no node master
		return nil, nil
	}

	nm, err := loadNodeMaster(dataDir, ks)
	if err == nil {
		return nm, nil
	}
	if !errors.Is(err, errNoNodeMaster) {
		return nil, err
	}

	// An absent file is a first start only if nothing sealed sits beside it. A
	// master minted next to sealed data cannot open it, and would hide that the
	// real one is gone.
	sealedFound, err := findSealedMaterial(dataDir)
	if err != nil {
		return nil, fmt.Errorf("deciding whether the missing node master is a first start: %w", err)
	}
	if len(sealedFound) > 0 {
		return nil, missingNodeMasterError(dataDir, sealedFound)
	}

	keysDir := filepath.Join(dataDir, "keys")
	keyPath := filepath.Join(keysDir, NodeMasterFileName)

	// Nothing sealed on disk: generate a new one
	plaintext := make([]byte, 32)
	if _, err := rand.Read(plaintext); err != nil {
		return nil, fmt.Errorf("generating node master: %w", err)
	}

	// Wrap it
	wrapped, err := sealed.Seal(sealed.PurposeNodeMaster, ks.KEK(), "node-master", 1, plaintext)
	if err != nil {
		return nil, fmt.Errorf("sealing node master: %w", err)
	}

	// Ensure the directory exists
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return nil, fmt.Errorf("creating keys directory: %w", err)
	}

	// Temporary file, fsync, rename and directory fsync, as one durable write.
	if err := writeNodeMasterFile(keyPath, wrapped); err != nil {
		return nil, fmt.Errorf("writing wrapped node master: %w", err)
	}

	return &NodeMaster{
		bytes:   plaintext,
		kid:     sealed.Kid(plaintext),
		created: true,
	}, nil
}

// ExportNodeMaster exports the node master key to a file.
func ExportNodeMaster(cfg *config.Config, path string) error {
	// Resolve the keystore
	ks, err := OpenKeystore(cfg)
	if err != nil {
		return fmt.Errorf("opening keystore: %w", err)
	}

	if ks == nil {
		return fmt.Errorf("cannot export: keystore is off")
	}

	// Load the existing node master. This must never create one: an export
	// run against the wrong data directory would otherwise silently mint a
	// fresh key there instead of failing loudly.
	nm, err := loadNodeMaster(cfg.Storage.DataDir, ks)
	if err != nil {
		if errors.Is(err, errNoNodeMaster) {
			return fmt.Errorf("cannot export: no node master exists in data directory %q; start the server once with encryption on to create one, or check that this is the right data directory", cfg.Storage.DataDir)
		}
		return fmt.Errorf("loading node master: %w", err)
	}

	// Refuse a destination inside the data directory
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolving export path: %w", err)
	}

	dataAbs, err := filepath.Abs(cfg.Storage.DataDir)
	if err != nil {
		return fmt.Errorf("resolving data directory: %w", err)
	}

	if len(abs) > 0 && len(dataAbs) > 0 && (abs == dataAbs || (strings.HasPrefix(abs, dataAbs+string(filepath.Separator)))) {
		return fmt.Errorf("export path must not be inside data_dir (%s is inside %s)", abs, dataAbs)
	}

	// Write the raw node master to a new file only. An existing path is
	// refused rather than replaced, and the final component is never followed:
	// the export is the root key, and a link planted in a writable directory
	// would put it wherever the link points.
	exportFile, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			// O_EXCL refuses a symbolic link with EEXIST as well, so the link
			// check only chooses the wording; the refusal itself is the open.
			if info, lstatErr := os.Lstat(abs); lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("export path %q is a symbolic link. Choose a new path, or remove the link first (rm %s); hoardDB does not write a node master through a link", abs, abs)
			}
			return fmt.Errorf("export path %q already exists. Choose a new path, or remove the old file first (rm %s) if you no longer need it", abs, abs)
		}
		return fmt.Errorf("creating export file %s: %w", abs, err)
	}
	if _, err := exportFile.Write(nm.Bytes()); err != nil {
		return errors.Join(fmt.Errorf("writing export file %s: %w", abs, err), exportFile.Close())
	}
	if err := exportFile.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing export file %s: %w", abs, err), exportFile.Close())
	}
	if err := exportFile.Close(); err != nil {
		return fmt.Errorf("closing export file %s: %w", abs, err)
	}

	fmt.Printf("Exported node master with kid %s\n", nm.Kid())
	return nil
}

// ImportNodeMaster imports a node master and rewraps it under the current keystore.
func ImportNodeMaster(cfg *config.Config, path string, force bool) error {
	// Read the export file
	nm, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading import file: %w", err)
	}

	if len(nm) != 32 {
		return fmt.Errorf("imported node master is %d bytes, want 32", len(nm))
	}

	// Resolve the current keystore
	ks, err := OpenKeystore(cfg)
	if err != nil {
		return fmt.Errorf("opening keystore: %w", err)
	}

	if ks == nil {
		return fmt.Errorf("cannot import: keystore is off")
	}

	// A master that opens none of the directory's sealed keys would leave a node
	// that cannot start; refuse it before anything is overwritten, -force or not.
	if err := verifyImportedMaster(cfg.Storage.DataDir, nm); err != nil {
		return err
	}

	// Check if a wrapped file already exists
	keysDir := filepath.Join(cfg.Storage.DataDir, "keys")
	keyPath := filepath.Join(keysDir, NodeMasterFileName)

	if _, err := os.Stat(keyPath); err == nil {
		if !force {
			return fmt.Errorf("wrapped node master already exists at %s\n       use -force to overwrite", keyPath)
		}
	}

	// Wrap the imported node master
	wrapped, err := sealed.Seal(sealed.PurposeNodeMaster, ks.KEK(), "node-master", 1, nm)
	if err != nil {
		return fmt.Errorf("sealing imported node master: %w", err)
	}

	// Ensure the directory exists
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("creating keys directory: %w", err)
	}

	// Temporary file, fsync, rename and directory fsync, as one durable write.
	if err := writeNodeMasterFile(keyPath, wrapped); err != nil {
		return fmt.Errorf("writing wrapped node master: %w", err)
	}

	fmt.Printf("Imported node master with kid %s\n", sealed.Kid(nm))
	return nil
}

// getHeaderKid extracts the kid from an envelope header for error messages.
func getHeaderKid(data []byte) string {
	if len(data) < 14 {
		return "unknown"
	}
	return string(data[6:14])
}
