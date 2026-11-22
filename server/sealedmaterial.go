package server

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// A key file that is absent is not evidence of a first start: the data it
// protects may still be there. Everything below decides "this directory holds
// sealed data" from the data, so that a start with a lost key file refuses
// instead of minting a replacement that cannot open what is beside it (spec §3 step 7 states the same rule for the store key).

const (
	// walDirName is the WAL's directory under the data directory.
	walDirName = "wal"
	// walSegmentGlob matches the WAL's segment files (storage/wal.go:segmentFileName).
	walSegmentGlob = "wal_*.log"
	// usersFileName is the users file under the data directory.
	usersFileName = "users.json"
)

// findSealedMaterial reports which artifacts in dataDir prove it held sealed
// data. It reads, never writes, and returns nothing for a directory that is
// genuinely fresh. The artifacts it checks, and only these:
//
//   - keys/store.wrapped exists (the node store's wrapped data key)
//   - wal/keys.wrapped exists (the WAL epoch registry)
//   - keys/blobs.wrapped exists (the blob epoch registry: the only copy of the
//     keys sealed blob files are written under)
//   - users.json starts with a v2 sealed envelope (magic 00 'H' 'D' 'B', version 02);
//     the {0x00, 0x01} form is encrypted under auth.token.key, not the node master
//   - a wal/wal_*.log segment's header says its entries are sealed: a v1
//     sealed header, or a segment format 2 header with the sealed flag (a
//     plain v2 segment has the same 24-byte layout and is not sealed;
//     storage.IsSealedWALSegmentHeader)
//   - store/KEYREGISTRY proves the Badger store was sealed (storeIsSealed)
//   - auth.token.key, cluster.token or keys/server.key is a sealed envelope
//     (credential_files.go)
//   - the ring's four: ring/keys/store.wrapped or ring/keys/wal.wrapped exists,
//     ring/badger's KEYREGISTRY is sealed, or a ring/wal segment is sealed
//     (ring encryption at rest spec §5.4 R-7, ringEvidence.sealedFound)
//
// An I/O error other than "does not exist" is returned: a start that cannot
// tell must not guess "fresh".
func findSealedMaterial(dataDir string) ([]string, error) {
	var found []string

	for _, wrappedFile := range []string{
		filepath.Join("keys", nodeStoreKeyFileName),
		filepath.Join(walDirName, WALEpochRegistryFileName),
		filepath.Join(nodeKeysDirName, blobEpochRegistryFileName),
	} {
		_, err := os.Stat(filepath.Join(dataDir, wrappedFile))
		switch {
		case err == nil:
			found = append(found, filepath.ToSlash(wrappedFile))
		case !errors.Is(err, fs.ErrNotExist):
			return nil, fmt.Errorf("checking %s for sealed data: %w", wrappedFile, err)
		}
	}

	usersHeader, err := readFilePrefix(filepath.Join(dataDir, usersFileName), sealed.EnvelopeHeaderLen)
	if err != nil {
		return nil, fmt.Errorf("checking %s for sealed data: %w", usersFileName, err)
	}
	if isNodeMasterSealedUsersFile(usersHeader) {
		found = append(found, usersFileName+" (sealed under the node master)")
	}

	sealedSegments, err := findSealedWALSegments(filepath.Join(dataDir, walDirName), walDirName)
	if err != nil {
		return nil, err
	}
	found = append(found, sealedSegments...)

	storeDir := filepath.Join(dataDir, nodeStoreDirName)
	if storeIsSealed(storeDir) {
		found = append(found, nodeStoreDirName+"/ (its key registry is sealed)")
	}

	sealedCredentials, err := findSealedCredentials(dataDir)
	if err != nil {
		return nil, err
	}
	found = append(found, sealedCredentials...)

	ringEvidence, err := readRingEvidence(filepath.Join(dataDir, ringDirName))
	if err != nil {
		return nil, err
	}
	return append(found, ringEvidence.sealedFound()...), nil
}

// isNodeMasterSealedUsersFile reports whether header opens a v2 sealed
// envelope. It matches users_crypto.go:usersFileGen's own test.
func isNodeMasterSealedUsersFile(header []byte) bool {
	return len(header) >= sealed.EnvelopeHeaderLen && isEncryptedUsersFile(header) && header[4] == sealed.Version
}

// findSealedWALSegments names the sealed segment files in walDir, each as
// label/<file>, describing many as the first plus a count. The legacy log is
// <data dir>/wal, labelled "wal"; the ring log is <data dir>/ring/wal, labelled
// "ring/wal".
func findSealedWALSegments(walDir, label string) ([]string, error) {
	segmentPaths, err := filepath.Glob(filepath.Join(walDir, walSegmentGlob))
	if err != nil {
		return nil, fmt.Errorf("listing WAL segments: %w", err)
	}
	var sealedNames []string
	for _, segmentPath := range segmentPaths {
		header, err := readFilePrefix(segmentPath, sealed.FileHeaderLen)
		if err != nil {
			return nil, fmt.Errorf("checking WAL segment %s for sealed data: %w", filepath.Base(segmentPath), err)
		}
		// v1 sealed, or segment format 2 with the sealed flag; never a plain
		// v2 segment, whose header has the same layout (spec §5).
		if storage.IsSealedWALSegmentHeader(header) {
			sealedNames = append(sealedNames, filepath.Base(segmentPath))
		}
	}
	switch len(sealedNames) {
	case 0:
		return nil, nil
	case 1:
		return []string{label + "/" + sealedNames[0] + " (sealed)"}, nil
	default:
		return []string{fmt.Sprintf("%s/%s and %d more sealed segments", label, sealedNames[0], len(sealedNames)-1)}, nil
	}
}

// findPlainWALSegments names the segment files in walDir whose header is a
// segment format 2 header without the sealed flag, each as label/<file>. A
// file with no header says nothing and is not named.
func findPlainWALSegments(walDir, label string) ([]string, error) {
	segmentPaths, err := filepath.Glob(filepath.Join(walDir, walSegmentGlob))
	if err != nil {
		return nil, fmt.Errorf("listing WAL segments: %w", err)
	}
	var plainNames []string
	for _, segmentPath := range segmentPaths {
		header, err := readFilePrefix(segmentPath, sealed.FileHeaderLen)
		if err != nil {
			return nil, fmt.Errorf("checking WAL segment %s for plaintext entries: %w", filepath.Base(segmentPath), err)
		}
		if storage.IsPlainWALSegmentHeader(header) {
			plainNames = append(plainNames, label+"/"+filepath.Base(segmentPath))
		}
	}
	return plainNames, nil
}

// readFilePrefix returns up to length leading bytes of path, and nothing for a
// file that does not exist.
func readFilePrefix(path string, length int) ([]byte, error) {
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	prefix := make([]byte, length)
	readCount, err := io.ReadFull(file, prefix)
	closeErr := file.Close()
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return prefix[:readCount], nil
}

// missingNodeMasterError is the refusal for a node master that is gone from a
// directory that still holds sealed data. AGENTS.md rule 3: the way out is part
// of the message.
func missingNodeMasterError(dataDir string, found []string) error {
	return fmt.Errorf("refusing to start: keys/%[1]s is missing from data directory %[2]q, but the directory holds sealed data "+
		"that only that node master can open (found: %[3]s). A new node master could not open it, so none was created and nothing was written. "+
		"Restore keys/%[1]s from a backup of this data directory, or import the escrowed node master with "+
		"'hoarddb-server -import-key <file>' while the server is stopped, then start again. "+
		"If you meant to start a new empty node, point data_dir at an empty directory instead",
		NodeMasterFileName, dataDir, strings.Join(found, ", "))
}

// missingWALRegistryError is the same refusal for the WAL epoch registry, which
// holds the only copy of the keys its segments are sealed under.
func missingWALRegistryError(dataDir string, sealedSegments []string) error {
	return fmt.Errorf("refusing to start: %[1]s/%[2]s is missing from data directory %[3]q, but the write-ahead log holds sealed segments (%[4]s) "+
		"that only the epoch keys in that file can open. A new registry would hold different keys, so none was created and nothing was written. "+
		"Restore %[1]s/%[2]s from a backup of this data directory: the epoch keys exist nowhere else, and the node master escrow does not cover them. "+
		"Only if you accept losing the whole log, move the %[1]s directory aside and start again",
		walDirName, WALEpochRegistryFileName, dataDir, strings.Join(sealedSegments, ", "))
}

// verifyImportedMaster refuses a node master that does not open the sealed
// keys already in dataDir. -import-key -force otherwise installs any 32 bytes
// and leaves a node that cannot start, having overwritten the file that could
// have been restored. It checks the store key, the WAL epoch registry, the
// blob epoch registry, the ring's two key files and the sealed credential
// files. Nothing is checked when no sealed key file is present:
// importing into a fresh directory is how a node is provisioned.
func verifyImportedMaster(dataDir string, master []byte) error {
	storeKeyPath := filepath.Join(dataDir, "keys", nodeStoreKeyFileName)
	wrappedStoreKey, err := os.ReadFile(storeKeyPath)
	switch {
	case err == nil:
		if _, err := openWrappedStoreKey(master, wrappedStoreKey, nodeStoreKeyIdentity); err != nil {
			return wrongImportedMasterError(master, storeKeyPath, wrappedStoreKey, err)
		}
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("reading %s to check the imported node master against it: %w", storeKeyPath, err)
	}

	registryPath := filepath.Join(dataDir, walDirName, WALEpochRegistryFileName)
	wrappedRegistry, err := os.ReadFile(registryPath)
	switch {
	case err == nil:
		if _, _, err := openWALEpochRegistry(master, wrappedRegistry, legacyWALRegistryIdentity); err != nil {
			return wrongImportedMasterError(master, registryPath, wrappedRegistry, err)
		}
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("reading %s to check the imported node master against it: %w", registryPath, err)
	}

	blobRegistryPath := filepath.Join(dataDir, nodeKeysDirName, blobEpochRegistryFileName)
	wrappedBlobRegistry, err := os.ReadFile(blobRegistryPath)
	switch {
	case err == nil:
		if _, err := openBlobEpochRegistry(master, wrappedBlobRegistry); err != nil {
			return wrongImportedMasterError(master, blobRegistryPath, wrappedBlobRegistry, err)
		}
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("reading %s to check the imported node master against it: %w", blobRegistryPath, err)
	}
	if err := verifyImportedMasterOpensRingKeys(dataDir, master); err != nil {
		return err
	}
	if err := verifyImportedMasterOpensCredentials(dataDir, master); err != nil {
		return err
	}
	return verifyImportedMasterOpensUsers(dataDir, master)
}

// verifyImportedMasterOpensRingKeys refuses an imported node master that does
// not open ring/keys/store.wrapped or ring/keys/wal.wrapped, whichever exist:
// the ring's data is sealed under them (ring encryption at rest spec §5.6).
func verifyImportedMasterOpensRingKeys(dataDir string, master []byte) error {
	keysDir := filepath.Join(dataDir, ringDirName, ringKeysDirName)
	storeKeyPath := filepath.Join(keysDir, ringStoreKeyFileName)
	storeWrapped, err := readRingKeyFile(storeKeyPath)
	if err != nil {
		return fmt.Errorf("checking the imported node master against it: %w", err)
	}
	if storeWrapped != nil {
		if _, err := openWrappedStoreKey(master, storeWrapped, ringStoreKeyIdentity); err != nil {
			return wrongImportedMasterError(master, storeKeyPath, storeWrapped, err)
		}
	}
	registryPath := filepath.Join(keysDir, ringWALKeyFileName)
	registryWrapped, err := readRingKeyFile(registryPath)
	if err != nil {
		return fmt.Errorf("checking the imported node master against it: %w", err)
	}
	if registryWrapped != nil {
		if _, _, err := openWALEpochRegistry(master, registryWrapped, ringWALKeyIdentity); err != nil {
			return wrongImportedMasterError(master, registryPath, registryWrapped, err)
		}
	}
	return nil
}

// verifyImportedMasterOpensUsers refuses an imported node master that does not
// open users.json when that file is sealed under a node master. A legacy
// signing-key envelope or plaintext file does not depend on the node master.
func verifyImportedMasterOpensUsers(dataDir string, master []byte) error {
	usersPath := UsersFilePath(dataDir)
	contents, err := os.ReadFile(usersPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading %s to check the imported node master against it: %w", usersPath, err)
	}
	if !isNodeMasterSealedUsersFile(contents) {
		return nil
	}
	if _, _, err := openUsersWithNodeMaster(contents, &NodeMaster{bytes: master, kid: sealed.Kid(master)}); err != nil {
		return wrongImportedMasterError(master, usersPath, contents, err)
	}
	return nil
}

func wrongImportedMasterError(master []byte, sealedPath string, wrapped []byte, cause error) error {
	return fmt.Errorf("the imported node master (kid %s) does not open %s, which was sealed under a node master with kid %s: "+
		"it is not this data directory's node master, so nothing was written. Import the file 'hoarddb-server -export-key' wrote for this node, "+
		"or restore keys/%s from a backup of this data directory: %w",
		sealed.Kid(master), sealedPath, getHeaderKid(wrapped), NodeMasterFileName, cause)
}
