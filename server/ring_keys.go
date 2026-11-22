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

	"github.com/dgraph-io/badger/v4"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The ring node's at-rest keys (ring encryption at rest spec §5.1): two files in <ring>/keys,
// each wrapped under the node master with the existing envelope. Their identities differ from
// the legacy store's and log's, so a ring key file never opens as a legacy one or the reverse.
const (
	ringKeysDirName       = "keys"
	ringStoreKeyFileName  = "store.wrapped"
	ringWALKeyFileName    = "wal.wrapped"
	ringStoreKeyIdentity  = "ring-store"
	ringWALKeyIdentity    = "ring-wal"
	ringKeyTempFilePrefix = ".tmp-"
	ringStoreDirName      = "badger"
	ringLogDirName        = "wal"
)

// modeSealsStore reports whether a node started in mode keeps a sealed store.
// A start whose mode disagrees with the store on disk is refused
// (nodeStoreDataKey for the legacy store, loadRingAtRest for the ring), so the
// mode is the store's state.
func modeSealsStore(mode string) bool {
	return mode == "on" || mode == "require"
}

// ringAtRest is the ring node's key chain below the node master: the keys loadRingAtRest
// unwrapped before the node opened, and the means to create the ones it found absent. It
// implements wire.AtRestKeys. A node with encryption off has none.
type ringAtRest struct {
	nodeMaster *NodeMaster
	ringDir    string
	// storeKey and logKeys are what the load unwrapped, or nil when the file did not exist;
	// StoreKey and LogKeys fill them when they create the file.
	storeKey []byte
	logKeys  storage.WALKeySource
}

// Seams the creation tests replace: the no-replace link of a key file to its final name, the
// directory syncs, and a hook right before the link that lets a test play a second creator.
var (
	ringKeyLink       = os.Link
	ringKeySyncDir    = syncDirectory
	beforeRingKeyLink func(finalPath string)
)

// loadRingAtRest is the read-only half of the ring's keys (spec §5.3, §5.4): it runs in the
// server before wire.OpenNode, refuses a ring directory whose evidence disagrees with the mode
// (R-3, R-4, R-5, R-9), and unwraps <ring>/keys/store.wrapped and <ring>/keys/wal.wrapped when
// they exist (R-8 when one does not open). It creates nothing, so a refusal leaves the ring
// directory as it was. It returns nil for a node whose mode does not seal.
func loadRingAtRest(cfg *config.Config, nm *NodeMaster) (*ringAtRest, error) {
	dataDir := cfg.Storage.DataDir
	ringDir := filepath.Join(dataDir, ringDirName)
	evidence, err := readRingEvidence(ringDir)
	if err != nil {
		return nil, adviseRingMember(err, cfg.Cluster.Enabled)
	}
	mode := cfg.Encryption.Mode
	if !modeSealsStore(mode) {
		if found := evidence.sealedFound(); len(found) > 0 {
			return nil, ringSealedModeOffError(dataDir, mode, found, cfg.Cluster.Enabled)
		}
		return nil, nil
	}
	if found := evidence.plaintextFound(); len(found) > 0 {
		return nil, ringPlaintextModeOnError(dataDir, mode, found, cfg.Cluster.Enabled)
	}
	if missing := evidence.missingKeyFiles(ringDir); len(missing) > 0 {
		return nil, ringKeysMissingError(ringDir, missing, cfg.Cluster.Enabled)
	}
	// require's structural check reads ring/badger's files before Badger opens it: opening
	// rotates the logs, which would destroy the evidence and is itself a write (spec §5.7,
	// BUG-0612's reason for the earlier store).
	if mode == "require" && evidence.storeExists {
		storeDir := filepath.Join(ringDir, ringStoreDirName)
		if problems := storeStructureProblems(storeDir); len(problems) > 0 {
			return nil, errRequireRingStoreNotSealed(storeDir, problems[0], cfg.Cluster.Enabled)
		}
	}
	if nm == nil {
		return nil, fmt.Errorf("encryption.mode is %q but no node master key is loaded; set encryption.keystore so the ring store at %s can be sealed", mode, ringDir)
	}
	keys := &ringAtRest{nodeMaster: nm, ringDir: ringDir}
	keysDir := filepath.Join(ringDir, ringKeysDirName)
	storeWrapped, err := readRingKeyFile(filepath.Join(keysDir, ringStoreKeyFileName))
	if err != nil {
		return nil, err
	}
	if storeWrapped != nil {
		if keys.storeKey, err = openWrappedStoreKey(nm.Bytes(), storeWrapped, ringStoreKeyIdentity); err != nil {
			return nil, ringKeyUnwrapError(filepath.Join(keysDir, ringStoreKeyFileName), nm, err)
		}
	}
	logWrapped, err := readRingKeyFile(filepath.Join(keysDir, ringWALKeyFileName))
	if err != nil {
		return nil, err
	}
	if logWrapped != nil {
		registry, _, err := openWALEpochRegistry(nm.Bytes(), logWrapped, ringWALKeyIdentity)
		if err != nil {
			return nil, ringKeyUnwrapError(filepath.Join(keysDir, ringWALKeyFileName), nm, err)
		}
		keys.logKeys = newWALKeySource(registry)
	}
	return keys, nil
}

// ringEvidence is what the ring directory's own files say about its mode, read without opening
// anything: the key files, whether ring/badger holds a store and whether its key registry is
// sealed, and which ring/wal segments are sealed and which are plain.
type ringEvidence struct {
	storeKeyFile, logKeyFile bool
	storeExists              bool
	registrySealed           bool
	sealedSegments           []string
	plainSegments            []string
}

// readRingEvidence reads ringDir's evidence. An I/O error other than "does not exist" is
// returned: a start that cannot tell must not guess.
func readRingEvidence(ringDir string) (ringEvidence, error) {
	var evidence ringEvidence
	var err error
	keysDir := filepath.Join(ringDir, ringKeysDirName)
	if evidence.storeKeyFile, err = ringFileExists(filepath.Join(keysDir, ringStoreKeyFileName)); err != nil {
		return ringEvidence{}, err
	}
	if evidence.logKeyFile, err = ringFileExists(filepath.Join(keysDir, ringWALKeyFileName)); err != nil {
		return ringEvidence{}, err
	}
	storeDir := filepath.Join(ringDir, ringStoreDirName)
	// Any data file, not MANIFEST alone: a store whose MANIFEST is gone but whose tables remain
	// is a store, and read as none it would get a key created beside it before the store's guard
	// refuses it (integration review F-1).
	if evidence.storeExists, err = storage.StoreHasDataFiles(storeDir); err != nil {
		return ringEvidence{}, err
	}
	if evidence.registrySealed, err = ringRegistrySealed(ringDir, storeDir); err != nil {
		return ringEvidence{}, err
	}
	logDir := filepath.Join(ringDir, ringLogDirName)
	if evidence.sealedSegments, err = findSealedWALSegments(logDir, ringDirName+"/"+ringLogDirName); err != nil {
		return ringEvidence{}, err
	}
	if evidence.plainSegments, err = findPlainWALSegments(logDir, ringDirName+"/"+ringLogDirName); err != nil {
		return ringEvidence{}, err
	}
	return evidence, nil
}

// ringRegistrySealed answers three ways where storeIsSealed answers two (review F-2): the ring
// store's KEYREGISTRY is sealed, is not sealed (or is absent), or cannot be read, which is an
// error naming the file, the cause and the fix. Only Badger's key mismatch means sealed; a
// registry that opens with an empty key means not sealed; any other failure means this start
// cannot tell, and a start that cannot tell must not guess "not encrypted".
func ringRegistrySealed(ringDir, storeDir string) (bool, error) {
	registryPath := filepath.Join(storeDir, badger.KeyRegistryFileName)
	registryFile, err := os.Open(registryPath)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, ringRegistryUnreadableError(registryPath, ringDir, err)
	}
	if err := registryFile.Close(); err != nil {
		return false, ringRegistryUnreadableError(registryPath, ringDir, err)
	}
	_, err = badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: storeDir, ReadOnly: true})
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, badger.ErrEncryptionKeyMismatch):
		return true, nil
	}
	return false, ringRegistryUnreadableError(registryPath, ringDir, err)
}

// ringRegistryUnreadableError is the refusal for a ring key registry this start cannot read.
func ringRegistryUnreadableError(registryPath, ringDir string, cause error) error {
	return &ringRegistryUnreadable{registryPath: registryPath, ringDir: ringDir, cause: cause}
}

// ringRegistryUnreadable is a ring key registry this start cannot read. The evidence read that
// finds it does not know the configuration (the host's node-master check reads it too), so the
// callers that do mark a cluster member (adviseRingMember): a member is never told to restore its
// ring directory from a backup, since a voter put back from an older copy has forgotten the tail
// of its Raft log (owner ruling 2026-11-19), and gets wire.ClusterMemberRingAdvice instead.
type ringRegistryUnreadable struct {
	registryPath, ringDir string
	cause                 error
	member                bool
}

func (e *ringRegistryUnreadable) Error() string {
	recovery := fmt.Sprintf("or, if the disk reports errors, restore %s from a backup of this data directory; then start again", e.ringDir)
	if e.member {
		recovery = "then start again. If the disk reports errors: " + wire.ClusterMemberRingAdvice(e.ringDir)
	}
	return fmt.Sprintf("cannot read %s to tell whether the ring store at %s is encrypted: %v. Nothing was opened or "+
		"written. Make %s and everything in it readable and writable by the user this server runs as (files left owned "+
		"by root after a start as root need chown -R to that user), %s", e.registryPath, e.ringDir, e.cause, e.ringDir, recovery)
}

func (e *ringRegistryUnreadable) Unwrap() error { return e.cause }

// adviseRingMember gives an unreadable ring key registry in err the advice for a cluster member
// when member is set, and returns err.
func adviseRingMember(err error, member bool) error {
	var unreadable *ringRegistryUnreadable
	if member && errors.As(err, &unreadable) {
		unreadable.member = true
	}
	return err
}

// linkRefusedByFilesystem reports whether a link failed because the filesystem does not do hard
// links, rather than because of the files: the errors FUSE, SMB/CIFS and network mounts give.
func linkRefusedByFilesystem(err error) bool {
	for _, refusal := range []syscall.Errno{syscall.EPERM, syscall.EXDEV, syscall.ENOTSUP, syscall.EOPNOTSUPP, syscall.ENOSYS, syscall.EMLINK} {
		if errors.Is(err, refusal) {
			return true
		}
	}
	return false
}

// ringFileExists reports whether path exists.
func ringFileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	}
	return false, fmt.Errorf("checking %s: %w", path, err)
}

// sealedFound names the evidence that the ring was sealed: either key file, a sealed key
// registry, a sealed segment (the four R-3 and R-7 look for).
func (e ringEvidence) sealedFound() []string {
	var found []string
	if e.storeKeyFile {
		found = append(found, ringDirName+"/"+ringKeysDirName+"/"+ringStoreKeyFileName)
	}
	if e.logKeyFile {
		found = append(found, ringDirName+"/"+ringKeysDirName+"/"+ringWALKeyFileName)
	}
	if e.registrySealed {
		found = append(found, ringDirName+"/"+ringStoreDirName+"/ (its key registry is sealed)")
	}
	return append(found, e.sealedSegments...)
}

// plaintextFound names the evidence that the ring was written in the clear (R-4, R-9): a store
// whose key registry is not sealed, with no key file beside it or with one (R-9), and every
// plain segment, whether or not wal.wrapped exists.
func (e ringEvidence) plaintextFound() []string {
	var found []string
	switch {
	case e.storeExists && !e.registrySealed && e.storeKeyFile:
		found = append(found, "a ring key file beside a store that was created without it")
	case e.storeExists && !e.registrySealed:
		found = append(found, ringDirName+"/"+ringStoreDirName+" holds a store created without a key")
	}
	for _, segment := range e.plainSegments {
		found = append(found, segment+" is not sealed")
	}
	return found
}

// missingKeyFiles names every ring key file that is gone beside the data it sealed (R-5).
func (e ringEvidence) missingKeyFiles(ringDir string) []string {
	var missing []string
	if e.registrySealed && !e.storeKeyFile {
		missing = append(missing, filepath.Join(ringDir, ringKeysDirName, ringStoreKeyFileName))
	}
	if len(e.sealedSegments) > 0 && !e.logKeyFile {
		missing = append(missing, filepath.Join(ringDir, ringKeysDirName, ringWALKeyFileName))
	}
	return missing
}

// ringSealedModeOffError is R-3: a sealed ring started with encryption off. A single node is told
// how to move its ring aside to run in plaintext; a cluster member is not (wire.ClusterMemberRingAdvice).
func ringSealedModeOffError(dataDir, mode string, found []string, member bool) error {
	ringDir := filepath.Join(dataDir, ringDirName)
	if member {
		return fmt.Errorf("the ring store at %s is encrypted (found: %s), but encryption.mode is %q. A node's "+
			"encryption is fixed when its ring store is created and is not switched in place. Start this node with "+
			"encryption.mode on (HOARDDB_ENCRYPTION_MODE=on) and the keystore that sealed it. %s. Every node of a "+
			"cluster must use the same setting",
			ringDir, strings.Join(found, ", "), mode, wire.ClusterMemberRingAdvice(ringDir))
	}
	return fmt.Errorf("the ring store at %s is encrypted (found: %s), but encryption.mode is %q. A node's "+
		"encryption is fixed when its ring store is created and is not switched in place. Start this node with "+
		"encryption.mode on (HOARDDB_ENCRYPTION_MODE=on) and the keystore that sealed it. To run this node in plaintext "+
		"instead, stop it and move %s aside (keep it: it is the only copy of this node's data), and also the node store "+
		"this build keeps beside it, %s and %s, keep encryption.keystore set, and start it with "+
		"encryption.mode off. A cluster member comes back empty and is not reseeded while its cluster holds a database, "+
		"and every node of a cluster must use the same setting",
		ringDir, strings.Join(found, ", "), mode, ringDir,
		filepath.Join(dataDir, nodeStoreDirName), filepath.Join(dataDir, "keys", nodeStoreKeyFileName))
}

// ringPlaintextModeOnError is R-4 (and R-9 by its evidence): a plaintext ring started with
// encryption on or require. A cluster member is not told to move its ring aside.
func ringPlaintextModeOnError(dataDir, mode string, found []string, member bool) error {
	ringDir := filepath.Join(dataDir, ringDirName)
	if member {
		return fmt.Errorf("the ring store at %s is not encrypted (found: %s), but encryption.mode is %q. A node's "+
			"encryption is fixed when its ring store is created and is not switched in place. Start this node with "+
			"encryption.mode off (HOARDDB_ENCRYPTION_MODE=off). %s. Every node of a cluster must use the same setting",
			ringDir, strings.Join(found, ", "), mode, wire.ClusterMemberRingAdvice(ringDir))
	}
	return fmt.Errorf("the ring store at %s is not encrypted (found: %s), but encryption.mode is %q. A node's "+
		"encryption is fixed when its ring store is created and is not switched in place. Start this node with "+
		"encryption.mode off (HOARDDB_ENCRYPTION_MODE=off). To run this node encrypted instead, stop it and move %s "+
		"aside (keep it: it is the only copy of this node's data), and also the node store this build keeps beside "+
		"it, %s, then start it with encryption.mode on and a keystore. A cluster member comes back empty and "+
		"is not reseeded while its cluster holds a database, and every node of a cluster must use the same setting",
		ringDir, strings.Join(found, ", "), mode, ringDir, filepath.Join(dataDir, nodeStoreDirName))
}

// ringKeysMissingError is R-5: sealed ring data whose key files are gone, every missing file in
// one text. Without a backup a single node is told to start with an empty ring; a cluster member
// is not.
func ringKeysMissingError(ringDir string, missing []string, member bool) error {
	verb := "is"
	if len(missing) > 1 {
		verb = "are"
	}
	without := fmt.Sprintf("Without a backup this node's ring data cannot be read: stop it, move %s aside (keep it) and "+
		"start it with an empty ring; a cluster member is not reseeded while its cluster holds a database", ringDir)
	if member {
		without = wire.ClusterMemberRingAdvice(ringDir)
	}
	return fmt.Errorf("the ring data at %s is sealed, but %s %s missing. encryption.mode off cannot open sealed "+
		"data, and a new key cannot either, so none was created and nothing was written. Restore ring/keys/ from a "+
		"backup of this data directory: these keys exist nowhere else, and the node-master escrow does not cover them. %s",
		ringDir, strings.Join(missing, " and "), verb, without)
}

// readRingKeyFile returns a key file's bytes, or nil when it does not exist.
func readRingKeyFile(path string) ([]byte, error) {
	wrapped, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading the ring key %s: %w", path, err)
	}
	return wrapped, nil
}

// ringKeyUnwrapError is R-8: a ring key file the node master does not open, refused in the load.
func ringKeyUnwrapError(path string, nm *NodeMaster, cause error) error {
	return fmt.Errorf("unwrapping the ring key %s under node master kid %s: %w. The node master opens but the "+
		"ring key does not, so the file is damaged or belongs to another data directory. Restore ring/keys/ from a "+
		"backup of this data directory; nothing in the ring directory was created or changed", path, nm.Kid(), cause)
}

// ringWinnerKeyError is a key file another process linked first that this process's node master
// does not open: it was created during this start, so nothing is said of the ring directory.
func ringWinnerKeyError(path string, nm *NodeMaster, cause error) error {
	return fmt.Errorf("another process created the ring key %s during this start, and node master kid %s does not "+
		"open it; stop every process that uses this data directory and start one: %w", path, nm.Kid(), cause)
}

// StoreKey returns the ring store's key: the one the load unwrapped, or a new one, created
// now because the load found no key file. Node.open calls it after the ring directory passed
// its format check and before Badger creates a file.
func (k *ringAtRest) StoreKey(storeDir string) ([]byte, error) {
	if k.storeKey != nil {
		return k.storeKey, nil
	}
	dataKey := make([]byte, 32)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, fmt.Errorf("generating the ring store key: %w", err)
	}
	wrapped, err := sealed.Seal(sealed.PurposeStoreKey, k.nodeMaster.Bytes(), ringStoreKeyIdentity, 1, dataKey)
	if err != nil {
		return nil, fmt.Errorf("sealing the ring store key: %w", err)
	}
	ringDir := filepath.Dir(storeDir)
	created, err := createRingKeyFile(ringDir, ringStoreKeyFileName, wrapped)
	if err != nil {
		return nil, err
	}
	if !created {
		// Another process linked its key first and writes under it: use that key, never
		// replace it (spec §5.3). That process holds Badger's lock, so this one is refused
		// right after with Badger's in-use error.
		path := filepath.Join(ringDir, ringKeysDirName, ringStoreKeyFileName)
		winner, err := readRingKeyFile(path)
		if err != nil {
			return nil, err
		}
		if dataKey, err = openWrappedStoreKey(k.nodeMaster.Bytes(), winner, ringStoreKeyIdentity); err != nil {
			return nil, ringWinnerKeyError(path, k.nodeMaster, err)
		}
	}
	k.storeKey = dataKey
	return dataKey, nil
}

// LogKeys returns the ring log's key source: the registry the load unwrapped, or a new one
// of one epoch, created now because the load found no registry file.
func (k *ringAtRest) LogKeys(logDir string) (storage.WALKeySource, error) {
	if k.logKeys != nil {
		return k.logKeys, nil
	}
	registry, wrapped, err := newSealedWALEpochRegistry(k.nodeMaster.Bytes(), ringWALKeyIdentity)
	if err != nil {
		return nil, err
	}
	ringDir := filepath.Dir(logDir)
	created, err := createRingKeyFile(ringDir, ringWALKeyFileName, wrapped)
	if err != nil {
		return nil, err
	}
	if !created {
		path := filepath.Join(ringDir, ringKeysDirName, ringWALKeyFileName)
		winner, err := readRingKeyFile(path)
		if err != nil {
			return nil, err
		}
		if registry, _, err = openWALEpochRegistry(k.nodeMaster.Bytes(), winner, ringWALKeyIdentity); err != nil {
			return nil, ringWinnerKeyError(path, k.nodeMaster, err)
		}
	}
	k.logKeys = newWALKeySource(registry)
	return k.logKeys, nil
}

// createRingKeyFile makes <ringDir>/keys/<name> hold contents, durably and without ever
// replacing a file of that name: create keys/ and sync the ring directory so the new entry
// survives a crash, write a uniquely named temporary file and sync it, link it to the final
// name (a link fails when the name exists, where a rename would replace it), remove the
// temporary name and sync keys/. It reports false, with nothing replaced, when another
// process created the file first. A failure here is an I/O failure; the ring directory may
// hold a keys/ directory or a temporary file afterwards. A leftover temporary file is ignored,
// not swept (review F-4): it is never read as a key and never counts as evidence, and a sweep
// could delete the temporary file of a second process about to link it.
func createRingKeyFile(ringDir, name string, contents []byte) (bool, error) {
	keysDir := filepath.Join(ringDir, ringKeysDirName)
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return false, fmt.Errorf("creating %s: %w", keysDir, err)
	}
	if err := ringKeySyncDir(ringDir); err != nil {
		return false, fmt.Errorf("syncing %s after creating %s: %w", ringDir, keysDir, err)
	}
	temp, err := os.CreateTemp(keysDir, name+ringKeyTempFilePrefix+"*")
	if err != nil {
		return false, fmt.Errorf("creating a temporary file for %s in %s: %w", name, keysDir, err)
	}
	tempPath := temp.Name()
	if _, err := temp.Write(contents); err != nil {
		return false, errors.Join(fmt.Errorf("writing %s: %w", tempPath, err), temp.Close())
	}
	if err := temp.Sync(); err != nil {
		return false, errors.Join(fmt.Errorf("syncing %s: %w", tempPath, err), temp.Close())
	}
	if err := temp.Close(); err != nil {
		return false, fmt.Errorf("closing %s: %w", tempPath, err)
	}
	finalPath := filepath.Join(keysDir, name)
	if beforeRingKeyLink != nil {
		beforeRingKeyLink(finalPath)
	}
	linkErr := ringKeyLink(tempPath, finalPath)
	if err := os.Remove(tempPath); err != nil {
		return false, errors.Join(linkErr, fmt.Errorf("removing %s: %w", tempPath, err))
	}
	if errors.Is(linkErr, fs.ErrExist) {
		return false, nil
	}
	if linkErr != nil && linkRefusedByFilesystem(linkErr) {
		return false, fmt.Errorf("linking %s to %s: %w: the filesystem under %s refused the hard link that creating a "+
			"ring key needs (some FUSE, SMB/CIFS and network mounts do); put data_dir on a local filesystem that supports "+
			"hard links (ext4, xfs, btrfs, zfs) and start again; this build has no fallback", tempPath, finalPath, linkErr, ringDir)
	}
	if linkErr != nil {
		return false, fmt.Errorf("linking %s to %s: %w", tempPath, finalPath, linkErr)
	}
	if err := ringKeySyncDir(keysDir); err != nil {
		return false, fmt.Errorf("syncing %s after writing %s: %w", keysDir, name, err)
	}
	return true, nil
}
