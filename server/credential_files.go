package server

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The node's credential files: the standalone signing key, the cluster token
// and the TLS private key. With encryption at rest on, each is sealed under
// the node master in the same envelope users.json and the store key use
// (at-rest-encryption spec §2.3), so a copy of the data directory alone does
// not hold them in a usable form: the node master is wrapped under a key that
// is not in the directory.
//
// They are four node-level files read at start (and the token again when a
// rotation changes it), so sealing them adds no per-bucket cost.

// clusterTokenFileName is the cluster token's file under the data directory.
const clusterTokenFileName = "cluster.token"

// credentialFile names one credential file and the envelope that seals it.
type credentialFile struct {
	// relPath is the file under the data directory, slash-separated.
	relPath string
	// purpose and identity derive the file's sealing key from the node
	// master (spec §2.5); each file has its own purpose byte.
	purpose  byte
	identity string
	// restore is the way out a refusal about this file names.
	restore string
	// alternativeRoute is a second way out, for a file the node can start
	// without. It is empty when restore is the only fix.
	alternativeRoute string
	// checkPlaintext reports whether contents are this file's plaintext
	// form, so that only a real plaintext file is sealed in place.
	checkPlaintext func(contents []byte) error
	// movedAside is the glob suffix of the copies a node keeps when it moves
	// the file aside (".wrong-*" for the pepper), or empty. Those copies are
	// not credential files and are not sealed in place; a sealing start names
	// the ones still in the clear.
	movedAside string
}

// clusterTokenSource is where an operator finds the token a cluster runs on. A start prints
// no token and reads no environment variable for it, so the only copy a person can read is the
// "token" field of cluster.token in another member's data directory, and only where that member
// does not seal its credential files.
const clusterTokenSource = "the \"token\" field of cluster.token in another member's " +
	"data directory (readable there only when that member runs without encryption at rest)"

// clusterTokenMoveAside is the way out of every refusal about cluster.token. This release has no
// cluster token: members trust each other by pinned keys, a first start writes no cluster.token,
// and the old replicated mode that used one is refused before the data directory is touched. No
// member holds a token to put in the place of a cluster.token a start stops on, so the only fix
// an operator can follow is to move the file aside.
const clusterTokenMoveAside = "move cluster.token aside (rename it to cluster.token.old in the same directory) and start again: " +
	"this release has no cluster token, a first start writes no cluster.token and nothing uses the token in one, " +
	"so nothing is lost without it"

var (
	authKeyCredential = credentialFile{
		relPath: authKeyFile, purpose: sealed.PurposeAuthKey, identity: "auth-key",
		restore: "Restore auth.token.key from a backup of this data directory; without it the users and login tokens it signed cannot be used",
		checkPlaintext: func(contents []byte) error {
			if len(contents) != authKeyLen {
				return fmt.Errorf("it is %d bytes, and a signing key is %d", len(contents), authKeyLen)
			}
			return nil
		},
	}
	clusterTokenCredential = credentialFile{
		relPath: clusterTokenFileName, purpose: sealed.PurposeClusterToken, identity: "cluster-token",
		restore:          "To get past this, " + clusterTokenMoveAside,
		alternativeRoute: clusterTokenMoveAside,
		checkPlaintext: func(contents []byte) error {
			var token ClusterToken
			if err := json.Unmarshal(contents, &token); err != nil {
				return fmt.Errorf("it does not parse as a token file: %w", err)
			}
			if token.Token == "" {
				return errors.New("it holds no token")
			}
			return nil
		},
	}
	tlsKeyCredential = credentialFile{
		relPath: "keys/server.key", purpose: sealed.PurposeTLSKey, identity: "tls-key",
		restore: "Restore keys/server.key from a backup of this data directory, or move keys/server.key and keys/server.crt aside to generate a new identity and give its fingerprint to every node and client that pinned the old one",
		checkPlaintext: func(contents []byte) error {
			if block, _ := pem.Decode(contents); block == nil || block.Type != "PRIVATE KEY" {
				return errors.New("it is not a PEM private key")
			}
			return nil
		},
	}

	// clusterPepperCredential is the ring cluster's password key (ring user store spec 5.4): a
	// copy every data member of a cluster holds, fetched from another member when it is missing.
	clusterPepperCredential = credentialFile{
		relPath: clusterpepper.FileName, purpose: sealed.PurposeClusterPepper, identity: "cluster-pepper",
		movedAside: ".wrong-*",
		restore: "Move auth.cluster.pepper aside and start the node: it asks another member for the cluster's pepper. " +
			"If no data node holds it any more, restore auth.cluster.pepper from a backup of this node's own data directory: " +
			"a sealed copy opens only on the node that sealed it, so another node's backup does not work. " +
			"Without either, the cluster's passwords cannot be checked",
		checkPlaintext: func(contents []byte) error {
			_, err := clusterpepper.DecodeFile(contents)
			return err
		},
	}

	// credentialFiles is every credential file, in the order a start seals them.
	credentialFiles = []credentialFile{authKeyCredential, clusterTokenCredential, tlsKeyCredential, clusterPepperCredential}
)

// path is the file's location under dataDir.
func (f credentialFile) path(dataDir string) string {
	return filepath.Join(dataDir, filepath.FromSlash(f.relPath))
}

// CredentialKeys is how a node reads and writes its credential files. A file
// is read in whichever form it is in: plaintext as it is, sealed under the
// node master. A file is written sealed only when the node seals its
// credentials, which is when encryption at rest is on.
//
// The zero value has no node master: it reads and writes plaintext and
// refuses a sealed file, naming the settings that open it.
type CredentialKeys struct {
	nodeMaster *NodeMaster
	seal       bool
}

// Sealed reports whether the node seals its credential files.
func (k CredentialKeys) Sealed() bool {
	return k.seal
}

// writeCredentialFile replaces a credential file by temporary file, fsync,
// rename and directory sync. A test replaces it to stop a write between the
// temporary and the rename, as a crash would.
var writeCredentialFile = storage.WriteFileDurably

// credentialKeysFor decides how cfg's start keeps its credential files. They
// are sealed when the mode seals the store and a node master is loaded,
// unless the data directory holds a store this start will refuse to open
// (a store with no wrapped store key: plaintext, or sealed with its key file
// lost). Sealing beside such a store would leave files that the start the
// operator falls back to cannot read.
//
// With a node master loaded but a mode that does not seal (an offline command
// given the keystore and not the mode), the files are sealed whenever the
// directory already holds sealed material: a node master never writes a
// credential back in the clear.
func credentialKeysFor(cfg *config.Config, nm *NodeMaster) (CredentialKeys, error) {
	if nm == nil {
		return CredentialKeys{}, nil
	}
	if !modeSealsStore(cfg.Encryption.Mode) {
		found, err := findSealedMaterial(cfg.Storage.DataDir)
		if err != nil {
			return CredentialKeys{}, fmt.Errorf("checking %s for sealed material before writing credential files: %w", cfg.Storage.DataDir, adviseRingMember(err, cfg.Cluster.Enabled))
		}
		return CredentialKeys{nodeMaster: nm, seal: len(found) > 0}, nil
	}
	refused, err := storeRefusesSealing(cfg.Storage.DataDir)
	if err != nil {
		return CredentialKeys{}, err
	}
	return CredentialKeys{nodeMaster: nm, seal: !refused}, nil
}

// storeRefusesSealing reports whether dataDir holds a node store with no
// wrapped store key beside it, which a sealing start refuses
// (nodeStoreDataKey).
func storeRefusesSealing(dataDir string) (bool, error) {
	manifestPath := filepath.Join(dataDir, nodeStoreDirName, "MANIFEST")
	if _, err := os.Stat(manifestPath); errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("checking the node store before sealing the credential files: %w", err)
	}
	storeKeyPath := filepath.Join(dataDir, "keys", nodeStoreKeyFileName)
	if _, err := os.Stat(storeKeyPath); errors.Is(err, fs.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("checking the node store's key before sealing the credential files: %w", err)
	}
	return false, nil
}

// OpenCredentialKeys opens what the offline commands need to read and write
// cfg's credential files (reset-password): the
// keystore cfg names and the node master already in the data directory. It
// never creates a node master. With no keystore, or no node master yet, the
// files are plaintext, and a sealed one is refused by its reader with the fix.
func OpenCredentialKeys(cfg *config.Config) (CredentialKeys, error) {
	ks, err := OpenKeystore(cfg)
	if err != nil {
		return CredentialKeys{}, fmt.Errorf("opening the keystore to read the credential files in %s: %w", cfg.Storage.DataDir, err)
	}
	if ks == nil {
		return CredentialKeys{}, nil
	}
	nm, err := loadNodeMaster(cfg.Storage.DataDir, ks)
	if errors.Is(err, errNoNodeMaster) {
		return CredentialKeys{}, nil
	}
	if err != nil {
		return CredentialKeys{}, fmt.Errorf("loading the node master to read the credential files in %s: %w", cfg.Storage.DataDir, err)
	}
	return credentialKeysFor(cfg, nm)
}

// isSealedCredential reports whether contents start with a whole-file sealed
// envelope. No plaintext credential file can: a signing key is 32 bytes,
// shorter than the header; a token file starts with '{'; a PEM key with '-';
// the cluster pepper with its magic, 'H'.
func isSealedCredential(contents []byte) bool {
	return len(contents) >= sealed.EnvelopeHeaderLen &&
		binary.BigEndian.Uint32(contents[0:4]) == sealed.Magic && contents[4] == sealed.Version
}

// readCredential returns file's plaintext and whether it was sealed on disk. A
// missing file is an error that matches fs.ErrNotExist.
func (k CredentialKeys) readCredential(dataDir string, file credentialFile) ([]byte, bool, error) {
	path := file.path(dataDir)
	contents, err := os.ReadFile(path)
	if err != nil {
		// The *fs.PathError already names the operation and the file; every
		// caller adds which credential it was reading.
		return nil, false, err
	}
	if !isSealedCredential(contents) {
		return contents, false, nil
	}
	if k.nodeMaster == nil {
		alternative := ""
		if file.alternativeRoute != "" {
			alternative = fmt.Sprintf(" If this member cannot get its settings back: %s.", file.alternativeRoute)
		}
		return nil, true, &sealedCredentialError{err: fmt.Errorf("%s is sealed under a node master, but no keystore is configured for this command or start. "+
			"Run it with the encryption settings this data directory was created with: encryption.keystore and its key, and encryption.mode on or require "+
			"(HOARDDB_ENCRYPTION_KEYSTORE, HOARDDB_ENCRYPTION_KEY_FILE, HOARDDB_ENCRYPTION_MODE, or the same configuration file).%s", path, alternative)}
	}
	plaintext, err := openSealedCredential(file, k.nodeMaster.Bytes(), contents)
	if err != nil {
		return nil, true, &sealedCredentialError{err: credentialOpenError(path, file, contents, k.nodeMaster, err)}
	}
	return plaintext, true, nil
}

// sealedCredentialError is a sealed credential file this start cannot open.
// Its text already names the file and the fix, so a caller adds no advice of
// its own.
type sealedCredentialError struct {
	err error
}

func (e *sealedCredentialError) Error() string { return e.err.Error() }

func (e *sealedCredentialError) Unwrap() error { return e.err }

// openSealedCredential opens a sealed credential file's contents under master.
func openSealedCredential(file credentialFile, master, contents []byte) ([]byte, error) {
	plaintext, _, err := sealed.Open(file.purpose, master, func(gen uint64) string {
		return fmt.Sprintf("%s/%d", file.identity, gen)
	}, contents)
	return plaintext, err
}

// credentialOpenError is the refusal of a sealed credential file the node
// master does not open: sealed by another node, or damaged.
func credentialOpenError(path string, file credentialFile, contents []byte, nm *NodeMaster, cause error) error {
	diskKid := getHeaderKid(contents)
	if diskKid == nm.Kid() {
		return fmt.Errorf("cannot open %s: it was sealed under this node's node master (kid %s) but does not authenticate, so it is damaged or was altered. %s. Nothing was changed: %w",
			path, diskKid, file.restore, cause)
	}
	return fmt.Errorf("cannot open %s: it was sealed under node master kid %s, and this node's node master is kid %s. "+
		"A sealed credential file belongs to the node that wrote it and cannot be copied to another node. %s. Nothing was changed: %w",
		path, diskKid, nm.Kid(), file.restore, cause)
}

// writeCredential replaces file with plaintext, sealed under the node master
// when the node seals its credentials. A sealed file is written at the
// generation after the one it replaces, the rule users.json follows (spec
// §2.3), so no two seals of one file share a derived key.
func (k CredentialKeys) writeCredential(dataDir string, file credentialFile, plaintext []byte) error {
	path := file.path(dataDir)
	contents := plaintext
	if k.seal {
		existing, err := readFilePrefix(path, sealed.EnvelopeHeaderLen)
		if err != nil {
			return fmt.Errorf("reading %s before replacing it: %w", path, err)
		}
		gen, err := usersFileGen(existing)
		if err != nil {
			return fmt.Errorf("refusing to replace %s: %w", path, err)
		}
		contents, err = sealed.Seal(file.purpose, k.nodeMaster.Bytes(), file.identity, gen, plaintext)
		if err != nil {
			return fmt.Errorf("sealing %s: %w", path, err)
		}
	}
	if err := writeCredentialFile(path, contents); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

// sealCredentialFiles is a sealing start's pass over the credential files,
// before anything reads them. A plaintext file, left by a build that did not
// seal it, is sealed in place under the same name. The rename that replaces
// it is atomic, so at every instant the file exists, plaintext or sealed, and
// a start stopped part-way leaves a file the next start reads and finishes.
// A temporary an interrupted write left beside a file is removed: it is never
// needed, because the file it was replacing is still whole, and it may hold
// the credential in the clear.
//
// The plaintext's old disk blocks are freed, not overwritten; only disk
// encryption covers freed blocks (the security page of the user docs says so).
func (k CredentialKeys) sealCredentialFiles(dataDir string, logger *slog.Logger) error {
	if !k.seal {
		return nil
	}
	for _, file := range credentialFiles {
		path := file.path(dataDir)
		plaintext, sealedOnDisk, err := k.readCredential(dataDir, file)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return err
		case !sealedOnDisk:
			if err := file.checkPlaintext(plaintext); err != nil {
				return fmt.Errorf("%s is neither sealed nor a plaintext %s: %w. It may be a sealed file whose header was damaged. %s. Nothing was changed",
					path, file.relPath, err, file.restore)
			}
			if err := k.writeCredential(dataDir, file, plaintext); err != nil {
				return fmt.Errorf("sealing the credential file %s under the node master: %w", path, err)
			}
			logger.Info("sealed a credential file under the node master", "path", path)
		}
		temporaryPath := path + ".tmp"
		if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("removing %s, which an interrupted write left: %w", temporaryPath, err)
		}
		if err := logPlaintextMovedAside(path, file, logger); err != nil {
			return err
		}
	}
	return nil
}

// logPlaintextMovedAside logs one line naming the moved-aside copies of file that are in the
// clear: a copy moved aside while the node did not seal is plaintext, and a sealing start leaves it
// as it is (one moved aside by a sealing node is the sealed file renamed). The operator deletes it
// once it is not needed, or keeps it on an encrypted disk.
func logPlaintextMovedAside(path string, file credentialFile, logger *slog.Logger) error {
	if file.movedAside == "" {
		return nil
	}
	copies, err := filepath.Glob(path + file.movedAside)
	if err != nil {
		return fmt.Errorf("listing the moved-aside copies of %s: %w", path, err)
	}
	var plaintext []string
	for _, copyPath := range copies {
		header, err := readFilePrefix(copyPath, sealed.EnvelopeHeaderLen)
		if err != nil {
			return fmt.Errorf("reading %s to tell whether it is sealed: %w", copyPath, err)
		}
		if !isSealedCredential(header) {
			plaintext = append(plaintext, copyPath)
		}
	}
	if len(plaintext) > 0 {
		logger.Warn("moved-aside copies of a credential file are kept in the clear: they are not sealed in place; delete them once they are not needed",
			"credential", file.relPath, "paths", plaintext)
	}
	return nil
}

// loadNodeTLS loads the node's TLS identity from the keys directory under
// dataDir, generating one on a first start. A sealing node writes the
// certificate first and the sealed key second: a start stopped between the
// two finds no key and generates both again, while a sealed key with no
// certificate beside it is refused rather than replaced, because replacing it
// changes the fingerprint every peer and client pinned.
func (k CredentialKeys) loadNodeTLS(dataDir string) (*tls.Config, string, error) {
	keyDir := filepath.Join(dataDir, "keys")
	certPath := filepath.Join(keyDir, "server.crt")
	keyPEM, keySealed, keyErr := k.readCredential(dataDir, tlsKeyCredential)
	if keyErr != nil && !errors.Is(keyErr, fs.ErrNotExist) {
		return nil, "", keyErr
	}
	certPEM, certErr := os.ReadFile(certPath)
	if certErr != nil && !errors.Is(certErr, fs.ErrNotExist) {
		return nil, "", fmt.Errorf("reading %s: %w", certPath, certErr)
	}
	switch {
	case keyErr == nil && certErr == nil:
		return transport.ServerTLSConfigFromPEM(certPEM, keyPEM)
	case !k.seal && !keySealed:
		// A node that does not seal its credentials generates and loads them
		// as it always has.
		return transport.MakeServerTLSConfig(keyDir)
	case keyErr == nil:
		return nil, "", fmt.Errorf("%s is missing beside the sealed TLS key %s, and hoardDB does not replace a sealed key. "+
			"Restore keys/server.crt from a backup of this data directory, or move keys/server.key aside to generate a new identity and give its fingerprint to every node and client that pinned the old one",
			certPath, tlsKeyCredential.path(dataDir))
	}
	identity, err := transport.NewEd25519Identity()
	if err != nil {
		return nil, "", fmt.Errorf("generating the node's TLS identity: %w", err)
	}
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return nil, "", fmt.Errorf("creating %s: %w", keyDir, err)
	}
	if err := writeCredentialFile(certPath, identity.CertPEM); err != nil {
		return nil, "", fmt.Errorf("writing %s: %w", certPath, err)
	}
	if err := k.writeCredential(dataDir, tlsKeyCredential, identity.KeyPEM); err != nil {
		return nil, "", err
	}
	return transport.ServerTLSConfigFromPEM(identity.CertPEM, identity.KeyPEM)
}

// findSealedCredentials names the credential files in dataDir that are sealed,
// for findSealedMaterial: each is evidence that a node master existed.
func findSealedCredentials(dataDir string) ([]string, error) {
	var found []string
	for _, file := range credentialFiles {
		header, err := readFilePrefix(file.path(dataDir), sealed.EnvelopeHeaderLen)
		if err != nil {
			return nil, fmt.Errorf("checking %s for sealed data: %w", file.relPath, err)
		}
		if isSealedCredential(header) && header[5] == file.purpose {
			found = append(found, file.relPath+" (sealed under the node master)")
		}
	}
	return found, nil
}

// verifyImportedMasterOpensCredentials refuses an imported node master that
// does not open a sealed credential file in dataDir.
func verifyImportedMasterOpensCredentials(dataDir string, master []byte) error {
	for _, file := range credentialFiles {
		path := file.path(dataDir)
		contents, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("reading %s to check the imported node master against it: %w", path, err)
		}
		if !isSealedCredential(contents) {
			continue
		}
		if _, err := openSealedCredential(file, master, contents); err != nil {
			return wrongImportedMasterError(master, path, contents, err)
		}
	}
	return nil
}
