package server

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// firstStartDataDirEntries are the only entries a data directory holds when
// the first start stopped before the TLS identity was complete: the files
// cmd/server writes before it (users.json, the cluster token, the signing
// key) and the keys directory. A store, a WAL, a membership record or anything
// else means the node got past its first start.
var firstStartDataDirEntries = map[string]bool{
	"keys":          true,
	"cluster.token": true,
	authKeyFile:     true,
	"users.json":    true,
}

// firstStartKeysDirEntries are the files a first start leaves in keys/.
var firstStartKeysDirEntries = map[string]bool{
	"server.key":       true,
	"server.crt":       true,
	NodeMasterFileName: true,
}

// recoverUnfinishedFirstStart handles the key pair a power cut in the first
// seconds of a node's first start leaves behind: both files present but empty
// or unparsable. Before the pair was written durably the files could reach the
// disk with length zero, and the node then never started again.
//
// The pair is removed, so the start that follows generates a new identity,
// only when no usable half remains and the data directory proves the first
// start never completed: it holds nothing but firstStartDataDirEntries and,
// in keys/, firstStartKeysDirEntries (temporary files of those included).
// Nothing in such a directory can have pinned the old fingerprint. In every
// other case the damaged files are left alone and the start is refused with
// the exact command to run.
func recoverUnfinishedFirstStart(dataDir string, logger *slog.Logger) error {
	keyPath := tlsKeyCredential.path(dataDir)
	certPath := filepath.Join(dataDir, "keys", "server.crt")
	keyProblem := damagedKeyFile(keyPath, func(contents []byte) error {
		if isSealedCredential(contents) {
			return nil
		}
		return tlsKeyCredential.checkPlaintext(contents)
	})
	certProblem := damagedKeyFile(certPath, func(contents []byte) error {
		block, _ := pem.Decode(contents)
		if block == nil || block.Type != "CERTIFICATE" {
			return errors.New("it is not a PEM certificate")
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return fmt.Errorf("it does not parse as a certificate: %w", err)
		}
		return nil
	})
	if keyProblem.err == nil && certProblem.err == nil {
		return nil
	}
	if keyProblem.err != nil && certProblem.err == nil && !certProblem.absent ||
		certProblem.err != nil && keyProblem.err == nil && !keyProblem.absent {
		return damagedKeysError(keyPath, certPath, keyProblem, certProblem,
			"The other file of the pair is intact, so this is not a first start that stopped early")
	}
	stray, err := firstStartStrayEntries(dataDir)
	if err != nil {
		return err
	}
	if len(stray) > 0 {
		return damagedKeysError(keyPath, certPath, keyProblem, certProblem,
			"The data directory also holds "+strings.Join(stray, ", ")+", so this node got past its first start and may have a fingerprint others pinned")
	}
	for _, path := range []string{keyPath, certPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("removing %s so this first start can be made again: %w", path, err)
		}
	}
	logger.Warn("the TLS key pair was empty or unreadable and nothing else was in the data directory: the first start had not finished (a power loss or crash). A new identity is generated",
		"key", keyPath, "cert", certPath)
	return nil
}

// keyFileProblem is what is wrong with one file of the pair.
type keyFileProblem struct {
	absent bool
	err    error
}

// damagedKeyFile reports why the file at path is unusable. An absent file is a
// problem with absent set; a usable file has a nil err and absent false.
func damagedKeyFile(path string, check func([]byte) error) keyFileProblem {
	contents, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return keyFileProblem{absent: true}
	case err != nil:
		return keyFileProblem{err: fmt.Errorf("it cannot be read: %w", err)}
	case len(contents) == 0:
		return keyFileProblem{err: errors.New("it is empty (0 bytes)")}
	}
	return keyFileProblem{err: check(contents)}
}

// firstStartStrayEntries names what the data directory holds beyond the
// entries a never-completed first start leaves.
func firstStartStrayEntries(dataDir string) ([]string, error) {
	var stray []string
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("deciding whether the empty TLS key files are an unfinished first start: reading %s: %w", dataDir, err)
	}
	for _, entry := range entries {
		if !firstStartDataDirEntries[strings.TrimSuffix(entry.Name(), ".tmp")] && !strings.HasPrefix(entry.Name(), ".users-") &&
			!strings.HasPrefix(entry.Name(), "users.json.") {
			stray = append(stray, entry.Name())
		}
	}
	keyEntries, err := os.ReadDir(filepath.Join(dataDir, "keys"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("deciding whether the empty TLS key files are an unfinished first start: reading the keys directory: %w", err)
	}
	for _, entry := range keyEntries {
		if !firstStartKeysDirEntries[strings.TrimSuffix(entry.Name(), ".tmp")] {
			stray = append(stray, "keys/"+entry.Name())
		}
	}
	return stray, nil
}

// damagedKeysError is the refusal: it names both files, what is wrong with
// each, why nothing was replaced, and the two ways out.
func damagedKeysError(keyPath, certPath string, keyProblem, certProblem keyFileProblem, reason string) error {
	describe := func(path string, problem keyFileProblem) string {
		switch {
		case problem.absent:
			return path + " is missing"
		case problem.err != nil:
			return path + ": " + problem.err.Error()
		}
		return path + " is intact"
	}
	return fmt.Errorf("the TLS key pair cannot be loaded. %s; %s. A power loss or crash during a node's first start leaves files like this. %s, so hoardDB does not replace it. "+
		"If this node's fingerprint was never given to any node or client, remove %s and %s and restart to generate a new identity. "+
		"Otherwise restore both files from a backup of this data directory",
		describe(keyPath, keyProblem), describe(certPath, certProblem), reason, keyPath, certPath)
}
