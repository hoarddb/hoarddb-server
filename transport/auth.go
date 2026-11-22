package transport

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// KnownServersPath returns the default path for the known_servers file,
// the counterpart of SSH's known_hosts file.
func KnownServersPath(homeDir string) string {
	return filepath.Join(homeDir, ".hoarddb", "known_servers")
}

// AuthorizedKeysPath returns the path to the authorized_keys file for a
// given database.
func AuthorizedKeysPath(dataDir, dbName string) string {
	return filepath.Join(dataDir, "keys", dbName, "authorized_keys")
}

// AuthorizedKeys holds the set of public keys permitted to connect.
type AuthorizedKeys struct {
	keys map[[32]byte]ed25519.PublicKey // SHA256(pubkey) -> pubkey
	mu   sync.RWMutex
}

// NewAuthorizedKeys creates an empty AuthorizedKeys set.
func NewAuthorizedKeys() *AuthorizedKeys {
	return &AuthorizedKeys{keys: make(map[[32]byte]ed25519.PublicKey)}
}

// Add registers a public key.
func (ak *AuthorizedKeys) Add(pub ed25519.PublicKey) {
	ak.mu.Lock()
	defer ak.mu.Unlock()
	fp := sha256.Sum256(pub)
	ak.keys[fp] = pub
}

// IsAuthorized returns true if the public key is in the set.
func (ak *AuthorizedKeys) IsAuthorized(pub ed25519.PublicKey) bool {
	ak.mu.RLock()
	defer ak.mu.RUnlock()
	fp := sha256.Sum256(pub)
	_, ok := ak.keys[fp]
	return ok
}

// LoadFromFiles loads authorized keys from a file (one hex-encoded
// ed25519 public key per line).
func (ak *AuthorizedKeys) LoadFromFiles(paths ...string) error {
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read authorized_keys %s: %w", path, err)
		}
		lines := splitLines(string(data))
		for _, line := range lines {
			if len(line) == 0 || line[0] == '#' {
				continue
			}
			pubBytes, err := hex.DecodeString(line)
			if err != nil {
				return fmt.Errorf("parse authorized_keys line: %w", err)
			}
			if len(pubBytes) != ed25519.PublicKeySize {
				return fmt.Errorf("authorized_keys: key must be %d bytes, got %d", ed25519.PublicKeySize, len(pubBytes))
			}
			ak.Add(ed25519.PublicKey(pubBytes))
		}
	}
	return nil
}

// Count returns the number of authorized keys.
func (ak *AuthorizedKeys) Count() int {
	ak.mu.RLock()
	defer ak.mu.RUnlock()
	return len(ak.keys)
}

// Fingerprint returns the SHA256 fingerprint of an ed25519 public key.
func Fingerprint(pub ed25519.PublicKey) string {
	fp := sha256.Sum256(pub)
	return "SHA256:" + hex.EncodeToString(fp[:])
}

// ParseFingerprint validates and canonicalises a fingerprint string. The
// canonical form is "SHA256:" plus the 64 lowercase hex characters of the
// SHA-256 digest of the ed25519 public key, as produced by Fingerprint. The
// prefix and hex digits are accepted case-insensitively; anything else — a
// bare digest with no prefix, the retired alternate prefix, the wrong length,
// or junk — is rejected with the expected shape named.
//
// One format exists on purpose: a comparison against a live handshake that
// mixes formats fails every join, while loose normalisation (suffix matching,
// say) accepts unrelated keys.
func ParseFingerprint(s string) (string, error) {
	trimmed := strings.TrimSpace(s)
	prefix, hexPart, ok := strings.Cut(trimmed, ":")
	if !ok || !strings.EqualFold(prefix, "SHA256") {
		return "", fmt.Errorf("fingerprint %q is not in the expected SHA256:<64 hex chars> form", s)
	}
	hexPart = strings.TrimSpace(hexPart)
	if len(hexPart) != sha256.Size*2 {
		return "", fmt.Errorf("fingerprint %q has %d hex characters, want %d (SHA256:<64 hex chars>)", s, len(hexPart), sha256.Size*2)
	}
	raw, err := hex.DecodeString(hexPart)
	if err != nil {
		return "", fmt.Errorf("fingerprint %q is not valid hex: %w", s, err)
	}
	if len(raw) != sha256.Size {
		return "", fmt.Errorf("fingerprint %q decodes to %d bytes, want %d", s, len(raw), sha256.Size)
	}
	return "SHA256:" + strings.ToLower(hexPart), nil
}

// VerifyPeerCertFunc returns a tls.Config VerifyPeerCertificate callback
// that performs SSH-style pinned-key verification against an AuthorizedKeys
// set. This replaces default CA-chain verification.
func VerifyPeerCertFunc(authorized *AuthorizedKeys) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no client certificate presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse client cert: %w", err)
		}
		pub, ok := leaf.PublicKey.(ed25519.PublicKey)
		if !ok {
			return errors.New("client key is not ed25519")
		}
		if !authorized.IsAuthorized(pub) {
			return fmt.Errorf("client key not authorized (fingerprint: %s)", Fingerprint(pub))
		}
		return nil
	}
}

// ClientVerifyPeerCertFunc returns a tls.Config VerifyPeerCertificate callback
// that verifies the server's ed25519 public key against the known_servers store.
// If the server is unknown, promptFunc is called; if it returns true, the key
// is trusted.
func ClientVerifyPeerCertFunc(known *KnownServers, host string, promptFunc func(host, fingerprint string) bool) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no server certificate presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse server cert: %w", err)
		}
		pub, ok := leaf.PublicKey.(ed25519.PublicKey)
		if !ok {
			return errors.New("server key is not ed25519")
		}
		fp := Fingerprint(pub)

		canonical, err := CanonicalServerAddress(host)
		if err != nil {
			return fmt.Errorf("cannot check server %s against %s: %w", host, known.Path(), err)
		}
		match, recorded, err := known.Lookup(canonical, pub)
		if err != nil {
			return fmt.Errorf("cannot check server %s against %s: %w", host, known.Path(), err)
		}
		switch match {
		case KnownServerKnown:
			return nil
		case KnownServerChanged:
			return &ChangedServerKeyError{Dialled: host, Host: canonical, Path: known.Path(), Recorded: recorded, Presented: pub}
		}

		// Unknown server — prompt for trust (TOFU). Trust re-reads the file
		// under its lock, so a key another process recorded meanwhile comes
		// back as a *ChangedServerKeyError, which is returned as it is.
		if promptFunc != nil && promptFunc(host, fp) {
			if err := known.Trust(host, pub); err != nil {
				var changed *ChangedServerKeyError
				if errors.As(err, &changed) {
					return err
				}
				return fmt.Errorf("save trusted key: %w", err)
			}
			return nil
		}

		return fmt.Errorf("server %s not trusted (fingerprint: %s)", host, fp)
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
