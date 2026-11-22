package transport

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

// FingerprintSet is the trust anchor for non-interactive callers — internode
// peers, tooling and tests. There is no prompt and no trust-on-first-use: an
// unknown fingerprint is a failure, because internode has no human to ask and
// the first join is exactly the window being closed.
//
// The configured list accepts two forms:
//
//   - a bare canonical fingerprint (SHA256:<64 hex>), trusted for any host;
//   - a mapped host:port=SHA256:<hex> entry, trusted only for that address.
//
// Mapped keys are registered under both the configured address and its derived
// internode address, so a value written the way seed_nodes is written still
// matches the address a dial actually uses. Nothing is paired positionally:
// bare entries are a set and mapped entries are a map, so an unreachable or
// reordered seed cannot silently retarget the trust.
type FingerprintSet struct {
	raw      []string
	expected map[string]struct{}
	mapped   map[string]string
	learned  *KnownServers

	// loggedMu guards logged, the set of hosts whose verification has already
	// been reported. One WARN per peer, not per handshake: a pooled client
	// handshakes once per socket and redials on every error, so per-handshake
	// logging turns a busy cluster's log into a flood and adds the I/O that
	// made this fix's tests flaky before it was bounded.
	loggedMu sync.Mutex
	logged   map[string]struct{}
}

// NewFingerprintSet parses the configured expected fingerprints and returns a
// set that verifies live handshakes against them. learned, when non-nil, is
// the store peers verified previously are recorded in, so a later dial can
// succeed without the operator's list.
func NewFingerprintSet(expected []string, learned *KnownServers) (*FingerprintSet, error) {
	fs := &FingerprintSet{
		raw:      append([]string(nil), expected...),
		expected: make(map[string]struct{}),
		mapped:   make(map[string]string),
		learned:  learned,
		logged:   make(map[string]struct{}),
	}
	for _, entry := range expected {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if host, value, ok := strings.Cut(entry, "="); ok {
			host = strings.TrimSpace(host)
			if host == "" {
				return nil, fmt.Errorf("seed fingerprint %q has an empty address", entry)
			}
			fp, err := ParseFingerprint(value)
			if err != nil {
				return nil, fmt.Errorf("seed fingerprint for %s: %w", host, err)
			}
			fs.mapped[host] = fp
			// The configured address is usually the ring address; the dial
			// uses the internode address. Register both so either matches.
			if internode, err := InternodePeerAddr(host); err == nil {
				fs.mapped[internode] = fp
			}
			continue
		}
		fp, err := ParseFingerprint(entry)
		if err != nil {
			return nil, fmt.Errorf("seed fingerprint: %w", err)
		}
		fs.expected[fp] = struct{}{}
	}
	return fs, nil
}

// Verify returns the tls.Config VerifyPeerCertificate callback for a dial to
// host. The callback accepts a certificate when its fingerprint is in the
// configured set (learning it for later dials) or already in the learned
// store; a changed key for a known host is a hard failure, and an unknown one
// is refused — never trusted on first sight.
func (fs *FingerprintSet) Verify(host string) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("no server certificate presented for %s", host)
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse server certificate for %s: %w", host, err)
		}
		pub, ok := leaf.PublicKey.(ed25519.PublicKey)
		if !ok {
			return fmt.Errorf("server %s key is not ed25519 (type: %T)", host, leaf.PublicKey)
		}
		fp := Fingerprint(pub)

		if _, ok := fs.expected[fp]; ok {
			fs.learn(host, pub)
			fs.logVerified(host, fp, "configured seed fingerprint list")
			return nil
		}
		if want, ok := fs.mapped[host]; ok {
			if want != fp {
				return fmt.Errorf("server %s presented %s, which does not match the configured seed fingerprint %s", host, fp, want)
			}
			fs.learn(host, pub)
			fs.logVerified(host, fp, "configured seed fingerprint list")
			return nil
		}
		if fs.learned != nil {
			if fs.learned.IsKnown(host, pub) {
				fs.logVerified(host, fp, "previously learned fingerprint")
				return nil
			}
			if knownAny, existingFp := fs.learned.IsKnownFingerprint(host); knownAny {
				return fmt.Errorf(
					"WARNING: server key for %s has changed!\n"+
						"  old fingerprint: %s\n"+
						"  new fingerprint: %s\n"+
						"  This could indicate a man-in-the-middle attack.\n"+
						"  If the peer's certificate rotated legitimately, update seed_fingerprints,\n"+
						"  or delete the line for %s from %s deliberately",
					host, existingFp, fp, host, fs.learned.Path(),
				)
			}
		}
		return fmt.Errorf("internode peer %s is not pinned (presented fingerprint %s); add it to seed_fingerprints or treat the peer as untrusted", host, fp)
	}
}

// ClientConfig builds the pinned client TLS config for a dial to host. It is
// the only way internode code should build a client config: the pinned
// verifier is always installed, so there is no path that accepts anything.
func (fs *FingerprintSet) ClientConfig(host string) (*tls.Config, error) {
	return MakeClientTLSConfig("", "", TrustPolicy{Expected: fs.raw, Store: fs.learned}, host)
}

// learn records a verified peer so a later dial does not need the operator's
// list. An already-recorded value is left alone: rewriting the file on every
// handshake is pure churn, and the connection that verified has not changed.
// A failed write is logged, not fatal: the connection that just verified is
// still authentic, and the next dial simply needs the configured anchor again.
//
// A different key already learned for the host is replaced: the key was
// verified against the operator's configured list, and updating that list is
// the documented way to rotate a peer's certificate (the "configuration" user-docs page).
func (fs *FingerprintSet) learn(host string, pub ed25519.PublicKey) {
	if fs.learned == nil {
		return
	}
	match, _, err := fs.learned.Lookup(host, pub)
	if err != nil {
		slog.Warn("failed to record learned peer fingerprint", "host", host, "error", err)
		return
	}
	switch match {
	case KnownServerKnown:
		return
	case KnownServerChanged:
		err = fs.learned.Replace(host, pub)
	default:
		err = fs.learned.Trust(host, pub)
	}
	if err != nil {
		slog.Warn("failed to record learned peer fingerprint", "host", host, "error", err)
	}
}

// logVerified reports a peer's verification once per host, naming whether the
// trust came from the configured list or from a previous connection.
func (fs *FingerprintSet) logVerified(host, fingerprint, source string) {
	fs.loggedMu.Lock()
	defer fs.loggedMu.Unlock()
	if _, ok := fs.logged[host]; ok {
		return
	}
	fs.logged[host] = struct{}{}
	slog.Warn("internode peer verified", "host", host, "fingerprint", fingerprint, "trust", source)
}
