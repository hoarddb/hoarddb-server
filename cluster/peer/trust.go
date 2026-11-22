package peer

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Trust is how a node proves itself to its peers and decides whom to believe: the
// node's own TLS certificate, and the set of ed25519 fingerprints it pins. A connection
// is accepted only when the other side presents a certificate in that set, in either
// direction (mutual pinned TLS), because the HDPX handshake carries no secret and an
// internode port open to a network must not take a stranger's frames (SPEC-GAP U57-3).
// There is no trust-on-first-use and no path that skips verification.
type Trust struct {
	server *tls.Config
	pins   *transport.FingerprintSet
	mu     sync.RWMutex
	set    map[string]struct{}
	// idPin is the id=fingerprint map a dial to a named member is held to; nil leaves a dial
	// to any pinned key (BindIDs).
	idPin func(id string) (string, bool)
}

// BindIDs makes a dial to a named member (ClientConfigFor) accept only that member's pinned
// fingerprint, so a pinned key of another member presented at its address is refused.
func (t *Trust) BindIDs(pinOf func(id string) (string, bool)) { t.idPin = pinOf }

// AddPin makes fingerprint fp acceptable at the TLS layer from now on. It widens only the
// inbound set (what Listener accepts): a dial still checks the pins NewTrust was given, or the
// member's pin under BindIDs. A pin proves a key,
// not an identity: whose key it is, is the node's business (see the node's peer admission).
func (t *Trust) AddPin(fp string) error {
	parsed, err := transport.ParseFingerprint(fp)
	if err != nil {
		return fmt.Errorf("pinning %q: %w", fp, err)
	}
	t.mu.Lock()
	t.set[parsed] = struct{}{}
	t.mu.Unlock()
	return nil
}

// NewTrust builds the trust of a node whose own certificate is in server (the config
// transport.MakeServerTLSConfig returns) and which pins the given fingerprints, in the
// forms seed_fingerprints accepts: a bare SHA256:<hex>, or addr=SHA256:<hex>.
func NewTrust(server *tls.Config, pins []string) (*Trust, error) {
	if server == nil || len(server.Certificates) == 0 {
		return nil, errors.New("pinned TLS needs the node's own certificate: load or create the node's key with transport.MakeServerTLSConfig first")
	}
	set, err := transport.NewFingerprintSet(pins, nil)
	if err != nil {
		return nil, fmt.Errorf("reading the pinned fingerprints: %w", err)
	}
	bare := make(map[string]struct{}, len(pins))
	for _, pin := range pins {
		_, value, _ := cutMapped(pin)
		fp, err := transport.ParseFingerprint(value)
		if err != nil {
			return nil, fmt.Errorf("reading the pinned fingerprints: %w", err)
		}
		bare[fp] = struct{}{}
	}
	return &Trust{server: server, pins: set, set: bare}, nil
}

func cutMapped(entry string) (host, value string, ok bool) {
	for i := range len(entry) {
		if entry[i] == '=' {
			return entry[:i], entry[i+1:], true
		}
	}
	return "", entry, false
}

// Listener wraps ln so every connection runs TLS 1.3 and must present a pinned client
// certificate.
func (t *Trust) Listener(ln net.Listener) net.Listener {
	conf := t.server.Clone()
	conf.ClientAuth = tls.RequireAnyClientCert
	conf.VerifyPeerCertificate = t.verifyClient
	return tls.NewListener(ln, conf)
}

func (t *Trust) verifyClient(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return errors.New("the peer presented no certificate; every internode connection must present its pinned certificate")
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("parsing the peer certificate: %w", err)
	}
	pub, ok := leaf.PublicKey.(ed25519.PublicKey)
	if !ok {
		return fmt.Errorf("the peer key is %T, not ed25519", leaf.PublicKey)
	}
	t.mu.RLock()
	_, pinned := t.set[transport.Fingerprint(pub)]
	t.mu.RUnlock()
	if !pinned {
		return fmt.Errorf("the peer presented %s, which is not in this node's pinned fingerprints; add it to seed_fingerprints", transport.Fingerprint(pub))
	}
	return nil
}

// ClientConfig is the TLS config for a dial to addr: it presents this node's certificate
// and accepts only a server whose fingerprint is pinned.
func (t *Trust) ClientConfig(addr string) (*tls.Config, error) {
	return &tls.Config{
		MinVersion: tls.VersionTLS13, NextProtos: []string{transport.ALPNFor(transport.ProtocolMajor)},
		Certificates: t.server.Certificates,
		// Go's chain check cannot verify a self-signed ed25519 certificate; the pinned
		// fingerprint check below replaces it and is always installed.
		InsecureSkipVerify:    true, //nolint:gosec // VerifyPeerCertificate pins the fingerprint
		VerifyPeerCertificate: t.pins.Verify(addr),
	}, nil
}

// ClientConfigFor is the TLS config for a dial to member id at addr. With BindIDs set and id
// known, the server must present exactly id's pinned fingerprint: a pinned key of another
// member at id's address is refused. An empty id (a dial that does not know who answers) is
// refused once ids are bound: it would accept any pinned key, which is the one hole the pinned
// model leaves. Without BindIDs it is held to the pinned set, as ClientConfig is.
func (t *Trust) ClientConfigFor(id, addr string) (*tls.Config, error) {
	if t.idPin == nil {
		return t.ClientConfig(addr)
	}
	if id == "" {
		return nil, fmt.Errorf("refused to build a TLS config for %s with no node id: a dial that names no peer would accept any pinned key. A join registers the address first", addr)
	}
	want, ok := t.idPin(id)
	if !ok {
		return nil, fmt.Errorf("no fingerprint is pinned for member %q, so a dial to %s cannot be verified. Add %s=SHA256:<hex> to seed_fingerprints", id, addr, id)
	}
	want, err := transport.ParseFingerprint(want)
	if err != nil {
		return nil, fmt.Errorf("the pinned fingerprint of member %q: %w", id, err)
	}
	conf, err := t.ClientConfig(addr)
	if err != nil {
		return nil, err
	}
	conf.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("no server certificate presented for member %q at %s", id, addr)
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parsing the certificate of member %q at %s: %w", id, addr, err)
		}
		pub, ok := leaf.PublicKey.(ed25519.PublicKey)
		if !ok {
			return fmt.Errorf("the key at %s is %T, not ed25519", addr, leaf.PublicKey)
		}
		if got := transport.Fingerprint(pub); got != want {
			return fmt.Errorf("%s presented %s, but member %q is pinned to %s: another member's key at this address is refused", addr, got, id, want)
		}
		return nil
	}
	return conf, nil
}
