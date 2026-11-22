package transport

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
)

// LoadServerTLSConfigFromPaths creates a tls.Config from key/cert file paths
// and returns the server's ed25519 fingerprint.
func LoadServerTLSConfigFromPaths(paths KeyPaths) (*tls.Config, string, error) {
	certPEM, err := os.ReadFile(paths.Cert)
	if err != nil {
		return nil, "", fmt.Errorf("load key pair: %w", err)
	}
	keyPEM, err := os.ReadFile(paths.Key)
	if err != nil {
		return nil, "", fmt.Errorf("load key pair: %w", err)
	}
	return ServerTLSConfigFromPEM(certPEM, keyPEM)
}

// ServerTLSConfigFromPEM creates a server tls.Config from a certificate and
// private key held in memory, and returns the server's ed25519 fingerprint. A
// node whose key file is sealed at rest opens it and passes the PEM here.
func ServerTLSConfigFromPEM(certPEM, keyPEM []byte) (*tls.Config, string, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, "", fmt.Errorf("load key pair: %w", err)
	}
	pub, err := ParseEd25519PublicKey(cert)
	if err != nil {
		return nil, "", fmt.Errorf("extract public key: %w", err)
	}
	fp := Fingerprint(pub)

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{ALPNFor(ProtocolMajor)},
	}
	return cfg, fp, nil
}

// MakeServerTLSConfig generates or loads a TLS config for the server.
func MakeServerTLSConfig(keyDir string) (*tls.Config, string, error) {
	paths, _, err := LoadOrCreateKeys(keyDir)
	if err != nil {
		return nil, "", fmt.Errorf("load/create keys: %w", err)
	}
	return LoadServerTLSConfigFromPaths(paths)
}

// TrustPolicy is how a client decides a server is who it claims. It has no
// zero value that silently skips verification: a caller must choose exactly
// one of a known-servers store (with an optional prompt), a pinned fingerprint
// set, or the explicit InsecureTrust escape hatch. An empty policy is an
// error, so "no trust policy" can no longer be expressed by forgetting a
// parameter — the mechanism that let six call sites ship unverified.
//
// Store and Prompt together are the CLI's trust-on-first-use path: an unknown
// server is not trusted silently, and a non-interactive session refuses rather
// than guessing. Store and Expected together are the internode path: the
// configured fingerprints are the anchor, and a peer verified once is learned
// into Store so later dials do not need the list.
//
// InsecureTrust is a named, logged, deliberate opt-out. It is never a default.
type TrustPolicy struct {
	Store    *KnownServers
	Expected []string
	Prompt   func(host, fingerprint string) bool
	Insecure *InsecureTrust
}

// InsecureTrust skips certificate verification on purpose. Reason is required
// and is logged at WARN on every connection attempt, so an operator can answer
// "is anything of mine unverified?" from the log without reading the code.
type InsecureTrust struct {
	Reason string
}

// MakeClientTLSConfig creates a client TLS config under the given trust
// policy. hoardDB uses self-signed ed25519 certificates rather than a CA
// chain, so Go's default chain verification is always disabled and must be
// replaced by a real VerifyPeerCertificate callback — installed from the
// policy, or deliberately skipped by InsecureTrust and nothing else.
//
// A policy naming neither a store, pinned fingerprints, nor InsecureTrust
// returns an error rather than a config that trusts anything.
func MakeClientTLSConfig(
	certPath, keyPath string,
	trust TrustPolicy,
	host string,
) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{ALPNFor(ProtocolMajor)},
	}

	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	// The explicit opt-out is checked first: its reason must be present, and it
	// is logged here so every connection attempt leaves a trace.
	if trust.Insecure != nil {
		if trust.Insecure.Reason == "" {
			return nil, fmt.Errorf("insecure TLS trust policy for %s requires a reason", host)
		}
		slog.Warn("TLS certificate verification disabled by an explicit insecure trust policy",
			"host", host, "reason", trust.Insecure.Reason)
		cfg.InsecureSkipVerify = true //nolint:gosec // explicit, reasoned, logged opt-out
		return cfg, nil
	}

	cfg.InsecureSkipVerify = true //nolint:gosec // self-signed ed25519 certs need pinned-fingerprint verification, installed below

	switch {
	case len(trust.Expected) > 0:
		fingerprints, err := NewFingerprintSet(trust.Expected, trust.Store)
		if err != nil {
			return nil, fmt.Errorf("pinned trust policy for %s: %w", host, err)
		}
		cfg.VerifyPeerCertificate = fingerprints.Verify(host)
	case trust.Store != nil:
		cfg.VerifyPeerCertificate = ClientVerifyPeerCertFunc(trust.Store, host, trust.Prompt)
	default:
		return nil, fmt.Errorf("TLS trust policy for %s is empty: pass a known-servers store, pinned fingerprints, or an explicit InsecureTrust{Reason}", host)
	}

	return cfg, nil
}

// ParseEd25519PublicKey extracts the ed25519 public key from a TLS certificate.
func ParseEd25519PublicKey(cert tls.Certificate) (ed25519.PublicKey, error) {
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("no certificates in chain")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}
	pub, ok := leaf.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("certificate key is not ed25519 (type: %T)", leaf.PublicKey)
	}
	return pub, nil
}
