package transport

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
)

type KeyPaths struct {
	Key  string
	Cert string
}

// NodeIdentity is a node's TLS identity held in memory: its private key as
// PKCS#8 PEM, its self-signed certificate as PEM, and the certificate's
// fingerprint. A caller that seals the key at rest writes it itself, so the
// key never touches the disk in the clear.
type NodeIdentity struct {
	KeyPEM      []byte
	CertPEM     []byte
	Fingerprint string
}

// NewEd25519Identity generates a node identity without writing anything.
func NewEd25519Identity() (NodeIdentity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("generate ed25519 key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("generate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"hoardDB"}, CommonName: "hoarddb-node"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("create certificate: %w", err)
	}
	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("marshal private key: %w", err)
	}
	return NodeIdentity{
		KeyPEM:      pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}),
		CertPEM:     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		Fingerprint: Fingerprint(pub),
	}, nil
}

// writeKeyFileDurably is the shared durable write (temporary file, fsync,
// rename, directory fsync) that key generation writes both files through. A
// test replaces it to record the writes: a power cut is not available to a test.
var writeKeyFileDurably = storage.WriteFileDurably

// GenerateEd25519Keys writes a new identity to dir. Both files are owner-only
// (0600): the server restricts its umask to 077, so its certificate was never
// group- or world-readable in practice, and this makes a library caller match.
func GenerateEd25519Keys(dir string) (KeyPaths, string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return KeyPaths{}, "", fmt.Errorf("create key directory: %w", err)
	}
	identity, err := NewEd25519Identity()
	if err != nil {
		return KeyPaths{}, "", err
	}
	keyPath := filepath.Join(dir, "server.key")
	if err := writeKeyFileDurably(keyPath, identity.KeyPEM); err != nil {
		return KeyPaths{}, "", fmt.Errorf("write key: %w", err)
	}
	certPath := filepath.Join(dir, "server.crt")
	if err := writeKeyFileDurably(certPath, identity.CertPEM); err != nil {
		return KeyPaths{}, "", fmt.Errorf("write cert: %w", err)
	}
	return KeyPaths{Key: keyPath, Cert: certPath}, identity.Fingerprint, nil
}

func LoadOrCreateKeys(dir string) (KeyPaths, string, error) {
	keyPath := filepath.Join(dir, "server.key")
	certPath := filepath.Join(dir, "server.crt")
	if _, err := os.Stat(keyPath); err == nil {
		if _, err := os.Stat(certPath); err == nil {
			cert, err := os.ReadFile(certPath)
			if err != nil {
				return KeyPaths{}, "", fmt.Errorf("read cert: %w", err)
			}
			block, _ := pem.Decode(cert)
			if block == nil {
				return KeyPaths{}, "", fmt.Errorf("decode cert PEM")
			}
			x509Cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return KeyPaths{}, "", fmt.Errorf("parse cert: %w", err)
			}
			pubBytes := x509Cert.PublicKey.(ed25519.PublicKey)
			return KeyPaths{Key: keyPath, Cert: certPath}, Fingerprint(pubBytes), nil
		}
	}
	return GenerateEd25519Keys(dir)
}

func LoadTLSConfig(paths KeyPaths, caPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(paths.Cert, paths.Key)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	if caPath != "" {
		caCert, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("parse CA cert")
		}
		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return config, nil
}

func LoadClientTLSConfig(certPath, keyPath, caPath string) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS13}
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}
	if caPath != "" {
		caCert, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("parse CA cert")
		}
		config.RootCAs = pool
	}
	return config, nil
}
