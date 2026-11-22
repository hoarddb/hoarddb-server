package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// ClusterToken represents a shared secret used to authenticate new nodes
// joining the cluster. It is generated once during first init and stored
// on disk with restricted permissions.
type ClusterToken struct {
	// Token is the 64-character hex-encoded cluster secret.
	Token string `json:"token"`
	// CreatedAt is when this token was generated.
	CreatedAt time.Time `json:"created_at"`
	// ExpiresAt is read from files earlier builds wrote and is ignored: the
	// cluster token is the secret every member presents for the life of the
	// cluster, so it does not expire. New files leave it zero.
	ExpiresAt time.Time `json:"expires_at"`
}

// GenerateClusterToken creates a cryptographically random 32-byte token
// and returns it as a 64-character hex string.
func GenerateClusterToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate random bytes: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// SaveClusterToken persists a ClusterToken to {dir}/cluster.token in
// JSON format with 0600 file permissions, on a node that does not seal its
// credential files. CredentialKeys.SaveClusterToken is the form a node that
// may seal them uses.
func SaveClusterToken(dir string, token ClusterToken) error {
	return CredentialKeys{}.SaveClusterToken(dir, token)
}

// SaveClusterToken persists a ClusterToken to {dir}/cluster.token as JSON,
// sealed under the node master when k seals the node's credentials. The file
// is replaced durably: a power loss leaves either the previous token or the
// new one, never an empty file that would stop the node from starting.
func (k CredentialKeys) SaveClusterToken(dir string, token ClusterToken) error {
	data, err := json.MarshalIndent(token, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal cluster token: %w", err)
	}
	if err := k.writeCredential(dir, clusterTokenCredential, data); err != nil {
		return fmt.Errorf("write cluster token: %w", err)
	}
	return nil
}

// LoadClusterToken reads and parses the cluster.token file from the given
// directory, on a node that does not seal its credential files: a sealed file
// is refused, naming the settings that open it.
func LoadClusterToken(dir string) (*ClusterToken, error) {
	return CredentialKeys{}.LoadClusterToken(dir)
}

// LoadClusterToken reads and parses the cluster.token file from the given
// directory, opening it under the node master when it is sealed. A missing
// file is an error that matches fs.ErrNotExist.
func (k CredentialKeys) LoadClusterToken(dir string) (*ClusterToken, error) {
	data, _, err := k.readCredential(dir, clusterTokenCredential)
	if err != nil {
		return nil, fmt.Errorf("read cluster token: %w", err)
	}
	var token ClusterToken
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, fmt.Errorf("parse cluster token: %w", err)
	}
	return &token, nil
}

// NodeInfo holds metadata about a known cluster node.
type NodeInfo struct {
	// NodeID is the unique identifier of the node.
	NodeID string `json:"node_id"`
	// Addr is the gRPC listen address.
	Addr string `json:"addr"`
	// Fingerprint is the SHA-256 fingerprint of the node's TLS certificate.
	Fingerprint string `json:"fingerprint"`
	// JoinedAt records when the node first joined.
	JoinedAt time.Time `json:"joined_at"`
	// Status is "active", "leaving", or "drained".
	Status string `json:"status"`
	// Duplicate is set while this node sees two live processes running under
	// the node's id (BUG-0600); 'status' has the full sentence and the fix.
	Duplicate string `bson:"duplicate,omitempty" json:"duplicate,omitempty"`
}

// FingerprintFromCert computes the canonical fingerprint of a PEM-encoded
// TLS certificate. It returns the same format as transport.Fingerprint, so a
// value recorded from a certificate and a value learned from a live
// handshake compare equal. The certificate key must be ed25519:
// hoardDB uses only self-signed ed25519 certificates, and a comparison that
// silently accepted another key type would be a trust hole.
func FingerprintFromCert(pemCert []byte) (string, error) {
	block, _ := pem.Decode(pemCert)
	if block == nil {
		return "", fmt.Errorf("decode PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse certificate: %w", err)
	}
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return "", fmt.Errorf("certificate key is not ed25519 (type: %T)", cert.PublicKey)
	}
	return transport.Fingerprint(pub), nil
}

// KnownNodes returns a snapshot of all known cluster nodes. The records are
// value copies taken under the lock, so a caller may read or encode them after
// releasing it while gossip still rewrites the table in place.
func (s *Server) KnownNodes() map[string]NodeInfo {
	s.knownNodesMu.RLock()
	defer s.knownNodesMu.RUnlock()
	snapshot := make(map[string]NodeInfo, len(s.knownNodes))
	for k, v := range s.knownNodes {
		snapshot[k] = *v
	}
	return snapshot
}

// InitClusterToken creates the initial cluster token on first start.
// If a token already exists, it loads and returns it. Returns the token
// string and whether it was newly created. A token file that exists but
// cannot be read or parsed stops the start: minting another would put this
// node on a secret no other member holds, so the node reports the file and
// how to restore it instead.
func InitClusterToken(dir string) (string, bool, error) {
	return CredentialKeys{}.InitClusterToken(dir)
}

// InitClusterToken is InitClusterToken for a data directory whose credential
// files k reads and writes: a new token is written sealed when k seals them,
// and a sealed one is opened.
func (k CredentialKeys) InitClusterToken(dir string) (string, bool, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", false, fmt.Errorf("create data directory: %w", err)
	}

	// Check if token already exists.
	existing, err := k.LoadClusterToken(dir)
	if err == nil {
		return existing.Token, false, nil
	}
	var sealedErr *sealedCredentialError
	if errors.As(err, &sealedErr) {
		return "", false, err
	}
	if !errors.Is(err, fs.ErrNotExist) {
		path := clusterTokenCredential.path(dir)
		return "", false, fmt.Errorf("reading %s: %w. Fix its permissions (chmod 600 %s), or %s. hoardDB will not replace a cluster token it cannot read", path, err, path, clusterTokenMoveAside)
	}

	// Generate new token.
	tokenStr, err := GenerateClusterToken()
	if err != nil {
		return "", false, fmt.Errorf("generate cluster token: %w", err)
	}

	token := ClusterToken{
		Token:     tokenStr,
		CreatedAt: time.Now(),
	}

	if err := k.SaveClusterToken(dir, token); err != nil {
		return "", false, fmt.Errorf("save cluster token: %w", err)
	}

	return tokenStr, true, nil
}

// RequestClusterToken loads the cluster token from the local storage.
// If no token exists locally, it returns an error. Token sharing between
// nodes happens out-of-band (a cluster.token copied in by hand, or a shared volume).
func (s *Server) RequestClusterToken() (string, error) {
	token, err := s.credentials.LoadClusterToken(s.config.Storage.DataDir)
	if err != nil {
		return "", fmt.Errorf("no local cluster token: %w; the cluster's token is %s", err, clusterTokenSource)
	}
	return token.Token, nil
}
