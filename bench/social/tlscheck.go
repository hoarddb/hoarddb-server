package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TLS on every engine (spec 3.1 "Security", P6 integrity failure 4). hoardDB
// cannot run without TLS, so the competitors do not either: the load client
// connects to MongoDB and MySQL over TLS with the certificate unchecked, as it
// does to hoardDB, and -mode tlscheck proves it per engine from the server's
// side where the server can say so.

// competitorTLSConfig is the client side of that rule. MinVersion is TLS 1.2,
// the oldest both servers keep on by default; verification is off because the
// certification's certificates are self-signed and generated per run.
func competitorTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // self-signed certificates, the same rule as the hoardDB client (spec 3.1)
	}
}

// tlsFinding is what the check learned about the connections to one server.
type tlsFinding struct {
	Node      string `json:"node"`
	Encrypted bool   `json:"encrypted"`
	Detail    string `json:"detail"`
}

// encryptionChecker is an engine that can say whether the connections its load
// client opens are encrypted.
type encryptionChecker interface {
	checkEncryption(ctx context.Context) ([]tlsFinding, error)
}

// tlsCheckResult is the one JSON line -mode tlscheck prints.
type tlsCheckResult struct {
	Mode              string       `json:"mode"`
	Engine            string       `json:"engine"`
	Connections       []tlsFinding `json:"connections"`
	Pass              bool         `json:"pass"`
	IntegrityFailures []string     `json:"integrity_failures"`
}

// judgeTLS turns findings into a result: every connection must be encrypted.
func judgeTLS(engine string, findings []tlsFinding) tlsCheckResult {
	result := tlsCheckResult{Mode: modeTLSCheck, Engine: engine, Connections: findings, Pass: len(findings) > 0, IntegrityFailures: []string{}}
	for _, finding := range findings {
		if !finding.Encrypted {
			result.Pass = false
			result.IntegrityFailures = append(result.IntegrityFailures, fmt.Sprintf("%s: %s", finding.Node, finding.Detail))
		}
	}
	if len(findings) == 0 {
		result.IntegrityFailures = append(result.IntegrityFailures, "the check inspected no connection")
	}
	return result
}

// runTLSCheck runs -mode tlscheck and prints the result; a connection that is
// not encrypted is an error, so the harness step fails.
func runTLSCheck(ctx context.Context, cfg *config, out io.Writer) error {
	checkContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	eng, err := newEngine(checkContext, cfg)
	if err != nil {
		return fmt.Errorf("opening %s for the TLS check (a refusal here can itself be the answer: a server that requires TLS turns a plaintext client away): %w", cfg.Engine, err)
	}
	defer eng.close()
	checker, ok := eng.(encryptionChecker)
	if !ok {
		return fmt.Errorf("-engine %s cannot run the TLS check", cfg.Engine)
	}
	findings, err := checker.checkEncryption(checkContext)
	if err != nil {
		return fmt.Errorf("checking encryption on %s: %w", cfg.Engine, err)
	}
	result := judgeTLS(cfg.Engine, findings)
	line, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encoding the TLS check result: %w", err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the TLS check result: %w", err)
	}
	if !result.Pass {
		return fmt.Errorf("TLS check failed: %s", strings.Join(result.IntegrityFailures, "; "))
	}
	return nil
}

// probeNodeEncryption connects to a hoardDB node the way the load client does
// and reports the session it got, then offers the node a plaintext frame. A node
// that completes the TLS handshake and turns the plaintext connection away is
// TLS-only; one that cannot complete the handshake, or that answers plaintext,
// fails.
func probeNodeEncryption(addr string, timeout time.Duration) tlsFinding {
	finding := tlsFinding{Node: addr}
	tlsConfig, err := sharedTLSConfig(addr)
	if err != nil {
		finding.Detail = err.Error()
		return finding
	}
	raw, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, tlsConfig)
	if err != nil {
		finding.Detail = fmt.Sprintf("the TLS handshake failed (%v); the node is not speaking TLS on this port", err)
		return finding
	}
	state := raw.ConnectionState()
	closeErr := raw.Close()
	if !state.HandshakeComplete || state.Version < tls.VersionTLS12 {
		finding.Detail = fmt.Sprintf("the session is %s, not an encrypted TLS 1.2 or newer session", tls.VersionName(state.Version))
		return finding
	}
	accepted, err := acceptsPlaintext(addr, timeout)
	switch {
	case err != nil:
		finding.Detail = fmt.Sprintf("probing for plaintext: %v", err)
	case accepted:
		finding.Detail = "the node answered a plaintext frame: it accepts unencrypted connections"
	case closeErr != nil:
		finding.Detail = fmt.Sprintf("closing the probe connection: %v", closeErr)
	default:
		finding.Encrypted = true
		finding.Detail = fmt.Sprintf("%s, cipher %s, plaintext refused", tls.VersionName(state.Version), tls.CipherSuiteName(state.CipherSuite))
	}
	return finding
}

// acceptsPlaintext sends a heartbeat frame without TLS and says whether a valid
// frame came back. A TLS server answers a stray plaintext record with an alert or
// closes the connection; neither decodes as a frame.
func acceptsPlaintext(addr string, timeout time.Duration) (bool, error) {
	plain, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false, fmt.Errorf("dialling %s: %w", addr, err)
	}
	defer func() { _ = plain.Close() }() // a probe connection; nothing depends on its close
	if err := plain.SetDeadline(time.Now().Add(timeout)); err != nil {
		return false, fmt.Errorf("setting a deadline: %w", err)
	}
	if _, err := plain.Write(transport.NewFrame(transport.OpPing, nil).Encode()); err != nil {
		return false, nil //nolint:nilerr // a write that fails is a connection turned away, which is the answer wanted
	}
	if _, err := transport.DecodeFrame(plain); err != nil {
		return false, nil //nolint:nilerr // an EOF, a reset or bytes that are not a frame (a TLS alert) are all a refusal
	}
	return true, nil
}
