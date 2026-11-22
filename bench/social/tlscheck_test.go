package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestMongoConnectionIsTLS (unit half): the options the tool builds carry a TLS
// configuration that skips certificate verification, the hoardDB client's rule,
// unless TLS is switched off for the record-only leg.
func TestMongoConnectionIsTLS(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		cfg := &config{MongoURI: "mongodb://127.0.0.1:27017", Conc: 2, MongoTLS: enabled}
		concern, err := mongoWriteConcern("w1j")
		if err != nil {
			t.Fatal(err)
		}
		clientOptions := mongoClientOptions(cfg, concern, readpref.Primary())
		switch {
		case enabled && (clientOptions.TLSConfig == nil || !clientOptions.TLSConfig.InsecureSkipVerify || clientOptions.TLSConfig.MinVersion < tls.VersionTLS12):
			t.Errorf("-mongo-tls on: TLS config %+v; want one that skips verification at TLS 1.2 or newer", clientOptions.TLSConfig)
		case !enabled && clientOptions.TLSConfig != nil:
			t.Error("-mongo-tls=false still set a TLS config")
		}
	}
	cfg, err := parseFlags([]string{"-engine", "mongo"})
	if err != nil || !cfg.MongoTLS {
		t.Errorf("-engine mongo parsed with MongoTLS=%v (%v); TLS must be the default, as for hoardDB", cfg.MongoTLS, err)
	}
	if cfg, err := parseFlags([]string{"-engine", "mongo", "-mongo-tls=false"}); err != nil || cfg.MongoTLS {
		t.Errorf("-mongo-tls=false parsed with MongoTLS=%v (%v)", cfg.MongoTLS, err)
	}
}

// TestMySQLConnectionIsTLS (unit half): every server of -mysql-dsn is dialled
// with tls=skip-verify unless the DSN says otherwise; tls=false contradicts the
// flag and is refused.
func TestMySQLConnectionIsTLS(t *testing.T) {
	cfg := &config{MySQLDSN: "u:p@tcp(a:3306)/social,u:p@tcp(b:3306)/social?tls=true", MySQLTLS: true}
	targets, err := cfg.mysqlTargets()
	if err != nil {
		t.Fatalf("mysqlTargets: %v", err)
	}
	if targets[0].TLSConfig != "skip-verify" || !strings.Contains(targets[0].FormatDSN(), "tls=skip-verify") {
		t.Errorf("first server TLS = %q (%s); want skip-verify in the DSN the pool opens", targets[0].TLSConfig, targets[0].FormatDSN())
	}
	if targets[1].TLSConfig != "true" {
		t.Errorf("second server TLS = %q; a tls= the DSN names itself must be kept", targets[1].TLSConfig)
	}
	if _, err := (&config{MySQLDSN: "u:p@tcp(a:3306)/social?tls=false", MySQLTLS: true}).mysqlTargets(); err == nil {
		t.Error("tls=false in the DSN was accepted while -mysql-tls is on")
	}
	off, err := (&config{MySQLDSN: "u:p@tcp(a:3306)/social"}).mysqlTargets()
	if err != nil || off[0].TLSConfig != "" {
		t.Errorf("-mysql-tls=false changed the DSN to %q (%v); the record-only leg must stay as given", off[0].TLSConfig, err)
	}
	if parsed, err := parseFlags([]string{"-engine", "mysql"}); err != nil || !parsed.MySQLTLS {
		t.Errorf("-engine mysql parsed with MySQLTLS=%v (%v); TLS must be the default", parsed.MySQLTLS, err)
	}
}

// selfSignedCertificate is a throwaway certificate for a TLS stub.
func selfSignedCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a key: %v", err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "stub"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating a certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// serveStub accepts connections on a loopback port and hands each to handle.
func serveStub(t *testing.T, listen func() (net.Listener, error), handle func(net.Conn)) string {
	t.Helper()
	listener, err := listen()
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				handle(conn)
			}()
		}
	}()
	return listener.Addr().String()
}

func tlsOnlyStub(t *testing.T) string {
	certificate := selfSignedCertificate(t)
	return serveStub(t, func() (net.Listener, error) {
		return tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	}, func(conn net.Conn) {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Read(make([]byte, 1))
	})
}

// plaintextStub answers every frame it is sent with a valid frame, as a server
// that offers no TLS would.
func plaintextStub(t *testing.T) string {
	return serveStub(t, func() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }, func(conn net.Conn) {
		if _, err := transport.DecodeFrame(conn); err == nil {
			_, _ = conn.Write(transport.NewFrame(transport.OpPong, nil).Encode())
		}
	})
}

// TestTLSCheckFailsOnPlaintext: a node that does not speak TLS, and one that
// answers plaintext, fail the check; a TLS-only node passes it.
func TestTLSCheckFailsOnPlaintext(t *testing.T) {
	plain := plaintextStub(t)
	if finding := probeNodeEncryption(plain, 2*time.Second); finding.Encrypted {
		t.Errorf("a plaintext node passed the check: %+v", finding)
	}
	if accepted, err := acceptsPlaintext(plain, 2*time.Second); err != nil || !accepted {
		t.Errorf("acceptsPlaintext(plaintext node) = %v, %v; want true", accepted, err)
	}
	secure := tlsOnlyStub(t)
	if accepted, err := acceptsPlaintext(secure, 2*time.Second); err != nil || accepted {
		t.Errorf("acceptsPlaintext(TLS-only node) = %v, %v; want false", accepted, err)
	}
	if finding := probeNodeEncryption(secure, 2*time.Second); !finding.Encrypted {
		t.Errorf("a TLS-only node failed the check: %+v", finding)
	}
	result := judgeTLS(engineHoardDB, []tlsFinding{probeNodeEncryption(secure, 2*time.Second), probeNodeEncryption(plain, 2*time.Second)})
	if result.Pass || len(result.IntegrityFailures) != 1 {
		t.Errorf("one plaintext node among two must fail the whole check: %+v", result)
	}
	if judgeTLS(engineMongo, nil).Pass {
		t.Error("a check that inspected no connection passed")
	}
}

// TestMongoConnectionIsTLSLive is the live half: the server itself counts the
// load client's connections as TLS handshakes. With TLS switched off the client
// either cannot connect (a requireTLS server) or the check reports it.
func TestMongoConnectionIsTLSLive(t *testing.T) {
	cfg := liveMongoConfig(t)
	cfg.MongoTLS = true
	engine, err := newMongoEngine(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connecting with TLS: %v", err)
	}
	defer engine.close()
	findings, err := engine.checkEncryption(context.Background())
	if err != nil || !judgeTLS(engineMongo, findings).Pass {
		t.Fatalf("TLS on: findings %+v (%v); want the server to count every connection as encrypted", findings, err)
	}
	cfg.MongoTLS = false
	cfg.MongoURI = withoutTLSOptions(t, cfg.MongoURI)
	plain, err := newMongoEngine(context.Background(), cfg)
	if err != nil {
		return // a server that requires TLS turned the plaintext client away: the check cannot pass either
	}
	defer plain.close()
	findings, err = plain.checkEncryption(context.Background())
	if err == nil && judgeTLS(engineMongo, findings).Pass {
		t.Errorf("TLS off: the check passed on %+v; plaintext connections must fail it", findings)
	}
}

// withoutTLSOptions removes the tls options from a connection string, so a test
// can show what the flag alone decides.
func withoutTLSOptions(t *testing.T, uri string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parsing the live URI: %v", err)
	}
	query := parsed.Query()
	for name := range query {
		if strings.HasPrefix(strings.ToLower(name), "tls") || strings.EqualFold(name, "ssl") {
			query.Del(name)
		}
	}
	query.Set("serverSelectionTimeoutMS", "3000") // a server that needs TLS never answers a plaintext client
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// TestMySQLConnectionIsTLSLive is the integration half: the server reports a
// negotiated cipher on every connection the load client holds. It skips unless
// SOCIAL_LIVE_MYSQL_DSN names a mysqld, which the build host does not have.
func TestMySQLConnectionIsTLSLive(t *testing.T) {
	world := liveMySQLWorld(t, false)
	engine, ok := world.social.(*mysqlEngine)
	if !ok {
		t.Fatalf("the live world is %T, not a MySQL engine", world.social)
	}
	engine.cfg.MySQLTLS = true
	findings, err := engine.checkEncryption(context.Background())
	if err != nil || !judgeTLS(engineMySQL, findings).Pass {
		t.Errorf("findings %+v (%v); want a cipher on every connection (put tls=skip-verify in SOCIAL_LIVE_MYSQL_DSN, or use a server with require_secure_transport)", findings, err)
	}
}

// TestHoardEngineChecksEveryNode: the engine's check inspects each node of the
// cluster and, past the TLS probe, proves the load client's own login works: the
// stubs speak no protocol, so the TLS-only one fails there, by name.
func TestHoardEngineChecksEveryNode(t *testing.T) {
	secure, plain := tlsOnlyStub(t), plaintextStub(t)
	engine := newHoardEngine(&config{Engine: engineHoardDB, Addrs: []string{secure, plain}, OpTimeout: 2 * time.Second})
	findings, err := engine.checkEncryption(context.Background())
	if err != nil || len(findings) != 2 || findings[0].Node != secure || findings[1].Node != plain {
		t.Fatalf("findings %+v (%v); want one per node, in order", findings, err)
	}
	if findings[0].Encrypted || !strings.Contains(findings[0].Detail, "log in") {
		t.Errorf("TLS-only stub finding %+v; want the failed login named", findings[0])
	}
	if findings[1].Encrypted {
		t.Errorf("plaintext node finding %+v; want it failed", findings[1])
	}
}
