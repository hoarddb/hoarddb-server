package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestCockroachDSNTLS: -cockroach-tls on asks for an encrypted connection with
// the certificate unchecked (sslmode=require, which does not verify), the rule
// the hoardDB client follows; off keeps the record-only plaintext leg. A DSN
// that names an sslmode of its own keeps it, except sslmode=disable under
// -cockroach-tls, which would silently undo the flag.
func TestCockroachDSNTLS(t *testing.T) {
	const base = "postgresql://root@db.example:26257/social"
	for _, test := range []struct {
		name, dsn string
		tls       bool
		wantMode  string
		wantErr   string
	}{
		{"tls on, none named", base, true, "require", ""},
		{"tls off, none named", base, false, "disable", ""},
		{"tls on, verify-full kept", base + "?sslmode=verify-full", true, "verify-full", ""},
		{"tls off, require kept", base + "?sslmode=require", false, "require", ""},
		{"tls on, disable refused", base + "?sslmode=disable", true, "", "sslmode=disable"},
		{"other parameters survive", base + "?application_name=bench", true, "require", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config{CockroachDSN: test.dsn, CockroachTLS: test.tls}
			got, err := cfg.cockroachConnectionString()
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("got %v; want an error naming %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got, "sslmode="+test.wantMode) {
				t.Errorf("connection string %q lacks sslmode=%s", got, test.wantMode)
			}
			if strings.Contains(test.name, "other") && !strings.Contains(got, "application_name=bench") {
				t.Errorf("connection string %q dropped application_name", got)
			}
		})
	}
}

// TestCockroachDSNNeedsADatabaseAndOneServer: the engine opens one database on
// one node; anything else is refused with the fix.
func TestCockroachDSNNeedsADatabaseAndOneServer(t *testing.T) {
	if _, err := (&config{CockroachDSN: "postgresql://root@db.example:26257"}).cockroachConnectionString(); err == nil || !strings.Contains(err.Error(), "/social") {
		t.Errorf("a DSN without a database gave %v; want an error that says to end it with /social", err)
	}
	if _, err := (&config{CockroachDSN: "postgresql://root@a:26257/social,postgresql://root@b:26257/social"}).cockroachConnectionString(); err == nil || !strings.Contains(err.Error(), "one node") {
		t.Errorf("two DSNs gave %v; want an error that says the engine drives one node", err)
	}
	if _, err := (&config{CockroachDSN: "not a url ::"}).cockroachConnectionString(); err == nil {
		t.Error("an unparsable DSN was accepted")
	}
}

// TestCockroachTargetHidesCredentials: the report records the endpoint, never
// the password.
func TestCockroachTargetHidesCredentials(t *testing.T) {
	engine := &cockroachEngine{cfg: &config{CockroachDSN: "postgresql://bench:s3cret@db.example:26257/social?sslmode=disable"}}
	got := engine.target()
	if strings.Contains(got, "s3cret") || strings.Contains(got, "bench@") || got != "db.example:26257/social" {
		t.Errorf("target is %q; want db.example:26257/social", got)
	}
}

// TestCockroachDSNWithDatabase: the live tests' scratch database replaces the
// DSN's database and the rest survives.
func TestCockroachDSNWithDatabase(t *testing.T) {
	got, err := cockroachDSNWithDatabase("postgresql://root@db.example:26257/social?sslmode=require&application_name=x", "social_live_1")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"/social_live_1", "sslmode=require", "application_name=x", "root@db.example:26257"} {
		if !strings.Contains(got, want) {
			t.Errorf("rewritten DSN %q lacks %q", got, want)
		}
	}
}

// TestJudgeCockroachConnection: a connection is encrypted when what the driver
// holds is a completed TLS 1.2+ session, and plaintext otherwise.
func TestJudgeCockroachConnection(t *testing.T) {
	plain, other := net.Pipe()
	t.Cleanup(func() { _ = plain.Close(); _ = other.Close() })
	if finding := judgeCockroachConnection("node", plain); finding.Encrypted || !strings.Contains(finding.Detail, "plaintext") {
		t.Errorf("a plain connection judged %+v; want unencrypted, saying plaintext", finding)
	}
	server := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	secure, err := tls.Dial("tcp", server.Listener.Addr().String(), competitorTLSConfig())
	if err != nil {
		t.Fatalf("dialling the test TLS server: %v", err)
	}
	t.Cleanup(func() { _ = secure.Close() })
	if finding := judgeCockroachConnection("node", secure); !finding.Encrypted || !strings.Contains(finding.Detail, "cipher") {
		t.Errorf("a TLS connection judged %+v; want encrypted, naming the cipher", finding)
	}
}
