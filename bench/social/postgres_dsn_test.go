package main

import (
	"strings"
	"testing"
)

// TestPostgresDSNTLS: -postgres-tls on asks for an encrypted connection with
// the certificate unchecked (sslmode=require, which does not verify), the rule
// the hoardDB client follows; off keeps the record-only plaintext leg. A DSN
// that names an sslmode of its own keeps it, except sslmode=disable under
// -postgres-tls, which would silently undo the flag.
func TestPostgresDSNTLS(t *testing.T) {
	const base = "postgresql://bench@db.example:5432/social"
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
			cfg := &config{PostgresDSN: test.dsn, PostgresTLS: test.tls}
			got, err := cfg.postgresConnectionString()
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

// TestPostgresDSNNeedsADatabaseAndOneServer: the engine opens one database on
// one node; anything else is refused with the fix.
func TestPostgresDSNNeedsADatabaseAndOneServer(t *testing.T) {
	if _, err := (&config{PostgresDSN: "postgresql://bench@db.example:5432"}).postgresConnectionString(); err == nil || !strings.Contains(err.Error(), "/social") {
		t.Errorf("a DSN without a database gave %v; want an error that says to end it with /social", err)
	}
	if _, err := (&config{PostgresDSN: "postgresql://bench@a:5432/social,postgresql://bench@b:5432/social"}).postgresConnectionString(); err == nil || !strings.Contains(err.Error(), "one server") {
		t.Errorf("two DSNs gave %v; want an error that says the engine drives one server", err)
	}
	if _, err := (&config{PostgresDSN: "not a url ::"}).postgresConnectionString(); err == nil {
		t.Error("an unparsable DSN was accepted")
	}
}

// TestPostgresTargetHidesCredentials: the report records the endpoint, never
// the password.
func TestPostgresTargetHidesCredentials(t *testing.T) {
	engine := &postgresEngine{cfg: &config{PostgresDSN: "postgresql://bench:s3cret@db.example:5432/social?sslmode=disable"}}
	got := engine.target()
	if strings.Contains(got, "s3cret") || strings.Contains(got, "bench@") || got != "db.example:5432/social" {
		t.Errorf("target is %q; want db.example:5432/social", got)
	}
}

// TestPostgresDSNWithDatabase: the live tests' scratch database replaces the
// DSN's database and the rest survives.
func TestPostgresDSNWithDatabase(t *testing.T) {
	got, err := postgresDSNWithDatabase("postgresql://bench@db.example:5432/social?sslmode=require&application_name=x", "social_live_1")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"/social_live_1", "sslmode=require", "application_name=x", "bench@db.example:5432"} {
		if !strings.Contains(got, want) {
			t.Errorf("rewritten DSN %q lacks %q", got, want)
		}
	}
}
