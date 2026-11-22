package main

import (
	"strings"
	"testing"
)

func TestPostgresURLFromAddressAndCredentials(t *testing.T) {
	target, err := postgresURL(&config{Addr: "db:5432", User: "bench", Password: "p w", Name: "wm", TLS: true})
	if err != nil {
		t.Fatal(err)
	}
	if target.Host != "db:5432" || target.Path != "/wm" || target.User.Username() != "bench" {
		t.Fatalf("url %s", target.Redacted())
	}
	if got, _ := target.User.Password(); got != "p w" {
		t.Fatalf("password %q", got)
	}
	if target.Query().Get("sslmode") != "require" {
		t.Fatalf("sslmode %q, want require with -tls", target.Query().Get("sslmode"))
	}
	plain, err := postgresURL(&config{Addr: "db:5432", Name: "wm"})
	if err != nil {
		t.Fatal(err)
	}
	if plain.User.Username() != "postgres" || plain.Query().Get("sslmode") != "disable" {
		t.Fatalf("defaults: %s", plain.Redacted())
	}
}

func TestPostgresURLKeepsADSNSslmode(t *testing.T) {
	target, err := postgresURL(&config{DSN: "postgresql://u:p@h:1/x?sslmode=verify-ca", Name: "wm", TLS: true})
	if err != nil {
		t.Fatal(err)
	}
	if target.Query().Get("sslmode") != "verify-ca" || target.Path != "/wm" {
		t.Fatalf("url %s", target.Redacted())
	}
	if _, err := postgresURL(&config{DSN: "::bad"}); err == nil || !strings.Contains(err.Error(), "-dsn") {
		t.Fatalf("err %v", err)
	}
}

func TestPostgresIsAKnownEngine(t *testing.T) {
	if _, err := parseConfig([]string{"-engine", "postgres", "-addr", "h:5432"}, envOf(nil)); err != nil {
		t.Fatal(err)
	}
}
