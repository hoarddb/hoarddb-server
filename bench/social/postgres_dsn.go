package main

import (
	"fmt"
	"net/url"
	"strings"
)

// postgresConnectionString is -postgres-dsn with the TLS rule applied: with
// -postgres-tls on (the default), a DSN that names no sslmode gets
// sslmode=require, which encrypts without checking the certificate (pgx does
// not verify under require), the rule the hoardDB client follows; with it off,
// a DSN that names none gets sslmode=disable, the record-only TLS-off leg. A
// DSN that names an sslmode keeps it, except sslmode=disable while
// -postgres-tls is on, which would silently undo the flag.
func (c *config) postgresConnectionString() (string, error) {
	if strings.Contains(c.PostgresDSN, ",") {
		return "", fmt.Errorf("-postgres-dsn names several servers; the postgres engine drives one server, so give a single postgresql:// URL")
	}
	target, err := url.Parse(c.PostgresDSN)
	if err != nil {
		return "", fmt.Errorf("parsing -postgres-dsn: %w", err)
	}
	if strings.TrimPrefix(target.Path, "/") == "" {
		return "", fmt.Errorf("-postgres-dsn names no database; end it with /social, for example postgresql://bench@host:5432/social")
	}
	query := target.Query()
	switch mode := query.Get("sslmode"); {
	case mode == "" && c.PostgresTLS:
		query.Set("sslmode", "require")
	case mode == "":
		query.Set("sslmode", "disable")
	case mode == "disable" && c.PostgresTLS:
		return "", fmt.Errorf("-postgres-dsn names sslmode=disable for %s while -postgres-tls is on; remove sslmode=disable from the DSN, or pass -postgres-tls=false for the record-only TLS-off leg", target.Host)
	}
	target.RawQuery = query.Encode()
	return target.String(), nil
}

// postgresDSNWithDatabase is dsn pointing at another database of the same
// server, account and parameters, for the live tests' scratch database.
func postgresDSNWithDatabase(dsn, database string) (string, error) {
	target, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parsing the DSN: %w", err)
	}
	target.Path = "/" + database
	return target.String(), nil
}
