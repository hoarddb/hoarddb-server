package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// cockroachConnectionString is -cockroach-dsn with the TLS rule applied: with
// -cockroach-tls on, a DSN that names no sslmode gets sslmode=require, which
// encrypts without checking the certificate (pgx does not verify under
// require), the rule the hoardDB client follows; with it off, a DSN that names
// none gets sslmode=disable, the record-only leg against the recipe's insecure
// node. A DSN that names an sslmode keeps it, except sslmode=disable while
// -cockroach-tls is on, which would silently undo the flag.
func (c *config) cockroachConnectionString() (string, error) {
	if strings.Contains(c.CockroachDSN, ",") {
		return "", fmt.Errorf("-cockroach-dsn names several servers; the cockroach engine drives one node, so give a single postgresql:// URL")
	}
	target, err := url.Parse(c.CockroachDSN)
	if err != nil {
		return "", fmt.Errorf("parsing -cockroach-dsn: %w", err)
	}
	if strings.TrimPrefix(target.Path, "/") == "" {
		return "", fmt.Errorf("-cockroach-dsn names no database; end it with /social, for example postgresql://root@host:26257/social?sslmode=disable")
	}
	query := target.Query()
	switch mode := query.Get("sslmode"); {
	case mode == "" && c.CockroachTLS:
		query.Set("sslmode", "require")
	case mode == "":
		query.Set("sslmode", "disable")
	case mode == "disable" && c.CockroachTLS:
		return "", fmt.Errorf("-cockroach-dsn names sslmode=disable for %s while -cockroach-tls is on; remove sslmode=disable from the DSN, or drop -cockroach-tls for the record-only TLS-off leg", target.Host)
	}
	target.RawQuery = query.Encode()
	return target.String(), nil
}

// cockroachDSNWithDatabase is dsn pointing at another database of the same
// server, account and parameters, for the live tests' scratch database.
func cockroachDSNWithDatabase(dsn, database string) (string, error) {
	target, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parsing the DSN: %w", err)
	}
	target.Path = "/" + database
	return target.String(), nil
}

// judgeCockroachConnection says whether the connection a driver holds is an
// encrypted session. The client's own connection state is the evidence:
// CockroachDB does not report a session's cipher from the server side.
func judgeCockroachConnection(node string, conn net.Conn) tlsFinding {
	finding := tlsFinding{Node: node}
	secured, ok := conn.(*tls.Conn)
	if !ok {
		finding.Detail = "the connection is plaintext (sslmode=disable, as the recipe's insecure node requires)"
		return finding
	}
	state := secured.ConnectionState()
	if !state.HandshakeComplete || state.Version < tls.VersionTLS12 {
		finding.Detail = fmt.Sprintf("the session is %s, not an encrypted TLS 1.2 or newer session", tls.VersionName(state.Version))
		return finding
	}
	finding.Encrypted = true
	finding.Detail = fmt.Sprintf("%s, cipher %s", tls.VersionName(state.Version), tls.CipherSuiteName(state.CipherSuite))
	return finding
}
