package main

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
)

// mysqlTargets parses every -mysql-dsn server and, with -mysql-tls on, makes the
// connection TLS with the certificate unchecked (tls=skip-verify), the same rule
// the hoardDB client follows. A DSN that names a tls= value of its own keeps it,
// except tls=false, which would silently undo the flag.
func (c *config) mysqlTargets() ([]*mysql.Config, error) {
	var targets []*mysql.Config
	for _, dsn := range c.mysqlDSNs() {
		target, err := mysql.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("parsing -mysql-dsn: %w", err)
		}
		if c.MySQLTLS {
			switch target.TLSConfig {
			case "":
				target.TLSConfig = "skip-verify"
			case "false":
				return nil, fmt.Errorf("-mysql-dsn names tls=false for %s while -mysql-tls is on; remove tls=false from the DSN, or pass -mysql-tls=false for the record-only TLS-off leg", target.Addr)
			}
		}
		targets = append(targets, target)
	}
	return targets, nil
}
