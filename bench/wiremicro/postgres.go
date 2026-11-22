package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
)

// newPostgresEngine opens PostgreSQL through pgx's database/sql adapter, the
// same adapter the CockroachDB engine uses. Each insert is one autocommit
// statement, so the server's synchronous_commit decides when it is on disk.
func newPostgresEngine(ctx context.Context, cfg *config) (*sqlEngine, error) {
	target, err := postgresURL(cfg)
	if err != nil {
		return nil, err
	}
	bare := *target
	bare.Path = "/postgres"
	admin, err := openPgx(bare.String())
	if err != nil {
		return nil, err
	}
	defer func() { _ = admin.Close() }()
	var exists bool
	if err := admin.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", cfg.Name).Scan(&exists); err != nil {
		return nil, fmt.Errorf("looking for database %q on PostgreSQL at %s (check -addr, -user, %s and -tls): %w", cfg.Name, target.Host, envPassword, err)
	}
	if !exists {
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+pgx.Identifier{cfg.Name}.Sanitize()); err != nil {
			return nil, fmt.Errorf("creating database %q (the user needs CREATEDB, or create it by hand): %w", cfg.Name, err)
		}
	}
	db, err := openPgx(target.String())
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging PostgreSQL at %s: %w", target.Host, err)
	}
	return &sqlEngine{
		cfg: cfg, kind: enginePostgres, db: db, targetTxt: target.Host,
		createSQL: func(int) string {
			return "CREATE TABLE " + sqlTableName + " (_id_ TEXT PRIMARY KEY, payload TEXT NOT NULL)"
		},
		insertSQL: "INSERT INTO " + sqlTableName + " (_id_, payload) VALUES ($1, $2)",
		getSQL:    "SELECT payload FROM " + sqlTableName + " WHERE _id_ = $1",
		variables: postgresVariables,
	}, nil
}

// postgresURL builds the postgresql:// URL from -dsn or from the address and
// credentials. sslmode=require encrypts without checking the certificate.
func postgresURL(cfg *config) (*url.URL, error) {
	target := &url.URL{Scheme: "postgresql"}
	if cfg.DSN != "" {
		parsed, err := url.Parse(cfg.DSN)
		if err != nil {
			return nil, errors.New("parsing -dsn: it is not a postgresql:// URL")
		}
		target = parsed
	} else {
		user := valueOr(cfg.User, "postgres")
		target.Host = cfg.Addr
		target.User = url.User(user)
		if cfg.Password != "" {
			target.User = url.UserPassword(user, cfg.Password)
		}
	}
	target.Path = "/" + cfg.Name
	query := target.Query()
	if query.Get("sslmode") == "" {
		query.Set("sslmode", map[bool]string{true: "require", false: "disable"}[cfg.TLS])
	}
	target.RawQuery = query.Encode()
	return target, nil
}

func postgresVariables(ctx context.Context, db *sql.DB) (map[string]string, error) {
	vars := map[string]string{}
	var version string
	if err := db.QueryRowContext(ctx, "SHOW server_version").Scan(&version); err != nil {
		return nil, fmt.Errorf("reading the PostgreSQL version: %w", err)
	}
	vars["version"] = version
	for _, setting := range []string{"synchronous_commit", "fsync", "full_page_writes"} {
		var value string
		if err := db.QueryRowContext(ctx, "SELECT current_setting($1)", setting).Scan(&value); err != nil {
			return nil, fmt.Errorf("reading PostgreSQL setting %s: %w", setting, err)
		}
		vars[setting] = value
	}
	return vars, nil
}
