package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

const (
	// pgDuplicateDatabase is the SQLSTATE of CREATE DATABASE on a name that exists.
	pgDuplicateDatabase = "42P04"
	// pgInsertRows bounds the rows of one multi-row INSERT: 500 rows of the widest table stay far
	// below the 65535 parameters one statement may bind.
	pgInsertRows = 500
	pgUnitBatch  = 50
)

// postgresSeeder loads a layout into tables with batched multi-row INSERTs.
type postgresSeeder struct {
	server  *pgx.ConnConfig
	workers int
	admin   *sql.DB
	db      *sql.DB
}

// postgresConfig parses -postgres-dsn and applies -postgres-tls. With the flag on the connection
// must be TLS: an sslmode that would fall back to plain text (the libpq default, prefer) is
// tightened to require, which encrypts without checking the certificate, the rule the hoardDB
// client follows; a verifying mode in the DSN is kept; sslmode=disable against the flag is refused.
// With the flag off the connection is plain text.
func postgresConfig(cfg config) (*pgx.ConnConfig, error) {
	target, err := pgx.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("parsing -postgres-dsn: %w", err)
	}
	if !cfg.PostgresTLS {
		target.TLSConfig = nil
		target.Fallbacks = nil
		return target, nil
	}
	if target.TLSConfig == nil {
		return nil, fmt.Errorf("-postgres-dsn names sslmode=disable for %s while -postgres-tls is on; remove sslmode=disable from the DSN, or pass -postgres-tls=false", target.Host)
	}
	if unverified := target.TLSConfig; unverified.InsecureSkipVerify && unverified.VerifyConnection == nil && unverified.VerifyPeerCertificate == nil {
		// require, prefer and allow check nothing; only the fallback to plain text differs. verify-ca
		// also sets InsecureSkipVerify (pgx checks the chain itself) but installs a verifier, so it stays.
		target.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, ServerName: target.Host} //nolint:gosec // self-signed certificates, the same rule as the hoardDB client (spec 3.1)
		target.Fallbacks = nil
	}
	return target, nil
}

func openPostgres(target *pgx.ConnConfig, workers int) *sql.DB {
	db := stdlib.OpenDB(*target)
	db.SetMaxOpenConns(workers)
	db.SetMaxIdleConns(workers)
	db.SetConnMaxLifetime(0)
	return db
}

func newPostgresSeeder(ctx context.Context, cfg config) (*postgresSeeder, error) {
	target, err := postgresConfig(cfg)
	if err != nil {
		return nil, err
	}
	admin := openPostgres(target, 1)
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("connecting to PostgreSQL at %s:%d (check -postgres-dsn and -postgres-tls): %w", target.Host, target.Port, err)
	}
	return &postgresSeeder{server: target, workers: cfg.Workers, admin: admin}, nil
}

func (s *postgresSeeder) close() {
	if s.db != nil {
		_ = s.db.Close()
	}
	_ = s.admin.Close()
}

// pgColumnType translates a column's MySQL type to PostgreSQL's: text is TEXT whatever its
// length (PostgreSQL stores both the same), a one-byte integer is SMALLINT and a BLOB is BYTEA.
func pgColumnType(mysqlType string) string {
	base := strings.TrimSuffix(mysqlType, " NOT NULL")
	switch {
	case strings.HasPrefix(base, "VARCHAR("):
		base = "TEXT"
	case base == "TINYINT":
		base = "SMALLINT"
	case base == "BLOB":
		base = "BYTEA"
	}
	return base + " NOT NULL"
}

// createTablePG renders one table as PostgreSQL DDL; secondary keys are separate CREATE INDEX
// statements because PostgreSQL has no inline KEY clause. No foreign keys, as on every engine here.
func createTablePG(spec table) string {
	lines := make([]string, 0, len(spec.Cols)+1)
	for _, col := range spec.Cols {
		lines = append(lines, fmt.Sprintf("  %s %s", col.Name, pgColumnType(col.SQLType)))
	}
	lines = append(lines, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(spec.PK, ", ")))
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n);", spec.Name, strings.Join(lines, ",\n"))
}

// createIndexesPG renders the secondary keys of a table.
func createIndexesPG(spec table) []string {
	out := make([]string, len(spec.Keys))
	for i, key := range spec.Keys {
		unique := ""
		if key.Unique {
			unique = "UNIQUE "
		}
		out[i] = fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);", unique, key.Name, spec.Name, strings.Join(key.Cols, ", "))
	}
	return out
}

// pgvectorNote says where a pgvector column would go. The extension is not required: no verb
// searches by vector yet, and a plain BYTEA holds the same 256 bytes the other engines hold.
const pgvectorNote = "-- With the pgvector extension installed, vec would be `vec vector(64) NOT NULL` (64 float32 values) with\n" +
	"-- `CREATE INDEX memories_by_vec ON memories USING hnsw (vec vector_l2_ops);`; this seed does not need it.\n"

// postgresDDL is the PostgreSQL schema text of a layout, each table under a comment that says
// which read its keys serve.
func postgresDDL(layout model.Layout, database string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "CREATE DATABASE %s ENCODING 'UTF8' TEMPLATE template0;\n-- psql: \\connect %s\n", database, database)
	for _, spec := range tables(layout) {
		fmt.Fprintf(&out, "-- %s: %s.\n", spec.Name, spec.Why)
		if spec.Name == "memories" {
			out.WriteString(pgvectorNote)
		}
		fmt.Fprintln(&out, createTablePG(spec))
		for _, statement := range createIndexesPG(spec) {
			fmt.Fprintln(&out, statement)
		}
	}
	return out.String()
}

// createSchema creates the layout's database and tables. A database that exists is refused
// with the way out.
func (s *postgresSeeder) createSchema(ctx context.Context, layout model.Layout, database string) error {
	// template0 so the database is UTF8 whatever the server's default encoding is.
	if _, err := s.admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s ENCODING 'UTF8' TEMPLATE template0", database)); err != nil {
		var serverError *pgconn.PgError
		if errors.As(err, &serverError) && serverError.Code == pgDuplicateDatabase {
			return fmt.Errorf("database %q already exists on this server, so it may already hold this layout and seeding again would collide with it. "+
				"Seed a fresh database with -database <name>, or drop it with: DROP DATABASE %s: %w", database, database, err)
		}
		return fmt.Errorf("creating database %q: %w", database, err)
	}
	withDatabase := s.server.Copy()
	withDatabase.Database = database
	s.db = openPostgres(withDatabase, s.workers)
	for _, spec := range tables(layout) {
		if _, err := s.db.ExecContext(ctx, createTablePG(spec)); err != nil {
			return fmt.Errorf("creating table %s: %w", spec.Name, err)
		}
		for _, statement := range createIndexesPG(spec) {
			if _, err := s.db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("creating an index on %s: %w", spec.Name, err)
			}
		}
	}
	return nil
}

// insertRows runs one multi-row INSERT per pgInsertRows rows.
func (s *postgresSeeder) insertRows(ctx context.Context, spec table, rows [][]any) error {
	columns := columnNames(spec)
	for len(rows) > 0 {
		batch := rows[:min(len(rows), pgInsertRows)]
		rows = rows[len(batch):]
		args := make([]any, 0, len(batch)*len(columns))
		groups := make([]string, len(batch))
		for i, row := range batch {
			placeholders := make([]string, len(columns))
			for j := range columns {
				placeholders[j] = "$" + strconv.Itoa(i*len(columns)+j+1)
			}
			groups[i] = "(" + strings.Join(placeholders, ",") + ")"
			args = append(args, row...)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", spec.Name, strings.Join(columns, ", "), strings.Join(groups, ","))
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("inserting %d rows into %s: %w", len(batch), spec.Name, err)
		}
	}
	return nil
}

// seed creates the database, loads the units in batches and verifies the row counts.
func (s *postgresSeeder) seed(ctx context.Context, gen *model.Generator, database string, progress *progress) (int64, error) {
	layout := gen.Layout()
	if err := s.createSchema(ctx, layout, database); err != nil {
		return 0, err
	}
	progress.phase("rows")
	specs := map[string]table{}
	for _, spec := range tables(layout) {
		specs[spec.Name] = spec
	}
	var written atomic.Int64
	err := pool(ctx, s.workers, chunks(gen.Units(), pgUnitBatch), func(ctx context.Context, batch []model.Unit) error {
		rows, err := rowsOf(layout, batch)
		if err != nil {
			return err
		}
		for name, tableRows := range rows {
			if err := s.insertRows(ctx, specs[name], tableRows); err != nil {
				return err
			}
			written.Add(int64(len(tableRows)))
			progress.add(int64(len(tableRows)))
		}
		return nil
	}, progress)
	if err != nil {
		return 0, fmt.Errorf("writing %s: %w", layout, err)
	}
	if err := s.verify(ctx, gen); err != nil {
		return 0, err
	}
	return written.Load(), nil
}

// verify counts the rows of every table, so a seed that lost rows does not report success.
func (s *postgresSeeder) verify(ctx context.Context, gen *model.Generator) error {
	want, err := expectedRows(gen.Layout(), gen.Counts())
	if err != nil {
		return err
	}
	for _, spec := range tables(gen.Layout()) {
		var got int64
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+spec.Name).Scan(&got); err != nil {
			return fmt.Errorf("counting table %s: %w", spec.Name, err)
		}
		if got != want[spec.Name] {
			return fmt.Errorf("table %s holds %d rows after the seed, want %d", spec.Name, got, want[spec.Name])
		}
	}
	return nil
}
