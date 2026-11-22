package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

const (
	// mysqlDatabaseExists and mysqlDeadlock are the server errors the seeder reacts to.
	mysqlDatabaseExists = 1007
	mysqlDeadlock       = 1213
	// mysqlInsertRows bounds the rows of one multi-row INSERT, which keeps a statement a few
	// megabytes at most; mysqlUnitBatch is how many units one task carries.
	mysqlInsertRows = 500
	mysqlUnitBatch  = 50
	// mysqlDeadlockAttempts bounds the tries of one INSERT, the first included.
	mysqlDeadlockAttempts = 10
)

// mysqlSeeder loads a layout into InnoDB tables with batched multi-row INSERTs.
type mysqlSeeder struct {
	server  *mysql.Config
	workers int
	admin   *sql.DB
	// data is opened on the layout's database once it exists.
	db *sql.DB
}

// mysqlConfig parses -mysql-dsn. With -mysql-tls on the connection is TLS with the certificate
// unchecked (tls=skip-verify), the rule the hoardDB client follows. A DSN that names a tls=
// value keeps it, except tls=false, which would silently undo the flag. Any database name in
// the DSN is dropped: the seeder creates the layout's own.
func mysqlConfig(cfg config) (*mysql.Config, error) {
	target, err := mysql.ParseDSN(cfg.MySQLDSN)
	if err != nil {
		return nil, fmt.Errorf("parsing -mysql-dsn: %w", err)
	}
	if cfg.MySQLTLS {
		switch target.TLSConfig {
		case "":
			target.TLSConfig = "skip-verify"
		case "false":
			return nil, fmt.Errorf("-mysql-dsn names tls=false for %s while -mysql-tls is on; remove tls=false from the DSN, or pass -mysql-tls=false", target.Addr)
		}
	}
	target.DBName = ""
	// Interpolating client-side makes a multi-row INSERT one round trip with no prepare.
	target.InterpolateParams = true
	return target, nil
}

func openMySQL(config *mysql.Config, workers int) (*sql.DB, error) {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("opening MySQL: %w", err)
	}
	db.SetMaxOpenConns(workers)
	db.SetMaxIdleConns(workers)
	db.SetConnMaxLifetime(0)
	return db, nil
}

func newMySQLSeeder(ctx context.Context, cfg config) (*mysqlSeeder, error) {
	target, err := mysqlConfig(cfg)
	if err != nil {
		return nil, err
	}
	admin, err := openMySQL(target, 1)
	if err != nil {
		return nil, err
	}
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("connecting to MySQL at %s (check -mysql-dsn and -mysql-tls): %w", target.Addr, err)
	}
	return &mysqlSeeder{server: target, workers: cfg.Workers, admin: admin}, nil
}

func (s *mysqlSeeder) close() {
	if s.db != nil {
		_ = s.db.Close()
	}
	_ = s.admin.Close()
}

// createSchema creates the layout's database and tables. A database that exists is refused
// with the way out.
func (s *mysqlSeeder) createSchema(ctx context.Context, layout model.Layout, database string) error {
	if _, err := s.admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s DEFAULT CHARACTER SET utf8mb4", database)); err != nil {
		var serverError *mysql.MySQLError
		if errors.As(err, &serverError) && serverError.Number == mysqlDatabaseExists {
			return fmt.Errorf("database %q already exists on this server, so it may already hold this layout and seeding again would collide with it. "+
				"Seed a fresh database with -database <name>, or drop it with: DROP DATABASE %s: %w", database, database, err)
		}
		return fmt.Errorf("creating database %q: %w", database, err)
	}
	withDatabase := s.server.Clone()
	withDatabase.DBName = database
	opened, err := openMySQL(withDatabase, s.workers)
	if err != nil {
		return err
	}
	s.db = opened
	for _, spec := range tables(layout) {
		if _, err := opened.ExecContext(ctx, createTable(spec)); err != nil {
			return fmt.Errorf("creating table %s: %w", spec.Name, err)
		}
	}
	return nil
}

// insertRows runs one multi-row INSERT per mysqlInsertRows rows.
func (s *mysqlSeeder) insertRows(ctx context.Context, spec table, rows [][]any) error {
	columns := columnNames(spec)
	group := "(" + strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",") + ")"
	for len(rows) > 0 {
		batch := rows[:min(len(rows), mysqlInsertRows)]
		rows = rows[len(batch):]
		args := make([]any, 0, len(batch)*len(columns))
		for _, row := range batch {
			args = append(args, row...)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", spec.Name, strings.Join(columns, ", "), strings.TrimSuffix(strings.Repeat(group+",", len(batch)), ","))
		if err := s.execRetryingDeadlock(ctx, spec.Name, query, args); err != nil {
			return fmt.Errorf("inserting %d rows into %s: %w", len(batch), spec.Name, err)
		}
	}
	return nil
}

// execRetryingDeadlock runs an INSERT, trying again when the server aborts it as a deadlock
// victim (parallel workers inserting into neighbouring index pages do this routinely).
func (s *mysqlSeeder) execRetryingDeadlock(ctx context.Context, tableName, query string, args []any) error {
	for attempt := 1; ; attempt++ {
		_, err := s.db.ExecContext(ctx, query, args...)
		var serverError *mysql.MySQLError
		if err == nil || !errors.As(err, &serverError) || serverError.Number != mysqlDeadlock {
			return err
		}
		if attempt == mysqlDeadlockAttempts {
			return fmt.Errorf("deadlock (MySQL error %d) on %s persisted through %d attempts: %w", mysqlDeadlock, tableName, attempt, err)
		}
		wait := time.NewTimer(time.Duration(attempt) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			wait.Stop()
			return fmt.Errorf("waiting to retry a deadlock on %s: %w", tableName, ctx.Err())
		case <-wait.C:
		}
	}
}

// seed creates the database, loads the units in batches and verifies the row counts.
func (s *mysqlSeeder) seed(ctx context.Context, gen *model.Generator, database string, progress *progress) (int64, error) {
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
	err := pool(ctx, s.workers, chunks(gen.Units(), mysqlUnitBatch), func(ctx context.Context, batch []model.Unit) error {
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
func (s *mysqlSeeder) verify(ctx context.Context, gen *model.Generator) error {
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
