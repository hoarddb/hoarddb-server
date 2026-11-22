package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// mysqlStore keeps the records in one InnoDB table keyed by id with a secondary
// index on owner. Statements are prepared once per connection pool.
type mysqlStore struct {
	db        *sql.DB
	flushMode string
	getStmt   *sql.Stmt
	ownerStmt *sql.Stmt
	updStmt   *sql.Stmt
	insStmt   *sql.Stmt
}

func newMySQLStore(ctx context.Context, dsn string, poolSize int) (*mysqlStore, error) {
	target, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing -mysql-dsn: %w", err)
	}
	if target.DBName == "" {
		return nil, fmt.Errorf("-mysql-dsn names no database; end it with /scale")
	}
	database := target.DBName
	target.DBName = ""
	admin, err := sql.Open("mysql", target.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("opening MySQL: %w", err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+database); err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("MySQL did not accept the connection at the -mysql-dsn given; is the container up (docker ps)? %w", err)
	}
	_ = admin.Close()
	target.DBName = database
	db, err := sql.Open("mysql", target.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("opening MySQL: %w", err)
	}
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)
	db.SetConnMaxLifetime(0)
	store := &mysqlStore{db: db}
	var flush string
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_flush_log_at_trx_commit").Scan(&flush); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("reading innodb_flush_log_at_trx_commit: %w", err)
	}
	store.flushMode = "innodb_flush_log_at_trx_commit=" + flush
	return store, nil
}

func (m *mysqlStore) name() string       { return "mysql" }
func (m *mysqlStore) durability() string { return m.flushMode }

func (m *mysqlStore) close() {
	for _, stmt := range []*sql.Stmt{m.getStmt, m.ownerStmt, m.updStmt, m.insStmt} {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	_ = m.db.Close()
}

func (m *mysqlStore) prepare(ctx context.Context) error {
	ddl := "CREATE TABLE IF NOT EXISTS records (id BIGINT NOT NULL PRIMARY KEY, owner BIGINT NOT NULL, seq BIGINT NOT NULL, " +
		"body VARBINARY(16000) NOT NULL, KEY idx_owner (owner)) ENGINE=InnoDB"
	if _, err := m.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("creating the records table: %w", err)
	}
	statements := []struct {
		target **sql.Stmt
		query  string
	}{
		{&m.getStmt, "SELECT id, owner, seq, body FROM records WHERE id = ?"},
		{&m.ownerStmt, "SELECT id, owner, seq, body FROM records WHERE owner = ?"},
		{&m.updStmt, "UPDATE records SET seq = ?, body = ? WHERE id = ?"},
		{&m.insStmt, "INSERT INTO records (id, owner, seq, body) VALUES (?, ?, 0, ?)"},
	}
	for _, one := range statements {
		stmt, err := m.db.PrepareContext(ctx, one.query)
		if err != nil {
			return fmt.Errorf("preparing %q: %w", one.query, err)
		}
		*one.target = stmt
	}
	return nil
}

// nextKey reads the highest key from the primary index, an O(log n) lookup; an
// exact COUNT(*) over millions of rows would stall the sweep between points.
func (m *mysqlStore) nextKey(ctx context.Context) (int64, error) {
	var n int64
	if err := m.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id) + 1, 0) FROM records").Scan(&n); err != nil {
		return 0, fmt.Errorf("counting records: %w", err)
	}
	return n, nil
}

// loadRange inserts multi-row INSERT IGNORE batches over several connections.
func (m *mysqlStore) loadRange(ctx context.Context, from, to int64, bodyBytes int) error {
	return parallelBatches(ctx, from, to, func(batchFrom, batchTo int64) error {
		var query strings.Builder
		query.WriteString("INSERT IGNORE INTO records (id, owner, seq, body) VALUES ")
		args := make([]any, 0, (batchTo-batchFrom)*3)
		for id := batchFrom; id < batchTo; id++ {
			if id > batchFrom {
				query.WriteByte(',')
			}
			query.WriteString("(?, ?, 0, ?)")
			rec := newRecord(id, bodyBytes)
			args = append(args, rec.ID, rec.Owner, rec.Body)
		}
		if _, err := m.db.ExecContext(ctx, query.String(), args...); err != nil {
			return fmt.Errorf("loading keys %d to %d: %w", batchFrom, batchTo, err)
		}
		return nil
	})
}

func (m *mysqlStore) get(ctx context.Context, id int64) error {
	var rec record
	var body []byte
	if err := m.getStmt.QueryRowContext(ctx, id).Scan(&rec.ID, &rec.Owner, &rec.Seq, &body); err != nil {
		return fmt.Errorf("reading key %d: %w", id, err)
	}
	return nil
}

func (m *mysqlStore) update(ctx context.Context, id, version int64, bodyBytes int) error {
	res, err := m.updStmt.ExecContext(ctx, version, bodyFor(id, version, bodyBytes), id)
	if err != nil {
		return fmt.Errorf("updating key %d: %w", id, err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return fmt.Errorf("updating key %d: no such record", id)
	}
	return nil
}

func (m *mysqlStore) insert(ctx context.Context, id int64, bodyBytes int) error {
	if _, err := m.insStmt.ExecContext(ctx, id, ownerOf(id), bodyFor(id, 0, bodyBytes)); err != nil {
		return fmt.Errorf("inserting key %d: %w", id, err)
	}
	return nil
}

func (m *mysqlStore) byOwner(ctx context.Context, owner int64) (int, error) {
	rows, err := m.ownerStmt.QueryContext(ctx, owner)
	if err != nil {
		return 0, fmt.Errorf("reading owner %d: %w", owner, err)
	}
	defer func() { _ = rows.Close() }()
	found := 0
	for rows.Next() {
		var rec record
		var body []byte
		if err := rows.Scan(&rec.ID, &rec.Owner, &rec.Seq, &body); err != nil {
			return 0, fmt.Errorf("reading owner %d: %w", owner, err)
		}
		found++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("reading owner %d: %w", owner, err)
	}
	return found, nil
}
