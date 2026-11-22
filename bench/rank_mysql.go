// bench/rank_mysql.go — the MySQL leg of the keyed rank (rank_bench.go).
//
// Both keyed stores are InnoDB tables under a VARCHAR PRIMARY KEY, which InnoDB
// clusters in a B+tree: there is no durable hash analogue, so the "hash" and
// "btree" rows are the same structure on this engine and are reported as such.
// The leg holds one *sql.Conn and server-side prepared statements, so each op
// is one request/response on one connection — never a pool.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

const rankMySQLDatabase = "hoardRank"

type rankMySQL struct {
	ctx   context.Context
	db    *sql.DB
	conn  *sql.Conn
	legID rankLeg
}

func newRankMySQL(ctx context.Context, leg rankLeg) (*rankMySQL, error) {
	db, err := sql.Open("mysql", mysqlDSN())
	if err != nil {
		return nil, fmt.Errorf("opening MySQL: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connecting to MySQL: %w", err)
	}
	engine := &rankMySQL{ctx: ctx, db: db, conn: conn, legID: leg}
	if err := engine.describe(); err != nil {
		_ = engine.close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+rankMySQLDatabase); err != nil {
		_ = engine.close()
		return nil, fmt.Errorf("creating database %s: %w", rankMySQLDatabase, err)
	}
	return engine, nil
}

// describe reads the settings that decide when a committed row is on disk and
// names the leg after them, so a run can never be labelled with a sync policy
// the server was not actually using.
func (m *rankMySQL) describe() error {
	var version, flushMethod string
	var flushLogAtCommit, syncBinlog, logBin int
	row := m.conn.QueryRowContext(m.ctx,
		"SELECT @@version, @@innodb_flush_log_at_trx_commit, @@sync_binlog, @@log_bin, @@innodb_flush_method")
	if err := row.Scan(&version, &flushLogAtCommit, &syncBinlog, &logBin, &flushMethod); err != nil {
		return fmt.Errorf("reading MySQL durability settings: %w", err)
	}
	m.legID.Engine = "MySQL"
	if flushLogAtCommit != 1 || (logBin == 1 && syncBinlog != 1) {
		m.legID.Engine = "MySQL no-fsync"
	}
	m.legID.Durability = fmt.Sprintf("mysqld %s; autocommit; innodb_flush_log_at_trx_commit=%d; log_bin=%d; sync_binlog=%d; innodb_flush_method=%s",
		version, flushLogAtCommit, logBin, syncBinlog, flushMethod)
	return nil
}

func (m *rankMySQL) leg() rankLeg { return m.legID }

// createdKnown is true: INSERT … ON DUPLICATE KEY UPDATE reports one affected
// row for a new record and two for an update.
func (m *rankMySQL) createdKnown() bool { return true }

func (m *rankMySQL) close() error {
	return errors.Join(m.conn.Close(), m.db.Close())
}

func (m *rankMySQL) open(store string) (rankStore, error) {
	table := rankMySQLDatabase + ".rank_" + store
	// Dropping and recreating gives every leg fresh state.
	if _, err := m.conn.ExecContext(m.ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		return nil, fmt.Errorf("dropping table %s: %w", table, err)
	}
	if _, err := m.conn.ExecContext(m.ctx, "CREATE TABLE "+table+` (
		id      VARCHAR(16)  NOT NULL PRIMARY KEY,
		seq     BIGINT       NOT NULL,
		payload VARCHAR(128) NOT NULL
	) ENGINE=InnoDB`); err != nil {
		return nil, fmt.Errorf("creating table %s: %w", table, err)
	}
	upsertStmt, err := m.conn.PrepareContext(m.ctx,
		"INSERT INTO "+table+" (id, seq, payload) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE seq = VALUES(seq), payload = VALUES(payload)")
	if err != nil {
		return nil, fmt.Errorf("preparing the upsert on %s: %w", table, err)
	}
	readStmt, err := m.conn.PrepareContext(m.ctx, "SELECT id, seq, payload FROM "+table+" WHERE id = ?")
	if err != nil {
		return nil, fmt.Errorf("preparing the point read on %s: %w", table, err)
	}
	scanStmt, err := m.conn.PrepareContext(m.ctx, "SELECT id, seq, payload FROM "+table+" WHERE id BETWEEN ? AND ? ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("preparing the range scan on %s: %w", table, err)
	}
	return &rankMySQLTable{ctx: m.ctx, upsertStmt: upsertStmt, readStmt: readStmt, scanStmt: scanStmt}, nil
}

type rankMySQLTable struct {
	ctx        context.Context
	upsertStmt *sql.Stmt
	readStmt   *sql.Stmt
	scanStmt   *sql.Stmt
}

func (t *rankMySQLTable) upsert(doc rankDoc) (bool, error) {
	execResult, err := t.upsertStmt.ExecContext(t.ctx, doc.Key, doc.Seq, doc.Payload)
	if err != nil {
		return false, fmt.Errorf("upserting %s: %w", doc.Key, err)
	}
	affected, err := execResult.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("reading rows affected for %s: %w", doc.Key, err)
	}
	return affected == 1, nil
}

func (t *rankMySQLTable) read(key string) (rankDoc, bool, error) {
	var stored rankDoc
	err := t.readStmt.QueryRowContext(t.ctx, key).Scan(&stored.Key, &stored.Seq, &stored.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return rankDoc{}, false, nil
	}
	if err != nil {
		return rankDoc{}, false, fmt.Errorf("selecting %s: %w", key, err)
	}
	return stored, true, nil
}

func (t *rankMySQLTable) scan(low, high string) ([]rankDoc, error) {
	rows, err := t.scanStmt.QueryContext(t.ctx, low, high)
	if err != nil {
		return nil, fmt.Errorf("selecting id BETWEEN %s AND %s: %w", low, high, err)
	}
	var band []rankDoc
	for rows.Next() {
		var stored rankDoc
		if err := rows.Scan(&stored.Key, &stored.Seq, &stored.Payload); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning a row of [%s, %s]: %w", low, high, err)
		}
		band = append(band, stored)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("reading the rows of [%s, %s]: %w", low, high, err)
	}
	return band, nil
}
