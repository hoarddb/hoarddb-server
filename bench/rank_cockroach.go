// bench/rank_cockroach.go — the CockroachDB leg of the keyed rank (rank_bench.go).
//
// Both keyed stores are tables under a STRING primary key, which CockroachDB
// keeps ordered in its LSM: there is no hash analogue, so the "hash" and
// "btree" rows are the same structure on this engine, as on MySQL. The leg
// holds one *sql.Conn and server-side prepared statements, so each op is one
// request/response on one connection, never a pool. The commit sync is the
// server default and is read, not set.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver name
)

const rankCockroachDatabase = "hoardrank"

// cockroachBenchDSN is read from the environment so no credential is stored in
// the repository. Example: postgresql://root@localhost:26257/defaultdb?sslmode=disable
func cockroachBenchDSN() (string, error) {
	dsn := os.Getenv("HOARDDB_BENCH_COCKROACH_DSN")
	if dsn == "" {
		return "", errors.New("HOARDDB_BENCH_COCKROACH_DSN is not set. Example: " +
			"HOARDDB_BENCH_COCKROACH_DSN='postgresql://root@localhost:26257/defaultdb?sslmode=disable' hoarddb-bench -db=cockroach")
	}
	return dsn, nil
}

func rankCockroachUpsert(table string) string {
	return "INSERT INTO " + table + " (id, seq, payload) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET seq = excluded.seq, payload = excluded.payload"
}

func rankCockroachRead(table string) string {
	return "SELECT id, seq, payload FROM " + table + " WHERE id = $1"
}

func rankCockroachScan(table string) string {
	return "SELECT id, seq, payload FROM " + table + " WHERE id BETWEEN $1 AND $2 ORDER BY id"
}

type rankCockroach struct {
	ctx   context.Context
	db    *sql.DB
	conn  *sql.Conn
	legID rankLeg
}

func newRankCockroach(ctx context.Context, leg rankLeg) (*rankCockroach, error) {
	dsn, err := cockroachBenchDSN()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening CockroachDB: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connecting to CockroachDB: %w", err)
	}
	engine := &rankCockroach{ctx: ctx, db: db, conn: conn, legID: leg}
	if err := engine.describe(); err != nil {
		_ = engine.close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+rankCockroachDatabase); err != nil {
		_ = engine.close()
		return nil, fmt.Errorf("creating database %s: %w", rankCockroachDatabase, err)
	}
	return engine, nil
}

// rankCockroachLeg names the leg after the server's reported sync policy, so a
// run can never be labelled with a policy the server was not using.
func rankCockroachLeg(leg rankLeg, version, syncDisabled string) rankLeg {
	leg.Engine = "CockroachDB"
	if syncDisabled == "true" {
		leg.Engine = "CockroachDB no-fsync"
	}
	leg.Durability = fmt.Sprintf("%s; autocommit; serializable; log synced before commit (server default); kv.raft_log.disable_synchronization_unsafe=%s", version, syncDisabled)
	return leg
}

func (c *rankCockroach) describe() error {
	var version, syncDisabled string
	if err := c.conn.QueryRowContext(c.ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("reading the CockroachDB version: %w", err)
	}
	if err := c.conn.QueryRowContext(c.ctx, "SHOW CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe").Scan(&syncDisabled); err != nil {
		return fmt.Errorf("reading the CockroachDB log sync setting: %w", err)
	}
	c.legID = rankCockroachLeg(c.legID, version, syncDisabled)
	return nil
}

func (c *rankCockroach) leg() rankLeg { return c.legID }

// createdKnown is false: an upsert reports one affected row for a new record
// and for an update alike.
func (c *rankCockroach) createdKnown() bool { return false }

func (c *rankCockroach) close() error {
	return errors.Join(c.conn.Close(), c.db.Close())
}

func (c *rankCockroach) open(store string) (rankStore, error) {
	table := rankCockroachDatabase + ".rank_" + store
	// Dropping and recreating gives every leg fresh state.
	if _, err := c.conn.ExecContext(c.ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		return nil, fmt.Errorf("dropping table %s: %w", table, err)
	}
	if _, err := c.conn.ExecContext(c.ctx, "CREATE TABLE "+table+` (
		id      VARCHAR(16)  NOT NULL PRIMARY KEY,
		seq     INT8         NOT NULL,
		payload VARCHAR(128) NOT NULL
	)`); err != nil {
		return nil, fmt.Errorf("creating table %s: %w", table, err)
	}
	upsertStmt, err := c.conn.PrepareContext(c.ctx, rankCockroachUpsert(table))
	if err != nil {
		return nil, fmt.Errorf("preparing the upsert on %s: %w", table, err)
	}
	readStmt, err := c.conn.PrepareContext(c.ctx, rankCockroachRead(table))
	if err != nil {
		return nil, fmt.Errorf("preparing the point read on %s: %w", table, err)
	}
	scanStmt, err := c.conn.PrepareContext(c.ctx, rankCockroachScan(table))
	if err != nil {
		return nil, fmt.Errorf("preparing the range scan on %s: %w", table, err)
	}
	return &rankCockroachTable{ctx: c.ctx, upsertStmt: upsertStmt, readStmt: readStmt, scanStmt: scanStmt}, nil
}

type rankCockroachTable struct {
	ctx        context.Context
	upsertStmt *sql.Stmt
	readStmt   *sql.Stmt
	scanStmt   *sql.Stmt
}

func (t *rankCockroachTable) upsert(doc rankDoc) (bool, error) {
	if _, err := t.upsertStmt.ExecContext(t.ctx, doc.Key, doc.Seq, doc.Payload); err != nil {
		return false, fmt.Errorf("upserting %s: %w", doc.Key, err)
	}
	return false, nil
}

func (t *rankCockroachTable) read(key string) (rankDoc, bool, error) {
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

func (t *rankCockroachTable) scan(low, high string) ([]rankDoc, error) {
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
