// bench/rank_postgres.go — the PostgreSQL leg of the keyed rank (rank_bench.go).
//
// Both keyed stores are tables under a VARCHAR primary key (a B-tree index):
// there is no durable hash analogue, so the "hash" and "btree" rows are the
// same structure on this engine, as on MySQL. The leg holds one *sql.Conn and
// server-side prepared statements, so each op is one request/response on one
// connection, never a pool. The commit sync is read from the server, not set.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver name
)

const rankPostgresSchema = "hoardrank"

func rankPostgresUpsert(table string) string {
	return "INSERT INTO " + table + " (id, seq, payload) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET seq = excluded.seq, payload = excluded.payload RETURNING (xmax = 0)"
}

func rankPostgresRead(table string) string {
	return "SELECT id, seq, payload FROM " + table + " WHERE id = $1"
}

func rankPostgresScan(table string) string {
	return "SELECT id, seq, payload FROM " + table + " WHERE id BETWEEN $1 AND $2 ORDER BY id"
}

type rankPostgres struct {
	ctx   context.Context
	db    *sql.DB
	conn  *sql.Conn
	legID rankLeg
}

func newRankPostgres(ctx context.Context, leg rankLeg) (*rankPostgres, error) {
	dsn, err := postgresBenchDSN()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening PostgreSQL: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connecting to PostgreSQL (check HOARDDB_BENCH_POSTGRES_DSN): %w", err)
	}
	engine := &rankPostgres{ctx: ctx, db: db, conn: conn, legID: leg}
	if err := engine.describe(); err != nil {
		_ = engine.close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+rankPostgresSchema); err != nil {
		_ = engine.close()
		return nil, fmt.Errorf("creating schema %s: %w", rankPostgresSchema, err)
	}
	return engine, nil
}

// rankPostgresLeg names the leg after the server's reported sync policy, so a
// run can never be labelled with a policy the server was not using.
func rankPostgresLeg(leg rankLeg, version, synchronousCommit, fsync string) rankLeg {
	leg.Engine = "PostgreSQL"
	if synchronousCommit != "on" || fsync != "on" {
		leg.Engine = "PostgreSQL no-fsync"
	}
	leg.Durability = fmt.Sprintf("postgres %s; autocommit; synchronous_commit=%s; fsync=%s", version, synchronousCommit, fsync)
	return leg
}

func (p *rankPostgres) describe() error {
	var version, synchronousCommit, fsync string
	row := p.conn.QueryRowContext(p.ctx,
		"SELECT current_setting('server_version'), current_setting('synchronous_commit'), current_setting('fsync')")
	if err := row.Scan(&version, &synchronousCommit, &fsync); err != nil {
		return fmt.Errorf("reading PostgreSQL durability settings: %w", err)
	}
	p.legID = rankPostgresLeg(p.legID, version, synchronousCommit, fsync)
	return nil
}

func (p *rankPostgres) leg() rankLeg { return p.legID }

// createdKnown is true: RETURNING (xmax = 0) is true for an inserted row and
// false for an updated one.
func (p *rankPostgres) createdKnown() bool { return true }

func (p *rankPostgres) close() error {
	return errors.Join(p.conn.Close(), p.db.Close())
}

func (p *rankPostgres) open(store string) (rankStore, error) {
	table := rankPostgresSchema + ".rank_" + store
	if _, err := p.conn.ExecContext(p.ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		return nil, fmt.Errorf("dropping table %s: %w", table, err)
	}
	if _, err := p.conn.ExecContext(p.ctx, "CREATE TABLE "+table+` (
		id      VARCHAR(16)  NOT NULL PRIMARY KEY,
		seq     BIGINT       NOT NULL,
		payload VARCHAR(128) NOT NULL
	)`); err != nil {
		return nil, fmt.Errorf("creating table %s: %w", table, err)
	}
	upsertStmt, err := p.conn.PrepareContext(p.ctx, rankPostgresUpsert(table))
	if err != nil {
		return nil, fmt.Errorf("preparing the upsert on %s: %w", table, err)
	}
	readStmt, err := p.conn.PrepareContext(p.ctx, rankPostgresRead(table))
	if err != nil {
		return nil, fmt.Errorf("preparing the point read on %s: %w", table, err)
	}
	scanStmt, err := p.conn.PrepareContext(p.ctx, rankPostgresScan(table))
	if err != nil {
		return nil, fmt.Errorf("preparing the range scan on %s: %w", table, err)
	}
	return &rankPostgresTable{ctx: p.ctx, upsertStmt: upsertStmt, readStmt: readStmt, scanStmt: scanStmt}, nil
}

type rankPostgresTable struct {
	ctx        context.Context
	upsertStmt *sql.Stmt
	readStmt   *sql.Stmt
	scanStmt   *sql.Stmt
}

func (t *rankPostgresTable) upsert(doc rankDoc) (bool, error) {
	var created bool
	if err := t.upsertStmt.QueryRowContext(t.ctx, doc.Key, doc.Seq, doc.Payload).Scan(&created); err != nil {
		return false, fmt.Errorf("upserting %s: %w", doc.Key, err)
	}
	return created, nil
}

func (t *rankPostgresTable) read(key string) (rankDoc, bool, error) {
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

func (t *rankPostgresTable) scan(low, high string) ([]rankDoc, error) {
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
