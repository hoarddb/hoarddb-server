package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// The PostgreSQL adapter for big-ass-test (internal benchmark, never published).
//
// Connection: -uri is the libpq URL `engines.sh uri postgres <VM>` prints (TLS with
// sslmode=require, scram-sha-256 login); the database in it is only where the first
// connection starts. The layouts are seeded into the databases <base>_social, _chat,
// _ledger and _agent, so a virtual user talks to four databases and PostgreSQL has no way to
// switch database on a connection. The adapter therefore keeps one pgx pool per database,
// shared by every virtual user. A pool is sized to the number of virtual users, but at most
// a quarter of what max_connections leaves free (less 20 connections of headroom for the
// seeder and the audit), so that 128 virtual users fit under the 200 connections engines.sh
// configures. Past that bound a request waits for a free connection, and the wait is part
// of its latency. -param pool_max=N sets the per-database bound. That is the only parameter.
//
// What is written: the SQL schema keeps one row for each of three pairs of stores (follows
// for Following and Followers, likes for Post.Likes and User.Likes, members for Room.Members
// and User.Rooms), so a write that names both sides makes one row, once. One logical write is
// one transaction: its rows, its denormalised counters (only those the seeded schema
// declares, and only when the element they name was newly created, which a keyed put reports
// through RETURNING (xmax = 0)) and, for agent.task, the claim. Counters of one document are
// one UPDATE, and the transaction updates its counter rows in table and id order, so two
// writes cannot wait for each other's rows in opposite orders. Rows go in with seq =
// SeqBase + Ordinal.
//
// What is read: pages are ORDER BY seq or key ascending with LIMIT, oldest first. Keys are
// ordered COLLATE "C" unless the database already has the C collation, so that a key sorts
// in byte order as in the model. Where a read needs two tables the adapter issues one join
// (the feed with its posts, the members of a room with their names), which is how a SQL
// application reads them; hoardDB reads the same data in several round trips and the
// difference is a fact about the layouts.
//
// Claim (agent.task): SELECT ... FOR UPDATE SKIP LOCKED takes the oldest task with state 0,
// then state 1 (leased) and state 2 (acknowledged), then the replacement task is inserted,
// all in one transaction. A store holds the rows with state 0, which is what Count reports.
// Written tasks sort by seq = SeqBase + ordinal, so a replacement from a lower-numbered virtual user is claimed before an older one from a higher-numbered user; hoardDB claims in arrival order.
//
// RoundTrips counts every statement the adapter sends, BEGIN and COMMIT included, one each.
// pgx prepares a statement on first use on a connection (one extra round trip, not counted);
// the cache then serves every later call. Commits is 1 for a write, 0 for a read.
//
// The adapter never retries. A deadlock (40P01) or serialization failure (40001) comes back
// as the operation's error and is counted as an error in the record: Reply has no field
// that could carry a retry count, and Settings is read before the run, so a hidden retry
// would be invisible.
//
// Settings returns engine_version, tls (the cipher in use, or "off"), pool_max_per_database
// (the effective cap, and pool_max_source says whether pool_max or the formula above set it)
// and the pg_settings rows in pgRecordedSettings as pg.<name>: an allowlist of durability,
// memory, WAL, checkpoint and TLS settings, so no listen address, file path or access rule
// reaches a record.
func init() { register("postgres", openPostgresEngine) }

const (
	// pgConnHeadroom is the share of max_connections kept free of the pools.
	pgConnHeadroom = 20
	// pgLayoutCount is how many databases share max_connections.
	pgLayoutCount = 4
)

// pgRecordedSettings are the pg_settings rows a record keeps.
var pgRecordedSettings = []string{
	"fsync", "synchronous_commit", "full_page_writes", "wal_sync_method", "wal_level", "wal_buffers",
	"wal_compression", "wal_writer_delay", "wal_writer_flush_after", "wal_log_hints", "commit_delay",
	"commit_siblings", "max_wal_size", "min_wal_size", "data_checksums", "wal_segment_size", "block_size",
	"checkpoint_timeout", "checkpoint_completion_target", "checkpoint_flush_after", "checkpoint_warning",
	"bgwriter_delay", "bgwriter_lru_maxpages", "bgwriter_flush_after",
	"shared_buffers", "effective_cache_size", "work_mem", "maintenance_work_mem", "temp_buffers", "huge_pages",
	"max_connections", "superuser_reserved_connections", "max_worker_processes", "max_parallel_workers",
	"max_parallel_workers_per_gather", "max_prepared_transactions", "max_wal_senders", "archive_mode",
	"autovacuum", "random_page_cost", "effective_io_concurrency", "default_transaction_isolation",
	"track_commit_timestamp",
	"ssl", "ssl_min_protocol_version", "ssl_max_protocol_version", "ssl_ciphers", "ssl_prefer_server_ciphers",
	"ssl_ecdh_curve",
}

// pgEngine is one PostgreSQL server under test.
type pgEngine struct {
	uri     string
	poolMax int32
	// poolSource is "param" when -param pool_max set poolMax, "derived" otherwise.
	poolSource string

	mu        sync.Mutex
	databases map[string]*pgDatabase
}

// pgDatabase is a pool onto one layout database, with what was learned when it was opened.
type pgDatabase struct {
	pool     *pgxpool.Pool
	layout   model.Layout
	collateC bool
}

func openPostgresEngine(ctx context.Context, cfg Config) (Engine, error) {
	if cfg.URI == "" {
		return nil, errors.New("-uri is empty. Pass the URL that 'engines.sh uri postgres <VM>' prints, postgres://user:password@host:port/database?sslmode=require")
	}
	poolMax := int32(0)
	for name, value := range cfg.Params {
		if name != "pool_max" {
			return nil, fmt.Errorf("unknown -param %q for engine postgres; the only one is pool_max=N (connections per layout database)", name)
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("-param pool_max=%q is not a positive number", value)
		}
		poolMax = int32(n)
	}
	connConfig, err := pgx.ParseConfig(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("parsing -uri (it must be a postgres:// URL, 'engines.sh uri postgres <VM>' prints one): %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("connecting to PostgreSQL at %s:%d (is it running? 'engines.sh start postgres single <VM> --level L-A'; is -uri the one 'engines.sh uri postgres <VM>' prints?): %w", connConfig.Host, connConfig.Port, err)
	}
	var free int
	err = conn.QueryRow(ctx, "SELECT current_setting('max_connections')::int - current_setting('superuser_reserved_connections')::int").Scan(&free)
	if closeErr := conn.Close(ctx); closeErr != nil && err == nil {
		err = fmt.Errorf("closing the probe connection: %w", closeErr)
	}
	if err != nil {
		return nil, fmt.Errorf("reading max_connections: %w", err)
	}
	poolSource := "param"
	if poolMax == 0 {
		poolSource = "derived"
		poolMax = int32(max(1, min(cfg.VUsers, (free-pgConnHeadroom)/pgLayoutCount)))
	}
	return &pgEngine{uri: cfg.URI, poolMax: poolMax, poolSource: poolSource, databases: map[string]*pgDatabase{}}, nil
}

// Settings reads the server back for the record.
func (e *pgEngine) Settings(ctx context.Context) (map[string]string, error) {
	connConfig, err := pgx.ParseConfig(e.uri)
	if err != nil {
		return nil, fmt.Errorf("parsing -uri: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("connecting to read the settings: %w", err)
	}
	out := map[string]string{"pool_max_per_database": strconv.Itoa(int(e.poolMax)), "pool_max_source": e.poolSource}
	readErr := func() error {
		var version, cipher string
		if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
			return fmt.Errorf("reading version(): %w", err)
		}
		out["engine_version"] = version
		err := conn.QueryRow(ctx, "SELECT coalesce((SELECT CASE WHEN ssl THEN cipher END FROM pg_stat_ssl WHERE pid = pg_backend_pid()), 'off')").Scan(&cipher)
		if err != nil {
			return fmt.Errorf("reading the TLS cipher: %w", err)
		}
		out["tls"] = cipher
		rows, err := conn.Query(ctx, "SELECT name, coalesce(setting, '') FROM pg_settings WHERE name = ANY($1) ORDER BY name", pgRecordedSettings)
		if err != nil {
			return fmt.Errorf("reading pg_settings: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name, setting string
			if err := rows.Scan(&name, &setting); err != nil {
				return fmt.Errorf("reading a pg_settings row: %w", err)
			}
			out["pg."+name] = setting
		}
		return rows.Err()
	}()
	closeErr := conn.Close(ctx)
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("closing the settings connection: %w", closeErr)
	}
	return out, nil
}

// Session hands out a virtual user's handle; the connections live in the shared pools.
func (e *pgEngine) Session(context.Context, int) (Session, error) { return &pgSession{engine: e}, nil }

// Close closes every pool.
func (e *pgEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, db := range e.databases {
		db.pool.Close()
	}
	e.databases = map[string]*pgDatabase{}
	return nil
}

// database returns the pool of a layout database, opening it and checking its schema the
// first time.
func (e *pgEngine) database(ctx context.Context, layout model.Layout, name string) (*pgDatabase, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if db, ok := e.databases[name]; ok {
		if db.layout != layout {
			return nil, fmt.Errorf("database %s was opened for layout %s and is now asked for as %s", name, db.layout, layout)
		}
		return db, nil
	}
	poolConfig, err := pgxpool.ParseConfig(e.uri)
	if err != nil {
		return nil, fmt.Errorf("parsing -uri: %w", err)
	}
	poolConfig.ConnConfig.Database = name
	poolConfig.MaxConns = e.poolMax
	poolConfig.MinConns = 0
	// A benchmark window is minutes long; a pool that retires connections would measure the reconnects.
	poolConfig.MaxConnLifetime = 24 * time.Hour
	poolConfig.MaxConnIdleTime = 24 * time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("opening a pool on database %s: %w", name, err)
	}
	collateC, err := checkPostgresSchema(ctx, pool, layout, name)
	if err != nil {
		pool.Close()
		return nil, err
	}
	db := &pgDatabase{pool: pool, layout: layout, collateC: collateC}
	e.databases[name] = db
	return db, nil
}

// checkPostgresSchema compares the live tables with what the adapter hard-codes, and reports
// whether the database already sorts text in byte order.
func checkPostgresSchema(ctx context.Context, pool *pgxpool.Pool, layout model.Layout, database string) (bool, error) {
	want, err := pgMaps[layout].expectedColumns()
	if err != nil {
		return false, fmt.Errorf("layout %s: %w", layout, err)
	}
	rows, err := pool.Query(ctx, "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = current_schema()")
	if err != nil {
		return false, fmt.Errorf("reading the columns of database %s (does it exist? seed it with 'bigasstest seed -engine postgres -database <base> ...'): %w", database, err)
	}
	have := map[string][]string{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			rows.Close()
			return false, fmt.Errorf("reading a column of database %s: %w", database, err)
		}
		have[table] = append(have[table], column)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("reading the columns of database %s: %w", database, err)
	}
	tables := make([]string, 0, len(want))
	for table := range want {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		cols := have[table]
		slices.Sort(cols)
		if len(cols) == 0 {
			return false, fmt.Errorf("database %s has no table %q, which the %s layout needs. Seed it with 'bigasstest seed -engine postgres' (the scenarios binary creates the tables)", database, table, layout)
		}
		if !slices.Equal(cols, want[table]) {
			return false, fmt.Errorf("table %s of database %s has columns %v, the adapter was written for %v. The seeded schema (bench/scenarios/schema.go) changed: update engine_postgres_map.go", table, database, cols, want[table])
		}
	}
	var collation string
	if err := pool.QueryRow(ctx, "SELECT datcollate FROM pg_database WHERE datname = current_database()").Scan(&collation); err != nil {
		return false, fmt.Errorf("reading the collation of database %s: %w", database, err)
	}
	return collation == "C" || collation == "POSIX", nil
}

// pgSession is one virtual user's handle.
type pgSession struct{ engine *pgEngine }

// Layout returns the operations on one layout database.
func (s *pgSession) Layout(l model.Layout, database string) (LayoutOps, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := s.engine.database(ctx, l, database)
	if err != nil {
		return nil, err
	}
	return newPostgresLayout(db, l), nil
}

// Close has nothing to release: the connections belong to the engine's pools.
func (s *pgSession) Close() error { return nil }
