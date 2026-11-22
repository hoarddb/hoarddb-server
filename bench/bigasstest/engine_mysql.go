package main

// The MySQL adapter. Registered as "mysql"; the scenarios binary seeds the tables it reads
// (bench/scenarios/schema.go), one database per layout, <base>_<layout>.
//
// Connection. Config.URI is a go-sql-driver DSN, the line `engines.sh uri mysql` prints
// (user:password@tcp(host:3306)/...). Any database name in it is ignored: every statement
// names its database, so one connection serves all four layouts. Config.Params understands one
// key, tls: "on" (the default) connects over TLS with the certificate unchecked
// (tls=skip-verify) unless the DSN names a tls= value, and refuses tls=false in the DSN;
// "off" leaves the DSN alone. The adapter sets interpolateParams, so a statement is one round
// trip with no prepare step, as the seeder does, and refuses clientFoundRows=true because it
// reads RowsAffected to tell a new row (1) from a replaced one (2) or an identical one (0).
// Every virtual user's session pins one connection; the pool is sized to Config.VUsers plus
// two for Settings and the schema check.
//
// Table names. Each layout's tables and columns are hard-coded in engine_mysql_schema.go and
// compared with information_schema when a session opens the layout; a missing table, a
// different column list or a missing secondary key (follows_by_followee, tasks_ready) stops
// the run with the seed command to run. No table or column name is derived at run time.
//
// How the layouts map onto the tables.
//   - A fifo or lifo store is a table keyed (owner, seq); a push inserts seq = SeqBase +
//     Ordinal. A hash or btree store is keyed (owner, key); a put is INSERT ... ON DUPLICATE
//     KEY UPDATE. Pages read oldest first: ORDER BY seq (or by the key) ASC LIMIT n.
//   - The paired stores keep one row: follows holds User.Following and User.Followers, likes
//     holds Post.Likes and User.Likes, members holds Room.Members, User.Rooms and the Member
//     document. A write that names both sides, or a side and the document, writes the row
//     once, and the "newly created" outcome of that row counts for every name it was given.
//   - The feed is one join (feed with posts), the room page is two statements (messages;
//     members joined with users for the names).
//   - Followers is follows_by_followee read as Followers elements.
//   - Run.Tasks is the queue with the state and lease_until columns. A claim is one
//     transaction: SELECT ... WHERE state = 0 ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED,
//     state = 1 (leased), state = 2 (acked), then the replacement push. An acked row stays in
//     the table, so Count of Run.Tasks counts state = 0 rows only. A claim that finds nothing
//     rolls back, applies nothing and returns no elements.
//     (Written tasks sort by seq = SeqBase + ordinal, so a replacement from a lower-numbered
//     virtual user is claimed before an older one from a higher-numbered user; hoardDB claims in
//     arrival order.)
//
// Counters. Each logical write is one transaction (Reply.Commits = 1) that includes the
// counter columns of WriteOp.Counters. A delta is applied only when the live schema declares
// its column, and a delta that names an element index only when that element created its row
// (an INSERT affecting 1 row; every push does). Deltas on one document become one UPDATE.
// The planner sends agent.memory's users.memory_n delta, but no delta for the task queue
// (runs.task_n), so that column does not move; the adapter does not invent it.
//
// Round trips. Reply.RoundTrips counts every statement sent, BEGIN and COMMIT included, and
// the PreRead selects (run before the transaction). A write is never pipelined: one statement
// per element, one per counter row.
//
// Deadlocks. Counter rows are updated in table and id order, so two writes over the same
// documents cannot lock them in opposite orders. A claim and its replacement task on one run's
// queue can still deadlock under many virtual users (the SKIP LOCKED scan and the insert take
// gap locks); the server aborts the victim with error 1213, which MySQL says to retry. The
// adapter reruns that whole transaction at most mysqlDeadlockAttempts times in all and counts
// each rerun in Reply.Retries, so the record is labelled "retries". Nothing else is retried.
//
// Bodies are stored and returned exactly as the WriteOp gives them: integers are BIGINT and
// come back as int64, text as string, the memory vector as the 256 raw bytes of a BLOB.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// mysqlSchemaCheckTimeout bounds the schema check that holds the engine's mutex.
const mysqlSchemaCheckTimeout = time.Minute

// mysqlSettingVariables are the server variables Settings reads back.
var mysqlSettingVariables = []string{
	"version_comment", "innodb_flush_log_at_trx_commit", "sync_binlog", "log_bin", "binlog_format",
	"innodb_buffer_pool_size", "innodb_flush_method", "innodb_doublewrite", "innodb_redo_log_capacity",
	"innodb_io_capacity", "innodb_io_capacity_max", "innodb_log_file_size", "innodb_log_buffer_size",
	"max_connections", "transaction_isolation", "character_set_server", "collation_server",
	"innodb_read_io_threads", "innodb_write_io_threads", "innodb_flush_neighbors", "innodb_adaptive_hash_index",
}

func init() { register("mysql", openMySQLEngine) }

// mysqlEngine is a MySQL server under test.
type mysqlEngine struct {
	db *sql.DB

	mu      sync.Mutex
	layouts map[string]*mysqlLayout
}

// openMySQLEngine parses the connection, opens the pool and pings the server.
func openMySQLEngine(ctx context.Context, cfg Config) (Engine, error) {
	target, err := mysqlTarget(cfg)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(target)
	if err != nil {
		return nil, fmt.Errorf("building the MySQL connector: %w", err)
	}
	db := sql.OpenDB(connector)
	slots := max(cfg.VUsers, 1) + 2
	db.SetMaxOpenConns(slots)
	db.SetMaxIdleConns(slots)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connecting to MySQL at %s (check -uri and -param tls=on|off): %w", target.Addr, err)
	}
	return &mysqlEngine{db: db, layouts: map[string]*mysqlLayout{}}, nil
}

// mysqlTarget turns Config into a driver configuration.
func mysqlTarget(cfg Config) (*mysql.Config, error) {
	if cfg.URI == "" {
		return nil, errors.New("-uri is empty. Pass the DSN `engines.sh uri mysql` prints, user:password@tcp(host:3306)/")
	}
	tlsMode := "on"
	for key, value := range cfg.Params {
		if key != "tls" {
			return nil, fmt.Errorf("unknown -param %q for engine mysql. The only key is tls=on|off", key)
		}
		tlsMode = value
	}
	if tlsMode != "on" && tlsMode != "off" {
		return nil, fmt.Errorf("-param tls=%s: use tls=on (TLS, certificate unchecked) or tls=off", tlsMode)
	}
	target, err := mysql.ParseDSN(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("parsing -uri as a go-sql-driver DSN: %w", err)
	}
	if target.ClientFoundRows {
		return nil, errors.New("-uri sets clientFoundRows=true, which would hide whether an upsert created a row. Remove it from the DSN")
	}
	if tlsMode == "on" {
		switch target.TLSConfig {
		case "":
			target.TLSConfig = "skip-verify"
		case "false":
			return nil, fmt.Errorf("-uri names tls=false for %s while -param tls=on; remove tls=false from the DSN or pass -param tls=off", target.Addr)
		}
	}
	target.DBName = ""
	target.InterpolateParams = true
	return target, nil
}

// Settings reads the server's version, the TLS cipher of the connection and the variables
// that decide durability and caching.
func (e *mysqlEngine) Settings(ctx context.Context) (map[string]string, error) {
	out := map[string]string{"driver": "go-sql-driver/mysql", "interpolate_params": "true"}
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading MySQL settings: %w", err)
	}
	defer func() { _ = conn.Close() }()
	var version string
	if err := conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return nil, fmt.Errorf("reading the MySQL version: %w", err)
	}
	out["engine_version"] = version
	var name, cipher string
	switch err := conn.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); {
	case errors.Is(err, sql.ErrNoRows):
		cipher = ""
	case err != nil:
		return nil, fmt.Errorf("reading the MySQL TLS cipher: %w", err)
	}
	out["tls"] = cipher
	if cipher == "" {
		out["tls"] = "off"
	}
	rows, err := conn.QueryContext(ctx, "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('"+strings.Join(mysqlSettingVariables, "','")+"')")
	if err != nil {
		return nil, fmt.Errorf("reading MySQL variables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var variable, setting string
		if err := rows.Scan(&variable, &setting); err != nil {
			return nil, fmt.Errorf("reading a MySQL variable: %w", err)
		}
		out[variable] = setting
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading MySQL variables: %w", err)
	}
	return out, nil
}

// Session pins one connection of the pool to virtual user index.
func (e *mysqlEngine) Session(ctx context.Context, index int) (Session, error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening the MySQL connection of virtual user %d: %w", index, err)
	}
	return &mysqlSession{engine: e, conn: conn}, nil
}

// Close closes the pool.
func (e *mysqlEngine) Close() error { return e.db.Close() }

// mysqlSession is one virtual user's connection.
type mysqlSession struct {
	engine *mysqlEngine
	conn   *sql.Conn
}

// Layout returns the operations on one layout, after checking its tables once per engine.
func (s *mysqlSession) Layout(l model.Layout, database string) (LayoutOps, error) {
	layout, err := s.engine.layout(l, database)
	if err != nil {
		return nil, err
	}
	return &mysqlOps{engine: s.engine, conn: s.conn, lay: layout}, nil
}

// Close returns the connection to the pool.
func (s *mysqlSession) Close() error { return s.conn.Close() }

// layout builds, or returns, the checked layout of a database.
func (e *mysqlEngine) layout(l model.Layout, database string) (*mysqlLayout, error) {
	if err := validDatabaseName(database); err != nil {
		return nil, fmt.Errorf("layout %s: %w", l, err)
	}
	key := string(l) + "/" + database
	e.mu.Lock()
	defer e.mu.Unlock()
	if layout := e.layouts[key]; layout != nil {
		return layout, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), mysqlSchemaCheckTimeout)
	defer cancel()
	columns, err := e.checkSchema(ctx, l, database)
	if err != nil {
		return nil, err
	}
	layout := newMySQLLayout(l, database, columns)
	e.layouts[key] = layout
	return layout, nil
}

// checkSchema compares the live tables of a database with the hard-coded ones and returns the
// live column set of each table.
func (e *mysqlEngine) checkSchema(ctx context.Context, l model.Layout, database string) (map[string]map[string]bool, error) {
	rows, err := e.db.QueryContext(ctx, "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = ? ORDER BY table_name, ordinal_position", database)
	if err != nil {
		return nil, fmt.Errorf("reading the columns of database %s: %w", database, err)
	}
	live := map[string][]string{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("reading the columns of database %s: %w", database, err)
		}
		live[table] = append(live[table], column)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("reading the columns of database %s: %w", database, err)
	}
	seedHint := fmt.Sprintf("Seed it with: bigasstest seed -engine mysql -layout %s -database %s ... (the scenarios binary creates <base>_<layout>)", l, strings.TrimSuffix(database, "_"+string(l)))
	declared := map[string]map[string]bool{}
	for _, table := range sortedKeys(mysqlTables[l]) {
		want := mysqlTables[l][table]
		got, ok := live[table]
		if !ok {
			return nil, fmt.Errorf("database %s has no table %s. %s", database, table, seedHint)
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			return nil, fmt.Errorf("table %s.%s has columns (%s), the adapter expects (%s). The adapter's table list in engine_mysql_schema.go must follow bench/scenarios/schema.go", database, table, strings.Join(got, ", "), strings.Join(want, ", "))
		}
		declared[table] = map[string]bool{}
		for _, column := range got {
			declared[table][column] = true
		}
	}
	for table, index := range mysqlIndexes[l] {
		var found int
		if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = ?", database, table, index).Scan(&found); err != nil {
			return nil, fmt.Errorf("reading the keys of table %s.%s: %w", database, table, err)
		}
		if found == 0 {
			return nil, fmt.Errorf("table %s.%s has no key %s, which the reads rely on. %s", database, table, index, seedHint)
		}
	}
	return declared, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
