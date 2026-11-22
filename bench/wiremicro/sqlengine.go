package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// sqlTableName is the table both SQL engines use. The key column is _id_ and
// the one data column is payload, the same two fields every engine stores.
const sqlTableName = "doc"

// sqlEngine is MySQL (InnoDB, autocommit) or CockroachDB (one implicit
// transaction per statement) behind database/sql. Each worker holds its own
// *sql.Conn, so the parallel phases measure -workers connections.
type sqlEngine struct {
	cfg       *config
	kind      string
	db        *sql.DB
	targetTxt string
	createSQL func(payloadBytes int) string
	insertSQL string
	getSQL    string
	variables func(ctx context.Context, db *sql.DB) (map[string]string, error)
}

func newMySQLEngine(ctx context.Context, cfg *config) (*sqlEngine, error) {
	target, err := mysqlConfig(cfg)
	if err != nil {
		return nil, err
	}
	bare := target.Clone()
	bare.DBName = ""
	admin, err := sql.Open("mysql", bare.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("opening MySQL: %w", err)
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+target.DBName+"` CHARACTER SET utf8mb4"); err != nil {
		return nil, fmt.Errorf("creating database %q (the user needs CREATE, or create it by hand): %w", target.DBName, err)
	}
	db, err := sql.Open("mysql", target.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("opening MySQL: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging MySQL at %s (check -addr, -user and %s): %w", target.Addr, envPassword, err)
	}
	return &sqlEngine{
		cfg: cfg, kind: engineMySQL, db: db, targetTxt: target.Addr,
		createSQL: func(int) string {
			return "CREATE TABLE " + sqlTableName + " (_id_ CHAR(36) CHARACTER SET ascii NOT NULL PRIMARY KEY, payload MEDIUMTEXT NOT NULL) ENGINE=InnoDB"
		},
		insertSQL: "INSERT INTO " + sqlTableName + " (_id_, payload) VALUES (?, ?)",
		getSQL:    "SELECT payload FROM " + sqlTableName + " WHERE _id_ = ?",
		variables: mysqlVariables,
	}, nil
}

// mysqlConfig builds the driver configuration from -dsn or from the address
// and credentials, with the database name and the TLS rule applied.
func mysqlConfig(cfg *config) (*mysql.Config, error) {
	target := mysql.NewConfig()
	if cfg.DSN != "" {
		parsed, err := mysql.ParseDSN(cfg.DSN)
		if err != nil {
			return nil, fmt.Errorf("parsing -dsn: %w", err)
		}
		target = parsed
	} else {
		target.Net, target.Addr = "tcp", cfg.Addr
		target.User, target.Passwd = valueOr(cfg.User, "root"), cfg.Password
	}
	target.DBName = cfg.Name
	if cfg.TLS && target.TLSConfig == "" {
		target.TLSConfig = "skip-verify"
	}
	return target, nil
}

func mysqlVariables(ctx context.Context, db *sql.DB) (map[string]string, error) {
	var version, flushLog, syncBinlog, logBin string
	row := db.QueryRowContext(ctx, "SELECT @@version, @@innodb_flush_log_at_trx_commit, @@sync_binlog, @@log_bin")
	if err := row.Scan(&version, &flushLog, &syncBinlog, &logBin); err != nil {
		return nil, fmt.Errorf("querying MySQL variables: %w", err)
	}
	return map[string]string{
		"version": version, "innodb_flush_log_at_trx_commit": flushLog, "sync_binlog": syncBinlog, "log_bin": logBin,
	}, nil
}

func newCockroachEngine(ctx context.Context, cfg *config) (*sqlEngine, error) {
	target, err := cockroachURL(cfg)
	if err != nil {
		return nil, err
	}
	bare := *target
	bare.Path = "/defaultdb"
	admin, err := openPgx(bare.String())
	if err != nil {
		return nil, err
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+pgx.Identifier{cfg.Name}.Sanitize()); err != nil {
		return nil, fmt.Errorf("creating database %q (check -addr and that the node is up): %w", cfg.Name, err)
	}
	db, err := openPgx(target.String())
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging CockroachDB at %s: %w", target.Host, err)
	}
	return &sqlEngine{
		cfg: cfg, kind: engineCockroach, db: db, targetTxt: target.Host,
		createSQL: func(int) string {
			return "CREATE TABLE " + sqlTableName + " (_id_ STRING PRIMARY KEY, payload STRING NOT NULL)"
		},
		insertSQL: "INSERT INTO " + sqlTableName + " (_id_, payload) VALUES ($1, $2)",
		getSQL:    "SELECT payload FROM " + sqlTableName + " WHERE _id_ = $1",
		variables: cockroachVariables,
	}, nil
}

// cockroachURL builds the postgresql:// URL from -dsn or from the address and
// credentials. sslmode=require encrypts without checking the certificate (pgx
// does not verify under require); sslmode=disable is the recipe's insecure node.
func cockroachURL(cfg *config) (*url.URL, error) {
	target := &url.URL{Scheme: "postgresql"}
	if cfg.DSN != "" {
		parsed, err := url.Parse(cfg.DSN)
		if err != nil {
			return nil, errors.New("parsing -dsn: it is not a postgresql:// URL")
		}
		target = parsed
	} else {
		target.Host = cfg.Addr
		target.User = url.User(valueOr(cfg.User, "root"))
		if cfg.Password != "" {
			target.User = url.UserPassword(valueOr(cfg.User, "root"), cfg.Password)
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

func openPgx(connection string) (*sql.DB, error) {
	parsed, err := pgx.ParseConfig(connection)
	if err != nil {
		return nil, fmt.Errorf("parsing the CockroachDB connection string: %w", err)
	}
	db := stdlib.OpenDB(*parsed)
	db.SetConnMaxLifetime(0)
	return db, nil
}

func cockroachVariables(ctx context.Context, db *sql.DB) (map[string]string, error) {
	var version, syncDisabled string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return nil, fmt.Errorf("reading the CockroachDB version: %w", err)
	}
	if err := db.QueryRowContext(ctx, "SHOW CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe").Scan(&syncDisabled); err != nil {
		return nil, fmt.Errorf("reading kv.raft_log.disable_synchronization_unsafe: %w", err)
	}
	return map[string]string{"version": version, "raft_log_sync_disabled": syncDisabled}, nil
}

func (e *sqlEngine) readVariables(ctx context.Context) (map[string]string, error) {
	return e.variables(ctx, e.db)
}

func (e *sqlEngine) target() string { return e.targetTxt }

func (e *sqlEngine) setup(ctx context.Context, payloadBytes int) error {
	if _, err := e.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+sqlTableName); err != nil {
		return fmt.Errorf("dropping the old %s table: %w", sqlTableName, err)
	}
	if _, err := e.db.ExecContext(ctx, e.createSQL(payloadBytes)); err != nil {
		return fmt.Errorf("creating the %s table: %w", sqlTableName, err)
	}
	e.db.SetMaxOpenConns(e.cfg.Workers + 2)
	e.db.SetMaxIdleConns(e.cfg.Workers + 2)
	return nil
}

func (e *sqlEngine) newWorker(ctx context.Context) (worker, error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening a %s connection: %w", e.kind, err)
	}
	return &sqlWorker{engine: e, conn: conn}, nil
}

func (e *sqlEngine) close() { _ = e.db.Close() }

type sqlWorker struct {
	engine *sqlEngine
	conn   *sql.Conn
}

func (w *sqlWorker) close() { _ = w.conn.Close() }

func (w *sqlWorker) insert(ctx context.Context, id, payload string) error {
	if _, err := w.conn.ExecContext(ctx, w.engine.insertSQL, id, payload); err != nil {
		return fmt.Errorf("insert %s: %w", id, err)
	}
	return nil
}

func (w *sqlWorker) get(ctx context.Context, id string) error {
	var payload string
	err := w.conn.QueryRowContext(ctx, w.engine.getSQL, id).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("get %s: document not found", id)
	}
	if err != nil {
		return fmt.Errorf("get %s: %w", id, err)
	}
	return nil
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
