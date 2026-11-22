package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// The timeline workload's two tables, the counterpart of mysqlSchemaUsers and
// mysqlSchemaPosts. What a PostgreSQL user would write, column by column:
// BIGINT ids (there is no unsigned type), TIMESTAMPTZ(3) for DATETIME(3), JSONB
// for JSON (the binary form, which is what an application that queries tags
// would pick), TEXT for text (PostgreSQL has no cost difference between TEXT and
// VARCHAR, so VARCHAR(n) is kept only where the data has a real bound). A heap
// table has no clustered key; the primary-key b-tree on (author, seq) is what
// "newest N of an author" scans backward, one heap fetch per row.
const postgresSchemaUsers = `CREATE TABLE users (
  id BIGINT NOT NULL,
  name VARCHAR(64) NOT NULL,
  bio TEXT NOT NULL,
  joined TIMESTAMPTZ(3) NOT NULL,
  followers INTEGER NOT NULL,
  following INTEGER NOT NULL,
  PRIMARY KEY (id)
)`

const postgresSchemaPosts = `CREATE TABLE posts (
  author BIGINT NOT NULL,
  seq BIGINT NOT NULL,
  text TEXT NOT NULL,
  created_at TIMESTAMPTZ(3) NOT NULL,
  tags JSONB NOT NULL,
  likes INTEGER NOT NULL,
  reply_to VARCHAR(48) NULL,
  PRIMARY KEY (author, seq)
)`

// postgresTableExists is the SQLSTATE of CREATE TABLE on an existing table
// (duplicate_table).
const postgresTableExists = "42P07"

// The SQLSTATEs a PostgreSQL client is expected to run again: a deadlock the
// server broke by aborting one transaction (parallel seed workers and the
// counter updates of W3 and W4 can take row locks in opposite orders), and a
// serialization failure, which only a SERIALIZABLE or REPEATABLE READ session
// raises; the default READ COMMITTED does not, but the retry costs nothing.
const (
	postgresDeadlock             = "40P01"
	postgresSerializationFailure = "40001"
)

// postgresEngine drives a single PostgreSQL server over database/sql with the
// pgx driver. The pool holds exactly -conc connections and every statement is
// server-side prepared, as in the MySQL engine.
type postgresEngine struct {
	cfg        *config
	db         *sql.DB
	insertPost *sql.Stmt
	insertUser *sql.Stmt
	selectPage *sql.Stmt
	selectUser *sql.Stmt
	countPosts *sql.Stmt
	newestSeq  *sql.Stmt
	facts      map[string]string

	// bulkDB is a second pool that sends each statement unprepared in the
	// extended protocol, so a multi-row INSERT of any width is one round trip
	// and needs no prepared statement of its own shape.
	bulkDB *sql.DB
	// ids mints the time-ordered key of feed entries, notifications, comments
	// and jobs.
	ids *timeOrderedIDs
	// seedRetryCount counts the INSERTs seeding repeated after PostgreSQL
	// aborted them as a deadlock victim (SQLSTATE 40P01).
	seedRetryCount atomic.Int64
	// transactionRetries counts the transactions and single statements the
	// operations ran again for the same reason.
	transactionRetries atomic.Int64
	// social holds the statements the social operations prepared, by text.
	socialMu sync.Mutex
	social   map[string]*sql.Stmt
}

// openPostgresPool opens a pool of at most conc connections. exec sends each
// statement unprepared, the counterpart of the MySQL bulk pool's client-side
// interpolation.
func openPostgresPool(connection string, conc int, exec bool) (*sql.DB, error) {
	parsed, err := pgx.ParseConfig(connection)
	if err != nil {
		return nil, fmt.Errorf("parsing the PostgreSQL connection string: %w", err)
	}
	if exec {
		parsed.DefaultQueryExecMode = pgx.QueryExecModeExec
	}
	db := stdlib.OpenDB(*parsed)
	db.SetMaxOpenConns(conc)
	db.SetMaxIdleConns(conc)
	db.SetConnMaxLifetime(0)
	return db, nil
}

func newPostgresEngine(ctx context.Context, cfg *config) (*postgresEngine, error) {
	connection, err := cfg.postgresConnectionString()
	if err != nil {
		return nil, err
	}
	if err := ensurePostgresDatabase(ctx, connection); err != nil {
		return nil, err
	}
	db, err := openPostgresPool(connection, cfg.Conc, false)
	if err != nil {
		return nil, err
	}
	bulkDB, err := openPostgresPool(connection, cfg.Conc, true)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	engine := &postgresEngine{cfg: cfg, db: db, bulkDB: bulkDB, ids: newTimeOrderedIDs(), social: map[string]*sql.Stmt{}}
	if err := engine.readFacts(ctx); err != nil {
		engine.close()
		return nil, err
	}
	return engine, nil
}

// ensurePostgresDatabase creates the target database when missing, over a
// connection to the maintenance database "postgres", which every cluster has.
// CREATE DATABASE has no IF NOT EXISTS in PostgreSQL, so the catalog is asked
// first; two harnesses racing to create it is not a case this tool has.
func ensurePostgresDatabase(ctx context.Context, connection string) error {
	target, err := url.Parse(connection)
	if err != nil {
		return fmt.Errorf("parsing the PostgreSQL connection string: %w", err)
	}
	database := strings.TrimPrefix(target.Path, "/")
	target.Path = "/postgres"
	bare, err := openPostgresPool(target.String(), 1, false)
	if err != nil {
		return err
	}
	defer func() { _ = bare.Close() }() // only used to CREATE DATABASE
	var exists bool
	if err := bare.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", database).Scan(&exists); err != nil {
		return fmt.Errorf("looking for database %q (check -postgres-dsn, the password and that the server is up): %w", database, err)
	}
	if exists {
		return nil
	}
	if _, err := bare.ExecContext(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
		return fmt.Errorf("creating database %q (the account needs CREATEDB, or create it by hand with 'createdb %s'): %w", database, database, err)
	}
	return nil
}

// readFacts records the server's durability and memory settings; they are the
// operator's to change, so the tool reads them rather than setting them. The
// six that decide what an acknowledged commit means and how much memory the
// engine may use come from pg_settings. fsync off is never a durable
// comparison, so a leg that finds it is failed by the witness; synchronous_commit
// off is the record-only relaxed level (L-B) and is only recorded.
func (c *postgresEngine) readFacts(ctx context.Context) error {
	var version string
	if err := c.db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("reading the PostgreSQL version: %w", err)
	}
	c.facts = map[string]string{"version": version, "tls": fmt.Sprint(c.cfg.PostgresTLS)}
	rows, err := c.db.QueryContext(ctx, "SELECT name, setting FROM pg_settings WHERE name IN ("+postgresFactSettings+")")
	if err != nil {
		return fmt.Errorf("reading pg_settings: %w", err)
	}
	defer func() { _ = rows.Close() }() // rows.Err below reports a read failure
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			return fmt.Errorf("reading pg_settings: %w", err)
		}
		c.facts[name] = setting
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading pg_settings: %w", err)
	}
	if c.facts["fsync"] == "off" {
		c.facts[durabilityFailureKey] = "fsync is off: PostgreSQL acknowledges commits without flushing the write-ahead log, so it is not the durable comparison; set fsync = on in postgresql.conf and reload"
	}
	return nil
}

// postgresFactSettings is the quoted name list of the pg_settings rows a leg
// records, written into the statement because the names are constants.
const postgresFactSettings = `'fsync', 'synchronous_commit', 'full_page_writes', 'wal_sync_method', 'wal_writer_delay',
  'shared_buffers', 'effective_cache_size', 'max_wal_size', 'max_connections', 'ssl'`

func (c *postgresEngine) target() string {
	target, err := url.Parse(c.cfg.PostgresDSN)
	if err != nil {
		return "(unparsable dsn)"
	}
	return target.Host + target.Path
}
func (c *postgresEngine) durability() map[string]string { return c.facts }
func (c *postgresEngine) desynced(error) bool           { return false }

func (c *postgresEngine) close() {
	for _, statement := range []*sql.Stmt{c.insertPost, c.insertUser, c.selectPage, c.selectUser, c.countPosts, c.newestSeq} {
		if statement != nil {
			_ = statement.Close()
		}
	}
	for _, statement := range c.social {
		_ = statement.Close()
	}
	_ = c.db.Close()
	if c.bulkDB != nil {
		_ = c.bulkDB.Close()
	}
}

func (c *postgresEngine) newSession() (engineSession, error) {
	return &postgresSession{engine: c}, nil
}

// isPostgresRetryable reports whether err is one of the two failures
// PostgreSQL uses to ask a client to run a transaction again: deadlock_detected
// and serialization_failure. They are the only errors a client is expected to retry.
func isPostgresRetryable(err error) bool {
	var serverError *pgconn.PgError
	return errors.As(err, &serverError) && (serverError.Code == postgresDeadlock || serverError.Code == postgresSerializationFailure)
}

// setup creates both tables, refusing a database that already has them, then
// prepares the workload's statements.
func (c *postgresEngine) setup(ctx context.Context) (schemaInfo, error) {
	for _, ddl := range []string{postgresSchemaUsers, postgresSchemaPosts} {
		if _, err := c.db.ExecContext(ctx, ddl); err != nil {
			var serverError *pgconn.PgError
			if errors.As(err, &serverError) && serverError.Code == postgresTableExists {
				return schemaInfo{}, fmt.Errorf("tables already exist in the database; pass -attach to reuse them or drop them first: %w", err)
			}
			return schemaInfo{}, fmt.Errorf("creating table: %w", err)
		}
	}
	if err := c.prepare(ctx); err != nil {
		return schemaInfo{}, err
	}
	return schemaInfo{Layout: "single-table", Detail: []string{postgresSchemaUsers, postgresSchemaPosts}, Buckets: 2}, nil
}

// attach reuses tables an earlier run created.
func (c *postgresEngine) attach(ctx context.Context) (schemaInfo, error) {
	if err := c.prepare(ctx); err != nil {
		return schemaInfo{}, fmt.Errorf("%w (is the database seeded? run once without -attach)", err)
	}
	return schemaInfo{Layout: "single-table", Detail: []string{"existing users and posts tables"}, Buckets: 2}, nil
}

func (c *postgresEngine) prepare(ctx context.Context) error {
	statements := []struct {
		target **sql.Stmt
		query  string
	}{
		{&c.insertPost, "INSERT INTO posts (author, seq, text, created_at, tags, likes, reply_to) VALUES ($1, $2, $3, $4, $5, $6, $7)"},
		{&c.insertUser, "INSERT INTO users (id, name, bio, joined, followers, following) VALUES ($1, $2, $3, $4, $5, $6)"},
		{&c.selectPage, "SELECT author, seq, text, created_at, tags, likes, reply_to FROM posts WHERE author = $1 ORDER BY seq DESC LIMIT $2"},
		{&c.selectUser, "SELECT id, name, bio, joined, followers, following FROM users WHERE id = $1"},
		{&c.countPosts, "SELECT COUNT(*) FROM posts WHERE author = $1"},
		{&c.newestSeq, "SELECT MAX(seq) FROM posts WHERE author = $1"},
	}
	for _, statement := range statements {
		prepared, err := c.db.PrepareContext(ctx, statement.query)
		if err != nil {
			return fmt.Errorf("preparing %q: %w", statement.query, err)
		}
		*statement.target = prepared
	}
	return nil
}

// cachedStatement returns the pool's prepared statement for query when there is one.
func (c *postgresEngine) cachedStatement(query string) (*sql.Stmt, bool) {
	c.socialMu.Lock()
	defer c.socialMu.Unlock()
	statement, ok := c.social[query]
	return statement, ok
}

// prepared returns the statement for query, preparing it on first use. The
// prepare waits for a pooled connection, so it must never run while the caller
// holds one in a transaction, and socialMu is not held across it: a worker
// inside a transaction only looks the cache up, and must not queue behind a
// prepare that is waiting for it to finish (the MySQL engine's BUG-0713).
func (c *postgresEngine) prepared(ctx context.Context, query string) (*sql.Stmt, error) {
	if statement, ok := c.cachedStatement(query); ok {
		return statement, nil
	}
	statement, err := c.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("preparing %q: %w", query, err)
	}
	c.socialMu.Lock()
	defer c.socialMu.Unlock()
	if raced, ok := c.social[query]; ok {
		_ = statement.Close() // another worker prepared the same text first; theirs stays
		return raced, nil
	}
	c.social[query] = statement
	return statement, nil
}

type postgresSession struct {
	engine *postgresEngine
}

func (s *postgresSession) close() {}

func (s *postgresSession) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.engine.cfg.OpTimeout)
}

// postgresUserID parses a user id; PostgreSQL has no unsigned column, so ids
// travel as INT8.
func postgresUserID(id string) (int64, error) {
	parsed, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("user id %q is not numeric: %w", id, err)
	}
	return parsed, nil
}

func (s *postgresSession) putProfile(parent context.Context, id string, profile map[string]interface{}) error {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	numericID, err := postgresUserID(id)
	if err != nil {
		return err
	}
	_, err = s.engine.insertUser.ExecContext(ctx, numericID, profile["name"], profile["bio"], profile["joined"], profile["followers"], profile["following"])
	if err != nil {
		return fmt.Errorf("writing profile: %w", err)
	}
	return nil
}

func (s *postgresSession) writePost(parent context.Context, id string, post map[string]interface{}) error {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	author, err := postgresUserID(id)
	if err != nil {
		return err
	}
	tags, err := json.Marshal(post["tags"])
	if err != nil {
		return fmt.Errorf("encoding tags: %w", err)
	}
	var replyTo interface{}
	if text := post["reply_to"].(string); text != "" {
		replyTo = text // an absent reply is NULL, as a SQL schema would store it
	}
	_, err = s.engine.insertPost.ExecContext(ctx, author, post["seq"], post["text"], post["created_at"], string(tags), post["likes"], replyTo)
	if err != nil {
		return fmt.Errorf("inserting post: %w", err)
	}
	return nil
}

func (s *postgresSession) count(ctx context.Context, author int64) (int64, error) {
	var total int64
	if err := s.engine.countPosts.QueryRowContext(ctx, author).Scan(&total); err != nil {
		return 0, fmt.Errorf("counting posts of %d: %w", author, err)
	}
	return total, nil
}

func (s *postgresSession) attachState(parent context.Context, id string) (int64, int64, error) {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	numericID, err := postgresUserID(id)
	if err != nil {
		return 0, 0, err
	}
	var profileID int64
	var name, bio string
	var joined pgTime
	var followers, following int64
	if err := s.engine.selectUser.QueryRowContext(ctx, numericID).Scan(&profileID, &name, &bio, &joined, &followers, &following); err != nil {
		return 0, 0, fmt.Errorf("profile missing or unreadable; -users is larger than the dataset or the database is not seeded: %w", err)
	}
	total, err := s.count(ctx, numericID)
	if err != nil {
		return 0, 0, err
	}
	var newest sql.NullInt64
	if err := s.engine.newestSeq.QueryRowContext(ctx, numericID).Scan(&newest); err != nil {
		return 0, 0, fmt.Errorf("reading newest seq: %w", err)
	}
	if !newest.Valid {
		return total, 0, nil
	}
	return total, newest.Int64 + 1, nil
}

// readTimeline runs the page query and the profile query. As for MongoDB and
// MySQL, a sampled read brackets the page with two counts taken outside the clock.
func (s *postgresSession) readTimeline(parent context.Context, request readRequest) (timelinePage, error) {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	author, err := postgresUserID(request.Author)
	if err != nil {
		return timelinePage{}, err
	}
	var countBefore int64
	if request.Sampled {
		if countBefore, err = s.count(ctx, author); err != nil {
			return timelinePage{}, err
		}
		request.Clock.start = time.Now()
	}
	posts, err := s.readPage(ctx, author, request)
	if err != nil {
		return timelinePage{}, err
	}
	var profileID int64
	var name, bio string
	var joined pgTime
	var followers, following int64
	if err := s.engine.selectUser.QueryRowContext(ctx, author).Scan(&profileID, &name, &bio, &joined, &followers, &following); err != nil {
		return timelinePage{}, fmt.Errorf("finding profile %s: %w", request.Author, err)
	}
	if !request.Sampled {
		return timelinePage{}, nil
	}
	request.Clock.stop = time.Now()
	countAfter, err := s.count(ctx, author)
	if err != nil {
		return timelinePage{}, err
	}
	return timelinePage{Posts: posts, TotalLow: countBefore, TotalHigh: countAfter, ProfileOwner: strconv.FormatInt(profileID, 10)}, nil
}

// readPage fetches every column of the newest posts, as a client rendering a
// timeline would; only author and seq are kept for a sampled read.
func (s *postgresSession) readPage(ctx context.Context, author int64, request readRequest) ([]postView, error) {
	rows, err := s.engine.selectPage.QueryContext(ctx, author, request.Limit)
	if err != nil {
		return nil, fmt.Errorf("finding timeline %s: %w", request.Author, err)
	}
	defer func() { _ = rows.Close() }() // rows.Err below reports any read failure
	var posts []postView
	for rows.Next() {
		var rowAuthor, seq int64
		var likes int32
		var text, tags string
		var createdAt pgTime
		var replyTo sql.NullString
		if err := rows.Scan(&rowAuthor, &seq, &text, &createdAt, &tags, &likes, &replyTo); err != nil {
			return nil, fmt.Errorf("reading timeline %s: %w", request.Author, err)
		}
		if request.Sampled {
			posts = append(posts, postView{Author: strconv.FormatInt(rowAuthor, 10), Seq: seq})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading timeline %s: %w", request.Author, err)
	}
	return posts, nil
}
