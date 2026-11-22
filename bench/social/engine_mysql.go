package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

const mysqlSchemaUsers = `CREATE TABLE users (
  id BIGINT UNSIGNED NOT NULL,
  name VARCHAR(64) NOT NULL,
  bio TEXT NOT NULL,
  joined DATETIME(3) NOT NULL,
  followers INT NOT NULL,
  following INT NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// The clustered primary key (author, seq) stores one author's posts together
// in sequence order, so "newest N" is a backward range scan with no sort and
// no secondary-index lookup per row.
const mysqlSchemaPosts = `CREATE TABLE posts (
  author BIGINT UNSIGNED NOT NULL,
  seq BIGINT NOT NULL,
  text TEXT NOT NULL,
  created_at DATETIME(3) NOT NULL,
  tags JSON NOT NULL,
  likes INT NOT NULL,
  reply_to VARCHAR(48) NULL,
  PRIMARY KEY (author, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// mysqlEngine drives MySQL over database/sql. The pool holds exactly -conc
// connections and every statement is server-side prepared.
type mysqlEngine struct {
	cfg        *config
	db         *sql.DB
	insertPost *sql.Stmt
	insertUser *sql.Stmt
	selectPage *sql.Stmt
	selectUser *sql.Stmt
	countPosts *sql.Stmt
	newestSeq  *sql.Stmt
	facts      map[string]string

	// bulkDB is a second pool whose driver fills placeholders on the client, so
	// a multi-row INSERT of any width is one round trip and needs no prepared
	// statement of its own shape (the server caps prepared statements).
	bulkDB *sql.DB
	// ids mints the time-ordered key of feed entries, notifications, comments
	// and jobs.
	ids *timeOrderedIDs
	// seedDeadlockRetries counts the INSERTs seeding repeated after MySQL
	// aborted them as deadlock victims.
	seedDeadlockRetries atomic.Int64
	// social holds the statements the social operations prepared, by text.
	socialMu sync.Mutex
	social   map[string]*sql.Stmt

	// replicas are the pools of the servers after the first in -mysql-dsn. The
	// first is the source and takes every write; reads go to the source alone
	// under RP-A and to the source and each replica in turn under RP-B.
	replicas []*mysqlReplica
	reads    *roundRobin
}

// mysqlReplica is a read pool on one replica with the statements prepared on it.
type mysqlReplica struct {
	db         *sql.DB
	mu         sync.Mutex
	statements map[string]*sql.Stmt
}

// prepared returns the statement for query on this replica, preparing it on first use.
func (r *mysqlReplica) prepared(ctx context.Context, query string) (*sql.Stmt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if statement, ok := r.statements[query]; ok {
		return statement, nil
	}
	statement, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("preparing %q on a replica: %w", query, err)
	}
	r.statements[query] = statement
	return statement, nil
}

// readStatement is the statement for a read, on the server the read routing
// picks for it.
func (m *mysqlEngine) readStatement(ctx context.Context, query string) (*sql.Stmt, error) {
	if node := m.reads.pick(); node > 0 {
		return m.replicas[node-1].prepared(ctx, query)
	}
	return m.prepared(ctx, query)
}

// mysqlDSNs are the servers of -mysql-dsn, source first. A comma inside a DSN
// must be percent-encoded.
func (c *config) mysqlDSNs() []string {
	var dsns []string
	for _, dsn := range strings.Split(c.MySQLDSN, ",") {
		dsns = append(dsns, strings.TrimSpace(dsn))
	}
	return dsns
}

// mysqlReadRouting is the routing reads follow: -mysql-read-routing when given,
// else the run's -routing. "roundrobin" spreads reads over every server.
func (c *config) mysqlReadRouting() string {
	switch c.MySQLReadRouting {
	case "primary":
		return routingA
	case "roundrobin":
		return routingB
	default:
		return c.Routing
	}
}

func newMySQLEngine(ctx context.Context, cfg *config) (*mysqlEngine, error) {
	targets, err := cfg.mysqlTargets()
	if err != nil {
		return nil, err
	}
	dsnConfig := targets[0]
	if dsnConfig.DBName == "" {
		return nil, fmt.Errorf("-mysql-dsn names no database; end it with /social")
	}
	if err := ensureMySQLDatabase(ctx, dsnConfig); err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsnConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("opening MySQL: %w", err)
	}
	db.SetMaxOpenConns(cfg.Conc)
	db.SetMaxIdleConns(cfg.Conc)
	db.SetConnMaxLifetime(0)
	bulkConfig := dsnConfig.Clone()
	bulkConfig.InterpolateParams = true
	bulkDB, err := sql.Open("mysql", bulkConfig.FormatDSN())
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("opening MySQL for bulk inserts: %w", err)
	}
	bulkDB.SetMaxOpenConns(cfg.Conc)
	bulkDB.SetMaxIdleConns(cfg.Conc)
	bulkDB.SetConnMaxLifetime(0)
	engine := &mysqlEngine{cfg: cfg, db: db, bulkDB: bulkDB, ids: newTimeOrderedIDs(), social: map[string]*sql.Stmt{}}
	if engine.replicas, err = openMySQLReplicas(cfg, targets[1:]); err != nil {
		_ = db.Close()
		_ = bulkDB.Close()
		return nil, err
	}
	engine.reads = newRoundRobin(1+len(engine.replicas), cfg.mysqlReadRouting())
	if err := engine.readFacts(ctx); err != nil {
		engine.close()
		return nil, err
	}
	return engine, nil
}

// openMySQLReplicas opens one pool per replica DSN, sized like the source's.
func openMySQLReplicas(cfg *config, targets []*mysql.Config) ([]*mysqlReplica, error) {
	var replicas []*mysqlReplica
	for _, replicaConfig := range targets {
		db, err := sql.Open("mysql", replicaConfig.FormatDSN())
		if err != nil {
			return nil, fmt.Errorf("opening MySQL replica %s: %w", replicaConfig.Addr, err)
		}
		db.SetMaxOpenConns(cfg.Conc)
		db.SetMaxIdleConns(cfg.Conc)
		db.SetConnMaxLifetime(0)
		replicas = append(replicas, &mysqlReplica{db: db, statements: map[string]*sql.Stmt{}})
	}
	return replicas, nil
}

// cachedStatement returns the pool's prepared statement for query when there is one.
func (m *mysqlEngine) cachedStatement(query string) (*sql.Stmt, bool) {
	m.socialMu.Lock()
	defer m.socialMu.Unlock()
	statement, ok := m.social[query]
	return statement, ok
}

// prepared returns the statement for query, preparing it on first use. A
// statement is prepared once on the pool and the driver re-prepares it lazily
// on each connection that runs it. The prepare waits for a pooled connection,
// so it must never run while the caller holds one in a transaction, and
// socialMu is not held across it: a worker inside a transaction only looks the
// cache up, and must not queue behind a prepare that is waiting for it to
// finish (BUG-0713).
func (m *mysqlEngine) prepared(ctx context.Context, query string) (*sql.Stmt, error) {
	if statement, ok := m.cachedStatement(query); ok {
		return statement, nil
	}
	statement, err := m.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("preparing %q: %w", query, err)
	}
	m.socialMu.Lock()
	defer m.socialMu.Unlock()
	if raced, ok := m.social[query]; ok {
		_ = statement.Close() // another worker prepared the same text first; theirs stays
		return raced, nil
	}
	m.social[query] = statement
	return statement, nil
}

// ensureMySQLDatabase creates the target database when missing, over a
// connection that does not name it.
func ensureMySQLDatabase(ctx context.Context, target *mysql.Config) error {
	bare := target.Clone()
	bare.DBName = ""
	db, err := sql.Open("mysql", bare.FormatDSN())
	if err != nil {
		return fmt.Errorf("opening MySQL: %w", err)
	}
	defer func() { _ = db.Close() }() // only used to CREATE DATABASE
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+target.DBName+"` CHARACTER SET utf8mb4"); err != nil {
		return fmt.Errorf("creating database %q (check -mysql-dsn; the user needs CREATE, or create it by hand): %w", target.DBName, err)
	}
	return nil
}

// readFacts records the server's durability settings; they are the operator's
// to change, so the tool reads them rather than setting them.
func (m *mysqlEngine) readFacts(ctx context.Context) error {
	var version, flushLog, syncBinlog string
	row := m.db.QueryRowContext(ctx, "SELECT @@version, @@innodb_flush_log_at_trx_commit, @@sync_binlog")
	if err := row.Scan(&version, &flushLog, &syncBinlog); err != nil {
		return fmt.Errorf("reading MySQL durability settings: %w", err)
	}
	m.facts = map[string]string{
		"version":                        version,
		"innodb_flush_log_at_trx_commit": flushLog,
		"sync_binlog":                    syncBinlog,
		"tls":                            fmt.Sprint(m.cfg.MySQLTLS),
	}
	return nil
}

func (m *mysqlEngine) target() string {
	var servers []string
	var database string
	for _, dsn := range m.cfg.mysqlDSNs() {
		dsnConfig, err := mysql.ParseDSN(dsn)
		if err != nil {
			return "(unparsable dsn)"
		}
		servers, database = append(servers, dsnConfig.Addr), dsnConfig.DBName
	}
	return strings.Join(servers, ",") + "/" + database
}
func (m *mysqlEngine) durability() map[string]string { return m.facts }
func (m *mysqlEngine) desynced(error) bool           { return false }

func (m *mysqlEngine) close() {
	for _, statement := range []*sql.Stmt{m.insertPost, m.insertUser, m.selectPage, m.selectUser, m.countPosts, m.newestSeq} {
		if statement != nil {
			_ = statement.Close()
		}
	}
	for _, statement := range m.social {
		_ = statement.Close()
	}
	for _, replica := range m.replicas {
		for _, statement := range replica.statements {
			_ = statement.Close()
		}
		_ = replica.db.Close()
	}
	_ = m.db.Close()
	if m.bulkDB != nil {
		_ = m.bulkDB.Close()
	}
}

func (m *mysqlEngine) newSession() (engineSession, error) { return &mysqlSession{engine: m}, nil }

// setup creates both tables, refusing a database that already has them, then
// prepares the workload's statements.
func (m *mysqlEngine) setup(ctx context.Context) (schemaInfo, error) {
	for _, ddl := range []string{mysqlSchemaUsers, mysqlSchemaPosts} {
		if _, err := m.db.ExecContext(ctx, ddl); err != nil {
			var serverError *mysql.MySQLError
			if errors.As(err, &serverError) && serverError.Number == 1050 { // ER_TABLE_EXISTS_ERROR
				return schemaInfo{}, fmt.Errorf("tables already exist in the database; pass -attach to reuse them or drop them first: %w", err)
			}
			return schemaInfo{}, fmt.Errorf("creating table: %w", err)
		}
	}
	if err := m.prepare(ctx); err != nil {
		return schemaInfo{}, err
	}
	return schemaInfo{Layout: "single-table", Detail: []string{mysqlSchemaUsers, mysqlSchemaPosts}, Buckets: 2}, nil
}

// attach reuses tables an earlier run created.
func (m *mysqlEngine) attach(ctx context.Context) (schemaInfo, error) {
	if err := m.prepare(ctx); err != nil {
		return schemaInfo{}, fmt.Errorf("%w (is the database seeded? run once without -attach)", err)
	}
	return schemaInfo{Layout: "single-table", Detail: []string{"existing users and posts tables"}, Buckets: 2}, nil
}

func (m *mysqlEngine) prepare(ctx context.Context) error {
	statements := []struct {
		target **sql.Stmt
		query  string
	}{
		{&m.insertPost, "INSERT INTO posts (author, seq, text, created_at, tags, likes, reply_to) VALUES (?, ?, ?, ?, ?, ?, ?)"},
		{&m.insertUser, "INSERT INTO users (id, name, bio, joined, followers, following) VALUES (?, ?, ?, ?, ?, ?)"},
		{&m.selectPage, "SELECT author, seq, text, created_at, tags, likes, reply_to FROM posts WHERE author = ? ORDER BY seq DESC LIMIT ?"},
		{&m.selectUser, "SELECT id, name, bio, joined, followers, following FROM users WHERE id = ?"},
		{&m.countPosts, "SELECT COUNT(*) FROM posts WHERE author = ?"},
		{&m.newestSeq, "SELECT MAX(seq) FROM posts WHERE author = ?"},
	}
	for _, statement := range statements {
		prepared, err := m.db.PrepareContext(ctx, statement.query)
		if err != nil {
			return fmt.Errorf("preparing %q: %w", statement.query, err)
		}
		*statement.target = prepared
	}
	return nil
}

type mysqlSession struct {
	engine *mysqlEngine
}

func (s *mysqlSession) close() {}

func (s *mysqlSession) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.engine.cfg.OpTimeout)
}

func parseUserID(id string) (uint64, error) {
	parsed, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("user id %q is not numeric: %w", id, err)
	}
	return parsed, nil
}

func (s *mysqlSession) putProfile(parent context.Context, id string, profile map[string]interface{}) error {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	numericID, err := parseUserID(id)
	if err != nil {
		return err
	}
	_, err = s.engine.insertUser.ExecContext(ctx, numericID, profile["name"], profile["bio"], profile["joined"], profile["followers"], profile["following"])
	if err != nil {
		return fmt.Errorf("writing profile: %w", err)
	}
	return nil
}

func (s *mysqlSession) writePost(parent context.Context, id string, post map[string]interface{}) error {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	author, err := parseUserID(id)
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

func (s *mysqlSession) count(ctx context.Context, author uint64) (int64, error) {
	var total int64
	if err := s.engine.countPosts.QueryRowContext(ctx, author).Scan(&total); err != nil {
		return 0, fmt.Errorf("counting posts of %d: %w", author, err)
	}
	return total, nil
}

func (s *mysqlSession) attachState(parent context.Context, id string) (int64, int64, error) {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	numericID, err := parseUserID(id)
	if err != nil {
		return 0, 0, err
	}
	var profileID uint64
	var name, bio, joined []byte
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

// readTimeline runs the page query and the profile query. As for MongoDB, a
// sampled read brackets the page with two counts taken outside the clock.
func (s *mysqlSession) readTimeline(parent context.Context, request readRequest) (timelinePage, error) {
	ctx, cancel := s.opContext(parent)
	defer cancel()
	author, err := parseUserID(request.Author)
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
	var profileID uint64
	var name, bio, joined []byte
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
	return timelinePage{Posts: posts, TotalLow: countBefore, TotalHigh: countAfter, ProfileOwner: strconv.FormatUint(profileID, 10)}, nil
}

// readPage fetches every column of the newest posts, as a client rendering a
// timeline would; only author and seq are kept for a sampled read.
func (s *mysqlSession) readPage(ctx context.Context, author uint64, request readRequest) ([]postView, error) {
	rows, err := s.engine.selectPage.QueryContext(ctx, author, request.Limit)
	if err != nil {
		return nil, fmt.Errorf("finding timeline %s: %w", request.Author, err)
	}
	defer func() { _ = rows.Close() }() // rows.Err below reports any read failure
	var posts []postView
	for rows.Next() {
		var rowAuthor uint64
		var seq, likes int64
		var text, createdAt, tags, replyTo sql.RawBytes
		if err := rows.Scan(&rowAuthor, &seq, &text, &createdAt, &tags, &likes, &replyTo); err != nil {
			return nil, fmt.Errorf("reading timeline %s: %w", request.Author, err)
		}
		if request.Sampled {
			posts = append(posts, postView{Author: strconv.FormatUint(rowAuthor, 10), Seq: seq})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading timeline %s: %w", request.Author, err)
	}
	return posts, nil
}
