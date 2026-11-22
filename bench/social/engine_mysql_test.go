package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSQL is an in-memory database/sql driver for the unit half of the MySQL
// tests. It records every statement and transaction event and answers a SELECT
// from canned rows keyed by a fragment of its text. No MySQL server is needed;
// what it cannot show is what a real server does with the SQL (see the live
// tests, which skip without SOCIAL_LIVE_MYSQL_DSN).
type fakeSQL struct {
	mu         sync.Mutex
	events     []string // "begin", "commit", "rollback" and "stmt"
	statements []fakeStatement
	rules      []fakeRows
	affected   int64
	// execFailure, when set, is asked about every Exec; a non-nil answer is the
	// error the statement fails with (it is still recorded).
	execFailure func(query string) error
	// onBegin, when set, runs as every transaction begins, before it returns:
	// a test uses it to hold all the workers inside their transactions at once.
	onBegin func()
}

// fakeStatement is one statement the driver received.
type fakeStatement struct {
	Query string
	Args  []driver.Value
	InTx  bool
}

// fakeRows is the answer to every SELECT whose text contains Match.
type fakeRows struct {
	Match   string
	Columns []string
	Rows    [][]driver.Value
}

var (
	fakeSQLRegistry sync.Map // name -> *fakeSQL
	fakeSQLOnce     sync.Once
)

type fakeSQLDriver struct{}

func (fakeSQLDriver) Open(name string) (driver.Conn, error) {
	state, ok := fakeSQLRegistry.Load(name)
	if !ok {
		return nil, fmt.Errorf("no fake database named %q", name)
	}
	return &fakeSQLConn{state: state.(*fakeSQL)}, nil
}

type fakeSQLConn struct {
	state *fakeSQL
	inTx  bool
}

func (c *fakeSQLConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeSQLStmt{conn: c, query: query}, nil
}
func (c *fakeSQLConn) Close() error { return nil }
func (c *fakeSQLConn) Begin() (driver.Tx, error) {
	c.state.record("begin")
	if gate := c.state.onBegin; gate != nil {
		gate()
	}
	c.inTx = true
	return &fakeSQLTx{conn: c}, nil
}

type fakeSQLTx struct{ conn *fakeSQLConn }

func (t *fakeSQLTx) Commit() error { t.conn.inTx = false; t.conn.state.record("commit"); return nil }
func (t *fakeSQLTx) Rollback() error {
	t.conn.inTx = false
	t.conn.state.record("rollback")
	return nil
}

type fakeSQLStmt struct {
	conn  *fakeSQLConn
	query string
}

func (s *fakeSQLStmt) Close() error  { return nil }
func (s *fakeSQLStmt) NumInput() int { return -1 }
func (s *fakeSQLStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.state.mu.Lock()
	defer s.conn.state.mu.Unlock()
	s.conn.state.events = append(s.conn.state.events, "stmt")
	s.conn.state.statements = append(s.conn.state.statements, fakeStatement{Query: s.query, Args: slices.Clone(args), InTx: s.conn.inTx})
	if failure := s.conn.state.execFailure; failure != nil {
		if err := failure(s.query); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(s.conn.state.affected), nil
}
func (s *fakeSQLStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.state.mu.Lock()
	defer s.conn.state.mu.Unlock()
	s.conn.state.events = append(s.conn.state.events, "stmt")
	s.conn.state.statements = append(s.conn.state.statements, fakeStatement{Query: s.query, Args: slices.Clone(args), InTx: s.conn.inTx})
	for _, rule := range s.conn.state.rules {
		if strings.Contains(s.query, rule.Match) {
			return &fakeSQLRows{rows: rule}, nil
		}
	}
	return &fakeSQLRows{}, nil
}

type fakeSQLRows struct {
	rows fakeRows
	next int
}

func (r *fakeSQLRows) Columns() []string { return r.rows.Columns }
func (r *fakeSQLRows) Close() error      { return nil }
func (r *fakeSQLRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows.Rows) {
		return io.EOF
	}
	copy(dest, r.rows.Rows[r.next])
	r.next++
	return nil
}

func (f *fakeSQL) record(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
}

// reset forgets what was recorded, keeping the canned answers.
func (f *fakeSQL) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events, f.statements = nil, nil
}

func (f *fakeSQL) count(event string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return countOf(f.events, event)
}

func countOf(events []string, event string) int {
	total := 0
	for _, seen := range events {
		if seen == event {
			total++
		}
	}
	return total
}

// recorded returns the statements received so far.
func (f *fakeSQL) recorded() []fakeStatement {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.statements)
}

// newFakeMySQLEngine is a mysqlEngine whose two pools are the fake driver.
func newFakeMySQLEngine(t *testing.T) (*mysqlEngine, *fakeSQL) {
	t.Helper()
	fakeSQLOnce.Do(func() { sql.Register("g4a-fake", fakeSQLDriver{}) })
	state := &fakeSQL{affected: 1}
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	fakeSQLRegistry.Store(name, state)
	t.Cleanup(func() { fakeSQLRegistry.Delete(name) })
	db, err := sql.Open("g4a-fake", name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	engine := &mysqlEngine{
		cfg: &config{Engine: engineMySQL, Database: "social", Conc: 4, OpTimeout: time.Second},
		db:  db, bulkDB: db, ids: newTimeOrderedIDs(), social: map[string]*sql.Stmt{}, reads: newRoundRobin(1, routingA),
	}
	return engine, state
}

func (f *fakeSQL) answer(match string, columns []string, rows ...[]driver.Value) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = append(f.rules, fakeRows{Match: match, Columns: columns, Rows: rows})
}

// mysqlStatementsOf is the statements without the transaction events.
func mysqlStatementsOf(state *fakeSQL) []string {
	var queries []string
	for _, statement := range state.recorded() {
		queries = append(queries, statement.Query)
	}
	return queries
}

var postColumnNames = []string{"author_id", "seq", "text", "created", "tags", "reply_to"}

// TestMySQLSchemaMatchesSpec: the tables, primary keys and secondary indexes of
// spec 6.2 exactly, and no foreign key (Q-D).
func TestMySQLSchemaMatchesSpec(t *testing.T) {
	wantKeys := map[string]string{
		"users": "id", "posts": "author_id, seq", "likes": "author_id, seq, liker_id", "comments": "author_id, seq, id",
		"follows": "follower_id, followee_id", "feed": "user_id, id", "notifications": "user_id, id", "media": "user_id, kind",
		"jobs": "id",
	}
	wantSecondary := map[string][]string{
		"follows": {"KEY follows_by_followee (followee_id, follower_id)"},
		"jobs":    {"KEY jobs_by_outbox (outbox, lease_until, id)"},
	}
	primary := regexp.MustCompile(`PRIMARY KEY \(([^)]*)\)`)
	secondary := regexp.MustCompile(`(?m)^\s*(?:UNIQUE )?KEY \w+ \([^)]*\)`)
	if len(mysqlSocialTables) != len(wantKeys) {
		t.Errorf("%d tables declared; spec 6.2 has %d", len(mysqlSocialTables), len(wantKeys))
	}
	for _, table := range mysqlSocialTables {
		if !strings.HasPrefix(table.DDL, "CREATE TABLE "+table.Name+" (") {
			t.Errorf("%s: DDL starts %q", table.Name, table.DDL[:min(40, len(table.DDL))])
		}
		if match := primary.FindStringSubmatch(table.DDL); match == nil || match[1] != wantKeys[table.Name] {
			t.Errorf("%s: primary key %v; spec 6.2 wants (%s)", table.Name, match, wantKeys[table.Name])
		}
		var keys []string
		for _, key := range secondary.FindAllString(table.DDL, -1) {
			keys = append(keys, strings.TrimSpace(key))
		}
		if !slices.Equal(keys, wantSecondary[table.Name]) {
			t.Errorf("%s: secondary keys %v; spec 6.2 wants %v", table.Name, keys, wantSecondary[table.Name])
		}
		if upper := strings.ToUpper(table.DDL); strings.Contains(upper, "FOREIGN KEY") || strings.Contains(upper, "REFERENCES") {
			t.Errorf("%s declares a foreign key; the primary schemas have none (Q-D)", table.Name)
		}
	}
}

// TestMySQLTransactionsPerOperation: W1 to W5 each commit once, and the
// statements they send are the "Req" column of spec 6.3.
func TestMySQLTransactionsPerOperation(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	engine.cfg.Fanout = fanoutBulk // W1 is the one transaction of -fanout bulk; single mode is in fanout_test.go
	state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, []driver.Value{int64(7)}, []driver.Value{int64(8)})
	ops := &mysqlOps{engine: engine}
	ctx := context.Background()
	post := postRecord{Author: "3", Seq: 100, Text: "t", Created: time.Now().UTC()}
	cases := []struct {
		name       string
		statements int64
		run        func() error
	}{
		{"W1", 3, func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err }},
		{"W2", 3, func() error { return ops.likePost(ctx, 4, 3, 1) }},
		{"W3", 4, func() error { return ops.follow(ctx, 4, 5) }},
		{"W4", 3, func() error { return ops.unfollow(ctx, 4, 5) }},
		{"W5", 3, func() error { return ops.commentOnPost(ctx, 4, 3, 1, "c") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state.reset()
			before := ops.requests()
			if err := tc.run(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := ops.requests() - before; got != tc.statements {
				t.Errorf("%s sent %d statements, want %d: %v", tc.name, got, tc.statements, mysqlStatementsOf(state))
			}
			if int64(len(state.recorded())) != tc.statements {
				t.Errorf("%s: the driver saw %d statements, the count says %d", tc.name, len(state.recorded()), tc.statements)
			}
			if begins, commits, rollbacks := state.count("begin"), state.count("commit"), state.count("rollback"); begins != 1 || commits != 1 || rollbacks != 0 {
				t.Errorf("%s: %d begin, %d commit, %d rollback; want exactly one transaction committed once", tc.name, begins, commits, rollbacks)
			}
		})
	}
}

// TestMySQLWritesThatChangeNothingMoveNoCounter: a duplicate like or follow and
// an absent edge leave every counter alone.
func TestMySQLWritesThatChangeNothingMoveNoCounter(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	state.affected = 0
	ops := &mysqlOps{engine: engine}
	ctx := context.Background()
	for name, run := range map[string]func() error{
		"like":     func() error { return ops.likePost(ctx, 4, 3, 1) },
		"follow":   func() error { return ops.follow(ctx, 4, 5) },
		"unfollow": func() error { return ops.unfollow(ctx, 4, 5) },
	} {
		state.reset()
		if err := run(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// The driver cannot raise a duplicate key, so the text is what proves a
		// repeat is absorbed by the server: INSERT IGNORE, not a plain INSERT.
		wantPrefix := map[string]string{"like": "INSERT IGNORE INTO likes", "follow": "INSERT IGNORE INTO follows", "unfollow": "DELETE FROM follows"}[name]
		if first := mysqlStatementsOf(state)[0]; !strings.HasPrefix(first, wantPrefix) {
			t.Errorf("%s starts with %q; want %q", name, first, wantPrefix)
		}
		for _, query := range mysqlStatementsOf(state) {
			if strings.HasPrefix(query, "UPDATE") {
				t.Errorf("a duplicate %s ran %q", name, query)
			}
		}
	}
}

// TestMySQLN1UsesSkipLocked: N1 reads the oldest rows with FOR UPDATE SKIP
// LOCKED inside a transaction, then deletes exactly those.
func TestMySQLN1UsesSkipLocked(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	state.answer("FROM notifications", []string{"id"}, []driver.Value{int64(11)}, []driver.Value{int64(12)})
	ops := &mysqlOps{engine: engine}
	taken, err := ops.readNotifications(context.Background(), 3, 10)
	if err != nil || taken != 2 {
		t.Fatalf("N1 took %d, %v; want the 2 rows the SELECT returned", taken, err)
	}
	statements := state.recorded()
	if len(statements) != 2 {
		t.Fatalf("N1 sent %d statements %v; want a SELECT and a DELETE", len(statements), mysqlStatementsOf(state))
	}
	if !strings.Contains(statements[0].Query, "FOR UPDATE SKIP LOCKED") || !statements[0].InTx {
		t.Errorf("N1 selects with %q (in a transaction: %v); want FOR UPDATE SKIP LOCKED inside one", statements[0].Query, statements[0].InTx)
	}
	if !strings.HasPrefix(statements[1].Query, "DELETE FROM notifications") || !statements[1].InTx || len(statements[1].Args) != 3 {
		t.Errorf("N1 deletes with %q %v; want a DELETE of the two selected ids in the same transaction", statements[1].Query, statements[1].Args)
	}
	if state.count("commit") != 1 {
		t.Errorf("N1 committed %d times; want once", state.count("commit"))
	}
}

// TestMySQLQ1ClaimUsesSkipLocked: Q1 claims its jobs with FOR UPDATE SKIP
// LOCKED inside a transaction, so two consumers never take the same job.
func TestMySQLQ1ClaimUsesSkipLocked(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	state.answer("FROM jobs WHERE outbox", []string{"id"}, []driver.Value{int64(1)})
	ops := &mysqlOps{engine: engine}
	if claimed, err := ops.queueRound(context.Background(), 1); err != nil || claimed != 1 {
		t.Fatalf("Q1 claimed %d, %v; want 1", claimed, err)
	}
	for _, statement := range state.recorded() {
		if strings.Contains(statement.Query, "FROM jobs WHERE outbox") {
			if !strings.Contains(statement.Query, "FOR UPDATE SKIP LOCKED") || !statement.InTx {
				t.Errorf("Q1 claims with %q (in a transaction: %v); want FOR UPDATE SKIP LOCKED inside one", statement.Query, statement.InTx)
			}
			return
		}
	}
	t.Error("Q1 sent no claim statement")
}

// TestMySQLDuplicateLikeAndFollowLive: a repeated like or follow is absorbed by
// the key (INSERT IGNORE): no error, the counters move once, and a repeated
// like still notifies the author.
func TestMySQLDuplicateLikeAndFollowLive(t *testing.T) {
	world := liveMySQLWorld(t, true)
	ctx := context.Background()
	ops := world.ops(t)
	engine := world.social.(*mysqlEngine)
	count := func(query string, args ...any) int64 {
		var total int64
		if err := engine.db.QueryRowContext(ctx, query, args...).Scan(&total); err != nil {
			t.Fatal(err)
		}
		return total
	}
	const liker, author, seq = 40, 3, 1
	noticeQuery := "SELECT COUNT(*) FROM notifications WHERE user_id = ? AND type = 'like' AND actor = ?"
	noticesBefore := count(noticeQuery, author, liker)
	for range 2 {
		if err := ops.likePost(ctx, liker, author, seq); err != nil {
			t.Fatalf("W2: %v", err)
		}
	}
	page, err := ops.postPage(ctx, author, seq)
	if err != nil {
		t.Fatal(err)
	}
	unique := count("SELECT COUNT(*) FROM likes WHERE author_id = ? AND seq = ?", author, seq)
	if page.Likes != unique {
		t.Errorf("likes_n is %d for %d like rows; a repeated like must not raise it", page.Likes, unique)
	}
	if notices := count(noticeQuery, author, liker); notices != noticesBefore+2 {
		t.Errorf("the author has %d like notifications from the liker, %d before; want 2 more (the repeat still notifies)", notices, noticesBefore)
	}
	actor, followee := notFollowing(world.graph)
	before, err := ops.profile(ctx, followee)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := ops.follow(ctx, actor, followee); err != nil {
			t.Fatalf("W3: %v", err)
		}
	}
	after, err := ops.profile(ctx, followee)
	if err != nil || after.FollowerCount != before.FollowerCount+1 {
		t.Errorf("followers_n went %d -> %d (%v) after following twice; want one more", before.FollowerCount, after.FollowerCount, err)
	}
}

// TestMySQLOperationContract (unit half): rows come back as the posts, profile
// and counts the operations promise, and the fan-out is one multi-row INSERT.
func TestMySQLOperationContract(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	engine.cfg.Fanout = fanoutBulk // the W1 checks below are those of the bulk fan-out
	state.answer("FROM posts WHERE author_id = ? AND seq", append(slices.Clone(postColumnNames), "likes_n", "comments_n"),
		[]driver.Value{int64(3), int64(1), []byte("hello"), []byte("2026-10-01 00:00:00.250"), []byte(`["a","b"]`), nil, int64(5), int64(2)})
	state.answer("FROM posts WHERE author_id = ? ORDER BY seq DESC", postColumnNames,
		[]driver.Value{int64(3), int64(9), []byte("newest"), []byte("2026-10-01 00:00:01.000"), []byte(`[]`), "3:2"},
		[]driver.Value{int64(3), int64(8), []byte("older"), []byte("2026-10-01 00:00:00.500"), []byte(`[]`), nil})
	state.answer("FROM users WHERE id", []string{"id", "name", "bio", "joined", "followers_n", "following_n"},
		[]driver.Value{int64(3), []byte("ada"), []byte("bio"), []byte("2025-01-02 03:04:05.678"), int64(4), int64(6)})
	state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, []driver.Value{int64(7)}, []driver.Value{int64(8)}, []driver.Value{int64(9)})
	ops := &mysqlOps{engine: engine}
	ctx := context.Background()

	page, err := ops.postPage(ctx, 3, 1)
	wantCreated := time.Date(2026, 10, 1, 0, 0, 0, 250_000_000, time.UTC)
	if err != nil || !page.Found || page.Likes != 5 || page.Comments != 2 || page.Post.Author != "3" || page.Post.Seq != 1 ||
		!page.Post.Created.Equal(wantCreated) || !slices.Equal(page.Post.Tags, []string{"a", "b"}) || page.Post.ReplyTo != "" {
		t.Errorf("R4 = %+v, %v; want post 3/1 with 5 likes and 2 comments from one statement", page, err)
	}
	posts, err := ops.profileTimeline(ctx, 3, 20)
	if err != nil || len(posts) != 2 || posts[0].Seq != 9 || posts[0].ReplyTo != "3:2" || posts[1].ReplyTo != "" {
		t.Errorf("R3 = %+v, %v; want the two rows, newest first, NULL reply as empty", posts, err)
	}
	profile, err := ops.profile(ctx, 3)
	if err != nil || profile.UserID != "3" || profile.Name != "ada" || profile.FollowerCount != 4 || profile.FollowingCount != 6 || profile.Joined.Year() != 2025 {
		t.Errorf("R2 = %+v, %v", profile, err)
	}

	state.reset()
	ledger := &recordingLedger{}
	fanout, err := ops.publishPost(ctx, postRecord{Author: "3", Seq: 100, Text: "t", Created: wantCreated, Tags: []string{"x"}}, ledger)
	if err != nil || fanout != 3 {
		t.Fatalf("W1 fanned out to %d, %v; want 3 followers", fanout, err)
	}
	statements := state.recorded()
	if len(statements) != 3 {
		t.Fatalf("W1 sent %v; want SELECT, INSERT posts, INSERT feed", mysqlStatementsOf(state))
	}
	feedInsert := statements[2]
	if !strings.HasPrefix(feedInsert.Query, "INSERT INTO feed") || strings.Count(feedInsert.Query, "(?,?,?,?,?,?,?,?,?)") != 3 || len(feedInsert.Args) != 27 {
		t.Errorf("the fan-out is %q with %d arguments; want one INSERT of 3 rows of 9 columns", feedInsert.Query, len(feedInsert.Args))
	}
	if !slices.Equal(ledger.issued, []int{7, 8, 9}) || !slices.Equal(ledger.acknowledged, []int{7, 8, 9}) {
		t.Errorf("ledger saw sending %v and delivered %v; want the three followers each", ledger.issued, ledger.acknowledged)
	}
	if !feedInsert.InTx || !statements[1].InTx {
		t.Error("the post and its feed copies must be written in one transaction")
	}
}

// recordingLedger records the ledger calls of one fan-out.
type recordingLedger struct{ issued, acknowledged []int }

func (l *recordingLedger) sending(follower int)   { l.issued = append(l.issued, follower) }
func (l *recordingLedger) delivered(follower int) { l.acknowledged = append(l.acknowledged, follower) }

// TestMySQLSeedKeepsOneLikePerTriple (unit half): likes go in with INSERT
// IGNORE against the (author, seq, liker) primary key, comments with a plain
// INSERT and all kept, and each post's likes_n is the unique count (P4).
func TestMySQLSeedKeepsOneLikePerTriple(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	graph := newSocialGraph(tinySpec, 7)
	if err := engine.seedSocial(context.Background(), graph, 2); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	likeCounts, commentCounts := seededReactionCounts(graph)
	var wantUniqueLikes int64
	for _, count := range likeCounts {
		wantUniqueLikes += int64(count)
	}
	if wantUniqueLikes >= int64(tinySpec.Likes) {
		t.Fatalf("the tiny graph holds no repeated like (%d unique of %d), so it cannot show the dedupe", wantUniqueLikes, tinySpec.Likes)
	}
	var likeRows, commentRows int
	var counterLikes, counterComments int64
	for _, statement := range state.recorded() {
		switch {
		case strings.HasPrefix(statement.Query, "INSERT IGNORE INTO likes"):
			likeRows += len(statement.Args) / 4
		case strings.HasPrefix(statement.Query, "INSERT INTO likes"):
			t.Error("likes were inserted without IGNORE, so a repeated (post, liker) would fail the seed")
		case strings.HasPrefix(statement.Query, "INSERT INTO comments"):
			commentRows += len(statement.Args) / 6
		case strings.HasPrefix(statement.Query, "INSERT INTO posts"):
			for row := 0; row < len(statement.Args); row += 8 {
				counterLikes += statement.Args[row+6].(int64)
				counterComments += statement.Args[row+7].(int64)
			}
		}
	}
	if likeRows != tinySpec.Likes {
		t.Errorf("%d like rows sent, want all %d generated (the key dedupes them)", likeRows, tinySpec.Likes)
	}
	if commentRows != tinySpec.Comments {
		t.Errorf("%d comment rows sent, want all %d", commentRows, tinySpec.Comments)
	}
	var wantComments int64
	for _, count := range commentCounts {
		wantComments += int64(count)
	}
	if counterLikes != wantUniqueLikes || counterComments != wantComments {
		t.Errorf("the posts carry likes_n %d and comments_n %d; want the unique likes %d and all comments %d", counterLikes, counterComments, wantUniqueLikes, wantComments)
	}
}

// TestMySQLRequestsPerOperationAreCounted: R1 to R7, M1 and Q1 against the
// "Req" column of spec 6.3, counted as statements.
func TestMySQLRequestsPerOperationAreCounted(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	state.answer("FROM jobs WHERE outbox", []string{"id"}, []driver.Value{int64(1)})
	ops := &mysqlOps{engine: engine}
	ctx := context.Background()
	cases := []struct {
		name string
		want int64
		run  func() error
	}{
		{"R1", 1, func() error { _, err := ops.homeTimeline(ctx, 3, 20); return err }},
		{"R3", 1, func() error { _, err := ops.profileTimeline(ctx, 3, 20); return err }},
		{"R4", 1, func() error { _, err := ops.postPage(ctx, 3, 1); return err }},
		{"R5", 1, func() error { _, err := ops.followCheck(ctx, 3, 4); return err }},
		{"R6", 1, func() error { _, err := ops.followingList(ctx, 3, 50); return err }},
		{"M1", 1, func() error { _, err := ops.mergedTimeline(ctx, []int{1, 2, 3}); return err }},
		{"W6", 1, func() error { return ops.uploadAvatar(ctx, 0, []byte("a")) }},
		{"Q1", 4, func() error { _, err := ops.queueRound(ctx, 1); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state.reset()
			before := ops.requests()
			if err := tc.run(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := ops.requests() - before; got != tc.want || int64(len(state.recorded())) != tc.want {
				t.Errorf("%s: count %d, driver saw %d, want %d: %v", tc.name, got, len(state.recorded()), tc.want, mysqlStatementsOf(state))
			}
		})
	}
}

func TestMySQLDateTimeScan(t *testing.T) {
	var got mysqlDateTime
	if err := got.Scan([]byte("2026-10-01 00:00:00.123")); err != nil || !got.Equal(time.Date(2026, 10, 1, 0, 0, 0, 123_000_000, time.UTC)) {
		t.Errorf("scanning bytes: %v, %v", got.Time, err)
	}
	if err := got.Scan(time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600))); err != nil || got.Location() != time.UTC {
		t.Errorf("scanning a time.Time: %v, %v; want UTC", got.Time, err)
	}
	if err := got.Scan(42); err == nil {
		t.Error("scanning an int succeeded")
	}
	if err := got.Scan([]byte("yesterday")); err == nil {
		t.Error("scanning text that is no date succeeded")
	}
}

// liveMySQLWorld connects to the server SOCIAL_LIVE_MYSQL_DSN names, in a
// scratch database it drops afterwards, or skips the test.
func liveMySQLWorld(t *testing.T, seeded bool) *socialWorld {
	t.Helper()
	dsn := os.Getenv("SOCIAL_LIVE_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set SOCIAL_LIVE_MYSQL_DSN (a DSN ending in /<database>, whose user may CREATE and DROP databases) to run against a local mysqld; none is installed on the build host")
	}
	database := fmt.Sprintf("social_live_%d", time.Now().UnixNano())
	dsn, err := liveDatabaseDSN(dsn, database)
	if err != nil {
		t.Fatalf("rewriting SOCIAL_LIVE_MYSQL_DSN: %v", err)
	}
	engine, err := newMySQLEngine(context.Background(), &config{Engine: engineMySQL, MySQLDSN: dsn, Database: database, Users: tinySpec.Users, Conc: 4, OpTimeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("connecting to MySQL: %v", err)
	}
	t.Cleanup(func() {
		if _, err := engine.db.ExecContext(context.Background(), "DROP DATABASE `"+database+"`"); err != nil {
			t.Logf("dropping %s: %v", database, err)
		}
		engine.close()
	})
	world := &socialWorld{social: engine}
	if seeded {
		world.seed(t)
	}
	return world
}

// TestMySQLMergedTimelineIsTheTrueNewestLive is the M1 ruling on MySQL.
func TestMySQLMergedTimelineIsTheTrueNewestLive(t *testing.T) {
	world := liveMySQLWorld(t, true)
	checkMergedTimelineIsTrueNewest(t, world)
}

// TestMySQLMergedTimelineIsOneOrderedQuery: M1 is one indexed IN query ordered
// by created with the limit of 20 as its last argument, so the answer is the
// true newest 20 across the followees.
func TestMySQLMergedTimelineIsOneOrderedQuery(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	ops := &mysqlOps{engine: engine}
	if _, err := ops.mergedTimeline(context.Background(), []int{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	statements := state.recorded()
	if len(statements) != 1 || !strings.Contains(statements[0].Query, "author_id IN (?,?,?) ORDER BY created DESC LIMIT ?") {
		t.Fatalf("M1 sent %v; want one IN query ordered by created descending with a limit", mysqlStatementsOf(state))
	}
	if last := statements[0].Args[len(statements[0].Args)-1]; last != int64(mergedPostsTotal) {
		t.Errorf("M1 limit is %v; want %d", last, mergedPostsTotal)
	}
}

// TestMySQLOperationContractLive is the integration half of
// TestMySQLOperationContract.
func TestMySQLOperationContractLive(t *testing.T) {
	world := liveMySQLWorld(t, true)
	checkOperationContract(t, world)
}

// TestMySQLCanonicalUserMatchesGraphLive: the seeded MySQL dataset hashes to
// what the generator says it holds.
func TestMySQLCanonicalUserMatchesGraphLive(t *testing.T) {
	world := liveMySQLWorld(t, true)
	checkCanonicalMatchesGraph(t, world)
}
