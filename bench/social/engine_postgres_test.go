package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// newFakePostgresEngine is a postgresEngine whose two pools are the fake
// driver the MySQL tests use: no PostgreSQL server is needed, and what it
// cannot show is what a real server does with the SQL (see the live tests,
// which skip without SOCIAL_LIVE_POSTGRES_DSN).
func newFakePostgresEngine(t *testing.T) (*postgresEngine, *fakeSQL) {
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
	engine := &postgresEngine{
		cfg: &config{Engine: enginePostgres, Database: "social", Conc: 4, OpTimeout: time.Second},
		db:  db, bulkDB: db, ids: newTimeOrderedIDs(), social: map[string]*sql.Stmt{},
	}
	return engine, state
}

// TestPostgresSchemaMatchesSpec: the tables, primary keys and secondary
// indexes of spec 6.2 exactly, the same as the MySQL schema, and no foreign key.
func TestPostgresSchemaMatchesSpec(t *testing.T) {
	wantKeys := map[string]string{
		"users": "id", "posts": "author_id, seq", "likes": "author_id, seq, liker_id", "comments": "author_id, seq, id",
		"follows": "follower_id, followee_id", "feed": "user_id, id", "notifications": "user_id, id", "media": "user_id, kind",
		"jobs": "id",
	}
	wantSecondary := map[string][]string{
		"follows": {"CREATE INDEX follows_by_followee ON follows (followee_id, follower_id)"},
		"jobs":    {"CREATE INDEX jobs_by_outbox ON jobs (outbox, lease_until, id)"},
	}
	primary := regexp.MustCompile(`PRIMARY KEY \(([^)]*)\)`)
	secondary := regexp.MustCompile(`^CREATE (?:UNIQUE )?INDEX \w+ ON \w+ \([^)]*\)$`)
	if len(postgresSocialTables) != len(wantKeys) {
		t.Errorf("%d tables declared; spec 6.2 has %d", len(postgresSocialTables), len(wantKeys))
	}
	for _, table := range postgresSocialTables {
		if !strings.HasPrefix(table.DDL, "CREATE TABLE "+table.Name+" (") {
			t.Errorf("%s: DDL starts %q", table.Name, table.DDL[:min(40, len(table.DDL))])
		}
		if match := primary.FindStringSubmatch(table.DDL); match == nil || match[1] != wantKeys[table.Name] {
			t.Errorf("%s: primary key %v; spec 6.2 wants (%s)", table.Name, match, wantKeys[table.Name])
		}
		var keys []string
		for _, key := range table.Indexes {
			if !secondary.MatchString(key) {
				t.Errorf("%s: index statement %q is not CREATE INDEX name ON table (columns)", table.Name, key)
			}
			keys = append(keys, key)
		}
		if strings.Contains(strings.ToUpper(table.DDL), "INDEX") {
			t.Errorf("%s: an index inside CREATE TABLE is CockroachDB syntax that PostgreSQL rejects", table.Name)
		}
		if !slices.Equal(keys, wantSecondary[table.Name]) {
			t.Errorf("%s: secondary indexes %v; spec 6.2 wants %v", table.Name, keys, wantSecondary[table.Name])
		}
		upper := strings.ToUpper(table.DDL)
		if strings.Contains(upper, "FOREIGN KEY") || strings.Contains(upper, "REFERENCES") {
			t.Errorf("%s declares a foreign key; the primary schemas have none (Q-D)", table.Name)
		}
		if strings.Contains(upper, "ENGINE=") || strings.Contains(upper, "UNSIGNED") || strings.Contains(upper, "DATETIME") {
			t.Errorf("%s uses MySQL-only DDL that PostgreSQL rejects", table.Name)
		}
	}
}

// TestPostgresMirrorsMySQLTableNames: the two engines hold the same tables, so
// a canonical hash compares like with like.
func TestPostgresMirrorsMySQLTableNames(t *testing.T) {
	var mysqlNames, postgresNames []string
	for _, table := range mysqlSocialTables {
		mysqlNames = append(mysqlNames, table.Name)
	}
	for _, table := range postgresSocialTables {
		postgresNames = append(postgresNames, table.Name)
	}
	if !slices.Equal(mysqlNames, postgresNames) {
		t.Errorf("PostgreSQL tables %v; MySQL tables %v; they must match", postgresNames, mysqlNames)
	}
}

// TestPostgresPlaceholdersAreNumbered: the Postgres wire protocol numbers its
// parameters, so a multi-row statement counts up across the rows.
func TestPostgresPlaceholdersAreNumbered(t *testing.T) {
	if got := pgPlaceholders(1, 3); got != "$1,$2,$3" {
		t.Errorf("pgPlaceholders(1, 3) = %q", got)
	}
	if got := pgPlaceholders(4, 2); got != "$4,$5" {
		t.Errorf("pgPlaceholders(4, 2) = %q", got)
	}
	if got := pgRowPlaceholders(2, 3); got != "($1,$2,$3),($4,$5,$6)" {
		t.Errorf("pgRowPlaceholders(2, 3) = %q", got)
	}
	if got := pgPlaceholders(1, 0); got != "" {
		t.Errorf("pgPlaceholders(1, 0) = %q; want empty", got)
	}
}

// TestPostgresTransactionsPerOperation: W1 to W5 each commit once, and the
// statements they send are the "Req" column of spec 6.3.
func TestPostgresTransactionsPerOperation(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	engine.cfg.Fanout = fanoutBulk // W1 is the one transaction of -fanout bulk; single mode is below
	state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, []driver.Value{int64(7)}, []driver.Value{int64(8)})
	ops := &postgresOps{engine: engine}
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

// TestPostgresWritesThatChangeNothingMoveNoCounter: a duplicate like or follow
// and an absent edge leave every counter alone.
func TestPostgresWritesThatChangeNothingMoveNoCounter(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	state.affected = 0
	ops := &postgresOps{engine: engine}
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
		// repeat is absorbed by the server: ON CONFLICT DO NOTHING, not a plain INSERT.
		wantPrefix := map[string]string{"like": "INSERT INTO likes", "follow": "INSERT INTO follows", "unfollow": "DELETE FROM follows"}[name]
		first := mysqlStatementsOf(state)[0]
		if !strings.HasPrefix(first, wantPrefix) {
			t.Errorf("%s starts with %q; want %q", name, first, wantPrefix)
		}
		if name != "unfollow" && !strings.HasSuffix(first, "ON CONFLICT DO NOTHING") {
			t.Errorf("%s is %q; want it to end with ON CONFLICT DO NOTHING so a repeat is the same row", name, first)
		}
		for _, query := range mysqlStatementsOf(state) {
			if strings.HasPrefix(query, "UPDATE") {
				t.Errorf("a duplicate %s ran %q", name, query)
			}
		}
	}
}

// TestPostgresN1UsesSkipLocked: N1 reads the oldest rows with FOR UPDATE SKIP
// LOCKED inside a transaction, then deletes exactly those.
func TestPostgresN1UsesSkipLocked(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	state.answer("FROM notifications", []string{"id"}, []driver.Value{int64(11)}, []driver.Value{int64(12)})
	ops := &postgresOps{engine: engine}
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
	if !strings.Contains(statements[1].Query, "IN ($2,$3)") {
		t.Errorf("N1 deletes with %q; want numbered placeholders IN ($2,$3)", statements[1].Query)
	}
	if state.count("commit") != 1 {
		t.Errorf("N1 committed %d times; want once", state.count("commit"))
	}
}

// TestPostgresQ1ClaimUsesSkipLocked: Q1 claims its jobs with FOR UPDATE SKIP
// LOCKED inside a transaction, so two consumers never take the same job.
func TestPostgresQ1ClaimUsesSkipLocked(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	state.answer("FROM jobs WHERE outbox", []string{"id"}, []driver.Value{int64(1)})
	ops := &postgresOps{engine: engine}
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

// TestPostgresOperationContract (unit half): rows come back as the posts,
// profile and counts the operations promise, and the bulk fan-out is one
// multi-row INSERT.
func TestPostgresOperationContract(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	engine.cfg.Fanout = fanoutBulk
	moment := time.Date(2026, 10, 1, 0, 0, 0, 250_000_000, time.UTC)
	state.answer("FROM posts WHERE author_id = $1 AND seq", append(slices.Clone(postColumnNames), "likes_n", "comments_n"),
		[]driver.Value{int64(3), int64(1), "hello", moment, []byte(`["a","b"]`), nil, int64(5), int64(2)})
	state.answer("FROM posts WHERE author_id = $1 ORDER BY seq DESC", postColumnNames,
		[]driver.Value{int64(3), int64(9), "newest", moment.Add(750 * time.Millisecond), []byte(`[]`), "3:2"},
		[]driver.Value{int64(3), int64(8), "older", moment, []byte(`[]`), nil})
	state.answer("FROM users WHERE id", []string{"id", "name", "bio", "joined", "followers_n", "following_n"},
		[]driver.Value{int64(3), "ada", "bio", time.Date(2025, 1, 2, 3, 4, 5, 678_000_000, time.UTC), int64(4), int64(6)})
	state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, []driver.Value{int64(7)}, []driver.Value{int64(8)}, []driver.Value{int64(9)})
	ops := &postgresOps{engine: engine}
	ctx := context.Background()

	page, err := ops.postPage(ctx, 3, 1)
	if err != nil || !page.Found || page.Likes != 5 || page.Comments != 2 || page.Post.Author != "3" || page.Post.Seq != 1 ||
		!page.Post.Created.Equal(moment) || !slices.Equal(page.Post.Tags, []string{"a", "b"}) || page.Post.ReplyTo != "" {
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
	fanout, err := ops.publishPost(ctx, postRecord{Author: "3", Seq: 100, Text: "t", Created: moment, Tags: []string{"x"}}, ledger)
	if err != nil || fanout != 3 {
		t.Fatalf("W1 fanned out to %d, %v; want 3 followers", fanout, err)
	}
	statements := state.recorded()
	if len(statements) != 3 {
		t.Fatalf("W1 sent %v; want SELECT, INSERT posts, INSERT feed", mysqlStatementsOf(state))
	}
	feedInsert := statements[2]
	if !strings.HasPrefix(feedInsert.Query, "INSERT INTO feed") || !strings.Contains(feedInsert.Query, "($1,$2,$3,$4,$5,$6,$7,$8,$9),($10,") || len(feedInsert.Args) != 27 {
		t.Errorf("the fan-out is %q with %d arguments; want one INSERT of 3 rows of 9 numbered columns", feedInsert.Query, len(feedInsert.Args))
	}
	if !slices.Equal(ledger.issued, []int{7, 8, 9}) || !slices.Equal(ledger.acknowledged, []int{7, 8, 9}) {
		t.Errorf("ledger saw sending %v and delivered %v; want the three followers each", ledger.issued, ledger.acknowledged)
	}
	if !feedInsert.InTx || !statements[1].InTx {
		t.Error("the post and its feed copies must be written in one transaction")
	}
}

// TestPostgresSinglePublishIsOneAutocommitWritePerFollower: the default
// fan-out writes one feed entry per request, as hoardDB does.
func TestPostgresSinglePublishIsOneAutocommitWritePerFollower(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	engine.cfg.Fanout = fanoutSingle
	state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, []driver.Value{int64(7)}, []driver.Value{int64(8)}, []driver.Value{int64(9)})
	ops := &postgresOps{engine: engine}
	if _, err := ops.publishPost(context.Background(), postRecord{Author: "3", Seq: 100, Text: "t", Created: time.Now().UTC()}, noLedger{}); err != nil {
		t.Fatal(err)
	}
	if got := feedInserts(mysqlStatementsOf(state)); got != 3 {
		t.Errorf("%d feed inserts; want one per follower (3)", got)
	}
	if state.count("begin") != 0 {
		t.Errorf("single fan-out began %d transactions; want autocommit statements", state.count("begin"))
	}
}

// TestPostgresSeedKeepsOneLikePerTriple (unit half): likes go in with ON
// CONFLICT DO NOTHING against the (author, seq, liker) primary key, comments
// with a plain INSERT and all kept, and each post's likes_n is the unique count.
func TestPostgresSeedKeepsOneLikePerTriple(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	graph := newSocialGraph(tinySpec, 7)
	if err := engine.seedSocial(context.Background(), graph, 2); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	likeCounts, commentCounts := seededReactionCounts(graph)
	var wantUniqueLikes int64
	for _, count := range likeCounts {
		wantUniqueLikes += int64(count)
	}
	var likeRows, commentRows int
	var counterLikes, counterComments int64
	for _, statement := range state.recorded() {
		switch {
		case strings.HasPrefix(statement.Query, "INSERT INTO likes"):
			if !strings.HasSuffix(statement.Query, "ON CONFLICT DO NOTHING") {
				t.Error("likes were inserted without ON CONFLICT DO NOTHING, so a repeated (post, liker) would fail the seed")
			}
			likeRows += len(statement.Args) / 4
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

// TestPostgresRequestsPerOperationAreCounted: R1 to R7, M1 and Q1 against the
// "Req" column of spec 6.3, counted as statements.
func TestPostgresRequestsPerOperationAreCounted(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	state.answer("FROM jobs WHERE outbox", []string{"id"}, []driver.Value{int64(1)})
	ops := &postgresOps{engine: engine}
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

// TestPostgresMergedTimelineIsOneOrderedQuery: M1 is one indexed IN query
// ordered by created with the limit of 20 as its last argument.
func TestPostgresMergedTimelineIsOneOrderedQuery(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	ops := &postgresOps{engine: engine}
	if _, err := ops.mergedTimeline(context.Background(), []int{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	statements := state.recorded()
	if len(statements) != 1 || !strings.Contains(statements[0].Query, "author_id IN ($1,$2,$3) ORDER BY created DESC LIMIT $4") {
		t.Fatalf("M1 sent %v; want one IN query ordered by created descending with a numbered limit", mysqlStatementsOf(state))
	}
	if last := statements[0].Args[len(statements[0].Args)-1]; last != int64(mergedPostsTotal) {
		t.Errorf("M1 limit is %v; want %d", last, mergedPostsTotal)
	}
}

// TestPostgresTimeScan: timestamps come back in UTC whatever the session zone.
func TestPostgresTimeScan(t *testing.T) {
	var got pgTime
	if err := got.Scan(time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600))); err != nil || got.Location() != time.UTC || got.Hour() != 2 {
		t.Errorf("scanning a time.Time: %v, %v; want the same instant in UTC", got.Time, err)
	}
	if err := got.Scan(42); err == nil {
		t.Error("scanning an int succeeded")
	}
	if err := got.Scan(nil); err == nil {
		t.Error("scanning NULL succeeded; every timestamp column is NOT NULL")
	}
}

// TestPostgresStatementsAreNumberedNotQuestionMarks: no statement the engine
// can send carries a MySQL placeholder, which the Postgres wire protocol rejects.
func TestPostgresStatementsAreNumberedNotQuestionMarks(t *testing.T) {
	engine, state := newFakePostgresEngine(t)
	state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, []driver.Value{int64(7)})
	state.answer("FROM notifications", []string{"id"}, []driver.Value{int64(11)})
	state.answer("FROM jobs WHERE outbox", []string{"id"}, []driver.Value{int64(1)})
	ops := &postgresOps{engine: engine}
	ctx := context.Background()
	post := postRecord{Author: "3", Seq: 100, Text: "t", Created: time.Now().UTC()}
	_, _ = ops.homeTimeline(ctx, 3, 20)
	_, _ = ops.profile(ctx, 3)
	_, _ = ops.profileTimeline(ctx, 3, 20)
	_, _ = ops.postPage(ctx, 3, 1)
	_, _ = ops.followCheck(ctx, 3, 4)
	_, _ = ops.followingList(ctx, 3, 5)
	_, _ = ops.avatar(ctx, 3)
	_, _ = ops.readNotifications(ctx, 3, 5)
	_, _ = ops.publishPost(ctx, post, noLedger{})
	_ = ops.likePost(ctx, 4, 3, 1)
	_ = ops.follow(ctx, 4, 5)
	_ = ops.unfollow(ctx, 4, 5)
	_ = ops.commentOnPost(ctx, 4, 3, 1, "c")
	_ = ops.uploadAvatar(ctx, 3, []byte("a"))
	_, _ = ops.mergedTimeline(ctx, []int{1, 2})
	_, _ = ops.queueRound(ctx, 1)
	if len(state.recorded()) == 0 {
		t.Fatal("no statements were sent")
	}
	for _, query := range mysqlStatementsOf(state) {
		if strings.Contains(query, "?") {
			t.Errorf("%q carries a question-mark placeholder", query)
		}
		for _, mysqlOnly := range []string{"INSERT IGNORE", "REPLACE INTO", "ON DUPLICATE KEY"} {
			if strings.Contains(query, mysqlOnly) {
				t.Errorf("%q uses %s, which PostgreSQL does not have", query, mysqlOnly)
			}
		}
	}
}

// livePostgresWorld connects to the server SOCIAL_LIVE_POSTGRES_DSN names,
// in a scratch database it drops afterwards, or skips the test.
func livePostgresWorld(t *testing.T, seeded bool) *socialWorld {
	t.Helper()
	dsn := os.Getenv("SOCIAL_LIVE_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SOCIAL_LIVE_POSTGRES_DSN (a postgresql:// URL for a server where the account may CREATE DATABASE, see HARNESS.md \"PostgreSQL leg\") to run against a real server; none is installed on the build host")
	}
	database := fmt.Sprintf("social_live_%d", time.Now().UnixNano())
	admin := dsn
	dsn, err := postgresDSNWithDatabase(dsn, database)
	if err != nil {
		t.Fatalf("rewriting SOCIAL_LIVE_POSTGRES_DSN: %v", err)
	}
	engine, err := newPostgresEngine(context.Background(), &config{Engine: enginePostgres, PostgresDSN: dsn, PostgresTLS: true, Database: database, Users: tinySpec.Users, Conc: 4, OpTimeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("connecting to PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		engine.close()
		admin, err := openPostgresPool(admin, 1, false)
		if err != nil {
			t.Logf("opening a connection to drop %s: %v", database, err)
			return
		}
		defer func() { _ = admin.Close() }() // only used to DROP DATABASE
		// WITH (FORCE) ends any session the pool has not yet closed.
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Logf("dropping %s: %v", database, err)
		}
	})
	world := &socialWorld{social: engine}
	if seeded {
		world.seed(t)
	}
	return world
}

// TestPostgresOperationContractLive is the integration half of
// TestPostgresOperationContract.
func TestPostgresOperationContractLive(t *testing.T) {
	checkOperationContract(t, livePostgresWorld(t, true))
}

// TestPostgresMergedTimelineIsTheTrueNewestLive is the M1 ruling on PostgreSQL.
func TestPostgresMergedTimelineIsTheTrueNewestLive(t *testing.T) {
	checkMergedTimelineIsTrueNewest(t, livePostgresWorld(t, true))
}

// TestPostgresCanonicalUserMatchesGraphLive: the seeded PostgreSQL dataset
// hashes to what the generator says it holds.
func TestPostgresCanonicalUserMatchesGraphLive(t *testing.T) {
	checkCanonicalMatchesGraph(t, livePostgresWorld(t, true))
}

// TestPostgresDuplicateLikeAndFollowLive: a repeated like or follow is absorbed
// by the key: no error, the counters move once, and a repeated like still
// notifies the author.
func TestPostgresDuplicateLikeAndFollowLive(t *testing.T) {
	world := livePostgresWorld(t, true)
	ctx := context.Background()
	ops := world.ops(t)
	engine := world.social.(*postgresEngine)
	count := func(query string, args ...any) int64 {
		var total int64
		if err := engine.db.QueryRowContext(ctx, query, args...).Scan(&total); err != nil {
			t.Fatal(err)
		}
		return total
	}
	const liker, author, seq = 40, 3, 1
	noticeQuery := "SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND type = 'like' AND actor = $2"
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
	if unique := count("SELECT COUNT(*) FROM likes WHERE author_id = $1 AND seq = $2", author, seq); page.Likes != unique {
		t.Errorf("likes_n is %d for %d like rows; a repeated like must not raise it", page.Likes, unique)
	}
	if notices := count(noticeQuery, author, liker); notices != noticesBefore+2 {
		t.Errorf("the author has %d like notifications from the liker, %d before; want 2 more (the repeat still notifies)", notices, noticesBefore)
	}
}
