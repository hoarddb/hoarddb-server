package main

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// The tables of the social schema (spec 6.2) in CockroachDB: the same tables,
// keys and secondary indexes as the MySQL schema, no foreign keys (Q-D),
// counters denormalised into columns. The types are CockroachDB's: INT8 for
// ids (there is no unsigned type), TIMESTAMPTZ(3) for DATETIME(3), JSONB for
// JSON and BYTEA for MEDIUMBLOB. CockroachDB stores every table in a primary-key
// ordered LSM, so the clustered key plays the part InnoDB's does.
const (
	cockroachTableUsers = `CREATE TABLE users (
  id INT8 NOT NULL,
  name VARCHAR(255) NOT NULL,
  bio TEXT NOT NULL,
  joined TIMESTAMPTZ(3) NOT NULL,
  followers_n INT4 NOT NULL,
  following_n INT4 NOT NULL,
  PRIMARY KEY (id)
)`

	// The primary key (author_id, seq) serves "newest N of an author" as a
	// backward range scan: no sort and no secondary index.
	cockroachTablePosts = `CREATE TABLE posts (
  author_id INT8 NOT NULL,
  seq INT8 NOT NULL,
  text TEXT NOT NULL,
  created TIMESTAMPTZ(3) NOT NULL,
  tags JSONB NOT NULL,
  reply_to VARCHAR(48) NULL,
  likes_n INT4 NOT NULL,
  comments_n INT4 NOT NULL,
  PRIMARY KEY (author_id, seq)
)`

	// The primary key is the unique (post author, post sequence, liker) of P4.
	cockroachTableLikes = `CREATE TABLE likes (
  author_id INT8 NOT NULL,
  seq INT8 NOT NULL,
  liker_id INT8 NOT NULL,
  ts TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (author_id, seq, liker_id)
)`

	cockroachTableComments = `CREATE TABLE comments (
  author_id INT8 NOT NULL,
  seq INT8 NOT NULL,
  id INT8 NOT NULL,
  commenter INT8 NOT NULL,
  text TEXT NOT NULL,
  ts TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (author_id, seq, id)
)`

	// The secondary index answers "who follows X" without a scan of every edge.
	cockroachTableFollows = `CREATE TABLE follows (
  follower_id INT8 NOT NULL,
  followee_id INT8 NOT NULL,
  since TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (follower_id, followee_id),
  INDEX follows_by_followee (followee_id, follower_id)
)`

	// seq and reply_to ride beside spec 6.2's columns because the canonical form
	// compares whole posts.
	cockroachTableFeed = `CREATE TABLE feed (
  user_id INT8 NOT NULL,
  id INT8 NOT NULL,
  post_id VARCHAR(48) NOT NULL,
  author_id INT8 NOT NULL,
  seq INT8 NOT NULL,
  text TEXT NOT NULL,
  created TIMESTAMPTZ(3) NOT NULL,
  tags JSONB NOT NULL,
  reply_to VARCHAR(48) NULL,
  PRIMARY KEY (user_id, id)
)`

	cockroachTableNotifications = `CREATE TABLE notifications (
  user_id INT8 NOT NULL,
  id INT8 NOT NULL,
  type VARCHAR(16) NOT NULL,
  actor INT8 NOT NULL,
  target VARCHAR(64) NOT NULL,
  ts TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (user_id, id)
)`

	cockroachTableMedia = `CREATE TABLE media (
  user_id INT8 NOT NULL,
  kind VARCHAR(16) NOT NULL,
  data BYTEA NOT NULL,
  PRIMARY KEY (user_id, kind)
)`

	// jobs backs the queue operation Q1: a claim takes the oldest unleased rows
	// of one outbox, so the index is (outbox, lease_until, id).
	cockroachTableJobs = `CREATE TABLE jobs (
  id INT8 NOT NULL,
  outbox INT4 NOT NULL,
  lease_until TIMESTAMPTZ(3) NOT NULL,
  created TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (id),
  INDEX jobs_by_outbox (outbox, lease_until, id)
)`

	// cockroachRestartCode is the SQLSTATE of serialization_failure, which
	// CockroachDB returns when two transactions contend and one must run again.
	cockroachRestartCode = "40001"
	// pgRetryAttempts bounds the tries of one INSERT, transaction or
	// single statement, the first included.
	pgRetryAttempts = 20
	// pgInsertRows bounds the rows of one multi-row INSERT, which keeps a
	// statement a few megabytes at most and its parameters under the protocol's 65,535.
	pgInsertRows = 500
	// pgUserBatch and pgReactionBatch are how many users, or
	// reactions, one seeding task carries.
	pgUserBatch     = 50
	pgReactionBatch = 1000

	// onConflictDoNothing makes an INSERT skip a row whose key exists, the
	// engine's own dedupe of repeated keys (MySQL's INSERT IGNORE).
	onConflictDoNothing = "ON CONFLICT DO NOTHING"
)

// pgRetryBackoff is the wait after the first restart; it grows with each
// further attempt, so parallel workers that collided do not collide again in step.
var pgRetryBackoff = 10 * time.Millisecond

// cockroachSocialTables lists the tables in creation order, with their names.
var cockroachSocialTables = []struct{ Name, DDL string }{
	{"users", cockroachTableUsers}, {"posts", cockroachTablePosts}, {"likes", cockroachTableLikes},
	{"comments", cockroachTableComments}, {"follows", cockroachTableFollows}, {"feed", cockroachTableFeed},
	{"notifications", cockroachTableNotifications}, {"media", cockroachTableMedia}, {"jobs", cockroachTableJobs},
}

// pgTime is how TIMESTAMPTZ(3) columns are read back: the driver returns
// a time.Time in the session's zone, and the canonical form compares instants in UTC.
type pgTime struct{ time.Time }

// Scan implements sql.Scanner.
func (t *pgTime) Scan(source any) error {
	moment, ok := source.(time.Time)
	if !ok {
		return fmt.Errorf("a TIMESTAMPTZ column came back as %T", source)
	}
	t.Time = moment.UTC()
	return nil
}

var _ sql.Scanner = (*pgTime)(nil)

// pgPlaceholders returns count numbered parameters starting at first:
// "$4,$5" for (4, 2). The Postgres wire protocol numbers its parameters.
func pgPlaceholders(first, count int) string {
	numbered := make([]string, count)
	for i := range numbered {
		numbered[i] = "$" + strconv.Itoa(first+i)
	}
	return strings.Join(numbered, ",")
}

// pgRowPlaceholders returns rows groups of columns numbered parameters,
// counting on across the groups: "($1,$2),($3,$4)".
func pgRowPlaceholders(rows, columns int) string {
	groups := make([]string, rows)
	for row := range groups {
		groups[row] = "(" + pgPlaceholders(row*columns+1, columns) + ")"
	}
	return strings.Join(groups, ",")
}

// waitToRetry sleeps the backoff of attempt, or returns early with the
// context's error when the run is cancelled.
func waitToRetry(ctx context.Context, attempt int, doing string) error {
	wait := time.NewTimer(time.Duration(attempt) * pgRetryBackoff)
	defer wait.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting to retry %s: %w", doing, ctx.Err())
	case <-wait.C:
		return nil
	}
}

// insertRows runs one multi-row INSERT per pgInsertRows rows on the
// unprepared pool, so a statement is one round trip however many rows it
// carries. conflict is "" or onConflictDoNothing.
func (c *cockroachEngine) insertRows(ctx context.Context, table string, columns []string, rows [][]any, conflict string) error {
	for len(rows) > 0 {
		batch := rows[:min(len(rows), pgInsertRows)]
		rows = rows[len(batch):]
		args := make([]any, 0, len(batch)*len(columns))
		for _, row := range batch {
			args = append(args, row...)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ", "), pgRowPlaceholders(len(batch), len(columns)))
		if conflict != "" {
			query += " " + conflict
		}
		if err := c.execRetryingRestart(ctx, table, query, args); err != nil {
			return fmt.Errorf("inserting %d rows into %s: %w", len(batch), table, err)
		}
	}
	return nil
}

// execRetryingRestart runs one bulk INSERT, trying again when the server asks
// the client to restart it (parallel seed workers writing neighbouring keys do
// this routinely). Each retry is counted in seedRetryCount.
func (c *cockroachEngine) execRetryingRestart(ctx context.Context, table, query string, args []any) error {
	for attempt := 1; ; attempt++ {
		_, err := c.bulkDB.ExecContext(ctx, query, args...)
		if !isCockroachRestart(err) {
			return err
		}
		if attempt == pgRetryAttempts {
			return fmt.Errorf("restart (SQLSTATE %s) on %s persisted through %d attempts: %w", cockroachRestartCode, table, attempt, err)
		}
		c.seedRetryCount.Add(1)
		if err := waitToRetry(ctx, attempt, "a restart on "+table); err != nil {
			return err
		}
	}
}

// seedRetries is how many restarts seeding needed.
func (c *cockroachEngine) seedRetries() int64 { return c.seedRetryCount.Load() }

// setupSocial creates every table of the social schema, refusing a database
// that already holds one.
func (c *cockroachEngine) setupSocial(ctx context.Context, _ datasetSpec) (schemaInfo, error) {
	detail := make([]string, 0, len(cockroachSocialTables))
	for _, table := range cockroachSocialTables {
		if _, err := c.db.ExecContext(ctx, table.DDL); err != nil {
			var serverError *pgconn.PgError
			if errors.As(err, &serverError) && serverError.Code == cockroachTableExists {
				return schemaInfo{}, fmt.Errorf("table %s already exists in the database; pass -attach to reuse the tables or drop them first: %w", table.Name, err)
			}
			return schemaInfo{}, fmt.Errorf("creating table %s: %w", table.Name, err)
		}
		detail = append(detail, table.DDL)
	}
	return schemaInfo{Layout: "tables-per-entity", Detail: detail, Buckets: len(cockroachSocialTables)}, nil
}

// seedSocial writes the dataset: users in batches, then every like and comment
// once the posts exist.
func (c *cockroachEngine) seedSocial(ctx context.Context, graph *socialGraph, conc int) error {
	likeCounts, commentCounts := seededReactionCounts(graph)
	err := seedPool(ctx, conc, "seeding users", chunks(graph.users(), pgUserBatch), func(ctx context.Context, batch []graphUser) error {
		return c.seedUsers(ctx, graph, batch, likeCounts, commentCounts)
	})
	if err != nil {
		return err
	}
	err = seedPool(ctx, conc, "seeding media", graph.media(), func(ctx context.Context, item mediaItem) error {
		return c.insertRows(ctx, "media", []string{"user_id", "kind", "data"}, [][]any{{int64(item.Owner), mediaName(item.Kind), graph.content(item)}}, "")
	})
	if err != nil {
		return err
	}
	// ON CONFLICT DO NOTHING is the engine's own dedupe of the repeated (post,
	// liker) pairs the generator draws: the primary key makes the repeat a no-op.
	err = seedPool(ctx, conc, "seeding likes", chunks(graph.likes(), pgReactionBatch), func(ctx context.Context, batch []reaction) error {
		rows := make([][]any, len(batch))
		for i, like := range batch {
			rows[i] = []any{int64(like.PostAuthor), int64(like.PostSeq), int64(like.Actor), graphEpoch}
		}
		// Workers that write keys in the same (author, seq, liker) order wait for
		// each other instead of restarting.
		slices.SortFunc(rows, func(left, right []any) int {
			return cmp.Or(cmp.Compare(left[0].(int64), right[0].(int64)), cmp.Compare(left[1].(int64), right[1].(int64)), cmp.Compare(left[2].(int64), right[2].(int64)))
		})
		return c.insertRows(ctx, "likes", []string{"author_id", "seq", "liker_id", "ts"}, rows, onConflictDoNothing)
	})
	if err != nil {
		return err
	}
	return seedPool(ctx, conc, "seeding comments", chunks(graph.comments(), pgReactionBatch), func(ctx context.Context, batch []reaction) error {
		rows := make([][]any, len(batch))
		for i, comment := range batch {
			rows[i] = []any{int64(comment.PostAuthor), int64(comment.PostSeq), c.ids.next(time.Now().UnixMilli()), int64(comment.Actor), seededCommentText, graphEpoch}
		}
		return c.insertRows(ctx, "comments", []string{"author_id", "seq", "id", "commenter", "text", "ts"}, rows, "")
	})
}

// seedUsers writes a batch of users with their posts, follow edges, feed and
// inbox. A user's feed is written oldest first with rising ids.
func (c *cockroachEngine) seedUsers(ctx context.Context, graph *socialGraph, batch []graphUser, likeCounts, commentCounts []uint16) error {
	var users, posts, follows, feed, notifications [][]any
	for _, user := range batch {
		rank := int64(user.Rank)
		profile := user.Profile
		users = append(users, []any{rank, profile.Name, profile.Bio, profile.Joined, profile.Followers, profile.Following})
		for seq := range graph.postsPerUser {
			index := user.Rank*graph.postsPerUser + seq
			post := recordOf(graph.post(user.Rank, seq))
			tags, err := tagsJSON(post.Tags)
			if err != nil {
				return err
			}
			posts = append(posts, []any{rank, post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo), int64(likeCounts[index]), int64(commentCounts[index])})
		}
		for _, followee := range user.Following {
			follows = append(follows, []any{rank, int64(followee), graphEpoch})
		}
		for _, ref := range user.Feed {
			post := recordOf(graph.post(int(ref.Author), int(ref.Seq)))
			tags, err := tagsJSON(post.Tags)
			if err != nil {
				return err
			}
			feed = append(feed, []any{rank, c.ids.next(time.Now().UnixMilli()), postKeyString(post.Author, post.Seq), int64(ref.Author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)})
		}
		for _, note := range user.Inbox {
			notifications = append(notifications, []any{rank, c.ids.next(time.Now().UnixMilli()), note.Kind, int64(note.Actor), profile.ID, graphEpoch})
		}
	}
	for _, step := range []struct {
		table   string
		columns []string
		rows    [][]any
	}{
		{"users", []string{"id", "name", "bio", "joined", "followers_n", "following_n"}, users},
		{"posts", []string{"author_id", "seq", "text", "created", "tags", "reply_to", "likes_n", "comments_n"}, posts},
		{"follows", []string{"follower_id", "followee_id", "since"}, follows},
		{"feed", []string{"user_id", "id", "post_id", "author_id", "seq", "text", "created", "tags", "reply_to"}, feed},
		{"notifications", []string{"user_id", "id", "type", "actor", "target", "ts"}, notifications},
	} {
		if err := c.insertRows(ctx, step.table, step.columns, step.rows, ""); err != nil {
			return err
		}
	}
	return nil
}
