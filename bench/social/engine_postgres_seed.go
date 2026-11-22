package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// The tables of the social schema (spec 6.2) in PostgreSQL: the same tables,
// keys and secondary indexes as the MySQL schema, no foreign keys (Q-D),
// counters denormalised into columns. The types are what a PostgreSQL user
// would choose: BIGINT for ids (there is no unsigned type), TIMESTAMPTZ(3) for
// DATETIME(3), JSONB for JSON and BYTEA for MEDIUMBLOB. A PostgreSQL table is a
// heap, so there is no clustered key to play the part InnoDB's does: every
// primary key is a b-tree beside the heap and a range read is an index scan
// with a heap fetch per row. CLUSTER would order the heap once but not keep it
// ordered under writes, so a user running a live workload does not rely on it.
const (
	postgresTableUsers = `CREATE TABLE users (
  id BIGINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  bio TEXT NOT NULL,
  joined TIMESTAMPTZ(3) NOT NULL,
  followers_n INTEGER NOT NULL,
  following_n INTEGER NOT NULL,
  PRIMARY KEY (id)
)`

	// The primary key (author_id, seq) serves "newest N of an author" as a
	// backward range scan: no sort and no secondary index.
	postgresTablePosts = `CREATE TABLE posts (
  author_id BIGINT NOT NULL,
  seq BIGINT NOT NULL,
  text TEXT NOT NULL,
  created TIMESTAMPTZ(3) NOT NULL,
  tags JSONB NOT NULL,
  reply_to VARCHAR(48) NULL,
  likes_n INTEGER NOT NULL,
  comments_n INTEGER NOT NULL,
  PRIMARY KEY (author_id, seq)
)`

	// The primary key is the unique (post author, post sequence, liker) of P4.
	postgresTableLikes = `CREATE TABLE likes (
  author_id BIGINT NOT NULL,
  seq BIGINT NOT NULL,
  liker_id BIGINT NOT NULL,
  ts TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (author_id, seq, liker_id)
)`

	postgresTableComments = `CREATE TABLE comments (
  author_id BIGINT NOT NULL,
  seq BIGINT NOT NULL,
  id BIGINT NOT NULL,
  commenter BIGINT NOT NULL,
  text TEXT NOT NULL,
  ts TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (author_id, seq, id)
)`

	// The secondary index (postgresIndexFollows) answers "who follows X" without a
	// scan of every edge; PostgreSQL declares an index in its own statement.
	postgresTableFollows = `CREATE TABLE follows (
  follower_id BIGINT NOT NULL,
  followee_id BIGINT NOT NULL,
  since TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (follower_id, followee_id)
)`

	// seq and reply_to ride beside spec 6.2's columns because the canonical form
	// compares whole posts.
	postgresTableFeed = `CREATE TABLE feed (
  user_id BIGINT NOT NULL,
  id BIGINT NOT NULL,
  post_id VARCHAR(48) NOT NULL,
  author_id BIGINT NOT NULL,
  seq BIGINT NOT NULL,
  text TEXT NOT NULL,
  created TIMESTAMPTZ(3) NOT NULL,
  tags JSONB NOT NULL,
  reply_to VARCHAR(48) NULL,
  PRIMARY KEY (user_id, id)
)`

	postgresTableNotifications = `CREATE TABLE notifications (
  user_id BIGINT NOT NULL,
  id BIGINT NOT NULL,
  type VARCHAR(16) NOT NULL,
  actor BIGINT NOT NULL,
  target VARCHAR(64) NOT NULL,
  ts TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (user_id, id)
)`

	postgresTableMedia = `CREATE TABLE media (
  user_id BIGINT NOT NULL,
  kind VARCHAR(16) NOT NULL,
  data BYTEA NOT NULL,
  PRIMARY KEY (user_id, kind)
)`

	// jobs backs the queue operation Q1: a claim takes the oldest unleased rows
	// of one outbox, so the index (postgresIndexJobs) is (outbox, lease_until, id).
	postgresTableJobs = `CREATE TABLE jobs (
  id BIGINT NOT NULL,
  outbox INTEGER NOT NULL,
  lease_until TIMESTAMPTZ(3) NOT NULL,
  created TIMESTAMPTZ(3) NOT NULL,
  PRIMARY KEY (id)
)`

	postgresIndexFollows = "CREATE INDEX follows_by_followee ON follows (followee_id, follower_id)"
	postgresIndexJobs    = "CREATE INDEX jobs_by_outbox ON jobs (outbox, lease_until, id)"
)

// postgresSocialTables lists the tables in creation order, with their names.
var postgresSocialTables = []struct {
	Name, DDL string
	Indexes   []string
}{
	{"users", postgresTableUsers, nil}, {"posts", postgresTablePosts, nil}, {"likes", postgresTableLikes, nil},
	{"comments", postgresTableComments, nil}, {"follows", postgresTableFollows, []string{postgresIndexFollows}},
	{"feed", postgresTableFeed, nil}, {"notifications", postgresTableNotifications, nil}, {"media", postgresTableMedia, nil},
	{"jobs", postgresTableJobs, []string{postgresIndexJobs}},
}

// insertRows runs one multi-row INSERT per pgInsertRows rows on the
// unprepared pool, so a statement is one round trip however many rows it
// carries. conflict is "" or onConflictDoNothing.
func (c *postgresEngine) insertRows(ctx context.Context, table string, columns []string, rows [][]any, conflict string) error {
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
		if err := c.execRetrying(ctx, table, query, args); err != nil {
			return fmt.Errorf("inserting %d rows into %s: %w", len(batch), table, err)
		}
	}
	return nil
}

// execRetrying runs one bulk INSERT, trying again when the server aborted it
// as a deadlock victim or asks for a serialization retry (parallel seed workers
// writing neighbouring keys can do this). Each retry is counted in seedRetryCount.
func (c *postgresEngine) execRetrying(ctx context.Context, table, query string, args []any) error {
	for attempt := 1; ; attempt++ {
		_, err := c.bulkDB.ExecContext(ctx, query, args...)
		if !isPostgresRetryable(err) {
			return err
		}
		if attempt == pgRetryAttempts {
			return fmt.Errorf("a retryable failure on %s persisted through %d attempts: %w", table, attempt, err)
		}
		c.seedRetryCount.Add(1)
		if err := waitToRetry(ctx, attempt, "a retryable failure on "+table); err != nil {
			return err
		}
	}
}

// seedRetries is how many retries seeding needed.
func (c *postgresEngine) seedRetries() int64 { return c.seedRetryCount.Load() }

// setupSocial creates every table of the social schema, refusing a database
// that already holds one.
func (c *postgresEngine) setupSocial(ctx context.Context, _ datasetSpec) (schemaInfo, error) {
	detail := make([]string, 0, len(postgresSocialTables))
	for _, table := range postgresSocialTables {
		if _, err := c.db.ExecContext(ctx, table.DDL); err != nil {
			var serverError *pgconn.PgError
			if errors.As(err, &serverError) && serverError.Code == postgresTableExists {
				return schemaInfo{}, fmt.Errorf("table %s already exists in the database; pass -attach to reuse the tables or drop them first: %w", table.Name, err)
			}
			return schemaInfo{}, fmt.Errorf("creating table %s: %w", table.Name, err)
		}
		detail = append(detail, table.DDL)
		for _, index := range table.Indexes {
			if _, err := c.db.ExecContext(ctx, index); err != nil {
				return schemaInfo{}, fmt.Errorf("creating an index on %s: %w", table.Name, err)
			}
			detail = append(detail, index)
		}
	}
	return schemaInfo{Layout: "tables-per-entity", Detail: detail, Buckets: len(postgresSocialTables)}, nil
}

// seedSocial writes the dataset: users in batches, then every like and comment
// once the posts exist.
func (c *postgresEngine) seedSocial(ctx context.Context, graph *socialGraph, conc int) error {
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
func (c *postgresEngine) seedUsers(ctx context.Context, graph *socialGraph, batch []graphUser, likeCounts, commentCounts []uint16) error {
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
