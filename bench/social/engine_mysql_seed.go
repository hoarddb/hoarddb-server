package main

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// The tables of the social schema (spec 6.2): normalised InnoDB tables with the
// keys a competent user would add and no foreign keys (Q-D). Counters are
// denormalised columns, the idiomatic choice for hot reads.
const (
	mysqlTableUsers = `CREATE TABLE users (
  id BIGINT UNSIGNED NOT NULL,
  name VARCHAR(255) NOT NULL,
  bio TEXT NOT NULL,
  joined DATETIME(3) NOT NULL,
  followers_n INT NOT NULL,
  following_n INT NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	// The clustered key (author_id, seq) serves "newest N of an author" as a
	// backward range scan: no sort and no secondary index.
	mysqlTablePosts = `CREATE TABLE posts (
  author_id BIGINT UNSIGNED NOT NULL,
  seq BIGINT NOT NULL,
  text TEXT NOT NULL,
  created DATETIME(3) NOT NULL,
  tags JSON NOT NULL,
  reply_to VARCHAR(48) NULL,
  likes_n INT NOT NULL,
  comments_n INT NOT NULL,
  PRIMARY KEY (author_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	// The primary key is the unique (post author, post sequence, liker) of P4.
	mysqlTableLikes = `CREATE TABLE likes (
  author_id BIGINT UNSIGNED NOT NULL,
  seq BIGINT NOT NULL,
  liker_id BIGINT UNSIGNED NOT NULL,
  ts DATETIME(3) NOT NULL,
  PRIMARY KEY (author_id, seq, liker_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	mysqlTableComments = `CREATE TABLE comments (
  author_id BIGINT UNSIGNED NOT NULL,
  seq BIGINT NOT NULL,
  id BIGINT NOT NULL,
  commenter BIGINT UNSIGNED NOT NULL,
  text TEXT NOT NULL,
  ts DATETIME(3) NOT NULL,
  PRIMARY KEY (author_id, seq, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	// The secondary key answers "who follows X" without a scan of every edge.
	mysqlTableFollows = `CREATE TABLE follows (
  follower_id BIGINT UNSIGNED NOT NULL,
  followee_id BIGINT UNSIGNED NOT NULL,
  since DATETIME(3) NOT NULL,
  PRIMARY KEY (follower_id, followee_id),
  KEY follows_by_followee (followee_id, follower_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	// seq and reply_to ride beside spec 6.2's columns because the canonical form
	// compares whole posts.
	mysqlTableFeed = `CREATE TABLE feed (
  user_id BIGINT UNSIGNED NOT NULL,
  id BIGINT NOT NULL,
  post_id VARCHAR(48) NOT NULL,
  author_id BIGINT UNSIGNED NOT NULL,
  seq BIGINT NOT NULL,
  text TEXT NOT NULL,
  created DATETIME(3) NOT NULL,
  tags JSON NOT NULL,
  reply_to VARCHAR(48) NULL,
  PRIMARY KEY (user_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	mysqlTableNotifications = `CREATE TABLE notifications (
  user_id BIGINT UNSIGNED NOT NULL,
  id BIGINT NOT NULL,
  type VARCHAR(16) NOT NULL,
  actor BIGINT UNSIGNED NOT NULL,
  target VARCHAR(64) NOT NULL,
  ts DATETIME(3) NOT NULL,
  PRIMARY KEY (user_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	mysqlTableMedia = `CREATE TABLE media (
  user_id BIGINT UNSIGNED NOT NULL,
  kind VARCHAR(16) NOT NULL,
  data MEDIUMBLOB NOT NULL,
  PRIMARY KEY (user_id, kind)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	// jobs backs the queue operation Q1: a claim takes the oldest unleased rows
	// of one outbox, so the key is (outbox, lease_until, id).
	mysqlTableJobs = `CREATE TABLE jobs (
  id BIGINT NOT NULL,
  outbox INT NOT NULL,
  lease_until DATETIME(3) NOT NULL,
  created DATETIME(3) NOT NULL,
  PRIMARY KEY (id),
  KEY jobs_by_outbox (outbox, lease_until, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	// mysqlTableExists is the server error for CREATE TABLE on an existing table.
	mysqlTableExists = 1050
	// mysqlDeadlock is the server error that aborts one of two transactions
	// that lock the same rows in opposite orders; MySQL asks the client to
	// retry it.
	mysqlDeadlock = 1213
	// mysqlDeadlockAttempts bounds the tries of one INSERT, the first included.
	mysqlDeadlockAttempts = 20
	// mysqlInsertRows bounds the rows of one multi-row INSERT, which keeps a
	// statement a few megabytes at most.
	mysqlInsertRows = 500
	// mysqlUserBatch and mysqlReactionBatch are how many users, or reactions,
	// one seeding task carries.
	mysqlUserBatch     = 50
	mysqlReactionBatch = 1000
)

// mysqlDeadlockBackoff is the wait after the first deadlock; it grows with each
// further attempt, so parallel workers that collided do not collide again in step.
var mysqlDeadlockBackoff = 10 * time.Millisecond

// mysqlSocialTables lists the tables in creation order, with their names.
var mysqlSocialTables = []struct{ Name, DDL string }{
	{"users", mysqlTableUsers}, {"posts", mysqlTablePosts}, {"likes", mysqlTableLikes},
	{"comments", mysqlTableComments}, {"follows", mysqlTableFollows}, {"feed", mysqlTableFeed},
	{"notifications", mysqlTableNotifications}, {"media", mysqlTableMedia}, {"jobs", mysqlTableJobs},
}

// mysqlNeverLeased is the lease_until of a job nobody has claimed; DATETIME
// starts in the year 1000, so the zero time cannot stand for it.
var mysqlNeverLeased = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// mysqlDateTime is how DATETIME(3) columns are read back: the driver returns
// bytes unless the DSN asks for parseTime, and the tool does not depend on it.
type mysqlDateTime struct{ time.Time }

// Scan implements sql.Scanner.
func (d *mysqlDateTime) Scan(source any) error {
	switch value := source.(type) {
	case time.Time:
		d.Time = value.UTC()
		return nil
	case []byte:
		return d.parse(string(value))
	case string:
		return d.parse(value)
	default:
		return fmt.Errorf("a DATETIME column came back as %T", source)
	}
}

func (d *mysqlDateTime) parse(text string) error {
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05.000", text, time.UTC)
	if err != nil {
		return fmt.Errorf("parsing the DATETIME %q: %w", text, err)
	}
	d.Time = parsed
	return nil
}

var _ sql.Scanner = (*mysqlDateTime)(nil)

// tagsJSON encodes a post's tags for the JSON column.
func tagsJSON(tags []string) (string, error) {
	encoded, err := json.Marshal(tagsOrEmpty(tags))
	if err != nil {
		return "", fmt.Errorf("encoding tags: %w", err)
	}
	return string(encoded), nil
}

// nullIfEmpty stores an absent reply as NULL, as a SQL schema would.
func nullIfEmpty(text string) driver.Value {
	if text == "" {
		return nil
	}
	return text
}

// placeholders returns n comma-separated question marks.
func placeholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

// rowPlaceholders returns rows groups of columns placeholders: "(?,?),(?,?)".
func rowPlaceholders(rows, columns int) string {
	group := "(" + placeholders(columns) + ")"
	return strings.TrimSuffix(strings.Repeat(group+",", rows), ",")
}

// insertRows runs one multi-row INSERT per mysqlInsertRows rows on the
// interpolating connection pool, so a statement is one round trip however many
// rows it carries. verb is "INSERT" or "INSERT IGNORE".
func (m *mysqlEngine) insertRows(ctx context.Context, verb, table string, columns []string, rows [][]any) error {
	for len(rows) > 0 {
		batch := rows[:min(len(rows), mysqlInsertRows)]
		rows = rows[len(batch):]
		args := make([]any, 0, len(batch)*len(columns))
		for _, row := range batch {
			args = append(args, row...)
		}
		query := fmt.Sprintf("%s INTO %s (%s) VALUES %s", verb, table, strings.Join(columns, ", "), rowPlaceholders(len(batch), len(columns)))
		if err := m.execRetryingDeadlock(ctx, table, query, args); err != nil {
			return fmt.Errorf("inserting %d rows into %s: %w", len(batch), table, err)
		}
	}
	return nil
}

// execRetryingDeadlock runs one bulk INSERT, trying again when the server
// aborts it as a deadlock victim (parallel seed workers inserting into the same
// index pages do this routinely). Each retry is counted in seedDeadlockRetries.
func (m *mysqlEngine) execRetryingDeadlock(ctx context.Context, table, query string, args []any) error {
	for attempt := 1; ; attempt++ {
		_, err := m.bulkDB.ExecContext(ctx, query, args...)
		var serverError *mysql.MySQLError
		if err == nil || !errors.As(err, &serverError) || serverError.Number != mysqlDeadlock {
			return err
		}
		if attempt == mysqlDeadlockAttempts {
			return fmt.Errorf("deadlock (MySQL error %d) on %s persisted through %d attempts: %w", mysqlDeadlock, table, attempt, err)
		}
		m.seedDeadlockRetries.Add(1)
		wait := time.NewTimer(time.Duration(attempt) * mysqlDeadlockBackoff)
		select {
		case <-ctx.Done():
			wait.Stop()
			return fmt.Errorf("waiting to retry a deadlock on %s: %w", table, ctx.Err())
		case <-wait.C:
		}
	}
}

// seedRetries is how many deadlock retries seeding needed.
func (m *mysqlEngine) seedRetries() int64 { return m.seedDeadlockRetries.Load() }

// setupSocial creates every table of the social schema, refusing a database
// that already holds one.
func (m *mysqlEngine) setupSocial(ctx context.Context, _ datasetSpec) (schemaInfo, error) {
	detail := make([]string, 0, len(mysqlSocialTables))
	for _, table := range mysqlSocialTables {
		if _, err := m.db.ExecContext(ctx, table.DDL); err != nil {
			var serverError *mysql.MySQLError
			if errors.As(err, &serverError) && serverError.Number == mysqlTableExists {
				return schemaInfo{}, fmt.Errorf("table %s already exists in the database; pass -attach to reuse the tables or drop them first: %w", table.Name, err)
			}
			return schemaInfo{}, fmt.Errorf("creating table %s: %w", table.Name, err)
		}
		detail = append(detail, table.DDL)
	}
	return schemaInfo{Layout: "tables-per-entity", Detail: detail, Buckets: len(mysqlSocialTables)}, nil
}

// seedSocial writes the dataset: users in batches, then every like and comment
// once the posts exist.
func (m *mysqlEngine) seedSocial(ctx context.Context, graph *socialGraph, conc int) error {
	likeCounts, commentCounts := seededReactionCounts(graph)
	err := seedPool(ctx, conc, "seeding users", chunks(graph.users(), mysqlUserBatch), func(ctx context.Context, batch []graphUser) error {
		return m.seedUsers(ctx, graph, batch, likeCounts, commentCounts)
	})
	if err != nil {
		return err
	}
	err = seedPool(ctx, conc, "seeding media", graph.media(), func(ctx context.Context, item mediaItem) error {
		return m.insertRows(ctx, "INSERT", "media", []string{"user_id", "kind", "data"}, [][]any{{uint64(item.Owner), mediaName(item.Kind), graph.content(item)}})
	})
	if err != nil {
		return err
	}
	// INSERT IGNORE is the engine's own dedupe of the repeated (post, liker)
	// pairs the generator draws: the primary key makes the repeat a no-op.
	err = seedPool(ctx, conc, "seeding likes", chunks(graph.likes(), mysqlReactionBatch), func(ctx context.Context, batch []reaction) error {
		rows := make([][]any, len(batch))
		for i, like := range batch {
			rows[i] = []any{uint64(like.PostAuthor), int64(like.PostSeq), uint64(like.Actor), graphEpoch}
		}
		// Workers that lock index entries in the same (author, seq, liker)
		// order wait for each other instead of deadlocking.
		slices.SortFunc(rows, func(left, right []any) int {
			return cmp.Or(cmp.Compare(left[0].(uint64), right[0].(uint64)), cmp.Compare(left[1].(int64), right[1].(int64)), cmp.Compare(left[2].(uint64), right[2].(uint64)))
		})
		return m.insertRows(ctx, "INSERT IGNORE", "likes", []string{"author_id", "seq", "liker_id", "ts"}, rows)
	})
	if err != nil {
		return err
	}
	return seedPool(ctx, conc, "seeding comments", chunks(graph.comments(), mysqlReactionBatch), func(ctx context.Context, batch []reaction) error {
		rows := make([][]any, len(batch))
		for i, comment := range batch {
			rows[i] = []any{uint64(comment.PostAuthor), int64(comment.PostSeq), m.ids.next(time.Now().UnixMilli()), uint64(comment.Actor), seededCommentText, graphEpoch}
		}
		return m.insertRows(ctx, "INSERT", "comments", []string{"author_id", "seq", "id", "commenter", "text", "ts"}, rows)
	})
}

// seedUsers writes a batch of users with their posts, follow edges, feed and
// inbox. A user's feed is written oldest first with rising ids.
func (m *mysqlEngine) seedUsers(ctx context.Context, graph *socialGraph, batch []graphUser, likeCounts, commentCounts []uint16) error {
	var users, posts, follows, feed, notifications [][]any
	for _, user := range batch {
		rank := uint64(user.Rank)
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
			follows = append(follows, []any{rank, uint64(followee), graphEpoch})
		}
		for _, ref := range user.Feed {
			post := recordOf(graph.post(int(ref.Author), int(ref.Seq)))
			tags, err := tagsJSON(post.Tags)
			if err != nil {
				return err
			}
			feed = append(feed, []any{rank, m.ids.next(time.Now().UnixMilli()), postKeyString(post.Author, post.Seq), uint64(ref.Author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)})
		}
		for _, note := range user.Inbox {
			notifications = append(notifications, []any{rank, m.ids.next(time.Now().UnixMilli()), note.Kind, uint64(note.Actor), profile.ID, graphEpoch})
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
		if err := m.insertRows(ctx, "INSERT", step.table, step.columns, step.rows); err != nil {
			return err
		}
	}
	return nil
}
