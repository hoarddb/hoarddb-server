package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// postgresOps is one worker's handle on the social operations. Statements run
// one after another (Q-A: one statement at a time); sent counts them, so an
// operation's request count is the difference around it. BEGIN and COMMIT are
// not counted: spec 6.3 counts the statements of a transaction. A transaction
// or statement PostgreSQL aborted as a deadlock victim (SQLSTATE 40P01) or for
// a serialization failure (40001) is run again, as its documentation tells a
// client to do, and each attempt's statements count.
type postgresOps struct {
	engine *postgresEngine
	sent   int64
	// unprepared holds the statements a transaction had to prepare on its own
	// connection because the pool had not prepared them yet; the pool prepares
	// them when the transaction is over.
	unprepared []string
}

func (c *postgresEngine) newSocialOps() (socialOps, error) { return &postgresOps{engine: c}, nil }

func (o *postgresOps) requests() int64 { return o.sent }
func (o *postgresOps) close()          {}

func (o *postgresOps) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, o.engine.cfg.OpTimeout)
}

// statement returns the prepared statement for query, bound to tx when given.
//
// Inside a transaction a statement the pool has not prepared yet is prepared on
// the transaction's own connection: asking the pool for a second one deadlocks
// when -depth workers each hold one.
func (o *postgresOps) statement(ctx context.Context, tx *sql.Tx, query string) (*sql.Stmt, error) {
	if tx == nil {
		return o.engine.prepared(ctx, query)
	}
	if cached, ok := o.engine.cachedStatement(query); ok {
		return tx.StmtContext(ctx, cached), nil
	}
	own, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("preparing %q in a transaction: %w", query, err)
	}
	o.unprepared = append(o.unprepared, query)
	return own, nil
}

// prepareDeferred prepares, on the pool and outside any transaction, what the
// last transaction prepared on its own connection, so the next one finds it cached.
func (o *postgresOps) prepareDeferred(ctx context.Context) error {
	queries := o.unprepared
	o.unprepared = nil
	for _, query := range queries {
		if _, err := o.engine.prepared(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

// exec runs one statement. Outside a transaction a retryable failure is retried
// here; in one it is the transaction's to retry, so it is returned.
func (o *postgresOps) exec(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	for attempt := 1; ; attempt++ {
		statement, err := o.statement(ctx, tx, query)
		if err != nil {
			return nil, err
		}
		o.sent++
		result, err := statement.ExecContext(ctx, args...)
		if err == nil {
			return result, nil
		}
		if tx != nil || !isPostgresRetryable(err) || attempt == pgRetryAttempts {
			return nil, fmt.Errorf("executing %q: %w", query, err)
		}
		o.engine.transactionRetries.Add(1)
		if err := waitToRetry(ctx, attempt, "a retryable failure of "+query); err != nil {
			return nil, err
		}
	}
}

// postgresQueryAll runs one SELECT and maps every row with scan.
func postgresQueryAll[T any](ctx context.Context, o *postgresOps, tx *sql.Tx, query string, scan func(*sql.Rows) (T, error), args ...any) ([]T, error) {
	statement, err := o.statement(ctx, tx, query)
	if err != nil {
		return nil, err
	}
	o.sent++
	rows, err := statement.QueryContext(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("querying %q: %w", query, err)
	}
	defer func() { _ = rows.Close() }() // rows.Err below reports a read failure
	var found []T
	for rows.Next() {
		row, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("reading %q: %w", query, err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading %q: %w", query, err)
	}
	return found, nil
}

// inTransaction runs body in one transaction on pool and commits once. When
// PostgreSQL aborts it for a deadlock or serialization failure, at any
// statement or at the commit, the transaction is rolled back and body runs
// again, up to pgRetryAttempts.
func (o *postgresOps) inTransaction(ctx context.Context, pool *sql.DB, body func(tx *sql.Tx) error) error {
	for attempt := 1; ; attempt++ {
		tx, err := pool.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("beginning a transaction: %w", err)
		}
		err = o.finish(tx, body(tx))
		if !isPostgresRetryable(err) || attempt == pgRetryAttempts {
			return errors.Join(err, o.prepareDeferred(ctx))
		}
		o.engine.transactionRetries.Add(1)
		if err := waitToRetry(ctx, attempt, "a retried transaction"); err != nil {
			return err
		}
	}
}

// finish commits tx when the body succeeded and rolls it back when it failed.
func (o *postgresOps) finish(tx *sql.Tx, bodyErr error) error {
	if bodyErr != nil {
		_ = tx.Rollback() // the failure being reported is the body's, not the rollback's
		return bodyErr
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing: %w", err)
	}
	return nil
}

const (
	postgresPostColumns   = "author_id, seq, text, created, tags, reply_to"
	postgresInsertNotice  = "INSERT INTO notifications (user_id, id, type, actor, target, ts) VALUES ($1, $2, $3, $4, $5, $6)"
	postgresBumpLikes     = "UPDATE posts SET likes_n = likes_n + 1 WHERE author_id = $1 AND seq = $2"
	postgresBumpComments  = "UPDATE posts SET comments_n = comments_n + 1 WHERE author_id = $1 AND seq = $2"
	postgresBumpFollowers = "UPDATE users SET followers_n = followers_n + $1 WHERE id = $2"
	postgresBumpFollowing = "UPDATE users SET following_n = following_n + $1 WHERE id = $2"
	postgresInsertPost    = "INSERT INTO posts (author_id, seq, text, created, tags, reply_to, likes_n, comments_n) VALUES ($1, $2, $3, $4, $5, $6, 0, 0)"
	postgresFeedColumns   = "INSERT INTO feed (user_id, id, post_id, author_id, seq, text, created, tags, reply_to) VALUES "
)

// postgresNeverLeased is the lease_until of a job nobody has claimed.
var postgresNeverLeased = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// scanPostgresPost reads the columns of postgresPostColumns.
func scanPostgresPost(rows *sql.Rows) (postRecord, error) {
	var post postRecord
	var author int64
	var created pgTime
	var tags []byte
	var replyTo sql.NullString
	if err := rows.Scan(&author, &post.Seq, &post.Text, &created, &tags, &replyTo); err != nil {
		return postRecord{}, err
	}
	if err := json.Unmarshal(tags, &post.Tags); err != nil {
		return postRecord{}, fmt.Errorf("decoding the tags %q: %w", tags, err)
	}
	post.Author, post.Created, post.ReplyTo = strconv.FormatInt(author, 10), created.Time, replyTo.String
	return post, nil
}

// notify inserts one notification for a user, inside tx.
func (o *postgresOps) notify(ctx context.Context, tx *sql.Tx, user int, kind string, actor int, target string) error {
	_, err := o.exec(ctx, tx, postgresInsertNotice, int64(user), o.engine.ids.next(time.Now().UnixMilli()), kind, int64(actor), target, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("notifying user %d of a %s: %w", user, kind, err)
	}
	return nil
}

func (o *postgresOps) homeTimeline(parent context.Context, actor, limit int) ([]postRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	return postgresQueryAll(ctx, o, nil, "SELECT "+postgresPostColumns+" FROM feed WHERE user_id = $1 ORDER BY id DESC LIMIT $2", scanPostgresPost, int64(actor), limit)
}

func (o *postgresOps) profile(parent context.Context, target int) (userRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	profiles, err := postgresQueryAll(ctx, o, nil, "SELECT id, name, bio, joined, followers_n, following_n FROM users WHERE id = $1", func(rows *sql.Rows) (userRecord, error) {
		var profile userRecord
		var id int64
		var joined pgTime
		err := rows.Scan(&id, &profile.Name, &profile.Bio, &joined, &profile.FollowerCount, &profile.FollowingCount)
		profile.UserID, profile.Joined = strconv.FormatInt(id, 10), joined.Time
		return profile, err
	}, int64(target))
	if err != nil {
		return userRecord{}, fmt.Errorf("reading user %d: %w", target, err)
	}
	if len(profiles) == 0 {
		return userRecord{}, fmt.Errorf("user %d not found", target)
	}
	return profiles[0], nil
}

func (o *postgresOps) profileTimeline(parent context.Context, target, limit int) ([]postRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	return postgresQueryAll(ctx, o, nil, "SELECT "+postgresPostColumns+" FROM posts WHERE author_id = $1 ORDER BY seq DESC LIMIT $2", scanPostgresPost, int64(target), limit)
}

func (o *postgresOps) postPage(parent context.Context, author int, seq int64) (postPage, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	pages, err := postgresQueryAll(ctx, o, nil, "SELECT "+postgresPostColumns+", likes_n, comments_n FROM posts WHERE author_id = $1 AND seq = $2", func(rows *sql.Rows) (postPage, error) {
		var post postRecord
		var authorID int64
		var created pgTime
		var tags []byte
		var replyTo sql.NullString
		page := postPage{Found: true}
		if err := rows.Scan(&authorID, &post.Seq, &post.Text, &created, &tags, &replyTo, &page.Likes, &page.Comments); err != nil {
			return postPage{}, err
		}
		if err := json.Unmarshal(tags, &post.Tags); err != nil {
			return postPage{}, fmt.Errorf("decoding the tags %q: %w", tags, err)
		}
		post.Author, post.Created, post.ReplyTo = strconv.FormatInt(authorID, 10), created.Time, replyTo.String
		page.Post = post
		return page, nil
	}, int64(author), seq)
	if err != nil {
		return postPage{}, fmt.Errorf("reading post %d of user %d: %w", seq, author, err)
	}
	if len(pages) == 0 {
		return postPage{}, nil
	}
	return pages[0], nil
}

func (o *postgresOps) followCheck(parent context.Context, actor, target int) (bool, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	edges, err := postgresQueryAll(ctx, o, nil, "SELECT 1 FROM follows WHERE follower_id = $1 AND followee_id = $2", func(rows *sql.Rows) (int, error) {
		var one int
		err := rows.Scan(&one)
		return one, err
	}, int64(actor), int64(target))
	if err != nil {
		return false, fmt.Errorf("checking whether user %d follows user %d: %w", actor, target, err)
	}
	return len(edges) > 0, nil
}

func (o *postgresOps) followingList(parent context.Context, actor, limit int) ([]string, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	followed, err := postgresQueryAll(ctx, o, nil, "SELECT followee_id FROM follows WHERE follower_id = $1 LIMIT $2", func(rows *sql.Rows) (string, error) {
		var followee int64
		err := rows.Scan(&followee)
		return strconv.FormatInt(followee, 10), err
	}, int64(actor), limit)
	if err != nil {
		return nil, fmt.Errorf("listing who user %d follows: %w", actor, err)
	}
	return followed, nil
}

func (o *postgresOps) avatar(parent context.Context, target int) ([]byte, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	blobs, err := postgresQueryAll(ctx, o, nil, "SELECT data FROM media WHERE user_id = $1 AND kind = $2", func(rows *sql.Rows) ([]byte, error) {
		var content []byte
		err := rows.Scan(&content)
		return content, err
	}, int64(target), "avatar")
	if err != nil {
		return nil, fmt.Errorf("reading the avatar of user %d: %w", target, err)
	}
	if len(blobs) == 0 {
		return nil, fmt.Errorf("user %d: %w", target, errNoAvatar)
	}
	return blobs[0], nil
}

// readNotifications takes the oldest limit notifications in one transaction:
// SELECT ... FOR UPDATE SKIP LOCKED then DELETE, so two readers never take the
// same row and a locked row is passed over rather than waited for.
func (o *postgresOps) readNotifications(parent context.Context, actor, limit int) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	taken := 0
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		ids, err := postgresQueryAll(ctx, o, tx, "SELECT id FROM notifications WHERE user_id = $1 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED", scanID, int64(actor), limit)
		if err != nil || len(ids) == 0 {
			return err
		}
		args := []any{int64(actor)}
		for _, id := range ids {
			args = append(args, id)
		}
		if _, err := o.exec(ctx, tx, "DELETE FROM notifications WHERE user_id = $1 AND id IN ("+pgPlaceholders(2, len(ids))+")", args...); err != nil {
			return err
		}
		taken = len(ids)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("reading the notifications of user %d: %w", actor, err)
	}
	return taken, nil
}

func (o *postgresOps) publishPost(parent context.Context, post postRecord, ledger deliveryLedger) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	author, err := strconv.Atoi(post.Author)
	if err != nil {
		return 0, fmt.Errorf("post author %q is not a user id: %w", post.Author, err)
	}
	followers, err := postgresQueryAll(ctx, o, nil, "SELECT follower_id FROM follows WHERE followee_id = $1", func(rows *sql.Rows) (int, error) {
		var follower int64
		err := rows.Scan(&follower)
		return int(follower), err
	}, int64(author))
	if err != nil {
		return 0, fmt.Errorf("listing the followers of user %d: %w", author, err)
	}
	tags, err := tagsJSON(post.Tags)
	if err != nil {
		return 0, err
	}
	publishing := publication{post: post, author: author, followers: followers, tags: tags}
	if o.engine.cfg.bulkFanout() {
		err = o.publishBulk(ctx, publishing, ledger)
	} else {
		err = o.publishSingly(ctx, publishing, ledger)
	}
	if err != nil {
		return 0, err
	}
	return len(followers), nil
}

// publishBulk stores the post and copies it into every follower's feed with
// one multi-row INSERT, in one transaction (-fanout bulk).
func (o *postgresOps) publishBulk(ctx context.Context, publishing publication, ledger deliveryLedger) error {
	post, author, followers, tags := publishing.post, publishing.author, publishing.followers, publishing.tags
	for _, follower := range followers {
		ledger.sending(follower)
	}
	err := o.inTransaction(ctx, o.engine.bulkDB, func(tx *sql.Tx) error {
		o.sent++
		if _, err := tx.ExecContext(ctx, postgresInsertPost, int64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)); err != nil {
			return fmt.Errorf("storing post %d of user %d: %w", post.Seq, author, err)
		}
		if len(followers) == 0 {
			return nil
		}
		args := make([]any, 0, len(followers)*9)
		for _, follower := range followers {
			args = append(args, int64(follower), o.engine.ids.next(time.Now().UnixMilli()), postKeyString(post.Author, post.Seq), int64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo))
		}
		o.sent++
		if _, err := tx.ExecContext(ctx, postgresFeedColumns+pgRowPlaceholders(len(followers), 9), args...); err != nil {
			return fmt.Errorf("copying post %d of user %d into %d feeds: %w", post.Seq, author, len(followers), err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, follower := range followers {
		ledger.delivered(follower)
	}
	return nil
}

// publishSingly stores the post and then writes one feed entry per follower,
// each its own autocommit statement, one after another in the order the
// followers were listed: hoardDB's sequence, which has no transaction either.
func (o *postgresOps) publishSingly(ctx context.Context, publishing publication, ledger deliveryLedger) error {
	post, author, followers, tags := publishing.post, publishing.author, publishing.followers, publishing.tags
	if _, err := o.exec(ctx, nil, postgresInsertPost, int64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)); err != nil {
		return fmt.Errorf("storing post %d of user %d: %w", post.Seq, author, err)
	}
	for _, follower := range followers {
		ledger.sending(follower)
		if _, err := o.exec(ctx, nil, postgresFeedColumns+pgRowPlaceholders(1, 9), int64(follower), o.engine.ids.next(time.Now().UnixMilli()), postKeyString(post.Author, post.Seq), int64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)); err != nil {
			return fmt.Errorf("copying post %d of user %d into the feed of user %d: %w", post.Seq, author, follower, err)
		}
		ledger.delivered(follower)
	}
	return nil
}

// likePost is one transaction: the like (ON CONFLICT DO NOTHING: a second like
// by the same user is the same row), the counter, only when the like was new,
// and the notification.
func (o *postgresOps) likePost(parent context.Context, liker, author int, seq int64) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		inserted, err := o.exec(ctx, tx, "INSERT INTO likes (author_id, seq, liker_id, ts) VALUES ($1, $2, $3, $4) "+onConflictDoNothing, int64(author), seq, int64(liker), time.Now().UTC())
		if err != nil {
			return err
		}
		if added, err := inserted.RowsAffected(); err != nil {
			return fmt.Errorf("counting the likes added: %w", err)
		} else if added > 0 {
			if _, err := o.exec(ctx, tx, postgresBumpLikes, int64(author), seq); err != nil {
				return err
			}
		}
		return o.notify(ctx, tx, author, "like", liker, fmt.Sprintf("%d/%s", author, postKey(seq)))
	})
	if err != nil {
		return fmt.Errorf("user %d liking post %d of user %d: %w", liker, seq, author, err)
	}
	return nil
}

func (o *postgresOps) follow(parent context.Context, actor, target int) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		inserted, err := o.exec(ctx, tx, "INSERT INTO follows (follower_id, followee_id, since) VALUES ($1, $2, $3) "+onConflictDoNothing, int64(actor), int64(target), time.Now().UTC())
		if err != nil {
			return err
		}
		if added, err := inserted.RowsAffected(); err != nil {
			return fmt.Errorf("counting the edges added: %w", err)
		} else if added == 0 {
			return nil // already following: nothing changed, so no counter moves
		}
		if _, err := o.exec(ctx, tx, postgresBumpFollowers, 1, int64(target)); err != nil {
			return err
		}
		if _, err := o.exec(ctx, tx, postgresBumpFollowing, 1, int64(actor)); err != nil {
			return err
		}
		return o.notify(ctx, tx, target, "follow", actor, strconv.Itoa(target))
	})
	if err != nil {
		return fmt.Errorf("user %d following user %d: %w", actor, target, err)
	}
	return nil
}

func (o *postgresOps) unfollow(parent context.Context, actor, target int) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		deleted, err := o.exec(ctx, tx, "DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2", int64(actor), int64(target))
		if err != nil {
			return err
		}
		if removed, err := deleted.RowsAffected(); err != nil {
			return fmt.Errorf("counting the edges removed: %w", err)
		} else if removed == 0 {
			return nil // there was no edge, so no counter moves
		}
		if _, err := o.exec(ctx, tx, postgresBumpFollowers, -1, int64(target)); err != nil {
			return err
		}
		_, err = o.exec(ctx, tx, postgresBumpFollowing, -1, int64(actor))
		return err
	})
	if err != nil {
		return fmt.Errorf("user %d unfollowing user %d: %w", actor, target, err)
	}
	return nil
}

func (o *postgresOps) commentOnPost(parent context.Context, commenter, author int, seq int64, text string) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		if _, err := o.exec(ctx, tx, "INSERT INTO comments (author_id, seq, id, commenter, text, ts) VALUES ($1, $2, $3, $4, $5, $6)",
			int64(author), seq, o.engine.ids.next(time.Now().UnixMilli()), int64(commenter), text, time.Now().UTC()); err != nil {
			return err
		}
		if _, err := o.exec(ctx, tx, postgresBumpComments, int64(author), seq); err != nil {
			return err
		}
		return o.notify(ctx, tx, author, "comment", commenter, fmt.Sprintf("%d/%s", author, postKey(seq)))
	})
	if err != nil {
		return fmt.Errorf("user %d commenting on post %d of user %d: %w", commenter, seq, author, err)
	}
	return nil
}

func (o *postgresOps) uploadAvatar(parent context.Context, user int, content []byte) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	if _, err := o.exec(ctx, nil, "INSERT INTO media (user_id, kind, data) VALUES ($1, $2, $3) ON CONFLICT (user_id, kind) DO UPDATE SET data = excluded.data", int64(user), "avatar", content); err != nil {
		return fmt.Errorf("uploading the avatar of user %d: %w", user, err)
	}
	return nil
}

// mergedTimeline is one SELECT over the followees' posts, newest first: the
// idiomatic form of the merge-on-read design (spec 6.3), whose answer is the
// true newest 20 across all followees.
func (o *postgresOps) mergedTimeline(parent context.Context, followees []int) ([]postRecord, error) {
	if len(followees) == 0 {
		return nil, nil
	}
	ctx, cancel := o.opContext(parent)
	defer cancel()
	args := make([]any, 0, len(followees)+1)
	for _, followee := range followees {
		args = append(args, int64(followee))
	}
	args = append(args, mergedPostsTotal)
	query := "SELECT " + postgresPostColumns + " FROM posts WHERE author_id IN (" + pgPlaceholders(1, len(followees)) + ") ORDER BY created DESC LIMIT $" + strconv.Itoa(len(followees)+1)
	return postgresQueryAll(ctx, o, nil, query, scanPostgresPost, args...)
}

// queueRound pushes one job, claims up to queueClaimSize with a lease in one
// transaction (SELECT ... FOR UPDATE SKIP LOCKED, then UPDATE) and
// acknowledges what it claimed with a DELETE.
func (o *postgresOps) queueRound(parent context.Context, outbox int) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	now := time.Now().UTC()
	if _, err := o.exec(ctx, nil, "INSERT INTO jobs (id, outbox, lease_until, created) VALUES ($1, $2, $3, $4)", o.engine.ids.next(now.UnixMilli()), outbox, postgresNeverLeased, now); err != nil {
		return 0, fmt.Errorf("pushing a job onto outbox %d: %w", outbox, err)
	}
	var claimed []int64
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		ids, err := postgresQueryAll(ctx, o, tx, "SELECT id FROM jobs WHERE outbox = $1 AND lease_until <= $2 ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED", scanID, outbox, now, queueClaimSize)
		if err != nil || len(ids) == 0 {
			return err
		}
		args := []any{now.Add(queueLeaseMs * time.Millisecond)}
		for _, id := range ids {
			args = append(args, id)
		}
		if _, err := o.exec(ctx, tx, "UPDATE jobs SET lease_until = $1 WHERE id IN ("+pgPlaceholders(2, len(ids))+")", args...); err != nil {
			return err
		}
		claimed = ids
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("claiming from outbox %d: %w", outbox, err)
	}
	if len(claimed) == 0 {
		return 0, nil
	}
	args := make([]any, len(claimed))
	for i, id := range claimed {
		args[i] = id
	}
	if _, err := o.exec(ctx, nil, "DELETE FROM jobs WHERE id IN ("+pgPlaceholders(1, len(claimed))+")", args...); err != nil {
		return 0, fmt.Errorf("acknowledging %d jobs of outbox %d: %w", len(claimed), outbox, err)
	}
	return len(claimed), nil
}
