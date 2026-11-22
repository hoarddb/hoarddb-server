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

// mysqlOps is one worker's handle on the social operations. Statements run one
// after another (Q-A: one statement at a time); sent counts them, so an
// operation's request count is the difference around it. BEGIN and COMMIT are
// not counted: spec 6.3 counts the statements of a transaction.
type mysqlOps struct {
	engine *mysqlEngine
	sent   int64
	// unprepared holds the statements a transaction had to prepare on its own
	// connection because the pool had not prepared them yet; the pool prepares
	// them when the transaction is over.
	unprepared []string
}

func (m *mysqlEngine) newSocialOps() (socialOps, error) { return &mysqlOps{engine: m}, nil }

func (o *mysqlOps) requests() int64 { return o.sent }
func (o *mysqlOps) close()          {}

func (o *mysqlOps) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, o.engine.cfg.OpTimeout)
}

// statement returns the prepared statement for query, bound to tx when given.
// A read outside a transaction goes where the read routing sends it; everything
// else runs on the source, which is the only server that takes writes.
//
// Inside a transaction a statement the pool has not prepared yet is prepared on
// the transaction's own connection: asking the pool for a second one deadlocks
// when -depth workers each hold one (BUG-0713).
func (o *mysqlOps) statement(ctx context.Context, tx *sql.Tx, query string, read bool) (*sql.Stmt, error) {
	if read && tx == nil {
		return o.engine.readStatement(ctx, query)
	}
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
func (o *mysqlOps) prepareDeferred(ctx context.Context) error {
	queries := o.unprepared
	o.unprepared = nil
	for _, query := range queries {
		if _, err := o.engine.prepared(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

func (o *mysqlOps) exec(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	statement, err := o.statement(ctx, tx, query, false)
	if err != nil {
		return nil, err
	}
	o.sent++
	result, err := statement.ExecContext(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("executing %q: %w", query, err)
	}
	return result, nil
}

// queryAll runs one SELECT and maps every row with scan.
func queryAll[T any](ctx context.Context, o *mysqlOps, tx *sql.Tx, query string, scan func(*sql.Rows) (T, error), args ...any) ([]T, error) {
	statement, err := o.statement(ctx, tx, query, true)
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

// inTransaction runs body in one transaction on pool and commits once.
func (o *mysqlOps) inTransaction(ctx context.Context, pool *sql.DB, body func(tx *sql.Tx) error) error {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning a transaction: %w", err)
	}
	return errors.Join(o.finish(tx, body(tx)), o.prepareDeferred(ctx))
}

// finish commits tx when the body succeeded and rolls it back when it failed.
func (o *mysqlOps) finish(tx *sql.Tx, bodyErr error) error {
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
	mysqlPostColumns   = "author_id, seq, text, created, tags, reply_to"
	mysqlInsertNotice  = "INSERT INTO notifications (user_id, id, type, actor, target, ts) VALUES (?, ?, ?, ?, ?, ?)"
	mysqlBumpLikes     = "UPDATE posts SET likes_n = likes_n + 1 WHERE author_id = ? AND seq = ?"
	mysqlBumpComments  = "UPDATE posts SET comments_n = comments_n + 1 WHERE author_id = ? AND seq = ?"
	mysqlBumpFollowers = "UPDATE users SET followers_n = followers_n + ? WHERE id = ?"
	mysqlBumpFollowing = "UPDATE users SET following_n = following_n + ? WHERE id = ?"
)

// scanPost reads the columns of mysqlPostColumns.
func scanPost(rows *sql.Rows) (postRecord, error) {
	var post postRecord
	var author uint64
	var created mysqlDateTime
	var tags []byte
	var replyTo sql.NullString
	if err := rows.Scan(&author, &post.Seq, &post.Text, &created, &tags, &replyTo); err != nil {
		return postRecord{}, err
	}
	if err := json.Unmarshal(tags, &post.Tags); err != nil {
		return postRecord{}, fmt.Errorf("decoding the tags %q: %w", tags, err)
	}
	post.Author, post.Created, post.ReplyTo = strconv.FormatUint(author, 10), created.Time, replyTo.String
	return post, nil
}

// notify inserts one notification for a user, inside tx.
func (o *mysqlOps) notify(ctx context.Context, tx *sql.Tx, user int, kind string, actor int, target string) error {
	_, err := o.exec(ctx, tx, mysqlInsertNotice, uint64(user), o.engine.ids.next(time.Now().UnixMilli()), kind, uint64(actor), target, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("notifying user %d of a %s: %w", user, kind, err)
	}
	return nil
}

func (o *mysqlOps) homeTimeline(parent context.Context, actor, limit int) ([]postRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	return queryAll(ctx, o, nil, "SELECT "+mysqlPostColumns+" FROM feed WHERE user_id = ? ORDER BY id DESC LIMIT ?", scanPost, uint64(actor), limit)
}

func (o *mysqlOps) profile(parent context.Context, target int) (userRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	profiles, err := queryAll(ctx, o, nil, "SELECT id, name, bio, joined, followers_n, following_n FROM users WHERE id = ?", func(rows *sql.Rows) (userRecord, error) {
		var profile userRecord
		var id uint64
		var joined mysqlDateTime
		err := rows.Scan(&id, &profile.Name, &profile.Bio, &joined, &profile.FollowerCount, &profile.FollowingCount)
		profile.UserID, profile.Joined = strconv.FormatUint(id, 10), joined.Time
		return profile, err
	}, uint64(target))
	if err != nil {
		return userRecord{}, fmt.Errorf("reading user %d: %w", target, err)
	}
	if len(profiles) == 0 {
		return userRecord{}, fmt.Errorf("user %d not found", target)
	}
	return profiles[0], nil
}

func (o *mysqlOps) profileTimeline(parent context.Context, target, limit int) ([]postRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	return queryAll(ctx, o, nil, "SELECT "+mysqlPostColumns+" FROM posts WHERE author_id = ? ORDER BY seq DESC LIMIT ?", scanPost, uint64(target), limit)
}

func (o *mysqlOps) postPage(parent context.Context, author int, seq int64) (postPage, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	pages, err := queryAll(ctx, o, nil, "SELECT "+mysqlPostColumns+", likes_n, comments_n FROM posts WHERE author_id = ? AND seq = ?", func(rows *sql.Rows) (postPage, error) {
		var post postRecord
		var authorID uint64
		var created mysqlDateTime
		var tags []byte
		var replyTo sql.NullString
		page := postPage{Found: true}
		if err := rows.Scan(&authorID, &post.Seq, &post.Text, &created, &tags, &replyTo, &page.Likes, &page.Comments); err != nil {
			return postPage{}, err
		}
		if err := json.Unmarshal(tags, &post.Tags); err != nil {
			return postPage{}, fmt.Errorf("decoding the tags %q: %w", tags, err)
		}
		post.Author, post.Created, post.ReplyTo = strconv.FormatUint(authorID, 10), created.Time, replyTo.String
		page.Post = post
		return page, nil
	}, uint64(author), seq)
	if err != nil {
		return postPage{}, fmt.Errorf("reading post %d of user %d: %w", seq, author, err)
	}
	if len(pages) == 0 {
		return postPage{}, nil
	}
	return pages[0], nil
}

func (o *mysqlOps) followCheck(parent context.Context, actor, target int) (bool, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	edges, err := queryAll(ctx, o, nil, "SELECT 1 FROM follows WHERE follower_id = ? AND followee_id = ?", func(rows *sql.Rows) (int, error) {
		var one int
		err := rows.Scan(&one)
		return one, err
	}, uint64(actor), uint64(target))
	if err != nil {
		return false, fmt.Errorf("checking whether user %d follows user %d: %w", actor, target, err)
	}
	return len(edges) > 0, nil
}

func (o *mysqlOps) followingList(parent context.Context, actor, limit int) ([]string, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	followed, err := queryAll(ctx, o, nil, "SELECT followee_id FROM follows WHERE follower_id = ? LIMIT ?", func(rows *sql.Rows) (string, error) {
		var followee uint64
		err := rows.Scan(&followee)
		return strconv.FormatUint(followee, 10), err
	}, uint64(actor), limit)
	if err != nil {
		return nil, fmt.Errorf("listing who user %d follows: %w", actor, err)
	}
	return followed, nil
}

func (o *mysqlOps) avatar(parent context.Context, target int) ([]byte, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	blobs, err := queryAll(ctx, o, nil, "SELECT data FROM media WHERE user_id = ? AND kind = ?", func(rows *sql.Rows) ([]byte, error) {
		var data []byte
		err := rows.Scan(&data)
		return data, err
	}, uint64(target), "avatar")
	if err != nil {
		return nil, fmt.Errorf("reading the avatar of user %d: %w", target, err)
	}
	if len(blobs) == 0 {
		return nil, fmt.Errorf("user %d: %w", target, errNoAvatar)
	}
	return blobs[0], nil
}

func scanID(rows *sql.Rows) (int64, error) {
	var id int64
	err := rows.Scan(&id)
	return id, err
}

// readNotifications takes the oldest limit notifications in one transaction:
// SELECT ... FOR UPDATE SKIP LOCKED then DELETE, so two readers never take the
// same row and a locked row is passed over rather than waited for.
func (o *mysqlOps) readNotifications(parent context.Context, actor, limit int) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	taken := 0
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		ids, err := queryAll(ctx, o, tx, "SELECT id FROM notifications WHERE user_id = ? ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED", scanID, uint64(actor), limit)
		if err != nil || len(ids) == 0 {
			return err
		}
		args := []any{uint64(actor)}
		for _, id := range ids {
			args = append(args, id)
		}
		if _, err := o.exec(ctx, tx, "DELETE FROM notifications WHERE user_id = ? AND id IN ("+placeholders(len(ids))+")", args...); err != nil {
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

func (o *mysqlOps) publishPost(parent context.Context, post postRecord, ledger deliveryLedger) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	author, err := strconv.Atoi(post.Author)
	if err != nil {
		return 0, fmt.Errorf("post author %q is not a user id: %w", post.Author, err)
	}
	followers, err := queryAll(ctx, o, nil, "SELECT follower_id FROM follows WHERE followee_id = ?", func(rows *sql.Rows) (int, error) {
		var follower uint64
		err := rows.Scan(&follower)
		return int(follower), err
	}, uint64(author))
	if err != nil {
		return 0, fmt.Errorf("listing the followers of user %d: %w", author, err)
	}
	tags, err := tagsJSON(post.Tags)
	if err != nil {
		return 0, err
	}
	if o.engine.cfg.bulkFanout() {
		err = o.publishBulk(ctx, publication{post: post, author: author, followers: followers, tags: tags}, ledger)
	} else {
		err = o.publishSingly(ctx, publication{post: post, author: author, followers: followers, tags: tags}, ledger)
	}
	if err != nil {
		return 0, err
	}
	return len(followers), nil
}

// publication is one post on its way to its author's followers.
type publication struct {
	post      postRecord
	author    int
	followers []int
	tags      string
}

const (
	mysqlInsertPost  = "INSERT INTO posts (author_id, seq, text, created, tags, reply_to, likes_n, comments_n) VALUES (?, ?, ?, ?, ?, ?, 0, 0)"
	mysqlFeedColumns = "INSERT INTO feed (user_id, id, post_id, author_id, seq, text, created, tags, reply_to) VALUES "
)

// publishBulk stores the post and copies it into every follower's feed with
// one multi-row INSERT, in one transaction (-fanout bulk).
func (o *mysqlOps) publishBulk(ctx context.Context, publishing publication, ledger deliveryLedger) error {
	post, author, followers, tags := publishing.post, publishing.author, publishing.followers, publishing.tags
	for _, follower := range followers {
		ledger.sending(follower)
	}
	err := o.inTransaction(ctx, o.engine.bulkDB, func(tx *sql.Tx) error {
		o.sent++
		if _, err := tx.ExecContext(ctx, mysqlInsertPost, uint64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)); err != nil {
			return fmt.Errorf("storing post %d of user %d: %w", post.Seq, author, err)
		}
		if len(followers) == 0 {
			return nil
		}
		args := make([]any, 0, len(followers)*9)
		for _, follower := range followers {
			args = append(args, uint64(follower), o.engine.ids.next(time.Now().UnixMilli()), postKeyString(post.Author, post.Seq), uint64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo))
		}
		o.sent++
		if _, err := tx.ExecContext(ctx, mysqlFeedColumns+rowPlaceholders(len(followers), 9), args...); err != nil {
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
func (o *mysqlOps) publishSingly(ctx context.Context, publishing publication, ledger deliveryLedger) error {
	post, author, followers, tags := publishing.post, publishing.author, publishing.followers, publishing.tags
	if _, err := o.exec(ctx, nil, mysqlInsertPost, uint64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)); err != nil {
		return fmt.Errorf("storing post %d of user %d: %w", post.Seq, author, err)
	}
	for _, follower := range followers {
		ledger.sending(follower)
		if _, err := o.exec(ctx, nil, mysqlFeedColumns+rowPlaceholders(1, 9), uint64(follower), o.engine.ids.next(time.Now().UnixMilli()), postKeyString(post.Author, post.Seq), uint64(author), post.Seq, post.Text, post.Created, tags, nullIfEmpty(post.ReplyTo)); err != nil {
			return fmt.Errorf("copying post %d of user %d into the feed of user %d: %w", post.Seq, author, follower, err)
		}
		ledger.delivered(follower)
	}
	return nil
}

// likePost is one transaction: the like (INSERT IGNORE: a second like by the
// same user is the same row), the counter, only when the like was new, and the
// notification.
func (o *mysqlOps) likePost(parent context.Context, liker, author int, seq int64) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		inserted, err := o.exec(ctx, tx, "INSERT IGNORE INTO likes (author_id, seq, liker_id, ts) VALUES (?, ?, ?, ?)", uint64(author), seq, uint64(liker), time.Now().UTC())
		if err != nil {
			return err
		}
		if added, err := inserted.RowsAffected(); err != nil {
			return fmt.Errorf("counting the likes added: %w", err)
		} else if added > 0 {
			if _, err := o.exec(ctx, tx, mysqlBumpLikes, uint64(author), seq); err != nil {
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

func (o *mysqlOps) follow(parent context.Context, actor, target int) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		inserted, err := o.exec(ctx, tx, "INSERT IGNORE INTO follows (follower_id, followee_id, since) VALUES (?, ?, ?)", uint64(actor), uint64(target), time.Now().UTC())
		if err != nil {
			return err
		}
		if added, err := inserted.RowsAffected(); err != nil {
			return fmt.Errorf("counting the edges added: %w", err)
		} else if added == 0 {
			return nil // already following: nothing changed, so no counter moves
		}
		if _, err := o.exec(ctx, tx, mysqlBumpFollowers, 1, uint64(target)); err != nil {
			return err
		}
		if _, err := o.exec(ctx, tx, mysqlBumpFollowing, 1, uint64(actor)); err != nil {
			return err
		}
		return o.notify(ctx, tx, target, "follow", actor, strconv.Itoa(target))
	})
	if err != nil {
		return fmt.Errorf("user %d following user %d: %w", actor, target, err)
	}
	return nil
}

func (o *mysqlOps) unfollow(parent context.Context, actor, target int) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		deleted, err := o.exec(ctx, tx, "DELETE FROM follows WHERE follower_id = ? AND followee_id = ?", uint64(actor), uint64(target))
		if err != nil {
			return err
		}
		if removed, err := deleted.RowsAffected(); err != nil {
			return fmt.Errorf("counting the edges removed: %w", err)
		} else if removed == 0 {
			return nil // there was no edge, so no counter moves
		}
		if _, err := o.exec(ctx, tx, mysqlBumpFollowers, -1, uint64(target)); err != nil {
			return err
		}
		_, err = o.exec(ctx, tx, mysqlBumpFollowing, -1, uint64(actor))
		return err
	})
	if err != nil {
		return fmt.Errorf("user %d unfollowing user %d: %w", actor, target, err)
	}
	return nil
}

func (o *mysqlOps) commentOnPost(parent context.Context, commenter, author int, seq int64, text string) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		if _, err := o.exec(ctx, tx, "INSERT INTO comments (author_id, seq, id, commenter, text, ts) VALUES (?, ?, ?, ?, ?, ?)",
			uint64(author), seq, o.engine.ids.next(time.Now().UnixMilli()), uint64(commenter), text, time.Now().UTC()); err != nil {
			return err
		}
		if _, err := o.exec(ctx, tx, mysqlBumpComments, uint64(author), seq); err != nil {
			return err
		}
		return o.notify(ctx, tx, author, "comment", commenter, fmt.Sprintf("%d/%s", author, postKey(seq)))
	})
	if err != nil {
		return fmt.Errorf("user %d commenting on post %d of user %d: %w", commenter, seq, author, err)
	}
	return nil
}

func (o *mysqlOps) uploadAvatar(parent context.Context, user int, content []byte) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	if _, err := o.exec(ctx, nil, "REPLACE INTO media (user_id, kind, data) VALUES (?, ?, ?)", uint64(user), "avatar", content); err != nil {
		return fmt.Errorf("uploading the avatar of user %d: %w", user, err)
	}
	return nil
}

// mergedTimeline is one SELECT over the followees' posts, newest first: the
// idiomatic form of the merge-on-read design (spec 6.3), whose answer is the
// true newest 20 across all followees.
func (o *mysqlOps) mergedTimeline(parent context.Context, followees []int) ([]postRecord, error) {
	if len(followees) == 0 {
		return nil, nil
	}
	ctx, cancel := o.opContext(parent)
	defer cancel()
	args := make([]any, 0, len(followees)+1)
	for _, followee := range followees {
		args = append(args, uint64(followee))
	}
	args = append(args, mergedPostsTotal)
	return queryAll(ctx, o, nil, "SELECT "+mysqlPostColumns+" FROM posts WHERE author_id IN ("+placeholders(len(followees))+") ORDER BY created DESC LIMIT ?", scanPost, args...)
}

// queueRound pushes one job, claims up to queueClaimSize with a lease in one
// transaction (SELECT ... FOR UPDATE SKIP LOCKED, then UPDATE) and
// acknowledges what it claimed with a DELETE.
func (o *mysqlOps) queueRound(parent context.Context, outbox int) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	now := time.Now().UTC()
	if _, err := o.exec(ctx, nil, "INSERT INTO jobs (id, outbox, lease_until, created) VALUES (?, ?, ?, ?)", o.engine.ids.next(now.UnixMilli()), outbox, mysqlNeverLeased, now); err != nil {
		return 0, fmt.Errorf("pushing a job onto outbox %d: %w", outbox, err)
	}
	var claimed []int64
	err := o.inTransaction(ctx, o.engine.db, func(tx *sql.Tx) error {
		ids, err := queryAll(ctx, o, tx, "SELECT id FROM jobs WHERE outbox = ? AND lease_until <= ? ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED", scanID, outbox, now, queueClaimSize)
		if err != nil || len(ids) == 0 {
			return err
		}
		args := []any{now.Add(queueLeaseMs * time.Millisecond)}
		for _, id := range ids {
			args = append(args, id)
		}
		if _, err := o.exec(ctx, tx, "UPDATE jobs SET lease_until = ? WHERE id IN ("+placeholders(len(ids))+")", args...); err != nil {
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
	if _, err := o.exec(ctx, nil, "DELETE FROM jobs WHERE id IN ("+placeholders(len(claimed))+")", args...); err != nil {
		return 0, fmt.Errorf("acknowledging %d jobs of outbox %d: %w", len(claimed), outbox, err)
	}
	return len(claimed), nil
}
