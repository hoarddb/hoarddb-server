package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// canonicalUser reads everything MySQL holds for a user, in canonical form
// (spec P4), with the keys the operations already use.
func (o *mysqlOps) canonicalUser(parent context.Context, rank int) (canonicalUser, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	user := uint64(rank)
	profile, err := o.profile(ctx, rank)
	if err != nil {
		return canonicalUser{}, err
	}
	raw := rawUser{Profile: profile, Media: map[string][]byte{}}
	posts, err := queryAll(ctx, o, nil, "SELECT "+mysqlPostColumns+" FROM posts WHERE author_id = ?", scanPost, user)
	if err != nil {
		return canonicalUser{}, fmt.Errorf("reading the posts of user %d: %w", rank, err)
	}
	likersOf, commentsOf, err := o.reactionsOf(ctx, user)
	if err != nil {
		return canonicalUser{}, err
	}
	for _, post := range posts {
		raw.Posts = append(raw.Posts, rawPost{Post: post, Likers: likersOf[post.Seq], Comments: commentsOf[post.Seq]})
	}
	if raw.Feed, err = queryAll(ctx, o, nil, "SELECT "+mysqlPostColumns+" FROM feed WHERE user_id = ? ORDER BY id DESC", scanPost, user); err != nil {
		return canonicalUser{}, fmt.Errorf("reading the feed of user %d: %w", rank, err)
	}
	raw.Inbox, err = queryAll(ctx, o, nil, "SELECT type, actor, target, ts FROM notifications WHERE user_id = ?", func(rows *sql.Rows) (rawNote, error) {
		var note rawNote
		var actor uint64
		var at mysqlDateTime
		err := rows.Scan(&note.Type, &actor, &note.Target, &at)
		note.Actor, note.TS = strconv.FormatUint(actor, 10), at.Time
		return note, err
	}, user)
	if err != nil {
		return canonicalUser{}, fmt.Errorf("reading the inbox of user %d: %w", rank, err)
	}
	if err := o.readEdges(ctx, user, &raw); err != nil {
		return canonicalUser{}, err
	}
	blobs, err := queryAll(ctx, o, nil, "SELECT kind, data FROM media WHERE user_id = ?", func(rows *sql.Rows) (mediaRow, error) {
		var blob mediaRow
		err := rows.Scan(&blob.Kind, &blob.Data)
		return blob, err
	}, user)
	if err != nil {
		return canonicalUser{}, fmt.Errorf("reading the media of user %d: %w", rank, err)
	}
	for _, blob := range blobs {
		raw.Media[blob.Kind] = blob.Data
	}
	return canonicalize(raw), nil
}

// mediaRow is one stored blob.
type mediaRow struct {
	Kind string
	Data []byte
}

// reactionsOf reads the likers and comments of all of an author's posts, keyed
// by post sequence.
func (o *mysqlOps) reactionsOf(ctx context.Context, author uint64) (map[int64][]string, map[int64][]rawComment, error) {
	type liked struct {
		Seq   int64
		Liker string
	}
	likes, err := queryAll(ctx, o, nil, "SELECT seq, liker_id FROM likes WHERE author_id = ?", func(rows *sql.Rows) (liked, error) {
		var like liked
		var liker uint64
		err := rows.Scan(&like.Seq, &liker)
		like.Liker = strconv.FormatUint(liker, 10)
		return like, err
	}, author)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the likes of user %d: %w", author, err)
	}
	type commented struct {
		Seq     int64
		Comment rawComment
	}
	comments, err := queryAll(ctx, o, nil, "SELECT seq, commenter, text, ts FROM comments WHERE author_id = ?", func(rows *sql.Rows) (commented, error) {
		var row commented
		var commenter uint64
		var at mysqlDateTime
		err := rows.Scan(&row.Seq, &commenter, &row.Comment.Text, &at)
		row.Comment.Author, row.Comment.TS = strconv.FormatUint(commenter, 10), at.Time
		return row, err
	}, author)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the comments of user %d: %w", author, err)
	}
	likersOf, commentsOf := map[int64][]string{}, map[int64][]rawComment{}
	for _, like := range likes {
		likersOf[like.Seq] = append(likersOf[like.Seq], like.Liker)
	}
	for _, comment := range comments {
		commentsOf[comment.Seq] = append(commentsOf[comment.Seq], comment.Comment)
	}
	return likersOf, commentsOf, nil
}

// readEdges fills the two directions of the follow graph.
func (o *mysqlOps) readEdges(ctx context.Context, user uint64, raw *rawUser) error {
	scanRank := func(rows *sql.Rows) (string, error) {
		var rank uint64
		err := rows.Scan(&rank)
		return strconv.FormatUint(rank, 10), err
	}
	following, err := queryAll(ctx, o, nil, "SELECT followee_id FROM follows WHERE follower_id = ?", scanRank, user)
	if err != nil {
		return fmt.Errorf("reading who user %d follows: %w", user, err)
	}
	followers, err := queryAll(ctx, o, nil, "SELECT follower_id FROM follows WHERE followee_id = ?", scanRank, user)
	if err != nil {
		return fmt.Errorf("reading the followers of user %d: %w", user, err)
	}
	raw.Following, raw.Followers = following, followers
	return nil
}
