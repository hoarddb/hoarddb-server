package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// canonicalUser reads everything PostgreSQL holds for a user, in canonical
// form (spec P4), with the keys the operations already use.
func (o *postgresOps) canonicalUser(parent context.Context, rank int) (canonicalUser, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	user := int64(rank)
	profile, err := o.profile(ctx, rank)
	if err != nil {
		return canonicalUser{}, err
	}
	raw := rawUser{Profile: profile, Media: map[string][]byte{}}
	posts, err := postgresQueryAll(ctx, o, nil, "SELECT "+postgresPostColumns+" FROM posts WHERE author_id = $1", scanPostgresPost, user)
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
	if raw.Feed, err = postgresQueryAll(ctx, o, nil, "SELECT "+postgresPostColumns+" FROM feed WHERE user_id = $1 ORDER BY id DESC", scanPostgresPost, user); err != nil {
		return canonicalUser{}, fmt.Errorf("reading the feed of user %d: %w", rank, err)
	}
	raw.Inbox, err = postgresQueryAll(ctx, o, nil, "SELECT type, actor, target, ts FROM notifications WHERE user_id = $1", func(rows *sql.Rows) (rawNote, error) {
		var note rawNote
		var actor int64
		var at pgTime
		err := rows.Scan(&note.Type, &actor, &note.Target, &at)
		note.Actor, note.TS = strconv.FormatInt(actor, 10), at.Time
		return note, err
	}, user)
	if err != nil {
		return canonicalUser{}, fmt.Errorf("reading the inbox of user %d: %w", rank, err)
	}
	if err := o.readEdges(ctx, user, &raw); err != nil {
		return canonicalUser{}, err
	}
	blobs, err := postgresQueryAll(ctx, o, nil, "SELECT kind, data FROM media WHERE user_id = $1", func(rows *sql.Rows) (mediaRow, error) {
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

// reactionsOf reads the likers and comments of all of an author's posts, keyed
// by post sequence.
func (o *postgresOps) reactionsOf(ctx context.Context, author int64) (map[int64][]string, map[int64][]rawComment, error) {
	type liked struct {
		Seq   int64
		Liker string
	}
	likes, err := postgresQueryAll(ctx, o, nil, "SELECT seq, liker_id FROM likes WHERE author_id = $1", func(rows *sql.Rows) (liked, error) {
		var like liked
		var liker int64
		err := rows.Scan(&like.Seq, &liker)
		like.Liker = strconv.FormatInt(liker, 10)
		return like, err
	}, author)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the likes of user %d: %w", author, err)
	}
	type commented struct {
		Seq     int64
		Comment rawComment
	}
	comments, err := postgresQueryAll(ctx, o, nil, "SELECT seq, commenter, text, ts FROM comments WHERE author_id = $1", func(rows *sql.Rows) (commented, error) {
		var row commented
		var commenter int64
		var at pgTime
		err := rows.Scan(&row.Seq, &commenter, &row.Comment.Text, &at)
		row.Comment.Author, row.Comment.TS = strconv.FormatInt(commenter, 10), at.Time
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
func (o *postgresOps) readEdges(ctx context.Context, user int64, raw *rawUser) error {
	scanRank := func(rows *sql.Rows) (string, error) {
		var rank int64
		err := rows.Scan(&rank)
		return strconv.FormatInt(rank, 10), err
	}
	following, err := postgresQueryAll(ctx, o, nil, "SELECT followee_id FROM follows WHERE follower_id = $1", scanRank, user)
	if err != nil {
		return fmt.Errorf("reading who user %d follows: %w", user, err)
	}
	followers, err := postgresQueryAll(ctx, o, nil, "SELECT follower_id FROM follows WHERE followee_id = $1", scanRank, user)
	if err != nil {
		return fmt.Errorf("reading the followers of user %d: %w", user, err)
	}
	raw.Following, raw.Followers = following, followers
	return nil
}
