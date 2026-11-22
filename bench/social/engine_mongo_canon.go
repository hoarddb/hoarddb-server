package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// authorKeyRange is a half-open _id or post range that holds exactly the keys that
// start with "author/": '/' is followed by '0' in ASCII, so [author/, author0)
// takes every post of the author and no post of an author whose id merely
// starts with the same digits.
func authorKeyRange(author string) bson.D {
	return bson.D{{Key: "$gte", Value: author + "/"}, {Key: "$lt", Value: author + "0"}}
}

// commentRow is a comment with the post it hangs on.
type commentRow struct {
	Post    string     `bson:"post"`
	Comment rawComment `bson:",inline"`
}

// canonicalUser reads everything MongoDB holds for a user, in canonical form
// (spec P4). It uses the same collections the operations do and no index the
// schema does not have: likes are read by the range of their _id.
func (o *mongoOps) canonicalUser(parent context.Context, rank int) (canonicalUser, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	user := userID(rank)
	profile, err := o.profile(ctx, rank)
	if err != nil {
		return canonicalUser{}, err
	}
	raw := rawUser{Profile: profile, Media: map[string][]byte{}}
	posts, err := findAll[postRecord](ctx, o, mongoPosts, bson.D{{Key: "author", Value: user}}, nil)
	if err != nil {
		return canonicalUser{}, fmt.Errorf("reading the posts of user %d: %w", rank, err)
	}
	likes, err := findAll[struct {
		Post  string `bson:"post"`
		Liker string `bson:"liker"`
	}](ctx, o, mongoLikes, bson.D{{Key: "_id", Value: authorKeyRange(user)}}, nil)
	if err != nil {
		return canonicalUser{}, fmt.Errorf("reading the likes of user %d: %w", rank, err)
	}
	comments, err := findAll[commentRow](ctx, o, mongoComments, bson.D{{Key: "post", Value: authorKeyRange(user)}}, nil)
	if err != nil {
		return canonicalUser{}, fmt.Errorf("reading the comments of user %d: %w", rank, err)
	}
	likersOf, commentsOf := map[string][]string{}, map[string][]rawComment{}
	for _, like := range likes {
		likersOf[like.Post] = append(likersOf[like.Post], like.Liker)
	}
	for _, comment := range comments {
		commentsOf[comment.Post] = append(commentsOf[comment.Post], comment.Comment)
	}
	for _, post := range posts {
		key := postKeyString(post.Author, post.Seq)
		raw.Posts = append(raw.Posts, rawPost{Post: post, Likers: likersOf[key], Comments: commentsOf[key]})
	}
	newestFirst := options.Find().SetSort(bson.D{{Key: "_id", Value: -1}})
	if raw.Feed, err = findAll[postRecord](ctx, o, mongoFeed, bson.D{{Key: "user", Value: user}}, newestFirst); err != nil {
		return canonicalUser{}, fmt.Errorf("reading the feed of user %d: %w", rank, err)
	}
	if raw.Inbox, err = findAll[rawNote](ctx, o, mongoNotifications, bson.D{{Key: "user", Value: user}}, nil); err != nil {
		return canonicalUser{}, fmt.Errorf("reading the inbox of user %d: %w", rank, err)
	}
	if err := o.readEdges(ctx, user, &raw); err != nil {
		return canonicalUser{}, err
	}
	for _, kind := range []string{"avatar", "photo"} {
		type blob struct {
			Data []byte `bson:"data"`
		}
		stored, matched, err := findOne[blob](ctx, o, mongoMedia, bson.D{{Key: "_id", Value: mediaKey(user, kind)}})
		if err != nil {
			return canonicalUser{}, fmt.Errorf("reading the %s of user %d: %w", kind, rank, err)
		}
		if matched {
			raw.Media[kind] = stored.Data
		}
	}
	return canonicalize(raw), nil
}

// readEdges fills the two directions of the follow graph.
func (o *mongoOps) readEdges(ctx context.Context, user string, raw *rawUser) error {
	type edge struct {
		Follower string `bson:"follower"`
		Followee string `bson:"followee"`
	}
	following, err := findAll[edge](ctx, o, mongoFollows, bson.D{{Key: "follower", Value: user}}, nil)
	if err != nil {
		return fmt.Errorf("reading who user %s follows: %w", user, err)
	}
	followers, err := findAll[edge](ctx, o, mongoFollows, bson.D{{Key: "followee", Value: user}}, nil)
	if err != nil {
		return fmt.Errorf("reading the followers of user %s: %w", user, err)
	}
	for _, followEdge := range following {
		raw.Following = append(raw.Following, followEdge.Followee)
	}
	for _, followEdge := range followers {
		raw.Followers = append(raw.Followers, followEdge.Follower)
	}
	return nil
}
