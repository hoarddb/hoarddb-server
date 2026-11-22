package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// The collections of the social schema (spec 6.2). Documents of the same
// entity share one collection: the idiomatic MongoDB schema does not embed an
// unbounded list in a document (16 MB limit, whole-document rewrites).
const (
	mongoLikes         = "likes"
	mongoComments      = "comments"
	mongoFollows       = "follows"
	mongoFeed          = "feed"
	mongoNotifications = "notifications"
	mongoMedia         = "media"
	mongoJobs          = "jobs"

	// mongoDuplicateKey is the server code of a unique-key violation.
	mongoDuplicateKey = 11000
	// mongoUserBatch and mongoReactionBatch are how many users, or reactions,
	// one seeding insert carries.
	mongoUserBatch     = 50
	mongoReactionBatch = 1000
)

// mongoIndexSpec is one index the social schema creates besides _id.
type mongoIndexSpec struct {
	Collection string
	Keys       bson.D
	Unique     bool
}

// mongoSocialCollections lists every collection the operations use.
var mongoSocialCollections = []string{
	mongoUsers, mongoPosts, mongoLikes, mongoComments, mongoFollows,
	mongoFeed, mongoNotifications, mongoMedia, mongoJobs,
}

// mongoSocialIndexes is spec 6.2's index column, plus the one the queue
// operation Q1 needs. There is deliberately no index on likes: nothing reads a
// like list, and the like's _id (post|liker) already makes (post author, post
// sequence, liker) unique (P4).
var mongoSocialIndexes = []mongoIndexSpec{
	{Collection: mongoPosts, Keys: bson.D{{Key: "author", Value: 1}, {Key: "seq", Value: -1}}, Unique: true},
	{Collection: mongoComments, Keys: bson.D{{Key: "post", Value: 1}, {Key: "_id", Value: 1}}},
	{Collection: mongoFollows, Keys: bson.D{{Key: "follower", Value: 1}, {Key: "followee", Value: 1}}, Unique: true},
	{Collection: mongoFollows, Keys: bson.D{{Key: "followee", Value: 1}, {Key: "follower", Value: 1}}},
	{Collection: mongoFeed, Keys: bson.D{{Key: "user", Value: 1}, {Key: "_id", Value: -1}}},
	{Collection: mongoNotifications, Keys: bson.D{{Key: "user", Value: 1}, {Key: "_id", Value: 1}}},
	{Collection: mongoJobs, Keys: bson.D{{Key: "outbox", Value: 1}, {Key: "lease_until", Value: 1}, {Key: "_id", Value: 1}}},
}

// postKeyString is a post's _id and the `post` field of its reactions.
func postKeyString(author string, seq int64) string { return fmt.Sprintf("%s/%d", author, seq) }

// likeKey is a like's _id: the unique (post author, post sequence, liker).
func likeKey(post, liker string) string { return post + "|" + liker }

// followKey is a follow edge's _id.
func followKey(follower, followee string) string { return follower + "|" + followee }

// mediaKey is a blob's _id.
func mediaKey(user, kind string) string { return user + "|" + kind }

func tagsOrEmpty(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}

// postDocument is a post as stored in posts.
func postDocument(post postRecord, likes, comments int64) bson.D {
	return bson.D{
		{Key: "_id", Value: postKeyString(post.Author, post.Seq)}, {Key: "author", Value: post.Author}, {Key: "seq", Value: post.Seq},
		{Key: "text", Value: post.Text}, {Key: "created", Value: post.Created}, {Key: "tags", Value: tagsOrEmpty(post.Tags)},
		{Key: "reply_to", Value: post.ReplyTo}, {Key: "likes_n", Value: likes}, {Key: "comments_n", Value: comments},
	}
}

// feedDocument is a copy of a post in a user's feed. It carries the sequence
// and the reply_to beside spec 6.2's columns, because the canonical form
// compares whole posts.
func feedDocument(user string, post postRecord, id int64) bson.D {
	return bson.D{
		{Key: "_id", Value: id}, {Key: "user", Value: user}, {Key: "post_id", Value: postKeyString(post.Author, post.Seq)},
		{Key: "author", Value: post.Author}, {Key: "seq", Value: post.Seq}, {Key: "text", Value: post.Text},
		{Key: "created", Value: post.Created}, {Key: "tags", Value: tagsOrEmpty(post.Tags)}, {Key: "reply_to", Value: post.ReplyTo},
	}
}

func notificationDocument(user, kind, actor, target string, at time.Time, id int64) bson.D {
	return bson.D{
		{Key: "_id", Value: id}, {Key: "user", Value: user}, {Key: "type", Value: kind},
		{Key: "actor", Value: actor}, {Key: "target", Value: target}, {Key: "ts", Value: at},
	}
}

func userDocumentWithKey(profile userProfile) bson.D {
	return bson.D{
		{Key: "_id", Value: profile.ID}, {Key: "userid", Value: profile.ID}, {Key: "name", Value: profile.Name}, {Key: "bio", Value: profile.Bio},
		{Key: "joined", Value: profile.Joined}, {Key: "followers_n", Value: profile.Followers}, {Key: "following_n", Value: profile.Following},
	}
}

// ignoreDuplicateKeys returns nil when err holds nothing but unique-key
// violations: the like the seed repeats is the same like, so a duplicate is
// the engine's own dedupe working. Any other write error, and any
// write-concern failure, is returned.
func ignoreDuplicateKeys(err error) error {
	if err == nil {
		return nil
	}
	var bulk mongo.BulkWriteException
	if !errors.As(err, &bulk) {
		return err
	}
	if bulk.WriteConcernError != nil {
		return err
	}
	for _, writeError := range bulk.WriteErrors {
		if writeError.Code != mongoDuplicateKey {
			return err
		}
	}
	return nil
}

func (m *mongoEngine) collection(name string) *mongo.Collection {
	return m.client.Database(m.cfg.Database).Collection(name)
}

// setupSocial creates every collection and index of the social schema, and
// refuses a database that already holds any of them.
func (m *mongoEngine) setupSocial(ctx context.Context, _ datasetSpec) (schemaInfo, error) {
	database := m.client.Database(m.cfg.Database)
	existing, err := database.ListCollectionNames(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$in", Value: mongoSocialCollections}}}})
	if err != nil {
		return schemaInfo{}, fmt.Errorf("listing collections of %q: %w", m.cfg.Database, err)
	}
	if len(existing) > 0 {
		return schemaInfo{}, fmt.Errorf("database %q already holds %v; pass -attach to reuse it or drop the database first", m.cfg.Database, existing)
	}
	for _, name := range mongoSocialCollections {
		if err := database.CreateCollection(ctx, name); err != nil {
			return schemaInfo{}, fmt.Errorf("creating collection %q: %w", name, err)
		}
	}
	detail := []string{"collections: " + strings.Join(mongoSocialCollections, ", ")}
	for _, spec := range mongoSocialIndexes {
		model := mongo.IndexModel{Keys: spec.Keys, Options: options.Index().SetUnique(spec.Unique)}
		if _, err := database.Collection(spec.Collection).Indexes().CreateOne(ctx, model); err != nil {
			return schemaInfo{}, fmt.Errorf("creating index %v on %s: %w", spec.Keys, spec.Collection, err)
		}
		detail = append(detail, fmt.Sprintf("index %s %v unique=%v", spec.Collection, spec.Keys, spec.Unique))
	}
	return schemaInfo{Layout: "collections-per-entity", Detail: detail, Buckets: len(mongoSocialCollections)}, nil
}

// insertAll inserts documents unordered, one command, and tolerates nothing but
// duplicate keys when tolerateDuplicates is set.
func (m *mongoEngine) insertAll(ctx context.Context, name string, documents []interface{}, tolerateDuplicates bool) error {
	if len(documents) == 0 {
		return nil
	}
	_, err := m.collection(name).InsertMany(ctx, documents, options.InsertMany().SetOrdered(false))
	if tolerateDuplicates {
		err = ignoreDuplicateKeys(err)
	}
	if err != nil {
		return fmt.Errorf("inserting %d documents into %s: %w", len(documents), name, err)
	}
	return nil
}

// seedSocial writes the dataset. Users go in batches of mongoUserBatch, each
// batch one insertMany per collection; reactions follow once every post exists.
func (m *mongoEngine) seedSocial(ctx context.Context, graph *socialGraph, conc int) error {
	likeCounts, commentCounts := seededReactionCounts(graph)
	err := seedPool(ctx, conc, "seeding users", chunks(graph.users(), mongoUserBatch), func(ctx context.Context, batch []graphUser) error {
		return m.seedUsers(ctx, graph, batch, likeCounts, commentCounts)
	})
	if err != nil {
		return err
	}
	err = seedPool(ctx, conc, "seeding media", graph.media(), func(ctx context.Context, item mediaItem) error {
		kind := mediaName(item.Kind)
		document := bson.D{{Key: "_id", Value: mediaKey(userID(item.Owner), kind)}, {Key: "data", Value: graph.content(item)}}
		if _, err := m.collection(mongoMedia).InsertOne(ctx, document); err != nil {
			return fmt.Errorf("storing the %s of user %d: %w", kind, item.Owner, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = seedPool(ctx, conc, "seeding likes", chunks(graph.likes(), mongoReactionBatch), func(ctx context.Context, batch []reaction) error {
		documents := make([]interface{}, len(batch))
		for i, like := range batch {
			post := postKeyString(userID(like.PostAuthor), int64(like.PostSeq))
			documents[i] = bson.D{{Key: "_id", Value: likeKey(post, userID(like.Actor))}, {Key: "post", Value: post}, {Key: "liker", Value: userID(like.Actor)}, {Key: "ts", Value: graphEpoch}}
		}
		return m.insertAll(ctx, mongoLikes, documents, true)
	})
	if err != nil {
		return err
	}
	return seedPool(ctx, conc, "seeding comments", chunks(graph.comments(), mongoReactionBatch), func(ctx context.Context, batch []reaction) error {
		documents := make([]interface{}, len(batch))
		for i, comment := range batch {
			post := postKeyString(userID(comment.PostAuthor), int64(comment.PostSeq))
			documents[i] = bson.D{{Key: "_id", Value: m.ids.next(time.Now().UnixMilli())}, {Key: "post", Value: post}, {Key: "author", Value: userID(comment.Actor)}, {Key: "text", Value: seededCommentText}, {Key: "ts", Value: graphEpoch}}
		}
		return m.insertAll(ctx, mongoComments, documents, false)
	})
}

// seedUsers writes a batch of users with their posts, follow edges, feed and
// inbox. A user's feed is written oldest first with rising ids, so the newest
// entry sorts first when read by _id descending.
func (m *mongoEngine) seedUsers(ctx context.Context, graph *socialGraph, batch []graphUser, likeCounts, commentCounts []uint16) error {
	var users, posts, follows, feed, notifications []interface{}
	for _, user := range batch {
		users = append(users, userDocumentWithKey(user.Profile))
		for seq := range graph.postsPerUser {
			index := user.Rank*graph.postsPerUser + seq
			posts = append(posts, postDocument(recordOf(graph.post(user.Rank, seq)), int64(likeCounts[index]), int64(commentCounts[index])))
		}
		for _, followee := range user.Following {
			follows = append(follows, bson.D{{Key: "_id", Value: followKey(user.Profile.ID, userID(int(followee)))}, {Key: "follower", Value: user.Profile.ID}, {Key: "followee", Value: userID(int(followee))}, {Key: "since", Value: graphEpoch}})
		}
		for _, ref := range user.Feed {
			feed = append(feed, feedDocument(user.Profile.ID, recordOf(graph.post(int(ref.Author), int(ref.Seq))), m.ids.next(time.Now().UnixMilli())))
		}
		for _, note := range user.Inbox {
			notifications = append(notifications, notificationDocument(user.Profile.ID, note.Kind, userID(note.Actor), user.Profile.ID, graphEpoch, m.ids.next(time.Now().UnixMilli())))
		}
	}
	for _, step := range []struct {
		name      string
		documents []interface{}
	}{{mongoUsers, users}, {mongoPosts, posts}, {mongoFollows, follows}, {mongoFeed, feed}, {mongoNotifications, notifications}} {
		if err := m.insertAll(ctx, step.name, step.documents, false); err != nil {
			return err
		}
	}
	return nil
}
