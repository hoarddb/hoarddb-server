package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoOps is one worker's handle on the social operations. Every command is
// sent on its own after the previous one returned (Q-A: serial), and sent
// counts them, so an operation's request count is the difference around it.
type mongoOps struct {
	engine *mongoEngine
	sent   int64
}

func (m *mongoEngine) newSocialOps() (socialOps, error) { return &mongoOps{engine: m}, nil }

func (o *mongoOps) requests() int64 { return o.sent }
func (o *mongoOps) close()          {}

func (o *mongoOps) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, o.engine.cfg.OpTimeout)
}

// notify inserts one notification for user.
func (o *mongoOps) notify(ctx context.Context, user, kind, actor, target string) error {
	o.sent++
	document := notificationDocument(user, kind, actor, target, time.Now().UTC(), o.engine.ids.next(time.Now().UnixMilli()))
	if _, err := o.engine.collection(mongoNotifications).InsertOne(ctx, document); err != nil {
		return fmt.Errorf("notifying user %s of a %s: %w", user, kind, err)
	}
	return nil
}

// findAll runs one find and decodes every document into a slice of T.
func findAll[T any](ctx context.Context, o *mongoOps, name string, filter bson.D, findOptions *options.FindOptions) ([]T, error) {
	o.sent++
	cursor, err := o.engine.collection(name).Find(ctx, filter, findOptions)
	if err != nil {
		return nil, fmt.Errorf("finding in %s: %w", name, err)
	}
	var found []T
	if err := cursor.All(ctx, &found); err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}
	return found, nil
}

// findOne runs one findOne and reports whether a document matched.
func findOne[T any](ctx context.Context, o *mongoOps, name string, filter bson.D) (found T, matched bool, err error) {
	o.sent++
	err = o.engine.collection(name).FindOne(ctx, filter).Decode(&found)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return found, false, nil
	}
	if err != nil {
		return found, false, fmt.Errorf("finding one in %s: %w", name, err)
	}
	return found, true, nil
}

func (o *mongoOps) homeTimeline(parent context.Context, actor, limit int) ([]postRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	newestFirst := options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(int64(limit))
	return findAll[postRecord](ctx, o, mongoFeed, bson.D{{Key: "user", Value: userID(actor)}}, newestFirst)
}

func (o *mongoOps) profile(parent context.Context, target int) (userRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	profile, matched, err := findOne[userRecord](ctx, o, mongoUsers, bson.D{{Key: "_id", Value: userID(target)}})
	if err != nil {
		return userRecord{}, fmt.Errorf("reading user %d: %w", target, err)
	}
	if !matched {
		return userRecord{}, fmt.Errorf("user %d not found", target)
	}
	return profile, nil
}

func (o *mongoOps) profileTimeline(parent context.Context, target, limit int) ([]postRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	newestFirst := options.Find().SetSort(bson.D{{Key: "seq", Value: -1}}).SetLimit(int64(limit))
	return findAll[postRecord](ctx, o, mongoPosts, bson.D{{Key: "author", Value: userID(target)}}, newestFirst)
}

// countedPost is a post document with the counters it carries.
type countedPost struct {
	Post     postRecord `bson:",inline"`
	Likes    int64      `bson:"likes_n"`
	Comments int64      `bson:"comments_n"`
}

func (o *mongoOps) postPage(parent context.Context, author int, seq int64) (postPage, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	counted, matched, err := findOne[countedPost](ctx, o, mongoPosts, bson.D{{Key: "_id", Value: postKeyString(userID(author), seq)}})
	if err != nil {
		return postPage{}, fmt.Errorf("reading post %d of user %d: %w", seq, author, err)
	}
	if !matched {
		return postPage{}, nil
	}
	return postPage{Found: true, Post: counted.Post, Likes: counted.Likes, Comments: counted.Comments}, nil
}

func (o *mongoOps) followCheck(parent context.Context, actor, target int) (bool, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	_, matched, err := findOne[bson.Raw](ctx, o, mongoFollows, bson.D{{Key: "_id", Value: followKey(userID(actor), userID(target))}})
	if err != nil {
		return false, fmt.Errorf("checking whether user %d follows user %d: %w", actor, target, err)
	}
	return matched, nil
}

func (o *mongoOps) followingList(parent context.Context, actor, limit int) ([]string, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	type edge struct {
		Followee string `bson:"followee"`
	}
	edges, err := findAll[edge](ctx, o, mongoFollows, bson.D{{Key: "follower", Value: userID(actor)}}, options.Find().SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("listing who user %d follows: %w", actor, err)
	}
	followed := make([]string, len(edges))
	for i, followEdge := range edges {
		followed[i] = followEdge.Followee
	}
	return followed, nil
}

func (o *mongoOps) avatar(parent context.Context, target int) ([]byte, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	type blob struct {
		Data []byte `bson:"data"`
	}
	stored, matched, err := findOne[blob](ctx, o, mongoMedia, bson.D{{Key: "_id", Value: mediaKey(userID(target), "avatar")}})
	if err != nil {
		return nil, fmt.Errorf("reading the avatar of user %d: %w", target, err)
	}
	if !matched {
		return nil, fmt.Errorf("user %d: %w", target, errNoAvatar)
	}
	return stored.Data, nil
}

// readNotifications is find then deleteMany: two commands that are not atomic
// (spec 6.3). A second reader may take the same notifications between them, so
// the count returned is what this call's delete removed.
func (o *mongoOps) readNotifications(parent context.Context, actor, limit int) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	type queued struct {
		ID int64 `bson:"_id"`
	}
	oldestFirst := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit)).SetProjection(bson.D{{Key: "_id", Value: 1}})
	taken, err := findAll[queued](ctx, o, mongoNotifications, bson.D{{Key: "user", Value: userID(actor)}}, oldestFirst)
	if err != nil || len(taken) == 0 {
		return 0, err
	}
	ids := make([]int64, len(taken))
	for i, notification := range taken {
		ids[i] = notification.ID
	}
	o.sent++
	deleted, err := o.engine.collection(mongoNotifications).DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return 0, fmt.Errorf("clearing the inbox of user %d: %w", actor, err)
	}
	return int(deleted.DeletedCount), nil
}

// followerRanks lists who follows author, one find.
func (o *mongoOps) followerRanks(ctx context.Context, author int) ([]int, error) {
	type edge struct {
		Follower string `bson:"follower"`
	}
	edges, err := findAll[edge](ctx, o, mongoFollows, bson.D{{Key: "followee", Value: userID(author)}}, options.Find().SetProjection(bson.D{{Key: "follower", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("listing the followers of user %d: %w", author, err)
	}
	ranks := make([]int, len(edges))
	for i, followEdge := range edges {
		rank, err := strconv.Atoi(followEdge.Follower)
		if err != nil {
			return nil, fmt.Errorf("follower %q of user %d is not a user id: %w", followEdge.Follower, author, err)
		}
		ranks[i] = rank
	}
	return ranks, nil
}

func (o *mongoOps) publishPost(parent context.Context, post postRecord, ledger deliveryLedger) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	author, err := strconv.Atoi(post.Author)
	if err != nil {
		return 0, fmt.Errorf("post author %q is not a user id: %w", post.Author, err)
	}
	followers, err := o.followerRanks(ctx, author)
	if err != nil {
		return 0, err
	}
	o.sent++
	if _, err := o.engine.collection(mongoPosts).InsertOne(ctx, postDocument(post, 0, 0)); err != nil {
		return 0, fmt.Errorf("storing post %d of user %d: %w", post.Seq, author, err)
	}
	if len(followers) == 0 {
		return 0, nil
	}
	if o.engine.cfg.bulkFanout() {
		err = o.copyInBulk(ctx, post, followers, ledger)
	} else {
		err = o.copySingly(ctx, post, followers, ledger)
	}
	if err != nil {
		return 0, err
	}
	return len(followers), nil
}

// copyInBulk puts the post into every follower's feed with one insertMany
// (-fanout bulk).
func (o *mongoOps) copyInBulk(ctx context.Context, post postRecord, followers []int, ledger deliveryLedger) error {
	copies := make([]interface{}, len(followers))
	for i, follower := range followers {
		ledger.sending(follower)
		copies[i] = feedDocument(userID(follower), post, o.engine.ids.next(time.Now().UnixMilli()))
	}
	o.sent++
	if _, err := o.engine.collection(mongoFeed).InsertMany(ctx, copies); err != nil {
		return fmt.Errorf("copying post %d of user %s into %d feeds: %w", post.Seq, post.Author, len(followers), err)
	}
	for _, follower := range followers {
		ledger.delivered(follower)
	}
	return nil
}

// copySingly puts the post into each follower's feed with its own insertOne,
// one after another in the order the followers were listed: hoardDB's
// sequence.
func (o *mongoOps) copySingly(ctx context.Context, post postRecord, followers []int, ledger deliveryLedger) error {
	for _, follower := range followers {
		ledger.sending(follower)
		o.sent++
		if _, err := o.engine.collection(mongoFeed).InsertOne(ctx, feedDocument(userID(follower), post, o.engine.ids.next(time.Now().UnixMilli()))); err != nil {
			return fmt.Errorf("copying post %d of user %s into the feed of user %d: %w", post.Seq, post.Author, follower, err)
		}
		ledger.delivered(follower)
	}
	return nil
}

// incrementCounter adds delta to one counter of one document.
func (o *mongoOps) incrementCounter(ctx context.Context, name, key, counter string, delta int64) error {
	o.sent++
	update := bson.D{{Key: "$inc", Value: bson.D{{Key: counter, Value: delta}}}}
	if _, err := o.engine.collection(name).UpdateByID(ctx, key, update); err != nil {
		return fmt.Errorf("changing %s of %s in %s: %w", counter, key, name, err)
	}
	return nil
}

// likePost inserts the like, keyed by (post author, post sequence, liker): a
// second like by the same user is a duplicate-key error and adds nothing, so
// the counter is raised only for a like that was new. There is no
// multi-document transaction, the idiomatic choice (spec 6.3).
func (o *mongoOps) likePost(parent context.Context, liker, author int, seq int64) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	post := postKeyString(userID(author), seq)
	o.sent++
	_, err := o.engine.collection(mongoLikes).InsertOne(ctx, bson.D{{Key: "_id", Value: likeKey(post, userID(liker))}, {Key: "post", Value: post}, {Key: "liker", Value: userID(liker)}, {Key: "ts", Value: time.Now().UTC()}})
	switch {
	case err == nil:
		if err := o.incrementCounter(ctx, mongoPosts, post, "likes_n", 1); err != nil {
			return err
		}
	case !mongo.IsDuplicateKeyError(err):
		return fmt.Errorf("user %d liking post %d of user %d: %w", liker, seq, author, err)
	}
	return o.notify(ctx, userID(author), "like", userID(liker), fmt.Sprintf("%d/%s", author, postKey(seq)))
}

func (o *mongoOps) follow(parent context.Context, actor, target int) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	o.sent++
	edge := bson.D{{Key: "_id", Value: followKey(userID(actor), userID(target))}, {Key: "follower", Value: userID(actor)}, {Key: "followee", Value: userID(target)}, {Key: "since", Value: time.Now().UTC()}}
	if _, err := o.engine.collection(mongoFollows).InsertOne(ctx, edge); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil // already following: nothing changed, so no counter moves
		}
		return fmt.Errorf("user %d following user %d: %w", actor, target, err)
	}
	if err := o.incrementCounter(ctx, mongoUsers, userID(target), "followers_n", 1); err != nil {
		return err
	}
	if err := o.incrementCounter(ctx, mongoUsers, userID(actor), "following_n", 1); err != nil {
		return err
	}
	return o.notify(ctx, userID(target), "follow", userID(actor), userID(target))
}

func (o *mongoOps) unfollow(parent context.Context, actor, target int) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	o.sent++
	deleted, err := o.engine.collection(mongoFollows).DeleteOne(ctx, bson.D{{Key: "_id", Value: followKey(userID(actor), userID(target))}})
	if err != nil {
		return fmt.Errorf("user %d unfollowing user %d: %w", actor, target, err)
	}
	if deleted.DeletedCount == 0 {
		return nil // there was no edge, so no counter moves
	}
	if err := o.incrementCounter(ctx, mongoUsers, userID(target), "followers_n", -1); err != nil {
		return err
	}
	return o.incrementCounter(ctx, mongoUsers, userID(actor), "following_n", -1)
}

func (o *mongoOps) commentOnPost(parent context.Context, commenter, author int, seq int64, text string) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	post := postKeyString(userID(author), seq)
	o.sent++
	comment := bson.D{{Key: "_id", Value: o.engine.ids.next(time.Now().UnixMilli())}, {Key: "post", Value: post}, {Key: "author", Value: userID(commenter)}, {Key: "text", Value: text}, {Key: "ts", Value: time.Now().UTC()}}
	if _, err := o.engine.collection(mongoComments).InsertOne(ctx, comment); err != nil {
		return fmt.Errorf("user %d commenting on post %d of user %d: %w", commenter, seq, author, err)
	}
	if err := o.incrementCounter(ctx, mongoPosts, post, "comments_n", 1); err != nil {
		return err
	}
	return o.notify(ctx, userID(author), "comment", userID(commenter), fmt.Sprintf("%d/%s", author, postKey(seq)))
}

func (o *mongoOps) uploadAvatar(parent context.Context, user int, content []byte) error {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	o.sent++
	_, err := o.engine.collection(mongoMedia).ReplaceOne(ctx, bson.D{{Key: "_id", Value: mediaKey(userID(user), "avatar")}}, bson.D{{Key: "data", Value: content}}, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("uploading the avatar of user %d: %w", user, err)
	}
	return nil
}

// mergedTimeline is one find over the followees' posts, newest first: the
// idiomatic form of the merge-on-read design (spec 6.3), whose answer is the
// true newest 20 across all followees.
func (o *mongoOps) mergedTimeline(parent context.Context, followees []int) ([]postRecord, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	authors := make([]string, len(followees))
	for i, followee := range followees {
		authors[i] = userID(followee)
	}
	newest := options.Find().SetSort(bson.D{{Key: "created", Value: -1}}).SetLimit(mergedPostsTotal)
	return findAll[postRecord](ctx, o, mongoPosts, bson.D{{Key: "author", Value: bson.D{{Key: "$in", Value: authors}}}}, newest)
}

// queueRound pushes one job, claims up to queueClaimSize with a lease (a
// findOneAndUpdate per job: MongoDB has no multi-document claim) and
// acknowledges what it claimed with one deleteMany.
func (o *mongoOps) queueRound(parent context.Context, outbox int) (int, error) {
	ctx, cancel := o.opContext(parent)
	defer cancel()
	jobs := o.engine.collection(mongoJobs)
	o.sent++
	freeSince := time.Time{}
	job := bson.D{{Key: "_id", Value: o.engine.ids.next(time.Now().UnixMilli())}, {Key: "outbox", Value: outbox}, {Key: "lease_until", Value: freeSince}, {Key: "created", Value: time.Now().UTC()}}
	if _, err := jobs.InsertOne(ctx, job); err != nil {
		return 0, fmt.Errorf("pushing a job onto outbox %d: %w", outbox, err)
	}
	var claimed []int64
	for len(claimed) < queueClaimSize {
		now := time.Now().UTC()
		o.sent++
		filter := bson.D{{Key: "outbox", Value: outbox}, {Key: "lease_until", Value: bson.D{{Key: "$lte", Value: now}}}}
		lease := bson.D{{Key: "$set", Value: bson.D{{Key: "lease_until", Value: now.Add(queueLeaseMs * time.Millisecond)}}}}
		var leased struct {
			ID int64 `bson:"_id"`
		}
		err := jobs.FindOneAndUpdate(ctx, filter, lease, options.FindOneAndUpdate().SetSort(bson.D{{Key: "_id", Value: 1}})).Decode(&leased)
		if errors.Is(err, mongo.ErrNoDocuments) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("claiming from outbox %d: %w", outbox, err)
		}
		claimed = append(claimed, leased.ID)
	}
	if len(claimed) == 0 {
		return 0, nil
	}
	o.sent++
	if _, err := jobs.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: claimed}}}}); err != nil {
		return 0, fmt.Errorf("acknowledging %d jobs of outbox %d: %w", len(claimed), outbox, err)
	}
	return len(claimed), nil
}
