package main

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// seedChannelDepth is how many items the seeding producer runs ahead of the
// workers.
const seedChannelDepth = 256

// setupSocial creates the database, the User bucket with its store fields (one
// bucket under H-1, one per shard under H-64) and the bucket-level FIFOs that
// operation Q1 uses.
func (h *hoardEngine) setupSocial(_ context.Context, _ datasetSpec) (schemaInfo, error) {
	caller, release, err := h.openCaller()
	if err != nil {
		return schemaInfo{}, fmt.Errorf("opening a connection for setup: %w", err)
	}
	defer release()
	var created server.CreateDatabaseResponse
	if err := caller.call(transport.OpCreateDB, server.CreateDatabaseRequest{Name: h.cfg.Database}, &created); err != nil {
		return schemaInfo{}, fmt.Errorf("creating database %q: %w", h.cfg.Database, err)
	}
	if !created.Success {
		return schemaInfo{}, fmt.Errorf("creating database %q: %s; if it is left over from an earlier run, delete it first", h.cfg.Database, created.Message)
	}
	place := h.placement()
	// The database exists, and every bucket below is independent of the others,
	// so only the database creation above is ordered; the buckets go out
	// -depth at a time because each replicated create waits on its own fsyncs.
	requests := make([]server.CreateBucketRequest, 0, place.shards+outboxBuckets)
	for _, name := range place.buckets() {
		requests = append(requests, server.CreateBucketRequest{Database: h.cfg.Database, Name: name, Type: server.BucketTypeHash, Options: map[string]interface{}{"stores": hoardStoreDeclarations()}})
	}
	for index := range outboxBuckets {
		requests = append(requests, server.CreateBucketRequest{Database: h.cfg.Database, Name: outboxBucket(index), Type: server.BucketTypeFIFO})
	}
	progress := h.newProgress("buckets", len(requests), bucketProgressEvery)
	err = seedEach(h, max(h.cfg.Conc, 1), "creating buckets", slices.Values(requests), counted(progress, createSocialBucket))
	if err != nil {
		return schemaInfo{}, err
	}
	userDetail := "hash bucket User { Posts btreeStore { Likes hashStore, Comments fifoStore }, Feed lifoStore, Following hashStore, Followers hashStore, Inbox fifoStore, Media blobStore }"
	layout := "nested-stores-H-1"
	switch {
	case place.perUser:
		layout = "nested-stores-H-U"
		userDetail = fmt.Sprintf("%d hash buckets %s to %s, one per user, each declared as User: %s",
			place.shards, place.names[0], place.names[place.shards-1], strings.TrimPrefix(userDetail, "hash bucket User "))
	case place.shards > 1:
		layout = fmt.Sprintf("nested-stores-H-%d", place.shards)
		userDetail = fmt.Sprintf("%d hash buckets %s to %s, each declared as User; a user lives in shard xxHash64(user id) mod %d: %s",
			place.shards, place.names[0], place.names[place.shards-1], place.shards, strings.TrimPrefix(userDetail, "hash bucket User "))
	}
	return schemaInfo{
		Layout: layout,
		Detail: []string{
			userDetail,
			fmt.Sprintf("%d FIFO buckets Outbox_<k> for operation Q1", outboxBuckets),
		},
		Buckets: place.shards + outboxBuckets,
	}, nil
}

func createSocialBucket(caller wireCaller, request server.CreateBucketRequest) error {
	var created server.CreateBucketResponse
	if err := caller.call(transport.OpCreateBucket, request, &created); err != nil {
		return fmt.Errorf("creating bucket %q: %w", request.Name, err)
	}
	if !created.Success {
		return fmt.Errorf("creating bucket %q: %s", request.Name, created.Message)
	}
	return nil
}

// recordOf is a generated post as the stored document.
func recordOf(post graphPost) postRecord {
	return postRecord{Author: userID(post.Author), Seq: int64(post.Seq), Text: post.Text, Created: post.Created, Tags: post.Tags, ReplyTo: post.ReplyTo}
}

// followerLists inverts the follow graph: user u is followed by
// followers[offsets[u]:offsets[u+1]].
func followerLists(graph *socialGraph) (offsets, followers []int32) {
	offsets = make([]int32, graph.spec.Users+1)
	for _, followee := range graph.followees {
		offsets[followee+1]++
	}
	for user := range graph.spec.Users {
		offsets[user+1] += offsets[user]
	}
	followers = make([]int32, len(graph.followees))
	fill := make([]int32, graph.spec.Users)
	for follower := range graph.spec.Users {
		for _, followee := range graph.following(follower) {
			followers[offsets[followee]+fill[followee]] = int32(follower)
			fill[followee]++
		}
	}
	return offsets, followers
}

// seedEach feeds items to conc workers, each with a connection of its own,
// and returns the first error. One item is handled by one worker from start to
// finish, so the pushes onto a user's own stores keep their order.
func seedEach[T any](h *hoardEngine, conc int, doing string, items iter.Seq[T], write func(caller wireCaller, item T) error) error {
	queue := make(chan T, seedChannelDepth)
	var group sync.WaitGroup
	var firstFailure atomic.Pointer[error]
	for range conc {
		group.Add(1)
		go func() {
			defer group.Done()
			caller, release, err := h.openCaller()
			if err != nil {
				failure := fmt.Errorf("%s: %w", doing, err)
				firstFailure.CompareAndSwap(nil, &failure)
			} else {
				defer release()
			}
			for item := range queue {
				if caller == nil || firstFailure.Load() != nil {
					continue // drain, so the producer never blocks
				}
				if err := write(caller, item); err != nil {
					failure := fmt.Errorf("%s: %w", doing, err)
					firstFailure.CompareAndSwap(nil, &failure)
				}
			}
		}()
	}
	for item := range items {
		if firstFailure.Load() != nil {
			break
		}
		queue <- item
	}
	close(queue)
	group.Wait()
	if failure := firstFailure.Load(); failure != nil {
		return *failure
	}
	return nil
}

// seedSocial writes the dataset: each user's document, posts, follow edges,
// feed (oldest first, so the newest is on top), inbox and media; then, once
// every post exists, the likes and comments, which hang on posts.
func (h *hoardEngine) seedSocial(_ context.Context, graph *socialGraph, conc int) error {
	followerOffsets, followers := followerLists(graph)
	progress := h.newProgress("ops", 0, opProgressEvery)
	err := seedEach(h, conc, "seeding users", graph.users(), counted(progress, func(caller wireCaller, user graphUser) error {
		return h.seedUser(caller, graph, user, followers[followerOffsets[user.Rank]:followerOffsets[user.Rank+1]])
	}))
	if err != nil {
		return err
	}
	err = seedEach(h, conc, "seeding media", graph.media(), counted(progress, func(caller wireCaller, item mediaItem) error {
		kind := mediaName(item.Kind)
		ops := h.opsOn(caller)
		put := ops.field(item.Owner, "Media")
		put.Element, put.Content, put.Data = kind, graph.content(item), map[string]interface{}{"kind": kind}
		_, err := ops.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("storing the %s of user %d", kind, item.Owner))
		return err
	}))
	if err != nil {
		return err
	}
	err = seedEach(h, conc, "seeding likes", graph.likes(), counted(progress, func(caller wireCaller, like reaction) error {
		ops := h.opsOn(caller)
		put := ops.postStore(like.PostAuthor, int64(like.PostSeq), "Likes")
		put.Element, put.Data = userID(like.Actor), map[string]interface{}{"liker": userID(like.Actor), "ts": graphEpoch}
		_, err := ops.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("seeding a like on post %d of user %d", like.PostSeq, like.PostAuthor))
		return err
	}))
	if err != nil {
		return err
	}
	return seedEach(h, conc, "seeding comments", graph.comments(), counted(progress, func(caller wireCaller, comment reaction) error {
		ops := h.opsOn(caller)
		push := ops.postStore(comment.PostAuthor, int64(comment.PostSeq), "Comments")
		push.Data = map[string]interface{}{"author": userID(comment.Actor), "text": seededCommentText, "ts": graphEpoch}
		_, err := ops.fieldCall(transport.OpFieldPush, push, fmt.Sprintf("seeding a comment on post %d of user %d", comment.PostSeq, comment.PostAuthor))
		return err
	}))
}

// seedUser writes one user's document and everything in the user's own stores.
func (h *hoardEngine) seedUser(caller wireCaller, graph *socialGraph, user graphUser, followerRanks []int32) error {
	var stored server.InsertResponse
	insert := server.InsertRequest{Database: h.cfg.Database, Bucket: h.placement().bucketOf(user.Rank), Key: user.Profile.ID, Data: userDocument(user.Profile)}
	if err := caller.call(transport.OpUpsert, insert, &stored); err != nil {
		return fmt.Errorf("storing user %d: %w", user.Rank, err)
	}
	if !stored.Success {
		return fmt.Errorf("storing user %d: %s", user.Rank, stored.Message)
	}
	ops := h.opsOn(caller)
	for seq := range graph.postsPerUser {
		post := recordOf(graph.post(user.Rank, seq))
		put := ops.field(user.Rank, "Posts")
		put.Element, put.Data = postKey(post.Seq), post.document()
		if _, err := ops.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("seeding post %d of user %d", seq, user.Rank)); err != nil {
			return err
		}
	}
	edge := map[string]interface{}{"since": graphEpoch}
	for _, followee := range user.Following {
		put := ops.field(user.Rank, "Following")
		put.Element, put.Data = userID(int(followee)), edge
		if _, err := ops.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("seeding who user %d follows", user.Rank)); err != nil {
			return err
		}
	}
	for _, follower := range followerRanks {
		put := ops.field(user.Rank, "Followers")
		put.Element, put.Data = userID(int(follower)), edge
		if _, err := ops.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("seeding the followers of user %d", user.Rank)); err != nil {
			return err
		}
	}
	for _, ref := range user.Feed {
		push := ops.field(user.Rank, "Feed")
		push.Data = recordOf(graph.post(int(ref.Author), int(ref.Seq))).document()
		if _, err := ops.fieldCall(transport.OpFieldPush, push, fmt.Sprintf("seeding the feed of user %d", user.Rank)); err != nil {
			return err
		}
	}
	for _, note := range user.Inbox {
		push := ops.field(user.Rank, "Inbox")
		push.Data = map[string]interface{}{"type": note.Kind, "actor": userID(note.Actor), "target": user.Profile.ID, "ts": graphEpoch}
		if _, err := ops.fieldCall(transport.OpFieldPush, push, fmt.Sprintf("seeding the inbox of user %d", user.Rank)); err != nil {
			return err
		}
	}
	return nil
}
