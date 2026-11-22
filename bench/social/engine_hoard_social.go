package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// hoardOps is one worker's handle on the social operations over one
// connection. Sub-operations are sent one after another (Q-A); nothing here
// starts a goroutine.
type hoardOps struct {
	caller   wireCaller
	database string
	// place routes each user to the bucket holding the user's document.
	place   placement
	release func()
	// batch sends each operation's sub-operations as one BATCH request: the
	// record-only -hoard-batch variant.
	batch bool
}

// openCaller opens one connection for the social operations, stamped with the
// run's write concern when it names one.
func (h *hoardEngine) openCaller() (wireCaller, func(), error) {
	addr := h.nextNode()
	var caller wireCaller
	var release func()
	var err error
	if h.dial != nil {
		caller, release, err = h.dial(addr)
	} else {
		var conn *wireConn
		if conn, err = dialWire(addr, h.cfg.Password, h.cfg.OpTimeout); err == nil {
			caller, release = conn, conn.close
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if h.cfg.WriteConcern != "" {
		caller = concernCaller{wireCaller: caller, level: h.cfg.WriteConcern}
	}
	return caller, release, nil
}

func (h *hoardEngine) newSocialOps() (socialOps, error) {
	caller, release, err := h.openCaller()
	if err != nil {
		return nil, fmt.Errorf("opening a connection for the social operations: %w", err)
	}
	ops := h.opsOn(caller)
	ops.release = release
	return &ops, nil
}

func (o *hoardOps) requests() int64 { return o.caller.requests() }
func (o *hoardOps) close()          { o.release() }

// field addresses a store field of one user.
func (o *hoardOps) field(user int, name string) server.FieldRequest {
	return server.FieldRequest{Database: o.database, Bucket: o.place.bucketOf(user), Key: userID(user), Field: name}
}

// postStore addresses an L3 store of one post: Likes or Comments.
func (o *hoardOps) postStore(author int, seq int64, name string) server.FieldRequest {
	request := o.field(author, name)
	request.Path = []interface{}{"Posts", postKey(seq)}
	return request
}

// fieldCall sends one field verb and turns a refusal or an unsuccessful reply
// into an error that names what was being done.
func (o *hoardOps) fieldCall(op transport.Opcode, request interface{}, doing string) (server.FieldResponse, error) {
	var reply server.FieldResponse
	if err := o.caller.call(op, request, &reply); err != nil {
		return reply, fmt.Errorf("%s: %w", doing, err)
	}
	if !reply.Success {
		return reply, fmt.Errorf("%s: %s", doing, reply.Message)
	}
	return reply, nil
}

func decodePosts(entries []server.FieldListEntry) ([]postRecord, error) {
	posts := make([]postRecord, len(entries))
	for i, entry := range entries {
		if err := bson.Unmarshal(entry.Element, &posts[i]); err != nil {
			return nil, fmt.Errorf("decoding element %d: %w", i, err)
		}
	}
	return posts, nil
}

func (o *hoardOps) homeTimeline(_ context.Context, actor, limit int) ([]postRecord, error) {
	reply, err := o.fieldCall(transport.OpFieldPeek, server.FieldPeekRequest{FieldRequest: o.field(actor, "Feed"), Limit: limit}, fmt.Sprintf("reading the feed of user %d", actor))
	if err != nil {
		return nil, err
	}
	return decodePosts(reply.Items)
}

func (o *hoardOps) profile(_ context.Context, target int) (userRecord, error) {
	var found server.FindResponse
	request := server.FindRequest{Database: o.database, Bucket: o.place.bucketOf(target), Key: userID(target)}
	if err := o.caller.call(transport.OpGet, request, &found); err != nil {
		return userRecord{}, fmt.Errorf("reading user %d: %w", target, err)
	}
	if !found.Found {
		return userRecord{}, fmt.Errorf("user %d not found: %s", target, found.Message)
	}
	var profile userRecord
	if err := bson.Unmarshal(found.Data, &profile); err != nil {
		return userRecord{}, fmt.Errorf("decoding user %d: %w", target, err)
	}
	return profile, nil
}

func (o *hoardOps) listPosts(target, limit int) ([]postRecord, error) {
	var page server.FieldListResponse
	request := server.FieldListRequest{FieldRequest: o.field(target, "Posts"), Limit: limit}
	if err := o.caller.call(transport.OpFieldList, request, &page); err != nil {
		return nil, fmt.Errorf("listing the posts of user %d: %w", target, err)
	}
	if !page.Success {
		return nil, fmt.Errorf("listing the posts of user %d: %s", target, page.Message)
	}
	return decodePosts(page.Entries)
}

func (o *hoardOps) profileTimeline(_ context.Context, target, limit int) ([]postRecord, error) {
	return o.listPosts(target, limit)
}

func (o *hoardOps) postPage(_ context.Context, author int, seq int64) (postPage, error) {
	if o.batch {
		return o.postPageBatched(author, seq)
	}
	doing := fmt.Sprintf("reading post %d of user %d", seq, author)
	getRequest := o.field(author, "Posts")
	getRequest.Element = postKey(seq)
	got, err := o.fieldCall(transport.OpFieldGet, getRequest, doing)
	if err != nil || !got.Found {
		return postPage{}, err
	}
	page := postPage{Found: true}
	if err := bson.Unmarshal(got.Element, &page.Post); err != nil {
		return postPage{}, fmt.Errorf("%s: decoding: %w", doing, err)
	}
	likes, err := o.fieldCall(transport.OpFieldLen, o.postStore(author, seq, "Likes"), doing+": counting likes")
	if err != nil {
		return postPage{}, err
	}
	comments, err := o.fieldCall(transport.OpFieldLen, o.postStore(author, seq, "Comments"), doing+": counting comments")
	if err != nil {
		return postPage{}, err
	}
	page.Likes, page.Comments = likes.Len, comments.Len
	return page, nil
}

func (o *hoardOps) followCheck(_ context.Context, actor, target int) (bool, error) {
	request := o.field(actor, "Following")
	request.Element = userID(target)
	got, err := o.fieldCall(transport.OpFieldGet, request, fmt.Sprintf("checking whether user %d follows user %d", actor, target))
	return got.Found, err
}

func (o *hoardOps) followingList(_ context.Context, actor, limit int) ([]string, error) {
	var page server.FieldListResponse
	request := server.FieldListRequest{FieldRequest: o.field(actor, "Following"), Limit: limit}
	if err := o.caller.call(transport.OpFieldList, request, &page); err != nil {
		return nil, fmt.Errorf("listing who user %d follows: %w", actor, err)
	}
	if !page.Success {
		return nil, fmt.Errorf("listing who user %d follows: %s", actor, page.Message)
	}
	followed := make([]string, len(page.Entries))
	for i, entry := range page.Entries {
		followed[i] = entry.ElementKey
	}
	return followed, nil
}

func (o *hoardOps) avatar(_ context.Context, target int) ([]byte, error) {
	request := o.field(target, "Media")
	request.Element = "avatar"
	got, err := o.fieldCall(transport.OpFieldGet, request, fmt.Sprintf("reading the avatar of user %d", target))
	if err != nil {
		return nil, err
	}
	if !got.Found {
		return nil, fmt.Errorf("user %d: %w", target, errNoAvatar)
	}
	return got.Content, nil
}

func (o *hoardOps) readNotifications(_ context.Context, actor, limit int) (int, error) {
	reply, err := o.fieldCall(transport.OpFieldPop, server.FieldPopRequest{FieldRequest: o.field(actor, "Inbox"), Limit: limit}, fmt.Sprintf("popping the inbox of user %d", actor))
	return len(reply.Items), err
}

// notify pushes one notification onto a user's Inbox.
func (o *hoardOps) notify(user int, kind string, actor int, subject string) error {
	request := o.field(user, "Inbox")
	request.Data = map[string]interface{}{"type": kind, "actor": userID(actor), "target": subject, "ts": time.Now().UTC()}
	_, err := o.fieldCall(transport.OpFieldPush, request, fmt.Sprintf("notifying user %d of a %s", user, kind))
	return err
}

// followerIDs lists a user's followers, a page at a time.
func (o *hoardOps) followerIDs(author int) ([]int, error) {
	var followers []int
	after := ""
	for {
		request := server.FieldListRequest{FieldRequest: o.field(author, "Followers"), Limit: followerPageSize}
		if after != "" {
			valueType, value, err := bson.MarshalValue(after)
			if err != nil {
				return nil, fmt.Errorf("encoding the follower cursor %q: %w", after, err)
			}
			request.After = bson.RawValue{Type: valueType, Value: value}
		}
		var page server.FieldListResponse
		if err := o.caller.call(transport.OpFieldList, request, &page); err != nil {
			return nil, fmt.Errorf("listing the followers of user %d: %w", author, err)
		}
		if !page.Success {
			return nil, fmt.Errorf("listing the followers of user %d: %s", author, page.Message)
		}
		for _, entry := range page.Entries {
			rank, err := strconv.Atoi(entry.ElementKey)
			if err != nil {
				return nil, fmt.Errorf("follower key %q of user %d is not a user id: %w", entry.ElementKey, author, err)
			}
			followers = append(followers, rank)
			after = entry.ElementKey
		}
		if !page.HasMore || len(page.Entries) == 0 {
			return followers, nil
		}
	}
}

func (o *hoardOps) publishPost(_ context.Context, post postRecord, ledger deliveryLedger) (int, error) {
	author, err := strconv.Atoi(post.Author)
	if err != nil {
		return 0, fmt.Errorf("post author %q is not a user id: %w", post.Author, err)
	}
	followers, err := o.followerIDs(author)
	if err != nil {
		return 0, err
	}
	if o.batch {
		return o.publishPostBatched(post, author, followers, ledger)
	}
	put := o.field(author, "Posts")
	put.Element, put.Data = postKey(post.Seq), post.document()
	if _, err := o.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("storing post %d of user %d", post.Seq, author)); err != nil {
		return 0, err
	}
	// The copies go out one shard after another, so a run of requests meets
	// one bucket at a time; each follower still gets exactly one.
	for _, index := range o.place.byShard(followers) {
		follower := followers[index]
		push := o.field(follower, "Feed")
		push.Data = post.document()
		ledger.sending(follower)
		if _, err := o.fieldCall(transport.OpFieldPush, push, fmt.Sprintf("copying post %d of user %d into the feed of user %d", post.Seq, author, follower)); err != nil {
			return 0, err
		}
		ledger.delivered(follower)
	}
	return len(followers), nil
}

func (o *hoardOps) likePost(_ context.Context, liker, author int, seq int64) error {
	if o.batch {
		return o.likePostBatched(liker, author, seq)
	}
	put := o.postStore(author, seq, "Likes")
	put.Element, put.Data = userID(liker), map[string]interface{}{"liker": userID(liker), "ts": time.Now().UTC()}
	if _, err := o.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("user %d liking post %d of user %d", liker, seq, author)); err != nil {
		return err
	}
	return o.notify(author, "like", liker, fmt.Sprintf("%d/%s", author, postKey(seq)))
}

// adjustCounts raises or lowers the follower and following counters, two updates.
func (o *hoardOps) adjustCounts(actor, target int, delta int64) error {
	for _, change := range []struct {
		user    int
		counter string
	}{{target, "followers_n"}, {actor, "following_n"}} {
		var updated server.UpdateResponse
		request := server.UpdateRequest{Database: o.database, Bucket: o.place.bucketOf(change.user), Key: userID(change.user), Inc: map[string]int64{change.counter: delta}}
		if err := o.caller.call(transport.OpUpdate, request, &updated); err != nil {
			return fmt.Errorf("changing %s of user %d: %w", change.counter, change.user, err)
		}
		if !updated.Success {
			return fmt.Errorf("changing %s of user %d: %s", change.counter, change.user, updated.Message)
		}
	}
	return nil
}

func (o *hoardOps) follow(_ context.Context, actor, target int) error {
	if o.batch {
		return o.followBatched(actor, target)
	}
	since := map[string]interface{}{"since": time.Now().UTC()}
	following := o.field(actor, "Following")
	following.Element, following.Data = userID(target), since
	followers := o.field(target, "Followers")
	followers.Element, followers.Data = userID(actor), since
	doing := fmt.Sprintf("user %d following user %d", actor, target)
	if _, err := o.fieldCall(transport.OpFieldPut, following, doing); err != nil {
		return err
	}
	if _, err := o.fieldCall(transport.OpFieldPut, followers, doing); err != nil {
		return err
	}
	if err := o.adjustCounts(actor, target, 1); err != nil {
		return err
	}
	return o.notify(target, "follow", actor, userID(target))
}

func (o *hoardOps) unfollow(_ context.Context, actor, target int) error {
	if o.batch {
		return o.unfollowBatched(actor, target)
	}
	following := o.field(actor, "Following")
	following.Element = userID(target)
	followers := o.field(target, "Followers")
	followers.Element = userID(actor)
	doing := fmt.Sprintf("user %d unfollowing user %d", actor, target)
	if _, err := o.fieldCall(transport.OpFieldDelete, following, doing); err != nil {
		return err
	}
	if _, err := o.fieldCall(transport.OpFieldDelete, followers, doing); err != nil {
		return err
	}
	return o.adjustCounts(actor, target, -1)
}

func (o *hoardOps) commentOnPost(_ context.Context, commenter, author int, seq int64, text string) error {
	if o.batch {
		return o.commentOnPostBatched(commenter, author, seq, text)
	}
	push := o.postStore(author, seq, "Comments")
	push.Data = map[string]interface{}{"author": userID(commenter), "text": text, "ts": time.Now().UTC()}
	if _, err := o.fieldCall(transport.OpFieldPush, push, fmt.Sprintf("user %d commenting on post %d of user %d", commenter, seq, author)); err != nil {
		return err
	}
	return o.notify(author, "comment", commenter, fmt.Sprintf("%d/%s", author, postKey(seq)))
}

func (o *hoardOps) uploadAvatar(_ context.Context, user int, content []byte) error {
	put := o.field(user, "Media")
	put.Element, put.Content, put.Data = "avatar", content, map[string]interface{}{"kind": "avatar"}
	_, err := o.fieldCall(transport.OpFieldPut, put, fmt.Sprintf("uploading the avatar of user %d", user))
	return err
}

// mergedTimeline reads up to 20 of the newest posts of every followee, one
// list request each and serially, and merges them to the true newest 20: no
// followee can hold more than 20 of the answer, so 20 from each is enough.
func (o *hoardOps) mergedTimeline(_ context.Context, followees []int) ([]postRecord, error) {
	// Followees are read one shard after another, but the lists are joined in
	// the order the caller gave, so ties in the merge fall out as under H-1.
	lists := make([][]postRecord, len(followees))
	for _, index := range o.place.byShard(followees) {
		posts, err := o.listPosts(followees[index], mergedPostsTotal)
		if err != nil {
			return nil, err
		}
		lists[index] = posts
	}
	var merged []postRecord
	for _, posts := range lists {
		merged = append(merged, posts...)
	}
	slices.SortStableFunc(merged, func(a, b postRecord) int { return b.Created.Compare(a.Created) })
	return merged[:min(len(merged), mergedPostsTotal)], nil
}

func (o *hoardOps) queueRound(_ context.Context, outbox int) (int, error) {
	bucket := outboxBucket(outbox)
	var pushed server.PushResponse
	job := map[string]interface{}{"job": time.Now().UnixNano(), "outbox": outbox}
	if err := o.caller.call(transport.OpFIFOPush, server.PushRequest{Database: o.database, Bucket: bucket, Data: job}, &pushed); err != nil {
		return 0, fmt.Errorf("pushing a job onto %s: %w", bucket, err)
	}
	if !pushed.Success {
		return 0, fmt.Errorf("pushing a job onto %s: %s", bucket, pushed.Message)
	}
	var claimed server.ClaimResponse
	if err := o.caller.call(transport.OpQueueClaim, server.ClaimRequest{Database: o.database, Bucket: bucket, Limit: queueClaimSize, LeaseMs: queueLeaseMs}, &claimed); err != nil {
		return 0, fmt.Errorf("claiming from %s: %w", bucket, err)
	}
	if !claimed.Found {
		return 0, nil
	}
	var acked server.LeaseResponse
	if err := o.caller.call(transport.OpQueueAck, server.LeaseRequest{Database: o.database, Bucket: bucket, LeaseID: claimed.LeaseID, ItemIDs: claimed.ItemIDs}, &acked); err != nil {
		return 0, fmt.Errorf("acknowledging %d jobs of %s: %w", len(claimed.ItemIDs), bucket, err)
	}
	if !acked.Success {
		return 0, fmt.Errorf("acknowledging %d jobs of %s: lease %s: %s", len(claimed.ItemIDs), bucket, acked.LeaseState, acked.Message)
	}
	return len(claimed.ItemIDs), nil
}
