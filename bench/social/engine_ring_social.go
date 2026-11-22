package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The social operations on the entity-centric layout (ringload): a user is a document of
// the User bucket and a post a document of the Post bucket, and every collection is a store
// field of the document it belongs to. The publish is fan-out on write, as in the legacy
// and SQL engines: one copy of the post is pushed onto each follower's Feed. What differs
// from them, and why, is written beside the operation and in HARNESS.md ("Ring leg").

// ringPageSize is the page of the reads that must see a whole store: the largest one request returns.
const ringPageSize = 1000

// newestPostsReference is the element of a user's Posts store: a reference to the Post
// document, whose text is stored once.
type ringPostRef struct {
	Post    string    `bson:"post"`
	Seq     int64     `bson:"seq"`
	Created time.Time `bson:"created"`
}

// ringPostDoc is a Post document as Get returns it.
type ringPostDoc struct {
	Author  string    `bson:"Author"`
	Seq     int64     `bson:"Seq"`
	Text    string    `bson:"Text"`
	Created time.Time `bson:"Created"`
	Tags    []string  `bson:"Tags"`
	ReplyTo string    `bson:"ReplyTo"`
}

func (p ringPostDoc) record() postRecord {
	return postRecord{Author: p.Author, Seq: p.Seq, Text: p.Text, Created: p.Created, Tags: p.Tags, ReplyTo: p.ReplyTo}
}

// postDoc is a post as the Post document's fields; an absent tag list is written as an empty array.
func ringPostFields(post postRecord) ringclient.Doc {
	tags := post.Tags
	if tags == nil {
		tags = []string{}
	}
	return ringclient.Doc{"Author": post.Author, "Seq": post.Seq, "Text": post.Text, "Created": post.Created, "Tags": tags, "ReplyTo": post.ReplyTo}
}

// ringUserDoc is a User document as Get returns it: the scalar fields, never the stores.
type ringUserDoc struct {
	Name           string    `bson:"Name"`
	Bio            string    `bson:"Bio"`
	Joined         time.Time `bson:"Joined"`
	FollowerCount  int64     `bson:"FollowerCount"`
	FollowingCount int64     `bson:"FollowingCount"`
	Avatar         []byte    `bson:"Avatar"`
	Photo          []byte    `bson:"Photo"`
}

// fields is the document as the scalars an Upsert replaces.
func (u ringUserDoc) fields() ringclient.Doc {
	doc := ringclient.Doc{"Name": u.Name, "Bio": u.Bio, "Joined": u.Joined, "FollowerCount": u.FollowerCount, "FollowingCount": u.FollowingCount}
	if len(u.Avatar) > 0 {
		doc["Avatar"] = u.Avatar
	}
	if len(u.Photo) > 0 {
		doc["Photo"] = u.Photo
	}
	return doc
}

// The scalars each read consumes, named once. A projected read returns exactly these, so a field
// an operation starts using must be added here; the tests compare projected and whole answers.
var (
	// ringPostScalars is every scalar a postRecord carries.
	ringPostScalars = []string{"Author", "Seq", "Text", "Created", "Tags", "ReplyTo"}
	// ringProfileScalars is the profile's own fields; the counts come from the store sizes.
	ringProfileScalars = []string{"Name", "Bio", "Joined"}
	// ringUserScalars is every scalar of the User document: the avatar upload writes them all back.
	ringUserScalars = []string{"Name", "Bio", "Joined", "FollowerCount", "FollowingCount", "Avatar", "Photo"}
	// ringAvatarScalars is the one field the avatar read returns.
	ringAvatarScalars = []string{"Avatar"}
)

// readDocument reads a document's scalars: only the named ones with get_fields, or the whole
// document with get when the run is -ring-no-project.
func (o *ringOps) readDocument(ctx context.Context, bucket, id string, fields []string) (bson.Raw, error) {
	if o.wholeDocuments {
		return o.api.get(ctx, bucket, id)
	}
	return o.api.getFields(ctx, bucket, id, fields)
}

// ringEdgeElement is an element of Following or Followers: the user at the other end.
type ringEdgeElement struct {
	User string `bson:"user"`
}

// ringLikeElement is an element of a post's Likes.
type ringLikeElement struct {
	Liker string `bson:"liker"`
}

// ringOps is one worker's handle on the social operations over one ring connection.
type ringOps struct {
	api ringAPI
	// serial sends the independent requests of one operation one after another, as the other
	// engines do; otherwise they are in flight together on the connection (-ring-serial).
	serial bool
	// wholeDocuments reads a document with get instead of asking get_fields for the scalars the
	// operation uses (-ring-no-project); the zero value projects.
	wholeDocuments bool
	// fanoutWidth is how many feed pushes of one publish are in flight at once; 0 or 1 sends them
	// one after another (-ring-fanout-parallel).
	fanoutWidth int
	// batchSize is how many feed pushes of one publish go in one RING_BATCH frame; 0 sends one
	// request each (-ring-batch). batch collects what the frames did and is shared by the workers.
	batchSize int
	batch     *batchTotals
}

func (r *ringEngine) newSocialOps() (socialOps, error) {
	api, err := r.open(context.Background())
	if err != nil {
		return nil, fmt.Errorf("opening a connection for the social operations: %w", err)
	}
	ops := &ringOps{api: api, serial: r.cfg.RingSerial, wholeDocuments: r.cfg.RingNoProject}
	if r.cfg.RingFanoutParallel {
		ops.fanoutWidth = r.cfg.RingFanoutWidth
	}
	if r.cfg.RingBatch {
		r.batchOnce.Do(func() { r.batch = newBatchTotals(r.cfg.RingBatchSize) })
		ops.batchSize, ops.batch = r.cfg.RingBatchSize, r.batch
	}
	return ops, nil
}

func (o *ringOps) requests() int64 { return o.api.requests() }
func (o *ringOps) close()          { o.api.close() }

func userStore(user int, field string) storeRef {
	return storeRef{bucket: ringload.UserBucket, id: ringload.UserID(user), field: field}
}

func postStoreOf(author int, seq int64, field string) storeRef {
	return storeRef{bucket: ringload.PostBucket, id: ringload.PostID(author, int(seq)), field: field}
}

// gather runs call for 0..count-1, all at once on the connection unless the run is serial,
// and returns the first error after every call has finished.
func (o *ringOps) gather(count int, call func(index int) error) error {
	if o.serial || count < 2 {
		for index := range count {
			if err := call(index); err != nil {
				return err
			}
		}
		return nil
	}
	failures := make([]error, count)
	var group sync.WaitGroup
	for index := range count {
		group.Add(1)
		go func() {
			defer group.Done()
			failures[index] = call(index)
		}()
	}
	group.Wait()
	for _, failure := range failures {
		if failure != nil {
			return failure
		}
	}
	return nil
}

// listAll reads a store field whole, a page at a time, handing each element to take.
func (o *ringOps) listAll(ctx context.Context, store storeRef, take func(bson.Raw) error) error {
	var after []byte
	for {
		page, err := o.api.list(ctx, store, ringPageSize, after)
		if err != nil {
			return fmt.Errorf("listing %s of %s %s: %w", store.field, store.bucket, store.id, err)
		}
		for _, element := range page.Elements {
			if err := take(element); err != nil {
				return fmt.Errorf("decoding %s of %s %s: %w", store.field, store.bucket, store.id, err)
			}
		}
		if !page.More || len(page.Elements) == 0 {
			return nil
		}
		after = page.Cursor
	}
}

// userIDsOf lists the user ids held by the edges of a Following or Followers store.
func (o *ringOps) userIDsOf(ctx context.Context, store storeRef) ([]string, error) {
	var ids []string
	err := o.listAll(ctx, store, func(element bson.Raw) error {
		var edge ringEdgeElement
		if err := bson.Unmarshal(element, &edge); err != nil {
			return err
		}
		ids = append(ids, edge.User)
		return nil
	})
	return ids, err
}

// posts reads the Post documents the references name, all at once unless the run is serial.
func (o *ringOps) posts(ctx context.Context, refs []ringPostRef) ([]postRecord, error) {
	posts := make([]postRecord, len(refs))
	if len(refs) == 0 {
		return posts, nil
	}
	ids := make([]string, len(refs))
	for index, ref := range refs {
		ids[index] = ref.Post
	}
	// One frame reads the whole page: the page's posts are all in PostBucket, so the ten reads a
	// ten-post timeline used to make are one request (RING_GET_MANY), as ringconn's GetMany
	// splits only past the frame's key limit. The answers are not one snapshot, which the
	// per-document reads were not either.
	var fields []string
	if !o.wholeDocuments {
		fields = ringPostScalars
	}
	answers, err := o.api.getMany(ctx, ringload.PostBucket, ids, fields)
	if err != nil {
		return nil, fmt.Errorf("reading the %d posts of a page in one frame: %w", len(ids), err)
	}
	if len(answers) != len(ids) {
		return nil, fmt.Errorf("reading the %d posts of a page in one frame: %d answers came back", len(ids), len(answers))
	}
	for index, answer := range answers {
		if answer.Err != nil {
			return nil, fmt.Errorf("reading post %s: %w", ids[index], answer.Err)
		}
		if !answer.Found {
			return nil, fmt.Errorf("reading post %s: %w", ids[index], ringclient.ErrNotFound)
		}
		var doc ringPostDoc
		if err := bson.Unmarshal(answer.Doc, &doc); err != nil {
			return nil, fmt.Errorf("decoding post %s: %w", ids[index], err)
		}
		posts[index] = doc.record()
	}
	return posts, nil
}

// newestRefs lists the first limit references of a user's Posts store; the store's keys fall as
// the sequence rises, so the first are the newest.
func (o *ringOps) newestRefs(ctx context.Context, target, limit int) ([]ringPostRef, error) {
	page, err := o.api.list(ctx, userStore(target, ringload.UserPosts), limit, nil)
	if err != nil {
		return nil, fmt.Errorf("listing the posts of user %d: %w", target, err)
	}
	refs := make([]ringPostRef, len(page.Elements))
	for index, element := range page.Elements {
		if err := bson.Unmarshal(element, &refs[index]); err != nil {
			return nil, fmt.Errorf("decoding a post reference of user %d: %w", target, err)
		}
	}
	return refs, nil
}

func decodeFeed(elements []bson.Raw) ([]postRecord, error) {
	posts := make([]postRecord, len(elements))
	for index, element := range elements {
		if err := bson.Unmarshal(element, &posts[index]); err != nil {
			return nil, fmt.Errorf("decoding element %d: %w", index, err)
		}
	}
	return posts, nil
}

// homeTimeline (R1) is one peek: the feed holds full copies of the posts.
func (o *ringOps) homeTimeline(ctx context.Context, actor, limit int) ([]postRecord, error) {
	elements, err := o.api.peek(ctx, userStore(actor, ringload.UserFeed), limit)
	if err != nil {
		return nil, fmt.Errorf("reading the feed of user %d: %w", actor, err)
	}
	return decodeFeed(elements)
}

// profile (R2) is the User document and the two counts. The follower and following counts
// are the sizes of the Followers and Following stores, not the FollowerCount and
// FollowingCount scalars, because the ring wire has no atomic increment yet (a counter would
// be a read, a write and a lost update under concurrency); they are the truth at every moment.
func (o *ringOps) profile(ctx context.Context, target int) (userRecord, error) {
	var doc ringUserDoc
	var followers, following int64
	err := o.gather(3, func(index int) error {
		switch index {
		case 0:
			raw, err := o.readDocument(ctx, ringload.UserBucket, ringload.UserID(target), ringProfileScalars)
			if err != nil {
				return fmt.Errorf("reading user %d: %w", target, err)
			}
			if err := bson.Unmarshal(raw, &doc); err != nil {
				return fmt.Errorf("decoding user %d: %w", target, err)
			}
		case 1:
			count, err := o.api.count(ctx, userStore(target, ringload.UserFollowers))
			followers = count
			return wrapCount(err, "followers", target)
		default:
			count, err := o.api.count(ctx, userStore(target, ringload.UserFollowing))
			following = count
			return wrapCount(err, "following", target)
		}
		return nil
	})
	if err != nil {
		return userRecord{}, err
	}
	return userRecord{UserID: ringload.UserID(target), Name: doc.Name, Bio: doc.Bio, Joined: doc.Joined, FollowerCount: followers, FollowingCount: following}, nil
}

func wrapCount(err error, what string, user int) error {
	if err != nil {
		return fmt.Errorf("counting the %s of user %d: %w", what, user, err)
	}
	return nil
}

// profileTimeline (R3) lists the newest references of the author and reads those Post documents.
func (o *ringOps) profileTimeline(ctx context.Context, target, limit int) ([]postRecord, error) {
	refs, err := o.newestRefs(ctx, target, limit)
	if err != nil {
		return nil, err
	}
	return o.posts(ctx, refs)
}

// postPage (R4) is the Post document and the sizes of its Likes and Comments.
func (o *ringOps) postPage(ctx context.Context, author int, seq int64) (postPage, error) {
	ref := ringload.PostID(author, int(seq))
	raw, err := o.readDocument(ctx, ringload.PostBucket, ref, ringPostScalars)
	if errors.Is(err, ringclient.ErrNotFound) {
		return postPage{}, nil
	}
	if err != nil {
		return postPage{}, fmt.Errorf("reading post %s: %w", ref, err)
	}
	var doc ringPostDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return postPage{}, fmt.Errorf("decoding post %s: %w", ref, err)
	}
	page := postPage{Found: true, Post: doc.record()}
	err = o.gather(2, func(index int) error {
		if index == 0 {
			count, err := o.api.count(ctx, postStoreOf(author, seq, ringload.PostLikes))
			page.Likes = count
			return wrapCount(err, "likes of post "+ref+", user", author)
		}
		count, err := o.api.count(ctx, postStoreOf(author, seq, ringload.PostComments))
		page.Comments = count
		return wrapCount(err, "comments of post "+ref+", user", author)
	})
	return page, err
}

// followCheck (R5) is one keyed get of the edge in the actor's Following store, as the legacy
// engine does. An absent key is a user who is not followed, not an error.
func (o *ringOps) followCheck(ctx context.Context, actor, target int) (bool, error) {
	_, err := o.api.storeGet(ctx, userStore(actor, ringload.UserFollowing), ringload.UserID(target))
	if errors.Is(err, ringclient.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking whether user %d follows user %d: %w", actor, target, err)
	}
	return true, nil
}

// followingList (R6) is one page of the Following store.
func (o *ringOps) followingList(ctx context.Context, actor, limit int) ([]string, error) {
	page, err := o.api.list(ctx, userStore(actor, ringload.UserFollowing), limit, nil)
	if err != nil {
		return nil, fmt.Errorf("listing who user %d follows: %w", actor, err)
	}
	followed := make([]string, len(page.Elements))
	for index, element := range page.Elements {
		var edge ringEdgeElement
		if err := bson.Unmarshal(element, &edge); err != nil {
			return nil, fmt.Errorf("decoding who user %d follows: %w", actor, err)
		}
		followed[index] = edge.User
	}
	return followed, nil
}

// avatar (R7) is the User document, whose Avatar scalar travels with the profile fields.
func (o *ringOps) avatar(ctx context.Context, target int) ([]byte, error) {
	raw, err := o.readDocument(ctx, ringload.UserBucket, ringload.UserID(target), ringAvatarScalars)
	if err != nil {
		return nil, fmt.Errorf("reading the avatar of user %d: %w", target, err)
	}
	var doc ringUserDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decoding user %d: %w", target, err)
	}
	if len(doc.Avatar) == 0 {
		return nil, fmt.Errorf("user %d: %w", target, errNoAvatar)
	}
	return doc.Avatar, nil
}

// readNotifications (N1) takes up to limit notifications off the Inbox: a claim and an ack,
// two requests. A pop of up to 10 exists on the wire now, but its elements must fit the 1 KiB a
// dedupe entry holds and ten notifications of about 100 bytes do not reliably, so the leg keeps
// claim and ack, which carry no such limit.
func (o *ringOps) readNotifications(ctx context.Context, actor, limit int) (int, error) {
	inbox := userStore(actor, ringload.UserInbox)
	lease, err := o.api.claim(ctx, inbox, limit)
	if err != nil {
		return 0, fmt.Errorf("claiming the inbox of user %d: %w", actor, err)
	}
	if len(lease.Elements) == 0 {
		return 0, nil
	}
	if err := o.api.ack(ctx, inbox, lease.ID); err != nil {
		return 0, fmt.Errorf("acknowledging %d notifications of user %d: %w", len(lease.Elements), actor, err)
	}
	return len(lease.Elements), nil
}

// notify pushes one notification onto a user's Inbox.
func (o *ringOps) notify(ctx context.Context, user int, kind string, actor int, subject string) error {
	entry := bson.D{{Key: "type", Value: kind}, {Key: "actor", Value: ringload.UserID(actor)}, {Key: "target", Value: subject}, {Key: "ts", Value: time.Now().UTC()}}
	if err := o.api.push(ctx, userStore(user, ringload.UserInbox), entry); err != nil {
		return fmt.Errorf("notifying user %d of a %s: %w", user, kind, err)
	}
	return nil
}

// publishPost (W1) is fan-out on write. It lists the author's followers, stores the Post
// document, indexes it in the author's Posts store, then pushes a copy onto each follower's
// Feed, one request after another. That is the legacy engine's sequence plus the one request
// that stores the Post document, which the legacy engine keeps inside the Posts element.
func (o *ringOps) publishPost(ctx context.Context, post postRecord, ledger deliveryLedger) (int, error) {
	author, err := strconv.Atoi(post.Author)
	if err != nil {
		return 0, fmt.Errorf("post author %q is not a user id: %w", post.Author, err)
	}
	followerIDs, err := o.userIDsOf(ctx, userStore(author, ringload.UserFollowers))
	if err != nil {
		return 0, err
	}
	ref := ringload.PostID(author, int(post.Seq))
	if err := o.api.insert(ctx, ringload.PostBucket, ref, ringPostFields(post)); err != nil {
		return 0, fmt.Errorf("storing post %d of user %d: %w", post.Seq, author, err)
	}
	index := bson.D{{Key: "post", Value: ref}, {Key: "seq", Value: post.Seq}, {Key: "created", Value: post.Created}}
	if err := o.api.put(ctx, userStore(author, ringload.UserPosts), ringload.PostKey(int(post.Seq)), index); err != nil {
		return 0, fmt.Errorf("indexing post %d of user %d: %w", post.Seq, author, err)
	}
	followers := make([]int, len(followerIDs))
	for position, followerID := range followerIDs {
		follower, err := strconv.Atoi(followerID)
		if err != nil {
			return 0, fmt.Errorf("follower key %q of user %d is not a user id: %w", followerID, author, err)
		}
		followers[position] = follower
	}
	if err := o.pushFeeds(ctx, followers, post.document(), ledger); err != nil {
		return 0, fmt.Errorf("copying post %d of user %d: %w", post.Seq, author, err)
	}
	return len(followerIDs), nil
}

// likePost (W2) puts the like in the Post document's Likes and notifies the author.
func (o *ringOps) likePost(ctx context.Context, liker, author int, seq int64) error {
	like := bson.D{{Key: "liker", Value: ringload.UserID(liker)}, {Key: "ts", Value: time.Now().UTC()}}
	if err := o.api.put(ctx, postStoreOf(author, seq, ringload.PostLikes), ringload.UserID(liker), like); err != nil {
		return fmt.Errorf("user %d liking post %d of user %d: %w", liker, seq, author, err)
	}
	return o.notify(ctx, author, "like", liker, fmt.Sprintf("%d/%s", author, postKey(seq)))
}

// follow (W3) puts both edges and notifies the target. It writes no counter: the profile's
// counts are the store sizes (see profile), so there is nothing to keep in step.
func (o *ringOps) follow(ctx context.Context, actor, target int) error {
	doing := fmt.Sprintf("user %d following user %d", actor, target)
	since := time.Now().UTC()
	out := bson.D{{Key: "user", Value: ringload.UserID(target)}, {Key: "since", Value: since}}
	in := bson.D{{Key: "user", Value: ringload.UserID(actor)}, {Key: "since", Value: since}}
	if err := o.api.put(ctx, userStore(actor, ringload.UserFollowing), ringload.UserID(target), out); err != nil {
		return fmt.Errorf("%s: %w", doing, err)
	}
	if err := o.api.put(ctx, userStore(target, ringload.UserFollowers), ringload.UserID(actor), in); err != nil {
		return fmt.Errorf("%s: %w", doing, err)
	}
	return o.notify(ctx, target, "follow", actor, ringload.UserID(target))
}

// unfollow (W4) removes both edges; an edge that is not there is already the wanted state.
func (o *ringOps) unfollow(ctx context.Context, actor, target int) error {
	doing := fmt.Sprintf("user %d unfollowing user %d", actor, target)
	if err := o.api.take(ctx, userStore(actor, ringload.UserFollowing), ringload.UserID(target)); err != nil && !errors.Is(err, ringclient.ErrNotFound) {
		return fmt.Errorf("%s: %w", doing, err)
	}
	if err := o.api.take(ctx, userStore(target, ringload.UserFollowers), ringload.UserID(actor)); err != nil && !errors.Is(err, ringclient.ErrNotFound) {
		return fmt.Errorf("%s: %w", doing, err)
	}
	return nil
}

// commentOnPost (W5) pushes the comment onto the Post document's Comments and notifies the author.
func (o *ringOps) commentOnPost(ctx context.Context, commenter, author int, seq int64, text string) error {
	comment := bson.D{{Key: "author", Value: ringload.UserID(commenter)}, {Key: "text", Value: text}, {Key: "ts", Value: time.Now().UTC()}}
	if err := o.api.push(ctx, postStoreOf(author, seq, ringload.PostComments), comment); err != nil {
		return fmt.Errorf("user %d commenting on post %d of user %d: %w", commenter, seq, author, err)
	}
	return o.notify(ctx, author, "comment", commenter, fmt.Sprintf("%d/%s", author, postKey(seq)))
}

// uploadAvatar (W6) reads the User document and writes it back with the new Avatar: an upsert
// replaces all scalar fields of a document, so the other fields are carried over. Two
// requests, where the legacy engine does one blob put.
func (o *ringOps) uploadAvatar(ctx context.Context, user int, content []byte) error {
	id := ringload.UserID(user)
	raw, err := o.readDocument(ctx, ringload.UserBucket, id, ringUserScalars)
	if err != nil {
		return fmt.Errorf("reading user %d to replace its avatar: %w", user, err)
	}
	var doc ringUserDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("decoding user %d: %w", user, err)
	}
	doc.Avatar = content
	if err := o.api.upsert(ctx, ringload.UserBucket, id, doc.fields()); err != nil {
		return fmt.Errorf("uploading the avatar of user %d: %w", user, err)
	}
	return nil
}

// mergedTimeline (M1) lists the newest references of every followee, keeps the newest 20 by
// creation time, and reads only those Post documents.
func (o *ringOps) mergedTimeline(ctx context.Context, followees []int) ([]postRecord, error) {
	lists := make([][]ringPostRef, len(followees))
	err := o.gather(len(followees), func(index int) error {
		refs, err := o.newestRefs(ctx, followees[index], mergedPostsTotal)
		lists[index] = refs
		return err
	})
	if err != nil {
		return nil, err
	}
	var merged []ringPostRef
	for _, refs := range lists {
		merged = append(merged, refs...)
	}
	slices.SortStableFunc(merged, func(a, b ringPostRef) int { return b.Created.Compare(a.Created) })
	return o.posts(ctx, merged[:min(len(merged), mergedPostsTotal)])
}

// queueRound (Q1) pushes a job onto an Outbox document's Jobs, claims up to ten and acks them.
func (o *ringOps) queueRound(ctx context.Context, outbox int) (int, error) {
	jobs := storeRef{bucket: ringload.OutboxBucket, id: strconv.Itoa(outbox), field: ringload.OutboxJobs}
	job := bson.D{{Key: "job", Value: time.Now().UnixNano()}, {Key: "outbox", Value: int64(outbox)}}
	if err := o.api.push(ctx, jobs, job); err != nil {
		return 0, fmt.Errorf("pushing a job onto outbox %d: %w", outbox, err)
	}
	lease, err := o.api.claim(ctx, jobs, queueClaimSize)
	if err != nil {
		return 0, fmt.Errorf("claiming from outbox %d: %w", outbox, err)
	}
	if len(lease.Elements) == 0 {
		return 0, nil
	}
	if err := o.api.ack(ctx, jobs, lease.ID); err != nil {
		return 0, fmt.Errorf("acknowledging %d jobs of outbox %d: %w", len(lease.Elements), outbox, err)
	}
	return len(lease.Elements), nil
}
