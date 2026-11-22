package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// seededCommentText is the text of every comment the seeder writes.
const seededCommentText = "seeded comment"

// The canonical form of a user (spec P4): what two engines must agree on after
// seeding, with everything that differs by construction taken out. hoardDB
// mints an `_id_` on every stored element, and the fan-out of concurrent posts
// by different authors may interleave differently on each engine, so the form
// drops the ids, groups a feed or an inbox by author, and compares only each
// author's own subsequence.

// rawNote is one inbox entry as an engine returns it.
type rawNote struct {
	ID     string    `bson:"_id_,omitempty"`
	Type   string    `bson:"type"`
	Actor  string    `bson:"actor"`
	Target string    `bson:"target"`
	TS     time.Time `bson:"ts"`
}

// rawComment is one comment as an engine returns it.
type rawComment struct {
	ID     string    `bson:"_id_,omitempty"`
	Author string    `bson:"author"`
	Text   string    `bson:"text"`
	TS     time.Time `bson:"ts"`
}

// rawPost is a post with its likers and comments.
type rawPost struct {
	Post     postRecord
	Likers   []string
	Comments []rawComment
}

// rawUser is everything an engine holds for one user, as read.
type rawUser struct {
	Profile   userRecord
	Posts     []rawPost
	Feed      []postRecord // in stored order, newest first
	Inbox     []rawNote
	Following []string
	Followers []string
	Media     map[string][]byte
}

// canonicalPost is a post as the form keeps it.
type canonicalPost struct {
	Seq      int64
	Text     string
	Created  time.Time
	Tags     []string
	ReplyTo  string
	Likers   []string
	Comments []string
}

// canonicalRun is one author's entries of a feed, in stored order.
type canonicalRun struct {
	Author  string
	Entries []canonicalPost
}

// canonicalNote is an inbox entry without its id.
type canonicalNote struct {
	Type, Actor, Target string
	TS                  time.Time
}

// canonicalUser is the comparable form of a user.
type canonicalUser struct {
	ID             string
	Name, Bio      string
	Joined         time.Time
	FollowerCount  int64
	FollowingCount int64
	Posts          []canonicalPost
	Feed           []canonicalRun
	Inbox          []canonicalNote
	Following      []string
	Followers      []string
	Media          map[string]string
}

// canonicalPostOf is a post without its id, in UTC, with an empty tag list for
// none: the server decodes "no tags" as nil and the generator makes an empty
// slice, and the form must not tell them apart.
func canonicalPostOf(post postRecord) canonicalPost {
	tags := slices.Clone(post.Tags)
	if tags == nil {
		tags = []string{}
	}
	return canonicalPost{Seq: post.Seq, Text: post.Text, Created: post.Created.UTC(), Tags: tags, ReplyTo: post.ReplyTo, Likers: []string{}, Comments: []string{}}
}

// canonicalize puts a user in canonical form: ids dropped, unordered sets
// sorted, and the feed grouped by author with each author's entries in the
// order they are stored, because that order is content and the interleaving
// between authors is not.
func canonicalize(raw rawUser) canonicalUser {
	user := canonicalUser{
		ID: raw.Profile.UserID, Name: raw.Profile.Name, Bio: raw.Profile.Bio, Joined: raw.Profile.Joined.UTC(),
		FollowerCount: raw.Profile.FollowerCount, FollowingCount: raw.Profile.FollowingCount,
		Posts: []canonicalPost{}, Feed: []canonicalRun{}, Inbox: []canonicalNote{},
		Following: sortedCopy(raw.Following), Followers: sortedCopy(raw.Followers), Media: map[string]string{},
	}
	for _, post := range raw.Posts {
		canonical := canonicalPostOf(post.Post)
		canonical.Likers = sortedCopy(post.Likers)
		for _, comment := range post.Comments {
			canonical.Comments = append(canonical.Comments, comment.Author+"|"+comment.Text+"|"+comment.TS.UTC().Format(time.RFC3339Nano))
		}
		slices.Sort(canonical.Comments)
		user.Posts = append(user.Posts, canonical)
	}
	slices.SortFunc(user.Posts, func(a, b canonicalPost) int { return cmp.Compare(a.Seq, b.Seq) })
	runs := map[string]*canonicalRun{}
	for _, entry := range raw.Feed {
		if runs[entry.Author] == nil {
			runs[entry.Author] = &canonicalRun{Author: entry.Author}
		}
		runs[entry.Author].Entries = append(runs[entry.Author].Entries, canonicalPostOf(entry))
	}
	for _, author := range slices.Sorted(maps.Keys(runs)) {
		user.Feed = append(user.Feed, *runs[author])
	}
	for _, note := range raw.Inbox {
		user.Inbox = append(user.Inbox, canonicalNote{Type: note.Type, Actor: note.Actor, Target: note.Target, TS: note.TS.UTC()})
	}
	slices.SortFunc(user.Inbox, func(a, b canonicalNote) int {
		return cmp.Or(cmp.Compare(a.Actor, b.Actor), cmp.Compare(a.Type, b.Type), cmp.Compare(a.Target, b.Target), a.TS.Compare(b.TS))
	})
	for kind, content := range raw.Media {
		digest := sha256.Sum256(content)
		user.Media[kind] = hex.EncodeToString(digest[:])
	}
	return user
}

func sortedCopy(values []string) []string {
	sorted := slices.Clone(values)
	if sorted == nil {
		sorted = []string{}
	}
	slices.Sort(sorted)
	return sorted
}

// canonicalHash is the hash of a sample of users in canonical form, whatever
// order the sample is given in.
func canonicalHash(users []canonicalUser) (string, error) {
	ordered := slices.Clone(users)
	slices.SortFunc(ordered, func(a, b canonicalUser) int { return cmp.Compare(a.ID, b.ID) })
	digest := sha256.New()
	for _, user := range ordered {
		encoded, err := json.Marshal(user)
		if err != nil {
			return "", fmt.Errorf("encoding user %s for the parity hash: %w", user.ID, err)
		}
		digest.Write(append(encoded, '\n'))
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// sampleUsers picks n distinct user ranks out of users, the same ones for the
// same seed, so every engine is compared on the same sample. They come back in
// rank order.
func sampleUsers(users, n int, seed uint64) []int {
	picked := rand.New(rand.NewPCG(seed, 0x5a3b)).Perm(users)[:min(n, users)]
	slices.Sort(picked)
	return picked
}

// graphCanonical is the canonical form the generator says the seeded users
// must have: the reference an engine's dataset is compared against.
func graphCanonical(graph *socialGraph, ranks []int) []canonicalUser {
	wanted := make(map[int]bool, len(ranks))
	for _, rank := range ranks {
		wanted[rank] = true
	}
	type postKeyed struct{ author, seq int }
	likers, comments := map[postKeyed][]string{}, map[postKeyed][]rawComment{}
	for like := range graph.likes() {
		if wanted[like.PostAuthor] {
			key := postKeyed{like.PostAuthor, like.PostSeq}
			likers[key] = append(likers[key], userID(like.Actor))
		}
	}
	for comment := range graph.comments() {
		if wanted[comment.PostAuthor] {
			key := postKeyed{comment.PostAuthor, comment.PostSeq}
			comments[key] = append(comments[key], rawComment{Author: userID(comment.Actor), Text: seededCommentText, TS: graphEpoch})
		}
	}
	media := map[int]map[string][]byte{}
	for item := range graph.media() {
		if !wanted[item.Owner] {
			continue
		}
		if media[item.Owner] == nil {
			media[item.Owner] = map[string][]byte{}
		}
		media[item.Owner][mediaName(item.Kind)] = graph.content(item)
	}
	followerOffsets, followers := followerLists(graph)
	var canonical []canonicalUser
	for user := range graph.users() {
		if !wanted[user.Rank] {
			continue
		}
		raw := rawUser{
			Profile: userRecord{UserID: user.Profile.ID, Name: user.Profile.Name, Bio: user.Profile.Bio, Joined: user.Profile.Joined, FollowerCount: user.Profile.Followers, FollowingCount: user.Profile.Following},
			Media:   media[user.Rank],
		}
		for seq := range graph.postsPerUser {
			key := postKeyed{user.Rank, seq}
			raw.Posts = append(raw.Posts, rawPost{Post: recordOf(graph.post(user.Rank, seq)), Likers: dedupe(likers[key]), Comments: comments[key]})
		}
		for _, ref := range slices.Backward(user.Feed) {
			raw.Feed = append(raw.Feed, recordOf(graph.post(int(ref.Author), int(ref.Seq))))
		}
		for _, note := range user.Inbox {
			raw.Inbox = append(raw.Inbox, rawNote{Type: note.Kind, Actor: userID(note.Actor), Target: user.Profile.ID, TS: graphEpoch})
		}
		for _, followee := range user.Following {
			raw.Following = append(raw.Following, userID(int(followee)))
		}
		for _, follower := range followers[followerOffsets[user.Rank]:followerOffsets[user.Rank+1]] {
			raw.Followers = append(raw.Followers, userID(int(follower)))
		}
		canonical = append(canonical, canonicalize(raw))
	}
	return canonical
}

// dedupe drops repeats: a like is keyed by its liker, so the same user liking
// a post twice is one like.
func dedupe(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}

// mediaName is the element key a blob is stored under.
func mediaName(kind mediaKind) string {
	if kind == mediaPhoto {
		return "photo"
	}
	return "avatar"
}

// listEverything reads every element of a store field, a page at a time.
func (o *hoardOps) listEverything(request server.FieldRequest, doing string) ([]server.FieldListEntry, error) {
	var everything []server.FieldListEntry
	var after bson.RawValue
	for {
		var page server.FieldListResponse
		if err := o.caller.call(transport.OpFieldList, server.FieldListRequest{FieldRequest: request, After: after, Limit: canonicalPageSize}, &page); err != nil {
			return nil, fmt.Errorf("%s: %w", doing, err)
		}
		if !page.Success {
			return nil, fmt.Errorf("%s: %s", doing, page.Message)
		}
		everything = append(everything, page.Entries...)
		if !page.HasMore || len(page.Entries) == 0 {
			return everything, nil
		}
		cursor := any(page.Entries[len(page.Entries)-1].ElementKey)
		if len(page.Next) > 0 {
			cursor = page.Next
		}
		valueType, value, err := bson.MarshalValue(cursor)
		if err != nil {
			return nil, fmt.Errorf("%s: encoding the page cursor: %w", doing, err)
		}
		after = bson.RawValue{Type: valueType, Value: value}
	}
}

// canonicalPageSize is the page of the reads that must see a whole store.
const canonicalPageSize = 1000

// peekEverything reads a queue-like store field whole without taking anything
// from it. A peek has no cursor, so a full page cannot be proved to be the
// whole store and is an error.
func (o *hoardOps) peekEverything(request server.FieldRequest, doing string) ([]server.FieldListEntry, error) {
	reply, err := o.fieldCall(transport.OpFieldPeek, server.FieldPeekRequest{FieldRequest: request, Limit: canonicalPageSize}, doing)
	if err != nil {
		return nil, err
	}
	if len(reply.Items) == canonicalPageSize {
		return nil, fmt.Errorf("%s: %d entries is the most one peek returns, so the store may hold more; compare a sample of users whose stores are smaller", doing, canonicalPageSize)
	}
	return reply.Items, nil
}

// canonicalUser reads one user's whole content from the server.
func (o *hoardOps) canonicalUser(ctx context.Context, rank int) (canonicalUser, error) {
	profile, err := o.profile(ctx, rank)
	if err != nil {
		return canonicalUser{}, err
	}
	raw := rawUser{Profile: profile, Media: map[string][]byte{}}
	posts, err := o.listEverything(o.field(rank, "Posts"), fmt.Sprintf("listing the posts of user %d", rank))
	if err != nil {
		return canonicalUser{}, err
	}
	for _, entry := range posts {
		var post rawPost
		if err := bson.Unmarshal(entry.Element, &post.Post); err != nil {
			return canonicalUser{}, fmt.Errorf("decoding a post of user %d: %w", rank, err)
		}
		if post.Likers, err = o.likerIDs(rank, post.Post.Seq); err != nil {
			return canonicalUser{}, err
		}
		if post.Comments, err = o.commentsOf(rank, post.Post.Seq); err != nil {
			return canonicalUser{}, err
		}
		raw.Posts = append(raw.Posts, post)
	}
	feed, err := o.peekEverything(o.field(rank, "Feed"), fmt.Sprintf("reading the feed of user %d", rank))
	if err != nil {
		return canonicalUser{}, err
	}
	if raw.Feed, err = decodePosts(feed); err != nil {
		return canonicalUser{}, fmt.Errorf("decoding the feed of user %d: %w", rank, err)
	}
	inbox, err := o.peekEverything(o.field(rank, "Inbox"), fmt.Sprintf("reading the inbox of user %d", rank))
	if err != nil {
		return canonicalUser{}, err
	}
	for _, entry := range inbox {
		var note rawNote
		if err := bson.Unmarshal(entry.Element, &note); err != nil {
			return canonicalUser{}, fmt.Errorf("decoding the inbox of user %d: %w", rank, err)
		}
		raw.Inbox = append(raw.Inbox, note)
	}
	for field, into := range map[string]*[]string{"Following": &raw.Following, "Followers": &raw.Followers} {
		entries, err := o.listEverything(o.field(rank, field), fmt.Sprintf("listing %s of user %d", strings.ToLower(field), rank))
		if err != nil {
			return canonicalUser{}, err
		}
		for _, entry := range entries {
			*into = append(*into, entry.ElementKey)
		}
	}
	for _, kind := range []string{"avatar", "photo"} {
		request := o.field(rank, "Media")
		request.Element = kind
		got, err := o.fieldCall(transport.OpFieldGet, request, fmt.Sprintf("reading the %s of user %d", kind, rank))
		if err != nil {
			return canonicalUser{}, err
		}
		if got.Found {
			raw.Media[kind] = got.Content
		}
	}
	return canonicalize(raw), nil
}

func (o *hoardOps) likerIDs(author int, seq int64) ([]string, error) {
	entries, err := o.listEverything(o.postStore(author, seq, "Likes"), fmt.Sprintf("listing the likes of post %d of user %d", seq, author))
	if err != nil {
		return nil, err
	}
	likers := make([]string, len(entries))
	for i, entry := range entries {
		likers[i] = entry.ElementKey
	}
	return likers, nil
}

func (o *hoardOps) commentsOf(author int, seq int64) ([]rawComment, error) {
	entries, err := o.listEverything(o.postStore(author, seq, "Comments"), fmt.Sprintf("listing the comments of post %d of user %d", seq, author))
	if err != nil {
		return nil, err
	}
	comments := make([]rawComment, len(entries))
	for i, entry := range entries {
		if err := bson.Unmarshal(entry.Element, &comments[i]); err != nil {
			return nil, fmt.Errorf("decoding a comment of post %d of user %d: %w", seq, author, err)
		}
	}
	return comments, nil
}
