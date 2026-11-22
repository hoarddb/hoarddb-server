package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The fixture is three users in the model. User 0 is an author with three posts (0:0, 0:1, 0:2)
// and the followers 1 and 2; user 1 follows 0 and has twelve notifications and a feed of two;
// user 2 follows 0 and 1 and owns an avatar and a photo.
var ringFixtureEpoch = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func ringFixture(t *testing.T) *ringModel {
	t.Helper()
	ctx := context.Background()
	model := newRingModel()
	conn := model.connect()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	user := func(id string, extra ringclient.Doc) {
		doc := ringclient.Doc{"Name": "user " + id, "Bio": "bio " + id, "Joined": ringFixtureEpoch, "FollowerCount": int64(0), "FollowingCount": int64(0)}
		for key, value := range extra {
			doc[key] = value
		}
		must(conn.insert(ctx, ringload.UserBucket, id, doc))
	}
	user("0", nil)
	user("1", nil)
	user("2", ringclient.Doc{"Avatar": []byte("avatar-bytes"), "Photo": []byte("photo-bytes")})
	for seq := range 3 {
		ref := ringload.PostID(0, seq)
		created := ringFixtureEpoch.Add(time.Duration(seq) * time.Hour)
		must(conn.insert(ctx, ringload.PostBucket, ref, ringclient.Doc{"Author": "0", "Seq": int64(seq), "Text": fmt.Sprintf("post %d", seq), "Created": created, "Tags": []string{"t"}, "ReplyTo": ""}))
		must(conn.put(ctx, userStore(0, ringload.UserPosts), ringload.PostKey(seq), map[string]any{"post": ref, "seq": int64(seq), "created": created}))
	}
	edge := func(owner, field string, other string) {
		id, _ := parseRank(owner)
		must(conn.put(ctx, userStore(id, field), other, map[string]any{"user": other, "since": ringFixtureEpoch}))
	}
	edge("0", ringload.UserFollowers, "1")
	edge("0", ringload.UserFollowers, "2")
	edge("1", ringload.UserFollowing, "0")
	edge("2", ringload.UserFollowing, "0")
	edge("2", ringload.UserFollowing, "1")
	edge("1", ringload.UserFollowers, "2")
	for seq := range 2 {
		must(conn.push(ctx, userStore(1, ringload.UserFeed), map[string]any{"author": "0", "seq": int64(seq), "text": fmt.Sprintf("post %d", seq), "created": ringFixtureEpoch.Add(time.Duration(seq) * time.Hour), "tags": []string{"t"}, "reply_to": ""}))
	}
	for note := range 12 {
		must(conn.push(ctx, userStore(1, ringload.UserInbox), map[string]any{"type": "like", "actor": fmt.Sprint(note), "target": "1", "ts": ringFixtureEpoch}))
	}
	must(conn.push(ctx, userStore(0, ringload.UserInbox), map[string]any{"type": "follow", "actor": "1", "target": "0", "ts": ringFixtureEpoch}))
	must(conn.insert(ctx, ringload.OutboxBucket, "5", ringclient.Doc{"Name": "outbox 5"}))
	model.clearLog()
	return model
}

func parseRank(text string) (int, error) {
	var rank int
	_, err := fmt.Sscan(text, &rank)
	return rank, err
}

// ringStep is one social operation and the requests it must send, in order when the run is serial.
type ringStep struct {
	name string
	run  func(ctx context.Context, ops *ringOps) error
	want []string
}

type noteLedger struct{ sent, done []int }

func (l *noteLedger) sending(follower int)   { l.sent = append(l.sent, follower) }
func (l *noteLedger) delivered(follower int) { l.done = append(l.done, follower) }

func ringSteps() []ringStep {
	return []ringStep{
		{"R1 home timeline", func(ctx context.Context, o *ringOps) error { _, err := o.homeTimeline(ctx, 1, 20); return err }, []string{"peek User/1/Feed"}},
		{"R2 profile", func(ctx context.Context, o *ringOps) error { _, err := o.profile(ctx, 0); return err },
			[]string{"get-fields User/0", "count User/0/Followers", "count User/0/Following"}},
		{"R3 profile timeline", func(ctx context.Context, o *ringOps) error { _, err := o.profileTimeline(ctx, 0, 2); return err },
			[]string{"list User/0/Posts", "get-many Post/0:2,0:1"}},
		{"R4 post page", func(ctx context.Context, o *ringOps) error { _, err := o.postPage(ctx, 0, 1); return err },
			[]string{"get-fields Post/0:1", "count Post/0:1/Likes", "count Post/0:1/Comments"}},
		{"R5 follow check", func(ctx context.Context, o *ringOps) error { _, err := o.followCheck(ctx, 2, 1); return err }, []string{"get-key User/2/Following"}},
		{"R6 following list", func(ctx context.Context, o *ringOps) error { _, err := o.followingList(ctx, 2, 50); return err }, []string{"list User/2/Following"}},
		{"R7 avatar", func(ctx context.Context, o *ringOps) error { _, err := o.avatar(ctx, 2); return err }, []string{"get-fields User/2"}},
		{"N1 notifications", func(ctx context.Context, o *ringOps) error { _, err := o.readNotifications(ctx, 1, 10); return err },
			[]string{"claim User/1/Inbox", "ack User/1/Inbox"}},
		{"W1 publish", func(ctx context.Context, o *ringOps) error {
			_, err := o.publishPost(ctx, postRecord{Author: "0", Seq: 3, Text: "post 3", Created: ringFixtureEpoch.Add(3 * time.Hour), Tags: []string{"t"}}, noLedger{})
			return err
		}, []string{"list User/0/Followers", "insert Post/0:3", "put User/0/Posts", "push User/1/Feed", "push User/2/Feed"}},
		{"W2 like", func(ctx context.Context, o *ringOps) error { return o.likePost(ctx, 1, 0, 1) }, []string{"put Post/0:1/Likes", "push User/0/Inbox"}},
		{"W3 follow", func(ctx context.Context, o *ringOps) error { return o.follow(ctx, 1, 2) },
			[]string{"put User/1/Following", "put User/2/Followers", "push User/2/Inbox"}},
		{"W4 unfollow", func(ctx context.Context, o *ringOps) error { return o.unfollow(ctx, 2, 1) }, []string{"take User/2/Following", "take User/1/Followers"}},
		{"W5 comment", func(ctx context.Context, o *ringOps) error { return o.commentOnPost(ctx, 1, 0, 1, "hi") },
			[]string{"push Post/0:1/Comments", "push User/0/Inbox"}},
		{"W6 upload avatar", func(ctx context.Context, o *ringOps) error { return o.uploadAvatar(ctx, 2, []byte("new")) }, []string{"get-fields User/2", "upsert User/2"}},
		{"M1 merged timeline", func(ctx context.Context, o *ringOps) error { _, err := o.mergedTimeline(ctx, []int{0, 1}); return err },
			[]string{"list User/0/Posts", "list User/1/Posts", "get-many Post/0:2,0:1,0:0"}},
		{"Q1 queue round", func(ctx context.Context, o *ringOps) error { _, err := o.queueRound(ctx, 5); return err },
			[]string{"push Outbox/5/Jobs", "claim Outbox/5/Jobs", "ack Outbox/5/Jobs"}},
	}
}

func ringOpsOn(model *ringModel, serial bool) *ringOps {
	return &ringOps{api: model.connect(), serial: serial}
}

// TestRingOperationsSendTheDocumentedRequests: each social operation sends exactly the requests
// HARNESS.md lists, in order when the run is serial, and the connection counts one per request.
func TestRingOperationsSendTheDocumentedRequests(t *testing.T) {
	for _, step := range ringSteps() {
		t.Run(step.name, func(t *testing.T) {
			model := ringFixture(t)
			ops := ringOpsOn(model, true)
			if err := step.run(t.Context(), ops); err != nil {
				t.Fatal(err)
			}
			if got := model.requestLog(); !slices.Equal(got, step.want) {
				t.Errorf("requests %q, want %q", got, step.want)
			}
			if ops.requests() != int64(len(step.want)) {
				t.Errorf("the handle counted %d requests, want %d", ops.requests(), len(step.want))
			}
		})
	}
}

// TestRingPipelinedOperationsSendTheSameRequests: letting the independent requests of an
// operation fly together changes the order they are sent in and nothing else.
func TestRingPipelinedOperationsSendTheSameRequests(t *testing.T) {
	for _, step := range ringSteps() {
		t.Run(step.name, func(t *testing.T) {
			model := ringFixture(t)
			if err := step.run(t.Context(), ringOpsOn(model, false)); err != nil {
				t.Fatal(err)
			}
			got, want := model.requestLog(), slices.Clone(step.want)
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("requests %q, want the same set as %q", got, want)
			}
		})
	}
}

// TestRingOperationsReturnWhatTheOtherEnginesReturn: the answers are the legacy engine's, so the
// verifier and the canonical hash read them the same way.
func TestRingOperationsReturnWhatTheOtherEnginesReturn(t *testing.T) {
	ctx := context.Background()
	for _, serial := range []bool{true, false} {
		t.Run(fmt.Sprintf("serial=%v", serial), func(t *testing.T) {
			model := ringFixture(t)
			ops := ringOpsOn(model, serial)

			timeline, err := ops.profileTimeline(ctx, 0, 20)
			if err != nil || len(timeline) != 3 || timeline[0].Seq != 2 || timeline[2].Seq != 0 || timeline[0].Text != "post 2" || timeline[0].Author != "0" {
				t.Fatalf("profile timeline %+v %v: want the three posts, newest first", timeline, err)
			}
			feed, err := ops.homeTimeline(ctx, 1, 20)
			if err != nil || len(feed) != 2 || feed[0].Seq != 1 {
				t.Fatalf("home timeline %+v %v: want two copies, newest first", feed, err)
			}
			profile, err := ops.profile(ctx, 0)
			if err != nil || profile.UserID != "0" || profile.Name != "user 0" || profile.FollowerCount != 2 || profile.FollowingCount != 0 || !profile.Joined.Equal(ringFixtureEpoch) {
				t.Fatalf("profile %+v %v", profile, err)
			}
			if follows, err := ops.followCheck(ctx, 2, 1); err != nil || !follows {
				t.Fatalf("user 2 follows user 1: %v %v", follows, err)
			}
			if follows, err := ops.followCheck(ctx, 1, 2); err != nil || follows {
				t.Fatalf("user 1 does not follow user 2: %v %v", follows, err)
			}
			if followed, err := ops.followingList(ctx, 2, 50); err != nil || !slices.Equal(followed, []string{"0", "1"}) {
				t.Fatalf("following of user 2: %v %v", followed, err)
			}

			if err := ops.follow(ctx, 1, 2); err != nil {
				t.Fatal(err)
			}
			if profile, _ := ops.profile(ctx, 2); profile.FollowerCount != 1 {
				t.Errorf("after user 1 followed user 2 the follower count is %d, want 1", profile.FollowerCount)
			}
			if follows, _ := ops.followCheck(ctx, 1, 2); !follows {
				t.Error("the follow did not show in the follow check")
			}
			if err := ops.unfollow(ctx, 1, 2); err != nil {
				t.Fatal(err)
			}
			if follows, _ := ops.followCheck(ctx, 1, 2); follows {
				t.Error("the unfollow did not show in the follow check")
			}
			if err := ops.unfollow(ctx, 1, 2); err != nil {
				t.Fatalf("unfollowing a user who is not followed must succeed, as a delete does: %v", err)
			}

			if err := ops.likePost(ctx, 1, 0, 1); err != nil {
				t.Fatal(err)
			}
			if err := ops.commentOnPost(ctx, 2, 0, 1, "nice"); err != nil {
				t.Fatal(err)
			}
			page, err := ops.postPage(ctx, 0, 1)
			if err != nil || !page.Found || page.Likes != 1 || page.Comments != 1 || page.Post.Text != "post 1" {
				t.Fatalf("post page %+v %v", page, err)
			}
			if missing, err := ops.postPage(ctx, 0, 9); err != nil || missing.Found {
				t.Fatalf("a missing post is a page that was not found, not an error: %+v %v", missing, err)
			}

			ledger := &noteLedger{}
			fanout, err := ops.publishPost(ctx, postRecord{Author: "0", Seq: 3, Text: "post 3", Created: ringFixtureEpoch.Add(3 * time.Hour)}, ledger)
			if err != nil || fanout != 2 || !slices.Equal(ledger.sent, []int{1, 2}) || !slices.Equal(ledger.done, []int{1, 2}) {
				t.Fatalf("publish: fan-out %d, ledger %+v, %v", fanout, ledger, err)
			}
			if feed, _ := ops.homeTimeline(ctx, 1, 20); len(feed) != 3 || feed[0].Seq != 3 {
				t.Errorf("the published post must be on top of the follower's feed: %+v", feed)
			}
			if timeline, _ := ops.profileTimeline(ctx, 0, 1); len(timeline) != 1 || timeline[0].Seq != 3 || timeline[0].Text != "post 3" {
				t.Errorf("the published post must be the author's newest: %+v", timeline)
			}

			if took, err := ops.readNotifications(ctx, 1, 10); err != nil || took != 10 {
				t.Fatalf("notifications read: %d %v, want 10 of 12", took, err)
			}
			if took, _ := ops.readNotifications(ctx, 1, 10); took != 2 {
				t.Errorf("the second read took %d, want the 2 that were left", took)
			}
			if took, err := ops.readNotifications(ctx, 1, 10); err != nil || took != 0 {
				t.Errorf("an empty inbox reads 0: %d %v", took, err)
			}

			if err := ops.uploadAvatar(ctx, 2, []byte("new avatar")); err != nil {
				t.Fatal(err)
			}
			if avatar, err := ops.avatar(ctx, 2); err != nil || string(avatar) != "new avatar" {
				t.Fatalf("avatar %q %v", avatar, err)
			}
			if profile, _ := ops.profile(ctx, 2); profile.Name != "user 2" || profile.Bio != "bio 2" {
				t.Errorf("replacing the avatar must keep the other fields: %+v", profile)
			}
			if _, err := ops.avatar(ctx, 0); !errors.Is(err, errNoAvatar) {
				t.Errorf("a user with no avatar gives errNoAvatar, got %v", err)
			}

			merged, err := ops.mergedTimeline(ctx, []int{0, 1})
			if err != nil || len(merged) != 4 || merged[0].Seq != 3 {
				t.Fatalf("merged timeline %+v %v: want the author's four posts, newest first", merged, err)
			}
			if claimed, err := ops.queueRound(ctx, 5); err != nil || claimed != 1 {
				t.Fatalf("queue round claimed %d %v, want the one job it pushed", claimed, err)
			}
			if count, _ := ops.api.count(ctx, storeRef{bucket: ringload.OutboxBucket, id: "5", field: ringload.OutboxJobs}); count != 0 {
				t.Errorf("the acknowledged job must leave the outbox: %d left", count)
			}
		})
	}
}

// failingPush wraps a connection and refuses the nth push.
type failingPush struct {
	ringAPI
	pushes, failAt int
}

func (f *failingPush) push(ctx context.Context, store storeRef, element any) error {
	f.pushes++
	if f.pushes == f.failAt {
		return &ringclient.Error{Code: cluster.CodeUnavailable, Message: "node busy"}
	}
	return f.ringAPI.push(ctx, store, element)
}

// TestRingPublishStopsAtTheFirstFailedCopyAndTellsTheLedger: a copy that fails is issued and
// not delivered, and no later follower is tried, so the bookkeeping brackets the truth.
func TestRingPublishStopsAtTheFirstFailedCopyAndTellsTheLedger(t *testing.T) {
	model := ringFixture(t)
	ops := &ringOps{api: &failingPush{ringAPI: model.connect(), failAt: 2}}
	ledger := &noteLedger{}
	_, err := ops.publishPost(t.Context(), postRecord{Author: "0", Seq: 3, Text: "post 3", Created: ringFixtureEpoch}, ledger)
	var refusal *ringclient.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		t.Fatalf("publish error %v, want the node's refusal", err)
	}
	if !slices.Equal(ledger.sent, []int{1, 2}) || !slices.Equal(ledger.done, []int{1}) {
		t.Errorf("ledger sent %v delivered %v, want 1 and 2 issued, only 1 delivered", ledger.sent, ledger.done)
	}
}

// failingTake refuses every take.
type failingTake struct{ ringAPI }

func (failingTake) take(context.Context, storeRef, string) error {
	return &ringclient.Error{Code: cluster.CodeNotOwner, Message: "not the owner"}
}

// TestRingUnfollowStillFailsOnARealRefusal: only an absent edge is forgiven.
func TestRingUnfollowStillFailsOnARealRefusal(t *testing.T) {
	ops := &ringOps{api: failingTake{ringOpsOn(ringFixture(t), true).api}}
	if err := ops.unfollow(t.Context(), 2, 1); err == nil {
		t.Fatal("a refusal other than not found must fail the unfollow")
	}
}

// TestRingCanonicalUserHoldsEverythingTheServerHolds: the canonical form of a user reads the
// document, its posts with likers and comments, both stores of edges, feed, inbox and media.
func TestRingCanonicalUserHoldsEverythingTheServerHolds(t *testing.T) {
	ctx := context.Background()
	for _, serial := range []bool{true, false} {
		model := ringFixture(t)
		ops := ringOpsOn(model, serial)
		if err := ops.likePost(ctx, 1, 0, 1); err != nil {
			t.Fatal(err)
		}
		if err := ops.commentOnPost(ctx, 2, 0, 1, "nice"); err != nil {
			t.Fatal(err)
		}
		got, err := ops.canonicalUser(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "0" || got.Name != "user 0" || got.FollowerCount != 2 || len(got.Posts) != 3 || !slices.Equal(got.Followers, []string{"1", "2"}) || len(got.Following) != 0 {
			t.Fatalf("canonical user 0: %+v", got)
		}
		if !slices.Equal(got.Posts[1].Likers, []string{"1"}) || len(got.Posts[1].Comments) != 1 || got.Posts[1].Text != "post 1" || got.Posts[0].Seq != 0 {
			t.Errorf("post 1 of user 0: %+v", got.Posts[1])
		}
		if len(got.Inbox) != 3 {
			t.Errorf("inbox of user 0 holds the seeded follow and the like and comment notifications: %+v", got.Inbox)
		}
		media, err := ops.canonicalUser(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(media.Media) != 2 || media.Media["avatar"] == "" || media.Media["avatar"] == media.Media["photo"] {
			t.Errorf("user 2 owns an avatar and a photo, the form holds their digests: %+v", media.Media)
		}
		feed, err := ops.canonicalUser(ctx, 1)
		if err != nil || len(feed.Feed) != 1 || feed.Feed[0].Author != "0" || len(feed.Feed[0].Entries) != 2 || feed.Feed[0].Entries[0].Seq != 1 {
			t.Fatalf("canonical user 1 feed: %+v %v", feed.Feed, err)
		}
	}
}

// TestRingCanonicalReadsAStoreLargerThanOnePage: a feed or an inbox longer than a page is read
// whole by following the list's cursor, in the order a pop would take.
func TestRingCanonicalReadsAStoreLargerThanOnePage(t *testing.T) {
	model := ringFixture(t)
	conn := model.connect()
	const notes = 2*ringPageSize + 50
	for note := range notes {
		element := map[string]any{"type": "like", "actor": fmt.Sprint(note), "target": "0", "ts": ringFixtureEpoch}
		if err := conn.push(t.Context(), userStore(0, ringload.UserInbox), element); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ringOpsOn(model, true).canonicalUser(t.Context(), 0)
	if err != nil || len(got.Inbox) != notes+1 {
		t.Fatalf("canonical inbox holds %d notifications (%v), want the %d pushed and the 1 seeded", len(got.Inbox), err, notes)
	}
}

// TestRingEngineHandsOutOneConnectionPerHandle: the pool is the workers' handles, -depth of them,
// and closing a handle closes its connection; nothing else dials during a read.
func TestRingEngineHandsOutOneConnectionPerHandle(t *testing.T) {
	model := ringFixture(t)
	engine, opened := ringModelEngine(&config{Engine: engineRing, Database: "social", OpTimeout: time.Second}, model)
	var handles []socialOps
	for range 5 {
		ops, err := engine.newSocialOps()
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, ops)
	}
	if opened.Load() != 5 {
		t.Fatalf("5 handles opened %d connections", opened.Load())
	}
	for _, ops := range handles {
		if _, err := ops.profile(t.Context(), 0); err != nil {
			t.Fatal(err)
		}
		ops.close()
	}
	if opened.Load() != 5 {
		t.Fatalf("reads opened more connections: %d", opened.Load())
	}
}

func TestRingErrorsAreBucketedByTheirCode(t *testing.T) {
	seen := map[string]cluster.Code{}
	for _, code := range cluster.Codes() {
		kind := errorKind(fmt.Errorf("operation R1: %w", &ringclient.Error{Code: code, Message: "m"}))
		if want := "server_ring_" + code.String(); kind != want {
			t.Errorf("code %s is bucketed as %q, want %q", code, kind, want)
		}
		if other, dup := seen[kind]; dup {
			t.Errorf("codes %s and %s share the bucket %q", other, code, kind)
		}
		seen[kind] = code
	}
	if kind := errorKind(fmt.Errorf("reading: %w", context.DeadlineExceeded)); kind != "timeout" {
		t.Errorf("a request deadline is %q, want timeout", kind)
	}
	if kind := errorKind(errors.New("ringclient: writing to the node: broken pipe")); kind != "io_or_decode" {
		t.Errorf("a broken connection is %q, want io_or_decode", kind)
	}
	missing := fmt.Errorf("no document: %w", ringclient.ErrNotFound)
	if kind := errorKind(&ringclient.Error{Code: cluster.CodeNotFound}); kind != "server_ring_NOT_FOUND" || errorKind(missing) == "" {
		t.Errorf("not found is %q", kind)
	}
}

func TestRingRefusalsLeaveTheConnectionInStep(t *testing.T) {
	if isDesynced(fmt.Errorf("W3: %w", &ringclient.Error{Code: cluster.CodeNotOwner})) {
		t.Error("a refusal the node sent leaves the connection in step")
	}
	if !isDesynced(errors.New("ringclient: no answer before the deadline")) {
		t.Error("a request with no answer is not known to be in step")
	}
}

func TestRingUploadedAvatarBytesRoundTrip(t *testing.T) {
	model := ringFixture(t)
	ops := ringOpsOn(model, true)
	blob := bytes.Repeat([]byte{0xAB, 0x01}, 4096)
	if err := ops.uploadAvatar(t.Context(), 0, blob); err != nil {
		t.Fatal(err)
	}
	got, err := ops.avatar(t.Context(), 0)
	if err != nil || !bytes.Equal(got, blob) {
		t.Fatalf("avatar of %d bytes came back as %d bytes: %v", len(blob), len(got), err)
	}
}
