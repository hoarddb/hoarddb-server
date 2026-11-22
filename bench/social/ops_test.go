package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// tinySpec is a dataset small enough to seed in a test and shaped like the
// real ones: ten posts per user, a few follows each, two notifications each.
var tinySpec = datasetSpec{
	Name: "tiny", Users: 60, Posts: 600, FollowEdges: 180, FeedEntries: 300,
	Likes: 120, Comments: 60, Inbox: 120, Avatars: 6, Photos: 3,
}

// socialWorld is a seeded hoardDB engine and the graph it was seeded from.
type socialWorld struct {
	// social is the engine under test; engine is the same engine when it is
	// hoardDB's, and nil for MongoDB and MySQL.
	social socialEngine
	engine *hoardEngine
	graph  *socialGraph
	// fake is nil for a live server.
	fake                  *fakeServer
	inFlight, maxInFlight *atomic.Int64
}

func newFakeWorld(t *testing.T) *socialWorld { return newFakeWorldPlaced(t, "h1") }

// newFakeWorldPlaced is a seeded fake world in a placement form. The fake
// refuses any request that addresses a user in a bucket other than the one the
// form puts the user in, using a reference shard function that shares no code
// with the engine's.
func newFakeWorldPlaced(t *testing.T, placement string) *socialWorld {
	t.Helper()
	world := &socialWorld{fake: newFakeServer(), inFlight: new(atomic.Int64), maxInFlight: new(atomic.Int64)}
	world.fake.expectBucket = referenceBucket(placement)
	cfg := &config{Engine: engineHoardDB, Database: "social", Users: tinySpec.Users, Conc: 4, OpTimeout: time.Second, Placement: placement}
	world.engine = &hoardEngine{cfg: cfg, dial: func(string) (wireCaller, func(), error) {
		return world.fake.dial(world.inFlight, world.maxInFlight), func() {}, nil
	}}
	world.social = world.engine
	world.seed(t)
	return world
}

// newLiveWorldUnseeded names a database on the server named by
// SOCIAL_LIVE_HOARD_ADDR, which is dropped when the test ends, and seeds nothing.
func newLiveWorldUnseeded(t *testing.T) *socialWorld {
	t.Helper()
	addr := os.Getenv("SOCIAL_LIVE_HOARD_ADDR")
	if addr == "" {
		t.Skip("set SOCIAL_LIVE_HOARD_ADDR (and SOCIAL_LIVE_HOARD_PASSWORD) to run against a local hoarddb-server")
	}
	password := os.Getenv("SOCIAL_LIVE_HOARD_PASSWORD")
	database := fmt.Sprintf("social_live_%d", time.Now().UnixNano())
	cfg := &config{Engine: engineHoardDB, Addr: addr, Password: password, Database: database, Users: tinySpec.Users, Conc: 4, OpTimeout: 30 * time.Second}
	admin := liveHoardConn(t)
	t.Cleanup(func() {
		var dropped server.DeleteDatabaseResponse
		if err := admin.call(transport.OpDeleteDB, server.DeleteDatabaseRequest{Name: database}, &dropped); err != nil {
			t.Logf("dropping %s: %v", database, err)
		}
	})
	engine := newHoardEngine(cfg)
	return &socialWorld{engine: engine, social: engine}
}

// newLiveWorld seeds the tiny dataset into a database of its own.
func newLiveWorld(t *testing.T) *socialWorld {
	t.Helper()
	world := newLiveWorldUnseeded(t)
	world.seed(t)
	return world
}

func (w *socialWorld) seed(t *testing.T) {
	t.Helper()
	w.graph = newSocialGraph(tinySpec, 7)
	ctx := context.Background()
	if _, err := w.social.setupSocial(ctx, tinySpec); err != nil {
		t.Fatalf("setting up the social schema: %v", err)
	}
	if err := w.social.seedSocial(ctx, w.graph, 4); err != nil {
		t.Fatalf("seeding the tiny dataset: %v", err)
	}
}

func (w *socialWorld) ops(t *testing.T) socialOps {
	t.Helper()
	ops, err := w.social.newSocialOps()
	if err != nil {
		t.Fatalf("opening the operations handle: %v", err)
	}
	t.Cleanup(ops.close)
	return ops
}

// busiestAuthor is a user with several followers, so fan-out is visible.
func (w *socialWorld) busiestAuthor() int {
	best := 0
	for user := range w.graph.followerCount {
		if w.graph.followerCount[user] > w.graph.followerCount[best] {
			best = user
		}
	}
	return best
}

// followersOf lists who follows user, from the graph.
func (w *socialWorld) followersOf(user int) []int {
	var followers []int
	for follower := range w.graph.spec.Users {
		if slices.Contains(w.graph.following(follower), int32(user)) {
			followers = append(followers, follower)
		}
	}
	return followers
}

func eachWorld(t *testing.T, body func(t *testing.T, world *socialWorld)) {
	t.Run("fake", func(t *testing.T) { body(t, newFakeWorld(t)) })
	t.Run("live", func(t *testing.T) { body(t, newLiveWorld(t)) })
}

func authorAndSeq(records []postRecord) []string {
	listed := make([]string, len(records))
	for i, record := range records {
		listed[i] = fmt.Sprintf("%s/%d", record.Author, record.Seq)
	}
	return listed
}

// TestOperationContract runs every operation of spec 6.3 against the seeded
// dataset and checks each answer against a reference model built from the
// graph: what the verifier is going to expect.
func TestOperationContract(t *testing.T) {
	eachWorld(t, func(t *testing.T, world *socialWorld) {
		checkOperationContract(t, world)
		checkMergedTimelineIsTrueNewest(t, world)
	})
}

// checkOperationContract is the body of TestOperationContract for any engine.
func checkOperationContract(t *testing.T, world *socialWorld) {
	ctx := context.Background()
	ops := world.ops(t)
	graph := world.graph
	actor := 3

	t.Run("seeded reads", func(t *testing.T) {
		profile, err := ops.profile(ctx, actor)
		if err != nil || profile.UserID != userID(actor) {
			t.Fatalf("R2 profile = %+v, %v; want user %s", profile, err, userID(actor))
		}
		posts, err := ops.profileTimeline(ctx, actor, 20)
		if err != nil || len(posts) != graph.postsPerUser {
			t.Fatalf("R3 returned %d posts, %v; want %d", len(posts), err, graph.postsPerUser)
		}
		for position, post := range posts {
			if want := int64(graph.postsPerUser - 1 - position); post.Seq != want || post.Author != userID(actor) {
				t.Fatalf("R3 position %d holds %s/%d; want %s/%d (newest first)", position, post.Author, post.Seq, userID(actor), want)
			}
		}
		var wantFeed []string
		for _, ref := range slices.Backward(graph.feedOf(actor)) {
			wantFeed = append(wantFeed, fmt.Sprintf("%d/%d", ref.Author, ref.Seq))
		}
		feed, err := ops.homeTimeline(ctx, actor, 20)
		if err != nil || !slices.Equal(authorAndSeq(feed), wantFeed[:min(20, len(wantFeed))]) {
			t.Fatalf("R1 = %v, %v; want %v", authorAndSeq(feed), err, wantFeed[:min(20, len(wantFeed))])
		}
		followed := graph.following(actor)
		for _, target := range followed {
			if follows, err := ops.followCheck(ctx, actor, int(target)); err != nil || !follows {
				t.Fatalf("R5 follows %d = %v, %v; want true", target, follows, err)
			}
		}
		stranger := 0
		for slices.Contains(followed, int32(stranger)) || stranger == actor {
			stranger++
		}
		if follows, err := ops.followCheck(ctx, actor, stranger); err != nil || follows {
			t.Fatalf("R5 follows %d = %v, %v; want false", stranger, follows, err)
		}
		listed, err := ops.followingList(ctx, actor, 50)
		if err != nil || len(listed) != len(followed) {
			t.Fatalf("R6 = %v, %v; want %d ids", listed, err, len(followed))
		}
		if inbox, err := ops.readNotifications(ctx, actor, 10); err != nil || inbox != tinySpec.Inbox/tinySpec.Users {
			t.Fatalf("N1 took %d, %v; want %d", inbox, err, tinySpec.Inbox/tinySpec.Users)
		}
		if again, err := ops.readNotifications(ctx, actor, 10); err != nil || again != 0 {
			t.Fatalf("N1 on a drained inbox took %d, %v; want 0", again, err)
		}
	})

	t.Run("post page and avatar", func(t *testing.T) {
		wantLikes, wantComments := map[[2]int]map[int]bool{}, map[[2]int]int{}
		for like := range graph.likes() {
			key := [2]int{like.PostAuthor, like.PostSeq}
			if wantLikes[key] == nil {
				wantLikes[key] = map[int]bool{}
			}
			wantLikes[key][like.Actor] = true
		}
		for comment := range graph.comments() {
			wantComments[[2]int{comment.PostAuthor, comment.PostSeq}]++
		}
		for key, likers := range wantLikes {
			page, err := ops.postPage(ctx, key[0], int64(key[1]))
			if err != nil || !page.Found || page.Likes != int64(len(likers)) || page.Comments != int64(wantComments[key]) {
				t.Fatalf("R4 post %v = %+v, %v; want %d likes and %d comments", key, page, err, len(likers), wantComments[key])
			}
			if page.Post.Author != userID(key[0]) || page.Post.Seq != int64(key[1]) {
				t.Fatalf("R4 post %v returned %s/%d", key, page.Post.Author, page.Post.Seq)
			}
			break
		}
		for item := range graph.media() {
			content, err := ops.avatar(ctx, item.Owner)
			if item.Kind != mediaAvatar {
				continue
			}
			if err != nil || !bytes.Equal(content, graph.content(item)) {
				t.Fatalf("R7 avatar of %d: %d bytes, %v; want the seeded %d", item.Owner, len(content), err, item.Bytes)
			}
		}
		noAvatar := 1
		for owner := range graph.media() {
			if owner.Owner == noAvatar {
				noAvatar++
			}
		}
		if _, err := ops.avatar(ctx, noAvatar); !errors.Is(err, errNoAvatar) {
			t.Fatalf("R7 of user %d without an avatar: %v; want errNoAvatar", noAvatar, err)
		}
	})

	t.Run("writes", func(t *testing.T) {
		author := world.busiestAuthor()
		followers := world.followersOf(author)
		post := postRecord{Author: userID(author), Seq: int64(graph.postsPerUser), Text: "a new post", Created: time.Now().UTC().Truncate(time.Millisecond), Tags: []string{"x"}}
		fanout, err := ops.publishPost(ctx, post, noLedger{})
		if err != nil || fanout != len(followers) {
			t.Fatalf("W1 fanned out to %d, %v; want %d followers", fanout, err, len(followers))
		}
		for _, follower := range followers {
			feed, err := ops.homeTimeline(ctx, follower, 1)
			if err != nil || len(feed) != 1 || feed[0].Seq != post.Seq || feed[0].Author != post.Author {
				t.Fatalf("W1: the feed of follower %d starts with %v, %v; want the new post", follower, authorAndSeq(feed), err)
			}
		}
		own, err := ops.profileTimeline(ctx, author, 1)
		if err != nil || len(own) != 1 || own[0].Seq != post.Seq {
			t.Fatalf("W1: R3 of the author starts with %v, %v; want the new post", authorAndSeq(own), err)
		}

		before, err := ops.postPage(ctx, author, 2)
		if err != nil {
			t.Fatalf("R4 before the reactions: %v", err)
		}
		liker := (author + 17) % tinySpec.Users
		if err := ops.likePost(ctx, liker, author, 2); err != nil {
			t.Fatalf("W2 like: %v", err)
		}
		if err := ops.commentOnPost(ctx, liker, author, 2, "agreed"); err != nil {
			t.Fatalf("W5 comment: %v", err)
		}
		after, err := ops.postPage(ctx, author, 2)
		if err != nil || after.Comments != before.Comments+1 || after.Likes < before.Likes || after.Likes > before.Likes+1 {
			t.Fatalf("R4 after a like and a comment: %+v, %v; before %+v", after, err, before)
		}
		// two reactions plus whatever the seed left in the inbox
		if taken, err := ops.readNotifications(ctx, author, 100); err != nil || taken < 2 {
			t.Fatalf("N1 after a like and a comment took %d, %v; want at least the 2 new notifications", taken, err)
		}

		follower, followee := (author+9)%tinySpec.Users, (author+11)%tinySpec.Users
		if slices.Contains(graph.following(follower), int32(followee)) {
			followee = (followee + 1) % tinySpec.Users
		}
		if err := ops.follow(ctx, follower, followee); err != nil {
			t.Fatalf("W3 follow: %v", err)
		}
		if follows, err := ops.followCheck(ctx, follower, followee); err != nil || !follows {
			t.Fatalf("R5 after W3 = %v, %v; want true", follows, err)
		}
		if profile, err := ops.profile(ctx, followee); err != nil || profile.FollowerCount < 1 {
			t.Fatalf("R2 after W3: %+v, %v; want the follower count raised", profile, err)
		}
		if err := ops.unfollow(ctx, follower, followee); err != nil {
			t.Fatalf("W4 unfollow: %v", err)
		}
		if follows, err := ops.followCheck(ctx, follower, followee); err != nil || follows {
			t.Fatalf("R5 after W4 = %v, %v; want false", follows, err)
		}

		avatar := bytes.Repeat([]byte{0xAB}, avatarBytes)
		if err := ops.uploadAvatar(ctx, noAvatarUser(graph), avatar); err != nil {
			t.Fatalf("W6 upload: %v", err)
		}
		if got, err := ops.avatar(ctx, noAvatarUser(graph)); err != nil || !bytes.Equal(got, avatar) {
			t.Fatalf("R7 after W6: %d bytes, %v; want the uploaded avatar", len(got), err)
		}
	})

	t.Run("merged timeline and queue", func(t *testing.T) {
		followees := make([]int, 0, mergedPostsTotal)
		for _, followee := range graph.following(actor) {
			followees = append(followees, int(followee))
		}
		merged, err := ops.mergedTimeline(ctx, followees)
		if want := min(mergedPostsTotal, len(followees)*graph.postsPerUser); err != nil || len(merged) != want {
			t.Fatalf("M1 returned %d posts, %v; want %d", len(merged), err, want)
		}
		if !slices.IsSortedFunc(merged, func(a, b postRecord) int { return b.Created.Compare(a.Created) }) {
			t.Fatalf("M1 is not newest first: %v", authorAndSeq(merged))
		}
		for range 2 {
			if claimed, err := ops.queueRound(ctx, 5); err != nil || claimed != 1 {
				t.Fatalf("Q1 claimed %d, %v; want the one job it pushed", claimed, err)
			}
		}
	})
}

// noAvatarUser is a user the seed gave no avatar.
func noAvatarUser(graph *socialGraph) int {
	owners := map[int]bool{}
	for item := range graph.media() {
		owners[item.Owner] = true
	}
	user := 1
	for owners[user] {
		user++
	}
	return user
}

// TestRequestsPerOperationAreCounted checks the request count of each
// operation against the "Req" column of spec 6.3.
func TestRequestsPerOperationAreCounted(t *testing.T) {
	world := newFakeWorld(t)
	ctx := context.Background()
	ops := world.ops(t)
	author := world.busiestAuthor()
	followers := len(world.followersOf(author))
	followees := []int{}
	for _, followee := range world.graph.following(3) {
		followees = append(followees, int(followee))
	}
	post := postRecord{Author: userID(author), Seq: 100, Text: "t", Created: time.Now().UTC()}
	cases := []struct {
		name string
		want int64
		run  func() error
	}{
		{"R1", 1, func() error { _, err := ops.homeTimeline(ctx, 3, 20); return err }},
		{"R2", 1, func() error { _, err := ops.profile(ctx, 3); return err }},
		{"R3", 1, func() error { _, err := ops.profileTimeline(ctx, 3, 20); return err }},
		{"R4", 3, func() error { _, err := ops.postPage(ctx, 3, 1); return err }},
		{"R5", 1, func() error { _, err := ops.followCheck(ctx, 3, 4); return err }},
		{"R6", 1, func() error { _, err := ops.followingList(ctx, 3, 50); return err }},
		{"R7", 1, func() error { _, err := ops.avatar(ctx, 0); return err }},
		{"N1", 1, func() error { _, err := ops.readNotifications(ctx, 3, 10); return err }},
		{"W1", int64(2 + followers), func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err }},
		{"W2", 2, func() error { return ops.likePost(ctx, 4, 3, 1) }},
		{"W3", 5, func() error { return ops.follow(ctx, 4, 5) }},
		{"W4", 4, func() error { return ops.unfollow(ctx, 4, 5) }},
		{"W5", 2, func() error { return ops.commentOnPost(ctx, 4, 3, 1, "c") }},
		{"W6", 1, func() error { return ops.uploadAvatar(ctx, 0, []byte("a")) }},
		{"M1", int64(len(followees)), func() error { _, err := ops.mergedTimeline(ctx, followees); return err }},
		{"Q1", 3, func() error { _, err := ops.queueRound(ctx, 1); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := ops.requests()
			if err := tc.run(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := ops.requests() - before; got != tc.want {
				t.Errorf("%s sent %d requests, want %d", tc.name, got, tc.want)
			}
		})
	}
}

// TestSerialSubOperationsByDefault: with the Q-A default no operation has two
// of its requests in flight at once.
func TestSerialSubOperationsByDefault(t *testing.T) {
	world := newFakeWorld(t)
	world.maxInFlight.Store(0) // seeding uses several connections on purpose
	world.fake.callDelay.Store(int64(time.Millisecond))
	ctx := context.Background()
	ops := world.ops(t)
	author := world.busiestAuthor()
	followees := []int{}
	for _, followee := range world.graph.following(3) {
		followees = append(followees, int(followee))
	}
	post := postRecord{Author: userID(author), Seq: 100, Text: "t", Created: time.Now().UTC()}
	steps := map[string]func() error{
		"R4": func() error { _, err := ops.postPage(ctx, 3, 1); return err },
		"W1": func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err },
		"W3": func() error { return ops.follow(ctx, 4, 5) },
		"W4": func() error { return ops.unfollow(ctx, 4, 5) },
		"M1": func() error { _, err := ops.mergedTimeline(ctx, followees); return err },
		"Q1": func() error { _, err := ops.queueRound(ctx, 1); return err },
	}
	for name, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if peak := world.maxInFlight.Load(); peak != 1 {
		t.Errorf("up to %d requests were in flight at once; the serial default allows 1", peak)
	}
}

// TestHeapPushUsesPriorityMember: a heap orders by the request's Priority, so
// an urgency that is only a document field would leave push order in charge.
func TestHeapPushUsesPriorityMember(t *testing.T) {
	fake := newFakeServer()
	fake.engines["Tickets"] = "heap"
	caller := fake.dial(nil, nil)
	if err := caller.call(transport.OpPut, server.InsertRequest{Database: "ps", Bucket: "Company", Key: "c1", Data: map[string]interface{}{"name": "c"}}, &server.InsertResponse{}); err != nil {
		t.Fatalf("creating the company: %v", err)
	}
	target := server.FieldRequest{Database: "ps", Bucket: "Company", Key: "c1", Field: "Tickets"}
	for _, urgency := range []int64{1, 9} {
		if err := pushByUrgency(caller, target, urgency, map[string]interface{}{"urgency": urgency}); err != nil {
			t.Fatalf("pushing urgency %d: %v", urgency, err)
		}
	}
	var priorities []int64
	for _, sent := range caller.log {
		if request, ok := sent.request.(server.FieldRequest); ok && sent.op == transport.OpFieldPush {
			priorities = append(priorities, request.Priority)
		}
	}
	if !slices.Equal(priorities, []int64{1, 9}) {
		t.Errorf("the pushes carried Priority %v; want [1 9]", priorities)
	}
	top, err := peekMostUrgent(caller, target)
	if err != nil {
		t.Fatalf("peeking: %v", err)
	}
	var element struct {
		Urgency int64 `bson:"urgency"`
	}
	if err := bson.Unmarshal(top, &element); err != nil || element.Urgency != 9 {
		t.Errorf("peek returned urgency %d, %v; want 9", element.Urgency, err)
	}
}

func TestPredecessorKeyStartsInclusive(t *testing.T) {
	cases := []struct {
		lo      string
		want    string
		wantErr bool
	}{
		{"0000000010", "0000000009", false},
		{"0000000100", "0000000099", false},
		{"0000000000", "", false},
		{"not-a-number", "", true},
	}
	for _, tc := range cases {
		got, err := predecessorKey(tc.lo)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("predecessorKey(%q) = %q, %v; want %q, error=%v", tc.lo, got, err, tc.want, tc.wantErr)
		}
	}
	fake := newFakeServer()
	caller := fake.dial(nil, nil)
	seedUserWithPosts(t, caller, 10)
	if _, err := listRange(caller, server.FieldRequest{Database: "d", Bucket: "User", Key: "u", Field: "Posts"}, postKey(7), postKey(3), 2); err != nil {
		t.Fatalf("listRange: %v", err)
	}
	first, ok := caller.log[len(caller.log)-3].request.(server.FieldListRequest)
	if !ok {
		t.Fatalf("expected the first list request, got %T", caller.log[len(caller.log)-3].request)
	}
	if got, want := first.After.StringValue(), mustPredecessor(t, postKey(7)); got != want {
		t.Errorf("the first page started after %q; an inclusive lower bound starts after its predecessor %q", got, want)
	}
}

func mustPredecessor(t *testing.T, key string) string {
	t.Helper()
	predecessor, err := predecessorKey(key)
	if err != nil {
		t.Fatalf("predecessorKey(%q): %v", key, err)
	}
	return predecessor
}

func seedUserWithPosts(t *testing.T, caller wireCaller, posts int) {
	t.Helper()
	if err := caller.call(transport.OpPut, server.InsertRequest{Database: "d", Bucket: "User", Key: "u", Data: map[string]interface{}{"name": "u"}}, &server.InsertResponse{}); err != nil {
		t.Fatalf("creating the user: %v", err)
	}
	for seq := range int64(posts) {
		request := server.FieldRequest{Database: "d", Bucket: "User", Key: "u", Field: "Posts", Element: postKey(seq), Data: map[string]interface{}{"seq": seq}}
		if err := caller.call(transport.OpFieldPut, request, &server.FieldResponse{}); err != nil {
			t.Fatalf("putting post %d: %v", seq, err)
		}
	}
}

// TestListAfterIsExclusive: a range read returns the in-range rows exactly,
// with a page boundary inside the range. `after` is exclusive, so starting at
// the lower bound itself would drop it.
func TestListAfterIsExclusive(t *testing.T) {
	fake := newFakeServer()
	caller := fake.dial(nil, nil)
	seedUserWithPosts(t, caller, 10)
	entries, err := listRange(caller, server.FieldRequest{Database: "d", Bucket: "User", Key: "u", Field: "Posts"}, postKey(7), postKey(3), 2)
	if err != nil {
		t.Fatalf("listRange: %v", err)
	}
	var got []string
	for _, entry := range entries {
		got = append(got, entry.ElementKey)
	}
	want := []string{postKey(7), postKey(6), postKey(5), postKey(4), postKey(3)}
	if !slices.Equal(got, want) {
		t.Errorf("range read returned %v; want exactly %v", got, want)
	}
}

// checkMergedTimelineIsTrueNewest: M1 returns the true newest 20 posts across
// all followees on every engine (owner ruling). One followee publishes 20 posts
// newer than everything seeded; they must be the whole answer, not five of
// them.
func checkMergedTimelineIsTrueNewest(t *testing.T, world *socialWorld) {
	t.Helper()
	ctx := context.Background()
	ops := world.ops(t)
	graph := world.graph
	actor := 0
	for len(graph.following(actor)) < 2 {
		actor++
	}
	var followees []int
	for _, followee := range graph.following(actor) {
		followees = append(followees, int(followee))
	}
	prolific := followees[0]
	var wantSeqs []int64
	for i := range mergedPostsTotal {
		post := postRecord{Author: userID(prolific), Seq: int64(graph.postsPerUser + i), Text: "newest", Created: graphEpoch.Add(time.Hour + time.Duration(i)*time.Second), Tags: []string{}}
		if _, err := ops.publishPost(ctx, post, noLedger{}); err != nil {
			t.Fatalf("publishing post %d of user %d: %v", i, prolific, err)
		}
		wantSeqs = append([]int64{post.Seq}, wantSeqs...)
	}
	merged, err := ops.mergedTimeline(ctx, followees)
	if err != nil {
		t.Fatalf("M1: %v", err)
	}
	var gotSeqs []int64
	for _, post := range merged {
		if post.Author != userID(prolific) {
			t.Fatalf("M1 returned %v; the 20 newest posts are all user %d's", authorAndSeq(merged), prolific)
		}
		gotSeqs = append(gotSeqs, post.Seq)
	}
	if !slices.Equal(gotSeqs, wantSeqs) {
		t.Errorf("M1 returned seqs %v; want the 20 newest %v", gotSeqs, wantSeqs)
	}
}

func TestMergedTimelineIsTheTrueNewest(t *testing.T) {
	eachWorld(t, checkMergedTimelineIsTrueNewest)
}
