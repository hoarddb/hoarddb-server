package ringload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	serverconfig "github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
)

// tinySpec is a dataset small enough to seed in a second: 50 users, 5 posts each.
var tinySpec = Spec{Name: "tiny", Users: 50, Posts: 250, FollowEdges: 150, FeedEntries: 150, Likes: 100, Comments: 60, Inbox: 100, Avatars: 5, Photos: 2}

// generatorFingerprint hashes figures drawn from the generator. ../ringseed_parity_test.go
// computes the same figures from the harness's own generator and pins the same string.
func generatorFingerprint(graph *socialGraph) string {
	digest := sha256.New()
	for _, user := range []int{0, 7, 31} {
		hashf(digest, "following %v\n", graph.following(user))
		hashf(digest, "feed %d %v\n", graph.feedQuota[user], graph.feedOf(user))
		post := graph.post(user, 2)
		hashf(digest, "post %s %s %v %q\n", post.Text, post.Created.Format(time.RFC3339Nano), post.Tags, post.ReplyTo)
	}
	count := 0
	for like := range graph.likes() {
		hashf(digest, "like %v\n", like)
		if count++; count == 20 {
			break
		}
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// goldenFingerprint is the fingerprint of tinySpec at DatasetSeed.
const goldenFingerprint = "45927e27afe7f29ff1eceab0912407abbe92cc7be2650c4a81c9246da7b19ea2"

func TestGeneratorIsDeterministic(t *testing.T) {
	first := generatorFingerprint(newSocialGraph(tinySpec, DatasetSeed))
	if second := generatorFingerprint(newSocialGraph(tinySpec, DatasetSeed)); first != second {
		t.Fatalf("two graphs from the same seed differ: %s and %s", first, second)
	}
}

func TestGeneratorMatchesTheHarness(t *testing.T) {
	if got := generatorFingerprint(newSocialGraph(tinySpec, DatasetSeed)); got != goldenFingerprint {
		t.Fatalf("the generator's output changed: fingerprint %s, want %s. Update ../graph.go and graph.go together, then both golden strings", got, goldenFingerprint)
	}
}

// TestDeclaredShapeIsTwoBucketsWhateverTheDatasetSize: the declaration does not mention a
// user, so the bucket count cannot grow with the data (rule 11). The Outbox bucket of the
// harness' queue operation is the third and exists only when asked for.
func TestDeclaredShapeIsTwoBucketsWhateverTheDatasetSize(t *testing.T) {
	declared := declaredBuckets()
	if len(declared) != 3 || len(bucketOrder) != 3 || bucketOrder[2] != OutboxBucket {
		t.Fatalf("declared %d buckets, order lists %d; want the social two and the optional outbox", len(declared), len(bucketOrder))
	}
	for _, name := range bucketOrder {
		if _, ok := declared[name]; !ok {
			t.Fatalf("bucket %q is in the creation order and not declared", name)
		}
	}
	stores := map[string]string{UserPosts: "btreeStore", UserFeed: "lifoStore", UserFollowing: "hashStore", UserFollowers: "hashStore", UserInbox: "fifoStore"}
	for _, field := range declared[UserBucket] {
		if want, ok := stores[field.Name]; ok && field.Type != want {
			t.Errorf("User.%s is %s, want %s", field.Name, field.Type, want)
		}
		delete(stores, field.Name)
	}
	if len(stores) != 0 {
		t.Errorf("User does not declare %v", stores)
	}
}

func startServer(t *testing.T) string {
	t.Helper()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := serverconfig.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = addr
	service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SeedRootUser("admin", "ringseed-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	return addr
}

func dialTest(t *testing.T, addr string, connections int) []*ringclient.Client {
	t.Helper()
	var clients []*ringclient.Client
	for range connections {
		client, err := ringclient.Dial(context.Background(), ringclient.Options{Addr: addr, User: "admin", Password: "ringseed-test-password", Insecure: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		clients = append(clients, client)
	}
	return clients
}

// TestSeedTinyDatasetIntoARealServer seeds 50 users, checks the shape and the contents of one
// user, and checks that a second run is refused with the way out.
func TestSeedTinyDatasetIntoARealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clients := dialTest(t, startServer(t), 2)
	graph := newSocialGraph(tinySpec, DatasetSeed)
	var log bytes.Buffer
	stats, err := Seed(ctx, Options{Spec: tinySpec, Database: "social", Clients: clients, Workers: 8, Progress: &log})
	if err != nil {
		t.Fatalf("%v\n%s", err, log.String())
	}
	if stats.Buckets != 2 || stats.Users != 50 || stats.Posts != 250 {
		t.Fatalf("stats %+v", stats)
	}
	page, err := clients[0].ShowBuckets(ctx, "social", ringclient.PageRequest{})
	if err != nil || len(page.Buckets) != 2 {
		t.Fatalf("the database has %d buckets after seeding 50 users, want 2: %+v %v", len(page.Buckets), page, err)
	}
	users, posts := clients[0].Bucket("social", UserBucket), clients[0].Bucket("social", PostBucket)
	user := 7
	doc, err := users.Get(ctx, UserID(user))
	if err != nil || doc.Lookup("Name").StringValue() == "" || doc.Lookup("FollowingCount").Int64() != int64(len(graph.following(user))) {
		t.Fatalf("user %d: %v %v", user, doc, err)
	}
	for store, want := range map[string]int64{
		UserFollowing: int64(len(graph.following(user))), UserFeed: int64(graph.feedQuota[user]),
		UserPosts: int64(graph.postsPerUser), UserInbox: int64(tinySpec.Inbox / tinySpec.Users),
	} {
		if got, err := users.StoreCount(ctx, UserID(user), store); err != nil || got != want {
			t.Errorf("User.%s of user %d holds %d, want %d (%v)", store, user, got, want, err)
		}
	}
	listed, err := users.StoreList(ctx, UserID(user), UserPosts, 1, nil)
	if err != nil || len(listed.Elements) != 1 || listed.Elements[0].Lookup("post").StringValue() != PostID(user, graph.postsPerUser-1) {
		t.Fatalf("the Posts store must list the newest post first: %v %v", listed, err)
	}
	post, err := posts.Get(ctx, PostID(user, 0))
	if err != nil || post.Lookup("Author").StringValue() != UserID(user) || post.Lookup("Text").StringValue() != graph.post(user, 0).Text {
		t.Fatalf("post %s: %v %v", PostID(user, 0), post, err)
	}

	again, err := Seed(ctx, Options{Spec: tinySpec, Database: "social", Clients: clients, Workers: 4})
	if err == nil || !strings.Contains(err.Error(), "-database") || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("a second seed must be refused with the way out, got %+v %v", again, err)
	}
	if count, _ := users.Count(ctx, nil); count != 50 {
		t.Fatalf("the refused run changed the user count to %d", count)
	}
}

func TestPoolStopsAtTheFirstError(t *testing.T) {
	var started atomic.Int64
	items := func(yield func(int) bool) {
		for i := range 10000 {
			if !yield(i) {
				return
			}
		}
	}
	boom := errors.New("boom")
	err := pool(context.Background(), 4, items, func(context.Context, int) error {
		if started.Add(1) == 10 {
			return boom
		}
		return nil
	}, newProgress(io.Discard))
	if !errors.Is(err, boom) {
		t.Fatalf("pool returned %v, want boom", err)
	}
	if started.Load() > 40 { // the failing call, the other workers' in-flight calls, nothing queued
		t.Fatalf("pool kept working after the failure: %d items", started.Load())
	}
}

func hashf(digest hash.Hash, format string, args ...any) {
	_, _ = fmt.Fprintf(digest, format, args...)
}

// TestSeedWritesTheMediaTheEdgeKeysAndTheOutbox seeds 50 users with the outbox and checks the
// parts the harness' ring engine reads: avatars and photos as binary scalars on the owners,
// the other user's id inside every follow edge (a list returns elements without keys), and the
// Outbox documents Q1 pushes onto.
func TestSeedWritesTheMediaTheEdgeKeysAndTheOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clients := dialTest(t, startServer(t), 2)
	graph := newSocialGraph(tinySpec, DatasetSeed)
	stats, err := Seed(ctx, Options{Spec: tinySpec, Database: "social", Clients: clients, Workers: 8, Outbox: true})
	if err != nil || stats.Buckets != 3 {
		t.Fatalf("seed with the outbox: %+v %v", stats, err)
	}
	users := clients[0].Bucket("social", UserBucket)
	wantAvatar, wantPhoto := graph.mediaOf(0)
	doc, err := users.Get(ctx, UserID(0))
	_, avatar := doc.Lookup("Avatar").Binary()
	_, photo := doc.Lookup("Photo").Binary()
	if err != nil || !bytes.Equal(avatar, wantAvatar) || !bytes.Equal(photo, wantPhoto) || len(wantAvatar) == 0 || len(wantPhoto) == 0 {
		t.Fatalf("user 0 must carry its avatar and photo: %v", err)
	}
	plain, err := users.Get(ctx, UserID(1))
	if _, _, hasAvatar := plain.Lookup("Avatar").BinaryOK(); err != nil || hasAvatar {
		t.Fatalf("user 1 owns no avatar: %v %v", plain, err)
	}
	followee := int(graph.following(7)[0])
	page, err := users.StoreList(ctx, UserID(7), UserFollowing, 1000, nil)
	if err != nil || len(page.Elements) != len(graph.following(7)) {
		t.Fatalf("following of user 7: %v %v", page, err)
	}
	found := false
	for _, edge := range page.Elements {
		found = found || edge.Lookup("user").StringValue() == UserID(followee)
	}
	if !found {
		t.Fatalf("no edge of user 7 names followee %d", followee)
	}
	if count, err := clients[0].Bucket("social", OutboxBucket).Count(ctx, nil); err != nil || count != OutboxDocs {
		t.Fatalf("the outbox holds %d documents, want %d: %v", count, OutboxDocs, err)
	}
}

func TestSeedWithoutConnectionsNamesTheFix(t *testing.T) {
	if _, err := Seed(context.Background(), Options{Spec: tinySpec, Database: "social"}); err == nil || !strings.Contains(err.Error(), "Options.Clients") {
		t.Fatalf("a seed with no connections must name Options.Clients: %v", err)
	}
}

// goldenMediaDigest is the digest of user 0's avatar and photo under tinySpec.
// ../ringseed_parity_test.go pins the same string from the harness' own generator.
const goldenMediaDigest = "9dc935c6283c917dc4d0e2ed744eaaea3d8c983c2144ce79698d92459caa47f5"

func TestMediaMatchesTheHarness(t *testing.T) {
	avatar, photo := newSocialGraph(tinySpec, DatasetSeed).mediaOf(0)
	digest := sha256.Sum256(append(append([]byte{}, avatar...), photo...))
	if got := hex.EncodeToString(digest[:]); got != goldenMediaDigest {
		t.Fatalf("the media of user 0 changed: digest %s, want %s. Update ../graph.go and graph.go together, then both golden strings", got, goldenMediaDigest)
	}
}

// TestSeedAfterCreateSchemaWritesDataOnly: the harness times the setup apart from the seed, so
// a seed that follows CreateSchema must not create the database a second time.
func TestSeedAfterCreateSchemaWritesDataOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clients := dialTest(t, startServer(t), 1)
	if err := CreateSchema(ctx, clients[0], "social", true); err != nil {
		t.Fatal(err)
	}
	if _, err := Seed(ctx, Options{Spec: tinySpec, Database: "social", Clients: clients, Workers: 4, Outbox: true}); err == nil {
		t.Fatal("a seed that creates the schema must be refused once CreateSchema ran")
	}
	if _, err := Seed(ctx, Options{Spec: tinySpec, Database: "social", Clients: clients, Workers: 4, Outbox: true, SchemaCreated: true}); err != nil {
		t.Fatal(err)
	}
	if count, err := clients[0].Bucket("social", UserBucket).Count(ctx, nil); err != nil || count != int64(tinySpec.Users) {
		t.Fatalf("users after the split setup and seed: %d %v", count, err)
	}
}

func TestRetryRefusedRepeatsARefusedWriteAndNothingElse(t *testing.T) {
	refused := errors.New("DECL_UNAVAILABLE: cannot read the declaration. Nothing was written. Run 'nodes'")
	calls := 0
	err := retryRefused(context.Background(), 3, time.Millisecond, func() error {
		calls++
		if calls == 1 {
			return refused
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("a node that refuses once: err %v after %d calls, want nil after 2", err, calls)
	}
	calls = 0
	unknown := errors.New("the connection broke before the reply")
	if err := retryRefused(context.Background(), 3, time.Millisecond, func() error { calls++; return unknown }); !errors.Is(err, unknown) || calls != 1 {
		t.Fatalf("a write that may have been applied was run %d times (err %v), want exactly once", calls, err)
	}
	calls = 0
	if err := retryRefused(context.Background(), 3, time.Millisecond, func() error { calls++; return refused }); err == nil || calls != 4 {
		t.Fatalf("a node that always refuses: err %v after %d calls, want the error after 4", err, calls)
	}
}
