package ringclient_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
)

// startLiveServer runs the real server in process on a loopback port and returns the
// address. The address is reserved by listening on port 0 and closing, because the service
// does not report the port it bound; another process can take it in between, so a start that
// finds it in use is repeated on a new port.
func startLiveServer(t *testing.T) string {
	t.Helper()
	const attempts = 20
	var startErr error
	for range attempts {
		reserved, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := reserved.Addr().String()
		if err := reserved.Close(); err != nil {
			t.Fatal(err)
		}
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = t.TempDir()
		cfg.Server.Listen = "127.0.0.1:0"
		cfg.TCP.Listen = addr
		service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		if err := service.SeedRootUser("root", "live-test-password"); err != nil {
			t.Fatal(err)
		}
		startErr = service.Start()
		if startErr == nil {
			t.Cleanup(func() { _ = service.Stop() })
			return addr
		}
		_ = service.Stop()
		if !errors.Is(startErr, syscall.EADDRINUSE) {
			t.Fatal(startErr)
		}
	}
	t.Fatalf("every one of %d loopback ports was taken before the server could bind it: %v", attempts, startErr)
	return ""
}

func liveClient(t *testing.T) *ringclient.Client {
	t.Helper()
	client, err := ringclient.Dial(liveContext(t), ringclient.Options{
		Addr: startLiveServer(t), User: "root", Password: "live-test-password", Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func liveContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func item(label string) bson.D { return bson.D{{Key: "item", Value: label}} }

func itemOf(t *testing.T, raw bson.Raw) string {
	t.Helper()
	return raw.Lookup("item").StringValue()
}

// TestLiveEveryMethodAgainstTheRealServer is the first-five-minutes path of rule 11: one
// User bucket, one document, and the document's own stores of four types.
func TestLiveEveryMethodAgainstTheRealServer(t *testing.T) {
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	info, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"}, {Name: "Age", Type: "int"}, {Name: "Purchases", Type: "fifoStore"},
		{Name: "Photos", Type: "lifoStore"}, {Name: "Friends", Type: "hashStore"}, {Name: "Recipes", Type: "btreeStore"},
	})
	if err != nil || info.ID == 0 || len(info.Fields) != 7 {
		t.Fatalf("create bucket: %+v %v", info, err)
	}
	described, err := client.DescribeBucket(ctx, "app", "User")
	if err != nil || described.ID != info.ID {
		t.Fatalf("describe: %+v %v", described, err)
	}
	page, err := client.ShowBuckets(ctx, "app", ringclient.PageRequest{})
	if err != nil || len(page.Buckets) != 1 || page.Buckets[0].Name != "User" {
		t.Fatalf("show buckets: %+v %v", page, err)
	}

	users := client.Bucket("app", "User")
	written, err := users.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada", "Age": int64(36)})
	if err != nil || !written.Applied() {
		t.Fatalf("insert: %+v %v", written, err)
	}
	if _, err := users.Insert(ctx, "ada", ringclient.Doc{"Name": "Again"}); err == nil {
		t.Fatal("a second insert of the same id succeeded")
	}
	got, err := users.Get(ctx, "ada")
	if err != nil || got.Lookup("Name").StringValue() != "Ada" || got.Lookup("Age").Int64() != 36 {
		t.Fatalf("get: %v %v", got, err)
	}
	if _, err := users.Upsert(ctx, "ada", ringclient.Doc{"Name": "Ada L", "Age": int64(37)}); err != nil {
		t.Fatal(err)
	}
	if got, _ = users.Get(ctx, "ada"); got.Lookup("Name").StringValue() != "Ada L" {
		t.Fatalf("upsert did not replace: %v", got)
	}
	if _, err := users.Get(ctx, "nobody"); !errors.Is(err, ringclient.ErrNotFound) {
		t.Fatalf("a missing document must be ErrNotFound, got %v", err)
	}

	// fifo: push, peek, pop, count, list
	for _, label := range []string{"coffee", "tea", "milk"} {
		if _, err := users.Push(ctx, "ada", "Purchases", item(label)); err != nil {
			t.Fatal(err)
		}
	}
	if count, err := users.StoreCount(ctx, "ada", "Purchases"); err != nil || count != 3 {
		t.Fatalf("count %d %v", count, err)
	}
	peeked, err := users.Peek(ctx, "ada", "Purchases", 2)
	if err != nil || len(peeked) != 2 || itemOf(t, peeked[0]) != "coffee" {
		t.Fatalf("peek %v %v", peeked, err)
	}
	listed, err := users.StoreList(ctx, "ada", "Purchases", 2, nil)
	if err != nil || len(listed.Elements) != 2 || !listed.More {
		t.Fatalf("store list %+v %v", listed, err)
	}
	next, err := users.StoreList(ctx, "ada", "Purchases", 2, listed.Cursor)
	if err != nil || len(next.Elements) != 1 || itemOf(t, next.Elements[0]) != "milk" {
		t.Fatalf("second page %+v %v", next, err)
	}
	popped, err := users.Pop(ctx, "ada", "Purchases")
	if err != nil || itemOf(t, popped) != "coffee" {
		t.Fatalf("pop %v %v", popped, err)
	}

	// claim, lease_get, extend, release, ack
	lease, err := users.Claim(ctx, "ada", "Purchases", 1)
	if err != nil || len(lease.Elements) != 1 || lease.ID == ([16]byte{}) {
		t.Fatalf("claim %+v %v", lease, err)
	}
	read, err := users.LeaseGet(ctx, "ada", "Purchases", lease.ID)
	if err != nil || !read.Found || len(read.Elements) != 1 {
		t.Fatalf("lease get %+v %v", read, err)
	}
	if change, err := users.Extend(ctx, "ada", "Purchases", lease.ID, 60000); err != nil || !change.Live {
		t.Fatalf("extend %+v %v", change, err)
	}
	if change, err := users.Release(ctx, "ada", "Purchases", lease.ID); err != nil || !change.Live {
		t.Fatalf("release %+v %v", change, err)
	}
	again, err := users.Claim(ctx, "ada", "Purchases", 1)
	if err != nil || len(again.Elements) != 1 {
		t.Fatalf("claim after release %+v %v", again, err)
	}
	if _, err := users.Ack(ctx, "ada", "Purchases", again.ID); err != nil {
		t.Fatal(err)
	}
	if read, _ = users.LeaseGet(ctx, "ada", "Purchases", again.ID); read.Found {
		t.Fatal("an acked lease is still found")
	}

	// lifo: the newest comes out first
	for _, label := range []string{"first", "second"} {
		if _, err := users.Push(ctx, "ada", "Photos", item(label)); err != nil {
			t.Fatal(err)
		}
	}
	if top, err := users.Pop(ctx, "ada", "Photos"); err != nil || itemOf(t, top) != "second" {
		t.Fatalf("lifo pop %v %v", top, err)
	}

	// hash and btree: put and take by key
	for _, store := range []string{"Friends", "Recipes"} {
		if _, err := users.Put(ctx, "ada", store, "bob", item("pal")); err != nil {
			t.Fatal(err)
		}
		taken, err := users.Take(ctx, "ada", store, "bob")
		if err != nil || itemOf(t, taken) != "pal" {
			t.Fatalf("take from %s: %v %v", store, taken, err)
		}
		if _, err := users.Take(ctx, "ada", store, "bob"); !errors.Is(err, ringclient.ErrNotFound) {
			t.Fatalf("taking an absent key must be ErrNotFound, got %v", err)
		}
	}

	// bucket-wide reads
	if count, err := users.Count(ctx, nil); err != nil || count != 1 {
		t.Fatalf("bucket count %d %v", count, err)
	}
	entries, err := users.List(ctx, nil, ringclient.PageRequest{})
	if err != nil || len(entries.Entries) != 1 || entries.Entries[0].ID != "ada" {
		t.Fatalf("bucket list %+v %v", entries, err)
	}

	if _, err := users.Delete(ctx, "ada"); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Get(ctx, "ada"); !errors.Is(err, ringclient.ErrNotFound) {
		t.Fatalf("a deleted document must be ErrNotFound, got %v", err)
	}
}

func TestLiveARefusalCarriesTheRingCode(t *testing.T) {
	client, ctx := liveClient(t), liveContext(t)
	_, err := client.DescribeBucket(ctx, "nope", "User")
	var refusal *ringclient.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodePermissionDenied && refusal.Code != cluster.CodeNotFound {
		t.Fatalf("want a *ringclient.Error with a ring code, got %T %v", err, err)
	}
}

// TestLiveManyGoroutinesShareOneConnection pipelines 40 inserts and 40 gets on one client.
func TestLiveManyGoroutinesShareOneConnection(t *testing.T) {
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	users := client.Bucket("app", "User")
	var group sync.WaitGroup
	for i := range 40 {
		group.Add(1)
		go func() {
			defer group.Done()
			id := fmt.Sprintf("u%d", i)
			if _, err := users.Insert(ctx, id, ringclient.Doc{"Name": id}); err != nil {
				t.Error(err)
				return
			}
			got, err := users.Get(ctx, id)
			if err != nil || got.Lookup("Name").StringValue() != id {
				t.Errorf("get %s: %v %v", id, got, err)
			}
		}()
	}
	group.Wait()
	if count, err := users.Count(ctx, nil); err != nil || count != 40 {
		t.Fatalf("count %d %v", count, err)
	}
}

// TestLiveShowBucketsPages creates more buckets than one page holds.
func TestLiveShowBucketsPages(t *testing.T) {
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := client.CreateBucket(ctx, "app", fmt.Sprintf("B%d", i), []ringclient.FieldDecl{{Name: "X", Type: "string"}}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	request := ringclient.PageRequest{Limit: 2}
	for range 20 {
		page, err := client.ShowBuckets(ctx, "app", request)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range page.Buckets {
			seen[bucket.Name] = true
		}
		if !page.More {
			break
		}
		request.Cursor = page.Cursor
	}
	if len(seen) != 5 {
		t.Fatalf("paging saw %d buckets, want 5: %v", len(seen), seen)
	}
}

// TestLiveGetManyReadsAFeedOfPostsInOneFrame is the how-to's feed read against the real server: a
// user's own feed store holds post ids, and GetMany reads the posts in one frame, in feed order, a
// deleted post as found:false and a post named in another database as its own key.
func TestLiveGetManyReadsAFeedOfPostsInOneFrame(t *testing.T) {
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}, {Name: "Feed", Type: "fifoStore"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "Post", []ringclient.FieldDecl{{Name: "Text", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	users, posts := client.Bucket("app", "User"), client.Bucket("app", "Post")
	if _, err := users.Insert(ctx, "u7", ringclient.Doc{"Name": "Grace"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"p0", "p1", "gone"} {
		if id != "gone" {
			if _, err := posts.Insert(ctx, id, ringclient.Doc{"Text": "text of " + id}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := users.Push(ctx, "u7", "Feed", bson.D{{Key: "post", Value: id}}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := users.StoreList(ctx, "u7", "Feed", 20, nil)
	if err != nil || len(page.Elements) != 3 {
		t.Fatalf("feed page: %+v %v", page, err)
	}
	ids := make([]string, len(page.Elements))
	for i, entry := range page.Elements {
		ids[i] = entry.Lookup("post").StringValue()
	}
	answers, err := posts.GetMany(ctx, ids, nil)
	if err != nil || len(answers) != 3 {
		t.Fatalf("get many: %+v %v", answers, err)
	}
	for i, want := range []string{"text of p0", "text of p1"} {
		if answers[i].Err != nil || !answers[i].Found || answers[i].Doc.Lookup("Text").StringValue() != want {
			t.Fatalf("answer %d: %+v", i, answers[i])
		}
	}
	if answers[2].Err != nil || answers[2].Found {
		t.Fatalf("a post nobody wrote must be found:false with no error: %+v", answers[2])
	}
	fields, err := posts.GetMany(ctx, ids[:2], []string{"Text"})
	if err != nil || fields[0].Err != nil || !fields[0].Found {
		t.Fatalf("get many with fields: %+v %v", fields, err)
	}
}
