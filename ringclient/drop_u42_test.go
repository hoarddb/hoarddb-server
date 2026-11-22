package ringclient_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestLiveDropBucketDeletesTheBucketAndFreesTheName: the first-five-minutes drop against the
// real server. The bucket and its documents are gone, a second drop is NOT_FOUND with the fix,
// the cached header of the dead bucket is forgotten, and the name can be created again as a
// new, empty bucket.
func TestLiveDropBucketDeletesTheBucketAndFreesTheName(t *testing.T) {
	addr := startLiveServer(t)
	ctx := liveContext(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	created, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}})
	if err != nil {
		t.Fatal(err)
	}
	users := client.Bucket("app", "User")
	if _, err := users.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}

	if err := client.DropBucket(ctx, "app", "User"); err != nil {
		t.Fatalf("DropBucket: %v", err)
	}
	// The documents go in the background; the bucket is gone, and the name free, when a drop is
	// NOT_FOUND again.
	var refusal *ringclient.Error
	deadline := time.Now().Add(20 * time.Second)
	for {
		err := client.DropBucket(ctx, "app", "User")
		if errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("a repeated drop = %v, want the bucket to finish dropping and then be NOT_FOUND", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	page, err := client.ShowBuckets(ctx, "app", ringclient.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, bucket := range page.Buckets {
		if bucket.Name == "User" {
			t.Fatalf("the dropped bucket is still listed: %+v", bucket)
		}
	}
	again, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}})
	if err != nil {
		t.Fatalf("the name is not free after the drop: %v", err)
	}
	if again.ID == created.ID {
		t.Fatal("the re-created bucket has the dropped bucket's id")
	}
	if _, err := client.Bucket("app", "User").Get(ctx, "ada"); err == nil {
		t.Fatal("a document of the dropped bucket came back from the new one")
	}
}

// TestLiveDropBucketExpectingRefusesAnotherBucket: a caller holding the id of the bucket it means is
// refused when the name carries another one, and the right id drops it.
func TestLiveDropBucketExpectingRefusesAnotherBucket(t *testing.T) {
	addr := startLiveServer(t)
	ctx := liveContext(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	info, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}})
	if err != nil {
		t.Fatal(err)
	}
	var refusal *ringclient.Error
	if err := client.DropBucketExpecting(ctx, "app", "User", info.ID+1); !errors.As(err, &refusal) || refusal.Code != cluster.CodeNotFound {
		t.Fatalf("a drop expecting another id = %v, want NOT_FOUND", err)
	}
	if described, err := client.DescribeBucket(ctx, "app", "User"); err != nil || described.State != "active" {
		t.Fatalf("the bucket after a refused drop: %+v %v", described, err)
	}
	if err := client.DropBucketExpecting(ctx, "app", "User", info.ID); err != nil {
		t.Fatalf("a drop expecting the right id: %v", err)
	}
}
