package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestStatsReportsDeclaredStoresAndAggregates(t *testing.T) {
	s := newTestServer(t)
	if resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore"}, StoreDeclaration{Name: "Photos", Engine: "blobStore"}); !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	ctx := context.Background()
	for d, counts := range [][2]int{{2, 1}, {5, 0}, {0, 1}} {
		key := fmt.Sprintf("p%d", d)
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: key, Data: map[string]interface{}{"title": key}}); err != nil || !resp.Success {
			t.Fatalf("inserting %s: %+v, %v", key, resp, err)
		}
		for n := range counts[0] {
			fieldOK(t)(s.FieldPushCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: "Comments", Data: map[string]interface{}{"n": n}}))
		}
		for n := range counts[1] {
			fieldOK(t)(s.FieldPutCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: "Photos", Element: fmt.Sprintf("ph%d", n), Content: []byte("small")}))
		}
	}
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "Post"})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	want := []StoreStats{{Name: "Comments", Engine: "fifo", Elements: 7}, {Name: "Photos", Engine: "blob", Elements: 2}}
	if !reflect.DeepEqual(stats.Stores, want) {
		t.Fatalf("stats report stores %+v, want %+v (elements over every document; instance states are not elements)", stats.Stores, want)
	}
	if stats.Entries != 3 {
		t.Fatalf("stats report %d entries, want the 3 documents", stats.Entries)
	}
}

func TestShowBucketsListsStoreNames(t *testing.T) {
	s := newPostServer(t)
	createTestBucket(t, s, "app", "Plain", BucketTypeHash)
	listed, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	got := map[string][]string{}
	for _, bucket := range listed.Buckets {
		got[bucket.Name] = bucket.Stores
	}
	want := map[string][]string{"Post": {"Comments", "Ranked", "Tags"}, "Plain": nil}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("show buckets lists stores %v, want %v", got, want)
	}
}
