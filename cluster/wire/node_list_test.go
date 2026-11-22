package wire

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u67Users inserts n users u00..: every third one lives in Bergen, the rest in Oslo.
func u67Users(t *testing.T, node *Node, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		city := "Oslo"
		if i%3 == 0 {
			city = "Bergen"
		}
		doc := bson.D{{Key: "_id_", Value: fmt.Sprintf("u%02d", i)}, {Key: "Name", Value: "name"}, {Key: "City", Value: city}}
		if _, err := node.Insert(context.Background(), u30iDB, "User", doc); err != nil {
			t.Fatal(err)
		}
	}
}

func u67Ids(t *testing.T, entries []cluster.ListEntry) []string {
	t.Helper()
	var ids []string
	for _, entry := range entries {
		ids = append(ids, bson.Raw(entry.Body).Lookup("_id_").StringValue())
	}
	return ids
}

// TestU67CountHonoursTheFilterLikeList: [GUARD Count passes the filter to the scanner].
func TestU67CountHonoursTheFilterLikeList(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	empty, err := node.Count(ctx, CountRequest{Database: u30iDB, Bucket: "User"})
	if err != nil || empty != 0 {
		t.Fatalf("Count of an empty bucket = %d, %v; want 0", empty, err)
	}
	u67Users(t, node, 12)
	all, err := node.Count(ctx, CountRequest{Database: u30iDB, Bucket: "User"})
	if err != nil || all != 12 {
		t.Fatalf("Count = %d, %v; want 12", all, err)
	}
	filter := bson.D{{Key: "City", Value: "Bergen"}}
	bergen, err := node.Count(ctx, CountRequest{Database: u30iDB, Bucket: "User", Filter: filter})
	listed, listErr := node.List(ctx, u30iDB, "User", filter, 0)
	if err != nil || listErr != nil || bergen != 4 || int(bergen) != len(listed) {
		t.Fatalf("Count(City=Bergen) = %d, %v; List found %d, %v; want 4 and equal", bergen, err, len(listed), listErr)
	}
	_, err = node.Count(ctx, CountRequest{Database: u30iDB, Bucket: "User", Filter: bson.D{{Key: "Shoe", Value: int32(1)}}})
	u30iCode(t, err, cluster.CodeFieldType)
	_, err = node.Count(ctx, CountRequest{Database: u30iDB, Bucket: "Nope"})
	if err == nil {
		t.Fatal("Count of an unknown bucket succeeded")
	}
	_, err = node.Count(ctx, CountRequest{Database: "nodb", Bucket: "User"})
	u30iCode(t, err, cluster.CodeNotFound)
	if err := node.Delete(ctx, u30iDB, "User", "u01"); err != nil {
		t.Fatal(err)
	}
	if after, err := node.Count(ctx, CountRequest{Database: u30iDB, Bucket: "User"}); err != nil || after != 11 {
		t.Fatalf("Count after a delete = %d, %v; want 11", after, err)
	}
}

// TestU67ListPagesWithoutDuplicateOrGap: [GUARD the cursor names the last emitted entry].
func TestU67ListPagesWithoutDuplicateOrGap(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	u67Users(t, node, 11)
	var seen []string
	var cursor []byte
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("the cursor never ended")
		}
		page, err := node.ListPage(ctx, ListPageRequest{Database: u30iDB, Bucket: "User", Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) > 3 || (page.More != (len(page.Cursor) > 0)) {
			t.Fatalf("page %d: %d entries, more %v, cursor %d bytes", pages, len(page.Entries), page.More, len(page.Cursor))
		}
		seen = append(seen, u67Ids(t, page.Entries)...)
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	whole, err := node.List(ctx, u30iDB, "User", nil, 0)
	if err != nil || len(whole) != 11 || len(seen) != 11 {
		t.Fatalf("List %d, %v; pages saw %d", len(whole), err, len(seen))
	}
	for i, doc := range whole {
		if got := bson.Raw(doc.Body).Lookup("_id_").StringValue(); got != seen[i] {
			t.Fatalf("page order differs from List at %d: %s vs %s", i, seen[i], got)
		}
	}
}

// TestU67ListPageFilterAndDefaultLimit: [GUARD the filter reaches the scanner; limit 0 is the default].
func TestU67ListPageFilterAndDefaultLimit(t *testing.T) {
	node, _ := u30iFresh(t)
	u67Users(t, node, 12)
	page, err := node.ListPage(context.Background(), ListPageRequest{Database: u30iDB, Bucket: "User", Filter: bson.D{{Key: "City", Value: "Bergen"}}})
	if err != nil || len(page.Entries) != 4 || page.More || page.Cursor != nil {
		t.Fatalf("ListPage(City=Bergen) = %d entries, more %v, %v; want 4 and no more", len(page.Entries), page.More, err)
	}
	for _, entry := range page.Entries {
		if bson.Raw(entry.Body).Lookup("City").StringValue() != "Bergen" {
			t.Fatalf("an entry outside the filter: %v", bson.Raw(entry.Body))
		}
	}
}

// TestU67ListPageRefusals: [GUARD limit bounds and cursor binding].
func TestU67ListPageRefusals(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	u67Users(t, node, 5)
	for name, tc := range map[string]struct {
		req  ListPageRequest
		said string
	}{
		"negative limit": {ListPageRequest{Database: u30iDB, Bucket: "User", Limit: -1}, "not a page size"},
		"limit too big":  {ListPageRequest{Database: u30iDB, Bucket: "User", Limit: 10001}, "10000"},
		"garbage cursor": {ListPageRequest{Database: u30iDB, Bucket: "User", Cursor: []byte("not a cursor at all, no")}, "cursor"},
	} {
		_, err := node.ListPage(ctx, tc.req)
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), tc.said) {
			t.Errorf("%s: %q does not say %q", name, err, tc.said)
		}
	}
	first, err := node.ListPage(ctx, ListPageRequest{Database: u30iDB, Bucket: "User", Limit: 2})
	if err != nil || !first.More {
		t.Fatalf("first page %+v, %v", first, err)
	}
	flipped := bytes.Clone(first.Cursor)
	flipped[len(flipped)-6] ^= 0xFF
	_, err = node.ListPage(ctx, ListPageRequest{Database: u30iDB, Bucket: "User", Cursor: flipped})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if _, err := node.CreateBucket(ctx, u30iDB, "Other", Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	_, err = node.ListPage(ctx, ListPageRequest{Database: u30iDB, Bucket: "Other", Cursor: first.Cursor})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "bucket id") {
		t.Errorf("a cursor of another bucket: %q, want it to name the bucket id", err)
	}
	_, err = node.ListPage(ctx, ListPageRequest{Database: u30iDB, Bucket: "User", Filter: bson.D{{Key: "Shoe", Value: int32(1)}}})
	u30iCode(t, err, cluster.CodeFieldType)
}

// TestU67ListPageByteCeilingIsAStopAndNeverASplit: [GUARD the ceiling stops the page at a
// whole document; a first document over it is RESULT_TOO_LARGE].
func TestU67ListPageByteCeilingIsAStopAndNeverASplit(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	u67Users(t, node, 6)
	ref, err := node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	one, err := node.listPage(ctx, pageRequest{ref: ref, limit: 1, ceiling: 1 << 20})
	if err != nil || len(one.Entries) != 1 {
		t.Fatal(one, err)
	}
	width := int64(len(cluster.AppendEntry(nil, one.Entries[0])))
	var seen []string
	var cursor []byte
	for pages := 0; ; pages++ {
		page, err := node.listPage(ctx, pageRequest{ref: ref, limit: 100, cursor: cursor, ceiling: width*5/2 + 8})
		if err != nil || (page.More && len(page.Entries) != 2) || len(page.Entries) > 2 {
			t.Fatalf("page %d = %d entries, %v; want 2 per page at a ceiling of 2.5 documents", pages, len(page.Entries), err)
		}
		seen = append(seen, u67Ids(t, page.Entries)...)
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	if len(seen) != 6 {
		t.Fatalf("the pages saw %v, want all 6", seen)
	}
	_, err = node.listPage(ctx, pageRequest{ref: ref, limit: 100, ceiling: width - 1})
	u30iCode(t, err, cluster.CodeResultTooLarge)
	if !strings.Contains(err.Error(), `.get()`) {
		t.Errorf("RESULT_TOO_LARGE %q, want the by-key read named", err)
	}
}
