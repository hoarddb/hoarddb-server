package wire

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

func u67ListAll(t *testing.T, node *Node, field string, limit int) []string {
	t.Helper()
	var items []string
	var after []byte
	for pages := 0; ; pages++ {
		if pages > 50 {
			t.Fatal("the cursor never ended")
		}
		page, err := node.ListField(context.Background(), FieldListRequest{StoreTarget: u62Target(field), After: after, Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		if page.More != (len(page.Cursor) > 0) || len(page.Elements) > max(limit, 100) {
			t.Fatalf("page %d: %d elements, more %v, cursor %d bytes", pages, len(page.Elements), page.More, len(page.Cursor))
		}
		for _, element := range page.Elements {
			items = append(items, u62Item(t, element))
		}
		if !page.More {
			return items
		}
		after = page.Cursor
	}
}

// TestU67ListFieldPagesEveryEngine: [GUARD ListField reads the store's own order and cursor].
func TestU67ListFieldPagesEveryEngine(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b", "c", "d", "e")
	u62Push(t, node, "Stack", "s1", "s2", "s3")
	for i, label := range []string{"low", "high", "mid"} {
		err := node.Push(ctx, PushRequest{StoreTarget: u62Target("Ranked"), Element: u63Item(label), Priority: int64([]int{1, 9, 5}[i]), EntryID: fmt.Sprintf("e%d", i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"k3", "k1", "k2"} {
		u63Put(t, node, "Table", key, "v-"+key)
		u63Put(t, node, "Tree", key, "t-"+key)
	}
	before := u63Position(node)
	for field, want := range map[string]string{
		"Queue": "a,b,c,d,e", "Stack": "s1,s2,s3", "Ranked": "high,mid,low", "Table": "v-k1,v-k2,v-k3", "Tree": "t-k1,t-k2,t-k3",
	} {
		for _, limit := range []int{1, 2, 0} {
			if got := strings.Join(u67ListAll(t, node, field, limit), ","); got != want {
				t.Errorf("%s at limit %d lists %s, want %s", field, limit, got, want)
			}
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("a list moved the pair from %+v to %+v", before, after)
	}
}

// TestU67CountFieldReadsTheStateCount: [GUARD CountField reads the field's own count].
func TestU67CountFieldReadsTheStateCount(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	count := func(field string) int64 {
		t.Helper()
		total, err := node.CountField(ctx, u62Target(field))
		if err != nil {
			t.Fatalf("CountField %s: %v", field, err)
		}
		return total
	}
	if got := count("Queue"); got != 0 {
		t.Fatalf("an empty field counts %d", got)
	}
	u62Push(t, node, "Queue", "a", "b", "c")
	u63Put(t, node, "Table", "k", "v")
	if got := count("Queue"); got != 3 || count("Table") != 1 || count("Files") != 0 {
		t.Fatalf("counts: Queue %d Table %d Files %d", count("Queue"), count("Table"), count("Files"))
	}
	if _, err := node.Pop(ctx, PopRequest{StoreTarget: u62Target("Queue")}); err != nil {
		t.Fatal(err)
	}
	if got := count("Queue"); got != 2 {
		t.Fatalf("after a pop the field counts %d, want 2", got)
	}
}

// TestU67FieldReadRefusals: [GUARD fieldCall for list and count, the limit and the cursor shape].
func TestU67FieldReadRefusals(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b")
	_, err := node.ListField(ctx, FieldListRequest{StoreTarget: u62Target("Files")})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "the field is a blobStore") || !strings.Contains(err.Error(), "count") {
		t.Errorf("list of a blobStore: %q, want the type and what does fit", err)
	}
	_, err = node.ListField(ctx, FieldListRequest{StoreTarget: u62Target("Name")})
	u30iCode(t, err, cluster.CodeFieldType)
	_, err = node.CountField(ctx, u62Target("Name"))
	u30iCode(t, err, cluster.CodeFieldType)
	missing := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "ghost", Field: "Queue"}
	_, err = node.ListField(ctx, FieldListRequest{StoreTarget: missing})
	u30iCode(t, err, cluster.CodeNotFound)
	_, err = node.CountField(ctx, missing)
	u30iCode(t, err, cluster.CodeNotFound)
	for name, limit := range map[string]int{"negative": -1, "above 1000": 1001} {
		_, err = node.ListField(ctx, FieldListRequest{StoreTarget: u62Target("Queue"), Limit: limit})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "1000") || !strings.Contains(err.Error(), "Nothing was read") {
			t.Errorf("%s limit: %q", name, err)
		}
	}
	_, err = node.ListField(ctx, FieldListRequest{StoreTarget: u62Target("Queue"), After: bytes.Repeat([]byte{1}, 5)})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "cursor") {
		t.Errorf("a cursor of the wrong width for a fifoStore: %q", err)
	}
	var ref = u63Field(t, node, "Queue", store.FieldFIFO)
	if len(ref.Owner) == 0 {
		t.Fatal("no owner")
	}
	if err := node.Delete(ctx, u30iDB, "Box", "b1"); err != nil {
		t.Fatal(err)
	}
	_, err = node.ListField(ctx, FieldListRequest{StoreTarget: u62Target("Queue")})
	u30iCode(t, err, cluster.CodeNotFound)
}
