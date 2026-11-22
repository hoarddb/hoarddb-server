package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Nested buckets step 11: a bucket's store fields survive a dump and a
// restore into a fresh server — every instance's list and pop order — over
// the real wire protocol.

func mustSend(t *testing.T, c *CLI, op transport.Opcode, req, resp any) {
	t.Helper()
	if err := c.handler.send(op, req, resp); err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

func seedFieldStoreBucket(t *testing.T, c *CLI) {
	t.Helper()
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	var created server.CreateBucketResponse
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{
			map[string]interface{}{"name": "Comments", "engine": "fifoStore"},
			map[string]interface{}{"name": "Ranked", "engine": "heapStore"},
			map[string]interface{}{"name": "Tags", "engine": "hashStore"},
		},
	}}, &created)
	if !created.Success {
		t.Fatalf("creating Post: %s", created.Message)
	}
	for d := range 5 {
		key := fmt.Sprintf("p%d", d)
		mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: key, Data: map[string]interface{}{"title": key}}, nil)
		for n := range d + 2 {
			mustSend(t, c, transport.OpFieldPush, &server.FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: "Comments", Data: map[string]interface{}{"n": n}}, nil)
			mustSend(t, c, transport.OpFieldPush, &server.FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: "Ranked", Priority: int64((n * 7) % 5), Data: map[string]interface{}{"n": n}}, nil)
			mustSend(t, c, transport.OpFieldPut, &server.FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: "Tags", Element: fmt.Sprintf("t%d", n), Data: map[string]interface{}{"n": n}}, nil)
		}
		// One pop, so the restored fifo's positions differ from the source's
		// and only the order can be compared.
		mustSend(t, c, transport.OpFieldPop, &server.FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: "Comments"}, nil)
	}
}

// fieldState is what one document's stores hold, decoded: each list in
// engine order, then each queue's pop order.
func fieldState(t *testing.T, c *CLI) map[string][]string {
	t.Helper()
	state := make(map[string][]string)
	for d := range 5 {
		key := fmt.Sprintf("p%d", d)
		for _, field := range []string{"Comments", "Ranked", "Tags"} {
			var page server.FieldListResponse
			mustSend(t, c, transport.OpFieldList, &server.FieldListRequest{FieldRequest: server.FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: field}}, &page)
			var listed []string
			for _, entry := range page.Entries {
				listed = append(listed, fmt.Sprintf("%s/%s/%s/%d", entry.ElementKey, priorityText(entry.Priority), entry.EntryID, entry.Element.Lookup("n").Int32()))
			}
			state[key+"."+field+" list"] = listed
		}
		for _, field := range []string{"Comments", "Ranked"} {
			var popped []string
			for {
				var resp server.FieldResponse
				mustSend(t, c, transport.OpFieldPop, &server.FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: field}, &resp)
				if !resp.Found {
					break
				}
				popped = append(popped, fmt.Sprint(resp.Items[0].Element.Lookup("n").Int32()))
			}
			state[key+"."+field+" pops"] = popped
		}
	}
	return state
}

func TestDumpRestoreRoundTripsFieldStores(t *testing.T) {
	source := newDumpTestServer(t)
	seedFieldStoreBucket(t, source)
	outDir := filepath.Join(t.TempDir(), "dump")
	summary, err := runDumpBuckets(source.handler, "app", "Post", outDir, io.Discard, nil)
	if err != nil || summary.incomplete {
		t.Fatalf("dumping Post: %+v, %v", summary, err)
	}
	metaRaw, err := os.ReadFile(filepath.Join(outDir, "app", "Post.metadata.json"))
	if err != nil {
		t.Fatalf("reading the metadata: %v", err)
	}
	var meta bucketMetadataFile
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatalf("parsing the metadata: %v", err)
	}
	// 5 documents; per document d, d+2 elements in each of 3 fields (60),
	// less one popped comment each.
	if meta.FormatVersion != 3 || meta.Documents != 5 || meta.Elements != 60-5 || len(meta.Stores) != 3 {
		t.Fatalf("the metadata reads format %d, %d documents, %d elements, %d stores; want 3, 5, 55, 3", meta.FormatVersion, meta.Documents, meta.Elements, len(meta.Stores))
	}
	storesRaw, err := os.ReadFile(filepath.Join(outDir, "app", "Post.stores.bson"))
	if err != nil {
		t.Fatalf("reading the stores file: %v", err)
	}
	if first := splitBSONDocumentsForTest(t, storesRaw)[0]; bson.Raw(first).Lookup("field").StringValue() == "" {
		t.Fatalf("the first record of the stores file names no field: %v", bson.Raw(first))
	}

	target := newDumpTestServer(t)
	restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil)
	if err != nil || len(restored.failures) != 0 {
		t.Fatalf("restoring Post: %+v, %v", restored, err)
	}
	want, got := fieldState(t, source), fieldState(t, target)
	for key := range want {
		if len(want[key]) == 0 {
			t.Fatalf("the source's %s is empty: the fixture tests nothing there", key)
		}
	}
	// The lists carry no fifo position — a restore assigns positions afresh,
	// and the source's popped comment makes them differ — only the order.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after restore the store fields read\n%v\nwant the source's\n%v", got, want)
	}
}

// flagsByComment reads, in pop order, each remaining comment's n and the
// flags on it, addressing each comment by the position list reports.
func flagsByComment(t *testing.T, c *CLI) []string {
	t.Helper()
	var page server.FieldListResponse
	mustSend(t, c, transport.OpFieldList, &server.FieldListRequest{FieldRequest: server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments"}}, &page)
	var described []string
	for _, comment := range page.Entries {
		var flags server.FieldListResponse
		mustSend(t, c, transport.OpFieldList, &server.FieldListRequest{FieldRequest: server.FieldRequest{
			Database: "app", Bucket: "Post", Key: "p1", Field: "Flags", Path: []interface{}{"Comments", *comment.Position},
		}}, &flags)
		line := fmt.Sprintf("comment %d:", comment.Element.Lookup("n").Int32())
		for _, flag := range flags.Entries {
			line += fmt.Sprintf(" %d", flag.Element.Lookup("flag").Int32())
		}
		described = append(described, line)
	}
	return described
}

// TestDumpRestoreRoundTripsL3: an L3 element is dumped with the L2 element
// it hangs on, and restored onto that element even though the restore gives
// the fifo's elements new positions.
func TestDumpRestoreRoundTripsL3(t *testing.T) {
	source := newDumpTestServer(t)
	mustSend(t, source, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	var created server.CreateBucketResponse
	mustSend(t, source, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{map[string]interface{}{"name": "Comments", "engine": "fifoStore",
			"fields": []interface{}{map[string]interface{}{"name": "Flags", "engine": "fifoStore"}}}},
	}}, &created)
	if !created.Success {
		t.Fatalf("creating Post: %s", created.Message)
	}
	mustSend(t, source, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)
	for n := range 4 {
		mustSend(t, source, transport.OpFieldPush, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": n}}, nil)
	}
	// Popping comment 0 leaves positions 1..3, which a restore renumbers 0..2.
	mustSend(t, source, transport.OpFieldPop, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments"}, nil)
	for position, flags := range map[int64][]int{1: {10, 11}, 3: {30}} {
		for _, flag := range flags {
			mustSend(t, source, transport.OpFieldPush, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Flags",
				Path: []interface{}{"Comments", position}, Data: map[string]interface{}{"flag": flag}}, nil)
		}
	}
	want := flagsByComment(t, source)
	if fmt.Sprint(want) != "[comment 1: 10 11 comment 2: comment 3: 30]" {
		t.Fatalf("the source's flags read %q: the fixture is not what the test assumes", want)
	}

	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(source.handler, "app", "Post", outDir, io.Discard, nil); err != nil || summary.incomplete {
		t.Fatalf("dumping Post: %+v, %v", summary, err)
	}
	target := newDumpTestServer(t)
	if restored, err := runRestoreBuckets(target.handler, restoreCLIOptions{Dir: outDir}, io.Discard, nil); err != nil || len(restored.failures) != 0 {
		t.Fatalf("restoring Post: %+v, %v", restored, err)
	}
	if got := flagsByComment(t, target); !reflect.DeepEqual(got, want) {
		t.Fatalf("after restore the flags read %q, want %q: an L3 element landed on another comment", got, want)
	}
}

// priorityText is a list entry's priority as text, "-" for an engine that has
// none, so a nil pointer never prints as an address.
func priorityText(priority *int64) string {
	if priority == nil {
		return "-"
	}
	return strconv.FormatInt(*priority, 10)
}
