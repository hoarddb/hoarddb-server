package wire

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// TestExpandOverAFieldTheCallsOwnFieldsRemovedIsRefused: [GUARD a list or a get_fields whose fields
// leave out the expanded field is refused, not answered "missing" for every row].
func TestExpandOverAFieldTheCallsOwnFieldsRemovedIsRefused(t *testing.T) {
	world := expandServe(t)
	spec := RingExpandSpec{Field: "Author", Bucket: "User", Fields: []string{"Name"}}
	_, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message", Fields: []string{"Text"},
		Options: RingReadOptions{Expand: []RingExpandSpec{spec}}})
	if text := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(text, `"Author"`) || !strings.Contains(text, "fields") {
		t.Fatalf("the list refusal does not name Author and fields: %s", text)
	}
	message := world.bucket(t, "Message")
	encID, _ := store.EncodeComponent("m3")
	_, err = world.server.read(context.Background(), message, RingRead{Class: RingClassGetFields, EncID: encID, Fields: []string{"Text"},
		Options: RingReadOptions{Expand: []RingExpandSpec{spec}}})
	refusalText(t, err, cluster.CodeInvalidArgument)
	// With the field in the list the call answers.
	reply, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message", Fields: []string{"Author"},
		Options: RingReadOptions{Expand: []RingExpandSpec{spec}}})
	if err != nil || len(reply.Expanded) != 6 {
		t.Fatalf("list with Author in fields = %d expansions, %v", len(reply.Expanded), err)
	}
	if got := world.server.node.ExpandStats().Reads.Load(); got != 5 {
		t.Fatalf("%d far reads: the refused calls read documents", got)
	}
}

// TestExpandWhereNamesOnlyFieldsTheExpansionReturns: [GUARD a where key the far fields leave out,
// or the far bucket does not declare, is refused, not a silent empty page].
func TestExpandWhereNamesOnlyFieldsTheExpansionReturns(t *testing.T) {
	world := expandServe(t)
	where := func(key string) []byte {
		raw, err := bson.Marshal(bson.D{{Key: key, Value: int32(21)}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	run := func(fields []string, key string) error {
		_, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message",
			Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: fields, Where: where(key)}}}})
		return err
	}
	if text := refusalText(t, run([]string{"Name"}, "Age"), cluster.CodeInvalidArgument); !strings.Contains(text, `"Age"`) || !strings.Contains(text, "leave it out") {
		t.Fatalf("a where outside the fields: %s", text)
	}
	if text := refusalText(t, run(nil, "Agee"), cluster.CodeInvalidArgument); !strings.Contains(text, `"Agee"`) || !strings.Contains(text, "declares no scalar field") {
		t.Fatalf("a where on an undeclared field: %s", text)
	}
	if err := run([]string{"Age"}, "Age"); err != nil {
		t.Fatalf("a where on a returned field was refused: %v", err)
	}
	if err := run(nil, "Age"); err != nil {
		t.Fatalf("a where on a default field was refused: %v", err)
	}
}

// TestExpandFilteredCountsEachDroppedRowOnce: [GUARD a row after the cut is counted on the page
// that returns it or drops it, not on the page before as well].
func TestExpandFilteredCountsEachDroppedRowOnce(t *testing.T) {
	world := expandServe(t)
	where, err := bson.Marshal(bson.D{{Key: "Age", Value: int32(22)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, ceiling := range []int64{0, 380, 600} {
		world.server.expandBytes = ceiling
		var cursor []byte
		kept, filtered, pages := 0, int32(0), 0
		for {
			reply, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message", Cursor: cursor,
				Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: []string{"Age"}, Where: where}}}})
			if err != nil {
				t.Fatal(err)
			}
			pages++
			kept += len(reply.Expanded)
			if reply.ExpandFiltered != nil {
				filtered += *reply.ExpandFiltered
			}
			if !reply.More {
				break
			}
			cursor = reply.Cursor
			if pages > 20 {
				t.Fatal("paging does not end")
			}
		}
		// Messages m0 and m2 are by u2, who is 22; the other four rows are dropped.
		if kept != 2 || filtered != 4 {
			t.Errorf("ceiling %d over %d pages: %d kept and %d counted filtered, want 2 and 4", ceiling, pages, kept, filtered)
		}
		if ceiling == 380 && pages < 2 {
			t.Errorf("ceiling %d did not cut the page, so the test proves nothing", ceiling)
		}
	}
}

// TestLocalWithoutExpandOnAListIsRefused: [GUARD local alone on a store list or a bucket list is
// refused, not accepted and inert, and not turned into an expansion].
func TestLocalWithoutExpandOnAListIsRefused(t *testing.T) {
	world := expandServe(t)
	room := world.bucket(t, "Room")
	encID, _ := store.EncodeComponent("r1")
	fieldID, _ := room.storeField("Members")
	_, err := world.server.read(context.Background(), room, RingRead{Class: RingClassStoreList, EncID: encID, FieldID: fieldID, Limit: 10,
		Options: RingReadOptions{Local: true}})
	if text := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(text, "local") || !strings.Contains(text, "drop local") {
		t.Fatalf("store list: %s", text)
	}
	_, err = world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message", Options: RingReadOptions{Local: true}})
	refusalText(t, err, cluster.CodeInvalidArgument)
	if _, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message",
		Options: RingReadOptions{Local: true, Expand: []RingExpandSpec{{Field: "Author", Bucket: "User"}}}}); err != nil {
		t.Fatalf("local with expand was refused: %v", err)
	}
}

// TestExpandOverAStoreOrUndeclaredFieldIsRefused: [GUARD get expand {Members: true} reads a store
// marker as an id and said "missing" for the row; it is refused naming the field].
func TestExpandOverAStoreOrUndeclaredFieldIsRefused(t *testing.T) {
	world := expandServe(t)
	room := world.bucket(t, "Room")
	for field, want := range map[string]string{"Members": "holds elements", "Nope": "declares no field"} {
		err := checkExpandNearFields(room, nil, []RingExpandSpec{{Field: field, Bucket: "User"}})
		if text := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(text, field) || !strings.Contains(text, want) {
			t.Errorf("expand over %s: %s", field, text)
		}
	}
}

// TestExpandCallsCountOncePerCall: [GUARD a stale-epoch retry is not a second call].
func TestExpandCallsCountOncePerCall(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	inner := world.server.expandFetch
	var calls int
	world.server.expandFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		calls++
		if calls == 1 {
			return Document{}, &cluster.Error{Code: cluster.CodeStaleEpoch, Message: "moved"}
		}
		return inner(ctx, ref, encID, project, local)
	}
	if _, _, _, _, err := world.server.expandWithRetry(context.Background(), world.near, expandSpecFor(), [][]string{{"u1"}}, false); err != nil {
		t.Fatal(err)
	}
	if got := world.server.node.ExpandStats().Calls.Load(); got != 1 {
		t.Fatalf("ExpandStats.Calls = %d after one call with one retry, want 1", got)
	}
}

// TestADottedExpandFieldIsTheDepthRefusalOnEveryRead: [GUARD a list or get that expands
// "Author.Manager" is told it goes one level, not that the bucket declares no such field].
func TestADottedExpandFieldIsTheDepthRefusalOnEveryRead(t *testing.T) {
	world := expandServe(t)
	spec := RingExpandSpec{Field: "Author.Manager", Bucket: "User"}
	_, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message", Options: RingReadOptions{Expand: []RingExpandSpec{spec}}})
	if text := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(text, "one level") {
		t.Fatalf("list: %s", text)
	}
	message := world.bucket(t, "Message")
	encID, _ := store.EncodeComponent("m1")
	_, err = world.server.read(context.Background(), message, RingRead{Class: RingClassGet, EncID: encID, Options: RingReadOptions{Expand: []RingExpandSpec{spec}}})
	if text := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(text, "one level") {
		t.Fatalf("get: %s", text)
	}
}
