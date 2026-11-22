package ringclient_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestLiveUpdateOperatorsAgainstTheRealServer: [GUARD Bucket.Update sends the nine codes and
// the owner's refusals reach the caller with their words].
func TestLiveUpdateOperatorsAgainstTheRealServer(t *testing.T) {
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "Item", []ringclient.FieldDecl{
		{Name: "Qty", Type: "int"}, {Name: "Price", Type: "double"}, {Name: "Tags", Type: "array"},
		{Name: "Label", Type: "string"}, {Name: "Alias", Type: "string"}, {Name: "Log", Type: "fifoStore"},
	}); err != nil {
		t.Fatal(err)
	}
	items := client.Bucket("app", "Item")
	if _, err := items.Insert(ctx, "i1", ringclient.Doc{"Qty": int32(3), "Price": 9.5, "Label": "a"}); err != nil {
		t.Fatal(err)
	}
	request := client.NextRequestID()
	change := ringclient.Update{
		Mul: bson.D{{Key: "Qty", Value: int32(4)}}, Min: bson.D{{Key: "Price", Value: 2.5}},
		Push: bson.D{{Key: "Tags", Value: "x"}}, Rename: bson.D{{Key: "Label", Value: "Alias"}},
	}
	first, err := items.Update(ctx, "i1", change, ringclient.WithRequestID(request))
	if err != nil || !first.Applied() {
		t.Fatalf("update: %+v %v", first, err)
	}
	again, err := items.Update(ctx, "i1", change, ringclient.WithRequestID(request))
	if err != nil || !again.Deduped {
		t.Fatalf("the retry: %+v %v, want it deduped", again, err)
	}
	if _, err := items.Update(ctx, "i1", ringclient.Update{AddToSet: bson.D{{Key: "Tags", Value: "x"}}, Inc: bson.D{{Key: "Qty", Value: int32(1)}}, Max: bson.D{{Key: "Price", Value: 3.0}}}); err != nil {
		t.Fatal(err)
	}
	got, err := items.Get(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	values, _ := got.Lookup("Tags").Array().Values()
	if got.Lookup("Qty").Int32() != 13 || got.Lookup("Price").Double() != 3.0 || got.Lookup("Alias").StringValue() != "a" || len(values) != 1 {
		t.Fatalf("after the updates: %v", got)
	}
	for name, tc := range map[string]struct {
		change ringclient.Update
		said   []string
	}{
		"mul of a string":      {ringclient.Update{Mul: bson.D{{Key: "Alias", Value: int32(2)}}}, []string{"mul", "Nothing was changed"}},
		"each is not built":    {ringclient.Update{Push: bson.D{{Key: "Tags", Value: bson.D{{Key: "$each", Value: bson.A{"a"}}}}}}, []string{"$each", "Tags"}},
		"a store field":        {ringclient.Update{Push: bson.D{{Key: "Log", Value: "x"}}}, []string{"Log", "store"}},
		"an undeclared field":  {ringclient.Update{Mul: bson.D{{Key: "Nope", Value: int32(2)}}}, []string{"Nope"}},
		"two operations":       {ringclient.Update{Mul: bson.D{{Key: "Qty", Value: int32(2)}}, Min: bson.D{{Key: "Qty", Value: int32(2)}}}, []string{"Qty", "two operations"}},
		"rename across types":  {ringclient.Update{Rename: bson.D{{Key: "Qty", Value: "Label"}}}, []string{"Qty", "Label", "same type"}},
		"rename to a new name": {ringclient.Update{Rename: bson.D{{Key: "Qty", Value: "Quantity"}}}, []string{"Quantity", "alter bucket"}},
	} {
		_, err := items.Update(ctx, "i1", tc.change)
		if err == nil {
			t.Errorf("%s: the update was accepted", name)
			continue
		}
		for _, word := range tc.said {
			if !strings.Contains(err.Error(), word) {
				t.Errorf("%s: %q does not say %q", name, err, word)
			}
		}
	}
	if _, err := items.Update(ctx, "ghost", ringclient.Update{Set: bson.D{{Key: "Qty", Value: int32(1)}}}); !errors.Is(err, ringclient.ErrNotFound) {
		t.Errorf("update of a missing document: %v, want NOT_FOUND", err)
	}
}
