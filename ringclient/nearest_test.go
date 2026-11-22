package ringclient_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// leVector is a vector as a nearest reads it: little-endian float32 values in a Binary.
func leVector(values ...float32) primitive.Binary {
	data := make([]byte, 4*len(values))
	for i, value := range values {
		binary.LittleEndian.PutUint32(data[4*i:], math.Float32bits(value))
	}
	return primitive.Binary{Data: data}
}

// vectorItem is an element of a Friends hashStore: its vector under the default field name.
func vectorItem(vector primitive.Binary) bson.D {
	return bson.D{{Key: "Vector", Value: vector}}
}

// seedFriends puts three elements in ada's Friends: near points along the query, mid halfway, far at right angles.
func seedFriends(t *testing.T, users *ringclient.Bucket, ctx context.Context) {
	t.Helper()
	for _, element := range []struct {
		key    string
		vector primitive.Binary
	}{
		{"near", leVector(1, 0, 0)},
		{"mid", leVector(1, 1, 0)},
		{"far", leVector(0, 1, 0)},
	} {
		if _, err := users.Put(ctx, "ada", "Friends", element.key, vectorItem(element.vector)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestNearestQueryDefaultsAndRefusals: a query that leaves k, metric and field zero gets the client's defaults
// (10, cosine, "Vector"), and a bad query is refused by the client before it is sent.
func TestNearestQueryDefaultsAndRefusals(t *testing.T) {
	users, ctx := u85Users(t), liveContext(t)
	seedFriends(t, users, ctx)

	got, err := users.Nearest(ctx, "ada", "Friends", ringclient.NearestQuery{Vector: leVector(1, 0, 0).Data})
	if err != nil || fmt.Sprint(got.Keys) != "[near mid far]" || got.Scanned != 3 {
		t.Fatalf("a query with every default = keys %v, scanned %d, %v; want all three keys best first", got.Keys, got.Scanned, err)
	}

	refusals := []struct {
		name  string
		query ringclient.NearestQuery
		want  string
	}{
		{"k above 100", ringclient.NearestQuery{Vector: leVector(1, 0, 0).Data, K: 101},
			"ringclient: Nearest asks for k 101; ask for 1 to 100"},
		{"an unknown metric", ringclient.NearestQuery{Vector: leVector(1, 0, 0).Data, Metric: "euclid"},
			`ringclient: Nearest metric "euclid" is not cosine, dot or l2`},
		{"a vector of 5 bytes", ringclient.NearestQuery{Vector: make([]byte, 5)},
			"ringclient: Nearest vector is 5 bytes; send 4 to 16384 bytes, a multiple of 4"},
		{"no vector", ringclient.NearestQuery{},
			"ringclient: Nearest vector is 0 bytes; send 4 to 16384 bytes, a multiple of 4"},
		{"a vector above 16384 bytes", ringclient.NearestQuery{Vector: make([]byte, 16388)},
			"ringclient: Nearest vector is 16388 bytes; send 4 to 16384 bytes, a multiple of 4"},
	}
	for _, refusal := range refusals {
		_, err := users.Nearest(ctx, "ada", "Friends", refusal.query)
		if err == nil || !strings.Contains(err.Error(), refusal.want) {
			t.Errorf("%s: err = %v, want a refusal containing %q", refusal.name, err, refusal.want)
		}
	}
}

// TestLiveNearestReturnsBestFirst: the reply's keys, scores and counts come back to the caller, best first.
func TestLiveNearestReturnsBestFirst(t *testing.T) {
	users, ctx := u85Users(t), liveContext(t)
	seedFriends(t, users, ctx)

	got, err := users.Nearest(ctx, "ada", "Friends", ringclient.NearestQuery{Vector: leVector(1, 0, 0).Data, K: 2})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got.Keys) != "[near mid]" || len(got.Elements) != 2 {
		t.Fatalf("keys %v with %d elements, want [near mid] with two elements", got.Keys, len(got.Elements))
	}
	if len(got.Scores) != 2 || math.Abs(got.Scores[0]-1) > 1e-6 || got.Scores[1] >= got.Scores[0] {
		t.Fatalf("scores %v, want two scores best first with the exact match at 1", got.Scores)
	}
	if got.Scanned != 3 || got.Skipped != 0 {
		t.Fatalf("scanned %d and skipped %d, want 3 and 0", got.Scanned, got.Skipped)
	}
}
