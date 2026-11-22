package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// clientSpellingsOfServerFields are keys the strip must remove. The dotted
// capital I (U+0130) lower-cases to "i" in the BSON struct decoder's field
// lookup but does not simple-fold to it, so a case-folding strip alone lets
// these through (BUG-0240).
var clientSpellingsOfServerFields = map[string][]string{
	"generation":          {"generation", "Generation", "generatİon"},
	"dropped_generations": {"dropped_generations", "Dropped_Generations", "dropped_generatİons"},
	"generations":         {"generations", "generatİons"},
	"previous_factor":     {"previous_factor", "Previous_Factor", "prevİous_factor"},
}

func payloadKeys(t *testing.T, payload []byte) []string {
	t.Helper()
	elements, err := bson.Raw(payload).Elements()
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(elements))
	for _, element := range elements {
		keys = append(keys, element.Key())
	}
	return keys
}

func TestStampPayloadRemovesEveryClientSpellingOfAServerField(t *testing.T) {
	for serverField, spellings := range clientSpellingsOfServerFields {
		for _, spelling := range spellings {
			t.Run(spelling, func(t *testing.T) {
				payload := mustMarshal(t, bson.D{{Key: "name", Value: "app"}, {Key: spelling, Value: "forged"}})
				stamped, err := stampPayload(payload, bson.D{{Key: serverField, Value: int64(7)}})
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for _, key := range payloadKeys(t, stamped) {
					if key == spelling {
						count++
					}
				}
				want := 0
				if spelling == serverField {
					want = 1 // the server's own element
				}
				if count != want {
					t.Fatalf("stamped payload carries %q %d times, want %d: keys %v", spelling, count, want, payloadKeys(t, stamped))
				}
			})
		}
	}
}

func TestStampDropRemovesEveryClientSpellingOfPreviousFactor(t *testing.T) {
	cluster := &Cluster{rf: 1, bucketFactors: map[placementKey]int32{}}
	for _, spelling := range clientSpellingsOfServerFields["previous_factor"] {
		t.Run(spelling, func(t *testing.T) {
			payload := mustMarshal(t, bson.D{{Key: "database", Value: "app"}, {Key: "name", Value: "users"}, {Key: spelling, Value: int32(9)}})
			stamped, _, err := cluster.stampDrop(ReplEntry{Op: int32(transport.OpDeleteBucket), Bucket: "users", Payload: payload}, placementOf("app", "users"))
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, key := range payloadKeys(t, stamped.Payload) {
				if key == spelling {
					count++
				}
			}
			want := 0
			if spelling == "previous_factor" {
				want = 1
			}
			if count != want {
				t.Fatalf("stamped drop carries %q %d times, want %d: keys %v", spelling, count, want, payloadKeys(t, stamped.Payload))
			}
		})
	}
}
