package server

import (
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// goldenInsertRequestFields is every exported field of InsertRequest with its
// struct tag, reviewed one by one: none of them can carry the insert intent.
// A new exported field fails TestClientCannotSetInsertIntent until someone
// has read it for that and added it here (document-placement build doc T-64).
var goldenInsertRequestFields = map[string]string{
	"Database":     `json:"database"`,
	"Bucket":       `json:"bucket"`,
	"Key":          `json:"key,omitempty"`,
	"Data":         `json:"data"`,
	"WriteConcern": `json:"write_concern,omitempty"`,
	"Admitted":     `bson:"admitted,omitempty" json:"admitted,omitempty"`,
	"RepairBound":  `bson:"repair_bound,omitempty" json:"repair_bound,omitempty"`,
	"RepairRead":   `bson:"repair_read,omitempty" json:"repair_read,omitempty"`,
}

// goldenBatchInsertRequestFields is the same list for BatchInsertRequest.
var goldenBatchInsertRequestFields = map[string]string{
	"Database":     `json:"database"`,
	"Bucket":       `json:"bucket"`,
	"Data":         `json:"data"`,
	"WriteConcern": `json:"write_concern,omitempty"`,
}

// TestClientCannotSetInsertIntent is T-64. The insert intent (refuse an
// existing _id_) comes from the opcode of the handler that decoded the
// request and is carried in an unexported field, which BSON never decodes, so
// no client byte can set it (spec §1.3). Part (a) holds the exported surface
// to a reviewed allowlist; part (b) sends the field names a client might try.
func TestClientCannotSetInsertIntent(t *testing.T) {
	t.Run("exported fields are the reviewed list", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			typeOf reflect.Type
			golden map[string]string
		}{
			{"InsertRequest", reflect.TypeFor[InsertRequest](), goldenInsertRequestFields},
			{"BatchInsertRequest", reflect.TypeFor[BatchInsertRequest](), goldenBatchInsertRequestFields},
		} {
			seen := map[string]bool{}
			for index := 0; index < tc.typeOf.NumField(); index++ {
				field := tc.typeOf.Field(index)
				if !field.IsExported() {
					continue
				}
				seen[field.Name] = true
				want, listed := tc.golden[field.Name]
				if !listed {
					t.Errorf("%s has the exported field %s (%q) that is not on the reviewed list: read it for a way to carry the insert intent, then add it to the list", tc.name, field.Name, field.Tag)
					continue
				}
				if string(field.Tag) != want {
					t.Errorf("%s.%s has the tag %q, the reviewed list has %q", tc.name, field.Name, field.Tag, want)
				}
			}
			for name := range tc.golden {
				if !seen[name] {
					t.Errorf("the reviewed list holds %s.%s, which no longer exists: remove it from the list", tc.name, name)
				}
			}
		}
	})

	t.Run("the verb field exists and is unexported", func(t *testing.T) {
		for _, typeOf := range []reflect.Type{reflect.TypeFor[InsertRequest](), reflect.TypeFor[BatchInsertRequest]()} {
			field, found := typeOf.FieldByName("verb")
			if !found {
				t.Errorf("%s has no field verb: the opcode's intent has nowhere to travel", typeOf.Name())
				continue
			}
			if field.IsExported() {
				t.Errorf("%s.verb is exported: BSON would decode a client's bytes into it", typeOf.Name())
			}
		}
	})

	t.Run("a request a Go caller builds has no intent", func(t *testing.T) {
		// Direct callers (95 test files) keep upsert semantics: the zero value
		// is verbUnset, and only a handler sets another (build doc BD-2, SA-9).
		if got := (InsertRequest{}).verb; got != verbUnset {
			t.Errorf("a zero InsertRequest has verb %d, want verbUnset", got)
		}
		if got := (BatchInsertRequest{}).verb; got != verbUnset {
			t.Errorf("a zero BatchInsertRequest has verb %d, want verbUnset", got)
		}
	})

	t.Run("an UPSERT carrying the intent names still replaces", func(t *testing.T) {
		s := newTestServer(t)
		handler := NewHandler(s)
		handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})
		mustInsert(t, s, "app", "docs", "k1", map[string]interface{}{"v": int32(1)})

		payload := mustMarshalBSON(t, bson.D{
			{Key: "database", Value: "app"}, {Key: "bucket", Value: "docs"}, {Key: "key", Value: "k1"},
			{Key: "refuse_existing", Value: true}, {Key: "refuseExisting", Value: true}, {Key: "verb", Value: int32(1)},
			{Key: "data", Value: bson.M{"v": int32(2)}},
		})
		answer := readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, transport.OpUpsert, payload))
		if answer.refused || answer.opcode != transport.OpUpsertOK {
			t.Fatalf("UPSERT of an existing _id_ answered %+v, want UPSERT_OK", answer)
		}
		if document := documentOf(t, s, "app", "docs", "k1"); document["v"] != int32(2) {
			t.Errorf("k1 reads %v after the UPSERT, want v replaced by 2", document)
		}
	})
}
