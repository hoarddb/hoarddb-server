package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestRecreateWithAnotherDeclarationIsRefused: a create of a bucket that
// exists, carrying a declaration, is answered by comparing it with the one
// the bucket has, as a replication factor is. The same declaration (or none)
// is the idempotent "already exists"; another one, or one the validator
// refuses, is refused and names what the bucket declares.
func TestRecreateWithAnotherDeclarationIsRefused(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", bson.A{bson.M{"name": "Age", "type": "int"}}, "")
	createTestBucket(t, s, "app", "Plain", BucketTypeHash)

	recreate := func(bucket string, options map[string]interface{}) *CreateBucketResponse {
		t.Helper()
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash, Options: options})
		if err != nil {
			t.Fatalf("re-creating %s: %v", bucket, err)
		}
		return resp
	}
	for _, same := range []map[string]interface{}{
		{"fields": bson.A{bson.M{"name": "Age", "type": "integer"}}},
		{"fields": bson.A{bson.M{"name": "Age", "type": "int"}}, "field_types": "strict"},
		nil,
	} {
		if resp := recreate("User", same); !resp.Success || !strings.Contains(resp.Message, "already exists") {
			t.Errorf("re-creating User with %v answered %+v; want the idempotent already exists", same, resp)
		}
	}
	for _, other := range []struct {
		bucket  string
		options map[string]interface{}
		says    []string
	}{
		{"User", map[string]interface{}{"fields": bson.A{bson.M{"name": "Age", "type": "string"}}},
			[]string{`bucket "User" already exists`, "Age int", "Age string", "not changed", "alter bucket User"}},
		{"User", map[string]interface{}{"fields": bson.A{bson.M{"name": "Age", "type": "int"}}, "field_types": "warn"},
			[]string{"field_types: strict", "field_types: warn"}},
		{"User", map[string]interface{}{"fields": bson.A{bson.M{"name": "Age", "type": "integr"}}},
			[]string{`"integr" is not a field type`}},
		{"Plain", map[string]interface{}{"fields": bson.A{bson.M{"name": "Age", "type": "int"}}},
			[]string{`bucket "Plain" already exists`, "declares no typed fields", "Age int"}},
	} {
		resp := recreate(other.bucket, other.options)
		if resp.Success {
			t.Errorf("re-creating %s with %v answered success %q; want it refused", other.bucket, other.options, resp.Message)
			continue
		}
		for _, part := range other.says {
			if !strings.Contains(resp.Message, part) {
				t.Errorf("re-creating %s with %v: the refusal %q does not say %q", other.bucket, other.options, resp.Message, part)
			}
		}
	}
	if fields, _ := lookupBucket(s, "app", "User").declaredFieldTypes().catalogForm(); len(fields) != 1 || fields[0].Type != "int" {
		t.Errorf("the refused re-creates changed User's declaration to %+v", fields)
	}
}
