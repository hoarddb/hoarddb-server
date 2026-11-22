package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The create's wire tree is read with refusals a developer can act on: the
// entry counted from 1, the bucket named, plain words for what is wrong, and
// the same answer every time.

func TestMalformedWireTreeRefusalsAreReadable(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	refuse := func(fields interface{}) string {
		t.Helper()
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Refused", Type: BucketTypeHash,
			Options: map[string]interface{}{"fields": fields}})
		if err != nil || resp.Success {
			t.Fatalf("a create with fields %v answered %+v, %v; want refused", fields, resp, err)
		}
		for _, goWord := range []string{"got ", "want a", "interface", "primitive.", "bson.", "map["} {
			if strings.Contains(resp.Message, goWord) {
				t.Errorf("the refusal %q uses a Go word (%q)", resp.Message, goWord)
			}
		}
		if !strings.Contains(resp.Message, `bucket "Refused"`) {
			t.Errorf("the refusal %q does not name the bucket", resp.Message)
		}
		return resp.Message
	}
	for _, refused := range []struct {
		fields interface{}
		says   string
	}{
		{"Age int", "must be a list"},
		{bson.A{bson.M{"name": "Name", "type": "string"}, "Age int"}, "typed field 2 "},
		{bson.A{bson.M{"name": "Age"}}, `typed field "Age" on bucket "Refused" has no type`},
		{bson.A{bson.M{"name": "Tags", "type": "array", "elem": bson.M{}}}, `has no type: give it one, for example {type: "string"}.`},
		{bson.A{bson.M{"name": "Age", "type": int32(3)}}, "type must be text"},
		{bson.A{bson.M{"name": "Address", "type": "object", "members": "City"}}, `the members of "Address"`},
	} {
		if message := refuse(refused.fields); !strings.Contains(message, refused.says) {
			t.Errorf("fields %v: the refusal %q does not say %q", refused.fields, message, refused.says)
		}
	}
	// Two unknown keys: the refusal names the same one every time.
	twoBad := bson.A{bson.M{"name": "Age", "type": "int", "zeta": 1, "alpha": 2, "mid": 3}}
	for range 20 {
		if message := refuse(twoBad); !strings.Contains(message, `"alpha"`) {
			t.Fatalf("with keys alpha, mid and zeta unknown the refusal reads %q; want the first in order, alpha, every time", message)
		}
	}
}

// TestRecordedFieldWithNoNameIsHeldAsUnreadable: a recorded field without a
// name is held whole, never re-encoded with an empty name.
func TestRecordedFieldWithNoNameIsHeldAsUnreadable(t *testing.T) {
	for _, nameless := range []bson.D{
		{{Key: "type", Value: "int"}},
		{{Key: "name", Value: ""}, {Key: "type", Value: "int"}},
		{{Key: "name", Value: int32(4)}, {Key: "type", Value: "int"}},
		{{Key: "name", Value: "Address"}, {Key: "type", Value: "object"}, {Key: "members", Value: bson.A{bson.D{{Key: "type", Value: "int"}}}}},
	} {
		encoded, err := bson.Marshal(nameless)
		if err != nil {
			t.Fatalf("encoding: %v", err)
		}
		var field catalogField
		if err := bson.Unmarshal(encoded, &field); err != nil {
			t.Fatalf("decoding %v: %v", nameless, err)
		}
		if field.recorded == nil {
			t.Errorf("the recorded field %v was read as readable; want it held whole", nameless)
		}
		again, err := bson.Marshal(field)
		if err != nil || !bytes.Equal(again, encoded) {
			t.Errorf("the recorded field %v re-encodes as %x (%v); want its bytes %x", nameless, again, err, encoded)
		}
	}
}
