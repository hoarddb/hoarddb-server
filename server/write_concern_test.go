package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUnknownWriteConcernIsRefused guards write-concern parsing. A mistyped concern was
// parsed as `one`, so a caller asking for `majority` was acknowledged by the
// leader alone with a success reply. The request must be refused as
// INVALID_ARGUMENT, the refusal must name the accepted values, and nothing may
// be applied.
func TestUnknownWriteConcernIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: testBucket, Type: BucketTypeHash,
	})

	for _, mistyped := range []string{"majorty", "quorum", "2"} {
		key := "typo-" + mistyped
		frame := tc.writeConcerned(0, key, mistyped)
		if frame.Opcode != transport.OpError {
			t.Errorf("write concern %q was accepted (response %s); it must be refused", mistyped, frame.Opcode)
			continue
		}
		var refusal ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
			t.Fatalf("decode refusal for %q: %v", mistyped, err)
		}
		if refusal.Code != int(transport.ErrInvalidArgument) {
			t.Errorf("write concern %q refused with code %d, want INVALID_ARGUMENT (%d)", mistyped, refusal.Code, transport.ErrInvalidArgument)
		}
		for _, want := range []string{mistyped, "majority", "all", "one"} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("refusal for %q does not mention %q: %s", mistyped, want, refusal.Message)
			}
		}
		if found, _ := tc.readFromNode(0, key); found {
			t.Errorf("write with unknown concern %q was applied", mistyped)
		}
	}
}

// TestWriteConcernBothSpellingsAreAccepted pins the field-name tolerance of
// frameMeta so the refusal fix cannot lose it: `write_concern` and
// `writeconcern` reach the same level.
func TestWriteConcernBothSpellingsAreAccepted(t *testing.T) {
	for _, field := range []string{"write_concern", "writeconcern"} {
		payload, err := bson.Marshal(bson.M{field: "majority"})
		if err != nil {
			t.Fatalf("marshal %s: %v", field, err)
		}
		level, err := decodeFrameMeta(payload).writeConcern()
		if err != nil || level != WcMajority {
			t.Errorf("field %q decoded to level %s (err=%v), want majority", field, level, err)
		}
	}
}

// TestParseWriteConcernRejectsUnknown: a string that is not a write concern is
// an error naming the accepted values, and the accepted spellings still parse.
func TestParseWriteConcernRejectsUnknown(t *testing.T) {
	for _, unknown := range []string{"majorty", "quorum", "2", "ALL!", "on e"} {
		_, err := ParseWriteConcern(unknown)
		if !errors.Is(err, ErrUnknownWriteConcern) {
			t.Errorf("ParseWriteConcern(%q) error = %v, want ErrUnknownWriteConcern", unknown, err)
			continue
		}
		if !strings.Contains(err.Error(), "majority") {
			t.Errorf("refusal for %q does not list the accepted values: %v", unknown, err)
		}
	}
	accepted := map[string]WriteConcernLevel{
		"1": WcOne, "one": WcOne, " ONE ": WcOne, "majority": WcMajority, "Majority": WcMajority,
		"all": WcAll, "all_nodes": WcAll,
	}
	for spelling, want := range accepted {
		if got, err := ParseWriteConcern(spelling); err != nil || got != want {
			t.Errorf("ParseWriteConcern(%q) = %s, %v; want %s", spelling, got, err, want)
		}
	}
}
