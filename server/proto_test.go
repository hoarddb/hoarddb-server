package server

import (
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestCreateBucketRequestWireNameIsReplicationFactor pins the BSON key a
// per-bucket replication factor travels under.
//
// The BSON codec lowercases a Go field name when the struct carries no bson
// tag, so an untagged ReplicationFactor ships as "replicationfactor" — and a
// wire name, once a driver has seen it, is permanent. This tree already carries
// that scar: server/write_path.go:frameMeta accepts both "writeconcern" and
// "write_concern" because the tag was left off the first time.
//
// Ported from a4d11c5 as is.
func TestCreateBucketRequestWireNameIsReplicationFactor(t *testing.T) {
	encoded, err := bson.Marshal(CreateBucketRequest{
		Database:          "app",
		Name:              "events",
		Type:              BucketTypeHash,
		ReplicationFactor: 3,
	})
	if err != nil {
		t.Fatalf("marshalling CreateBucketRequest: %v", err)
	}

	var decoded bson.M
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling the encoded request: %v", err)
	}

	factor, present := decoded["replication_factor"]
	if !present {
		keys := make([]string, 0, len(decoded))
		for key := range decoded {
			keys = append(keys, key)
		}
		t.Fatalf("no %q key on the wire; the document carries %v", "replication_factor", keys)
	}
	if asInt, ok := factor.(int32); !ok || asInt != 3 {
		t.Fatalf("replication_factor on the wire = %v (%T), want int32(3)", factor, factor)
	}
	if _, wrong := decoded["replicationfactor"]; wrong {
		t.Fatal("the untagged wire name replicationfactor is present; the bson tag is missing")
	}
}

// TestCreateBucketRequestOmitsAnAbsentReplicationFactor keeps the frame a
// client that says nothing about replication sends byte-identical to the one it
// sent before this field existed. "Absent" is the value that means inherit, and
// it must not become a zero on the wire that an older or a stricter reader can
// mistake for "zero copies".
//
// Ported from a4d11c5 as is.
func TestCreateBucketRequestOmitsAnAbsentReplicationFactor(t *testing.T) {
	encoded, err := bson.Marshal(CreateBucketRequest{
		Database: "app",
		Name:     "events",
		Type:     BucketTypeHash,
	})
	if err != nil {
		t.Fatalf("marshalling CreateBucketRequest: %v", err)
	}

	var decoded bson.M
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling the encoded request: %v", err)
	}
	for key := range decoded {
		if key == "replication_factor" || key == "replicationfactor" {
			t.Fatalf("an unset replication factor was written to the wire as %q; the omitempty is missing", key)
		}
	}
}

// TestBucketInfoWireNamesAreReplicationFactorAndInherited pins the keys a
// listing row carries the factor under; an untagged field would travel as
// "replicationfactor".
func TestBucketInfoWireNamesAreReplicationFactorAndInherited(t *testing.T) {
	encoded, err := bson.Marshal(BucketInfo{Name: "u", Type: BucketTypeHash, ReplicationFactor: 1, Inherited: true})
	if err != nil {
		t.Fatalf("marshalling BucketInfo: %v", err)
	}
	raw := bson.Raw(encoded)
	if factor, ok := raw.Lookup("replication_factor").Int32OK(); !ok || factor != 1 {
		t.Fatalf("replication_factor on the wire = %v (present %v), want int32 1; the row is %v", factor, ok, raw)
	}
	if inherited, ok := raw.Lookup("inherited").BooleanOK(); !ok || !inherited {
		t.Fatalf("inherited on the wire = %v (present %v), want true; the row is %v", inherited, ok, raw)
	}
}

// encodedKeys returns the top-level keys of a value as BSON encodes it, in
// order.
func encodedKeys(t *testing.T, value any) []string {
	t.Helper()
	encoded, err := bson.Marshal(value)
	if err != nil {
		t.Fatalf("marshalling %T: %v", value, err)
	}
	elements, err := bson.Raw(encoded).Elements()
	if err != nil {
		t.Fatalf("reading the elements of %T: %v", value, err)
	}
	keys := make([]string, 0, len(elements))
	for _, element := range elements {
		keys = append(keys, element.Key())
	}
	return keys
}

// TestTheStreamHandshakeWireNames pins the BSON name of every field the stream
// handshake added (spec §19.3.3, §20.4; build doc step 4.2): two nodes of
// different builds must agree on them, and an untagged field would travel
// under its lowercased Go name. Each row sets every field, so a missing tag
// shows as a wrong key, and the rows without a stream pin that a push and a
// response that carry none are the bytes they were before.
func TestTheStreamHandshakeWireNames(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  []string
	}{
		{"ReplStream", ReplStream{Incarnation: []byte{1}, Open: true, Rebase: &ReplRebase{}, AdvanceTo: 9},
			[]string{"incarnation", "open", "rebase", "advance_to"}},
		{"ReplRebase", ReplRebase{From: []byte{1}, ValidThrough: 2, StartAt: 3, Log: []byte{4}},
			[]string{"from", "valid_through", "start_at", "log"}},
		{"MigrateStartRequest's incarnation", MigrateStartRequest{Incarnation: []byte{1}},
			[]string{"token", "database", "bucket", "incarnation"}},
		{"MigrateStartResponse's incarnation", MigrateStartResponse{Incarnation: []byte{1}},
			[]string{"ok", "store_type", "num_chunks", "high_water", "incarnation"}},
		{"ReplPosition", ReplPosition{Known: true, Incarnation: []byte{1}, Log: []byte{2}, AppliedThrough: 3},
			[]string{"known", "incarnation", "log", "applied_through"}},
		{"ReplPushRequest without a stream", ReplPushRequest{Token: "t", FromNode: "n"},
			[]string{"token", "from_node", "entries"}},
		{"ReplPushRequest with a stream", ReplPushRequest{Token: "t", FromNode: "n", Stream: &ReplStream{Open: true}},
			[]string{"token", "from_node", "entries", "stream"}},
		{"ReplPushResponse without a handshake", ReplPushResponse{Success: true},
			[]string{"success", "applied_through"}},
		{"ReplPushResponse with a handshake", ReplPushResponse{Success: true, Position: &ReplPosition{}, Refusal: "reopen"},
			[]string{"success", "applied_through", "position", "refusal"}},
		{"ReplPushResponse with an epoch refusal (dispatch 6)", ReplPushResponse{Refusal: refusalEpoch, Epoch: 2},
			[]string{"success", "applied_through", "refusal", "epoch"}},
		{"ReplEntry in a term (dispatch 9)", ReplEntry{Seq: 1, Op: 1, Bucket: "b", Key: []byte{1}, Payload: []byte{1}, OwnerOutcome: 1, Term: 2},
			[]string{"seq", "op", "bucket", "key", "payload", "owner_outcome", "term"}},
		{"ReplPushRequest with a question (dispatch 9)", ReplPushRequest{Token: "t", FromNode: "n", Handoff: &ReplHandoff{Ask: []ReplBucketRef{{Database: "d", Bucket: "b"}}}},
			[]string{"token", "from_node", "entries", "handoff"}},
		{"GossipResponse's history and removal (dispatch 9)", GossipResponse{History: []MembershipRecord{{Epoch: 2}}, RemovedAt: 3},
			[]string{"success", "epoch", "self", "history", "removed_at", "bootstrapped"}},
		// join-freeze spec §16, F-A: the mark travels both ways, and always.
		{"GossipRequest's bootstrapped mark (join freeze)", GossipRequest{Token: "t", NodeID: "n", Addr: "a"},
			[]string{"token", "node_id", "addr", "epoch", "bootstrapped"}},
		// join-freeze spec §3.4, §8.5: what a node says about settling.
		{"GossipRequest's settling (join freeze)", GossipRequest{Settled: 2, SettledOf: []MemberSettled{{ID: "a", Settled: 2}}, HandOff: &HandOffProgress{Epoch: 2}},
			[]string{"token", "node_id", "addr", "epoch", "bootstrapped", "settled", "settled_of", "handoff"}},
		{"GossipResponse's settling (join freeze)", GossipResponse{Settled: 2, SettledOf: []MemberSettled{{ID: "a", Settled: 2}}, HandOff: &HandOffProgress{Epoch: 2}},
			[]string{"success", "epoch", "self", "bootstrapped", "settled", "settled_of", "handoff"}},
		{"MemberSettled", MemberSettled{ID: "a", Settled: 2}, []string{"id", "settled"}},
		{"HandOffProgress", HandOffProgress{Epoch: 2, Total: 3, Done: 1, GaveUp: true, WaitingOn: []string{"a"}, Unconfirmed: []PlacementToCopy{{}}},
			[]string{"epoch", "total", "done", "gave_up", "waiting_on", "unconfirmed"}},
		{"PlacementToCopy", PlacementToCopy{Database: "d", Bucket: "b", Node: "n"}, []string{"database", "bucket", "node"}},
		{"ReplHandoff", ReplHandoff{Ask: []ReplBucketRef{{Database: "d", Bucket: "b"}}}, []string{"ask"}},
		{"ReplBucketRef", ReplBucketRef{Database: "d", Bucket: "b"}, []string{"database", "bucket"}},
		{"ReplPushResponse with copies (dispatch 9)", ReplPushResponse{Success: true, Copies: []ReplCopy{{Holds: true}}},
			[]string{"success", "applied_through", "copies"}},
		{"ReplCopy", ReplCopy{Holds: true, Term: 1, Through: 2, BaseTerm: 3, BaseThrough: 4, Opened: true, Moved: 5, Outside: 6, Owes: true},
			[]string{"holds", "term", "through", "base_term", "base_through", "opened", "moved", "outside", "owes"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := encodedKeys(t, tc.value); !slices.Equal(got, tc.want) {
				t.Fatalf("%s travels as %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
