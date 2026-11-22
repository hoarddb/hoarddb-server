package wire

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/namepolicy"
)

// u76Refused asserts err is a *cluster.Error of the wanted code whose text names the thing and
// carries a fix to run or a rule to follow.
func u76Refused(t *testing.T, label string, err error, code cluster.Code, thing string) {
	t.Helper()
	var refusal *cluster.Error
	if !errors.As(err, &refusal) {
		t.Errorf("%s: err %v is not a refusal", label, err)
		return
	}
	if refusal.Code != code {
		t.Errorf("%s: code %v, want %v (%s)", label, refusal.Code, code, refusal.Message)
	}
	if !strings.Contains(refusal.Message, thing) {
		t.Errorf("%s: text does not name %q: %s", label, thing, refusal.Message)
	}
	if !strings.Contains(refusal.Message, "Run '") && !strings.Contains(refusal.Message, "choose") && !strings.Contains(refusal.Message, "Use ") {
		t.Errorf("%s: text names no fix: %s", label, refusal.Message)
	}
}

// TestU76NodeCreateDatabaseAppliesTheSharedNamePolicy: [GUARD every name the legacy door
// refuses is refused by Node.CreateDatabase with a text that names it and the fix, and
// nothing is committed].
func TestU76NodeCreateDatabaseAppliesTheSharedNamePolicy(t *testing.T) {
	node, err := OpenNode(t.Context(), NodeConfig{Dir: t.TempDir(), StorageNames: []namepolicy.StorageDir{
		{Name: "store", Refusal: `database name "store" is reserved: it is the name of this node's store directory; choose another name`}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	cases := []struct {
		label, name string
		code        cluster.Code
		thing       string
	}{
		{"auth master", "_system", cluster.CodeReservedName, "_system"},
		{"storage directory", "store", cluster.CodeReservedName, "store"},
		{"traversal", "../../x", cluster.CodeInvalidArgument, "../../x"},
		{"separator", "a/b", cluster.CodeInvalidArgument, "a/b"},
		{"backslash", `a\b`, cluster.CodeInvalidArgument, `a\\b`},
		{"dot", ".", cluster.CodeInvalidArgument, `"."`},
		{"dotdot", "..", cluster.CodeInvalidArgument, `".."`},
		{"absolute", "/etc", cluster.CodeInvalidArgument, "/etc"},
		{"NUL", "a\x00b", cluster.CodeInvalidArgument, "NUL"},
		{"over 255 bytes", strings.Repeat("d", 256), cluster.CodeInvalidArgument, "256 bytes"},
		{"empty", "", cluster.CodeInvalidArgument, "database name"},
	}
	for _, tc := range cases {
		_, err := node.CreateDatabase(t.Context(), tc.name)
		u76Refused(t, tc.label, err, tc.code, tc.thing)
		if _, ok := node.topology.DatabaseID(tc.name); ok && tc.name != "" {
			t.Errorf("%s: the refused database was committed", tc.label)
		}
	}
	if _, err := node.CreateDatabase(t.Context(), strings.Repeat("d", 255)); err != nil {
		t.Errorf("a name at the 255-byte limit is refused: %v", err)
	}
}

// TestU76NodeCreateBucketAppliesTheSharedNamePolicy: [GUARD separators, ./.. and a name over
// 255 bytes are refused by the create path every caller shares].
func TestU76NodeCreateBucketAppliesTheSharedNamePolicy(t *testing.T) {
	node, _ := u63Fresh(t)
	if _, err := node.CreateDatabase(t.Context(), "shop"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ label, name, thing string }{
		{"separator", "a/b", "a/b"}, {"backslash", `a\b`, `a\\b`}, {"dot", ".", `"."`}, {"dotdot", "..", `".."`},
		{"NUL", "a\x00b", "NUL"}, {"over 255 bytes", strings.Repeat("b", 256), "256 bytes"},
	} {
		_, err := node.CreateBucket(t.Context(), "shop", tc.name)
		u76Refused(t, tc.label, err, cluster.CodeInvalidArgument, tc.thing)
	}
	if _, err := node.CreateBucket(t.Context(), "shop", strings.Repeat("b", 255)); err != nil {
		t.Errorf("a bucket name at the 255-byte limit is refused: %v", err)
	}
}

// TestU76TheWireRefusesWhatTheLegacyDoorRefuses: [GUARD create_database and create_bucket
// over RING_ADMIN answer with the policy's refusal, not success].
func TestU76TheWireRefusesWhatTheLegacyDoorRefuses(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "_system"}), cluster.CodeReservedName)
	u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "../../x"}), cluster.CodeInvalidArgument)
	u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: strings.Repeat("d", 1021)}), cluster.CodeInvalidArgument)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "a/b", Fields: []RingAdminField{}}), cluster.CodeInvalidArgument)
	u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "..", Fields: []RingAdminField{}}), cluster.CodeInvalidArgument)
}

// TestU76AFieldNamedIdIsRefusedNotDropped: [GUARD create_bucket with an _id_ field answers
// INVALID_ARGUMENT naming the fix instead of silently dropping the field and its type].
func TestU76AFieldNamedIdIsRefusedNotDropped(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	refusal := u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "Thing",
		Fields: []RingAdminField{{Name: "_id_", Type: "fifoStore"}}}), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "_id_") || !strings.Contains(refusal.Message, "Run 'create bucket Thing in shop") {
		t.Errorf("the refusal does not name the field and the fix: %s", refusal.Message)
	}
	if page := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminShowBuckets, Database: "shop"})); len(page.Buckets) != 0 {
		t.Errorf("a refused create left a bucket: %+v", page.Buckets)
	}
}
