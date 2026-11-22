package wire

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

func expireTestTable(t *testing.T) machine.FieldTable {
	t.Helper()
	table, err := buildFieldTable("app", "Session", []FieldEntry{
		{Name: "Token", Type: "string"}, {Name: "ExpiresAt", Type: "datetime"}, {Name: "Log", Type: "fifoStore"},
		{Name: "Index", Type: "hashStore"}, {Name: "Stack", Type: "lifoStore"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func TestBuildExpireTableResolvesNamesToIDs(t *testing.T) {
	table := expireTestTable(t)
	got, err := buildExpireTable("app", "Session", table, ExpireSpec{BucketField: "ExpiresAt", StorePaths: map[string]string{"Stack": "meta.at", "Log": "ts"}})
	if err != nil {
		t.Fatal(err)
	}
	want := machine.ExpireTable{BucketField: 3, Stores: []machine.ExpireStore{{FieldID: 4, Path: "ts"}, {FieldID: 6, Path: "meta.at"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("table = %+v, want %+v (stores in field-id order, names resolved to ids)", got, want)
	}
	if zero, err := buildExpireTable("app", "Session", table, ExpireSpec{}); err != nil || !reflect.DeepEqual(zero, machine.ExpireTable{}) {
		t.Fatalf("an empty spec gave %+v, %v, want the zero table", zero, err)
	}
}

func TestExpireFieldMustBeADeclaredDatetime(t *testing.T) {
	table := expireTestTable(t)
	for name, tc := range map[string]struct{ field, want string }{
		"missing":    {"Nope", "does not declare"},
		"a string":   {"Token", `declared string`},
		"a store":    {"Log", `declared fifoStore`},
		"any (_id_)": {"_id_", "declared any"},
	} {
		_, err := buildExpireTable("app", "Session", table, ExpireSpec{BucketField: tc.field})
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, tc.want) {
			t.Errorf("%s: %v, want INVALID_ARGUMENT containing %q", name, err, tc.want)
			continue
		}
		if !strings.Contains(refusal.Message, "add ") || !strings.Contains(refusal.Message, "datetime") || !strings.Contains(refusal.Message, "set expire at") {
			t.Errorf("%s: %q must name the exact commands that fix it", name, refusal.Message)
		}
	}
}

func TestExpireStoreMustBeFifoOrLifo(t *testing.T) {
	table := expireTestTable(t)
	for name, tc := range map[string]struct{ field, path, want string }{
		"a hash store": {"Index", "ts", "hashStore"},
		"a scalar":     {"Token", "ts", "string"},
		"missing":      {"Nope", "ts", "does not declare"},
		"empty path":   {"Log", "", "one to three"},
		"deep path":    {"Log", "a.b.c.d", "one to three"},
	} {
		_, err := buildExpireTable("app", "Session", table, ExpireSpec{StorePaths: map[string]string{tc.field: tc.path}})
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, tc.want) {
			t.Errorf("%s: %v, want INVALID_ARGUMENT containing %q", name, err, tc.want)
			continue
		}
		if !strings.Contains(refusal.Message, "expire at") {
			t.Errorf("%s: %q must name the exact statement that fixes it", name, refusal.Message)
		}
	}
}

func expireCreateFields() []FieldEntry {
	return []FieldEntry{{Name: "UserId", Type: "string"}, {Name: "ExpiresAt", Type: "datetime"}, {Name: "Log", Type: "fifoStore"}}
}

func TestCreateWithExpiryWritesFieldElevenAndIsIdempotent(t *testing.T) {
	ring := newU39Ring()
	spec := ExpireSpec{BucketField: "ExpiresAt", StorePaths: map[string]string{"Log": "ts"}}
	id, err := u39Create(ring, 0xCCCC).RunWithExpiry(context.Background(), u39DB, "Session", expireCreateFields(), spec)
	if err != nil {
		t.Fatalf("RunWithExpiry: %v", err)
	}
	decl, found := ring.declaration("Session")
	want := machine.ExpireTable{BucketField: 3, Stores: []machine.ExpireStore{{FieldID: 4, Path: "ts"}}}
	if !found || decl.BucketID != id || !reflect.DeepEqual(decl.Expire, want) {
		t.Fatalf("declaration = %+v found %v, want expire table %+v", decl, found, want)
	}
	before := len(ring.writes)
	if again, err := u39Create(ring).RunWithExpiry(context.Background(), u39DB, "Session", expireCreateFields(), spec); err != nil || again != id {
		t.Fatalf("repeat = %x, %v, want the same id", again, err)
	}
	if len(ring.writes) != before {
		t.Fatal("a repeated create wrote documents")
	}
	// The same fields with another expiry are another bucket: BUCKET_EXISTS, never a silent change.
	_, err = u39Create(ring).RunWithExpiry(context.Background(), u39DB, "Session", expireCreateFields(), ExpireSpec{})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeBucketExists {
		t.Fatalf("a create naming no expiry over an expiring bucket = %v, want BUCKET_EXISTS", err)
	}
}
