package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// stubRingTarget is a ring in memory: the databases and the declarations in them, and what
// the run created, so the pure checks of prepareTarget need no server.
type stubRingTarget struct {
	databases        map[string]map[string][]ringclient.FieldDecl
	createdDatabases []string
	createdBuckets   map[string][]ringclient.FieldDecl
}

func newStubRingTarget() *stubRingTarget {
	return &stubRingTarget{databases: map[string]map[string][]ringclient.FieldDecl{}, createdBuckets: map[string][]ringclient.FieldDecl{}}
}

func (s *stubRingTarget) withBucket(database, bucket string, fields ...ringclient.FieldDecl) *stubRingTarget {
	if s.databases[database] == nil {
		s.databases[database] = map[string][]ringclient.FieldDecl{}
	}
	s.databases[database][bucket] = fields
	return s
}

func (s *stubRingTarget) DatabaseExists(_ context.Context, database string) (bool, error) {
	_, found := s.databases[database]
	return found, nil
}

func (s *stubRingTarget) CreateDatabase(_ context.Context, database string) error {
	s.createdDatabases = append(s.createdDatabases, database)
	s.databases[database] = map[string][]ringclient.FieldDecl{}
	return nil
}

func (s *stubRingTarget) DescribeBucket(_ context.Context, database, bucket string) (ringclient.BucketInfo, bool, error) {
	fields, found := s.databases[database][bucket]
	if !found {
		return ringclient.BucketInfo{}, false, nil
	}
	info := ringclient.BucketInfo{Database: database, Name: bucket, Fields: []ringclient.FieldInfo{{ID: 1, Name: "_id_", Type: "any"}}}
	for index, field := range fields {
		info.Fields = append(info.Fields, ringclient.FieldInfo{ID: uint16(index + 2), Name: field.Name, Type: field.Type})
	}
	return info, true, nil
}

func (s *stubRingTarget) CreateBucket(_ context.Context, database, bucket string, fields []ringclient.FieldDecl) error {
	s.createdBuckets[database+"."+bucket] = fields
	s.withBucket(database, bucket, fields...)
	return nil
}

// scanOf scans documents held in memory.
func scanOf(t *testing.T, documents ...bson.D) scanResult {
	t.Helper()
	scan, err := scanFile(opensBSON(bsonFileOf(t, documents...)))
	if err != nil {
		t.Fatalf("scanFile: %v", err)
	}
	return scan
}

func usersParams() ringIngestParams {
	return ringIngestParams{Database: "app", Bucket: "users", File: "users.bson"}
}

// refusalText unwraps the user's sentence out of what prepareTarget returned.
func refusalText(t *testing.T, err error) string {
	t.Helper()
	var refusal *ingestRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("error %v (%T), want an *ingestRefusal that names the fix", err, err)
	}
	return refusal.Message
}

func TestUndeclaredFieldIntoExistingBucketNamesTheAlterFix(t *testing.T) {
	target := newStubRingTarget().withBucket("app", "users", ringclient.FieldDecl{Name: "name", Type: "string"})
	scan := scanOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}, {Key: "email", Value: "a@x"}})
	_, err := prepareTarget(context.Background(), target, usersParams(), scan)
	text := refusalText(t, err)
	want := `users.bson has a field "email" that bucket "users" does not declare. Nothing was written. Add it: 'alter bucket users in app { add email string }', or remove it from the file.`
	if text != want {
		t.Errorf("refusal:\n got %q\nwant %q", text, want)
	}
	if len(target.createdBuckets) != 0 {
		t.Errorf("the run created %v before refusing", target.createdBuckets)
	}
}

func TestTypeMismatchNamesTheNewBucketFix(t *testing.T) {
	target := newStubRingTarget().withBucket("app", "users", ringclient.FieldDecl{Name: "age", Type: "int"})
	var documents []bson.D
	documents = append(documents, bson.D{{Key: "_id", Value: "first"}, {Key: "age", Value: int32(5)}})
	for i := 0; i < 12; i++ {
		documents = append(documents, bson.D{{Key: "_id", Value: int32(i)}, {Key: "age", Value: "old"}})
	}
	_, err := prepareTarget(context.Background(), target, usersParams(), scanOf(t, documents...))
	text := refusalText(t, err)
	want := `Field "age" is a string in 12 documents of users.bson and bucket "users" declares int. A declared type cannot be changed, so load into a new bucket: --into app.users2, or fix the 12 values (the first is document 2).`
	if text != want {
		t.Errorf("refusal:\n got %q\nwant %q", text, want)
	}
}

// TestTypeCompatibilityFollowsTheDeclaredWord: a declared word holds the words typed-fields
// 3.1 says it accepts, null is always accepted, and a store field is never a file's field.
func TestTypeCompatibilityFollowsTheDeclaredWord(t *testing.T) {
	tests := []struct {
		declared string
		value    any
		wantOK   bool
	}{
		{"any", "x", true},
		{"string", "x", true},
		{"string", int32(1), false},
		{"int", int64(1), true},
		{"int32", int32(1), true},
		{"double", 1.5, true},
		{"double", int32(1), true},
		{"int", 1.5, false},
		{"number", 1.5, true},
		{"number", "x", false},
		{"bool", true, true},
		{"datetime", true, false},
		{"hashStore", "x", false},
	}
	for _, tc := range tests {
		t.Run(tc.declared+"/"+bsonWordOf(t, tc.value), func(t *testing.T) {
			target := newStubRingTarget().withBucket("app", "users", ringclient.FieldDecl{Name: "f", Type: tc.declared})
			scan := scanOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: "f", Value: tc.value}}, bson.D{{Key: "_id", Value: "b"}, {Key: "f", Value: nil}})
			_, err := prepareTarget(context.Background(), target, usersParams(), scan)
			if (err == nil) != tc.wantOK {
				t.Errorf("a %s value into a %s field: error %v, want ok = %v", bsonWordOf(t, tc.value), tc.declared, err, tc.wantOK)
			}
		})
	}
}

func bsonWordOf(t *testing.T, value any) string {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "f", Value: value}})
	if err != nil {
		t.Fatal(err)
	}
	return planTypeWord(bson.Raw(raw).Lookup("f").Type)
}

func TestNoCreateRefusesAMissingBucketAndNamesBothFixes(t *testing.T) {
	target := newStubRingTarget().withBucket("app", "other")
	params := usersParams()
	params.NoCreate = true
	_, err := prepareTarget(context.Background(), target, params, scanOf(t, bson.D{{Key: "_id", Value: "a"}}))
	text := refusalText(t, err)
	want := `Bucket "users" not found in database "app". Run 'show buckets in app' to list what exists, or leave out --no-create and ingest will create it from the file (run with --dry-run first to see the declaration).`
	if text != want {
		t.Errorf("refusal:\n got %q\nwant %q", text, want)
	}
	// And a missing database, with --no-create, says its own fix.
	_, err = prepareTarget(context.Background(), newStubRingTarget(), params, scanOf(t, bson.D{{Key: "_id", Value: "a"}}))
	text = refusalText(t, err)
	want = `Database "app" not found. Run 'show databases' to list what exists, or 'create database app', or leave out --no-create.`
	if text != want {
		t.Errorf("refusal:\n got %q\nwant %q", text, want)
	}
}

func TestMissingDatabaseIsCreatedWithoutNoCreate(t *testing.T) {
	target := newStubRingTarget()
	scan := scanOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}, {Key: "age", Value: int32(3)}})
	prepared, err := prepareTarget(context.Background(), target, usersParams(), scan)
	if err != nil {
		t.Fatalf("prepareTarget: %v", err)
	}
	if len(target.createdDatabases) != 1 || target.createdDatabases[0] != "app" {
		t.Errorf("created databases %v, want [app]", target.createdDatabases)
	}
	fields := target.createdBuckets["app.users"]
	if len(fields) != 2 || fields[0].Name != "name" || fields[0].Type != "string" || fields[1].Name != "age" || fields[1].Type != "int" {
		t.Errorf("created bucket fields %+v, want name string and age int", fields)
	}
	if !prepared.CreatedDatabase || !prepared.CreatedBucket {
		t.Errorf("prepared %+v should say the run created the database and the bucket", prepared)
	}
}

// TestDryRunCreatesNothingAndSaysWhatItWould: nothing is created, and the plan says so.
func TestDryRunCreatesNothingAndSaysWhatItWould(t *testing.T) {
	target := newStubRingTarget()
	params := usersParams()
	params.DryRun = true
	prepared, err := prepareTarget(context.Background(), target, params, scanOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}}))
	if err != nil {
		t.Fatalf("prepareTarget: %v", err)
	}
	if len(target.createdDatabases) != 0 || len(target.createdBuckets) != 0 {
		t.Errorf("a dry run created %v and %v", target.createdDatabases, target.createdBuckets)
	}
	if !prepared.WouldCreateDatabase || !prepared.WouldCreateBucket || len(prepared.Declaration) != 1 {
		t.Errorf("prepared %+v should say it would create the database and the bucket, with one declared field", prepared)
	}
}

// TestSeveralUndeclaredFieldsAreAllNamedInOneAlter: one refusal lists every missing field.
func TestSeveralUndeclaredFieldsAreAllNamedInOneAlter(t *testing.T) {
	target := newStubRingTarget().withBucket("app", "users")
	scan := scanOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: "email", Value: "x"}, {Key: "age", Value: int32(1)}, {Key: "odd name", Value: true}})
	_, err := prepareTarget(context.Background(), target, usersParams(), scan)
	text := refusalText(t, err)
	for _, want := range []string{"3 fields that bucket \"users\" does not declare", "'alter bucket users in app { add email string, add age int }'", `"odd name"`} {
		if !strings.Contains(text, want) {
			t.Errorf("refusal %q does not say %q", text, want)
		}
	}
}

// TestTypeMismatchUsesTheRightArticle: the refusal reads "is an int" and "is a string", never
// "is a int".
func TestTypeMismatchUsesTheRightArticle(t *testing.T) {
	target := newStubRingTarget().withBucket("app", "users", ringclient.FieldDecl{Name: "age", Type: "string"})
	_, err := prepareTarget(context.Background(), target, usersParams(),
		scanOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: "age", Value: int32(5)}}))
	text := refusalText(t, err)
	if !strings.Contains(text, `Field "age" is an int in 1 document of users.bson`) {
		t.Errorf("the refusal does not say %q:\n%s", "is an int", text)
	}
	for _, word := range []string{"array", "object", "objectid", "any"} {
		if got := withArticle(word); got != "an "+word {
			t.Errorf("withArticle(%q) = %q, want %q", word, got, "an "+word)
		}
	}
	if got := withArticle("string or int"); got != "a string or int" {
		t.Errorf("withArticle(%q) = %q", "string or int", got)
	}
}
