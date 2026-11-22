package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// bucketRecordNames returns the names of the bucket records of a dump, in file order: the
// order restore meets them in.
func bucketRecordNames(t *testing.T, path string) []string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	reader := newDumpReader(file)
	var names []string
	for {
		kind, raw, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return names
		}
		if err != nil {
			t.Fatal(err)
		}
		if kind != dumpKindBucket {
			continue
		}
		record, err := decodeRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		bucket := record.(*dumpBucket)
		names = append(names, bucket.DB+"."+bucket.Name)
	}
}

// fillLinkedRing builds database app where the buckets that point come before the bucket
// they point at in the dump (the ring lists buckets by name): Comment links to Post and to
// User, Post links to User through a string and through a hashStore, and User links to
// itself. Every bucket holds documents, so a link that was dropped on restore shows.
func fillLinkedRing(t *testing.T, client *ringclient.Client, database string) {
	t.Helper()
	ctx := restoreTestContext(t)
	if _, err := client.CreateDatabase(ctx, database, 1); err != nil {
		t.Fatal(err)
	}
	create := func(name string, fields ...ringclient.FieldDecl) {
		t.Helper()
		if _, err := client.CreateBucket(ctx, database, name, fields); err != nil {
			t.Fatalf("creating %s.%s: %v", database, name, err)
		}
	}
	create("User", ringclient.FieldDecl{Name: "Name", Type: "string"}, ringclient.FieldDecl{Name: "Boss", Type: "string", Of: "User"})
	create("Post", ringclient.FieldDecl{Name: "Title", Type: "string"}, ringclient.FieldDecl{Name: "Author", Type: "string", Of: "User"},
		ringclient.FieldDecl{Name: "Likers", Type: "hashStore", Of: "User"})
	create("Comment", ringclient.FieldDecl{Name: "Body", Type: "string"}, ringclient.FieldDecl{Name: "On", Type: "string", Of: "Post"},
		ringclient.FieldDecl{Name: "By", Type: "string", Of: "User"})
	for _, name := range []string{"ada", "grace"} {
		if _, err := client.Bucket(database, "User").Insert(ctx, name, ringclient.Doc{"Name": name, "Boss": "ada"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Bucket(database, "Post").Insert(ctx, "p1", ringclient.Doc{"Title": "hello", "Author": "ada"}); err != nil {
		t.Fatal(err)
	}
	written(t)(client.Bucket(database, "Post").Put(ctx, "p1", "Likers", "grace", bson.D{{Key: "Name", Value: "grace"}}))
	if _, err := client.Bucket(database, "Comment").Insert(ctx, "c1", ringclient.Doc{"Body": "nice", "On": "p1", "By": "grace"}); err != nil {
		t.Fatal(err)
	}
}

// declaredOf returns, field by field, what the ring says a bucket's fields are of.
func declaredOf(t *testing.T, client *ringclient.Client, database, bucket string) map[string]string {
	t.Helper()
	info, err := client.DescribeBucket(restoreTestContext(t), database, bucket)
	if err != nil {
		t.Fatalf("describing %s.%s: %v", database, bucket, err)
	}
	of := map[string]string{}
	for _, field := range info.Fields {
		if field.Of != "" {
			of[field.Name] = field.Of
		}
	}
	return of
}

// TestRestoreCreatesALinkTargetBeforeTheBucketThatPointsAtIt: the dump lists Comment first,
// and Comment links to Post and User. A restore that created buckets in file order would be
// refused by the ring for a link to a bucket that does not exist yet. The second dump must be
// byte-equal to the first: the links, their order and every document came back.
func TestRestoreCreatesALinkTargetBeforeTheBucketThatPointsAtIt(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillLinkedRing(t, source, "app")
	first := dumpToFile(t, sourceAddr)
	if names := bucketRecordNames(t, first); len(names) != 3 || names[0] != "app.Comment" {
		t.Fatalf("this test needs the pointing bucket first in the dump; the file lists %v", names)
	}

	code, _, stderr := restoreOnHost(t, targetAddr, first)
	if code != ExitOK {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	for _, bucket := range []string{"Comment", "Post", "User"} {
		want, got := declaredOf(t, source, "app", bucket), declaredOf(t, target, "app", bucket)
		if fmt.Sprint(want) != fmt.Sprint(got) {
			t.Errorf("bucket %s links %v after the restore, %v before", bucket, got, want)
		}
	}
	second := dumpToFile(t, targetAddr)
	want, got := dumpBodies(t, first), dumpBodies(t, second)
	if len(want) != len(got) {
		t.Fatalf("the dump of the restored ring has %d records, the original %d", len(got), len(want))
	}
	for index := range want {
		if !bytes.Equal(want[index], got[index]) {
			t.Fatalf("record %d of the dump of the restored ring differs from the original", index)
		}
	}
}

// TestRestoreCreatesASelfLinkedBucket: a bucket that links to itself needs no ordering, and
// the ring takes the link at the create.
func TestRestoreCreatesASelfLinkedBucket(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	ctx := restoreTestContext(t)
	if _, err := source.CreateDatabase(ctx, "org", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CreateBucket(ctx, "org", "Person", []ringclient.FieldDecl{{Name: "Boss", Type: "string", Of: "Person"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Bucket("org", "Person").Insert(ctx, "ceo", ringclient.Doc{"Boss": "ceo"}); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := restoreOnHost(t, targetAddr, dumpToFile(t, sourceAddr)); code != ExitOK {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	if got := declaredOf(t, target, "org", "Person"); got["Boss"] != "Person" {
		t.Errorf("the self link came back as %v", got)
	}
}

// TestRestoreRefusesBucketsThatLinkInACircleBeforeCreatingAnything: the ring cannot make a
// circle (a bucket may be 'of' only a bucket that exists when it is created, and alter
// cannot add a reference), so a dump that holds one was not written by the ring. Restore
// cannot create it either, and says so before it writes anything. A longer circle is found
// the same way as a pair.
func TestRestoreRefusesBucketsThatLinkInACircleBeforeCreatingAnything(t *testing.T) {
	path := linkDump(t, func(user, post *dumpBucket) {
		user.Fields = append(user.Fields, dumpField{Name: "Latest", Type: "string", Of: "Post"})
		post.Fields = append(post.Fields, dumpField{Name: "Author", Type: "string", Of: "User"})
	})
	text := restoreRefusalText(t, checkRestoreTargets(context.Background(), &ringRestoreFlags{file: path}, &stubRestoreTarget{}, path))
	for _, want := range []string{"in a circle", `"app.Post"`, `"app.User"`, "Nothing was written."} {
		if !strings.Contains(text, want) {
			t.Errorf("the circular-link refusal does not contain %q:\n%s", want, text)
		}
	}
}

// TestRestoreRenamesLinksToTheRenamedDatabase: with --as the one database lands under a new
// name, and a link written as <Database>.<Bucket> that names the renamed database follows it.
// The ring writes a link inside its own database without the database, so the dump is made
// by hand to carry the qualified form, which the ring accepts at create time.
func TestRestoreRenamesLinksToTheRenamedDatabase(t *testing.T) {
	targetAddr, target := newRingTestHost(t)
	records := []any{
		dumpTestHeader(),
		&dumpDB{T: dumpKindDB, Name: "app", RF: 1, Storage: "disk"},
		&dumpBucket{T: dumpKindBucket, DB: "app", Name: "Post", Fields: []dumpField{{Name: "Author", Type: "string", Of: "app.User"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}},
		&dumpBucket{T: dumpKindBucket, DB: "app", Name: "User", Fields: []dumpField{{Name: "Name", Type: "string"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}},
	}
	path := writeVerifyTestDump(t, records, dumpCounters{Databases: 1, Buckets: 2})
	if code, _, stderr := restoreOnHost(t, targetAddr, path, "--db", "app", "--as", "app_restored"); code != ExitOK {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	if got := declaredOf(t, target, "app_restored", "Post"); got["Author"] == "" {
		t.Errorf("the link did not come back: %v", got)
	}
	if _, err := target.DescribeBucket(restoreTestContext(t), "app", "User"); err == nil {
		t.Errorf("a database named app exists on the target after a restore under another name")
	}
}

func linkDump(t *testing.T, mutate func(user, post *dumpBucket)) string {
	t.Helper()
	records := dumpVerifyTestRecords(t, "hashStore")
	user := records[2].(*dumpBucket)
	post := &dumpBucket{T: dumpKindBucket, DB: user.DB, Name: "Post", Fields: []dumpField{{Name: "Title", Type: "string"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}}
	mutate(user, post)
	records = append(records[:3], append([]any{post}, records[3:]...)...)
	return writeVerifyTestDump(t, records, dumpVerifyTestCounts)
}

// TestRestoreRefusesALinkToAnAbsentDatabase: Post links to a bucket of a database that is
// neither in the dump nor on the target. Nothing is created, and the text names the fix.
func TestRestoreRefusesALinkToAnAbsentDatabase(t *testing.T) {
	path := linkDump(t, func(_, post *dumpBucket) {
		post.Fields = append(post.Fields, dumpField{Name: "Author", Type: "string", Of: "Accounts.Person"})
	})
	target := &stubRestoreTarget{describeErr: &ringclient.Error{Code: cluster.CodeNotFound, Message: "no such bucket"}}
	text := restoreRefusalText(t, checkRestoreTargets(context.Background(), &ringRestoreFlags{file: path}, target, path))
	for _, want := range []string{`Bucket "Post" field "Author"`, `"Accounts.Person"`, "Nothing was written.", "Create it on the target ring first"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal does not contain %q:\n%s", want, text)
		}
	}
}

// TestRestoreOfOneDatabaseRefusesALinkIntoAnUnrestoredDatabaseOfTheDump: --db app leaves zeta
// out, so a link into zeta needs its bucket on the target, and the refusal names the restore
// of zeta that fixes it.
func TestRestoreOfOneDatabaseRefusesALinkIntoAnUnrestoredDatabaseOfTheDump(t *testing.T) {
	records := dumpVerifyTestRecords(t, "hashStore")
	records[2].(*dumpBucket).Fields = append(records[2].(*dumpBucket).Fields, dumpField{Name: "Home", Type: "string", Of: "zeta.Place"})
	records = append(records, &dumpDB{T: dumpKindDB, Name: "zeta", RF: 1},
		&dumpBucket{T: dumpKindBucket, DB: "zeta", Name: "Place", Fields: []dumpField{{Name: "Name", Type: "string"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}})
	path := writeVerifyTestDump(t, records, dumpVerifyTestCounts)
	target := &stubRestoreTarget{describeErr: &ringclient.Error{Code: cluster.CodeNotFound, Message: "no such bucket"}}
	text := restoreRefusalText(t, checkRestoreTargets(context.Background(), &ringRestoreFlags{file: path, database: "app"}, target, path))
	if !strings.Contains(text, "--db zeta") || !strings.Contains(text, "Nothing was written.") {
		t.Errorf("the refusal does not name the restore of the database that holds the target:\n%s", text)
	}
}

// TestRestoreAcceptsALinkToABucketAlreadyOnTheTarget: a bucket outside the run that exists
// on the target is a legal link target.
func TestRestoreAcceptsALinkToABucketAlreadyOnTheTarget(t *testing.T) {
	path := linkDump(t, func(_, post *dumpBucket) {
		post.Fields = append(post.Fields, dumpField{Name: "Author", Type: "string", Of: "Accounts.Person"})
	})
	if err := checkRestoreTargets(context.Background(), &ringRestoreFlags{file: path}, &stubRestoreTarget{}, path); err != nil {
		t.Errorf("a link to a bucket on the target was refused: %v", err)
	}
}

// TestRestoreRefusesALinkToABucketTheDumpDoesNotHold: Post links to a bucket of its own
// database that is not in the dump, so it cannot exist on an empty target.
func TestRestoreRefusesALinkToABucketTheDumpDoesNotHold(t *testing.T) {
	path := linkDump(t, func(_, post *dumpBucket) {
		post.Fields = append(post.Fields, dumpField{Name: "Author", Type: "string", Of: "Missing"})
	})
	text := restoreRefusalText(t, checkRestoreTargets(context.Background(), &ringRestoreFlags{file: path}, &stubRestoreTarget{}, path))
	for _, want := range []string{`Bucket "Post" field "Author"`, `"app.Missing"`, "not in this dump", "Nothing was written."} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal does not contain %q:\n%s", want, text)
		}
	}
}

// TestRestoreAcceptsALinkToALaterDatabaseOfTheSameRun: a link into a database the file holds
// later is no longer a refusal; the databases that link into each other are created together.
func TestRestoreAcceptsALinkToALaterDatabaseOfTheSameRun(t *testing.T) {
	records := dumpVerifyTestRecords(t, "hashStore")
	user := records[2].(*dumpBucket)
	user.Fields = append(user.Fields, dumpField{Name: "Home", Type: "string", Of: "zeta.Place"})
	later := []any{
		&dumpDB{T: dumpKindDB, Name: "zeta", RF: 1},
		&dumpBucket{T: dumpKindBucket, DB: "zeta", Name: "Place", Fields: []dumpField{{Name: "Name", Type: "string"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}},
	}
	path := writeVerifyTestDump(t, append(append([]any{}, records...), later...), dumpVerifyTestCounts)
	plan, err := planRestoreTargets(context.Background(), &ringRestoreFlags{file: path}, &stubRestoreTarget{}, path)
	if err != nil {
		t.Fatalf("pass 2 refused a link into a later database of the run: %v", err)
	}
	if !plan.group["app"] || !plan.group["zeta"] || len(plan.groupOrder) != 2 || plan.groupOrder[0].Name != "Place" {
		t.Errorf("the group is %v, order %v: want app and zeta, Place first", plan.group, plan.groupOrder)
	}
}

// TestStoppedRestoreNamesTheBucketsItCreatedAheadForLinks: the restore creates User and Post
// before it reaches their records, because Comment links to them. When it stops in Comment,
// the report names the two that exist empty, so the operator knows what to drop.
func TestStoppedRestoreNamesTheBucketsItCreatedAheadForLinks(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillLinkedRing(t, source, "app")
	path := dumpToFile(t, sourceAddr)

	creates := 0
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	restoreBeforeBucketCreate = func() {
		creates++
		if creates != 3 {
			return
		}
		// An application makes Comment with its document between the restore's check and its
		// create, so the restore's insert of c1 is refused.
		restoreBeforeBucketCreate = nil
		ctx := restoreTestContext(t)
		if _, err := target.CreateBucket(ctx, "app", "Comment", []ringclient.FieldDecl{
			{Name: "Body", Type: "string"}, {Name: "On", Type: "string", Of: "Post"}, {Name: "By", Type: "string", Of: "User"}}); err != nil {
			t.Errorf("creating app.Comment: %v", err)
			return
		}
		if _, err := target.Bucket("app", "Comment").Insert(ctx, "c1", ringclient.Doc{"Body": "written by the application"}); err != nil {
			t.Errorf("inserting c1: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, "now holds a document") {
		t.Fatalf("restore exited %d, want %d with the not-empty refusal:\n%s", code, ExitRefused, stderr)
	}
	if !strings.Contains(stderr, `Buckets app.User, app.Post, app.Comment were created empty because other buckets link to them`) {
		t.Errorf("the report does not name the buckets created ahead:\n%s", stderr)
	}
}

// interleavedRing builds the shape no order of whole databases restores: database a holds Y,
// database b holds X of a.Y, and then a gets Z of b.X. Every create was legal on the ring and no
// bucket links in a circle, but a links into b and b links into a.
func interleavedRing(t *testing.T, client *ringclient.Client) {
	t.Helper()
	ctx := restoreTestContext(t)
	for _, database := range []string{"a", "b"} {
		if _, err := client.CreateDatabase(ctx, database, 1); err != nil {
			t.Fatal(err)
		}
	}
	steps := []struct {
		database, bucket, of string
	}{{"a", "Y", ""}, {"b", "X", "a.Y"}, {"a", "Z", "b.X"}}
	for _, step := range steps {
		fields := []ringclient.FieldDecl{{Name: "Name", Type: "string"}}
		if step.of != "" {
			fields = append(fields, ringclient.FieldDecl{Name: "Ref", Type: "string", Of: step.of})
		}
		if _, err := client.CreateBucket(ctx, step.database, step.bucket, fields); err != nil {
			t.Fatalf("creating %s.%s: %v", step.database, step.bucket, err)
		}
		if _, err := client.Bucket(step.database, step.bucket).Insert(ctx, "e1", ringclient.Doc{"Name": "xxxx" + step.bucket}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRestoreOfDatabasesThatLinkIntoEachOtherRestoresWhole: the interleaved ring dumps and
// restores whole, byte-equal on a second dump. One database alone cannot be restored: each
// refusal says the databases must be restored together, which does work.
func TestRestoreOfDatabasesThatLinkIntoEachOtherRestoresWhole(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	interleavedRing(t, source)
	first := dumpToFile(t, sourceAddr)

	wholeAddr, whole := newRingTestHost(t)
	if code, _, stderr := restoreOnHost(t, wholeAddr, first); code != ExitOK {
		t.Fatalf("restore of the whole dump exited %d:\n%s", code, stderr)
	}
	for _, check := range []struct{ database, bucket, field, of string }{{"b", "X", "Ref", "a.Y"}, {"a", "Z", "Ref", "b.X"}} {
		if got := declaredOf(t, whole, check.database, check.bucket); got[check.field] != check.of {
			t.Errorf("%s.%s links %v after the restore, want %s", check.database, check.bucket, got, check.of)
		}
	}
	want, got := dumpBodies(t, first), dumpBodies(t, dumpToFile(t, wholeAddr))
	if len(want) != len(got) {
		t.Fatalf("the dump of the restored ring has %d records, the original %d", len(got), len(want))
	}
	for index := range want {
		if !bytes.Equal(want[index], got[index]) {
			t.Fatalf("record %d differs from the original", index)
		}
	}

	for _, database := range []string{"a", "b"} {
		aloneAddr, _ := newRingTestHost(t)
		code, _, stderr := restoreOnHost(t, aloneAddr, first, "--db", database)
		if code != ExitRefused || !strings.Contains(stderr, "restore them together, without --db") {
			t.Errorf("--db %s alone exited %d, want a refusal that says to restore them together:\n%s", database, code, stderr)
		}
	}
}

// TestRestoreLinkIntoAnEarlierDatabaseOfTheSameRun: z_audit links into m_accounts, which the
// file holds first, so one run restores both.
func TestRestoreLinkIntoAnEarlierDatabaseOfTheSameRun(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	ctx := restoreTestContext(t)
	for _, database := range []string{"m_accounts", "z_audit"} {
		if _, err := source.CreateDatabase(ctx, database, 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.CreateBucket(ctx, "m_accounts", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CreateBucket(ctx, "z_audit", "Entry", []ringclient.FieldDecl{{Name: "Who", Type: "string", Of: "m_accounts.User"}}); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := restoreOnHost(t, targetAddr, dumpToFile(t, sourceAddr)); code != ExitOK {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	if got := declaredOf(t, target, "z_audit", "Entry"); got["Who"] != "m_accounts.User" {
		t.Errorf("Entry links %v after the restore", got)
	}
}

// TestRestoreRefusesAnAheadBucketAnApplicationWroteTo: User is created empty ahead of the file
// because Comment and Post link to it, and the file reaches it last. An application that writes
// to it in between is refused when the restore reaches it, not merged with. The window is the
// round trip between the emptiness check and the first insert, not the length of the file.
func TestRestoreRefusesAnAheadBucketAnApplicationWroteTo(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillLinkedRing(t, source, "app")
	path := dumpToFile(t, sourceAddr)

	t.Cleanup(func() { restoreAfterPassTwo = nil })
	restoreAfterPassTwo = nil
	creates := 0
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	restoreBeforeBucketCreate = func() {
		creates++
		if creates != 3 { // User and Post exist; Comment is about to be created
			return
		}
		restoreBeforeBucketCreate = nil
		if _, err := target.Bucket("app", "User").Insert(restoreTestContext(t), "intruder", ringclient.Doc{"Name": "written by the application"}); err != nil {
			t.Errorf("inserting the intruder: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, `bucket "User" in database "app" was created empty for a link and now holds a document`) || !strings.Contains(stderr, "--as app_restored") {
		t.Fatalf("restore exited %d, want %d with the not-empty refusal and the --as command:\n%s", code, ExitRefused, stderr)
	}
	page, err := target.Bucket("app", "User").List(restoreTestContext(t), nil, ringclient.PageRequest{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].ID != "intruder" {
		t.Errorf("User holds %d entries, want only the intruder: the restore merged into it", len(page.Entries))
	}
}

// TestRestoreRefusesALinkTargetAnApplicationCreatedBeforeTheEarlyCreate: while the restore
// creates User ahead of the file, an application makes Post, which the restore creates next.
// The early create must check that the name is free, as the in-file create does, and refuse
// instead of adopting it.
func TestRestoreRefusesALinkTargetAnApplicationCreatedBeforeTheEarlyCreate(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	ctx := restoreTestContext(t)
	if _, err := source.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	postFields := []ringclient.FieldDecl{{Name: "Author", Type: "string", Of: "User"}}
	for _, name := range []string{"Comment", "Post"} {
		if _, err := source.CreateBucket(ctx, "app", name, postFields); err != nil {
			t.Fatal(err)
		}
	}
	path := dumpToFile(t, sourceAddr)

	// The early creates run User, Comment, Post. While Comment is created, an application
	// makes Post, so Post's own early create finds its name taken.
	creates := 0
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	restoreBeforeBucketCreate = func() {
		creates++
		if creates != 2 {
			return
		}
		restoreBeforeBucketCreate = nil
		if _, err := target.CreateBucket(ctx, "app", "Post", postFields); err != nil {
			t.Errorf("creating app.Post: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, `bucket "Post" already exists in database "app"`) {
		t.Fatalf("restore exited %d, want %d with the bucket-exists refusal:\n%s", code, ExitRefused, stderr)
	}
}

// TestStoppedRestoreOfLinkedDatabasesDoesNotPrintACommandThatMislinks: a and z link into each
// other's buckets (z.Entry is of a.User), so a restore that stops must not print `--db a --as
// a_restored` plus a plain `--db z`: z would stay declared of the abandoned a.User while its
// data went to a_restored. It says the databases can only be restored together.
func TestStoppedRestoreOfLinkedDatabasesDoesNotPrintACommandThatMislinks(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	ctx := restoreTestContext(t)
	for _, database := range []string{"a", "z"} {
		if _, err := source.CreateDatabase(ctx, database, 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.CreateBucket(ctx, "a", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CreateBucket(ctx, "z", "Entry", []ringclient.FieldDecl{{Name: "Who", Type: "string", Of: "a.User"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Bucket("a", "User").Insert(ctx, "ada", ringclient.Doc{"Name": "ada"}); err != nil {
		t.Fatal(err)
	}
	path := dumpToFile(t, sourceAddr)

	creates := 0
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	restoreBeforeBucketCreate = func() {
		creates++
		if creates != 2 { // a.User exists; z.Entry is about to be created
			return
		}
		restoreBeforeBucketCreate = nil
		if _, err := target.Bucket("a", "User").Insert(restoreTestContext(t), "intruder", ringclient.Doc{"Name": "x"}); err != nil {
			t.Errorf("inserting the intruder: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused {
		t.Fatalf("restore exited %d, want %d:\n%s", code, ExitRefused, stderr)
	}
	if strings.Contains(stderr, "--db a --as") || strings.Contains(stderr, "--db z") {
		t.Errorf("the report prints a command that restores one linked database alone:\n%s", stderr)
	}
	if !strings.Contains(stderr, "can only be restored together") {
		t.Errorf("the report does not say the databases can only be restored together:\n%s", stderr)
	}
}

// TestAheadBucketBookkeepingIsNotQuadratic: noting that a bucket's records were reached is a map
// lookup. A hundred thousand buckets used to take seconds (a search and a slice shift each).
func TestAheadBucketBookkeepingIsNotQuadratic(t *testing.T) {
	var progress restoreProgress
	const buckets = 200000
	for index := 0; index < buckets; index++ {
		progress.createdAhead("app", fmt.Sprintf("B%d", index))
	}
	started := time.Now()
	for index := 0; index < buckets; index++ {
		progress.beginBucket("app", fmt.Sprintf("B%d", index))
		if index%1000 == 0 {
			progress.aheadWaiting()
		}
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Errorf("reaching %d buckets created ahead took %s: the bookkeeping is not constant per bucket", buckets, elapsed)
	}
	if _, waiting := progress.aheadWaiting(); waiting != 0 {
		t.Errorf("%d buckets still wait after all were reached", waiting)
	}
}

// TestStoppedRestoreOfAGroupAfterTheFileChangedPrintsNoRestoreAloneCommand: the file is edited
// after pass 2, so the footer does not match at the end and the report lists the databases it
// wrote in full. For databases that link into each other it must not print `--db a --as
// a_restored` and `--db b --as b_restored` and then say no command restores one alone.
func TestStoppedRestoreOfAGroupAfterTheFileChangedPrintsNoRestoreAloneCommand(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	interleavedRing(t, source)
	path := dumpToFile(t, sourceAddr)
	targetAddr, _ := newRingTestHost(t)
	t.Cleanup(func() { restoreAfterPassTwo = nil })
	restoreAfterPassTwo = func() {
		restoreAfterPassTwo = nil
		info, err := os.Stat(path)
		if err != nil {
			t.Error(err)
			return
		}
		content, err := os.ReadFile(path)
		if err != nil {
			t.Error(err)
			return
		}
		content[bytes.Index(content, []byte("xxxx"))] = 'y'
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Error(err)
			return
		}
		if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
			t.Error(err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitUsage || !strings.Contains(stderr, "changed while it was being restored") {
		t.Fatalf("restore exited %d, want %d with the changed-file report:\n%s", code, ExitUsage, stderr)
	}
	if strings.Contains(stderr, "--db a --as") || strings.Contains(stderr, "--db b --as") {
		t.Errorf("the report prints a command that restores one database of the group alone:\n%s", stderr)
	}
	if !strings.Contains(stderr, "can only be restored together") {
		t.Errorf("the report does not say the databases can only be restored together:\n%s", stderr)
	}
}

// TestRestoreRefusesAGroupDatabaseThatGainedABucketAfterPassTwo: a writer makes a bucket in a
// database of the group after pass 2 and before the group's database is created. The check
// right before each create refuses, as it does for a database outside a group.
func TestRestoreRefusesAGroupDatabaseThatGainedABucketAfterPassTwo(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	interleavedRing(t, source)
	path := dumpToFile(t, sourceAddr)
	t.Cleanup(func() { restoreAfterPassTwo = nil })
	restoreAfterPassTwo = func() {
		restoreAfterPassTwo = nil
		ctx := restoreTestContext(t)
		if _, err := target.CreateDatabase(ctx, "a", 1); err != nil {
			t.Errorf("creating a: %v", err)
			return
		}
		if _, err := target.CreateBucket(ctx, "a", "Stray", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
			t.Errorf("creating a.Stray: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, `database "a" gained 1 bucket (Stray)`) {
		t.Fatalf("restore exited %d, want %d with the gained-a-bucket refusal:\n%s", code, ExitRefused, stderr)
	}
}

// TestRestoreRefusesAGroupDatabaseThatGainedABucketWhileTheGroupWasBuilt: a writer makes a bucket
// of another name in a database of the group after the group's databases exist. The file's
// db record re-reads the database and refuses.
func TestRestoreRefusesAGroupDatabaseThatGainedABucketWhileTheGroupWasBuilt(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	interleavedRing(t, source)
	path := dumpToFile(t, sourceAddr)
	creates := 0
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	restoreBeforeBucketCreate = func() {
		creates++
		if creates != 3 { // the last of the group's three linked buckets
			return
		}
		restoreBeforeBucketCreate = nil
		if _, err := target.CreateBucket(restoreTestContext(t), "a", "Stray", []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
			t.Errorf("creating a.Stray: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, `database "a" holds 3 buckets, and the restore made 2 buckets in it`) {
		t.Fatalf("restore exited %d, want %d with the group-database refusal:\n%s", code, ExitRefused, stderr)
	}
}
