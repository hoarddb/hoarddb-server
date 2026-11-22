package wire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

const u30iDB = "app"

func u30iOpen(t testing.TB, dir string) *Node {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, vouchedTestConfig(t, NodeConfig{Dir: dir}))
	if err != nil {
		t.Fatalf("OpenNode(%s): %v", dir, err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Errorf("closing the node: %v", err)
		}
	})
	return node
}

// u30iFresh is a node with database app and bucket User { Name, Age, City, Purchases }.
func u30iFresh(t *testing.T) (*Node, string) {
	t.Helper()
	dir := t.TempDir()
	node := u30iOpen(t, dir)
	u30iSchema(t, node)
	return node, dir
}

func u30iSchema(t testing.TB, node *Node) {
	t.Helper()
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "User",
		Field{Name: "Name", Type: "string"}, Field{Name: "Age", Type: "int"}, Field{Name: "City", Type: "string"},
		Field{Name: "Purchases", Type: "fifoStore"}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func u30iUser(id, name string, age int32) bson.D {
	return bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: name}, {Key: "Age", Value: age}, {Key: "City", Value: "Oslo"}}
}

func u30iString(t *testing.T, doc Document, field string) string {
	t.Helper()
	value, err := bson.Raw(doc.Body).LookupErr(field)
	if err != nil {
		t.Fatalf("field %q is not in %s: %v", field, bson.Raw(doc.Body), err)
	}
	return value.StringValue()
}

func u30iCode(t *testing.T, err error, want cluster.Code) {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != want {
		t.Fatalf("err = %v, want a *cluster.Error with code %v", err, want)
	}
}

func TestNodeInsertGetRoundTrip(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	inserted, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36))
	if err != nil || inserted.ID != "u1" || inserted.Pos.Zero() {
		t.Fatalf("Insert = %+v, %v, want id u1 at a position", inserted, err)
	}
	doc, err := node.Get(ctx, u30iDB, "User", "u1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := u30iString(t, doc, "Name"); got != "Ada" {
		t.Fatalf("Name = %q, want Ada", got)
	}
	if got := bson.Raw(doc.Body).Lookup("Age").Int32(); got != 36 {
		t.Fatalf("Age = %d, want 36", got)
	}
	_, err = node.Get(ctx, u30iDB, "User", "nobody")
	u30iCode(t, err, cluster.CodeNotFound)
}

func TestNodeMintsAnIDWhenTheDocumentHasNone(t *testing.T) {
	node, _ := u30iFresh(t)
	inserted, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "Name", Value: "Grace"}})
	if err != nil || len(inserted.ID) != 36 {
		t.Fatalf("Insert = %+v, %v, want a minted 36-character id", inserted, err)
	}
	if _, err := node.Get(context.Background(), u30iDB, "User", inserted.ID); err != nil {
		t.Fatalf("Get of the minted id: %v", err)
	}
}

func TestNodeRefusesADuplicateInsertAndAnUndeclaredField(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	_, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Other", 1))
	u30iCode(t, err, cluster.CodeDocumentExists)
	doc, err := node.Get(ctx, u30iDB, "User", "u1")
	if err != nil || u30iString(t, doc, "Name") != "Ada" {
		t.Fatalf("a refused insert changed the document: %v %v", doc, err)
	}
	_, err = node.Insert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: "u2"}, {Key: "Shoe", Value: int32(44)}})
	u30iCode(t, err, cluster.CodeFieldType)
	_, err = node.Get(ctx, u30iDB, "User", "u2")
	u30iCode(t, err, cluster.CodeNotFound)
}

func TestNodeReportsAnUnknownDatabaseAndBucketWithTheFix(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	_, err := node.Insert(ctx, "nope", "User", u30iUser("u1", "Ada", 1))
	u30iCode(t, err, cluster.CodeNotFound)
	if !strings.Contains(err.Error(), "create database nope") {
		t.Fatalf("the error does not name the fix: %v", err)
	}
	_, err = node.Insert(ctx, u30iDB, "Ghost", u30iUser("u1", "Ada", 1))
	if !errors.Is(err, ErrBucketNotFound) || !strings.Contains(err.Error(), "create bucket Ghost") {
		t.Fatalf("err = %v, want ErrBucketNotFound naming 'create bucket Ghost'", err)
	}
}

// TestNodeWriteToAnAbsentBucketIdIsNotFound: the owner path told an id with no reverse
// index answers NOT_FOUND, where it used to say SCHEMA_NOT_CURRENT (spec-patch 76).
func TestNodeWriteToAnAbsentBucketIdIsNotFound(t *testing.T) {
	node, _ := u30iFresh(t)
	ref, err := node.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	op := ref.template()
	op.Class, op.BucketID = cluster.OpDelete, 0x7777
	_, err = node.send(context.Background(), ref, []byte{0x01}, op, writeOptions{requestID: [16]byte{9}})
	u30iCode(t, err, cluster.CodeNotFound)
}

func TestNodeUpsertKeepsTheFieldsItDoesNotCarry(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.Upsert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatalf("upsert of an absent document: %v", err)
	}
	if _, err := node.Upsert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: "u1"}, {Key: "Name", Value: "Ada L"}}); err != nil {
		t.Fatalf("upsert of a live document: %v", err)
	}
	doc, err := node.Get(ctx, u30iDB, "User", "u1")
	if err != nil {
		t.Fatal(err)
	}
	if u30iString(t, doc, "Name") != "Ada L" || u30iString(t, doc, "City") != "Oslo" || bson.Raw(doc.Body).Lookup("Age").Int32() != 36 {
		t.Fatalf("after the upsert the document is %s, want Name replaced and Age and City kept", bson.Raw(doc.Body))
	}
	_, err = node.Upsert(ctx, u30iDB, "User", bson.D{{Key: "Name", Value: "no id"}})
	u30iCode(t, err, cluster.CodeInvalidArgument)
}

func TestNodeDeleteThenReinsert(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	if err := node.Delete(ctx, u30iDB, "User", "u1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := node.Get(ctx, u30iDB, "User", "u1")
	u30iCode(t, err, cluster.CodeNotFound)
	if err := node.Delete(ctx, u30iDB, "User", "never-existed"); err != nil {
		t.Fatalf("deleting an absent document is not an error: %v", err)
	}
	if _, err := node.Insert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: "u1"}, {Key: "Name", Value: "Back"}}); err != nil {
		t.Fatalf("insert over the tombstone: %v", err)
	}
	doc, err := node.Get(ctx, u30iDB, "User", "u1")
	if err != nil || u30iString(t, doc, "Name") != "Back" {
		t.Fatalf("Get after reinsert = %v, %v", doc, err)
	}
	if _, err := bson.Raw(doc.Body).LookupErr("Age"); err == nil {
		t.Fatalf("the deleted document's Age came back with the new one: %s", bson.Raw(doc.Body))
	}
}

func TestNodeDedupeReplayReturnsTheOriginalResult(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	request := [16]byte{0xAA, 1, 2, 3}
	first, err := node.Insert(ctx, u30iDB, "User", bson.D{{Key: "Name", Value: "Ada"}}, WithRequestID(request))
	if err != nil || first.Deduped {
		t.Fatalf("first insert = %+v, %v", first, err)
	}
	second, err := node.Insert(ctx, u30iDB, "User", bson.D{{Key: "Name", Value: "Ada"}}, WithRequestID(request))
	if err != nil {
		t.Fatalf("the retry of an insert that was answered: %v", err)
	}
	if !second.Deduped || second.ID != first.ID || second.Pos != first.Pos {
		t.Fatalf("retry = %+v, want the first answer %+v marked deduped", second, first)
	}
	listed, err := node.List(ctx, u30iDB, "User", nil, 0)
	if err != nil || len(listed) != 1 {
		t.Fatalf("List = %d documents, %v, want exactly 1 after a retry", len(listed), err)
	}
}

func TestNodeClaimAndAck(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	for _, item := range []string{"book", "pen", "lamp"} {
		if err := node.Push(ctx, PushRequest{StoreTarget: u30iTarget("u1", "Purchases"), Element: bson.D{{Key: "item", Value: item}}}); err != nil {
			t.Fatalf("Push %s: %v", item, err)
		}
	}
	marker := func() string {
		doc, err := node.Get(ctx, u30iDB, "User", "u1")
		if err != nil {
			t.Fatal(err)
		}
		return u30iString(t, doc, "Purchases")
	}
	if got := marker(); got != "fifoStore (3 elements)" {
		t.Fatalf("Purchases = %q after three pushes", got)
	}
	request := [16]byte{0xC1, 1}
	lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: u30iTarget("u1", "Purchases"), Count: 2}, WithRequestID(request))
	if err != nil || lease.ID == ([16]byte{}) || lease.Until == 0 {
		t.Fatalf("Claim = %+v, %v, want a lease", lease, err)
	}
	if got := u30iItems(t, lease.Elements); len(got) != 2 || got[0] != "book" || got[1] != "pen" {
		t.Fatalf("Claim returned %v, want the two oldest purchases [book pen]", got)
	}
	again, err := node.Claim(ctx, ClaimRequest{StoreTarget: u30iTarget("u1", "Purchases"), Count: 2}, WithRequestID(request))
	if err != nil || again.ID != lease.ID || again.Until != lease.Until {
		t.Fatalf("the retried claim = %+v, %v, want the original lease %+v", again, err, lease)
	}
	if err := node.Ack(ctx, AckRequest{StoreTarget: u30iTarget("u1", "Purchases"), Lease: lease.ID}); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := marker(); got != "fifoStore (1 element)" {
		t.Fatalf("Purchases = %q after acking two of three", got)
	}
	_, err = node.Claim(ctx, ClaimRequest{StoreTarget: u30iTarget("ghost", "Purchases"), Count: 1})
	u30iCode(t, err, cluster.CodeNotFound)
	_, err = node.Claim(ctx, ClaimRequest{StoreTarget: u30iTarget("u1", "Name"), Count: 1})
	u30iCode(t, err, cluster.CodeFieldType)
}

func TestNodeSurvivesAReopen(t *testing.T) {
	dir := t.TempDir()
	node, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	u30iSchema(t, node)
	ctx := context.Background()
	var last cluster.Position
	for i := 0; i < 20; i++ {
		inserted, err := node.Insert(ctx, u30iDB, "User", u30iUser(fmt.Sprintf("u%d", i), "Ada", int32(i)))
		if err != nil {
			t.Fatal(err)
		}
		last = inserted.Pos
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := u30iOpen(t, dir)
	for i := 0; i < 20; i++ {
		doc, err := reopened.Get(ctx, u30iDB, "User", fmt.Sprintf("u%d", i))
		if err != nil || bson.Raw(doc.Body).Lookup("Age").Int32() != int32(i) {
			t.Fatalf("after reopen u%d = %v, %v", i, doc, err)
		}
	}
	slot := reopened.slotOf(u30iDB, "User", "u0")
	if pair := reopened.pairs.Snapshot(slot, 1); pair.AppliedPos.Zero() || pair.Ordinal == 0 {
		t.Fatalf("after reopen the pair of u0 is %+v, want its applied position and ordinal restored (TestNodeReloadsPairRecordsWithoutTheLog pins the record)", pair)
	}
	// A restarted pair must not hand out a position the log already holds.
	next, err := reopened.Insert(ctx, u30iDB, "User", u30iUser("after", "Ada", 1))
	if err != nil {
		t.Fatalf("insert after reopen: %v", err)
	}
	if next.Pos.Zero() || next.Pos.Epoch != last.Epoch {
		t.Fatalf("position %s after reopen, want one in epoch %d", next.Pos, last.Epoch)
	}
	listed, err := reopened.List(ctx, u30iDB, "User", nil, 0)
	if err != nil || len(listed) != 21 {
		t.Fatalf("List after reopen = %d, %v, want 21", len(listed), err)
	}
}

// TestNodeRebuildsFromTheLogWhenBadgerIsLost: the log is the record of every
// acknowledged write, so a node whose Badger lost its batches is whole again after the
// tail scan, pair records and dedupe entries included.
func TestNodeRebuildsFromTheLogWhenBadgerIsLost(t *testing.T) {
	dir := t.TempDir()
	node, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	u30iSchema(t, node)
	ctx := context.Background()
	request := [16]byte{0xEE, 7}
	first, err := node.Insert(ctx, u30iDB, "User", bson.D{{Key: "Name", Value: "kept"}}, WithRequestID(request))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(fmt.Sprintf("u%d", i), "Ada", int32(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	loseStoreForTheReplay(t, dir)

	reopened := u30iOpen(t, dir)
	for i := 0; i < 10; i++ {
		if _, err := reopened.Get(ctx, u30iDB, "User", fmt.Sprintf("u%d", i)); err != nil {
			t.Fatalf("u%d was not rebuilt from the log: %v", i, err)
		}
	}
	// A second scan of the same log must change nothing: every record is already applied.
	slot := reopened.slotOf(u30iDB, "User", "u3")
	once := reopened.pairs.Snapshot(slot, 1)
	if once.Ordinal == 0 || once.AppliedPos.Zero() {
		t.Fatalf("the pair of u3 is %+v after the rebuild, want applied work", once)
	}
	if err := reopened.tail.Scan(ctx, 1); err != nil {
		t.Fatalf("scanning the log a second time: %v", err)
	}
	if twice := reopened.pairs.Snapshot(slot, 1); twice != once {
		t.Fatalf("pair after a second scan = %+v, want %+v unchanged", twice, once)
	}
	if listed, err := reopened.List(ctx, u30iDB, "User", nil, 0); err != nil || len(listed) != 11 {
		t.Fatalf("List after two scans = %d, %v, want 11", len(listed), err)
	}
	retry, err := reopened.Insert(ctx, u30iDB, "User", bson.D{{Key: "Name", Value: "kept"}}, WithRequestID(request))
	if err != nil || !retry.Deduped || retry.ID != first.ID {
		t.Fatalf("a retry after the rebuild = %+v, %v, want the first answer deduped", retry, err)
	}
}

func TestNodeConcurrentWritersKeepEveryDocument(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	const writers, perWriter = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(fmt.Sprintf("w%d-%d", w, i), "Ada", int32(i))); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("a concurrent insert failed: %v", err)
	}
	listed, err := node.List(ctx, u30iDB, "User", nil, 0)
	if err != nil || len(listed) != writers*perWriter {
		t.Fatalf("List = %d documents, %v, want %d", len(listed), err, writers*perWriter)
	}
}

func TestNodeListFiltersAndLimits(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		city := "Oslo"
		if i%3 == 0 {
			city = "Bergen"
		}
		doc := bson.D{{Key: "_id_", Value: fmt.Sprintf("u%d", i)}, {Key: "City", Value: city}}
		if _, err := node.Insert(ctx, u30iDB, "User", doc); err != nil {
			t.Fatal(err)
		}
	}
	bergen, err := node.List(ctx, u30iDB, "User", bson.D{{Key: "City", Value: "Bergen"}}, 0)
	if err != nil || len(bergen) != 10 {
		t.Fatalf("List(City=Bergen) = %d, %v, want 10", len(bergen), err)
	}
	limited, err := node.List(ctx, u30iDB, "User", nil, 7)
	if err != nil || len(limited) != 7 {
		t.Fatalf("List(limit 7) = %d, %v, want 7", len(limited), err)
	}
	_, err = node.List(ctx, u30iDB, "User", bson.D{{Key: "Shoe", Value: int32(1)}}, 0)
	u30iCode(t, err, cluster.CodeFieldType)
}

// slotOf is the slot of a document by names, for tests that look at its pair.
func (n *Node) slotOf(db, bucket, id string) uint16 {
	encID, err := store.EncodeComponent(id)
	if err != nil {
		panic(err)
	}
	return storage.SlotOf(storage.PlacementKey(db, bucket, encID))
}

// u30iTarget names a store field of a User document in the test database.
func u30iTarget(id, field string) StoreTarget {
	return StoreTarget{Database: u30iDB, Bucket: "User", ID: id, Field: field}
}

// u30iItems is the item text of each claimed element, in order.
func u30iItems(t *testing.T, elements [][]byte) []string {
	t.Helper()
	items := make([]string, len(elements))
	for i, element := range elements {
		items[i] = bson.Raw(element).Lookup("item").StringValue()
	}
	return items
}
