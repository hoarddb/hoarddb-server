package wire

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// expandWorld is a node with users u0..u6 and a server whose far reads and owners are counted.
type expandWorld struct {
	server *ringServer
	near   bucketRef
	mu     sync.Mutex
	reads  map[string]int
	owners map[string]int
}

func newExpandWorld(t *testing.T, authz RingAuthorizer) *expandWorld {
	t.Helper()
	node, _ := u30iFresh(t)
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(fmt.Sprintf("u%d", i), fmt.Sprintf("Name%d", i), int32(20+i))); err != nil {
			t.Fatal(err)
		}
	}
	near, err := node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	world := &expandWorld{near: near, reads: map[string]int{}, owners: map[string]int{}}
	world.server = &ringServer{node: node, authz: authz}
	world.server.expandOwner = func(_ bucketRef, slot uint16) string { return fmt.Sprintf("node-%d", slot%3) }
	world.server.expandFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		world.mu.Lock()
		world.reads[string(encID)]++
		world.owners[world.server.expandOwner(ref, ref.slot(encID))]++
		world.mu.Unlock()
		return node.readExpanded(ctx, ref, encID, project, local)
	}
	return world
}

func expandIDs(rows int, spread int, field string) [][]string {
	ids := make([][]string, rows)
	for i := range ids {
		ids[i] = []string{fmt.Sprintf("u%d", i%spread)}
	}
	return ids
}

func expandSpecFor() []RingExpandSpec {
	return []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: []string{"Name", "Age"}}}
}

// TestExpandGroupsIdsByOwnerAndSendsOneReadPerDistinctId: [GUARD 100 ids from 7 authors are 7 reads].
func TestExpandGroupsIdsByOwnerAndSendsOneReadPerDistinctId(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	answers, kept, filtered, _, err := world.server.expandRows(context.Background(), world.near, expandSpecFor(), expandIDs(100, 7, "Author"), false)
	if err != nil || filtered != 0 || len(answers) != 100 || len(kept) != 100 {
		t.Fatalf("expandRows = %d answers, %d kept, %d filtered, %v", len(answers), len(kept), filtered, err)
	}
	total := 0
	for id, count := range world.reads {
		if count != 1 {
			t.Errorf("id %x was read %d times", id, count)
		}
		total += count
	}
	if total != 7 {
		t.Fatalf("%d reads for 7 distinct ids", total)
	}
	if len(world.owners) == 0 || len(world.owners) > 3 {
		t.Fatalf("reads went to %d owners: %v", len(world.owners), world.owners)
	}
	// Row 3 answers with u3's profile, not another row's.
	doc := bson.Raw(answers[3]).Lookup("Author").Document()
	if name := doc.Lookup("doc", "Name").StringValue(); name != "Name3" {
		t.Fatalf("row 3 expanded to %q, want Name3", name)
	}
	if got := world.server.node.ExpandStats().Reads.Load(); got != 7 {
		t.Fatalf("ExpandStats.Reads = %d, want 7", got)
	}
}

// TestExpandMarksMissingFarDocuments: [GUARD one deleted user does not fail the page; spec 3.2
// step 1: a null or absent reference expands to nothing, a dangling non-empty id is marked missing].
func TestExpandMarksMissingFarDocuments(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	ids := [][]string{{"u1"}, {"ghost"}, {""}}
	answers, _, _, _, err := world.server.expandRows(context.Background(), world.near, expandSpecFor(), ids, false)
	if err != nil || len(answers) != 3 {
		t.Fatalf("expandRows = %d answers, %v", len(answers), err)
	}
	if got := bson.Raw(answers[0]).Lookup("Author").Document().Lookup("expanded").Boolean(); !got {
		t.Error("row 0 (u1) was not expanded")
	}
	dangling := bson.Raw(answers[1]).Lookup("Author").Document()
	if dangling.Lookup("expanded").Boolean() {
		t.Error("row 1 (ghost) was expanded")
	}
	if reason := dangling.Lookup("reason").StringValue(); reason != "missing" {
		t.Errorf("reason = %q, want missing", reason)
	}
	if _, err := bson.Raw(answers[2]).LookupErr("Author"); err == nil {
		t.Errorf("row 2 (null reference) carries an Author expansion %s, want nothing", bson.Raw(answers[2]))
	}
	if got := world.server.node.ExpandStats().Missing.Load(); got != 1 {
		t.Errorf("Missing = %d, want 1: a dangling id counts, an empty reference is not dangling", got)
	}
}

// TestExpandFailsTheWholePageWhenAnOwnerIsDown: [GUARD no page with silently unexpanded rows].
func TestExpandFailsTheWholePageWhenAnOwnerIsDown(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	down := errors.New("node-1 is unreachable")
	inner := world.server.expandFetch
	world.server.expandFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		if world.server.expandOwner(ref, ref.slot(encID)) == "node-1" {
			return Document{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: down.Error()}
		}
		return inner(ctx, ref, encID, project, local)
	}
	answers, _, _, _, err := world.server.expandRows(context.Background(), world.near, expandSpecFor(), expandIDs(40, 7, "Author"), false)
	if err == nil || answers != nil {
		t.Fatalf("a page with a down owner returned %d answers and %v", len(answers), err)
	}
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeUnavailable || !strings.Contains(err.Error(), "node-1") {
		t.Fatalf("error = %v, want UNAVAILABLE naming node-1", err)
	}
}

type denyBucket struct{ bucket string }

func (d denyBucket) Authorize(_ context.Context, access RingAccess) error {
	if access.Bucket == d.bucket {
		return errors.New("no grant")
	}
	return nil
}

// TestExpandChecksTheSessionMayReadTheFarBucket: [GUARD expand never widens a grant].
func TestExpandChecksTheSessionMayReadTheFarBucket(t *testing.T) {
	world := newExpandWorld(t, denyBucket{bucket: "User"})
	_, _, _, _, err := world.server.expandRows(context.Background(), world.near, expandSpecFor(), expandIDs(5, 5, "Author"), false)
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodePermissionDenied {
		t.Fatalf("error = %v, want PERMISSION_DENIED", err)
	}
	if len(world.reads) != 0 {
		t.Fatalf("%d reads happened before authorization", len(world.reads))
	}
}

// TestExpandDepthTwoIsRefused: [GUARD a dotted field is a second level and is refused unread].
func TestExpandDepthTwoIsRefused(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	specs := []RingExpandSpec{{Field: "Author.Manager", Bucket: "User"}}
	_, _, _, _, err := world.server.expandRows(context.Background(), world.near, specs, [][]string{{"u1"}}, false)
	var coded *cluster.Error
	if !errors.As(err, &coded) || !strings.Contains(err.Error(), "one level") {
		t.Fatalf("error = %v, want the depth refusal", err)
	}
	if len(world.reads) != 0 {
		t.Fatal("a refused expand read documents")
	}
}

// TestExpandUndeclaredFieldNamesNoBucket: [GUARD a spec with no bucket and no declaration is refused].
func TestExpandUndeclaredFieldNamesNoBucket(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	_, _, _, _, err := world.server.expandRows(context.Background(), world.near, []RingExpandSpec{{Field: "Name"}}, [][]string{{"u1"}}, false)
	if err == nil || !strings.Contains(err.Error(), "not declared of any bucket") {
		t.Fatalf("error = %v", err)
	}
}

// TestExpandWhereDropsRowsAndCountsThem: [GUARD where removes unmatched rows and says how many].
func TestExpandWhereDropsRowsAndCountsThem(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	where, err := bson.Marshal(bson.D{{Key: "Age", Value: int32(23)}})
	if err != nil {
		t.Fatal(err)
	}
	specs := []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: []string{"Age"}, Where: where}}
	ids := [][]string{{"u1"}, {"u3"}, {"ghost"}, {"u3"}}
	answers, kept, filtered, _, err := world.server.expandRows(context.Background(), world.near, specs, ids, false)
	if err != nil || filtered != 2 || len(answers) != 2 || kept[0] != 1 || kept[1] != 3 {
		t.Fatalf("answers=%d kept=%v filtered=%d err=%v", len(answers), kept, filtered, err)
	}
}

// TestExpandGoroutinesEndWithTheCall: [GUARD no read goroutine outlives expandRows].
func TestExpandGoroutinesEndWithTheCall(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		if _, _, _, _, err := world.server.expandRows(context.Background(), world.near, expandSpecFor(), expandIDs(50, 7, "Author"), false); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d before, %d after", before, after)
	}
}

// TestGroupByOwnerKeepsFirstAppearanceOrder: [GUARD groups are distinct and ordered].
func TestGroupByOwnerKeepsFirstAppearanceOrder(t *testing.T) {
	var far bucketRef
	refs := []expandRef{{ref: far, encID: []byte("a")}, {ref: far, encID: []byte("b")}, {ref: far, encID: []byte("a")}, {ref: far, encID: []byte("c")}}
	groups := groupByOwner(refs, func(_ bucketRef, _ uint16) string { return "x" })
	if len(groups) != 1 || len(groups[0].refs) != 3 {
		t.Fatalf("groups = %+v", groups)
	}
}

// TestServedIsOwnerWhenAnyFarReadWentToTheOwner: [GUARD one value per call, local only when all were].
func TestServedIsOwnerWhenAnyFarReadWentToTheOwner(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	inner := world.server.expandFetch
	served := func(owner map[string]bool) expandFetcher {
		return func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
			doc, err := inner(ctx, ref, encID, project, local)
			doc.Served = servedLocal
			if owner[string(encID)] {
				doc.Served = servedOwner
			}
			return doc, err
		}
	}
	u0, _ := store.EncodeComponent("u0")
	world.server.expandFetch = served(map[string]bool{})
	_, _, _, got, err := world.server.expandRows(context.Background(), world.near, expandSpecFor(), [][]string{{"u0"}, {"u1"}}, true)
	if err != nil || got != servedLocal {
		t.Fatalf("all local: served %q, %v", got, err)
	}
	world.server.expandFetch = served(map[string]bool{string(u0): true})
	_, _, _, got, err = world.server.expandRows(context.Background(), world.near, expandSpecFor(), [][]string{{"u0"}, {"u1"}}, true)
	if err != nil || got != servedOwner {
		t.Fatalf("one forwarded: served %q, %v", got, err)
	}
}

// TestExpandTwoSpecsOnTheSameFarDocumentBothAnswer: [GUARD Author and Editor, both of User and
// both u1, each get the document; a read is shared only when the projection is the same].
func TestExpandTwoSpecsOnTheSameFarDocumentBothAnswer(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	same := []RingExpandSpec{
		{Field: "Author", Bucket: "User", Fields: []string{"Name"}},
		{Field: "Editor", Bucket: "User", Fields: []string{"Name"}},
	}
	answers, _, _, _, err := world.server.expandRows(context.Background(), world.near, same, [][]string{{"u1", "u1"}}, false)
	if err != nil || len(answers) != 1 {
		t.Fatalf("expandRows = %d answers, %v", len(answers), err)
	}
	for _, label := range []string{"Author", "Editor"} {
		if !bson.Raw(answers[0]).Lookup(label, "expanded").Boolean() {
			t.Errorf("%s of the same far document came back unexpanded: %s", label, bson.Raw(answers[0]))
		}
	}
	if len(world.reads) != 1 || world.server.node.ExpandStats().Reads.Load() != 1 {
		t.Fatalf("two specs with one projection read %d documents, want 1", world.server.node.ExpandStats().Reads.Load())
	}

	different := []RingExpandSpec{
		{Field: "Author", Bucket: "User", Fields: []string{"Name"}},
		{Field: "Editor", Bucket: "User", Fields: []string{"Name", "Age"}},
	}
	answers, _, _, _, err = world.server.expandRows(context.Background(), world.near, different, [][]string{{"u1", "u1"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	row := bson.Raw(answers[0])
	if !row.Lookup("Author", "expanded").Boolean() || !row.Lookup("Editor", "expanded").Boolean() {
		t.Fatalf("a spec came back unexpanded: %s", row)
	}
	if _, err := row.Lookup("Author", "doc").Document().LookupErr("Age"); err == nil {
		t.Errorf("Author asked for Name only and got Age: %s", row)
	}
	if _, err := row.Lookup("Editor", "doc").Document().LookupErr("Age"); err != nil {
		t.Errorf("Editor asked for Age and did not get it: %s", row)
	}
}

// ownerSplitWorld is a server whose far reads take readTime each and whose owner is node-0 or
// node-1 by the slot's parity, over perOwner distinct ids for each owner. Nothing is read from
// Badger: every id answers with an empty body.
func ownerSplitWorld(t *testing.T, perOwner int, readTime time.Duration, fail func(owner string) error) (*expandWorld, [][]string) {
	t.Helper()
	world := newExpandWorld(t, u70AllowAll{})
	world.server.expandOwner = func(_ bucketRef, slot uint16) string { return fmt.Sprintf("node-%d", slot%2) }
	world.server.expandFetch = func(ctx context.Context, ref bucketRef, encID []byte, _ []uint16, _ bool) (Document, error) {
		owner := world.server.expandOwner(ref, ref.slot(encID))
		if err := fail(owner); err != nil {
			return Document{}, err
		}
		select {
		case <-time.After(readTime):
			return Document{Body: []byte{5, 0, 0, 0, 0}, Served: servedOwner}, nil
		case <-ctx.Done():
			world.mu.Lock()
			world.reads["cancelled "+owner]++
			world.mu.Unlock()
			return Document{}, ctx.Err()
		}
	}
	var ids [][]string
	counts := map[string]int{}
	for n := 0; counts["node-0"] < perOwner || counts["node-1"] < perOwner; n++ {
		id := fmt.Sprintf("x%d", n)
		encID, err := store.EncodeComponent(id)
		if err != nil {
			t.Fatal(err)
		}
		owner := world.server.expandOwner(world.near, world.near.slot(encID))
		if counts[owner] < perOwner {
			counts[owner]++
			ids = append(ids, []string{id})
		}
	}
	return world, ids
}

// TestExpandReadsOwnersInParallel: [GUARD two owners cost about one owner's time, not the sum].
func TestExpandReadsOwnersInParallel(t *testing.T) {
	const readTime = 20 * time.Millisecond
	noFail := func(string) error { return nil }
	world, ids := ownerSplitWorld(t, 100, readTime, noFail)
	specs := []RingExpandSpec{{Field: "Author", Bucket: "User"}}
	onlyFirst := ids[:0:0]
	for _, row := range ids {
		encID, _ := store.EncodeComponent(row[0])
		if world.server.expandOwner(world.near, world.near.slot(encID)) == "node-0" {
			onlyFirst = append(onlyFirst, row)
		}
	}
	start := time.Now()
	if _, _, _, _, err := world.server.expandRows(context.Background(), world.near, specs, onlyFirst, false); err != nil {
		t.Fatal(err)
	}
	oneOwner := time.Since(start)
	start = time.Now()
	if _, _, _, _, err := world.server.expandRows(context.Background(), world.near, specs, ids, false); err != nil {
		t.Fatal(err)
	}
	both := time.Since(start)
	if both > oneOwner*3/2 {
		t.Fatalf("two owners took %v and one owner took %v: the owners were read one after the other", both, oneOwner)
	}
}

// TestExpandAnOwnerErrorCancelsTheOtherOwners: [GUARD the first failing owner stops the rest].
func TestExpandAnOwnerErrorCancelsTheOtherOwners(t *testing.T) {
	down := &cluster.Error{Code: cluster.CodeUnavailable, Message: "node-1 is unreachable"}
	world, ids := ownerSplitWorld(t, 50, 5*time.Second, func(owner string) error {
		if owner == "node-1" {
			// Late enough that node-0's reads are in flight and blocked when it fails.
			time.Sleep(150 * time.Millisecond)
			return down
		}
		return nil
	})
	start := time.Now()
	_, _, _, _, err := world.server.expandRows(context.Background(), world.near, []RingExpandSpec{{Field: "Author", Bucket: "User"}}, ids, false)
	if err == nil || !strings.Contains(err.Error(), "node-1") {
		t.Fatalf("error = %v, want the node-1 failure", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the call took %v after an owner failed at once: the other owner's reads were not cancelled", took)
	}
	world.mu.Lock()
	defer world.mu.Unlock()
	if world.reads["cancelled node-0"] == 0 {
		t.Fatal("no read of node-0 saw its context cancelled")
	}
}
