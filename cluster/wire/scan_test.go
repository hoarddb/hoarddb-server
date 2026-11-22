package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// objectIDType is the BSON type byte of an ObjectID.
const objectIDType bsontype.Type = 0x07

const (
	u48DB       uint64 = 3
	u48BucketA  uint64 = 41
	u48BucketB  uint64 = 42
	u48Epoch    uint64 = 9
	u48PostsFld uint16 = 4
)

type u48Topo struct {
	self  string
	table *cluster.SlotTable
	rf    int
}

func (t *u48Topo) Committed() uint64                      { return u48Epoch }
func (t *u48Topo) Self() string                           { return t.self }
func (t *u48Topo) Members() int                           { return 3 }
func (t *u48Topo) Table() *cluster.SlotTable              { return t.table }
func (t *u48Topo) Target(uint16, uint64) cluster.Position { return cluster.Position{} }
func (t *u48Topo) Database(db uint64) (DatabaseInfo, bool) {
	return DatabaseInfo{RF: t.rf}, db == u48DB
}

// u48Gate is the read gate of one scan node: every pair it could own is replayed and
// stop-proved, the lease is held and every slot is ready, so only ownership decides.
func u48Gate(topo Topology) *ReadGate {
	pairs := &cluster.PairSet{}
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		pairs.With(slot, u48DB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved })
	}
	lease := &fakeLease{}
	lease.valid.Store(true)
	return NewReadGate(ReadGateConfig{
		Topology: topo, Pairs: pairs, Clock: fixedClock{}, Lease: lease, Schema: u48Schema{},
		Slots: &u36States{state: map[uint16]machine.SlotState{}},
	})
}

type u48Schema struct{}

func (u48Schema) View(_, bucketID uint64) SchemaView {
	return SchemaView{Present: true, Decl: machine.Declaration{BucketID: bucketID, State: machine.StateActive,
		Fields: machine.FieldTable{NextFieldID: 5, Entries: []machine.FieldEntry{
			{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"},
			{ID: 3, Type: "int", Name: "Age"}, {ID: u48PostsFld, Type: "fifoStore", Name: "Posts"},
		}}}}
}
func (u48Schema) Refetch(context.Context, uint64, uint64) error { return nil }

// u48Router sends a sub-scan to the scanner of the owner it names.
type u48Router struct {
	mu       sync.Mutex
	scanners map[string]*SlotScanner
	down     map[string]bool
	calls    map[string]int
	refuse   map[string]*cluster.Error // first call to the node fails with this
}

func (r *u48Router) enter(owner cluster.Member) (*SlotScanner, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[owner.ID]++
	if r.down[owner.ID] {
		return nil, fmt.Errorf("dial %s: connection refused", owner.ID)
	}
	if refusal := r.refuse[owner.ID]; refusal != nil {
		delete(r.refuse, owner.ID)
		return nil, refusal
	}
	return r.scanners[owner.ID], nil
}

func (r *u48Router) ScanList(ctx context.Context, owner cluster.Member, req cluster.SlotListRequest) (cluster.SlotListReply, error) {
	scanner, err := r.enter(owner)
	if err != nil {
		return cluster.SlotListReply{}, err
	}
	return scanner.List(ctx, req)
}

func (r *u48Router) ScanCount(ctx context.Context, owner cluster.Member, req cluster.SlotCountRequest) (int64, error) {
	scanner, err := r.enter(owner)
	if err != nil {
		return 0, err
	}
	return scanner.Count(ctx, req)
}

type u48ScanTopo struct {
	table     *cluster.SlotTable
	refreshes int
}

func (t *u48ScanTopo) Epoch() uint64                 { return u48Epoch }
func (t *u48ScanTopo) Table() *cluster.SlotTable     { return t.table }
func (t *u48ScanTopo) Refresh(context.Context) error { t.refreshes++; return nil }

type u48Resolver struct{}

func (u48Resolver) Resolve(_ context.Context, database, bucket string) (cluster.BucketTarget, error) {
	switch bucket {
	case "Users":
		return cluster.BucketTarget{DatabaseID: u48DB, BucketID: u48BucketA, RF: 2}, nil
	case "Others":
		return cluster.BucketTarget{DatabaseID: u48DB, BucketID: u48BucketB, RF: 2}, nil
	}
	return cluster.BucketTarget{}, &cluster.Error{Code: cluster.CodeNotFound, Message: "no such bucket " + database + "." + bucket}
}

// u48CountingKV counts iterator seeks.
type u48CountingKV struct {
	storage.KV
	seeks *atomic.Int64
}

func (k u48CountingKV) NewSnapshot() storage.KVSnapshot {
	return u48CountingSnap{KVSnapshot: k.KV.NewSnapshot(), seeks: k.seeks}
}

type u48CountingSnap struct {
	storage.KVSnapshot
	seeks *atomic.Int64
}

func (s u48CountingSnap) NewIterator(opts storage.IterOptions) storage.KVIterator {
	return u48CountingIter{KVIterator: s.KVSnapshot.NewIterator(opts), seeks: s.seeks}
}

type u48CountingIter struct {
	storage.KVIterator
	seeks *atomic.Int64
}

func (i u48CountingIter) Seek(key []byte) { i.seeks.Add(1); i.KVIterator.Seek(key) }

type u48World struct {
	t      *testing.T
	kv     storage.KV
	seeks  atomic.Int64
	router *u48Router
	topo   *u48ScanTopo
	coord  *cluster.Coordinator
}

func newU48World(t *testing.T, limits cluster.ScanLimits) *u48World {
	t.Helper()
	badger, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing Badger: %v", err)
		}
	})
	world := &u48World{t: t, kv: badger}
	table := cluster.BuildTable(cluster.ClusterState{Epoch: int64(u48Epoch), Members: testMembers("n1", "n2", "n3")})
	router := &u48Router{scanners: map[string]*SlotScanner{}, down: map[string]bool{}, calls: map[string]int{}, refuse: map[string]*cluster.Error{}}
	for _, id := range []string{"n1", "n2", "n3"} {
		topo := &u48Topo{self: id, table: table, rf: 2}
		router.scanners[id] = NewSlotScanner(SlotScannerConfig{
			KV: u48CountingKV{KV: badger, seeks: &world.seeks}, Topology: topo, Schema: u48Schema{}, Gate: u48Gate(topo),
		})
	}
	world.router = router
	world.topo = &u48ScanTopo{table: table}
	world.coord = cluster.NewCoordinator(cluster.CoordinatorConfig{Topology: world.topo, Scanner: router, Buckets: u48Resolver{}, Limits: limits})
	return world
}

var u48DefaultLimits = cluster.ScanLimits{DefaultLimit: 100, MaxLimit: 10000, MaxResultBytes: 16 << 20}

func u48Slot(bucket string, encID []byte) uint16 {
	return storage.SlotOf(storage.PlacementKey("app", bucket, encID))
}

// insert writes one document of bucket id into the shared store.
func (w *u48World) insert(bucketName string, bucketID uint64, id any, name string, posts uint64) []byte {
	w.t.Helper()
	encID, err := store.EncodeComponent(id)
	if err != nil {
		w.t.Fatalf("EncodeComponent(%v): %v", id, err)
	}
	slot := u48Slot(bucketName, encID)
	var idType bsontype.Type
	var idBytes []byte
	if oid, ok := id.(primitive.ObjectID); ok {
		idType, idBytes = objectIDType, oid[:]
	} else if idType, idBytes, err = bson.MarshalValue(id); err != nil {
		w.t.Fatalf("MarshalValue: %v", err)
	}
	nameType, nameBytes, err := bson.MarshalValue(name)
	if err != nil {
		w.t.Fatalf("MarshalValue: %v", err)
	}
	batch := w.kv.NewBatch()
	defer batch.Discard()
	ref := storage.BucketRef{Slot: slot, DatabaseID: u48DB, BucketID: bucketID}
	insert := store.DocumentInsert{Ref: ref, EncID: encID, Header: append([]byte{byte(idType)}, idBytes...),
		Fields: []store.DocumentField{{FieldID: 2, Stored: append([]byte{byte(nameType)}, nameBytes...)}}}
	if err := store.WriteInsert(batch, insert); err != nil {
		w.t.Fatalf("WriteInsert: %v", err)
	}
	if posts > 0 {
		stateKey := store.FieldStateKey(store.StorePrefix(slot, u48DB, bucketID, encID, u48PostsFld))
		if err := batch.Set(stateKey, store.EncodeFieldInstanceState(store.FieldInstanceState{Count: posts})); err != nil {
			w.t.Fatalf("Set state: %v", err)
		}
	}
	if err := batch.Commit(false); err != nil {
		w.t.Fatalf("Commit: %v", err)
	}
	return encID
}

func (w *u48World) list(req cluster.ListRequest) (cluster.ListResponse, error) {
	req.Database = "app"
	if req.Bucket == "" {
		req.Bucket = "Users"
	}
	return w.coord.List(context.Background(), req)
}

func u48Order(entries []cluster.ListEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = fmt.Sprintf("%04d:%x", entry.Slot, entry.EncID)
	}
	return out
}

func u48ExpectedOrder(bucket string, ids []any) []string {
	type row struct {
		slot uint16
		enc  []byte
	}
	rows := make([]row, len(ids))
	for i, id := range ids {
		enc, _ := store.EncodeComponent(id)
		rows[i] = row{u48Slot(bucket, enc), enc}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].slot != rows[j].slot {
			return rows[i].slot < rows[j].slot
		}
		return bytes.Compare(rows[i].enc, rows[j].enc) < 0
	})
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%04d:%x", r.slot, r.enc)
	}
	return out
}

// u48SameSlotIDs finds n string ids that share one slot of bucket Users.
func u48SameSlotIDs(n int) []any {
	bySlot := map[uint16][]any{}
	for i := 0; ; i++ {
		id := fmt.Sprintf("k%d", i)
		enc, _ := store.EncodeComponent(id)
		slot := u48Slot("Users", enc)
		bySlot[slot] = append(bySlot[slot], id)
		if len(bySlot[slot]) == n {
			return bySlot[slot]
		}
	}
}

func u48MixedIDs() []any {
	oid := primitive.NewObjectID()
	ids := []any{"ada", "bob", "cy", int32(2), int32(10), int64(-5), 3.5, oid, true, "zed"}
	return append(ids, u48SameSlotIDs(3)...)
}

func TestListIsOrderedBySlotThenEncodedID(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	ids := u48MixedIDs()
	for _, id := range ids {
		world.insert("Users", u48BucketA, id, "n", 0)
	}
	page, err := world.list(cluster.ListRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	got, want := u48Order(page.Entries), u48ExpectedOrder("Users", ids)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("order\n got %v\nwant %v", got, want)
	}
	// The order must not be the order of the id values: the numeric rows would sit together.
	byValue := sort.SliceIsSorted(page.Entries, func(i, j int) bool { return page.Entries[i].EncID[0] < page.Entries[j].EncID[0] })
	if byValue {
		t.Fatalf("the test set is already ordered by id tag, so it cannot tell slot order from value order")
	}
	if page.More || page.Cursor != nil {
		t.Fatalf("a complete list has more=%v cursor=%x", page.More, page.Cursor)
	}
}

func TestListBodyIsTheDocumentAsBSON(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	world.insert("Users", u48BucketA, "ada", "Ada", 1)
	world.insert("Users", u48BucketA, "bob", "Bob", 3)
	page, err := world.list(cluster.ListRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Entries) != 2 {
		t.Fatalf("entries = %d", len(page.Entries))
	}
	for _, entry := range page.Entries {
		var doc bson.D
		if err := bson.Unmarshal(entry.Body, &doc); err != nil {
			t.Fatalf("body is not BSON: %v", err)
		}
		if len(doc) != 3 || doc[0].Key != "_id_" || doc[1].Key != "Name" || doc[2].Key != "Posts" {
			t.Fatalf("body = %v, want _id_, Name, Posts in that order", doc)
		}
		wantMarker := map[string]string{"ada": "fifoStore (1 element)", "bob": "fifoStore (3 elements)"}[doc[0].Value.(string)]
		if doc[2].Value != wantMarker {
			t.Fatalf("marker = %v, want %q", doc[2].Value, wantMarker)
		}
	}
}

func TestListPagesExactlyOnce(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	ids := u48MixedIDs()
	for _, id := range ids {
		world.insert("Users", u48BucketA, id, "n", 0)
	}
	var seen []cluster.ListEntry
	var cursor []byte
	pages := 0
	for {
		page, err := world.list(cluster.ListRequest{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		pages++
		seen = append(seen, page.Entries...)
		if !page.More {
			break
		}
		if page.Cursor == nil || pages > 20 {
			t.Fatalf("page %d: more without a usable cursor", pages)
		}
		cursor = page.Cursor
	}
	got, want := u48Order(seen), u48ExpectedOrder("Users", ids)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("pages concatenate to\n got %v\nwant %v", got, want)
	}
	if pages != 5 {
		t.Fatalf("pages = %d, want 5 for 13 documents at limit 3", pages)
	}
}

func TestCursorIsBoundAndTheCeilingStopsWhole(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	for _, id := range []string{"a", "b", "c"} {
		world.insert("Users", u48BucketA, id, "n", 0)
		world.insert("Others", u48BucketB, id, "n", 0)
	}
	page, err := world.list(cluster.ListRequest{Limit: 1})
	if err != nil || page.Cursor == nil {
		t.Fatalf("first page: %v cursor %x", err, page.Cursor)
	}
	var coded *cluster.Error
	if _, err := world.list(cluster.ListRequest{Bucket: "Others", Cursor: page.Cursor}); !errors.As(err, &coded) ||
		coded.Code != cluster.CodeInvalidArgument || !strings.Contains(coded.Message, "it was issued for bucket id 41") ||
		!strings.Contains(coded.Message, "app.Others") {
		t.Fatalf("a cursor of Users on Others: %v", err)
	}
	flip := bytes.Clone(page.Cursor)
	flip[len(flip)/2] ^= 1
	if _, err := world.list(cluster.ListRequest{Cursor: flip}); !errors.As(err, &coded) || !strings.Contains(coded.Message, "the checksum does not match") {
		t.Fatalf("a flipped byte: %v", err)
	}
	bump := bytes.Clone(page.Cursor)
	bump[0] = 2
	if _, err := world.list(cluster.ListRequest{Cursor: bump}); !errors.As(err, &coded) || !strings.Contains(coded.Message, "its format version is not 1") {
		t.Fatalf("a version bump: %v", err)
	}

	// The ceiling: two 2 MiB documents under 3 MiB page whole, one per page.
	ceiling := newU48World(t, cluster.ScanLimits{DefaultLimit: 100, MaxLimit: 10000, MaxResultBytes: 3 << 20})
	big := strings.Repeat("x", 2<<20)
	ceiling.insert("Users", u48BucketA, "p", big, 0)
	ceiling.insert("Users", u48BucketA, "q", big, 0)
	first, err := ceiling.list(cluster.ListRequest{})
	if err != nil || len(first.Entries) != 1 || !first.More || first.Cursor == nil {
		t.Fatalf("ceiling page 1: entries=%d more=%v err=%v", len(first.Entries), first.More, err)
	}
	var doc bson.D
	if err := bson.Unmarshal(first.Entries[0].Body, &doc); err != nil {
		t.Fatalf("the last document was cut, the body is not BSON: %v", err)
	}
	second, err := ceiling.list(cluster.ListRequest{Cursor: first.Cursor})
	if err != nil || len(second.Entries) != 1 || second.More || bytes.Equal(second.Entries[0].EncID, first.Entries[0].EncID) {
		t.Fatalf("ceiling page 2: entries=%d more=%v err=%v", len(second.Entries), second.More, err)
	}
	// A first document over the ceiling is RESULT_TOO_LARGE with the by-key fix.
	tooSmall := newU48World(t, cluster.ScanLimits{DefaultLimit: 100, MaxLimit: 10000, MaxResultBytes: 1 << 20})
	tooSmall.insert("Users", u48BucketA, "p", big, 0)
	if _, err := tooSmall.list(cluster.ListRequest{}); !errors.As(err, &coded) || coded.Code != cluster.CodeResultTooLarge ||
		!strings.Contains(coded.Message, `db.Users["<id>"].get()`) {
		t.Fatalf("a document over the ceiling: %v", err)
	}
}

func TestListLimitsAreChecked(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	var coded *cluster.Error
	if _, err := world.list(cluster.ListRequest{Limit: 10001}); !errors.As(err, &coded) || !strings.Contains(coded.Message, "above this release's maximum page of 10000") {
		t.Fatalf("limit above max: %v", err)
	}
	if _, err := world.list(cluster.ListRequest{Limit: -1}); !errors.As(err, &coded) || !strings.Contains(coded.Message, "is not a page size") {
		t.Fatalf("limit below one: %v", err)
	}
	for i := 0; i < 150; i++ {
		world.insert("Users", u48BucketA, fmt.Sprintf("id%03d", i), "n", 0)
	}
	page, err := world.list(cluster.ListRequest{})
	if err != nil || len(page.Entries) != 100 || !page.More {
		t.Fatalf("default limit: entries=%d more=%v err=%v", len(page.Entries), page.More, err)
	}
}

func TestDownOwnerFailsTheWholeListAndCount(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	for i := 0; i < 60; i++ {
		world.insert("Users", u48BucketA, fmt.Sprintf("id%03d", i), "n", 0)
	}
	world.router.down["n2"] = true
	page, err := world.list(cluster.ListRequest{})
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeOwnerDown || !strings.Contains(coded.Message, "n2") {
		t.Fatalf("List with n2 down: %v", err)
	}
	if len(page.Entries) != 0 {
		t.Fatalf("a failed list returned %d documents", len(page.Entries))
	}
	count, err := world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users"})
	if !errors.As(err, &coded) || coded.Code != cluster.CodeOwnerDown || !strings.Contains(coded.Message, "n2") || count.Count != 0 {
		t.Fatalf("Count with n2 down: %+v %v", count, err)
	}
}

func TestCountIsOneInteger(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	for i := 0; i < 250; i++ {
		name := "even"
		if i%2 == 1 {
			name = "odd"
		}
		world.insert("Users", u48BucketA, fmt.Sprintf("id%03d", i), name, 0)
	}
	count, err := world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users"})
	if err != nil || count.Count != 250 {
		t.Fatalf("Count = %+v, %v, want 250 with the default limit at 100", count, err)
	}
	filter, _ := bson.Marshal(bson.D{{Key: "Name", Value: "odd"}})
	count, err = world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users", Filter: filter})
	if err != nil || count.Count != 125 {
		t.Fatalf("filtered Count = %+v, %v, want 125", count, err)
	}
	page, err := world.list(cluster.ListRequest{Filter: filter, Limit: 1000})
	if err != nil || len(page.Entries) != 125 {
		t.Fatalf("filtered List: %d, %v", len(page.Entries), err)
	}
	undeclared, _ := bson.Marshal(bson.D{{Key: "Nope", Value: 1}})
	var coded *cluster.Error
	if _, err := world.list(cluster.ListRequest{Filter: undeclared}); !errors.As(err, &coded) || coded.Code != cluster.CodeFieldType {
		t.Fatalf("undeclared filter field: %v", err)
	}
}

func TestACountSkipsTombstonesAndOtherBuckets(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	world.insert("Users", u48BucketA, "a", "n", 0)
	enc := world.insert("Users", u48BucketA, "b", "n", 0)
	world.insert("Others", u48BucketB, "c", "n", 0)
	batch := world.kv.NewBatch()
	if err := batch.Set(store.DocumentPrefix(u48Slot("Users", enc), u48DB, u48BucketA, enc), []byte{0x00}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	count, err := world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users"})
	if err != nil || count.Count != 1 {
		t.Fatalf("Count = %+v %v, want 1", count, err)
	}
	page, err := world.list(cluster.ListRequest{})
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("List = %d %v, want 1", len(page.Entries), err)
	}
}

func TestAnOwnerThatDoesNotOwnASlotRefuses(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	var mask cluster.SlotMask
	for slot := uint16(0); slot < 8192; slot++ {
		mask.Set(slot)
	}
	scanner := world.router.scanners["n1"]
	var coded *cluster.Error
	_, err := scanner.List(context.Background(), cluster.SlotListRequest{Epoch: u48Epoch, DBID: u48DB, BucketID: u48BucketA, Mask: mask, Want: 10})
	if !errors.As(err, &coded) || coded.Code != cluster.CodeNotOwner {
		t.Fatalf("a full mask at one node: %v", err)
	}
	var none cluster.SlotMask
	_, err = scanner.Count(context.Background(), cluster.SlotCountRequest{Epoch: u48Epoch + 1, DBID: u48DB, BucketID: u48BucketA, Mask: none})
	if !errors.As(err, &coded) || coded.Code != cluster.CodeStaleEpoch {
		t.Fatalf("a request at another epoch: %v", err)
	}
}

// u48Flaky wraps the router: a node refuses its next calls with the stated code, and every
// request is recorded.
type u48Flaky struct {
	inner    cluster.OwnerScanner
	mu       sync.Mutex
	refuse   map[string]int // node -> refusals left
	code     cluster.Code
	listReqs []cluster.SlotListRequest
}

func (f *u48Flaky) refused(node string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse[node] > 0 {
		f.refuse[node]--
		return &cluster.Error{Code: f.code, Message: "refused by " + node}
	}
	return nil
}

func (f *u48Flaky) ScanList(ctx context.Context, owner cluster.Member, req cluster.SlotListRequest) (cluster.SlotListReply, error) {
	f.mu.Lock()
	f.listReqs = append(f.listReqs, req)
	f.mu.Unlock()
	if err := f.refused(owner.ID); err != nil {
		return cluster.SlotListReply{}, err
	}
	return f.inner.ScanList(ctx, owner, req)
}

func (f *u48Flaky) ScanCount(ctx context.Context, owner cluster.Member, req cluster.SlotCountRequest) (int64, error) {
	if err := f.refused(owner.ID); err != nil {
		return 0, err
	}
	return f.inner.ScanCount(ctx, owner, req)
}

func (w *u48World) useFlaky(code cluster.Code, refusals int) *u48Flaky {
	flaky := &u48Flaky{inner: w.router, refuse: map[string]int{"n1": refusals}, code: code}
	w.coord = cluster.NewCoordinator(cluster.CoordinatorConfig{Topology: w.topo, Scanner: flaky, Buckets: u48Resolver{}, Limits: u48DefaultLimits})
	return flaky
}

func TestACoordinatorRefreshesOnceAndRestartsAtTheSamePosition(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		world.insert("Users", u48BucketA, id, "n", 0)
	}
	first, err := world.list(cluster.ListRequest{Limit: 2})
	if err != nil || first.Cursor == nil {
		t.Fatalf("first page: %v", err)
	}
	cursor, err := cluster.DecodeCursor(first.Cursor, u48DB, u48BucketA)
	if err != nil {
		t.Fatal(err)
	}
	flaky := world.useFlaky(cluster.CodeStaleEpoch, 1)
	second, err := world.list(cluster.ListRequest{Limit: 2, Cursor: first.Cursor})
	if err != nil || len(second.Entries) != 2 || world.topo.refreshes != 1 {
		t.Fatalf("one refusal: entries=%d refreshes=%d err=%v", len(second.Entries), world.topo.refreshes, err)
	}
	if len(flaky.listReqs) < 4 {
		t.Fatalf("the retry sent no second round of sub-scans: %d requests", len(flaky.listReqs))
	}
	for _, req := range flaky.listReqs {
		if req.ResumeSlot != cursor.Slot || !bytes.Equal(req.ResumeID, cursor.EncID) {
			t.Fatalf("a sub-scan resumed at (%d, %x), want the cursor's (%d, %x) on every attempt", req.ResumeSlot, req.ResumeID, cursor.Slot, cursor.EncID)
		}
	}
}

func TestASecondRefusalSurfacesItsCodeForListAndCount(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	world.insert("Users", u48BucketA, "a", "n", 0)
	var coded *cluster.Error
	world.useFlaky(cluster.CodeNotOwner, 2)
	if _, err := world.list(cluster.ListRequest{}); !errors.As(err, &coded) || coded.Code != cluster.CodeNotOwner {
		t.Fatalf("a second list refusal must surface its code: %v", err)
	}
	if world.topo.refreshes != 1 {
		t.Fatalf("refreshes = %d, want exactly 1", world.topo.refreshes)
	}
	world.topo.refreshes = 0
	world.useFlaky(cluster.CodeNotOwner, 2)
	if _, err := world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users"}); !errors.As(err, &coded) || coded.Code != cluster.CodeNotOwner {
		t.Fatalf("a second count refusal must surface its code: %v", err)
	}
	if world.topo.refreshes != 1 {
		t.Fatalf("count refreshes = %d, want exactly 1", world.topo.refreshes)
	}
}

func TestACountRefreshesOnceAndAnswers(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	for _, id := range []string{"a", "b", "c"} {
		world.insert("Users", u48BucketA, id, "n", 0)
	}
	world.useFlaky(cluster.CodeStaleEpoch, 1)
	count, err := world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users"})
	if err != nil || count.Count != 3 || world.topo.refreshes != 1 {
		t.Fatalf("Count = %+v refreshes=%d err=%v, want 3 after one refresh", count, world.topo.refreshes, err)
	}
}

// insertAge adds a document whose Age (field 3) is stored as the given Go value.
func (w *u48World) insertAge(id string, age any) {
	w.t.Helper()
	encID := w.insert("Users", u48BucketA, id, "n", 0)
	ageType, ageBytes, err := bson.MarshalValue(age)
	if err != nil {
		w.t.Fatalf("MarshalValue(%v): %v", age, err)
	}
	slot := u48Slot("Users", encID)
	batch := w.kv.NewBatch()
	defer batch.Discard()
	key := store.FieldKey(store.DocumentPrefix(slot, u48DB, u48BucketA, encID), 3)
	if err := batch.Set(key, append([]byte{byte(ageType)}, ageBytes...)); err != nil {
		w.t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		w.t.Fatal(err)
	}
}

// TestAFilterMatchesNumbersAcrossTypes: equality on a numeric field compares values, so a
// document stored with int32 5 is found by int64 5 and by 5.0, for list and for count.
func TestAFilterMatchesNumbersAcrossTypes(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	world.insertAge("i32", int32(5))
	world.insertAge("i64", int64(5))
	world.insertAge("dbl", float64(5))
	world.insertAge("frac", 5.5)
	world.insertAge("zero", int32(0))
	world.insertAge("negzero", math.Copysign(0, -1))
	world.insertAge("text", "5")
	world.insertAge("big", int64(math.MaxInt64))
	cases := []struct {
		name   string
		filter any
		want   int64
	}{
		{"int32 5", int32(5), 3},
		{"int64 5", int64(5), 3},
		{"double 5.0", float64(5), 3},
		{"double 5.5", 5.5, 1},
		{"int 0", int64(0), 2},
		{"double -0", math.Copysign(0, -1), 2},
		{"string 5", "5", 1},
		{"NaN", math.NaN(), 0},
		{"double 2^63 is not int64 max", float64(1 << 63), 0},
		{"int64 max", int64(math.MaxInt64), 1},
	}
	for _, tc := range cases {
		filter, err := bson.Marshal(bson.D{{Key: "Age", Value: tc.filter}})
		if err != nil {
			t.Fatal(err)
		}
		count, err := world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users", Filter: filter})
		if err != nil || count.Count != tc.want {
			t.Fatalf("%s: count = %d, %v, want %d", tc.name, count.Count, err, tc.want)
		}
		page, err := world.list(cluster.ListRequest{Filter: filter})
		if err != nil || int64(len(page.Entries)) != tc.want {
			t.Fatalf("%s: list = %d, %v, want %d", tc.name, len(page.Entries), err, tc.want)
		}
	}
}

// TestAScanSeeksOncePerSlotInItsMask: a full-bucket list over three owners costs one
// iterator Seek per slot, 8192 in all, however many buckets the store holds.
func TestAScanSeeksOncePerSlotInItsMask(t *testing.T) {
	world := newU48World(t, u48DefaultLimits)
	world.insert("Users", u48BucketA, "a", "n", 0)
	for i := 0; i < 500; i++ { // other buckets cost nothing
		world.insert("Others", u48BucketB, fmt.Sprintf("o%d", i), "n", 0)
	}
	world.seeks.Store(0)
	if _, err := world.list(cluster.ListRequest{}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if world.seeks.Load() != 8192 {
		t.Fatalf("a list seeked %d times, want exactly 8192 (one per slot)", world.seeks.Load())
	}
	world.seeks.Store(0)
	if _, err := world.coord.Count(context.Background(), cluster.CountRequest{Database: "app", Bucket: "Users"}); err != nil {
		t.Fatalf("Count: %v", err)
	}
	if world.seeks.Load() != 8192 {
		t.Fatalf("a count seeked %d times, want 8192", world.seeks.Load())
	}
}
