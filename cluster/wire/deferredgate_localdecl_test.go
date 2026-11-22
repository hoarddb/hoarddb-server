package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// G7 of the deferred pair gate spec (section 5, invariant G-5): a deferred pair
// reads a declaration that lives in its own slot from its own store when the routed read
// fails or answers not found, and only that one. One node holds database g7 with three
// buckets, Box, Crate and Bin, and four documents, each in its own pair:
//
//   - box: a Box document in the slot of Box's declaration;
//   - crateHome: a Crate document in the slot of Crate's declaration;
//   - crateOut: a Crate document in a third slot, so its declaration is in another pair;
//   - bin: a Bin document in the slot of Bin's declaration, which also holds Bin's
//     reverse-index document (id to name): Bin's id is drawn so that it lands there.
//
// Box's and Crate's ids are drawn so that their reverse-index documents are in none of those
// slots. Each pair gets pushes above a store put back behind its log, so all four are deferred
// at the restart. The routed declaration read then answers not found, as the slot's owner does
// when it is another node that lacks the document; the routed reverse-index read can be made to
// answer not found too.

const g7DB = "g7"

// g7Doc is one document of the fixture, in its own pair.
type g7Doc struct {
	bucket, id string
	slot       uint16
	items      []string
	// tail is the pair's applied position after the acknowledged pushes.
	tail cluster.Position
}

// g7Declarations is the fixture's routed declaration read: while absent is set it answers
// not found, as the owner of the declaration's slot does when that is another node whose
// store lacks the document. The reverse-index read is answered after onName, when set, has
// seen it: not found while namesAbsent is set, as ever otherwise.
type g7Declarations struct {
	DeclarationReader
	absent, namesAbsent *atomic.Bool
	onName              *atomic.Pointer[func(DeclarationRead)]
}

func (r g7Declarations) ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	if hook := r.onName.Load(); hook != nil {
		(*hook)(read)
	}
	if r.namesAbsent.Load() {
		return "", false, nil
	}
	return r.DeclarationReader.ReadBucketName(ctx, owner, read)
}

func (r g7Declarations) ReadDeclaration(ctx context.Context, owner cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	if r.absent.Load() {
		return nil, false, nil
	}
	return r.DeclarationReader.ReadDeclaration(ctx, owner, read)
}

type g7Fixture struct {
	t                          *testing.T
	node                       *Node
	db                         uint64
	boxID, crateID, binID      uint64
	queueField                 uint16
	boxVersion                 int64
	box, crateHome, crate, bin *g7Doc
	absent, namesAbsent        *atomic.Bool
	onName                     *atomic.Pointer[func(DeclarationRead)]
	unhold                     func()
}

func g7DeclSlot(t *testing.T, bucket string) uint16 {
	t.Helper()
	encoded, err := store.EncodeComponent(bucket)
	if err != nil {
		t.Fatal(err)
	}
	return storage.SlotOf(storage.PlacementKey(g7DB, systemBucketName, encoded))
}

// g7ReverseSlot is the slot of the reverse-index document of bucketID, which maps it to its name.
func g7ReverseSlot(t *testing.T, bucketID uint64) uint16 {
	t.Helper()
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], bucketID)
	encoded, err := store.EncodeComponent(raw[:])
	if err != nil {
		t.Fatal(err)
	}
	return storage.SlotOf(storage.PlacementKey(g7DB, systemBucketName, encoded))
}

// g7DrawID is a CreateBucketConfig.DrawID that draws the first id from start on whose
// reverse-index document lands in a slot want accepts: bucket ids are random, and the fixture
// needs to know which pair holds each bucket's name.
func g7DrawID(t *testing.T, start uint64, want func(uint16) bool) func() (uint64, error) {
	t.Helper()
	return func() (uint64, error) {
		for id := start; id < start+10_000_000; id++ {
			if id != store.BucketsBucketID && want(g7ReverseSlot(t, id)) {
				return id, nil
			}
		}
		return 0, fmt.Errorf("no bucket id from %d on has its reverse-index document where the fixture needs it", start)
	}
}

// g7Place finds a document id of bucket whose slot satisfies want.
func g7Place(t *testing.T, bucket string, want func(uint16) bool) *g7Doc {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		id := fmt.Sprintf("%s-%d", bucket, i)
		slot, err := PlacementOf(g7DB, bucket, id)
		if err != nil {
			t.Fatal(err)
		}
		if want(slot) {
			return &g7Doc{bucket: bucket, id: id, slot: slot}
		}
	}
	t.Fatalf("no id of %s has the slot the fixture needs", bucket)
	return nil
}

func g7Target(doc *g7Doc) StoreTarget {
	return StoreTarget{Database: g7DB, Bucket: doc.bucket, ID: doc.id, Field: "Queue"}
}

// newG7Fixture builds the four deferred pairs. With withoutBoxDeclaration the store Box's
// pair is put back to lacks Box's declaration document, so the local read finds nothing.
func newG7Fixture(t *testing.T, withoutBoxDeclaration bool) *g7Fixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	f := &g7Fixture{t: t, absent: &atomic.Bool{}, namesAbsent: &atomic.Bool{}, onName: &atomic.Pointer[func(DeclarationRead)]{}}
	var err error
	if f.db, err = node.CreateDatabaseRF(ctx, g7DB, 1); err != nil {
		t.Fatalf("CreateDatabaseRF: %v", err)
	}
	boxDecl, crateDecl, binDecl := g7DeclSlot(t, "Box"), g7DeclSlot(t, "Crate"), g7DeclSlot(t, "Bin")
	if boxDecl == crateDecl || boxDecl == binDecl || crateDecl == binDecl {
		t.Fatalf("Box, Crate and Bin declare in slots %d, %d and %d; the fixture needs three pairs", boxDecl, crateDecl, binDecl)
	}
	declSlot := func(slot uint16) bool { return slot == boxDecl || slot == crateDecl || slot == binDecl }
	fields := []Field{{Name: "Name", Type: "string"}, {Name: "Queue", Type: "fifoStore"}}
	node.creator.cfg.DrawID = g7DrawID(t, 1<<40, func(slot uint16) bool { return !declSlot(slot) })
	if f.boxID, err = node.CreateBucket(ctx, g7DB, "Box", fields...); err != nil {
		t.Fatalf("CreateBucket Box: %v", err)
	}
	node.creator.cfg.DrawID = g7DrawID(t, f.boxID+1, func(slot uint16) bool { return !declSlot(slot) })
	if f.crateID, err = node.CreateBucket(ctx, g7DB, "Crate", fields...); err != nil {
		t.Fatalf("CreateBucket Crate: %v", err)
	}
	node.creator.cfg.DrawID = g7DrawID(t, 1<<41, func(slot uint16) bool { return slot == binDecl })
	if f.binID, err = node.CreateBucket(ctx, g7DB, "Bin", fields...); err != nil {
		t.Fatalf("CreateBucket Bin: %v", err)
	}
	box, err := node.fetcher.Lookup(ctx, f.db, "Box", node.topology.Published(f.db))
	if err != nil {
		t.Fatal(err)
	}
	f.boxVersion = box.Decl.Version
	crate, err := node.fetcher.Lookup(ctx, f.db, "Crate", node.topology.Published(f.db))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(box.Decl.Fields.Entries, crate.Decl.Fields.Entries) {
		t.Fatalf("Box and Crate declare different field ids: %+v and %+v", box.Decl.Fields.Entries, crate.Decl.Fields.Entries)
	}
	for _, entry := range crate.Decl.Fields.Entries {
		if entry.Name == "Queue" {
			f.queueField = entry.ID
		}
	}
	f.box = g7Place(t, "Box", func(slot uint16) bool { return slot == boxDecl })
	f.crateHome = g7Place(t, "Crate", func(slot uint16) bool { return slot == crateDecl })
	f.crate = g7Place(t, "Crate", func(slot uint16) bool { return !declSlot(slot) })
	f.bin = g7Place(t, "Bin", func(slot uint16) bool { return slot == binDecl })
	docs := []*g7Doc{f.box, f.crateHome, f.crate, f.bin}
	for _, doc := range docs {
		if _, err := node.Insert(ctx, g7DB, doc.bucket, bson.D{{Key: "_id_", Value: doc.id}, {Key: "Name", Value: doc.id}}); err != nil {
			t.Fatalf("Insert %s: %v", doc.id, err)
		}
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	saved := make([]savedPair, 0, len(docs))
	for _, doc := range docs {
		saved = append(saved, savePair(t, dir, doc.slot, f.db))
	}
	if withoutBoxDeclaration {
		encoded, err := store.EncodeComponent("Box")
		if err != nil {
			t.Fatal(err)
		}
		declaration := store.DocumentPrefix(f.box.slot, f.db, store.BucketsBucketID, encoded)
		removed := 0
		for key := range saved[0].keys {
			if bytes.HasPrefix([]byte(key), declaration) {
				delete(saved[0].keys, key)
				removed++
			}
		}
		if removed == 0 {
			t.Fatal("the saved pair of Box's declaration slot holds no declaration document to remove")
		}
	}

	node = s0OpenClusterNode(t, dir, nil)
	for _, doc := range docs {
		for i := 1; i <= 2; i++ {
			item := fmt.Sprintf("%s-x%d", doc.id, i)
			bug0760Retry(t, "Push "+item, func() error {
				return node.Push(ctx, PushRequest{StoreTarget: g7Target(doc), Element: bson.D{{Key: "item", Value: item}}})
			})
			doc.items = append(doc.items, item)
		}
		doc.tail = node.pairs.Snapshot(doc.slot, f.db).AppliedPos
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	for _, pair := range saved {
		rewindPair(t, dir, pair)
	}

	pass := make(chan struct{})
	var once sync.Once
	f.unhold = func() { once.Do(func() { close(pass) }) }
	bootReplayPass = func() {
		select {
		case <-pass:
		case <-time.After(2 * time.Minute):
		}
	}
	t.Cleanup(func() { bootReplayPass = nil })
	f.absent.Store(true)
	reopened, _, release := openHeld(t, dir, func(n *Node) {
		n.fetcher.cfg.Reader = g7Declarations{DeclarationReader: n.fetcher.cfg.Reader, absent: f.absent, namesAbsent: f.namesAbsent, onName: f.onName}
	})
	t.Cleanup(f.unhold)
	release()
	f.node = reopened
	for _, doc := range docs {
		if !reopened.own.isDeferred(doc.slot, f.db) || !reopened.pairs.Snapshot(doc.slot, f.db).AppliedPos.Less(doc.tail) {
			t.Fatalf("the fixture's pair of %s (slot %d) is not deferred behind its log: deferred %v, applied %s, log tail %s",
				doc.id, doc.slot, reopened.own.isDeferred(doc.slot, f.db), reopened.pairs.Snapshot(doc.slot, f.db).AppliedPos, doc.tail)
		}
	}
	return f
}

// awaitReleased waits until none of docs is deferred.
func (f *g7Fixture) awaitReleased(within time.Duration, docs ...*g7Doc) {
	f.t.Helper()
	deadline := time.Now().Add(within)
	for {
		waiting := []string{}
		for _, doc := range docs {
			if f.node.own.isDeferred(doc.slot, f.db) {
				waiting = append(waiting, doc.id)
			}
		}
		if len(waiting) == 0 {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("the pairs of %v were still deferred %s after their replays were let go, with the routed declaration read answering not found", waiting, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// staysDeferred fails unless doc's pair is deferred for the whole of d, applied below its tail.
func (f *g7Fixture) staysDeferred(doc *g7Doc, d time.Duration, why string) {
	f.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !f.node.own.isDeferred(doc.slot, f.db) {
			f.t.Fatalf("the pair of %s (slot %d) was released: %s", doc.id, doc.slot, why)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if applied := f.node.pairs.Snapshot(doc.slot, f.db).AppliedPos; !applied.Less(doc.tail) {
		f.t.Fatalf("the pair of %s is still deferred but applied at %s, its log tail %s: %s", doc.id, applied, doc.tail, why)
	}
}

// holdsOnce fails unless the store of doc's pair is at its tail and holds each push once.
func (f *g7Fixture) holdsOnce(doc *g7Doc) {
	f.t.Helper()
	applied, entries, err := f.node.PairDump(doc.slot, f.db)
	if err != nil {
		f.t.Fatal(err)
	}
	if applied != doc.tail {
		f.t.Fatalf("the pair of %s is applied at %s, want its log tail %s", doc.id, applied, doc.tail)
	}
	for _, item := range doc.items {
		count := 0
		for _, entry := range entries {
			count += bytes.Count(entry.Value, []byte(item+"\x00"))
		}
		if count != 1 {
			f.t.Fatalf("the pair of %s holds the acknowledged push %s %d times, want once", doc.id, item, count)
		}
	}
}

// queue reads doc's Queue through the node once it serves the pair.
func (f *g7Fixture) queue(doc *g7Doc) []string {
	f.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		elements, err := f.node.Peek(context.Background(), PeekRequest{StoreTarget: g7Target(doc), Count: 100})
		if err == nil {
			items := make([]string, 0, len(elements))
			for _, element := range elements {
				items = append(items, u62Item(f.t, element))
			}
			return items
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("Peek of %s still refused 30 s after the release: %v", doc.id, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestADeferredPairReadsItsOwnDeclaration is G7. A pair deferred on a push whose declaration
// is in its own slot is released from its own store's document and holds the push; a pair
// whose declaration is in another pair of this node is not, though that pair's store holds
// the document and its own replay reads it; and only a boot replay reads locally: an apply
// that is not one (a copy's) still waits for the routed read. The local document fills no
// cache, which every pair's apply would read.
// [BREAK: no local read: box stays deferred; read any locally held pair's declaration, or
// key the read on the node's deferred set: crate is released; a local read inside
// declaredEngines.view, which every apply reads: the copy's read succeeds; fill the cache from
// the local read: crate is released. Dropping rec.BootReplay from FillWithResult's condition is
// caught by TestOnlyTheReplayingRecordReadsItsDeclarationLocally, not here]
func TestADeferredPairReadsItsOwnDeclaration(t *testing.T) {
	f := newG7Fixture(t, false)

	// While every pair is deferred and held: an apply that is not a boot replay reads no local
	// document, even of a bucket whose declaration's pair is itself replaying here.
	if _, err := (declaredEngines{node: f.node}).FieldEngine(f.db, f.crateID, f.queueField); !errors.Is(err, ErrDeclUnavailable) {
		t.Fatalf("an apply outside a boot replay resolved Crate's Queue while the routed read answers not found (%v): only a boot replay of the declaration's own pair may read its own store", err)
	}

	f.unhold()
	f.awaitReleased(20*time.Second, f.box, f.crateHome)
	f.holdsOnce(f.box)
	f.holdsOnce(f.crateHome)
	f.staysDeferred(f.crate, 3*time.Second, "its push names Crate, whose declaration is in another pair; only the declaration in the pair being replayed may be read locally")
	for _, id := range []uint64{f.boxID, f.crateID} {
		if view := f.node.schema.View(f.db, id); view.Present {
			t.Fatalf("the declaration of bucket %d is cached after the routed read answered not found: a local read must serve only the record being replayed", id)
		}
	}

	// The routed read answers again: the last pair is released and every push is served once.
	f.absent.Store(false)
	f.awaitReleased(20*time.Second, f.crate)
	f.holdsOnce(f.crate)
	for _, doc := range []*g7Doc{f.box, f.crateHome, f.crate} {
		if got := f.queue(doc); len(got) != len(doc.items) || got[0] != doc.items[0] || got[1] != doc.items[1] {
			t.Fatalf("Queue of %s after the release = %v, want %v", doc.id, got, doc.items)
		}
	}
}

// TestADeferredPairWaitsWhenItsOwnStoreLacksTheDeclaration is G7's not-found case: the store
// of Box's pair lacks the declaration, so the local read proves nothing. The pair stays
// deferred, its pushes in its log and not written as empty writes, while the pair beside it
// is released by its own declaration.
// [BREAK: treat a local not-found as a drop: box is released without its pushes]
func TestADeferredPairWaitsWhenItsOwnStoreLacksTheDeclaration(t *testing.T) {
	f := newG7Fixture(t, true)
	f.unhold()
	f.awaitReleased(20*time.Second, f.crateHome)
	f.staysDeferred(f.box, 3*time.Second, "its own store lacks Box's declaration and the routed read answers not found, and a local not-found proves nothing")
}

// TestOnlyTheReplayingRecordReadsItsDeclarationLocally is G7 with two applies at once: the boot
// replay of Box's pair is inside its routed declaration read, and an apply that is not a boot
// replay (a copy's) resolves a Box record meanwhile, both through the engine source and through
// DocumentApplier.FillWithResult with one of Box's logged pushes. Neither may read Box's
// declaration from this node's store: what allows the read is the record's own pair and its
// BootReplay mark, not a mark of what the node is replaying, which every other apply on the node
// would see. The same push marked BootReplay is applied from the local declaration, so the
// refusal is the mark's doing.
// [BREAK: key the local read on a node-wide "pair being replayed" mark; drop rec.BootReplay
// from FillWithResult's condition: the copy's FillWithResult succeeds]
func TestOnlyTheReplayingRecordReadsItsDeclarationLocally(t *testing.T) {
	f := newG7Fixture(t, false)
	var boxName [8]byte
	binary.BigEndian.PutUint64(boxName[:], f.boxID)
	encoded, err := store.EncodeComponent(boxName[:])
	if err != nil {
		t.Fatal(err)
	}
	inside, resume := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	hook := func(read DeclarationRead) {
		// Only the first read waits; a sync.Once would make the copy's read wait for it too.
		if bytes.Equal(read.EncodedID, encoded) && first.CompareAndSwap(false, true) {
			close(inside)
			<-resume
		}
	}
	f.onName.Store(&hook)
	t.Cleanup(func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	})

	f.unhold()
	select {
	case <-inside:
	case <-time.After(20 * time.Second):
		t.Fatal("the boot replay never read the name of Box's id")
	}
	_, copyErr := (declaredEngines{node: f.node}).FieldEngine(f.db, f.boxID, f.queueField)
	push := f.lastLogged(f.box, f.boxID)
	push.BootReplay = false
	fillErr := f.fill(push)
	close(resume)
	if !errors.Is(copyErr, ErrDeclUnavailable) {
		t.Fatalf("an apply outside the boot replay resolved Box's Queue while the replay of Box's own pair was reading its declaration (%v): it read the declaration from this node's store", copyErr)
	}
	if !errors.Is(fillErr, ErrDeclUnavailable) {
		t.Fatalf("FillWithResult of Box's push at %s, not marked as a boot replay, gave %v while the routed read answers not found: an apply that is not a boot replay read the declaration from this node's store", push.Pos, fillErr)
	}
	f.awaitReleased(20*time.Second, f.box, f.crateHome)
	f.holdsOnce(f.box)
	push.BootReplay = true
	if err := f.fill(push); err != nil {
		t.Fatalf("FillWithResult of Box's push at %s marked as a boot replay of its own pair: %v; want it applied from this node's own declaration", push.Pos, err)
	}
}

// lastLogged is the last record of doc's pair in the node's log that names bucketID.
func (f *g7Fixture) lastLogged(doc *g7Doc, bucketID uint64) AppliedRecord {
	f.t.Helper()
	var last *AppliedRecord
	for _, record := range loggedOps(f.t, f.node) {
		if record.Slot == doc.slot && record.DB == f.db && record.Op.BucketID == bucketID {
			last = &record
		}
	}
	if last == nil {
		f.t.Fatalf("the log holds no record of slot %d naming bucket %d", doc.slot, bucketID)
	}
	return *last
}

// fill applies rec through an applier built as the node builds its own, into a batch that is
// discarded: only the error matters.
func (f *g7Fixture) fill(rec AppliedRecord) error {
	applier := NewDocumentApplier(f.node.kv, nil).WithFieldEngines(declaredEngines{node: f.node})
	batch := f.node.kv.NewBatch()
	defer batch.Discard()
	_, err := applier.FillWithResult(batch, rec)
	return err
}

// TestADeferredPairReadsItsBucketsNameOnlyFromItsOwnPair is G7's reverse-index document
// (Fetcher.nameInPair): with the routed reads of both documents answering not found, Bin's pair,
// which holds Bin's declaration and Bin's id-to-name document, is released from its own store;
// Box's pair holds Box's declaration but not Box's name, so it stays deferred until the routed
// name read answers again.
// [BREAK: route every reverse-index read (drop nameInPair's local branch): bin stays deferred;
// read the reverse-index document from this node's store whatever its slot: box is released]
func TestADeferredPairReadsItsBucketsNameOnlyFromItsOwnPair(t *testing.T) {
	f := newG7Fixture(t, false)
	if g7ReverseSlot(t, f.binID) != f.bin.slot || g7ReverseSlot(t, f.boxID) == f.box.slot {
		t.Fatalf("the fixture's ids: Bin's name in slot %d (want %d), Box's in slot %d (want not %d)", g7ReverseSlot(t, f.binID), f.bin.slot, g7ReverseSlot(t, f.boxID), f.box.slot)
	}
	f.namesAbsent.Store(true)
	f.unhold()
	f.awaitReleased(20*time.Second, f.bin)
	f.holdsOnce(f.bin)
	f.staysDeferred(f.box, 3*time.Second, "Box's id-to-name document is in another pair and the routed read of it answers not found; only a document in the pair being replayed may be read locally")

	f.namesAbsent.Store(false)
	f.awaitReleased(20*time.Second, f.box, f.crateHome)
	f.holdsOnce(f.box)
}

// TestALocalDeclarationBelowTheRecordsVersionIsNotUsed is G7's version rule (spec section 5,
// last item of the rule): the local document settles a record only at or above the declaration
// version the owner wrote it under, and only for a declaration in the record's own pair.
// [BREAK: drop the version comparison; drop the slot comparison]
func TestALocalDeclarationBelowTheRecordsVersionIsNotUsed(t *testing.T) {
	f := newG7Fixture(t, false)
	resolve := func(slot uint16, bucketID uint64, version int64) (store.FieldEngine, error) {
		record := AppliedRecord{Slot: slot, DB: f.db, Op: cluster.Op{DeclVersion: version}, BootReplay: true}
		return (declaredEngines{node: f.node}).inPair(record).FieldEngine(f.db, bucketID, f.queueField)
	}
	f.unhold()
	f.awaitReleased(20*time.Second, f.box, f.crateHome)
	if engine, err := resolve(f.box.slot, f.boxID, f.boxVersion); err != nil || engine != store.FieldFIFO {
		t.Fatalf("a record of Box's own pair at the declaration's version %d resolved Queue as %v, %v; want the fifo engine from this node's store", f.boxVersion, engine, err)
	}
	if engine, err := resolve(f.box.slot, f.boxID, f.boxVersion+1); !errors.Is(err, ErrDeclUnavailable) {
		t.Fatalf("a record written under version %d resolved Queue as %v, %v from a local declaration at version %d; want the routed read's failure", f.boxVersion+1, engine, err, f.boxVersion)
	}
	if engine, err := resolve(f.box.slot, f.crateID, 0); !errors.Is(err, ErrDeclUnavailable) {
		t.Fatalf("a record of Box's pair naming Crate, whose declaration is in another pair, resolved Queue as %v, %v; want the routed read's failure", engine, err)
	}
}
