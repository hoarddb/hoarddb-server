package wire

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	u35Slot     uint16 = 9
	u35DB       uint64 = 3
	u35Bucket   uint64 = 41
	u35FifoID   uint16 = 5
	u35FieldAge uint16 = 2
)

var u35Header = []byte{0x02, 'p', '1', 0}

func u35Ref() storage.BucketRef {
	return storage.BucketRef{Slot: u35Slot, DatabaseID: u35DB, BucketID: u35Bucket}
}

func u35Doc(t *testing.T, id string) DocumentKey {
	t.Helper()
	enc, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatalf("encoding %q: %v", id, err)
	}
	return DocumentKey{Ref: u35Ref(), EncID: enc}
}

// u35Commit runs fill against a fresh snapshot and batch and commits it.
func u35Commit(t *testing.T, kv storage.KV, fill func(snap storage.KVSnapshot, batch storage.KVBatch) error) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := fill(snap, batch); err != nil {
		t.Fatalf("filling the batch: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing: %v", err)
	}
}

func u35Insert(doc DocumentKey) InsertPath {
	return InsertPath{
		Insert: store.DocumentInsert{
			Ref:    doc.Ref,
			EncID:  doc.EncID,
			Header: u35Header,
			Fields: []store.DocumentField{{FieldID: u35FieldAge, Stored: []byte{0x10, 7}}},
		},
		Limit: 1 << 20,
	}
}

// u35Seed inserts doc with three fifo elements under its store prefix.
func u35Seed(t *testing.T, kv storage.KV, doc DocumentKey) [][]byte {
	t.Helper()
	prefix := store.StorePrefix(doc.Ref.Slot, doc.Ref.DatabaseID, doc.Ref.BucketID, doc.EncID, u35FifoID)
	var elements [][]byte
	u35Commit(t, kv, func(_ storage.KVSnapshot, batch storage.KVBatch) error {
		if err := store.WriteInsert(batch, u35Insert(doc).Insert); err != nil {
			return err
		}
		for i := byte(1); i <= 3; i++ {
			key := append(append([]byte(nil), prefix...), 0x01, i)
			elements = append(elements, key)
			if err := batch.Set(key, []byte{i}); err != nil {
				return err
			}
		}
		return nil
	})
	return elements
}

func u35Dump(t *testing.T, kv storage.KV) map[string]string {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	found := map[string]string{}
	for it.Seek(nil); it.Valid(); it.Next() {
		stored, err := it.Value()
		if err != nil {
			t.Fatalf("reading %x: %v", it.Key(), err)
		}
		found[string(it.Key())] = string(stored)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("walking the store: %v", err)
	}
	return found
}

func u35State(t *testing.T, kv storage.KV, doc DocumentKey) HeaderState {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	state, err := ReadHeaderState(snap, doc)
	if err != nil {
		t.Fatalf("reading the header state: %v", err)
	}
	return state
}

func u35Delete(t *testing.T, kv storage.KV, doc DocumentKey, pos cluster.Position) {
	t.Helper()
	u35Commit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		return ApplyDelete(snap, batch, doc, pos)
	})
}

func u35Sweeper(kv storage.KV) *Sweeper { return NewSweeper(kv, &cluster.PairSet{}) }

func u35RecordCount(dump map[string]string) int {
	prefix := string(storage.NodeKey(storage.NSReclaim, []byte{'d'}))
	count := 0
	for key := range dump {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

// TestDeleteIsTombstoneThenSweep: after the ack the document is absent and the
// 'd' record carries this position; the sweep removes st, then doc's fields,
// then the header, then the record, and touches no neighbour.
func TestDeleteIsTombstoneThenSweep(t *testing.T) {
	kv := kvfault.New()
	doc := u35Doc(t, "a")
	neighbour := u35Doc(t, "ab")
	u35Seed(t, kv, doc)
	u35Seed(t, kv, neighbour)
	before := u35Dump(t, kv)

	pos := cluster.Position{Epoch: 4, Seq: 9}
	u35Delete(t, kv, doc, pos)

	if state := u35State(t, kv, doc); state != HeaderTombstone {
		t.Fatalf("header state after the delete = %d, want the tombstone", state)
	}
	after := u35Dump(t, kv)
	record := ReclaimRecord{Doc: doc, Pos: pos}
	if _, ok := after[string(record.Key())]; !ok {
		t.Fatalf("no 'd' record stamped with %s", pos)
	}
	if got := len(after) - len(before); got != 1 {
		t.Fatalf("the delete added %d keys net, want exactly the one record", got)
	}
	if after[string(store.DocumentPrefix(u35Slot, u35DB, u35Bucket, doc.EncID))] != "\x00" {
		t.Fatalf("header is not the single byte 0x00")
	}

	sweeper := u35Sweeper(kv)
	for batches := 0; ; batches++ {
		if batches > 20 {
			t.Fatal("the sweep did not finish")
		}
		progress, err := sweeper.SweepBatch(record, 1)
		if err != nil {
			t.Fatalf("sweeping: %v", err)
		}
		if progress.Done {
			break
		}
		if state := u35State(t, kv, doc); state != HeaderTombstone {
			t.Fatalf("batch %d left the header %d before the fields were gone: a crash here would leave a live-looking document", batches, state)
		}
	}
	final := u35Dump(t, kv)
	for key := range final {
		if bytes.HasPrefix([]byte(key), store.DocumentPrefix(u35Slot, u35DB, u35Bucket, doc.EncID)) &&
			!bytes.HasPrefix([]byte(key), store.DocumentPrefix(u35Slot, u35DB, u35Bucket, neighbour.EncID)) {
			t.Fatalf("key %x of the deleted document survived the sweep", key)
		}
	}
	if u35RecordCount(final) != 0 {
		t.Fatal("the 'd' record survived the sweep")
	}
	if len(final) != len(before)/2 {
		t.Fatalf("the neighbour lost keys: %d left of %d", len(final), len(before)/2)
	}
	again, err := sweeper.SweepBatch(record, 100)
	if err != nil || !again.Done || again.Deleted != 0 {
		t.Fatalf("a second pass = %+v, %v; want done and nothing deleted", again, err)
	}
}

// TestDeleteOfAnAbsentOrDeletedDocumentWritesNothing: the position is consumed
// by the caller; the batch holds no key, so no second record exists.
func TestDeleteOfAnAbsentOrDeletedDocumentWritesNothing(t *testing.T) {
	kv := kvfault.New()
	doc := u35Doc(t, "a")
	u35Delete(t, kv, doc, cluster.Position{Epoch: 1, Seq: 1})
	if len(u35Dump(t, kv)) != 0 {
		t.Fatal("deleting an absent document wrote keys")
	}
	u35Seed(t, kv, doc)
	u35Delete(t, kv, doc, cluster.Position{Epoch: 1, Seq: 2})
	first := u35Dump(t, kv)
	u35Delete(t, kv, doc, cluster.Position{Epoch: 1, Seq: 3})
	if u35RecordCount(u35Dump(t, kv)) != 1 || len(u35Dump(t, kv)) != len(first) {
		t.Fatal("deleting a tombstone wrote a second record")
	}
}

// TestDeleteThenReinsertDuringSeedKeepsTheStoreState is conventions §11's
// sequence on a document with three fifo elements, replayed on a second copy in
// seed order: both end with zero elements, one batch, no schedule difference.
func TestDeleteThenReinsertDuringSeedKeepsTheStoreState(t *testing.T) {
	doc := u35Doc(t, "a")
	owner := kvfault.New()
	u35Seed(t, owner, doc)
	u35Delete(t, owner, doc, cluster.Position{Epoch: 1, Seq: 2})

	// The seed copies the owner's state as of position 1:2, tombstone and
	// elements included (conv §11 rule 4); the re-insert then arrives as a
	// positioned record on top of it.
	seeded := kvfault.New()
	for key, stored := range u35Dump(t, owner) {
		if strings.HasPrefix(key, string(storage.NodeKey(storage.NSReclaim))) {
			continue // reclaim records are node-local and never travel
		}
		u35Commit(t, seeded, func(_ storage.KVSnapshot, batch storage.KVBatch) error {
			return batch.Set([]byte(key), []byte(stored))
		})
	}

	for name, kv := range map[string]storage.KV{"owner": owner, "seeded copy": seeded} {
		batches := 0
		u35Commit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
			batches++
			state, err := WriteInsertPath(snap, batch, u35Insert(doc))
			if err != nil {
				return err
			}
			if state != HeaderTombstone {
				t.Fatalf("%s: the insert saw state %d, want the tombstone", name, state)
			}
			return nil
		})
		if batches != 1 {
			t.Fatalf("%s: %d batches, want one", name, batches)
		}
	}

	ownerKeys, seededKeys := u35Dump(t, owner), u35Dump(t, seeded)
	if len(seededKeys) != len(ownerKeys) {
		t.Fatalf("the copies hold %d and %d keys", len(ownerKeys), len(seededKeys))
	}
	if fmt.Sprint(ownerKeys) != fmt.Sprint(seededKeys) {
		t.Fatalf("the two copies differ:\n owner  %q\n seeded %q", ownerKeys, seededKeys)
	}
	storePrefix := string(storage.Key(u35Slot, u35DB, u35Bucket, storage.NSFieldStore))
	for key := range ownerKeys {
		if strings.HasPrefix(key, storePrefix) {
			t.Fatalf("store key %x survived the re-insert", key)
		}
	}
	if u35RecordCount(ownerKeys) != 0 {
		t.Fatal("the 'd' record survived the re-insert")
	}
	if u35State(t, owner, doc) != HeaderLive {
		t.Fatal("the re-inserted document is not live")
	}
	// A late sweep of the old record must delete nothing of the new document.
	stale := ReclaimRecord{Doc: doc, Pos: cluster.Position{Epoch: 1, Seq: 2}}
	progress, err := u35Sweeper(owner).SweepBatch(stale, 100)
	if err != nil || progress.Deleted != 0 || !progress.Reinserted {
		t.Fatalf("a stale record's sweep = %+v, %v; want nothing deleted", progress, err)
	}
	if fmt.Sprint(u35Dump(t, owner)) != fmt.Sprint(ownerKeys) {
		t.Fatal("a stale record's sweep changed the re-inserted document")
	}
}

// u35CountingBatch counts the writes handed to it.
type u35CountingBatch struct {
	storage.KVBatch
	writes int
}

func (b *u35CountingBatch) Set(key, stored []byte) error {
	b.writes++
	return b.KVBatch.Set(key, stored)
}

func (b *u35CountingBatch) Delete(key []byte) error {
	b.writes++
	return b.KVBatch.Delete(key)
}

// TestUpsertOverATombstoneTakesTheInsertPath: header live in one batch, both
// prefixes cleaned, the record gone.
func TestUpsertOverATombstoneTakesTheInsertPath(t *testing.T) {
	kv := kvfault.New()
	doc := u35Doc(t, "a")
	u35Seed(t, kv, doc)
	u35Delete(t, kv, doc, cluster.Position{Epoch: 1, Seq: 2})

	var state HeaderState
	u35Commit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		var err error
		state, err = WriteInsertPath(snap, batch, u35Insert(doc))
		return err
	})
	if state != HeaderTombstone {
		t.Fatalf("state = %d, want the tombstone reported", state)
	}
	if u35State(t, kv, doc) != HeaderLive {
		t.Fatal("the header is not live after the upsert")
	}
	dump := u35Dump(t, kv)
	if u35RecordCount(dump) != 0 {
		t.Fatal("the record survived")
	}
	for key := range dump {
		if strings.HasPrefix(key, string(storage.Key(u35Slot, u35DB, u35Bucket, storage.NSFieldStore))) {
			t.Fatalf("store key %x survived", key)
		}
	}
	if len(dump) != 2 {
		t.Fatalf("%d keys, want the header and the one scalar", len(dump))
	}

	// A live header is the update path's: this function writes nothing.
	snap := kv.NewSnapshot()
	defer snap.Discard()
	counting := &u35CountingBatch{KVBatch: kv.NewBatch()}
	defer counting.Discard()
	state, err := WriteInsertPath(snap, counting, u35Insert(doc))
	if err != nil || state != HeaderLive {
		t.Fatalf("over a live header = %d, %v; want HeaderLive", state, err)
	}
	if counting.writes != 0 {
		t.Fatalf("the live branch wrote %d keys", counting.writes)
	}
}

// TestReinsertOverABigTombstoneIsRefusedNamingTheBytes: refused, nothing
// written, the text names both byte counts.
func TestReinsertOverABigTombstoneIsRefusedNamingTheBytes(t *testing.T) {
	kv := kvfault.New()
	doc := u35Doc(t, "a")
	u35Seed(t, kv, doc)
	u35Delete(t, kv, doc, cluster.Position{Epoch: 1, Seq: 2})
	before := u35Dump(t, kv)

	req := u35Insert(doc)
	req.Limit = 100
	snap := kv.NewSnapshot()
	defer snap.Discard()
	batch := kv.NewBatch()
	defer batch.Discard()
	err := InsertOverTombstone(snap, batch, req)
	var tooLarge *CleanupTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("err = %v, want a CleanupTooLargeError", err)
	}
	if tooLarge.Bytes <= tooLarge.Limit || tooLarge.Limit != 100 {
		t.Fatalf("bytes %d, limit %d: want bytes over a limit of 100", tooLarge.Bytes, tooLarge.Limit)
	}
	text := tooLarge.Message("People", "a")
	for _, want := range []string{fmt.Sprint(tooLarge.Bytes), "100", `"People"`, `"a"`, "Nothing was written", "db.People.delete"} {
		if !strings.Contains(text, want) {
			t.Fatalf("message %q lacks %q", text, want)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the refused batch: %v", err)
	}
	if fmt.Sprint(u35Dump(t, kv)) != fmt.Sprint(before) {
		t.Fatal("the refused insert wrote something")
	}
	if err := CheckTombstoneCleanup(snap, req); !errors.As(err, &tooLarge) {
		t.Fatalf("the precondition check = %v, want the same refusal", err)
	}
	req.Limit = 1 << 20
	if err := CheckTombstoneCleanup(snap, req); err != nil {
		t.Fatalf("a roomy limit refused: %v", err)
	}
}

// TestStoreVerbOnATombstoneIsRefused: INVALID_ARGUMENT, and nothing recreates
// the document.
func TestStoreVerbOnATombstoneIsRefused(t *testing.T) {
	kv := kvfault.New()
	doc := u35Doc(t, "a")
	u35Seed(t, kv, doc)
	check := func() cluster.Code {
		snap := kv.NewSnapshot()
		defer snap.Discard()
		code, err := CheckStoreVerbTarget(snap, doc)
		if err != nil {
			t.Fatalf("checking: %v", err)
		}
		return code
	}
	if code := check(); code != 0 {
		t.Fatalf("a live document refused with %d", code)
	}
	u35Delete(t, kv, doc, cluster.Position{Epoch: 1, Seq: 2})
	if code := check(); code != cluster.CodeInvalidArgument {
		t.Fatalf("a tombstone gave code %d, want INVALID_ARGUMENT", code)
	}
	text := StoreVerbOnTombstoneMessage("app", "People", "a")
	for _, want := range []string{`"a"`, "app.People", "is deleted", "db.People.delete"} {
		if !strings.Contains(text, want) {
			t.Fatalf("message %q lacks %q", text, want)
		}
	}
	if _, err := u35Sweeper(kv).SweepBatch(ReclaimRecord{Doc: doc, Pos: cluster.Position{Epoch: 1, Seq: 2}}, 100); err != nil {
		t.Fatalf("sweeping: %v", err)
	}
	if code := check(); code != cluster.CodeNotFound {
		t.Fatalf("an absent document gave code %d, want NOT_FOUND", code)
	}
}

// TestSweepNeverDeletesAnotherDocument: ids "a", "a\x00" and "ab" share a
// bucket and each sweep removes only its own keys.
func TestSweepNeverDeletesAnotherDocument(t *testing.T) {
	kv := kvfault.New()
	ids := []string{"a", "a\x00", "ab"}
	for _, id := range ids {
		u35Seed(t, kv, u35Doc(t, id))
	}
	doomed := u35Doc(t, "a")
	pos := cluster.Position{Epoch: 1, Seq: 2}
	u35Delete(t, kv, doomed, pos)
	progress, err := u35Sweeper(kv).SweepBatch(ReclaimRecord{Doc: doomed, Pos: pos}, 1000)
	if err != nil || !progress.Done {
		t.Fatalf("sweep = %+v, %v", progress, err)
	}
	for _, id := range ids[1:] {
		if u35State(t, kv, u35Doc(t, id)) != HeaderLive {
			t.Fatalf("document %q lost its header", id)
		}
	}
	if got := len(u35Dump(t, kv)); got != 2*(1+1+3) {
		t.Fatalf("%d keys left, want the two neighbours' 10", got)
	}
}

// TestReclaimKeyRoundTrip: the 'd' key parses from the end and rejects damage.
func TestReclaimKeyRoundTrip(t *testing.T) {
	record := ReclaimRecord{Doc: u35Doc(t, "a\x00b"), Pos: cluster.Position{Epoch: 3, Seq: 77}}
	decoded, err := DecodeReclaimKey(record.Key())
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if decoded.Pos != record.Pos || decoded.Doc.Ref != record.Doc.Ref || !bytes.Equal(decoded.Doc.EncID, record.Doc.EncID) {
		t.Fatalf("round trip = %+v, want %+v", decoded, record)
	}
	key := record.Key()
	if want := storage.NodeKey(storage.NSReclaim, []byte{'d'}); !bytes.HasPrefix(key, want) {
		t.Fatalf("key %x does not start with %x", key, want)
	}
	for name, bad := range map[string][]byte{
		"empty":        nil,
		"short":        key[:20],
		"wrong kind":   append([]byte{}, append(key[:11], append([]byte{'t'}, key[12:]...)...)...),
		"wrong prefix": append([]byte{0x00}, key[1:]...),
		"no encID":     append(append([]byte{}, key[:12+18]...), key[len(key)-16:]...),
	} {
		if _, err := DecodeReclaimKey(bad); err == nil {
			t.Fatalf("%s decoded", name)
		}
	}
}

// TestReclaimKeyDecoderSurvivesMutation: 100k mutations, no panic, bounded
// allocation, and an accepted key re-encodes identically.
func TestReclaimKeyDecoderSurvivesMutation(t *testing.T) {
	seed := ReclaimRecord{Doc: u35Doc(t, "abc"), Pos: cluster.Position{Epoch: 2, Seq: 5}}.Key()
	rng := rand.New(rand.NewSource(35))
	accepted := 0
	for i := 0; i < 100_000; i++ {
		mutated := append([]byte(nil), seed...)
		switch rng.Intn(4) {
		case 0:
			mutated[rng.Intn(len(mutated))] = byte(rng.Intn(256))
		case 1:
			mutated = mutated[:rng.Intn(len(mutated)+1)]
		case 2:
			mutated = append(mutated, byte(rng.Intn(256)))
		default:
			mutated[rng.Intn(len(mutated))] ^= 1 << uint(rng.Intn(8))
			mutated[rng.Intn(len(mutated))] = byte(rng.Intn(256))
		}
		record, err := DecodeReclaimKey(mutated)
		if err != nil {
			continue
		}
		accepted++
		if again := record.Key(); !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-encodes as %x", mutated, again)
		}
	}
	// A decoder that rejects everything would pass the loop above.
	if accepted < 10_000 {
		t.Fatalf("only %d of 100000 mutations were accepted; the scope and position bytes are free", accepted)
	}
}

// TestReclaimKeyDecoderAllocationIsBoundedByTheKey: a huge garbage id is refused
// before anything is copied, and a good key costs a few small allocations.
func TestReclaimKeyDecoderAllocationIsBoundedByTheKey(t *testing.T) {
	good := ReclaimRecord{Doc: u35Doc(t, "abc"), Pos: cluster.Position{Epoch: 2, Seq: 5}}.Key()
	huge := append(append([]byte(nil), good[:12+18]...), bytes.Repeat([]byte{0x01}, 8<<20)...)
	huge = append(huge, good[len(good)-16:]...)
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := DecodeReclaimKey(huge); err == nil {
			t.Fatal("an 8 MiB id decoded")
		}
	}); allocs > 8 {
		t.Fatalf("refusing an 8 MiB id took %v allocations", allocs)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 100; i++ {
		if _, err := DecodeReclaimKey(huge); err == nil {
			t.Fatal("an 8 MiB id decoded")
		}
	}
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 1<<20 {
		t.Fatalf("100 refusals of an 8 MiB id allocated %d bytes", grown)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := DecodeReclaimKey(good); err != nil {
			t.Fatal(err)
		}
	}); allocs > 8 {
		t.Fatalf("decoding a good key took %v allocations", allocs)
	}
}

// TestReclaimKeyIDRules: an id of exactly 1024 bytes is accepted, 1025 refused,
// and two components are refused as one id.
func TestReclaimKeyIDRules(t *testing.T) {
	sized := func(n int) []byte {
		t.Helper()
		overhead := len(u35Doc(t, "").EncID)
		enc := u35Doc(t, strings.Repeat("x", n-overhead)).EncID
		if len(enc) != n {
			t.Fatalf("built an id of %d bytes, want %d", len(enc), n)
		}
		return enc
	}
	build := func(enc []byte) []byte {
		return ReclaimRecord{Doc: DocumentKey{Ref: u35Ref(), EncID: enc}, Pos: cluster.Position{Epoch: 1, Seq: 1}}.Key()
	}
	if _, err := DecodeReclaimKey(build(sized(1024))); err != nil {
		t.Fatalf("an id of 1024 bytes: %v", err)
	}
	if _, err := DecodeReclaimKey(build(sized(1025))); err == nil {
		t.Fatal("an id of 1025 bytes decoded")
	}
	two := append(u35Doc(t, "a").EncID, u35Doc(t, "b").EncID...)
	if _, err := DecodeReclaimKey(build(two)); err == nil {
		t.Fatal("two components decoded as one id")
	}
}

// TestReclaimKeyDecodeWrapsTheComponentCause: the refusal of a bad id keeps the
// cause for errors.Is and errors.As.
func TestReclaimKeyDecodeWrapsTheComponentCause(t *testing.T) {
	key := ReclaimRecord{Doc: DocumentKey{Ref: u35Ref(), EncID: []byte{0xEE, 0xEE}}, Pos: cluster.Position{Epoch: 1, Seq: 1}}.Key()
	_, err := DecodeReclaimKey(key)
	if err == nil {
		t.Fatal("an unknown component tag decoded")
	}
	if errors.Unwrap(err) == nil {
		t.Fatalf("the refusal %q keeps no cause", err)
	}
}

// TestSweepOfBigKeysConvergesInSeveralBatches: a deleted document whose keys
// together exceed one batch (an element key may be 65000 bytes) is swept in
// several batches, the header and record in the last one, and never sticks.
func TestSweepOfBigKeysConvergesInSeveralBatches(t *testing.T) {
	kv := kvfault.New()
	kv.SetMaxBatchBytes(200_000)
	doc := u35Doc(t, "a")
	prefix := store.StorePrefix(doc.Ref.Slot, doc.Ref.DatabaseID, doc.Ref.BucketID, doc.EncID, u35FifoID)
	u35Commit(t, kv, func(_ storage.KVSnapshot, batch storage.KVBatch) error {
		return store.WriteInsert(batch, u35Insert(doc).Insert)
	})
	for i := byte(1); i <= 4; i++ {
		key := append(append([]byte(nil), prefix...), 0x01, i)
		key = append(key, bytes.Repeat([]byte{i}, storage.MaxKeyBytes-len(key))...)
		u35Commit(t, kv, func(_ storage.KVSnapshot, batch storage.KVBatch) error {
			return batch.Set(key, []byte{i})
		})
	}
	pos := cluster.Position{Epoch: 1, Seq: 2}
	u35Delete(t, kv, doc, pos)

	sweeper := u35Sweeper(kv)
	record := ReclaimRecord{Doc: doc, Pos: pos}
	batches := 0
	for {
		progress, err := sweeper.SweepBatch(record, 1000)
		if err != nil {
			t.Fatalf("batch %d: %v", batches, err)
		}
		batches++
		if progress.Done {
			break
		}
		if batches > 10 {
			t.Fatal("the sweep does not converge")
		}
		if state := u35State(t, kv, doc); state != HeaderTombstone {
			t.Fatalf("batch %d removed the header before the last batch", batches)
		}
	}
	if batches < 2 {
		t.Fatalf("%d batch for keys that cannot share one", batches)
	}
	if left := u35Dump(t, kv); len(left) != 0 {
		t.Fatalf("%d keys left after the sweep", len(left))
	}
}

// u35Elements are the three store keys a u35 document carries.
func u35Elements(doc DocumentKey) [][]byte {
	prefix := store.StorePrefix(doc.Ref.Slot, doc.Ref.DatabaseID, doc.Ref.BucketID, doc.EncID, u35FifoID)
	var keys [][]byte
	for i := byte(1); i <= 3; i++ {
		keys = append(keys, append(append([]byte(nil), prefix...), 0x01, i))
	}
	return keys
}

// u35WholeDocument reports the first missing key of a live document, or "".
func u35WholeDocument(t *testing.T, kv storage.KV, doc DocumentKey) string {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	wanted := append(u35Elements(doc), store.FieldKey(doc.header(), u35FieldAge))
	for _, key := range wanted {
		if _, err := snap.Get(key); err != nil {
			return fmt.Sprintf("%x: %v", key, err)
		}
	}
	return ""
}

// TestConcurrentSweepAndReinsertNeverRemoveALiveKey is conv §11 rule 3: the
// sweep's pair lock and header re-read mean a re-inserted document keeps all its
// keys however deletes, re-inserts and sweeps of its record interleave, and a
// neighbouring id is never touched.
func TestConcurrentSweepAndReinsertNeverRemoveALiveKey(t *testing.T) {
	kv := kvfault.New()
	pairs := &cluster.PairSet{}
	sweeper := NewSweeper(kv, pairs)
	doc := u35Doc(t, "a")
	neighbours := []DocumentKey{u35Doc(t, "ab"), u35Doc(t, "a\x00")}
	for _, neighbour := range neighbours {
		u35Seed(t, kv, neighbour)
	}

	const rounds = 200
	var seq atomic.Uint64
	var wg sync.WaitGroup
	failures := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case failures <- fmt.Sprintf(format, args...):
		default:
		}
	}

	// The owner: under the pair lock, delete a live document or re-insert a
	// deleted one together with its three elements, as the apply would.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			pairs.With(u35Slot, u35DB, func(*cluster.Pair) {
				snap := kv.NewSnapshot()
				defer snap.Discard()
				batch := kv.NewBatch()
				defer batch.Discard()
				state, err := ReadHeaderState(snap, doc)
				if err != nil {
					fail("reading the header: %v", err)
					return
				}
				switch state {
				case HeaderLive:
					if missing := u35WholeDocument(t, kv, doc); missing != "" {
						fail("a live document lost a key: %s", missing)
					}
					err = ApplyDelete(snap, batch, doc, cluster.Position{Epoch: 1, Seq: seq.Add(1)})
				default:
					if _, err = WriteInsertPath(snap, batch, u35Insert(doc)); err == nil {
						for _, key := range u35Elements(doc) {
							if err = batch.Set(key, []byte{1}); err != nil {
								break
							}
						}
					}
				}
				if err == nil {
					err = batch.Commit(false)
				}
				if err != nil {
					fail("applying: %v", err)
				}
			})
		}
	}()

	// Sweepers of the document's records, some of them long stale.
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < rounds*2; i++ {
				rec := ReclaimRecord{Doc: doc, Pos: cluster.Position{Epoch: 1, Seq: uint64(worker*1000 + i)}}
				if _, err := sweeper.SweepBatch(rec, 1+worker); err != nil {
					fail("sweeping: %v", err)
				}
			}
		}(worker)
	}
	wg.Wait()
	close(failures)
	for message := range failures {
		t.Error(message)
	}

	if u35State(t, kv, doc) == HeaderLive {
		if missing := u35WholeDocument(t, kv, doc); missing != "" {
			t.Fatalf("the final live document lost a key: %s", missing)
		}
	}
	for _, neighbour := range neighbours {
		if u35State(t, kv, neighbour) != HeaderLive || u35WholeDocument(t, kv, neighbour) != "" {
			t.Fatalf("neighbour %x was touched", neighbour.EncID)
		}
	}
}
