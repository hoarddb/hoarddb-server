package wire

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// expiryReadWorld is a one-owner cluster whose bucket expires at field 3 (declared Age, the
// third field of the fixture's declaration; the reads only look at the stored scalar).
type expiryReadWorld struct {
	*u36World
	now     int64
	reader  *Reader
	scanner *SlotScanner
}

func newExpiryReadWorld(t *testing.T, now int64) *expiryReadWorld {
	t.Helper()
	world := &expiryReadWorld{u36World: newU36World(t), now: now}
	world.fx.schema.decl.Expire = machine.ExpireTable{BucketField: 3}
	clock := func() int64 { return world.now }
	world.reader = NewReader(ReaderConfig{Gate: world.gate, KV: world.fx.kv, Holder: world.barrier, PeerReachable: func(uint16, uint64) bool { return true }, Forward: world.fwd, Self: "n1", WallMillis: clock})
	world.scanner = NewSlotScanner(SlotScannerConfig{KV: world.fx.kv, Topology: world.fx.topo, Schema: world.fx.schema, Gate: world.gate, WallMillis: clock})
	return world
}

// put stores a live document p<id> whose expire field holds expiresAt, or no field when 0.
func (w *expiryReadWorld) put(id string, expiresAt int64) {
	w.t.Helper()
	prefix := store.DocumentPrefix(w.slot, testDB, testBucket, w.encID(id))
	batch := w.fx.kv.NewBatch()
	idValue := append([]byte{0x02, byte(len(id) + 1), 0, 0, 0}, append([]byte(id), 0)...)
	if err := batch.Set(prefix, idValue); err != nil {
		w.t.Fatal(err)
	}
	if expiresAt != 0 {
		if err := batch.Set(store.FieldKey(prefix, 3), datetimeScalar(expiresAt)); err != nil {
			w.t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		w.t.Fatal(err)
	}
}

func (w *expiryReadWorld) listed(project []uint16) []string {
	w.t.Helper()
	var mask cluster.SlotMask
	mask.Set(w.slot)
	reply, err := w.scanner.List(context.Background(), cluster.SlotListRequest{Epoch: testEpoch, DBID: testDB, BucketID: testBucket, Mask: mask, Want: 50, MaxBytes: 1 << 20, Fields: project})
	if err != nil {
		w.t.Fatalf("List: %v", err)
	}
	var ids []string
	for _, entry := range reply.Entries {
		ids = append(ids, string(entry.EncID))
	}
	return ids
}

func (w *expiryReadWorld) counted() int64 {
	w.t.Helper()
	var mask cluster.SlotMask
	mask.Set(w.slot)
	count, err := w.scanner.Count(context.Background(), cluster.SlotCountRequest{Epoch: testEpoch, DBID: testDB, BucketID: testBucket, Mask: mask})
	if err != nil {
		w.t.Fatalf("Count: %v", err)
	}
	return count
}

func TestExpiredAtBoundary(t *testing.T) {
	stored := datetimeScalar(1700000000000)
	for _, tc := range []struct {
		name string
		now  int64
		want bool
	}{{"before", 1699999999999, false}, {"equal", 1700000000000, true}, {"after", 1700000000001, true}} {
		if got := expiredAt(stored, tc.now); got != tc.want {
			t.Errorf("%s: expiredAt(…, %d) = %v, want %v", tc.name, tc.now, got, tc.want)
		}
	}
	if expiredAt(nil, 1<<62) {
		t.Error("an absent expire field expired")
	}
}

func TestExpiredDocumentIsAbsentToGetListAndCountBeforeTheSweep(t *testing.T) {
	world := newExpiryReadWorld(t, 2_000)
	world.put("gone", 1_000)
	world.put("edge", 2_000)
	world.put("live", 3_000)
	world.put("forever", 0)
	if _, err := world.reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("gone")); err == nil {
		t.Fatal("get of an expired document found it")
	}
	if _, err := world.reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("live")); err != nil {
		t.Fatalf("get of a live document: %v", err)
	}
	if got, want := world.counted(), int64(2); got != want {
		t.Fatalf("count = %d, want %d (live and forever)", got, want)
	}
	for _, project := range [][]uint16{nil, {2}} {
		if got := world.listed(project); len(got) != 2 {
			t.Fatalf("list with projection %v returned %d documents, want 2: an expired one is visible until the sweep", project, len(got))
		}
	}
}

func TestGetOfAnExpiredDocumentSaysExpired(t *testing.T) {
	world := newExpiryReadWorld(t, 2_000)
	world.put("gone", 1_000)
	_, err := world.reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("gone"))
	u36Refusal(t, err, cluster.CodeNotFound)
	if !strings.Contains(err.Error(), "expired") || !strings.Contains(err.Error(), "is not in") {
		t.Fatalf("message %q must be the ordinary miss text and say the document expired", err.Error())
	}
	_, err = world.reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("absent"))
	u36Refusal(t, err, cluster.CodeNotFound)
	if strings.Contains(err.Error(), "expired") {
		t.Fatalf("a plain miss %q says expired", err.Error())
	}
}

func TestDocumentWithoutTheExpireFieldNeverExpires(t *testing.T) {
	world := newExpiryReadWorld(t, 1<<62)
	world.put("forever", 0)
	if _, err := world.reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("forever")); err != nil {
		t.Fatalf("a document without the field expired: %v", err)
	}
	if world.counted() != 1 {
		t.Fatal("count dropped a document without the expire field")
	}
}

// TestAnExpiredGetCarriesTheExpiredDetail: a miss on an expired document says so in the structured
// detail of its NOT_FOUND, which a driver reads without parsing the message, and the detail survives
// the wire.
func TestAnExpiredGetCarriesTheExpiredDetail(t *testing.T) {
	world := newExpiryReadWorld(t, 2_000)
	world.put("gone", 1_000)
	_, err := world.reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("gone"))
	refusal := ringRefusalOf(err)
	if refusal.Code != cluster.CodeNotFound || refusal.Detail == nil || !refusal.Detail.Expired {
		t.Fatalf("refusal = %+v, want NOT_FOUND with detail expired", refusal)
	}
	raw, encodeErr := EncodeRingError(refusal)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	back, decodeErr := DecodeRingError(raw)
	if decodeErr != nil || back.Detail == nil || !back.Detail.Expired {
		t.Fatalf("after the wire: %+v, %v", back, decodeErr)
	}
	_, err = world.reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("absent"))
	if plain := ringRefusalOf(err); plain.Detail != nil {
		t.Fatalf("a plain miss carries detail %+v", plain.Detail)
	}
}

// TestAnExpiredFalseDetailIsASecondSpellingOfNoDetail: a NOT_FOUND either carries {expired: true} or no
// detail; {expired: false} would be a second encoding of a plain miss, so the encoder refuses it and the
// decoder does not accept it.
func TestAnExpiredFalseDetailIsASecondSpellingOfNoDetail(t *testing.T) {
	if _, err := EncodeRingError(RingError{Code: cluster.CodeNotFound, Message: "m", Detail: &cluster.Detail{}}); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("EncodeRingError of NOT_FOUND with detail {expired: false} = %v, want a refusal naming expired", err)
	}
	raw, err := bson.Marshal(bson.D{{Key: "code", Value: int32(cluster.CodeNotFound)}, {Key: "message", Value: "m"},
		{Key: "detail", Value: bson.D{{Key: "expired", Value: false}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRingError(raw); err == nil {
		t.Fatal("DecodeRingError accepted a NOT_FOUND detail {expired: false}")
	}
	good, err := EncodeRingError(RingError{Code: cluster.CodeNotFound, Message: "m", Detail: &cluster.Detail{Expired: true}})
	if err != nil {
		t.Fatal(err)
	}
	if back, err := DecodeRingError(good); err != nil || back.Detail == nil || !back.Detail.Expired {
		t.Fatalf("{expired: true} did not round trip: %+v, %v", back, err)
	}
}
