package wire

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u49Coordinator is a Coordinator over the fixture's Writer.
func u49Coordinator(fx *fixture, mint func() (string, error)) *Coordinator {
	return NewCoordinator(CoordinatorConfig{Sender: fx.w, Mint: mint})
}

// u49Request is an insert of one Name field into the fixture's bucket.
func u49Request(id string) InsertRequest {
	template := insertOp("template")
	return InsertRequest{
		Database: "app", Bucket: "users", DBID: testDB, ID: id,
		Template:  cluster.Op{BucketID: testBucket, DeclVersion: testSeen, SchemaVersionSeen: testSeen, Fields: template.Fields},
		RequestID: [16]byte{9, 9, 9},
	}
}

// u49RealDedupe gives the fixture's writer the real dedupe reader over its Badger, so
// the entry a retry finds is the one the first apply wrote in its own batch.
func u49RealDedupe(fx *fixture) {
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, pairRecorder{}),
		Clock:   fixedClock{}, Lease: fx.lease, Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl,
		WCTimeout: time.Second, DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
}

func u49SlotOf(id string) uint16 {
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		panic(err)
	}
	return storage.SlotOf(storage.PlacementKey("app", "users", encoded))
}

// TestMintedInsertRetriesToTheSameID: the client sent no _id_, so the coordinator derives
// it from the request id. The first answer is lost; a retry with the same request id
// derives the same id, hits the same slot, is deduped by the entry the apply recorded,
// and returns the same id with one document and one log record.
func TestMintedInsertRetriesToTheSameID(t *testing.T) {
	fx := newFixture(t)
	req := u49Request("")
	id := cluster.DeriveID(req.RequestID, req.OpIndex)
	slot := u49SlotOf(id)
	fx.markReady(slot)
	u49RealDedupe(fx)
	coordinator := u49Coordinator(fx, func() (string, error) { return "", errors.New("a request id derives the id; nothing may mint") })

	first, err := coordinator.Insert(context.Background(), req)
	if err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	if first.ID != id || !first.Minted || first.Deduped {
		t.Fatalf("first = %+v, want derived id %q", first, id)
	}
	// The answer is lost; the client retries with the same request id.
	retry, err := coordinator.Insert(context.Background(), req)
	if err != nil {
		t.Fatalf("retry Insert: %v", err)
	}
	if retry.ID != id || !retry.Deduped || retry.Pos != first.Pos {
		t.Fatalf("retry = %+v, want id %q deduped at %v", retry, id, first.Pos)
	}
	if !fx.hasDocument(slot, id) {
		t.Fatal("the document is missing")
	}
	if got := len(fx.logged()); got != 1 {
		t.Fatalf("log holds %d records, want 1", got)
	}
}

// TestAClientMintedInsertRetriesToTheSameID: the client mints with cluster.MintID before
// its first send; the retry carries that id and is deduped the same way.
func TestAClientMintedInsertRetriesToTheSameID(t *testing.T) {
	fx := newFixture(t)
	id, err := cluster.MintID()
	if err != nil {
		t.Fatalf("MintID: %v", err)
	}
	slot := u49SlotOf(id)
	fx.markReady(slot)
	req := u49Request(id)
	u49RealDedupe(fx)
	coordinator := u49Coordinator(fx, func() (string, error) { return "", errors.New("the client already minted") })
	first, err := coordinator.Insert(context.Background(), req)
	if err != nil || first.ID != id || first.Minted {
		t.Fatalf("first = %+v, %v", first, err)
	}
	retry, err := coordinator.Insert(context.Background(), req)
	if err != nil || retry.ID != id || !retry.Deduped || retry.Pos != first.Pos || len(fx.logged()) != 1 {
		t.Fatalf("retry = %+v, %v, log %d", retry, err, len(fx.logged()))
	}
}

// TestADerivedIDIsAUUIDv7PerRequestAndOp: same (request id, op index) gives the same
// id, different op indexes give different ids, and every id is a canonical UUIDv7.
func TestADerivedIDIsAUUIDv7PerRequestAndOp(t *testing.T) {
	request := [16]byte{1, 2, 3, 4, 5, 6, 0xff, 8, 0xff, 10, 11, 12, 13, 14, 15, 16}
	seen := map[string]uint8{}
	for op := range 255 {
		id := cluster.DeriveID(request, uint8(op))
		if again := cluster.DeriveID(request, uint8(op)); again != id {
			t.Fatalf("op %d derived %q then %q", op, id, again)
		}
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.Version() != 7 || parsed.Variant() != uuid.RFC4122 || len(id) != 36 {
			t.Fatalf("id %q is not a canonical UUIDv7 (err %v)", id, err)
		}
		if other, dup := seen[id]; dup {
			t.Fatalf("ops %d and %d derived %q", other, op, id)
		}
		seen[id] = uint8(op)
	}
	if other := cluster.DeriveID([16]byte{9}, 0); other == cluster.DeriveID(request, 0) {
		t.Fatal("two request ids derived one id")
	}
}

// TestACoordinatorMintsOnlyWhenTheClientDidNot: a bare wire request (no id, no request
// id) is minted at the coordinator before the slot is computed, the id comes back in the answer, and the
// logged record carries it. A retry of that insert mints again (the one non-dedupable
// case, G10), so two documents exist when no request id dedupes it.
func TestACoordinatorMintsOnlyWhenTheClientDidNot(t *testing.T) {
	fx := newFixture(t)
	minted := []string{"mint-one-id", "mint-two-id"}
	calls := 0
	coordinator := u49Coordinator(fx, func() (string, error) {
		id := minted[calls]
		calls++
		return id, nil
	})
	fx.markReady(u49SlotOf(minted[0]))
	fx.markReady(u49SlotOf(minted[1]))

	bare := u49Request("")
	bare.RequestID = [16]byte{}
	first, err := coordinator.Insert(context.Background(), bare)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if first.ID != minted[0] || !first.Minted || calls != 1 {
		t.Fatalf("first = %+v after %d mints, want id %q minted once", first, calls, minted[0])
	}
	if !fx.hasDocument(u49SlotOf(minted[0]), minted[0]) {
		t.Fatal("the minted document is missing")
	}
	if _, err := coordinator.Insert(context.Background(), bare); err != nil {
		t.Fatalf("second Insert: %v", err)
	}
	if calls != 2 {
		t.Fatalf("mints = %d, want 2: a retry of an id-less insert mints again", calls)
	}
}

// TestMintedIDIsNeverMintedByTheOwner: handed an Op with no id, the owner path refuses and
// writes nothing, and no non-test file of this package other than the coordinator can mint.
func TestMintedIDIsNeverMintedByTheOwner(t *testing.T) {
	fx := newFixture(t)
	req := fx.request(testSlot, "x")
	req.Op.EncID, req.Op.IDValue = nil, nil
	if _, err := fx.w.Write(context.Background(), req); err == nil {
		t.Fatal("an insert with no id was ordered; the owner must refuse it, never mint")
	}
	if got := len(fx.logged()); got != 0 {
		t.Fatalf("log holds %d records, want 0", got)
	}

	applier := NewDocumentApplier(fx.kv, pairRecorder{})
	idless := insertOp("x")
	idless.EncID, idless.IDValue = nil, nil
	if code, err := applier.Precondition(testSlot, testDB, idless); err == nil && code == 0 {
		t.Fatal("Precondition accepted an Op with no id")
	}
	batch := fx.kv.NewBatch()
	defer batch.Discard()
	if err := applier.Fill(batch, AppliedRecord{Slot: testSlot, DB: testDB, Op: idless, Pos: cluster.Position{Epoch: testEpoch, Seq: 1}}); err == nil {
		t.Fatal("Fill applied an Op with no id instead of refusing it")
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package: %v", err)
	}
	for _, name := range files {
		// pepper.go generates the cluster password key once, at bootstrap, outside any apply path.
		if strings.HasSuffix(name, "_test.go") || name == "coordinator.go" || name == "pepper.go" {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for _, banned := range []string{"NewDocumentID", "MintID", "DeriveID", "uuid.", "rand."} {
			if strings.Contains(string(source), banned) {
				t.Fatalf("%s mentions %q: applying a logged write must never generate a value", name, banned)
			}
		}
	}
}

// TestAnEncodedIDIsKeptNotMinted: a template that already carries an encoded int or date
// id is used as given and nothing mints.
func TestAnEncodedIDIsKeptNotMinted(t *testing.T) {
	cases := map[string]struct {
		key     any
		idValue []byte
	}{
		"int":  {int64(42), []byte{0x12, 42, 0, 0, 0, 0, 0, 0, 0}},
		"date": {time.UnixMilli(1_700_000_000_000).UTC(), []byte{0x09, 0, 0, 0, 0, 0, 0, 0, 0}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t)
			encoded, err := store.EncodeComponent(tc.key)
			if err != nil {
				t.Fatalf("EncodeComponent: %v", err)
			}
			slot := storage.SlotOf(storage.PlacementKey("app", "users", encoded))
			fx.markReady(slot)
			coordinator := u49Coordinator(fx, func() (string, error) { return "", errors.New("must not mint") })
			req := u49Request("")
			req.Template.EncID, req.Template.IDValue = encoded, tc.idValue
			answer, err := coordinator.Insert(context.Background(), req)
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if answer.Minted || answer.ID != "" || !bytes.Equal(answer.EncID, encoded) {
				t.Fatalf("answer = %+v, want the given encoded id and no mint", answer)
			}
			snapshot := fx.kv.NewSnapshot()
			defer snapshot.Discard()
			if _, err := snapshot.Get(store.DocumentPrefix(slot, testDB, testBucket, encoded)); err != nil {
				t.Fatalf("document missing: %v", err)
			}
		})
	}
}

// TestTheStringValueEnvelope pins the bytes of a type-tagged string.
func TestTheStringValueEnvelope(t *testing.T) {
	want := []byte{0x02, 0x04, 0x00, 0x00, 0x00, 'a', 'd', 'a', 0x00}
	if got := stringValue("ada"); !bytes.Equal(got, want) {
		t.Fatalf("stringValue(ada) = % x, want % x", got, want)
	}
}

// TestMintFailureIsRefusedNotGuessed: a failing mint is MINT_FAILED with the reason, and
// nothing is written or logged.
func TestMintFailureIsRefusedNotGuessed(t *testing.T) {
	fx := newFixture(t)
	coordinator := u49Coordinator(fx, func() (string, error) { return "", errors.New("clock went backwards") })
	bare := u49Request("")
	bare.RequestID = [16]byte{}
	_, err := coordinator.Insert(context.Background(), bare)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeMintFailed {
		t.Fatalf("err = %v, want MINT_FAILED", err)
	}
	if !strings.Contains(refusal.Message, "clock went backwards") || !strings.Contains(refusal.Message, "Nothing was written") {
		t.Fatalf("message = %q, want the reason and the fix", refusal.Message)
	}
	if got := len(fx.logged()); got != 0 {
		t.Fatalf("log holds %d records, want 0", got)
	}
}

// TestAClientIDInsertTwiceIsDocumentExists: with no dedupe entry, resending a
// client-id insert is DOCUMENT_EXISTS, the signal that it landed.
func TestAClientIDInsertTwiceIsDocumentExists(t *testing.T) {
	fx := newFixture(t)
	fx.markReady(u49SlotOf("fixed"))
	coordinator := u49Coordinator(fx, nil)
	if _, err := coordinator.Insert(context.Background(), u49Request("fixed")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	req := u49Request("fixed")
	req.RequestID = [16]byte{8}
	_, err := coordinator.Insert(context.Background(), req)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeDocumentExists {
		t.Fatalf("err = %v, want DOCUMENT_EXISTS", err)
	}
}
