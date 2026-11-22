package wire

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u39Real is a create over the real Writer, real Badger and the real KVDedupeReader. The
// in-memory ring of createb_test.go has no dedupe, which is how a zero request id went
// unseen: the owner's dedupe key is (request id, op index) inside one (slot, db, bucket).
type u39Real struct {
	t   *testing.T
	rig *u31Rig

	mu     sync.Mutex
	writes int
	sent   []WriteRequest
	// beforeWrite, when set, runs ahead of each write with its 0-based number.
	beforeWrite func(n int, req WriteRequest)
	// crash, when set, is asked before each write; die means the process dies at that
	// write, applyFirst that the owner applied it first (a lost answer).
	crash      func(n int, req WriteRequest) (die, applyFirst bool)
	crashError error
	// published is the schema_version creates run at; 0 means u39Version.
	published int64
}

func newU39Real(t *testing.T) *u39Real {
	t.Helper()
	rig := newU31Rig(t, 8192)
	rig.schema.decl = machine.SystemDeclaration()
	rig.schema.seen, rig.schema.published = u39Version, u39Version
	for slot := 0; slot < 8192; slot++ {
		rig.markReady(uint16(slot))
	}
	return &u39Real{t: t, rig: rig, crashError: errors.New("the process died")}
}

// Write is the Sender: the real Writer. It also holds the order invariant of conv 21: a
// declaration is never written for an id whose reverse index does not exist.
func (r *u39Real) Write(ctx context.Context, req WriteRequest) (WriteAnswer, error) {
	r.mu.Lock()
	n := r.writes
	r.writes++
	r.sent = append(r.sent, req)
	before, crash := r.beforeWrite, r.crash
	r.mu.Unlock()
	if family, err := store.BucketsFamilyOf(req.Op.EncID); err == nil && family == store.BucketsDeclaration {
		r.checkClaimed(req)
	}
	if before != nil {
		before(n, req)
	}
	if crash != nil {
		if die, applyFirst := crash(n, req); die {
			if applyFirst {
				if _, err := r.rig.w.Write(ctx, req); err != nil {
					r.t.Errorf("the write before the crash: %v", err)
				}
			}
			return WriteAnswer{}, r.crashError
		}
	}
	return r.rig.w.Write(ctx, req)
}

// checkClaimed fails the test when a declaration insert names an id with no reverse index.
func (r *u39Real) checkClaimed(req WriteRequest) {
	for _, field := range req.Op.Fields {
		if field.FieldID != 2 {
			continue
		}
		raw := field.Value[len(field.Value)-8:]
		var id uint64
		for _, b := range raw {
			id = id<<8 | uint64(b)
		}
		if _, claimed := r.reverse(id); !claimed {
			r.t.Errorf("a declaration was written for id %x before its reverse index", id)
		}
	}
}

// document reads one _buckets document's stored fields from Badger, and whether it exists.
func (r *u39Real) document(encID []byte) (map[uint16][]byte, bool) {
	r.t.Helper()
	prefix := store.DocumentPrefix(u39Slot(encID), testDB, store.BucketsBucketID, encID)
	snapshot := r.rig.kv.NewSnapshot()
	defer snapshot.Discard()
	header, err := snapshot.Get(prefix)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, false
	} else if err != nil {
		r.t.Errorf("reading a _buckets header: %v", err)
		return nil, false
	}
	if bytes.Equal(header, tombstoneHeader) {
		return nil, false
	}
	stored := map[uint16][]byte{}
	for fieldID := uint16(2); fieldID <= 8; fieldID++ {
		raw, err := snapshot.Get(store.FieldKey(prefix, fieldID))
		if errors.Is(err, storage.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			r.t.Errorf("reading field %d: %v", fieldID, err)
			continue
		}
		stored[fieldID] = raw
	}
	return stored, true
}

// Read is the DeclarationSource: the declaration of name, straight from Badger.
func (r *u39Real) Read(_ context.Context, _ uint64, name string) (machine.Declaration, []byte, bool, error) {
	encID, err := store.EncodeComponent(name)
	if err != nil {
		return machine.Declaration{}, nil, false, err
	}
	stored, found := r.document(encID)
	if !found {
		return machine.Declaration{}, nil, false, nil
	}
	decl, err := store.DecodeDeclaration(stored)
	return decl, nil, found, err
}

// reverse is the name stored under id.
func (r *u39Real) reverse(bucketID uint64) (string, bool) {
	encID, err := store.EncodeComponent(u39IDBytes(bucketID))
	if err != nil {
		r.t.Errorf("encoding id: %v", err)
		return "", false
	}
	stored, found := r.document(encID)
	if !found {
		return "", false
	}
	return u39String(stored[7]), true
}

func (r *u39Real) declaration(name string) (machine.Declaration, bool) {
	decl, _, found, err := r.Read(context.Background(), testDB, name)
	if err != nil {
		r.t.Errorf("reading declaration %q: %v", name, err)
	}
	return decl, found
}

// u39Draws returns a DrawID that yields ids in order and is safe to call from one create.
func u39Draws(ids ...uint64) func() (uint64, error) {
	next := 0
	return func() (uint64, error) {
		if next >= len(ids) {
			return 0, errors.New("the test ran out of ids")
		}
		next++
		return ids[next-1], nil
	}
}

func (r *u39Real) createWith(name string, fields []FieldEntry, draw func() (uint64, error)) (uint64, error) {
	return NewCreateBucket(CreateBucketConfig{
		Sender: r, Declarations: r, Topology: u39Topology{},
		DatabaseName: func(uint64) (string, bool) { return u39DBName, true },
		Published: func(uint64) int64 {
			if r.published != 0 {
				return r.published
			}
			return u39Version
		},
		DrawID: draw,
	}).Run(context.Background(), testDB, name, fields)
}

func (r *u39Real) create(name string, ids ...uint64) (uint64, error) {
	return r.createWith(name, u39Fields(), u39Draws(ids...))
}

func u39ReverseSlot(id uint64) uint16 {
	encID, err := store.EncodeComponent(u39IDBytes(id))
	if err != nil {
		panic(err)
	}
	return u39Slot(encID)
}

func u39DeclarationSlot(name string) uint16 {
	encID, err := store.EncodeComponent(name)
	if err != nil {
		panic(err)
	}
	return u39Slot(encID)
}

// u39TwoIDsOnOneSlot finds two ids whose reverse documents land on one slot (one pair).
func u39TwoIDsOnOneSlot() (uint64, uint64) {
	seen := map[uint16]uint64{}
	for id := uint64(1); ; id++ {
		slot := u39ReverseSlot(id)
		if other, ok := seen[slot]; ok {
			return other, id
		}
		seen[slot] = id
	}
}

// u39IDOnSlot finds an id whose reverse document lands on slot.
func u39IDOnSlot(slot uint16) uint64 {
	for id := uint64(1); ; id++ {
		if u39ReverseSlot(id) == slot {
			return id
		}
	}
}

// requireOneBucket fails unless name has one declaration whose id's reverse index names it.
func (r *u39Real) requireOneBucket(name string) uint64 {
	r.t.Helper()
	decl, found := r.declaration(name)
	if !found {
		r.t.Fatalf("no declaration for %q", name)
	}
	if owner, claimed := r.reverse(decl.BucketID); !claimed || owner != name {
		r.t.Fatalf("declaration of %q carries %x whose reverse index is %q (found %v)", name, decl.BucketID, owner, claimed)
	}
	return decl.BucketID
}

// TestRealWriterKeepsTwoReverseIndexesOnOnePair: two creates whose reverse documents share
// a pair both land. A zero request id made the owner's dedupe answer the second as a repeat
// of the first, so the create reported success with no reverse index.
func TestRealWriterKeepsTwoReverseIndexesOnOnePair(t *testing.T) {
	first, second := u39TwoIDsOnOneSlot()
	real := newU39Real(t)
	for name, id := range map[string]uint64{"Alpha": first, "Beta": second} {
		if got, err := real.create(name, id); err != nil || got != id {
			t.Fatalf("create %s = %x, %v, want %x", name, got, err, id)
		}
	}
	for name, id := range map[string]uint64{"Alpha": first, "Beta": second} {
		if owner, found := real.reverse(id); !found || owner != name {
			t.Fatalf("reverse index of %x = %q found %v, want %q", id, owner, found, name)
		}
	}
}

// TestRealWriterKeepsAReverseIndexOnTheDeclarationPair: the id's reverse document lands on
// the declaration's own slot (1 in 8192), so both writes share one pair.
func TestRealWriterKeepsAReverseIndexOnTheDeclarationPair(t *testing.T) {
	id := u39IDOnSlot(u39DeclarationSlot("Person"))
	real := newU39Real(t)
	if got, err := real.create("Person", id); err != nil || got != id {
		t.Fatalf("create = %x, %v", got, err)
	}
	if got := real.requireOneBucket("Person"); got != id {
		t.Fatalf("declaration id = %x", got)
	}
}

// TestRealWriterThreeCollisionsCreateNothing: three draws that other buckets own are
// BUCKET_ID_COLLISION, no declaration exists for the name, and the other buckets'
// documents are exactly as they were.
func TestRealWriterThreeCollisionsCreateNothing(t *testing.T) {
	real := newU39Real(t)
	if _, err := real.create("Other", 0x1111); err != nil {
		t.Fatalf("Other: %v", err)
	}
	if _, err := real.create("Third", 0x4444); err != nil {
		t.Fatalf("Third: %v", err)
	}
	before := [][]map[uint16][]byte{}
	snapshot := func() []map[uint16][]byte {
		var docs []map[uint16][]byte
		for _, name := range []string{"Other", "Third"} {
			encID, _ := store.EncodeComponent(name)
			stored, _ := real.document(encID)
			docs = append(docs, stored)
		}
		for _, id := range []uint64{0x1111, 0x4444} {
			encID, _ := store.EncodeComponent(u39IDBytes(id))
			stored, _ := real.document(encID)
			docs = append(docs, stored)
		}
		return docs
	}
	before = append(before, snapshot())
	_, err := real.create("Person", 0x1111, 0x4444, 0x1111)
	_ = u39Code(t, err, cluster.CodeBucketIDCollision)
	if _, found := real.declaration("Person"); found {
		t.Fatal("a declaration exists for a name that claimed no id")
	}
	after := snapshot()
	for n := range after {
		if len(after[n]) != len(before[0][n]) {
			t.Fatalf("document %d changed: %v then %v", n, before[0][n], after[n])
		}
		for fieldID, stored := range before[0][n] {
			if string(after[n][fieldID]) != string(stored) {
				t.Fatalf("document %d field %d changed", n, fieldID)
			}
		}
	}
}

// TestRealWriterLosingTheDeclarationRaceKeepsOneBucket: another create of the same name
// lands its declaration between this create's claim and its declaration insert. The same
// table answers with the winner's id; a different table is BUCKET_EXISTS; either way the
// name has one declaration and the loser deletes its claim (U76).
func TestRealWriterLosingTheDeclarationRaceKeepsOneBucket(t *testing.T) {
	for _, sameTable := range []bool{true, false} {
		real := newU39Real(t)
		raced := false
		real.beforeWrite = func(_ int, req WriteRequest) {
			if family, _ := store.BucketsFamilyOf(req.Op.EncID); family != store.BucketsDeclaration || raced {
				return
			}
			raced = true
			if _, err := real.create("Person", 0xA1); err != nil {
				t.Errorf("the winning create: %v", err)
			}
		}
		fields := u39Fields()
		if !sameTable {
			fields = append(fields, FieldEntry{Type: "string", Name: "City"})
		}
		id, err := real.createWith("Person", fields, u39Draws(0xB2))
		if sameTable {
			if err != nil || id != 0xA1 {
				t.Fatalf("same table: loser = %x, %v, want the winner's a1", id, err)
			}
		} else {
			_ = u39Code(t, err, cluster.CodeBucketExists)
		}
		if got := real.requireOneBucket("Person"); got != 0xA1 {
			t.Fatalf("declaration id = %x, want the winner's a1", got)
		}
		if owner, found := real.reverse(0xB2); found {
			t.Fatalf("the loser's claim = %q, want it reclaimed (U76)", owner)
		}
	}
}

// TestRealWriterTwoNamesDrawingOneIDGetDistinctIDs: two creates of different names draw
// the same id at once. The reverse insert is the authority, so exactly one wins it and the
// other redraws; run under -race with real concurrency.
func TestRealWriterTwoNamesDrawingOneIDGetDistinctIDs(t *testing.T) {
	for round := 0; round < 20; round++ {
		real := newU39Real(t)
		var wait sync.WaitGroup
		var alphaID, betaID uint64
		var alphaErr, betaErr error
		wait.Add(2)
		go func() {
			defer wait.Done()
			alphaID, alphaErr = real.create("Alpha", 0x77, 0x99)
		}()
		go func() {
			defer wait.Done()
			betaID, betaErr = real.create("Beta", 0x77, 0x88)
		}()
		wait.Wait()
		if alphaErr != nil || betaErr != nil {
			t.Fatalf("Alpha %v, Beta %v", alphaErr, betaErr)
		}
		if alphaID == betaID {
			t.Fatalf("both buckets hold id %x", alphaID)
		}
		if real.requireOneBucket("Alpha") != alphaID || real.requireOneBucket("Beta") != betaID {
			t.Fatal("a declaration lost its id")
		}
	}
}

// TestRealWriterTwoCreatesOfOneNameKeepOneDeclaration: two concurrent creates of one name
// with different ids and one table. Exactly one declaration, and both callers get its id.
func TestRealWriterTwoCreatesOfOneNameKeepOneDeclaration(t *testing.T) {
	for round := 0; round < 20; round++ {
		real := newU39Real(t)
		var wait sync.WaitGroup
		ids := make([]uint64, 2)
		errs := make([]error, 2)
		for n := range ids {
			wait.Add(1)
			go func() {
				defer wait.Done()
				ids[n], errs[n] = real.create("Person", uint64(0xA1+n))
			}()
		}
		wait.Wait()
		if errs[0] != nil || errs[1] != nil || ids[0] != ids[1] {
			t.Fatalf("ids %x errs %v", ids, errs)
		}
		if got := real.requireOneBucket("Person"); got != ids[0] {
			t.Fatalf("declaration id %x, callers got %x", got, ids[0])
		}
	}
}

// TestRealWriterDedupesACrashRetryCorrectly: the process dies at each write of a create,
// before or after the owner applied it. A claim that landed and a declaration that landed
// are never applied twice, the retry ends with one bucket whose declaration carries only
// an id with a reverse index, and no id is shared.
func TestRealWriterDedupesACrashRetryCorrectly(t *testing.T) {
	// The id's reverse document shares the declaration's pair, the case a zero request id
	// breaks even on a single create.
	first := u39IDOnSlot(u39DeclarationSlot("Person"))
	for dying := 0; dying < 2; dying++ {
		for _, applyFirst := range []bool{false, true} {
			real := newU39Real(t)
			real.crash = func(n int, _ WriteRequest) (bool, bool) { return n == dying, applyFirst }
			if _, err := real.create("Person", first); !errors.Is(err, real.crashError) {
				t.Fatalf("write %d applyFirst %v: first run = %v, want the crash", dying, applyFirst, err)
			}
			real.crash = nil
			id, err := real.create("Person", 0xBBBB, 0xCCCC)
			if err != nil {
				t.Fatalf("write %d applyFirst %v: retry: %v", dying, applyFirst, err)
			}
			if got := real.requireOneBucket("Person"); got != id {
				t.Fatalf("write %d applyFirst %v: declaration id %x, retry returned %x", dying, applyFirst, got, id)
			}
			if applyFirst && dying == 1 && id != first {
				t.Fatalf("the declaration landed with a lost answer, so the retry must return %x, got %x", first, id)
			}
		}
	}
}

// TestRingWritesCarryDistinctDerivedRequestIDs pins the derivation: nonzero, stable across
// a rerun of the same create, different for every distinct write of one create.
func TestRingWritesCarryDistinctDerivedRequestIDs(t *testing.T) {
	run := func() []WriteRequest {
		real := newU39Real(t)
		if _, err := real.create("Other", 0x1111); err != nil {
			t.Fatalf("Other: %v", err)
		}
		real.sent = nil
		if _, err := real.create("Person", 0x1111, 0x2222); err != nil {
			t.Fatalf("Person: %v", err)
		}
		return real.sent
	}
	first, second := run(), run()
	if len(first) != len(second) || len(first) != 3 {
		t.Fatalf("%d and %d writes, want 3 each", len(first), len(second))
	}
	seen := map[[16]byte]int{}
	for n := range first {
		id := first[n].RequestID
		if id == ([16]byte{}) || first[n].OpIndex != 0 {
			t.Fatalf("write %d carries request id %x op index %d", n, id, first[n].OpIndex)
		}
		if id != second[n].RequestID {
			t.Fatalf("write %d derived %x then %x: a retry must re-derive the same id", n, id, second[n].RequestID)
		}
		if prior, dup := seen[id]; dup {
			t.Fatalf("writes %d and %d share request id %x", prior, n, id)
		}
		seen[id] = n
	}
}

// u39OtherTable is a table that differs from u39Fields.
func u39OtherTable() []FieldEntry {
	return append(u39Fields(), FieldEntry{Type: "string", Name: "City"})
}

// TestRealWriterDoesNotDedupeADifferentTableUnderOneID: two creates of one name draw the
// same id with different tables, and the first lands completely between the second's
// read and its writes. The second's writes must not be answered from the first's dedupe
// entries: it gets BUCKET_EXISTS, and the catalog keeps the first table.
func TestRealWriterDoesNotDedupeADifferentTableUnderOneID(t *testing.T) {
	real := newU39Real(t)
	raced := false
	real.beforeWrite = func(_ int, _ WriteRequest) {
		if raced {
			return
		}
		raced = true
		if _, err := real.create("Person", 0xA1); err != nil {
			t.Errorf("the winning create: %v", err)
		}
	}
	_, err := real.createWith("Person", u39OtherTable(), u39Draws(0xA1, 0xA2, 0xA3))
	_ = u39Code(t, err, cluster.CodeBucketExists)
	decl, found := real.declaration("Person")
	if !found || len(decl.Fields.Entries) != 4 || decl.BucketID != 0xA1 {
		t.Fatalf("catalog = %+v found %v, want the first create's four-entry table under a1", decl, found)
	}
}

// TestRealWriterDoesNotReplayARecreateAtANewVersion: the bucket is dropped (its documents
// gone, the dedupe entries still inside their window) and created again with the same id
// at a higher schema_version. The old inserts must not be replayed.
func TestRealWriterDoesNotReplayARecreateAtANewVersion(t *testing.T) {
	real := newU39Real(t)
	if _, err := real.create("Person", 0xA1); err != nil {
		t.Fatalf("create: %v", err)
	}
	batch := real.rig.kv.NewBatch()
	defer batch.Discard()
	for _, encID := range [][]byte{u39Encoded(t, "Person"), u39Encoded(t, u39IDBytes(0xA1))} {
		prefix := store.DocumentPrefix(u39Slot(encID), testDB, store.BucketsBucketID, encID)
		if err := batch.Delete(prefix); err != nil {
			t.Fatalf("delete: %v", err)
		}
		for fieldID := uint16(2); fieldID <= 8; fieldID++ {
			if err := batch.Delete(store.FieldKey(prefix, fieldID)); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit: %v", err)
	}
	real.published = u39Version + 1
	real.rig.schema.seen, real.rig.schema.published = real.published, real.published
	if _, err := real.create("Person", 0xA1); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if got := real.requireOneBucket("Person"); got != 0xA1 {
		t.Fatalf("recreated id = %x", got)
	}
}

// TestRealWriterStillDedupesTheSameCreateRetried: the same table at the same version
// derives the same request ids, so a lost answer is answered from the dedupe entry.
func TestRealWriterStillDedupesTheSameCreateRetried(t *testing.T) {
	real := newU39Real(t)
	real.crash = func(n int, _ WriteRequest) (bool, bool) { return n == 1, true }
	if _, err := real.create("Person", 0xA1); !errors.Is(err, real.crashError) {
		t.Fatalf("first run = %v", err)
	}
	real.crash = nil
	// Replaying the declaration insert itself, as a retry whose read missed it would.
	answer, err := real.rig.w.Write(context.Background(), real.sent[1])
	if err != nil || !answer.Deduped {
		t.Fatalf("replayed declaration = %+v, %v, want a deduped answer", answer, err)
	}
}

func u39Encoded(t *testing.T, id any) []byte {
	t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	return encoded
}
