package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	u39DB      = uint64(7)
	u39DBName  = "app"
	u39Version = int64(5)
)

// u39Doc is one ring document: its fields by id, as the owner stored them.
type u39Doc map[uint16][]byte

type u39Key struct {
	slot  uint16
	encID string
}

// u39Ring is the _buckets documents of one database behind the Sender, the declaration
// reader and the name reader. It refuses an insert of an existing document the way the
// owner's check 6a does, and records every write it was handed.
type u39Ring struct {
	docs   map[u39Key]u39Doc
	writes []WriteRequest
	// failWrite, when set, is asked before write n (0-based) is applied.
	failWrite func(n int, req WriteRequest) error
	// degradeWrite, when set, answers write n applied_degraded.
	degradeWrite func(n int) bool
	reads        int
}

func newU39Ring() *u39Ring { return &u39Ring{docs: map[u39Key]u39Doc{}} }

func u39Slot(encID []byte) uint16 {
	return storage.SlotOf(storage.PlacementKey(u39DBName, systemBucketName, encID))
}

func u39IDBytes(id uint64) []byte {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], id)
	return raw[:]
}

func (r *u39Ring) Write(_ context.Context, req WriteRequest) (WriteAnswer, error) {
	n := len(r.writes)
	r.writes = append(r.writes, req)
	if r.failWrite != nil {
		if err := r.failWrite(n, req); err != nil {
			return WriteAnswer{}, err
		}
	}
	key := u39Key{req.Slot, string(req.Op.EncID)}
	switch req.Op.Class {
	case cluster.OpInsert:
		if _, exists := r.docs[key]; exists {
			return WriteAnswer{}, &cluster.Error{Code: cluster.CodeDocumentExists, Message: "document exists"}
		}
		doc := u39Doc{}
		for _, field := range req.Op.Fields {
			doc[field.FieldID] = field.Value
		}
		r.docs[key] = doc
	default:
		return WriteAnswer{}, errors.New("a create writes only inserts")
	}
	outcome := machine.OutcomeApplied
	copies := uint8(2)
	if r.degradeWrite != nil && r.degradeWrite(n) {
		outcome, copies = machine.OutcomeAppliedDegraded, 1
	}
	return WriteAnswer{Outcome: outcome, CopiesConfirmed: copies, Pos: cluster.Position{Epoch: 1, Seq: uint64(n + 1)}}, nil
}

func (r *u39Ring) Read(_ context.Context, dbID uint64, name string) (machine.Declaration, []byte, bool, error) {
	r.reads++
	encID, err := store.EncodeComponent(name)
	if err != nil {
		return machine.Declaration{}, nil, false, err
	}
	doc, ok := r.docs[u39Key{u39Slot(encID), string(encID)}]
	if !ok {
		return machine.Declaration{}, nil, false, nil
	}
	decl, err := store.DecodeDeclaration(doc)
	return decl, nil, true, err
}

// u39String reads a stored BSON string: type, 4-byte length, bytes, zero.
func u39String(stored []byte) string {
	if len(stored) < 6 {
		return ""
	}
	return string(stored[5 : len(stored)-1])
}

func (r *u39Ring) declaration(name string) (machine.Declaration, bool) {
	decl, _, found, err := r.Read(context.Background(), u39DB, name)
	if err != nil {
		panic(err)
	}
	return decl, found
}

func (r *u39Ring) reverse(bucketID uint64) (string, bool) {
	encID, err := store.EncodeComponent(u39IDBytes(bucketID))
	if err != nil {
		panic(err)
	}
	doc, ok := r.docs[u39Key{u39Slot(encID), string(encID)}]
	if !ok {
		return "", false
	}
	return u39String(doc[7]), true
}

type u39Topology struct{}

func (u39Topology) Committed() uint64                      { return 1 }
func (u39Topology) Self() string                           { return "node-a" }
func (u39Topology) Members() int                           { return 3 }
func (u39Topology) Table() *cluster.SlotTable              { return nil }
func (u39Topology) Target(uint16, uint64) cluster.Position { return cluster.Position{} }
func (u39Topology) Database(uint64) (DatabaseInfo, bool) {
	return DatabaseInfo{RF: 3, DefaultConcern: machine.ConcernMajority}, true
}

// u39Create builds a CreateBucket over ring; mints are drawn from ids in order.
func u39Create(ring *u39Ring, ids ...uint64) *CreateBucket {
	next := 0
	return NewCreateBucket(CreateBucketConfig{
		Sender: ring, Declarations: ring, Topology: u39Topology{},
		DatabaseName: func(uint64) (string, bool) { return u39DBName, true },
		Published:    func(uint64) int64 { return u39Version },
		DrawID: func() (uint64, error) {
			if next >= len(ids) {
				return 0, errors.New("the test ran out of minted ids")
			}
			next++
			return ids[next-1], nil
		},
	})
}

func u39Fields() []FieldEntry {
	return []FieldEntry{{Type: "string", Name: "Name"}, {Type: "int", Name: "Age"}, {Type: "fifoStore", Name: "Purchases"}}
}

// u39Code fails the test unless err is a refusal with the code, and returns its message.
func u39Code(t *testing.T, err error, want cluster.Code) string {
	t.Helper()
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != want {
		t.Fatalf("error = %v, want a %s refusal", err, want)
	}
	return refusal.Message
}

// TestCreateWritesTwoDocumentsOnTwoSlots pins the shape and the order: the reverse index
// claims the id first, then the declaration under the name at version s, each at majority.
func TestCreateWritesTwoDocumentsOnTwoSlots(t *testing.T) {
	ring := newU39Ring()
	id, err := u39Create(ring, 0x1f4a2c9b7de30015).Run(context.Background(), u39DB, "Person", u39Fields())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if id != 0x1f4a2c9b7de30015 {
		t.Fatalf("id = %x", id)
	}
	if len(ring.writes) != 2 {
		t.Fatalf("%d writes, want the reverse index then the declaration", len(ring.writes))
	}
	reverseWrite, declWrite := ring.writes[0], ring.writes[1]
	if declWrite.Slot == reverseWrite.Slot {
		t.Fatalf("both documents on slot %d: the spec places them on two slots", declWrite.Slot)
	}
	for n, write := range ring.writes {
		if write.Concern != machine.ConcernMajority || write.DBID != u39DB {
			t.Fatalf("write %d concern %v db %d, want majority and db %d", n, write.Concern, write.DBID, u39DB)
		}
		if write.Op.Class != cluster.OpInsert || write.Op.BucketID != store.BucketsBucketID || write.Op.SchemaVersionSeen != u39Version {
			t.Fatalf("write %d op = class %d bucket %x seen %d", n, write.Op.Class, write.Op.BucketID, write.Op.SchemaVersionSeen)
		}
	}
	decl, found := ring.declaration("Person")
	if !found || decl.BucketID != id || decl.Version != u39Version || decl.State != machine.StateActive {
		t.Fatalf("declaration = %+v found %v", decl, found)
	}
	if len(decl.Fields.Entries) != 4 || decl.Fields.Entries[0].Name != "_id_" || decl.Fields.Entries[3].ID != 4 || decl.Fields.NextFieldID != 5 {
		t.Fatalf("table = %+v", decl.Fields)
	}
	if name, found := ring.reverse(id); !found || name != "Person" {
		t.Fatalf("reverse index = %q found %v", name, found)
	}
	reverseDoc := ring.docs[u39Key{reverseWrite.Slot, string(reverseWrite.Op.EncID)}]
	if u39String(reverseDoc[4]) != machine.StateActive || len(reverseDoc) != 2 {
		t.Fatalf("reverse document = %v, want state active and name only", reverseDoc)
	}
}

// TestCreateIsIdempotentAcrossACrash: the process dies after the reverse index claimed an
// id and before the declaration. The claim is an inert orphan; the retry draws a new id
// and writes the declaration, and a finished create writes nothing at all.
func TestCreateIsIdempotentAcrossACrash(t *testing.T) {
	ring := newU39Ring()
	crash := errors.New("the process died before the declaration insert")
	ring.failWrite = func(n int, _ WriteRequest) error {
		if n == 1 {
			return crash
		}
		return nil
	}
	if _, err := u39Create(ring, 0xAAAA).Run(context.Background(), u39DB, "Person", u39Fields()); !errors.Is(err, crash) {
		t.Fatalf("first Run = %v, want the crash", err)
	}
	if _, found := ring.declaration("Person"); found {
		t.Fatal("the crash left a declaration")
	}
	ring.failWrite = nil
	id, err := u39Create(ring, 0xBBBB).Run(context.Background(), u39DB, "Person", u39Fields())
	if err != nil || id != 0xBBBB {
		t.Fatalf("retry = %x, %v, want id bbbb", id, err)
	}
	if decl, found := ring.declaration("Person"); !found || decl.BucketID != 0xBBBB {
		t.Fatalf("declaration = %+v found %v", decl, found)
	}
	if name, _ := ring.reverse(0xAAAA); name != "Person" {
		t.Fatalf("the orphan claim of aaaa = %q, want it inert and still naming Person", name)
	}
	before := len(ring.writes)
	if again, err := u39Create(ring).Run(context.Background(), u39DB, "Person", u39Fields()); err != nil || again != 0xBBBB {
		t.Fatalf("third run = %x, %v", again, err)
	}
	if len(ring.writes) != before {
		t.Fatalf("a finished create wrote %d more documents", len(ring.writes)-before)
	}
}

// TestCreateWithADifferentTableIsBucketExists: the retry is old or an alter ran, and the
// refusal never tells the operator to drop and does point at describe.
func TestCreateWithADifferentTableIsBucketExists(t *testing.T) {
	ring := newU39Ring()
	if _, err := u39Create(ring, 0xBBBB).Run(context.Background(), u39DB, "Person", u39Fields()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	other := append(u39Fields(), FieldEntry{Type: "string", Name: "City"})
	_, err := u39Create(ring).Run(context.Background(), u39DB, "Person", other)
	refusal := u39Code(t, err, cluster.CodeBucketExists)
	if strings.Contains(refusal, "drop bucket") || !strings.Contains(refusal, "describe bucket") {
		t.Fatalf("the refusal = %s", refusal)
	}
}

// TestOneBucketIDIsNeverShared: three draws that other buckets already own are
// BUCKET_ID_COLLISION with no declaration for the name, and no other bucket's document is
// changed or deleted. A declaration written before the claim would remain here.
func TestOneBucketIDIsNeverShared(t *testing.T) {
	ring := newU39Ring()
	if _, err := u39Create(ring, 0x1111).Run(context.Background(), u39DB, "Other", nil); err != nil {
		t.Fatalf("Other: %v", err)
	}
	if _, err := u39Create(ring, 0x4444).Run(context.Background(), u39DB, "Third", nil); err != nil {
		t.Fatalf("Third: %v", err)
	}
	snapshot := map[u39Key]string{}
	for key, doc := range ring.docs {
		snapshot[key] = string(doc[4]) + string(doc[7]) + string(doc[2])
	}
	ring.writes = nil
	_, err := u39Create(ring, 0x1111, 0x4444, 0x1111).Run(context.Background(), u39DB, "Person", u39Fields())
	message := u39Code(t, err, cluster.CodeBucketIDCollision)
	if !strings.Contains(message, "nothing was created") || !strings.Contains(message, "Person") {
		t.Fatalf("refusal = %s", message)
	}
	if _, found := ring.declaration("Person"); found {
		t.Fatal("a declaration exists for a name that never claimed an id")
	}
	if len(ring.writes) != 3 {
		t.Fatalf("%d writes, want exactly the three refused claims", len(ring.writes))
	}
	for n, write := range ring.writes {
		if write.Op.Class != cluster.OpInsert {
			t.Fatalf("write %d is class %d", n, write.Op.Class)
		}
	}
	for key, doc := range ring.docs {
		if want, ok := snapshot[key]; !ok || want != string(doc[4])+string(doc[7])+string(doc[2]) {
			t.Fatalf("a document of another bucket changed: %v", key)
		}
	}
	if len(ring.docs) != len(snapshot) {
		t.Fatalf("%d documents, was %d", len(ring.docs), len(snapshot))
	}
}

// TestACollisionRedrawsAndCompletes: one refused claim, then a free id; the declaration
// carries only the id this create owns.
func TestACollisionRedrawsAndCompletes(t *testing.T) {
	ring := newU39Ring()
	if _, err := u39Create(ring, 0x1111).Run(context.Background(), u39DB, "Other", nil); err != nil {
		t.Fatalf("Other: %v", err)
	}
	ring.writes = nil
	id, err := u39Create(ring, 0x1111, 0x2222).Run(context.Background(), u39DB, "Person", u39Fields())
	if err != nil || id != 0x2222 {
		t.Fatalf("Run = %x, %v, want 2222", id, err)
	}
	if len(ring.writes) != 3 {
		t.Fatalf("%d writes, want refused claim, claim, declaration", len(ring.writes))
	}
	if decl, _ := ring.declaration("Person"); decl.BucketID != 0x2222 {
		t.Fatalf("declaration id = %x", decl.BucketID)
	}
	if name, _ := ring.reverse(0x1111); name != "Other" {
		t.Fatalf("id 1111 = %q", name)
	}
}

// TestCreateCommitsNoRaftEntry: a create is ring documents and nothing else. The source
// holds no Raft proposal, and at run time every write is an insert into _buckets.
func TestCreateCommitsNoRaftEntry(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "createb.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing createb.go: %v", err)
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			if strings.HasPrefix(typed.Sel.Name, "Propose") || typed.Sel.Name == "Raft" {
				t.Errorf("createb.go reaches for %s: a create commits no Raft entry", typed.Sel.Name)
			}
		case *ast.ImportSpec:
			if strings.Contains(typed.Path.Value, "raft") {
				t.Errorf("createb.go imports %s", typed.Path.Value)
			}
		}
		return true
	})
	ring := newU39Ring()
	if _, err := u39Create(ring, 0x77).Run(context.Background(), u39DB, "Person", u39Fields()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for n, write := range ring.writes {
		if write.Op.BucketID != store.BucketsBucketID || write.Op.Class != cluster.OpInsert {
			t.Fatalf("write %d is class %d into bucket %x", n, write.Op.Class, write.Op.BucketID)
		}
	}
}

// TestReservedNamesAreRefused: a leading underscore, the empty name and an over-long name
// are refused before any read or write.
func TestReservedNamesAreRefused(t *testing.T) {
	for _, name := range []string{"_buckets", "_x", "_"} {
		ring := newU39Ring()
		_, err := u39Create(ring, 0x1).Run(context.Background(), u39DB, name, nil)
		refusal := u39Code(t, err, cluster.CodeReservedName)
		if !strings.Contains(refusal, name) {
			t.Fatalf("refusal for %q does not name it: %s", name, refusal)
		}
		if len(ring.writes) != 0 || ring.reads != 0 {
			t.Fatalf("%q: %d writes and %d reads before the refusal", name, len(ring.writes), ring.reads)
		}
	}
	for _, name := range []string{"", strings.Repeat("n", 256)} {
		ring := newU39Ring()
		_, err := u39Create(ring, 0x1).Run(context.Background(), u39DB, name, nil)
		_ = u39Code(t, err, cluster.CodeInvalidArgument)
		if len(ring.writes) != 0 || ring.reads != 0 {
			t.Fatalf("name of %d bytes: %d writes, %d reads", len(name), len(ring.writes), ring.reads)
		}
	}
	ring := newU39Ring()
	if _, err := u39Create(ring, 0x1).Run(context.Background(), u39DB, strings.Repeat("n", 255), nil); err != nil {
		t.Fatalf("a 255-byte name is the longest allowed: %v", err)
	}
	if _, err := u39Create(ring, 0x2).Run(context.Background(), u39DB, "a_b", nil); err != nil {
		t.Fatalf("an inner underscore is allowed: %v", err)
	}
}

// TestCreateRefusesADegradedDeclarationWrite: applied_degraded on the declaration is not a
// success, and a degraded claim stops the create before any declaration exists.
func TestCreateRefusesADegradedDeclarationWrite(t *testing.T) {
	ring := newU39Ring()
	ring.degradeWrite = func(n int) bool { return n == 1 }
	_, err := u39Create(ring, 0x9).Run(context.Background(), u39DB, "Person", u39Fields())
	refusal := u39Code(t, err, cluster.CodeUnavailable)
	if !strings.Contains(refusal, "1 of 3 copies") {
		t.Fatalf("refusal = %s", refusal)
	}
	if len(ring.writes) != 2 {
		t.Fatalf("%d writes, want the claim and the refused declaration", len(ring.writes))
	}

	second := newU39Ring()
	second.degradeWrite = func(n int) bool { return n == 0 }
	_, err = u39Create(second, 0x9).Run(context.Background(), u39DB, "Person", u39Fields())
	_ = u39Code(t, err, cluster.CodeUnavailable)
	if len(second.writes) != 1 {
		t.Fatalf("%d writes: the create went on after a degraded claim", len(second.writes))
	}
	if _, found := second.declaration("Person"); found {
		t.Fatal("a declaration was written after a degraded claim")
	}
}
