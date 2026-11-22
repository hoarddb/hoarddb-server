package wire

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u40Sender wraps the node's Sender so a test can watch each declaration write, run something
// ahead of it, or change its answer.
type u40Sender struct {
	inner  Sender
	mu     sync.Mutex
	sent   []WriteRequest
	fields [][]cluster.FieldOp
	// before runs ahead of the n-th write (0-based).
	before func(n int)
	// answer, when set, replaces the owner's answer after the write was applied.
	answer func(WriteAnswer) WriteAnswer
}

func (s *u40Sender) Write(ctx context.Context, req WriteRequest) (WriteAnswer, error) {
	s.mu.Lock()
	n := len(s.sent)
	s.sent = append(s.sent, req)
	before := s.before
	s.mu.Unlock()
	if before != nil {
		before(n)
	}
	// Watch the fields the owner actually wrote, which only the owner knows.
	if rewrite := req.Rewrite; rewrite != nil {
		req.Rewrite = func() ([]cluster.FieldOp, error) {
			fields, err := rewrite()
			if err == nil {
				s.mu.Lock()
				s.fields = append(s.fields, fields)
				s.mu.Unlock()
			}
			return fields, err
		}
	}
	answer, err := s.inner.Write(ctx, req)
	if err == nil && s.answer != nil {
		answer = s.answer(answer)
	}
	return answer, err
}

// crashingBumper is the bump a process that died before committing it.
type crashingBumper struct{ calls int }

var errU40Crash = errors.New("the process died before the bump")

func (b *crashingBumper) BumpSchema(context.Context, uint64, int64, int64, cluster.BumpNote) error {
	b.calls++
	return errU40Crash
}

// u40Rig is a node with database app and bucket User { Name, Age, City, Purchases }, and an
// AlterBucket over a watched sender.
type u40Rig struct {
	t      *testing.T
	node   *Node
	dbID   uint64
	sender *u40Sender
	alter  *AlterBucket
}

func newU40Rig(t *testing.T) *u40Rig {
	t.Helper()
	node, _ := u30iFresh(t)
	dbID, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	sender := &u40Sender{inner: nodeSender{node: node}}
	config := node.alterer.cfg
	config.Sender = sender
	return &u40Rig{t: t, node: node, dbID: dbID, sender: sender, alter: NewAlterBucket(config)}
}

// another is a second coordinator over the same node and the same watched sender.
func (r *u40Rig) another() *AlterBucket {
	config := r.node.alterer.cfg
	config.Sender = r.sender
	return NewAlterBucket(config)
}

// anotherNode is a coordinator of another node: same ring, no queue shared with this node's.
func (r *u40Rig) anotherNode() *AlterBucket {
	config := r.node.alterer.cfg
	config.Sender, config.Queue = r.sender, nil
	return NewAlterBucket(config)
}

func (r *u40Rig) run(change AlterChange) machine.FieldTable {
	r.t.Helper()
	table, err := r.alter.Run(context.Background(), r.dbID, "User", change)
	if err != nil {
		r.t.Fatalf("alter %+v: %v", change, err)
	}
	return table
}

func (r *u40Rig) declaration() machine.Declaration {
	r.t.Helper()
	decl, _, found, err := r.node.fetcher.Read(context.Background(), r.dbID, "User")
	if err != nil || !found {
		r.t.Fatalf("reading the declaration: found %v, %v", found, err)
	}
	return decl
}

func u40Add(name, word string) AlterChange {
	return AlterChange{Add: []FieldEntry{{Name: name, Type: word}}}
}

func u40ID(t *testing.T, table machine.FieldTable, name string) uint16 {
	t.Helper()
	for _, entry := range table.Entries {
		if entry.Name == name {
			return entry.ID
		}
	}
	t.Fatalf("field %q is not in the table %+v", name, table.Entries)
	return 0
}

func u40Has(table machine.FieldTable, name string) bool {
	for _, entry := range table.Entries {
		if entry.Name == name {
			return true
		}
	}
	return false
}

func (r *u40Rig) insertUser(id string, extra ...bson.E) error {
	doc := append(u30iUser(id, "Ada", 36), extra...)
	_, err := r.node.Insert(context.Background(), u30iDB, "User", doc)
	return err
}

// TestAlterAddsAFieldAndEveryWriterUsesIt: the table in force gains the field at the next
// free id, no existing document is rewritten, and one bump published it.
func TestAlterAddsAFieldAndEveryWriterUsesIt(t *testing.T) {
	r := newU40Rig(t)
	for i := range 20 {
		if err := r.insertUser(fmt.Sprintf("u%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	before := map[uint16]cluster.Pair{}
	for _, slot := range r.node.pairs.HeldSlots(r.dbID) {
		before[slot] = r.node.pairs.Snapshot(slot, r.dbID)
	}
	if err := r.insertUser("x", bson.E{Key: "Email", Value: "a@b"}); err == nil {
		t.Fatal("a write naming a field that is not declared yet was accepted")
	}

	table := r.run(u40Add("Email", "string"))

	if got := u40ID(t, table, "Email"); got != 6 {
		t.Fatalf("Email got id %d, want 6 (User declared ids 1 to 5)", got)
	}
	if got := r.node.topology.Published(r.dbID); got != 2 {
		t.Fatalf("published = %d, want 2: one bump", got)
	}
	if err := r.insertUser("with-email", bson.E{Key: "Email", Value: "ada@example.org"}); err != nil {
		t.Fatalf("a write naming the new field after the alter: %v", err)
	}
	doc, err := r.node.Get(context.Background(), u30iDB, "User", "with-email")
	if err != nil {
		t.Fatal(err)
	}
	if got := u30iString(t, doc, "Email"); got != "ada@example.org" {
		t.Fatalf("Email = %q", got)
	}
	old, err := r.node.Get(context.Background(), u30iDB, "User", "u3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bson.Raw(old.Body).LookupErr("Email"); err == nil {
		t.Fatal("an existing document gained an Email value: the alter rewrote documents")
	}
	declSlot := u40DeclSlot(t, "User")
	for _, slot := range r.node.pairs.HeldSlots(r.dbID) {
		moved := r.node.pairs.Snapshot(slot, r.dbID) != before[slot]
		// The new insert moved its own pair; only the declaration's pair may have moved for the alter itself.
		if slot != declSlot && moved && slot != u40DocSlot(t, r.node, "User", "with-email") {
			t.Fatalf("slot %d moved during an add: an alter writes the declaration and nothing else", slot)
		}
	}
	// One declaration write of one key (conv 20 step 2) and nothing per document.
	if got := len(r.sender.fields); got != 1 || len(r.sender.fields[0]) != 1 || r.sender.fields[0][0].FieldID != 8 {
		t.Fatalf("the alter wrote %v, want one write of field 8 only", r.sender.fields)
	}
}

// TestAlterDropRetiresTheFieldAndRefusesWrites: a dropped field's id is retired, never
// reused. Until the sweep (U43) deletes the values, the field is invisible to a read, a write
// naming it is FIELD_TYPE, and adding the same name again gets a fresh id.
func TestAlterDropRetiresTheFieldAndRefusesWrites(t *testing.T) {
	r := newU40Rig(t)
	if err := r.insertUser("ada"); err != nil {
		t.Fatal(err)
	}
	table := r.run(AlterChange{Drop: []string{"Age"}})
	if u40Has(table, "Age") {
		t.Fatalf("Age is still declared after its drop: %+v", table.Entries)
	}
	if len(table.Retired) != 1 || table.Retired[0].ID != 3 || table.Retired[0].Epoch == 0 {
		t.Fatalf("retired = %+v, want id 3 with the epoch of the alter", table.Retired)
	}
	doc, err := r.node.Get(context.Background(), u30iDB, "User", "ada")
	if err != nil {
		t.Fatalf("a document with a retired field's value must still read: %v", err)
	}
	if _, err := bson.Raw(doc.Body).LookupErr("Age"); err == nil {
		t.Fatalf("a read shows the retired field Age: %s", bson.Raw(doc.Body))
	}
	if got := u30iString(t, doc, "Name"); got != "Ada" {
		t.Fatalf("the other fields must read as before: Name = %q", got)
	}
	err = r.insertUser("grace")
	u30iCode(t, err, cluster.CodeFieldType)

	// A write that names the retired id 3 is refused by the owner.
	u30iCode(t, r.node.rawUpsertWithRetiredField(t), cluster.CodeFieldType)

	again := r.run(u40Add("Age", "int"))
	if got := u40ID(t, again, "Age"); got == 3 {
		t.Fatal("re-adding Age reused the retired id 3: its old values would resurface")
	}
}

// rawUpsertWithRetiredField sends, under the header in force now, a write that names retired
// field id 3 on the wire, as a client holding the old table would build it after refreshing
// only its header.
func (n *Node) rawUpsertWithRetiredField(t *testing.T) error {
	t.Helper()
	ref, err := n.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("raw")
	if err != nil {
		t.Fatal(err)
	}
	op := ref.template()
	op.Class = cluster.OpUpsert
	op.IDValue = stringValue("raw")
	op.Fields = []cluster.FieldOp{{FieldID: 3, Value: []byte{0x10, 7, 0, 0, 0}}}
	_, err = n.send(context.Background(), ref, encID, op, writeOptions{requestID: [16]byte{9}})
	return err
}

// TestTypeChangeIsRefusedWithTheFix: a declared type never changes. The refusal names the
// field, both types and the three steps that do work, and commits nothing.
func TestTypeChangeIsRefusedWithTheFix(t *testing.T) {
	r := newU40Rig(t)
	_, err := r.alter.Run(context.Background(), r.dbID, "User", u40Add("Age", "string"))
	u30iCode(t, err, cluster.CodeInvalidArgument)
	for _, want := range []string{`field "Age" in app.User is declared int and this statement declares it string`,
		"a declared type never changes", "alter bucket User in app { add <NewField> <type> }", "alter bucket User in app { drop Age }"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal lacks %q:\n%v", want, err)
		}
	}
	if decl := r.declaration(); decl.Pending != nil {
		t.Fatalf("a refused alter left a pending table: %+v", decl.Pending)
	}
	if got := r.node.topology.Published(r.dbID); got != 1 {
		t.Fatalf("a refused alter moved schema_version to %d", got)
	}
	if len(r.sender.sent) != 0 {
		t.Fatal("a refused alter wrote to the ring")
	}
}

// TestFieldIDsAreNeverReused: dropping the field with the highest id, then adding another,
// gives the new field a fresh id, and re-adding the dropped name gives another fresh one.
func TestFieldIDsAreNeverReused(t *testing.T) {
	r := newU40Rig(t)
	r.run(u40Add("X", "string"))
	table := r.run(AlterChange{Drop: []string{"X"}})
	if u40Has(table, "X") {
		t.Fatal("X survived its drop")
	}
	y := u40ID(t, r.run(u40Add("Y", "string")), "Y")
	if y != 7 {
		t.Fatalf("Y got id %d, want 7: id 6 belonged to the dropped X", y)
	}
	x := u40ID(t, r.run(u40Add("X", "string")), "X")
	if x != 8 {
		t.Fatalf("a re-added X got id %d, want 8", x)
	}
}

// TestAlterIsAReadModifyWriteAtTheOwner: a coordinator that read the declaration before
// another alter finished must not build its table from what it read. The second alter here
// reads at version 1, then the first alter runs to its end inside the second's write, and the
// owner builds from the table then in force: both fields exist, with different ids.
func TestAlterIsAReadModifyWriteAtTheOwner(t *testing.T) {
	r := newU40Rig(t)
	first := r.anotherNode()
	var ran atomic.Bool
	r.sender.before = func(int) {
		if ran.CompareAndSwap(false, true) {
			if _, err := first.Run(context.Background(), r.dbID, "User", u40Add("Nick", "string")); err != nil {
				t.Errorf("the first alter: %v", err)
			}
		}
	}
	table, err := r.alter.Run(context.Background(), r.dbID, "User", u40Add("Email", "string"))
	if err != nil {
		t.Fatal(err)
	}
	if !u40Has(table, "Nick") || !u40Has(table, "Email") {
		t.Fatalf("a field was lost: %+v", table.Entries)
	}
	if nick, email := u40ID(t, table, "Nick"), u40ID(t, table, "Email"); nick == email {
		t.Fatalf("Nick and Email share id %d: values written under one would read as the other", nick)
	}
	if got := r.node.topology.Published(r.dbID); got != 3 {
		t.Fatalf("published = %d, want 3: one bump for each alter", got)
	}
}

// TestConcurrentAltersLoseNoField: two coordinators add ten fields each, all at once. Every
// field ends up declared exactly once and no two share an id.
func TestConcurrentAltersLoseNoField(t *testing.T) {
	r := newU40Rig(t)
	var wg sync.WaitGroup
	for _, coordinator := range []*AlterBucket{r.alter, r.another()} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 10 {
				name := fmt.Sprintf("f%p_%d", coordinator, i)
				if _, err := coordinator.Run(context.Background(), r.dbID, "User", u40Add(name, "string")); err != nil {
					t.Errorf("adding %s: %v", name, err)
				}
			}
		}()
	}
	wg.Wait()
	table, _ := machine.EffectiveTable(r.declaration(), r.node.topology.Published(r.dbID))
	if len(table.Entries) != 5+20 {
		t.Fatalf("the table holds %d fields, want 25", len(table.Entries))
	}
	ids := map[uint16]string{}
	for _, entry := range table.Entries {
		if other, ok := ids[entry.ID]; ok {
			t.Fatalf("%s and %s share id %d", entry.Name, other, entry.ID)
		}
		ids[entry.ID] = entry.Name
	}
}

// TestCrashBetweenPendingAndBumpLeavesTheOldTableInForce: the process dies after the pending
// table is durable and before the bump. The old table stays in force and the bucket stays
// writable; the new field is refused as undeclared; running the same alter again writes the
// same table and commits the bump.
func TestCrashBetweenPendingAndBumpLeavesTheOldTableInForce(t *testing.T) {
	r := newU40Rig(t)
	crash := &crashingBumper{}
	config := r.node.alterer.cfg
	config.Sender, config.Bumper = r.sender, crash
	interrupted := NewAlterBucket(config)

	_, err := interrupted.Run(context.Background(), r.dbID, "User", u40Add("Email", "string"))
	if !errors.Is(err, errU40Crash) || crash.calls != 1 {
		t.Fatalf("the alter returned %v after %d bump calls, want the crash after one", err, crash.calls)
	}
	decl := r.declaration()
	if decl.Pending == nil || decl.Pending.Version != 2 {
		t.Fatalf("the pending table is not durable: %+v", decl.Pending)
	}
	if u40Has(decl.Fields, "Email") {
		t.Fatal("the new table was written into field 5 before the bump: the bucket is offline until the alter is re-run")
	}
	if got := r.node.topology.Published(r.dbID); got != 1 {
		t.Fatalf("published = %d, want 1", got)
	}
	// The bucket is fully usable on the old table.
	if err := r.insertUser("still-works"); err != nil {
		t.Fatalf("a write between the pending write and the bump: %v", err)
	}
	if _, err := r.node.Get(context.Background(), u30iDB, "User", "still-works"); err != nil {
		t.Fatalf("a read between the pending write and the bump: %v", err)
	}
	if err := r.insertUser("early", bson.E{Key: "Email", Value: "e"}); err == nil {
		t.Fatal("the pending field was writable before its bump")
	}
	described, err := r.node.bucket(context.Background(), u30iDB, "User")
	if err != nil || u40Has(described.table, "Email") {
		t.Fatalf("describe shows the pending field before its bump: %v %+v", err, described.table.Entries)
	}

	// The same alter again finishes it, with the same ids.
	table := r.run(u40Add("Email", "string"))
	if got := u40ID(t, table, "Email"); got != 6 || !equalTables(table, decl.Pending.Table) {
		t.Fatalf("the re-run built %+v, the interrupted one wrote %+v", table, decl.Pending.Table)
	}
	if got := r.node.topology.Published(r.dbID); got != 2 {
		t.Fatalf("published = %d after the re-run, want 2", got)
	}
	if err := r.insertUser("late", bson.E{Key: "Email", Value: "e"}); err != nil {
		t.Fatalf("a write naming the field after the re-run: %v", err)
	}
}

func equalTables(a, b machine.FieldTable) bool {
	return string(store.EncodeFieldTable(a)) == string(store.EncodeFieldTable(b))
}

// TestAlterRepeatedIsIdempotent: the same alter twice gives the same table, commits one
// bump, and the second writes nothing.
func TestAlterRepeatedIsIdempotent(t *testing.T) {
	r := newU40Rig(t)
	first := r.run(u40Add("Email", "string"))
	writes := len(r.sender.sent)
	second := r.run(u40Add("Email", "string"))
	if !equalTables(first, second) {
		t.Fatalf("the repeat built another table:\n%+v\n%+v", first, second)
	}
	if len(r.sender.sent) != writes {
		t.Fatal("a repeat of a finished alter wrote to the ring again")
	}
	if got := r.node.topology.Published(r.dbID); got != 2 {
		t.Fatalf("published = %d after a repeat, want 2: a repeat commits no bump", got)
	}
	drop := r.run(AlterChange{Drop: []string{"Age"}})
	if again := r.run(AlterChange{Drop: []string{"Age"}}); !equalTables(drop, again) {
		t.Fatal("a repeated drop built another table")
	}
	if got := r.node.topology.Published(r.dbID); got != 3 {
		t.Fatalf("published = %d, want 3", got)
	}
}

// TestAlterRefusesADegradedDeclarationWrite: a declaration written to fewer copies than the
// concern asks is UNAVAILABLE, and no bump is committed on it.
func TestAlterRefusesADegradedDeclarationWrite(t *testing.T) {
	r := newU40Rig(t)
	r.sender.answer = func(answer WriteAnswer) WriteAnswer {
		answer.Outcome, answer.CopiesConfirmed = machine.OutcomeAppliedDegraded, 1
		return answer
	}
	_, err := r.alter.Run(context.Background(), r.dbID, "User", u40Add("Email", "string"))
	u30iCode(t, err, cluster.CodeUnavailable)
	if !strings.Contains(err.Error(), "the declaration write was confirmed by 1 of") {
		t.Fatalf("the refusal does not say the write was not durable:\n%v", err)
	}
	if got := r.node.topology.CommittedSchema(r.dbID); got != 1 {
		t.Fatalf("a bump was committed on a degraded declaration write: schema_version %d", got)
	}
}

// TestAlterFoldsAPublishedPendingFirst: an alter built over a published pending table writes
// three keys, field 3 and field 5 from that table and field 8 for its own, so a quiet
// declaration stays small; and it builds from the published table, so the first alter's
// field is still there.
func TestAlterFoldsAPublishedPendingFirst(t *testing.T) {
	r := newU40Rig(t)
	first := r.run(u40Add("Email", "string"))
	if got := r.declaration(); got.Pending == nil || u40Has(got.Fields, "Email") {
		t.Fatalf("after one alter field 5 must still hold the old table and field 8 the new: %+v", got)
	}
	second := r.run(u40Add("Nick", "string"))
	if !u40Has(second, "Email") || !u40Has(second, "Nick") {
		t.Fatalf("the second alter lost a field: %+v", second.Entries)
	}
	if u40ID(t, second, "Email") != u40ID(t, first, "Email") {
		t.Fatal("Email changed id across the second alter")
	}
	written := r.sender.fields[len(r.sender.fields)-1]
	var ids []uint16
	for _, field := range written {
		ids = append(ids, field.FieldID)
	}
	if fmt.Sprint(ids) != "[3 5 8]" {
		t.Fatalf("the second alter wrote fields %v, want [3 5 8]: the fold", ids)
	}
	decl := r.declaration()
	if decl.Version != 2 || !u40Has(decl.Fields, "Email") || u40Has(decl.Fields, "Nick") || decl.Pending == nil || decl.Pending.Version != 3 {
		t.Fatalf("folded declaration = version %d fields %+v pending %+v", decl.Version, decl.Fields.Entries, decl.Pending)
	}
}

// TestAStaleWriterSeesSchemaNotCurrent: a write built on the table before an alter is refused
// SCHEMA_NOT_CURRENT for the bucket that changed once the owner holds the new declaration, and
// a write to another bucket of the database is re-admitted and accepted.
func TestAStaleWriterSeesSchemaNotCurrent(t *testing.T) {
	r := newU40Rig(t)
	ctx := context.Background()
	if _, err := r.node.CreateBucket(ctx, u30iDB, "Other", Field{Name: "Label", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	staleUser, err := r.node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	staleOther, err := r.node.bucket(ctx, u30iDB, "Other")
	if err != nil {
		t.Fatal(err)
	}
	r.run(u40Add("Email", "string"))
	// The owner's cache entry is refreshed by any use after the bump; the stale write below
	// meets the declaration as it is now.
	if _, err := r.node.bucket(ctx, u30iDB, "User"); err != nil {
		t.Fatal(err)
	}

	send := func(ref bucketRef, doc bson.D) error {
		id, fields, err := ref.documentOp(doc)
		if err != nil {
			t.Fatal(err)
		}
		encID, err := store.EncodeComponent(id)
		if err != nil {
			t.Fatal(err)
		}
		op := ref.template()
		op.Class, op.IDValue, op.Fields = cluster.OpInsert, stringValue(id), fields
		_, err = r.node.send(ctx, ref, encID, op, writeOptions{requestID: [16]byte{byte(len(id))}})
		return err
	}
	u30iCode(t, send(staleUser, u30iUser("stale", "Ada", 36)), cluster.CodeSchemaNotCurrent)
	if err := send(staleOther, bson.D{{Key: "_id_", Value: "ok"}, {Key: "Label", Value: "x"}}); err != nil {
		t.Fatalf("a write to a bucket the alter did not touch must be re-admitted and succeed: %v", err)
	}
}

// TestAlterRefusals: a missing bucket, a reserved name, an empty change and a doubled name
// are each refused before anything is written.
func TestAlterRefusals(t *testing.T) {
	r := newU40Rig(t)
	ctx := context.Background()
	_, err := r.alter.Run(ctx, r.dbID, "Nope", u40Add("A", "string"))
	u30iCode(t, err, cluster.CodeNotFound)
	_, err = r.alter.Run(ctx, r.dbID, "_buckets", u40Add("A", "string"))
	u30iCode(t, err, cluster.CodeReservedName)
	_, err = r.alter.Run(ctx, r.dbID, "User", AlterChange{})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	_, err = r.alter.Run(ctx, r.dbID, "User", AlterChange{Add: []FieldEntry{{Name: "A", Type: "string"}}, Drop: []string{"A"}})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	_, err = r.alter.Run(ctx, r.dbID, "User", AlterChange{Drop: []string{"_id_"}})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if len(r.sender.sent) != 0 {
		t.Fatal("a refused alter wrote to the ring")
	}
}

func TestPlanAlterCaps(t *testing.T) {
	base := machine.FieldTable{NextFieldID: 2, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}}}
	for i := 0; i < store.MaxFieldTableEntries-1; i++ {
		var err error
		base, _, err = planAlter(planInput{db: "app", bucket: "B", base: base, change: u40Add(fmt.Sprintf("f%d", i), "int"), epoch: 1})
		if err != nil {
			t.Fatalf("field %d: %v", i, err)
		}
	}
	_, _, err := planAlter(planInput{db: "app", bucket: "B", base: base, change: u40Add("one-too-many", "int"), epoch: 1})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "already declares 999 fields, the maximum") {
		t.Fatalf("cap text = %v", err)
	}
	// At the cap one alter may drop a field and add another.
	swapped, changed, err := planAlter(planInput{db: "app", bucket: "B", base: base, epoch: 1,
		change: AlterChange{Add: []FieldEntry{{Name: "new", Type: "int"}}, Drop: []string{"f0"}}})
	if err != nil || !changed || len(swapped.Entries) != store.MaxFieldTableEntries {
		t.Fatalf("swap at the cap: %v changed %v entries %d", err, changed, len(swapped.Entries))
	}

	exhausted := machine.FieldTable{NextFieldID: 0, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}}}
	_, _, err = planAlter(planInput{db: "app", bucket: "B", base: exhausted, change: u40Add("x", "int"), epoch: 1})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "has used all 65535 field ids") {
		t.Fatalf("exhaustion text = %v", err)
	}
	last := machine.FieldTable{NextFieldID: 65535, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}}}
	next, _, err := planAlter(planInput{db: "app", bucket: "B", base: last, change: u40Add("x", "int"), epoch: 1})
	if err != nil || next.Entries[1].ID != 65535 || next.NextFieldID != 0 {
		t.Fatalf("the last id: %v %+v", err, next)
	}
}

func u40DeclSlot(t *testing.T, name string) uint16 {
	t.Helper()
	encID, err := store.EncodeComponent(name)
	if err != nil {
		t.Fatal(err)
	}
	return storage.SlotOf(storage.PlacementKey(u30iDB, systemBucketName, encID))
}

func u40DocSlot(t *testing.T, node *Node, bucket, id string) uint16 {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, bucket)
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	return ref.slot(encID)
}

// movingBumper refuses its first bump STATE_MOVED, as a commit that raced another command does,
// and passes the rest to the node.
type movingBumper struct {
	node  *Node
	calls int
}

func (b *movingBumper) BumpSchema(ctx context.Context, dbID uint64, from, to int64, note cluster.BumpNote) error {
	b.calls++
	if b.calls == 1 {
		return &cluster.Error{Code: cluster.CodeStateMoved, Message: "refused: the cluster state moved while this command was built"}
	}
	return b.node.BumpSchema(ctx, dbID, from, to, note)
}

// TestAlterRebuildsAfterAStateMovedBump: a bump refused STATE_MOVED changes nothing, the alter
// reads again and writes the same pending table, and the second bump publishes it once.
func TestAlterRebuildsAfterAStateMovedBump(t *testing.T) {
	r := newU40Rig(t)
	moving := &movingBumper{node: r.node}
	config := r.node.alterer.cfg
	config.Sender, config.Bumper = r.sender, moving
	table, err := NewAlterBucket(config).Run(context.Background(), r.dbID, "User", u40Add("Email", "string"))
	if err != nil {
		t.Fatal(err)
	}
	if moving.calls != 2 || len(r.sender.sent) != 2 {
		t.Fatalf("%d bump calls and %d declaration writes, want 2 and 2: a refused bump repeats from the write", moving.calls, len(r.sender.sent))
	}
	if got := u40ID(t, table, "Email"); got != 6 {
		t.Fatalf("Email got id %d, want 6: the rebuilt table must be the same one", got)
	}
	if got := r.node.topology.Published(r.dbID); got != 2 {
		t.Fatalf("published = %d, want 2", got)
	}
}

// TestARepeatDoesNotAnswerWithATableThisNodeHasNotPublished: a repeat of a finished alter reads
// the committed table; while this node has not published the bump it must not answer success,
// because the reply is described from the published table.
func TestARepeatDoesNotAnswerWithATableThisNodeHasNotPublished(t *testing.T) {
	r := newU40Rig(t)
	r.run(u40Add("Email", "string"))
	var lagging atomic.Bool
	lagging.Store(true)
	config := r.node.alterer.cfg
	config.Sender = r.sender
	config.Published = func(dbID uint64) int64 {
		if lagging.Load() {
			return r.node.topology.Published(dbID) - 1
		}
		return r.node.topology.Published(dbID)
	}
	repeat := NewAlterBucket(config)
	if _, err := repeat.Run(context.Background(), r.dbID, "User", u40Add("Email", "string")); err == nil {
		t.Fatal("a repeat answered success while the committed version was ahead of the published one")
	}
	lagging.Store(false)
	if _, err := repeat.Run(context.Background(), r.dbID, "User", u40Add("Email", "string")); err != nil {
		t.Fatalf("once published, the repeat is a no-op success: %v", err)
	}
}

// TestManyCoordinatorsAddingFieldsAllSucceed: eight coordinators each run four alters against
// one bucket at once. Every alter returns nil (concurrent alters converge, they do not give up)
// and the table ends with every field exactly once, each under its own id.
func TestManyCoordinatorsAddingFieldsAllSucceed(t *testing.T) {
	u40ManyCoordinators(t, true)
}

// TestManyCoordinatorsWithoutTheQueueStillConverge: the coordinators of other nodes share no
// queue, so the retry loop and its backoff alone must let three of them finish.
func TestManyCoordinatorsWithoutTheQueueStillConverge(t *testing.T) {
	u40ManyCoordinators(t, false)
}

func u40ManyCoordinators(t *testing.T, queued bool) {
	r := newU40Rig(t)
	coordinators, each := 8, 4
	if !queued {
		coordinators, each = 3, 3
	}
	var wg sync.WaitGroup
	for c := range coordinators {
		config := r.node.alterer.cfg
		config.Sender = r.sender
		if !queued {
			config.Queue = nil
		}
		coordinator := NewAlterBucket(config)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				name := fmt.Sprintf("c%d_f%d", c, i)
				if _, err := coordinator.Run(context.Background(), r.dbID, "User", u40Add(name, "string")); err != nil {
					t.Errorf("adding %s: %v", name, err)
				}
			}
		}()
	}
	wg.Wait()
	table, _ := machine.EffectiveTable(r.declaration(), r.node.topology.Published(r.dbID))
	if want := 5 + coordinators*each; len(table.Entries) != want {
		t.Fatalf("the table holds %d fields, want %d", len(table.Entries), want)
	}
	names, ids := map[string]bool{}, map[uint16]bool{}
	for _, entry := range table.Entries {
		if names[entry.Name] || ids[entry.ID] {
			t.Fatalf("field %s (id %d) appears twice", entry.Name, entry.ID)
		}
		names[entry.Name], ids[entry.ID] = true, true
	}
}

// TestAlterBackoffIsBoundedAndJittered: the wait grows with the attempt, never passes the cap,
// never drops under half the exponential delay, and the same seed gives the same waits.
func TestAlterBackoffIsBoundedAndJittered(t *testing.T) {
	draw := func(seed uint64) []time.Duration {
		rng := rand.New(rand.NewPCG(seed, seed))
		var waits []time.Duration
		for attempt := range 12 {
			waits = append(waits, alterBackoff(attempt, rng))
		}
		return waits
	}
	a, b, other := draw(7), draw(7), draw(8)
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatal("one seed gave two sequences")
	}
	if fmt.Sprint(a) == fmt.Sprint(other) {
		t.Fatal("two seeds gave one sequence: there is no jitter")
	}
	for attempt, wait := range a {
		full := min(alterBackoffBase<<min(attempt, 6), alterBackoffCap)
		if wait < full/2 || wait > full {
			t.Fatalf("attempt %d waits %v, want %v to %v", attempt, wait, full/2, full)
		}
	}
}

// TestAlterQueueTakesTurnsAndHonoursTheContext: a second alter of one database waits for the
// first, another database does not, and a cancelled wait returns.
func TestAlterQueueTakesTurnsAndHonoursTheContext(t *testing.T) {
	var queue AlterQueue
	release, err := queue.wait(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	other, err := queue.wait(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	other()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := queue.wait(ctx, 1); err == nil {
		t.Fatal("a second alter of the same database ran while the first held its turn")
	}
	release()
	again, err := queue.wait(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	again()
}
