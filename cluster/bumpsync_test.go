package cluster

import (
	"bytes"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

// u41Hold holds the lock of pair (slot, db) until release is closed, and reports when it has it.
func u41Hold(set *PairSet, slot uint16, db uint64) (held <-chan struct{}, release func()) {
	acquired := make(chan struct{})
	free := make(chan struct{})
	go set.With(slot, db, func(*Pair) {
		close(acquired)
		<-free
	})
	var once sync.Once
	return acquired, func() { once.Do(func() { close(free) }) }
}

func u41Wait(t *testing.T, what string, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func u41NotYet(t *testing.T, what string, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
		t.Fatalf("%s happened while a pair lock it must wait for was still held", what)
	case <-time.After(150 * time.Millisecond):
	}
}

func u41Published(t *testing.T, b *BumpApplier, db uint64) int64 {
	t.Helper()
	version, ok := b.Published(db)
	if !ok {
		t.Fatalf("database %d has no published version", db)
	}
	return version
}

// TestBumpPublishesPerPair: the new version is published inside a pair's lock cycle, after
// the lock is acquired and before the next pair is reached. A writer that holds a pair
// lock therefore finishes first and is ordered before the bump for that pair.
func TestBumpPublishesPerPair(t *testing.T) {
	var pairs PairSet
	var bumps BumpApplier
	const db = 7
	bumps.Stage(db, 4)
	for _, slot := range []uint16{3, 9} {
		pairs.With(slot, db, func(*Pair) {})
	}
	held3, release3 := u41Hold(&pairs, 3, db)
	held9, release9 := u41Hold(&pairs, 9, db)
	defer release3()
	defer release9()
	u41Wait(t, "slot 3 lock", held3)
	u41Wait(t, "slot 9 lock", held9)

	done := make(chan struct{})
	go func() {
		bumps.Apply(db, 5, &pairs)
		close(done)
	}()
	u41NotYet(t, "the publication", done)
	if got := u41Published(t, &bumps, db); got != 4 {
		t.Fatalf("published = %d while the first pair's lock is held, want 4: publishing before the pair cycle lets a writer inside the section see the new version", got)
	}
	release3()
	deadline := time.Now().Add(5 * time.Second)
	for u41Published(t, &bumps, db) != 5 {
		if time.Now().After(deadline) {
			t.Fatal("the version was not published in the first pair's cycle: publishing after the last lock is released leaves every other pair on the old version until the whole pass ends")
		}
		time.Sleep(time.Millisecond)
	}
	u41NotYet(t, "the end of the pass", done)
	release9()
	u41Wait(t, "the pass to end", done)
}

// TestBumpTakesEveryHeldPairLockInSlotOrder: the pass waits for the lowest held slot first,
// then the next, and returns only after the last; a database with no held pair still
// publishes, and another database's pairs are not touched.
func TestBumpTakesEveryHeldPairLockInSlotOrder(t *testing.T) {
	var pairs PairSet
	var bumps BumpApplier
	const db, other = 2, 3
	bumps.Stage(db, 1)
	for _, slot := range []uint16{40, 5, 8191, 0} {
		pairs.With(slot, db, func(*Pair) {})
	}
	pairs.With(6, other, func(*Pair) {})
	held, release := map[uint16]<-chan struct{}{}, map[uint16]func(){}
	for _, slot := range []uint16{0, 40, 8191} {
		held[slot], release[slot] = u41Hold(&pairs, slot, db)
		defer release[slot]()
		u41Wait(t, "a pair lock", held[slot])
	}
	otherHeld, otherRelease := u41Hold(&pairs, 6, other)
	defer otherRelease()
	u41Wait(t, "the other database's lock", otherHeld)

	done := make(chan struct{})
	go func() {
		bumps.Apply(db, 2, &pairs)
		close(done)
	}()
	// Releasing the highest slot first changes nothing: the pass is waiting at slot 0, the
	// lowest, and has published nothing yet.
	release[8191]()
	u41NotYet(t, "the pass", done)
	if got := u41Published(t, &bumps, db); got != 1 {
		t.Fatalf("published = %d after only slot 8191 was released, want 1: the pass must take slot 0 before slot 8191", got)
	}
	release[0]()
	deadline := time.Now().Add(5 * time.Second)
	for u41Published(t, &bumps, db) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("slot 0 was released and the version was still not published: the pass is not taking the lowest slot first")
		}
		time.Sleep(time.Millisecond)
	}
	u41NotYet(t, "the pass", done)
	release[40]()
	u41Wait(t, "the pass", done)
	if got := u41Published(t, &bumps, db); got != 2 {
		t.Fatalf("published = %d after the pass, want 2", got)
	}
	if got, ok := bumps.Published(other); ok {
		t.Fatalf("database %d was published at %d by a bump of database %d", other, got, db)
	}

	var empty PairSet
	bumps.Stage(9, 1)
	bumps.Apply(9, 2, &empty)
	if got := u41Published(t, &bumps, 9); got != 2 {
		t.Fatalf("a node holding no pair of the database must still publish: published = %d, want 2", got)
	}
}

// TestABumpMovesNoEpochOrSlot: a declaration change is not a ring change (conv 20 step 4). The
// bump moves schema_version of its own database by one and state_seq by one, and leaves the
// epoch, every member, every other database and the slot table as they were.
func TestABumpMovesNoEpochOrSlot(t *testing.T) {
	st := fsmCluster(t)
	fsmApply(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("shop", st.NextDatabaseID, 2))
	before := st.clone()
	tableBefore := BuildTable(before)

	res := fsmApply(t, &st, CommandBumpSchema, BumpSchemaBody(1, 1, 2))

	if res.EpochMoved || st.Epoch != before.Epoch {
		t.Fatalf("epoch %d to %d (moved %v): a schema bump must not move the epoch", before.Epoch, st.Epoch, res.EpochMoved)
	}
	if st.StateSeq != before.StateSeq+1 {
		t.Fatalf("state_seq %d to %d, want exactly one", before.StateSeq, st.StateSeq)
	}
	if !reflect.DeepEqual(st.Members, before.Members) {
		t.Fatalf("a schema bump changed the members:\n%+v\nto\n%+v", before.Members, st.Members)
	}
	if !reflect.DeepEqual(BuildTable(st), tableBefore) {
		t.Fatal("a schema bump changed the slot table")
	}
	if st.Databases[0].SchemaVersion != 2 || st.Databases[1].SchemaVersion != before.Databases[1].SchemaVersion {
		t.Fatalf("schema versions = %d and %d, want 2 and %d", st.Databases[0].SchemaVersion, st.Databases[1].SchemaVersion, before.Databases[1].SchemaVersion)
	}
}

type recordingPublisher struct {
	staged    map[uint64]int64
	published map[uint64]int64
	forgotten []uint64
	notes     []recordedNote
	order     []string
}

type recordedNote struct {
	db   uint64
	to   int64
	note BumpNote
}

func (r *recordingPublisher) Stage(db uint64, version int64) { r.staged[db] = version }
func (r *recordingPublisher) Publish(db uint64, to int64) {
	r.published[db] = to
	r.order = append(r.order, "publish")
}
func (r *recordingPublisher) Forget(db uint64) { r.forgotten = append(r.forgotten, db) }
func (r *recordingPublisher) Note(db uint64, to int64, note BumpNote) {
	r.notes = append(r.notes, recordedNote{db: db, to: to, note: note})
	r.order = append(r.order, "note")
}

// TestRestoreForgetsDatabasesTheSnapshotDrops: a node that installs a snapshot taken after a
// database was dropped must forget that database's published word, as applying the drop does.
func TestRestoreForgetsDatabasesTheSnapshotDrops(t *testing.T) {
	st := fsmCluster(t)
	fsmApply(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("shop", st.NextDatabaseID, 2))
	publisher := &recordingPublisher{staged: map[uint64]int64{}, published: map[uint64]int64{}}
	fsm := &clusterFSM{schema: publisher, state: st}

	after := st.clone()
	after.Databases = after.Databases[:1]
	raw, err := EncodeSnapshot(after)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	if len(publisher.forgotten) != 1 || publisher.forgotten[0] != 2 {
		t.Fatalf("forgotten = %v, want [2]: the dropped database's word survives the snapshot", publisher.forgotten)
	}
	if publisher.published[1] != 1 {
		t.Fatalf("the surviving database was not published: %v", publisher.published)
	}
}
