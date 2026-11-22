package cluster

import (
	"math/rand"
	"reflect"
	"testing"

	hraft "github.com/hashicorp/raft"
)

const u43DB int64 = 2

func u43NoteState(t *testing.T) ClusterState {
	t.Helper()
	st := fsmCluster(t)
	if st.NextDatabaseID != u43DB {
		t.Fatalf("the next database id is %d, the tests assume %d", st.NextDatabaseID, u43DB)
	}
	fsmApply(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("shop", st.NextDatabaseID, 1))
	return st
}

func u43Apply(t *testing.T, fsm *clusterFSM, body []byte) applyOutcome {
	t.Helper()
	raw, err := EncodeCommand(Command{Version: CommandVersion, Kind: CommandBumpSchema, BaseSeq: uint64(fsm.state.StateSeq), Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return fsm.Apply(&hraft.Log{Data: raw}).(applyOutcome)
}

// TestABumpNoteRoundTrips: both shapes of note survive the body, and the command accepts them.
func TestABumpNoteRoundTrips(t *testing.T) {
	for _, note := range []BumpNote{
		{BucketID: 0x1f4a2c9b7de30015, Retired: []uint16{5, 9}, Epoch: 12},
		{BucketID: 7, Drop: true},
	} {
		st := u43NoteState(t)
		body := BumpSchemaNoteBody(u43DB, 1, 2, note)
		got, ok := ParseBumpNote(body)
		if !ok || !reflect.DeepEqual(got, note) {
			t.Fatalf("note %+v came back as %+v (ok %v)", note, got, ok)
		}
		fsmApply(t, &st, CommandBumpSchema, body)
	}
}

// TestABumpWithoutANoteHasNone: the plain body names no bucket, so no node sweeps anything.
func TestABumpWithoutANoteHasNone(t *testing.T) {
	if note, ok := ParseBumpNote(BumpSchemaBody(2, 1, 2)); ok {
		t.Fatalf("a plain bump parsed as a note: %+v", note)
	}
}

// TestAHalfFormedNoteIsRefused: a note that names no work, both kinds of work, a bad id list
// or stray members is refused by the command, so every node refuses it the same way.
func TestAHalfFormedNoteIsRefused(t *testing.T) {
	bad := []BumpNote{
		{BucketID: 9},
		{BucketID: 9, Drop: true, Retired: []uint16{4}, Epoch: 1},
		{BucketID: 9, Retired: []uint16{1}, Epoch: 1},
		{BucketID: 9, Retired: []uint16{6, 6}, Epoch: 1},
		{BucketID: 9, Retired: []uint16{7, 6}, Epoch: 1},
		{BucketID: 9, Retired: make([]uint16, MaxBumpRetired+1), Epoch: 1},
	}
	for i := range bad[len(bad)-1].Retired {
		bad[len(bad)-1].Retired[i] = uint16(i + 2)
	}
	for _, note := range bad {
		st := u43NoteState(t)
		if _, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandBumpSchema, BaseSeq: uint64(st.StateSeq), Body: BumpSchemaNoteBody(u43DB, 1, 2, note)}); err == nil {
			t.Fatalf("note %+v was accepted", note)
		}
	}
}

// TestPublisherIsToldTheNoteAfterPublication: the FSM tells the publisher the note only after
// the version is published, so a sweep never starts against a node that still fences the old
// table.
func TestPublisherIsToldTheNoteAfterPublication(t *testing.T) {
	st := u43NoteState(t)
	publisher := &recordingPublisher{staged: map[uint64]int64{}, published: map[uint64]int64{}}
	fsm := &clusterFSM{schema: publisher, state: st}
	note := BumpNote{BucketID: 5, Retired: []uint16{4}, Epoch: 3}
	if outcome := u43Apply(t, fsm, BumpSchemaNoteBody(u43DB, 1, 2, note)); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if len(publisher.notes) != 1 || !reflect.DeepEqual(publisher.notes[0].note, note) || publisher.notes[0].db != uint64(u43DB) || publisher.notes[0].to != 2 {
		t.Fatalf("notes = %+v, want %+v for database 2 at 2", publisher.notes, note)
	}
	if !reflect.DeepEqual(publisher.order, []string{"publish", "note"}) {
		t.Fatalf("order = %v, want publish then note", publisher.order)
	}
}

// TestARefusedBumpTellsNoOne: a bump the state refuses (a stale from) publishes nothing and
// delivers no note.
func TestARefusedBumpTellsNoOne(t *testing.T) {
	st := u43NoteState(t)
	publisher := &recordingPublisher{staged: map[uint64]int64{}, published: map[uint64]int64{}}
	fsm := &clusterFSM{schema: publisher, state: st}
	note := BumpNote{BucketID: 5, Drop: true}
	if outcome := u43Apply(t, fsm, BumpSchemaNoteBody(u43DB, 7, 8, note)); outcome.err == nil {
		t.Fatal("a bump from a version that is not the committed one was accepted")
	}
	if len(publisher.notes) != 0 {
		t.Fatalf("a refused bump delivered %+v", publisher.notes)
	}
}

// TestBumpNoteMutations: 100000 mutations of a valid body never panic, and a body that still
// parses as a note re-encodes to a body that parses to the same note.
func TestBumpNoteMutations(t *testing.T) {
	rng := rand.New(rand.NewSource(43))
	seeds := [][]byte{
		BumpSchemaNoteBody(u43DB, 1, 2, BumpNote{BucketID: 0x1f4a2c9b7de30015, Retired: []uint16{5, 9, 300}, Epoch: 12}),
		BumpSchemaNoteBody(u43DB, 1, 2, BumpNote{BucketID: 7, Drop: true}),
	}
	for range 100_000 {
		body := append([]byte(nil), seeds[rng.Intn(len(seeds))]...)
		for range 1 + rng.Intn(3) {
			body[rng.Intn(len(body))] ^= byte(1 + rng.Intn(255))
		}
		note, ok := ParseBumpNote(body)
		if !ok {
			continue
		}
		again, ok := ParseBumpNote(BumpSchemaNoteBody(u43DB, 1, 2, note))
		if !ok || !reflect.DeepEqual(again, note) {
			t.Fatalf("a parsed note %+v re-encoded as %+v (ok %v)", note, again, ok)
		}
	}
}
