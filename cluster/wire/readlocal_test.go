package wire

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// copyReaderOf is a reader on n2, a copy of world.slot, over the same Badger contents. stale
// is its cluster.stale_reads setting (nil: false).
func copyReaderOf(world *u36World, lease ServeChecker, stale func() bool) *Reader {
	return copyReaderWithPairs(world, lease, stale, &cluster.PairSet{})
}

// copyReaderWithPairs is copyReaderOf over the copy's own pair set, so a test can set the flags
// a copy carries after a failed apply or a restart.
func copyReaderWithPairs(world *u36World, lease ServeChecker, stale func() bool, pairs *cluster.PairSet) *Reader {
	copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
	gate := NewReadGate(ReadGateConfig{Topology: copyTopo, Pairs: pairs, Clock: fixedClock{}, Lease: lease, Schema: world.fx.schema, Slots: world.states})
	return NewReader(ReaderConfig{Gate: gate, KV: world.fx.kv, Holder: world.barrier, PeerReachable: func(uint16, uint64) bool { return true },
		Forward: world.fwd, Self: "n2", StaleReads: stale})
}

func (w *u36World) localRead(reader *Reader, id string) (Document, error) {
	return reader.Read(context.Background(), ReadRequest{Slot: w.slot, DBID: testDB, BucketID: testBucket, EncID: w.encID(id), Local: true})
}

// TestLocalReadIsServedFromAReadyCopyAndSaysSo: [GUARD a local read of a ready copy does not cross the network].
func TestLocalReadIsServedFromAReadyCopyAndSaysSo(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	doc, err := world.localRead(copyReaderOf(world, world.fx.lease, nil), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Served != servedLocal || doc.ServedBy != "n2" || world.fwd.count() != 0 {
		t.Fatalf("served %q by %q after %d forwards, want local by n2 and none", doc.Served, doc.ServedBy, world.fwd.count())
	}
	owned, err := world.get("p1")
	if err != nil || owned.Served != servedOwner {
		t.Fatalf("the owner's read says %q, %v", owned.Served, err)
	}
}

// TestLocalReadFallsBackToTheOwner: [GUARD a seeding copy never answers].
func TestLocalReadFallsBackToTheOwner(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	world.states.set(world.slot, machine.SlotSeeding)
	doc, err := world.localRead(copyReaderOf(world, world.fx.lease, nil), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Served != servedOwner || world.fwd.count() != 1 || doc.ServedBy != "n1" {
		t.Fatalf("served %q by %q after %d forwards, want one forward to the owner", doc.Served, doc.ServedBy, world.fwd.count())
	}
}

// TestLocalReadOfAMissingDocumentAsksTheOwner: [GUARD a copy that is only behind never says not found].
func TestLocalReadOfAMissingDocumentAsksTheOwner(t *testing.T) {
	world := newU36World(t)
	doc, err := world.localRead(copyReaderOf(world, world.fx.lease, nil), "never-written")
	if err != nil || doc.Served != servedOwner || world.fwd.count() != 1 {
		t.Fatalf("served %q, %d forwards, %v", doc.Served, world.fwd.count(), err)
	}
}

// TestLocalReadDoesNotAnswerOnceTheLeaseLapsedUnderTheRead: [GUARD a copy that loses its lease
// under the read does not answer]. The declaration-bump half is
// TestCopyVerdictRefusesAReadWhoseDeclarationVersionMoved.
func TestLocalReadDoesNotAnswerOnceTheLeaseLapsedUnderTheRead(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	lapsing := &flakyLease{validFor: 1}
	doc, err := world.localRead(copyReaderOf(world, lapsing, nil), "p1")
	if err == nil && doc.Served == servedLocal {
		t.Fatalf("a copy whose lease lapsed during the read answered it: %+v", doc)
	}
}

// TestStaleReadsSettingServesKeyedReadsFromACopy: [GUARD cluster.stale_reads reaches keyed reads].
func TestStaleReadsSettingServesKeyedReadsFromACopy(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	read := func(stale bool) Document {
		reader := copyReaderOf(world, world.fx.lease, func() bool { return stale })
		doc, err := reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("p1"))
		if err != nil {
			t.Fatal(err)
		}
		return doc
	}
	if doc := read(false); doc.Served != servedOwner {
		t.Fatalf("with stale_reads off the read was served %q", doc.Served)
	}
	if doc := read(true); doc.Served != servedLocal {
		t.Fatalf("with stale_reads on the read was served %q", doc.Served)
	}
}

// TestLocalReadRefusesACopyThatStoppedApplying: [GUARD a copy fenced for replay is stale without
// bound, so it never answers: flagReplayNeeded, flagBootReplay, and a record asked to rebuild].
func TestLocalReadRefusesACopyThatStoppedApplying(t *testing.T) {
	for name, setup := range map[string]func(world *u36World, pairs *cluster.PairSet){
		"replay needed": func(world *u36World, pairs *cluster.PairSet) {
			pairs.With(world.slot, testDB, func(pair *cluster.Pair) { pair.Flags |= flagReplayNeeded })
		},
		"boot replay": func(world *u36World, pairs *cluster.PairSet) {
			pairs.With(world.slot, testDB, func(pair *cluster.Pair) { pair.Flags |= flagReplayNeeded | flagBootReplay })
		},
		"asked to rebuild": func(world *u36World, _ *cluster.PairSet) { world.states.setRebuilding(world.slot) },
	} {
		t.Run(name, func(t *testing.T) {
			world := newU36World(t)
			world.fx.mustWrite(world.slot, "p1")
			pairs := &cluster.PairSet{}
			setup(world, pairs)
			doc, err := world.localRead(copyReaderWithPairs(world, world.fx.lease, nil, pairs), "p1")
			if err != nil {
				t.Fatal(err)
			}
			if doc.Served != servedOwner || world.fwd.count() != 1 {
				t.Fatalf("served %q after %d forwards, want the owner's answer after one forward", doc.Served, world.fwd.count())
			}
		})
	}
}

// TestAForwardedReadIsNotAnsweredByACopy: [GUARD a node frame asked the owner by name, so a copy
// never answers it from its own store, even with stale_reads on and a frame epoch equal to the
// committed epoch (owner ruling 2026-11-08)].
func TestAForwardedReadIsNotAnsweredByACopy(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	reader := copyReaderOf(world, world.fx.lease, func() bool { return true })
	doc, err := reader.Read(context.Background(), ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), FromNode: true, FrameEpoch: testEpoch, Local: true})
	if err == nil && doc.Served == servedLocal {
		t.Fatalf("a forwarded read was answered from the copy: %+v", doc)
	}
}

// TestServedFromIsSetWhenACopyAnswers: [GUARD a get that no call asked to be local still says
// served local when cluster.stale_reads made a copy answer; a plain owner get says nothing, so
// the wire of every ordinary get is unchanged].
func TestServedFromIsSetWhenACopyAnswers(t *testing.T) {
	if got := servedMember(false, Document{Served: servedLocal}); got != servedLocal {
		t.Errorf("a copy answer without local says %q, want local", got)
	}
	if got := servedMember(false, Document{Served: servedOwner}); got != "" {
		t.Errorf("an ordinary owner get says %q, want nothing", got)
	}
	if got := servedMember(true, Document{Served: servedOwner}); got != servedOwner {
		t.Errorf("a local get answered by the owner says %q, want owner", got)
	}
}
