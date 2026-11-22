package wire

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// bug0759Named is a world whose database and bucket have names, as a running cluster's do.
func bug0759Named(t *testing.T) *u36World {
	t.Helper()
	world := newU36World(t)
	world.fx.topo.dbRow.Name = "app"
	world.fx.schema.name = "users"
	world.withStoreSchema()
	return world
}

// bug0759Names fails when a read refusal prints the database or bucket as a number, or leaves a
// placeholder, or speaks of writes.
func bug0759Names(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: no refusal", what)
	}
	text := err.Error()
	if !strings.Contains(text, "app.users") {
		t.Errorf("%s: the refusal does not name the bucket as app.users: %s", what, text)
	}
	for _, id := range []string{"database 3", "database 41", " 3.41"} {
		if strings.Contains(text, id) {
			t.Errorf("%s: the refusal prints an id (%q) where the names are known: %s", what, id, text)
		}
	}
	for _, bad := range []string{"<db>", "<bucket>", "<owner>", "<id>", "<seq>", "<cause>", "<node>", "<slot>", "<e>", "<epoch>", "paused"} {
		if strings.Contains(text, bad) {
			t.Errorf("%s: the refusal contains %q: %s", what, bad, text)
		}
	}
}

// TestAReadRefusalNamesTheDatabaseAndBucket (BUG-0759): the holds and reroute refusals of a store
// read, a scan and a document read say app.users, not the ids the gate holds.
func TestAReadRefusalNamesTheDatabaseAndBucket(t *testing.T) {
	t.Run("a held store read", func(t *testing.T) {
		node, slot := u36Settled(t)
		h := newU36Handoff(t, node, slot, true)
		h.topo.name = "app"
		h.fx.schema.name = "users"
		h.node.pairs.With(slot, u33DB, func(p *cluster.Pair) { p.Flags = flagStopProved }) // log tail not replayed
		_, err := h.reader.CountField(context.Background(), FieldRead{Request: h.fieldRequest(), FieldID: 5, Engine: store.FieldFIFO})
		bug0759Names(t, "count", err)
		if !strings.Contains(err.Error(), "replayed") || !strings.Contains(err.Error(), "Nothing was returned") {
			t.Errorf("the hold refusal must say what is held and that nothing was returned: %v", err)
		}
	})
	t.Run("a held scan", func(t *testing.T) {
		world := bug0759Named(t)
		world.fx.setFlags(world.slot, flagStopProved)
		bug0759Names(t, "scan", bug0764Scan(world, world.slot))
	})
	t.Run("a second hop", func(t *testing.T) {
		world := bug0759Named(t)
		world.insertWithStore("p1", 0, 0)
		req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", Hop: 1}
		_, err := bug0759Copy(world).CountField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO})
		u36Refusal(t, err, cluster.CodeNotOwner)
		bug0759Names(t, "second hop", err)
	})
	t.Run("a missing document", func(t *testing.T) {
		world := bug0759Named(t)
		req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("ghost"), ID: "ghost"}
		_, err := world.reader.CountField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO})
		u36Refusal(t, err, cluster.CodeNotFound)
		bug0759Names(t, "missing document", err)
	})
}

func bug0759Copy(world *u36World) *Reader { return bug0766Copy(world) }
