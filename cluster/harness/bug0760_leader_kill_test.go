package harness_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

type k9Saved struct {
	record []byte
	keys   map[string][]byte
}

func k9End(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func k9Open(t *testing.T, dir string) *storage.BadgerKV {
	kv, err := storage.OpenBadgerKV(filepath.Join(dir, "badger"), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("open badger %s: %v", dir, err)
	}
	return kv
}

func k9Save(t *testing.T, dir string, slot uint16, db uint64) k9Saved {
	kv := k9Open(t, dir)
	defer func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	}()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	s := k9Saved{keys: map[string][]byte{}}
	rec, err := snap.Get(wire.LocalPairRecordKey(slot, db))
	if err != nil {
		t.Fatalf("pair record: %v", err)
	}
	s.record = append([]byte(nil), rec...)
	prefix := storage.PairPrefix(slot, db)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: k9End(prefix)})
	defer it.Close()
	for it.Seek(prefix); it.Valid(); it.Next() {
		v, err := it.Value()
		if err != nil {
			t.Fatal(err)
		}
		s.keys[string(it.Key())] = append([]byte(nil), v...)
	}
	return s
}

func k9Rewind(t *testing.T, dir string, slot uint16, db uint64, s k9Saved) {
	kv := k9Open(t, dir)
	defer func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	}()
	snap := kv.NewSnapshot()
	prefix := storage.PairPrefix(slot, db)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: k9End(prefix)})
	b := kv.NewBatch()
	defer b.Discard()
	for it.Seek(prefix); it.Valid(); it.Next() {
		if err := b.Delete(append([]byte(nil), it.Key()...)); err != nil {
			t.Fatal(err)
		}
	}
	it.Close()
	snap.Discard()
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b2 := kv.NewBatch()
	defer b2.Discard()
	if err := b2.Set(wire.LocalPairRecordKey(slot, db), s.record); err != nil {
		t.Fatal(err)
	}
	for k, v := range s.keys {
		if err := b2.Set([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
	if err := b2.Commit(true); err != nil {
		t.Fatal(err)
	}
}

func k9Items(raw [][]byte) []string {
	var out []string
	for _, el := range raw {
		for i := 1; i <= 9; i++ {
			if bytes.Contains(el, []byte(fmt.Sprintf("e%d\x00", i))) {
				out = append(out, fmt.Sprintf("e%d", i))
			}
		}
	}
	return out
}

// TestAKilledLeaderOwnerRestartsWithUnappliedPushesAndPop: three nodes, RF 2, the owner of a doc killed
// with pushes and a pop in its log that its pair record has not counted, declaration slot owned
// by ANOTHER node (remote route).
func TestAKilledLeaderOwnerRestartsWithUnappliedPushesAndPop(t *testing.T) {
	ctx := context.Background()
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	leaderID := c.AwaitLeader(10 * time.Second)
	leader := c.Node(leaderID)
	dbID, err := leader.CreateDatabaseRF(ctx, "k9", 2)
	if err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(ctx, "k9", "Box", wire.Field{Name: "Name", Type: "string"}, wire.Field{Name: "Queue", Type: "fifoStore"}); err != nil {
		t.Fatal(err)
	}
	encName, _ := store.EncodeComponent("Box")
	declSlot := storage.SlotOf(storage.PlacementKey("k9", "_buckets", encName))
	declOwner := ""
	probe := c.Node(c.IDs()[0])
	for i := 0; i < 2_000_000 && declOwner == ""; i++ {
		id := fmt.Sprintf("x%d", i)
		if s, _ := wire.PlacementOf("k9", "Box", id); s == declSlot {
			declOwner, _, err = probe.Owner(ctx, "k9", "Box", id)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var docID, owner string
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("d%d", i)
		o, _, err := probe.Owner(ctx, "k9", "Box", id)
		if err == nil && o == leaderID {
			docID, owner = id, o
			break
		}
	}
	slot, _ := wire.PlacementOf("k9", "Box", docID)
	t.Logf("decl slot %d owned by %q; doc %s slot %d owned by %s", declSlot, declOwner, docID, slot, owner)
	if _, err := c.Node(owner).Insert(ctx, "k9", "Box", bson.D{{Key: "_id_", Value: docID}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	c.Kill(owner)
	before := k9Save(t, c.DataDir(owner), slot, dbID)
	c.Restart(owner)
	target := wire.StoreTarget{Database: "k9", Bucket: "Box", ID: docID, Field: "Queue"}
	retry := func(what string, f func() error) {
		deadline := time.Now().Add(30 * time.Second)
		for {
			err := f()
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %v (bg %v)", what, err, c.BackgroundErrors())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	for i := 1; i <= 5; i++ {
		item := fmt.Sprintf("e%d", i)
		retry("push "+item, func() error {
			return c.Node(owner).Push(ctx, wire.PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: item}}})
		})
	}
	retry("pop", func() error {
		p, err := c.Node(owner).Pop(ctx, wire.PopRequest{StoreTarget: target})
		if err == nil && !p.Found {
			return fmt.Errorf("pop found nothing")
		}
		return err
	})
	c.Kill(owner)
	k9Rewind(t, c.DataDir(owner), slot, dbID, before)
	c.Restart(owner)
	want := []string{"e2", "e3", "e4", "e5"}
	for _, via := range []string{owner} {
		var got []string
		deadline := time.Now().Add(30 * time.Second)
		for {
			raw, err := c.Node(via).Peek(ctx, wire.PeekRequest{StoreTarget: target, Count: 100})
			if err == nil {
				got = k9Items(raw)
				break
			}
			if time.Now().After(deadline) {
				var ce *cluster.Error
				if errors.As(err, &ce) {
					t.Logf("cluster.Error code=%v message=%q", ce.Code, ce.Message)
				}
				t.Fatalf("peek via %s: %v (bg %v)", via, err, c.BackgroundErrors())
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Logf("peek via %s = %v", via, got)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("peek via %s = %v, want %v", via, got, want)
		}
	}
	// Local store of the restarted owner, not only what the copy serves.
	raw, err := c.Node(owner).Peek(ctx, wire.PeekRequest{StoreTarget: target, Count: 100})
	if err != nil {
		t.Fatalf("peeking the restarted owner: %v", err)
	}
	t.Logf("owner peek: %v; background: %v", k9Items(raw), c.BackgroundErrors())
}
