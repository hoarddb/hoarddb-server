package wire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// BUG-0824: the store's applies are not a prefix of the log across a memtable rotation. Badger's
// DB.Sync syncs the active memtable's log before the log of a memtable that was rotated out and is
// still waiting for its flush, so a power loss between the two keeps a later write of a pair, and
// the pair record that names its position, without an earlier one. The boot replay skips every
// record at or below the position the pair record names (cluster/replay.go:Replayer.deliver), so
// the earlier write, acknowledged and held by the node's own log, is never applied again.
//
// The store half (that Badger leaves such an image) is reproduced inside Badger's package, where the
// hook is: third_party/badger/db_sync_order_linux_test.go:TestSyncCoversRotatedMemtableLogBeforeActive.
// This file reproduces the node half on that image.
//
// State after the store fix of 2026-11-15 (DB.Sync syncs the rotated logs oldest first, then the
// active one; the badger sync order patch decision): a power cut inside DB.Sync no longer
// leaves this image, and the tests in Badger's package carry that. The image itself is still
// reachable: the kernel's own writeback can write the newer log's pages before the older log's, with
// or without a Sync in flight, and nothing orders that; a write error on the older log followed by a
// restart leaves it too. The node half below is therefore still a defect, tracked as BUG-0827 (the
// part BUG-0824 left open), and the test is skipped, not deleted: it is the failing test of
// whichever fix closes that bug (a replay that does not trust a non-prefix store turns it green; a
// sync at the rotation makes its image unreachable by writeback and retires it).

// bug0824MemLogs lists the memtable logs of the node's store, oldest first.
func bug0824MemLogs(t *testing.T, dir string) []string {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dir, "badger", "*.mem"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(logs)
	return logs
}

// bug0824Insert inserts a User document with the given Name and returns its acknowledgement.
func bug0824Insert(t *testing.T, node *Node, id, name string) Inserted {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		inserted, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: name}})
		if err == nil {
			return inserted
		}
		if !errors.Is(err, ErrPairBusy) || time.Now().After(deadline) {
			t.Fatalf("Insert %s: %v", id, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// bug0824CopyFile copies one file's current bytes, as the page cache holds them.
func bug0824CopyFile(t *testing.T, from, to string) {
	t.Helper()
	content, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestReplayRecoversAckedWriteLostInRotatedMemtableLog boots a node on the image a power loss
// inside DB.Sync leaves and reads back every acknowledged write of one pair.
//
// The pair takes three inserts, each acknowledged (the node's log is synced through it before the
// answer): "before", then "early" into memtable log A, then, after other documents filled A and
// rotated it out, "late" into the new log B. The image is built from two copies of the store's
// files, taken at moments the test controls, so it does not depend on how fast the flusher runs:
//
//   - the whole store right after a store sync, before "early": what the disk is certain to hold of
//     log A, its tables and its manifest until the next sync that covers them;
//   - log B after "late" and a store sync: B synced.
//
// Together they are the store at the instant DB.Sync has synced B and not yet A, with A's flush
// not finished: B whole, A as of its last sync. The node's own log and its Raft state are copied
// whole after a clean stop; they were synced before each acknowledgement.
//
// Today the node boots, the pair's record names the position of "late", the replay passes "early"
// in the log and skips it by position, and "early" is gone.
func TestReplayRecoversAckedWriteLostInRotatedMemtableLog(t *testing.T) {
	t.Skip("FINDING: on a store image that keeps a later write of a pair and its pair record without an earlier one, " +
		"the boot replay skips the earlier, acknowledged write by position and it is gone. DB.Sync no longer leaves that image " +
		"(the store fix of 2026-11-15, BUG-0824); kernel writeback of the newer memtable log ahead of a rotated one still can, " +
		"and so can a write error on the older log followed by a restart. Fails 20 of 20 when run; tracked as BUG-0827")
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := s0Schema(t, node)
	s0AwaitFirstPass(t, node, dbID)
	ids, slot := s0IDsInOneSlot(t, 3)
	before, early, late := ids[0], ids[1], ids[2]

	// Another pair holds a record with a log cursor below every write of this test's pair, so the
	// boot scan starts below "early" and the replay reads its record: the loss asserted below is
	// then the skip by position, not a scan that started past it.
	const anchor = "bug-0824-anchor"
	if s0SlotOf(t, anchor) == slot {
		t.Fatalf("the anchor document is in the pair under test (slot %d); pick another id", slot)
	}
	bug0824Insert(t, node, anchor, anchor)
	bug0824Insert(t, node, before, before)

	// The first copy: the store as its last sync left it. Log A is the active one.
	if err := node.kv.Sync(); err != nil {
		t.Fatalf("syncing the store: %v", err)
	}
	logsAtSync := bug0824MemLogs(t, dir)
	if len(logsAtSync) != 1 {
		t.Fatalf("the store has the memtable logs %v after its sync, want one active log", logsAtSync)
	}
	rotatedLog := logsAtSync[0]
	image := t.TempDir()
	imageStore := filepath.Join(image, "badger")
	if err := os.Mkdir(imageStore, 0o700); err != nil {
		t.Fatal(err)
	}
	storeFiles, err := os.ReadDir(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatal(err)
	}
	for _, storeFile := range storeFiles {
		// The LOCK file belongs to the running store.
		if !storeFile.Type().IsRegular() || storeFile.Name() == "LOCK" {
			continue
		}
		bug0824CopyFile(t, filepath.Join(dir, "badger", storeFile.Name()), filepath.Join(imageStore, storeFile.Name()))
	}

	earlyAck := bug0824Insert(t, node, early, early)
	if logs := bug0824MemLogs(t, dir); len(logs) != 1 || logs[0] != rotatedLog {
		t.Fatalf("the memtable rotated before the early write (logs %v); the scenario did not set up", logs)
	}
	recon := node.cl.seeding.Load().recon
	anchorRecord, anchorFound := recon.Record(s0SlotOf(t, anchor), dbID)
	earlyRecord, earlyFound := recon.Record(slot, dbID)
	if !anchorFound || !earlyFound || anchorRecord.WALCursor == 0 || anchorRecord.WALCursor >= earlyRecord.WALCursor || earlyRecord.Applied != earlyAck.Pos {
		t.Fatalf("the anchor's record is %+v (found %v) and the pair's is %+v (found %v), want the anchor's log cursor below the early write's; the scenario did not set up",
			anchorRecord, anchorFound, earlyRecord, earlyFound)
	}
	earlyCursor := earlyRecord.WALCursor

	// Other documents fill log A until the store rotates it out and opens log B.
	filler := strings.Repeat("f", 256<<10)
	activeLog := ""
	for fill := 0; activeLog == ""; fill++ {
		if fill > 2000 {
			t.Fatalf("no memtable rotation after %d filler documents", fill)
		}
		bug0824Insert(t, node, fmt.Sprintf("bug-0824-fill-%04d", fill), filler)
		if logs := bug0824MemLogs(t, dir); logs[len(logs)-1] != rotatedLog {
			activeLog = logs[len(logs)-1]
		}
	}

	lateAck := bug0824Insert(t, node, late, late)
	if logs := bug0824MemLogs(t, dir); logs[len(logs)-1] != activeLog {
		t.Fatalf("the memtable rotated again before the late write (logs %v, want the newest to be %s); the scenario did not set up", logs, activeLog)
	}
	if !earlyAck.Pos.Less(lateAck.Pos) {
		t.Fatalf("the early write holds position %s and the late one %s; the scenario did not set up", earlyAck.Pos, lateAck.Pos)
	}

	// The second copy: log B, synced.
	if err := node.kv.Sync(); err != nil {
		t.Fatalf("syncing the store: %v", err)
	}
	bug0824CopyFile(t, activeLog, filepath.Join(imageStore, filepath.Base(activeLog)))

	// Everything else of the node: its own log and its Raft state, after a clean stop.
	if err := node.Close(); err != nil {
		t.Fatalf("closing the node: %v", err)
	}
	nodeEntries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, nodeEntry := range nodeEntries {
		switch {
		case nodeEntry.Name() == "badger":
		case nodeEntry.IsDir():
			if err := os.CopyFS(filepath.Join(image, nodeEntry.Name()), os.DirFS(filepath.Join(dir, nodeEntry.Name()))); err != nil {
				t.Fatalf("copying %s into the image: %v", nodeEntry.Name(), err)
			}
		case nodeEntry.Type().IsRegular():
			bug0824CopyFile(t, filepath.Join(dir, nodeEntry.Name()), filepath.Join(image, nodeEntry.Name()))
		}
	}

	reopened := s0OpenClusterNode(t, image, nil)
	ctx := context.Background()
	// The first read of a pair after a restart can be held once while its log tail is replayed.
	s0Await(t, "the late write to be readable after the restart", 30*time.Second, func() bool {
		_, err := reopened.Get(ctx, u30iDB, "User", late)
		return err == nil
	})
	if _, err := reopened.Get(ctx, u30iDB, "User", before); err != nil {
		t.Fatalf("%q, written before the store's last full sync, is not readable after the restart: %v; the image is not the one intended", before, err)
	}
	if synced := reopened.wal.SyncedSequence(); synced < earlyCursor {
		t.Fatalf("the node's log on the image is synced through sequence %d, below the early write's %d: the image is not the one intended", synced, earlyCursor)
	}
	pair := reopened.pairs.Snapshot(slot, dbID)
	t.Logf("after the restart on the image: slot %d database %d applied %s (early was acknowledged at %s, log sequence %d; late at %s); the boot scan starts at or below the anchor's log sequence %d",
		slot, dbID, pair.AppliedPos, earlyAck.Pos, earlyCursor, lateAck.Pos, anchorRecord.WALCursor)
	if pair.AppliedPos != lateAck.Pos {
		t.Fatalf("after the restart the pair's applied position is %s, want %s (the late write's): the image is not the one intended", pair.AppliedPos, lateAck.Pos)
	}
	_, err = reopened.Get(ctx, u30iDB, "User", early)
	if err == nil {
		return
	}
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeNotFound {
		t.Fatalf("reading the early write after the restart: %v", err)
	}
	t.Fatalf("the acknowledged write %q (position %s) is missing after the restart, while the later write %q (position %s) of the same pair is there: "+
		"the pair's record names %s as applied, so the boot replay skipped position %s although the node's log holds it (BUG-0824): %v",
		early, earlyAck.Pos, late, lateAck.Pos, pair.AppliedPos, earlyAck.Pos, err)
}
