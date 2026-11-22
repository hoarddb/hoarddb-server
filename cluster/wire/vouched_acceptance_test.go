package wire

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/internal/storeimage"
	"github.com/hoarddb/hoarddb-server/storage"

	badger "github.com/dgraph-io/badger/v4"
)

// The acceptance tests of the vouched cut (store-vouched-cut-spec.md §9): T1, the bug file's
// acceptance test, T3a's node half, and T3b, the route slice 2 (the value-log sync at a flush)
// closes.

// openNodeWithMemtable opens a node without a cluster in dir with memtables of memTableMB.
func openNodeWithMemtable(t *testing.T, dir string, memTableMB int) *Node {
	t.Helper()
	node, err := OpenNode(context.Background(), vouchedTestConfig(t, NodeConfig{Dir: dir, MemTableMB: memTableMB}))
	if err != nil {
		t.Fatalf("opening the node in %s: %v", dir, err)
	}
	t.Cleanup(func() {
		// Close runs once: a test that closed the node already gets that close's answer again.
		if err := node.Close(); err != nil {
			t.Errorf("closing the node in %s: %v", dir, err)
		}
	})
	return node
}

// memLogs lists the memtable logs of a node's store, oldest first.
func memLogs(t *testing.T, dir string) []string {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dir, "badger", "*.mem"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(logs)
	return logs
}

// scopeKeys counts the keys of a store under one bucket's scope in one pair.
func scopeKeys(t *testing.T, kv storage.KV, slot uint16, db, bucket uint64) int {
	t.Helper()
	scope := storage.Scope(slot, db, bucket)
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	iter := snapshot.NewIterator(storage.IterOptions{Lower: scope, Upper: storage.PrefixEnd(scope)})
	defer iter.Close()
	count := 0
	for iter.Seek(scope); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}

// openImageStoreAlone opens a copy of an image's store without the cut, as today's open did.
func openImageStoreAlone(t *testing.T, image string) *storage.BadgerKV {
	t.Helper()
	capture, err := storeimage.Take(filepath.Join(image, "badger"))
	if err != nil {
		t.Fatal(err)
	}
	copyDir := filepath.Join(t.TempDir(), "badger")
	if err := storeimage.Write(copyDir, capture); err != nil {
		t.Fatal(err)
	}
	kv, err := storage.OpenBadgerKV(copyDir, storage.BadgerTuning{MemTableMB: 64}, vouchedTestStoreKey(t, copyDir))
	if err != nil {
		t.Fatalf("opening a copy of the image's store: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	})
	return kv
}

func requestID(n byte) WriteOption { return WithRequestID([16]byte{0x0c, 0x27, n}) }

// TestAckedWritesSurviveAHoledStoreImage is T1, BUG-0827's acceptance test, on the image of
// bug0824_test.go:TestReplayRecoversAckedWriteLostInRotatedMemtableLog: a store that keeps a later
// write of a pair and the pair record naming it without the earlier one. Two acknowledged
// increments of one field and two pushes to one FIFO field of one document, each with a request
// id: the earlier two go into memtable log A after a store sync, the later two into log B after A
// rotated out. The image: the marker and log A as of that sync, log B as it is now. After the
// restart the counter equals the acknowledged value and the queue holds both elements once each,
// in order.
func TestAckedWritesSurviveAHoledStoreImage(t *testing.T) {
	dir := t.TempDir()
	node := openNodeWithMemtable(t, dir, 8)
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	box, err := node.CreateBucket(ctx, u30iDB, "Box", Field{Name: "Name", Type: "string"}, Field{Name: "Count", Type: "int"},
		Field{Name: "Queue", Type: "fifoStore"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Fill", Field{Name: "Data", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	db, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Box", bson.D{{Key: "_id_", Value: "b1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	slot, err := PlacementOf(u30iDB, "Box", "b1")
	if err != nil {
		t.Fatal(err)
	}
	inc := func(n byte) {
		t.Helper()
		if _, err := node.Update(ctx, UpdateRequest{Database: u30iDB, Bucket: "Box", ID: "b1", Inc: bson.D{{Key: "Count", Value: int64(1)}}}, requestID(n)); err != nil {
			t.Fatalf("increment %d: %v", n, err)
		}
	}
	push := func(n byte, item string) {
		t.Helper()
		target := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}
		if err := node.Push(ctx, PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: item}}}, requestID(n)); err != nil {
			t.Fatalf("push %s: %v", item, err)
		}
	}

	// The first copy: the store as its last sync left it. Log A is the active one.
	if err := node.kv.Sync(); err != nil {
		t.Fatal(err)
	}
	logsAtSync := memLogs(t, dir)
	if len(logsAtSync) != 1 {
		t.Fatalf("SCENARIO NOT REACHED: the store has the memtable logs %v after its sync, want one active log", logsAtSync)
	}
	logA := logsAtSync[0]
	atSync := takeStore(t, dir)

	inc(1)
	push(2, "a")
	if logs := memLogs(t, dir); len(logs) != 1 || logs[0] != logA {
		t.Fatalf("SCENARIO NOT REACHED: the memtable rotated before the earlier writes (logs %v)", logs)
	}
	filler := strings.Repeat("f", 256<<10)
	logB := ""
	for fill := 0; logB == ""; fill++ {
		if fill > 2000 {
			t.Fatalf("SCENARIO NOT REACHED: no memtable rotation after %d filler documents", fill)
		}
		if _, err := node.Insert(ctx, u30iDB, "Fill", bson.D{{Key: "_id_", Value: fmt.Sprintf("fill-%04d", fill)}, {Key: "Data", Value: filler}}); err != nil {
			t.Fatal(err)
		}
		if logs := memLogs(t, dir); logs[len(logs)-1] != logA {
			logB = logs[len(logs)-1]
		}
	}
	inc(3)
	push(4, "b")
	if logs := memLogs(t, dir); logs[len(logs)-1] != logB {
		t.Fatalf("SCENARIO NOT REACHED: the memtable rotated again before the later writes (logs %v)", logs)
	}
	// The second copy: log B, synced.
	if err := node.kv.Sync(); err != nil {
		t.Fatal(err)
	}
	now := takeStore(t, dir)
	lateCursor := cursorOf(t, node.kv, pairKey{slot, db})
	acknowledgedKeys := scopeKeys(t, node.kv, slot, db, box)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	image := t.TempDir()
	writeStoreImage(t, image, atSync, now.Only(func(name string) bool { return name == filepath.Base(logB) }))
	copyNodeExceptStore(t, dir, image)

	// The hole is in the image: opened alone, as today's open does, its store has the pair record of
	// the later writes and fewer of the document's keys than the node acknowledged.
	alone := openImageStoreAlone(t, image)
	if got := cursorOf(t, alone, pairKey{slot, db}); got != lateCursor {
		t.Fatalf("SCENARIO NOT REACHED: the image's pair record has cursor %d, want the later writes' %d", got, lateCursor)
	}
	if got := scopeKeys(t, alone, slot, db, box); got >= acknowledgedKeys {
		t.Fatalf("SCENARIO NOT REACHED: the image holds %d of the document's keys, the node acknowledged %d: there is no hole", got, acknowledgedKeys)
	}

	restarted := openNodeWithMemtable(t, image, 8)
	doc, err := restarted.Get(ctx, u30iDB, "Box", "b1")
	if err != nil {
		t.Fatal(err)
	}
	count, err := bson.Raw(doc.Body).LookupErr("Count")
	if err != nil {
		t.Errorf("the counter is gone after the restart: %v (document %s)", err, bson.Raw(doc.Body))
	} else if got, ok := count.AsInt64OK(); !ok || got != 2 {
		t.Errorf("the counter is %v after the restart, want the acknowledged 2", count)
	}
	elements, err := restarted.Peek(ctx, PeekRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}, Count: 10})
	if err != nil {
		t.Fatal(err)
	}
	var items []string
	for _, element := range elements {
		items = append(items, bson.Raw(element).Lookup("item").StringValue())
	}
	if strings.Join(items, ",") != "a,b" {
		t.Errorf("the queue holds %v after the restart, want the two acknowledged pushes once each, in order: [a b]", items)
	}
}

// bigValue is a string above Badger's value threshold, so its bytes go to the value log and the
// memtable holds a pointer; a pattern no other write makes.
func bigValue() string {
	var b strings.Builder
	for b.Len() < 1<<20+4096 {
		fmt.Fprintf(&b, "vouched-large-%08d;", b.Len())
	}
	return b.String()
}

// TestLargeValueUnderAnUnfinishedSyncIsReplayedWhole is T3a's node half, BUG-0827's route (c): a
// 1 MiB document whose pointer reached the disk in a memtable log a sync made durable, while the
// value log's msync had not run. The image keeps the log and the value log and marker as of the
// last completed sync. The commit is above the marker, so the cut discards it and the node replays
// it: the document must read back whole, never empty.
func TestLargeValueUnderAnUnfinishedSyncIsReplayedWhole(t *testing.T) {
	dir := t.TempDir()
	node := openSingleNode(t, dir, false)
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Big", Field{Name: "Data", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if err := node.kv.Sync(); err != nil {
		t.Fatal(err)
	}
	atLastSync := takeStore(t, dir)
	// The marker of the last completed sync; none under the mutation "open without the option",
	// which is the merge base's open and must fail on the document, not here.
	previous, hadMarker := markerVersionOf(t, atLastSync["VOUCHED"])

	large := bigValue()
	if _, err := node.Insert(ctx, u30iDB, "Big", bson.D{{Key: "_id_", Value: "large"}, {Key: "Data", Value: large}}); err != nil {
		t.Fatal(err)
	}
	parked, released := make(chan struct{}), make(chan struct{})
	var parkOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	restore := badger.SetSyncTestHooks(badger.SyncTestHooks{BeforeValueLogSync: func() {
		parkOnce.Do(func() {
			close(parked)
			<-released
		})
	}})
	syncDone := make(chan error, 1)
	go func() { syncDone <- node.kv.Sync() }()
	t.Cleanup(func() {
		release()
		restore()
	})
	select {
	case <-parked:
	case <-time.After(vouchedTestTimeout):
		t.Fatal("SCENARIO NOT REACHED: the store sync never reached the point between the memtable logs' msync and the value log's")
	}
	// At the park: the memtable logs now (synced), the marker's real bytes (the last completed
	// sync's, since the marker is written only after DB.Sync returns), the value log as of that sync.
	atPark := takeStore(t, dir)
	if parked, ok := markerVersionOf(t, atPark["VOUCHED"]); hadMarker && (!ok || parked != previous) {
		// Not fatal: under the mutation "write the marker before DB.Sync returns" the marker already
		// vouches the pointer, and the behavioural assertion below is the one that must then fail.
		t.Errorf("the marker at the park is not the last completed sync's (version %d)", previous)
	}
	release()
	restore()
	if err := <-syncDone; err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	// Every value-log file as of the last completed sync, and none created after it: a file the
	// large value opened after that sync holds bytes no sync made durable.
	isValueLog := func(name string) bool { return strings.HasSuffix(name, ".vlog") }
	image := t.TempDir()
	writeStoreImage(t, image, atPark.Only(func(name string) bool { return !isValueLog(name) }), atLastSync.Only(isValueLog))
	copyNodeExceptStore(t, dir, image)
	requireValueMissing(t, dir, image, large)

	restarted := openSingleNode(t, image, false)
	doc, err := restarted.Get(ctx, u30iDB, "Big", "large")
	if err != nil {
		t.Fatalf("the 1 MiB document is gone after the restart: %v", err)
	}
	if got := dataOf(doc); got != large {
		t.Errorf("the 1 MiB document reads back with %d bytes of its %d-byte value after a cut between the two msyncs: "+
			"its pointer was kept without the value-log bytes", len(got), len(large))
	}
}

// TestLargeValueVouchedByAFlushIsWhole is T3b, BUG-0827's route (c) through a flush: the same
// 1 MiB value, its memtable flushed before any store sync. Once the MANIFEST names the table, the
// table vouches the pointer whatever the marker says (C = max(V, T)). The image is the store at the
// instant the MANIFEST change naming the table is synced (badger.FlushTableNamed): the tables, the
// MANIFEST, the memtable logs and the marker as they are then, and each value-log file at the
// prefix its last msync made durable (badger.SyncTestHooks.AfterValueLogFileSync), zeros after it.
// Before slice 2 a flush synced no value log, so that prefix was the last store sync's and the
// document read back empty; slice 2 syncs the value log before the table is named.
func TestLargeValueVouchedByAFlushIsWhole(t *testing.T) {
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "badger")
	var durableMu sync.Mutex
	durable := map[string]int64{} // each value-log file's longest prefix an msync made durable
	restoreSyncHooks := badger.SetSyncTestHooks(badger.SyncTestHooks{AfterValueLogFileSync: func(path string, covered int64) {
		if filepath.Dir(path) != storeDir {
			return
		}
		durableMu.Lock()
		defer durableMu.Unlock()
		durable[filepath.Base(path)] = max(durable[filepath.Base(path)], covered)
	}})
	t.Cleanup(restoreSyncHooks)
	node := openNodeWithMemtable(t, dir, 8)
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Big", Field{Name: "Data", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if err := node.kv.Sync(); err != nil {
		t.Fatal(err)
	}
	tablesAtSync := tableFiles(takeStore(t, dir))

	large := bigValue()
	if _, err := node.Insert(ctx, u30iDB, "Big", bson.D{{Key: "_id_", Value: "large"}, {Key: "Data", Value: large}}); err != nil {
		t.Fatal(err)
	}
	// The image at the first table naming from here on: the flush of the memtable holding the
	// pointer, which nothing rotated out before it. The flusher waits while the hook copies.
	images := make(chan storeimage.Capture, 1)
	var imageOnce sync.Once
	t.Cleanup(badger.SetFlushStepHook(func(step badger.FlushStep, db *badger.DB) {
		if step != badger.FlushTableNamed || filepath.Clean(db.Opts().Dir) != storeDir {
			return
		}
		imageOnce.Do(func() {
			now, err := storeimage.Take(storeDir)
			if err != nil {
				t.Errorf("copying the store at the table's naming: %v", err)
				close(images)
				return
			}
			durableMu.Lock()
			defer durableMu.Unlock()
			image := now.Only(func(name string) bool { return !strings.HasSuffix(name, ".vlog") })
			for name, content := range now.Only(func(name string) bool { return strings.HasSuffix(name, ".vlog") }) {
				covered, synced := durable[name]
				if !synced {
					continue // created after the last msync of any value-log file: nothing of it is durable
				}
				cut := make([]byte, len(content))
				copy(cut, content[:min(int(covered), len(content))])
				image[name] = cut
			}
			images <- image
		})
	}))
	// More writes until the memtable holding the pointer rotates and its flush names a table: the
	// flush is observed, never slept for.
	filler := strings.Repeat("g", 256<<10)
	var image storeimage.Capture
	deadline := time.Now().Add(vouchedTestTimeout)
	for fill := 0; image == nil; fill++ {
		select {
		case captured, ok := <-images:
			if !ok {
				t.Fatal("the image at the table's naming was not taken")
			}
			image = captured
			continue
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("SCENARIO NOT REACHED: no table was named after the large document")
		}
		if fill < 200 {
			if _, err := node.Insert(ctx, u30iDB, "Big", bson.D{{Key: "_id_", Value: fmt.Sprintf("fill-%04d", fill)}, {Key: "Data", Value: filler}}); err != nil {
				t.Fatal(err)
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if tableFiles(image) <= tablesAtSync {
		t.Fatal("SCENARIO NOT REACHED: the image names no table flushed after the sync")
	}
	imageDir := t.TempDir()
	writeStoreImage(t, imageDir, image)
	copyNodeExceptStore(t, dir, imageDir)

	restarted := openNodeWithMemtable(t, imageDir, 8)
	if kept, vouched := restarted.kv.VouchedOpenReport().Kept, restarted.kv.VouchedOpenReport().Vouched; kept <= vouched {
		t.Fatalf("SCENARIO NOT REACHED: the image's tables vouch nothing above the marker (kept %d, marker %d)", kept, vouched)
	}
	doc, err := restarted.Get(ctx, u30iDB, "Big", "large")
	if err != nil {
		t.Fatalf("the 1 MiB document is gone after the restart: %v", err)
	}
	if got := dataOf(doc); got != large {
		t.Errorf("the 1 MiB document reads back with %d bytes of its %d-byte value: its flushed table vouches a pointer "+
			"whose value-log bytes were never made durable", len(got), len(large))
	}
}

// tableFiles counts the tables of a capture.
func tableFiles(capture storeimage.Capture) int {
	count := 0
	for name := range capture {
		if strings.HasSuffix(name, ".sst") {
			count++
		}
	}
	return count
}

// dataOf returns the document's Data string, empty when it has none or a value of another type
// (a pointer kept without its value-log bytes reads back as an empty value of no type).
func dataOf(doc Document) string {
	value, err := bson.Raw(doc.Body).LookupErr("Data")
	if err != nil || value.Type != bson.TypeString {
		return ""
	}
	return value.StringValue()
}

// requireValueMissing checks that the image's value log really lacks the value while the store it
// was taken from holds it, in two ways. The bytes written to the image's value-log files are fewer
// than the value is long, and those of the closed store at running are not: this holds for a sealed
// value log too, whose written bytes are ciphertext and whose unwritten tail is zero like a
// plaintext one's. And no value-log file of the image holds the value's plaintext, which only a
// plaintext value log can show.
func requireValueMissing(t *testing.T, running, image, value string) {
	t.Helper()
	if written := valueLogBytesWritten(t, running); written < len(value) {
		t.Fatalf("SCENARIO NOT REACHED: the closed store's value log holds %d written bytes, fewer than the value's %d: "+
			"the value never went to the value log, so the image cannot be shown to lack it", written, len(value))
	}
	if written := valueLogBytesWritten(t, image); written >= len(value) {
		t.Fatalf("SCENARIO NOT REACHED: the image's value log holds %d written bytes, enough for the value's %d", written, len(value))
	}
	chunk := []byte(value[len(value)/2 : len(value)/2+64])
	entries, err := filepath.Glob(filepath.Join(image, "badger", "*.vlog"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(content, chunk) {
			t.Fatalf("SCENARIO NOT REACHED: the image's value log %s already holds the value's bytes", filepath.Base(path))
		}
	}
}

// valueLogBytesWritten is the sum, over the value-log files of the store of the node directory
// dir, of each file's length up to its last non-zero byte: Badger sizes a value-log file ahead of
// its writes and leaves the rest zero.
func valueLogBytesWritten(t *testing.T, dir string) int {
	t.Helper()
	entries, err := filepath.Glob(filepath.Join(dir, "badger", "*.vlog"))
	if err != nil {
		t.Fatal(err)
	}
	written := 0
	for _, path := range entries {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		written += len(bytes.TrimRight(content, "\x00"))
	}
	return written
}
