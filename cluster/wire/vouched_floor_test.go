package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/internal/storeimage"
	"github.com/hoarddb/hoarddb-server/storage"

	badger "github.com/dgraph-io/badger/v4"
)

// The tests of the vouched cut's log floor (store-vouched-cut-spec.md §3.6, I-8): T12, T12c, T13.
// Every pre-assertion that checks the scenario was built fails with a message that begins
// "SCENARIO NOT REACHED:", so it is never mistaken for the behavioural failure.

const vouchedTestTimeout = 60 * time.Second

// idsInSlot returns count document ids of bucket in database db that PlacementOf puts in slot.
func idsInSlot(t *testing.T, db, bucket string, slot uint16, count int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < 2_000_000 && len(ids) < count; i++ {
		candidate := fmt.Sprintf("vouched-%d-%d", slot, i)
		got, err := PlacementOf(db, bucket, candidate)
		if err != nil {
			t.Fatal(err)
		}
		if got == slot {
			ids = append(ids, candidate)
		}
	}
	if len(ids) < count {
		t.Fatalf("found %d ids in slot %d, want %d", len(ids), slot, count)
	}
	return ids
}

// pairCursor is one pair record's log cursor.
type pairCursor struct {
	key    pairKey
	cursor uint64
}

// recordCursors lists the log cursor of every pair record of kv: the 32-byte records of a node
// without a cluster and the seed records of a cluster node.
func recordCursors(t *testing.T, kv storage.KV) []pairCursor {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSSeedState)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyRangeEnd(lower)})
	defer iter.Close()
	var cursors []pairCursor
	for iter.Seek(lower); iter.Valid(); iter.Next() {
		key := iter.Key()
		raw, err := iter.Value()
		if err != nil {
			t.Fatal(err)
		}
		pair := pairKey{slot: binary.BigEndian.Uint16(key[len(lower):]), db: binary.BigEndian.Uint64(key[len(lower)+2:])}
		if len(raw) == localPairRecordLen {
			cursors = append(cursors, pairCursor{pair, binary.BigEndian.Uint64(raw[16:])})
			continue
		}
		record, err := DecodeSeedRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		cursors = append(cursors, pairCursor{pair, record.WALCursor})
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	return cursors
}

// cursorOf returns the log cursor of key's record in kv, 0 when it has none.
func cursorOf(t *testing.T, kv storage.KV, key pairKey) uint64 {
	t.Helper()
	for _, record := range recordCursors(t, kv) {
		if record.key == key {
			return record.cursor
		}
	}
	return 0
}

// lowestOtherCursor is the lowest non-zero record cursor of any pair but except, in the store of
// image, read from a copy of it opened alone without the cut.
func lowestOtherCursor(t *testing.T, imageStore string, except pairKey) uint64 {
	t.Helper()
	capture, err := storeimage.Take(imageStore)
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
	defer func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	}()
	lowest := uint64(0)
	for _, record := range recordCursors(t, kv) {
		if record.key != except && record.cursor != 0 && (lowest == 0 || record.cursor < lowest) {
			lowest = record.cursor
		}
	}
	return lowest
}

// markerOf reads the version and log floor of the sync marker in a store directory.
func markerOf(t *testing.T, storeDir string) (version, floor uint64) {
	t.Helper()
	version, floor, found, err := storage.ReadVouchedMarker(storeDir)
	if err != nil {
		t.Fatalf("reading the sync marker of %s: %v", storeDir, err)
	}
	if !found {
		t.Fatalf("SCENARIO NOT REACHED: the store %s has no sync marker", storeDir)
	}
	return version, floor
}

// copyNodeExceptStore copies every directory and file of a stopped node's data directory but its
// store into image: the node's own log and its Raft state, synced before each acknowledgement.
func copyNodeExceptStore(t *testing.T, dir, image string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		switch {
		case entry.Name() == "badger":
		case entry.IsDir():
			if err := os.CopyFS(filepath.Join(image, entry.Name()), os.DirFS(filepath.Join(dir, entry.Name()))); err != nil {
				t.Fatalf("copying %s into the image: %v", entry.Name(), err)
			}
		case entry.Type().IsRegular():
			content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(image, entry.Name()), content, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// writeStoreImage writes the node image's store from captures, later ones replacing earlier files.
func writeStoreImage(t *testing.T, image string, captures ...storeimage.Capture) {
	t.Helper()
	if err := storeimage.Write(filepath.Join(image, "badger"), captures...); err != nil {
		t.Fatal(err)
	}
}

func takeStore(t *testing.T, dir string) storeimage.Capture {
	t.Helper()
	capture, err := storeimage.Take(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatal(err)
	}
	return capture
}

func isMarker(name string) bool { return name == "VOUCHED" }

// captureNodeLog sends slog's records, INFO and above, to a buffer for the rest of the test.
func captureNodeLog(t *testing.T) *syncBuffer {
	t.Helper()
	buffer := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buffer, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buffer
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// readUser reads one User document, retrying for a while a read held because the node has not
// applied its cluster state yet (a node restarted on an image answers that for its first moments).
func readUser(node *Node, id string) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := node.Get(context.Background(), u30iDB, "User", id)
		if err == nil || !strings.Contains(err.Error(), "holding the read") || time.Now().After(deadline) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tailNotReplayedRefusal is the read gate's retryable answer (cluster.CodeUnavailable) for a pair
// whose log tail the restart has not replayed yet, from the gate's cause to its advice to retry
// (ReadGate.holdCause and ReadGate.holdRefusal).
const tailNotReplayedRefusal = "cannot be read on this node yet: its log tail has not been replayed since the restart. Nothing was returned. Retry in a few seconds"

// readUserOnceItsTailIsReplayed is readUser on a cluster node that has just restarted. OpenNode
// returns before the restart has replayed every pair's log tail, and until it has the read gate
// refuses the pair's reads with tailNotReplayedRefusal; a read made at once raced that replay and
// lost it under load (BUG-0884). The read is retried while it is refused with exactly that answer;
// any other error, and that answer still given after 30 s, is returned.
func readUserOnceItsTailIsReplayed(node *Node, id string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := readUser(node, id)
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable ||
			!strings.Contains(refusal.Message, tailNotReplayedRefusal) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// openSingleNode opens a node without a cluster in dir, with the app database and its User bucket
// when schema is set.
func openSingleNode(t *testing.T, dir string, schema bool) *Node {
	t.Helper()
	node, err := OpenNode(context.Background(), vouchedTestConfig(t, NodeConfig{Dir: dir}))
	if err != nil {
		t.Fatalf("opening the node in %s: %v", dir, err)
	}
	t.Cleanup(func() {
		// Close runs once: a test that closed the node already gets that close's answer again.
		if err := node.Close(); err != nil {
			t.Errorf("closing the node in %s: %v", dir, err)
		}
	})
	if schema {
		ctx := context.Background()
		if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
			t.Fatal(err)
		}
		if _, err := node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"}); err != nil {
			t.Fatal(err)
		}
	}
	return node
}

// insertUser inserts one User document, retrying the "busy" answer of a first reconcile pass.
func insertUser(t *testing.T, node *Node, id string) Inserted {
	t.Helper()
	inserted, err := tryInsertUser(node, id)
	if err != nil {
		t.Fatal(err)
	}
	return inserted
}

// tryInsertUser is insertUser for a goroutine other than the test's, or a hook inside a store
// sync, where failing the test would exit the goroutine with the sync's locks held.
func tryInsertUser(node *Node, id string) (Inserted, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		inserted, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}})
		if err == nil {
			return inserted, nil
		}
		if !errors.Is(err, ErrPairBusy) || time.Now().After(deadline) {
			return Inserted{}, fmt.Errorf("inserting %s: %w", id, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// freeSlot returns a slot of database db that holds no record in kv and is not in avoid.
func freeSlot(t *testing.T, kv storage.KV, db uint64, avoid ...uint16) uint16 {
	t.Helper()
	held := map[uint16]bool{}
	for _, record := range recordCursors(t, kv) {
		if record.key.db == db && record.cursor != 0 {
			held[record.key.slot] = true
		}
	}
	for _, slot := range avoid {
		held[slot] = true
	}
	for slot := uint16(100); slot < storage.SlotCount; slot++ {
		if !held[slot] {
			return slot
		}
	}
	t.Fatal("no free slot")
	return 0
}

// heldCommit parks the next commit of one pair inside beforeAdvanceCommit until release is called.
type heldCommit struct {
	parked  chan struct{}
	release func()
}

func holdNextCommit(t *testing.T, slot uint16, db uint64) *heldCommit {
	t.Helper()
	parked, released := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	var once sync.Once
	h := &heldCommit{parked: parked, release: func() { once.Do(func() { close(released) }) }}
	previous := beforeAdvanceCommit
	beforeAdvanceCommit = func(s uint16, d uint64) {
		if s == slot && d == db && armed.CompareAndSwap(true, false) {
			close(parked)
			<-released
		}
	}
	t.Cleanup(func() {
		h.release()
		beforeAdvanceCommit = previous
	})
	return h
}

func waitParked(t *testing.T, parked <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-parked:
	case <-time.After(vouchedTestTimeout):
		t.Fatalf("SCENARIO NOT REACHED: %s never reached its commit", what)
	}
}

// discardLine is the INFO line of §2 a node writes when its store's cut discarded commits.
func discardLine(report storage.VouchedOpenReport, start uint64) string {
	return fmt.Sprintf("store: discarded %d commits above the sync marker (version %d) and kept everything through %d; "+
		"%d log(s) truncated, %d removed. The node re-applies them from its log, starting at log sequence %d.",
		report.DiscardedCommits, report.Vouched, report.Kept, report.LogsTruncated, report.LogsRemoved, start)
}

// inversion is what T12 and its twin build: pair P's first write appended before Q's write and
// committed after it, a store sync between the two commits, and every other pair with a record
// advanced past P's cursor by a document write into its slot.
type inversion struct {
	db         uint64
	pSlot      uint16
	pAck       Inserted
	pCursor    uint64
	image      string
	lowestKept uint64 // the lowest record cursor of the image's store, P's left out
}

// buildInversion runs the scenario on node (whose data directory is dir) and returns the node
// image: the store files as of after P's commit, the marker as of the sync between the commits,
// the node's log and Raft state after a clean stop. The node is closed on return.
func buildInversion(t *testing.T, node *Node, dir string, db uint64) inversion {
	t.Helper()
	pSlot := freeSlot(t, node.kv, db)
	qSlot := freeSlot(t, node.kv, db, pSlot)
	pID := idsInSlot(t, u30iDB, "User", pSlot, 1)[0]
	qID := idsInSlot(t, u30iDB, "User", qSlot, 1)[0]

	held := holdNextCommit(t, pSlot, db)
	type outcome struct {
		inserted Inserted
		err      error
	}
	pDone := make(chan outcome, 1)
	go func() {
		inserted, err := tryInsertUser(node, pID)
		pDone <- outcome{inserted, err}
	}()
	waitParked(t, held.parked, "P's first write")

	insertUser(t, node, qID)
	// The anchor-free state, built: every other pair that has a record takes a document write into
	// its own slot, never an alter, so its cursor moves above P's.
	for _, record := range recordCursors(t, node.kv) {
		if record.key.slot == pSlot || record.key.slot == qSlot || record.cursor == 0 || record.key.db != db {
			continue
		}
		insertUser(t, node, idsInSlot(t, u30iDB, "User", record.key.slot, 1)[0])
	}
	if err := node.kv.Sync(); err != nil {
		t.Fatalf("the store sync between the two commits: %v", err)
	}
	atSync := takeStore(t, dir)

	held.release()
	var pAck Inserted
	select {
	case done := <-pDone:
		if done.err != nil {
			t.Fatalf("SCENARIO NOT REACHED: P's write: %v", done.err)
		}
		pAck = done.inserted
	case <-time.After(vouchedTestTimeout):
		t.Fatal("SCENARIO NOT REACHED: P's write never returned once its commit was released")
	}
	if beforeImage != nil {
		beforeImage()
	}
	pCursor := cursorOf(t, node.kv, pairKey{pSlot, db})
	if pCursor == 0 {
		t.Fatal("SCENARIO NOT REACHED: P has no record cursor after its acknowledged write")
	}
	now := takeStore(t, dir)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	image := t.TempDir()
	writeStoreImage(t, image, now, atSync.Only(isMarker))
	copyNodeExceptStore(t, dir, image)

	lowest := lowestOtherCursor(t, filepath.Join(image, "badger"), pairKey{pSlot, db})
	if lowest <= pCursor {
		t.Fatalf("SCENARIO NOT REACHED: the image's lowest record cursor other than P's is %d, not above P's cursor %d: "+
			"a pair anchors the scan below P's write", lowest, pCursor)
	}
	return inversion{db: db, pSlot: pSlot, pAck: pAck, pCursor: pCursor, image: image, lowestKept: lowest}
}

// checkReplayed asserts, on a node restarted on inv.image, that P's acknowledged write is there,
// that a new write to P takes the next position, and the discard line.
func checkReplayed(t *testing.T, node *Node, inv inversion, nodeLog *syncBuffer) {
	t.Helper()
	report := node.kv.VouchedOpenReport()
	if report.DiscardedCommits < 1 {
		t.Fatalf("SCENARIO NOT REACHED: the restarted store discarded nothing (%+v)", report)
	}
	pID := idsInSlot(t, u30iDB, "User", inv.pSlot, 1)[0]
	if err := readUserOnceItsTailIsReplayed(node, pID); err != nil {
		t.Errorf("P's acknowledged first write (position %s, log cursor %d) is gone after the restart: %v", inv.pAck.Pos, inv.pCursor, err)
	}
	next := insertUser(t, node, idsInSlot(t, u30iDB, "User", inv.pSlot, 2)[1])
	if next.Pos.Epoch != inv.pAck.Pos.Epoch || next.Pos.Seq != inv.pAck.Pos.Seq+1 {
		t.Errorf("a new write to P took position %s after P's acknowledged %s: want the next one", next.Pos, inv.pAck.Pos)
	}
	start := min(node.kv.VouchedFloor(), inv.lowestKept)
	if want := discardLine(report, start); !strings.Contains(nodeLog.String(), want) {
		t.Errorf("the node's log lacks the discard line\n%s\nlog:\n%s", want, nodeLog.String())
	}
}

// TestAFirstWriteAboveTheCutIsReplayedWithoutAnAnchor is T12 on a node without a cluster: pair P
// has no record, its first write is appended before Q's and committed after it, and the marker
// lies between the two commits. With no other record's cursor below P's, only the log floor makes
// the boot scan reach P's record (I-8).
func TestAFirstWriteAboveTheCutIsReplayedWithoutAnAnchor(t *testing.T) {
	dir := t.TempDir()
	node := openSingleNode(t, dir, true)
	db, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	inv := buildInversion(t, node, dir, db)

	nodeLog := captureNodeLog(t)
	restarted := openSingleNode(t, inv.image, false)
	checkReplayed(t, restarted, inv, nodeLog)
}

// TestAZeroCursorPairsFirstWriteIsReplayedWithoutAnAnchor is T12c on a one-member cluster node: P's
// record is the interim ready record at cursor 0, written by the first reconcile pass. The second
// case cuts the boot replay itself short: a marker advance during the scan, then a kill.
func TestAZeroCursorPairsFirstWriteIsReplayedWithoutAnAnchor(t *testing.T) {
	t.Run("the interim record", func(t *testing.T) {
		dir := t.TempDir()
		node := s0OpenClusterNode(t, dir, nil)
		db := s0Schema(t, node)
		s0AwaitFirstPass(t, node, db)
		inv := buildInversion(t, node, dir, db)
		if record, found := zeroCursorInterim(t, inv, db); !found {
			t.Fatalf("SCENARIO NOT REACHED: P (slot %d) had no interim record at cursor 0 before its write (%+v)", inv.pSlot, record)
		}

		nodeLog := captureNodeLog(t)
		restarted := s0OpenClusterNode(t, inv.image, nil)
		checkReplayed(t, restarted, inv, nodeLog)
	})

	t.Run("a marker advance during the boot scan, then a kill", func(t *testing.T) {
		dir := t.TempDir()
		node := s0OpenClusterNode(t, dir, nil)
		db := s0Schema(t, node)
		s0AwaitFirstPass(t, node, db)
		inv := buildInversionWithLaterWrites(t, node, dir, db)

		// The restart: on the boot replay's first commit the store syncs, a marker advance during the
		// scan; on the next one the store's files are copied, as a kill leaves them.
		var booting atomic.Pointer[Node]
		previousScan := beforeTailScan
		beforeTailScan = func(n *Node) { booting.Store(n) }
		t.Cleanup(func() { beforeTailScan = previousScan })
		var calls atomic.Int32
		var syncErr error
		var midScan storeimage.Capture
		var takeErr error
		previousCommit := beforeAdvanceCommit
		beforeAdvanceCommit = func(uint16, uint64) {
			n := booting.Load()
			if n == nil {
				return
			}
			// With the floor in place the first commit is P's (its cursor is the lowest above the cut);
			// without it the scan starts past P and P is never replayed, which the last boot shows.
			switch calls.Add(1) {
			case 1:
				syncErr = n.kv.Sync()
			case 2:
				midScan, takeErr = storeimage.Take(filepath.Join(inv.image, "badger"))
			}
		}
		t.Cleanup(func() { beforeAdvanceCommit = previousCommit })
		second := s0OpenClusterNode(t, inv.image, nil)
		beforeAdvanceCommit, beforeTailScan = previousCommit, previousScan
		if calls.Load() < 2 || syncErr != nil || takeErr != nil {
			t.Fatalf("SCENARIO NOT REACHED: the boot replay made %d commits (want 2 or more), the sync during it: %v, the copy: %v",
				calls.Load(), syncErr, takeErr)
		}
		if err := second.Close(); err != nil {
			t.Fatal(err)
		}
		third := t.TempDir()
		writeStoreImage(t, third, midScan)
		copyNodeExceptStore(t, inv.image, third)

		restarted := s0OpenClusterNode(t, third, nil)
		pID := idsInSlot(t, u30iDB, "User", inv.pSlot, 1)[0]
		if err := readUserOnceItsTailIsReplayed(restarted, pID); err != nil {
			t.Errorf("P's acknowledged first write is gone after a boot replay cut short by a kill: %v", err)
		}
		next := insertUser(t, restarted, idsInSlot(t, u30iDB, "User", inv.pSlot, 2)[1])
		if next.Pos.Epoch != inv.pAck.Pos.Epoch || next.Pos.Seq != inv.pAck.Pos.Seq+1 {
			t.Errorf("a new write to P took position %s after P's acknowledged %s: want the next one", next.Pos, inv.pAck.Pos)
		}
	})
}

// zeroCursorInterim reports whether P's record in the image's store, opened alone, is a ready
// seed record; at P's first write it was the interim at cursor 0 (s0AwaitFirstPass checked the
// idle pair's, and every in-target pair gets one).
func zeroCursorInterim(t *testing.T, inv inversion, db uint64) (pairCursor, bool) {
	t.Helper()
	for _, record := range recordCursorsOfImage(t, inv.image) {
		if record.key == (pairKey{inv.pSlot, db}) {
			return record, true
		}
	}
	return pairCursor{}, false
}

func recordCursorsOfImage(t *testing.T, image string) []pairCursor {
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
		t.Fatal(err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	}()
	return recordCursors(t, kv)
}

// buildInversionWithLaterWrites is buildInversion, plus writes to another fresh pair R after P's
// acknowledgement and before the image, so the boot replay has records to apply after P's.
func buildInversionWithLaterWrites(t *testing.T, node *Node, dir string, db uint64) inversion {
	t.Helper()
	previous := beforeImage
	beforeImage = func() {
		rSlot := freeSlot(t, node.kv, db)
		for _, id := range idsInSlot(t, u30iDB, "User", rSlot, 3) {
			insertUser(t, node, id)
		}
	}
	t.Cleanup(func() { beforeImage = previous })
	return buildInversion(t, node, dir, db)
}

// beforeImage, when set, runs in buildInversion after P's acknowledgement and before the store's
// files are copied.
var beforeImage func()

// TestTheFloorSampleIsReadInOrder is T13: the sample's three reads in order (the synced sequence,
// the holds, then the version), from the right sources, and no hold left behind by a step that
// appended nothing.
func TestTheFloorSampleIsReadInOrder(t *testing.T) {
	t.Run("(a) a first write between the synced sequence and the holds", func(t *testing.T) {
		dir := t.TempDir()
		node := openSingleNode(t, dir, true)
		db, err := node.database(u30iDB)
		if err != nil {
			t.Fatal(err)
		}
		xSlot := freeSlot(t, node.kv, db)
		xID := idsInSlot(t, u30iDB, "User", xSlot, 1)[0]
		var once sync.Once
		ran := false
		var hookErr error
		previous := betweenFloorReads
		betweenFloorReads = func() {
			once.Do(func() {
				ran = true
				_, hookErr = tryInsertUser(node, xID)
			})
		}
		t.Cleanup(func() { betweenFloorReads = previous })
		if err := node.kv.Sync(); err != nil {
			t.Fatal(err)
		}
		betweenFloorReads = previous
		if hookErr != nil {
			t.Fatalf("SCENARIO NOT REACHED: %v", hookErr)
		}
		xCursor := cursorOf(t, node.kv, pairKey{xSlot, db})
		if !ran || xCursor == 0 {
			t.Fatalf("SCENARIO NOT REACHED: the write between the floor's reads ran %v, its cursor %d", ran, xCursor)
		}
		if _, floor := markerOf(t, filepath.Join(dir, "badger")); floor > xCursor {
			t.Errorf("the marker's floor %d is above the cursor %d of a first write appended between the floor's reads", floor, xCursor)
		}
	})

	t.Run("(b) a first write whose commit returns after the version was read", func(t *testing.T) {
		dir := t.TempDir()
		node := openSingleNode(t, dir, true)
		db, err := node.database(u30iDB)
		if err != nil {
			t.Fatal(err)
		}
		ySlot := freeSlot(t, node.kv, db)
		yID := idsInSlot(t, u30iDB, "User", ySlot, 1)[0]
		var once sync.Once
		var yAck Inserted
		var hookErr error
		restore := badger.SetSyncTestHooks(badger.SyncTestHooks{AfterCapture: func() {
			once.Do(func() { yAck, hookErr = tryInsertUser(node, yID) })
		}})
		t.Cleanup(restore)
		if err := node.kv.Sync(); err != nil {
			t.Fatal(err)
		}
		restore()
		if hookErr != nil {
			t.Fatalf("SCENARIO NOT REACHED: %v", hookErr)
		}
		yCursor := cursorOf(t, node.kv, pairKey{ySlot, db})
		if yCursor == 0 {
			t.Fatal("SCENARIO NOT REACHED: the write inside the hook left no record")
		}
		if _, floor := markerOf(t, filepath.Join(dir, "badger")); floor > yCursor {
			t.Errorf("the marker's floor %d is above the cursor %d of a first write whose commit returned after the version was read",
				floor, yCursor)
		}
		now := takeStore(t, dir)
		if err := node.Close(); err != nil {
			t.Fatal(err)
		}
		image := t.TempDir()
		writeStoreImage(t, image, now)
		copyNodeExceptStore(t, dir, image)
		lowestKept := lowestOtherCursor(t, filepath.Join(image, "badger"), pairKey{ySlot, db})
		nodeLog := captureNodeLog(t)
		restarted := openSingleNode(t, image, false)
		if err := readUser(restarted, yID); err != nil {
			t.Errorf("the write at position %s, cursor %d, is gone after the restart: %v", yAck.Pos, yCursor, err)
		}
		// Here a record cursor (the declaration pairs') lies below the marker's floor, so the scan start
		// is not the floor: the discard line must carry the start the boot used, not the floor it read
		// at the store's open (T12's line clause, where the floor is the start, cannot tell them apart).
		report := restarted.kv.VouchedOpenReport()
		start := min(restarted.kv.VouchedFloor(), lowestKept)
		if report.DiscardedCommits < 1 || start == restarted.kv.VouchedFloor() {
			t.Fatalf("SCENARIO NOT REACHED: the restart discarded %d commits and its scan start %d is the floor %d",
				report.DiscardedCommits, start, restarted.kv.VouchedFloor())
		}
		if want := discardLine(report, start); !strings.Contains(nodeLog.String(), want) {
			t.Errorf("the node's log lacks the discard line\n%s\nlog:\n%s", want, nodeLog.String())
		}
	})

	t.Run("(c) entries appended and not fsynced at the sync", func(t *testing.T) { floorAboveUnsyncedEntries(t) })

	t.Run("(d) a refused first write leaves no hold", func(t *testing.T) { refusedFirstWritesLeaveNoHold(t) })
}

// holdOf returns key's hold in the tracker and whether it has one.
func holdOf(f *logFloor, key pairKey) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	at, held := f.holds[key]
	return at, held
}

// floorAboveUnsyncedEntries is T13 (c), the review's image: two entries of pairs that have a
// cursor are appended and not fsynced (their writers parked before the fsync), the store syncs,
// and the image holds the log as of its last fsync and the marker as of now. The marker's floor
// must be at or below the reopened log's last sequence + 1, and a first write that takes a
// reused sequence and is cut is replayed.
func floorAboveUnsyncedEntries(t *testing.T) {
	dir := t.TempDir()
	node := openSingleNode(t, dir, true)
	db, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	aSlot := freeSlot(t, node.kv, db)
	bSlot := freeSlot(t, node.kv, db, aSlot)
	aIDs, bIDs := idsInSlot(t, u30iDB, "User", aSlot, 2), idsInSlot(t, u30iDB, "User", bSlot, 2)
	insertUser(t, node, aIDs[0])
	insertUser(t, node, bIDs[0])
	if cursorOf(t, node.kv, pairKey{aSlot, db}) == 0 || cursorOf(t, node.kv, pairKey{bSlot, db}) == 0 {
		t.Fatal("SCENARIO NOT REACHED: A and B have no cursor after their first writes")
	}
	walBefore, err := storeimage.Take(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	syncedBefore := node.wal.SyncedSequence()

	var parkedCount atomic.Int32
	released := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	node.wal.SetBeforeSyncHook(func(uint64) {
		parkedCount.Add(1)
		<-released
	})
	t.Cleanup(func() {
		release()
		node.wal.SetBeforeSyncHook(nil)
	})
	done := make(chan error, 2)
	go func() { _, err := tryInsertUser(node, aIDs[1]); done <- err }()
	go func() { _, err := tryInsertUser(node, bIDs[1]); done <- err }()
	deadline := time.Now().Add(vouchedTestTimeout)
	for parkedCount.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("SCENARIO NOT REACHED: %d writers parked before their fsync, want 2", parkedCount.Load())
		}
		time.Sleep(time.Millisecond)
	}
	lastAtSync := node.wal.LastSequence()
	if err := node.kv.Sync(); err != nil {
		t.Fatal(err)
	}
	now := takeStore(t, dir)
	_, floor := markerOf(t, filepath.Join(dir, "badger"))
	node.wal.SetBeforeSyncHook(nil)
	release()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("a parked write: %v", err)
		}
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	image := t.TempDir()
	copyNodeExceptStore(t, dir, image)
	if err := os.RemoveAll(filepath.Join(image, "wal")); err != nil {
		t.Fatal(err)
	}
	if err := storeimage.Write(filepath.Join(image, "wal"), walBefore); err != nil {
		t.Fatal(err)
	}
	writeStoreImage(t, image, now)

	reopened := openSingleNode(t, image, false)
	reopenedLast := reopened.wal.LastSequence()
	if reopenedLast >= lastAtSync || reopenedLast != syncedBefore {
		t.Fatalf("SCENARIO NOT REACHED: the reopened log ends at %d; the running log had given out %d at the sync and synced %d before the appends",
			reopenedLast, lastAtSync, syncedBefore)
	}
	if floor > reopenedLast+1 {
		t.Errorf("the marker's floor %d is above the reopened log's last sequence %d + 1: the floor counted entries that were not fsynced, "+
			"and the log gives those sequences out again", floor, reopenedLast)
	}

	// A first write that takes a reused sequence, cut by the next image, is replayed.
	zSlot := freeSlot(t, reopened.kv, db, aSlot, bSlot)
	zID := idsInSlot(t, u30iDB, "User", zSlot, 1)[0]
	insertUser(t, reopened, zID)
	if zCursor := cursorOf(t, reopened.kv, pairKey{zSlot, db}); zCursor > lastAtSync {
		t.Fatalf("SCENARIO NOT REACHED: the first write after the restart took cursor %d, not a reused sequence (at most %d)", zCursor, lastAtSync)
	}
	again := takeStore(t, image)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	second := t.TempDir()
	writeStoreImage(t, second, again)
	copyNodeExceptStore(t, image, second)
	restarted := openSingleNode(t, second, false)
	if restarted.kv.VouchedOpenReport().DiscardedCommits < 1 {
		t.Fatalf("SCENARIO NOT REACHED: the second image's store discarded nothing")
	}
	if _, err := restarted.Get(context.Background(), u30iDB, "User", zID); err != nil {
		t.Errorf("a first write that took a reused log sequence and was cut is gone after the restart: %v", err)
	}
}

// recordingPipeline records what PrepareLocked answered, over the writer's own LocalAdvancer.
type recordingPipeline struct {
	*LocalAdvancer
	heldOnce atomic.Bool
	calls    atomic.Int32
}

func (a *recordingPipeline) PrepareLocked(pair *cluster.Pair, slot uint16, db uint64) (bool, bool, error) {
	a.calls.Add(1)
	recordless, held, err := a.LocalAdvancer.PrepareLocked(pair, slot, db)
	if held {
		a.heldOnce.Store(true)
	}
	return recordless, held, err
}

// refusedFirstWritesLeaveNoHold is T13 (d): a first write the Reconciler's admitAdvance refuses,
// one whose stage ends with errNothingStaged, and on the pipelined RF 1 path one refused at
// planStage and one whose append fails, each leave no hold; a hold a failed first apply holds
// survives a later refused write to the same pair.
func refusedFirstWritesLeaveNoHold(t *testing.T) {
	t.Run("refused by admitAdvance", func(t *testing.T) {
		node := s0OpenClusterNode(t, t.TempDir(), nil)
		db := s0Schema(t, node)
		s0AwaitFirstPass(t, node, db)
		recon := node.cl.seeding.Load().recon
		slot := freeSlot(t, node.kv, db)
		key := reconcileKey{slot, db}
		recon.mu.Lock()
		recon.busy[key] = struct{}{}
		recon.mu.Unlock()
		err := recon.ApplyAdvance(slot, db, AdvanceRequest{
			Stage:    func(storage.KVBatch, *cluster.Pair) (Advance, error) { return Advance{}, errors.New("not reached") },
			Commit:   func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
			Appended: func() bool { return false },
		})
		recon.mu.Lock()
		delete(recon.busy, key)
		recon.mu.Unlock()
		if !errors.Is(err, ErrPairBusy) {
			t.Fatalf("SCENARIO NOT REACHED: ApplyAdvance on a busy pair = %v, want ErrPairBusy", err)
		}
		if at, held := holdOf(node.floor, pairKey{slot, db}); held {
			t.Errorf("a first write refused by admitAdvance left a hold at %d", at)
		}
		if got, want := node.floor.sample(), node.wal.SyncedSequence()+1; got != want {
			t.Errorf("the next sample is %d, want the synced sequence + 1 = %d", got, want)
		}
	})

	t.Run("a stage that ends with errNothingStaged", func(t *testing.T) {
		node := openSingleNode(t, t.TempDir(), true)
		db, err := node.database(u30iDB)
		if err != nil {
			t.Fatal(err)
		}
		slot := freeSlot(t, node.kv, db)
		advancer, ok := node.writer.cfg.Advancer.(*LocalAdvancer)
		if !ok || advancer.floor == nil {
			t.Fatalf("SCENARIO NOT REACHED: the node's writer advancer is %T with no floor tracker", node.writer.cfg.Advancer)
		}
		staged := false
		err = advancer.Advance(slot, db, AdvanceStep{
			Stage: func(storage.KVBatch, *cluster.Pair) (AdvanceTo, error) {
				staged = true
				_, held := holdOf(node.floor, pairKey{slot, db})
				if !held {
					return AdvanceTo{}, errors.New("SCENARIO NOT REACHED: no hold was registered before the stage")
				}
				return AdvanceTo{}, errNothingStaged
			},
			Commit:   func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
			Appended: func() bool { return false },
		})
		if !staged || !errors.Is(err, errNothingStaged) {
			t.Fatalf("SCENARIO NOT REACHED: the step staged %v and returned %v", staged, err)
		}
		if at, held := holdOf(node.floor, pairKey{slot, db}); held {
			t.Errorf("a stage that ended with errNothingStaged left a hold at %d", at)
		}
		if got, want := node.floor.sample(), node.wal.SyncedSequence()+1; got != want {
			t.Errorf("the next sample is %d, want the synced sequence + 1 = %d", got, want)
		}
	})

	pipelined := func(t *testing.T) (*fixture, *recordingPipeline, *logFloor) {
		fx := newFixture(t)
		local, ok := fx.w.cfg.Advancer.(*LocalAdvancer)
		if !ok {
			t.Fatalf("the fixture's advancer is %T", fx.w.cfg.Advancer)
		}
		floor := newLogFloor(fx.wal.WAL)
		local.floor = floor
		recorder := &recordingPipeline{LocalAdvancer: local}
		fx.w.cfg.Advancer = recorder
		return fx, recorder, floor
	}

	t.Run("pipelined: refused at planStage", func(t *testing.T) {
		// On a node: the fixture's applier builds no update. An update of a missing document is refused
		// at planStage, after PrepareLocked took the hold and before anything is appended.
		node := openSingleNode(t, t.TempDir(), true)
		db, err := node.database(u30iDB)
		if err != nil {
			t.Fatal(err)
		}
		local, ok := node.writer.cfg.Advancer.(*LocalAdvancer)
		if !ok {
			t.Fatalf("the node's writer advancer is %T", node.writer.cfg.Advancer)
		}
		recorder := &recordingPipeline{LocalAdvancer: local}
		node.writer.cfg.Advancer = recorder
		slot := freeSlot(t, node.kv, db)
		missing := idsInSlot(t, u30iDB, "User", slot, 1)[0]
		before := node.wal.LastSequence()
		_, err = node.Update(context.Background(), UpdateRequest{Database: u30iDB, Bucket: "User", ID: missing, Set: bson.D{{Key: "Name", Value: "x"}}})
		if err == nil || node.wal.LastSequence() != before || !recorder.heldOnce.Load() {
			t.Fatalf("SCENARIO NOT REACHED: the update of a missing document returned %v, the log moved from %d to %d, PrepareLocked held %v",
				err, before, node.wal.LastSequence(), recorder.heldOnce.Load())
		}
		if at, held := holdOf(node.floor, pairKey{slot, db}); held {
			t.Errorf("a first write refused at planStage left a hold at %d", at)
		}
		if got, want := node.floor.sample(), node.wal.SyncedSequence()+1; got != want {
			t.Errorf("the next sample is %d, want the synced sequence + 1 = %d", got, want)
		}
	})

	t.Run("pipelined: the append fails", func(t *testing.T) {
		fx, recorder, floor := pipelined(t)
		refusal := errors.New("no segment could be made")
		fx.wal.refuseUnsynced.Store(&refusal)
		_, err := fx.write(testSlot, "d1")
		fx.wal.refuseUnsynced.Store(nil)
		if err == nil || !recorder.heldOnce.Load() {
			t.Fatalf("SCENARIO NOT REACHED: the write with a refused append returned %v, PrepareLocked held %v", err, recorder.heldOnce.Load())
		}
		if at, held := holdOf(floor, pairKey{testSlot, testDB}); held {
			t.Errorf("a first write whose append failed left a hold at %d", at)
		}
	})

	t.Run("a failed first apply's hold survives a later refused write", func(t *testing.T) {
		// On the advancer: through the write path the later write never reaches the advancer, because a
		// pair whose apply failed is fenced and its writes are held before the first section (seen on
		// the pipelined path while this row was built). The hold matters exactly there: a step that
		// appended nothing must release only a hold it added (store-vouched-cut-build.md §2).
		node := openSingleNode(t, t.TempDir(), true)
		db, err := node.database(u30iDB)
		if err != nil {
			t.Fatal(err)
		}
		slot := freeSlot(t, node.kv, db)
		key := pairKey{slot, db}
		advancer, ok := node.writer.cfg.Advancer.(*LocalAdvancer)
		if !ok {
			t.Fatalf("the node's writer advancer is %T", node.writer.cfg.Advancer)
		}
		failedApply := errors.New("the first apply failed after its append")
		err = advancer.Advance(slot, db, AdvanceStep{
			Stage:    func(storage.KVBatch, *cluster.Pair) (AdvanceTo, error) { return AdvanceTo{}, failedApply },
			Commit:   func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
			Appended: func() bool { return true },
		})
		failedAt, held := holdOf(node.floor, key)
		if !errors.Is(err, failedApply) || !held {
			t.Fatalf("SCENARIO NOT REACHED: the failed first apply returned %v and holds %v", err, held)
		}
		err = advancer.Advance(slot, db, AdvanceStep{
			Stage:    func(storage.KVBatch, *cluster.Pair) (AdvanceTo, error) { return AdvanceTo{}, errNothingStaged },
			Commit:   func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
			Appended: func() bool { return false },
		})
		if !errors.Is(err, errNothingStaged) {
			t.Fatalf("SCENARIO NOT REACHED: the later step returned %v, want errNothingStaged", err)
		}
		if at, held := holdOf(node.floor, key); !held || at != failedAt {
			t.Errorf("a later step that appended nothing took away the hold %d of the pair's failed first apply (now %d, held %v): "+
				"the replay of that apply would sit below the floor", failedAt, at, held)
		}
	})
}
