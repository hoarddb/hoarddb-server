package wire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A ring store with no marker and no data file is a first start, and the store writes a marker
// with nothing vouched and opens. When the rest of the ring directory shows the node has run
// before, it is not a first start: ring/badger was moved aside or deleted, and starting on an
// empty store would serve as empty, as ready, every pair this node holds by a seed (seeded data
// never passes through the log), and later every write the log no longer holds. Review R-1: the
// first step of the old reseed advice ("move ring/badger aside") led there, and nothing refused it.

// TestAnEmptyRingStoreAfterAnEarlierLifeIsRefused: [GUARD on a cluster member, a store first start
// is refused when the node's Raft holds state or its log holds records, before the store writes
// anything, so the next start is refused again; red when the open takes it as a first start]. The
// signals are the Raft state and the log, never the incarnation record, which every start rewrites
// before the store opens, so a crash inside a real first start would trip it. A single node is
// TestASingleNodeRebuildsAnEmptyStoreFromAWholeLog's (owner ruling 2026-11-19, review R2 F-3).
func TestAnEmptyRingStoreAfterAnEarlierLifeIsRefused(t *testing.T) {
	rows := []struct {
		name string
		// ran builds a ring directory that has run before and returns it.
		ran func(t *testing.T) string
		// lose removes what the operator moved aside.
		lose []string
		// says is what the refusal reports as the earlier life.
		says []string
	}{
		{"badger moved aside after writes", ranRingDir, []string{"badger"},
			[]string{"its Raft state holds a term, a log entry or a snapshot", "its log holds records through sequence "}},
		{"badger moved aside, no write ever (a seeded joiner's shape: nothing in the log)", ranWithoutWrites, []string{"badger"},
			[]string{"its Raft state holds a term, a log entry or a snapshot"}},
		{"badger and raft moved aside, the log kept", ranRingDir, []string{"badger", "raft"},
			[]string{"its log holds records through sequence "}},
	}
	t.Run("the marker kept and every data file deleted", func(t *testing.T) {
		// Review R2 F-5: an operator who deletes inside the directory instead of moving it.
		dir := ranRingDir(t)
		store := filepath.Join(dir, "badger")
		deleteAllButTheMarker(t, store)
		requireRefusedTwiceKeepingTheMarker(t, dir)
	})
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			dir := row.ran(t)
			moveAside(t, dir, row.lose...)
			store := filepath.Join(dir, "badger")
			requireRefusedTwice(t, dir, true, append([]string{
				"the node store " + store + " holds no data file, but this node has run before: ",
				"data copied in by a seed never passes through the log", "Nothing was written to the store.",
				"If " + store + " was moved aside or deleted, put it back"}, row.says...), singleNodeRebuildAdviceFor(dir))
		})
	}
}

// TestASingleNodeRebuildsAnEmptyStoreFromAWholeLog: [GUARD a single node (not a cluster member)
// whose store is gone and whose log holds every write from sequence 1 rebuilds the store by the
// boot replay and starts, and one whose log begins later is refused, saying the writes before it
// are in neither; red when a single node is refused like a member, or opens over a cut log]. Nothing
// seeds or truncates a single node in this build, so its log is the record of every write (owner
// ruling 2026-11-19 on review R2 F-3). Retention must never truncate a log while the store's marker
// vouches version 0, or a store rebuilt this way would miss what the log no longer holds (BUG-0829).
func TestASingleNodeRebuildsAnEmptyStoreFromAWholeLog(t *testing.T) {
	rebuilt := []struct {
		name string
		ran  func(t *testing.T) string
		lose []string
		want bool // the written document is back
	}{
		{"badger moved aside after writes", ranRingDir, []string{"badger"}, true},
		{"badger moved aside, no write ever", ranWithoutWrites, []string{"badger"}, false},
	}
	for _, row := range rebuilt {
		t.Run(row.name, func(t *testing.T) {
			dir := row.ran(t)
			moveAside(t, dir, row.lose...)
			node := u30iOpen(t, dir)
			if !node.kv.VouchedOpenReport().FirstStart {
				t.Fatal("SCENARIO NOT REACHED: the store's open was not a first start")
			}
			if !row.want {
				return
			}
			if _, err := node.Get(context.Background(), u30iDB, "User", "u1"); err != nil {
				t.Fatalf("the store was not rebuilt from the log: %v", err)
			}
		})
	}
	t.Run("the marker kept and every data file deleted: rebuilt", func(t *testing.T) {
		dir := ranRingDir(t)
		deleteAllButTheMarker(t, filepath.Join(dir, "badger"))
		node := u30iOpen(t, dir)
		if _, err := node.Get(context.Background(), u30iDB, "User", "u1"); err != nil {
			t.Fatalf("the store was not rebuilt from the log: %v", err)
		}
	})
	t.Run("a log without its Raft state is refused", func(t *testing.T) {
		// The databases' declarations are in the Raft state; the log alone cannot rebuild what it
		// names.
		dir := ranRingDir(t)
		moveAside(t, dir, "badger", "raft")
		store := filepath.Join(dir, "badger")
		requireRefusedTwice(t, dir, false, []string{
			"the node store " + store + " holds no data file, and this node's Raft state is gone while its log holds " +
				"records through sequence ",
			"the log cannot be replayed without the declarations the Raft state holds, so the store cannot be rebuilt. " +
				"Nothing was written to the store. If " + store + " was moved aside or deleted, put it back"},
			singleNodeKeepAdviceFor(dir))
	})
	t.Run("a log that does not begin at sequence 1 is refused", func(t *testing.T) {
		dir, first := ranWithATruncatedLog(t)
		moveAside(t, dir, "badger")
		store := filepath.Join(dir, "badger")
		requireRefusedTwice(t, dir, false, []string{
			fmt.Sprintf("the node store %s holds no data file, and this node's log begins at sequence %d, not 1: "+
				"the writes before %d are in neither the store nor the log, so starting would lose them. "+
				"Nothing was written to the store. If %s was moved aside or deleted, put it back", store, first, first, store)},
			singleNodeKeepAdviceFor(dir))
	})
}

// moveAside renames the named parts of the ring directory dir out of it.
func moveAside(t *testing.T, dir string, parts ...string) {
	t.Helper()
	aside := t.TempDir()
	for _, part := range parts {
		if err := os.Rename(filepath.Join(dir, part), filepath.Join(aside, part)); err != nil {
			t.Fatal(err)
		}
	}
}

// requireRefusedTwice opens dir twice, as a member or a single node, and requires the empty-store
// refusal each time, with every text of says and the given single-node advice (or the member's),
// and no marker written, so the next start is refused again. single is the single-node advice: the
// one wanted for a single node, the one banned for a member.
func requireRefusedTwice(t *testing.T, dir string, member bool, says []string, single string) {
	t.Helper()
	for attempt := 1; attempt <= 2; attempt++ {
		node, err := OpenNode(context.Background(), nodeConfigFor(t, dir, member))
		if err == nil {
			_ = node.Close() // the open was to be refused; the failure below is the result
			t.Fatalf("start %d: the node opened on an empty store", attempt)
		}
		var refusal *EmptyStoreAfterEarlierLifeError
		if !errors.As(err, &refusal) {
			t.Fatalf("start %d: the refusal is %T, want an EmptyStoreAfterEarlierLifeError: %v", attempt, err, err)
		}
		text := err.Error()
		for _, want := range says {
			if !strings.Contains(text, want) {
				t.Errorf("start %d: the refusal lacks %q: %s", attempt, want, text)
			}
		}
		requireRingAdvice(t, text, dir, member, single)
		if _, statErr := os.Stat(filepath.Join(dir, "badger", "VOUCHED")); !os.IsNotExist(statErr) {
			t.Fatalf("start %d: the refused start wrote a marker (%v), so the next start would pass", attempt, statErr)
		}
	}
}

// ranWithATruncatedLog runs a single node with small log segments, writes until the log has rotated,
// truncates it below its last sequence and closes it. It returns the directory and the log's first
// sequence, which is above 1.
func ranWithATruncatedLog(t *testing.T) (string, uint64) {
	t.Helper()
	dir := t.TempDir()
	node := u30iOpenNoCleanup(t, dir)
	node.SetLogSegmentBytes(4 << 10)
	u30iSchema(t, node)
	for i := range 64 {
		if _, err := node.Insert(context.Background(), u30iDB, "User", u30iUser(fmt.Sprintf("t%d", i), "Ada", int32(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.TruncateLogBelow(node.wal.LastSequence()); err != nil {
		t.Fatal(err)
	}
	first, err := node.wal.FirstSequence()
	if err != nil {
		t.Fatal(err)
	}
	if first <= 1 {
		t.Fatalf("SCENARIO NOT REACHED: the truncated log still begins at %d", first)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, first
}

// TestARealFirstStartStillOpens: a ring directory whose earlier start stopped before the store
// wrote its marker holds the format marker and the incarnation record only; it is a first start,
// and it opens. The guard's signals are the Raft state and the log, not those two files.
func TestARealFirstStartStillOpens(t *testing.T) {
	dir := t.TempDir()
	if err := ensureRingFormat(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := nextIncarnation(NodeConfig{Dir: dir, Now: time.Now}); err != nil {
		t.Fatal(err)
	}
	node := u30iOpen(t, dir)
	if !node.kv.VouchedOpenReport().FirstStart {
		t.Fatal("the store's open was not a first start")
	}
}

// ranWithoutWrites opens a single node on a new directory and closes it with no write: its Raft
// holds the bootstrap, its log holds nothing.
func ranWithoutWrites(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := u30iOpenNoCleanup(t, dir).Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// loseStoreForTheReplay removes the ring store under dir so that the next open rebuilds it from the
// log, which the replay tests check: a single node rebuilds an empty store from a log that begins at
// sequence 1 (refuseEmptyStoreAfterEarlierLife).
func loseStoreForTheReplay(t testing.TB, dir string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(dir, "badger")); err != nil {
		t.Fatal(err)
	}
}

// deleteAllButTheMarker removes every entry of the store directory but its sync marker.
func deleteAllButTheMarker(t *testing.T, store string) {
	t.Helper()
	entries, err := os.ReadDir(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "VOUCHED" {
			if err := os.RemoveAll(filepath.Join(store, entry.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// requireRefusedTwiceKeepingTheMarker opens dir twice as a member and requires the empty-store
// refusal each time, with the marker still the only entry of the store.
func requireRefusedTwiceKeepingTheMarker(t *testing.T, dir string) {
	t.Helper()
	store := filepath.Join(dir, "badger")
	for attempt := 1; attempt <= 2; attempt++ {
		node, err := OpenNode(context.Background(), nodeConfigFor(t, dir, true))
		if err == nil {
			_ = node.Close() // the open was to be refused; the failure below is the result
			t.Fatalf("start %d: a member opened a store that kept its marker and lost its data files", attempt)
		}
		var refusal *EmptyStoreAfterEarlierLifeError
		if !errors.As(err, &refusal) {
			t.Fatalf("start %d: the refusal is %T, want an EmptyStoreAfterEarlierLifeError: %v", attempt, err, err)
		}
		requireRingAdvice(t, err.Error(), dir, true, singleNodeRebuildAdviceFor(dir))
		entries, err := os.ReadDir(store)
		if err != nil || len(entries) != 1 {
			t.Fatalf("start %d: the refused start changed the store (%d entries, %v)", attempt, len(entries), err)
		}
	}
}

// ranWithASchemaOnly runs a single node that declared a database and a bucket and wrote no
// document: its Raft holds the declaration, its log holds nothing a replay needs.
func ranWithASchemaOnly(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	node := u30iOpenNoCleanup(t, dir)
	u30iSchema(t, node)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestASingleNodeThatLostItsStoreAndItsLogIsRefusedAndSaysTheLogIsMissing: [GUARD a single node whose
// store and log are both gone, and whose Raft holds a database, refuses to open and says the log is
// missing, never that the log holds writes through sequence 0; red when it rebuilds an empty store
// under a false WARN]. A node that never wrote a declaration has nothing to lose and still opens.
func TestASingleNodeThatLostItsStoreAndItsLogIsRefusedAndSaysTheLogIsMissing(t *testing.T) {
	for _, ran := range []func(*testing.T) string{ranRingDir, ranWithASchemaOnly} {
		dir := ran(t)
		moveAside(t, dir, "badger", "wal")
		store := filepath.Join(dir, "badger")
		requireRefusedTwice(t, dir, false, []string{
			"the node store " + store + " holds no data file, and this node's log is missing while its Raft state holds a database",
			"the store cannot be rebuilt from a log that is not there. Nothing was written to the store."}, singleNodeKeepAdviceFor(dir))
		node, err := OpenNode(context.Background(), nodeConfigFor(t, dir, false))
		if err == nil {
			_ = node.Close() // the open was to be refused
			t.Fatal("opened")
		}
		if strings.Contains(err.Error(), "through 0") {
			t.Fatalf("the refusal speaks of sequence 0: %v", err)
		}
	}
	t.Run("a node that never declared a database still opens", func(t *testing.T) {
		dir := ranWithoutWrites(t)
		moveAside(t, dir, "badger")
		if err := os.RemoveAll(filepath.Join(dir, "wal")); err != nil {
			t.Fatal(err)
		}
		node := u30iOpen(t, dir)
		if !node.kv.VouchedOpenReport().FirstStart {
			t.Fatal("not a first start")
		}
	})
}

// TestALogDirectoryRemovedUnderARunningNodeGetsTheNodesAdvice: [GUARD the storage refusal about a
// directory removed while the node ran ends in the node's keep advice, never in an emptied
// directory; red when it offers 'leave it empty']. A member cannot be opened in a unit test (it needs
// a listener); the member's clause for these texts is pinned by TestARegisteredAdviceReplacesTheDefaultInEachDirectoryRefusal
// and by ringDirAdvice, which the registration uses.
func TestALogDirectoryRemovedUnderARunningNodeGetsTheNodesAdvice(t *testing.T) {
	dir := t.TempDir()
	node := u30iOpenNoCleanup(t, dir)
	defer func() { _ = node.Close() }() // the refusal under test is the result; a close error adds nothing
	u30iSchema(t, node)
	if err := os.RemoveAll(filepath.Join(dir, "wal")); err != nil {
		t.Fatal(err)
	}
	// The guard stats the directory at most every 100 ms; the first write after the removal may
	// share the answer of the last stat.
	var err error
	for range 20 {
		time.Sleep(150 * time.Millisecond)
		if _, err = node.Insert(context.Background(), u30iDB, "User", u30iUser("g1", "Ada", 1)); err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("a write was acknowledged after its directory was removed")
	}
	if !strings.Contains(err.Error(), singleNodeKeepAdviceFor(dir)) {
		t.Errorf("the refusal does not carry the single node's advice: %v", err)
	}
	for _, banned := range []string{"leave it empty", "remove-node", "re-seed"} {
		if strings.Contains(err.Error(), banned) {
			t.Errorf("the refusal says %q: %v", banned, err)
		}
	}
}
