package harness_test

import (
	"fmt"
	"math/bits"
	"path/filepath"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/storage"
)

// oldReporterBatchBits is the most bits one SEEDED entry could carry when the reporter sent
// 64 slots per command (the pre-fix seedReporter.takeBatch drained at most 64 per direction).
// A reporter that sends everything pending per flush carries more in some entry, whatever the
// host's speed: each node's 5,000-odd bits turn ready over seconds, far faster than the 32
// bits a second below which no flush would ever reach 64 pending within the 2 s age limit.
// This is the timing-free guard. The number of entries is not asserted, only logged: it is the
// number of 250 ms flushes in which bits were pending (the fix measured 1 to 10 per node on a
// 4-vCPU VM, 64 slots per command measured 148 to 190), so any bound on it depends on how long
// the seed takes on the host.
const oldReporterBatchBits = 64

// TestANewDatabaseCommitsAFewSeededEntriesPerNode is BUG-0808's cause on a real cluster: the
// seed bits of a new database filled the Raft log with one entry per 64 slots, 207 to 264
// entries per database on the three-node rig, and the log's fsyncs pushed the quorum rounds
// past their 300 ms bound. The count is read from a node's Raft log itself, every command
// entry decoded by kind, after every pair of the database has its committed bit.
// [BREAK: drain at most 64 slots per command in seedReporter.takeBatch]
func TestANewDatabaseCommitsAFewSeededEntriesPerNode(t *testing.T) {
	const rf = 2
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, NoProbes: true})
	leaderID := c.AwaitLeader(10 * time.Second)
	id, err := c.Node(leaderID).CreateDatabaseRF(proofCtx(t), proofDB, rf)
	if err != nil {
		t.Fatalf("creating the RF=%d database: %v", rf, err)
	}
	dbID := int64(id)
	c.AwaitWhileSeedingMoves("every pair of the new database to have its committed bit", 90*time.Second, 240*time.Second, func() bool {
		return committedSeedBits(c.Node(leaderID).State(), dbID) == storage.SlotCount*rf
	})
	c.AwaitConverged(10 * time.Second)

	var reader string
	for _, nodeID := range c.IDs() {
		if nodeID != leaderID {
			reader = nodeID
			break
		}
	}
	c.Kill(reader)
	perNode := seededEntriesByNode(t, filepath.Join(c.DataDir(reader), "raft", "raft.db"), dbID)
	t.Logf("bits carried by each SEEDED entry for database %d, by node: %v", dbID, perNode)
	for _, nodeID := range c.IDs() {
		got := len(perNode[nodeID])
		if got == 0 {
			t.Errorf("%s put no SEEDED entry for database %d in the log, yet holds ready pairs of it", nodeID, dbID)
		}
		if widest := widestEntry(perNode[nodeID]); widest <= oldReporterBatchBits {
			t.Errorf("%s: no SEEDED entry for database %d carries more than %d bits (the widest carries %d), which is how a reporter that sends 64 slots per command looks: want every pending bit of the database in one command",
				nodeID, dbID, oldReporterBatchBits, widest)
		}
	}
}

// widestEntry is the most bits any one entry of a node carries.
func widestEntry(carried []int) int {
	widest := 0
	for _, bitCount := range carried {
		widest = max(widest, bitCount)
	}
	return widest
}

// committedSeedBits counts the set bits of every node's committed row for dbID.
func committedSeedBits(state cluster.ClusterState, dbID int64) int {
	total := 0
	for _, row := range state.SeedBitmaps {
		if row.DBID != dbID {
			continue
		}
		for _, b := range row.Bits {
			total += bits.OnesCount8(b)
		}
	}
	return total
}

// seededEntriesByNode opens a stopped node's Raft log and lists, by the node they report for,
// how many bits each SEEDED entry for dbID carries, in log order. A log compacted past its
// first entry no longer holds every entry, so it fails the test instead of undercounting.
func seededEntriesByNode(t *testing.T, path string, dbID int64) map[string][]int {
	t.Helper()
	store, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		t.Fatalf("opening the Raft log %s: %v", path, err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the Raft log %s: %v", path, err)
		}
	}()
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("reading the first index of %s: %v", path, err)
	}
	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("reading the last index of %s: %v", path, err)
	}
	if first > 1 {
		t.Fatalf("the Raft log starts at index %d: a snapshot compacted it, so it cannot be counted", first)
	}
	perNode := make(map[string][]int)
	for index := first; index <= last; index++ {
		var entry hraft.Log
		if err := store.GetLog(index, &entry); err != nil {
			t.Fatalf("reading Raft entry %d: %v", index, err)
		}
		if entry.Type != hraft.LogCommand {
			continue
		}
		cmd, err := cluster.DecodeCommand(entry.Data)
		if err != nil {
			t.Fatalf("decoding Raft entry %d: %v", index, err)
		}
		if cmd.Kind != cluster.CommandSeeded {
			continue
		}
		node, db, carried, err := seededTarget(cmd.Body)
		if err != nil {
			t.Fatalf("Raft entry %d: %v", index, err)
		}
		if db == dbID {
			perNode[node] = append(perNode[node], carried)
		}
	}
	return perNode
}

// seededTarget reads the node and database a SEEDED body names and counts the bits it sets.
func seededTarget(body []byte) (string, int64, int, error) {
	doc := bsoncore.Document(body)
	node, okNode := doc.Lookup("node").StringValueOK()
	db, okDB := doc.Lookup("db_id").Int64OK()
	_, mask, okBits := doc.Lookup("bits").BinaryOK()
	if !okNode || !okDB || !okBits {
		return "", 0, 0, fmt.Errorf("a SEEDED body without node, db_id or bits: %v", doc)
	}
	carried := 0
	for _, b := range mask {
		carried += bits.OnesCount8(b)
	}
	return node, db, carried, nil
}
