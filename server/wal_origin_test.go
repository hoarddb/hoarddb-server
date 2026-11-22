package server

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestNoProductionCallerAppendsWithoutAnOrigin: an entry appended with
// WAL.Append carries no origin, so a replay cannot tell whether this node wrote
// it or applied it for another. Every production append goes through
// Cluster.appendWAL, which requires one.
func TestNoProductionCallerAppendsWithoutAnOrigin(t *testing.T) {
	bareAppend := regexp.MustCompile(`\bwal\.Append\(`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the server package: %v", err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		checked++
		if location := bareAppend.FindIndex(source); location != nil {
			line := strings.Count(string(source[:location[0]]), "\n") + 1
			t.Errorf("%s:%d appends to the write-ahead log with no origin; use AppendFrom through Cluster.appendWAL", file, line)
		}
	}
	if checked < 10 {
		t.Fatalf("looked at %d production files; the glob found the wrong directory", checked)
	}
}

// TestWALEntriesRecordWhereTheyCameFrom runs the two write paths on a real
// three-node cluster and reads each node's log: the owner's entries say they
// were originated there, under the epoch it read; a replica's say whose entry
// they applied and at which of the owner's sequences.
func TestWALEntriesRecordWhereTheyCameFrom(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	const pushes = 4
	for i := 0; i < pushes; i++ {
		tc.ok(0, transport.OpFIFOPush, PushRequest{
			Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"n": i}, WriteConcern: "all",
		})
	}

	cluster := tc.nodes[0].server.core.cluster
	owner, err := cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve the owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)

	logs := make([][]storage.WALEntry, len(tc.nodes))
	for i := range tc.nodes {
		waitForWALSequence(t, tc.nodes[i], 1+1+pushes)
		entries, err := tc.nodes[i].server.core.wal.ReadFrom(1)
		if err != nil {
			t.Fatalf("reading node %d's log: %v", i+1, err)
		}
		logs[i] = entries
	}

	ownerEpoch := tc.nodes[ownerIndex].server.core.cluster.Epoch()
	ownedBySeq := map[uint64]storage.WALEntry{}
	bucketEntries := 0
	for _, entry := range logs[ownerIndex] {
		if entry.Bucket == "" {
			continue
		}
		bucketEntries++
		ownedBySeq[entry.Seq] = entry
		if entry.Origin.Kind != storage.WALOriginOwn || entry.Origin.Epoch != ownerEpoch {
			t.Errorf("the owner's entry %d (op %d) has origin %+v; want own at epoch %d", entry.Seq, entry.Op, entry.Origin, ownerEpoch)
		}
	}
	if bucketEntries != 1+pushes {
		t.Fatalf("the owner logged %d bucket entries, want %d", bucketEntries, 1+pushes)
	}

	for i, entries := range logs {
		if i == ownerIndex {
			continue
		}
		replicated := 0
		for _, entry := range entries {
			if entry.Bucket == "" {
				continue
			}
			replicated++
			want, found := ownedBySeq[entry.Origin.Seq]
			if entry.Origin.Kind != storage.WALOriginReplica || entry.Origin.Source != owner.ID || !found ||
				want.Op != entry.Op || want.Bucket != entry.Bucket {
				t.Errorf("node %d's entry %d (op %d) has origin %+v; want replica of the owner %s at one of its sequences",
					i+1, entry.Seq, entry.Op, entry.Origin, owner.ID)
			}
		}
		if replicated != 1+pushes {
			t.Errorf("node %d logged %d bucket entries, want %d", i+1, replicated, 1+pushes)
		}
	}

	// A database-level change is broadcast: the node that took the request
	// originated it, every other node applied it.
	coordinatorID := tc.addrs[0]
	var coordinatorSeq uint64
	for _, entry := range logs[0] {
		if entry.Bucket == "" && entry.Op == uint8(transport.OpCreateDB) {
			coordinatorSeq = entry.Seq
			if entry.Origin.Kind != storage.WALOriginOwn {
				t.Errorf("the coordinator's create-database entry has origin %+v; want own", entry.Origin)
			}
		}
	}
	if coordinatorSeq == 0 {
		t.Fatal("the coordinator's log holds no create-database entry")
	}
	for i := 1; i < len(logs); i++ {
		found := false
		for _, entry := range logs[i] {
			if entry.Bucket != "" || entry.Op != uint8(transport.OpCreateDB) {
				continue
			}
			found = true
			if entry.Origin.Kind != storage.WALOriginReplica || entry.Origin.Source != coordinatorID || entry.Origin.Seq != coordinatorSeq {
				t.Errorf("node %d's create-database entry has origin %+v; want replica of %s at sequence %d",
					i+1, entry.Origin, coordinatorID, coordinatorSeq)
			}
		}
		if !found {
			t.Errorf("node %d's log holds no create-database entry", i+1)
		}
	}
}
