package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The ring store's refusals name the damage and no advice; the node adds the advice, and it depends
// on what the node is (store-vouched-cut review R-1). A cluster member is never told to move its
// ring directory aside or to start on an empty one: it keeps its key and id, its peers admit it,
// and it rejoins as a voter that has forgotten its term, vote and log, which can lose committed
// cluster state (BUG-0870 design note §2). A single node has no peers, so nothing can seed it: it
// is told plainly that starting empty loses the data unless it is restored from a backup.

// The two advice texts as an operator reads them, spelled out so a change to either is a change
// to this test. The member text is ClusterMemberRingAdvice, which the server's encryption refusals
// give a member too (server/ring_keys.go); the decision is the
// configuration alone, NodeConfig.Cluster, which the server sets exactly when cluster.enabled is.
func memberAdviceFor(ringDir string) string {
	return "This node is configured as a member of a cluster (cluster.enabled), so do not move " + ringDir + " aside " +
		"while the cluster runs: that can lose committed cluster state. Stop this node, keep " + ringDir + ", start it again on an empty ring directory " +
		"with the same node id, keys and address, and run 'replace-node <node> --with <node>@<addr>' on any member"
}

// singleNodeKeepAdviceFor is a single node's advice when a part of its ring directory was moved
// aside or deleted and is to be put back: nothing else holds the data, so the directory is kept.
func singleNodeKeepAdviceFor(ringDir string) string {
	return "This node is not a member of a cluster, so no other node holds this data: keep " + ringDir
}

// singleNodeEmptyRingAdviceFor is a single node's advice when its ring directory is not in the
// format this release reads: nothing in it was readable at all, and the node starts on an empty one.
func singleNodeEmptyRingAdviceFor(ringDir string) string {
	return "This node is not a member of a cluster, so it can start with an empty ring directory: move " + ringDir +
		" aside (keep it if you want it) and start the node again; it starts with no data"
}

// singleNodeUnreadAdviceFor is a single node's advice when this release cannot read a record of
// its log (report "the sequence number above") or its sync marker's format (report "the text
// above"): the directory is the only copy and is kept, a dump or backup is the way back to the
// data, and the evidence is reported (review F1 of 2026-11-21).
func singleNodeUnreadAdviceFor(ringDir, report string) string {
	return "This node is not a member of a cluster, so no other node holds this data and " + ringDir +
		" is the only copy: do not delete it. If you have a dump or backup, move " + ringDir +
		" aside (keep it), start the node again (it starts with no data), and restore the dump into it. " +
		"If you have none, the same steps start the node with no data and leave this directory unread; " +
		"keep it and report " + report
}

func singleNodeUnreadLogAdviceFor(ringDir string) string {
	return singleNodeUnreadAdviceFor(ringDir, "the sequence number above")
}

func singleNodeUnreadMarkerAdviceFor(ringDir string) string {
	return singleNodeUnreadAdviceFor(ringDir, "the text above")
}

// singleNodeRebuildAdviceFor is a single node's advice when its store is the problem: nothing seeds
// or truncates a single node, so it rebuilds its store from its own log when the log is whole (owner
// ruling 2026-11-19 on review R2 F-3).
func singleNodeRebuildAdviceFor(ringDir string) string {
	store, log := filepath.Join(ringDir, "badger"), filepath.Join(ringDir, "wal")
	return "This node is not a member of a cluster, so it can rebuild its store from its own log: move " + store +
		" aside (keep it) and start the node; it rebuilds the store from " + log + " when the log holds every write " +
		"from sequence 1, and refuses to start, saying so, when it does not"
}

// requireNoOtherBuild fails when a refusal sends the operator to a build other than this one. No
// other build was released (owner ruling 2026-11-21), so a refusal names what it found and the fix
// that works for this release.
func requireNoOtherBuild(t *testing.T, refusal string) {
	t.Helper()
	for _, banned := range []string{"earlier build", "earlier development build", "another build", "previous release", "the build that wrote",
		"earlier version", "older version", "previous version", "older build", "earlier hoardDB", "earlier engine"} {
		if strings.Contains(refusal, banned) {
			t.Errorf("the refusal says %q, and no other build was released: %s", banned, refusal)
		}
	}
}

// requireRingAdvice fails unless refusal ends in the member advice or in the given single-node
// advice, and never in the other's, a reseed, the seed list or a claim that the data is lost.
func requireRingAdvice(t *testing.T, refusal, ringDir string, member bool, single string) {
	t.Helper()
	want, other := single, memberAdviceFor(ringDir)
	if member {
		want, other = other, want
	}
	if !strings.HasSuffix(refusal, ". "+want) {
		t.Errorf("the refusal does not end in the advice for a %s:\n got: %s\nwant suffix: %s", nodeKind(member), refusal, want)
	}
	if strings.Contains(refusal, other) {
		t.Errorf("a %s is given the other kind of node's advice: %s", nodeKind(member), refusal)
	}
	for _, banned := range []string{"reseed", "HOARDDB_SEED_NODES", "its peers", "seeded by", "empty data directory", "BUG-",
		"a backup of this data directory", "Restore this node from its own data", "is lost", "remove-node"} {
		if strings.Contains(refusal, banned) {
			t.Errorf("the refusal says %q, advice that loses data on this node: %s", banned, refusal)
		}
	}
	if member && strings.Contains(refusal, ringDir+" aside (keep it") {
		t.Errorf("a cluster member is told to move its ring directory aside: %s", refusal)
	}
}

func nodeKind(member bool) string {
	if member {
		return "cluster member"
	}
	return "single node"
}

// nodeConfigFor returns the configuration of a single node or a cluster member on dir: the
// refusals tested here come from the store's open, which runs before the node dials any peer, so
// a member's empty options suffice.
func nodeConfigFor(t testing.TB, dir string, member bool) NodeConfig {
	cfg := vouchedTestConfig(t, NodeConfig{Dir: dir})
	if member {
		cfg.Cluster = &ClusterOptions{}
	}
	return cfg
}

// ranRingDir opens a single node on a new directory, writes to it and closes it, so its store holds
// a marker, a MANIFEST and the table the close flushed.
func ranRingDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	node := u30iOpenNoCleanup(t, dir)
	u30iSchema(t, node)
	if _, err := node.Insert(context.Background(), u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func u30iOpenNoCleanup(t *testing.T, dir string) *Node {
	t.Helper()
	node, err := OpenNode(context.Background(), vouchedTestConfig(t, NodeConfig{Dir: dir}))
	if err != nil {
		t.Fatalf("OpenNode(%s): %v", dir, err)
	}
	return node
}

// TestARefusedRingStoreGetsTheAdviceForWhatTheNodeIs: [GUARD the ring store's refusals reach the
// operator with the advice for a cluster member or for a single node, chosen by NodeConfig.Cluster,
// and neither names a reseed or the seed list; red when storage adds reseed advice or the node adds
// none]. One case per kind of refusal: a damaged marker, no marker beside data files, and a damage
// refusal of the store's files (StoreDamagedError).
func TestARefusedRingStoreGetsTheAdviceForWhatTheNodeIs(t *testing.T) {
	damages := []struct {
		name   string
		damage func(t *testing.T, store string)
		want   func(err error) bool
	}{
		{"a damaged marker", func(t *testing.T, store string) {
			tearBothMarkerSlots(t, store)
		}, func(err error) bool { var e *storage.VouchedMarkerDamagedError; return errors.As(err, &e) }},
		{"no marker beside data files", func(t *testing.T, store string) {
			if err := os.Remove(filepath.Join(store, "VOUCHED")); err != nil {
				t.Fatal(err)
			}
		}, func(err error) bool { var e *storage.VouchedMarkerMissingError; return errors.As(err, &e) }},
		{"a missing MANIFEST beside tables", func(t *testing.T, store string) {
			tables, err := filepath.Glob(filepath.Join(store, "*.sst"))
			if err != nil || len(tables) == 0 {
				t.Fatalf("SCENARIO NOT REACHED: the close left no table (%v)", err)
			}
			if err := os.Remove(filepath.Join(store, "MANIFEST")); err != nil {
				t.Fatal(err)
			}
		}, func(err error) bool { var e *storage.StoreDamagedError; return errors.As(err, &e) }},
		// Review R2 F-6: Badger's own open refusals. A table whose index fails its checksum is
		// refused by badger.Open (the guards check only the footer's lengths); without the strip
		// the legacy text ('remove-node ... empty data directory') came before the node's advice.
		{"a table whose index fails its checksum at Badger's open", func(t *testing.T, store string) {
			flipATableIndexByte(t, store)
		}, func(err error) bool { var e *storage.StoreDamagedError; return errors.As(err, &e) }},
		// A table the MANIFEST names and the directory lacks: Badger refuses with "file does not
		// exist for table N", which reached the operator with no advice.
		{"a table the MANIFEST names is missing", func(t *testing.T, store string) {
			tables, err := filepath.Glob(filepath.Join(store, "*.sst"))
			if err != nil || len(tables) == 0 {
				t.Fatalf("SCENARIO NOT REACHED: the close left no table (%v)", err)
			}
			if err := os.Remove(tables[0]); err != nil {
				t.Fatal(err)
			}
		}, func(err error) bool { var e *storage.StoreDamagedError; return errors.As(err, &e) }},
	}
	for _, tc := range damages {
		for _, member := range []bool{false, true} {
			t.Run(tc.name+", "+nodeKind(member), func(t *testing.T) {
				dir := ranRingDir(t)
				tc.damage(t, filepath.Join(dir, "badger"))
				node, err := OpenNode(context.Background(), nodeConfigFor(t, dir, member))
				if err == nil {
					_ = node.Close() // the open was to be refused; the failure below is the result
					t.Fatal("the node opened on a damaged store")
				}
				if !tc.want(err) {
					t.Fatalf("the refusal is not the store's (%T): %v", err, err)
				}
				requireRingAdvice(t, err.Error(), dir, member, singleNodeRebuildAdviceFor(dir))
			})
		}
	}
}

// TestAMarkerWithBothSlotsTornGetsTheWholeRefusalForWhatTheNodeIs: [GUARD a sync marker in which
// neither slot is whole is refused naming the file, then the advice for this node, byte for byte:
// the member clause for a cluster member, the store's rebuild advice (which keeps the store
// directory) for a single node; red while the marker is one record (slice S-3)].
func TestAMarkerWithBothSlotsTornGetsTheWholeRefusalForWhatTheNodeIs(t *testing.T) {
	for _, member := range []bool{false, true} {
		t.Run(nodeKind(member), func(t *testing.T) {
			dir := ranRingDir(t)
			path := filepath.Join(dir, "badger", "VOUCHED")
			tearBothMarkerSlots(t, filepath.Join(dir, "badger"))
			node, err := OpenNode(context.Background(), nodeConfigFor(t, dir, member))
			if err == nil {
				_ = node.Close() // the open was to be refused; the failure below is the result
				t.Fatal("the node opened on a marker with both slots torn")
			}
			advice := singleNodeRebuildAdviceFor(dir)
			if member {
				advice = memberAdviceFor(dir)
			}
			want := "opening Badger in " + dir + ": the store's sync marker " + path + " is damaged (neither of its two slots is whole: each fails its " +
				"checksum), so this node cannot tell which of its last writes reached the disk. Nothing was changed. " + advice
			if err.Error() != want {
				t.Fatalf("the refusal is not the whole text for a %s:\n got: %s\nwant: %s", nodeKind(member), err, want)
			}
		})
	}
}

// TestAnUnreadFormatGetsTheAdviceForWhatTheNodeIs: [GUARD a directory or a marker this release
// does not read is refused with the advice for this node, and a cluster member is never sent to
// move its ring directory aside or to be reseeded; red when a member is given the single node's
// advice, or either text names a build that was never released]. No other build was released
// (owner ruling 2026-11-21), so neither refusal has a build to go back to. A single node whose
// directory is of another format starts on an empty one; one whose marker is of another format is
// told the directory is the only copy, to keep it, and how to get its data back from a dump.
// Both exit 70, "do not restart" (review R-5).
func TestAnUnreadFormatGetsTheAdviceForWhatTheNodeIs(t *testing.T) {
	rows := []struct {
		name   string
		build  func(t *testing.T) string
		says   string
		single func(ringDir string) string
	}{
		{"a ring directory of another format", func(t *testing.T) string {
			dir := ranRingDir(t)
			mustWrite(t, filepath.Join(dir, ringFormatFile), "hoarddb ring format 6\n")
			return dir
		}, "is not in the format this release reads, which is format 9 (FORMAT says: hoarddb ring format 6).", singleNodeEmptyRingAdviceFor},
		{"a sync marker of another format", func(t *testing.T) string {
			dir := ranRingDir(t)
			writeFormat1Marker(t, filepath.Join(dir, "badger"))
			return dir
		}, "has format 1, and this release reads format 2 only. Nothing was changed.", singleNodeUnreadMarkerAdviceFor},
	}
	for _, row := range rows {
		for _, member := range []bool{false, true} {
			t.Run(row.name+", "+nodeKind(member), func(t *testing.T) {
				dir := row.build(t)
				node, err := OpenNode(context.Background(), nodeConfigFor(t, dir, member))
				if err == nil {
					_ = node.Close() // the open was to be refused; the failure below is the result
					t.Fatal("the node opened a directory this release does not read")
				}
				text := err.Error()
				requireNoOtherBuild(t, text)
				if !strings.Contains(text, row.says) {
					t.Errorf("the refusal does not say %q: %s", row.says, text)
				}
				for _, banned := range []string{"fresh data directory", "storage.data_dir", "HOARDDB_DATA_DIR"} {
					if strings.Contains(text, banned) {
						t.Errorf("an unread-format refusal says %q: %s", banned, text)
					}
				}
				requireRingAdvice(t, text, dir, member, row.single(dir))
			})
		}
	}
}

// TestAReplayRefusalGetsTheAdviceForWhatTheNodeIs: [GUARD the boot replay's refusal of a record
// (a version this release does not read, or one that cannot be trusted) reaches the operator with the advice
// for a member or a single node; red when the node adds none or the replayer's text sends the node
// to a fresh data directory]. The replayer holds no configuration, so Node.open adds the advice
// (ringRefusalWithAdvice), as for the store's refusals.
func TestAReplayRefusalGetsTheAdviceForWhatTheNodeIs(t *testing.T) {
	refusal := &cluster.ReplayRecordError{Seq: 1, Text: "retained-log record at sequence 1: a refusal of the replay",
		Err: errors.New("the cause")}
	for _, member := range []bool{false, true} {
		t.Run(nodeKind(member), func(t *testing.T) {
			dir := t.TempDir()
			err := ringRefusalWithAdvice(fmt.Errorf("replaying the log: %w", refusal), dir, member)
			var kept *cluster.ReplayRecordError
			if !errors.As(err, &kept) {
				t.Fatalf("the advice hides the replay's refusal: %v", err)
			}
			requireRingAdvice(t, err.Error(), dir, member, singleNodeUnreadLogAdviceFor(dir))
			if !member && strings.Contains(err.Error(), "keep it if you want it") {
				t.Errorf("a single node is told its only copy is optional to keep: %v", err)
			}
		})
	}
	if err := ringRefusalWithAdvice(errors.New("the disk is full"), t.TempDir(), false); strings.Contains(err.Error(), "member of a cluster") {
		t.Errorf("an error that is not a refusal of the ring directory got the advice: %v", err)
	}
}

// TestACorruptKeyGetsTheAdviceForWhatTheNodeIs: [GUARD CORRUPT_KEY carries the advice for a member
// or a single node, chosen by NodeConfig.Cluster, and the node hands it to every reader of its
// store; red when the node passes no advice or the wrong kind's]. The shared text names the damage
// only (cluster: TestTheCorruptKeyTextGivesNoDirectoryAdvice).
func TestACorruptKeyGetsTheAdviceForWhatTheNodeIs(t *testing.T) {
	for _, member := range []bool{false, true} {
		t.Run(nodeKind(member), func(t *testing.T) {
			dir := t.TempDir()
			node := &Node{cfg: nodeConfigFor(t, dir, member)}
			err := corruptKey(7, []byte{0xff}, errors.New("unknown tag"), node.ringAdvice())
			var coded *cluster.Error
			if !errors.As(err, &coded) || coded.Code != cluster.CodeCorruptKey {
				t.Fatalf("corruptKey returned %v, want CORRUPT_KEY", err)
			}
			requireRingAdvice(t, err.Error(), dir, member, singleNodeRebuildAdviceFor(dir))
		})
	}
	t.Run("the node wires it into its reader and scanner", func(t *testing.T) {
		dir := t.TempDir()
		node := u30iOpen(t, dir)
		want := singleNodeRebuildAdviceFor(dir)
		if node.reader.cfg.CorruptKeyAdvice != want || node.scanner.cfg.CorruptKeyAdvice != want {
			t.Errorf("the reader's advice is %q and the scanner's %q, want %q", node.reader.cfg.CorruptKeyAdvice,
				node.scanner.cfg.CorruptKeyAdvice, want)
		}
	})
}

// TestADamagedDedupeEntryReportGetsTheAdviceForWhatTheNodeIs: [GUARD the dedupe sweeper's report of
// an entry that does not decode ends in the advice for this node's kind, never a reseed; red when
// the node gives the sweeper no advice]. The report goes to the node's error report, read by the
// operator.
func TestADamagedDedupeEntryReportGetsTheAdviceForWhatTheNodeIs(t *testing.T) {
	dir := t.TempDir()
	node := u30iOpen(t, dir)
	if got, want := node.dedupe.advice, singleNodeRebuildAdviceFor(dir); got != want {
		t.Errorf("the node's dedupe sweeper carries advice %q, want %q", got, want)
	}
	err := damagedDedupeEntry([]byte{1}, 3, 9, errors.New("format 0x02"), memberAdviceFor(dir))
	requireRingAdvice(t, err.Error(), dir, true, singleNodeRebuildAdviceFor(dir))
}

// flipATableIndexByte flips one byte in the middle of the index of the store's first table, leaving
// the footer's lengths whole, so the guards pass it and Badger's open fails the index checksum.
func flipATableIndexByte(t *testing.T, store string) {
	t.Helper()
	tables, err := filepath.Glob(filepath.Join(store, "*.sst"))
	if err != nil || len(tables) == 0 {
		t.Fatalf("SCENARIO NOT REACHED: the close left no table (%v)", err)
	}
	raw, err := os.ReadFile(tables[0])
	if err != nil {
		t.Fatal(err)
	}
	checksumLength := int(binary.BigEndian.Uint32(raw[len(raw)-4:]))
	indexLengthAt := len(raw) - 4 - checksumLength - 4
	indexLength := int(binary.BigEndian.Uint32(raw[indexLengthAt:]))
	raw[indexLengthAt-indexLength/2] ^= 0xff
	if err := os.WriteFile(tables[0], raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// memberClauseFormat is the member clause's format, byte for byte; since the encryption merge it is
// the one copy (ClusterMemberRingAdvice), which the server's encryption refusals give as well;
// %s is <data_dir>/ring, and each caller adds its own full stop.
const memberClauseFormat = "This node is configured as a member of a cluster (cluster.enabled), so do not move %[1]s aside while the cluster runs: that can lose committed cluster state. Stop this node, keep %[1]s, start it again on an empty ring directory with the same node id, keys and address, and run 'replace-node <node> --with <node>@<addr>' on any member"

// TestTheMemberClauseIsTheEncryptionBranchsString: [GUARD the member clause is the string the ring
// store's refusals and the encryption refusals both give; red on any drift of a byte].
func TestTheMemberClauseIsTheEncryptionBranchsString(t *testing.T) {
	dir := "/data/ring"
	if got, want := ClusterMemberRingAdvice(dir), fmt.Sprintf(memberClauseFormat, dir); got != want {
		t.Fatalf("the member clause is\n %q\nwant\n %q", got, want)
	}
	if got := memberAdviceFor(dir); got != fmt.Sprintf(memberClauseFormat, dir) {
		t.Fatalf("the tests' member advice drifted from the shared clause: %q", got)
	}
}

// TestTheStoresRuntimeRefusalsCarryTheNodesAdvice: [GUARD the node sets its advice on its store, so
// a damaged read and the write refusal of a store stalled behind a damaged table end in it; red when
// the node sets none] (review R2 F-4).
func TestTheStoresRuntimeRefusalsCarryTheNodesAdvice(t *testing.T) {
	dir := t.TempDir()
	u30iOpen(t, dir)
	store := filepath.Join(dir, "badger")
	for name, err := range map[string]error{
		"a damaged read":  &storage.DamagedReadError{Dir: store, Cause: errors.New("table block 3 failed its checksum")},
		"a stalled write": &storage.StoreRefusesWritesError{Dir: store, Cause: badger.ErrL0StalledByDamagedTable},
	} {
		t.Run(name, func(t *testing.T) {
			requireRingAdvice(t, err.Error(), dir, false, singleNodeRebuildAdviceFor(dir))
		})
	}
}
