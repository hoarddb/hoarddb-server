package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
)

// kvEntry is one key and value written to a test store.
type kvEntry struct {
	key, value []byte
}

func documentEntry(slot uint16, database, bucket uint64, body, value string) kvEntry {
	return kvEntry{storage.Key(slot, database, bucket, storage.NSDocuments, []byte(body)), []byte(value)}
}

// pairRecordEntry is the ready seed record of one pair; cursor is node-local.
func pairRecordEntry(t *testing.T, slot uint16, database, epoch, seq, cursor uint64) kvEntry {
	t.Helper()
	var record wire.SeedRecord
	record.State = machine.SeedReady
	record.Applied = cluster.Position{Epoch: epoch, Seq: seq}
	record.WALCursor = cursor
	encoded, err := wire.EncodeSeedRecord(record)
	if err != nil {
		t.Fatalf("encoding a seed record: %v", err)
	}
	return kvEntry{wire.SeedRecordKey(slot, database), encoded}
}

// baseEntries is the replicated content two healthy copies hold: two pairs, a document, an
// index entry and a store-field element in the first.
func baseEntries(t *testing.T) []kvEntry {
	return []kvEntry{
		documentEntry(5, 7, 1, "d1", "alpha"),
		documentEntry(5, 7, 1, "d2", "beta"),
		documentEntry(5, 7, 1, "d3", "gamma"),
		{storage.Key(5, 7, 1, storage.NSIndex, []byte("Name\x00alpha")), []byte("d1")},
		{storage.Key(5, 7, 1, storage.NSFieldStore, []byte("d1"), []byte{0, 1, 0x04, 9}), []byte("order-9")},
		documentEntry(9, 7, 2, "e1", "delta"),
		pairRecordEntry(t, 5, 7, 1, 40, 100),
		pairRecordEntry(t, 9, 7, 1, 12, 101),
	}
}

func openTestStore(t *testing.T, dir string, encryptionKey []byte) *storage.BadgerKV {
	t.Helper()
	store, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 32}, encryptionKey)
	if err != nil {
		t.Fatalf("opening the test store %s: %v", dir, err)
	}
	return store
}

// writeCycle opens dir, sets and deletes the given keys in one batch, and closes the store
// cleanly, so each cycle leaves its own table behind.
func writeCycle(t *testing.T, dir string, sets []kvEntry, deletes [][]byte) {
	t.Helper()
	store := openTestStore(t, dir, nil)
	batch := store.NewBatch()
	for _, entry := range sets {
		if err := batch.Set(entry.key, entry.value); err != nil {
			t.Fatalf("staging a key: %v", err)
		}
	}
	for _, key := range deletes {
		if err := batch.Delete(key); err != nil {
			t.Fatalf("staging a delete: %v", err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing a batch: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("closing the test store: %v", err)
	}
}

// buildStore writes entries into a new store directory in one cycle.
func buildStore(t *testing.T, entries []kvEntry) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	writeCycle(t, dir, entries, nil)
	return dir
}

func digestOf(t *testing.T, dir, node string) digestFile {
	t.Helper()
	digest, err := digestStore(dir, node)
	if err != nil {
		t.Fatalf("digesting %s: %v", dir, err)
	}
	return digest
}

func pairOf(t *testing.T, digest digestFile, slot uint16, database uint64) pairDigest {
	t.Helper()
	for _, pair := range digest.Pairs {
		if pair.Slot == slot && pair.Database == database {
			return pair
		}
	}
	t.Fatalf("digest of %s has no pair (%d, %d)", digest.Node, slot, database)
	return pairDigest{}
}

// directoryChecksum is a name, size, modification time and content hash of every file under dir.
func directoryChecksum(t *testing.T, dir string) map[string]string {
	t.Helper()
	sums := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			sums[relative] = "dir " + info.ModTime().String()
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(content)
		sums[relative] = info.ModTime().String() + " " + hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatalf("checksumming %s: %v", dir, err)
	}
	return sums
}

func tableFileCount(t *testing.T, dir string) int {
	t.Helper()
	tables, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("listing tables in %s: %v", dir, err)
	}
	return len(tables)
}

func TestDigestIgnoresTableLayoutAndHistory(t *testing.T) {
	cleanDir := buildStore(t, baseEntries(t))

	// The same final state reached the long way: a wrong value, a key that is deleted again,
	// the right value later, with a clean close (and so a table) after each step.
	churnedDir := filepath.Join(t.TempDir(), "store")
	gone := documentEntry(5, 7, 1, "gone", "temporary")
	wrongValue := documentEntry(5, 7, 1, "d1", "wrong")
	writeCycle(t, churnedDir, []kvEntry{wrongValue, gone, pairRecordEntry(t, 5, 7, 1, 3, 1)}, nil)
	writeCycle(t, churnedDir, baseEntries(t), nil)
	writeCycle(t, churnedDir, nil, [][]byte{gone.key})
	if tables := tableFileCount(t, churnedDir); tables < 2 {
		t.Fatalf("the churned store has %d table files, want at least 2 so its layout differs from the clean one", tables)
	}

	clean := digestOf(t, cleanDir, "a")
	churned := digestOf(t, churnedDir, "b")
	for _, slotDatabase := range [][2]uint64{{5, 7}, {9, 7}} {
		cleanPair := pairOf(t, clean, uint16(slotDatabase[0]), slotDatabase[1])
		churnedPair := pairOf(t, churned, uint16(slotDatabase[0]), slotDatabase[1])
		if cleanPair != churnedPair {
			t.Errorf("pair (%d, %d) differs between a clean and a churned layout: %+v vs %+v", slotDatabase[0], slotDatabase[1], cleanPair, churnedPair)
		}
	}
	if live := pairOf(t, clean, 5, 7).LiveKeys; live != 5 {
		t.Errorf("pair (5, 7) counts %d live keys, want 5 (three documents, an index entry, a field element)", live)
	}
	for _, verdict := range compareDigests([]digestFile{clean, churned}) {
		if verdict.Kind != verdictEqual {
			t.Errorf("pair (%d, %d): %s, want equal", verdict.Slot, verdict.Database, verdict.Detail)
		}
	}
}

func TestDigestDiffersAndCompareNamesTheKey(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(entries []kvEntry) []kvEntry
		wantKind string
		wantKey  string
	}{
		{
			name:     "a write is missing",
			mutate:   func(entries []kvEntry) []kvEntry { return append(entries[:2:2], entries[3:]...) },
			wantKind: "missing on",
			wantKey:  `"d3"`,
		},
		{
			name: "an extra key",
			mutate: func(entries []kvEntry) []kvEntry {
				return append(entries[:len(entries):len(entries)], documentEntry(5, 7, 1, "d4", "epsilon"))
			},
			wantKind: "missing on",
			wantKey:  `"d4"`,
		},
		{
			name: "a different value",
			mutate: func(entries []kvEntry) []kvEntry {
				changed := append([]kvEntry(nil), entries...)
				changed[1] = documentEntry(5, 7, 1, "d2", "BETA")
				return changed
			},
			wantKind: "value differs",
			wantKey:  `"d2"`,
		},
	}
	reference := baseEntries(t)
	referenceDir := buildStore(t, reference)
	referenceDigest := digestOf(t, referenceDir, "a")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			variantDir := buildStore(t, tc.mutate(baseEntries(t)))
			variantDigest := digestOf(t, variantDir, "b")
			if pairOf(t, variantDigest, 5, 7).Digest == pairOf(t, referenceDigest, 5, 7).Digest {
				t.Fatalf("the digest of pair (5, 7) is the same with %s", tc.name)
			}
			if pairOf(t, variantDigest, 9, 7) != pairOf(t, referenceDigest, 9, 7) {
				t.Errorf("pair (9, 7) was not touched and must not differ")
			}
			verdicts := compareDigests([]digestFile{referenceDigest, variantDigest})
			if got := verdictOf(t, verdicts, 5, 7).Kind; got != verdictDifferent {
				t.Fatalf("pair (5, 7) verdict is %v, want different", got)
			}
			if got := verdictOf(t, verdicts, 9, 7).Kind; got != verdictEqual {
				t.Errorf("pair (9, 7) verdict is %v, want equal", got)
			}
			differences, err := firstDifferences(referenceDir, variantDir, 5, 7, 10)
			if err != nil {
				t.Fatalf("second pass: %v", err)
			}
			if len(differences) != 1 {
				t.Fatalf("second pass found %d differences, want 1: %v", len(differences), differences)
			}
			text := differences[0].String()
			if !strings.Contains(text, tc.wantKind) || !strings.Contains(text, tc.wantKey) {
				t.Errorf("difference reads %q, want it to say %q and name %s", text, tc.wantKind, tc.wantKey)
			}
		})
	}
}

func verdictOf(t *testing.T, verdicts []pairVerdict, slot uint16, database uint64) pairVerdict {
	t.Helper()
	for _, verdict := range verdicts {
		if verdict.Slot == slot && verdict.Database == database {
			return verdict
		}
	}
	t.Fatalf("no verdict for pair (%d, %d)", slot, database)
	return pairVerdict{}
}

func TestDigestIgnoresNodeLocalKeys(t *testing.T) {
	referenceDir := buildStore(t, baseEntries(t))

	localDifferences := baseEntries(t)
	// A different WAL cursor in a pair record, a dedupe entry, a bucket state document, and
	// three node-block records: every one is this node's own and a copy need not match it.
	localDifferences[6] = pairRecordEntry(t, 5, 7, 1, 40, 99999)
	localDifferences = append(localDifferences,
		kvEntry{storage.Key(5, 7, 1, storage.NSDedupe, bytes.Repeat([]byte{7}, 17)), []byte{3, 1, 1, 1}},
		kvEntry{storage.Key(5, 7, 1, storage.NSState), []byte("applied_seq 77")},
		kvEntry{storage.NodeKey(storage.NSCatalog, []byte("node-local")), []byte("x")},
		kvEntry{storage.NodeKey(storage.NSReplication, []byte{'p'}, []byte("peer")), []byte("pos")},
		kvEntry{storage.NodeKey(storage.NSExpiry, []byte("t")), []byte("e")},
	)
	localDir := buildStore(t, localDifferences)

	reference := digestOf(t, referenceDir, "a")
	local := digestOf(t, localDir, "b")
	for _, verdict := range compareDigests([]digestFile{reference, local}) {
		if verdict.Kind != verdictEqual {
			t.Errorf("pair (%d, %d): %s, want equal when only node-local keys differ", verdict.Slot, verdict.Database, verdict.Detail)
		}
	}
	if skipped := local.SkippedKeys["dedupe"] + local.SkippedKeys["state"]; skipped != 2 {
		t.Errorf("digest counts %d dedupe and state keys as skipped, want 2: %v", skipped, local.SkippedKeys)
	}
}

func TestToolDoesNotModifyTheDirectory(t *testing.T) {
	firstDir := buildStore(t, baseEntries(t))
	secondDir := buildStore(t, append(baseEntries(t), documentEntry(5, 7, 1, "d4", "epsilon")))
	firstBefore, secondBefore := directoryChecksum(t, firstDir), directoryChecksum(t, secondDir)

	digestPath := filepath.Join(t.TempDir(), "a.json")
	otherPath := filepath.Join(t.TempDir(), "b.json")
	var output bytes.Buffer
	if err := runDigest([]string{"--store", firstDir, "--node", "a", "--out", digestPath}, &output); err != nil {
		t.Fatalf("faultctl digest: %v", err)
	}
	if err := runDigest([]string{"--store", secondDir, "--node", "b", "--out", otherPath}, &output); err != nil {
		t.Fatalf("faultctl digest: %v", err)
	}
	err := runCompare([]string{"--keys", "5", "--dir", "a=" + firstDir, "--dir", "b=" + secondDir, digestPath, otherPath}, &output)
	if err == nil {
		t.Fatalf("faultctl compare reported success for two different copies:\n%s", output.String())
	}

	for name, dirs := range map[string][2]map[string]string{
		"first":  {firstBefore, directoryChecksum(t, firstDir)},
		"second": {secondBefore, directoryChecksum(t, secondDir)},
	} {
		before, after := dirs[0], dirs[1]
		if len(before) != len(after) {
			t.Errorf("%s directory holds %d entries after the tool ran, %d before", name, len(after), len(before))
		}
		for path, sum := range before {
			if after[path] != sum {
				t.Errorf("%s directory: %s changed (%q before, %q after)", name, path, sum, after[path])
			}
		}
	}
}

func TestComparePairsAtDifferentPositionsAreNotComparable(t *testing.T) {
	behind := baseEntries(t)
	behind[6] = pairRecordEntry(t, 5, 7, 1, 39, 100)
	behind = append(behind[:2:2], behind[3:]...) // and it lacks d3: still not a verdict on content
	behindDigest := digestOf(t, buildStore(t, behind), "behind")
	aheadDigest := digestOf(t, buildStore(t, baseEntries(t)), "ahead")

	verdict := verdictOf(t, compareDigests([]digestFile{aheadDigest, behindDigest}), 5, 7)
	if verdict.Kind != verdictNotComparable || !strings.Contains(verdict.Detail, "positions differ") {
		t.Fatalf("verdict is %v %q, want not comparable: positions differ", verdict.Kind, verdict.Detail)
	}

	var output bytes.Buffer
	files := map[string]digestFile{"ahead.json": aheadDigest, "behind.json": behindDigest}
	paths := map[string]string{}
	for name, digest := range files {
		paths[name] = filepath.Join(t.TempDir(), name)
		if err := writeDigestFile(paths[name], digest); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	if err := runCompare([]string{paths["ahead.json"], paths["behind.json"]}, &output); err == nil {
		t.Errorf("compare exited 0 with a pair that is not comparable:\n%s", output.String())
	}
	if !strings.Contains(output.String(), "not comparable: positions differ") {
		t.Errorf("output lacks %q:\n%s", "not comparable: positions differ", output.String())
	}
	if strings.Contains(output.String(), ": different") {
		t.Errorf("a pair at different positions was called different:\n%s", output.String())
	}
}

func TestCompareSucceedsOnlyWhenEverySharedPairIsEqual(t *testing.T) {
	first := buildStore(t, baseEntries(t))
	second := buildStore(t, baseEntries(t))
	paths := []string{filepath.Join(t.TempDir(), "a.json"), filepath.Join(t.TempDir(), "b.json")}
	var output bytes.Buffer
	for index, dir := range []string{first, second} {
		if err := runDigest([]string{"--store", dir, "--node", string(rune('a' + index)), "--out", paths[index]}, &output); err != nil {
			t.Fatalf("faultctl digest: %v", err)
		}
	}
	output.Reset()
	if err := runCompare(paths, &output); err != nil {
		t.Fatalf("faultctl compare of two equal copies: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "2 pairs equal") {
		t.Errorf("output lacks the equal count:\n%s", output.String())
	}
}

func TestDigestRefusesARunningNode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	running := openTestStore(t, dir, nil)
	defer func() { _ = running.Close() }() // test cleanup; the assertion is the refusal below
	_, err := digestStore(dir, "a")
	if err == nil {
		t.Fatal("digest of a store a node holds open succeeded")
	}
	for _, want := range []string{"running", "stop"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not say %q", err, want)
		}
	}
}

func TestDigestRefusesAnEncryptedStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	sealed := openTestStore(t, dir, bytes.Repeat([]byte{1}, 32))
	if err := sealed.Close(); err != nil {
		t.Fatalf("closing the sealed store: %v", err)
	}
	_, err := digestStore(dir, "a")
	if err == nil || !strings.Contains(err.Error(), "encrypted at rest") {
		t.Fatalf("digest of an encrypted store: %v, want a refusal that says it is encrypted at rest", err)
	}
}

func TestDigestRefusesAMissingStore(t *testing.T) {
	_, err := digestStore(filepath.Join(t.TempDir(), "absent"), "a")
	if err == nil || !strings.Contains(err.Error(), "--store") {
		t.Fatalf("digest of a directory that is not a store: %v, want a refusal that names --store", err)
	}
}

func TestPairWithPendingDeleteSweepsIsNotComparable(t *testing.T) {
	// A delete replicates as a tombstone and the sweep of the document's other keys is node-local:
	// until both nodes have swept, healthy copies differ in keys. The node that has not swept holds
	// a delete reclaim record for the pair and the digest must say so instead of calling it different.
	sweptDir := buildStore(t, baseEntries(t))
	unsweptEntries := append(baseEntries(t), documentEntry(5, 7, 1, "deleted", "stale"))
	reclaimKey := append(storage.NodeKey(storage.NSReclaim, []byte{'d'}, storage.Scope(5, 7, 1), []byte("id")), make([]byte, 16)...)
	unsweptEntries = append(unsweptEntries, kvEntry{reclaimKey, nil})
	unsweptDir := buildStore(t, unsweptEntries)

	swept := digestOf(t, sweptDir, "swept")
	unswept := digestOf(t, unsweptDir, "unswept")
	if got := pairOf(t, unswept, 5, 7).ReclaimPending; got != 1 {
		t.Fatalf("unswept node counts %d pending delete sweeps for pair (5, 7), want 1", got)
	}
	verdict := verdictOf(t, compareDigests([]digestFile{swept, unswept}), 5, 7)
	if verdict.Kind != verdictNotComparable || !strings.Contains(verdict.Detail, "unswept still has 1 deleted documents to sweep") {
		t.Fatalf("verdict is %v %q, want not comparable naming the unswept node", verdict.Kind, verdict.Detail)
	}
	if got := verdictOf(t, compareDigests([]digestFile{swept, unswept}), 9, 7).Kind; got != verdictEqual {
		t.Errorf("pair (9, 7) has no delete pending and must stay equal, got %v", got)
	}
}

func TestCompareCountsPairsHeldByOneNode(t *testing.T) {
	both := baseEntries(t)
	onlyFirst := append(baseEntries(t), documentEntry(11, 7, 3, "x", "y"), pairRecordEntry(t, 11, 7, 1, 5, 1))
	paths := []string{filepath.Join(t.TempDir(), "a.json"), filepath.Join(t.TempDir(), "b.json")}
	var output bytes.Buffer
	for index, entries := range [][]kvEntry{onlyFirst, both} {
		if err := runDigest([]string{"--store", buildStore(t, entries), "--node", string(rune('a' + index)), "--out", paths[index]}, &output); err != nil {
			t.Fatalf("faultctl digest: %v", err)
		}
	}
	output.Reset()
	if err := runCompare(paths, &output); err != nil {
		t.Fatalf("compare: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "1 pairs are held by only one of these nodes") {
		t.Errorf("output does not count the pair held by one node:\n%s", output.String())
	}
}

func TestCompareThreeNodesNamesTheOddOneOut(t *testing.T) {
	changed := baseEntries(t)
	changed[0] = documentEntry(5, 7, 1, "d1", "CHANGED")
	var files []digestFile
	for index, entries := range [][]kvEntry{baseEntries(t), baseEntries(t), changed} {
		files = append(files, digestOf(t, buildStore(t, entries), string(rune('a'+index))))
	}
	verdicts := compareDigests(files)
	if got := verdictOf(t, verdicts, 5, 7); got.Kind != verdictDifferent || len(got.Nodes) != 3 {
		t.Errorf("pair (5, 7): %v over %v, want different across 3 nodes", got.Kind, got.Nodes)
	}
	if got := verdictOf(t, verdicts, 9, 7).Kind; got != verdictEqual {
		t.Errorf("pair (9, 7): %v, want equal", got)
	}
}

func TestSeedingAndRetiredPairsAreJudgedByState(t *testing.T) {
	ready := digestFile{Format: 1, Node: "a", Pairs: []pairDigest{{Slot: 1, Database: 1, State: "ready", AppliedSeq: 5, Digest: "x"}, {Slot: 2, Database: 1, State: "ready", AppliedSeq: 5, Digest: "x"}}}
	other := digestFile{Format: 1, Node: "b", Pairs: []pairDigest{{Slot: 1, Database: 1, State: "seeding", AppliedSeq: 5, Digest: "y"}, {Slot: 2, Database: 1, State: "retired", AppliedSeq: 5, Digest: "y"}}}
	verdicts := compareDigests([]digestFile{ready, other})
	if len(verdicts) != 1 || verdicts[0].Kind != verdictNotComparable || !strings.Contains(verdicts[0].Detail, "seeding") {
		t.Fatalf("verdicts %+v, want one: slot 1 not comparable because b is seeding, and the retired pair left out", verdicts)
	}
}
