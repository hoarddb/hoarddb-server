package storage

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

var (
	vouchedPrefixSeed = flag.Uint64("vouched-prefix-seed", 0,
		"T10 (TestStoreReopensAsAPrefix): the first round's seed, 0 for one from the clock; round r runs seed+r, so "+
			"a failing round reruns alone with its own seed and -vouched-prefix-rounds=1")
	vouchedPrefixRounds = flag.Int("vouched-prefix-rounds", 200, "T10: the number of rounds")
	// The sealed T10 runs fewer rounds by default: each round costs what a plaintext round does
	// plus the cipher, and the full suite already runs the 200 plaintext rounds. A full sealed run
	// passes -vouched-prefix-encrypted-rounds=200 (the build report records one).
	vouchedPrefixEncryptedRounds = flag.Int("vouched-prefix-encrypted-rounds", 40,
		"T10 with a key (TestAnEncryptedStoreReopensAsAPrefixOrIsRefusedAtItsFirstFlush): the number of rounds; "+
			"the seed is -vouched-prefix-seed's")
)

// prefixTuning gives T10 the smallest memtables Badger opens with its 1 MiB value threshold
// (7 MiB; the threshold must stay under the batch bound, 15 % of the memtable): a round's fill
// commits rotate them a few times and the flusher turns the rotated ones into tables.
var prefixTuning = BadgerTuning{MemTableMB: 7}

const (
	prefixPage = 4096
	// A file larger than this is read in chunks up to its first chunk of zeros: the value log
	// is a sparse 256 MiB mapping and reading all of it for every capture would dominate the
	// test. The values are never zero bytes, so a chunk of zeros is the end of what was written.
	prefixWholeFileLimit = 4 << 20
	prefixChunk          = 1 << 20
	// A large value goes to the value log (Badger's threshold is 1 MiB).
	prefixLargeValue = 1<<20 + 4096
)

// TestStoreReopensAsAPrefix is T10 (I-1): random commits, syncs, rotations and flushes on small
// memtables; an image of a killed process built from copies taken at quiet instants, each
// memtable log at its bytes at the last completed sync or later and cut at a page boundary, each
// value-log file at its bytes at its last msync (a store sync's, a flush's or a rotation's) or
// later, cut likewise, the tables and the MANIFEST as of the kill instant, and the marker's real
// bytes as of any completed sync. The reopened store must equal the model of the commits through
// C = max(V, T): every commit at or below C, nothing above it. Then more commits, a second image
// and a second reopen: a commit the first cut discarded never comes back.
//
// The seed fixes the operations, not the flusher's timing, so a rerun of a failing round
// repeats its operations and may place its rotations and flushes elsewhere.
func TestStoreReopensAsAPrefix(t *testing.T) {
	reach := runPrefixRounds(t, *vouchedPrefixRounds, false)
	if *vouchedPrefixRounds >= 200 {
		// Each mutation of the card needs its case at least once in a full run, or a green run
		// says nothing about it. Only the cases a full run reaches every time are gated.
		isTrue(t, reach.discarded > 0, "SCENARIO NOT REACHED: no round's cut discarded a commit")
		// Not gated: a cut that reaches two logs or more is hit 0 to 5 times in 200 rounds (C1 review R1:
		// 0 on one VM, 71/5/19 on another), so a gate on it failed about one full run in several. The
		// case is built on purpose, with a held flusher, by TestACutThatReachesThreeLogsKeepsOnlyTheVouchedCommit;
		// the count stays in the log line above.
		isTrue(t, reach.truncatedLogKept > 0, "SCENARIO NOT REACHED: no second image kept a log the first cut truncated")
	}
}

// TestAnEncryptedStoreReopensAsAPrefixOrIsRefusedAtItsFirstFlush: [GUARD T10 on a store sealed
// under the test key: every image either reopens as the prefix through C = max(V, T), exactly as
// in plaintext, or, when it is the image of a crash during the store's very first flush, is
// refused with the first-flush refusal and changes nothing, never repaired; every round's first
// store is sealed; red when the key is not passed through the vouched open: the first store's key
// registry then opens with an empty key, and a first-flush image is set aside instead of refused].
// Ring encryption at rest spec §7 and §9.1 step 5. The rounds are -vouched-prefix-encrypted-rounds
// (40 by default; the build report records a 200-round run).
func TestAnEncryptedStoreReopensAsAPrefixOrIsRefusedAtItsFirstFlush(t *testing.T) {
	withVouchedTestKey(t)
	reach := runPrefixRounds(t, *vouchedPrefixEncryptedRounds, true)
	t.Logf("T10 with a key: %d rounds refused at a first flush", reach.refusedAtFirstFlush)
}

// runPrefixRounds runs T10's rounds; encrypted says the stores are sealed (withVouchedTestKey).
func runPrefixRounds(t *testing.T, rounds int, encrypted bool) prefixReach {
	base := *vouchedPrefixSeed
	if base == 0 {
		base = uint64(time.Now().UnixNano())
	}
	t.Logf("T10: seed %d, %d rounds, sealed %v (rerun with -vouched-prefix-seed=%d and %d rounds)", base, rounds, encrypted, base, rounds)
	var reach prefixReach
	hold := &flushHold{}
	defer badger.SetFlushTestHook(hold.hook)()
	durable := &valueLogDurability{covered: map[string]int64{}}
	defer badger.SetSyncTestHooks(badger.SyncTestHooks{AfterValueLogFileSync: durable.synced})()
	for round := range rounds {
		seed := base + uint64(round)
		ok := t.Run(fmt.Sprintf("round %d seed %d", round, seed), func(t *testing.T) {
			defer hold.letGo() // a failed round must not leave the next round's store held
			runPrefixRound(t, seed, &reach, hold, durable, encrypted)
		})
		if !ok {
			t.Fatalf("round %d failed; rerun it alone with -run '%s' -vouched-prefix-seed=%d and one round", round, t.Name(), seed)
		}
	}
	t.Logf("T10 reach: %d rounds whose first cut discarded commits, %d whose cut shortened or removed two logs or more, %d whose "+
		"second image kept a log the first cut had truncated, %d with a value in the value log, %d with a table "+
		"flushed after the image's last sync, %d of them with a value in the value log (BUG-0827 slice 2's case); %d "+
		"flusher holds timed out", reach.discarded, reach.laterLog, reach.truncatedLogKept, reach.largeValue,
		reach.flushAfterSync, reach.flushAfterSyncLarge, hold.timeouts.Load())
	return reach
}

// prefixReach counts the rounds that reached each case the mutations need.
type prefixReach struct {
	discarded, laterLog, truncatedLogKept, largeValue, flushAfterSync, flushAfterSyncLarge int
	// refusedAtFirstFlush counts the sealed rounds an image of whose was refused at its first flush.
	refusedAtFirstFlush int
}

// valueLogDurability records, for every value-log file by path, the longest prefix an msync made
// durable (badger.SyncTestHooks.AfterValueLogFileSync, called by the store sync's msync, a flush's
// and a rotation's), for the whole run: the paths of each round's stores differ.
type valueLogDurability struct {
	mu      sync.Mutex
	covered map[string]int64
}

func (d *valueLogDurability) synced(path string, covered int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.covered[path] = max(d.covered[path], covered)
}

// of returns the durable prefix of each value-log file of the store at dir, by file name.
func (d *valueLogDurability) of(dir string) map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	durable := map[string]int{}
	for path, covered := range d.covered {
		if filepath.Dir(path) == dir {
			durable[filepath.Base(path)] = int(covered)
		}
	}
	return durable
}

// prefixValue is a value by its recipe: never zero bytes, regenerated for the comparison.
type prefixValue struct {
	seed uint64
	size int
}

func (v prefixValue) bytes() []byte {
	out := make([]byte, v.size)
	state := v.seed | 1
	for i := 0; i < len(out); i += 8 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		for j := 0; j < 8 && i+j < len(out); j++ {
			out[i+j] = byte(state>>(8*j)) | 1
		}
	}
	return out
}

// prefixCommit is one commit of the model: its sets and deletes, in commit order. Every commit
// sets its own key c/<seq>, which also gives its version.
type prefixCommit struct {
	seq     int
	version uint64
	sets    map[string]prefixValue
	deletes []string
}

func prefixCommitKey(seq int) string { return fmt.Sprintf("c/%06d", seq) }

// prefixModelAt is the store the commits at or below version leave.
func prefixModelAt(commits []prefixCommit, version uint64) map[string]prefixValue {
	state := map[string]prefixValue{}
	for _, c := range commits {
		if c.version > version {
			continue
		}
		for _, key := range c.deletes {
			delete(state, key)
		}
		for key, value := range c.sets {
			state[key] = value
		}
	}
	return state
}

// prefixFile is one file of a capture: its bytes up to the end of what was written, and its size.
type prefixFile struct {
	content []byte
	size    int64
}

// prefixCapture is what an image needs of a store directory at a quiet instant: the length of
// what each file holds (the logs only grow, so a later copy's bytes up to that length are this
// instant's), the marker's bytes, whether a sync completed right before it, and the prefix of
// each value-log file the msyncs before it made durable.
type prefixCapture struct {
	lengths         map[string]int
	marker          []byte
	synced          bool
	valueLogDurable map[string]int
}

// prefixLife drives one process's life on a store: the model's commits and the captures. The
// kill instant x is one capture, drawn as the captures are taken (one in k replaces the one
// held, so each is equally likely); only its files are kept whole.
type prefixLife struct {
	t        *testing.T
	rng      *rand.Rand
	kv       *BadgerKV
	dir      string
	commits  *[]prefixCommit
	captures []prefixCapture
	x        int
	xFiles   map[string]prefixFile
	// killAtFirst fixes x at the first capture.
	killAtFirst bool
	large       bool // this round puts some values in the value log
	nextSeq     *int
	// hold, while the life holds it, keeps the store's flusher in front of its next flush, so
	// the memtables rotated meanwhile wait in db.imm and their logs stay on disk, the only copy
	// of their commits. Without it a rotated memtable is flushed within milliseconds, its log
	// goes with the flush (BUG-0858's fix), and an image almost never holds a log after the one
	// the cut shortens. (A reader's pin kept a flushed log on disk before that fix; it no longer
	// does.)
	hold    *flushHold
	holding bool
	holder  bool // the life takes and releases the hold at random steps
	// durable is the run's record of the value-log msyncs.
	durable *valueLogDurability
	// pins, while the life is pinning, are iterators that each hold a reference on the memtables
	// that existed when it was made: the state in which T10 found BUG-0858 (a reader holding a
	// flushed memtable). Under the fix the flush removes the log whatever readers hold, so the
	// pins must change nothing; they stay beside the holds (review F4) so that a regression of
	// "the log goes whatever references the memtable has" shows here too. An iterator pins only
	// the memtables of its making, so a new one is taken after every fill commit.
	pinning bool
	pins    []*badger.Iterator
	pinTxns []*badger.Txn
	pinner  bool // the life takes and releases the pin at random steps
	// sparseSyncs makes four syncs in five plain commits instead, so several memtables fill after
	// the last sync and the cut reaches a log after the one it shortens.
	sparseSyncs bool
}

// toggleHold holds the flusher when the life does not, and otherwise lets it go.
func (l *prefixLife) toggleHold() {
	if l.holding {
		l.letGo()
		return
	}
	l.holding = true
	l.hold.holdAt(l.dir)
}

func (l *prefixLife) letGo() {
	l.hold.letGo()
	l.holding = false
}

// togglePin starts pinning when the life is not, and otherwise releases every pin.
func (l *prefixLife) togglePin() {
	if l.pinning {
		l.unpin()
		return
	}
	l.pinning = true
	l.addPin()
}

func (l *prefixLife) addPin() {
	txn := l.kv.db.NewTransaction(false)
	l.pinTxns = append(l.pinTxns, txn)
	l.pins = append(l.pins, txn.NewIterator(badger.DefaultIteratorOptions))
}

func (l *prefixLife) unpin() {
	for i, pin := range l.pins {
		pin.Close()
		l.pinTxns[i].Discard()
	}
	l.pins, l.pinTxns, l.pinning = nil, nil, false
}

// flushHoldTimeout bounds a hold: a hold that keeps the flusher longer lets it go and is counted
// (flushHold.timeouts). Writes wait once three memtables queue behind the held one, and a life
// rotates one or two, so a hold that times out says the round wrote more than expected.
const flushHoldTimeout = 20 * time.Second

// flushHold holds the flusher of the store at one directory in front of its next flush
// (badger.SetFlushTestHook), until letGo. One per test run; the hook ignores every other store.
type flushHold struct {
	mu       sync.Mutex
	dir      string
	release  chan struct{}
	timeouts atomic.Int64
}

func (h *flushHold) hook(db *badger.DB) {
	h.mu.Lock()
	release, dir := h.release, h.dir
	h.mu.Unlock()
	if release == nil || filepath.Clean(db.Opts().Dir) != dir {
		return
	}
	select {
	case <-release:
	case <-time.After(flushHoldTimeout):
		h.timeouts.Add(1)
	}
}

// holdAt holds the flusher of the store at dir from its next flush on; a hold already in place
// stays where it is.
func (h *flushHold) holdAt(dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.release == nil {
		h.dir, h.release = filepath.Clean(dir), make(chan struct{})
	}
}

// letGo releases the flusher; a store must not be closed while it is held.
func (h *flushHold) letGo() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.release != nil {
		close(h.release)
		h.release = nil
	}
}

// refusedAtFirstFlush reports, for a sealed store (encrypted), whether the open of the image at dir
// failed with the first-flush refusal; it fails the test if that refusal changed anything in the
// image. Any other failure, and every failure of a plaintext open, is left to the caller.
func refusedAtFirstFlush(t *testing.T, encrypted bool, dir string, err error) bool {
	t.Helper()
	if !encrypted || err == nil || !isFirstFlushRefusal(err) {
		return false
	}
	setAside, globErr := filepath.Glob(filepath.Join(dir, "*"+firstFlushQuarantineSuffix+"*"))
	noErr(t, globErr)
	isTrue(t, len(setAside) == 0, "the refused open of the sealed image set files aside: %v", setAside)
	return true
}

func runPrefixRound(t *testing.T, seed uint64, reach *prefixReach, hold *flushHold, durable *valueLogDurability, encrypted bool) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	var commits []prefixCommit
	nextSeq := 0
	large := rng.IntN(3) == 0

	// The first life, on a first start.
	dir := filepath.Join(t.TempDir(), "badger")
	kv, err := openVouchedTestStore(dir, prefixTuning)
	noErr(t, err)
	first := &prefixLife{t: t, rng: rng, kv: kv, dir: dir, commits: &commits, large: large, nextSeq: &nextSeq, hold: hold,
		holder: true, pinner: true, sparseSyncs: rng.IntN(2) == 0, durable: durable}
	first.syncAndCapture()
	if rng.IntN(2) == 0 {
		first.toggleHold()
	}
	if rng.IntN(2) == 0 {
		first.togglePin()
	}
	first.steps(30 + rng.IntN(40))
	first.letGo()
	first.unpin()
	noErr(t, kv.Close())
	if encrypted {
		requireSealedStore(t, dir)
	}

	imageDir := filepath.Join(t.TempDir(), "image-1")
	image := buildPrefixImage(t, rng, first)
	if image.flushAfterSync {
		reach.flushAfterSync++
		if large {
			reach.flushAfterSyncLarge++
		}
	}
	if large {
		reach.largeValue++
	}
	writePrefixImage(t, imageDir, image.files)
	replayedLogs := memLogsOf(image.files)
	marker := markerOnDisk(t, imageDir)

	// Held from the open on: the logs the replay made immutable wait in front of their flush, so
	// they stay on disk until the hold is let go. The second image then holds the logs the cut
	// truncated, which is what a kill before their flush leaves.
	hold.holdAt(imageDir)
	reopened, err := openVouchedTestStore(imageDir, prefixTuning)
	if refusedAtFirstFlush(t, encrypted, imageDir, err) {
		reach.refusedAtFirstFlush++
		return
	}
	noErr(t, err, "the first image did not open")
	// And pinned at once, as before the fix: an iterator holds every memtable the replay made.
	// Under the fix that keeps no log on disk; the hold does that.
	pinTxn := reopened.db.NewTransaction(false)
	pin := pinTxn.NewIterator(badger.DefaultIteratorOptions)
	pinned := true
	unpin := func() {
		if pinned {
			pin.Close()
			pinTxn.Discard()
			pinned = false
		}
	}
	defer unpin()

	report := reopened.VouchedOpenReport()
	equal(t, marker.version, report.Vouched, "the open did not cut at the image's marker")
	cut := checkPrefixStore(t, reopened, commits, marker.version, "the first image")
	if report.DiscardedCommits > 0 {
		reach.discarded++
	}
	if report.LogsTruncated+report.LogsRemoved >= 2 {
		reach.laterLog++
	}
	var discarded []prefixCommit
	kept := commits[:0:0]
	for _, c := range commits {
		if c.version > cut {
			discarded = append(discarded, c)
		} else {
			kept = append(kept, c)
		}
	}

	// The second life, on the reopened store: one commit and a sync at once (the discarded
	// versions are handed out again from cut+1, and the marker now vouches at least one of them),
	// then more operations.
	second := &prefixLife{t: t, rng: rng, kv: reopened, dir: imageDir, commits: &kept, large: large, nextSeq: &nextSeq,
		killAtFirst: rng.IntN(2) == 0, durable: durable}
	second.commitSmall(true)
	second.capture(true)
	truncatedKept := report.LogsTruncated > 0
	for name := range replayedLogs {
		if _, ok := second.captures[0].lengths[name]; !ok {
			truncatedKept = false
		}
	}
	hold.letGo()
	unpin()
	second.steps(5 + rng.IntN(20))
	noErr(t, reopened.Close())

	image2Dir := filepath.Join(t.TempDir(), "image-2")
	image2 := buildPrefixImage(t, rng, second)
	if truncatedKept && image2.x == 0 {
		reach.truncatedLogKept++
	}
	writePrefixImage(t, image2Dir, image2.files)
	marker2 := markerOnDisk(t, image2Dir)
	again, err := openVouchedTestStore(image2Dir, prefixTuning)
	if refusedAtFirstFlush(t, encrypted, image2Dir, err) {
		reach.refusedAtFirstFlush++
		return
	}
	noErr(t, err, "the second image did not open (the first cut discarded %d commits)", len(discarded))
	defer func() { noErr(t, again.Close()) }()
	equal(t, marker2.version, again.VouchedOpenReport().Vouched, "the second open did not cut at its image's marker")
	for _, c := range discarded {
		isTrue(t, !storeHas(t, again, prefixCommitKey(c.seq)), "the commit %d (version %d), which the first cut "+
			"discarded, is back after the second image", c.seq, c.version)
	}
	checkPrefixStore(t, again, kept, marker2.version, "the second image", discarded...)
}

// steps runs n random operations, each followed by nothing or a capture.
func (l *prefixLife) steps(n int) {
	for range n {
		roll := l.rng.IntN(100)
		if roll >= 72 && roll < 88 && l.sparseSyncs && l.rng.IntN(5) != 0 {
			roll = 0 // a plain commit instead of a sync
		}
		switch {
		case roll < 45:
			l.commitSmall(false)
		case roll < 67:
			l.commitFill()
		case roll < 72 && l.large:
			l.commitLarge()
		case roll < 82:
			l.syncAndCapture()
		case roll < 88:
			l.commitSmall(true)
			l.capture(true)
		case roll < 92 && l.holder:
			l.toggleHold()
		case roll < 96 && l.pinner:
			l.togglePin()
		default:
			l.capture(false)
		}
	}
}

// commitSmall commits a few sets and deletes over a small key space.
func (l *prefixLife) commitSmall(synced bool) {
	sets := map[string]prefixValue{}
	var deletes []string
	for range 1 + l.rng.IntN(12) {
		key := fmt.Sprintf("k/%03d", l.rng.IntN(300))
		if l.rng.IntN(6) == 0 {
			deletes = append(deletes, key)
			delete(sets, key)
			continue
		}
		sets[key] = prefixValue{seed: l.rng.Uint64(), size: 16 + l.rng.IntN(4000)}
	}
	l.commit(sets, deletes, synced)
}

// commitFill commits about 768 KiB in one unsynced commit, so the 7 MiB memtables rotate.
func (l *prefixLife) commitFill() {
	sets := map[string]prefixValue{}
	for range 24 {
		sets[fmt.Sprintf("f/%03d", l.rng.IntN(300))] = prefixValue{seed: l.rng.Uint64(), size: 32 << 10}
	}
	l.commit(sets, nil, false)
	if l.pinning {
		l.addPin()
	}
}

// commitLarge commits one value larger than Badger's value threshold: its bytes go to the value
// log and the memtable holds a pointer.
func (l *prefixLife) commitLarge() {
	l.commit(map[string]prefixValue{
		fmt.Sprintf("v/%02d", l.rng.IntN(20)): {seed: l.rng.Uint64(), size: prefixLargeValue},
	}, nil, false)
}

func (l *prefixLife) commit(sets map[string]prefixValue, deletes []string, synced bool) {
	t := l.t
	t.Helper()
	seq := *l.nextSeq
	*l.nextSeq = seq + 1
	sets[prefixCommitKey(seq)] = prefixValue{seed: uint64(seq) + 1, size: 8}
	batch := l.kv.NewBatch()
	for _, key := range deletes {
		noErr(t, batch.Delete([]byte(key)))
	}
	for key, value := range sets {
		noErr(t, batch.Set([]byte(key), value.bytes()))
	}
	noErr(t, batch.Commit(synced))
	*l.commits = append(*l.commits, prefixCommit{seq: seq, version: versionOfKey(t, l.kv, prefixCommitKey(seq)), sets: sets, deletes: deletes})
}

func (l *prefixLife) syncAndCapture() {
	noErr(l.t, l.kv.Sync())
	l.capture(true)
}

// capture copies the store directory at this quiet instant (no commit or sync of the test runs).
// The flusher and the compactors run on, so the copy is taken twice and kept only when both
// agree: the MANIFEST is read first, and a flush or a compaction that ran during a copy changes
// it, so two equal copies are one state of the directory. The value log's durable prefixes are
// read before and after the copies and kept only when they did not move: a flush's msync between
// them would make a table the copies hold vouch bytes the earlier read does not count durable,
// and one read after the copies could count bytes the copies do not hold.
func (l *prefixLife) capture(synced bool) {
	t := l.t
	t.Helper()
	for attempt := range 20 {
		durableBefore := l.durable.of(l.dir)
		first, err := capturePrefixDir(l.dir)
		noErr(t, err)
		second, err := capturePrefixDir(l.dir)
		noErr(t, err)
		if samePrefixCapture(first, second) && maps.Equal(durableBefore, l.durable.of(l.dir)) {
			l.keep(second, synced, durableBefore)
			return
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	t.Fatalf("SCENARIO NOT REACHED: the store at %s did not hold still for a copy in 20 attempts", l.dir)
}

// keep records the capture's lengths, marker and value-log durable prefixes, and its files when it
// becomes the kill instant.
func (l *prefixLife) keep(files map[string]prefixFile, synced bool, valueLogDurable map[string]int) {
	capture := prefixCapture{lengths: map[string]int{}, synced: synced, valueLogDurable: valueLogDurable}
	for name, file := range files {
		capture.lengths[name] = len(file.content)
	}
	if marker, ok := files[vouchedFileName]; ok {
		capture.marker = marker.content
	}
	l.captures = append(l.captures, capture)
	k := len(l.captures) - 1
	if (l.killAtFirst && k == 0) || (!l.killAtFirst && l.rng.IntN(k+1) == 0) {
		l.x, l.xFiles = k, files
	}
}

func capturePrefixDir(dir string) (map[string]prefixFile, error) {
	files := map[string]prefixFile{}
	read := func(name string) error {
		file, err := readPrefixFile(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			return nil // removed between the listing and the read: not in the directory now
		}
		if err != nil {
			return err
		}
		files[name] = file
		return nil
	}
	if err := read(badger.ManifestFilename); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || name == badger.ManifestFilename || name == "LOCK" {
			continue
		}
		if err := read(name); err != nil {
			return nil, err
		}
	}
	return files, nil
}

func readPrefixFile(path string) (read prefixFile, err error) {
	file, err := os.Open(path)
	if err != nil {
		return prefixFile{}, fmt.Errorf("opening %s: %w", path, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return prefixFile{}, fmt.Errorf("reading the size of %s: %w", path, err)
	}
	if info.Size() <= prefixWholeFileLimit {
		content, err := io.ReadAll(file)
		if err != nil {
			return prefixFile{}, fmt.Errorf("reading %s: %w", path, err)
		}
		return prefixFile{content: trimLogZeros(path, content), size: info.Size()}, nil
	}
	var content []byte
	chunk := make([]byte, prefixChunk)
	zero := make([]byte, prefixChunk)
	for offset := int64(0); offset < info.Size(); offset += prefixChunk {
		n, err := file.ReadAt(chunk, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return prefixFile{}, fmt.Errorf("reading %s at %d: %w", path, offset, err)
		}
		if bytes.Equal(chunk[:n], zero[:n]) {
			break
		}
		content = append(content, chunk[:n]...)
	}
	return prefixFile{content: trimLogZeros(path, content), size: info.Size()}, nil
}

// trimLogZeros drops the zeros after what a memtable log or a value log holds: Badger
// preallocates both, so the bytes past the last entry are zeros that a later capture overwrites.
// The size is kept, and an image puts the zeros back.
func trimLogZeros(path string, content []byte) []byte {
	if !strings.HasSuffix(path, ".mem") && !strings.HasSuffix(path, ".vlog") {
		return content
	}
	return bytes.TrimRight(content, "\x00")
}

func samePrefixCapture(a, b map[string]prefixFile) bool {
	if len(a) != len(b) {
		return false
	}
	for name, file := range a {
		other, ok := b[name]
		if !ok || other.size != file.size || !bytes.Equal(other.content, file.content) {
			return false
		}
	}
	return true
}

// prefixImage is the store a kill at capture x leaves.
type prefixImage struct {
	files          map[string]prefixFile
	x              int
	flushAfterSync bool
}

// buildPrefixImage builds the image of a kill at the life's capture x: s is the last completed
// sync at or before x, m any completed sync at or before it. The marker is m's; each memtable
// log is x's bytes up to a random page boundary at or above the end of what s made durable (a
// later page may or may not have reached the disk), and a log created after s may be missing
// altogether (its directory entry was never made durable); each value-log file is cut the same
// way, at or above the end of what s or a later msync before x made durable (a flush's, since
// BUG-0827's slice 2, or a rotation's); every other file is x's.
//
// Before slice 2 a flush did not sync the value log, and this test took the value log whole when
// a table was flushed after s, so as not to build that known hole (T3b). Now a table flushed
// after s, with the value log cut down to its last msync, is the case slice 2 closes.
func buildPrefixImage(t *testing.T, rng *rand.Rand, l *prefixLife) prefixImage {
	t.Helper()
	x := l.x
	var syncs []int
	for i := 0; i <= x; i++ {
		if l.captures[i].synced {
			syncs = append(syncs, i)
		}
	}
	isTrue(t, len(syncs) > 0, "SCENARIO NOT REACHED: no completed sync at or before the kill instant")
	s := syncs[len(syncs)-1]
	m := s
	if rng.IntN(2) == 0 {
		m = syncs[rng.IntN(len(syncs))]
	}
	atS := l.captures[s].lengths
	flushAfterSync := false
	for name := range l.xFiles {
		if _, ok := atS[name]; strings.HasSuffix(name, ".sst") && !ok {
			flushAfterSync = true
		}
	}
	isTrue(t, l.captures[m].marker != nil, "the capture of a completed sync holds no marker")
	files := map[string]prefixFile{}
	for name, file := range l.xFiles {
		synced, existed := atS[name]
		switch {
		case name == vouchedFileName:
			files[name] = prefixFile{content: l.captures[m].marker, size: int64(len(l.captures[m].marker))}
		case strings.HasSuffix(name, ".mem") && !existed && rng.IntN(3) == 0:
			// Created after the last sync: its directory entry may not be durable.
		case strings.HasSuffix(name, ".mem"):
			files[name] = pageCut(t, rng, name, synced, file)
		case strings.HasSuffix(name, ".vlog"):
			durable := min(max(synced, l.captures[x].valueLogDurable[name]), len(file.content))
			files[name] = pageCut(t, rng, name, durable, file)
		default:
			files[name] = file
		}
	}
	return prefixImage{files: files, x: x, flushAfterSync: flushAfterSync}
}

// pageCut is later's bytes up to a random page boundary, or to synced (the length s made
// durable) when that is further: a log only grows, so its bytes up to synced are s's.
func pageCut(t *testing.T, rng *rand.Rand, name string, synced int, later prefixFile) prefixFile {
	t.Helper()
	isTrue(t, synced <= len(later.content), "the log %s shrank between two captures of one life", name)
	low := synced / prefixPage
	high := (len(later.content) + prefixPage - 1) / prefixPage
	cutAt := min((low+rng.IntN(high-low+1))*prefixPage, len(later.content))
	return prefixFile{content: bytes.Clone(later.content[:max(cutAt, synced)]), size: later.size}
}

func writePrefixImage(t *testing.T, dir string, files map[string]prefixFile) {
	t.Helper()
	noErr(t, os.MkdirAll(dir, 0o700))
	for name, file := range files {
		path := filepath.Join(dir, name)
		noErr(t, os.WriteFile(path, file.content, 0o600))
		noErr(t, os.Truncate(path, file.size))
	}
}

func memLogsOf(files map[string]prefixFile) map[string]bool {
	logs := map[string]bool{}
	for name := range files {
		if strings.HasSuffix(name, ".mem") {
			logs[name] = true
		}
	}
	return logs
}

// checkPrefixStore asserts that kv holds exactly the model through C = max(V, T), V the
// marker's version and T the highest version of a table, and returns C.
func checkPrefixStore(t *testing.T, kv *BadgerKV, commits []prefixCommit, vouched uint64, which string, discarded ...prefixCommit) uint64 {
	t.Helper()
	cut := vouched
	for _, table := range kv.db.Tables() {
		cut = max(cut, table.MaxVersion)
	}
	want := prefixModelAt(commits, cut)
	got := map[string][]byte{}
	noErr(t, kv.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("reading %s: %w", it.Item().Key(), err)
			}
			got[string(it.Item().KeyCopy(nil))] = value
		}
		return nil
	}))
	var problems []string
	for key, value := range want {
		have, ok := got[key]
		switch {
		case !ok:
			problems = append(problems, "missing "+key)
		case !bytes.Equal(have, value.bytes()):
			problems = append(problems, fmt.Sprintf("%s holds %d bytes that are not its value at C", key, len(have)))
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			problems = append(problems, "unexpected "+key)
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		var above []string
		for _, c := range commits {
			if c.version > cut {
				above = append(above, fmt.Sprintf("%d@%d", c.seq, c.version))
			}
		}
		t.Fatalf("%s reopened is not the prefix of its commits through C = max(V %d, T) = %d: %d differences, the "+
			"first %s; the commits above C (seq@version) are %v; what the store holds and what wrote the first differing "+
			"keys: %s", which, vouched, cut, len(problems),
			strings.Join(problems[:min(len(problems), 8)], ", "), above, prefixKeyHistory(kv, problems, commits, discarded))
	}
	return cut
}

// prefixKeyHistory says, for the first differing keys of a failed check, the version the store
// holds each at and every commit that set or deleted it, the ones an earlier cut discarded
// included: what a reader needs to tell a lost delete from a commit that came back.
func prefixKeyHistory(kv *BadgerKV, problems []string, commits, discarded []prefixCommit) string {
	var lines []string
	for _, problem := range problems[:min(len(problems), 4)] {
		key := problem[strings.LastIndex(problem, " ")+1:]
		if at := strings.Index(problem, " holds "); at > 0 {
			key = problem[:at]
		}
		held := "absent"
		_ = kv.db.View(func(txn *badger.Txn) error { // a diagnostic read: a failure leaves "absent"
			if item, err := txn.Get([]byte(key)); err == nil {
				held = fmt.Sprintf("held at version %d", item.Version())
			}
			return nil
		})
		var wrote []string
		note := func(list []prefixCommit, label string) {
			for _, c := range list {
				if _, set := c.sets[key]; set {
					wrote = append(wrote, fmt.Sprintf("set by %d@%d%s", c.seq, c.version, label))
				}
				for _, deleted := range c.deletes {
					if deleted == key {
						wrote = append(wrote, fmt.Sprintf("deleted by %d@%d%s", c.seq, c.version, label))
					}
				}
			}
		}
		note(commits, "")
		note(discarded, " (discarded by the first cut)")
		lines = append(lines, fmt.Sprintf("%s: %s; %s", key, held, strings.Join(wrote, ", ")))
	}
	return strings.Join(lines, " | ")
}
