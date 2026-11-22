package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// Operation type constants for WAL entries.
const (
	OpInsert byte = 0
	OpDelete byte = 1
	OpUpdate byte = 2
)

// Default maximum WAL segment file size before rotation (64 MB).
const defaultMaxFileSize int64 = 64 * 1024 * 1024

// walSegmentPattern matches WAL segment filenames: wal_NNNNN.log
var walSegmentPattern = regexp.MustCompile(`^wal_(\d+)\.log$`)

// walSpareTmpName is the name every segment is born under: it gets its header,
// is fsynced, and only then is renamed to its wal_N.log name (spec §2.1, owner
// D9). It never matches walSegmentPattern, so no reader, WAL.Truncate or
// pre-change build ever takes a segment still being born for a segment.
const walSpareTmpName = "wal_spare.tmp"

// walSpareReadyName is a born, durable spare a preallocating run keeps ready
// for its next rotation (spec §2.1). Like walSpareTmpName it is never a
// segment name.
const walSpareReadyName = "wal_spare.ready"

// walStateFileName is the state file builds before format 2 kept beside the
// segments, with an active index and a last sequence written without an fsync
// (BUG-0720). The open derives both from the segments and the incarnation
// chain, and removes the file once the log has a v2 segment with a durable name
// (spec §3.4 step 8).
const walStateFileName = "state.bson"

// WAL entry framing. Each entry begins with an 8-byte big-endian word holding
// the body length in its low 32 bits and a CRC32C of the body in its high 32
// bits.
//
// The high half was padding before the checksum existed, so segments written by
// an older build still read: a zero high half means "no checksum was recorded"
// and the length is validated on its own. Mixed segments are safe for the same
// reason — each entry describes itself.
//
// The checksum exists because a full disk leaves torn entries behind: a partial
// write leaves a body whose length prefix describes bytes that were never
// written. Without a checksum that body parses as garbage or, worse, as a
// plausible length that panics the reader. This log has already cost one node a
// permanent crash loop that way.
const (
	// walMaxEntrySize bounds a single entry. It must be at least as large as
	// anything Append accepts, or a written entry could not be read back --
	// silent data loss of exactly the kind the segment store had.
	walMaxEntrySize = 128 << 20
	// walMinEntrySize is 8 bytes of sequence + 1 of op + 4 of bucket length.
	walMinEntrySize = 13
)

// Sealed WAL format constants (§3.5).
const (
	walFileHeaderLen   = 24
	walSeqFieldLen     = 8
	walNonceLen        = 12
	walTagLen          = 16
	walSealedMinBody   = 5                                          // op + bucketLen (empty bucket)
	walSealedMinOnDisk = walNonceLen + walSealedMinBody + walTagLen // 33
)

// walSealedMaxOnDisk is the maximum on-disk length for a sealed entry.
var walSealedMaxOnDisk = walMaxEntrySize + walNonceLen + walTagLen

// sealLimit is the maximum number of entries per sealed WAL file key before rotation.
// Package variable (not const) so tests can lower it.
var sealLimit = int64(1 << 31)

// decodeWALHeader splits a header word into its length and checksum halves.
// checksummed is false for an entry written before the checksum existed.
func decodeWALHeader(word uint64) (bodyLen int, checksum uint32, checksummed bool) {
	bodyLen = int(uint32(word))
	checksum = uint32(word >> 32)
	return bodyLen, checksum, checksum != 0
}

// isFileSealedWAL checks if the first 6 bytes of a segment file are the 24-byte
// header's: `00 48 44 42 02 04` (magic, version, purpose). Every v1 sealed and
// every v2 segment starts with them; the flags byte tells the two apart.
func isFileSealedWAL(header []byte) bool {
	if len(header) < 6 {
		return false
	}
	return header[0] == 0x00 && header[1] == 0x48 && header[2] == 0x44 &&
		header[3] == 0x42 && header[4] == 0x02 && header[5] == sealed.PurposeWALEntry
}

// walEntryAAD builds a sealed entry's additional authenticated data: the
// segment's 24-byte file header, the sequence number, and the absolute offset
// of the entry's first byte — the length word, not the nonce (§3.5).
//
// Every byte of it is readable before the entry is decrypted, which is the rule
// that governs every AAD in this format. Position goes here and never into the
// nonce, because rollbackPartialWrite reuses a sequence number and an offset,
// and a v1 tail repaired by an earlier build did too — reuse an AEAD nonce and
// the key is gone.
func walEntryAAD(fileHeader []byte, seq uint64, entryOffset int64) []byte {
	aad := make([]byte, walFileHeaderLen+walSeqFieldLen+8)
	copy(aad, fileHeader)
	binary.BigEndian.PutUint64(aad[walFileHeaderLen:], seq)
	binary.BigEndian.PutUint64(aad[walFileHeaderLen+walSeqFieldLen:], uint64(entryOffset))
	return aad
}

// walEntryAEAD builds the AES-GCM cipher for one segment's entries, from the
// file key the key source derives for that segment under that epoch.
func walEntryAEAD(keys WALKeySource, epochID uint64, segIdx int) (cipher.AEAD, error) {
	fileKey, err := keys.FileKey(epochID, segIdx)
	if err != nil {
		return nil, fmt.Errorf("deriving the file key for WAL segment %d under epoch %d: %w", segIdx, epochID, err)
	}
	block, err := aes.NewCipher(fileKey)
	if err != nil {
		return nil, fmt.Errorf("building the cipher for WAL segment %d: %w", segIdx, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("building the AEAD for WAL segment %d: %w", segIdx, err)
	}
	return gcm, nil
}

// walScanResult is what reading one segment produced: the entries it could
// decode and the offset from which the segment stops being trustworthy.
type walScanResult struct {
	entries []WALEntry
	// validEnd is the offset of the first byte that is not part of a valid
	// entry. Equal to the file size when the segment is intact.
	validEnd int64
	// torn is true when the segment ends in a partial entry, or an entry
	// failed its checksum. Everything from validEnd on is unusable.
	torn bool
	// reason explains a torn tail in one sentence, for the startup warning.
	reason string
	// entryCount and lastSeq describe every framed entry the scan walked past,
	// decrypted or not: a sealed segment scanned without a key yields no
	// entries but still reports both, because the framing carries them in the
	// clear. The open takes the tail's last sequence from these.
	entryCount int
	lastSeq    uint64
	// sealedWithoutKey is true when the segment is sealed and no key source was
	// offered. The framing was validated and the bodies were not read, so a
	// caller that wants entries must refuse rather than report an empty segment.
	sealedWithoutKey bool
}

// crc32cTable is shared: building the table per entry would allocate on every
// single write.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// WALOriginKind says where a log entry came from.
type WALOriginKind byte

const (
	// WALOriginLegacy marks an entry written before the origin existed. It has
	// no origin block on disk and is read exactly as it always was.
	WALOriginLegacy WALOriginKind = 0
	// WALOriginOwn marks an entry this node appended because it decided it owned
	// the bucket (or the database-level change).
	WALOriginOwn WALOriginKind = 1
	// walOriginReplicaBeforeOutcome is the block of a replica entry written by a
	// build before the block carried the owner's outcome and the term
	// (BUG-0581): kind(1) len(source)(1) source seq(8). It is read, as
	// WALOriginReplica with OutcomeNotLogged set, and never written. It has
	// its own kind byte because read as kind 3 it would take the entry's first
	// data bytes for the two fields.
	walOriginReplicaBeforeOutcome WALOriginKind = 2
	// WALOriginReplica marks an entry this node applied from another node's
	// stream; Source and Seq say whose and at which of its sequences,
	// OwnerOutcome and Term what the owner's apply did and the term it shipped
	// the entry in.
	WALOriginReplica WALOriginKind = 3
)

// WALOrigin is the optional block after an entry's key. Epoch is set on an own
// entry only: the membership epoch the writer read when it decided it was the
// owner. Source, Seq, OwnerOutcome and Term are set on a replica entry only.
// The owner's outcome is logged because only the wire carried it: a replica
// that refused an entry its owner applied and died before its record of the
// divergence committed left a log entry its restart could not tell from an
// agreed refusal, and counted it applied (BUG-0581).
//
// OutcomeNotLogged marks a replica entry read from a kind 2 block, which an
// earlier build wrote: its OwnerOutcome and Term are zero because the build
// did not log them, not because the owner's outcome was unknown. The owner
// decided on 2026-10-16 that such an entry replays as on the build that wrote
// it, so a reader that judges by the logged outcome skips it.
type WALOrigin struct {
	Kind             WALOriginKind
	Epoch            uint64
	Source           string
	Seq              uint64
	OwnerOutcome     byte
	Term             uint64
	OutcomeNotLogged bool
}

// walOriginFlag is bit 31 of the bucket-length word. A bucket name is never 2
// GiB long (walMaxEntrySize bounds a whole entry), so an entry without the flag
// parses as it always has and a build that does not know the flag reads a
// flagged entry as a bucket name running past the entry.
const walOriginFlag uint32 = 1 << 31

// walMaxOriginSource bounds a replica entry's source id: its length is one byte.
const walMaxOriginSource = 255

// WALEntry represents a single entry read from the WAL.
type WALEntry struct {
	Seq    uint64
	Op     byte
	Bucket string
	Key    []byte
	Data   []byte
	Origin WALOrigin
}

// validate reports why AppendFrom cannot write this origin. Legacy is refused
// because it is what an entry without a block already is: a caller that means
// "no origin" calls Append.
func (o WALOrigin) validate() error {
	switch o.Kind {
	case WALOriginOwn:
		return nil
	case WALOriginReplica:
		if len(o.Source) > walMaxOriginSource {
			return fmt.Errorf("source id %q is %d bytes; an origin block holds at most %d", o.Source, len(o.Source), walMaxOriginSource)
		}
		if o.OutcomeNotLogged {
			return errors.New("a replica entry without the owner's outcome is kind 2, which this build reads and never writes")
		}
		return nil
	case WALOriginLegacy:
		return errors.New("kind 0 is legacy, which has no origin block; use Append for an entry without an origin")
	default:
		return fmt.Errorf("origin kind %d is not one this build writes", o.Kind)
	}
}

// encodedLen is the size of the origin block, or 0 for a legacy entry.
func (o WALOrigin) encodedLen() int {
	switch o.Kind {
	case WALOriginOwn:
		return 1 + len(binary.AppendUvarint(nil, o.Epoch))
	case WALOriginReplica:
		return 1 + 1 + len(o.Source) + 8 + 1 + len(binary.AppendUvarint(nil, o.Term))
	default:
		return 0
	}
}

// appendTo writes the origin block: own is kind(1) uvarint(epoch); replica is
// kind(1) len(source)(1) source seq(8) owner_outcome(1) uvarint(term), seq
// big-endian.
func (o WALOrigin) appendTo(body []byte) []byte {
	switch o.Kind {
	case WALOriginOwn:
		body = append(body, byte(WALOriginOwn))
		return binary.AppendUvarint(body, o.Epoch)
	case WALOriginReplica:
		body = append(body, byte(WALOriginReplica), byte(len(o.Source)))
		body = append(body, o.Source...)
		body = binary.BigEndian.AppendUint64(body, o.Seq)
		body = append(body, o.OwnerOutcome)
		return binary.AppendUvarint(body, o.Term)
	default:
		return body
	}
}

// WAL is a sequential append-only write-ahead log. Entries are written in
// order with monotonic sequence numbers. Every Append waits for its entry to
// be durably fsynced before returning, providing the durability guarantee for
// crash recovery and replication — see Append and waitForSync for how
// concurrent callers share that fsync ("group commit") instead of paying for
// one each.
type WAL struct {
	dir string
	// dirGuard refuses appends once dir is no longer the directory the log
	// opened: an append to the open, unlinked segment would be fsynced and
	// acknowledged, and lost at the next restart (BUG-0390).
	dirGuard    *dirGuard
	mu          sync.Mutex
	activeFile  *os.File
	activeIndex int
	// writeOff is where the next entry is written in the active segment, and
	// so the end of the bytes any reader may look at there: everything below it
	// is whole entries. It is the bound instead of the file size because a
	// preallocated segment is longer than its entries.
	writeOff    int64
	lastSeq     uint64
	maxFileSize int64
	recovery    *WALRecovery
	keys        WALKeySource // nil for plaintext (legacy) format
	sealCounter int64        // entries sealed in the current file; used with sealLimit
	// activeFormat is how the active segment frames its entries: v2 for every
	// segment this build creates, v1 for a tail an earlier build wrote, which
	// keeps its own format until it rotates so no segment is ever mixed. Its
	// header is the first 24 bytes of every sealed entry's AAD. sealAEAD is the
	// cipher for the active segment's file key, nil on a plain one. Both are
	// replaced on rotation.
	activeFormat walSegmentFormat
	sealAEAD     cipher.AEAD

	// incarnations is the log's incarnation chain, the link this open minted
	// last (wal_incarnation.go). OpenWAL sets it and nothing changes it after.
	incarnations []WALIncarnation
	// skips are the runs of sequences the log passed over (FastForward),
	// guarded by mu. FastForward replaces the slice and never changes one a
	// reader may hold.
	skips []WALSkip

	// syncMu, syncCond, syncing and syncedSeq coordinate group-commit fsync:
	// see waitForSync. syncErr is set once an fsync fails and poisons the WAL
	// (see poisonedErr). All four are guarded by syncMu, independently of mu.
	syncMu    sync.Mutex
	syncCond  *sync.Cond
	syncing   bool
	syncedSeq uint64
	syncErr   error
	// writtenSeq is the highest sequence whose entry writeLocked has written to
	// the log; waitForSync's leader reads it before its fsync to know what the
	// fsync covers. Stored under mu, read without it: taking mu in waitForSync
	// would deadlock against rotate and Close, which hold mu while they wait on
	// syncWG for the very goroutines in waitForSync.
	writtenSeq atomic.Uint64
	// syncWG counts Append calls currently inside waitForSync. rotate and
	// Close wait on it before Sync+Close'ing the file those calls might still
	// be fsyncing, so that call can't race a concurrent Sync/Close on the
	// same *os.File and report a spurious failure.
	syncWG sync.WaitGroup
	// poisoned holds syncErr once it is set, so WriteRefusal reports a
	// poisoned log without taking syncMu.
	poisoned atomic.Pointer[walPoison]
	// rotationRefusal is the error the last attempt to prepare a next segment
	// failed with, and when the next may try (refuseSegment); nil once one is
	// prepared. It is an atomic so WriteRefusal reads it without mu.
	rotationRefusal atomic.Pointer[walRotationRefusal]
	// closed is set by Close, under mu. A nil activeFile is a closed log only
	// with it; without it, and unpoisoned, it is a log with no active segment
	// (spec §4.3), which the next write activates.
	closed bool
	// counters are what Stats reports.
	counters walCounters

	// preallocate and spareBytes are fixed at open: whether segments are
	// preallocated, and the size every spare is filled to. The filler reads
	// spareBytes without mu, so it is not maxFileSize, which a test may lower.
	preallocate bool
	spareBytes  int64
	// activePreallocated is whether the active segment was born a spare: its
	// write failures poison, and R2 cuts it when it is retired. Guarded by mu.
	activePreallocated bool
	// activeDataFlush is whether the active segment's group commit may be the
	// data-only flush: preallocated and never grown past its preallocation.
	// Stored under mu before writtenSeq, read by waitForSync's leader after it.
	activeDataFlush atomic.Bool
	// spareMu guards spareState, spareCurrent and fillWarning. It is a leaf: a rotation
	// takes it under mu, the filler takes it alone, and nothing holding it
	// takes mu.
	spareMu      sync.Mutex
	spareState   spareState
	spareCurrent *spareFill
	// fillWarning is what the log has said about failing fills; spareMu
	// guards it too.
	fillWarning fillWarning
	// fillerStop, fillerWake and fillerDone are the filler's exit, its wake-up
	// and what Close waits on; all nil or unused without preallocation.
	fillerStop     chan struct{}
	fillerWake     chan struct{}
	fillerDone     sync.WaitGroup
	fillerStopOnce sync.Once

	// beforeSyncHook, readDecodeHook, readSnapshotHook and openCursorHook are
	// test seams; see SetBeforeSyncHook. readSnapshotHook runs after a read has
	// taken its snapshot and before it opens any segment; openCursorHook runs
	// inside OpenCursor between its horizon check and its snapshot.
	beforeSyncHook atomic.Pointer[func(seq uint64)]
	// fsyncHook runs in waitForSync's leader just before each group-commit
	// file.Sync; see SetFsyncHook.
	fsyncHook atomic.Pointer[func()]
	// sys is the table of syscalls the log makes where the order or the
	// failure of a call matters. Production fills it with the real calls; only
	// in-package tests replace entries, before the first append, to record an
	// order or to fail a call a healthy disk would not.
	sys              walSyscalls
	readDecodeHook   func()
	readSnapshotHook func()
	openCursorHook   func()
}

// segmentLimit is the rotation size opts asks for.
func segmentLimit(opts WALOptions) int64 {
	if opts.SegmentBytes > 0 {
		return opts.SegmentBytes
	}
	return defaultMaxFileSize
}

// WALRecovery records what OpenWAL had to discard to make the log readable.
// A non-nil result means the log ended in a partial entry — the signature of a
// crash or a full disk — and non-zero bytes were dropped. Segment is the
// highest segment the open cut bytes from and RecoveredTo the offset it cut it
// to; DroppedBytes is the sum over every segment the open cut (BD-3). A clean
// zero tail drops nothing and is not a recovery.
type WALRecovery struct {
	Segment      int
	RecoveredTo  int64
	DroppedBytes int64
	Reason       string
}

// Recovery returns what OpenWAL discarded, or nil when the log was intact.
func (w *WAL) Recovery() *WALRecovery {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.recovery
}

// segmentFileName returns the filename of the WAL segment with the given index.
func segmentFileName(idx int) string {
	return fmt.Sprintf("wal_%05d.log", idx)
}

// segmentFilePath returns the full path for a WAL segment file.
func segmentFilePath(dir string, idx int) string {
	return filepath.Join(dir, segmentFileName(idx))
}

// isWALSegmentFile checks if a filename matches the WAL segment file pattern.
func isWALSegmentFile(name string) bool {
	return walSegmentPattern.MatchString(name)
}

// holdsLogDisk reports whether a file in the log directory is disk the log
// holds: a segment, or a spare, ready or being filled.
func holdsLogDisk(name string) bool {
	return isWALSegmentFile(name) || name == walSpareReadyName || name == walSpareTmpName
}

// parseWALSegmentIndex extracts the numeric index from a WAL segment filename.
func parseWALSegmentIndex(name string) (int, error) {
	matches := walSegmentPattern.FindStringSubmatch(name)
	if matches == nil {
		return 0, fmt.Errorf("invalid WAL segment filename: %s", name)
	}
	var idx int
	for _, c := range matches[1] {
		idx = idx*10 + int(c-'0')
	}
	return idx, nil
}

// OpenWAL creates or opens a WAL in the given directory: OpenWALWithOptions
// with WALOptions{}, a growing log with 64 MiB segments.
// keys is a WALKeySource for sealed format; nil means plaintext (legacy) format.
func OpenWAL(dir string, keys WALKeySource) (*WAL, error) {
	return OpenWALWithOptions(dir, keys, WALOptions{})
}

// WALOptions are fixed for the life of an open log.
type WALOptions struct {
	// Preallocate asks for preallocated segments: each is born a whole
	// segment of written zeros by a background filler, so its group commit is
	// a data-only flush. It reserves two segments of disk. The caller resolves
	// it to a plain bool; storage never sees "auto".
	Preallocate bool
	// SegmentBytes is the size at which the active segment rotates; 0 means
	// defaultMaxFileSize (64 MiB).
	SegmentBytes int64
}

// OpenWALWithOptions opens the log in dir, creating it when there is none, in
// the order of spec §3.4. It derives everything from the segments and the
// incarnation chain, never from a state file:
//
//  1. the spare names: a wal_spare.tmp is an interrupted birth and goes; a
//     wal_spare.ready goes too unless the log preallocates;
//  2. the tail is the highest segment that holds an entry: the open walks down
//     from the highest one, cutting every segment it walks over to its header;
//  3. the tail is cut where its entries end and fsynced, always: after a
//     process crash the bytes the scan read can still be page cache, and the
//     open counts them as synced (BUG-0718);
//  4. the incarnation is minted at the tail's last sequence, floored at the
//     top skip and at the previous link's base;
//  5. a new segment is born at the highest index + 1. Every open starts one,
//     so nothing is ever written over a region the scan had to judge: a torn
//     entry can keep a zero length word while its later sectors landed, and an
//     append at the end of the entries would make those bytes follow it;
//  6. the state file of builds before format 2 is removed, once the new
//     segment's name is durable;
//  7. with Preallocate, the filler starts, to make the spare the next
//     rotation takes.
//
// With Preallocate the new segment of step 5 is a ready spare left by the
// previous run, validated, or one the open fills itself.
func OpenWALWithOptions(dir string, keys WALKeySource, opts WALOptions) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating WAL directory: %w", err)
	}
	guard, err := newDirGuard(dir)
	if err != nil {
		return nil, fmt.Errorf("opening the WAL: %w", err)
	}

	w := &WAL{
		dir:         dir,
		dirGuard:    guard,
		maxFileSize: segmentLimit(opts),
		keys:        keys,
		preallocate: opts.Preallocate,
		spareBytes:  segmentLimit(opts),

		sys: walSyscallsAtOpen(),
	}
	w.syncCond = sync.NewCond(&w.syncMu)

	if err := w.clearSpareNames(opts.Preallocate); err != nil {
		return nil, err
	}
	if opts.Preallocate {
		// A ready spare a previous run left is kept for the new segment,
		// where it is validated like any spare.
		switch _, err := os.Lstat(filepath.Join(dir, walSpareReadyName)); {
		case err == nil:
			w.spareState = spareReady
		case !errors.Is(err, fs.ErrNotExist):
			return nil, fmt.Errorf("checking for a spare WAL segment in %s: %w", dir, err)
		}
	}

	segments := w.listSegments()
	tailLastSeq, err := w.retireTail(segments)
	if err != nil {
		return nil, err
	}

	// Every open mints a link, at the last sequence found, before anything can
	// be appended or read: a position a peer learned from an earlier process is
	// then told apart from one learned from this process, however much this
	// process appends (spec §20.4).
	chain, skips, err := mintIncarnation(dir, tailLastSeq, len(segments) > 0)
	if err != nil {
		return nil, err
	}
	w.incarnations, w.skips = chain, skips
	// The base is the last sequence found, floored at the top skip and at the
	// previous link's base: a process that skipped or wrote and then left
	// nothing readable still never gave a sequence below it.
	w.lastSeq = chain[len(chain)-1].Base

	next := 1
	if len(segments) > 0 {
		next = segments[len(segments)-1].index + 1
	}
	if err := w.startOpenSegment(next); err != nil {
		return nil, err
	}

	// What is on disk at open is synced: the tail was fsynced above, and every
	// segment below it was fsynced by the rotation that retired it.
	w.syncedSeq = w.lastSeq
	w.writtenSeq.Store(w.lastSeq)

	if opts.Preallocate {
		w.startFiller()
	}
	return w, nil
}

// clearSpareNames removes what the spare names hold at open (spec §2.1): a
// wal_spare.tmp is a birth a crash interrupted and is never finished; a
// wal_spare.ready left by a preallocating run is removed when this run does not
// preallocate, since nothing would ever take it and it holds a segment's worth
// of disk.
func (w *WAL) clearSpareNames(preallocate bool) error {
	if err := w.sys.remove(filepath.Join(w.dir, walSpareTmpName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing the interrupted segment birth %s in %s: %w", walSpareTmpName, w.dir, err)
	}
	if preallocate {
		return nil
	}
	err := w.sys.remove(filepath.Join(w.dir, walSpareReadyName))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("removing the preallocated spare segment %s in %s: %w", walSpareReadyName, w.dir, err)
	}
	slog.Info("removed the preallocated spare segment left by an earlier start with storage.wal_preallocate on",
		"dir", w.dir, "file", walSpareReadyName)
	return nil
}

// retireTail finds the tail, the highest segment that holds an entry, and
// returns its last sequence (0 when no segment holds one). Walking down from
// the highest segment, every segment that holds no entry is cut to where its
// entries would start (24 bytes on v2; a v1 one by v1 rules); only the highest
// can have bytes to drop, because a rotation cuts a segment durably before the
// next one has a name. The tail is cut where its entries end and fsynced,
// whether or not anything was cut. Every non-zero byte cut is summed into one
// recovery report (BD-3).
func (w *WAL) retireTail(segments []segmentRef) (uint64, error) {
	var recovery *WALRecovery
	var dropped int64
	var below segmentBelow
	for i := len(segments) - 1; i >= 0; i-- {
		retired, err := w.retireSegment(segments, i, &below)
		if err != nil {
			return 0, err
		}
		if retired.dropped > 0 {
			dropped += retired.dropped
			if recovery == nil {
				recovery = &WALRecovery{Segment: segments[i].index, RecoveredTo: retired.cutTo, Reason: retired.reason}
			}
		}
		if retired.holdsEntries {
			if recovery != nil {
				recovery.DroppedBytes = dropped
				w.recovery = recovery
			}
			return retired.lastSeq, nil
		}
	}
	if recovery != nil {
		recovery.DroppedBytes = dropped
		w.recovery = recovery
	}
	return 0, nil
}

// segmentBelow remembers the nearest segment under the open's candidate that
// holds an entry, and that entry's sequence, so the walk-back reads each
// segment's first frame at most once: a search for a candidate holds for every
// lower candidate above the segment it found.
type segmentBelow struct {
	searched bool
	// position is the found segment's place in the listing, -1 for none.
	position int
	firstSeq uint64
}

// firstSequenceBelow is the first sequence of the nearest segment below
// position candidate that holds an entry, or 0 when none does. It is what the
// open compares a v2 candidate's first entry against (spec §3.3 rule 5 at
// open): one frame read, never the whole segment below.
func (w *WAL) firstSequenceBelow(segments []segmentRef, candidate int, below *segmentBelow) (uint64, error) {
	if below.searched && below.position < candidate {
		return below.firstSeq, nil
	}
	*below = segmentBelow{searched: true, position: -1}
	for j := candidate - 1; j >= 0; j-- {
		seq, found, err := w.firstSequenceOf(segments[j], -1)
		if err != nil {
			return 0, fmt.Errorf("reading the first sequence of WAL segment %s below %s: %w", segments[j].name, segments[candidate].name, err)
		}
		if found {
			below.position, below.firstSeq = j, seq
			return seq, nil
		}
	}
	return 0, nil
}

// retiredSegment is what the open did to one segment of the walk-back.
type retiredSegment struct {
	holdsEntries bool
	lastSeq      uint64
	// cutTo is where the segment now ends: the end of its last entry, or of
	// its header when it holds none.
	cutTo int64
	// dropped is the bytes cut that a report counts: on v2 the non-zero
	// extent (zeros after the last entry are no damage), on v1 every byte, as
	// the v1 repair always reported.
	dropped int64
	reason  string
}

// retireSegment scans segments[i] by its format's rules, cuts it where its
// entries end, and fsyncs it when it holds an entry or was cut.
func (w *WAL) retireSegment(segments []segmentRef, i int, below *segmentBelow) (retiredSegment, error) {
	seg := segments[i]
	f, err := os.OpenFile(filepath.Join(w.dir, seg.name), os.O_RDWR, 0)
	if err != nil {
		return retiredSegment{}, fmt.Errorf("opening WAL segment %s at open: %w", seg.name, err)
	}
	retired, err := w.retireOpenSegment(f, segments, i, below)
	if closeErr := f.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("closing WAL segment %s at open: %w", seg.name, closeErr)
	}
	return retired, err
}

// retireOpenSegment is retireSegment on the segment's open file.
func (w *WAL) retireOpenSegment(f *os.File, segments []segmentRef, i int, below *segmentBelow) (retiredSegment, error) {
	seg := segments[i]
	info, err := f.Stat()
	if err != nil {
		return retiredSegment{}, fmt.Errorf("stat WAL segment %s at open: %w", seg.name, err)
	}
	size := info.Size()
	format, err := readSegmentFormat(f, size, seg.name)
	if err != nil {
		return retiredSegment{}, fmt.Errorf("scanning WAL segment %s: %w", seg.name, err)
	}
	var prevFirst uint64
	if format.v2 {
		if prevFirst, err = w.firstSequenceBelow(segments, i, below); err != nil {
			return retiredSegment{}, err
		}
	}
	scanned, err := w.scanSegmentFile(f, size, seg.index, prevFirst)
	if err != nil {
		return retiredSegment{}, fmt.Errorf("scanning WAL segment %s: %w", seg.name, err)
	}

	retired := retiredSegment{
		holdsEntries: scanned.entryCount > 0,
		lastSeq:      scanned.lastSeq,
		cutTo:        scanned.validEnd,
		dropped:      size - scanned.validEnd,
		reason:       scanned.reason,
	}
	if format.v2 {
		if retired.dropped, err = nonZeroExtentFrom(f, scanned.validEnd, size); err != nil {
			return retiredSegment{}, fmt.Errorf("measuring the bytes after the entries of WAL segment %s: %w", seg.name, err)
		}
		if retired.dropped > 0 && !scanned.torn {
			retired.reason = fmt.Sprintf("non-zero bytes after a zero length word at offset %d", scanned.validEnd)
		}
	}

	if retired.cutTo < size {
		if err := w.sys.ftruncate(f, retired.cutTo); err != nil {
			return retiredSegment{}, fmt.Errorf("cutting WAL segment %s at offset %d, where its entries end: %w", seg.name, retired.cutTo, err)
		}
	}
	if retired.cutTo < size || retired.holdsEntries {
		if err := w.sys.fsync(f); err != nil {
			return retiredSegment{}, fmt.Errorf("fsyncing WAL segment %s at open: %w", seg.name, err)
		}
	}
	if retired.holdsEntries {
		slog.Debug("retired the WAL tail segment at open; new entries go to a new segment",
			"segment", seg.name, "entries_end_at", retired.cutTo, "last_sequence", retired.lastSeq)
	} else if retired.cutTo < size {
		slog.Debug("a WAL segment above the tail holds no entry and was cut to its header at open",
			"segment", seg.name, "ends_at", retired.cutTo)
	}
	return retired, nil
}

// nonZeroExtentFrom is the number of bytes from offset from up to and
// including the last non-zero byte before size, 0 when they are all zero. A
// zero tail is the unused end of a segment, and only bytes that were written
// are a dropped partial entry.
func nonZeroExtentFrom(f *os.File, from, size int64) (int64, error) {
	chunk := make([]byte, 1<<20)
	var lastNonZero int64 = -1
	for offset := from; offset < size; {
		n := min(int64(len(chunk)), size-offset)
		if _, err := f.ReadAt(chunk[:n], offset); err != nil {
			return 0, fmt.Errorf("reading offset %d: %w", offset, err)
		}
		for i := n - 1; i >= 0; i-- {
			if chunk[i] != 0 {
				lastNonZero = offset + i
				break
			}
		}
		offset += n
	}
	if lastNonZero < 0 {
		return 0, nil
	}
	return lastNonZero + 1 - from, nil
}

// removeStateFiles removes the state file of builds before format 2, and a
// leftover of its temporary, then fsyncs the directory. The open calls it after
// the new v2 segment's name is durable, so a build from before format 2 keeps
// its state file for as long as the log holds no v2 segment it would refuse
// (spec §2.2.1, the upgrade row).
func (w *WAL) removeStateFiles() error {
	removed := false
	for _, name := range []string{walStateFileName, walStateFileName + ".tmp"} {
		err := w.sys.remove(filepath.Join(w.dir, name))
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return fmt.Errorf("removing %s from %s, which this build no longer keeps: %w", name, w.dir, err)
		default:
			removed = true
		}
	}
	if !removed {
		return nil
	}
	if err := w.sys.syncDir(w.dir); err != nil {
		return fmt.Errorf("making the removal of %s from %s durable: %w", walStateFileName, w.dir, err)
	}
	return nil
}

// bornSegment is a segment prepareSegment has born under a spare name: its
// file open for writing (under its segment name, see fileUnderName), with its
// format and, sealed, its file key's cipher. Until nameSegment renames it, it is
// not a segment, and a crash leaves only a spare the next open deletes or
// validates.
type bornSegment struct {
	file   *os.File
	index  int
	format walSegmentFormat
	aead   cipher.AEAD
	// bornAs is the spare name the file has on disk until nameSegment:
	// wal_spare.tmp for a growing segment, wal_spare.ready for a preallocated
	// one.
	bornAs string
	// preallocated is true for a segment born a whole spare of zeros.
	preallocated bool
}

// prepareSegment does every part of segment idx's birth that can fail without
// changing the log (a rotation's R0): a preallocating log takes a spare
// (prepareSpareSegment), any other births a growing one (prepareGrowingSegment).
// waited is whether it held mu for a fill: one in flight, or its own.
func (w *WAL) prepareSegment(idx int) (seg bornSegment, waited bool, err error) {
	if w.preallocate {
		return w.prepareSpareSegment(idx)
	}
	seg, err = w.prepareGrowingSegment(idx)
	return seg, false, err
}

// prepareGrowingSegment is the off mode's birth (spec §2.1, owner D9, B1, B4
// and B6): under walSpareTmpName, it writes the full v2 header in this open's
// key mode and epoch and fsyncs it, derives the file key, and opens the file
// for writing. A failure removes the wal_spare.tmp, so it holds no disk, and
// leaves nothing else changed: a rotation that cannot prepare its next segment
// refuses the entry and keeps the current one (BUG-0716).
func (w *WAL) prepareGrowingSegment(idx int) (bornSegment, error) {
	name := segmentFileName(idx)
	tmpPath := filepath.Join(w.dir, walSpareTmpName)
	// A leftover is a birth a crash interrupted: never a segment, never
	// finished, and in the way of this one.
	if err := w.sys.remove(tmpPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return bornSegment{}, fmt.Errorf("removing a leftover %s before WAL segment %s is born: %w", walSpareTmpName, name, err)
	}

	var epochID uint64
	if w.keys != nil {
		epochID = w.keys.CurrentEpoch()
	}
	format := newWALv2Format(w.keys != nil, epochID)
	var aead cipher.AEAD
	if format.sealed {
		var err error
		aead, err = walEntryAEAD(w.keys, format.epochID, idx)
		if err != nil {
			return bornSegment{}, fmt.Errorf("WAL segment %s: %w", name, err)
		}
	}

	file, err := w.writeSpareHeader(tmpPath, format, name)
	if err == nil {
		file, err = fileUnderName(file, segmentFilePath(w.dir, idx))
	}
	if err != nil {
		// The file is not a segment yet; leaving it would only hold disk.
		if removeErr := w.sys.remove(tmpPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("removing %s after the failed birth: %w", walSpareTmpName, removeErr))
		}
		return bornSegment{}, err
	}
	return bornSegment{file: file, index: idx, format: format, aead: aead, bornAs: walSpareTmpName}, nil
}

// nameSegment renames a prepared segment from its spare name to its segment
// name. Only a rename that the caller then makes durable with a directory fsync
// (syncDir) may be followed by a write under that name (BUG-0719). A refused
// rename removes the spare; the caller still owns seg.file.
func (w *WAL) nameSegment(seg bornSegment) error {
	name := segmentFileName(seg.index)
	tmpPath := filepath.Join(w.dir, seg.bornAs)
	if err := w.sys.rename(tmpPath, segmentFilePath(w.dir, seg.index)); err != nil {
		err = fmt.Errorf("naming WAL segment %s: %w", name, err)
		if errors.Is(err, fs.ErrExist) {
			// A segment under that name may hold entries, and a birth never
			// replaces one, whatever index it was asked for.
			err = fmt.Errorf("WAL segment %s already exists in %s and a new segment never replaces one; this data directory is not one this build wrote as it is: start the node with a new data directory: %w",
				name, w.dir, err)
		}
		if removeErr := w.sys.remove(tmpPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("removing %s after the refused rename: %w", seg.bornAs, removeErr))
		}
		return err
	}
	return nil
}

// birthSegment creates segment idx the way the open does, with no segment to
// retire first: prepareSegment, then the rename to its segment name and the
// directory fsync. A pre-change build scans the highest wal_N.log, and must find
// a header there that refuses it, never a headerless file it would read as an
// empty plain segment. The directory fsync is what makes the name durable before
// any entry is written under it and acknowledged (BUG-0719).
func (w *WAL) birthSegment(idx int) (bornSegment, error) {
	seg, _, err := w.prepareSegment(idx)
	if err != nil {
		return bornSegment{}, err
	}
	if err := w.nameDurably(seg); err != nil {
		return bornSegment{}, errors.Join(err, seg.file.Close())
	}
	return seg, nil
}

// nameDurably renames a prepared segment to its segment name and fsyncs the
// directory (R3's rename and R5). The caller still owns seg.file.
func (w *WAL) nameDurably(seg bornSegment) error {
	if err := w.nameSegment(seg); err != nil {
		return err
	}
	if err := w.sys.syncDir(w.dir); err != nil {
		return fmt.Errorf("making the name of WAL segment %s durable: %w", segmentFileName(seg.index), err)
	}
	return nil
}

// writeSpareHeader creates path, writes format's header at offset 0 and
// fsyncs it, so the header is durable before the file can be given a segment
// name: an entry whose header never reached disk is an entry nothing can name
// a generation or an epoch for, and on a sealed segment the header is also the
// first 24 bytes of that entry's AAD. It returns the file still open for
// writing. segmentName is for the errors.
func (w *WAL) writeSpareHeader(path string, format walSegmentFormat, segmentName string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("creating %s for WAL segment %s: %w", walSpareTmpName, segmentName, err)
	}
	if _, err := w.sys.writeAt(f, format.header, 0); err != nil {
		return nil, errors.Join(fmt.Errorf("writing the header of WAL segment %s: %w", segmentName, err), f.Close())
	}
	if err := w.sys.fsync(f); err != nil {
		return nil, errors.Join(fmt.Errorf("fsyncing the header of WAL segment %s: %w", segmentName, err), f.Close())
	}
	return f, nil
}

// startOpenSegment is the open's step 7 and 8: it births segment idx, makes it
// the active one and then removes the state file of builds before format 2.
// When the segment cannot be prepared (R0: most often no room for it) the open
// still succeeds, with no active segment and writes refused until the next
// write activates one (spec §3.4 step 7, §4.3): a node that cannot write can
// still serve reads and say why in HEALTH_CHECK. The state file stays until a
// segment is named, for the pre-change build it is kept for. A failure after
// R0 fails the open: the segment may have a name a crash can lose.
func (w *WAL) startOpenSegment(idx int) error {
	seg, _, err := w.prepareSegment(idx)
	if err != nil {
		w.activeIndex = idx - 1
		refusal := w.refuseSegment(err, true)
		slog.Warn("the write-ahead log opened without an active segment; writes are refused until it has one",
			"dir", w.dir, "error", refusal)
		return nil
	}
	if err := w.nameDurably(seg); err != nil {
		return errors.Join(err, seg.file.Close())
	}
	w.install(seg)
	if err := w.removeStateFiles(); err != nil {
		return errors.Join(err, w.activeFile.Close())
	}
	return nil
}

// activate gives a log with no active segment one, at the next write under mu
// (spec §4.3, P9): a rotation with nothing to retire, so R0, R3's rename, R5
// and R6. R0's failure refuses the write and keeps the log without a segment;
// a failure after it poisons, as in a rotation. The state file an open without
// a segment kept is removed once the segment is named; a failure to remove it
// is logged and not returned, since the entry this write makes is safe either
// way and the file is read by no build that can open this log.
func (w *WAL) activate() error {
	next, _, err := w.prepareSegment(w.activeIndex + 1)
	if err != nil {
		return w.refuseSegment(err, true)
	}
	if err := w.nameDurably(next); err != nil {
		return w.poisonRotation(next, err)
	}
	w.install(next)
	w.rotationRefusal.Store(nil)
	if err := w.removeStateFiles(); err != nil {
		slog.Warn("could not remove the write-ahead log state file, which this release does not use", "dir", w.dir, "error", err)
	}
	return nil
}

// install makes a named, durable segment the active one (a rotation's R6). It
// cannot fail: the file was opened and its key derived when it was prepared. A
// new segment has a new file key, so the seal counter that bounds it restarts.
// A segment that was the spare leaves the spare idle, and the filler is woken
// to make the next one.
func (w *WAL) install(seg bornSegment) {
	w.activeFile = seg.file
	w.activeIndex = seg.index
	w.activeFormat = seg.format
	w.sealAEAD = seg.aead
	w.sealCounter = 0
	w.writeOff = seg.format.entryStart()
	w.activePreallocated = seg.preallocated
	w.activeDataFlush.Store(seg.preallocated)
	if seg.preallocated {
		w.setSpareIdle()
		w.wakeFiller()
	}
}

// walRotationRetryInterval is how long a rotation that could not prepare its
// next segment is not tried again: appends in between are refused at once with
// the cached error instead of each repeating a birth that just failed.
const walRotationRetryInterval = 10 * time.Second

// walRotationRefusal is the error R0 failed with, and when the next attempt is
// allowed. standing is set when the log has no active segment: it refuses
// every write until one is activated, so WriteRefusal reports it past retryAt
// too, where a full current segment would take the next write that fits.
type walRotationRefusal struct {
	err      error
	retryAt  time.Time
	standing bool
}

// refuseSegment records cause, R0's failure, as the log's refusal and returns
// the error the write is refused with (spec §4.3). The log is not poisoned:
// nothing was changed, and a write after walRotationRetryInterval tries again.
// standing is whether the log is left with no active segment.
func (w *WAL) refuseSegment(cause error, standing bool) error {
	err := w.segmentRefusal(cause)
	w.rotationRefusal.Store(&walRotationRefusal{err: err, retryAt: w.sys.now().Add(walRotationRetryInterval), standing: standing})
	return err
}

// segmentRefusal is the text of R0's failure (spec §4.3), which an operator
// reads in the server log and HEALTH_CHECK. A preallocating log out of room
// names the room to free and the key that stops reserving it; anything else,
// and every failure of a log that does not preallocate, is a segment that
// could not be created, where the key is no help.
func (w *WAL) segmentRefusal(cause error) error {
	if w.preallocate && errors.Is(cause, syscall.ENOSPC) {
		return fmt.Errorf("the write-ahead log in %s has no room for its next %s segment: %w. Writes are refused until there is.\n"+
			"Free at least %s on the filesystem that holds %s (run: df -h %s), and writes resume on their own within 10 s;\n"+
			"or set storage.wal_preallocate: off (HOARDDB_WAL_PREALLOCATE=off) and restart to stop reserving space ahead of the log",
			w.dir, byteSize(w.spareBytes), cause, byteSize(2*w.spareBytes), w.dir, w.dir)
	}
	return fmt.Errorf("the write-ahead log in %s could not start its next segment: %w. Writes are refused until it can.\n"+
		"Check that %s is writable and has free space (run: df -h %s); writes resume on their own within 10 s",
		w.dir, cause, w.dir, w.dir)
}

// byteSize writes a segment size the way an operator reads one: 64 MiB.
func byteSize(n int64) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// cachedRotationRefusal returns the rotation's refusal while the next attempt
// is not yet allowed, or nil. It takes no lock, so WriteRefusal does not wait
// behind a rotation holding mu.
func (w *WAL) cachedRotationRefusal() error {
	refusal := w.rotationRefusal.Load()
	if refusal == nil || !w.sys.now().Before(refusal.retryAt) {
		return nil
	}
	return refusal.err
}

// WriteRefusal returns the error appends are refused with, or nil: the log
// was poisoned and needs a restart; its directory was removed or replaced
// under it; it has no active segment; or a rotation could not prepare the next
// segment less than walRotationRetryInterval ago. The server reports it in
// HEALTH_CHECK and /ready, so a node that refuses every write is not reported
// ready. It takes no lock.
func (w *WAL) WriteRefusal() error {
	if poisoned := w.poisoned.Load(); poisoned != nil {
		return fmt.Errorf("the write-ahead log refuses writes: %w", poisonedError(poisoned.cause))
	}
	if err := w.refusedNow(); err != nil {
		return err
	}
	if refusal := w.rotationRefusal.Load(); refusal != nil && refusal.standing {
		return fmt.Errorf("the write-ahead log refuses writes: %w", refusal.err)
	}
	return nil
}

// refusedNow is what refuses a write before it takes mu: a log directory that
// is gone, or R0's refusal while the next attempt is not yet allowed. A
// standing refusal past that time is not one: the write is what tries again.
func (w *WAL) refusedNow() error {
	if gone := w.dirGuard.check(); gone != nil {
		return fmt.Errorf("the write-ahead log refuses writes: %w", gone)
	}
	if refused := w.cachedRotationRefusal(); refused != nil {
		return fmt.Errorf("the write-ahead log refuses writes: %w", refused)
	}
	return nil
}

// ErrLogClosed is what SyncThrough reports for a log that was closed after the caller's entry was
// appended. Close fsyncs the active file, so the entry is durable unless that fsync failed, but the
// flush this caller asked for was not made: the caller cannot say the entry is lost.
var ErrLogClosed = errors.New("the log is closed")

// Append writes a new entry to the WAL with the next sequence number and
// waits for it to be durably fsynced before returning. See waitForSync for
// how the fsync itself is shared across concurrent callers.
//
// Append writes an entry with no origin block -- byte for byte what the log has
// always held. Production callers use AppendFrom so that every entry says where
// it came from.
func (w *WAL) Append(bucket string, op byte, key []byte, data []byte) (uint64, error) {
	return w.appendEntry(bucket, op, key, data, WALOrigin{})
}

// AppendFrom is Append with an origin. A node that has written to its log with
// this cannot go back to a build that predates the origin block: that build
// reads a flagged entry as a torn tail, truncates it and reuses its sequence
// number.
func (w *WAL) AppendFrom(bucket string, op byte, key, data []byte, origin WALOrigin) (uint64, error) {
	if err := origin.validate(); err != nil {
		return 0, fmt.Errorf("appending to bucket %q: %w", bucket, err)
	}
	return w.appendEntry(bucket, op, key, data, origin)
}

func (w *WAL) appendEntry(bucket string, op byte, key, data []byte, origin WALOrigin) (uint64, error) {
	if err := w.poisonedErr(); err != nil {
		return 0, err
	}
	if err := w.refusedNow(); err != nil {
		return 0, err
	}

	seq, file, err := w.writeLocked(bucket, op, key, data, origin)
	if err != nil {
		return 0, err
	}
	defer w.syncWG.Done()

	if hook := w.beforeSyncHook.Load(); hook != nil {
		(*hook)(seq)
	}
	if err := w.waitForSync(seq, file); err != nil {
		return 0, fmt.Errorf("fsyncing WAL: %w", err)
	}
	return seq, nil
}

// AppendUnsyncedFrom is AppendFrom without the wait for the fsync: the entry is
// written and given its sequence, and is durable only once SyncThrough has
// returned for that sequence or a later one. A caller must neither apply nor
// acknowledge the entry before then. It exists so that one caller appending
// several entries in a row pays one fsync for all of them, as concurrent
// callers of AppendFrom already share one.
func (w *WAL) AppendUnsyncedFrom(bucket string, op byte, key, data []byte, origin WALOrigin) (uint64, error) {
	if err := origin.validate(); err != nil {
		return 0, fmt.Errorf("appending to bucket %q: %w", bucket, err)
	}
	if err := w.poisonedErr(); err != nil {
		return 0, err
	}
	// As Append: the entry is acknowledged once SyncThrough returns, and
	// SyncThrough would flush a segment whose directory is gone.
	if err := w.refusedNow(); err != nil {
		return 0, err
	}
	seq, _, err := w.writeLocked(bucket, op, key, data, origin)
	if err != nil {
		return 0, err
	}
	// Nothing waits on this file for the entry yet: SyncThrough counts itself
	// in again, under mu, for the file active when it runs.
	w.syncWG.Done()
	return seq, nil
}

// SyncThrough returns once every entry at or below seq is durable, sharing the
// fsync with whichever appends wait at the same time (waitForSync). The file it
// syncs is the one active now: an entry written to an earlier file was synced
// when rotate closed that file.
func (w *WAL) SyncThrough(seq uint64) error {
	w.mu.Lock()
	// Checked under mu: a call that waited on mu while a rotation poisoned the
	// log must get the poison, not the nil file R3 left (S6 verifier F2).
	if err := w.poisonedErr(); err != nil {
		w.mu.Unlock()
		return err
	}
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("fsyncing WAL through sequence %d: %w", seq, ErrLogClosed)
	}
	if w.activeFile == nil {
		// No active segment: everything written is synced (the open synced
		// it), so only a sequence above that needs a segment to flush.
		if seq <= w.syncedSequence() {
			w.mu.Unlock()
			return nil
		}
		// Nothing is written and nothing changes while seq is at or below the synced sequence. Only a
		// sequence above it tries to activate a segment, which is the one state change this branch makes.
		err := w.cachedRotationRefusal()
		if err == nil {
			err = w.activate()
		}
		if err != nil {
			w.mu.Unlock()
			return fmt.Errorf("fsyncing WAL through sequence %d: %w", seq, err)
		}
	}
	file := w.activeFile
	// Counted under mu, as writeLocked counts an append, so rotate and Close
	// wait for this fsync before they sync and close the file.
	w.syncWG.Add(1)
	w.mu.Unlock()
	defer w.syncWG.Done()

	if hook := w.beforeSyncHook.Load(); hook != nil {
		(*hook)(seq)
	}
	if err := w.waitForSync(seq, file); err != nil {
		return fmt.Errorf("fsyncing WAL: %w", err)
	}
	return nil
}

// writeLocked encodes one entry and writes it to the active file under mu,
// exactly as Append always has — sequence numbers and file position stay
// correct because this part is still fully serialized. It stops short of the
// fsync: that is Append's job, via waitForSync, once mu is released.
func (w *WAL) writeLocked(bucket string, op byte, key []byte, data []byte, origin WALOrigin) (uint64, *os.File, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Checked again under mu: an append that passed its callers' checks can
	// wait here while a rotation poisons the log or is refused.
	if err := w.poisonedErr(); err != nil {
		return 0, nil, err
	}
	if err := w.cachedRotationRefusal(); err != nil {
		return 0, nil, err
	}
	if w.closed {
		return 0, nil, fmt.Errorf("writing WAL entry for bucket %q: the log is closed", bucket)
	}
	if w.activeFile == nil {
		if err := w.activate(); err != nil {
			return 0, nil, fmt.Errorf("writing WAL entry for bucket %q: %w", bucket, err)
		}
	}

	// Encode the entry body.
	bucketBytes := []byte(bucket)
	bucketLen := uint32(len(bucketBytes))
	keyLen := uint32(len(key))

	// Body layout (everything after the 8-byte header):
	// 8 (seq) + 1 (op) + 4 (bucket len) + len(bucket) + 4 (key len) + len(key)
	// + [origin block] + len(data)
	bodyLen := 8 + 1 + 4 + len(bucketBytes) + 4 + len(key) + origin.encodedLen() + len(data)
	if bodyLen > walMaxEntrySize {
		// Refusing here is the only honest option: a body this large could not
		// be read back, and accepting it would be silent data loss. The
		// sequence is left untouched — a refused entry never existed.
		return 0, nil, fmt.Errorf("WAL entry of %d bytes exceeds the %d-byte limit (bucket %q, key %q): write the payload in smaller entries",
			bodyLen, walMaxEntrySize, bucket, key)
	}

	// The sequence field is filled in once the rotation is decided: nothing
	// else in the body depends on it.
	body := make([]byte, walSeqFieldLen, bodyLen)

	// Write operation type.
	body = append(body, op)

	// Write bucket length + bucket name. The origin flag rides in the length
	// word's top bit, which a bucket length never uses.
	if origin.Kind != WALOriginLegacy {
		bucketLen |= walOriginFlag
	}
	bucketLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(bucketLenBuf, bucketLen)
	body = append(body, bucketLenBuf...)
	body = append(body, bucketBytes...)

	// Write key length + key.
	keyLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLenBuf, keyLen)
	body = append(body, keyLenBuf...)
	body = append(body, key...)

	// Write the origin block, inside the body the checksum covers.
	body = origin.appendTo(body)

	// Write data.
	body = append(body, data...)

	// A sealed entry trades the body's 8-byte sequence for 12 nonce bytes, an
	// 8-byte clear sequence and a 16-byte tag, so it is 36 bytes longer than
	// its body either way: 8 + 8 + 12 + (bodyLen - 8) + 16.
	entryLen := int64(8 + bodyLen)
	if w.activeFormat.sealed {
		entryLen = int64(bodyLen + 8 + walNonceLen + walTagLen)
	}

	// The size check and the seal-counter check share this one rotation, and
	// the entry is framed and written at the offset the decision was made on.
	if w.writeOff+entryLen > w.maxFileSize || (w.keys != nil && w.sealCounter >= sealLimit) {
		if err := w.rotate(); err != nil {
			return 0, nil, fmt.Errorf("rotating WAL file: %w", err)
		}
	}

	// The sequence is drawn only now, after the rotation, and committed only
	// after the write: no error return above or below can give a sequence away
	// that no entry holds (BUG-0716). One rule here replaces an undo on each
	// return, which is exactly what a new return forgets.
	seq := w.lastSeq + 1
	binary.BigEndian.PutUint64(body[:walSeqFieldLen], seq)

	entry, err := w.frameEntry(seq, body, w.writeOff)
	if err != nil {
		return 0, nil, err
	}

	if w.activePreallocated && w.writeOff+int64(len(entry)) > w.spareBytes {
		// An entry larger than a whole segment, written to a fresh one,
		// extends the file past its preallocation. Its size changes, so from
		// here until it is retired the segment is flushed with fsync: the
		// data-only flush is kept for files whose size never changed (spec
		// §4.1). Stored before writtenSeq, so a leader that covers this entry
		// sees it.
		w.activeDataFlush.Store(false)
	}
	if _, err := w.sys.writeAt(w.activeFile, entry, w.writeOff); err != nil {
		err = fmt.Errorf("writing WAL entry: %w", err)
		if w.activePreallocated {
			// A failed overwrite of a preallocated block means a failing
			// device, and a cut would shrink a file whose flush relies on its
			// size never changing. The next open retires the tail at its last
			// whole entry.
			return 0, nil, w.poison(err)
		}
		return 0, nil, w.rollbackPartialWrite(err)
	}
	w.lastSeq = seq
	w.writeOff += int64(len(entry))
	// Counted with the write it describes, under mu: a failed write returned
	// above and was never added, so the counter is the bytes on disk.
	w.counters.bytesAppended.Add(uint64(len(entry)))
	if w.activeFormat.sealed {
		w.sealCounter++
	}
	w.writtenSeq.Store(seq)

	// Counted before mu is released: rotate and Close also run under mu, and
	// must not Sync+Close this file believing nothing is waiting on it when
	// this call is about to (see the syncWG field doc).
	w.syncWG.Add(1)

	return seq, w.activeFile, nil
}

// frameEntry turns one encoded body into the bytes that go on disk, in the
// active segment's format.
//
// On a plain segment that is the 8-byte length+CRC word followed by the body,
// unchanged. On a sealed one the body's leading sequence number moves out into
// the clear framing (where the scan, the open's retireTail and FirstSequence
// can all read it without a key) and everything after it is sealed against the file
// header, that sequence and this entry's offset. In format 2 the CRC also
// covers the segment's generation, and on a sealed segment the clear sequence.
func (w *WAL) frameEntry(seq uint64, body []byte, startOffset int64) ([]byte, error) {
	format := w.activeFormat
	if !format.sealed {
		entry := make([]byte, 0, 8+len(body))
		entry = append(entry, frameWord(len(body), format.checksum(nil, body))...)
		return append(entry, body...), nil
	}

	// Sealing against the wrong file header or a stale key would produce a
	// segment nothing can read back, so the invariant is checked rather than
	// assumed: OpenWAL and rotate both establish it before any write.
	if w.sealAEAD == nil || len(format.header) != walFileHeaderLen {
		return nil, fmt.Errorf("sealing WAL entry %d: the active segment has no file key", seq)
	}

	nonce := make([]byte, walNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generating a nonce for WAL entry %d: %w", seq, err)
	}
	// Seal appends to its first argument, so onDisk is nonce || ciphertext || tag.
	onDisk := w.sealAEAD.Seal(nonce, nonce, body[walSeqFieldLen:], walEntryAAD(format.header, seq, startOffset))

	seqField := binary.BigEndian.AppendUint64(make([]byte, 0, walSeqFieldLen), seq)
	entry := make([]byte, 0, 8+walSeqFieldLen+len(onDisk))
	entry = append(entry, frameWord(len(onDisk), format.checksum(seqField, onDisk))...)
	entry = append(entry, seqField...)
	return append(entry, onDisk...), nil
}

// waitForSync blocks until seq is durably fsynced on file, sharing that
// fsync with whichever other Append calls are waiting at the same time
// ("group commit"): the first goroutine to arrive becomes this round's
// leader and flushes the file; everyone else waits for the result instead
// of calling Sync() themselves. This is exactly as durable as fsyncing
// individually — nobody returns before their own entry's fsync has
// succeeded — it just means N concurrent writers share the cost of one
// fsync instead of paying for N.
//
// The leader marks synced every entry written before its fsync began
// (writtenSeq, read before the call), not only its own: those entries are in
// the file the fsync covers, and a waiter with one of them must not lead a
// second fsync for it (BUG-0712). An entry written once the fsync has begun
// may have missed it, so the mark never reaches past what was read first.
//
// A single growing sequence counter (syncedSeq) is enough to track this
// safely even across a file rotation. A rotation runs under mu, strictly
// before this runs for any entry of the next segment: it flushes the outgoing
// segment (R1) before it closes it and names the next one (R3), makes that name
// durable (R5) before anything is written under it, and poisons the log if any
// of R1 to R5 fails, so no later flush is ever reported. No rotation can pass
// R1 while this call is counted in syncWG. So every entry at or below
// writtenSeq is either in file or in a segment a rotation already flushed, and
// for any seq V whose flush here succeeds, every smaller seq is durable too.
// That holds by induction no matter how many rotations happened in between.
func (w *WAL) waitForSync(seq uint64, file *os.File) error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	for seq > w.syncedSeq {
		// A waiter woken by a failed fsync must not run one of its own: an
		// fsync after a failed one can report success for pages the kernel
		// already dropped, which would acknowledge a write that is not on disk.
		if w.syncErr != nil {
			return w.syncErr
		}
		if w.syncing {
			w.syncCond.Wait()
			continue
		}

		w.syncing = true
		// seq can exceed writtenSeq only when SyncThrough is asked for a
		// sequence not yet written; marking seq synced then is what the log
		// has always done for that call.
		coveredSeq := max(w.writtenSeq.Load(), seq)
		// Chosen after writtenSeq is read: an entry this flush covers that
		// grew the segment stored the change before it stored writtenSeq.
		flush := w.segmentFlush()
		w.syncMu.Unlock()
		if hook := w.fsyncHook.Load(); hook != nil {
			(*hook)()
		}
		flushStarted := time.Now()
		err := flush(file)
		w.counters.observeFlush(time.Since(flushStarted))
		w.syncMu.Lock()
		w.syncing = false
		w.syncCond.Broadcast()

		if err != nil {
			// A failed fsync leaves no reliable way to know which of this
			// round's writes actually reached disk, and other goroutines may
			// have appended further entries to the file while this fsync was
			// in flight — truncating live here could discard writes a later
			// fsync would have confirmed. Poison the log instead: every
			// future Append fails fast rather than building on an uncertain
			// tail, and the next open retires the on-disk tail at its last
			// whole entry (retireTail) when the process starts again.
			w.setSyncErrLocked(err)
			return err
		}
		// Linux reports an fsync error once, to one fsync of the file. If another fsync of it (a
		// failed one elsewhere in the log) poisoned the log while this one ran, this nil is not
		// evidence that the covered entries are on disk (BUG-0807).
		if w.syncErr != nil {
			return w.syncErr
		}
		w.syncedSeq = coveredSeq
	}
	return nil
}

// segmentFlush is the flush of the active segment's entries (spec §4.1): the
// data-only one on a preallocated segment whose size never changed, since
// every block it has was written and fsynced when it was born, and a
// whole-file fsync on a growing or grown one, whose size is part of what must
// be durable.
func (w *WAL) segmentFlush() func(*os.File) error {
	if w.activeDataFlush.Load() {
		return w.sys.flushData
	}
	return w.sys.flush
}

// poison makes cause the failure every later write and SyncThrough reports
// (the first one wins), and returns it as they will see it.
func (w *WAL) poison(cause error) error {
	w.syncMu.Lock()
	if w.syncErr == nil {
		w.setSyncErrLocked(cause)
	}
	w.syncMu.Unlock()
	return w.poisonedErr()
}

// walPoison is the failure that poisoned the log, as WriteRefusal reads it.
type walPoison struct{ cause error }

// setSyncErrLocked poisons the log with cause, under syncMu: syncErr for the
// writers, which check it under that lock, and its copy WriteRefusal reads
// without one.
func (w *WAL) setSyncErrLocked(cause error) {
	w.syncErr = cause
	w.poisoned.Store(&walPoison{cause: cause})
}

// poisonedErr reports the fsync failure that poisoned the WAL, if any.
func (w *WAL) poisonedErr() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	if w.syncErr != nil {
		return poisonedError(w.syncErr)
	}
	return nil
}

// poisonedError is how a poisoned log refuses: the failure, and the restart
// whose open repairs the log.
func poisonedError(cause error) error {
	return fmt.Errorf("WAL is unusable after a previous fsync failure; restart the node so recovery can repair the log: %w", cause)
}

// syncedSequence is the highest sequence known durable.
func (w *WAL) syncedSequence() uint64 {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	return w.syncedSeq
}

// rollbackPartialWrite truncates the active file back to the write offset,
// where the failed entry started, so a failed append leaves no torn bytes behind
// it: a partial write longer than the next entry would otherwise leave the tail
// of the failed one after it. Nothing to undo in memory: writeLocked commits the
// sequence and the offset only after a write succeeds. It returns the original
// error, joined with a description of the rollback when the rollback itself
// failed: a log that could not be rolled back is damaged and the caller must not
// keep appending.
func (w *WAL) rollbackPartialWrite(cause error) error {
	if err := w.sys.ftruncate(w.activeFile, w.writeOff); err != nil {
		return errors.Join(cause, fmt.Errorf(
			"rolling back a partially written WAL entry: %w — the log has a torn entry at offset %d and must be repaired before further writes",
			err, w.writeOff))
	}
	return cause
}

// ReadFrom reads all WAL entries with sequence numbers >= seq. It scans
// through WAL segment files starting from the one that may contain the
// target sequence.
func (w *WAL) ReadFrom(seq uint64) ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var result []WALEntry
	segments := w.listSegments()

	// prevSeq is the last sequence of the previous non-empty segment: a v2
	// segment's first entry must rise above it (spec §3.3 rule 5), and an empty
	// segment between them does not reset it.
	var prevSeq uint64
	for i, seg := range segments {
		segPath := filepath.Join(w.dir, seg.name)
		f, err := os.Open(segPath)
		if err != nil {
			return nil, fmt.Errorf("opening WAL segment %s: %w", seg.name, err)
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("stat WAL segment %s: %w", seg.name, err)
		}
		size := info.Size()
		if limit := w.entryLimitLocked(seg.index); limit >= 0 && limit < size {
			size = limit
		}
		scanned, err := w.scanSegmentFile(f, size, seg.index, prevSeq)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("reading WAL segment %s: %w", seg.name, err)
		}

		// Returning nothing for a segment whose bodies were never opened would
		// report a hole in the log as an empty stretch of it, which is the one
		// answer replay must never get.
		if scanned.sealedWithoutKey && scanned.entryCount > 0 {
			return nil, fmt.Errorf(
				"WAL segment %s is sealed under epoch key material this node was not given, so its %d entries cannot be read: start the node with the encryption keystore that wrote them, or restore the log from a replica",
				seg.name, scanned.entryCount)
		}

		// Open repairs a torn tail in the segment it leaves active. Damage in
		// any earlier segment is a hole in the middle of the log: the entries
		// after it cannot be trusted to be a complete history, so this is an
		// error for a human rather than something to quietly skip past.
		if scanned.torn && i != len(segments)-1 {
			return nil, fmt.Errorf(
				"WAL segment %s is damaged from offset %d (%s) and is not the active segment: entries after the damage cannot be trusted. Remove that segment and any later ones, or restore the log from a replica",
				seg.name, scanned.validEnd, scanned.reason)
		}

		for _, e := range scanned.entries {
			if e.Seq >= seq {
				result = append(result, e)
			}
		}
		if scanned.entryCount > 0 {
			prevSeq = scanned.lastSeq
		}
	}

	return result, nil
}

// entryLimitLocked is the number of a segment's bytes a reader may look at: the
// write offset for the active segment, and -1 (the whole file) for any other,
// which ends at its last entry. The caller holds mu.
func (w *WAL) entryLimitLocked(segmentIndex int) int64 {
	if w.activeFile != nil && segmentIndex == w.activeIndex {
		return w.writeOff
	}
	return -1
}

// segmentRef is a WAL segment file on disk and its numeric index.
type segmentRef struct {
	name  string
	index int
}

// listSegments returns the WAL segment files in index order.
func (w *WAL) listSegments() []segmentRef {
	dirEntries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil
	}
	var segments []segmentRef
	for _, e := range dirEntries {
		if e.IsDir() || !isWALSegmentFile(e.Name()) {
			continue
		}
		idx, err := parseWALSegmentIndex(e.Name())
		if err != nil {
			continue
		}
		segments = append(segments, segmentRef{name: e.Name(), index: idx})
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].index < segments[j].index
	})
	return segments
}

// SegmentBytes is the total size of the WAL's segment files on disk, with the
// spare: a ready one and one being filled are disk the log holds as much as a
// segment is. It does not reuse listSegments, which returns nothing on a
// ReadDir error and would read as 0 bytes: an unreadable directory is an error
// here. It takes no lock and reads only directory metadata, so the sum is
// approximate by nature; a file removed or truncated between the listing and
// its stat has no bytes left and is skipped.
func (w *WAL) SegmentBytes() (int64, error) {
	dirEntries, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, fmt.Errorf("listing the WAL directory %s: %w", w.dir, err)
	}
	var total int64
	for _, e := range dirEntries {
		if e.IsDir() || !holdsLogDisk(e.Name()) {
			continue
		}
		segmentInfo, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("reading the size of WAL segment %s: %w", e.Name(), err)
		}
		total += segmentInfo.Size()
	}
	return total, nil
}

// scanSegmentFile decodes every valid entry in an open segment and reports
// where validity ends. It never allocates from an unvalidated length: the
// length is checked against the bytes actually remaining in the file before
// anything is read into a buffer, so a garbage or torn prefix cannot make the
// reader panic.
//
// A damaged tail is reported, not fatal. It is what a crash or a full disk
// leaves behind, and refusing to read the entries before it would make the
// node unrecoverable — the failure mode this exists to end.
//
// The segment's first bytes decide its format, never the key source, so a
// keyless open still reads a sealed segment's framing and a keyed one still
// reads a plain segment. A v1 segment is scanned by the rules it was written
// under. A v2 segment's entries end at the first of (spec §3.3): too few bytes
// for a frame; a zero word (the zero tail; a zero first word means the segment
// holds no entry, which is not damage); an implausible length; a failed CRC,
// which an entry of another generation fails; a sequence that does not rise
// above the previous entry's, or for the first entry above prevSeq, the last
// sequence of the previous non-empty segment (0 when there is none to compare).
func (w *WAL) scanSegmentFile(f *os.File, size int64, segIdx int, prevSeq uint64) (walScanResult, error) {
	format, err := readSegmentFormat(f, size, segmentFileName(segIdx))
	if err != nil {
		return walScanResult{}, err
	}
	if format.sealed {
		return w.scanSealedSegmentFile(f, size, segIdx, format, prevSeq)
	}

	var res walScanResult
	start := format.entryStart()
	header := make([]byte, 8)
	for offset := start; offset < size; {
		remaining := size - offset

		// A partial header is a torn tail: the length prefix itself was cut.
		if remaining < 8 {
			res.endAt(offset, fmt.Sprintf("%d bytes of an entry header at offset %d", remaining, offset))
			return res, nil
		}
		if _, err := f.ReadAt(header, offset); err != nil {
			return res, fmt.Errorf("reading entry header at offset %d: %w", offset, err)
		}
		word := binary.BigEndian.Uint64(header)
		if format.v2 && word == 0 {
			res.endAtZeroWord(offset, start)
			return res, nil
		}

		bodyLen, checksum, _ := decodeWALHeader(word)
		if bodyLen < walMinEntrySize || bodyLen > walMaxEntrySize {
			res.endAt(offset, fmt.Sprintf("implausible entry length %d at offset %d", bodyLen, offset))
			return res, nil
		}
		if int64(bodyLen) > remaining-8 {
			res.endAt(offset, fmt.Sprintf("entry at offset %d claims %d bytes but only %d remain", offset, bodyLen, remaining-8))
			return res, nil
		}

		body := make([]byte, bodyLen)
		if _, err := f.ReadAt(body, offset+8); err != nil {
			return res, fmt.Errorf("reading entry body at offset %d: %w", offset, err)
		}
		verified := format.checksumVerified(checksum)
		if verified && format.checksum(nil, body) != checksum {
			res.endAt(offset, fmt.Sprintf("entry at offset %d failed its checksum", offset))
			return res, nil
		}

		// For plaintext entries, seq is in the body; extract it.
		bodySeq := binary.BigEndian.Uint64(body[0:8])
		if format.v2 && bodySeq <= prevSeq {
			res.endAt(offset, fmt.Sprintf("entry at offset %d has sequence %d, which does not rise above %d", offset, bodySeq, prevSeq))
			return res, nil
		}
		entry, err := parseWALEntry(body[8:], bodySeq)
		if err != nil {
			// An entry that passed its checksum was written whole, so failing to
			// parse means the writer was a build this one cannot read. Truncating
			// it as a torn tail would give its sequence number to a different
			// write; the sealed scan already refuses the same case.
			if verified {
				return res, fmt.Errorf(
					"WAL segment %s has an entry at offset %d (sequence %d) that passed its checksum and cannot be read: %w",
					segmentFileName(segIdx), offset, bodySeq, err)
			}
			res.endAt(offset, fmt.Sprintf("entry at offset %d is malformed: %v", offset, err))
			return res, nil
		}
		res.entries = append(res.entries, entry)
		res.entryCount++
		res.lastSeq = entry.Seq
		prevSeq = entry.Seq
		offset += 8 + int64(bodyLen)
	}

	res.validEnd = size
	return res, nil
}

// endAt records that the segment's valid entries end at offset, for reason:
// everything from there on is unusable.
func (res *walScanResult) endAt(offset int64, reason string) {
	res.validEnd = offset
	res.torn = true
	res.reason = reason
}

// endAtZeroWord records a v2 zero word at offset. As the segment's first word
// it means the segment holds no entry, which is not damage. After an entry it
// is a zero tail: the end of the log in the tail segment, damage in any other.
func (res *walScanResult) endAtZeroWord(offset, start int64) {
	if offset == start {
		res.validEnd = offset
		return
	}
	res.endAt(offset, fmt.Sprintf("zeros from offset %d", offset))
}

// scanSealedSegmentFile is scanSegmentFile's sealed branch. The framing —
// length, CRC and sequence — is in the clear, so a torn tail is found, reported
// and repaired with no key at all; only the bodies need one.
//
// A CRC that passes and an Open that fails is not a torn tail: nothing a crash
// does produces a body that checksums correctly and refuses to decrypt. That is
// corruption or tampering, and it is returned as an error rather than quietly
// truncated away. In format 2 the CRC covers the generation and the clear
// sequence too, so a stale or torn frame fails it and never reaches the AEAD.
func (w *WAL) scanSealedSegmentFile(f *os.File, size int64, segIdx int, format walSegmentFormat, prevSeq uint64) (walScanResult, error) {
	var res walScanResult

	var aead cipher.AEAD
	if w.keys == nil {
		res.sealedWithoutKey = true
	} else {
		var err error
		aead, err = walEntryAEAD(w.keys, format.epochID, segIdx)
		if err != nil {
			return res, fmt.Errorf("WAL segment %s: %w", segmentFileName(segIdx), err)
		}
	}

	start := format.entryStart()
	framing := make([]byte, 8+walSeqFieldLen)
	for offset := start; offset < size; {
		remaining := size - offset

		if remaining < int64(len(framing)) {
			res.endAt(offset, fmt.Sprintf("%d bytes of an entry header at offset %d", remaining, offset))
			return res, nil
		}
		if _, err := f.ReadAt(framing, offset); err != nil {
			return res, fmt.Errorf("reading entry header at offset %d: %w", offset, err)
		}

		word := binary.BigEndian.Uint64(framing[:8])
		if format.v2 && word == 0 {
			res.endAtZeroWord(offset, start)
			return res, nil
		}
		onDiskLen := int64(uint32(word))
		checksum := uint32(word >> 32)
		seq := binary.BigEndian.Uint64(framing[8:])

		if onDiskLen < walSealedMinOnDisk || onDiskLen > int64(walSealedMaxOnDisk) {
			res.endAt(offset, fmt.Sprintf("implausible entry length %d at offset %d", onDiskLen, offset))
			return res, nil
		}
		if onDiskLen > remaining-int64(len(framing)) {
			res.endAt(offset, fmt.Sprintf("entry at offset %d claims %d bytes but only %d remain",
				offset, onDiskLen, remaining-int64(len(framing))))
			return res, nil
		}

		onDisk := make([]byte, onDiskLen)
		if _, err := f.ReadAt(onDisk, offset+int64(len(framing))); err != nil {
			return res, fmt.Errorf("reading entry body at offset %d: %w", offset, err)
		}
		if format.checksum(framing[8:], onDisk) != checksum {
			res.endAt(offset, fmt.Sprintf("entry at offset %d failed its checksum", offset))
			return res, nil
		}
		if format.v2 && seq <= prevSeq {
			res.endAt(offset, fmt.Sprintf("entry at offset %d has sequence %d, which does not rise above %d", offset, seq, prevSeq))
			return res, nil
		}

		res.entryCount++
		res.lastSeq = seq
		prevSeq = seq

		if aead != nil {
			body, err := aead.Open(nil, onDisk[:walNonceLen], onDisk[walNonceLen:], walEntryAAD(format.header, seq, offset))
			if err != nil {
				return res, fmt.Errorf(
					"WAL segment %s is corrupt: the entry at offset %d (sequence %d, epoch %d) passed its checksum but could not be decrypted, so it has been altered or the wrong key was offered: %w",
					segmentFileName(segIdx), offset, seq, format.epochID, err)
			}
			entry, err := parseWALEntry(body, seq)
			if err != nil {
				return res, fmt.Errorf(
					"WAL segment %s is corrupt: the entry at offset %d (sequence %d, epoch %d) decrypted but is malformed: %w",
					segmentFileName(segIdx), offset, seq, format.epochID, err)
			}
			res.entries = append(res.entries, entry)
		}

		offset += int64(len(framing)) + onDiskLen
	}

	res.validEnd = size
	return res, nil
}

// parseWALEntry parses an entry body that starts at the operation byte:
// op(1), bucketLen(4), bucket, keyLen(4), key, [origin], data. The sequence
// number is never part of buf -- the legacy caller reads it from the first 8
// bytes of the on-disk body and passes the rest, the sealed caller reads it
// from the clear framing -- because a sealed body does not carry it (§3.5).
//
// The origin block is present when bit 31 of bucketLen is set. It sits inside
// buf, so the checksum of a plain entry and the seal of a sealed one cover it.
func parseWALEntry(buf []byte, seq uint64) (WALEntry, error) {
	if len(buf) < walSealedMinBody {
		return WALEntry{}, fmt.Errorf("entry too short: %d bytes", len(buf))
	}

	pos := 0

	// Operation type (1 byte).
	op := buf[pos]
	pos++

	// Bucket length (4 bytes, bit 31 is the origin flag) + bucket name.
	bucketWord := binary.BigEndian.Uint32(buf[pos : pos+4])
	hasOrigin := bucketWord&walOriginFlag != 0
	bucketLen := int(bucketWord &^ walOriginFlag)
	pos += 4
	if pos+bucketLen > len(buf) {
		return WALEntry{}, fmt.Errorf("bucket name extends beyond entry: need %d, have %d", pos+bucketLen, len(buf))
	}
	bucket := string(buf[pos : pos+bucketLen])
	pos += bucketLen

	// Key length (4 bytes) + key.
	if pos+4 > len(buf) {
		return WALEntry{}, fmt.Errorf("key length field extends beyond entry")
	}
	keyLen := int(binary.BigEndian.Uint32(buf[pos : pos+4]))
	pos += 4
	if pos+keyLen > len(buf) {
		return WALEntry{}, fmt.Errorf("key extends beyond entry: need %d, have %d", pos+keyLen, len(buf))
	}
	key := make([]byte, keyLen)
	copy(key, buf[pos:pos+keyLen])
	pos += keyLen

	var origin WALOrigin
	if hasOrigin {
		var err error
		origin, pos, err = parseWALOrigin(buf, pos)
		if err != nil {
			return WALEntry{}, err
		}
	}

	// Data: remaining bytes.
	data := make([]byte, len(buf)-pos)
	copy(data, buf[pos:])

	return WALEntry{
		Seq:    seq,
		Op:     op,
		Bucket: bucket,
		Key:    key,
		Data:   data,
		Origin: origin,
	}, nil
}

// parseWALOrigin reads the origin block that starts at pos and returns the
// position after it. A replica block is kind 3, or kind 2 of an earlier build,
// read with OutcomeNotLogged set. A kind other than own or replica is an error that names
// it: the entry was written by a newer build, and reading on would misplace its
// data. Kind 0 under the flag is corruption, and says so.
func parseWALOrigin(buf []byte, pos int) (WALOrigin, int, error) {
	if pos >= len(buf) {
		return WALOrigin{}, pos, errors.New("origin block extends beyond entry: no kind byte")
	}
	kind := WALOriginKind(buf[pos])
	pos++
	switch kind {
	case WALOriginOwn:
		epoch, width := binary.Uvarint(buf[pos:])
		if width <= 0 {
			return WALOrigin{}, pos, errors.New("origin block extends beyond entry: epoch is not a complete varint")
		}
		return WALOrigin{Kind: WALOriginOwn, Epoch: epoch}, pos + width, nil
	case WALOriginReplica, walOriginReplicaBeforeOutcome:
		if pos >= len(buf) {
			return WALOrigin{}, pos, errors.New("origin block extends beyond entry: no source length")
		}
		sourceLen := int(buf[pos])
		pos++
		if pos+sourceLen+8 > len(buf) {
			return WALOrigin{}, pos, fmt.Errorf("origin block extends beyond entry: need %d, have %d", pos+sourceLen+8, len(buf))
		}
		source := string(buf[pos : pos+sourceLen])
		pos += sourceLen
		seq := binary.BigEndian.Uint64(buf[pos : pos+8])
		pos += 8
		if kind == walOriginReplicaBeforeOutcome {
			return WALOrigin{Kind: WALOriginReplica, Source: source, Seq: seq, OutcomeNotLogged: true}, pos, nil
		}
		if pos >= len(buf) {
			return WALOrigin{}, pos, errors.New("origin block extends beyond entry: no owner outcome")
		}
		outcome := buf[pos]
		pos++
		term, width := binary.Uvarint(buf[pos:])
		if width <= 0 {
			return WALOrigin{}, pos, errors.New("origin block extends beyond entry: term is not a complete varint")
		}
		return WALOrigin{Kind: WALOriginReplica, Source: source, Seq: seq, OwnerOutcome: outcome, Term: term}, pos + width, nil
	case WALOriginLegacy:
		// A flagged origin block is never written with the legacy marker, so this
		// is damage and not a newer writer; the caller's error names the segment,
		// offset and sequence.
		return WALOrigin{}, pos, errors.New("the entry is corrupt: its origin block says kind 0 (legacy), which a writer never flags")
	default:
		return WALOrigin{}, pos, fmt.Errorf("origin kind %d: written by a newer build than this one; upgrade this node to read the log", kind)
	}
}

// FastForward moves the log past through, so the next sequence it gives is
// above it, when the log has not reached it yet; it reports whether it moved.
// It is for a log that went backwards (BUG-0481): a peer that holds a position
// in it above where it ends holds other writes at the sequences the log would
// give next. The sequences passed over are recorded as a skip in the chain
// file, durably, before it returns, so no later open writes them and no reader
// takes their absence for a hole. Everything the log holds is fsynced first: a
// written and unsynced entry lost to a crash after the skip would leave a hole
// below it that nothing would ever fill.
func (w *WAL) FastForward(through uint64) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Checked under mu: a call that waited on mu while a rotation's flush
	// failed must not fsync after that failure and record a skip on a poisoned
	// log (S6 verifier F1).
	if err := w.poisonedErr(); err != nil {
		return false, err
	}
	if through <= w.lastSeq {
		return false, nil
	}
	if err := w.refusedNow(); err != nil {
		return false, err
	}
	if w.closed {
		return false, fmt.Errorf("moving the log past sequence %d: the log is closed", through)
	}
	if w.activeFile == nil {
		if err := w.activate(); err != nil {
			return false, fmt.Errorf("moving the log past sequence %d: %w", through, err)
		}
	}
	// A group-commit leader still flushing this file would race this fsync for the one error Linux
	// reports: the fsync that loses gets nil for pages the kernel dropped. Wait for it, as rotate and
	// Close do; mu is held, so no new SyncThrough or append can start meanwhile, and the leader never
	// takes mu. A flush that failed meanwhile poisoned the log, so look again.
	w.syncWG.Wait()
	if err := w.poisonedErr(); err != nil {
		return false, err
	}
	if err := w.sys.fsync(w.activeFile); err != nil {
		// Poisoned, not just returned: Linux reports an fsync error once, so a later fsync that
		// succeeds cannot show that the entries written before this one are on disk (BUG-0807).
		return false, w.poison(fmt.Errorf("fsyncing the log before moving it past sequence %d: %w", through, err))
	}
	skips := append([]WALSkip(nil), w.skips...)
	if count := len(skips); count > 0 && skips[count-1].Through == w.lastSeq {
		skips[count-1].Through = through
	} else {
		skips = append(skips, WALSkip{After: w.lastSeq, Through: through})
	}
	if err := writeIncarnationChain(w.dir, w.incarnations, skips); err != nil {
		return false, fmt.Errorf("recording that the log in %s passes over sequences %d to %d: %w", w.dir, w.lastSeq+1, through, err)
	}
	w.skips = skips
	w.lastSeq = through
	return true, nil
}

// Skips returns the runs of sequences the log has passed over, oldest first.
func (w *WAL) Skips() []WALSkip {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]WALSkip(nil), w.skips...)
}

// LastSequence returns the last sequence the log has given, or passed over
// (FastForward): the next append is given the one after it.
func (w *WAL) LastSequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeq
}

// FirstSequence returns the sequence number of the first entry still retained
// in the log, or LastSequence()+1 when the log holds no entries.
//
// It is the lowest sequence a caller can still read from the log, which makes
// it the horizon that catch-up checks against: a peer whose acknowledged
// position is below it can no longer be repaired from the log, and advancing
// its acknowledgement past it would silently skip the missing entries. It
// reads only the first segment's header and first entry body, so it is cheap
// enough for the replication hot path.
func (w *WAL) FirstSequence() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seg := range w.listSegments() {
		seq, found, err := w.firstSequenceOf(seg, w.entryLimitLocked(seg.index))
		if err != nil {
			return 0, err
		}
		if found {
			return seq, nil
		}
	}
	return w.lastSeq + 1, nil
}

// firstSequenceOf reads one segment's first sequence number. found is false for
// a segment that holds no entry at all. limit is the number of the segment's
// bytes that hold entries when it is the active segment (the write offset), or
// -1 for a retired segment, which ends at its last entry.
//
// On a sealed segment this is three reads and no decryption: the 24-byte
// header, the entry's length word, and the 8 clear sequence bytes that follow
// it. That is what keeping the sequence out of the sealed body buys.
func (w *WAL) firstSequenceOf(seg segmentRef, limit int64) (uint64, bool, error) {
	path := filepath.Join(w.dir, seg.name)
	f, err := os.Open(path)
	if err != nil {
		return 0, false, fmt.Errorf("opening WAL segment %s for its first sequence: %w", seg.name, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, false, fmt.Errorf("stat WAL segment %s: %w", seg.name, err)
	}
	size := info.Size()
	if limit >= 0 && limit < size {
		size = limit
	}
	if size == 0 {
		return 0, false, nil
	}

	format, err := readSegmentFormat(f, size, seg.name)
	if err != nil {
		return 0, false, err
	}
	start := format.entryStart()
	if format.header != nil && size < start+format.framingLen() {
		// A header and nothing after it: the segment holds no entry.
		return 0, false, nil
	}
	framing := make([]byte, format.framingLen())
	if _, err := f.ReadAt(framing, start); err != nil {
		return 0, false, fmt.Errorf("reading the first entry framing of WAL segment %s: %w", seg.name, err)
	}
	word := binary.BigEndian.Uint64(framing[:8])
	if format.v2 && word == 0 {
		// A zero first word is a format rule, not a bound: it holds for a
		// caller without mu that cannot know the write offset (spec §3.3).
		return 0, false, nil
	}

	if format.sealed {
		onDiskLen := int64(uint32(word))
		if onDiskLen < walSealedMinOnDisk || onDiskLen > int64(walSealedMaxOnDisk) {
			return 0, false, fmt.Errorf("WAL segment %s begins with an implausible entry length %d", seg.name, onDiskLen)
		}
		return binary.BigEndian.Uint64(framing[8:]), true, nil
	}

	bodyLen, _, _ := decodeWALHeader(word)
	if bodyLen < walMinEntrySize || bodyLen > walMaxEntrySize {
		return 0, false, fmt.Errorf("WAL segment %s begins with an implausible entry length %d", seg.name, bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := f.ReadAt(body, start+8); err != nil {
		return 0, false, fmt.Errorf("reading entry body of WAL segment %s: %w", seg.name, err)
	}

	// For plaintext entries, seq is in the body; extract it.
	bodySeq := binary.BigEndian.Uint64(body[0:8])
	entry, err := parseWALEntry(body[8:], bodySeq)
	if err != nil {
		return 0, false, fmt.Errorf("parsing first entry of WAL segment %s: %w", seg.name, err)
	}
	return entry.Seq, true, nil
}

// Truncate removes WAL segment files where all entries have seq <= upToSeq.
// The file containing upToSeq is kept, but entries before upToSeq within it
// are logically skipped (the file is not physically truncated to avoid
// breaking the sequential write pattern — callers should read from upToSeq).
//
// The WAL serves two consumers and a caller must satisfy both before pruning:
//   - this node's own crash replay: an entry may only be pruned once its
//     effects are durably synced into the local stores it targeted (see
//     Cluster.pruneWAL in server/retention.go), because Badger-backed stores
//     are opened without per-write sync;
//   - catch-up of lagging replicas: an entry may only be pruned once every
//     peer that holds the bucket has acknowledged at least that sequence
//     (firstSeq, see WAL.FirstSequence, is the guard catch-up checks).
func (w *WAL) Truncate(upToSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments := w.listSegments()

	// Only each segment's first sequence is read, never its entries. Sequence
	// numbers are monotonic across the log, so a segment whose successor starts
	// at or below upToSeq holds nothing above it and can go, and the first
	// segment whose successor starts above upToSeq is the one that contains the
	// bound and is kept. A sealed log is therefore pruned without decrypting a
	// single entry — and prunable even by a node that holds no key.
	//
	// An empty segment covers no sequence, so it starts where the next
	// non-empty segment after it does: it goes with everything behind it, and
	// never makes the segment in front of it look prunable. Empty segments at
	// the end have no such successor (a crash after rotation, or a failed write
	// rolled back to a new segment's start), so they start above every bound
	// and the last non-empty segment is kept as the last one would be. Reading
	// an empty segment as starting at 0 lost entries above the bound
	// (BUG-0180).
	starts := make([]uint64, len(segments))
	startKnown := make([]bool, len(segments))
	for i := len(segments) - 1; i >= 0; i-- {
		seq, found, err := w.firstSequenceOf(segments[i], w.entryLimitLocked(segments[i].index))
		if err != nil {
			return fmt.Errorf("reading the first sequence of WAL segment %s for truncation: %w", segments[i].name, err)
		}
		switch {
		case found:
			starts[i], startKnown[i] = seq, true
		case i+1 < len(segments):
			starts[i], startKnown[i] = starts[i+1], startKnown[i+1]
		}
	}

	for i, seg := range segments {
		if i+1 >= len(segments) || !startKnown[i+1] || starts[i+1] > upToSeq {
			break
		}
		segPath := filepath.Join(w.dir, seg.name)
		if err := os.Remove(segPath); err != nil {
			return fmt.Errorf("removing WAL segment %s: %w", seg.name, err)
		}
	}

	return nil
}

// Close stops the filler, fsyncs the active file and closes it. It writes
// nothing else: the next open derives the log's state from the segments and
// the incarnation chain. It does not cut a preallocated segment's zero tail:
// every open retires the tail, after a crash as after Close, so a cut here
// would be a second path that only a clean shutdown runs. A ready spare stays
// on disk for the next start.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// See rotate() for why: wait for any Append still fsyncing this file
	// before we Sync+Close it ourselves.
	w.syncWG.Wait()
	// The filler never takes mu, so it can be waited for with mu held; a fill
	// it cancels deletes its wal_spare.tmp.
	w.stopFiller()

	w.closed = true
	if w.activeFile != nil {
		if err := w.sys.fsync(w.activeFile); err != nil {
			return fmt.Errorf("fsyncing WAL on close: %w", err)
		}
		if err := w.activeFile.Close(); err != nil {
			return fmt.Errorf("closing WAL active file: %w", err)
		}
		w.activeFile = nil
	}
	return nil
}

// rotate retires the active segment N and installs N+1, in spec §2.2's
// order, under mu and before the entry that needs it draws a sequence:
//
//	R0 prepare N+1 under a spare name (prepareSegment): born under
//	   wal_spare.tmp, or with preallocation the ready spare, waited for or
//	   filled when there is none. A failure refuses the entry and changes
//	   nothing: N stays active, open and unchanged, and the refusal is cached
//	   for walRotationRetryInterval (BUG-0716).
//	R1 wait for the appends still flushing N, then flush N.
//	R2 a preallocated N only: cut it to the write offset and fsync it.
//	R3 close N, rename the spare to N+1's name.
//	R5 fsync the directory, so N+1's name is durable before anything is written
//	   under it and acknowledged (BUG-0719).
//	R6 install N+1 (install), which cannot fail.
//
// Every failure after R0 poisons the log (BUG-0717): N's entries may not all
// be durable, or N+1 may have a name a crash can lose, and a later flush that
// succeeds says nothing about either. activeIndex and writeOff stay N's, so
// readers still read all of it; activeFile is nil once R3 has closed it.
func (w *WAL) rotate() error {
	started := time.Now()
	next, waited, err := w.prepareSegment(w.activeIndex + 1)
	if waited {
		w.counters.observeRotationWait(time.Since(started))
	}
	if err != nil {
		return w.refuseSegment(err, false)
	}
	w.rotationRefusal.Store(nil)

	outgoing := segmentFileName(w.activeIndex)
	// Every Append that already wrote to N captured it and released mu before
	// calling waitForSync (see writeLocked). Since this runs under mu, any such
	// call still in flight is the only thing syncWG can be counting: nothing
	// has written to N+1, which has no name yet. Wait for them so this flush
	// and close cannot race a group-commit flush of the same file.
	w.syncWG.Wait()
	if err := w.segmentFlush()(w.activeFile); err != nil {
		return w.poisonRotation(next, fmt.Errorf("flushing WAL segment %s before rotating past it: %w", outgoing, err))
	}
	if w.activePreallocated {
		// R2: a segment below the tail never has a zero tail, and the cut is
		// durable before the next segment has a name (spec §2.2, §3.3).
		if err := w.sys.ftruncate(w.activeFile, w.writeOff); err != nil {
			return w.poisonRotation(next, fmt.Errorf("cutting WAL segment %s to its entries before rotating past it: %w", outgoing, err))
		}
		if err := w.sys.fsync(w.activeFile); err != nil {
			return w.poisonRotation(next, fmt.Errorf("fsyncing WAL segment %s after cutting it to its entries: %w", outgoing, err))
		}
	}

	closeErr := w.sys.close(w.activeFile)
	w.activeFile = nil
	if closeErr != nil {
		return w.poisonRotation(next, fmt.Errorf("closing WAL segment %s before rotating past it: %w", outgoing, closeErr))
	}
	if err := w.nameSegment(next); err != nil {
		return w.poisonRotation(next, err)
	}
	if err := w.sys.syncDir(w.dir); err != nil {
		return w.poisonRotation(next, fmt.Errorf("making the name of WAL segment %s durable: %w", segmentFileName(next.index), err))
	}

	w.install(next)
	w.counters.rotations.Add(1)
	return nil
}

// poisonRotation poisons the log with cause, a rotation's failure after R0,
// closes the prepared segment that will now never be installed, and returns
// the poison as every later write will see it. The next open repairs the log
// (spec §3.4): it deletes a wal_spare.tmp and walks back over an empty N+1.
func (w *WAL) poisonRotation(next bornSegment, cause error) error {
	err := w.poison(cause)
	if closeErr := next.file.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("closing the prepared WAL segment %s: %w", segmentFileName(next.index), closeErr))
	}
	return err
}
