/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by the hoardDB project, 2026-10-07: logFile.doneWriting syncs the
 * file whether or not SyncWrites is set. See hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-10-24: logFile.open and logFile.Truncate
 * advise the kernel that the log mapping is accessed at random (MADV_RANDOM),
 * so an msync writes back pages rather than readahead-sized folios;
 * memTable.UpdateSkipList keeps readahead while it replays. See
 * hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-11-17: under a vouched cut
 * (WithVouchedCut) the replay at open leaves out every entry above the
 * vouched version, truncates each log there and makes the truncation and the
 * removal of the logs it empties durable before Open returns. See
 * hoardDB/third_party/README.md.
 *
 * Modified by the hoardDB project, 2026-11-18: a flushed memtable's log is
 * removed at its flush (DB.retireMemTableLog) whatever references the
 * memtable has; the last reference then releases the unlinked file
 * (logFile.closeRetired). See hoardDB/third_party/README.md.
 * Modified by the hoardDB project, 2026-11-19: a flush whose MANIFEST change
 * fails keeps its table file and is not retried (memTable.failedNaming,
 * hoardDB's BUG-0868). See hoardDB/third_party/README.md.
 * Modified by the hoardDB project, 2026-11-19: DB.newMemTable counts the log
 * it creates (DB.LogFilesCreated). See hoardDB/third_party/README.md.
 * Modified by the hoardDB project, 2026-11-20: logFile.doneWriting and
 * logFile.Truncate (on a value-log file) sync through syncValueLogFile, and
 * logFile.syncFailed keeps a value-log file's failed msync (hoardDB's
 * BUG-0827, slice 2). See hoardDB/third_party/README.md.
 */

package badger

import (
	"bufio"
	"bytes"
	"crypto/aes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// memTable structure stores a skiplist and a corresponding WAL. Writes to memTable are written
// both to the WAL and the skiplist. On a crash, the WAL is replayed to bring the skiplist back to
// its pre-crash form.
type memTable struct {
	// TODO: Give skiplist z.Calloc'd []byte.
	sl         *skl.Skiplist
	wal        *logFile
	maxVersion uint64
	opt        Options
	buf        *bytes.Buffer
	// cut is set only while Open replays this log under a vouched cut
	// (WithVouchedCut); nil otherwise. hoardDB.
	cut *vouchedCutScan
	// logRetired is set once the flush has removed the log's name
	// (DB.retireMemTableLog); the last reference then releases the file
	// without removing it again. hoardDB.
	logRetired atomic.Bool
	// named is the table the MANIFEST names for this memtable while the
	// removal of its log is not yet durable: it holds the flush's reference
	// and is in no level. A retry of the flush retires the log and inserts
	// this table; it never builds a second one. retireFailed is set when the
	// directory fsync after the removal failed: the removal is then not known
	// to be durable for the rest of the process, and every retry returns it.
	// Only the flusher touches either, and DropPrefix while the flusher is
	// stopped. Neither is ever set on the active memtable: the flusher takes
	// rotated memtables only, and DropPrefix rotates the active one out before
	// it flushes it (review F1: flushed in place, a failed removal left the
	// active memtable with a named table, and writes acknowledged into it
	// were lost). hoardDB.
	named        *table.Table
	retireFailed error
	// failedNaming is the table whose MANIFEST change failed: addChanges
	// applies the change in memory and writes it before the sync, so when the
	// sync fails the MANIFEST may name the table on disk, now or at its next
	// rewrite. The file is kept (DecrRef would delete a table the MANIFEST may
	// name) and the naming is not retried, as a failed directory fsync is not
	// (a later sync can succeed without the record having reached the disk):
	// nameFailed is returned by every later flush of this memtable, change 4
	// refuses writes, the memtable's log stays, and Close closes the table and
	// keeps it. Whatever the MANIFEST holds, the next Open then finds a store it
	// can open: the named table and its log, which it removes; or an unnamed
	// table, which it deletes (or, for a first flush, hoardDB's guard sets
	// aside) and the log it replays. hoardDB's BUG-0868.
	failedNaming *table.Table
	nameFailed   error
}

func (db *DB) openMemTables(opt Options) error {
	// We don't need to open any tables in in-memory mode.
	if db.opt.InMemory {
		return nil
	}
	files, err := os.ReadDir(db.opt.Dir)
	if err != nil {
		return errFile(err, db.opt.Dir, "Unable to open mem dir.")
	}

	var fids []int
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), memFileExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseInt(file.Name()[:fsz-len(memFileExt)], 10, 64)
		if err != nil {
			return errFile(err, file.Name(), "Unable to parse log id.")
		}
		fids = append(fids, int(fid))
	}

	// Sort in ascending order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	// hoardDB: under a vouched cut the logs are replayed oldest first, which is
	// version order, so the first entry above the vouched version is the cut
	// point and every later entry is above it too.
	if db.opt.vouchedCut {
		db.replayCut = &vouchedCutScan{version: db.opt.vouchedVersion}
		defer func() { db.replayCut = nil }()
	}
	for _, fid := range fids {
		flags := os.O_RDWR
		if db.opt.ReadOnly {
			flags = os.O_RDONLY
		}
		if db.replayCut != nil {
			db.replayCut.startLog()
		}
		mt, err := db.openMemTable(fid, flags)
		if err != nil {
			return y.Wrapf(err, "while opening fid: %d", fid)
		}
		cutHere := db.replayCut != nil && db.replayCut.logCut
		// If this memtable is empty we don't need to add it. This is a
		// memtable that was completely truncated.
		if mt.sl.Empty() {
			walPath := mt.wal.path
			mt.DecrRef()
			if cutHere {
				// The removal is what takes the discarded bytes off the disk,
				// and DecrRef only logs a failure to remove.
				if _, statErr := os.Stat(walPath); statErr == nil {
					return fmt.Errorf("the memtable log %s, which the vouched cut emptied, could not be removed "+
						"(the error is logged above)", walPath)
				} else if !os.IsNotExist(statErr) {
					return fmt.Errorf("checking that the memtable log %s, which the vouched cut emptied, was removed: %w",
						walPath, statErr)
				}
				db.replayCut.logsRemoved++
			}
			continue
		}
		if cutHere {
			db.replayCut.logsTruncated++
		}
		// These should no longer be written to. So, make them part of the imm.
		db.imm = append(db.imm, mt)
	}
	if len(fids) != 0 {
		db.nextMemFid = fids[len(fids)-1]
	}
	db.nextMemFid++
	// hoardDB: the removals of the logs the cut emptied are durable before
	// Open returns, as the truncations are (UpdateSkipList).
	if cut := db.replayCut; cut != nil {
		if cut.logsTruncated+cut.logsRemoved > 0 {
			if err := cutSyncDir(db.opt.Dir); err != nil {
				return y.Wrapf(err, "while syncing the directory %s after the vouched cut", db.opt.Dir)
			}
		}
		db.vouchedReport.DiscardedTxns = cut.discardedTxns
		db.vouchedReport.LogsTruncated, db.vouchedReport.LogsRemoved = cut.logsTruncated, cut.logsRemoved
	}
	return nil
}

const memFileExt string = ".mem"

func (db *DB) openMemTable(fid, flags int) (*memTable, error) {
	filepath := db.mtFilePath(fid)
	s := skl.NewSkiplist(arenaSize(db.opt))
	mt := &memTable{
		sl:  s,
		opt: db.opt,
		buf: &bytes.Buffer{},
		cut: db.replayCut,
	}
	// We don't need to create the wal for the skiplist in in-memory mode so return the mt.
	if db.opt.InMemory {
		return mt, z.NewFile
	}

	mt.wal = &logFile{
		fid:      uint32(fid),
		path:     filepath,
		registry: db.registry,
		writeAt:  vlogHeaderSize,
		opt:      db.opt,
	}
	lerr := mt.wal.open(filepath, flags, 2*db.opt.MemTableSize)
	if lerr != z.NewFile && lerr != nil {
		return nil, y.Wrapf(lerr, "While opening memtable: %s", filepath)
	}

	// Have a callback set to delete WAL when skiplist reference count goes down to zero. That is,
	// when it gets flushed to L0.
	s.OnClose = func() {
		if mt.logRetired.Load() {
			if err := mt.wal.closeRetired(); err != nil {
				db.opt.Errorf("while releasing the removed log: %s, err: %v", filepath, err)
			}
			return
		}
		if err := mt.wal.Delete(); err != nil {
			db.opt.Errorf("while deleting file: %s, err: %v", filepath, err)
		}
	}

	if lerr == z.NewFile {
		return mt, lerr
	}
	err := mt.UpdateSkipList()
	mt.cut = nil
	return mt, y.Wrapf(err, "while updating skiplist")
}

func (db *DB) newMemTable() (*memTable, error) {
	mt, err := db.openMemTable(db.nextMemFid, os.O_CREATE|os.O_RDWR)
	if err == z.NewFile {
		db.nextMemFid++
		db.logFilesCreated.Add(1) // hoardDB: before any commit is written to it.
		return mt, nil
	}

	if err != nil {
		db.opt.Errorf("Got error: %v for id: %d\n", err, db.nextMemFid)
		return nil, y.Wrapf(err, "newMemTable")
	}
	return nil, fmt.Errorf("File %s already exists", mt.wal.Fd.Name())
}

func (db *DB) mtFilePath(fid int) string {
	return filepath.Join(db.opt.Dir, fmt.Sprintf("%05d%s", fid, memFileExt))
}

func (mt *memTable) SyncWAL() error {
	return mt.wal.Sync()
}

func (mt *memTable) isFull() bool {
	if mt.sl.MemSize() >= mt.opt.MemTableSize {
		return true
	}
	if mt.opt.InMemory {
		// InMemory mode doesn't have any WAL.
		return false
	}
	return int64(mt.wal.writeAt) >= mt.opt.MemTableSize
}

func (mt *memTable) Put(key []byte, value y.ValueStruct) error {
	entry := &Entry{
		Key:       key,
		Value:     value.Value,
		UserMeta:  value.UserMeta,
		meta:      value.Meta,
		ExpiresAt: value.ExpiresAt,
	}

	// wal is nil only when badger in running in in-memory mode and we don't need the wal.
	if mt.wal != nil {
		// If WAL exceeds opt.ValueLogFileSize, we'll force flush the memTable. See logic in
		// ensureRoomForWrite.
		if err := mt.wal.writeEntry(mt.buf, entry, mt.opt); err != nil {
			return y.Wrapf(err, "cannot write entry to WAL file")
		}
	}
	// We insert the finish marker in the WAL but not in the memtable.
	if entry.meta&bitFinTxn > 0 {
		return nil
	}

	// Write to skiplist and update maxVersion encountered.
	mt.sl.Put(key, value)
	if ts := y.ParseTs(entry.Key); ts > mt.maxVersion {
		mt.maxVersion = ts
	}
	y.NumBytesWrittenToL0Add(mt.opt.MetricsEnabled, entry.estimateSizeAndSetThreshold(mt.opt.ValueThreshold))
	return nil
}

func (mt *memTable) UpdateSkipList() error {
	if mt.wal == nil || mt.sl == nil {
		return nil
	}
	// hoardDB: the replay reads the log once, in order, and a replayed memtable
	// is never appended to again (openMemTables makes it immutable), so it keeps
	// readahead for the replay. Without it a cold replay of a 256 MiB log took
	// 1.1-1.4 s against 0.5 s (BUG-0700).
	mt.wal.adviseAccess(true)
	endOff, err := mt.wal.iterate(true, 0, mt.replayFunction(mt.opt))
	mt.wal.adviseAccess(false)
	if err != nil {
		return y.Wrapf(err, "while iterating wal: %s", mt.wal.Fd.Name())
	}
	if endOff < mt.wal.size.Load() && mt.opt.ReadOnly {
		return y.Wrapf(ErrTruncateNeeded, "end offset: %d < size: %d", endOff, mt.wal.size.Load())
	}
	cut := mt.cut != nil && mt.cut.logCut
	if cut {
		endOff = min(endOff, mt.cut.logCutOffset)
	}
	if err := mt.wal.Truncate(int64(endOff)); err != nil {
		return err
	}
	// hoardDB: the versions the cut discarded are handed out again, so the old
	// bytes must be off the disk before the first commit: a later, higher
	// vouched version would vouch them. Truncate msyncs before it truncates
	// and does not fsync after. A log the cut emptied is removed instead, and
	// openMemTables fsyncs the directory.
	if cut && !mt.sl.Empty() {
		if err := cutSyncFile(mt.wal.Fd); err != nil {
			return y.Wrapf(err, "while syncing the memtable log %s after the vouched cut", mt.wal.path)
		}
	}
	return nil
}

// IncrRef increases the refcount
func (mt *memTable) IncrRef() {
	mt.sl.IncrRef()
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (mt *memTable) DecrRef() {
	mt.sl.DecrRef()
}

func (mt *memTable) replayFunction(opt Options) func(Entry, valuePointer) error {
	first := true
	return func(e Entry, vp valuePointer) error { // Function for replaying.
		if first {
			opt.Debugf("First key=%q\n", e.Key)
		}
		first = false
		// hoardDB: under a vouched cut an entry above the vouched version enters
		// neither the skiplist nor maxVersion; UpdateSkipList then truncates the
		// log where the first one starts.
		if mt.cut != nil {
			if discard, err := mt.cut.discards(e.Key, vp.Offset, mt.wal.path); discard || err != nil {
				return err
			}
		}
		if ts := y.ParseTs(e.Key); ts > mt.maxVersion {
			mt.maxVersion = ts
		}
		v := y.ValueStruct{
			Value:     e.Value,
			Meta:      e.meta,
			UserMeta:  e.UserMeta,
			ExpiresAt: e.ExpiresAt,
		}
		// This is already encoded correctly. Value would be either a vptr, or a full value
		// depending upon how big the original value was. Skiplist makes a copy of the key and
		// value.
		mt.sl.Put(e.Key, v)
		return nil
	}
}

type logFile struct {
	*z.MmapFile
	path string
	// This is a lock on the log file. It guards the fd’s value, the file’s
	// existence and the file’s memory map.
	//
	// Use shared ownership when reading/writing the file or memory map, use
	// exclusive ownership to open/close the descriptor, unmap or remove the file.
	lock     sync.RWMutex
	fid      uint32
	size     atomic.Uint32
	dataKey  *pb.DataKey
	baseIV   []byte
	registry *KeyRegistry
	writeAt  uint32
	opt      Options
	// syncFailed, on a value-log file, is the first failure of its msync,
	// returned by every later one (syncValueLogFile). hoardDB.
	syncFailed atomic.Pointer[error]
}

func (lf *logFile) Truncate(end int64) error {
	if fi, err := lf.Fd.Stat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v\n", lf.Fd.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	y.AssertTrue(!lf.opt.ReadOnly)
	// hoardDB (BUG-0827, slice 2, review F2): z.MmapFile.Truncate msyncs the
	// mapping itself, outside syncValueLogFile, so a failure there was not
	// kept: the write that grew the file failed, the retry's msync succeeded
	// on pages the failure may have dropped, and a later flush named a table
	// over them. A value-log file is msynced through the keep first; the
	// msync inside Truncate then finds no page this goroutine dirtied.
	if strings.HasSuffix(lf.path, ".vlog") {
		if err := syncValueLogFile(lf, 0); err != nil {
			return y.Wrapf(err, "while syncing value-log file %s before truncating it", lf.path)
		}
	}
	lf.size.Store(uint32(end))
	if err := lf.MmapFile.Truncate(end); err != nil {
		return err
	}
	// hoardDB: on Linux mremap keeps the advice, but on other systems z unmaps
	// and maps afresh, and the new mapping needs it again.
	lf.adviseAccess(false)
	return nil
}

// accessAdviceFailureLogged makes a refused madvise one warning per process:
// the hint fails the same way for every file on a host.
var accessAdviceFailureLogged atomic.Bool

// adviseAccess tells the kernel whether the log mapping is read in sequence:
// readahead false is MADV_RANDOM, true is MADV_NORMAL. hoardDB: a WAL is
// appended a few KB at a time and msynced on every durable write. Without
// MADV_RANDOM, a kernel that sizes file folios by the readahead window (ext4 on
// recent Linux, for one) grows the folios at the log's tail toward megabytes,
// and each msync writes back whole folios: seeding a 230 MB dataset wrote 72-74
// GB, and 0.86-0.88 GB with the hint (BUG-0700). With it, faults allocate
// single pages. Every log mapping is advised MADV_RANDOM, except a memtable
// WAL while it is replayed at open. The hint is advisory, so a refusal is
// logged and the mapping is used as it is.
func (lf *logFile) adviseAccess(readahead bool) {
	if err := z.Madvise(lf.Data, readahead); err != nil {
		if accessAdviceFailureLogged.CompareAndSwap(false, true) {
			lf.opt.Warningf("%v", y.Wrapf(err,
				"advising the access pattern (readahead %v) for log file %s: a sync of this and later "+
					"log files may write back more than it changed; further failures are not logged",
				readahead, lf.path))
		}
	}
}

// encodeEntry will encode entry to the buf
// layout of entry
// +--------+-----+-------+-------+
// | header | key | value | crc32 |
// +--------+-----+-------+-------+
func (lf *logFile) encodeEntry(buf *bytes.Buffer, e *Entry, offset uint32) (int, error) {
	h := header{
		klen:      uint32(len(e.Key)),
		vlen:      uint32(len(e.Value)),
		expiresAt: e.ExpiresAt,
		meta:      e.meta,
		userMeta:  e.UserMeta,
	}

	hash := crc32.New(y.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	y.Check2(writer.Write(headerEnc[:sz]))
	// we'll encrypt only key and value.
	if lf.encryptionEnabled() {
		// TODO: no need to allocate the bytes. we can calculate the encrypted buf one by one
		// since we're using ctr mode of AES encryption. Ordering won't changed. Need some
		// refactoring in XORBlock which will work like stream cipher.
		eBuf := make([]byte, 0, len(e.Key)+len(e.Value))
		eBuf = append(eBuf, e.Key...)
		eBuf = append(eBuf, e.Value...)
		if err := y.XORBlockStream(
			writer, eBuf, lf.dataKey.Data, lf.generateIV(offset)); err != nil {
			return 0, y.Wrapf(err, "Error while encoding entry for vlog.")
		}
	} else {
		// Encryption is disabled so writing directly to the buffer.
		y.Check2(writer.Write(e.Key))
		y.Check2(writer.Write(e.Value))
	}
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	y.Check2(buf.Write(crcBuf[:]))
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (lf *logFile) writeEntry(buf *bytes.Buffer, e *Entry, opt Options) error {
	buf.Reset()
	plen, err := lf.encodeEntry(buf, e, lf.writeAt)
	if err != nil {
		return err
	}
	y.AssertTrue(plen == copy(lf.Data[lf.writeAt:], buf.Bytes()))
	lf.writeAt += uint32(plen)

	lf.zeroNextEntry()
	return nil
}

func (lf *logFile) decodeEntry(buf []byte, offset uint32) (*Entry, error) {
	var h header
	hlen := h.Decode(buf)
	kv := buf[hlen:]
	if lf.encryptionEnabled() {
		var err error
		// No need to worry about mmap. because, XORBlock allocates a byte array to do the
		// xor. So, the given slice is not being mutated.
		if kv, err = lf.decryptKV(kv, offset); err != nil {
			return nil, err
		}
	}
	e := &Entry{
		meta:      h.meta,
		UserMeta:  h.userMeta,
		ExpiresAt: h.expiresAt,
		offset:    offset,
		Key:       kv[:h.klen],
		Value:     kv[h.klen : h.klen+h.vlen],
	}
	return e, nil
}

func (lf *logFile) decryptKV(buf []byte, offset uint32) ([]byte, error) {
	return y.XORBlockAllocate(buf, lf.dataKey.Data, lf.generateIV(offset))
}

// KeyID returns datakey's ID.
func (lf *logFile) keyID() uint64 {
	if lf.dataKey == nil {
		// If there is no datakey, then we'll return 0. Which means no encryption.
		return 0
	}
	return lf.dataKey.KeyId
}

func (lf *logFile) encryptionEnabled() bool {
	return lf.dataKey != nil
}

// Acquire lock on mmap/file if you are calling this
func (lf *logFile) read(p valuePointer) (buf []byte, err error) {
	offset := p.Offset
	// Do not convert size to uint32, because the lf.Data can be of size
	// 4GB, which overflows the uint32 during conversion to make the size 0,
	// causing the read to fail with ErrEOF. See issue #585.
	size := int64(len(lf.Data))
	valsz := p.Len
	lfsz := lf.size.Load()
	if int64(offset) >= size || int64(offset+valsz) > size ||
		// Ensure that the read is within the file's actual size. It might be possible that
		// the offset+valsz length is beyond the file's actual size. This could happen when
		// dropAll and iterations are running simultaneously.
		int64(offset+valsz) > int64(lfsz) {
		err = y.ErrEOF
	} else {
		buf = lf.Data[offset : offset+valsz]
	}
	return buf, err
}

// generateIV will generate IV by appending given offset with the base IV.
func (lf *logFile) generateIV(offset uint32) []byte {
	iv := make([]byte, aes.BlockSize)
	// baseIV is of 12 bytes.
	y.AssertTrue(12 == copy(iv[:12], lf.baseIV))
	// remaining 4 bytes is obtained from offset.
	binary.BigEndian.PutUint32(iv[12:], offset)
	return iv
}

func (lf *logFile) doneWriting(offset uint32) error {
	// hoardDB: sync unconditionally. valueLog.sync (DB.Sync) syncs only the newest file, so
	// this is the last sync a rotated file gets. The Truncate below syncs only when it changes
	// the size, and a file that one request grew past its preallocation already ends at
	// offset, so without this its tail is never synced when SyncWrites is off (BUG-0217).
	if err := syncValueLogFile(lf, offset); err != nil {
		return y.Wrapf(err, "Unable to sync value log: %q", lf.path)
	}

	// Before we were acquiring a lock here on lf.lock, because we were invalidating the file
	// descriptor due to reopening it as read-only. Now, we don't invalidate the fd, but unmap it,
	// truncate it and remap it. That creates a window where we have segfaults because the mmap is
	// no longer valid, while someone might be reading it. Therefore, we need a lock here again.
	lf.lock.Lock()
	defer lf.lock.Unlock()

	if err := lf.Truncate(int64(offset)); err != nil {
		return y.Wrapf(err, "Unable to truncate file: %q", lf.path)
	}

	// Previously we used to close the file after it was written and reopen it in read-only mode.
	// We no longer open files in read-only mode. We keep all vlog files open in read-write mode.
	return nil
}

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
func (lf *logFile) iterate(readOnly bool, offset uint32, fn logEntry) (uint32, error) {
	if offset == 0 {
		// If offset is set to zero, let's advance past the encryption key header.
		offset = vlogHeaderSize
	}

	// For now, read directly from file, because it allows
	reader := bufio.NewReader(lf.NewReader(int(offset)))
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
		lf:           lf,
	}

	var lastCommit uint64
	var validEndOffset uint32 = offset

	var entries []*Entry
	var vptrs []valuePointer

loop:
	for {
		e, err := read.Entry(reader)
		switch {
		// We have not reached the end of the file but the entry we read is
		// zero. This happens because we have truncated the file and
		// zero'ed it out.
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == errTruncate:
			break loop
		case err != nil:
			return 0, err
		case e == nil:
			continue
		case e.isZero():
			break loop
		}

		var vp valuePointer
		vp.Len = uint32(e.hlen + len(e.Key) + len(e.Value) + crc32.Size)
		read.recordOffset += vp.Len

		vp.Offset = e.offset
		vp.Fid = lf.fid

		switch {
		case e.meta&bitTxn > 0:
			txnTs := y.ParseTs(e.Key)
			if lastCommit == 0 {
				lastCommit = txnTs
			}
			if lastCommit != txnTs {
				break loop
			}
			entries = append(entries, e)
			vptrs = append(vptrs, vp)

		case e.meta&bitFinTxn > 0:
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil || lastCommit != txnTs {
				break loop
			}
			// Got the end of txn. Now we can store them.
			lastCommit = 0
			validEndOffset = read.recordOffset

			for i, e := range entries {
				vp := vptrs[i]
				if err := fn(*e, vp); err != nil {
					if err == errStop {
						break
					}
					return 0, errFile(err, lf.path, "Iteration function")
				}
			}
			entries = entries[:0]
			vptrs = vptrs[:0]

		default:
			if lastCommit != 0 {
				// This is most likely an entry which was moved as part of GC.
				// We shouldn't get this entry in the middle of a transaction.
				break loop
			}
			validEndOffset = read.recordOffset

			if err := fn(*e, vp); err != nil {
				if err == errStop {
					break
				}
				return 0, errFile(err, lf.path, "Iteration function")
			}
		}
	}
	return validEndOffset, nil
}

// Zero out the next entry to deal with any crashes.
func (lf *logFile) zeroNextEntry() {
	z.ZeroOut(lf.Data, int(lf.writeAt), int(lf.writeAt+maxHeaderSize))
}

func (lf *logFile) open(path string, flags int, fsize int64) error {
	mf, ferr := z.OpenMmapFile(path, flags, int(fsize))
	lf.MmapFile = mf
	if ferr == nil || ferr == z.NewFile {
		// hoardDB: before the first write faults a page in (BUG-0700).
		lf.adviseAccess(false)
	}

	if ferr == z.NewFile {
		if err := lf.bootstrap(); err != nil {
			os.Remove(path)
			return err
		}
		lf.size.Store(vlogHeaderSize)

	} else if ferr != nil {
		return y.Wrapf(ferr, "while opening file: %s", path)
	}
	lf.size.Store(uint32(len(lf.Data)))

	if lf.size.Load() < vlogHeaderSize {
		// Every vlog file should have at least vlogHeaderSize. If it is less than vlogHeaderSize
		// then it must have been corrupted. But no need to handle here. log replayer will truncate
		// and bootstrap the logfile. So ignoring here.
		return nil
	}

	// Copy over the encryption registry data.
	buf := make([]byte, vlogHeaderSize)

	y.AssertTruef(vlogHeaderSize == copy(buf, lf.Data),
		"Unable to copy from %s, size %d", path, lf.size.Load())
	keyID := binary.BigEndian.Uint64(buf[:8])
	// retrieve datakey.
	if dk, err := lf.registry.DataKey(keyID); err != nil {
		return y.Wrapf(err, "While opening vlog file %d", lf.fid)
	} else {
		lf.dataKey = dk
	}
	lf.baseIV = buf[8:]
	y.AssertTrue(len(lf.baseIV) == 12)

	// Preserved ferr so we can return if this was a new file.
	return ferr
}

// bootstrap will initialize the log file with key id and baseIV.
// The below figure shows the layout of log file.
// +----------------+------------------+------------------+
// | keyID(8 bytes) |  baseIV(12 bytes)|	 entry...     |
// +----------------+------------------+------------------+
func (lf *logFile) bootstrap() error {
	var err error

	// generate data key for the log file.
	var dk *pb.DataKey
	if dk, err = lf.registry.LatestDataKey(); err != nil {
		return y.Wrapf(err, "Error while retrieving datakey in logFile.bootstarp")
	}
	lf.dataKey = dk

	// We'll always preserve vlogHeaderSize for key id and baseIV.
	buf := make([]byte, vlogHeaderSize)

	// write key id to the buf.
	// key id will be zero if the logfile is in plain text.
	binary.BigEndian.PutUint64(buf[:8], lf.keyID())
	// generate base IV. It'll be used with offset of the vptr to encrypt the entry.
	if _, err := cryptorand.Read(buf[8:]); err != nil {
		return y.Wrapf(err, "Error while creating base IV, while creating logfile")
	}

	// Initialize base IV.
	lf.baseIV = buf[8:]
	y.AssertTrue(len(lf.baseIV) == 12)

	// Copy over to the logFile.
	y.AssertTrue(vlogHeaderSize == copy(lf.Data[0:], buf))

	// Zero out the next entry.
	lf.zeroNextEntry()
	return nil
}
