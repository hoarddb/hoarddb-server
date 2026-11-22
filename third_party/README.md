# third_party

Patched copies of dependencies, carried with a `replace` in `hoardDB/go.mod`. Each one is a recorded
exception to "prefer boring, well-documented technology" (the project's Developer-First rule 5) and to
"never vendor unless reproducibility demands it", and each has an exit condition. Nothing else belongs
here.

## badger: Badger v4.9.6 with thirteen changes

1. `DB.Sync` also syncs the immutable memtables.
2. A table block that cannot be read is an error, not a missing key.
3. A value-log file is synced when it rotates, whether or not `SyncWrites` is set.
4. A memtable flush that keeps failing refuses writes, and `Close` returns.
5. `Close` returns when level 0 is stalled behind a table that cannot be read, and names the table.
6. A write blocked behind a level 0 stalled by a damaged table is refused, naming the table.
7. The memtable WAL and value-log mappings are advised random access (`MADV_RANDOM`).
8. A new table is written back in steps and waited on once, instead of one blocking msync.
9. `DB.Sync` syncs the active memtable's log outside the DB lock too, so writes and reads do not wait for it.
10. `DB.Sync` syncs the rotated-out memtables' logs, oldest first, before the active one; after a log fails, that call syncs no later log.
11. A table's bloom filter also holds a hash of each key's prefix, and an iterator over one prefix leaves out the
    tables whose filter does not hold it. The only change of the thirteen to the on-disk format.
12. An opt-in vouched cut: `DB.SyncVouched` returns the highest commit version a sync made durable, and a store
    opened with `WithVouchedCut(version)` discards every memtable-log entry above it, durably, before the first
    commit; the calls that write out of version order are refused under it. With or without the cut, a flush
    removes its memtable's log, durably, after the MANIFEST names the table and before the table is in level 0,
    and `Open` removes a log whose table the MANIFEST already names instead of replaying it (hoardDB's BUG-0858).
13. A memtable flush syncs the value log before its table is built, so before the MANIFEST names it; a failed
    value-log msync made through `valueLog.sync`, `logFile.doneWriting` or `logFile.Truncate` is kept and returned by
    every later one; a flush that follows a new value-log file fsyncs the value directory (hoardDB's BUG-0827, slice 2
    of the vouched cut).

Each change's reasoning, tradeoff and exit condition are summarised in its section below.

### Change 1: `DB.Sync` also syncs the immutable memtables

- **Upstream:** `github.com/dgraph-io/badger/v4` v4.9.6, Apache-2.0. `badger/LICENSE` is upstream's,
  unchanged; upstream ships no NOTICE file. The tree is the module-cache copy of v4.9.6 verbatim
  (commit `996b93a` on the fix branch), less its `.vscode/` directory.
- **What changed:** `badger/db.go` (`DB.Sync`, plus a modification notice in the file header, which
  Apache-2.0 §4(b) requires) and one new test, `badger/db_sync_linux_test.go`. The whole change is
  [`badger-v4.9.6-sync-immutable-memtables.patch`](badger-v4.9.6-sync-immutable-memtables.patch), a
  `patch -p1` diff against the upstream module.
- **Why:** upstream's `DB.Sync` syncs only the active memtable's log and the value log. A memtable that
  fills is rotated to the immutable list without a sync, and its log is the only durable copy of its
  entries until the flush to a level-0 table finishes. So a write committed, rotated out by another
  writer, and then "synced" was lost on power failure. hoardDB's every durable write is a commit
  followed by `DB.Sync` (`storage/kv_badger.go`), so that was the bug. The patched `Sync` takes a
  reference on each immutable memtable under the DB lock, syncs its log outside the lock, and releases
  it.
- **Why a patch and not a setting:** the alternative is Badger's store-wide `SyncWrites` option, which
  syncs every write instead of only the writes that must be durable. Measured on hoardDB's workload it was
  36 to 140 times slower, so hoardDB carries this one-function change until upstream takes it (the change
  has been sent upstream as dgraph-io/badger#2343).
- **Exit:** when an upstream Badger release contains the change, bump the version in `go.mod`, delete
  the `replace` line and this directory, and run `storage/kv_badger_sync_linux_test.go`, which must stay
  green on the upstream release.

### Change 2: a corrupt table block is an error

- **What changed:** `level_handler.go` (`levelHandler.get` returns the error a table iterator stopped
  on), `table/iterator.go` (`Iterator.Error`, `ConcatIterator.Error`, `table.ErrBlockRead`; a concat
  iterator stays on the table that failed; `seekForPrev` stops on the error), `table/merge_iterator.go`
  (a child that failed stops the merge; `MergeIterator.Error`), `iterator.go` (`badger.Iterator.Error`),
  `levels.go` (a compaction whose iterator stopped on an error fails and keeps its inputs),
  `y/iterator.go` (`y.IteratorError`), `table/table.go` (`Table.block` verifies the checksum before it
  decodes the entry count and bounds-checks the trailer; `Block.verifyCheckSum` refuses an unknown
  checksum algorithm: BUG-0377), each with a modification notice in its header, and two new test
  files, `badger/corrupt_block_test.go` and `badger/table/block_trailer_test.go`. The whole change is
  [`badger-v4.9.6-corrupt-block-errors.patch`](badger-v4.9.6-corrupt-block-errors.patch), a `patch -p1`
  diff against the upstream module; it touches no file the first patch does.
- **Why:** upstream reads an iterator that could not load a block (checksum mismatch, decompression or
  decryption failure, read error) as one that ran out of keys. A `Get` of a key in the block answered
  "key not found", an iteration stepped over the rest of the table, and a compaction deleted the table
  with the keys past the damage in it. And it decoded a block's entry count before comparing its
  checksum, so a damaged count panicked with a slice out of range.
- **Why a patch:** nothing above Badger can recover an error the library dropped, and the alternative,
  verifying every table at open, costs a read of the whole store at every start and misses later damage.
- **Exit:** when an upstream release surfaces block errors on these paths, bump the version and run
  `storage/badger_corruption_test.go:TestACorruptTableBlockIsAnErrorNotAMiss` and
  `server/store_damaged_read_test.go:TestADamagedStoreFileReadIsAnErrorNamingTheBucket`, which must stay
  green; drop this patch from the procedure below. The directory and the `replace` go when neither
  change is left.

### Change 3: a rotated value-log file is synced

- **What changed:** `badger/memtable.go` (`logFile.doneWriting` syncs unconditionally, plus a modification
  notice in the file header) and one new test in `badger/db_sync_linux_test.go`. The whole change is
  [`badger-v4.9.6-sync-outgrown-vlog.patch`](badger-v4.9.6-sync-outgrown-vlog.patch), a `patch -p1` diff;
  its test hunk applies on top of change 1's.
- **Why:** a file that one request grew past its preallocation rotates without the msync `Truncate` would
  have done, and `DB.Sync` syncs only the newest file, so its tail was not durable (BUG-0217).
- **Exit:** when an upstream release syncs such a file at rotation, bump, drop the patch from the
  procedure below, and run `TestSyncCoversAValueLogFileThatOutgrewItsPreallocation` on the upstream tree.

### Change 4: a store that cannot flush refuses writes

- **What changed:** `db.go` (`DB.flushMemtable` sets a refusal after three failed flushes in a row, and
  stops retrying a memtable that has failed three times once `Close` has signalled it; `ErrWritesRefused`, `DB.WriteRefusal`; `DB.sendToWriteCh` refuses
  while it is set; `DB.ensureRoomForWrite` creates the next memtable before handing the full one over;
  `DB.close` reports the memtables it left unflushed; `prepareToDrop` fails a drop over one, and
  `startMemoryFlush` re-queues them in a channel large enough to hold them all; `DB.writeRequests` reports
  the requests of a group before a refusal as written and fails only the rest), with a second modification notice in its header, and one new test file,
  `badger/flush_failure_test.go`. The whole change is
  [`badger-v4.9.6-flush-failure-refuses-writes.patch`](badger-v4.9.6-flush-failure-refuses-writes.patch), a
  `patch -p1` diff against the upstream module **after the other three**: it touches `db.go`, as the first does,
  in other hunks.
- **Why:** upstream retries a failed memtable flush every second forever. Writes are acknowledged until every
  memtable is full, then block, and `Close` never returns. And when a full memtable's successor cannot be
  created, upstream leaves no active memtable, and the next write exits the process.
- **Why a patch:** Badger only logs a flush error, so nothing above it can refuse writes on it or make `Close`
  return.
- **Exit:** when an upstream release bounds the retry and keeps the memtable on a failed rotation, bump the
  version and run `server/store_flush_failure_test.go:TestANodeThatCannotPersistRefusesWritesAndStops`, which
  must stay green; drop this patch from the procedure below.

### Change 5: Close of a store stalled behind a damaged level-0 table

- **What changed:** `db.go` (`DB.close` wakes the flush goroutine while it waits to push the active memtable,
  and returns `ErrL0StalledByDamagedTable` when level 0 was past its limit), `levels.go`
  (`levelsController.doCompact` records the latest compaction failure on an unreadable table block;
  `addLevel0Table` records that Close pushed past the stall limit; `stalledByUnreadableTable`), each with a
  modification notice, and one new test file, `badger/stalled_close_test.go`. The whole change is
  [`badger-v4.9.6-close-stalled-l0.patch`](badger-v4.9.6-close-stalled-l0.patch), a `patch -p1` diff **after the
  other four**.
- **Why:** after change 2 a compaction that meets a damaged block keeps its inputs, so a damaged level-0 table
  keeps level 0 at its stall limit; the flush goroutine waits for room that no compaction will make, the channel
  behind it fills, and `Close` waited for room in that channel forever (BUG-0381). The node could not be stopped.
  The stall of writes is not changed: it is a documented limitation.
- **Why a patch:** the wait is inside Badger and nothing above it can reach the flush goroutine.
- **Exit:** when an upstream release makes `Close` independent of the level-0 stall, or keeps a table with an
  unreadable block out of compaction, drop it; `badger/stalled_close_test.go` and
  `storage/bug0381_close_stalled_test.go:TestClosingAStoreStalledBehindADamagedTableReturnsAndNamesIt` must stay green.

### Change 6: a write behind a level 0 stalled by a damaged table is refused

- **What changed:** `db.go` (`DB.ensureRoomForWrite` returns the refusal instead of waiting on `flushCond`;
  `DB.WriteRefusal` reports it; `DB.wakeBlockedWriters`), `levels.go` (`levelsController.damagedStallRefusal`;
  `doCompact` wakes blocked writers when it records a failure), each with a modification notice, and one new
  test file, `badger/stalled_write_test.go`. The whole change is
  [`badger-v4.9.6-refuse-writes-behind-damaged-l0.patch`](badger-v4.9.6-refuse-writes-behind-damaged-l0.patch),
  a `patch -p1` diff **after the other five**.
- **Why:** after change 5 the node can be stopped, but a write blocked by the stall still waited forever
  (BUG-0381). The refusal needs both facts, a recorded compaction failure on an unreadable block and level 0
  at its stall limit; there is no timeout, so a slow flush of a healthy store is never refused
  (`TestAWriteBlockedByASlowFlushIsNeverRefusedWithoutACorruptBlock`). It wraps `ErrWritesRefused` and
  `ErrL0StalledByDamagedTable`; the node store words it with the restore advice, and `/ready` reports it.
- **Known edges:** the record is never cleared (the damage is on disk until the node is restored), so a
  damaged table in a deeper level plus a genuinely slow level 0 would be refused too; a writer is woken when a
  compaction fails, and the compactor retries many times a second.
- **Why a patch:** the wait is inside Badger.
- **Exit:** same as change 5. Tests: `badger/stalled_write_test.go`,
  `storage/bug0381_refuse_stalled_write_test.go`, `server/store_damaged_stall_refusal_test.go`.

### Change 7: the log mappings are advised random access

- **What changed:** `memtable.go` (`logFile.adviseAccess`, called with random access by `logFile.open` once the
  file is mapped and by `logFile.Truncate` after every remap; `memTable.UpdateSkipList` keeps readahead for the
  replay of a WAL at open and advises random access after it), with a second modification notice in its header, and one
  new test file, `badger/log_madvise_linux_test.go`. The whole change is
  [`badger-v4.9.6-advise-random-log-mappings.patch`](badger-v4.9.6-advise-random-log-mappings.patch), a
  `patch -p1` diff **after the other six** (it touches `memtable.go`, as the third does, in other hunks).
- **Why:** every durable write ends in `DB.Sync`, an msync of the memory-mapped memtable WAL. A kernel that sizes
  page-cache folios for a file mapping by the readahead window (seen on Linux 7.0, ext4, `read_ahead_kb` 8192)
  grows the folios at the tail of an appended log toward megabytes, and each msync writes whole folios back:
  67 to 80 GB written to seed a 230 MB dataset (BUG-0700). `MADV_RANDOM` turns readahead off for the mapping,
  so a fault allocates one page and an msync writes the pages that changed. `logFile.open` and
  `logFile.Truncate` are the only places a WAL or value-log mapping is created or replaced; the other mappings, the
  discard file and the table files, are not synced per write and are left as they are. Linux's `mremap` keeps the advice; `Truncate` gives it again because on
  other systems `z` unmaps and maps afresh.
- **Cost:** each new page of a log is its own page fault (a zero-fill of the sparse file, counted as a major fault:
  about 117,000 per S-10k seed, no device reads). Value-log reads lose readahead (hoardDB keeps values inline, so
  the value log is rarely read). The WAL replay at open keeps readahead: with the hint it took 1.1-1.4 s cold for a
  256 MiB log against 0.5 s, and that is why `UpdateSkipList` lifts it while it replays (a replayed memtable is
  never appended to again). A refused `madvise` is logged once per process and the mapping is used as it is.
- **Why a patch:** the mappings are created inside Badger; nothing above it can reach them.
- **Exit:** when an upstream release advises its log mappings, or writes its logs with `pwrite` and `fdatasync`
  instead of an msync, bump, drop the patch, and run `badger/log_madvise_linux_test.go` (if the advice is kept)
  and `storage/bug0700_write_amplification_linux_test.go:TestAcknowledgedWritesDoNotAmplifyDiskWrites` on a host
  with large file folios (it skips elsewhere).

### Change 8: a new table can be written back in steps (opt-in)

- **What changed:** `table/table.go` (`CreateTable` syncs through `syncNewTable`; `Options.SyncMode`,
  `Options.SyncStepSize`; the sync error is wrapped with `%w`), three new files `table/table_sync.go`
  (`syncNewTable`, `writeBackInSteps`, `DefaultTableSyncStepSize`), `table/table_sync_linux.go`
  (`sync_file_range(2)`) and `table/table_sync_other.go` (the msync everywhere else), `options/options.go`
  (`options.TableSyncMode`: `TableSyncMsync`, the zero value and default, and `TableSyncStepwise`),
  `options.go` (`Options.TableSyncMode`, `Options.TableSyncStepSize`, their `With` setters and defaults,
  `buildTableOptions`), `db.go` (`checkAndSetOptions` refuses an unknown mode), each with a modification notice,
  and two new test files, `table/table_writeback_test.go` and `table/table_writeback_linux_test.go`, plus
  `TestTableWritebackOptionsReachTheTables` in `options_test.go`. The whole change is
  [`badger-v4.9.6-stepwise-table-writeback.patch`](badger-v4.9.6-stepwise-table-writeback.patch), a `patch -p1`
  diff **after the other seven** (it touches `db.go`, `table/table.go` and `options_test.go`, as earlier ones
  do, in other hunks).
- **Why:** upstream syncs every new table (each memtable flush and each compaction output) with one
  `msync(MS_SYNC)` of the whole file, which is an fdatasync: on ext4 each one waits for a journal commit, and
  the writes behind it stall. Measured with a prototype (PoC runs on 4-vCPU ext4 VMs, 2026-10-26, 6,000 offered
  ops/s open loop, single runs; not yet repeated with this build): write p99 214-335 ms with the msync, 46 ms with
  4 MiB steps and one wait. The stepwise mode writes the data back with
  `sync_file_range(WRITE)` per step and one `WAIT_BEFORE|WRITE|WAIT_AFTER` over the file, and leaves the file's
  metadata to the MANIFEST fsync that every caller makes before a table is used or its inputs are removed.
- **What it relies on:** a filesystem that commits its journal in order, so the MANIFEST fsync also commits
  the table's allocation and size (ext4 with its journal, in its default `data=ordered` mode; XFS). On btrfs,
  ZFS or ext4 without a journal that does not hold, and on ext4 with `fast_commit` it is not established.
- **Opt-in at every level (owner, 2026-10-26):** upstream's msync is the default of `DefaultOptions`, of the zero
  `Options` and of a zero `table.Options` (`TableSyncMsync` is the zero value of `options.TableSyncMode`), and of
  hoardDB's `storage.badger_table_sync`. The filesystem cannot be checked from here and the failure is silent
  until a power cut, so nothing gets the stepwise writeback without asking for it: not a node, not a tool or test
  that opens Badger or builds table options without hoardDB's config. `TestTableWritebackZeroOptionsIsUpstreamsOneSync`,
  `TestTableWritebackOptionsReachTheTables`. What would let the default flip: the decision record's "The default is msync".
- **When the kernel refuses the call:** `ENOSYS`, `EPERM` (a seccomp filter), `EINVAL` or `EOPNOTSUPP` from any
  `sync_file_range`, first step or later, syncs that table with upstream's msync instead, and a process-wide flag
  sends every later table straight to the msync (`table/table_sync.go:isRangedWritebackRefusal`,
  `rangedWritebackRefused`). Any other error, `EIO` above all, fails the table with `%w` and names
  `storage.badger_table_sync: msync`; `CreateTable` then unmaps, closes and removes the half-written file
  (`discardUnsyncedTable`; upstream left all three after a failed msync), so a retried flush does not leak a
  descriptor and a mapping per attempt. Tests: `TestTableWritebackRefusedFallsBackToMsync`,
  `TestTableWritebackRefusalIsRememberedForLaterTables`, `TestTableWritebackFailureLeavesNoFileOrMapping`.
- **Why a patch:** the sync is inside `CreateTable`; nothing above Badger can reach it.
- **Exit:** when an upstream release stops the per-table blocking msync or makes it configurable, bump, take
  the patch out of the procedure below, and run the power-loss check in the decision record before trusting the
  replacement. The table-writeback patch decision record in the hoardDB repository has the power-loss procedure.

### Change 9: `DB.Sync` syncs the active memtable's log outside the DB lock

- **What changed:** `db.go` (`DB.Sync` captures the active and the immutable memtables under one read lock,
  takes a reference on each, releases the lock and then syncs every log; `DB.syncs`, read-locked by `Sync` for
  its whole run and write-locked by `DB.close` around `valueLog.Close`; a `Sync` once `Close` has begun returns
  `ErrDBClosed`; `syncActiveWALHook` and `syncCaptureHook`, test-only seams), with a modification notice in its header, and one new
  test file, `badger/db_sync_lock_linux_test.go`. The whole change is
  [`badger-v4.9.6-sync-active-wal-outside-lock.patch`](badger-v4.9.6-sync-active-wal-outside-lock.patch), a
  `patch -p1` diff **after the other eight** (it touches `db.go`, as several do, and the `DB.Sync` hunk of the
  first).
- **Why:** change 1 moved the immutable memtables' syncs outside the lock but left the active memtable's msync
  (12-20 ms on the test VMs) under `db.lock.RLock`, as upstream has it. A write that rotates the memtable
  (`DB.ensureRoomForWrite` takes `db.lock.Lock`) waited for it, and every read (`DB.getMemTables`) queued behind
  that pending lock, so on a standalone node at default durability most work waited for the msync.
- **Why it is safe:** the decision record's addendum of 2026-10-27 has the argument with citations. In short: the
  one capture covers every entry committed before the call, the reference keeps a concurrent flush, `DropAll` or
  `Close` from unmapping and deleting a log mid-sync, and the write path already appended to the active log beside
  its msync. Because `Close` no longer waits for the msync at `db.lock`, it now waits for `Sync`s in progress before
  it closes the value log; without that, a racing `Sync` waited forever in `valueLog.sync`.
- **Why a patch:** the lock is inside Badger.
- **Exit:** when an upstream release syncs the active log outside the lock (or stops syncing through an msynced
  mapping), bump, drop the patch from the procedure below, and run `badger/db_sync_lock_linux_test.go` on the
  upstream tree (its hook is this patch's; port it or replace it with the release's own seam).

### Change 10: `DB.Sync` syncs the rotated-out memtables' logs before the active one

- **What changed:** `db.go` (`DB.Sync` syncs the logs of the immutable memtables it captured, oldest first, and
  then the active memtable's log, where it synced the active one first; the first memtable log whose msync fails
  ends the syncing of memtable logs in that call, so this call syncs no later log, and the error names that log; `syncActiveWALHook` is now
  reached after the immutable logs are synced and not at all when one of them failed), with a modification notice
  in its header, and one new test file, `badger/db_sync_order_linux_test.go`. The whole change is
  [`badger-v4.9.6-sync-rotated-logs-first.patch`](badger-v4.9.6-sync-rotated-logs-first.patch), a `patch -p1` diff
  **after the other nine** (it rewrites the sync loop of `DB.Sync` that changes 1 and 9 left). It creates the test
  file, so it does not apply on a tree that already has `db_sync_order_linux_test.go`.
- **Why:** synced active-first, a power cut between the active log's msync and a rotated log's kept a later commit
  and lost an earlier one. hoardDB's boot replay skips a record at or below the position its pair record names, so
  an acknowledged write lost that way was never applied again (BUG-0824). Every commit in one memtable precedes
  every commit in the next, so synced oldest first, what `Sync` has forced to disk is a prefix of the commits at
  every instant. A log synced after an older one failed would leave the same state, hence the stop. The stop
  holds for the call that saw the failure, not for the store: a concurrent `Sync` the kernel did not report the
  failure to, and kernel writeback, can still write the later logs.
- **What it does not do:** it orders what `Sync` forces, not what the kernel writes back on its own. Kernel
  writeback can still put the active log's pages on disk before a rotated log's, with or without a `Sync` in
  flight; closing that needs a sync at the rotation (`DB.ensureRoomForWrite`) or a change to hoardDB's replay. It
  does not make the store a prefix after a real write error on an older log either, and a value in the value log,
  which `Sync` still writes last, can be left as a pointer without its bytes (hoardDB's BUG-0827 tracks all three). The
  sync-order patch decision record in the hoardDB repository has
  the invariant, the cut points and what stays open.
- **Cost:** none: the same msyncs in another order. Nothing per bucket.
- **Why a patch:** the order is inside `DB.Sync`.
- **Exit:** with changes 1 and 9: when an upstream release syncs the immutable logs before the active one, or syncs
  a memtable's log when it rotates, drop the patch from the procedure below and run
  `badger/db_sync_order_linux_test.go` on the upstream tree (its hook is change 9's).

### Change 11: a table's bloom filter also answers for key prefixes (an on-disk format change)

- **What changed:** `options/options.go` (`BloomPrefixRule`, `MaxBloomPrefixRuleID`), `options.go`
  (`Options.BloomPrefixRules`, `WithBloomPrefixRules`; `buildTableOptions` hands the rules and the DB's counters
  to every builder), `db.go` (`DB.bloomPrefix`; `checkAndSetOptions` refuses a rule id outside 1 to 32, a repeated
  id and a nil `Len`), `table/builder.go` (`Builder.addHelper` adds the key's prefix hash; `Builder.Done` sizes and
  builds the filter over key and prefix hashes; `Builder.buildIndex` writes the rule bits, only with a filter),
  `table/table.go` (`Options.BloomPrefixRules`, `Options.BloomPrefixCounters`; `TableInterface`;
  `cheapIndex.BloomPrefixRules`, read in `Table.initIndex`; `Table.HasBloomPrefixRule`, `Table.DoesNotHavePrefix`),
  `iterator.go` (`IteratorOptions.BloomPrefix`; `pickTable` and `pickTables` drop a table whose filter does not
  hold the prefix; `Txn.NewIterator` decides whether the prefix may be used), `level_handler.go`
  (`levelHandler.appendIterators` adds its level's tally to the counters), `fb/flatbuffer.fbs` and
  `fb/TableIndex.go` (`TableIndex.bloom_prefix_rules`; the Go file is edited by hand to what `flatc` generates,
  because `flatc` was not installed), each with a modification notice in its header; two new files,
  `prefix_bloom.go` (`DB.acceptBloomPrefix`, `DB.BloomPrefixStats`, the pick's tally) and `table/prefix_bloom.go`
  (`Builder.addPrefixHash`, `BloomPrefixRuleBit`, `BloomPrefixCounters`); `iterator_test.go` (`tableMock` gains the
  two methods of `TableInterface`); and four new test files, `prefix_bloom_test.go`,
  `prefix_bloom_iterator_test.go`, `table/prefix_bloom_test.go` and `table/prefix_bloom_index_test.go`. The whole
  change is [`badger-v4.9.6-prefix-bloom-filter.patch`](badger-v4.9.6-prefix-bloom-filter.patch), a `patch -p1`
  diff **after the other ten** (it touches `db.go`, `iterator.go`, `level_handler.go`, `options.go`,
  `options/options.go` and `table/table.go`, as earlier ones do, in other hunks).
- **Why:** a read of one document, or of one store field's elements, opens a table iterator and seeks in every
  table whose key range covers its prefix, and most of those tables hold no key under it. Under a rule
  (`{ID, Len}`), the builder puts a hash of each key's prefix into the table's bloom filter and records the rule's
  id in the table's index; an iterator that names the prefix (`IteratorOptions.BloomPrefix`) then leaves out every
  table that carries the id and whose filter does not hold the hash. Measured on the read path, 2026-11-15, five
  paired rounds against `next` on the test VMs: a median of +4.97 % requests per second, 57 % of a document read's
  candidate tables left out (the decision record's "Measured" has the run and its limits).
- **It changes the on-disk format, the only one of the thirteen that does.** A table's index has one more field and
  its filter more entries. With no rules (`DefaultOptions`, the zero `Options`) a table is byte for byte
  upstream's. hoardDB sets two rules for every instance it opens (`storage/badger.go:badgerOptions`), so the ring
  format moved to 6 and a ring directory of an earlier build is refused at start
  (`cluster/wire/ringformat.go`). A table without a rule's id (built before the rule, or by an instance without
  it) is never left out for that rule, and that, not the format marker, is what keeps such a table readable.
- **What it relies on, and loses reads silently without:** a rule's `Len` reads only the prefix it returns, so
  every key under a prefix answers that prefix's length; no key is answered by two rules; an id names one `Len`
  for good (a `Len` that changes one answer takes a new id); and the caller reads only keys under `BloomPrefix`
  from a filtered iterator, because the tables left out may hold the newest version of any other key. Badger
  checks the first three only as far as `Txn.NewIterator` can (exactly one rule answers the prefix's whole
  length, none answers it shorter, and it agrees with `Prefix`); the last is hoardDB's
  (`storage/kv.go:DocumentIterOptions`, `StoreIterOptions`).
- **Cost:** 7 bits in the filter (upstream's size per entry) for each run of keys under one prefix in a table,
  none when the prefix is itself a key of the table (a document's header); 6 bytes in each table's index; a length
  decode and a hash per key built; one filter lookup per candidate table of a filtered iterator. Nothing per
  bucket. The counters (`DB.BloomPrefixStats`) are per DB and internal.
- **Why a patch:** tables are built, and picked for an iterator, inside Badger; upstream's own filter probe for
  iterators asks for a whole key (`prefixIsKey`), which is wrong for a document whose newest field sits in a table
  without its header.
- **Exit:** there is no upstream change to wait for. If a release offers a per-table prefix filter with the same
  guarantee, moving to it is a ring format bump (the decision record's "Exit condition"). Until then the tests are
  the patch tests of `scripts/badger-patch-tests.sh` and, with hoardDB's rules,
  `storage/prefix_bloom_test.go:TestPrefixBloomNeverSkipsAPresentDocument`,
  `store/prefix_bloom_test.go:TestPrefixBloomNeverSkipsAPresentElement` and
  `storage/prefix_bloom_unmarked_test.go:TestUnmarkedTableIsNeverSkipped`, which must stay green. The prefix bloom
  patch decision record in the hoardDB repository has the safety argument.

### Change 12: a store can be opened at a vouched version (opt-in)

- **What changed:** `options.go` (`Options.WithVouchedCut` and its two unexported fields), `db.go` (`DB.writtenVersion`,
  raised by `DB.writeRequests` right after `DB.writeToLSM` returns for a request and before its waiters are released;
  `DB.SyncVouched`, which reads it before the capture and then runs `DB.Sync`'s body unchanged, with `DB.Sync` a call
  of it; `syncAfterUnlockHook` right after the `db.lock.RUnlock()` that follows the captures and `syncValueLogHook`
  before the value log's msync, test-only seams that change neither the captures nor the order; `checkAndSetOptions`
  refuses the option with `ReadOnly`, `InMemory` and managed mode; `Open` sets `nextTxnTs` and the written version to
  the larger of `DB.MaxVersion()` and the vouched version and fills the report; `DB.RunValueLogGC`, `DB.DropAll`,
  `DB.DropPrefix` and `DB.BanNamespace` are refused under the option), `memtable.go` (`DB.openMemTables` replays the
  logs under one scan state, counts the logs it cut and removed, fails the open if a log it emptied is still there,
  and fsyncs the directory when it cut anything; `memTable.replayFunction` leaves out every entry above the vouched
  version; `memTable.UpdateSkipList` truncates at the cut point and fsyncs the log), `backup.go` (`DB.Load` refused),
  `stream_writer.go` (`StreamWriter.Prepare` and `PrepareIncremental` refused, because `DB.NewStreamWriter` returns
  no error), each with a modification notice in its header; one new file, `vouched.go` (`VouchedCutReport`,
  `DB.VouchedCutReport`, `ErrVouchedCutRefused`, the scan, the seams `cutSyncFile` and `cutSyncDir`, and the one
  exported test seam `SetSyncTestHooks`); and two new test files, `vouched_cut_test.go` and
  `vouched_cut_linux_test.go`. The whole change is
  [`badger-v4.9.6-vouched-cut.patch`](badger-v4.9.6-vouched-cut.patch), a `patch -p1` diff **after the other eleven**
  (it touches `db.go`, `memtable.go` and `options.go`, as earlier ones do, in other hunks, and the `DB.Sync` hunk of
  changes 1, 9 and 10).
- **Why:** a ring node's store runs with `SyncWrites` off, so after a power cut or a write error its memtable logs can
  hold a later commit without an earlier one (kernel writeback of a newer log, a failed msync of an older one, or a
  value pointer whose value-log bytes never reached the disk), and hoardDB's boot replay, which skips by position,
  never restores the earlier, acknowledged write (hoardDB's BUG-0827). hoardDB records the version each reported sync
  returned (its `VOUCHED` marker file, written by hoardDB after `DB.SyncVouched` returns) and opens the store with it:
  Badger then discards everything above it, truncates each log there and makes that durable before the first commit,
  so the store opens as a real past state and hoardDB replays its own log from a floor below every record above the
  cut. Whatever a flushed table holds is kept: the store opens holding exactly the commits up to the larger of the
  vouched version and the tables' highest. An entry at or below the vouched version after the cut point fails the
  open, naming the log and both versions. The calls refused under the option write at versions they choose or drop
  data outside any commit; the ring store calls none of them.
- **Tests:** `vouched_cut_linux_test.go` (`TestEntriesAboveTheMarkerAreGoneAndStayGone`,
  `TestLargeValueUnderAnUnfinishedSyncIsReplayedWhole`, `TestAFlushedTableVouchesWithoutASync`,
  `TestNoVersionAtOrBelowTheMarkerIsTakenAgain`, `TestSyncVouchedReadsTheVersionBeforeTheCapture`) and
  `vouched_cut_test.go` (`TestTheCutIsDurableBeforeTheFirstCommit`, `TestTheCutIsOptIn`), every one in
  `scripts/badger-patch-tests.sh`. They build each image from copies of the store's files taken at a named instant,
  never by rewriting a file.
- **Cost:** none per bucket, per document or per store field. One atomic load and one atomic store per write request,
  with or without the option; one atomic load per sync. At open, under the option, one comparison per replayed entry
  (the replay the open already makes) and, only when the cut discarded something, one fsync per truncated log and one
  of the directory.
- **What it relies on, and loses silently without:** the version is read **before** the capture (a version read after
  it can vouch a commit that returned into a memtable rotated in after the capture, whose log that call never syncs);
  the written version is raised after the request is in the memtable and before its waiters are released; the patch
  line in the procedure below, because a re-apply of the vendored tree without it drops the cut and nothing fails
  until a power cut; hoardDB writes its marker before `badger.Open` creates the first file and only after
  `DB.SyncVouched` returned; and every call that writes out of version order is refused under the option (a new one
  in an upstream release must be added to the refusals).
- **The flushed log's retirement (added 2026-11-18, hoardDB's BUG-0858; applies to every store on disk, with or
  without the cut).** *What changed:* `levels.go` (`levelsController.addLevel0Table` split into `nameLevel0Table`,
  the synced MANIFEST change, and `insertLevel0Table`, the level-0 insert after the stall wait; `addLevel0Table`
  calls both, unchanged for its other uses), `db.go` (`DB.handleMemTableFlush` on disk is `DB.flushToNamedTable`:
  table, MANIFEST, the log's removal, level 0; `Open` calls `DB.removeLogsOfNamedTables` after the levels are loaded
  and before compactions and the flusher start, a step added with nothing in `Open` moved; `DB.close` calls
  `DB.releaseNamedTables`; `DB.flushMemtable` calls the flush test hook), `memtable.go` (`memTable.logRetired`,
  `memTable.named`, `memTable.retireFailed`; the skiplist's `OnClose` releases a retired log with
  `logFile.closeRetired` instead of deleting it), `vouched.go` (`VouchedCutReport.LogsOfNamedTables`), each with a
  modification notice; one new file, `flushed_log.go` (`DB.retireMemTableLog`, the seams `retireRemoveLog` and
  `retireSyncDir`, `DB.removeLogsOfNamedTables`, `DB.releaseNamedTables`, `logFile.closeRetired`, a second
  exported test seam, `SetFlushTestHook`, a third, `SetFlushStepHook` (called at the five steps of a flush so a test
  can copy the directory at each crash point), and the package variable `openCompactionsHook`); three new test files,
  `flushed_log_retire_test.go`, `flushed_log_retire_linux_test.go` and `flushed_log_order_test.go`; and seven sync tests adapted (`TestSyncCoversImmutableMemtables`, the two
  rotated-log order tests' setups, `TestSyncActiveWALKeepsTheLogThroughAFlush`,
  `TestSyncActiveWALReturnsWhenCloseRacesIt`, `TestSyncActiveWALCoversARotationDuringTheSync`,
  `TestSyncActiveWALCaptureIsOneSnapshot`), which held the flusher at the level-0 stall or looked for a flushed log
  by its name. *Why:* a flushed memtable's log was removed only when its last reference went, with no directory
  fsync, and nothing synced it at the flush, so a reader or a sync capture kept it on disk after its table was in
  level 0, where a compaction can drop a delete marker with the versions under it. Replayed after a crash, the log
  brought the deleted key back: after a power cut because it could hold the set without the delete, and after a
  plain kill under the cut, because the cut keeps the log's entries at or below the vouched version and discards the
  delete above it. *The order:* table synced, MANIFEST change synced, log unlinked, directory fsynced, table into
  level 0; a crash inside that window leaves the log beside a table no compaction has read, and `Open` removes such a
  log (every entry at or below the tables' highest version) before any compaction starts: replayed, it would be
  flushed into a second table that can outlive a compaction of the first. Every open then fsyncs the directory
  before compactions start, whether or not it removed a log: an unlink an earlier process applied only in the page
  cache (a sticky fsync failure, an open that failed after its own unlink) is no longer visible to this open.
  *Failures:* the memtable whose log is retired is never the active one (the flusher takes rotated memtables only;
  `DB.DropPrefix` rotates the active memtable out before it flushes it, since flushed in place a failed removal left
  writes acknowledged afterwards in a memtable whose table was already built). Once the MANIFEST names the table, a
  retry never builds a second one (`memTable.named`); a failed unlink is retried; a failed directory fsync is kept for
  the life of the process (a later fsync can succeed without the unlink having reached the disk), so the table stays
  out of level 0, the memtable keeps serving reads, change 4 refuses writes, `Close` closes and keeps the table and
  says so, and the next `Open` removes the log if it still sees it and fsyncs the directory either way. **A failed
  MANIFEST change during a flush (added 2026-11-19, hoardDB's BUG-0868; the same failure in a compaction,
  `levelsController.runCompactDef`, is not covered and is filed as its own bug):** `manifestFile.addChanges` applies the change in memory and
  writes it before the sync, so a failed sync may leave the MANIFEST naming the table; upstream's failure path then
  deleted the file and the retry named a second table, and the store did not reopen ("file does not exist for table
  1"). The flush now keeps the table file and never retries the naming (`memTable.failedNaming`, `memTable.nameFailed`,
  the same failed-fsync argument as the directory's): every retry returns the error, change 4 refuses writes, the
  memtable's log stays, `Close` closes and keeps the table, and the next `Open` finds either the named table (it
  removes the log) or an unnamed one (it deletes it, or hoardDB's guard sets a first flush's aside, and replays the
  log). The patch file carries it (regenerated 2026-11-19 with the S-5 seams; `scripts/badger-patch-check.sh`). A failed open
  step fails `Open` with no DB and an error that wraps its cause. *Tests:* `TestAFlushedLogIsGoneBeforeItsTableIsVisible`,
  `TestALogLeftBesideItsNamedTableIsNotReplayed`, `TestTheRemovalIsDurableBeforeTheTableIsInALevel`,
  `TestACrashInsideTheRemovalLeavesTheKeyDeleted`, `TestAFailedDirectoryFsyncNamesNoSecondTable`,
  `TestAFailedDirectoryFsyncIsNotRetriedIntoSuccess`,
  `TestAManifestSyncFailingOnceInAFlushLeavesAStoreThatReopens` (BUG-0868, below),
  `TestAFailedUnlinkIsRetriedWithoutASecondTable`,
  `TestARetiredLogServesItsReaderAndAParkedSync`, `TestAFailedRemovalInsideDropPrefixLosesNoAcknowledgedWrite`,
  `TestTheOpenSyncsTheDirectoryBeforeCompactionsStart` (which also fails when the step moves after the compactions start),
  `TestAFailedOpenStepReturnsNoStoreAndItsCause`, `TestManagedModeKeepsTheLogOfACommitBelowTheTables`,
  `TestALegacyDropPrefixKeepsTheVersionOrderTheOpenStepRelies`, every one in
  `scripts/badger-patch-tests.sh`, each on both opens where both apply (`DropPrefix` is refused under the cut); with
  hoardDB's rules, `storage/vouched_stale_log_test.go`'s three `TestADeleteInAFlushedTableStaysDeleted…` tests (the
  vouched open's power cut and kill, and the legacy open) and
  `storage/vouched_stale_log_lowered_test.go:TestADeleteStaysDeletedWhenACompactionLowersTheTablesHighestVersion`.
  *Cost:* one unlink and one directory fsync per flush, on the flusher, off the write path, and one directory fsync per
  open; one atomic store per flush and one atomic load when a memtable's last reference goes; at open, one
  comparison per replayed log. Nothing per bucket, document or store field. *Relies on:* the order of the flush's
  steps (`flushToNamedTable`); `removeLogsOfNamedTables` relies on commit versions in write order, which the calls
  refused under the cut break (`DB.RunValueLogGC`, `DB.Load`, the stream writer, `DB.BanNamespace`) and managed mode
  breaks (the step is skipped there); hoardDB's legacy store calls none of them (it calls `DB.DropPrefix`, which
  keeps versions), and a new caller of one of them on a store opened without the cut would turn the step into a loss
  of that caller's writes.
- **The log-file count (added 2026-11-19, hoardDB's slice S-3 of the vouched cut, BUG-0860):** `DB.LogFilesCreated`
  returns a count that `DB.newMemTable` and `valueLog.createVlogFile` move when they create a file, before any commit is
  written to it, and that `Open` starts at 1 (`db.go`, `memtable.go`, and `value.go`, which gains a modification notice).
  hoardDB writes its `VOUCHED` marker in place (two slots, one fdatasync, no rename) and fsyncs the store directory
  only when the count, read after `DB.SyncVouched` returned (so after its version), moved since its last directory
  fsync: every file that holds a commit at or below that version was created, and counted, before the version was
  published, so its directory entry is durable before the marker vouches it. *Test:*
  `TestLogFilesCreatedMovesWithEveryNewLogFile` (in `scripts/badger-patch-tests.sh`); hoardDB's
  `storage/vouched_slots_test.go` holds the caller's side. *Cost:* one atomic add per file created, one atomic load per
  sync. *Relies on:* every log and value-log file being created through those two functions; a new place that creates
  one must move the count too.
- **Upstream status:** none. The cut exists for a caller that keeps its own log to replay from; upstream has no such
  caller, and nothing has been sent. The flushed log's retirement fixes upstream's behaviour too (a power cut while a
  reader held a flushed memtable); nothing has been sent.
- **Why a patch:** the replay at open, the logs' truncation and the commit versions are inside Badger; nothing above it
  can drop an entry from a memtable log before the store serves.
- **Exit:** when an upstream release opens a store as a prefix of its commits by itself, or hoardDB turns `SyncWrites`
  on; then drop the patch from the procedure below and run the spec's T1, T2, T10 and T12 against the release. The
  vouched-cut patch decision record, kept with hoardDB's design records, has the alternatives, among them an exported
  `DB.WrittenVersion()`, and the exported test seam's reason.

### Change 13: a flush syncs the value log before its table is named

- **What changed:** `db.go` (`DB.flushToNamedTable` calls `valueLog.sync` before it builds the table once the value
  log is open; `DB.valueLogOpen`, closed by `Open` after `valueLog.open`; `Open` calls `DB.syncValueLogFilesAtOpen`
  after `DB.removeLogsOfNamedTables`, a step added with nothing moved), `value.go`
  (`valueLog.sync` msyncs through `syncValueLogFile`), `memtable.go` (`logFile.doneWriting` msyncs through it;
  `logFile.syncFailed`), `vouched.go` (`SyncTestHooks.AfterValueLogFileSync`), each with a modification notice; one new
  file, `value_log_sync.go` (`syncValueLogFile`, which keeps a file's failed msync and returns it from every later
  one; `DB.syncValueLogFilesAtOpen`; the seams `valueLogFileSync` and `valueLogFileSyncAtOpen` and the hook behind
  `AfterValueLogFileSync`); one new test file,
  `flush_value_log_linux_test.go`. The whole change is
  [`badger-v4.9.6-sync-value-log-at-flush.patch`](badger-v4.9.6-sync-value-log-at-flush.patch), a `patch -p1` diff
  **after the other twelve** (it touches `db.go`, `memtable.go`, `value.go` and `vouched.go` in hunks of changes 3 and 12).
- **Why:** a named table vouches every value pointer it holds, whatever hoardDB's marker says (change 12: the store
  reopens holding every commit up to the larger of the marker and the tables' highest version), and upstream's flush
  syncs no value log. A power cut after the MANIFEST named the table could keep the table and lose the value-log bytes
  its pointers name; the value then read back empty with no error (`VerifyValueChecksum` is off). hoardDB's values of
  1 MiB or more are in the value log, and under a write load a memtable is flushed within seconds of the write, well
  inside the kernel's writeback delay, so this was the normal case for such a value (BUG-0827, route (c) through a
  flush; the vouched cut spec's T3b).
- **What it syncs, and why that is enough:** the memtable is immutable, so its pointers name bytes written before the
  flush: in the newest value-log file, which `valueLog.sync` msyncs whole, or in a rotated one, which
  `logFile.doneWriting` synced before the newest file id moved (change 3; a failed rotation leaves the file the newest).
  The sync runs before the table is built, so a failure leaves no table file to clean up. When the value log created
  a file since the last such fsync (`valueLog.filesCreated`, read before the msync), the flush also fsyncs the value
  directory: a file's msync does not commit its directory entry on every filesystem (`DB.syncValueLogForFlush`; a
  failed directory fsync is kept on the DB the same way). **Precondition: `SyncWrites` off**, as hoardDB opens every
  store (`storage/badger.go`, pinned by `storage/vouched_sync_writes_test.go:TestTheRingStoreRunsWithSyncWritesOff`):
  with it on, `valueLog.sync` returns without an msync and `valueLog.write`'s own msync only logs a failure.
- **A failed msync is kept** (`logFile.syncFailed`): the kernel can drop the dirty pages of a failed writeback and
  report the error once, so a retry's msync can succeed with the bytes gone. Kept, a flush retry never names the table
  (change 4 refuses writes after three failures, as for any flush that keeps failing), and a later `DB.Sync` fails
  instead of reporting those bytes durable (hoardDB would write a marker that vouches them). The next `Open` replays
  the memtable logs. **Which msyncs are kept:** every msync of a value-log file made by `valueLog.sync` (a store sync,
  a flush), by `logFile.doneWriting` (a rotation) and by `logFile.Truncate` (a request that grows the newest file past
  its preallocation, a rotation's shrink, the open's truncation of the last file): `Truncate` msyncs a value-log file
  through the keep before `z.MmapFile.Truncate`, whose own msync then finds no page this goroutine dirtied. Not kept:
  the msync `valueLog.write` defers under `SyncWrites` (off in hoardDB).
- **Carried, not fixed (a lock change):** on the growth path `valueLog.write` calls `logFile.Truncate` without
  `logFile.lock`, while `valueLog.sync` msyncs `curlf.Data` under its read lock; a flush's msync racing the remap can
  fail (`ENOMEM`), and the keep turns that one failure into refused writes until a restart. Trigger: one request whose
  value-log bytes pass the newest file's preallocation (2 × `ValueLogFileSize`, 256 MiB at hoardDB's 128 MiB, with
  rotation after 128 MiB), so one transaction carrying more than about 128 MiB of values of 1 MiB or more; such a
  request was the same race for a `DB.Sync` before, as a one-off failed sync.
- **The open:** `Open` starts the flusher and hands it the memtables it replayed before it opens the value log, and the
  hand-over blocks when they outnumber the flush queue, so their flush can neither use the value log nor wait for it
  (a wait deadlocked `TestARefusedWriteIsAbsentAndAnAcknowledgedOnePresent`). Their pointers need durable bytes too: a
  killed process leaves value-log bytes in the page cache. So when the replay left memtables, `Open` fsyncs every
  value-log file in the value directory before the flusher starts (`DB.syncValueLogFilesAtOpen`; a failure fails
  `Open`), and a flush that finds the value log not yet open skips its own sync: nothing writes the value log before
  `Open` returns. Calling `valueLog.sync` there instead read the value log's state while `Open` wrote it (`-race`) and,
  finding no file, synced nothing.
- **Tests:** `flush_value_log_linux_test.go` (`TestLargeValueVouchedByAFlushIsWhole`, the store half of T3b, with the
  value in the newest and in a rotated value-log file; `TestAFlushSyncsTheValueLogBeforeNamingItsTable`, exactly one
  msync per flush before the naming, with and without a value pointer; `TestAFailedValueLogSyncInAFlushIsNotRetriedIntoSuccess`;
  `TestAMemtableReplayedAtOpenSyncsTheValueLogBeforeItsTable`; `TestAFailedValueLogSyncAtOpenFailsTheOpen`;
  `TestAFailedValueLogMsyncOutsideAFlushIsKept`, at a rotation and at a growth;
  `TestAFlushSyncsTheValueDirectoryAfterAValueLogFileWasCreated`), every one in `scripts/badger-patch-tests.sh`. Each
  image of a power cut takes every value-log file at the prefix its last msync made durable
  (`SyncTestHooks.AfterValueLogFileSync`). hoardDB's `cluster/wire/vouched_acceptance_test.go:TestLargeValueVouchedByAFlushIsWhole`
  is T3b at node level and `storage/vouched_prefix_property_test.go:TestStoreReopensAsAPrefix` (T10) builds images
  holding a table flushed after the last store sync.
- **Cost:** one value-log msync per flush, on the flusher goroutine, off the write path, whether or not the memtable
  holds a pointer: it writes the value-log pages dirtied since the last msync (none, for a store whose values are all
  under the threshold) and costs the filesystem's sync of the file. Host-dependent: the review's probe of one file
  mapping measured 3 ms with nothing dirty, 8 ms at 1 MiB, 15 ms at 16 MiB, 80 ms at 64 MiB and 188 ms at 128 MiB (load
  3.6 to 5); this builder's dev-host runs gave 11 to 27 ms at 0 to 1 MiB under heavier load. **Worst case per flush
  about 0.19 s** (a whole 128 MiB file dirty). Not measured on the test VMs or the rig. **Added latency on the write
  path:** `valueLog.sync` holds the file's read lock for the msync, and a rotation's `doneWriting` takes its write lock,
  so a rotation that coincides with a flush's msync waits for it, and Go's `RWMutex` queues new value reads behind that
  pending writer: bounded by one msync, rare. Also one directory fsync per flush that follows a new value-log file (once
  per 128 MiB of value bytes) and one extra msync, of clean pages, per rotation (`Truncate`'s). A flush happens once per memtable (64 MiB on the ring
  store, about every 5 s at the rig's write rate). Nothing per bucket, document or store field; one atomic load per
  value-log msync. At an open that replayed memtables, one fsync per value-log file.
- **Upstream status:** none; nothing has been sent. Upstream's own exposure is the same (a flushed table can name
  value-log bytes no sync made durable) and its `SyncWrites` closes it at a write cost.
- **Why a patch:** the flush and the value log are inside Badger; a caller cannot run code between a flush's table and
  its MANIFEST change.
- **Exit:** when an upstream release syncs the value log before a flushed table is named (or hoardDB turns
  `SyncWrites` on): drop the patch from the procedure below and run the five tests above and hoardDB's T3b against the
  release. The vouched-cut patch decision record has this change under "Change 13".

### Re-applying on a Badger upgrade

Whoever bumps Badger owns this. Until upstream has the change:

    cd hoardDB
    go mod download github.com/dgraph-io/badger/v4@vNEW
    rm -rf third_party/badger
    cp -r "$(go env GOMODCACHE)/github.com/dgraph-io/badger/v4@vNEW" third_party/badger
    chmod -R u+w third_party/badger
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-sync-immutable-memtables.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-corrupt-block-errors.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-sync-outgrown-vlog.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-flush-failure-refuses-writes.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-close-stalled-l0.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-refuse-writes-behind-damaged-l0.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-advise-random-log-mappings.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-stepwise-table-writeback.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-sync-active-wal-outside-lock.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-sync-rotated-logs-first.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-prefix-bloom-filter.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-vouched-cut.patch)
    (cd third_party/badger && patch -p1 < ../badger-v4.9.6-sync-value-log-at-flush.patch)
    # set the require line in go.mod to vNEW, keep the replace, then:
    go test -count=1 -run TestBadgerSyncCoversARotatedMemtable ./storage/
    go test -count=1 -run TestACorruptTableBlockIsAnErrorNotAMiss ./storage/
    go test -count=1 -run TestADamagedStoreFileReadIsAnErrorNamingTheBucket ./server/
    go test -count=1 -run TestANodeThatCannotPersistRefusesWritesAndStops ./server/
    go test -count=1 -run TestClosingAStoreStalledBehindADamagedTableReturnsAndNamesIt ./storage/
    go test -count=1 -run TestAWriteBehindADamagedLevelZeroTableIsRefusedWithTheRepair ./storage/
    go test -count=1 -v -run TestAcknowledgedWritesDoNotAmplifyDiskWrites ./storage/   # skips on small-folio hosts
    go test -count=1 -run 'TestPrefixBloomNeverSkipsAPresentDocument|TestUnmarkedTableIsNeverSkipped' ./storage/
    go test -count=1 -run TestPrefixBloomNeverSkipsAPresentElement ./store/
    go test -count=1 -run 'TestVouchedMarkerFile|TestTheMarkerMovesOnlyOnAReportedSync' ./storage/
    go test -count=1 -run 'TestADeleteInAFlushedTableStaysDeleted|TestADeleteStaysDeletedWhenACompactionLowersTheTablesHighestVersion' ./storage/
    go test -count=1 -run 'TestAFirstWriteAboveTheCutIsReplayedWithoutAnAnchor|TestAZeroCursorPairsFirstWriteIsReplayedWithoutAnAnchor' ./cluster/wire/
    go test -count=1 -run 'TestLargeValueVouchedByAFlushIsWhole' ./cluster/wire/
    ../scripts/badger-patch-tests.sh

`scripts/badger-patch-tests.sh` runs the named patch tests in `third_party/badger` and `./table/` with
`TMPDIR` on a real disk (it fails on tmpfs and on a skipped test), and is gate 14 of `scripts/ci.sh`
(BUG-0479). **The test names live in that script's `PATCH_TESTS` and nowhere else: a new patch adds its
test there once.**

If a patch does not apply, read the functions it changes in the new version before forcing it: for the
first, `DB.Sync` and `DB.ensureRoomForWrite`; for the second, `levelHandler.get`, `ConcatIterator.Next`,
`MergeIterator.fix` and `Table.block`; for the third, `logFile.doneWriting` and `logFile.Truncate`; for the
fourth, `DB.flushMemtable`, `DB.ensureRoomForWrite` and `DB.close`; for the fifth, `DB.close`, `levelsController.addLevel0Table` and `levelsController.doCompact`; for the sixth, `DB.ensureRoomForWrite` and `DB.WriteRefusal`; for the seventh, `logFile.open`, `logFile.Truncate`, and any new place that maps a log file; for the eighth, `table.CreateTable` and each caller of it (`DB.handleMemTableFlush`, `levelsController.subcompact`, `sortedWriter.createTable`), every one of which must still fsync the MANIFEST before the table is used; for the ninth, `DB.Sync`, `DB.close`, and every place that sets `db.mt` or removes a memtable from `db.imm` (each must hold `db.lock.Lock`, and `close` must set `isClosed` under it before `db.mt` goes nil); for the tenth, `DB.Sync` and every place that adds to `db.imm` (each must append, so that `db.imm` stays in rotation order, oldest first); for the eleventh, `Builder.addHelper`, `Builder.Done`, `Builder.buildIndex`, `Table.initIndex`, `IteratorOptions.pickTable`, `IteratorOptions.pickTables`, `levelHandler.appendIterators`, `fb/flatbuffer.fbs` (a field upstream added to `TableIndex` goes before `bloom_prefix_rules`, and that moves the ring format), and any new place where a key enters a table or a table is picked for an iterator; for the twelfth, `memTable.replayFunction`, `memTable.UpdateSkipList`, `DB.openMemTables`, `DB.writeRequests` (where `writtenVersion` is raised: after the request is in the memtable, before its waiters are released), `DB.Sync` and `DB.SyncVouched` (the version is read before the capture), `Open`'s setting of `nextTxnTs`, `DB.newMemTable` and `valueLog.createVlogFile` (each must move `logFilesCreated` before a commit is written to the file it creates), and each refused call (`DB.RunValueLogGC`, `DB.Load`, `DB.DropAll`, `DB.DropPrefix`, `StreamWriter.Prepare`, `StreamWriter.PrepareIncremental`, `DB.BanNamespace`, managed mode in `Open`), plus any new call that writes at a version it chooses or drops data outside a commit, which must be refused under the cut too, and, for its flushed log's retirement, `DB.handleMemTableFlush`, `levelsController.addLevel0Table` (the MANIFEST change must stay synced before the log's removal, and the level-0 insert after its directory fsync), the skiplist `OnClose` in `DB.openMemTable`, `Open` between `newLevelsController` and the start of compactions, and `DB.close` after `stopMemoryFlush`, and `DB.DropPrefix`, which must rotate the active memtable out before it flushes it; for the thirteenth, `DB.handleMemTableFlush` and `DB.flushToNamedTable` (the value log is synced before the table is built, and so before the MANIFEST names it), `valueLog.sync`, `logFile.doneWriting` and any new place that msyncs a value-log file (each goes through `syncValueLogFile`, which keeps a failure), any new place that rotates a value-log file (the rotated file must be synced before the newest id moves), and `Open` (`DB.syncValueLogFilesAtOpen` before the flusher starts; `DB.valueLogOpen` closed after `valueLog.open`; a flush must never wait for it, since `Open` blocks handing replayed memtables to the flusher). Upstream may have fixed the hole another way, which is that change's exit
condition. Regenerate the patch file under the new version's name and update its decision record.

`scripts/badger-patch-check.sh` applies the thirteen patches, in the order above, to upstream v4.9.6 from the
module cache and fails unless the result is `third_party/badger` (upstream's `.vscode` aside). Run it after
any edit under `third_party/badger`; when it fails, regenerate the patch of the change you edited as the diff
from upstream plus the patches before it to the tree, and the patches after it if they no longer apply (added
2026-11-19, review R2 F-8: the vouched-cut patch had dropped S-5's seams and the BUG-0868 fix). An edit of change
12's code goes into `badger-v4.9.6-vouched-cut.patch` and changes 13's context: regenerate both.
