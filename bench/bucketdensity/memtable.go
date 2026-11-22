package main

import "github.com/hoarddb/hoarddb-server/storage"

// benchMemTableMB is the memtable every store in this bench opens with: the
// 32 MiB VER-17 and VER-19 were measured at. It is a measurement pin, not the
// shipped default — storage no longer has one, the server resolves a size — so
// a re-run reproduces the per-bucket figures those measurements published.
const benchMemTableMB = 32

// benchTuning is the Badger tuning every OpenBadger/OpenBadgerKV call in this
// bench passes, so no call can fall back to a size the run did not record.
func benchTuning() storage.BadgerTuning {
	return storage.BadgerTuning{MemTableMB: benchMemTableMB}
}
