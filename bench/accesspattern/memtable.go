package main

import "github.com/hoarddb/hoarddb-server/storage"

// benchMemTableMB is the memtable every store in this bench opens with: the
// 32 MiB the access-pattern results (bench/RESULTS.md, 2026-09-25) were
// measured at. It is a measurement pin, not the shipped default — storage no
// longer has one — so a re-run reproduces those results.
const benchMemTableMB = 32

// benchTuning is the Badger tuning every OpenBadgerKV call in this bench
// passes, so no call can fall back to a size the run did not record.
func benchTuning() storage.BadgerTuning {
	return storage.BadgerTuning{MemTableMB: benchMemTableMB}
}
