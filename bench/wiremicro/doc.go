// Command wiremicro is a like-for-like single-document microbenchmark of one
// engine per invocation: hoardDB (over the real TLS listener, RING_ADMIN and
// RING_KEYED), CockroachDB, MySQL, PostgreSQL or MongoDB, every one at durable settings.
//
// The workload is the same for every engine: a table, collection or bucket
// keyed by a 36-character UUIDv7-style string with one payload field of
// -payload bytes. Four phases run in order, each reporting ops/s and the
// p50, p99, p99.9 and max latency of at least 20000 operations: a serial
// insert, a parallel insert (-workers connections), then a serial and a
// parallel get by id over -preload preloaded documents. An insert is a durable
// acknowledged write: a hoardDB reply with write_outcome applied, a MySQL
// InnoDB autocommit with innodb_flush_log_at_trx_commit=1 and sync_binlog=1, a
// MongoDB w:1 j:true write, a CockroachDB commit at its default.
//
// Before the first operation the tool reads back the durability settings of
// the server it is about to measure, prints them, and refuses to run when one
// is relaxed. hoardDB reports none of its settings on the wire, so the run
// states them: -hoard-env names the file the server was launched with
// (HOARDDB_UNSAFE_ACK_BEFORE_SYNC must not be true) and -hoard-table-sync
// states HOARDDB_BADGER_TABLE_SYNC (stepwise first, then msync). The result is
// one JSON file and one human table. Results are internal: competitor figures
// are never published.
//
// See README.md in this directory for the command line of every engine.
package main
