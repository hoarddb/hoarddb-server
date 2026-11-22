# wiremicro: single-document microbenchmark across engines

**Internal use only.** The numbers this tool produces for CockroachDB, MySQL and MongoDB are never published
(owner rule). Result files stay in the private tree and are never committed here. Every result
file carries `"internal_only": true`.

One invocation drives one engine: `-engine hoarddb|cockroach|mysql|postgres|mongo`. The workload is identical for all four:
a table, collection or bucket keyed by a 36-character UUIDv7-style string (`_id_`; MongoDB's own key `_id` holds it)
and one payload field of `-payload` bytes (default 256). Phases, in this order, each at least 20000 operations by
default (`-ops`; parallel phases split the operations across `-workers` connections, default 16, one connection each):

1. `insert_serial`: 1 connection
2. `insert_parallel`: `-workers` connections
3. an unmeasured preload of `-preload` documents (default 1000)
4. `get_serial` and `get_parallel`: get by id of a preloaded document, random id per operation

Each phase reports ops/s and p50, p99, p99.9 and max latency (nearest rank, microseconds). The table goes to stdout,
the JSON to `-out` (default `wiremicro-<engine>-<run-id>.json`).

`-phases insert_serial` (a comma list, default all) runs only those phases; the get phases preload first.
For a crash check on hoardDB, `-ledger <file>` appends every acknowledged insert id to a client-side file with an
fsync per ack; after a server `kill -9` and restart on the same data directory,
`-verify-ledger <file>` (same `-addr`, `-engine hoarddb`) gets every id and exits non-zero if any is missing.
It cannot simulate a power cut: a missing fsync survives `kill -9`.

## Durability: what is asked for, what is read back, what is refused

An insert is a durable acknowledged write. Before the first write the tool reads the settings back from the server
(not from its own flags) and prints them. If one is relaxed it exits without writing anything. The relaxed
counterparts are the rows of the durability table in [`../social/HARNESS.md`](../social/HARNESS.md); this tool
checks the durable column:

| Engine | Durable setting | Read back as | Refused when |
| --- | --- | --- | --- |
| hoardDB | `HOARDDB_UNSAFE_ACK_BEFORE_SYNC` unset; insert reply `write_outcome` is `applied` | the server's launch environment file (`-hoard-env`); the server reports none of its settings on the wire | the file sets it true, or a reply is not `applied` |
| hoardDB table sync | `HOARDDB_BADGER_TABLE_SYNC` stated with `-hoard-table-sync` (`stepwise` first, then `msync` as a labelled control) | the same file, if it sets the variable; the server does not report it on the wire, so the run states it | the file and the flag disagree, or the flag is missing |
| MySQL | InnoDB autocommit, `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1` | `@@innodb_flush_log_at_trx_commit`, `@@sync_binlog` | either is not 1 |
| PostgreSQL | autocommit, `synchronous_commit=on`, `fsync=on`, `full_page_writes=on` | `current_setting()` of each | any is not `on` |
| MongoDB | write concern `w:1, j:true` | the concern set on the client every insert uses; `serverStatus.storageEngine.persistent` | `j` is false, `w` is 0, or the engine is in-memory |
| CockroachDB | server default (log synced before commit) | `kv.raft_log.disable_synchronization_unsafe` | it is `true` |

Every hoardDB run states its table-sync mode: the server's mode is set on the server with
`HOARDDB_BADGER_TABLE_SYNC` (`stepwise` or `msync`), and `-hoard-table-sync` must say which one it is. Run stepwise
first. `-datadir-fs-note 'free text'` records the filesystem and disk of the server's data directory in the result
(the tool cannot see another machine's disk).

## Addresses and credentials

Nothing is fixed in code or scripts. Use role names from the VM inventory in your shell, never addresses in a file:

| Setting | Flag | Environment |
| --- | --- | --- |
| host:port | `-addr` | `WIREMICRO_ADDR` |
| user | `-user` (defaults: hoardDB `admin`, MySQL and CockroachDB `root`) | `WIREMICRO_USER` |
| password | `-password` (prefer the environment: a flag shows in `ps`) | `WIREMICRO_PASSWORD` |
| full connection string (competitors) | `-dsn` | `WIREMICRO_DSN` |
| hoardDB certificate pin | `-hoard-fingerprint` | `WIREMICRO_HOARD_FINGERPRINT` |

hoardDB is always TLS (AUTH as the bootstrap user, certificate unchecked unless pinned). The competitor recipes below
are plaintext; add `-tls` for an encrypted connection with the certificate unchecked, and say which you ran.

## Running each engine on the benchmark VMs

Build once, from `github.com/hoarddb/hoarddb-server/` in the tree under test, and copy the binary to the client VM:

    go build -o <dir>/wiremicro ./bench/wiremicro

All four are run from the client VM against the server VM, with the same `-ops`, `-workers`, `-payload` and
`-preload`. Run them one after another, never while another benchmark uses the same VM.

**hoardDB** (server started by the usual benchmark scripts, for example `scripts/bench/single-node-profile.sh`, with
the environment written to a file; the unsafe ack variable must not be in it):

    WIREMICRO_PASSWORD=<bootstrap password> <dir>/wiremicro -engine hoarddb -addr <server>:<tcp port> \
      -hoard-env <launch env file> -hoard-table-sync stepwise -datadir-fs-note '<fs and disk>' \
      -out hoarddb-stepwise.json

Each run creates its own database (`wiremicro_<run-id>`) because hoardDB has no drop; use `-run-id` to name it.

**MySQL** (docker recipe: the `mysql` line of the private competitor single-node script,
without `--innodb-flush-log-at-trx-commit=2`; the server default is 1, and the tool refuses 2):

    WIREMICRO_PASSWORD=<root password> <dir>/wiremicro -engine mysql -addr <competitor>:3306 \
      -datadir-fs-note '<fs and disk>' -out mysql.json

**PostgreSQL** (a server started with the defaults of 17; the tool refuses `synchronous_commit=off` and `fsync=off`;
`-tls` asks for `sslmode=require`, the default is `disable`; the user needs CREATEDB or the database must exist):

    WIREMICRO_PASSWORD=<postgres password> <dir>/wiremicro -engine postgres -addr <competitor>:5432 \
      -datadir-fs-note '<fs and disk>' -out postgres.json

**MongoDB** (the `mongo` line of the same script; its `w1j` concern is what this tool sets on the client):

    <dir>/wiremicro -engine mongo -addr <competitor>:27017 -datadir-fs-note '<fs and disk>' -out mongo.json

**CockroachDB** (the `cockroach` line of the same script, run with `BENCH_WITH_COCKROACH=1`, or the node of
the private `docker-compose.cockroach.yml` on a laptop; both insecure single nodes):

    <dir>/wiremicro -engine cockroach -addr <competitor>:26257 -datadir-fs-note '<fs and disk>' -out cockroach.json

Remove the containers and volumes the recipes created when the runs are done. Settings for the containers live in the
script and the compose file; they are not repeated here so they cannot drift.

`-smoke -ops 200` runs a quick wiring check; its output says `SMOKE (not a result)` and must never be filed as one.

## Tests

`go test ./bench/wiremicro -race -count=1` needs no server. The live tests are skipped unless
`WIREMICRO_LIVE_MYSQL`, `WIREMICRO_LIVE_MONGO`, `WIREMICRO_LIVE_COCKROACH` or `WIREMICRO_LIVE_HOARDDB` holds a
host:port (hoardDB also needs `WIREMICRO_LIVE_HOARD_ENV` and `WIREMICRO_LIVE_HOARD_TABLE_SYNC`, and
`WIREMICRO_PASSWORD` for it and for MySQL).

## What it does not do

No warm-up phase and no think time: each connection issues its next operation as soon as the last one is
acknowledged (closed loop). One document per operation, no batching. Results from a laptop are not results.
