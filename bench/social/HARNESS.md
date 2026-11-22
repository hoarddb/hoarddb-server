# bench/social: the flags the shell harness calls

`bench/social` is one binary. The certification's shell harness (`social.sh`) calls it and
reads one JSON line from stdout (or from the file named by `-out`, appended). A mode that
checks something exits 1 when the check fails, and the line it printed says why. This file
is the contract: `TestHarnessFlagContract` fails when a flag listed here is missing from
`-help`, when an example below does not parse, or when a flag the harness needs is not
listed.

A payload change not shown here is a change the harness cannot see: when a flag is renamed
or removed, this file changes in the same commit.

## Modes

`-mode` says what the process does. Without it, `-mix` runs one leg (it seeds first unless
`-attach` is given); without either it runs the older timeline workload.

| `-mode` | What it does | Prints | Fails when |
| --- | --- | --- | --- |
| `seed` | creates the schema and seeds `-dataset` (refuses a database that has one) | `seed_s`, `setup_s`, `bulk_load_posts_per_s` | the seed fails |
| `warm` | attaches, runs `-mix` unmeasured in `-warm-window` windows until two consecutive windows differ by under 5% (at least `-warm-min`, at most `-warm-max`; `-warm-short` is 5 minutes) | `warm`, `window_ops_per_s` | a read is wrong. A warm-up that did not settle prints `"warm": false` and exits 0 (spec 6.6) |
| `run` | attaches and measures one leg of `-mix`; appends a `perf-record/2` line | the leg record | a read is wrong, or hoardDB's launch files contradict `-level` (a P6 integrity failure). Request errors are counted in the record; the harness checks them |
| `parity-hash` | reads the sampled users and hashes them in canonical form (spec P4) | `sample_hash`, `expected_hash`, `matches_generator`, counts | the read fails. The harness compares `sample_hash` across engines |
| `converge` | waits until every node returns the sampled users as the first node does (spec P5) | `converge_seconds`, per-node `converged_after_s` | a node has not caught up after `-converge-timeout` |
| `witness` | times 200 depth-1 writes in a scratch namespace and compares the median with `-sync-ms`, the cheapest data-only flush on the disk (spec 3.3, coordinator ruling 2026-10-24); it passes at 0.3 of that figure or above (`witness_min_ratio`); `-sync-overwrite-ms` and `-sync-append-ms` are printed beside it with their ratios | `depth1_write_p50_ms`, `single_sync_ms_p50`, `sync_overwrite_ms_p50`, `single_sync_append_ms_p50`, `p50_over_single_sync`, `witness_min_ratio` | the median is under 0.3 of `-sync-ms`, or hoardDB's launch files contradict `-level`. At `L-B` a fast write passes, marked `expected_relaxed` |
| `syncprobe` | measures three sync figures in `-sync-dir`: a 4 KiB pwrite plus fdatasync to a preallocated block (the data-only flush), and 4 KiB writes opened with O_DSYNC to a preallocated file overwritten in place and to a file that grows | `sync_datasync_ms_p50`, `sync_overwrite_ms_p50`, `sync_append_ms_p50` | the directory cannot be written. `social.sh witness` runs it on the server VM and passes `sync_datasync_ms_p50` as `-sync-ms` |
| `ledgercheck` | reads back the ledger a leg wrote with `-ledger-out` (`-ledger <file>`): for each author, the highest acknowledged post must be on the author's timeline; the post that was in flight at the reset is reported present or absent and never fails the check; `-ledger-owner <node>` limits it to the authors that node owns | `authors`, `checked`, `skipped`, `found`, `in_flight_present`, `in_flight_absent`, `missing`, `pass` | an acknowledged post is missing; the line names the author and the sequence |
| `walcheck` | reads a copy of a stopped node's plain write-ahead log (`-wal-dir`) and requires every author's highest acknowledged post in `-ledger` to be in it, under the author's bucket; the in-flight post is reported and never fails; `-ledger-owner` limits it as for `ledgercheck`. It opens the copy, which repairs its tail, so never point it at a live node's directory | the same fields as `ledgercheck` | an acknowledged post is not in the log |
| `tlscheck` | proves the connections the load client opens are encrypted (spec P6) | one finding per server | a connection is not encrypted |
| `clientcpu` | calibrates the client: CPU per operation and resident memory at 64 and 128 workers | `client_cpu_us_per_op`, `client_rss_mb` | the calibration fails |
| `soak` | runs the `-mix` (sk) on the seeded dataset (`-attach`, open loop at `-rate`) as a resumable soak in windows of `-soak-window`; it samples the server through `/proc` and `-metrics-addr`, so it runs on the server VM | window records | the soak fails |
| `driftreport` | prints the drift report of a soak directory | the report | the directory has no soak |
| `merge-hist` | merges the `.hist` files named after the flags, one per client VM of a two-client leg, and prints the percentiles of their union (n.a. under 1,000 samples); `-out` appends the line to a file | `count`, `p50`, `p95`, `p99`, `p999`, `max` in ms | a file is missing or is not a histogram |

## Flags

Every flag below has an example that parses; the examples are one argument list each.

| Flag | Meaning | Example |
| --- | --- | --- |
| `-mode` | what to run, from the table above | `-mode seed -dataset S-10k` |
| `-engine` | `hoarddb`, `mongo`, `mysql`, `postgres`, `cockroach` or `ring` (hoardDB over the entry-ring wire, in the entity-centric layout: see "Ring leg") | `-engine mysql -mode parity-hash` |
| `-dataset` | `S-10k`, `S-100k` or `S-1M` | `-mode parity-hash -dataset S-100k` |
| `-placement` | the placement form hoardDB lays the data out in and the leg records: `h1` (one `User` bucket, the default), `h64` (`User_00` to `User_63`) or `hu` (one `User_<id>` bucket per user, recorded as placement `per-entity`). `h256` is phase 2 (build step S41) and refused; `-engine mongo`, `-engine mysql`, `-engine postgres`, `-engine cockroach` and `-engine ring` take `h1` only, their single layout (see "Placement"); a ring leg is recorded as placement `entity-documents` | `-mix rh -placement h64` |
| `-shards` | shard count recorded with the leg; 0 takes it from `-placement` | `-mix rh -shards 1` |
| `-addr` | hoardDB nodes, comma-separated; the first is the entry node under `-routing a`. `-engine ring` drives exactly one | `-addr n1:7433,n2:7433,n3:7433 -mode converge` |
| `-routing` | `a` sends every connection to one node (RP-A); `b` spreads reads over all nodes (RP-B) | `-mix rh -routing b` |
| `-mix` | `rh`, `wh`, `sk` or `op:<ID>` | `-mode run -mix op:R3` |
| `-loop` | `closed` or `open` (open needs `-rate`) | `-mix rh -loop open -rate 3000` |
| `-depth` | operations in flight: workers, and the pool size of the competitors | `-mix wh -depth 64` |
| `-login-concurrency` | logins this process has in flight at once (default 16); the sockets of a `-depth` burst wait their turn, since a default node refuses about a hundred logins in flight from one address. A login refused as RATE_LIMITED is retried with backoff up to 8 times, then fails the run naming this flag | `-mix rh -depth 128 -login-concurrency 16` |
| `-seconds` | measured length in seconds | `-mix rh -seconds 300` |
| `-rate` | offered operations per second in total, for an open loop | `-mix rh -loop open -rate 1500` |
| `-client-vms` | split `-rate` and `-depth` evenly over this many client VMs | `-mix rh -depth 64 -client-vms 2` |
| `-client-index` | which client VM this process is | `-mix rh -depth 64 -client-vms 2 -client-index 1` |
| `-repeat` | which back-to-back repeat this is, recorded in the leg | `-mix rh -repeat 2` |
| `-seed` | seed of the workload generators | `-mix rh -seed 7` |
| `-out` | append the JSON line here instead of printing it | `-mode run -mix rh -out leg.json` |
| `-series-out` | write one JSON line per second of the leg to this file: `{"s":<second>,"reads":<n>,"writes":<n>}`, seconds counted from the start of the measured window (warm-up seconds are negative), the last partial second also carrying `len_s`, then one closing line `{"per_client":{"reads":[...],"writes":[...]}}` with each load client's measured counts; written even when the leg ends in an error, so a killed leg keeps its rate; refused without `-mix`; it judges nothing | `-mix wh -series-out series.jsonl` |
| `-variant` | name of a record-only variant, recorded in the leg | `-mix rh -variant cache-raised` |
| `-fanout` | how MongoDB, MySQL, PostgreSQL and CockroachDB write the feed entries of a publish: `single` (the default: one write per follower, one after another, as hoardDB does) or `bulk` (one bulk write, the old behaviour); hoardDB refuses `bulk`; recorded as `fanout` in the leg record | `-engine mysql -mix op:W1 -fanout bulk` |
| `-hoard-batch` | send each hoardDB operation's sub-operations as one BATCH request (publish, like, follow, unfollow, comment, post page); a record-only variant, recorded as `variant` `batch` and refused beside any other `-variant` or another engine; never the comparison of record | `-engine hoarddb -mix op:W1 -hoard-batch` |
| `-ring-fingerprint` | pin the ring node's certificate to this fingerprint (the server prints it at start); without it the certificate is unchecked, as for the other hoardDB engine (`-engine ring`) | `-engine ring -ring-fingerprint 0a1b2c3d` |
| `-ring-serial` | send the independent requests of one ring operation (the post documents of a timeline, a profile's counts) one after another, as the other legs do, instead of in flight together on the connection; recorded as `ring_serial` in the leg's engine settings (`-engine ring`) | `-engine ring -mix rh -ring-serial` |
| `-ring-no-project` | read whole documents (`get`, which reads every store field's state) where the ring leg by default asks `get_fields` for only the scalars an operation uses (post and profile documents, the avatar, the avatar upload's read); request counts per operation are the same either way; recorded as `ring_no_project` in the leg's engine settings (`-engine ring`) | `-engine ring -mix rh -ring-no-project` |
| `-ring-batch`, `-ring-batch-size N` | send a publish's (W1) feed copies as RING_BATCH frames of N pushes (default 64, 1 to 255) instead of one request per follower; a frame with a not-attempted operation is resent whole under its request id, a definitive refusal is counted and fails the publish. Refused with `-ring-fanout-parallel`. Recorded as `ring_batch` and `ring_batch_size`; the engine prints the frame size, counts, per-op latency and ops/s when the leg closes (`-engine ring`) | `-engine ring -mix wh -ring-batch -ring-batch-size 64` |
| `-ring-fanout-parallel`, `-ring-fanout-width N` | push a publish's (W1) feed copies to the followers in flight together on the connection, at most N at a time (default 8), each under its own request id and retried alone, under that id, when its outcome is unknown; a follower named twice is pushed in order. Off by default (one push after another, as the other legs do), so earlier numbers stay comparable; report both modes. Recorded as `ring_fanout_parallel` (and `ring_fanout_width`) (`-engine ring`) | `-engine ring -mix wh -ring-fanout-parallel -ring-fanout-width 8` |
| `-config` | configuration `C1` to `C5b`; empty takes C1, C2 or C3 from the node count, so say it for five nodes and for a two-member MongoDB set with an arbiter (three hosts, C2) | `-mix rh -config C5a` |
| `-copies` | copies of the data kept; 0 means one for a single node and the node count otherwise, so say it where they differ (the arbiter holds none; hoardDB at RF=3 on five nodes) | `-mix rh -copies 2` |
| `-level` | durability level claimed: `L-A`, `L-B`, `L-R`, `L-R-all` | `-mode witness -sync-ms 2.9 -level L-A` |
| `-write-concern` | hoardDB write concern sent on every mutation that can carry one: `one`, `majority`, `all` | `-mode seed -write-concern all` |
| `-hoard-env` | the environment hoardDB was launched with | `-mode witness -sync-ms 2.9 -hoard-env launch.env` |
| `-hoard-log` | hoardDB's server log, for the unsafe-ack WARN line | `-mode witness -sync-ms 2.9 -hoard-log server.log` |
| `-sync-ms` | the witness reference: the latency in ms of the cheapest **data-only flush** on the server's disk, a 4 KiB pwrite to a preallocated block followed by fdatasync. It must be the `sync_datasync_ms_p50` of `-mode syncprobe`. Never an O_DSYNC figure: those also commit file metadata, which the server's own sync does not, and one of them as the reference fails a correct durable engine (hoardDB at L-A ran at 0.61 to 0.82 of the data-only flush, the unsafe path at 0.04 to 0.09) | `-mode witness -sync-ms 2.9` |
| `-sync-overwrite-ms` | the O_DSYNC overwrite of a preallocated block (`sync_overwrite_ms_p50`), recorded beside `-sync-ms` in the witness line and in the leg record's `witness`; never the reference | `-mode witness -sync-ms 2.9 -sync-overwrite-ms 3.2` |
| `-sync-append-ms` | the O_DSYNC write that extends a file (`sync_append_ms_p50`), recorded beside `-sync-ms` in the witness line and in the leg record's `witness`, with its ratio; never the reference | `-mode witness -sync-ms 2.9 -sync-append-ms 4.1` |
| `-witness-p50-ms` | with `-mode run`: the depth-1 write median the witness printed (`depth1_write_p50_ms`); the leg's `witness` records it with its ratio to `-sync-ms` | `-mode run -mix rh -sync-ms 2.9 -witness-p50-ms 1.8` |
| `-sync-dir` | with `-mode syncprobe`: a directory on the disk under test | `-mode syncprobe -sync-dir .` |
| `-ledger-out` | the file a `-mix` leg writes its acknowledged-write ledger to when it ends, even when the connection dropped mid-run: per author the highest acknowledged post, the post in flight, the bucket and its owner node | `-mix wh -attach -ledger-out ledger.json` |
| `-ledger` | with `-mode ledgercheck`: the ledger file to check | `-mode ledgercheck -ledger ledger.json` |
| `-ledger-owner` | with `-mode ledgercheck`: check only the authors whose bucket this node owns, for a node that answers reads for the buckets it owns only | `-mode ledgercheck -ledger ledger.json -ledger-owner node-a` |
| `-wal-dir` | with `-mode walcheck`: a copy of a stopped node's plain `wal` directory | `-mode walcheck -ledger ledger.json -wal-dir wal-copy` |
| `-mongo-uri` | connection string; name every member of a replica set | `-engine mongo -mongo-uri mongodb://a:27017,b:27017,c:27017/social?replicaSet=rs` |
| `-mongo-wc` | MongoDB write concern: `w1j`, `w1`, `majority` (a `w=` or `journal=` in the URI wins and is recorded) | `-engine mongo -mongo-wc majority` |
| `-mongo-read-pref` | read preference; empty follows `-routing` (primary under a, nearest under b) | `-engine mongo -mongo-read-pref secondaryPreferred` |
| `-mongo-tls` | TLS with the certificate unchecked, on by default | `-engine mongo -mongo-tls=false` |
| `-mysql-dsn` | the source first, then replicas, comma-separated | `-engine mysql -mysql-dsn u:p@tcp(a:3306)/social,u:p@tcp(b:3306)/social` |
| `-mysql-read-routing` | `primary` or `roundrobin`; empty follows `-routing` | `-engine mysql -mysql-read-routing roundrobin` |
| `-mysql-tls` | `tls=skip-verify` unless the DSN names `tls=`, on by default | `-engine mysql -mysql-tls=false` |
| `-postgres-dsn` | one `postgresql://` URL naming the database, with the password in the URL or in `PGPASSWORD`; the engine drives a single server, so a comma is refused. The database is created when missing | `-engine postgres -postgres-dsn postgresql://bench@db:5432/social` |
| `-postgres-tls` | on by default: `sslmode=require` (encrypted, certificate unchecked) unless the URL names an `sslmode`; `-postgres-tls=false` is the record-only TLS-off leg | `-engine postgres -postgres-tls=false` |
| `-cockroach-dsn` | one `postgresql://` URL naming the database; the engine drives a single node, so a comma is refused. The database is created when missing | `-engine cockroach -cockroach-dsn postgresql://root@db:26257/social` |
| `-cockroach-tls` | `sslmode=require` (encrypted, certificate unchecked) unless the URL names an `sslmode`; **off by default** because the recipe's node is insecure, so a default leg is the record-only TLS-off leg | `-engine cockroach -cockroach-tls` |
| `-verify-safety` | check reads in the safety form, for replicated routing | `-mix rh -routing b -verify-safety` |
| `-attach` | reuse the seeded dataset instead of seeding (a `-mix` run without `-mode`) | `-mix rh -attach` |
| `-hist-prefix` | write the histograms to `<prefix>-read.hist` and `<prefix>-write.hist` | `-mix rh -hist-prefix rh-c64` |
| `-metrics-addr` | the server's Prometheus listener, read by the soak for goroutine counts | `-mode soak -metrics-addr localhost:9100` |
| `-sample` | users the parity hash and the convergence check read (default 1,000) | `-mode parity-hash -sample 200` |
| `-converge-timeout` | how long a node may take to catch up (default 120 s) | `-mode converge -converge-timeout 120s` |
| `-warm-short` | the 5-minute warm-up used after a restart or a reseed | `-mode warm -mix rh -warm-short` |
| `-warm-converged` | with `run`: false when the preceding warm-up did not settle, recorded as `warm=false` | `-mode run -mix rh -warm-converged=false` |
| `-summarize` | print the reading tables of the leg-record files named after the flags | `-summarize leg1.json leg2.json` |
| `-csv` | with `-summarize`, print CSV | `-summarize -csv leg1.json` |

## Two client VMs: merging the latency

With `-client-vms 2` each client writes its own `<prefix>-read.hist` and `<prefix>-write.hist`.
A record built from the two leg records can sum counts, rates and errors, but the percentiles of
the union are not those of either client nor their average. Merge the histograms:

    social -mode merge-hist -out merged-read.json c1/leg-read.hist c2/leg-read.hist
    social -mode merge-hist -out merged-write.json c1/leg-write.hist c2/leg-write.hist

Each prints one line, `{"mode":"merge-hist","files":[...],"count":N,"p50":..,"p95":..,"p99":..,"p999":..,"max":..}`
in milliseconds, with the percentiles `null` under 1,000 samples. The harness that combines two
client records takes its read and write percentiles from these two lines and drops the
`latency-unmerged` label.

## Fault events and the cluster files

`social.sh fault hoarddb E1|E2|E3|E4` restarts, removes and joins nodes through `cluster.sh`, which keeps the
cluster's members in `$CERT_RUN/members`: the cluster has to have been started with `cluster.sh` (or
`engines.sh start hoarddb 3 ...`, which calls it) from the same controller, or `remove` and `join` do not know
the members. `social.sh` follows that file: before every fault, and after the event's own remove or join, the
VMs it records for hoardDB are the members, so the load client, canaries and samplers never address a removed
node. E2 leaves two nodes, and `fault` refuses with fewer than three: rebuild the three (`engines.sh start
hoarddb 3 VM1 VM2 VM3 --fresh`, then `social.sh ensure-seeded hoarddb <dataset> <placement>`, the dataset and
placement the cluster had) before E3. That rebuild empties the data and the reseed is long (about 28 minutes for
S-10k on the live cluster), so **run E3 before E2**, or give E2 a cluster of its own; E1 and E4 only restart a
node and can run in any order.

Prerequisites, checked before anything is touched (each failure prints the command that fixes it):

- `social.sh witness hoarddb VM1`: every leg, so every fault leg, is refused without a durability witness.
- An offered rate: `--rate N` (open loop, operations per second), or the closed legs of the mix on this
  configuration (`social.sh run hoarddb WH closed 64 300 rpb 1`, three repeats), after which the rate is half the
  common rate or half the engine's own closed median.
- A seeded dataset, and for E2 and E3 a primary bucket whose owner can be read: `CERT_FAULT_DB` and
  `CERT_FAULT_BUCKET` default to `social` and, by the placement the dataset was seeded with, `User` under `h1`
  (the dataset's one bucket, `userBucketName`) or `User_00` under `h64` (which has no `User`, only `User_00` to
  `User_63`). An explicit `CERT_FAULT_BUCKET` wins over the placement. E2 refuses before it kills anything when
  no node prints the owner.

### Units, evidence directories and fresh starts (T23, T24, T28, T29)

The legs of T23, T24, T28 and T29 are written under the unit names `T23-legs-<variant>`, `T24-legs-<variant>`,
`T28-legs-<variant>` and `T29-legs`, never under the evidence directory (`T23-<variant>` and so on), because
`run-test.sh` deletes the evidence directory when the test is run again directly; the 6-hour soak files of T29
would go with it. `report.sh` and `social.sh` skip a kept earlier attempt (`<unit>.failed-<n>`) so its legs are
not counted twice.

T23 and T24 seed their own data, and every topology and placement form uses one data directory per VM, so both
start the engine with `--fresh` (the data directories are emptied; the installed binaries are not): the second
form (h64 after h1, or h1 after h64) and a rerun start from nothing. T29 (the soak) seeds its own data on VM1 and
also starts `--fresh`, so it never inherits T22a's h1 seed or an earlier soak's. T37 already starts `--fresh`.
T28 inherits T24's cluster and data and starts without `--fresh`; `social.sh ensure-seeded` seeds only if the data is absent and refuses a placement the
data is not laid out in.

E3 joins the VM named by `CERT_JOIN_VM` (default VM4), which must be in the inventory and have the server binary
deployed. The owner and node list of the primary bucket come from the `Nodes:` line of `describe bucket`: each
node is asked in turn and the first answer is taken (a build may print it on every node or only on the owner;
trying each is harmless). `cluster.sh join` returns once the member is added and the settle wait that follows
ends; handoffs can still be running (a live join took about 52 s and its handoffs settled 3.5 s after), so E3
waits for `hoarddb_handoffs_in_progress` to read 0 on every node before it reads the counters, and an unreadable
gauge is a failure, never 0.

E1 and E4 count the catch-up from the restart until `hoarddb_cluster_peers_unreachable`,
`hoarddb_replication_peers_not_in_sync` and, where the build exports them, `hoarddb_replication_lag_entries` and
`_lag_seconds` all read 0 on every node, twice running. The record keeps when the node went down, was started and
answered, and whether any of those gauges was ever above 0. When none was, the figure is the polling floor and
is labelled `no-catchup-observed`.

After a write leg the data is no longer what the generator makes (WH and RH both write), so a parity check
against the generator cannot pass after a fault. The check that follows a fault is `social.sh converge`
(every node returns what the first one does) and the bucket entry counts. The E2 probe documents are deleted
afterwards. Fault legs are written under `<unit>/faults/legs/`, not the calibration or table trees. Everything
copied into the evidence (logs inside the server-log tars, `hook.out`, the cluster scripts' echoes) is scrubbed
of machine addresses.

## A quiet host

Certification runs need a quiet host. The VMs share one physical machine: a leg measured while other
tests run on it drifts (a live leg drifted 63% on the fsync canary). `social.sh` measures the canary
before and after every leg and labels a drift over 10% `canary-drift`; the label says the numbers are
not comparable, it does not make them so. Run nothing else on the host during a certification leg.

## What a record says

A leg record carries the labels the flags set: `config`, `nodes`, `level`, `routing`
(`RP-A` or `RP-B`), `write_concern`, `copies`, `placement` and `shards`. `placement` is
`single`, `shard-64`, `shard-256` or `per-entity` (spec 9.2); a record is never labelled
with a placement the data was not laid out in. `engine_settings` holds what the engine
reported, including hoardDB's launch facts (`replication_factor`, `write_concern`,
`unsafe_ack_before_sync`, `unsafe_ack_warning_in_log`) and the variant name.

`fanout` is `single` or `bulk` (`-fanout`) and says how the MongoDB and MySQL engines wrote the feed
entries of a publish (W1, and the W1 share of any mix): `single` sends one write per follower, one after
another on one connection, exactly like hoardDB's sequence (same entries, same order, no transaction round
them, so each is its own autocommit write on MySQL); `bulk` sends the post and then every feed entry in one
`insertMany` or one multi-row `INSERT` inside one transaction. `req_per_op` of W1 counts the requests sent,
so it is 1 + 1 + N (follower lookup, post, N feed entries) in `single` and 3 in `bulk` on both
competitors. A figure is never read without `fanout`: the summariser keeps the two modes in separate
groups and prints `[fanout bulk]` or `[fanout single]` beside the placement. hoardDB's engine has one way to
publish and records `single`; a record written before the flag has no `fanout` and was `bulk`.

`ops.W1.fanout_mean` and `ops.W1.resume_requests_mean` (hoardDB's engine only, W1 only: absent from other
operations, from the MongoDB and MySQL engines in both `-fanout` modes, where a publish is not 2 requests plus one
per follower, and from records written before the fields) split W1's requests. A counted publish sends 2
requests (the post, and the one page of followers: a user has at most 500 followers, `graph.go`
`followerCap`, and a page holds 1000), plus one per follower it copies to (`fanout_mean`, the mean
followers per publish), plus one to resume the author's post numbering from the newest stored post the
first time the run publishes as that author (`resume_requests_mean`, between 0 and 1; it grows with the
number of distinct authors the leg draws). So `req_per_op - fanout_mean - resume_requests_mean` is 2 unless a
code change adds a round trip, while the other two follow the data: followers accumulate as the leg runs, so
a faster node publishes to more of them. `scripts/perf-gate.sh` judges the difference.

## Placement

A bucket is the unit of placement in hoardDB (one owner and its replicas), so the layout of the users decides
whether a cluster can spread writes at all. Two forms are built (spec 6.2):

- `h1`: the single `User` bucket. The default, and the one every configuration measures first.
- `h64`: the same `User` declaration (nested stores per document, hash typed, no DDL comments) created 64
  times as `User_00` to `User_63`. A user lives in shard `xxHash64(user id) mod 64`, the hash the ring uses,
  computed by the client from the decimal user id; `TestShardFunctionIsStable` pins it to fixed vectors.

The same seed, the same dataset and the same operations run over either form, and the data is the same:
`-mode parity-hash` over `h64` prints the same `sample_hash` as over `h1` and the generator's
`expected_hash` (`TestH64CanonicalHashEqualsH1AndTheGenerator`). Every operation routes a user through the
shard. An operation that touches several users (the fan-out of W1, the merge of M1) sends its sub-operations
one after another, grouped by shard; M1 joins the lists in the order it was given so the answer is H-1's.
Pass the same `-placement` to `seed`, `warm`, `run` and `parity-hash`: a run over data laid out in another
form finds no bucket. Through `social.sh` the placement is the one the dataset was seeded with.

A leg over `h64` records `owners_per_node` and `holders_of_primary_bucket`, read from `describe bucket` for
each of the 64 buckets (the replica set, owner first): `owners_per_node` counts the buckets each node owns,
in the order of `owner_node_ids` (the server's node ids, sorted; the id is not a VM name, so the controller
maps it), and `holders_of_primary_bucket` is the replica set of `User_00`. The ring does not split 64 buckets
evenly over five nodes (spec 6.2 measured 22/9/11/11/11), so a curve over `h64` is never described as
scale-out without this map beside it. A server that names no nodes (a single, unclustered one) is recorded
as one node, `standalone`, owning all 64. `h1` records carry no map: one owner by construction.

`hu` is built (one `User_<id>` bucket per user; its shard count is the dataset's user count). `h256` is phase 2
(S41). The placement is a shard count (`placement.go`), so `h256` is a table row and its bucket names, not new
code, when the owner opens that phase.

MongoDB and MySQL have one layout each. `social.sh` and the flags accept `h1` for them, meaning "that single
layout", and refuse every other form.

A write concern reaches a hoardDB mutation only where its request type has a
`write_concern` field: field puts, pushes, deletes and pops, plain puts and pushes. An
update (`adjustCounts` in W3 and W4), a queue claim and a queue ack have none on the wire
and run at the node's default.

## PostgreSQL leg

`-engine postgres` is PostgreSQL over its own wire (`github.com/jackc/pgx/v5` through `database/sql`, the driver the
CockroachDB leg uses): the same nine tables, keys and operations as the MySQL leg, the same request counts, the same
canonical form for `parity-hash`, the same `-depth` pool of exactly that many connections, the same open and closed
loops and the same `-fanout single` default. Its tables and statements are in `engine_postgres*.go`.

The schema is the one a PostgreSQL user would write, with the reason above each table in `engine_postgres_seed.go`:
`BIGINT` ids, `TIMESTAMPTZ(3)`, `JSONB`, `BYTEA`, one `CREATE INDEX` statement per secondary index (`follows` by
followee, `jobs` by outbox), no foreign keys. A PostgreSQL table is a heap, so a primary key is a b-tree beside it and
there is no clustered key as in InnoDB; the harness does not run `CLUSTER`, which does not stay ordered under writes.

What differs in behaviour, because the server differs:

- **Retries.** The default isolation, READ COMMITTED, raises no serialization failure, but two transactions that lock
  rows in opposite orders (W3 and W4 move two counters, parallel seed workers write neighbouring keys) can deadlock, and
  the server aborts one with SQLSTATE `40P01`. The engine runs it again, and a `40001` too, up to 20 attempts, each
  attempt's statements counted in `req_per_op`. The seed's retries are the leg's note `seed_deadlock_retries=<n>`.
- **TLS on by default**, like MongoDB and MySQL. `-mode tlscheck` asks the server (`pg_stat_ssl`, one row per backend)
  what each of the `-depth` connections negotiated.
- **Settings read back.** `fsync`, `synchronous_commit`, `full_page_writes`, `wal_sync_method`, `wal_writer_delay`,
  `shared_buffers`, `effective_cache_size`, `max_wal_size`, `max_connections` and `ssl` come from `pg_settings` and are
  recorded with the leg. `fsync = off` is an integrity failure and fails the leg; `synchronous_commit = off` is the
  record-only relaxed level and is only recorded.
- **Placement and routing.** `h1` only, one server, `RP-A`.

### Running the leg

The server is installed and started by the certification scripts (`scripts/certification/engines.sh`). By hand:

```
social -mode seed -engine postgres -postgres-dsn postgresql://bench:<password>@<host>:5432/social -dataset S-10k
social -engine postgres -postgres-dsn postgresql://bench:<password>@<host>:5432/social -dataset S-10k -mix rh -out postgres.json
```

`SOCIAL_LIVE_POSTGRES_DSN` (a URL for a server where the account may `CREATE DATABASE`) switches on the live tests of
`engine_postgres_test.go`, which run in a scratch database they drop afterwards; without it they skip.

## CockroachDB leg

`-engine cockroach` is the Postgres-wire competitor beside MongoDB and MySQL: the same social schema (nine tables,
same keys, no foreign keys), the same operations and request counts, the same canonical form for `parity-hash`, the
same `-depth` pool of exactly that many connections, the same open and closed loops and `-fanout single` default.
It uses `github.com/jackc/pgx/v5` through `database/sql`, so the pooling code mirrors the MySQL engine's. Its tables
and statements are in `engine_cockroach*.go`; the types differ only where CockroachDB has no equivalent (`INT8` for
ids, `TIMESTAMPTZ(3)`, `JSONB`, `BYTEA`).

What differs in behaviour, because the server differs:

- **Restarts.** CockroachDB runs every transaction serializable and aborts one of two contending transactions with
  SQLSTATE `40001`, asking the client to run it again. The engine does, up to 20 attempts, each attempt's statements
  counted in `req_per_op`. The seed's INSERT retries are the leg's note `seed_deadlock_retries=<n>` (the note keeps
  the MySQL name; it counts restarts here), and an error that outlasts the bound is recorded in `errors_by_type` as
  `server_postgres_40001` (the prefix names the wire, so a PostgreSQL deadlock is `server_postgres_40P01`).
- **One node, no TLS by default.** The recipe starts `cockroach start-single-node --insecure`, which does not speak
  TLS. A default leg therefore connects in the clear and is recorded with `tls=false`; `-mode tlscheck` reports it as
  unencrypted and fails, as it must. The comparison of record needs a secure node (`--certs-dir`) and `-cockroach-tls`;
  that recipe is not built here.
- **Placement and routing.** `h1` only, one node, `RP-A`. A multi-node CockroachDB cluster is not built.

### Durability, side by side

All three are asked for a synced log write before the acknowledgement. Nothing is weakened for CockroachDB.

| | hoardDB (L-A) | MongoDB | MySQL | CockroachDB |
| --- | --- | --- | --- | --- |
| Setting used | `HOARDDB_UNSAFE_ACK_BEFORE_SYNC` unset | `-mongo-wc w1j`: `w:1, j:true` | `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1` (the defaults) | none: the server default. The write-ahead log is synced before a commit is acknowledged |
| What the harness reads back | launch environment and server log | write concern | both variables | `version()` and `kv.raft_log.disable_synchronization_unsafe` (must be `false`; `true` is an integrity failure) |
| The relaxed counterpart | `HOARDDB_UNSAFE_ACK_BEFORE_SYNC=true` | `w1` (journal every 100 ms) | `innodb_flush_log_at_trx_commit=2` | none offered by the harness |

`scripts/bench/competitor-single-node.sh` runs MongoDB (`w1j`) and MySQL with `innodb_flush_log_at_trx_commit=2`, the
relaxed pair of the single-node profile. CockroachDB has no relaxed setting to match that pair and is not weakened to
fit it: its figure there is a durable one, set against two relaxed ones, and the table above says so. Compare it
with MySQL at 1/1 and MongoDB at `w1j`, or against hoardDB at L-A.

### Running the leg on the benchmark VMs

On the competitor VM (4 vCPU, 16 GB, the disk the other competitors use), one container, data on a named volume,
cache and SQL memory set explicitly so it does not take the VM or run starved. CockroachDB's defaults are a small
storage cache (`--cache`) and a quarter of RAM for SQL working memory (`--max-sql-memory`); against MySQL's 8 GB
buffer pool the small cache would understate it, so both are set to 4 GiB (half the 16 GB VM together, the rest left to
the page cache and the OS). Record the values with the leg:

```
docker run -d --name bench-cockroach --network host -v bench-cockroach-data:/cockroach/cockroach-data \
  cockroachdb/cockroach:v25.2.0 start-single-node --insecure --listen-addr=:26257 --http-addr=:8081 \
  --cache=4GiB --max-sql-memory=4GiB
```

`docker-compose.cockroach.yml` at the repository root is the same node for a laptop, with a 1 GiB cache and SQL memory. On a client VM, with the harness
binary built from this tree:

```
social -mode seed -engine cockroach -cockroach-dsn postgresql://root@<role:competitor VM>:26257/social \
  -dataset S-10k -placement h1 -fanout single
social -engine cockroach -cockroach-dsn postgresql://root@<role:competitor VM>:26257/social -dataset S-10k \
  -placement h1 -fanout single -mode run -mix wh -depth 64 -warmup 5s -seconds 30 -routing a -loop open -rate 6000 \
  -out cockroach.json
```

`BENCH_WITH_COCKROACH=1 scripts/bench/competitor-single-node.sh` runs the same two commands after MongoDB and MySQL
and prints the same summary and disk lines. The first start takes a while to accept SQL; the script waits for it.
Remove the container and volume afterwards (`docker rm -f bench-cockroach; docker volume rm bench-cockroach-data`).
The Go tests for the engine need no server; set `SOCIAL_LIVE_COCKROACH_DSN` to a node's URL to run their live halves.

### Licence and publishing question for the owner

The server is not a dependency of the module (it runs in docker only); the driver, `pgx`, is MIT. The server is not
open-source: it is under the "CockroachDB Software License" (cockroachlabs.com/cockroachdb-software-license, read
2026-10-31, no version or date shown on the page). Quoted, not interpreted:

- It requires a License Key for production use: "You will not operate the Software or a Modified Version without a
  License Key except for Limited Non-Production Use", which covers "design, prototyping, testing, or development
  purposes in non-production, internal development environments" and "deploying a database that runs solely on a
  single node".
- On benchmarks: "If You perform or disclose, or direct or permit any third party to perform or disclose, any
  Benchmark," You "will include in any disclosure and will disclose to Licensor all information necessary to
  replicate such Benchmark", and "Licensor may perform and disclose the results of benchmarks of Your products or
  services".
- It does not address offering the software as a hosted service; the grant is limited to "operating, evaluating,
  testing, fixing, integrating with, and improving CockroachDB".

Questions for the owner, not answered here: does a single-node benchmark VM count as Limited Non-Production Use; does
publishing a figure make the repository a "Benchmark" disclosure that must carry the replication details, and to whom;
and does the hosted-service plan change anything. Do not quote a CockroachDB figure outside the repository until that
is settled.

## Ring leg

`-engine ring` is the same social workload over the entry-ring wire, in the layout the entity-centric design
asks for (the data-lives-with-its-entity rule): a user is a **document** and the user's own collections are **store fields of
that document**, per entity, instead of one nested bucket per placement form. It uses the typed client
`github.com/hoarddb/hoarddb-server/ringclient` with one connection per worker, so `-depth` is the pool, as for the other engines. Only
the seed code is shared with the `ringseed` command: both call `bench/social/ringload`, which holds the one
copy of the generator that is not the harness' own (`TestRingSeedParity` and `TestRingSeedMediaParity` pin it
to the harness generator, `TestRingLoadSeedIsTheHarnessSeed` pins the dataset table and seed).

```
create bucket User { Name string, Bio string, Joined datetime, FollowerCount int, FollowingCount int,
    Avatar binary, Photo binary,
    Posts btreeStore, Feed lifoStore, Following hashStore, Followers hashStore, Inbox fifoStore };
create bucket Post { Author string, Seq int, Text string, Created datetime, Tags array, ReplyTo string,
    Likes hashStore, Comments fifoStore };
create bucket Outbox { Name string, Jobs fifoStore };
```

Three buckets whatever the dataset size; 1,000 `Outbox` documents are what Q1 pushes onto (the legacy engine's
1,000 `Outbox_<k>` buckets). A user's `Posts` store holds a reference to each `Post` document (`post`, `seq`,
`created`), keyed so the newest sorts first; the text is stored once, in the `Post` document, which also
carries the post's own `Likes` and `Comments`.

### Commands

On the server VM, start `hoarddb-server` with its launch environment saved (`-hoard-env` and `-hoard-log` read
it, as for the other hoardDB engine). On a client VM, with the harness binary built from this tree:

```
social -mode seed -engine ring -addr <role:server VM>:<driver port> -password <bootstrap password> -dataset S-10k -depth 32
social -mode parity-hash -engine ring -addr <role:server VM>:<driver port> -password <bootstrap password> -dataset S-10k
social -mode run -engine ring -addr <role:server VM>:<driver port> -password <bootstrap password> -dataset S-10k \
  -mix wh -depth 64 -warmup 5s -seconds 30 -loop open -rate 6000 -out ring.json
```

Add `-ring-fingerprint <value the server printed at start>` to pin the certificate instead of leaving it
unchecked. `-mode witness`, `-mode tlscheck`, `-mode ledgercheck` and `-mode soak` work on the ring engine;
`-mode converge` is refused (the ring leg drives one node: run `parity-hash` against each node and compare
`sample_hash`), as are the older timeline workload, `-routing b`, a second `-addr`, `-write-concern`,
`-fanout bulk` and `-hoard-batch`, each with the flag to use instead. `-mode witness` leaves its scratch
database on the node, because the ring wire cannot drop a database; its name carries the time. A leg is
recorded as placement `entity-documents` with `ring_serial` in its engine settings. `-mode parity-hash` prints
the same `sample_hash` as the legacy engine for the same dataset (`TestRingParityHashMatchesTheLegacyEngine`),
media included.

### What each operation sends

One request is one ring call. The legacy column is `h1`.

| Operation | Legacy hoardDB | Ring |
| --- | --- | --- |
| R1 home timeline | 1 peek of `Feed` | 1 peek of `User.Feed`, which holds full copies of the posts |
| R2 profile | 1 get | 3: get the `User` document, count `Followers`, count `Following` |
| R3 profile timeline | 1 list of `Posts` (full posts) | 2: list `Posts` (references), then one `get-many` frame for the n `Post` documents (n up to 20) |
| R4 post page | 3: get the post, count `Likes`, count `Comments` | 3, the same, on the `Post` document |
| R5 follow check | 1 keyed get of `Following` | 1: keyed get of the edge in `User.Following` (`store_get`, class `0x88`); an absent key is "not followed" |
| R6 following list | 1 list | 1 list |
| R7 avatar | 1 blob get | 1: get the `User` document, whose `Avatar` field is the blob |
| N1 notifications | 1 pop of up to 10 | 2: claim up to 10 from `Inbox`, ack them (1 when the inbox is empty). A `pop(10)` is one request on the wire now, but its elements must fit the 1 KiB a dedupe entry holds and ten notifications do not reliably, so the leg keeps claim and ack |
| W1 publish | followers pages + 1 put + one push per follower | followers pages + 2 + one push per follower: list `Followers`, insert the `Post` document, put the reference in `Posts`, push the post onto each follower's `Feed` |
| W2 like | 2: put the like, notify | 2: put in the `Post` document's `Likes`, push onto the author's `Inbox` |
| W3 follow | 5: two puts, two counter updates, notify | 3: put in `Following`, put in `Followers`, notify |
| W4 unfollow | 4: two deletes, two counter updates | 2: take from `Following`, take from `Followers`; an edge that is already gone is not an error |
| W5 comment | 2: push, notify | 2: push onto the `Post` document's `Comments`, push onto the author's `Inbox` |
| W6 upload avatar | 1 blob put | 2: get the `User` document, upsert it with the new `Avatar` (an upsert replaces all scalar fields) |
| M1 merged timeline | one list per followee (full posts) | one list per followee (references), then one `get-many` frame for only the newest 20 `Post` documents |
| Q1 queue round | 3: push, claim, ack on an `Outbox_<k>` bucket | 3: push, claim, ack on the `Jobs` store of `Outbox` document `<k>` (2 if another worker claimed the job) |

`req_per_op` in the leg record counts exactly these calls (the connection counts every call it sends), and
`TestRingOperationsSendTheDocumentedRequests` pins each row.

### Where fan-out happens

On write, as in the other engines: a publish pushes one copy of the post onto the `Feed` of each follower, one
request after another, and R1 is one peek. The followers are read from the author's `Followers` store, whose
elements carry the follower's id (a store list also returns each element's key now; the seed shape is left as it is so the parity hash stays the same). The cap of 500
followers per author bounds the fan-out as it does everywhere. Nothing is fanned out on read.

### Pipelining

The ring wire matches replies to requests by id, so one connection can have many in flight. The ring leg uses
that for the **independent requests inside one operation**: the three reads of R2, the two counts of R4, the
n `Post` gets of R3 and M1, and the per-followee lists of M1. The sequential dependencies stay sequential
(the fan-out of W1, claim before ack). The other engines send every request of an operation one after another
(Q-A). `-ring-serial` sends the ring leg's requests one after another as well; run the ring leg both ways
and record both, and use `-ring-serial` when the comparison is with the other legs. The request counts are the
same either way.

### Durability and what the leg records

The server side is the same statement as for the other hoardDB engine, read from the launch files the
harness is given. A durable ring leg (level `L-A`) needs `HOARDDB_UNSAFE_ACK_BEFORE_SYNC` **unset** in the
launch environment and no WARN line about it in the log; `-mode witness` proves it by timing. The Badger table
sync is `HOARDDB_BADGER_TABLE_SYNC`: `msync` (the safe default) or `stepwise`; state the value in the launch
environment and in the report, since it moves the write tail (see "A quiet host"). The write-ahead log
preallocation is `HOARDDB_WAL_PREALLOCATE`, default `auto`; the leg records the configured value and the
effective one. A ring write is acknowledged when the node reports it `applied`, and the engine counts any other
outcome as an error. The ring wire has no per-request write concern the harness sets; the leg's replication
factor and write concern are the node's.

### What is not comparable to the MySQL, MongoDB and CockroachDB legs

- **Layout.** The ring leg stores documents with store fields. The other legs store rows or documents in a
  fixed set of tables or collections with the collections of a user spread over them. Equal data, different
  structure; `-mode parity-hash` shows the content is equal, not the cost of reading it.
- **Request counts differ per operation** (the table above), in both directions: R2, R3, M1, W6 and the
  Post documents cost more requests; W3 and W4 cost fewer.
- **Counters.** The ring wire has no atomic increment yet. The profile's follower and following counts are the
  sizes of the `Followers` and `Following` stores (always right); the `FollowerCount` and `FollowingCount`
  scalars keep their seed values and are not read. The other legs update a counter column or field in step
  with the edge. When the ring gets update operators, W3 and W4 gain two requests each and R2 loses two.
- **Keyed reads of a store element** exist on the ring wire since U85 (`store_get`), so R5 is one keyed
  get as in the other legs. A list returns each element's key too; the seed still stores the user id inside
  each edge, so the seed shape and the parity hash are unchanged.
- **Blobs.** Avatars and photos are binary fields of the user document, because the wire cannot write a
  blobStore yet: R7 reads the whole document and W6 rewrites its scalars.
- **Pipelining** (above): a ring leg without `-ring-serial` has more requests in flight per worker than any
  other leg.
- **One node.** The ring routing across nodes is not driven by this harness; the leg is a single node,
  `RP-A`, and `-mode converge` and `-routing b` are refused.
- **The ring leg is not `h1`, `h64` or `hu`.** Its records group under placement `entity-documents`; never
  average it with one of them.
