# bench/social leg record: `perf-record/2`

One JSON document per measured leg, one line each. Two releases (or the same release on other machines) are compared by diffing these, so a field is never renamed or removed; a new field is added, optional, and documented here.
`social -summarize [-csv] leg1.json leg2.json ...` reads them and prints the reading tables (the spec's section 9.4) under a header of caveats; `-csv` prints one row per group.

Numbers are stored unrounded. The summary rounds to three significant figures. A percentile backed by fewer than 1,000 samples is `null` in the record and `n.a.` in every table and the CSV, whatever the file says: the summariser applies the rule itself from `count`.

## Fields

| Key | Meaning |
| --- | --- |
| `schema` | always `perf-record/2`; the summariser refuses anything else |
| `release`, `commit`, `date` | the hoardDB version stamp, its commit, the leg's date |
| `engine`, `engine_version`, `engine_settings` | `hoarddb`, `mongo`, `mysql` or `cockroach`; the version; the settings read back from the server, every non-default listed. For hoardDB these are read from the launch environment and the server log the harness saved (`replication_factor`, `write_concern`, `unsafe_ack_before_sync`, `unsafe_ack_warning_in_log`, `durability_level`); `tls` says whether the client connected to a competitor over TLS; `variant` names a record-only variant; `read_preference` and `read_preference_source` say how a MongoDB leg read |
| `config`, `nodes`, `level`, `routing`, `write_concern`, `copies` | the configuration (`C1`..`C5`), node count, durability level (`L-A`, `L-B`, `L-R`), read routing (`RP-A`, `RP-B`), write concern, copies kept |
| `fanout` | `single` or `bulk`: whether the MongoDB, MySQL and CockroachDB publish wrote one feed entry per request (as hoardDB does) or all in one bulk write; absent from records written before the flag, which were `bulk`. Two fan-out modes are never one group |
| `placement`, `shards`, `owners_per_node`, `holders_of_primary_bucket`, `copies_below_factor` | the placement form (`single`, `shard-64`, `shard-256`, `per-entity`) and what it did; two placement forms are never one group. A `shard-64` leg also carries `owner_node_ids` (the nodes `owners_per_node` counts, in order) and its holders are those of `User_00`; a `single` leg carries neither. `copies_below_factor` is `null` on every leg: nothing counts writes acknowledged below the factor yet, and a 0 would read as "none were" |
| `dataset`, `mix`, `loop`, `depth` | `S-10k`/`S-100k`/`S-1M`; `RH`/`WH`/`SK`/`op:<ID>`; `closed` or `open`; requests in flight |
| `offered_rate`, `rate_basis`, `utilisation` | open loop only: the absolute offered rate, `common` or `self-relative`, and offered rate over the engine's own closed median |
| `warm`, `warmup_s`, `measured_s`, `seed` | warm-up outcome and lengths, workload seed |
| `repeat` | **optional.** The N of `social.sh run ... <N>`: which back-to-back repeat of the same leg this is. `perf-record/2` stays `/2`; a record without it reads as repeat `0`. It is what lets the median of three be read from the records |
| `ops_per_s`, `reads_per_s`, `writes_per_s` | throughput over the measured window |
| `ops` | per operation: `count`, `req_per_op`, `latency_ms` |
| `latency_ms` | `read`, `write`, `all`, each `{count, p50, p95, p99, p999, max, mean}` in milliseconds (`count` is an addition so a reader can apply the 1,000-sample rule) |
| `histogram` | name of the leg's `.hist` file |
| `errors_by_type`, `verified_reads`, `misses` | request errors by type; verified reads; reads that found nothing (must be 0) |
| `resources` | `server[]` and `clients[]` per VM (CPU p95, RSS, disk write, steal, CPU per operation), `shard_top5_op_share` |
| `vm`, `canary`, `witness` | fsync facts and the fsync sidecar during the leg; canary before and after; the durability witness (`single_sync_ms_p50` is the cheapest data-only flush, pwrite plus fdatasync to a preallocated block, the reference; optional `sync_overwrite_ms_p50` and `single_sync_append_ms_p50` are the synchronised overwrite and append (O_DSYNC on Linux, O_SYNC elsewhere), recorded beside it; `depth1_write_p50_ms` is the witness median and `p50_over_single_sync` its ratio to the reference, which must be at least 0.3) and fsyncs per acknowledged write |
| `labels`, `integrity_failures`, `notes` | labels (recorded, never failures), integrity failures (the comparison is invalid; today the launch files contradicting `-level`, `durability_integrity_failure` in `engine_settings`), free notes |

## The median of three, and the `same-vm-repeats` label

The summariser groups records by (config, engine, placement, mix, loop, depth, level, routing) and reads a median of `ops_per_s` from the repeats of each group. The group's **basis** is computed from `resources.server[].vm`:

| Basis | When | Effect |
| --- | --- | --- |
| `single` | one repeat | no median; the figure is the leg |
| `same-vm` | several repeats, all on the same set of server VMs | the rows carry the label **`same-vm-repeats`** (a label, never a failure) and the header prints `LIMITATION same-vm-repeats: ...` naming the config/engine groups |
| `rotated` | the repeats ran on different VM sets | no label. When rotated repeats are added to same-VM ones, the median is of the per-VM-set medians and the median of all repeats is printed beside it |

C2 to C5 never rotate (spec 3.1), so their groups carry the label in both phases. A row's latencies, resources and read/write rates are those of the repeat nearest the median.
