# hoardDB benchmark results

Recorded measurements for hoardDB benchmarks. Every A/B below was run with
the same bench binary, interleaved on fresh data directories, `-conc`
matched to pool size, so QUIC-vs-TCP comparisons are like-for-like by
construction.

**Insert figures from `bench -db=hoarddb` taken on builds before its insert phase honoured `-conc` are single-stream.** Until then the
insert phase of `-mode=single` and `-mode=batch` sent one request at a time whatever `-conc` said;
only the read phase (and the size of the socket pool) followed it. Every insert rate in this file
that comes from that binary, in any section below, is one request in flight, and the rows that
repeat the same figure at 1, 4, 16 and 64 clients measure one stream four times. They are not
evidence that writes do not scale: the same server took about 3.3 times the single-stream rate with
64 requests in flight on four cores. The `bench/social` figures use their own client and
are not affected. New runs print `insert-conc=` and `read-conc=` in the header line.

## Cross-database comparisons

Comparisons against other databases are internal and are not published here.

## Access pattern: native key order vs query-time computation (2026-09-25)

> **Read this first (2026-09-26).** The **element counts** in this section are the finding and
> they stand: the native side answers from one key, the query-time side from n. The **time ratios
> are withdrawn as not reproducible** — a re-run on the same host spread the native side 20x on
> `fifo` and 186x on `lifo`, including a 3.6ms "O(1)" read. Cause, evidence and the fix are in
> boundary 7 at the end of the section. Quote the counts and the key layouts; do not quote a ratio
> from this section. The write leg is re-verified and stands.

Harness: `github.com/hoarddb/hoarddb-server/bench/accesspattern`. Single node, single Badger store, one
process, no client and no network — both sides of every leg are the SAME store
on the SAME data, so the only difference measured is the key encoding.

This is the measurement behind the pitch in
the product pitch, which until now had no number behind
it: "sort 10B rows on every request" against "LIFO: pop top of stack, O(1)".

    go run ./bench/accesspattern -n=100000 -repeats=3

### The four read legs — one element against every element

| Leg | Question | Native | Query-time | Ratio | Elements touched |
|-----|----------|--------|------------|-------|------------------|
| fifo | the oldest element | 27.9us | 16.8ms | 603x | 1 vs 100,000 |
| lifo | the newest element | 16.7us | 14.2ms | 853x | 1 vs 100,000 |
| heap | the top-priority item | 27.6us | 122.4ms | 4,434x | 1 vs 100,000 |
| btree | the low end of a range | 35.0us | 24.6ms | 702x | 1 vs 100,000 |

Medians of three interleaved runs, 100,000 elements, fresh directory per run,
fill synced before either side is measured. Both sides were asserted to return
the SAME element before any time was recorded, so a ratio here is two answers to
one question.

**The element count is the claim; the time is the consequence.** Every native
side answers from one key — a `Peek`, a `PeekItem`, a `Range(…, 1)` — because
the order IS the key order:

- `fifo`/`lifo`: an entry is `[s][id][0x04][position:8]` (`store/queue.go`),
  so "oldest" is the first key and "newest" is the last (`fromCursor`).
- `heap`: an element is `[s][id][0x04][inverted priority:8][entry id]`
  (`store/heap.go`), so "highest priority" is the first key and `Peek` is a seek
  — no in-memory heap and no O(entries) rebuild on open.
- `btree`: a document is `[s][id][0x01][EncodeComponent(user key)]`
  (`store/btree.go`), so a range is a bounded seek; `limit 1` stops the walk at
  the first entry.

The query-time side reads every element to work out which one it needed, then
sorts. It is deliberately naive — no index, no optimizer, no cache — and that
is the whole boundary of this measurement.

### The write leg — what no index closes

| Elements | Native keys | Indexed keys | Native time | Indexed time | Ratio |
|----------|-------------|--------------|-------------|--------------|-------|
| 100,000 | 100,000 | 200,000 | 234.5ms | 735.8ms | 3.14x |
| 1,000,000 | 1,000,000 | 2,000,000 | 4.05s | 9.06s | 2.24x |

**The key count doubles exactly; the time ratio does not stay constant, and the
smaller figure is the honest one to quote at scale.** At 100,000 elements the
index costs 3.14x; at 1,000,000 it costs 2.24x, because both sides then spend
most of their time in Badger's commit path rather than in the extra write. The
KEYS figure is the stable claim and it is exact — counted by walking the
bucket's own key ranges, not assumed from the code path. The index entry is the
same write a document engine performs on every write so that its own `ORDER BY`
becomes a seek: the cost hoardDB's key order does not pay, at 2x the storage
per document at any scale.

### Honest boundaries, and what these numbers are not

1. **The query-time side has no index.** That is the point of the comparison and
   also its limitation: a mature competitor with a real covering index narrows
   the read legs substantially. The write leg is the half no index closes, and
   it is the half the pitch should lean on.
2. **This is not a head-to-head against any other database.** No competitor was
   run. The comparison is hoardDB's own keyspace against the cost of computing
   the same answer at query time in the same process — the same
   never-compare-across-axes rule that governs the Valkey comparison below.
3. **The read-leg ratios grow with n, and that is the signature to look for.**
   The native side is O(1) and the query-time side is O(n) to O(n log n), so the
   ratio should grow roughly linearly — and it does:

   | Elements | fifo | heap |
   |----------|------|------|
   | 2,000 | 27x | 196x |
   | 100,000 | 603x | 4,434x |
   | 1,000,000 | 2,116x | 45,588x |

   A 50x increase in n bought 3.5x on fifo and 10x on heap; the native side's own
   time did not grow (27.9us at 100K, 115us at 1M, i.e. noise around a
   constant) while the scan side grew with the data. Quote the element count
   (1 vs n) and the growth, not the ratio alone: the ratio is what the
   asymptotics predict, not a fixed advantage.
4. **No client, no network, no concurrency.** This measures the storage layer
   only. The end-to-end read path has its own ceiling (`Server.Find` and the
   bucket lock), which is a different measurement with a different bottleneck.
5. **The write ratio is a range, not a number: 3.14x at 100K, 2.24x at 1M.**
   Quote the doubled key count, which holds at both sizes, and treat the time
   ratio as the smaller end of a band.
6. **`read_bytes` does not move on this store and no byte figure is quoted.**
   Badger serves reads from memory-mapped files, so `/proc/self/io` reports
   neither the reads nor the writes: `read_bytes` stayed at 0 and `rchar` moved
   only by the 98 bytes of reading the counter file itself. The harness counts
   ELEMENTS instead. A counter has to move on the code under test, and the way to
   know is to run it once and look.
7. **The read-leg TIME ratios are not reproducible and are withdrawn
   (2026-09-26).** The element counts in the tables above stand — they are read
   off the harness, which asserts both sides return the same element before
   timing anything. The time ratios do not. A re-run of the same command
   (`-n=100000 -repeats=5`) on the same host gave, on the native side alone:

   | Leg | per-run native (5 runs) | spread |
   |-----|------------------------|---------|
   | fifo | 52.3, 39.9, 25.1, 62.1, 31.3us | **20x** |
   | lifo | 110.0, 133.9, 19.4, 133.8, **3614**us | **186x** |
   | heap | 19.9, 22.9, 38.6, 37.2, 40.7us | 2.0x |
   | btree | 34.0, 58.0, 45.5, 44.9, 13.7us | 4.2x |

   The `lifo` row contains a 3.6ms "native" read — 100x the others — which
   contradicts the O(1) claim the whole section rests on, so it is not a tail
   value, it is a broken measurement. Across three separate invocations at
   n=100,000 the fifo median landed at 413x, 565x and 702x. **Cause, read and not
   guessed:** each repeat opens a **fresh Badger directory**
   (`main.go:141`, `dir := filepath.Join(root, fmt.Sprintf("%s-r%d", …))`), so
   every native read is a cold first touch of an mmap'd file — page faults, no
   value cache, and a CPU that another process can take. The interleaving
   (`main.go:136`) spreads host drift across legs correctly, but it does not
   make any single sample warm, and a median of 3 inherits whichever repeat got
   the quiet machine. The scan side is insensitive to this (it touches 100,000
   elements either way), which is why the ratio moves and the absolute scan time
   does not. **What would fix it:** a warm-up pass over the same keys before the
   timed read, per repeat — not a longer median, which averages the defect away
   instead of removing it. Until then: **quote the element count (1 vs n) and
   the key layouts, not the ratio.** The write leg is unaffected and its numbers
   below are re-verified (3.11x at 100K against 3.14x claimed; 2.22x at 1M
   against 2.24x — both within 1%).

## TCP driver listener vs QUIC (tcp-protocol-spec.md section 10)

Date: 2026-09-17. Single node, loopback, the development box. Server runs
both listeners: QUIC on 4433/udp, TCP on 7433/tcp, same cert, same ALPN
`hoarddb/1`. Reads are random point lookups by key (`user_%06d`) with a
hit/miss check; inserts are batch (1000 docs/call). `hits` reported by the
bench.

### Reads (100K dataset, shared for both transports, reads-only runs)

| conc | TCP reads/s | QUIC reads/s | TCP FD (max) | QUIC FD (max) | TCP RSS | QUIC RSS |
|------|------------|--------------|--------------|---------------|---------|----------|
| 1    | 39,632     | 14,119       | ~8           | 7             | ~26 MB  | ~25 MB   |
| 32   | 269,560    | 83,999       | 38           | 7             | 25.8 MB | 43.3 MB  |
| 128  | 354,573    | 105,078      | 134          | 7             | 29.8 MB | 56.6 MB  |
| 512  | 358,182    | 109,542      | 518          | 7             | 44.1 MB | 55.0 MB  |

- **FD cost of concurrency is exactly the spec's stated ceiling** (section
  6): TCP scales linearly in sockets (128 → 134 FDs, 512 → 518 FDs), QUIC
  holds a constant 7 FDs at every concurrency on one connection. That is
  the accepted v1 cost and it is now measured rather than assumed.
- Both transports saturate the storage-layer read ceiling between
  conc=128 and conc=512 (TCP 354→358K/s, QUIC 105→109K/s).
- **Prediction that did NOT hold (recorded per spec section 10):** the
  spec predicted driver-latency parity ("reads within ~2%", "parity or
  slightly better than QUIC"). TCP is roughly 3× faster than QUIC on
  loopback point reads at every concurrency (14.1K/s vs 39.6K/s at conc=1;
  109.5K/s vs 358.2K/s at conc=512). The measured QUIC round trip at
  conc=1 (~71µs/read) matches the ~70µs figure in the spec; the TCP round
  trip (~25µs/read) is genuinely cheaper because a plain TLS stream has no
  per-request QUIC stream open/close and packet machinery. The rejection
  gate ("if TCP trails QUIC on reads beyond ~2%") does not trigger: the
  gap is in TCP's favor, so the strategy stands. The parity prediction
  itself was wrong and is hereby corrected — expect TCP to *beat* QUIC on
  driver latency, not match it.
- Insert, conc=1, 100K records: TCP 151–252K/s, QUIC 197–207K/s across
  runs (this is a noisy development box; the bands overlap, no consistent
  direction).

### Pipelining window (spec section 10, fourth measurement)

Small-op (GET) workload, pipelined at the window depth, per-op latency:

| Server window | p50  | p99  |
|---------------|------|------|
| `PipelineFrames=1` (strict lockstep) | 15µs | 42µs |
| `PipelineFrames=4` (default)         | 10µs | 25µs |

The window buys back ~33% at p50 and ~40% at p99 of the per-op round trip
on loopback. The spec's decision rule ("if it buys nothing, the default
should drop to 1") does not trigger: the default stays 4. Note the caveat
the spec names in the same section: at W>1 responses may complete out of
order, so a pipelining driver must match responses to request IDs (the
server echoes them) and must not assume send order.

### Honest non-results

- The old stored QUIC-only numbers (the ~21% insert variance band quoted
  in the server comment for `defaultMaxConcurrentStreams`) were not
  re-established on this box; a single run at conc=512 would be needed to
  confirm the stream-admission gate is not the first ceiling hit. The
  transport A/B above used the same gate for both transports, so the
  comparison is unaffected.
- Per-IP cap (`HOARDB_TCP_MAX_CONNS_PER_IP`, default 64) was disabled for
  these runs because the bench dials 512 sockets from one IP; a real
  deployment sizing a bench host must raise it, and the per-IP cap is
  otherwise enforced (unit-tested).

## Internode TCP vs QUIC (internode TCP build doc, section 5)

Date: 2026-09-17. 3-node RF=3 cluster on loopback, seed-set boot, all
nodes sharing the cluster token. Driver bench fixed on QUIC against node
A; only the internode leg toggles (`HOARDB_TCP_INTERNODE_ENABLED`).
WriteConcern=majority, so every insert rides the
coordinator→owner→replica→ack pipeline over the internode transport.
100K records, reads random point lookups, all hit (the bench's miss
counter verified 100k/0 on both runs).

| internode transport | insert | read | hits | TCP conns on node A | fallbacks |
|---|---|---|---|---|---|
| QUIC (A-quic) | 80.1K/s (12.5µs/insert) | 12.8K/s | 100k/0 | 0 | 0 |
| TCP (B-tcp) | 78.9K/s (12.7µs/insert) | 12.4K/s | 100k/0 | 2 (both peers) | 3 (boot only) |

Replication verified on both runs: the bench database materialises on all
three nodes (`replica-copies-elsewhere=2`), and the TCP run's node-A log
shows `peer connection transport=tcp` for both peers. Repeated runs sit
inside the same band (TCP 74.3−88.1K/s, QUIC 74.3−82K/s across runs).

**Answer to the open question: the internode split is a wash.** Through
the replication pipeline, internode QUIC vs TCP are indistinguishable
within this machine's run-to-run noise. The prediction that held: bulk
traffic (replication push, migration, ack round trips) is storage/ack-
bound, not transport-bound, so replacing QUIC with TCP gains nothing on
the measured path — and loses nothing. The prediction that did *not*
show up: the per-op latency edge TCP showed on the driver port (≈3×
faster point reads) does not surface through the cluster path, where each
insert is a multi-hop, majority-acked transaction rather than a single
frame round trip.

### Pre-existing cluster findings surfaced by this work

Both were diagnosed during the internode-TCP A/B and fixed in the
cluster-join-correctness pass (2026-09-17); recorded here so
the history is readable.

1. **Adopted members lacked replication senders** — FIXED. `peerFor`/
   `adoptMembership` created the peer entry for a node added after
   `Cluster.Start`, but the sender/verify goroutines were only spawned in
   `Start()` over boot-time members, so a node joined post-boot never
   received WAL entries and a majority write whose replica set included it
   stalled on the write-concern deadline. Now `ensurePeerRunning` is
   invoked by both `Start()` and every `adoptMembership` path (commit,
   vote, gossip reconciliation). Regression test
   `TestAdoptedMemberReceivesReplication` reproduces the exact failure
   (verified: fails with "1 of 2 copies confirmed" without the fix) and
   passes with it. Live: the previously-failing 2-node join-boot bench
   (100K records, WriteConcern=majority) completes at 67.2K/s inserts,
   all hits, with the replica copy materialised on the joiner.
2. **CLI `join <addr>` recorded an address identity** — FIXED. A bare
   address made `parseMember` record `ID=addr`, which a node announcing a
   NODE_ID could never adopt ("node identity X is not a member"). Now
   `handleQUICClusterAdd` probes the target's announced identity (a
   read-only gossip exchange; `GossipResponse.Self` is the explicit,
   backward-compatible signal) and records that instead; an unreachable
   target fails with the explicit `join <node-id>@addr` remedy. Regression
   test `TestClusterAddResolvesBareAddress` (verified to fail without the
   probe). Live: `join 127.0.0.1:4443` now adds `itest-b` and the joiner
   converges onto epoch 2.
   Note the negotiated semantics: a node whose own boot seed list is a
   bare address announces address identity (memberList re-derives self
   from the seed), which is the legitimate address-identity mode — the
   probe records whatever the target actually announces.

## Catalog and the collector (before 2026-10-04, after 2026-10-16)

What one idle bucket costs the Go garbage collector, and what the collector costs a write
load, with N buckets in memory. The baseline below (2026-10-04) is the code before catalog GC
CG-13 made catalog entries pointer-free (`3ecad024`, merged `dd0f6bd8`, 2026-10-09); the
"after" table follows it (2026-10-16).
**Nothing asserts these numbers** (a timing assertion fails on a loaded host); they are context
for the next person. Benchmarks: `BenchmarkCatalogGCMark` and `BenchmarkGCCPUPerOp` in
`server/catalog_gc_test.go`, run with `-benchtime=1x -count=3`; the 1,000,000 row once, with
`HOARDDB_CATALOG_1M=1`.

**Host:** a 16 vCPU KVM guest on an Intel Core i9-14900K, 28 GB, `GOMAXPROCS=16`
(the forced collection runs at 1 P), shared with other agents. One-minute load average during
the runs: 3.7 to 9.9 (mark runs), 6.0 to 6.5 (CPU-per-op and the 1,000,000 row). The second
host the spec asks for was not used in this run.

Forced full GC is the median of three runs, each the best of three `runtime.GC()` at 1 P:

| Shape | N | objects / bucket | scannable B / bucket | forced GC | runs (ms) |
| --- | --- | --- | --- | --- | --- |
| plain | 1,000 | 3.0 | 256 | 1.7 ms | 1.0, 1.7, 1.7 |
| plain | 10,000 | 3.0 | 258 | 5.6 ms | 3.8, 5.6, 7.4 |
| plain | 100,000 | 3.0 | 251 | 93 ms | 42, 118, 93 |
| plain | 1,000,000 | 3.0 | 272 | 995 ms | one run |
| store field | 1,000 | 5.0 | 319 | 2.9 ms | 2.2, 2.9, 3.0 |
| store field | 10,000 | 5.0 | 322 | 7.6 ms | 4.4, 7.6, 7.9 |
| store field | 100,000 | 5.0 | 315 | 197 ms | 101, 215, 197 |
| index | 1,000 | 13.0 | 740 | 3.5 ms | 2.5, 3.5, 3.5 |
| index | 10,000 | 13.0 | 738 | 15.9 ms | 8.8, 15.9, 16.2 |
| index | 100,000 | 13.0 | 731 | 459 ms | 250, 539, 458 |
| replication factor | 1,000 | 3.0 | 260 | 2.2 ms | 2.2, 2.9, 1.8 |
| replication factor | 10,000 | 3.0 | 258 | 6.5 ms | 3.9, 6.5, 6.6 |
| replication factor | 100,000 | 3.0 | 251 | 111 ms | 62, 111, 125 |

The spread at 100,000 is the host's load, not the catalog: read the ratios. Store field and
index shapes are hash buckets (only hash and btree buckets declare either); the others are
empty LIFO buckets.

GC CPU under 400,000 inserts into 64 hot buckets from one goroutine, plain buckets (one run each):

| N | GC cycles | GC CPU / insert | inserts / s | load average |
| --- | --- | --- | --- | --- |
| 1,000 | 18 | 0.16 µs | 44.0k | 6.3 |
| 10,000 | 18 | 0.69 µs | 41.1k | 6.5 |
| 100,000 | 14 | 1.67 µs | 36.7k | 6.0 |
| 1,000,000 | 6 | 16.8 µs | 33.0k | 6.4 |

### After the catalog change (2026-10-16)

**Host:** a 4-vCPU test VM (16 GB), `GOMAXPROCS=4` (the forced collection runs at 1 P), a different
and smaller host than the baseline's 16-vCPU guest, so read the per-bucket columns and the shape of
the curve, not the milliseconds side by side. One-minute load average 0.6 to 1.0 on the VM before
the runs and 1.1 to 1.7 during them. Built from branch `fix/bug-0243-closeout` (the `main` code at
`0ad2d14a` plus a benchmark-only fix, below). Benchmark runs: `-benchtime=1x -count=3`, median of
three; the 1,000,000 row is one run (`HOARDDB_CATALOG_1M=1`). Shapes: plain (an empty LIFO) and
typed-only (two declared typed fields, no store field or index).

| Shape | N | objects / bucket | scannable B / bucket | heap B / bucket | forced GC |
| --- | --- | --- | --- | --- | --- |
| plain | 1,000 | 0.031 | 1.4 | 141.6 | 1.4 ms |
| plain | 10,000 | 0.134 | 2.3 | 152.7 | 1.9 ms |
| plain | 100,000 | 0.016 | 0.28 | 127.9 | 3.0 ms |
| plain | 1,000,000 (one run) | 0.0075 | 0.22 | 168.5 | 3.6 ms |
| typed-only | 1,000 | 0.046 | 1.5 | 142.1 | 1.2 ms |
| typed-only | 10,000 | 0.136 | 2.4 | 153.5 | 2.0 ms |
| typed-only | 100,000 | 0.016 | 0.31 | 127.9 | 1.2 ms |

Before: 3.0 objects and about 250 scannable bytes per bucket and 93 ms at 100,000 (995 ms at a
million). After: 0.016 objects, 0.3 scannable bytes, 3 ms at 100,000 and 3.6 ms at a million. The
cost no longer grows with the bucket count in any way a run on this host can see. The 1,000 and
10,000 rows sit above the 100,000 row most likely because a fixed number of
objects that do not depend on N are divided by a small N; the gate that asserts the limit excludes the first 1,000
buckets for that reason. The first run at N=1,000 in each series reads cold (0 objects, 58 heap B)
and is the one the median discards. The 1,000,000 row's 168 B heap per bucket is above the
128 B at 100,000; it was one run and is not explained here.

The two gates, run on the same VM without `-short` (limit 0.1 objects and 16 scannable B per bucket,
first 1,000 excluded, 21,000 buckets):

| Gate | plain | typed-only |
| --- | --- | --- |
| `TestIdleBucketsAddNoGCTracedObjects` | 0.0696 objects, 1.31 B | 0.0673 objects, 1.12 B |
| `TestIdleBucketsAddNoScannableHeap` | 0.0667 objects, 1.09 B | 0.0663 objects, 1.03 B |

GC CPU under 400,000 inserts into 64 hot buckets from one goroutine, plain buckets (one run each,
the same VM; 1,000,000 not run):

| N | GC cycles | GC CPU / insert | inserts / s | load average |
| --- | --- | --- | --- | --- |
| 1,000 | 21 | 0.071 µs | 54.3k | 1.7 |
| 10,000 | 24 | 0.097 µs | 48.4k | 1.6 |
| 100,000 | 18 | 0.085 µs | 42.6k | 1.6 |

Before, at 100,000: 1.67 µs. After: 0.085 µs, about 20 times less, and flat from 1,000 to 100,000.

**Benchmark fix recorded with this run.** The old N=1,000 row of `BenchmarkCatalogGCMark` could print
1.8e16 objects per bucket: it subtracted two unsigned collector counters, and at that size a
collection frees more than the build adds. The delta is now clamped at zero
(`server/catalog_gc_test.go:perBucketGrowth`, test `TestCatalogGCObjectsDeltaDoesNotWrap`).
