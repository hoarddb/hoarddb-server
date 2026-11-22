# bigasstest

Internal four-layout benchmark. Not published; nothing from it goes on any public surface.

The layouts `social`, `chat`, `ledger` and `agent` are seeded by the `scenarios` binary into
`<database>_<layout>` on **one node of one engine**, and all four are served **at the same time**.
Each virtual user is an independent client with one request in flight (closed loop; `-loop open
-rate R` offers Poisson arrivals instead). For every request it picks one of the four databases
uniformly, then a read or a write by that layout's read share, then an operation by the weights in
`mix.go`, then a seeded target entity (uniformly, or Zipf-skewed, see below). Each virtual user has
its own PCG stream seeded from `-rand-seed` and its index, so every engine is offered the same
requests. Exactly three draws (layout, read or write, operation) come before the entity draw.

The read share per layout is the **profile**, `-profile R|S`, and every record carries the profile
name and the four shares:

| profile | social | chat | ledger | agent | pooled reads |
|---|---|---|---|---|---|
| `R` (default, the headline) | 90% | 60% | 70% | 50% | 67.5% |
| `S` (the layouts spec's own mixes) | 20% | 30% | 30% | 10% | 22.5% |

R is an application-shape judgement of how read-heavy each layout is; S is the layouts spec's
shares, which are write-heavy in every layout (they exist to cover write hazards). Weights within
reads: social feed 10, inbox 5, followers 5; chat room page 70, user rooms 30; ledger account page
70, invoices page 30; agent memories 50, session page 50. Write weights are the spec's, renormalised
within writes. A request stream is a function of the profile, so results of different profiles (and
of the former pooled 95/5 mix) are not comparable.

    bigasstest seed    -engine ring -scale 100 -- <connection flags of the scenarios binary>
    bigasstest run     -engine ring -uri ... -facts facts.json -out records/ -data-dir /var/lib/x \
                       -reseed-cmd ./reseed.sh              # sweep -vusers 1,4,16,64,128, 3 reps each
    bigasstest summary records/
    bigasstest calibrate -rate 2000,6000         # the scale to seed, see "Growth and the scale to seed"

`run` sweeps `-vusers` in the order given (default `1,4,16,64,128`), `-reps` repetitions per level
(default 3). Every repetition is its own JSON record (`<engine>-<level>-v<N>-r<R>.json`) and starts
from freshly seeded data: `-reseed-cmd` runs before every repetition but the first, and a sweep of
more than one repetition is refused without it. A warm-up (windows of `-warm-window` until two
successive windows are within `-warm-tol`, at least `-warm-min`, at most `-warm-max`; `warm=false`
otherwise) precedes `-seconds` of measured load. The duration flags (`-seconds`, `-warm-window`, `-warm-min`,
`-warm-max`, `-request-timeout`) take Go durations with a unit: `-seconds 60s`, not `-seconds 60`, which is refused. The
wrapper's own `--seconds` and `--settle` are whole seconds (`--seconds 60`), its `--warm-*` flags are durations (`--warm-min 60s`). `scripts/certification/bigasstest.sh` wraps all of it.

What a record holds: the virtual-user count, loop, rate, warm figures, the engine's read-back
settings, the host facts (`-facts`; the record is refused if one is missing), the model fingerprint,
hits, misses, verify failures, errors, client CPU (`client-bound` above 85%), `growth_pct` (`drift`
above 10%), throughput and p50/p95/p99/p99.9 overall, per layout and per operation, round trips and
commits per operation, and the post-run audit. `-op social.follow_scan` (or `ledger.expenses_all`)
runs one record-only operation instead of the mix. The disk guard (`-data-dir`, or `-free-cmd` for a
remote disk) aborts below `-min-free-gb` (3), writes the record with `aborted: disk_guard` and exits
non-zero.

## Entity access: uniform or Zipf

`-access uniform` (the default) picks every target entity uniformly among the layout's seeded ones.
`-access zipf -zipf-s 1.0` picks by a seeded Zipf draw: rank k (counting from 1) has probability
proportional to 1/k^s. The draw replaces the uniform draw one for one (one random number per entity
pick), for **every** read and write entity pick, so the stream of operations is unchanged and only
the targets differ. The exponent has no upper limit; 1.0 is the usual choice and the standard
library's Zipf (which needs an exponent above 1) is not used, the cumulative table is built directly.

**How rank maps to an entity:** rank 0 (the hottest) is the first document of the bucket in seeding
order, rank 1 the second, and so on. The seeding order is the model's own index (`u0`, `u1`, ...
`a0`, `a1`, ...), identical on every engine and every run at one seed and scale, so every engine is
offered the same hot entities. The model has no popularity of its own beyond that index, so a hot
entity is hot only because it is picked often; its data is no larger than any other's. Picks of a
member inside a document (the author of a message among the room's members) stay uniform.

Both modes record `access`, `zipf_s` and `page_depth`; the record file name and the summary group
carry them too, so a uniform and a Zipf leg never overwrite or merge. Run both:

    scripts/certification/bigasstest.sh run ... --access uniform
    scripts/certification/bigasstest.sh run ... --access zipf --zipf-s 1.0

## Page size and eligible owners

A paged read keeps its spec page (feed 10, inbox 20, room messages 50, expenses 50, session messages
20, memories 100, user rooms 20, invoices 20; followers and room members are read whole) and its
entity is picked only among the **eligible owners**: those seeded with at least that many elements of
the store. The eligible set is a function of the model, so it is the same on every engine, and under
`-access zipf` the rank order is over the eligible owners in seeding order (writes still rank over
every document of their bucket). When fewer than 25% of a bucket's owners qualify, the read falls
back to the largest limit that 25% of them hold, never below 1, and the record says so. The model
seeds every owner of every paged store with at least twice the spec page (owner ruling 2026-11-07),
so the eligible share is 100% and no read falls back: feed 40 (the author of an inbox's only
followee writes 40 posts, so every inbox holds at least 40), room messages 100, expenses 100, invoices 40,
session messages 40, memories 200; the user's rooms are a hash store with no offset, so every user is in
at least 20 rooms (the chat layout adds rooms of 3 to 8 members until that holds, 145 per scale
step). The fallback stays in the code for a hand-made model, and a test fails if the model ever
seeds an owner with less (`TestEveryPagedReadKeepsItsSpecLimitForEveryOwner`, scales 1, 20 and 300). The record
carries, per paged read, `read_pages`: `spec_limit`, `limit`, `owners`, `eligible_owners`,
`eligible_share` and `fallback` (`elements_per_read` stays). A page of an ordered store (fifo, btree)
ends inside the owner's seeded elements and so never holds a written element; a page of a hash store
is the first keys in key order and may, and the read of every element (followers, members) holds
whatever was written, by definition.

## Page offset: reads that start inside a store

`-page-depth P` (default 0.30) is the share of paged reads of an **ordered** store (fifo, lifo or
btree: the feed, inbox, room messages, expenses and session messages) that start at a random offset
instead of the first element. A hash store has no order to take an offset in, so followers, user
rooms, invoices and memories never do. The offset is drawn so that `offset + limit` stays inside that
owner's **seeded** element count (`ReadOp.Offset`, 1 to seeded minus limit): a read never sees a
written element, its content is exactly the seeded elements from the offset, and every engine does
the same work. The verifier checks the page against the seeded model from that offset; an engine
that ignores the offset fails it. The planner makes its two offset draws for every such read, used
or not, so the operations and entities of a stream are the same at any depth.

A read at an offset is asked only of owners seeded with at least `limit + offset` elements: see
"Page size and eligible owners". The record file name and the summary group carry the depth.
The record carries `page_depth` and, per operation, `offset_reads`, so the realised share is
`offset_reads / count` (smaller than `-page-depth` where an owner has no room).

How each adapter reads at an offset, and what it costs on that engine:

| engine | how | skipped rows scanned? |
|---|---|---|
| hoardDB (`ring`) | the ring client has no offset, only the cursor of the previous page, so the adapter reads forward from the first element and discards the first `offset` (one round trip per 1000 elements; an offset inside the first 1000 is one round trip that carries `offset + limit` elements) | yes, by the server; the round trips are counted |
| MySQL, PostgreSQL | a range on the primary key, `seq >= offset` (pushed stores, whose seeded seq counts 0, 1, 2, ...) or `key > AfterKey` (the btree), never `OFFSET` | no |
| MongoDB | the same range on the `(owner, seq)` or `(owner, key)` index, never `skip()` | no |

This is a difference between the products and is printed, not hidden.

## Latency budget and thin operations

Every record carries, per operation, per layout and overall, the share of requests that took under
10 ms and under 50 ms (`under_10ms_pct`, `under_50ms_pct`; strictly under; `null` when nothing was
timed). They count the measured window only, from the same latencies as the percentiles (queueing
time included in an open loop), and a request that failed is not in them (its operation's `errors`
is). The summary shows them as the `<10ms%` and `<50ms%` columns, for the overall row, each layout and
each operation.

The summary also lists every operation of the mix with its measured sample count (the fewest over the
successful repetitions, because the thinnest repetition limits what the figures can say) and marks
an operation under 1,000 samples as `thin`. The record carries `thin` per operation and a `thin-ops`
label; an operation that was never drawn still has a row with zero samples. Percentiles below
1,000 samples are `n.a.` for the same reason. Under profile R the write operations are the thin ones
at short durations, because the social writes are 10% of the layout's 25%.

## Time shares, page sizes and server CPU in the record

Three additive items, for reading a Zipf run without estimating:

- `ops.<kind>.latency_sum_ms` is the summed latency of the measured requests of one operation; an
  operation's share of the time is its sum over the sum of all operations.
- `ops.<kind>.documents_per_op` and `elements_per_op` are the documents and elements a successful
  request returned on average (a room page's members when the layout reads them as documents).
- `server_cpu_seconds` and `server_cpu_cores` (CPU time over the measured window, and that over the
  window's length) need `-server-cpu-cmd`, a command that prints the server process's cumulative CPU
  seconds; the wrapper builds it (an ssh line). Without the flag both fields are absent.

## Growth and the scale to seed

Writes change the data they are read against. `growth_pct` is the elements and documents the engine
acknowledged during a repetition (warm-up included) as a share of what was seeded, and a record above
10% is labelled `drift`. Under profile R writes are 32.5% of the requests, so the line is reached
much sooner than under the old pooled 95/5 mix. The growth depends on the engine's throughput, so
the scale to seed is derived from the request rate and the duration:

    growth_pct = 100 x rate x seconds x U / (E x scale)

with U the acknowledged units per request (reads add none; 0.42 under profile R, measured by drawing
the planner's own stream, so it follows the profile, the access mode and the post fan-out), E the
seeded elements and documents per unit of scale (49,468; it was 8,071 before the density change; proportional to the scale within 1% from scale
1 to 30, a test checks it) and seconds the warm-up plus the measured window.

    bigasstest calibrate [-profile R] [-access uniform] [-rate RATE[,RATE...]] [-seconds 300s] [-warm-min 5m]
                         [-warm-max 15m] [-target-pct 8] [-max-scale 300] [-client-ram-gb N] [-scale N] [-print-scale]

prints the model, the scale that holds `growth_pct` at or under the target (a margin under the drift
line) for a table of rates, and the recommended scale for `-rate`; `-print-scale` puts only that
number on stdout. A short local run on the in-memory fake checks the model (`TestGrowthModelMatchesAFakeRun`: the growth the runner records equals the prediction, uniform and Zipf).

**Default scale: 100, with the timing plan S below.** Since the density change (every owner of a paged
store holds two pages) one unit of scale seeds 49,468 documents and elements, 6.1 times the 8,071 it
seeded before. `calibrate` derives the scale for other durations and rates from the formula above.

**The fastest engine sets the scale, not the slowest.** The growth is the writes an engine acknowledged
during a repetition as a share of what was seeded, so the engine that serves the most requests per
second grows the data most: pass `calibrate -rate` the measured rate of the fastest engine at the
highest virtual-user level. `-rate` takes one rate or a comma list of measured rates (`-rate 2000,6000`)
and uses the largest; its default is the fastest engine's measured rate so far. The rate is an input,
not a result: after a pilot give it the measured rates of every engine (the wrapper reads `CAL_RATE`, a
comma list too), or pass `--scale`. `calibrate -profile S` prints the table of the write-heavy profile.

**The load generator's memory is the other limit.** The seeded model lives in the client: measured
resident memory is about 56 MB per step of scale plus 1.6 GB (7.2 to 7.35 GB at scale 100, 10 GB at
scale 150), about three times the 19 MB per step this page used to claim. `calibrate -client-ram-gb N`
refuses a scale whose modelled memory is over 60% of the client VM's RAM and names the largest scale
that fits; `calibrate -scale N` checks a scale you chose the same way. The wrapper reads the client's
MemTotal and does both on every run, so a scale the client cannot hold is refused before anything is
seeded.

**Default plan S (`bigasstest.sh --plan S`, the wrapper's default, scale 100)**: `--scale 100
--warm-window 30s --warm-min 60s --warm-max 120s --seconds 90 --settle 60`, 150 s per repetition.
With the runner's own timings (600 s per repetition: 5 min warm-up plus 300 s measured) the data
drifts far over the 10% line at scale 100, so a scale-100 run on those timings is labelled
`drift`; use plan S, or raise the scale (and the client memory) instead. `--plan runner` leaves every
timing to the runner and the scale to `calibrate`; any flag given beats the plan.

**Restoring hoardDB instead of reseeding (`--seed-copy DIR`, ring only).** Seeding hoardDB takes many minutes at
scale 100, and every repetition reseeds. With `--seed-copy DIR` (a directory on
the server VM) the wrapper seeds once, stops the server cleanly (the stop trims the WAL), copies the data directory to DIR and starts it again; every later fresh start, between
repetitions and in later runs of the same scale, database and checkout, restores that copy and starts
(seconds, not minutes) and the client's `seed.sh` skips. The copy is the whole directory, so it keeps
`keys/server.crt`, `users.json`, the auth keys, `ring/incarnation` and `ring/FORMAT`. The runner's own
count check of every bucket before each repetition stays on, so a restore that is not the seeded data
is refused. The other engines keep a fresh reseed. Remove the copy with
`bigasstest.sh clean ring SERVER --client CLIENT --seed-copy DIR`.

The wrapper's default is plan S at scale 100 (`--plan S`); with `--plan runner` its scale is the one
`bigasstest calibrate -print-scale` recommends for the wrapper's own `--profile`, `--access`, `--seconds` and
warm-up flags and `CAL_RATE`. `--scale N` overrides either, and is checked against the client's memory. The wrapper also takes `--profile`,
`--access`, `--zipf-s` and `--page-depth` and passes them to `bigasstest run`; run the uniform and the
Zipf leg as two separate wrapper runs into two output directories.

## A faulty benchmark VM, and the integrity probe

The first benchmark VM (the first one in the VM list, the quiet pair's first host) is faulty: it corrupts
data silently. database initialisation fails on it often, server processes die of SIGSEGV on it, and counts
come back wrong. Do not use it as the server VM or the client VM; every
result measured on it is void, whichever engine ran. The wrapper cannot know which VM that is, so it
proves both VMs, server and client, before it seeds anything, with an integrity probe of three checks: a
random file in /dev/shm hashed twice (`ram`: memory or CPU), the file written to disk and read back with
O_DIRECT (`direct`: the disk path), and the file copied through the page cache, synced, the caches dropped when
sudo allows, and read back (`buffered`). It runs 16 iterations of 64 MB per VM (about two minutes), prints the
three counters, and any mismatch refuses the run with the VM role, the failed check and the fix (reboot, or move
to another VM). `BAT_INTEGRITY_ITERATIONS` and `BAT_INTEGRITY_MB` change the size, `BAT_INTEGRITY_ITERATIONS=0`
skips the checks for a VM you have just checked some other way. The same step records each VM's host name, load
average and CPU steal ticks in the facts. Before each engine run the server VM also gets a baseline probe (median
of 200 4 KiB O_DSYNC writes, time to sha256 256 MB of zeros), stored in the facts; the first run writes a reference
file (`baseline-reference.json` in the output directory, or `BAT_BASELINE_REF`). A later run that is more than
10% off that file warns; it never refuses. The warning is the same for every engine, and the records of that run
carry the label `baseline-drift`.

## hoardDB batched fan-out (`-param batch=1`)

A write that fans out to several elements goes, by default, one round trip per element (a social post is
one document upsert and up to nine pushes). `-param batch=1` sends the several writes of one operation
as **one** ringclient batch frame (writes only, up to 255 operations; `ringclient.Batch`). It covers every
`WriteOp` with more than one document or element: a post, a group post, a chat message, a join, an agent
message. A `PreRead` and the claim and ack of `agent.task` are reads or depend on a reply, so they stay
separate round trips; a write of one element stays a plain request. `Reply.RoundTrips` counts frames, so
the record's `round_trips_per_op` shows the saving. A batch is not a transaction: its operations are
applied or refused one by one, so one refusal fails the write and leaves the others applied (the audit
treats an unacknowledged write as a ceiling). `Retries` and `Commits` stay 0: ringclient may resend a
whole frame once after a stale-header refusal, as it does for a lone request, and that is invisible to the
adapter in both modes. The first use of a bucket on a connection costs one `describe_bucket` in both modes.

The record says which mode it ran in: `settings.batch` and the record field `batch` are `on` or `off`
(absent for other engines), and a batched record gets `-batched` in its leg name, so the two variants never
overwrite each other or share a summary row. The wrapper runs both with `--hoarddb-batch both` (unbatched leg
first, then a reseed, then the batched leg; the order is a possible bias, so repeat the pair in the other
order if the two legs differ by little). The other engines are unaffected. `TestRingBatchedWritesStoreTheSameData`
(live, `HOARDDB_ADDR`) applies the same planned writes through both modes and requires the same stored data.

## hoardDB get-many reads (`-param multiget=on|off`)

By default the adapter reads the documents a page names one `Get` per id. `-param multiget=on` reads
them with one `ringclient` get-many call (`Bucket.GetMany`; `RING_GET_MANY` on the wire, up to 255 keys a
frame). It covers every read that fetches many documents by id: the `Post` documents of `social.feed` (ids
from the feed elements' bodies) and the `User` documents of `chat.room_page` (ids from the members store).
The other reads fetch no documents by id. The default is `off`, so both forms can be swept; any other
word is refused (`ring: -param multiget="1" is not on or off. ...`).

`Reply.RoundTrips` counts the frames actually sent: `social.feed` is 2 (the page, one frame) instead of
1 + one per post, and `chat.room_page` is 3 (messages, members, one frame) instead of 2 + one per member.
Those counts hold while a store read fits one page (1,000 elements) and the ids fit one frame; above 255
ids the client splits the call into frames it sends together, and the adapter counts `ceil(n / 255)`
(room page: `2 + ceil(members / 255)`). Chunking is the client's job, not the adapter's. An empty page
sends no frame. A document that is gone fails the operation with the bucket and id, as the single `Get`
does, so the two arms do the same work and their round trips and latencies compare.

The record says which form it ran: `settings.multiget` and the record field `multiget` are `on` or
`off` (absent for other engines), and a `multiget=on` record gets `-multiget` in its leg name, so the
two variants never overwrite each other or share a summary row. No throughput figure from this switch
is to be published (competitor comparisons stay internal). The unit tests run over a fake that counts
calls (`engine_hoarddb_reads_test.go`); nothing in them starts a server.

## Coverage: what a leg exercised, and what the advisor says

**`capabilities` in the record.** Every record carries a `capabilities` list (`capabilities.go` is the
vocabulary): the hoardDB abilities its leg exercised, so a number is never read as covering more than
it did. The list follows what the run did, not only its flags: `entity-buckets` and `store-fields` when any
request ran, `claim-ack` when an agent task was drawn, `batch-frame` when a write ran in a batch leg,
`get-many` and `native-layout` when a feed or room page was read in that leg, `declared-aggregates` when a
maintained sum was audited, so a restricted `-op` run does not claim more than it exercised. An engine with none of
these writes an empty list; a record without the list is refused when it is written. Built abilities no
leg uses yet, so no figure here says anything about them: `inc` on a hash or btree element, guarded update
with `return`, expiry (`expire at`), fetch-through `expand`, nearest-k on a store field, memory databases,
the dead-letter verbs, and a read of the declared aggregates (the aggregates leg only maintains them).
The lifo, heap and blob stores are declared by the layouts and no request touches them.

**`bigasstest plan-check`.** The product's own advisor, `hoarddb plan`, is run in process on each layout
described the way a relational source would look: one kind per document bucket, one child kind per store
field that refers to its owner, and one trace line per way the mix reads it (a page oldest first for a
fifo or btree store, a keyed read for the keyed hash stores, a list for the others). For every store field
it prints `agree`, `differ` or `open` against the store word the benchmark seeds, or `not-exercised` for a
store no request touches. It connects to nothing and measures nothing; a `differ` is a finding to report
(for example, that the advisor reads "a page in key order" as a list and proposes a fifoStore where the
benchmark seeds a btreeStore, because only a range read asks for a btreeStore), not a failure of the run.
The stores, fields and counts of the inputs are derived from the model (`plan_concordance.go`), so a
changed layout changes them. The refs between buckets (`documentRefs`), the hash stores a request reads by
key (`keyedStores`) and the stores no request touches (`untouchedStores`, today `User.Friends`) are hand
tables, because the schema has no marker for them; a test checks every key names a real bucket field, but
the agree rows of the hash stores rest on those tables, not on the schema alone. The keyed hash stores
(`Post.Likes`, `User.Likes`, `User.Settings`, `Run.Scratch`) are written by key and never read by key in
the mix: their trace line stands for an upsert by key. Result with those tables: 11 agree, 1 differ, 7
open, 4 not exercised.

## The idiomatic hoardDB baseline (`--hoarddb-idiomatic`)

The idiomatic leg is `batch=on` plus `multiget=on` on the common layout: what a developer writes by
following `docs/user/howto-social.md` (a feed holds references, the posts are read with one get-many
frame, a write that touches several elements goes as one batch frame). It is not a new switch in the
adapter and not a new layout; it is the name of that one combination, so the default leg (everything
off) stays the like-for-like row against the other engines, and the idiomatic leg answers "what does
hoardDB do when used the way it is documented". Its records are named `...-batched-multiget` and carry
the label `idiomatic`; the native layout (`native=on`) is a further, more denormalised leg and is
never the idiomatic one. The wrapper takes `--hoarddb-idiomatic` (ring only) and refuses any other
batch, multiget or native request with it, so the label cannot sit on a different leg. Do not average
it with another leg.

## hoardDB declared aggregates (`-layout-variant common+agg`, `-param aggregates=on`)

The ledger's `Account` bucket can declare maintained aggregates on its stores: on `Expenses` a sum of
`amount`, a sum of `amount` by `category` and a count by `category`, on `Invoices` a sum of `amount`
(`scenarios/model/ledger.go`, `LedgerAggregates`, the one definition; the seeder, the adapter's probe and
the audit all read it). The SQL arms keep `expense_total` and `invoice_total` in the same transaction as
every write; the declared aggregates are hoardDB's own answer to that, one extra state key in the batch
of each element. The mix is unchanged: no request reads an aggregate (a new read op is a later stage), so
this leg shows what maintaining them costs the writes, nothing more.

Seed with `scenarios -layout-variant common+agg` (or `native+agg`; `bigasstest seed` passes the word
through, the wrapper does it with `--hoarddb-aggregates on`). The seeder checks every aggregate is ready
and that the maintained expense sum of every account equals the model's `expense_total`. Run with
`-param aggregates=on`: opening the ledger probes the bucket (declared aggregates exactly when the flag is
on, all ready) and refuses a seed of the other kind with the way out. After the window the audit reads the
maintained `Sum` of every Expenses and Invoices store the run wrote to (`SumReader`, `Bucket.Sum`) and
requires it to equal the sum of that store's elements read back, which in turn must equal seeded plus
acknowledged amounts, so a write that updated the element but lost the aggregate (or the reverse) fails
the run. The record's `maintained_sums_audited` says how many it read, and an aggregates run whose mix
writes expenses but read none fails (it proved nothing); `declared-aggregates` is listed in the
capabilities only when at least one was read. The reads that fetch the sum are the audit's, never a
request of the mix. The record carries
`aggregates` (`on` or `off`) and an aggregates-on record gets `-agg` in its leg name; never average it
with the leg without. The idiomatic label is not given to an aggregates-on leg. No figure from this
switch is to be published (competitor comparisons stay internal).

## Totals by category (`-op ledger.totals_by_category`)

A record-only read, not in the mix (so the request stream and its fingerprint do not move): the sum of
`amount` and the number of expenses of one account by category, one row per category sorted by category
(`Reply.Totals`). Each engine answers it the way an application on that engine would: hoardDB reads the
declared aggregate "sum amount by category" (`SumBy`, whose rows also hold the count; one frame for the
six categories, so one round trip) and so needs the `+agg` seed and `-param aggregates=on`, refusing
otherwise with the way out; PostgreSQL and MySQL run a `GROUP BY category` over the account's rows of the
expenses table the pages read; MongoDB runs a `$match` and `$group` over the same collection. None of them
reads a maintained summary row: the SQL arms' `expense_total` column is the whole-account total only,
and a by-category summary table would be a second schema the other legs do not have. The reply is checked
against the seeded totals of the account exactly (no expense is written while a record-only read runs), so
a wrong total, a wrong count, a missing or invented category and unsorted rows each fail the run. At the
default depth of 100 expenses the engines differ little; the figure to read is at `+deep<N>` with N in the
thousands, where a `GROUP BY` reads N rows and the aggregate reads six keys. Leg names add the op as for
any record-only leg. No figure from this read is to be published (competitor comparisons stay internal).

## A deep ledger (`-layout-variant ...+deep<N>`, `-ledger-expenses N`)

The ledger seeds 100 expenses per account. At that depth a sum over an account's expenses reads about a
hundred rows on every engine, so a maintained aggregate and a `GROUP BY` look alike. `+deep<N>` seeds N
expenses per account instead (`model.Options.LedgerExpenses`, `model.NewWith`; the first 100 expenses of an
account are the same as in the default ledger, the invoices after them are not, so the fingerprint differs).
It applies to every engine, not only hoardDB, and composes with `+agg` and `native`: `common+agg+deep10000`.
Seed with `scenarios -layout-variant <word>` (or through `bigasstest seed`, or the wrapper's
`--ledger-expenses N`), and run with `-ledger-expenses N` so the client's model matches the data; the
runner's count check refuses a run whose depth is not the seed's. The record carries `ledger_expenses`
(absent for the default) and the leg name gets `-deep<N>`; never average legs of different depth. The
client holds the whole model in memory, about N * 30 * scale expenses, so use a small scale for a large N
(depth 10000 at scale 1 is 3 * 10^5 expenses), and the wrapper refuses `--ledger-expenses` without
`--scale`, because the default scale and the one `bigasstest calibrate` recommends assume 100 expenses.
Rough size, read from the code and not measured: one expense is a `model.Element` (three strings, a key,
a sequence number and a slice header, about 100 B) plus three `Field` values (32 B each) and their boxed
values, about 250 B, held once in the dataset the verifier reads; so 3 * 10^5 expenses is on the order of
75 MB, before the verifier's own indexes. Check it against the client VM's memory before a large depth. The mix is unchanged, so the pages read still hold 50
elements; what changes is how deep the offset reads can start and what the audit and the totals read
(`ledger.totals_by_category`) have to cover.

## hoardDB-native layout (`-layout-variant native`, `-param native=on`)

The common model stores the same logical data on every engine, so hoardDB pays a keyed read per row
of a page. The native variant is the layout a hoardDB developer would pick: it keeps every bucket,
store type, id, key, order, count and the request stream of the common model and changes exactly two
element bodies (`scenarios/model/native.go` is the one definition):

| Element | Common body | Native body |
|---|---|---|
| `User.Feed` (social) | `{post, at}` | `{post, at, author, text}`, copied from the `Post` document |
| `Room.Members` (chat) | `{user, since}` | `{user, since, name}`, copied from the member's `User` document |

Nothing else differs: not the inbox (its page shows no text), not `User.Rooms`, not the ledger or agent
layouts (already one store page per read), and no declared aggregates or `of` expansion.

- **Seed** with `bigasstest seed -layout-variant native` (or `scenarios -layout-variant native` for one
  layout). Only the `hoarddb` seeder knows it; every other engine refuses the flag. The seed report
  carries `variant` and the model's `native_fingerprint`, and `bigasstest seed` refuses a report that
  disagrees (a scenarios binary from before the variant included). The wrapper seeds each leg with the
  variant it needs (`--hoarddb-native`, below) and tags a `--seed-copy` with it.
- **Run** with `-param native=on` (ring only; default `off`). A feed page is one `StoreList` (1 round
  trip, was 1 + one per post, or 2 with `multiget=on`) and a room page is two, whatever the member
  count. `native=on` with `multiget=on` is refused: the pair would be a mislabelled duplicate. With
  `batch=1` it is allowed: the writes are the same operations.
- **Wrapper.** `scripts/certification/bigasstest.sh --hoarddb-native off|on|both` (ring only, default `off`)
  passes `-param native=off|on` on every ring leg and adds `-native` to the leg's records and run log
  (`run-unbatched-native.log`) and `+nat` to its profile directory (`profiles/profile-off+nat`). `both` runs the
  common leg, then the native leg, reseeded between. `native=on` needs `multiget=off`, so `--hoarddb-native on`
  with `--hoarddb-multiget on` or `both` is refused; the one allowed mix is `--hoarddb-multiget both
  --hoarddb-native both`, the product minus that pair: per batch mode the legs are common/multiget off,
  common/multiget on, native/multiget off, in that order. `--hoarddb-batch` is the outermost switch (each batch mode
  runs the whole list). The facts carry `hoarddb_native`; the profile leg uses the native layout when
  `--hoarddb-native on` was given (run once per variant to profile both).
- **Seed copy.** The `--seed-copy` tag ends in `variant=common|native`. A copy of the other variant is never
  restored: the leg says so (`... holds the common layout but this leg needs the native layout, so it is NOT
  restored ...`), seeds fresh, and the copy is kept, not overwritten. Legs of one variant share a copy; keep one
  directory per variant (`both` with a single directory restores for the common leg and reseeds the native one
  every run). A copy made before the variant was in the tag counts as common.
- **Probe.** When a session opens the social or chat layout, the adapter reads the first feed element of
  `u0` and the first member of `r0` and refuses a server of the other variant (`this server holds the
  common layout but -param native=on was given. Reseed with -layout-variant native or change the flag`).
- **Verification.** The adapter strips the copies back into the common shape (`model.Strip`), so the
  shared verifier runs unchanged: an element is compared with the seeded one, and the `Post` or `User`
  document rebuilt from the copy with the seeded document. A read of a native page on a common seed fails
  naming `-layout-variant native`; there is no silent fallback.
- **Audit.** A page never reaches a written element, so a written copy is checked after the run: for
  every `Feed` and `Members` store the run wrote to, `EmbedReader.ReadEmbeds` returns the stored bodies
  and each copy must equal the document it was taken from (the `Post` of the same write, the member's
  `User` document in `WriteOp.Refs`; seeded elements must equal the seeded document). A wrong copy fails
  the repetition.
- **Record.** `settings.native` and the record field `native` are `on` or `off`, `native_fingerprint` is
  per layout (the model's hash of the embedded fields; a native record without it is refused), and a
  native record gets `-native` in its leg name, so it never shares a file or a summary table with the common
  leg. The summary names the table "hoardDB layout: native".

**Rule for every report.** A native row is shown only beside hoardDB-common, labelled "hoardDB, its own
layout"; never in a cross-engine ranking against engines running the common model (that compares a
denormalised arm with a normalised one), never published, and always with the note "freshness cost not
measured": the mix has no edit, delete or rename, so the benchmark rewards the copy and never charges for
refreshing it. Common stays the headline row.

`WriteOp.Refs` is the one addition to the adapter contract: documents a write copies data from without
writing them (the joining member's `User`). Every other adapter ignores it. The unit tests run over fakes;
`TestContractRingNative` and `TestNativeProbeRefusesAMismatchedSeed` start an in-process server and run the
contract with native on and off.

## hoardDB server configuration (the hoardDB level)

`scripts/certification/bigasstest.sh` starts the hoardDB node itself and pins its configuration the way
`engines.sh` pins MySQL's flush and PostgreSQL's fsync. After every start it reads `/proc/<pid>/environ` and
the server log; a value that is not there refuses the run. The driver cannot read any of this, so every
pinned value is in the facts file (`hoarddb_*`, plus `hoarddb_pinned_config` and `hoarddb_pinned_log`).
Every setting keeps an acknowledged write on disk (L-A); `L-B` (`HOARDDB_UNSAFE_ACK_BEFORE_SYNC=true`, record
only) already existed and is unchanged.

| Variable | Pinned | Evidence | Judgement? |
|---|---|---|---|
| `HOARDDB_UNSAFE_ACK_BEFORE_SYNC` | `false` at L-A, `true` at L-B | `docs/user/configuration.md` "Storage durability": false means a node answers a write only once it is on disk | no |
| `HOARDDB_WAL_PREALLOCATE` | `on` | the private serial-insert benchmark note (the ring node honours it; a large gain on serial inserts, same under msync and stepwise); the private WAL-preallocate rerun note (on beat off in every paired leg, RF=2); the data-only `fdatasync` of a zero-filled segment is still synced before the ack | yes: `auto` is on only for ext2/3/4 and the docs call XFS unmeasured; `on` is forced everywhere; the wrapper requires the server log to say the ring node's log is preallocated |
| `HOARDDB_BADGER_TABLE_SYNC` | `stepwise` on journaled ext4 without fast_commit, else `msync` (`BAT_TABLE_SYNC=stepwise\|msync` forces) | `configuration.md` "New table files": stepwise is tested and safe only on ext4 with its journal, unknown with fast_commit, unsafe on btrfs and ZFS; serial-insert note: stepwise and msync within noise of each other for the write path, so the gain is the tail only | **yes**: both are L-A durable where stepwise applies; stepwise "can lower write tail latency" and was not measured against msync under this mixed load; the probe reads the filesystem from `/proc/fs/jbd2` and `/proc/fs/ext4/<dev>/fc_info`, and the reason is in `hoarddb_badger_table_sync_reason` |
| `HOARDDB_WRITE_CONCERN` | `one` | the node default at replication factor 1 (`configuration.md`); written down so the read-back can prove it | no |
| `HOARDDB_PPROF_ENABLED` | `false` in every measured leg, `true` in the profile leg only | pprof binds the fixed `127.0.0.1:6060` of the server VM; the wrapper checks the log shows no listener in a measured leg and loopback in the profile leg, and fetches only from the VM itself | no |
| `HOARDDB_PPROF_BLOCK_RATE_NS`, `HOARDDB_PPROF_MUTEX_FRACTION` | `1000000`, `100`, profile leg only | the server samples block and mutex events only when these are set (they apply only with pprof on) | no |
| `HOARDDB_TCP_PIPELINE_FRAMES` | not set (4) | the runner keeps one request in flight per connection, so read-ahead cannot matter | no |
| `HOARDDB_BADGER_BLOCK_CACHE_MB` | not set (128) | a raised cache exists as a separate `cache-raised` variant in `engines.sh`; the other engines run their own default caches | **yes**: 67% of requests are reads, a larger block cache may help them; left at the default for fairness with the engines' defaults |
| `HOARDDB_MAX_CONNS`, `HOARDDB_TCP_MAX_CONNS_PER_IP` | 8192, 4096 | so 128 virtual users from one address are not refused (as `engines.sh` does) | no |

## Profiles (their own leg, never inside a measured one)

A profiler costs throughput, so no sampler is ever attached to a measured leg. The measured sweep runs with
the hoardDB pprof listener **off** (`HOARDDB_PPROF_ENABLED=false`, no listener and no block or mutex sampling,
checked from the server's environment and log at every start) and with no perf, pidstat or iostat running.
After the sweep and its summary are written, the wrapper runs **one extra profile leg per engine**: 64 virtual
users (`--profile-vusers`), one repetition, 60 s measured, the same mix, the engine restarted fresh. Its records
carry `profiled: true` (`-profiled` on the runner), get `-profiled` in their file names, and the summary lists
them in a section of their own: they are in no median. `--no-profile` skips the leg. The profile leg uses the
batch mode `on` when `--hoarddb-batch on` was given and `off` otherwise.

`--hoarddb-multiget off|on|both` (ring only, default `off`) passes `-param multiget=off|on` the way `--hoarddb-batch`
passes its param, and `both` sweeps the one-Get-per-id read first, then the get-many read, with a reseed between. The two
switches are independent: the legs are their product, batch outermost, so `--hoarddb-batch both --hoarddb-multiget
both` is four legs (batch off/multiget off, batch off/multiget on, batch on/multiget off, batch on/multiget on). The
facts carry `hoarddb_multiget`; the profile leg uses multiget `on` when `--hoarddb-multiget on` was given and `off`
otherwise (run once per form to profile both). The runner must contain the multiget param.

The capture starts when the runner writes `measure-start-v64-r1` into `-mark-dir` (the measured window of the
profile leg opened, so the warm-up is over) and lasts 30 s (`--profile-seconds`). Files go under
`OUTDIR/profiles/profile-<mode>/` (outside the repository); the profiled record names them in
`facts.profile_files` and `facts.profile_status`. A failed capture is a line in `status.txt`, not a failed run.

- hoardDB: only in the profile leg the server starts with `HOARDDB_PPROF_ENABLED=true` plus
  `HOARDDB_PPROF_BLOCK_RATE_NS=1000000` and `HOARDDB_PPROF_MUTEX_FRACTION=100`. The wrapper fetches, on the
  server VM over its loopback (never exposed, never tunnelled): `cpu.pb.gz`, `block.pb.gz` and `mutex.pb.gz`
  (delta profiles of the same window) and `goroutine-mid.txt`. The sampling knobs are what make the block and
  mutex profiles non-empty; a server that predates them leaves those two empty and `status.txt` says so.
- Other engines: `perf record -F 99 -g -p <server pid>` for 30 s (`perf.data.gz`, `perf-report.txt`). perf (and
  sysstat) is installed with apt only if missing and only with passwordless sudo, just before the profile leg,
  and removed right after it (`profile_undo`). If perf cannot run: `vmstat 1`, `iostat -x 1` and
  `pidstat -u -r -d -p <pid> 1` for the same window.
- Local rehearsal of the capture script and the pinned start, with no VM: `scripts/certification/selfcheck.d/46-bigasstest-wrapper.sh`.

## Verification (the shared code, never the adapter)

Latency stops at the last reply byte; the check runs afterwards, before the user's next request.
A seeded element must equal the model byte for byte, in order for fifo and btree stores and as a set
for hash stores. Written content is recognised by content (the runner registers what `gen.go` made
for each write id before sending it). A page must hold at least `min(limit, seeded count)` elements,
so an empty result is a miss (`errShortPage`). After the run, for every store written to, the engine's
count must equal the seeded count plus the distinct acknowledged writes, pushes minus acknowledged
claims for the agent task queue; each ledger account's running sum must equal a full read. An
unacknowledged write only raises the ceiling. A failed check fails the repetition.

## Writing an adapter (frozen interface, `adapter.go`)

An adapter owns `engine_<name>*.go` and `engine_<name>_test.go` and edits no shared file. It registers
in `init()` with `register("ring", open)`. Rules, all repeated in the godoc at the top of `adapter.go`:

- `Engine.Settings` must return at least `engine_version` and `tls` (the cipher, or `off`), plus the
  engine's own read-back (hoardDB `HOARDDB_*` environment, MySQL variables, Mongo write concern and
  cache, PG `pg_settings`). `Engine.Session(ctx, i)` is one connection (or pool slot) per virtual user;
  pools are sized to `Config.VUsers`.
- `Session.Layout(l, database)` gets the **full** database name `<base>_<layout>`.
- `Reply` carries `Documents`, `Elements`, `RoundTrips`, `Commits` and `Retries`. Integers are `int64`,
  text is `string`, vectors are `[]byte`. A document read returns **scalar fields only** (no store fields).
  Adapters never verify, never retry silently (a retry is allowed only if it is counted in `Reply.Retries`;
  the runner sums it per operation and labels a record that has any), never pipeline sub-operations and
  never add or drop a field; the seeded SQL schemas have no marker column, so a written element is
  stored exactly as the `WriteOp` gives it.
- Reads and what each `Reply` must hold: the table at the top of `adapter.go`. Pages read oldest first.
  Page sizes are the spec's, or the fallback of "Page size and eligible owners" (`ReadOp.Limit`, and
  `ReadOp.MemberLimit` for the members of `chat.room_page`, 0: every member), and an ordered page never
  holds written elements. The record carries them as `elements_per_read` and `read_pages`. An adapter applies
  `Limit` and `MemberLimit` as given (0 means every element), also to hash stores. `ReadOp.Offset` (and
  `ReadOp.AfterKey` for the btree) starts a page of an ordered store inside it, see "Page offset" below;
  use a range where the engine has one and say in the doc comment of the read which form you chose.
- Two read-backs serve the post-run audit, off the measured window. `LayoutOps` must also implement
  `StoreReader.ReadStore(ctx, ref)` (every element of one store of one document, keys set, any order): the
  audit reads back every keyed store the run wrote and requires each key to hold what an acknowledged put gave
  it. An engine that declares the denormalised counters (SQL, Mongo) must implement
  `CounterReader.ReadCounters(ctx, bucket, id)` too: every counter a write touched must equal the seeded value
  plus the deltas applied (a delta applies only when the element it names was newly created). hoardDB derives
  its counters and does not implement it. The contract suite refuses an adapter without them.
- `LayoutOps.Count` with an empty Owner and Store must work for every bucket of the layout (document
  count): before each repetition the runner compares the counts of every bucket and of eight owners spread
  over each sampled store with the seed, and refuses a data set that was not just seeded.
- Every request runs under `-request-timeout` (default 30 s, under the 60 s histogram ceiling). A request
  past it counts as an error with its elapsed time in the latency, so one hung request cannot stall a level;
  adapters must honour the context they are given. A latency above the ceiling (open loop) is clamped and
  counted in `latency_clamped`, never an error.
- The runner reads `Engine.Settings` twice, before the load (`settings`) and after the measured window
  (`settings_after`), so end-of-run counters reach the record.
- `WriteOp` carries `Documents` (**upserted**, not inserted: a repeat replaces), `Elements` (push for fifo and lifo stores, put-by-key for hash and
  btree; SQL `seq = SeqBase + Ordinal`, `SeqBase = 2^40`), `PreRead` stores to read first (counted in
  `RoundTrips`), `Counters` (applied only by engines whose schema declares them, only when the named
  element was newly created) and an optional `Claim` (claim 1, ack, then apply `Elements` as the
  replacement; if the queue gives nothing, apply nothing and return no elements).
- `StoreRef{Bucket, Owner, Store}`; an empty Owner and Store count the documents of the bucket.
  `LayoutOps.Count` serves the audit. The ledger audit's full read is the read kind `ledger.expenses_all`.
- SQL: the rows of one operation go in one transaction (`Commits`); document stores without
  transactions write one after another.

Every adapter's live test is one call: `runContract(t, contractTarget{Engine: eng, Name: "ring",
Database: "bat", SeedArgs: [...]})`. It seeds scale 2 through a freshly built `scenarios` binary (or
through `Seeder`), runs every read and write kind of every layout through the runner's own request
path, verifies and audits. `fake_test.go` is an in-memory reference implementation of the rules.

## Files

`adapter.go` (interface), `runner.go` (sweep, loops, warm-up, collection), `exec.go` (one request),
`mix.go`/`plan.go` (what to do), `gen.go` (run-time content), `expect.go` (seeded data set),
`verify.go`, `audit.go`, `seed.go`, `record.go`, `summary.go`, `diskguard*.go`, `hist.go`, `arrival.go`,
`clientcpu.go`, `mark.go` (window marks for the profile capture), `flags.go`, `zipf.go`, `calibrate.go`, `main.go`. Tests: `go test ./bench/bigasstest/ -race`.
`BIGASSTEST_MEASURE=1 go test -run WorldMemory -v` measures the client's memory at scale 300
(the world model alone, about 5.7 GB for 14.8 million seeded documents and elements at the 2026-11-07
density; a real run's resident memory is larger, see "Growth and the scale to seed"). `go test ./bench/bigasstest/` at its full length builds scale 300
one layout at a time and peaks near 4.3 GB; `-short` stops at scale 20.
