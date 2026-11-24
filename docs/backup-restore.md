# Backup and restore

`hoarddb dump` writes **one file** for the session's view of the database: every
database, bucket, entity and store element the session may see. On a cluster it reads every
member, because each member answers only for the data it owns (see
[Dump a cluster](#dump-a-cluster)). Blob payloads, users,
roles and the other items listed under [What a dump guarantees](#what-a-dump-guarantees)
are not in it. It runs while the database keeps serving: it takes no lock
and blocks no write. It is **not a point-in-time copy** — stop the writers first if you
need the file to be one moment. A dump run by a user who is not root holds only what that
user may see; the file records the user in its header, and the end of the run says so.

`hoarddb restore` puts a dump back into a ring that does not hold the database yet.
Users, roles and blob payloads are not in a dump, so they are not restored either;
[Restore a dump](#restore-a-dump) covers what a restore does carry and where it stops.

## Take a dump

With a database `app` whose bucket `users` holds three documents:

```sh
$ hoarddb dump --out app.hoarddump
Dumping while the database stays online. This dump is not a point-in-time copy: each entity is read once, entities are read at different moments, and an entity that is written to while it is read can be saved half-updated. For a copy of one moment, stop the writers (or dump during a quiet period) first.
Dumped what user "root" may see. Databases that user may not see are not in this file.
Done: app.hoarddump, 3 entities, 0 elements. This dump is not a point-in-time copy (see the start of this output); if writers were active, it may mix moments.
```

The first and last lines say what the file is not. Every line goes to standard error, so
`--json` can print its one object on standard output for a script.

Check the file before you rely on it, with no server involved:

```sh
$ hoarddb restore app.hoarddump --verify
This dump was taken online and is not a point-in-time copy: entities were read at different moments, and an entity written to during the dump may be half-updated.
Verified app.hoarddump: databases 1, buckets 1, entities 3, elements 0
```

The commands above sign in as the root user: pass `--user` and `--password`, or set
`HOARDDB_ROOT_USER` and `HOARDDB_ROOT_PASSWORD` to keep the password out of `ps`, as
[Get started](getting-started.md#connect-the-cli) does. `--insecure` skips the certificate
check, so it is for a server you started yourself; against any other host, leave it out and
pass `--fingerprint SHA256:<hex>`.

> **Upgrade the CLI before you trust a dump of a cluster of more than one node.** The 1.1.0
> `hoarddb dump` read only the member it connected to, so on a cluster its file held that
> member's share of the data and it still exited `0`. Dumps of a single node are not
> affected. Take a new dump of every cluster with this release's CLI; it works against 1.1.0
> servers, and 1.1.0's `restore` reads the files it writes.

## Dump a cluster

Each member of a cluster answers listings for the slots it owns, so `dump` connects to every
data member and reads each one's share. You give one member's address; the dump learns the
others from it (the same list `nodes` prints) and signs in to each with the same user and
password.

- **Trust.** Every member has its own certificate. Pass `--fingerprint` once for each member
  (each one prints its fingerprint when it starts; a member's own key is also pinned, under
  its id, in the other members' `hash_ring.seed_fingerprints`). A member whose certificate matches none of them stops the
  dump, which names the member and the certificate it presented; the dump never falls back to
  an unverified connection. The fingerprints are one list, not one per member: each member
  must present one of the pinned keys, and the dump then checks that the server is the member
  it expects (its node id and the cluster's epoch). `--insecure` skips the check for every
  member.
- **Addresses.** The dump connects to the client address each member publishes. A member
  whose client listener is on a loopback or wildcard address and that has no
  `HOARDDB_TCP_ADVERTISE` publishes none; the dump stops and names it. Give its address with
  `--member <node id>=<host:port>`, once for each such member, or set `HOARDDB_TCP_ADVERTISE`
  on it.
- **A member that is down or cannot be reached stops the dump.** Its data can be read only
  through itself while it is up, so a dump without it would look complete and not be. When the
  cluster has marked the member down, the data it owned is read from its other copies; a
  database with a single copy, or one whose every copy is down, stops the dump and is named,
  so you can start the member again or leave that database out with `--db`. `--db` and
  `--bucket` apply the same rule to their scope only.
- **The membership must not change during the dump.** The dump checks that every member is at
  the same membership epoch when it starts and again before it completes the file. If a member
  went down or came back, or a database's number of copies changed, in between, a document
  whose owner moved could be in the file twice or not at all, so the dump stops with exit `8`
  and writes no file; run it again once `nodes` shows the same members everywhere. Writes
  during the dump are not checked: the file is not a point-in-time copy (see below).
- **Order.** Databases are in name order. Within a database, the members' shares follow each
  other in member id order, each in the member's own order. So two dumps of a quiet cluster
  hold the same records in the same order whichever member you connected to.

## What a dump guarantees

> **What a dump guarantees.** The file is whole or it does not exist: it ends with a summary and a footer that count every record and carry a checksum, and `restore` checks both before it touches a server. Each entity (a document and all of its stores) is read once. The elements of every store are in the store's own order, so a fifo pops in the same order after a restore.
>
> **A dump is not a point-in-time copy.** It reads while your application keeps writing, and it reads each entity once, at its own moment. A write that lands during the dump may or may not be in the file; two entities can be from different moments; **an entity that is written to while it is being read can be saved half-updated** (for example its fields from before an update and one of its stores from after it); a document created after the dump passed its place is missing; a document deleted after is still in; a reference from one entity to another can dangle (a feed entry whose post was deleted a moment later). **The dump does not detect any of this and does not warn about single entities**; it says at its start and its end that it is not a point-in-time copy.
>
> **How to get a copy of one moment.** Stop the writers (or take the dump during a quiet period when nothing writes), then run `hoarddb dump`. With no writes during the run, the file is exactly the database as it was.
>
> **Queue messages that are claimed or dead-lettered are not included in a dump.** A message a consumer has claimed and not yet acknowledged, and a message that was dead-lettered after five failed deliveries, are not in the file, and the dump cannot tell how many there were; it says so at its end whenever it holds a queue (`fifoStore` or `lifoStore`) field, and restore says so too. To include claimed messages, stop the consumers or let them acknowledge or release their messages, then dump; if a consumer died holding a lease, a claim or an acknowledgement on that queue returns its messages after the lease runs out (a message on its fifth return goes to the dead letters instead), and waiting alone does not. Dead-lettered messages cannot be included, listed or cleared. A dump exits 0 when it wrote a whole file; the exit code says nothing about queue messages or about writes during the dump.
>
> **What a dump does not contain.** Users, roles, blob payloads, aggregate values, the dedupe window, claimed and dead-lettered queue messages, data in a node's earlier single-node store, and anything the user who ran it may not see.
>
> Dump is read-only: it takes no lock and no pin, so it never blocks a write or a membership change.

## Flags

| Flag | What it does |
|---|---|
| `--out <file>` | The file to write. Default: `hoarddb-<run start, UTC>.hoarddump` in the current directory. |
| `--db <name>` | Dump only this database. Repeat for more than one. |
| `--bucket <name>` | Dump only this bucket. Needs exactly one `--db`. |
| `--parallel <n>` | How many entities of a bucket are read at once, from 1 to 64. Default 8. For a database nobody writes to, the file is the same whatever the number; `--parallel 1` reads one entity at a time. One reader holds up to about 170 MiB with maximum-size elements (measured), and the total is not linear in the number; see [Memory, speed and `--parallel`](#memory-speed-and---parallel). |
| `--json` | Print one JSON object to standard output after the text report: the file, the four counts, the consistency word and statement, whether queue messages were left out, and the lines about data in a node's earlier store (`earlier_store_left_out`). |
| `--member <node id>=<host:port>` | The client address of a cluster member that publishes none. Repeat for each such member. |
| `--leave-legacy-store-data` | Dump the ring even when a node also holds data in its earlier single-node store, which the file does not carry; see [Data in a node's earlier store](#data-in-a-nodes-earlier-store). |
| `--address`, `--user`, `--password`, `--fingerprint`, `--insecure` | The connection, as [Get started](getting-started.md#connect-the-cli) shows. On a cluster, repeat `--fingerprint` once for each member. |

## Memory, speed and `--parallel`

A dump reads the entities of a bucket one page at a time, and with `--parallel 8` (the
default) up to eight of them at once. The file lists the entities in the same order
whatever the number, so a dump of a database nobody writes to is the same file with
`--parallel 1` and with `--parallel 8`.

Memory does not grow with the size of an entity or of the database. Each entity being
read waits in a buffer of up to 1 MiB; a larger entity is held in a temporary file named
`<out>.spill.<n>` next to the output, and at most `--parallel` such files exist at a
time. The dump deletes each one as soon as it has been copied into the file, and on every
way out of the run. A spill file is a local temporary file: the file's checksum covers
the bytes as they were copied into the dump, so a byte of a spill file that is damaged
before the copy is not detected.

The ring takes a store element of at most 1 MiB, and a page of a store is capped by bytes, so a
page holds at most about 31 elements of 1 MiB. With maximum-size elements one reader holds up
to about 170 MiB (one measurement with `--parallel 1`: the reply, its decoded copy and the chunk being
written). The total is not linear in `--parallel`: 2 readers measured about 180 MiB and 8
readers about 290 MiB. A worst case at `--parallel 64` was not measured. Real runs are far
below that: a run over entities of 100,000 elements of 1 KiB peaked at about 4 MiB. Lower
`--parallel` on a small machine.

A dump that was killed (`kill -9`) can leave a `.partial` file and `.spill.*` files next
to `--out`: delete them and run the dump again.

## Warnings

These lines go to standard error and do not change the exit code.

- **A bucket was dropped during the dump.** The dump names the bucket and goes on with the
  next one. With `--parallel 1` the line is `Bucket app.Post was dropped while the dump was
  reading it, so this file holds only what was read before the drop; the entity being read
  at that moment may be only partly in it.`; with a higher `--parallel` it ends `the
  entities being read at that moment are not in it.` Either way the file is whole and
  `hoarddb restore --verify` passes.
- **With `--leave-legacy-store-data`, a node also holds data in its earlier single-node
  store.** The dump says, at its start and its end, `Node <id> also holds <n> buckets in its
  earlier single-node store ... that data is NOT in this file` (see
  [Data in a node's earlier store](#data-in-a-nodes-earlier-store)).

## Data in a node's earlier store

Programs that use the earlier driver interface (the public Node and Python drivers, the Go
driver's root package) write to a store on each node that the ring does not read. **That data
is not part of a ring dump: dump and restore do not carry it.** Before it writes anything,
the dump asks every member whether it holds such buckets, and if one does (or cannot say), it
refuses with exit `2`, names the nodes, and names the flag that goes on:

```text
dump: Node n1 holds data in its earlier single-node store, or could not say whether it does. Node n1 also holds 1 bucket in its earlier single-node store, ... That data is not part of a ring dump: dump and restore do not carry it, so this file would not be a whole backup of these nodes. To dump the ring and leave that data behind, run the same command again with --leave-legacy-store-data, and keep the data directory of each node named here until you have moved that data yourself. No dump file was written.
```

With `--leave-legacy-store-data` the dump writes the ring's data and repeats the node's line
at its start and its end, and `--json` carries the lines as `earlier_store_left_out`. No
command in this release copies that data into the ring; keep the node's data directory until
you have moved it yourself.

## When the ring is changing

Before it starts, the dump refuses a member that is down while it owns data no other member
holds, and a member it cannot reach (see [Dump a cluster](#dump-a-cluster)). During the run,
a page refused because its slot is moving is asked for again, up to 5 tries one second apart;
if it is still refused, the dump stops with exit `8`, names the page and the command to look at
the members (`nodes`), and no file is written. A membership change that the members finish
during the run stops the dump with exit `8` before the file is completed. Run the same dump
again.

## The file

- One file, mode `0600`, owned by the user who ran the dump.
- It is written as `<name>.partial` and renamed to `<name>` only after the footer is
  written and synced, so a cut-off file never sits under the name you would restore from.
  A failed or interrupted run removes the `.partial` file.
- An existing `<name>` is refused, never overwritten. Pick another `--out`.
- The file ends with a summary of every count and a footer with a checksum over
  everything before it.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | A whole file was written. |
| `1` | Usage: a flag, a positional argument, an `--out` that cannot be written, an `--out` that already exists, or a cluster member with no client address (give it with `--member`). No file was written. |
| `2` | The server refused, there was nothing to dump, or a node holds data in its earlier single-node store and `--leave-legacy-store-data` was not given. No file was written. The message names the fix; running the same command again gives the same refusal. |
| `4` | Interrupted; the `.partial` file was removed. |
| `6` | The connection to the address you gave, or to another member, failed, or a member's certificate matched no `--fingerprint`. |
| `7` | Sign-in failed, on the address you gave or on another member. |
| `8` | The outcome is unknown or the cluster is changing: the connection dropped before the answer arrived, a page was still refused as moved after 5 tries, a database has data on a member that is down and on no other copy, or the membership changed during the run. No complete dump file was written, because the file gets its final name only after its footer; the run says so and you can start the dump again. |

Dump never exits `3`: it writes a whole file or no file.

## When it refuses

A ring with no database this session may see is an error, not an empty file:

```text
The ring on this server holds no databases that user "<user>" may see. Check the user's permissions, or create one with 'create database <name>'.
```

A dump that finds no bucket in any database in scope is refused for the same reason:

```text
The ring holds no buckets in <scope>, so there is nothing to dump. Run 'show buckets in <database>' to list what exists, or 'create bucket <name> in <database> { Name string }' to create one.
```

A `--db` the session cannot see names the listing that shows what exists
(`Database "x" not found. Run 'show databases'...`), and a `--bucket` that is absent names
the bucket listing.

## Checking a dump

`hoarddb restore <file> --verify` reads the whole file once, checks the format, the counts
and the checksum, and prints the dump's consistency statement and the four counts. It
contacts no server, so it works with the database stopped, and it needs no running node.

## Restore a dump

Restore never merges into existing data. A ring that still holds the database refuses the
run before it writes anything, and the refusal names the two ways forward:

```sh
$ hoarddb restore app.hoarddump
This dump was taken online and is not a point-in-time copy: entities were read at different moments, and an entity written to during the dump may be half-updated.
Restore refused: database "app" already holds 1 bucket (users), and restore never merges into existing data. Nothing was written.
Restore into a new name:  hoarddb restore app.hoarddump --db app --as app_restored
Or empty "app" first, then wait until 'show buckets in app' lists nothing:  drop bucket users in app
(There is no drop database statement yet, so the buckets are dropped one by one.)
```

The dump's own statement comes first, because a restore is not one moment either. Restoring
under another name leaves the original database alone:

```sh
$ hoarddb restore app.hoarddump --db app --as app_restored
This dump was taken online and is not a point-in-time copy: entities were read at different moments, and an entity written to during the dump may be half-updated.
Restoring database app from app.hoarddump: databases 1, buckets 1, entities 3, elements 0
Restored app_restored.users: 3 entities, 0 elements
Restored app.hoarddump: databases 1, buckets 1, entities 3, elements 0 in <time>.
```

Every line goes to standard error, so `--json` can print its one object on standard output.
The line after these names the engine the restore wrote to.

### What restore does

1. It checks the whole file first, with no server involved: the same check `--verify` makes,
   of the format, the counts and the checksum. It prints the dump's not-point-in-time
   statement before anything else. A damaged or cut-off file never reaches the server, and
   the run exits `1`.
2. It reads the ring and refuses, before it creates anything, with exit `2` and text that
   names the fix. On a cluster it reads every member for this, as a dump does, because each
   member lists only the buckets it owns; the same `--fingerprint`, `--insecure` and
   `--member` rules apply. It refuses a database that already holds buckets (restore never merges and never
   overwrites — there is no flag for either and no `--resume`); a database whose buckets are
   still being dropped, because a name is free again only when its drop has finished (run
   'show buckets in <db>' until it lists nothing, then run restore again); a database that
   exists with another replication factor or storage; a database whose number of copies the
   target cannot hold (more than one copy needs at least three voters, and each copy a data
   member: form the cluster first); a link that cannot be created (see
   [Links between buckets](#links-between-buckets)). Writes go through the member you named,
   which sends each one to its owner.
3. It recreates each database and bucket with the same fields in the same order, then puts
   back every entity and every store element in the store's own order, so a fifo or lifo pops
   in its original order. It confirms each document's insert before it pushes into it.
   Aggregates are declared again and rebuild on the server; the run says how many are
   rebuilding.
4. It prints one line per bucket and a summary.

Restore needs a session that may create databases and buckets and write to them; a refusal
names the operation that was not allowed. The file must be a regular file that restore can
read more than once, so a pipe and standard input are refused.

### Links between buckets

A bucket field can be `of` another bucket. Restore creates the buckets that take part in a
link right after their database, the target first and empty, and fills them when it reaches
their records. Databases whose buckets link into each other (database a holds a bucket linked
to one in b, and b holds one linked to one in a) are created together, then every linked
bucket of theirs in one order, targets first, and only then is anything filled. It refuses,
before anything is created:

- A link to a bucket that was dropped before the dump, shown as `#<number>`. Fix the field on the
  source, for example with `alter bucket Post in app { drop Author }`, take a new dump, and
  restore that.
- A link to a bucket that is not in the dump and not on the target ring. A target on the
  ring is checked by name only: restore checks nothing about its fields or documents.
- Buckets that link to each other in a circle. The ring cannot make one, so a dump the ring
  wrote never holds it.

Any link from a bucket of one database to a bucket of another makes the two databases a
group, even a one-way link (z links into a): without `--db`, restore creates a group together
and restores it whole. With `--db`, a database whose links point into a database that is not
on the ring is refused, so databases that link into each other cannot be restored one at a
time; `--db a` alone works when nothing in `a` links out. A link from the selected database
into one that is already on the ring is fine.

With `--as`, a link written `<Database>.<Bucket>` that names the renamed database follows the
new name.

### Flags

| Flag | What it does |
|---|---|
| `<file>` | The dump to restore. One file. |
| `--db <name>` | Restore only this database of the dump. Without it, every database in the file. |
| `--as <new name>` | Restore the database `--db` selects under this name. It needs `--db`. |
| `--verify` | Only check the file; contact no server. |
| `--json` | Print one JSON object to standard output as well as the text lines. |
| `--member <node id>=<host:port>` | The client address of a cluster member that publishes none. Repeat for each such member. |
| `--address`, `--user`, `--password`, `--fingerprint`, `--insecure` | The connection, as [Get started](getting-started.md#connect-the-cli) shows. On a cluster, repeat `--fingerprint` once for each member. |

The object of a completed restore holds `file`, `databases`, `buckets`, `entities`,
`elements`, `blob_fields_left_empty`, `aggregates_rebuilding` and `consistency` (its `word`
and `statement`); when the file has a queue field it adds `queue_messages_left_out` and
`queue_messages_left_out_text`. A run that failed prints `error`, `exit` and `state`, which
is what it left behind.

### When a restore stops

A restore that stops after it began writing leaves the databases and buckets it created so
far on the ring. It prints what it created: the databases written in full, the database in
progress with its complete buckets by name (the first 10, then "and N more"), the one
bucket that is not complete, the buckets it created empty ahead of their turn because
another bucket links to them, documents that exist without their stores, and a torn-group
note when a store frame failed or was not confirmed. A bucket is filled completely before
the next one starts, except for the buckets created ahead for links.

Running the same restore again is refused, because the target is no longer empty. Recovery
is per database, and the run prints one command per unfinished database: `hoarddb restore
<file> --db <name> --as <new name>` for the database in progress, `hoarddb restore <file>
--db <name>` for one it never started, and nothing for one it finished. Or drop the buckets
it names, wait until 'show buckets in <db>' lists nothing, and run the same restore again.

For databases that link into each other, no command restores one of them alone and leaves the
others right (they would stay linked to the buckets left behind), so the run prints none for
them: drop every bucket in each of them, wait until `show buckets in <database>` lists nothing
for each, and run the same restore again.

Do not run two restores of one dump into one ring at once: each document is won by one of
them and the loser stops, so one or both stop, and a fifo is never pushed into twice. Wait
for drops to finish before restoring into the name of a dropped bucket. Ctrl-C stops the
restore at the next call it makes and exits `4`.

### Exit codes (restore)

| Code | Meaning |
|---|---|
| `0` | Restored everything the file holds. |
| `1` | Usage or unreadable input: a flag, a damaged or cut-off file, a file that is not a regular file, a file that changed while it was being restored, or a cluster member with no client address (give it with `--member`). |
| `2` | The ring refused: a pre-write refusal above, or a write the ring refused. The text names the entity, the operation and the server's reason. |
| `4` | Interrupted. |
| `6` | The connection to the address you gave, or to another member, failed. |
| `7` | Sign-in failed, on the address you gave or on another member. |
| `8` | The outcome is unknown: a frame was sent and never answered, even after the resends; or, before anything was written, a target database has data on a member that is down. Read before you re-run. |

Restore never exits `3`.

### What is not carried

- **Users and roles.** A dump holds neither; create them on the target before or after the
  restore.
- **Blob payloads.** The blob field is re-created empty, and the run prints `Bucket <db>.<bucket> field <field> (blobStore): payloads are not in a dump, so this field restored empty.` once
  per field.
- **Aggregate values.** They are rebuilt from the documents.
- **Claimed and dead-lettered queue messages.** When the file has a queue field, restore
  prints: `Queue messages that were claimed or dead-lettered when the dump ran are not in this file, so they are not restored.`
- **The dedupe window.** It is not in a dump.
- **Anything the user who ran the dump could not see.**

A restore is not one moment either: it carries the dump's own statement forward.

### Known limits

- The retry commands the run prints are not quoted for a shell. A database name or file path
  with a space or other shell characters needs quotes added by you.
- A bucket whose name contains a space cannot be addressed by any statement, so its drop
  command in a refusal cannot be typed as printed. Restore under a new name with `--as`
  instead.
- A bucket that appears in the target between restore's check and its create can be adopted
  when it has the same declaration; nothing else is merged. Do not write to the target while
  a restore runs.
- The ring takes a store element of at most 1 MiB, so a dump the ring wrote holds none
  bigger. A dump edited by hand to hold a bigger one is refused at that element (exit `2`)
  and the restore stops there.
- A bucket created empty for a link is open to writers until the restore reaches it. The
  restore reads one entry before it fills such a bucket and refuses (exit `2`) if anything
  is there; a writer can still insert in the round trip between that read and the first
  insert, as for any bucket. A database of a group is read again when the file reaches it,
  and refused if it holds a bucket the restore did not make; a writer that adds one after
  that read is not noticed, as for any database.
- Restore keeps one record in memory for each bucket that declares or is the target of a
  link: about 0.6 KB for a bucket of two fields and about 0.9 KB for eight, growing with the
  number of fields (about 60 MiB and 90 MiB at 100,000 such buckets; measured on one host by
  an opt-in test). It creates those buckets
  before it fills any, so a restore killed with a million linked buckets leaves a million
  empty buckets to drop.
