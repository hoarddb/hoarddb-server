# Changelog

All notable changes to hoardDB are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). hoardDB's first release
is 1.1, so everything below is its entry, written from the project history up to
the release.

Release numbers will follow `MAJOR.MINOR`. A change that a program could notice,
such as a changed setting or a changed on-disk layout, is listed under **Changed**
or **Removed** with what to do about it. The rules for upgrading are in
[docs/user/compatibility.md](docs/compatibility.md).

## [1.1.1]

### Fixed

- **An insert sent right after a node restart could be acknowledged and not written.** For a few seconds after a cluster node restarts, and while a member returns or ownership of a slot moves, the node holds the slot and orders no write. An insert sent through a driver or the CLI in that window was answered `applied`, with no copy confirmed and position 0:0, and the document was never stored. It is now refused `UNAVAILABLE` with "Nothing was written. Retry it in a few seconds", as every other write already was; a retry with the same request id is safe. Inserts the 1.1 server acknowledged with `copies_confirmed` 0 were not stored: write them again.
- **An insert answered `applied` when its write concern was not met.** An insert stored on fewer copies than its concern asks for (for example on one node of two while the other is down) was answered `applied`; it now carries the outcome the owner decided, `applied_degraded` or `unknown`, as updates, deletes and store writes already did.
- **The lost-majority refusal and the dropping-bucket refusal print no unfilled figures.** A read refused because the node could not see a majority of its voters printed `<v> voters (<n> answered)` as written, a write printed `too few answered`, and a refusal of a bucket being dropped printed `<n> of 8192 slots swept` or `0 of 8192`. The texts now say how many voters the cluster has where the node knows it, and no longer name a count the node does not have.
- **`hoarddb dump` and `hoarddb restore` wait out a brief loss of quorum instead of stopping.** A cluster member that had not seen a majority of its voters for a moment, or had not yet renewed its lease, ended the whole dump or restore. Both now ask again after a short, growing wait, for up to a minute, and a dump still reads each entity once. A member that stays without a majority for that minute stops the run with the same message as before, which names the fix.
- **A batch that has an operation on a store refused now leaves the later operations on that store unapplied, so their order is kept.** The server used to go on with the rest of the batch, so a push refused in a brief loss of quorum could be followed by an applied push to the same store, and sending the refused one again put it behind. The later operations on that store, also those behind an operation whose outcome the server could not confirm, are now refused with a message that says so and that nothing was applied; send them again in order, which resending the batch with the same request id does. Operations on other stores and documents in the batch carry on as before, and `hoarddb restore` resends such a batch instead of stopping.
- **Writes accepted with one copy while an owner was down could be lost when it came back.** At replication factor 2,
a write to a slot whose owner was down was stored on the one other copy (at 3, the same held for a write that reached
only one copy). When the owner returned and that copy did not
answer it at once (it can miss the first seconds after a restart), the owner took the slot back without the write, and
the other copy then dropped it too. A returning or restarted owner now waits until the copy that took its writes has
answered; while it waits, writes to that slot are refused with a retryable error. If that copy cannot come back, the
cluster marks it down after `failover_ms` and the slot opens without what only it held; `replace-node` replaces a node
that will never return. The same wait applies to any handoff, a `join` for one: a new owner takes a slot only when every
live copy and holder has answered it, so at replication factor 3 a handoff can wait while one copy is unreachable from
the new owner, where two copies were enough before. The wait ends when that copy answers or is committed down; if the
leader reaches the copy and the new owner does not, the wait lasts until the network heals. Writes to that slot are
refused with a retryable error meanwhile. No per-bucket cost; O(1) in buckets; no per-bucket metric.
- **`hoarddb dump` on a cluster of more than one node wrote a partial backup and exited 0.** Each member of a cluster
answers listings only for the data it owns, and the 1.1 dump read through the one member it connected to, so its file
held that member's share and said nothing. The dump now connects to every data member and reads each one's share, and
every database, bucket, entity and store element is in the file once. It stops with no file, naming the member and
the fix, when a member it needs is down or cannot be reached, when a member's certificate matches no `--fingerprint`
(it never falls back to an unverified connection), or when the cluster's membership changes while it runs. Two dumps of
a quiet cluster hold the same records in the same order whichever member they went through. **Take a new dump of every
cluster with this CLI; dumps the 1.1 CLI took of a cluster of more than one node are incomplete.** Single-node dumps
were not affected. The file format is unchanged: the 1.1 `restore` reads the new files, and the new CLI dumps 1.1
servers.
- **`hoarddb restore` into a cluster checked only one member for existing data.** Its refusal of a database that
already holds buckets now reads every member, so it no longer misses buckets another member lists.
- **`hoarddb restore` refused a database it could not hold only after creating the ones before it.** A dump database
with more copies than the target cluster can hold is now refused before anything is created.

### Added

- **`--fingerprint` once per member, and `--member <node id>=<host:port>`, on `dump` and `restore`.** Every member has
its own certificate, so `--fingerprint` may be repeated. `--member` gives the client address of a member that publishes
none (a client listener on a loopback or wildcard address with no `HOARDDB_TCP_ADVERTISE`).
- **`hoarddb dump` refuses when a node holds data in its earlier single-node store.** Programs using the earlier
driver interface (the public Node and Python drivers, the Go driver's root package) write to a store the ring does not
read, and dump and restore do not carry it. The 1.1 dump left it out without a word. The dump now asks every node,
refuses with exit 2 naming the nodes that hold such data, and goes on only with `--leave-legacy-store-data`, in which
case it says at its start and its end that the data is not in the file and carries the lines in `--json` as
`earlier_store_left_out`. No command copies that data into the ring yet.
- **`hoarddb seed` loads the example data the how-to pages use.** `hoarddb seed agent --insecure` creates a database named
`scenarios_agent` on a running ring server and fills it with the generated sample data of one how-to page, the same on every run,
so the transcripts on those pages match what you see. The layouts are `social`, `chat`, `ledger`, `agent`, `video` and `all`;
each goes into its own database, `scenarios_<layout>`, or `<name>_<layout>` with `--database <name>` (`--database demo` gives
`demo_agent`). It takes the connection flags of the other commands, and `--insecure` for a server you started yourself. It refuses a
database that already holds the layout's buckets before it writes anything, and prints the `drop bucket` lines that empty it or
the `--database` name to use instead. It creates a fixed set of buckets per layout (social 4, chat 3, ledger 1, agent 3, video 3;
14 for `all`), however much data it loads; no per-bucket metric.

## [1.1]

### Added

- **A durability drill and an escrow rehearsal.** `scripts/durability-drill.sh` starts a real server, writes under
load, kills it with `kill -9` mid-write, restarts it on the same data directory and reads back every acknowledged write,
on a plain and an encrypted-at-rest node: it fails and names the id when one is missing. It shows that nothing
acknowledged is lost when the server process dies, not across a power cut. `scripts/ring-encryption-acceptance.sh
--leg escrow` rehearses a lost key: a sealed node whose wrapped node master and key file are deleted refuses to start,
and is recovered from the file `hoarddb-server -export-key` wrote.
- **The refusal to run an offline command beside a running server says what to do.** A key export or import, like a
password reset, refuses while the server is running and now says to stop it first, without naming a password reset.

- **Encryption at rest now covers ring mode on each node.** With `encryption.mode` `on` or `require` and a keystore, a
node seals its ring store (documents and store fields) and its log under its own key, kept in `ring/keys/` and wrapped
by the node key. A cluster node no longer refuses to start with encryption on: the nodes of a cluster that each run
with it write, restart and serve their data sealed, and adding a node to such a cluster works: the joiner makes its
own keys, seals every record it copies from the others, and restarts sealed. A single node no longer falls back to the earlier
engine's statements. **Every node of a cluster must use the same setting; the cluster does not check
it**, and a node with encryption off keeps plaintext copies of the records it holds. Each node's `/metrics` reports its
mode (`hoarddb_node_encryption_info`) and whether its store's files are sealed (`hoarddb_node_encryption_sealed`), and
the driver's health answer's `encryption` list of what is not sealed (`not_sealed`) now describes the ring; the HTTP
`/health` probe does not carry the mode. Cluster metadata in `ring/raft`, server logs (which may contain document ids and keys) and dumps are not
encrypted; use disk encryption for them. A node's encryption is fixed when its ring is created: a start whose mode does
not match the ring is refused. A single node's refusal says how to move `ring/` aside to switch; a cluster member's says
not to move `ring/` aside while the cluster runs, because that can lose committed cluster state, and gives the same
recovery as a damaged ring store: stop the node, keep `ring/`, start it again on an empty ring directory with the same
node id, keys and address, and run `replace-node <node> --with <node>@<addr>` on any member. Creating the ring keys needs hard links: put `data_dir` on a
filesystem that supports them (some FUSE, SMB/CIFS and network mounts do not; this release has no fallback). An
encrypted ring store cut by a crash during its very first flush is refused at the next start, where a plaintext one is
repaired. The refusal gives the same advice as any damaged ring store: a cluster member is told to run `replace-node`,
and a single node, which then holds only its very first writes, to move its store aside and rebuild it from its log. Cost per
bucket: none.
- **`make ring-encryption-acceptance` checks ring encryption at rest on real nodes.** It builds the server and the
CLI, writes marker strings through the CLI into documents and stores on a sealed single node and on three sealed
nodes in a cluster, and greps the raw files: copied while running, after a `kill -9` in the middle of writes and
after a stop. A plaintext node is the control that must show the markers. It also checks the start refusals (another
key, a missing key file, mode off over a sealed ring, a damaged ring key file), the mode switch, `/metrics` and the
sealed password key file. `make ring-encryption-acceptance-selftest` proves its checks find a planted marker. Tooling
only. The security page now gives the setup, the backups (`ring/keys/` included), what a sealed node needs to start
again, the cluster rule and a check of every node; the configuration, limitations and defaults pages describe the
ring. Cost per bucket: none.
- **Users can be managed from a program over the ring connection: `ringclient` gains `CreateUser`, `AlterUser`, `DropUser`, `GrantRole`, `RevokeRole`, `ShowUsers` and `DescribeUser`.** They are seven new `RING_ADMIN` commands (`create_user`, `alter_user`, `drop_user`, `grant_role`, `revoke_role`, `show_users`, `describe_user`), each needing `user-manager@*` or `root@*` (a grant on one database is denied with `grant role user-manager@* to <user>` named as the fix). The password never leaves the client: it sends an Argon2id credential derived from it. A create carries `expect_drop_seq`, the `user_drop_seq` the last `show_users` or `describe_user` returned, and is refused `CONFLICT` when a user was dropped since, so a retry sent with the same request id and the same value is applied at most once and never brings back a dropped account; the other changes take an optional `expect_seq` (the revision a read returned) and are refused `CONFLICT` when the user changed since. Granting a role already held and revoking one not held succeed and change nothing; dropping a name that does not exist is `NOT_FOUND`. On a node without `cluster.enabled` they change the node's own users, with the session closes, audit lines and refusals the CLI statements have; a retried change under the same request id is answered success within ten minutes and applied once. On a cluster member they change the cluster's users (below). The CLI statements are unchanged on a node without `cluster.enabled`. The driver wire vectors of user administration are now these `RING_ADMIN` commands; the seven user opcodes are not vectored for drivers. See [docs/user/user-management.md](docs/user-management.md), "Managing users from a program". Cost: nothing per bucket or store (no key, catalog field, goroutine, descriptor or mapping) and no per-bucket metric; per node one `int64` drop counter; each call is O(1) in buckets (`show_users` is O(users), at most 10,000), and a membership change is not affected.
- **In a cluster (`cluster.enabled` on), users, roles and grants now live in the cluster's replicated state: a user created, changed or dropped through any member takes effect on every member.** The ring user commands (`ringclient` `CreateUser`, `AlterUser`, `DropUser`, `GrantRole`, `RevokeRole`) sent to any data member become cluster commands; every member applies them in the same order and ends its own sessions of an account that is dropped, disabled, given a new password, or dropped and created again. A change needs a majority of the voters and answers once the member asked shows it; a login does not need a majority and is checked against the member's own copy, so a member cut off from the others keeps accepting an account changed elsewhere until it rejoins. A change that finds no majority answers `UNAVAILABLE` within 20 seconds, with "was not applied ... nothing changed" when no leader took it and "may or may not have been applied" when the answer was lost; a retry under the same request id is answered as done while the cluster remembers that id (until 256 later user changes), whatever the retried body says; past that only the client's fence holds (a create's `DropSeq` always refuses, an alter, drop, grant or revoke is held only by the revision the client passed). A resumption token is bound to the account it was minted for: a token of a dropped account never resumes on an account created again under the same name. In a cluster a user's revision is that user's own (it moves only when the user changes), and `user_drop_seq` is the cluster's. An account holds at most 64 grants. A member cut off from the cluster serves logins from the users it last applied with no time bound (`auth_config_max_staleness_seconds` has no effect on a cluster member). A member checks passwords with the cluster's password key (`auth.cluster.pepper`) and, without it, refuses every password login before checking anything, saying why: the cluster has no users yet, the key has not arrived from another member yet, or the node is a witness. Cost: nothing per bucket or store and no per-bucket metric; per member one goroutine that installs the user table when a user command commits (an O(users) copy then, an identity check otherwise), and `hoarddb_auth_config_version` reports the cluster `state_seq` it installed; a membership change is not affected.
- **The first administrator of a new cluster is created once, by the first data node started with `HOARDDB_ROOT_PASSWORD` (or `HOARDDB_ROOT_PASSWORD_FILE`, or the password a first start asks for on a terminal).** Once the node has started, knows a leader and is a committed member, it writes the cluster's password key, creates `root` on every database through the cluster (only the password's hash is kept, in the cluster's state; the password is never stored or logged) and logs `created the cluster's first administrator`. The setting on a node of a cluster that already has users, on a node that joins, or on the first node restarted, is ignored with a warning in the log and on stderr: `HOARDDB_ROOT_PASSWORD was not used: this cluster already has users. Log in as an existing administrator, or unset it.` Two data nodes started with it create one administrator. A cluster started without it has no users (every login answers `this cluster has no users yet`): set it on one data node and start that node again. A node that joins a cluster, and a witness, never ask for a root password and no longer refuse to start without a terminal; only the first node of a new cluster asks, on its first start. Cost: per node at most one goroutine, which exits once the administrator exists or the cluster has users; nothing per bucket or store, no per-bucket metric, nothing on a membership change.
- **Removed: a witness no longer accepts logins or statements.** It holds the cluster's user table but not its password key, and answers every login with `this node is a witness: it holds no password key and serves no logins or statements. Connect to a data node instead; 'nodes' on any data node lists them.` A witness given `HOARDDB_ROOT_PASSWORD` prints `HOARDDB_ROOT_PASSWORD was not used: this node is a witness, which holds no password key and serves no logins. Set it on a data node instead.` at start. Before, a witness accepted logins for its own users and ran statements such as `create database`.
- **Changed: a resumption token of a cluster account names its cluster, and a refresh checks the account again on the record it mints from.** A cluster rebuilt with the same token key and the same first administrator refuses every token of the old cluster (`this token was minted in another cluster`), and a drop, disable or password change that lands while a token is being refreshed ends that refresh instead of minting a new token. Tokens of a node without a cluster are unchanged. Cost: one claim per cluster token; nothing per bucket or store.
- **Changed: `hoarddb-server -reset-password` refuses on a cluster node.** On a node whose configuration has `cluster.enabled`, it refuses before it asks for a password and writes nothing: `--reset-password does not work on a cluster node: users live in the cluster's replicated state, which a stopped node cannot change. Start the cluster, log in on any node as another administrator (an account with user-manager or root), and run 'alter user <name> password'. ...` Keep at least two administrators in a cluster. A node without a cluster resets as before.
- **Changed: a cluster member keeps no `users.json` and refuses to start when it finds one.** Move it aside as the refusal says (`mv <path> <path>.not-clustered`) and start again, with `HOARDDB_ROOT_PASSWORD` set on one data node if the cluster has no administrator yet; users of a node that ran without a cluster are not carried into a cluster. The CLI's user and role statements send the same commands, so they work on a cluster member too. User names, roles and password hashes of a cluster are kept in its metadata (`ring/raft`), which is not encrypted at any `encryption.mode`; the hashes cannot be checked without the cluster's password key, but the names and roles are readable.

- **`replace-node <node> --with <node>@<addr>` brings back a ring node that lost its data, while the cluster holds data.** Move the node's `<data dir>/ring` aside (keep it, and keep `keys/`), start the node again with the same node id, seeds and address, wait until `nodes` shows it down, and run the statement on any member: the node joins under its own id, pulls every pair it owns from the other copies, becomes active when it holds them, and votes again if it voted before (`status seeding` on that node shows the pulls). The address must be the node's own (`cannot replace <node> at <typed>: this release replaces a node at its own address, <addr>`). It is refused, and nothing changes, while the node is still active (`node <node> is active at epoch <e>, and replace-node gives the identity of a node that is down or dead to a new process`), when the node at the address still holds its cluster state (`holds cluster state, so it did not lose its data`), and when the node held the only copy of some pairs (every database with replication factor 1): `replace-node <node> would leave <n> pairs of database "<db>" (replication factor <rf>) with no copy ... or, to accept the loss, run 'replace-node <node> --with <node>@<addr> --accept-loss'`. A copy counts only when an active (or joining) member has reported it: one on a down or dead member, or one not reported yet, does not, so start the other member first or retry after a few seconds. With `--accept-loss` those pairs come back empty and their documents are lost; the answer then ends `It accepted the loss of <n> pairs in <k> database(s), which only <node> held.` and the leader logs it. A cluster of two voters cannot run it with one node lost (no majority): restore that node's data first. `REPLACE_TARGET_HAS_DATA` (exit 8) is now the code of the refusal for a node that still holds its cluster state. Repeating the statement while the node is joining finishes a replace that was cut short. `--accept-loss` replaces the `--clear` switch the grammar used to parse (it was never sent). On the wire, `RING_ADMIN`'s `replace_node` takes an optional boolean `accept_loss` (left off when false), and `MEMBERSHIP_RELAY`'s witness byte is now a flags byte (bit 0 witness, bit 1 accept_loss, the other bits refused), so every node of a cluster must run this build. The texts that said `replace-node` is not built (a diverged node's exit line, a snapshot another build wrote, the start-up line of a node waiting to be admitted) now name it, and `join` of a down or dead member adds `If it lost its data, start it again on an empty ring directory and run 'replace-node <node> --with <node>@<addr>'`. The `REPLACE_TARGET_HAS_DATA` text no longer names `--clear`. A cluster member's refusal for a damaged ring store now ends `Stop this node, keep <dir>, start it again on an empty ring directory with the same node id, keys and address, and run 'replace-node <node> --with <node>@<addr>' on any member`. `leave` and `remove-node` are still not available. Cost: nothing per bucket or store (no key, catalog field, goroutine, descriptor or mapping) and no per-bucket or per-member metric; the statement scans the committed seed bitmaps once (slots × databases × members) and the replace moves O(pairs the node owns) store syncs, never O(buckets). See [docs/user/cluster-add-a-node.md](docs/cluster-add-a-node.md), "If a member lost its data".

- **`hoarddb restore` now restores buckets that link to other buckets.** A field that is `of` another bucket is restored: the buckets taking part in a link are created right after their database, the target first and empty, and filled when the file reaches their records. Databases that have a link between their buckets, one way or both (a holds a bucket linked to one in b, and b holds one linked to one in a, which a ring allows) are created together, then every linked bucket of theirs in one order across them, and only then filled; such a group restores whole, databases that link into each other can only be restored together, and restoring one alone is refused with that advice; a group database is read again when the file reaches it and refused if a writer added a bucket. Before anything is created, restore refuses a link to a bucket that is not in the dump and not on the target ring (a target on the ring is checked by name only), a link to a bucket that was dropped before the dump (`#<id>`), and buckets that link to each other in a circle (a ring cannot make one). A bucket created empty for a link is checked to be still empty right before it is filled and the restore refuses if a writer put something in it. `--as` rewrites a link written `<Database>.<Bucket>` that names the renamed database, and a restore that stops names the buckets it created ahead; for databases that link into each other it prints no command that restores one alone, because that would leave the others linked to the buckets left behind. The ring takes a store element of at most 1 MiB (a fifo element of 1,048,563 bytes is accepted and one byte more is refused), so a dump the ring wrote holds none bigger; a hand-made dump that holds a bigger one stops the restore with exit `2` at that element, and nothing after it is sent. Restore now says "1 bucket" in the singular in its refusals. Cost per bucket: none on the server beyond the bucket's own create; while it runs restore holds one record in client memory for each bucket that declares or is the target of a link, growing with the number of fields (client memory, not a per-bucket server cost); a restore killed part-way through a set of linked buckets leaves the linked buckets it created empty; no per-bucket metric; restore is O(buckets) as before: one DescribeBucket read and one create per bucket, plus one read of the first entry of each bucket created for a link; a membership change is not affected. Restore's memory stays at about three frames (a frame is capped at 16 MiB) whatever the file's size. The time per bucket on a virtual machine is not available yet.

- **`hoarddb restore <file>` now works in ring mode: it puts a dump back into an empty ring.** It checks the whole file before it contacts the server, refuses a database that already holds buckets (naming the `--db <name> --as <new name>` command that restores under another name, and the `drop bucket` commands that empty it), refuses a database whose buckets are still being dropped, and one with another replication factor, all before anything is created. It then recreates each database and bucket and puts every entity back with all its stores, confirming each document's insert before it pushes into it, so a fifo or lifo pops in its original order and a restore never pushes into a document it did not create. Blob payloads are not in a dump, so a blob field restores empty and the run says so; users and roles are not in a dump and are not restored; queue messages that were claimed or dead-lettered when the dump ran are not restored. A restore that stops (a refused write, a lost connection, an interrupt) says what it created and prints one retry command per unfinished database; running it again into the same names is refused, there is no merge, no overwrite flag and no `--resume`. Cost per bucket: the CLI adds none on the server beyond the bucket's own create ; the client keeps no per-bucket state and no per-bucket metric is added. A restore does O(buckets) Raft commits, one create per bucket, and one extra round trip per group of up to 255 documents. It takes no pin or lease, so it never blocks a membership change.

- **`hoarddb dump` now works in ring mode:** one file, taken online while the database keeps serving. A dump is not a point-in-time copy: each entity is read once, entities are read at different moments, and an entity written to during the dump can be saved half-updated; stop the writers first for a copy of one moment. Queue messages that are claimed or dead-lettered are not included in a dump. See [Backup and restore](docs/backup-restore.md). Cost per bucket: none on the server (a client verb over existing reads); no per-bucket metric; the dump visits every bucket once, so its run time is O(buckets), with memory bounded by one page of buckets, one document page and one store chunk; a membership change is not affected (read-only, no pin).

- **`hoarddb dump --parallel <n>` reads up to n entities of a bucket at once, and a dump now retries a refused page.** `--parallel` takes 1 to 64 (default 8); for a database nobody writes to, the file is the same whatever the number. Memory does not grow with the size of an entity or the database: each entity being read waits in a buffer of up to 1 MiB, with a temporary `<out>.spill.<n>` file beside the output for a larger one (at most n at a time, always deleted). The ring takes a store element of at most 1 MiB; with maximum-size elements one reader holds many times that, the total is not linear in `--parallel`, and a worst case at a high `--parallel` was not measured. A page the ring refuses as moved or down is asked for again, up to 5 tries one second apart, then the dump stops with exit 8 and says what to run. A bucket dropped during the run is a warning that names it, and the run goes on; the file passes `restore --verify`. Nothing is re-read: each entity is still read once and a dump is still not a point-in-time copy. Cost per bucket: none on the server and no per-bucket state in the dump; per bucket one DescribeBucket and its list pages, and no server-side scan beyond the reads themselves; the run is O(buckets); a membership change costs a bounded page retry, not O(buckets) work.

- **`ringclient.Bucket.PushHeap` pushes to a `heapStore` field over the ring, alone or queued on a `Batch`.** `users.PushHeap(ctx, "ada", "Books", "dune", 9, element)` adds `element` under entry id `dune` with priority 9; `StoreList` then returns the keys and priorities, highest priority first. An element is identified by its priority and entry id together: the same pair overwrites, the same entry id at a different priority adds a second element (a `get` by entry id answers with the higher priority first), and the argument is at most 1 MiB. `cluster/wire` exports `HeapPushArg`, the same bytes the server already decodes, so no wire format changed. The CLI still has no heap `push` statement: it takes one argument and a heap needs a priority and an entry id.

- **FreeBSD 15.x on amd64 builds from source.** The port's builder built it and ran the getting-started path there on 2026-11-18; we do not run FreeBSD in CI. Seven type and constant differences kept the module from compiling there — a signed `Statfs_t.Bavail` and `Rlimit.Cur`, and no `syscall.O_DSYNC` — and the CLI needed the BSD termios request numbers for its background query. The two disk-usage figures now clamp an exhausted or negative `Bavail` instead of wrapping it, which would have reported about 16 EiB free and disabled the fill guard; the sync probe opens with `O_SYNC` off Linux so it still measures a durable write; and `scripts/build-matrix.sh` vets `freebsd/amd64` beside `darwin/arm64`, so the next break is caught by CI. No released binary (no `PLATFORMS` row) and no wire or on-disk change. The interactive session there has no line editing or pager and refuses `-password` without a value, `status` reports its descriptor, mapping and memory ceilings as unavailable, and a store killed during its first flush is not repaired automatically. See [Supported platforms](docs/installation.md#supported-platforms). Cost per bucket: none; no per-bucket metric; O(1) in buckets and on a membership change.

- **`faultctl digest` and `faultctl compare` check offline whether two nodes hold the same data for the pairs they share.** `digest` opens one stopped node's `<data_dir>/ring/badger` read-only and writes a file with, for every (slot, database) pair, the pair record's state and applied position, the live key count and a SHA-256 over the pair's replicated keys and values in key order; `compare` takes two or three of those files and prints equal, different (with counts) or not comparable (positions differ, a copy is still seeding, or deletes are still being swept) per shared pair, and with `--dir NAME=DIR --keys N` names the first differing keys. It exits 0 only when every shared pair is equal at the same applied position. It never writes to a data directory and refuses a running, uncleanly stopped or encrypted store with the command that fixes it. Dedupe entries and the per-bucket state document are left out as node-local; blob payload files are not read. Tooling only: no server, wire or on-disk change. Cost per bucket: none.
- **A data node of a cluster keeps a password key file, `auth.cluster.pepper`, and `status`, `describe_node` and the health report show each node's `password_key`.** Until a cluster has users no node holds the key and every data node reports `none_yet`; the key is created by the data node that creates the cluster's first administrator. It is 32 random bytes kept in the data directory beside `auth.token.key`, sealed under the node master like the other credential files when encryption at rest is on (sealing purpose `0x0C`), a `0600` plaintext file otherwise, and it never enters the cluster's replicated metadata, a log line or an error. A data node that does not hold it asks a member over the pinned connection between nodes (two new control messages, `PEPPER_REQUEST` `0x29` and `PEPPER_REPLY` `0x2A`); a member answers only a node its committed state lists as a data member that is joining, active or down, and refuses a witness. A file that names another key than the cluster's is kept as `auth.cluster.pepper.wrong-<kid>` (not sealed if encryption is turned on later) and the node fetches the cluster's. `password_key` is `held`, `fetching`, `none_yet` or `witness`; after `join`, wait for `held` before relying on logins on the new node. Once a cluster has users, losing the key on every data node loses them; a cluster with one data node holds a single copy. Back up each data node's data directory (and, with encryption on, its node master escrow) and run at least two data nodes; with encryption on, only a node's own backup restores its sealed copy.

- **`RING_GET_MANY` (`0xE1`) reads up to 255 documents by id in one frame, and `ringclient` gains `Client.GetMany` and `Bucket.GetMany`.** An application that holds a list of ids (the posts a feed names, the members of a room) no longer pays one round trip per document: one frame carries the keys, which may name different buckets and databases, and the reply has one answer per key in request order, a missing document as `found: false` and a failed key as that key's own error (the call itself succeeds). `GetMany` splits more than 255 keys into concurrent frames and rejoins them in order, and a key refused for a stale declaration re-describes its bucket once and is sent again. It reads documents only (`get` and `get_fields`; a write class, another read class and `expand` are refused, naming the key), each key is read at its own owner's current state and not one snapshot, and `local: true` works per key. The reply is bounded by the smaller of `max_result_bytes` and 16 MiB, charged as answers arrive, so a frame of large documents holds the budget plus 16 reads in flight; a key that does not fit answers `RESULT_TOO_LARGE` and can be asked for again in a smaller frame. There is no fallback to lone gets: a server that does not know the opcode answers with its own refusal. New client opcode `0xE1` (read-only), driver vectors for it, and a new section 8 of the client wire note. Pre-v1.1 and not versioned: direct. Cost per bucket and per store: none; no per-bucket metric; O(keys in the frame); a membership change is O(1).

- **`inc` adds to the numbers of a hashStore or btreeStore element by key, and creates the element when the key is absent.** `db.App["a1"].Hits.inc("2026-11-05T14", {t: 1, s: 30})` is Countly's hour counter, lila's day document or Mattermost's per-channel mention count as one write at the owner: it reads the element, adds each dotted path (up to three levels) with `update`'s `$inc` arithmetic (an int stays an int until it leaves its range and then widens to a long; a double makes a double; a long that would overflow is refused), writes it back in the element's own batch and replicates the record, so a copy repeats the same add from the same prior element and two callers never lose an add. `{return: "after"}` prints the element as applied (`Bucket.IncAfter` in the Go client; a retry with the same request id answers the same element). A store that declares sums or counts keeps them exact across the add, and an `inc` that would leave an element with no number at a summed path is refused before anything is written, as a put of it is. A path that holds a string, a decimal amount and an overflow are refused before a position is taken. A `fifoStore`, `lifoStore` or `heapStore` refuses it and names `update`'s `$inc`; `if:` on an element is refused ("not in this release"). New store verb 5 (`inc`; the opcode validator's range grows by one), `RingBucket.Inc`/`IncAfter` in the CLI client interface, `Bucket.Inc` and `Bucket.IncAfter` in `ringclient`, and two driver vectors (`inc creates an element`, `inc with return after`); the drivers add `inc` against those vectors. In a batch, an `inc` with `return: "after"` is refused ("not applied", nothing changed) when the frame's reply budget has no room left to carry its element, instead of being applied with the element dropped from the answer; send it alone or in a smaller frame. Pre-v1.1 and not versioned: direct. Cost per bucket and per store: none (no new key kind; one point read and one element write per call, plus an aggregate's keys when declared); no per-bucket metric; O(1) in buckets and on a membership change.

- **`hoarddb plan` suggests a design from your data and how your application reads it.** It reads BSON or JSON Lines files, or a small model file, plus an optional access trace, and writes a Markdown plan: a five-rule scorecard with a verdict, the hoardDB read that replaces each traced query, one `create bucket` statement per bucket with its evidence, what hoardDB will not serve, and what each design costs. Open choices are asked in a terminal, or left open with `--no-input`; `--answers` and `--save-answers` replay them, and `--json` prints the plan as one JSON object. It is deterministic, connects to nothing, creates nothing, and never replaces a file it did not write unless `--force`. See [Let hoardDB suggest a design](docs/plan.md). Cost per bucket: none.

- **`update` and `delete` take `if` and `return`: change a document only if it still says what you expect, and get it back.** `db.Shipment.update({_id_: "s1", if: {Status: "processing"}, $set: {Status: "shipped"}, return: "after"})` applies only while the named scalar fields hold those values (`null` matches a field that was never set) and answers the document as the update left it; when a field differs nothing is written, no position is taken, and the answer is `not_applied` with the field and the current document, so the next decision needs no second read. `return: "before"` gives the document as it was, which makes `$inc` with `return: "before"` a sequence allocator; `delete` takes `if` and `return: "before"`; an upsert takes no `if`. A document over the 1,024-byte dedupe cap is returned whole on the first answer and a retry says `document_truncated` with the `get` to run. The ring reply gains the outcome `not_applied` and the members `reason`, `guard_field`, `document` and `document_truncated`, the forward reply's `deduped` byte becomes a flags byte, and `hoarddb_update_guard_total{outcome}` counts the results. `insert` and `upsert` refuse `if` and `return` by name (PARSE_ERROR in the CLI, INVALID_ARGUMENT in the Go client). A retry that crosses the upgrade, with a request id first sent to a build before this change, answers `REQUEST_ID_REUSED` (the dedupe fingerprint now covers `return`): the write is not repeated; read the document with `get` to see whether it applied. Pre-v1.1 and not versioned: added directly. Cost per bucket: none; no per-bucket metric; O(1) in buckets and on a membership change; a plain update or delete record is unchanged from the trailer byte already recorded below.

- **`HOARDDB_PPROF_BLOCK_RATE_NS` (default 1000000) and `HOARDDB_PPROF_MUTEX_FRACTION` (default 100) fill the `block` and `mutex` profiles when pprof is enabled.** Before, those two profiles were always empty because the server never asked the runtime to sample. They apply only with `HOARDDB_PPROF_ENABLED`; `0` turns each off; a non-number stops the start naming the variable. Cost per bucket: none.

- **`hoarddb_wal_bytes_appended_total` counts the bytes the write-ahead log appends.** It is a node-wide counter of the framed length of every entry the log wrote since the node started, so a storage-format change can be measured against what the log actually writes rather than an estimate. It is exported with the other `hoarddb_wal_*` series, on a node that keeps a log, and starts again at a restart. Nothing on disk or on the wire changes. Pre-v1.1 and not versioned: direct, with no alias.

- **`join` and `promote` add a second node to a running cluster, from any member.** Start the new node with the first node in its seed list, pin its key on every member (a restart each, until a reload exists), then run `join <node>@<host:port>`: the statement is forwarded to the Raft leader, which asks the new node who it is over a fifth handshake channel (`identity`, served on the pinned key alone), adds it, registers it, gives it a vote while the cluster has a free voter slot (`cluster.voters`, witnesses included) and activates it; a witness (`cluster.witness`) is active and votes at once. `promote <node>` gives a vote later. The statements answer one line, byte for byte what the server committed, and a repeat of a `join` answers the same position. `join` is refused while any database exists, because a joiner cannot yet be seeded: form the cluster, then create data. New key `cluster.allow_membership` (`HOARDDB_CLUSTER_ALLOW_MEMBERSHIP`, default true in cluster mode) locks the membership statements on a node and on the leader on its behalf; `true` with `cluster.enabled: false` is refused at start. A Raft dial that names no peer and whose address no member and no configured server holds is now refused instead of being held to "any pinned key", a leader that dies between adding a joiner and registering it no longer leaves a phantom nonvoter (the next leader removes it), and a promotion counts the Raft configuration's voters plus the witnesses against `cluster.voters`. Breaking, pre-v1.1 and not versioned: the cluster state's member row gains `joined_state_seq` (26 bytes; the minimal state is 516 bytes and its snapshot file 548), so **a node must start on a fresh `ring/` data directory**; channel 5 and kinds `0x1B` to `0x1E` are new. A joiner states its own client address in the identity reply (`client_addr`, optional) and the leader records it; a member that publishes none has no driver address. When a promotion would pass `cluster.voters`, the refusal names `leave` and `remove-node`, which arrive in the next release. Cost per join: one identity connection of at most 802 bytes each way and two to four Raft operations; nothing per bucket or store.

- **A cluster node can publish a client address, and `TOPOLOGY` says who votes and who is a witness.** `tcp.advertise` (`HOARDDB_TCP_ADVERTISE`, a `host:port`) sets the address a member of the entry-ring cluster tells drivers to connect to; it takes effect once the node is in a ring cluster (`cluster.enabled`), and a node that is not logs a warning and ignores it; unset, a member publishes `tcp.listen` when another machine could dial it and nothing when it is a wildcard or loopback address, and a member that publishes nothing has no driver address (it is not given its node address, which serves no driver). The cluster checks the address when it commits it: `host:port` with a port from 1 to 65535, no scheme, user name, space or zone, and **on the same host as the node's own address** (a different port is fine), because a driver sends its password to whatever a member row names. A one-voter node publishes none. `ringclient.Topology` exposes `Member.Addr`, the client address when there is one and empty otherwise (with `DataAddr`, `ClientAddr` and `Voter`); it does not dial members itself, and the `owner_addr` of `STALE_EPOCH` is the same address, empty when the owner published none. The `RING_TOPOLOGY` members blob changed: each row is now `state, flags, id, data_addr, client_addr` (flags bit 0: the member votes), a new `witnesses` blob, absent when there are none, lists the witnesses (voting or not) apart from the ranked members and the table, and `voters` is the committed voter count. `ringclient.Topology` gains `Witnesses` and `Member.Voter`. A leader now reconciles the state's voter list with the Raft configuration every heartbeat, so a leader that died between a promotion's two steps no longer leaves the displayed quorum wrong. The old blob layout is not read any more and the driver test vectors are regenerated: both drivers must read the new layout. **One way up:** a member of an older build cannot read a cluster command or snapshot that carries a client address and stops advancing, so upgrade every member before any node that publishes one joins; see docs/user/compatibility.md. A cluster state that publishes no client address is byte-identical to before. Cost: one string per member, nothing per bucket.
- **`RING_BATCH` (0xE0) sends up to 255 writes in one request.** One frame carries one client prologue, a count byte and that many `RING_KEYED` write bodies; the reply is a `RING_REPLY` with `outcome` (`ok` or `partial`) and one answer per operation, in request order, each the reply or the error the operation would have had alone. It is an unordered bulk write: there is no atomicity across operations, a refused operation does not stop the others, and operations on one document apply in the order sent. Each operation is authorized on its own, so a batch never widens a session's grant, and each is forwarded to its own owner. To retry after a lost answer, resend the whole frame under the same request id: the operations that were applied answer `deduped`. The Go client gets `Client.NewBatch`, `Bucket.In(batch)` and `Batch.Send`; a 256th operation is refused before anything is sent. A publish that fanned out to 20 followers is 2 requests, not 22; no throughput figure is claimed before it is measured. Nothing on disk changes and no per-bucket cost is added. The client wire description is updated and the driver test vectors are regenerated.

- **The ring wire carries the seeding commands; the CLI statements wait for the seeding engine.** `seeding_status`, `seeding_retry` and `seeding_forget` are RING_ADMIN commands (also `ringclient.SeedingStatus`, `SeedingRetry`, `SeedingForget`) that need the cluster-manager role granted on every database (`cluster-manager@*`; a grant scoped to one database is refused), and answer from the node's `SeedingView`. No seeding engine sets that view yet, so `status seeding`, `seeding retry` and `seeding forget` parse but ring mode refuses them as not available yet, and `help` lists them under what is not available; they switch on when the engine lands. A slot has six states: placing, seeding, stalled, ready, retired and retiring (the range delete has started and cannot be stopped). `seeding forget` removes only a retired record and keeps its range, so the data goes on the next seed; on any other state it answers the new `SEED_HELD` (0x2A, exit 8). `SEED_NOT_HELD` now has a separate text for forget (no record) and for retry (the slot's own state).
- **Cluster mode has its own configuration keys, checked at start.** `cluster.enabled`, `cluster.listen`, `cluster.advertise` and `cluster.witness` (environment `HOARDDB_CLUSTER_ENABLED`, `HOARDDB_CLUSTER_LISTEN`, `HOARDDB_CLUSTER_ADVERTISE` and `HOARDDB_CLUSTER_WITNESS`) are read and validated, and the start is refused, naming the key to change, when cluster mode has no `server.node_id` of its own, when it is combined with encryption at rest, when a wildcard listen address has no `cluster.advertise`, or when a listen or advertise address is not `host:port`. A node with none of these set starts exactly as before. See [Cluster mode](docs/configuration.md#cluster-mode).

- **`cluster.enabled` starts the entry-ring peer listener.** The node binds `cluster.listen` (default `server.listen`, port 4433), accepts only keys pinned in `hash_ring.seed_fingerprints` as `<node-id>=SHA256:<hex>` (its own key is added for it), advertises `cluster.advertise` as its address, and logs `entry-ring node open` with its id, address, `bootstrap` or `join` and fingerprint. A node starts a new cluster when `hash_ring.seed_nodes` is empty or names only itself and joins when it names any other node, so the first node lists nobody and later nodes list the nodes that exist (a node that waits to be joined logs a warning). A connection to a node is accepted only if it presents the key pinned under that node's id, and a seed with no pinned fingerprint stops the start and names the entry to add on both nodes. A peer listener on the driver or node-to-node port is refused, naming `cluster.listen`. With `cluster.enabled` the node-to-node hash ring is not built at all: `hash_ring.seed_nodes` drives the entry-ring cluster only, and `server.node_id` may not contain a colon. Cluster mode runs with encryption at rest on; see the ring encryption entry above. `replication.dedupe_window` now reaches the ring node. The node-to-node listener of the hash ring stays at `server.listen` plus one. A node with `cluster.enabled` unset binds nothing new. Adding a second node (`join`, `promote`) is not in this release. See [Cluster mode](docs/configuration.md#cluster-mode).

- **The cluster membership commands have a wire, and a node refuses them for now.** `RING_ADMIN` accepts `join`, `promote`, `leave`, `remove_node` and `replace_node`. Both doors need the unscoped grant: `cluster-manager@*` or `root@*`. **A cluster-manager or root grant on one database is denied** for `CLUSTER_ADD`, `CLUSTER_REMOVE` and the five new commands, because cluster roles are cluster-wide; grant `cluster-manager@*` instead. Every node still refuses all five new commands with `membership changes are not available on this node yet: set cluster.enabled: true and restart; ...`, because the node-side behaviour is not built yet. A session without the grant gets the one `PERMISSION_DENIED` text. The wire is described in the ring admin wire notes; the CLI statements are still unavailable.
- **Ring mode returns only the fields you name.** `db.User.list({fields: ["Name", "Bio"], limit: 10})` and `db.User["ada"].get({fields: ["Name"]})` return `_id_` and just those declared scalar fields (`_id_` is always there, named or not, and the fields come back in declaration order whatever order you wrote them). A filter still sees every field, so `db.User.list({filter: {Age: 36}, fields: ["Name"]})` selects on a field it does not return. A name the bucket does not declare, or a store field's name, is refused with the exact statement that fixes it; an exclude form and the elements of a store are not in this release. A projection reads fewer keys than the whole document: it reads no store state at all and does not fetch the value of a scalar it neither returns nor filters on. The `more:` line a page prints repeats `fields`. `ringclient` gains `Bucket.GetFields` and `PageRequest.Fields`. On the wire this is the `fields` member of `RING_LIST`, read class `0x89` and FORWARD_READ sub-kind 2; a request without `fields` is byte for byte what it was. The `get` line of `help` shows its optional document. The driver test vectors are regenerated.

- **Ring mode reads one element of a keyed store, lists keys, pops several, and peeks at most 100.** `db.User["ada"].Friends.get("grace")` reads one element of a hashStore or btreeStore (a heapStore by entry id) in one request and answers NOT_FOUND naming the key when it is absent; `get("<lease>")` on a fifoStore or lifoStore still reads a lease. `list` and `peek` of a hashStore or btreeStore print the key, two spaces, then the element, and a heapStore prints the entry id, the priority and the element. `pop(3)` removes up to 100 elements in one write (a plain `pop()` still takes one); the elements must fit about 1 KiB together or the pop is refused with the count that fits, and `claim` and `ack` have no such limit. A peek of more than 100 is refused naming the limit (the client used to allow 1000 and the server refused above 100), and `list` pages through more. `ringclient` gains `StoreGet`, `PopN` and `PeekKeyed`, and `ElementPage` gains `Keys` and `Priorities`. On the wire this is read class `0x88`, the `keys` and `priorities` members of `RING_REPLY` and the optional `{n}` arg of a pop; the driver test vectors are regenerated.

- **`alter bucket` adds and drops fields in ring mode.** `alter bucket User in app { add Email string, drop Age }` (also `hoarddb -ring`, the `alter_bucket` RING_ADMIN command and `ringclient.AlterBucket`) adds or drops fields of a running bucket. Adding rewrites no document, however many the bucket holds: existing documents read the new field as unset. Dropping makes the field unreadable and unwritable at once (a write that names it is `FIELD_TYPE`); its values are deleted in the background by the node's retired-field sweep (one budgeted pass over the pairs it holds, touching only that field's keys), and the name can be added again later under a new field id. A field's type never changes (add a new field, copy, drop the old one). The new table is written as a pending declaration and takes effect with one `BUMP_SCHEMA` commit, so a crash in between leaves the old table in force and the bucket writable, and running the same alter again finishes it. A client that built its write before an alter is told `SCHEMA_NOT_CURRENT` and the ring client learns the bucket again and re-sends. It needs the db-manager role on the database, as the legacy `alter bucket` does.
- **`drop bucket` runs in ring mode.** `drop bucket User in app` (also `hoarddb -ring`, the `drop_bucket` RING_ADMIN command and `ringclient.DropBucket`) deletes a bucket and every document and store in it. The statement returns as soon as the bucket is dropping: it stops answering at once (`BUCKET_DROPPING`), `show buckets` lists it as `dropping`, each node that holds its keys deletes them in the background, and the name is free to create again when it has left the list; the new bucket has a new id and none of the old documents. Closing the connection after the statement returns stops nothing. A drop cut short is finished by running it again; one that already finished is `NOT_FOUND`. There is no prompt and no `--yes`. It commits one schema bump, adds no goroutine, file or metric per bucket, and slows no other bucket. It needs the db-manager role on the database. `drop database` is still refused in ring mode.
- **`db.<bucket>.update` takes `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$push`, `$addToSet` and `$rename`, and applies them as one write.** `$mul` on a missing field gives zero of the factor's type, `$min` and `$max` order values by BSON type bracket, `$push` appends one value (`$each`, `$position`, `$slice` and `$sort` are refused by name), `$addToSet` treats `1` and `1.0` as equal, and two operators on one field are refused. `$rename` moves a value to another declared field of the same type and overwrites what the target held. Every refusal happens before the write, names the field and says "Nothing was changed". The statement used to read the document and upsert it, so a writer that came in between was overwritten; it now sends one owner update, so that no longer happens. The operators are listed in the ring client conventions, under "UPDATE operators". `ringclient.Bucket.Update` is the Go form.

- **The `hoarddb -ring` help is a Quick Start, one page per statement, and a one-screen reference.** `help` is 23 lines: the first five minutes in order, every line a statement that runs as printed, with no placeholders and no `e.g.`. `help <word>` prints one page of at most 30 lines for a statement family (`help create`, `help show`), a store verb (`help push`, `help claim`; `help list`, `help count` and `help get` show the bucket and the store level), `help db` (the document statements) or `help types` (the scalar words, the six stores and the verbs each runs). `help all` lists every statement with one line each and names the statements ring mode cannot run yet. At the prompt a refused statement prints its fix on a line of its own (`Fix:`), a parse error labels the form, a short example and the help page, and a mistyped first word gets "Did you mean". A terminal session greets with one hint (`No databases yet. Start with: create database app`, or the databases and `use <name>`). A piped run prints the same single lines as before, except that a parse error now quotes the shortest example of the statement, not the longest. The refusal for a missing store field no longer quotes `alter bucket`, which ring mode cannot run. `help` examples were checked against a running server: `create database app { replication_factor: 2 }` is refused on a one-node cluster and was replaced.

- **`RING_TOPOLOGY` (0xDE) now answers.** Any authenticated session gets the cluster view a smart driver routes with: the epoch, the `state_seq`, the ranked members, the full 8192-slot ranking and the databases it holds a role on (a session with no grants gets none). Send the `epoch` and `state_seq` you hold and a matching view answers `unchanged` with nothing else. `ringclient.Topology` decodes it and `Topology.Owner(database, slot)` picks the owner. The driver test vectors (`clients/testdata/driver-vectors/v1.json`) now cover every ring opcode and `CLUSTER_PROBE`, with strict-refusal cases.


- **The server now serves the entry-ring opcodes (0xDA to 0xDF) on its driver port, from a new `ring/` directory under `data_dir`.** An authenticated client reaches them through the same grants as the other opcodes. The ring node follows `storage.badger_table_sync` and `server.node_id` and adds no setting. With `encryption.mode` `on` or `require` the ring node seals its store and its log (see the ring encryption entry above). There is still no client statement that creates an entry-ring database or bucket.

- **Hash buckets are created `whole` by default for now; `placement: spread` is opt-in until the spread collection work completes.** A `create bucket` that names no `placement` is recorded `whole` (the answer reads `(type hash, whole)`), so a unique field is accepted on it; naming `placement: spread` is unchanged. The default returns to spread when the spread collection work completes.

- **A `placement: spread` bucket now spreads its documents over the cluster's nodes.** Each document goes to one of
256 parts by its `_id_`, and each part lives on the node the ring names, so inserts, gets, updates, deletes, store-field
operations and `insert many` reach the document through any node, as before. The collection is answered through one
node, the owner of its name, which is its router: while that node is down the bucket cannot be read or written at any
replication factor, until it returns or `remove-node` hands the bucket's name to a node that holds a copy of it. A `whole` bucket at factor 2 or more stays
readable from its copies. `find`, `count`, `distinct`, `search`, `describe` (and the one-bucket info) and `dump`
cover every part and answer as a whole bucket of the same documents does: `find` returns the same page in the same
order, with or without a sort, and `count` and `describe` give the collection's totals. `distinct` and `search` return
the same values and documents, in the order the parts give them. A `BATCH` whose items name a spread bucket is routed
by each item's key and applied in batch order, so it answers as the same batch on a whole bucket does. The dump of a
spread bucket is one dump of the bucket, which `restore` reads as any other. While a node that
holds some of the bucket's documents does not answer, or one of its parts is held by no node, these reads are refused
with a message naming the node, and nothing is returned; a dump of a second spread bucket while one runs is refused
partway, as a second dump on a node is. `drop bucket` drops the whole collection: the router marks it as being
dropped, drops every part, then drops the bucket's name. From the mark on, every other operation on the bucket is
refused with a message to run `drop bucket` again; if a part's node does not answer, the drop is refused partway, and
running it again once that node returns (or is removed) finishes it. `alter bucket` and declaring store fields are
refused on a spread bucket with a message that gives the dump, drop, create and restore sequence that does it, and so
is `migrate` sent to a node that holds the bucket's name (its parts move with the rebalance). A first insert into
a new part is refused, with nothing stored, while too few copies of the bucket's name are reachable to record where the
document went; retry it once the nodes are back.

- **`DOCUMENT_EXISTS` (`0x12`)**: the refusal of an insert whose `_id_` the bucket already holds, and of a bulk
insert that repeats an `_id_` (see **Changed**). It is a constraint refusal, the same on every copy: a driver never
retries it, and a connection survives it.

- **`placement: whole | spread` on hash buckets.** `create bucket User { placement: whole }` records
where a hash bucket lives; with no `placement` the bucket is recorded `whole` in this build, and
`placement: spread` opts in until the spread collection work completes, when spread becomes the default. The
create answer names it (`OK  created bucket app.User (type hash, whole)`), and `describe bucket` shows a `Placement`
row. `placement: spread` on a FIFO, LIFO, heap, B+Tree or blob bucket is refused, and a spread
bucket's name is at most 250 bytes (a whole bucket's is 255). `CREATE_BUCKET` answers and `BUCKET_STATS` answers gain
an additive `placement` field.

- **`storage.wal_preallocate` (`HOARDDB_WAL_PREALLOCATE`): `off` (the default), `on` or `auto`.** With `on`, a node
with a write-ahead log writes zeros through each 64 MiB log segment before it uses it and keeps the next one
ready, so a flush of the log writes data only; it can lower the latency of replicated writes. It holds 128 MiB
of disk ahead of the log and writes about as many bytes of zeros as of log entries. `auto` turns it on for
ext2/3/4 only. Each start logs the mode in effect and why, and `status` shows it. A change needs a restart. Any
other value stops the server from starting. See "Write-ahead log segments" in
[Configuration](docs/configuration.md). New metrics, node-wide with no labels and only on a node that
keeps a log: `hoarddb_wal_preallocated`, `hoarddb_wal_flush_seconds`, `hoarddb_wal_rotations_total`,
`hoarddb_wal_rotation_waits_total`, `hoarddb_wal_rotation_wait_seconds_total`,
`hoarddb_wal_zero_fill_bytes_total`, `hoarddb_wal_zero_fill_seconds_total`,
`hoarddb_wal_spare_fill_failures_total` and `hoarddb_wal_spare_ready`. `HEALTH_CHECK` gains an additive `wal`
field, and `status` a `WAL:` row. A node that cannot make its next log segment (no room on the disk) now refuses
writes with the error that says what to free or change, instead of failing the rotation, and a node
whose open cannot make its first segment now starts with writes refused instead of failing to start.

- **`upsert` (`UPSERT` `0x16`, answered `UPSERT_OK` `0x17`; `BATCH_UPSERT` `0x24`, answered `BATCH_UPSERT_OK` `0x25`):
replaces a hash or btree document's scalar fields and keeps its stores; refused on queues, heaps and blob
buckets.** HQL: `db.User.upsert({_id_: "jane", Name: "Jane"})`. `insert` still replaces an existing document
in this release; the opcodes exist first so every writer that relies on the replace can move to them.

- **`BATCH` (`0x22`, answered `BATCH_OK` `0x23`) and the HQL `batch { ... }` statement: many store-field
writes and reads, to any buckets, in one request.** A batch carries a store field's push, put, delete, get,
len and peek and an update of one key, at most 1,000 items and 16 MiB; a larger batch is refused whole with
`BATCH_TOO_LARGE` and the split it needs. **A batch is not a transaction**: each item is applied, refused or
left unknown on its own, and its answer is the lone request's own reply with its own `write_outcome`. Items
of one bucket apply in batch order, and after one that is not applied the later items of that bucket are not
run; items of different buckets have no order. Every check a lone request meets (authorization included)
runs on every item before any item runs. One write concern, on the batch, covers every write item. On a
single node a batch's writes share one sync; on a cluster the node the client reached sends each owner one
request with its items. Answers count against one reply budget (the smaller of `query.max_result_bytes` and
16 MiB): a read past it is answered `RESULT_TOO_LARGE`, an update past it is applied and answered without its
document (`doc_omitted: true`). Servers advertise the `batch` capability to drivers and the `batch` feature
to members. New metrics, aggregates only: `hoarddb_batch_requests_total{outcome}`,
`hoarddb_batch_items_total{opcode,outcome}`, `hoarddb_batch_items`, `hoarddb_batch_forwards_total`,
`hoarddb_batch_items_in_flight` and `hoarddb_batch_answers_trimmed_total{kind}`. Piped, a batch exits 2 when
refused whole and 3 when some statement was not a success. See "Batches" in
[HQL](docs/hql-spec.md) and [Limits](docs/limits.md). The Go and Node drivers' batch builders
follow in their own repositories.
- **`storage.badger_table_sync` (`HOARDDB_BADGER_TABLE_SYNC`) chooses how each new table file is made durable;
the default, `msync`, is the safe behaviour.** A new table file (a memtable written out, or a
compaction's output) is synced on its own before the store's manifest names it. On ext4 each of those syncs waits
for a journal commit while writes queue behind it. `stepwise`, an opt-in, writes the file in 4 MiB pieces, waits
once for all of it, and leaves the file's size and location to the manifest sync that follows; it can lower write
tail latency on ext4. **It has been tested only on ext4 with its journal (XFS is expected to be safe but is not yet tested)**: on btrfs, ZFS or ext4 without a journal
a power cut can leave the store unable to open, and on ext4 with `fast_commit` it is unknown. The server cannot
detect the filesystem, so it never picks `stepwise` itself. A change needs a restart and applies to new table files
only. Any other value stops the server from starting. Where the kernel or a container's security profile refuses
`sync_file_range`, `stepwise` syncs the `msync` way on its own. See "New table files and your filesystem" in
[Configuration](docs/configuration.md).
- **`hoarddb_ondemand_reads_total{outcome}` and `hoarddb_ondemand_wait_seconds`**: reads of a bucket a node
owns and has not taken over yet, while a membership change settles, by how they were answered (`landed`,
`forwarded`, `retry`, `not_found`), and how long they waited (at most one second each). Neither carries a bucket
or database label. Steady `retry` means a member the take-over waits on does not answer; `nodes` names it.
- **`hoarddb ingest <directory> --into-db <database>`** loads every `.bson` file of a directory into a
bucket named after the file, with `--max-buckets` (default 1,000) and `--allow-dots` as its guards, and
resumes at the first unfinished file from `<directory>/.hoardingest`. A `<name>.metadata.json` beside a
file has its indexes reported before any bucket is created (which hoardDB can declare, the statement that
declares them, the reason for the rest); none is created or translated. See [Ingest](docs/ingest.md).
- **Limits on the cost of a query filter.** A `$regex` pattern is limited to 4,096 bytes and to a
compiled size of 512 steps (2,048 for all the patterns of one filter), `$and`/`$or` to 64 filters nested
8 deep, `$in`/`$nin` to 1,000 values, and a filter to 1,000 clauses. A filter past these limits is refused
before any document is read. The regex work on one document, the pattern's steps times the bytes it
scans, is limited too, so a long field can be refused at the first document that reaches it.
Each refusal names the limit and what to do instead. A `$regex` is also compiled once per query instead of
once per document. See [Limits](docs/limits.md).
- **`hoarddb_request_faults_recovered_total`**, a counter of request handler faults
the server recovered from, with one series per port (`driver`, `internode`). Alert on
any increase: each one is a server bug, and the error-level log line with the same
fault id carries what to report.
- **Driver vectors cover user administration, token revocation and the `after` of `requeue`/`discard`.**
`clients/testdata/driver-vectors/v1.json` gained 24 rows: the seven `USER_*` opcodes with their refusals
(including the salt rules and the last-administrator guard), a token minted before a password
change or a grant (`TOKEN_INVALID`), and `DEAD_REQUEUE`/`DEAD_DISCARD` with `after`. A driver replays them
against a server built from the same commit.
- **Lossless queue processing: `claim`, `ack`, `release` and `extend`.** `claim` takes up
to 100 entries of a fifo, lifo or heap bucket under a lease of 1 second to 1 hour;
the entries stay in the store until `ack` removes them. An expired lease or a
`release` writes them back, so a lost answer is a redelivery rather than a loss
(at-least-once). `extend` moves a lease's deadline, and a consumer may extend as
often as it needs. A claimed entry is invisible to `pop`, `peek`, `find` and
`scan` until it is acked or returned. `claim` reports `pending`, `dead` and
`returned`; `peek`, an unfiltered `count` and `pop` carry `pending`, `dead` and
`expired`; and `len()` at 0, an empty `pop` and an empty `claim` name the dead
entries.
The CLI prints the full loop, and the user pages "Processing a queue without
losing work" and "Dead entries" document it.
- **Dead entries: `dead`, `requeue` and `discard`.** An entry claimed 5 times
without an acknowledgement, or a heap element whose `_id_` is pushed again while
claimed (`collided`), moves to the bucket's unbounded dead list. `dead` pages
the list, and `requeue`/`discard` put entries back (with the delivery count reset)
or drop them. The server logs a paced line naming up to 10 buckets a minute, on
the owner only, and a hard-cap refusal on the same line.
- **Queue metrics.** `hoarddb_queue_claims_total`,
`hoarddb_queue_claimed_entries_total`, `hoarddb_queue_lease_settlements_total`,
`hoarddb_queue_entries_returned_total` (entries returned because a lease ended
or was released), `hoarddb_queue_entries_dead_lettered_total`,
`hoarddb_queue_dead_requeued_total`, `hoarddb_queue_dead_discarded_total`,
`hoarddb_queue_empty_claims_unlogged_total`,
`hoarddb_queue_lease_clock_steps_total` and
`hoarddb_queue_claims_entry_too_large_total`. Node aggregates only, never
labelled by bucket or database.
- **The cluster feature `queue_claim`.** The seven lease and dead writes declare
it; a cluster refuses one before the append while a gossiped member does not
advertise it. `DEAD_LIST` is a read and declares nothing.
- **`pending` and `dead` on a bucket's row.** `BUCKET_INFO` and each `list
buckets` row carry `pending` and `dead` (`omitempty`), so an operator can find
the buckets that hold claimed or dead entries. No per-bucket metric is added.
- **A lifo's remembered cursor no longer walks the delete markers after push and
pop churn.**

- **A bucket's replication factor can be raised over the wire.** `ALTER_BUCKET`
  (`0x8E`), with `database`, `name` and `replication_factor`, is accepted from a
  client session that holds `db-manager` on the database, as a bucket create is;
  a `reader` or `writer` is refused `PERMISSION_DENIED`. Its answer names the
  members the change newly places, which hold nothing yet. Every member acts on
  it: a copy the change keeps is rewritten at the new factor, and a member it
  newly places records that it needs a copy and copies the bucket in by itself
  with the pending-copy worker (see below); run `migrate <database> <bucket>` on
  the member only when `auto_rebalance` is off. The CLI's `alter bucket` command and the driver verbs follow. Cluster members
  advertise `bucket_rf_alter` on gossip, and an alter is refused while a member
  that has gossiped does not.
- **`alter bucket <name> { replication_factor: N }` raises a bucket's factor
  from the CLI.** It sends `ALTER_BUCKET` and prints the owner's answer: the
  members that will copy the bucket, a bucket that now declares the factor it
  inherited, or that nothing changed. The body holds one option; `type` is
  refused with the 1.1 conversion named, any other key keeps the index message,
  and two keys are refused with a statement for each. Before this the CLI
  refused every factor change.
- **A bucket's replication factor can be lowered.** `alter bucket <name> {
  replication_factor: N }` below a bucket's current factor is accepted once the
  remaining copies have shown that they hold everything the removed copies hold;
  each removed copy is then deleted, and its space is reclaimed in the
  background after the delete. A decrease whose remaining copies cannot show
  coverage is refused, changing nothing, with the copy that is behind named
  (its copy is behind, or it did not answer), the instruction to run `show
  buckets` on the owner and retry when the row reads N/N, and a `remove-node`
  hint when that copy is not coming back; before this, every lower factor was
  refused. With fewer members than the factor, no copy is deleted.
- **`describe bucket` and `db.<bucket>.stats()` show the replication factor.**
  A `Replication factor` row reads `3 (declared)` for a factor given at
  `create bucket`, or `1 (inherited)` for the node's default. `BUCKET_STATS`
  answers two new optional fields, `replication_factor` and `inherited`, with
  the meaning they have in `BUCKET_INFO`; an older server sends neither and the
  row is left out.
- **A member an increase places copies the bucket in by itself.** After an
  `alter bucket` that raises a bucket's factor, every member the change newly
  placed pulls its copy from the bucket's owner with the pending-copy worker:
  one pull at a time, retried from 1 s up to 30 s. No `migrate` command is
  needed. A copy refused because the member's disk is above the high-water mark
  waits and says why once an hour, with the remedy; a member that the owner
  answers no longer places drops the record and copies nothing. With
  `auto_rebalance` off nothing copies the bucket: the owner logs one line naming
  the `migrate <database> <bucket> on <member>` command for each member, and
  each member lists its pending copies (the first ten named) once every five
  minutes.
- **`describe bucket` and `db.<bucket>.stats()` print the owner's copies, and why one is
  missing.** A `Copies` row reads `3 of 3 in sync`, `1 of 3 in sync, copying to node-b
  (4h), node-c (12m)` (the age of the alter that placed it, from the owner's clock), or
  `2 of 3 in sync, node-c not in sync`; a `Nodes` row names the set with the owner first
  (`node-a (owner), node-b, node-c`). A node that holds the bucket's needs-a-copy record
  adds `Pending copy here: since <time>, last attempt <time>: <reason>, next retry <time>`
  (RFC 3339, UTC). The answer is the owner's: a node that is not the owner forwards the
  describe to it and adds only that line from its own record, and an owner it cannot reach
  gives `unknown: the owner did not answer, so this node cannot count the copies`. A
  bucket the owner is taking over reads `unknown: the owner is taking this bucket over
  from <node> and cannot count the copies yet`. A server that predates the fields prints
  no copies row, as before.
- **`hoarddb_buckets_under_replicated`, one unlabelled series per node.** It counts the
  buckets this node owns with fewer copies in sync than their replication factor, a copy
  still pending included; a decrease is intentional and does not count, a node alone reads
  0, and `show buckets` names the copies that are missing. The count is recomputed after a
  stream leaves or returns to the in-sync set, a pending copy is written or cleared, or a
  membership change is adopted; those triggers are coalesced to at most one pass every 10
  seconds, and no write triggers a pass or waits on one. After a membership change, the
  first pass that finds buckets short logs one WARN with the count and up to ten bucket
  names.
- **The owner's view of a bucket's copies is on the wire.** `BUCKET_INFO` and each
  `LIST_BUCKETS` row carry `in_sync`, `state`, `not_in_sync` and `pending_copies` (the
  members still to receive their copy, with the time the alter that placed them applied);
  `BUCKET_STATS` carries those four and `nodes` (the set, owner first), `previous_owner`
  and `pending_copy` (the member's own line, composed on the node asked). `state` is
  `ok`, `copying`, `under-replicated`, `owner-elsewhere`, `standalone`,
  `owner-unreachable` or `taking-over`. A node that is not the owner sends
  `owner-elsewhere` and no counts, and the numeric and list fields are absent at zero, so a
  driver against a server that sends none reads the answer it always did. The pending
  copies are `pending_copies`, not `pending`: that name is the queue's claimed-entries
- **`ALTER_BUCKET` (`0x8E`) is in the driver vectors, as a client opcode.** Six exchanges
  join `clients/testdata/driver-vectors/v1.json`: a raise, a lower, declaring the factor a
  bucket inherited, a request that changes nothing, an in-band refusal (a factor above the
  member count) and an `UNAVAILABLE` with `write_outcome: not_applied` from a node not yet
  added. A driver's allowed fields are `database`, `name` and `replication_factor`;
  `epoch` is the server's. The opcode is no longer listed among those no driver sends, and
  a test refuses that list if the entry comes back.
- **`make bucket-rf-acceptance` exercises the factor on three real nodes.** The script
  builds the server and the CLI, starts three nodes on free ports in a scratch directory
  it creates and refuses to reuse, creates a hash bucket at one copy with one document,
  raises it to 3, waits for `show buckets` on the owner to read `3/3` (the two new members
  copy the bucket in by themselves), reads the document from every node's own copy, lowers
  the bucket to 2, and checks that the demoted node's row stops saying `owner elsewhere`
  and prints the owner's count. It signals only the processes it started, never a process
  name, and refuses a scratch directory it did not create (exit 2).
  (`scripts/bucket-rf-acceptance.sh`)
- **A login advertises `query_pipelining`.** `AUTH_OK`'s capabilities now name
  it. A server that advertises it promises a driver that queries in flight on
  one connection run independently and their answers come back matched by
  request ID, that each request gets one answer while the connection lives,
  that a result over the ceiling is `RESULT_TOO_LARGE` and leaves the connection
  up, that a query is abandoned when its client goes away, and that a
  connection with a request in flight is not closed for idleness. A driver may
  keep more than one request in flight on a connection only against a server
  that advertises it.
- **`scripts/ci.sh` skips the race gate on a tree that already passed it, and
  `--full-race` runs it anyway.** Gate 4 (the race gate) goes through
  `scripts/race-same-tree.sh`, which reports it SKIPPED (not a pass: the summary
  and the last line say so) when this exact clean committed tree, the arguments,
  the Go toolchain, the environment and the machine class already passed it in
  the last 7 days (`HOARDDB_RACE_MEMO_MAX_AGE_DAYS`). It records a memo only
  after a full green run with clean coverage on an unchanged tree, in
  `${XDG_CACHE_HOME:-~/.cache}/hoarddb/race-memo` (`HOARDDB_RACE_MEMO_DIR`
  moves it); a red run withdraws it. Any doubt runs the gate: uncommitted
  changes, ignored files under `github.com/hoarddb/hoarddb-server/`, a Go workspace or `vendor/`, a
  tracked symlink, a test seam variable, a subset run, an untrusted memo
  directory, an unusable or old memo. A docs-only commit is a new tree and runs
  the gate. `scripts/ci.sh --full-race --require-all` never skips and is the
  release run.
- **`scripts/test-shards.sh` says where it ran, and `--print-class` says where it
  would.** Every run prints `test-shards: executed-on: all=local` or
  `server=remote:<n> others=local serial=remote` (n counts the VMs still alive
  at the end, so a run that lost one reports fewer than it started with).
  `--print-class` prints the machine class from host resolution alone (no ssh,
  no run directory): `local` or `remote:<n>`, then a hash of the sorted host
  list. `scripts/race-same-tree.sh`, the identical-tree skip of the race gate
  (gate 4), compares the two before it records or skips.
- **`scripts/ci.sh` ends with one line about the race gate, and a skip can say
  why.** Every run that reached the gates now ends with `ci.sh: race gate: FULL
  RUN (passed)`, `FULL RUN (FAILED)`, `SKIPPED (...)` or `NOT REACHED` (an
  earlier gate stopped the run), so a skipped gate 4 cannot read as a pass.
  `--list`, `--help`, a refused argument, a failed preflight, `--bootstrap` and
  `--lint-bin` print no such line. A skippable gate (4, 12, 13) can write one
  line of reason to the file named by `HOARDDB_GATE_SKIP_REASON_FILE`, and the
  summary shows it in square brackets. Gate 4 now counts as skippable, and a
  race-gate command that exits 77 is reported as a failure (exit 1), never as
  SKIPPED.
- **`scripts/ci.sh --require-all` fails a run that skipped a gate.** Gates 12
  and 13 report SKIPPED when Docker is missing, and the run still exits 0. With
  `--require-all` the same run prints the summary and then exits 69 (sysexits
  `EX_UNAVAILABLE`), naming the skipped gates. A gate that fails still exits with
  its own status. The release run is `scripts/ci.sh --full-race --require-all`.
- **`scripts/ci.sh --help`, and unknown arguments are refused.** `--help` (or
  `-h`) prints the usage and runs nothing. An argument `ci.sh` does not know used
  to fall through to a full run; it now exits 2 with `ci.sh: unknown argument
  '--x' (try --help)` before the preflight or any gate starts. `--list`,
  `--bootstrap` and `--lint-bin` must be the first argument (after
  `--no-preflight`); elsewhere they exit 2 as well. The failure line of every
  gate now reads `ci.sh: stopping - gate N`, with the same hyphen for all gates.
- **`scripts/ci.sh` runs a preflight first.** The checks of gate 7 (repository
  hygiene) and gate 6 (`make docs-all`) run in seconds before gate 1, so a
  private reference or a broken doc fails at once, naming itself, instead of
  after the long gates have run. Gates 6 and 7 still run in their place. `--no-preflight`
  (or `HOARDDB_CI_NO_PREFLIGHT=1`) skips it.
- **`scripts/test-shards.sh` says when its timings file is stale.** Every run
  prints `timings: N of M tests not in FILE (P%)` and warns when more than 3% of
  the listed tests are missing. `--strict-timings` (or `HOARDDB_STRICT_TIMINGS=1`)
  fails the run instead, and `--max-missing-percent N` moves the line.
- **A search says when it was cut at its limit: `has_more`.** A `SEARCH` answer
  carried `total`, which is the number of documents returned, so a search cut at
  its `limit` looked the same as a complete one. The answer now has `has_more`
  (BSON and JSON name `has_more`, the same name and meaning as `find`'s): `true`
  when at least one further matching document exists beyond the limit. The server
  looks for one further real match after it holds `limit` results, so the extra
  work does not grow with the number of matches. When no further match exists,
  proving that costs a walk to the end of the bucket on a full scan: a search that
  has exactly `limit` matches, all early, in a large bucket now takes as long as
  one that finds fewer than `limit` (a full walk of the bucket where a search that stopped at its limit took almost no time before), which is the most a full-scan search already cost. `total` keeps its value, the
  number returned; a search has no `skip`, so a truncated search is answered by a
  larger `limit`. The field is always present, `false` on a complete answer.

- **On a terminal, a document names the stores its bucket declares, and `_id_`
  prints first.** A store field lives beside its document, so `db.User.get("ada")`
  printed a document that looked like a bucket with no stores. At a terminal, a
  document read out of a bucket (`find`, `get`, `search`, `range`, `first`,
  `last`, `consistent-get`, an `update`'s result) now carries each declared store
  as its declaration (`"Photos": "lifoStore"`), after its own fields, with
  `_id_` first. Piped, redirected and scripted output, `dump`, and every other
  case are byte-for-byte unchanged. The display costs one small call per bucket
  per session (`BUCKET_INFO`, which does not walk the stores), kept until a
  statement that can change a declaration runs; if the call fails the document
  prints as before, with no message. Documented in the HQL spec.

- **A session's output settings can be changed while it runs: `set`.** The three
  flags that shape output had no in-session equivalent, so switching to indented
  documents meant ending the session and starting another. Now `set pretty on`,
  `set extjson on` and `set pager off` change them where you are; `set <setting>`
  on its own says what the setting is; a name that is not a setting, or a value
  that is not on/off, is refused naming what does exist. It is the shape
  `write-concern <level>` established. The settings are the same three the flags
  set, and a piped session that turns paging on is told paging needs a terminal at
  both ends rather than left wondering why nothing changed.

- **The REPL recalls the session's own lines, and keeps no history file.** Up and
  Down walk the lines typed in the running session — that is the line editor's
  own list, and it goes when the session does. Nothing is read from or written to
  disk, so no record of your statements is left behind: a statement names buckets,
  users and keys, and yesterday's ones were worth more than the recall they
  bought. Ctrl-C abandons a line and leaves it out, as it always did, and a piped
  run has no history at all. This replaces the `~/.hoarddb/history` file added
  earlier in this same unreleased cycle, so nothing that shipped ever had it.

- **`--pretty` lays a document out to be read.** `--pretty` prints each document
  on several lines, two spaces per level, instead of one JSON object per line;
  documents in a list are separated by a blank line. The one-per-line form is
  unchanged and remains the default, because it is what a script or an agent
  reads — this is for the other reader. The values are not re-encoded to be
  indented: the compact form is reflowed, so a 64-bit integer, a decimal and a
  date's own text come out exactly as they went in.

- **The session banner shows the server's CPUs, memory and load.** The box gains a `cpus` row
  (with `of N on the server` when a container limit lowered the count, and the 1, 5 and 15 minute load
  averages) and a `server ram` row (with `limit cgroup` when a container memory limit is the total). The
  figures come from the health reply the banner already reads, so no request is added. A server that
  cannot report a figure, an older server, and a server that is not on Linux show fewer rows; nothing
  shows as `0 B` or `n/a`. Piped output is unchanged.

- **An interactive session opens with a banner describing the node.** A terminal
  session now greets you with a boxed summary instead of two bare lines: the
  node's id, address, version, uptime, the cluster's member count, the
  replication factor, the memory the node itself measures it using, the session's
  user and database, and the time. The frame and every label — the left column's
  words and the right column's `version`, `uptime` and `RF` —
  paint the theme's `accent`, the same purple as the prompt's own mark; every
  value keeps the terminal's own foreground, because a value is what a reader has
  to trust on a colour scheme this CLI has never seen. `--theme plain` and
  `NO_COLOR` print the box without colour. Bucket, memtable
  and encryption figures are deliberately absent:
  `status` carries them, and repeating them here made the greeting a dashboard. A
  node that is not healthy is the one exception — it gets a warning line naming a
  reason and pointing at `status` for the rest. The box prints once, at the start
  of the session; **`show banner` prints it again on demand**, for a session that
  has scrolled past it or a script that wants the node's state as one block. The
  box needs a terminal at **both** ends: a session typing at a terminal while its
  output goes to a file or a pipe gets the two plain lines instead, so no frame
  ever lands in a redirected output. A piped run is unchanged: the two plain
  lines it always printed, and nothing else, so a script's output does not grow a
  frame it has to skip.

- **A long report arrives a screen at a time.** `help`, `status`, a `describe` of
  a wide bucket, `show`, `nodes history` and `theme` are longer than a terminal,
  so the CLI now shows them the way `more(1)` does: the first screen, then a
  prompt saying where you are and which keys move you — space, `f` or Page Down
  for the next screen, enter, `j` or Down for one line, `b`, `k` or Page Up back,
  `g`/`G` or Home/End for the first and the last, and `q` or Escape to stop (an
  arrow key scrolls; a pager that read its first byte as the quit key left `[B`
  in the next prompt). The screen is the alternate one, so quitting leaves your
  scrollback exactly as it was and the report never mixes into the session's own
  lines. Paging happens only when both ends of the session are a terminal **and
  the terminal can take the escapes**: a piped run, a redirected output, a
  `TERM=dumb` terminal, `--json` output and the golden transcripts are
  byte-for-byte what they were, which is what keeps every script that reads the
  CLI honest. Output that fits is printed in place — taking the screen away to
  show less than a screen holds would be worse than not paging. `--no-pager`
  prints everything in full. A command whose output is work being done rather
  than a report about it (`ingest`, `dump`, `restore`) is never held, because
  showing a progress stream only at the end looks like a hang. A stop signal —
  SIGTERM, SIGHUP, or a SIGINT that did not come from the keyboard — while a
  report is up leaves the alternate screen and restores the terminal before the
  process ends, so the shell that gets it back is still usable, and the process
  then ends **by that signal**: the shell reports 128+n (143 for SIGTERM, 129 for
  SIGHUP, 130 for SIGINT) where it used to see a plain exit status 1. **The pager
  is not available on macOS today**: the CLI cannot read single keystrokes there,
  so every report prints in full with no error.

- **No escape sequences where the terminal cannot take them.** With `TERM=dumb`,
  with no `TERM` set, or with output that is not a terminal, the CLI now writes
  no escape sequence at all. Before, the line editor still erased to the end of
  the line on every keystroke on a dumb terminal and into a redirected file, and
  an unset `TERM` still got colour, the banner's colours and a background-colour
  query. Such a session reads its lines plain: your terminal's own line editing
  applies, and Up, Down and Tab do not. `NO_COLOR` on a capable terminal turns
  off colour only. The ingest progress line follows the same rule.

- **The health reply says what machine the node runs on.** A new `host` member
  of the `HEALTH_CHECK` answer carries `cpus` (the CPUs the process may use: a
  cgroup v2 or v1 CPU quota, rounded up, caps the machine's count, and
  `host_cpus` then names the count it was capped from), `memory_total_bytes`
  and `memory_used_bytes` (the container's memory limit when one below the
  machine's is set, else the machine's `MemTotal` and `MemAvailable`;
  `memory_source` says `cgroup` or `host`) and `load` (the 1, 5 and 15 minute
  load averages). It is per node and read at most once a second. Off Linux
  only `cpus` is sent. Like the rest of the health reply it is served to an
  authenticated connection only: `/health`, `/ready` and `/live` do not carry
  it.

- **`BUCKET_INFO` (opcode `0xD9`) answers one bucket's declaration without
  walking its stores.** It returns that bucket's row of `LIST_BUCKETS` (type,
  declared store field names, replication factor, entries) under a `bucket`
  member, in time that does not depend on how many elements the store fields
  hold: `BUCKET_STATS`, which counts them, took 1,002 and then 100,002 iterator
  steps for a field of 1,000 and then 100,000 elements, `BUCKET_INFO` none. It
  needs the read permission `BUCKET_STATS` needs, answers a missing bucket or
  database with the same `NOT_FOUND`, and is served on a cluster the same way.
  The reply also carries `store_fields`, a list of `{name, engine}` for each
  declared store field in declaration order, with the engine spelled as
  `BUCKET_STATS` spells it (`hash`, `btree`, `fifo`, `lifo`, `heap`, `blob`),
  so a caller learns what each store field is without counting its elements. It
  is left out for a bucket with no store fields; `bucket.stores` still names
  them, and the `LIST_BUCKETS` rows are unchanged.

- **A dump records each bucket's replication factor, and a restore keeps it.**
  A bucket created with `{ replication_factor: N }` dumps with
  `"replication_factor": N` in its `metadata.json`, and `restore` re-creates it
  with that factor, into a new bucket or under `--drop`. Before this, every
  restored bucket silently came back at the target's default. A factor the
  target cannot grant (standalone, or fewer members than the factor) restores at
  the node default; a merge into an existing bucket keeps that bucket's factor;
  `--drop` from a dump with no factor lowers a declared one. Each is named at the
  end of the restore summary and listed in `--json` as `factor_not_restored`,
  `factor_kept` or `factor_lowered`. The dump format stays `format_version` 2:
  the field is additive, and a dump without it restores inheriting, as before.
  On the wire, `DUMP_START`'s manifest and `RESTORE_START` carry
  `replication_factor`, and `RESTORE_START` answers with the three fields.

- **Four hand-off metrics** on every node's `/metrics`, unlabelled:
  `hoarddb_handoffs_in_progress` (buckets on the node with a hand-off record;
  after a `join` or `remove-node` it reads 0 on every node once every moved
  bucket is taken over), `hoarddb_handoff_copies_total`,
  `hoarddb_handoffs_without_previous_owner_total` and
  `hoarddb_handoff_entries_reconciled_total`. The `remove-node` answer and its
  WARN say how to read the gauge.

- **`nodes history`** prints the answering member's membership history, oldest
  first, one line per epoch with an 8-digit digest of its member set
  (`epoch 3: n1 n3 n5 n9  (digest 5c1e0a7b)`), to compare members line by line.
  On the wire, `LIST_NODES` takes an optional `history` field, and answers with
  `membership_history` and `history_reported` only when asked; an answer to a
  request without it is unchanged. The CLI refuses an answer without
  `history_reported`: `member <node> does not report its membership history: it
  runs a build without 'nodes history'. Upgrade it, then run 'nodes history'
  again`.
- **`hoarddb_membership_changes_total{outcome}`**, a counter of the membership
  changes a node proposed, by outcome: `committed`, `finished_another`,
  `already_committed`, `refused`, `no_majority`, `indeterminate`.

- **A gate that checks the documentation's CLI verbs against the parser.** Every
  `db.<bucket>.<verb>` form in the CLI's own help and in every page under
  `docs/user/` is substituted, parsed and dispatched, and the gate fails on any
  form the CLI refuses as a statement. The existing check
  (`website/scripts/claims-check.sh`) covers the plain bucket form only: its
  pattern matches `db.Users.find(` and none of the 69 store-field forms, because
  those carry a bracketed document key, and it reads its verb list from
  `cli/handlers.go` while the store-field verbs are dispatched from
  `cli/declare.go`. All seven defects found in one session were store-field
  forms. A form a page shows being refused is not a failure — `hql-spec.md`
  documents refusals on purpose.
- `join` now refuses a node that does not answer, and a node that still holds the record of its own removal: start the node first, and empty a removed node's data directory before adding it back.
- **Six store types** behind one set of verbs: `hash`, `btree`, `fifo`, `lifo`,
  `heap` and `blob`. Create a bucket with `create bucket <name> { type: <type> }`.
- **Backup, restore and loading a file.** `hoarddb dump` and `restore` write
  and read a logical backup; `hoarddb ingest` loads a BSON file or a JSON
  Lines file into a bucket, with progress, a summary and a resume when it is
  interrupted.
- **Users and roles.** Create users, grant per-database roles, disable and drop
  them, and rotate the root password with `hoarddb-server -reset-password`.
  Passwords are prompted for, never passed on a command line.
- **Dates** are stored as dates and compared by instant, written
  `{"$date": "<RFC 3339>"}`.
- **Every Extended JSON wrapper the CLI can read is stored as its type** in an
  insert, an update, a filter and a field-store element: `$numberInt`,
  `$numberLong`, `$numberDouble`, `$numberDecimal`, `$binary`, `$uuid`, `$oid`,
  `$regularExpression`, `$timestamp` and `$date`. Before, only `$date` was; the
  others were stored as a document with the wrapper's name as its one key and
  printed as if they had worked. The deprecated wrappers (`$symbol`, `$code`,
  `$scope`, `$dbPointer`, `$undefined`, `$minKey`, `$maxKey`) and a malformed
  wrapper are refused with a message that names the wrapper and the form it takes.
  A document prints these types as the same wrappers, so what you read is what you
  can type back. See the "Numbers and other types" section of
  [docs/user/hql-spec.md](docs/hql-spec.md).
- **Indexes** on a hash bucket: single-field, compound, unique, and expiring
  (TTL) indexes, and a `search` that says whether it used one.
- **Store fields** inside a document: a field of a document can be a store of its
  own (queue, stack, heap, ordered or blob), with its elements stored separately.
- **A replication factor per bucket**, set when the bucket is created, and a
  cluster that refuses to guess: a node that stays unreachable is reported, never
  removed for you.
- **Encryption at rest for a whole node**, off by default, with the key held
  outside the data directory (key file, command, or `systemd-creds`), and
  `hoarddb-server -export-key` / `-import-key` for escrow.
- **Ask a node whether it is encrypted.** (A `hoarddb`
  session has no `encryption` statement, and a node's mode and sealed state
  are on its `/metrics`.) The CLI's `encryption` command prints
  whether the store is sealed (from its files, not from the configuration), the
  mode, the keystore, the key ids and what the node writes in the clear anyway.
  `describe bucket`, `db.<bucket>.stats()` and `show buckets` print
  `Encrypted: yes` or `no`, which adds one line to their output. `/metrics`
  carries `hoarddb_node_encryption_sealed` and `hoarddb_node_encryption_info`,
  and an encrypted node's audit log records a `key.unwrap` entry per start. See
  [docs/user/configuration.md](docs/configuration.md#checking-a-running-node).
- **Node capacity readings.** `hoardDB status` shows how close the node is to its
  file-descriptor, memory-mapping, memory and disk limits, and the same readings
  are available as metrics when metrics are enabled.
- **Replication gauges.** `/metrics` carries `hoarddb_replication_peers_not_in_sync`,
  `hoarddb_replication_lag_entries`, `hoarddb_replication_lag_seconds` and
  `hoarddb_cluster_peers_unreachable`, one unlabelled series each per node. A
  stream that leaves the in-sync set logs one WARN with the member and the reason,
  and one INFO when it returns; each node logs which member holds its write-ahead
  log back, once every five minutes. See "Is the cluster in sync?" in
  [docs/user/replication.md](docs/replication.md#is-the-cluster-in-sync).
- **Limits you can set:** `storage.max_blob_bytes` (16 MiB by default, at most
  31.9 MiB), `storage.max_document_bytes` (24 MiB by default, at most 30 MiB),
  `storage.memtable_mb` (32 to 1024 MiB), and `storage.blob_dir` to keep blob
  payloads on their own volume. See [docs/user/limits.md](docs/limits.md).
- **A Docker image** and a three-node `docker compose` file whose published ports
  match the ports a client uses; binaries built for Linux on amd64 and
  arm64, with SHA-256 checksums (`scripts/build-matrix.sh`).
- **A CLI that says what to do.** Errors name the problem and the fix, a refused
  batch says how to split it, a permission refusal names the grant that would
  fix it, a read that finds nothing says why, a piped script stops at the first
  failure and exits non-zero, and `-v` prints the version and the protocol.
- **`hoarddb-server --config <file>`** (or `HOARDDB_CONFIG=<file>`) reads a YAML config
  file. Settings layer as built-in defaults, then the file, then `HOARDDB_*`
  variables. The server read no file before, though the docs described one. A
  missing file, invalid YAML or a key the server does not define stops the start
  with a message that names the problem and the fix; `key_command` for the
  `command` keystore can now be set. The documented example file now holds only
  real keys, and the `bucket_limits` / `l2_soft_limit` / `l2_hard_limit` settings,
  which were documented but never implemented, are gone from the docs.
- **The CLI's `update` takes all nine update operators.** `$push`, `$addToSet`,
  `$mul`, `$min`, `$max` and `$rename` joined `$set`, `$unset` and `$inc`. The help
  already listed them, and the server already applied them for a program that sent
  the request itself, but the CLI refused them with "is not supported". It still
  refuses `$pull` and upsert, and the refusal now lists the nine. A path with a
  position needs no quotes in the shell: `{Lines[0].Qty: 3}` is the key
  `Lines[0].Qty`. ([docs/user/hql-spec.md](docs/hql-spec.md),
  "Update operators".)

- **`hoarddb restore <file> --verify` checks a dump file with no server.** It reads the whole file once and checks the magic, that every record is exactly the layout of dump format 1, that the counts in the summary match what the file holds and that the checksum in the footer matches, then prints what the file holds (databases, buckets, entities, elements). It needs no running server and no ring mode and sends nothing. A cut-off file is named as incomplete and a changed file as damaged, each with what to do next, and a file written by a newer hoarddb that this version cannot read is refused by name; all exit 1. Every dump is read online and is not a point-in-time copy, so `--verify` prints that first, and, when the file declares a queue field, that claimed and dead-lettered queue messages are not in it. This is the file reader and writer only: `hoarddb dump` and `hoarddb restore` themselves arrive in later builds, and `restore` without `--verify` behaves as before. Pre-v1.1 and not versioned: direct. Cost per bucket: none; no per-bucket metric; memory is one record at a time whatever the file holds; O(1) in buckets (the verify is O(records) by design).

### Changed

- **Removed: the cluster token. A first start no longer prints a cluster token and no longer writes `cluster.token`, and `HOARDDB_CLUSTER_TOKEN` has no effect.** Nothing read the token: the members of a cluster trust each other by pinned keys (`HOARDDB_SEED_FINGERPRINTS`), and each node signs the login tokens it issues with its own `auth.token.key`. Two members started with different tokens joined and served. The `=== CLUSTER TOKEN ===` notice of a first start is gone, on a terminal and off one. A server started with `HOARDDB_CLUSTER_TOKEN` or `HOARDDB_CLUSTER_TOKEN_FILE` set starts as if it were not, and logs one warning: `HOARDDB_CLUSTER_TOKEN is not used by this release: a node joins a cluster on its pinned key (HOARDDB_SEED_FINGERPRINTS), not on a token. Unset it`; a `_FILE` that names a missing or empty file no longer stops the start. A `cluster.token` already in a data directory is not deleted. The messages that told an operator to run `get-token`, a statement a `hoarddb` session does not have, are gone with the notice. The compose file no longer makes or mounts a token. Cost per bucket: none.
- **Removed: the `-token` flag, the `get-token` and `rotate-token` commands and the `HOARDDB_CLUSTER_TOKEN` setting of the `hoarddb` program, and the `--token` options of `join`, `remove-node`, `ingest` and `dump`.** None of them read a cluster token any more. A command line that still passes `-token`, `ingest --token` or `dump --token` now stops with `flag provided but not defined: -token`, and the `join` and `remove-node` statements no longer take the option: delete it. The disk-full refusals show `remove-node <node>` without a token. Cost per bucket: none; no metric.
- **Changed: the refusal of a damaged `cluster.token` now gives a fix you can follow.** A start with encryption at rest that finds a `cluster.token` that is neither sealed nor a token file still stops and changes nothing, but no longer tells you to copy the token from another member (no member has one). It says to move the file aside (`cluster.token.old`) and start again; nothing is lost, because nothing uses the token. Cost per bucket: none; no metric.

- **A ring node's store now opens only at a point every synced write reached, and re-applies the rest from its own log (the vouched cut).** A node keeps recent writes in memtable logs that are not forced to disk on every write, so after a power cut or a disk write error the store could come back holding a later write without an earlier one, and the start-up replay, which skips what the store says it applied, never restored the earlier, acknowledged write. Now, after every store sync, the node records how far the store is known to be on disk in a small file, `<data dir>/ring/badger/VOUCHED` (8 KiB, two counters, nothing secret; it is not encrypted). At start it discards what the store holds past that point, makes the discard durable, and re-applies those writes from its log in their order. A key deleted just before a power cut or a kill no longer comes back when the store had moved the delete to a table file. There is nothing to configure. What you see:
  - **After a kill or a power cut,** one line at start: `store: discarded <n> commits above the sync marker (version <v>) and kept everything through <v>; <n> log(s) truncated, <n> removed. The node re-applies them from its log, starting at log sequence <s>.` A node that is not in a cluster finishes that before it serves. A cluster node serves first, and the pairs whose replay needs a bucket's declaration from another node refuse reads and writes until their replay finishes; the replay's own line says how many (`<n> pairs refuse reads and writes until their replay finishes`). After a clean stop nothing is discarded. A clean stop logs `store: <n> store syncs, <n> sync-marker advances (marker writes took <t> s in all) since this node opened.` `status` gains no line.
  - **A store that cannot be opened safely is refused, nothing in it is changed, and the text ends with what to do for the kind of node it is.** A store with data files and no marker: `the store <dir> holds data files and no sync marker <dir>/VOUCHED, so this node cannot tell which of its last writes reached the disk. Nothing was changed`. A damaged marker: `the store's sync marker <file> is damaged (<what>), so this node cannot tell which of its last writes reached the disk. Nothing was changed`. A marker of another format: `... has format <n>, and this release reads format 2 only. Nothing was changed` (exit code 70, as for a ring directory of another format). There is no switch to open such a store anyway. On a cluster member each of these ends `Stop this node, keep <ring dir>, start it again on an empty ring directory with the same node id, keys and address, and run 'replace-node <node> --with <node>@<addr>' on any member`. On a node that is not in a cluster a store refusal ends `move <ring dir>/badger aside (keep it) and start the node; it rebuilds the store from <ring dir>/wal when the log holds every write from sequence 1, and refuses to start, saying so, when it does not`, and a refusal about a log record or the marker's format ends `no other node holds this data and <ring dir> is the only copy: do not delete it. If you have a dump or backup, move <ring dir> aside (keep it), start the node again (it starts with no data), and restore the dump into it. If you have none, the same steps start the node with no data and leave this directory unread; keep it and report the sequence number above` (`the text above` for the marker).
  - **A node whose store directory is empty although the node has run before.** A cluster member refuses to start: `the node store <dir> holds no data file, but this node has run before: its Raft state holds a term, a log entry or a snapshot ... Starting it empty would serve this node's data as empty ... If <dir> was moved aside or deleted, put it back`, then the `replace-node` advice above. If the store is lost, `replace-node` is the way back (see Added). A node that is not in a cluster rebuilds its store from its log and starts, with a warning that says so; it refuses only when its log does not begin at sequence 1, when its log is missing while it holds a database, or when its Raft state is gone, and each text says which.
  - **The texts of a damaged ring store no longer send a node to an empty data directory to be seeded by its peers.** That advice was unsafe for a cluster member. Refusals at start, damaged reads, a write stalled behind a damaged table, `CORRUPT_KEY`, a damaged dedupe entry, a log record this release does not read and a ring directory of another format now end in the advice for the kind of node it is.
  - **A store cut by a power loss during its very first flush starts again by itself in more cases** (the leftover table is kept beside its name with `.first-flush-crash`), including a first flush with nothing synced yet; it used to refuse and name a backup. An encrypted store in that state is still refused.
  - **One failed write of the store's manifest during a flush no longer leaves a store that does not reopen.** The node stops taking writes (`the node store at <dir> refuses writes: ... Writes stay refused until the node is restarted, and reads are still served. Check the disk (the kernel log), then restart the node`), and the restart finds the table or replays the log it kept. The same failure during a compaction is not fixed.
  - **If the marker file itself is half-written at a crash,** the node opens on its other, older half and replays from its log what the unfinished write covered; one warning names both halves. Damage to the disk under the newer half looks the same and can then discard writes the log does not hold (data copied in by a seed); the warning says so and names a backup of the data directory.
  - **Speed.** A store sync that moves the marker costs one more small write to disk (one 4 KiB block). An ordinary write to an established document makes no store sync and is not slowed. Adding a node to a cluster makes one store sync per pair it copies in (five when the cut was first built), and dropping a bucket makes one store sync for the whole drop instead of one per slot the node holds.

  A ring directory written before this change does not open (the ring format moved; see the format entry below). Not tested: a real power cut, a real disk error, filesystems other than ext4; the tests use simulated crash images. One window is known and left open: when two store syncs run at once and one fails, the other may still record its own progress before the failure is seen, and if the operating system has not yet reported the failure to either, the marker can cover writes that are not on disk. Cost per bucket, per store field and per document: none (one file, one open descriptor and one small map per node); no per-bucket metric; O(1) in buckets; a membership change stays O(pairs moved), one store sync and one marker write per pair.

- **The cluster state level numbering restarts at 1 for the first release.** `hoarddb-server -v` prints
`state_level=1 state_range=1-1`, `status` prints `State version: 1-1` and `Cluster version: 1 (1.1)`, and the release
table records level 1 as first released in 1.1. **A `ring/` data directory whose `CLUSTER_VERSION` marker is above this range does not open.** Such a directory is
refused at start with exit code 70 and the cluster-version text (`the cluster version is <n> and this build runs
versions 1 to 1`), because its `CLUSTER_VERSION` marker is above this build's range; nothing in it is changed. The ring
format stays 8. Pre-v1.1 and not versioned: changed directly, with no converter. Cost per bucket: none; no metric.

- **A member refusing a peer that writes a newer cluster version now says which side to fix.** When a peer's handshake
writes a cluster version above the highest this node's build runs, the refusal (logged once a minute and listed under
`Refused peers` in `status`) used to call this node's version "the cluster's" and told the operator to start the peer
on the older build. It now reads `refused <channel> connection from node "<node>": it writes cluster version <w>, above
the highest this build runs (<max>). If the cluster has moved to version <w> ('status' against <node> prints its Cluster
version), this node's build is the older one: upgrade this node, and every member still on its build, to a build whose
state_range includes <w>, then restart it. If the cluster still runs version <v>, <node> is too new for it: start <node>
on a build whose state_range includes <v>. hoarddb-server -v prints state_range on each.` Cost per bucket: none; no
metric.

- **The CLI's user and role statements run over the session's own connection.** `create user`, `alter user`, `drop user`, `grant role`, `revoke role`, `show users`, `user list` and `user describe` send the ring user commands (the ones `ringclient`'s `CreateUser` … `DescribeUser` send) over the ring connection. The password sources are unchanged (asked twice with no echo at a terminal, the lines after the statement in a piped script, `--password-stdin`, `--password-file`), the password is still derived into a credential on the client, and a password shorter than the server's `HOARDDB_PASSWORD_MIN_LENGTH` is still refused before anything is sent. A create reads the users first and is refused (`CONFLICT`) when a user was dropped in between; an alter reads the user first and is refused (`CONFLICT`) when the user store changed in between. What changes for a script: the lines print `(revision N)` where they printed `(config version N)`, the `USERS` line is `USERS (revision N)` with no master, refusals are the server's ring texts with their code (`NOT_FOUND: there is no user "ada". Run 'show users' to list the users.`), and their exit status is the exit table's for that code. A refusal a retry cannot clear exits 2: a name that exists is the new code `USER_EXISTS` (0x2B), and the last administrator and the 10,000-user limit are `INVALID_ARGUMENT`; a user dropped since the read, a stale revision and a reused salt are `CONFLICT` (exit 8), because running the statement again reads the users again. In a piped script, a line after a password statement that is not a statement (a password line the source did not take: a second line with `--password-stdin`, any line with `--password-file`) stops the run with exit 1 and is not printed. **`master` is removed**: there is no auth master on the ring; the statement is refused as one the CLI does not run. On a cluster member they change the cluster's users, as the ring user commands do (see Added). `ringclient.Client` gains `PasswordMinLength`, the minimum the server reports at login; an `AUTH_OK` body that does not decode now fails the login. Cost: nothing per bucket or store, no per-bucket metric; a `create user` reads the user list once (O(users), at most 10,000 rows), and a membership change is not affected.

- **`encryption.mode: require` no longer refuses cluster peers whose storage is not encrypted.** It now means: seal like
`on`, and also run a structural check of this node's own ring store before it opens, refusing to start when the check
fails (`on` reports the finding in `/health` and warns). No node checks another node's setting in this release: run
every node of a cluster with the same `encryption.mode`.

- **The driver vectors lose the 20 exchanges that were captured on a three-node cluster without cluster mode.** `clients/testdata/driver-vectors/v1.json` no longer holds `health`, `list nodes`, `list nodes with membership history`, `put on a cluster with write concern all`, `put on a cluster with a write concern the server does not know`, `put with write concern all and one node stopped`, `consistent get on a cluster`, `bucket info on a cluster`, `health of a node not yet added`, `get at a node not yet added`, `put at a node not yet added`, the seven `alter bucket:` rows that ran on that cluster (`raise the factor`, `lower the factor`, `declare the inherited factor`, `no change`, `refused in-band`, `refused, retry later`, `missing bucket`), `put to a bucket that does not exist, on a cluster` and `batch spanning two owners on a cluster`. The server refuses that mode at start, so the generator can no longer start the cluster they ran on, and nothing a driver sends the ring depends on them: a driver's ring behaviour is pinned by the `ring ...` vectors. No other vector changed, and no vector has topology `cluster3` any more. A driver test that named one of these rows should drop it; `alter bucket: on a standalone node` stays.

- **The `-ring` flag is gone, and so is its one-line refusal.** The ring is the only session, so `hoarddb -ring` is now an ordinary unknown-flag error (`flag provided but not defined: -ring`), like any other flag that does not exist. Run the same command without it.

- **Changed: the HQL reference (`docs/user/hql-spec.md`) is now a short page, and the `get-token` and `rotate-token` commands are no longer documented.** The page lists the statement families the ring runs, what is not available yet, and points at `help all` for the full statement list; the earlier statements and examples it carried are no longer on it. The ring has no `get-token` or `rotate-token` statement: read the cluster token from the `"token"` field of `data/cluster.token` on a node without encryption at rest.

- **A plain `hoarddb` session is the ring session.** `hoarddb` with no flag opens the ring (databases, buckets, documents and the six stores). User and role statements (`create user`, `alter user`, `drop user`, `grant`, `revoke`, `show users`) run over the ring connection (see the entry above). The refusals of `dump`, `restore` and `ingest` say how to list or create what is missing. `dump`, `restore` and `ingest` now stop on SIGTERM as well as SIGINT, with exit 4 and no partial file, as the exit table already said.
- **`TOPOLOGY` no longer lists a witness's client address.** A witness holds no password key and will refuse every login once the replicated user store lands, so the reply now writes an empty client address in each witness row; its id, state, data address and voter flag are unchanged, and the ranked members' client addresses are unchanged. In `ringclient`, `Topology.Witnesses[i].Addr` and `.ClientAddr` are now empty (a caller already skips an empty address). The cluster state still records the address the witness advertised; only the reply drops it. Pre-v1.1 and not versioned: direct. Cost per bucket and per store: none; no per-bucket metric; O(1) in buckets and on a membership change.

- **`hoarddb ingest <file> --into <database>.<bucket>` loads a BSON or JSON Lines file into the ring.** It reads the file once to work out its fields and their types, creates the database and the bucket from what it found (or keeps the declaration of a bucket you already made, and refuses with the `alter bucket` statement that adds a field it does not declare), then writes every document as an upsert, so running the same command again changes nothing that is already loaded. An `_id` becomes the ring's text id: an ObjectId is its 24 lowercase hex digits, a string stays as it is, and any other type gets a prefix such as `~i:1`. `--dry-run` says what it would create without creating it; `--on-error skip --rejects <file>` keeps the documents it cannot load; `--batch` sets the documents per request (1 to 255). A lost connection is resent under the same request id, so a lost reply adds no document. Exit status 3 means the run completed with documents skipped. `ingest` no longer reads a resume file, standard input or a directory, and has no `--type`, `--id-as`, `--write-concern`, `--allow-expiring`, `--state`, `--no-resume`, `--resume-anyway`, `--accept-duplicates`, `--from-offset`, `--again`, `--checkpoint-every`, `--into-db`, `--max-buckets` or `--allow-dots`; each of them, and a three-part `--into`, refuses with the command to use instead. Loading a whole directory, and loading a file into a store field of other documents, are not available in ring mode: load each file with `--into`. Pre-v1.1 and not versioned: changed directly. `ingest` always talks to the ring and prints a note saying that a plain `hoarddb` session still opens the legacy engine. Cost per bucket: none, the work is in the client.
- **A replication factor or seed list without cluster mode is refused at start.** A replication factor above 1 (`HOARDDB_REPLICATION_FACTOR`), or a seed list (`HOARDDB_SEED_NODES`), without `HOARDDB_CLUSTER_ENABLED=true` is refused: the server stops at start, before it opens a port or touches the data directory, and names the setting and the fix: set `HOARDDB_CLUSTER_ENABLED=true` to run a ring cluster, or remove the settings for one node. `HOARDDB_REPLICATION_FACTOR` above 1 is refused in ring mode too (owner ruling 2026-11-19): the ring ignored it and the operator got one copy. The message gives `create database <name> { replication_factor: N }` and says to unset the variable; the CLI is exempt. `docker-compose.yml` is rewritten to start a ring (`node1` bootstraps; `join node2@node2:4433` and `join node3@node3:4433` are meant to form it); the rewrite has not been run under Docker yet. Not supported before the first release, so no alias. The mode's code is deleted in a later change. See [Replication Factor](docs/configuration.md#replication-factor).
- **`create database <name> { storage: memory }` is refused until the in-memory store exists.** The option was accepted and recorded, but no in-memory store was built: a memory database wrote to the same disk store and log as any other, so it was not lost on restart as its name says. The CLI, `ringclient` and the server now refuse it with "storage: memory is not available yet: this build keeps every database on disk, ... Create it with storage: disk (the default): create database <name>", and `storage: disk` and the default are unchanged. A database already recorded as memory by a test cluster still opens and `describe database` still shows it; the on-disk format does not change.

- **Removed: the cluster keys `cluster.heartbeat_ms`, `cluster.step_down_ms`, `cluster.failover_ms`, `cluster.dead_ms`, `cluster.probe_interval_ms`, `cluster.probe_timeout_ms`, `cluster.probe_failures` and `cluster.stale_reads`, and their `HOARDDB_CLUSTER_*` variables.** They were documented and parsed, and nothing read them: the cluster runs on its committed values (a lease grant every 100 ms, `step_down_ms` 2000, `failover_ms` 5000, `dead_ms` 600000, a probe every 1000 ms with a 500 ms timeout, 3 failed probes, stale reads off), which no file, variable or statement changes in this release. A configuration file or environment variable that still sets one is now refused at start with a message that names the key and says to remove it; before, it was accepted and did nothing. Until hoardDB is public no previous version is supported, so there is no alias and no deprecation. The start-up check of those values against each other is gone with the keys (the test harness keeps it for the values it commits). The `read_preference` refusal no longer tells the operator to set `cluster.stale_reads`. No log, wire or on-disk format changes. Cost per bucket: none; no per-bucket metric; O(1) in buckets and on a membership change.



- **A busy cluster node no longer refuses reads and writes with `LEASE_LAPSED` because its housekeeping ran long.** The lease is now renewed on a goroutine of its own, one round a heartbeat counted from each round's start, so work such as the handoff's per-pair retry no longer sits between two renewals; a renewal round may take up to about three heartbeats (300 ms at the default `heartbeat_ms` of 100) before the lease lapses, where before any delay over about 100 ms did; the exception is a follower's round on a slow link, which can wait behind a relay call (a seed report, a membership or proposal relay) for its turn on the shared control connection and then take longer, so a link slow enough to hold a relay for about three heartbeats can still lapse the lease. The leader's own lease now ends `step_down_ms` after its quorum round started rather than after it ended, so the longer allowance shortens no fence. A node still refuses when a round takes longer than that or the process stalls for over three heartbeats, and `NO_QUORUM` still answers once the last round is more than three heartbeats old. No setting, default, text or wire format changes; the `lease lapse` line now names the renewer's phases (`round`, `sleep`, `wake`) instead of the supervisor step's. Cost per bucket: none; one goroutine per node; no per-bucket metric; O(1) in buckets and on a membership change.

- **A `LEASE_LAPSED` refusal prints how long ago the node stopped serving and the committed `step_down_ms`, and names the bucket, for reads and for writes.** Before, the text reached the developer as `stopped serving reads and writes <lapsed_ms> ms ago ... (step_down_ms <step_down_ms>)` with the raw slot bytes and a numeric database id where the bucket should be named. The age is measured from the moment the lease stopped being valid (the expiry time for an expiry, not the time a request noticed it), taken from the same lease check that refused the request, and is at least 1 ms. The text template is unchanged. A node that has never held a lease reports 1 ms. Nothing on the wire changes. Also new: each node writes one WARN line when a lapse ends, `lease lapse node=... cause=... gap_ms=... phase_max=...`, at most ten a second, saying which cause voided the lease and which phase (`round`, `sleep` or `wake`, or which freeze of the process) spent the time, and whether the tick that noticed it was late (`tick_late`); it changes no lease rule. Cost per bucket: none (one small record per node and one goroutine waking every 10 ms); no per-bucket metric; O(1) in buckets and on a membership change.
- **A ring node started with no cluster state no longer takes its peers' traffic until a member admits it, so a member whose `ring` directory was moved aside no longer rejoins by itself.** A member started again on an empty `<data dir>/ring` (its `keys/` kept, so its peers still trust it) used to be caught up by the Raft leader as the voter it had been, with an empty log; the leader still counted it as holding what it had acknowledged, which could make the cluster forget committed changes, and the node then marked the slots only it had reported as ready and empty. Now a node that opens to join with no cluster state refuses every member's connection (`refused <peer> on the raft channel: this node holds no cluster state and no member has admitted it ...`) until a member's `join` of it asks who it is; that ask admits it. A new node joins exactly as before. A member that lost its data stays out, its peers mark it down and keep serving every database with a replication factor of 2 or more, and `replace-node` brings it back (see Added); if you moved its `ring` directory aside, keep it. A retried `join` of a voter that had not finished joining and lost its data is refused rather than admitted; `replace-node` brings that node back too. The start-up line of such a node now reads `this node holds no cluster state, so it serves its peers only once a member admits it` and names both cases. On the peer wire, `IDENTITY_REQUEST` grows from `[nonce:8]` to `[nonce:8][flags:1]` (bit 0 admits the node; any other bit, and the 8-byte request, are refused), so every node of a cluster must run this build. See [docs/user/cluster-add-a-node.md](docs/cluster-add-a-node.md), "If a member lost its data".

- **The ring format is now 8 and the cluster state level is 6: a `ring/` data directory in another format does not open, single nodes included. Start with an empty one.** The cluster's replicated state (`ring/raft`) now carries a user table: the rows, the id of the cluster's password key, the `state_seq` of the last drop and the last 256 applied user changes, changed only by six new cluster commands (`0x11` to `0x16`) that no statement sends yet; logins and user statements are unchanged in this build. A node refuses a directory marked with another format before it reads, changes or deletes anything in it, with the fresh-directory text and exit code 70, and nodes of different formats refuse each other at the handshake. Pre-v1.1 and not versioned: changed directly, with no converter. Cost: nothing per bucket, store or document and no per-bucket metric; the empty table adds 72 bytes to the cluster state, and a row is about 390 to 490 bytes of BSON (at most 10,000 rows, about 3.9 to 4.4 MB).
- **The last-administrator guard counts only accounts that can manage users: root or user-manager on every database. A cluster-manager no longer counts.** Dropping, disabling, stripping or revoking the authority of the last enabled such account is refused, and the refusal names the grant to give first: `drop user "root" was refused: it is the last enabled account that can manage users (root, or user-manager on every database). Give another enabled account that authority first, for example 'grant role user-manager@* to ops', then run the statement again`. Before, a cluster-manager (which cannot create, alter or grant users) or root on a single database kept the guard satisfied, and the refusal advised granting cluster-manager. A `users.json` whose only enabled administrator is a cluster-manager, or root on one database, no longer loads, and `--reset-password` refuses it too. The refusal says how to recover: a plaintext `users.json` (a file from before encryption) can be given an administrator by hand; otherwise stop the server, move the file aside and start it with `HOARDDB_ROOT_PASSWORD` set, which seeds a new root user and loses every other account in the old file. The same predicate guards the cluster's replicated user table.

- **The ring format is now 6 (it was 5) and the cluster state level is 4: a `ring/` data directory in another format does not open. Start with an empty one.** Every table the storage engine writes now also records, in its bloom filter, which documents and which store fields it holds a key of, and a `get` of a document and a `list`, `pop` or `peek` of a store field ask that filter and open only the tables that may hold their keys. No read returns anything different. A table in another format has no such record, so the node refuses a ring directory marked with another format (or with a store and no `FORMAT` file) before it reads, changes or deletes anything in it: `refusing the ring directory: the ring data directory <dir> was written in format 5 (FORMAT says: hoarddb ring format 5), and this build reads format 6 only; ...`, followed by the fix: set `storage.data_dir` (`HOARDDB_DATA_DIR`) to an empty directory and let the node be seeded by its peers, or move the directory aside and keep it. **A start refused for the ring format now exits 70** (it exited 1), the same "wrong build, do not restart" status as a cluster version the build cannot run, so a service manager told `RestartPreventExitStatus=70` does not loop on it. `hoarddb-server -v` prints `state_level=4`, and nodes of different formats refuse each other at the handshake. Pre-v1.1 and not versioned: changed directly, with no converter; a format 6 directory is refused the same way. Cost: nothing per bucket (no key, catalog field, file descriptor, memory mapping or goroutine) and no per-bucket metric. Per document, under one byte of filter in each table that holds one of its fields without its header, which is a table an `update` wrote into; a bucket that only inserts pays nothing. Per store field of a document, under one byte of filter in each table that holds a key of that store. Each table's index grows by 6 bytes. The filter is always on and has no setting.

- **The storage engine's default Level-0 table count is 2 (stall at 7), down from Badger's 5 (stall at 15).** A point read seeks fewer Level-0 tables, so it costs the server less CPU. The write side (compaction work and write stalls under a burst) was not measured. Stored data is unchanged and no setting is needed; the experiment variable `HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES` still overrides it. Pre-v1.1 and not versioned: direct. Cost per bucket and per store: none; no per-bucket metric; O(1) in buckets and on a membership change.

- **A cluster records its cluster version, and a node on a build that cannot run it stops with exit code 70.** The cluster state carries a committed `cluster_version`, set when the cluster is bootstrapped (to the bootstrapping build's highest state level) and moved only by the cluster's own command, which the `finalize` statement of a later release proposes. `status` shows it as `Cluster version: <V>`, and `describe_node` gains the optional `cluster_version`. A node whose build cannot run the cluster's version (one started on an older build after the cluster moved past it) refuses to start, or stops when it meets the version in a snapshot or a log entry, with text that names the build to start, says to keep the data directory, and exits 70 so a service manager does not restart it; an unreadable `CLUSTER_VERSION` marker exits 70 too. On disk and on the wire: the raft snapshot gains the field `cluster_version` (directly after `v`; the minimal state is 537 bytes and its snapshot file 569), the BOOTSTRAP command gains `cluster_version`, REGISTER_MEMBER gains `min_level` and `max_level`, a new command kind `0x10` (SET_CLUSTER_VERSION) exists, and each node keeps a one-line `CLUSTER_VERSION` file beside `ring/FORMAT`. **A `ring/` data directory in another format does not open: start with an empty one.** The ring format is now 5 (it was 4), and the cluster state level is 3, so a format 4 directory is refused with the fresh-directory text. Pre-v1.1 and not versioned: changed directly, with no converter. Cost per bucket: none; no per-bucket metric; the marker is rewritten only when the version moves.

- **The state level is now 2: the expiry, aggregate and reference formats are counted.** Expiry (op-record flags `0x04` and `0x08`), maintained aggregates (flag `0x10`, declaration field 12) and fetch-through references (declaration field 13) changed what a node writes to disk and sends, so `hoarddb-server -v` prints `state_level=2`. A build at level 1 and a build at level 2 refuse each other loudly (on `join` and on connect, listed under `Refused peers`), and data directories are not compatible across the bump: before the first release there is no converter, so start with an empty data directory. The on-disk golden now also records the op trailer bits, the pending-expire marker, the index, aggregate and reference layout constants, the aggregate inner key, the expiry key kinds and the forwarded-read sub-kinds and reply flags, so a change to any of them fails the build until the level is bumped. Cost per bucket: none.

- **Nodes declare a state version, and a node on another build is refused when it joins and when it connects.** The handshake now carries the lowest, highest and written state level of the build (bytes 6, 7 and 58, which were zero), and the identity reply carries the range and the build string. Before the first release a build declares one level, so two builds work together only when `hoarddb-server -v` prints the same `state_level`. `join` refuses a node it cannot work with before it registers anything, with text that names the build, its range, the cluster's version and the release to upgrade to; every acceptor refuses a connection from a member that returns on another build, logs it once a minute and lists it in `status` under `Refused peers`; a dialer whose peer closes right after the handshake says it may run another build. A node from before this change is refused as `sends no state version`. `describe_node` gains optional `state_range` and `refused_peers`. A golden test fails the build when a command field, a value a command accepts, a snapshot name, what a command does to the state, the ring FORMAT marker, an op-record flag or a declaration field changes without a bump of `cluster.StateLevel`. Pre-v1.1 and not versioned: changed directly, with no alias. Cost per bucket: none.
- **A keyed write in ring mode that names an undeclared field now answers `FIELD_TYPE` with the declared fields listed, every time.** Before, after the client relearned the bucket header once, the refusal came back as a bare client-side error that no code matched, so a driver could not tell it from a transport failure.

- **`delete` in ring mode refuses every key except `_id_`, `if` and `return`.** Before, a delete ignored any other key it was given. A misspelt option such as `retrun` now stops the statement with `PARSE_ERROR` and names the key instead of deleting the document as if the option were not there.

- **Every existing dedupe fingerprint changes once: the fingerprint of every operation now includes its `return` mode.** A retry that is still inside the dedupe window when a node runs this build is refused as a reused request id, the error a request id sent with a different operation already gets, rather than answered from the recorded entry. Retries older than the window are unaffected. A client that retries across the upgrade should send a new request id.

- **`store.DecodeStringID` is exported for the ring CLI, which names a document by the id the user typed.** Go modules that import `github.com/hoarddb/hoarddb-server/store` gain one function; nothing else in the package changes.

- **Ring mode can declare when documents and queue elements expire, and an expired document is absent to reads.** `create bucket` takes `expire at <Field>` (a declared `datetime` field) and `expire at "<path>"` after a fifoStore or lifoStore field; `alter bucket` takes `set expire at <Field>` and `drop expire at`; `describe bucket` prints an `Expires:` block; `help expire` says the field must hold the expiry instant, not the creation time. An alter's expiry rides the pending table, so it takes effect on every node at the schema bump, and dropping a field that is part of the expiry removes that part of the expiry in the same alter. Refusals (`EXPIRE_FIELD_TYPE`, `EXPIRE_STORE_TYPE`, `EXPIRE_TWICE`, `EXPIRE_NOT_DECLARED`) name the field and the statement that fixes it. A document past its time is not found by get, list or count, and an expired queue element is skipped by pop, peek, claim, list and count (at most 64 are removed per verb); the retention tick removes expired documents and elements through the owner's write path. A record carries expire items and the owner's clock (record flags `0x04` system, `0x08` expire), the node keeps a time-ordered expiry queue (node-block byte `0x11`), and declaration field 8 gains an optional expiry section behind marker byte `0xE1`. A keyed get of an expired document answers NOT_FOUND with the detail `{expired: true}`; a client built before this change cannot decode that detail and reports a decode error instead of a not-found for that one case (a deliberate break before the first release: update the drivers, see clients/handbook/errors.md section 9). On-disk and record layouts changed directly before the first release, not versioned. Cost per bucket: +2 bytes in the declaration when it declares expiry, no resident bytes, no timer, goroutine or file.

- **`expand`: a list brings the documents its ids point at, in one call (fetch-through).** `db.Room["general"].Members.list({expand: true})`, `db.Message.list({expand: {Author: true}})` and `get({expand: {...}})` read, by key, the far document of each id and print it under the row, or `(not found)`. A field can be declared `of` a bucket (`Members hashStore of User`, `Author string of User`, or `of ref.User` across databases): `describe bucket` shows it, `expand` then needs no bucket name, and a write that sends a non-text id is refused. `where` keeps rows whose far document matches; `local: true` lets a ready copy answer and the reply says `served: local` or `served: owner`. The declaration gains field 13 (the references), the read classes get, store list and get_fields take an options trailer, and list requests and replies gain `local`, `expand`, `expanded`, `expand_filtered` and `served_from`. `cluster.stale_reads` now takes effect on keyed document reads; it was stored and never read. Scatters are unchanged. Reading the documents a list points at is on [data types](docs/data-types.md). Cost per bucket: 10 B on disk per field declared `of` and one count byte, none for a bucket without one; resident, a 24 B slice header in each cached declaration (the cache is bounded) and 16 B per reference; no goroutine, descriptor or per-bucket metric; a membership change is O(1).

- **Writes to one pair of a replication-factor-1 database no longer wait for each other's disk flush, so some answers change.** The owner appends a write, releases the pair's lock, and shares one flush with the other writes of the pair. Reads do not wait for a pending flush and never see a write that is not applied; `status`, POSITION_QUERY, catch-up and a slot handoff wait for it, so they can lag by one flush. A write whose flush fails is now answered with an error saying whether it was written is not known, and telling the client to retry with the same request id; the retry is deduplicated (before, this was UNAVAILABLE). When an earlier write of the pair could not be applied, the writes saved behind it are answered UNAVAILABLE with the same text as any write whose apply failed (`durable at <position> but applying it failed ... Retry it with the same request id to read its answer once the pair has recovered`) plus a detail saying the write is saved in the log, not lost, and not visible yet; the pair then replays its log, which applies them, and holds reads until it has. When several writes of the pair ask for that replay at the same time they share one, so a replay that fails is reported to all of them, and the next write runs a new one. That is the one instruction: retry with the same request id. The exception is a pair that has no record yet (the first write after a database is created, before the first reconcile pass): if that one write cannot be applied, its replay does not apply it. Nothing is written behind that write, so only it is affected, as before this change. Answers that were a plain error before can therefore now carry the apply-failure detail. Replication factor 2 and above keep the old path. Pre-v1.1 and not versioned: changed directly. Cost per bucket: none.

- **A node whose cluster state disagrees with the cluster's now stops with exit status 70 and a named log line, instead of staying up.** On the first committed entry it refuses for a reason its leader's own rules would not have (any refusal except `STATE_MOVED`, an entry it cannot decode, or an entry stamped ahead of its state), it logs one error line that names the entry, its term, the leader when the node knows it, the cause and the fix, and it exits with status 70 (`EX_SOFTWARE`) after a stop of at most 10 seconds. Before, it kept serving its stale view. Start it on the build the proposing member runs and keep its data directory; a node that stops again with the same line cannot be repaired in place by this release, and the way back is the copy taken before the upgrade, restored on every node. A snapshot this build cannot read stops the start with exit status 70 and a line that names what it cannot read, and a snapshot the leader sends that this build cannot read stops the node with 70. The user documentation for the code and the restart policy is [Add a node to a cluster](docs/cluster-add-a-node.md#a-node-that-stops-itself-exit-code-70). Pre-v1.1 and not versioned: changed directly, with no alias. Cost per bucket: none.

- **A node that is replaying its log, or that has diverged, answers no client request with data, and a refused command is refused before it reaches the log.** Until the node's log is applied, every client frame is answered UNAVAILABLE `replaying its log`, so a restarted node no longer serves reads and writes from a half-replayed state; retry the request. A diverged node refuses a proposal with `this node's cluster state disagrees with the cluster's ... Send the command to another member.`, writes no snapshot (the library logs `failed to take snapshot` and retries until the exit) and starts no seeding or reconcile pass that moves data. A command the leader's dry run refuses is answered at once and is never appended, so a refusal no longer becomes a committed entry that another build can disagree with. The unread-field refusal now says to start the node on the newer build and not to remove the field. Pre-v1.1 and not versioned: changed directly, with no alias. Cost per bucket: none.

- **A node that is replaying its log or has diverged now says so in `status` and in the health check, and a signal during a long start stops the server at once.** `status` printed `Serving: ok` on such a node; it now prints `Serving: replaying its log` or `Serving: diverged` with the next step, and `HEALTH_CHECK` answers not healthy with the same reason first, so a readiness probe no longer sends traffic to it. With authentication on, a login is refused until the node serves, so `status` works once it does. The replay gate now opens only when the node applied every command its log holds, however long one entry takes; before, a stall of a few hundred milliseconds in the middle of a replay could open it on a half-replayed state. A SIGINT or SIGTERM received while the server waits up to 30 s for its log now ends that wait and runs the normal stop, instead of waiting for it to time out. Pre-v1.1 and not versioned: changed directly. Cost per bucket: none.

- **A cluster node refuses to start when the address peers would dial names an IPv6 zone, including a zoned `server.listen`.** With `cluster.enabled` and neither `cluster.listen` nor `cluster.advertise` set, the peer listener binds `server.listen` and peers are told to dial it, so a value such as `[fe80::1%eth0]:4433` there now stops the start naming `server.listen` and the fix (`HOARDDB_LISTEN=<this node's address>:<port>`, or set `cluster.advertise`). A zoned `cluster.listen` or `cluster.advertise` was already refused. A node that is not in cluster mode, or that sets `cluster.advertise`, is unaffected. Cost: nothing per bucket.
- **`create bucket` and `alter bucket` in ring mode refuse an index instead of printing OK and dropping it.** The grammar still parses `index ByTotal (Total desc)`, but the ring wire does not build it, and the statement used to answer `OK` with the clause silently gone. It is now refused before any server call with `INVALID_ARGUMENT`, nothing created or changed, and the help pages say indexes are not built yet. Cost per bucket: none.

- **A store field keeps sums and counts of its elements current: `with (sum Amount, sum Amount by Currency, count by Category)` on `create bucket`.** The engine maintains them in the same batch as the element: a push, put or insert adds, a pop, take, ack or dead-letter subtracts, and a put that replaces subtracts the old element and adds the new. Read them with `sum("Amount")`, `sum("Amount", {by: "Currency"})`, `count({by: "Category"})` and `describe()` on the store (new ring read class 0x8B, on the same grant as `list`, because the by forms list every group value). An element with no number at a summed path, a decimal, or an element that would take the absolute values an aggregate covers past 2^62 (about 4.6e18) is refused before a position is taken; that bound, not one on the sum, is what keeps every pop, take, ack and dead-letter from overflowing. Each aggregate key holds the sum, the sum of absolute values and (by forms) the count, 17 or 25 bytes. A record that touches a store with aggregates grows by about 16 bytes per aggregate, a bucket's declaration by about 14 bytes per aggregate (5 + fLen + bLen), and nothing is resident per bucket; there is no per-bucket metric. An aggregate read at a node that does not hold the document is forwarded to the node that does, as every other store read is (new FORWARD_READ store operation 7). Adding an aggregate to a store that already has elements is not built yet. The ring data format changed (record flag 0x10, declaration field 12, inner key 0x06 in a field store): before the first release and not versioned, delete the data directory and start fresh.

- **The ring data format changed again (update and delete records end in a trailer byte): a data directory in another format is refused at startup.** Every update and delete record body now ends in one trailer byte (guard flag and return mode), so the FORMAT marker moved from 3 to 4 (directories marked 3 exist both with and without that byte, so the number 3 could not say which a directory was; 4 always has it). A node started on a directory marked 3 stops at the first read with the FORMAT refusal, which names the directory and the fix, and changes nothing in it; before, such a directory failed in log replay with `trailer needs 1 bytes`. Start with a fresh data directory (set `storage.data_dir` or `HOARDDB_DATA_DIR` to an empty directory, or move the old one aside). Cost per bucket: none; a plain update or delete record grows by one byte.

- **`seeding forget` now refuses a retired record that this node may hold the only copy of, and a new `anyway` form goes ahead.** When no other member of the slot's copy set has confirmed a copy covering the fence (a database at replication factor 1, or every other copy absent, not ready, or not heard from since this node started), `seeding forget <db> <slot>` answers SEED_HELD, changes nothing, and names `status seeding <db> <slot>` and the fix: `seeding forget <db> <slot> anyway`. The check is made under the same lock as the delete, so a record that retires while the forget runs is judged on what it is then. Forgetting lets the next seed of the slot delete its kept range, so the data would be gone. A retired record that a member has confirmed a copy of is forgotten as before, with no `anyway`. Wire change, additive: the `seeding_forget` request takes an optional `anyway` boolean that is left out when false. Cost: nothing per bucket.

- **`status seeding`, `seeding retry` and `seeding forget` now run on a cluster node, and a seed copy is paced, can stall and can pause.** `status seeding [<db> [<slot>]]` lists the slots this node is placing, seeding, stalled or retired; a retired row names the member the deletion gate still waits for. `seeding retry <db> <slot>` starts a seeding or stalled slot again from a fresh cut on the next legal source; `seeding forget <db> <slot>` drops the record of a retired slot (the range is kept and the data goes on the next seed). A node with no cluster still answers with an empty list and refuses retry and forget. `replication.seed_bytes_per_second` is now enforced (it did nothing before; `0` is unlimited); a pull with no applied chunk for `replication.seed_idle_seconds` is stalled and retries on the next legal source; a receiver at or above `replication.seed_disk_high_water_pct` pauses its pulls and keeps their data; an under-replicated pair seeds first; and five node-wide series (`hoarddb_node_slots_ready`, `_seeding`, `_retired`, `hoarddb_node_seed_bytes_total`, `hoarddb_node_seed_retries_total`) report it, with no label. Cost: nothing per bucket and no per-bucket metric; `status seeding` walks every held pair once per call (O(held pairs)), and the pacer and the stall clock keep one small state per pull in flight.

- **The ring data format changed again (storage on the database row): a data directory in another format is refused at startup.** A database row in the cluster state now records whether the database is on disk or in memory, so the FORMAT marker moved from 2 to 3. A node started on a directory of the previous build stops at the first read with the FORMAT refusal, which names the directory and the fix, and changes nothing in it. Start with a fresh data directory (set `storage.data_dir` or `HOARDDB_DATA_DIR` to an empty directory, or move the old one aside). Cost per bucket: none.

- **A write whose copy holder is down no longer waits the whole `replication.wc_timeout_ms` before it answers `applied_degraded`.** With one of three nodes killed under sequential writers, acknowledged writes slowed to a crawl, because every write to a pair that node held waited the whole timeout. The owner now remembers, per peer and in memory, that a copy stopped answering (a concern wait ran the full timeout without it and without any ack from it on another pair, or three real sends to it failed in a row; the transport's backoff refusals after one dropped connection do not count) and stops waiting for it: such a write answers `applied_degraded` once the owner and every copy that can still answer have it, at the latency of the owner's own append. A copy the cluster has committed `down` also ends the wait at once, including for writes already waiting. Sends to the suspect copy never stop, so each write probes it, and the mark clears when it acks, when a probe sees it return or when the cluster marks it `active`; each transition is logged once at INFO. The answer means what it meant: `applied` still requires the level to be reached by real acknowledgements. For a copy that is gone, an early `applied_degraded` is held by the same nodes as the one after the full timeout; for a copy that was only slow, the client gets `applied_degraded` where waiting would have given `applied`, for as long as the mark lasts (2 s, doubling to 30 s on repeats, cleared by the copy's next applied ack). Nothing on the wire or on disk changes, there is no new setting, and `copies_confirmed` is unchanged. Cost per bucket: none; per peer: one small in-memory record, kept only for a peer that failed or acked.

- **`create database app { replication_factor: 2 }` now works in ring mode on a cluster of three voters.** The ring admin's `create_database` sends the factor the statement names to the cluster state machine instead of refusing every factor above 1, so a database can be created at RF 2 (or more, up to the active data members) once the cluster has at least three voters. A factor above 1 on fewer than three voters, or above the data members, is still refused `RF_UNSUPPORTED`, and that refusal now names only the condition that failed and the command that fixes it (`join <node>@<addr>` to add a voter or a data node, then re-run the create) instead of quoting `alter database`, which cannot fix a create that was refused and which ring mode cannot run. Nothing on the wire or on disk changes. Cost per bucket: none.

- **A joiner is activated from the committed seeding bits, and a new copy seeds from another member when one holds the data.** The activation gate reads the `SEEDED` bit each member has committed for each pair it holds, instead of asking every pair. A join with data present therefore activates up to 2 seconds (the reporter's interval) after its last pair is ready, and a refused activation names how many pairs the joiner has reported so far. An RF raise or a table shift on an active node now seeds the new copy when another member holds the data, and writes an empty pair ready only when nobody does; before, such a pair was written ready and empty without asking. A seeding pair that leaves its copy set keeps its record until the range delete has taken it, so a crash cannot leave a resumable cut over a deleted range. `Reconciler.Forget` exists for the seeding view, but `seeding forget` still answers "not available yet" until the seeding engine lands. No wire or on-disk format changes. Cost per bucket: none; per pair: two bits read in memory, with no round trip.

- **A member the cluster committed `down` now returns to `active` on its own.** A node that was killed or cut off for longer than failure detection comes back `active` within a few seconds of its return, without an `activate` or `replace-node` command. No command, wire message or on-disk format changes. Before the first release, no compatibility.

- **A copy that fell below its owner's log, or holds a tail nobody owns, is now reset and seeded again instead of staying behind.** A copy whose next needed record was no longer in the owner's log was answered `LOG_BELOW_HORIZON` and stayed behind for good, and a copy with a divergent tail was refused with "restart this node empty". Its catch-up now asks the node to reset and seed the pair: the pair goes back to `seeding`, serves nothing and counts for nothing, its range is deleted at `storage.reclaim_keys_per_second` (the same budget the retire and dedupe sweeps share, now one object on the node) and pulled again from the owner, and the owner pushes to it once it is ready. While the pair is seeding a write is `applied_degraded`. The owner lowers its view of a copy only on an applied acknowledgement that follows a refusal, and counts a copy for a retried request only while its latest answer was applied. A catch-up that is running when the pair is reset applies nothing more. Two faults that kept such a copy from ever reseeding are fixed: the owner re-armed one read forever from a remembered log cursor that pointed into a truncated segment, and the copy did not recognise the horizon answer across the peer channel. Nothing on the wire or on disk changes shape; nothing truncates the ring log yet. Cost per bucket: none; per node: one reclaim budget object; per replicated write: one more lookup under the reconciler's mutex .

- **A restarted copy no longer replays what it already applied, and no longer fails to open after a store verb (RF 2 and more).** A copy applied each replicated record in memory only: its pair record (position, log cursor, ordinal) was never written, so after a restart it replayed its whole log from the position it was seeded at. A copy that had applied a `push`, `pop`, `take` or `claim` could not open at all, and a replay that got through would have applied a non-idempotent record twice on the copy only. The copy now writes its pair record in the same batch as the data, as the owner does, and reports the position it acked after a restart. A copy that applied nothing since its seed is unchanged. Nothing on the wire or on disk changes shape. Cost per applied record: the pair record in the batch the owner already pays for; per bucket: none.

- **Every pair of a cluster node now has one record shape, and every data member reconciles its pairs.** A pair written by the owner path kept a 32-byte position record while a seeded pair kept a seed record, and only a joining node ran the reconcile pass. At boot a cluster node now rewrites its 32-byte records as `ready` seed records (71 bytes each, the same position, log cursor and ordinal; a single node keeps the 32-byte shape), the first write to a pair with no record creates its record in the write's own batch, and a replay never creates one. The reconcile pass runs on every data member once its Raft log is applied, not only on a joiner; a pair a node is in the copy set for and has no record of is written `ready` empty until the committed seeded bits exist. A `POSITION_QUERY` for a pair a node holds no record of now answers `seeding`, never `ready` at `0:0`; a write or a replicated record for such a pair is still admitted on a node that is not joining. Pre-v1.1, not versioned: **a ring data directory in the old position-record format is rewritten in place at its first open** (the position records only; nothing else moves). Cost per held pair: 39 bytes more at rest; per bucket: none.

- **The ring's on-disk format is revised; a ring data directory in another format is refused at start.** The retained-log record header is 5 bytes smaller and the record 4 (an 18-byte header: version, the request id, the op index; the class is the entry's own op byte and the body's length is the entry's) and its body carries a flags byte for the index stamp (`0x00` and no stamp when a write touches no indexed field); the dedupe entry's value is 16 bytes smaller (format 3, varint fields; the 36-byte key is unchanged); a ring log entry carries no bucket label (the key and the body name the bucket); and dead dedupe entries are now swept on the node's reclaim loop at every `replication.retention_check_seconds` (new key, default 60, 1 to 3600, `HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS`) on the `storage.reclaim_keys_per_second` budget. The node writes `<data_dir>/ring/FORMAT` (`hoarddb ring format 3`) before its first store write and reads it first at every open: a ring directory with a store and no marker, or a marker naming another format, is refused with a message naming the fix (start on a fresh data directory), and nothing in it is read, changed or deleted; nothing is migrated. **Upgrade procedure:** stop the whole cluster, start every node of the new build on a fresh data directory, bootstrap and rejoin; a node of the previous build that connects to this one is refused at the peer handshake's envelope version (now 2), so nodes of different builds are never mixed. The cluster-probe driver vectors move by that version byte; no client-visible byte changes. Cost per bucket: none.

- **A restarted or newly promoted owner now brings a copy that was behind up to date without waiting for another write (RF 3 and more).** A write is acknowledged once the owner and one copy hold it, so a third copy can still be waiting when the owner stops; the owner kept what it had sent to each copy only in memory, and after a restart nothing sent the pair again, so a pair that was never written again stayed short of its replication factor for good. The owner now sends the last record of each pair it owns, once, to the copies, after a restart and when a move makes it the owner; a copy that is behind asks for the rest, a copy that is current acknowledges it.

- **Replication to a copy no longer waits for the copy's fsync one record at a time (RF 2 and more).** A node used to send one record to one copy, wait for the copy to fsync and apply it, then send the next, so 16 writers at RF 2 delivered about as many writes per second as one writer at RF 1. The copy now acknowledges receipt at once, applies each pair's records in order on a pool of workers (different pairs share the log's group commit), and sends its durable acknowledgement after the record is fsynced and applied; a write is still counted as confirmed by a copy only on that acknowledgement. `replication.repl_window` now takes effect (it was read and ignored; the window was always 64). A copy that loses an acknowledgement is shipped to again after 5 seconds. The first shipment of a pair no longer reads and decodes the whole retained log to find the pair's records (it started at the first retained record, which made a run of new pairs quadratic in the log), and `replication.repl_window` is now refused at start outside 1 to 4096. RF 2 write throughput rose, but it is not claimed as parity with RF 1. Nothing on the wire or on disk changes; nodes of different builds must still not be mixed.

- **Cluster commands now carry compare-and-set fields, and the cluster state machine enforces the database name policy.** `SET_SETTING` takes an optional `expect_value` and `SET_MEMBER_STATE` an optional `expect_state`; a command whose expected value no longer holds is refused `CONFLICT` and changes nothing. `CREATE_DATABASE` refuses reserved, over-long (more than 255 bytes) and path-unsafe names and a replication factor above the number of ranked data members (`RF_UNSUPPORTED`) in the state machine itself, not only in the calling code. A node that is not the Raft leader now relays create database, schema bump, member activation and setting commands to the leader instead of failing with "not the Raft leader". This is pre-v1.1 and not versioned: nodes of different builds must not be mixed.

### Fixed

- **A document of 1 MiB or more no longer reads back empty after a power cut that fell just after the store moved it from memory to a table file.** Applies to the ring layout. A large value is kept in a separate value file, and the store's index holds a pointer to it. When the store moved its in-memory writes to a table file it made the table durable and did not make the value file durable first, so a power loss right after could keep the pointer and lose the value: reading the document then returned an empty value and no error. Under a write load that move happens every few seconds, so this was the normal case for a large document written in the seconds before a power cut. The store now writes the value file to disk before the table is recorded. If writing the value file to disk ever fails, the store keeps the failure: it stops moving writes to table files, refuses new writes after the third failed attempt (as it does for any move that keeps failing), and every later sync fails, instead of a retry reporting success on a file whose unwritten pages the operating system may have dropped. Restart the node after checking the disk. Nothing changes on disk or on the wire and there is no setting. Cost: one write-to-disk of the value file per move, on a background task, off the write path (how long depends on the host and on how much of the value file is not yet on disk; a move happens once per 64 MiB of writes); a value read can wait up to that long when it coincides with a move and with the store starting a new value file. Per bucket: none; no per-bucket metric; O(1) in buckets and on a membership change. Not tested against a real disk error or a real power cut (simulated crash images only).

- **The refusal of a store verb that does not fit a field now names the database, bucket and field, the type the field is declared as, and what the verb takes there.** Before, the text said "the store verb does not fit the field or its arg is not the layout the verb takes" and listed every verb, without saying which field or which of the two causes. The server now says either `push cannot be applied to field "Purchases" of bucket "User" in database "app", which is a hashStore` or `the arg of field ... is not the layout push takes on a fifoStore field`, with the layouts that verb takes and `describe bucket User in app`. `ringclient` `Bucket.Push` and `Bucket.PushHeap` also check the declared type before sending: `Purchases is a fifoStore, not a heapStore`, naming the call that fits. Message text only; no wire change. Cost per bucket and per store: none; no per-bucket metric; O(1) in buckets and on a membership change.

- **A restarted node of a replicated ring (replication factor 2 or more) no longer spends a long time re-reading its write-ahead log before it ships its tails, and a stop no longer waits for that.** After a restart, and after a failover or a member's return, each replication lane read the node's one shared log from every pair's last record to the end of the log, one entry at a time, once per pair: on a node with a long log and many pairs per lane that is billions of entries per lane, which kept cores busy for hours, and a `SIGTERM` that arrived during it waited for one seek of the log per pair still to be read and needed `SIGKILL`. A lane now stops each pair's read at the pair's last known record, reads the pairs of one pass in log order on one cursor, and checks for a stop between pairs and before every seek; on a restart a pair costs one record and the whole pass walks the log at most once (a pair that is read again after its copy refused a frame or its send failed starts from its last acknowledged record, a read that stops at its window of 64 records starts the next one with a seek, and such reads can overlap each other: that is not bounded by this change). A pair whose boot replay has not finished, and a catch-up served to a copy, are read as before, and a pair recovered after a failed apply is now queued for its copies at its new tail. Nothing to configure; no wire message or on-disk format changes. Pre-v1.1 and not versioned: direct, with no alias. Cost per bucket and per store: none (the lane keeps nothing per bucket; per pair it reads two numbers that already exist in memory, and one pass holds a small transient record per queued pair); no per-bucket metric; O(1) in buckets; a membership change still costs one in-memory pass over the pairs this node owns, and the lane work it causes is O(log span + pairs) per lane instead of O(pairs x history).
- **After a restart or any other epoch move, a replicated ring node (replication factor 2 or more) no longer re-reads its write-ahead log on each pair's first write.** Applies to the ring layout. After the cluster moved to a new epoch, the first write of every pair started a new epoch at seq 1, and the node's replication lane for each copy read the node's one shared log from that pair's old tail up to the new record: about half the log, once per pair and once per copy. Under load right after a restart the lanes did nothing else, writes waited the whole write-concern timeout and were acknowledged as degraded, and the RF 2 smoke test of 2026-11-19 crawled. The lane now starts a pair's first read of a new epoch near the pair's newest record, and a copy that was behind or away across the move is still sent every record it lacks, in order. Not yet covered: a node that loses a pair to another owner and regains it without restarting. Nothing to configure; no wire message or on-disk format changes. Pre-v1.1 and not versioned: direct, with no alias. Cost per bucket and per store: none (a small in-memory record per replicated pair this node has queued, nothing per bucket); no per-bucket metric; O(1) in buckets; a membership change adds no work beyond the first write of each pair.
- **A replicated ring node (replication factor 2 or more) no longer leaves frames with no ack deadline when a lane pass is slow.** Applies to the ring layout. A lane pass read every pair it was to ship before it sent any, and armed each copy's ack deadline at the read; the retry tick cleared the deadline of a copy with nothing yet in flight, so frames sent after a slow read waited on an acknowledgement with no timer. If that acknowledgement was lost, the copy stayed behind until the pair's next write or a probe of the copy. The deadline now starts when a frame goes in flight, so a lost acknowledgement is recovered one ack timeout after the send. Nothing to configure; no wire message or on-disk format changes. Pre-v1.1 and not versioned: direct, with no alias. Cost per bucket and per store: none (no field added, nothing per bucket); no per-bucket metric; O(1) in buckets; a membership change adds no work.


- **A node that restarts with a slot still replaying its log no longer tells other nodes how far that slot got.** After a hard stop a slot whose replay needs a bucket's declaration finishes that replay in the background, after the node has begun to serve. Clients were already refused meanwhile, but other nodes were answered from the slot's position as the restart left it, which is below writes the node had acknowledged and still holds in its log: a new owner could open the slot below those writes, a copy that held more could be told it had diverged and delete its good data to seed again, a seed could be cut at the low position, and a handoff could report it to the old owner. While the replay is unfinished the node now answers a position query, a catch-up and a seed registration for that slot with an error (`slot <n> of database <id> on node <node> is replaying its log after a restart and gives no position until that has finished. ... Ask again in a few seconds; 'nodes' shows whether the node that holds the bucket's declaration is up`), does not count its own copy in its own handoff, and sends no handoff-ready message for it. The asking node treats that as no answer and asks again, so a handoff, catch-up or seed that needs this node waits for the replay instead of going on without it; that wait is the point of the change. **Such a slot reads a declaration that lives in that same slot from its own store** when the node that now owns the declaration's slot answers that it has no such bucket, or cannot be reached: the replay applies the slot's writes in log order, so its store already holds every earlier change of that declaration. Without it, after a membership change moved a declaration's slot to a new node, the slot's old copy and the new owner waited for each other for good (the old copy asked the new owner for the declaration, which its store lacked; the new owner waited for the old copy's position). Only the slot being replayed: a declaration in another slot this node holds may be older, so a slot that needs one keeps waiting, as does one whose own copy is missing, is being dropped, or is older than the version the write was made under; nothing read this way is kept in the declaration cache, and no other read path changes. Nothing on the wire or on disk changes: the refusal travels as the node channel's existing error answer. Cost per bucket, per store and per slot: none (one lookup in the set of slots still replaying, per answer, and one or two point reads of this node's store per replayed write whose declaration read failed); no metric; O(1) in buckets and on a membership change.

- **Creating a database no longer floods the Raft log, which could make a freshly formed three-node cluster refuse reads and writes with NO_QUORUM or LEASE_LAPSED in its first seconds (fixed in part: such refusals can still happen).** Each node reports which of a new database's slots it holds ready to the Raft leader, which commits them as `SEEDED` commands. A node sent them 64 slots at a time, and every command carries the whole 1,024-byte bitmap, so one database creation put hundreds of entries into the Raft log within a few seconds; with the Raft log on the same disk as the data, each append was slow, the leader spent most of its time storing them, and its quorum rounds overran the bound (three heartbeats), so nodes refused. A node now sends all its pending bits of a database in one `SEEDED` (and one `RETIRE`) per 250 ms flush, sent once 64 bits are pending or the oldest is 2 s old as before, and a bit that is reported waits no longer than it did (a refused command still drops its bits, as before; any other failure, including a proposal that finds no Raft leader within the node's 5-second call limit, keeps them for the next flush). Nothing to configure. No wire or on-disk format change: the command body was already the whole bitmap. Cost per bucket and per store: none; no per-bucket metric; O(1) in buckets; per database, at most two Raft entries of about 1.1 KB per node per flush while that database has pending bits, where it was one per 64 slots (about 43 per node per new database at replication factor 1 on three nodes, 86 at 2); per flush O(1) commands per database, and per membership change O(databases) (a join into a cluster of many databases still lacks a per-node bound), instead of O(slots/64). This does not make the refusals go away. On the three-node test rig, fresh clusters seeding at replication factor 1 were refused less often with the change than without it (suggestive, not conclusive); at replication factor 2 a refusal still happened with the change, and also without it, for a reason not yet found. The quorum bound itself is unchanged, and a slow lease round still makes a node refuse (the bug file lists what stays open).
- **A `create bucket` sent right after `create database` on a cluster no longer fails because the node that stores the bucket's declaration has not caught up yet (first part).** `create database` returns once the node you ran it on has applied it. The `create bucket` that follows writes two small documents at the nodes that store them, and a node that was still a few milliseconds behind did not know the database yet and refused with `bucket "18446744073709551615" not found in database "1"`, ids that name nothing you typed. Every request a node forwards to another node carries the newest cluster state the sender had applied; a receiving node that does not know the request's database yet waits for that state (at most 2 seconds, or until the node shuts down; no deadline of the request crosses to it, so when the lag reaches 2 seconds the sender may give up first) before it judges the request. When it is still behind after the wait and the database is still unknown to it, it answers a retryable `UNAVAILABLE` that names the database id ("database 1 is not known on n2 yet ... run the statement again in a moment") instead of the `NOT_FOUND`. The same wait applies to the forwarded reads, such as the read of a bucket's declaration. A request about a database the node knows, or from a node that is not ahead, is judged at once, as before. Nothing to configure. The check costs one lookup of the database per forwarded request when the node is up to date, and nothing per bucket or per pair; there is no new metric and no new goroutine. No wire or on-disk format changes. The restart hold on a database created after boot is not part of this change.
- **A failover right after a node's activation no longer loses the slot's acknowledged writes in the case seen in testing (the remaining window, fixed in part).** When an activation made a new node the second copy of a slot, the copy it replaced left the slot's copy set still holding every acknowledged write until its deletion was allowed, and the new copy held them only after its catch-up. If the owner failed before that catch-up, the new copy became the owner, asked only the slot's copies where the data was, and opened empty: it answered that an acknowledged document did not exist, and nothing was logged. A node taking over a slot, and a copy catching up, now also ask every member that left the slot's copy set and has not deleted its copy yet (its committed SEEDED bit for the slot is still set), and take its position and its log: the new owner opens only at or above the highest position any such member reports, after catching up from it. A member that does not answer, or answers with an error, holds the slot until it answers. A member that is down holds the slot until it is declared dead: the cluster's committed `dead_ms` setting after it went down, the same value the failure detector uses, 10 minutes by default (no command changes it yet, and the `cluster.dead_ms` configuration key does not set it); the log then says the slot opened without it, naming it, and while it waits the log says which member it waits for and to start it again. A joining member is not waited for: it never counted for a write. Nothing to configure; no wire message or on-disk format changed. Cost per bucket and per store: none; no per-bucket metric. Per slot taken over: one extra position query per member that left its copy set and has not deleted yet (none once deletions finish), and one bit read per member and database; O(members) per promotion, O(1) in buckets. **Still open, by reading:** the rule trusts the SEEDED bit, and a copy whose seed completes while it is in the copy set acknowledges writes about 2 s before its bit commits, and a clear queued before an activation can drop the bit of a copy that still holds the slot. An activation within those 2 s followed by an owner failure before the new copy catches up can still lose acknowledged writes; the only source reachable today is a copy that reseeds after its delete and is ranked back into the copy set.
- **A node added to a cluster is now brought up to date promptly after its activation, instead of staying an empty copy of slots written while it was joining until each slot's next write (fixed in part).** A joining node pulls the slots it will hold, and the owner of a slot sends it nothing after that pull until the node is activated. When the activation made the node a copy of a slot whose owner did not change, nothing sent it the writes made between its pull and its activation: the copy stayed at the position its pull reached (an empty slot when the database was empty then) until the slot's next write, and the copy it replaced left the slot's copy set. If the owner then failed, the new copy became the owner and served the slot without those writes: in a three-node test with two copies, six acknowledged writes to a document, the activation of a fourth node, then the owner stopped, the new owner answered that the document did not exist. The owner now sends the newest record of every slot it owns to its copies when a member becomes active, as it already did for a member back from down; a copy that is behind refuses it and catches up from the owner. In the same test the new copy holds every write 0.3 s after the activation in most runs and within 13 s in all twenty. **Left open by this change, and closed by the entry above:** a failover in the window between the activation and the new copy's catch-up still lost the acknowledged writes (graded S1). The window had no fixed size (it included the replication backoff, up to 30 s per retry), and it could be hit with no fault injected: in a test that stopped the owner right after the activation, every acknowledged write of the slot was lost in 1 of 3 runs. The cause was that a failover consulted only the slot's copies, never the retired copy that still held the data. Nothing to configure; no wire message or on-disk format changed. Cost per bucket and per store: none; no per-bucket metric. A membership change that makes a member active costs one in-memory pass over the slots the node owns and one replicated frame per written slot per copy, as a member back from down already did: O(slots held), O(1) in buckets.
- **A copy whose seed was restarted while it was asking its source to go on no longer ends up serving documents the cluster has deleted.** A node that seeds a slot asks another copy to register the pull, and after an interrupted transfer asks again to continue it. If the pull was replaced by a new one while that request was on its way (a membership or replication factor change that takes the node out of the slot's copy set and back, a failover, `seeding retry`), the answer to the old request was applied to the new pull. The new pull then skipped the step that empties the slot's data on the node before a pull starts copying (after an interrupted transfer, that data is the part the old pull had copied), and if the transfer was interrupted once more before it made progress, it started again from a newer state of the source on top of the old data. The slot turned `ready` still holding documents that had been deleted on the source in between; reads answered by that copy returned them, and nothing repaired the copy afterwards. The node now checks that an answer belongs to the pull the slot is running before it uses it, and otherwise starts the new pull from the beginning, with the delete first. Nothing to configure. A copy that already finished seeding this way is not repaired, and the server cannot detect one today: its record looks like that of any correctly seeded copy, and no command compares a copy with its source. No wire or on-disk format change. Cost per bucket, per store and per slot: none; no per-bucket metric; O(1) in buckets; a membership change is O(1).
- **A copy that is seeding a slot no longer deletes the slot's data when the slot's record is already gone as the source's answer arrives.** A node that seeds a slot asks another copy to register the pull and, once the copy accepts, first empties the slot's data on the node and then starts asking for chunks. If the slot's seeding record had been removed while that request was on its way, the answer was still treated as a registered pull: the node emptied the slot's range from what it had read before the request, deleting anything written there in the meantime, and asked the source to start sending chunks for a slot it held no record of. Nothing on this build can remove the record that way (only the slot's own pull run takes a record out of seeding, there is one run per slot and one puller per node), so no cluster is known to have been affected; the fix is a guard for a later change that breaks that. The node now refuses an answer for a pull when the slot holds no seeding record, looks at the record again immediately before the delete and deletes nothing unless the slot still holds the same pull in its first phase. The check and the delete are not one step (the delete runs for as long as the range is large and cannot hold the slot's lock), so the guard narrows the window and the rule above closes it. Nothing to configure. No wire or on-disk format change. Cost per bucket, per store and per slot: none; no per-bucket metric; O(1) in buckets; a membership change is O(1).
- **A write that was saved but could not be applied on the node that orders it now says so instead of reporting success, also when another node forwarded it.** The write is durable and the node replays it, but an insert, `create bucket`, `alter bucket` or `drop bucket` read only "applied" and dropped the failure, and any write that reached the owner through another node (an insert, update, delete or store write sent to a non-owner) came back applied too, an update or delete that asked for the document back with an empty one. Each now answers UNAVAILABLE, says the write is durable, and says what to do: retry with the same request id (the retry is answered from the saved write) or run the same bucket command again, which is safe. No wire or on-disk byte changes.

- **A store sync no longer writes the newest memtable log to disk ahead of the rotated ones, an order in which a power cut kept a later write and lost an earlier one (partly fixed).** The node store keeps recent writes in memtable logs and makes them durable with a sync. When a memtable fills it is rotated out, and its log stays the only copy of its writes until it is flushed. The sync wrote the newest log to disk first and the rotated ones after it, so a power loss between the two left the store with a later write and without an earlier one. A cluster node's start-up replay skips a logged write at or below the position the store says it has applied, so an acknowledged write lost that way was not applied again and read as not found after the restart. The sync now writes the rotated logs first, oldest first, and the newest last, so what the sync has forced to disk is always an unbroken run of the writes in the order they were made; if writing one log fails, that sync writes no later log and returns the error, and the store refuses writes until a restart, as it already did after a failed sync. That stop covers the one sync: after a disk write error the later logs can still reach the disk on their own, and a restart after such an error may come back without the earlier write (not tested with a real disk error). **Still open:** the operating system writes memory to disk on its own schedule too, and can write the newer log before an older one whether or not a sync is running; a power loss right then can leave the same gap. How often that happens is not measured. Also still open: a document of 1 MiB or more is kept in a separate value file that the sync writes last, and a power loss after the logs are written and before that file is can leave the document's entry on disk without its content; reading it from the store then returns an empty value and no error (seen in a store-level test; what a cluster node returns for it is not yet tested). A process crash or a `kill` on a healthy disk loses nothing either way: both logs are replayed from memory the kernel still holds. Nothing changes on disk or on the wire, there is no setting, and a sync costs what it did (the same writes in another order). Cost per bucket and per store: none; no per-bucket metric; O(1) in buckets and on a membership change. The reasoning is in the sync-order patch decision record in the hoardDB repository.

- **`seeding forget` on a retired slot no longer lets the node serve that slot's old data when it is named a copy of the slot again.** `seeding forget` removes the node's record of a retired slot and keeps the slot's data on disk. A node treats a slot it has no record of as new and empty, so when a later change (a replication factor raised again, a membership change) named the node a copy of the slot once more and no other copy had reported the slot seeded, the node accepted writes over the kept data, or marked the slot ready as it stood: a ready copy made of the retired copy's old documents, with documents the cluster had deleted since and without the ones it had written. The same happened at once when the slot was forgotten while the node was still in its copy set. The node now looks at the slot's key range before it creates a record over it. While the range holds data it accepts no write and no replicated record for the slot (the refusal says so and names `status seeding`), and its next seeding pass, which `seeding forget` now starts by itself, queues the slot for a seed. The kept data is deleted only once another copy has accepted that pull, and the slot is then seeded from it. While no copy can be pulled from (replication factor 1, or every other copy is itself not ready), the data stays on disk, the slot stays `seeding`, and its `status seeding` row gives the reason and says the data is kept; the slot recovers by itself when a copy appears. The kept data is deleted in one other case: the node leaves the slot's copy set again before any copy accepted, and drops the slot as it drops any slot it was still seeding. **This order now holds for every seed:** a slot's range is reset after a source accepts the registration, not before it is asked, so a reseed with nothing to pull from no longer empties the range first. The reply and the help of `seeding forget` say this instead of "its range is kept and the data goes on the next seed". No setting, wire message or on-disk format changed. Cost per bucket and per store: none; one key-range seek when a slot's record is created, one per slot a pass finds with no record (a second, on a fresh snapshot, when the first finds data) and one per retry of a pull that waits for a source; no per-bucket metric; O(1) in buckets and on a membership change beyond the pass that already runs.

- **A node that leaves a slot's copy set while it is still seeding the slot, and is named a copy again while it deletes what it had pulled, no longer ends with a ready copy that is missing documents.** Lowering a database's replication factor, or a membership change, can take a node out of a slot's copy set while the node is still pulling that slot. The node then drops the part it had pulled. It used to remove the slot's seed record first and delete the data afterwards, at `storage.reclaim_keys_per_second` (10,000 keys per second by default), and a node treats a slot it has no record of as new and empty, so it accepts writes for it. If the replication factor was raised again while the delete was still running, the node applied replicated writes into the range that was being deleted, marked the slot ready, and the delete then removed documents the cluster had acknowledged; a restart in the middle of the delete left the same half-deleted range with no record. The node now keeps the slot `seeding` until its range is empty: the record is first rewritten as an attempt that has not started (so nothing can resume it and nothing is applied under it), the range is deleted, and the record is removed last. While that runs the slot accepts no write and no replicated record on that node, and `status seeding` keeps listing it until the delete is done: as `placing` while the delete waits for a seed worker, then with the reason "this node is no longer in the pair's copy set: the part of the seed it had pulled is being deleted". The state word beside that reason is `seeding`, and turns `stalled` once `replication.seed_idle_seconds` have passed since the slot's last pulled chunk, which for a slot that waited or for a large delete is the usual case; the reason, not the word, says the delete is running, and `seeding retry` is not needed for it. After a restart the node either seeds the slot again from an empty range or finishes the delete. The delete also moved from the node's seeding driver to one of its seed workers (`replication.seed_slots_in_flight`), so dropping a large partial seed no longer holds up every other seeding decision on the node; the delete takes its turn in the same queue as the slots the node is still seeding and has no priority there. No setting, wire message or on-disk format changed.

- **A copy that is promoted to owner while it is still catching up no longer resets the data it has just started serving.** A copy that fell more than one catch-up chunk behind (1,024 records or 4 MiB) fetches the records it missed in the background, for up to 30 seconds. If the owner was marked down in that time and the copy became the owner, the background catch-up kept asking its old source; once the new owner had taken a write, the source answered that the new owner held records it had never ordered, and the new owner put its own data for that slot back to `seeding` and rebuilt it from another copy while clients were writing to it. On a three-node test cluster with replication factor 3 the first form of the test failed intermittently before the fix; the slot was briefly unavailable and no acknowledged write was lost in those runs. A catch-up that began on a copy now stops as soon as the node owns the data, and only one catch-up runs for a slot at a time. Nothing to configure.

- **A restarted node's hold now follows the `step_down_ms` the cluster committed, also when no membership change followed it.** After `set cluster step_down_ms` the restart hold kept the value of the last membership change (2.1 seconds at the defaults), so with `step_down_ms` raised to 12000 a restarted node opened its pairs after about 2.2 seconds instead of 12.1. The hold is now `step_down_ms` plus `heartbeat_ms` from the process start for the committed values: raising `step_down_ms` lengthens it at once, and lowering it does not shorten it until the next membership change. Until a restarted node has applied its own log and its ownership check has seen the applied state, it does not know yet which pairs it owns: a read, a write or a peer-forwarded request to one of its pairs in that window is refused at once with a retryable error that says so (it is not parked and does not wait), including a read another node forwarded to it; retry in a few seconds, and if it persists check 'nodes'. That window ends within moments of the log being applied, and after it the hold above applies. No setting, wire byte or on-disk byte changes. Cost per bucket: none.

- **A database created while a node is still reconciling no longer has pairs the node refuses writes to for tens of seconds.** On a cluster with a disk that syncs in milliseconds, the first inserts into a database created right after the cluster formed could be refused with "slot N of database D is retired (out of the copy set) on this node, so it orders no write" until something else changed the cluster state. A node's reconcile pass takes the committed state when it starts and runs for a while; a database committed after that was not in it, and the pass retired the record a live write had just created for a pair of the new database, although the cluster state kept the pair on that node. The pass now leaves such a pair for the pass over the newer state. No wire or on-disk change.

- **A node that restarts no longer serves a pair it owns before it is safe to.** A restarted node used to order and answer for the pairs it owned at once, even when it had become their owner while it was down or just before it stopped and the old owner might still be serving under its lease, so a read could miss an acknowledged write and two nodes could order the same pair. Now, after a restart, each pair the node owns is unreadable and unwritable for up to `step_down_ms` plus `heartbeat_ms` (2.1 seconds at the defaults), counted from the process start, and until the node has asked its copies for their positions and caught up. A copy that is silent but not yet committed down keeps the pair unreadable until it answers or is committed down. A node that owns most of a database's slots opens them over a few seconds, 256 pairs at a time . A read or write refused in this window says the node restarted and to retry in a few seconds. A read in that window waits, then is refused with a message that says to retry in a few seconds. A first boot, a single-member cluster and a node that was never restarted are not delayed. No setting, wire byte or on-disk byte changes.

- **A node killed during its first memtable flush now starts again by itself.** It used to refuse with "MANIFEST names no table ... restore this node from a backup" although every write was still in the memtable log. The half-written first table is now kept beside its old name as `000001.sst.first-flush-crash`, the log is replayed, and the server logs a warning saying so. Other states with a manifest that names no table are still refused, and the message now names the table files.

### Changed

- **Removed: the start-up join of the hash ring.** After the listeners opened, the server used to dial each address in `hash_ring.seed_nodes` once ("attempting to join cluster via seed", then "successfully joined cluster via seed") to check that both sides agreed on membership; `Server.JoinSeedNode` and its caller in `cmd/server` are deleted and those log lines no longer appear. Nothing replaces the check at start: the earlier ring still confirms membership in its own background exchange, which is why `HEALTH_CHECK` still answers `membership: confirmed`. `hash_ring.seed_nodes` is now what `cluster.enabled` reads to find the entry-ring cluster. Nothing to change in your configuration.

- **`storage.wal_preallocate` now defaults to `auto` (it was `off`).** On ext2, ext3 and ext4 the write-ahead log zero-fills each segment before it uses it, so a flush writes data only. Serial inserts on the entry-ring node are faster, because the node no longer pays a file-extending `fsync` per insert. The key is shared by the node-to-node ring's log and the entry-ring node's log, and the entry-ring node honours it. The cost is one 64 MiB segment kept zero-filled ahead of the active one, 128 MiB of disk per node while idle, and nothing per bucket. On other filesystems (XFS, btrfs, ZFS, tmpfs, overlayfs) `auto` resolves to off, as before. To opt out, set `HOARDDB_WAL_PREALLOCATE=off` (or `storage.wal_preallocate: "off"`). Start-up log lines: a node without a node-to-node ring log says the key has no effect only when you set the key, and the entry-ring node's line begins `entry-ring:`. Tested with `kill -9` on the server process; a host crash or power loss with the new default is not tested yet.

- **A data directory from before spread collections is refused at start.** A hash bucket recorded with no `placement`,
or `spread` with no `parts`, stops the server with `this data directory holds buckets from before spread collections;
start with a new data directory`. Nothing is converted: dump what you need with a build that still reads it, or start
fresh. A `placement: whole` bucket and a spread bucket made by this build start as before. This replaces the one-build
rule that read a hash bucket with no placement as `whole`. A spread bucket's catalog entry gains `parts` (256), and the
parts a later step creates carry `part` and `part_of`.

- **A bucket name may not hold the control character `U+001F`, and a frame that names one is answered "not found".**
`U+001F` was already refused in a created name as a control character; it is now also reserved for the parts of a
spread collection, which the server names itself. A request that names such a bucket, or a create that sets `part` or
`part_of`, is refused before it is routed, with the name shown without the part suffix. Parts never appear in
`show buckets`, in the bucket count of `show databases`, or in `hoarddb_node_buckets`.

- **On a bucket recorded `spread`, `unique` on a field other than `_id_` is refused**, with the `placement: whole` create
that works built from your declaration. Create the bucket `{ placement: whole }` to declare a unique field. `alter bucket
{ placement: ... }` is refused naming the recreate (dump, drop, create, restore), and a `create` that asks an existing
bucket for another placement says it was not changed. `restore --drop` checks the create it is about to issue before it drops the live bucket, so a restore the
unique-on-spread rule refuses leaves the existing bucket and its data in place.

- **Dump manifests record a hash bucket's placement, and restore uses it.** `metadata.json` and the dump manifest gain an
additive `placement` field (`whole` or `spread`; the format version stays 3), and `restore` creates the bucket with it, so
a whole bucket with a unique index comes back whole. A dump written before this has none and restores as `spread`.
`help placement` explains the choice.

- **On a bucket recorded `spread`, an update matching a filter is refused; update by `_id_`, or re-create the bucket
`whole`.** A hash bucket is `spread` unless you ask for `placement: whole`, and a request that updates by filter
(`multi`, or a `filter` with no key) on one answers `INVALID_ARGUMENT`, `write_outcome: not_applied`, with the commands
that fix it: `db.User.update("ada", {$set: {City: "London"}})`, find then update each id, or `create bucket User in app {
type: hash, placement: whole }`. A keyed update on a spread bucket, and any update on a whole bucket, are unchanged. The
`hoarddb` shell never sent a filtered update; a driver that sets `multi` meets the refusal on a spread bucket. Nothing is
logged, and a node that holds no copy of the bucket forwards the update and the owner refuses it. No cost per bucket.
- **The refusal of a payload that is not well-formed BSON names the byte where reading stopped.** The words
in parentheses are now the BSON library's: "The document is not well-formed BSON (bson: malformed document
at byte 0: too few bytes for a document's length prefix). Send it with a hoardDB driver, or encode it as one
BSON document." Before, they read "(malformed BSON: 3 bytes do not hold a document's length prefix)". The
error code (`INVALID_ARGUMENT`), the opening and the closing advice are unchanged, as is the refusal of a
document nested too deep. The driver vector "a request payload that is not BSON" carries the new message, so
each driver's copy of the vectors needs re-syncing. Match on the code, not the words.
- **A `search` with an equality on a code-with-scope value now matches what `find` matches.** The search
fallback that no index serves compared such a value with the documents of its scope decoded differently
from `find`, so the two could answer differently for the same filter (32 answers apart in the test
comparison). Both now read the stored value the same way.
- **A stored document that is framed correctly but holds bytes that are not BSON inside no longer fails a
`find`, `count` or `distinct`.** A field that cannot be read in such a document counts as absent, as a
field the document does not have; before, the whole query failed with a decode error.
- **A request carrying a malformed value, such as a boolean byte other than 0 or 1, is refused wherever
it enters the server** (from a client, a peer, a restore or the write-ahead log replay), as
`INVALID_ARGUMENT` "is not well-formed BSON", even under a key the request does not use. Before, a value
the request did not read could pass.
- **An update whose result would nest deeper than 200 levels is refused with the nesting-depth message**,
as a result deeper than 100 levels already was. Before, it was answered `INTERNAL`.
- **A multi-document update and a `distinct` decode only what they use**: the documents an update changes,
and the one field `distinct` reads. Neither decodes every document it walks any more, so a document with
unreadable bytes outside the field no longer fails them.
- **Indexes read only the fields they name.** A write, delete or TTL expiry in a hash bucket with
indexes, an index rebuild at startup, and the refusal of a conflicted unique index now read a stored
document's indexed fields instead of decoding all of it. A stored document with unreadable bytes outside
those fields can now be overwritten, deleted, expired and indexed, with its index entries kept exact;
before, each of those failed with a decode error, and a rebuild left the index unusable. A document
whose indexed field itself cannot be read is still refused, and its entries are left as they were. The
refusal of a conflicted unique index names the shared value in that case too.
- **A bulk insert that repeats an `_id_` is refused before it is stored; a bulk upsert keeps the last occurrence.
Bulk answers carry per-item `results`.** `BATCH_PUT` naming one `_id_` twice is refused whole with the new
`DOCUMENT_EXISTS` code (`0x12`), `write_outcome: not_applied`, nothing logged or replicated; the message names
the `_id_` and its positions and `results` marks the repeated position. It used to keep the last document
silently. `BATCH_UPSERT` still keeps the last one. `BATCH_PUT_OK` and `BATCH_UPSERT_OK` gain `results`: one
`{id, write_outcome, code?, message?}` per request position. Send a batch that meant last-wins as `BATCH_UPSERT`.
- **`insert` with an existing `_id_` is refused (`DOCUMENT_EXISTS`, `0x12`); it used to replace silently. Retry an
ambiguous insert with `upsert`.** A `PUT` (the CLI's `insert`) of an `_id_` the hash or btree bucket already holds
answers an `ERROR` frame `DOCUMENT_EXISTS`, `write_outcome: not_applied`, and the stored document is unchanged. The
message is `Document "<id>" already exists in bucket "<bucket>" (database "<db>"). To replace it, run
db.<bucket>.upsert({_id_: "<id>", ...}); to change some fields, run db.<bucket>.update("<id>", {$set: {...}}). If you
are retrying an insert whose answer you did not get, it may have been stored: retry with upsert.` A program that
relied on `insert` replacing sends `UPSERT` (`BATCH_UPSERT` for a batch) instead. The owner decides: two inserts of
one `_id_` through different nodes give one success and one refusal, and replicas and a restart's replay keep the
owner's answer. A replica that held an `_id_` its owner did not logs one ERROR and counts it in
`hoarddb_replica_divergences_total`.
- **`BTREE_INSERT` refuses an existing key**, as `PUT` does.
- **`push` on hash and btree is an insert**: an `_id_` the bucket holds is refused with `DOCUMENT_EXISTS`.
- **A bulk insert with any existing `_id_` is refused whole.** `BATCH_PUT` stores nothing when any of its `_id_`s is
held; the `ERROR` frame's `results` gives each held position `DOCUMENT_EXISTS` and every other position
`not_applied` ("not applied: another document in this batch already exists").
- **A `PUT` on a blob bucket is refused; `put` stays the replace verb.** A blob bucket is written with `BLOB_WRITE`
(the CLI's `db.<blob>.put(...)`), which replaces as before. A document `PUT` or `BTREE_INSERT` into a blob bucket is
refused `INVALID_ARGUMENT` ("a blob bucket is written with put"), and a blob bucket's wrong-type refusals no longer
list `put` and `insert` among the verbs it takes. The driver vector "put on a blob bucket stores the document as the
payload" is replaced by "put on a blob bucket is refused".

- **Performance: writes and reads on a node no longer queue behind another write's log sync.** Every
acknowledged write at default durability ends in a sync of the store's in-memory table log, and the store held
its internal lock for that sync's whole run, so a write that needed a new table, and every read
queued behind it, waited for it. The sync now runs after the lock is released; a write still waits for its own
sync, as before. At default durability, batched and unbatched writes both get faster and write tail latency drops; with the relaxed acknowledgement, unchanged. Durability is unchanged: everything committed before a
sync is still covered by it, verified by tests that check the log pages are written back and recovered. Nothing
to configure.
- **A node that does not own a bucket now forwards up to 96 client requests at once to each owner, up from 24.**
A write sent to a non-owner (behind a load balancer, or with clients not routed to owners) is forwarded and holds
one of the forwarding node's internode request slots to the owner until the owner has logged, synced and
replicated it. There were 24 such slots per owner, so with more than 24 concurrent writers on one node the rest
waited on that node rather than reaching the owner. Each of the three request connections per peer now
carries 32 requests in flight instead of 8; the replication and membership connection keeps 8, and the internode
listener reads ahead 32 frames per connection (was 8) within the same 32 MiB per connection. Nothing to configure,
and no connection, file descriptor or metric is added; `hoarddb_internode_in_flight{lane="request"}` can now read
up to 96 per peer. Routing clients to owners still avoids the forward altogether.
- **Nodes with a write-ahead log write it in segment format 2, and a hoardDB that does not know format 2 refuses to start on it.** A
node with a replication factor above 1 or seed nodes starts a new log segment in format 2 at its first start on
this build, whatever `storage.wal_preallocate` says, and at every start after that. Segments in the old format
are still read and are removed by retention as before. A build that does not know format 2, started on such a data directory,
stops with `parsing sealed file header: flags must be 0x00` and changes nothing. To go back, restore the copy of
the data directory taken before the upgrade (see "Going back is not supported" in
[Compatibility](docs/compatibility.md)). The log no longer keeps `wal/state.bson`.
- **A member one epoch behind no longer refuses direct writes `CONFLICT` because a node it removed gossiped a newer
membership.** After a removal followed by a join, the removed node (still running) reports the newer epoch to a
member that has adopted the removal but not the join. The member took that for a divergence and, until it
resynced from a real member, answered a write sent straight to it `CONFLICT`, which a driver never retries. It now notes the view and goes on; a node in neither its member set nor its removed history still
raises the divergence refusal, and a real divergence still says `CONFLICT`.
- **A request sent on to a bucket's owner whose membership disagrees with a peer's is refused `UNAVAILABLE`,
not `INTERNAL`.** The refusal clears by itself once the nodes exchange their membership, but the node the client
reached answered `INTERNAL` ("owner ... refused GET for bucket ...: cluster membership disagrees with a peer"),
which a driver never retries, and a refused write said `write_outcome: unknown` although nothing was applied. It is now `UNAVAILABLE` with `write_outcome: not_applied` on a write, and the message says to retry
and, if it persists, to run `nodes history` against every member. A driver that retries `UNAVAILABLE` retries it.
Also, a node started to join no longer enters this disagreement when a node the cluster removed, still running,
gossips a newer membership to it, so it no longer refuses those requests in the first place.

- **`describe bucket`, `drop bucket` and `alter bucket` now read an `in <database>` clause.**
`describe bucket Post in app` (the line the unknown-store-field error prints) used to ignore the clause and run
against the `use` database, or send no database at all; `drop bucket X in app` could drop `X` in another database.
The clause now names the database and wins over `use`. A statement with no `in` clause and no `use` refuses before
sending anything and names both fixes; a stray word after the bucket name of `describe` or `drop` is refused with a
usage line instead of being ignored. `show buckets in` and `create bucket ... in` read the clause the same way, and
a dangling `in` with no database is now refused there too. Nothing to change if you always ran `use` first.

- **A burst of correct logins from one address is no longer refused as "too many failed logins".**
Logins still being checked hold places under `HOARDDB_TCP_AUTH_MAX_FAILS` (default 20), so the 21st
concurrent login from one address was refused `RATE_LIMITED` with a text claiming failed logins, although
none had failed. Such a login now waits up to 5 seconds for one in flight to settle and is let in;
at most `HOARDDB_TCP_AUTH_MAX_FAILS` logins per address are still checked at once, so parallel guessing is
bounded as before. Up to four times that many may wait; a login past that, or whose wait runs out, is refused
`RATE_LIMITED` with "too many logins from this address are being checked at once", which names the setting,
counts as no failure. The audit log records it under `auth.failure` as "login refused: N being checked, M waiting;
no password was checked", not as consecutive failures. Reconnect and retry, or raise `HOARDDB_TCP_AUTH_MAX_FAILS`.
See [Limits](docs/limits.md).

- **A read of a bucket that has just moved no longer answers "not found".** After a `join` or a
`remove-node`, while the membership change settles, a read at the bucket's new owner that has not
copied the bucket yet waits up to one second for it and is answered from the copy; past that it goes
to the bucket's previous owner, or answers "unavailable, retry". A node that took buckets over from a
removed node does not report the change settled until that node has handed them over, or has not
answered for 3 seconds. A read of a bucket whose only copy was on such a node is then "not found",
naming the node. A restore in that window waits the same way and never creates the bucket beside the
cluster's copy.
- **A write of a bucket whose only copy is on a node being removed is retried, not "not found".**
While a node removed from the cluster is still handing its buckets over, a write to one of them (or to a
name that would have been on it) at the node that takes it over answers "unavailable, retry", nothing
stored, naming the removed node. Once the hand-off is done, or the node has not answered for 3 seconds,
a write of a name that is still not there is "not found", naming the node. Before, such a write was
"not found" with the advice to create the bucket, or "unavailable" for ever without naming a node.
- **Two membership changes in a row no longer make a bucket that is still being handed over read "not found".** At replication factor 1, if a `join` or a second `remove-node` moved a bucket again before the
removed node's copy had landed, its new owner answered reads and writes "not found ... create it" for up to
30 seconds. It now answers "unavailable, retry", naming the removed node, until the copy lands. A rolling
replace (join, then remove) therefore shows retries for longer: the members wait for the removed node to
hand its buckets over again at the second change, for as long as it runs and has not done so. Stopping the
removed node ends the wait within 3 seconds, as before.
- **A member restarted during a removal's hand-off settles again.** A member restarted after
the removed node said it had handed everything over, and before that member reported the change settled,
used to wait for the removed node until the removed node was stopped, answering "unavailable, retry" for
names it had owned. The removed node now tells such a member again when the member checks on it.


- **A sealed `cluster.token` read without a keystore now names a route this start can still take.**
  The refusal already names the encryption settings that open the file; it now also says to move the token
  aside and start with `HOARDDB_CLUSTER_TOKEN` set to the token `hoarddb get-token` prints on another
  member. The other credential files keep their existing advice, which has no such alternative.
- **A username that holds a control character or is over 255 bytes is refused when you create it.**
  `create user` and the first-start root user answer, for example, `Username "a\x1b[2Jb" contains a
  control character (U+001B at byte 1), so it was refused. ... use printable characters only: create user
  <name> roles [reader@<database>]`. Accounts made before this rule keep working and can still be altered
  and dropped.
- **The example Docker Compose cluster no longer publishes the internode ports (`4434`, `4436`, `4438`).**
  The nodes reach each other on the compose network. Only the client ports are published, on `127.0.0.1`.
  A node outside the compose project needs the port published on an address you choose.
- **The example cluster hands the root password to the nodes as a file, and each node gets only its own
  private key.** The password is no longer an environment variable inside the containers (it was visible in
  `docker inspect`); set `HOARDDB_ROOT_PASSWORD` on the host as before. The shared `pki` volume is replaced by
  `pki-node1`, `pki-node2` and `pki-node3`, so after upgrading a stack, `docker compose down -v` and start
  again.
- **The container image moved to a supported Alpine (3.24), and every base image is pinned by digest.** The
  image still runs as uid 1000 and still needs a root password at first start. The Dockerfile header records
  the choice and how to bump the digests.
- **The comparison-benchmark compose files for other databases publish their ports on `127.0.0.1` only.**

- **A database, bucket or field name that holds a control character is refused when you create it.**
  `create database`, `create bucket` (including typed fields at every level and store fields with
  their nested ones), `alter bucket ... { Name xStore }` and `ingest --into` now answer, for
  example, `Bucket name "a\x1b[2Jb" in database "app" contains a control character (U+001B at byte
  1), so it was refused. A terminal acts on control characters when it prints a name, so use
  printable characters only: create bucket <name> in "app" { type: hash }`. Control characters are
  the C0 and C1 controls and DEL. A name made before this change stays listed, readable, writable and
  droppable, and a replay or a restart does not refuse it. `restore` goes through the same create, so
  a dump that holds such a name is refused for that bucket, before anything is changed, with the
  `hoarddb ingest <dump dir>/<database>/<bucket>.bson --into <database>.<new name>` that keeps its
  documents.

- **`show buckets`'s replication column prints the owner's counts.** On a cluster a row
  reads `rf 3 (3/3)`, `rf 3 (1/3, copying to node-b (4h))`, `rf 3 (2/3, node-c not in
  sync)`, `rf 3 (2/3, under-replicated: 2 members)` or `rf 3 (standalone: 1 copy)` where
  it read `rf 3`; a bucket on the node's own factor keeps `inherited` first inside the
  parenthesis (`rf 3 (inherited, 2/3, ...)`). A node that is not the bucket's owner prints
  `rf 3 (owner elsewhere)`, and a copy kept outside the set reads `rf 3 (leaving)` there,
  where it used to read `rf 3, leaving`. The counts come from the owner's memory, so the
  new fields cost no request. A standalone node at factor 1 prints its old column (`rf 1`,
  or `rf 1 (inherited)`), and a server that predates the fields prints the old `rf 3`,
  `rf 1 (inherited)` and `rf 3, leaving`. A script that read the old column should match
  the parenthesis, or use the `in_sync` and `state` fields.
- **Every driver reports an empty list and a single-value miss the same way (driver-visible).** A verb that
returns a list (`pop`, `peek`, `claim` and their store-field forms) answers an empty list and no error when
nothing matched; a verb that returns one value (`get` of a missing key, btree `first`/`last` on an empty tree,
a field `get` of a missing element) is a typed not-found error (Go `ErrNotFound`, and the Go driver no longer has an
`ErrEmpty`; Node a thrown `NotFoundError`, never `null`). Before, Go returned `ErrEmpty` for
an empty `Pop`/`Peek`/`Claim` and Node returned `null` for a miss. The wire and the driver vectors are unchanged.
- **A create of an existing bucket at another replication factor names the command that changes it.**
  The reply still says the bucket's factor was not changed, but its advice is now `To use another factor,
  run: alter bucket <name> { replication_factor: N }`. It used to tell the caller to dump the bucket and
  restore it into a new one, which stopped being necessary when `alter bucket` changed a factor in place.

- **A verb a store type does not answer is one refusal: an `ERROR` frame, `INVALID_ARGUMENT`.**
  `pop`, `peek`, `get` and `delete` on a type that lacks them, `push` and `insert many` on a blob, `claim`,
  `ack`, `release`, `extend`, `dead`, `requeue` and `discard` on a hash, btree or blob, `find` on a blob and
  `consistent get` on anything but a hash or btree used to answer in band (`found: false` or `success:
  false` with a message), while `range`, `update`, `exists`, blob read and write and the like were `ERROR`
  frames. All of these are `ERROR` frames now, a refused write carries `write_outcome: not_applied`,
  and every message ends with the verbs the type takes ("A hash bucket takes: put, insert, ..."). A driver that read the in-band
  `found`/`success` of these verbs as a refusal must read the `ERROR` frame instead; the CLI prints
  them as `pop failed: server error (4): …` and classifies them `INVALID_ARGUMENT` (exit 2, unchanged)
  rather than `REFUSED`. The driver vectors moved with it: four rows changed shape, five gained the
  message tail, three are new. A `consistent get` of a wrong-type bucket that a cluster coordinates from a
  node outside the bucket's replica set is the same refusal (a holder's refusal now reaches the
  coordinator as one, in a new `wrong_type` field of the node-to-node versioned read).

- **A `consistent get` of a bucket or database that does not exist is an `ERROR` frame, `NOT_FOUND`,
  like `get`.** It answered in band, `found: false` with the "bucket not found ... Run
  'show buckets ...'" message, which a driver that tests `found` reads as a missing key. The message is
  unchanged; a driver reads the `ERROR` frame. One driver-vector row is new
  (`consistent get of a bucket that does not exist`).

- **A cluster `consistent get` of a bucket or database that does not exist is `NOT_FOUND`, at every node.** It answered code 6, "its owner ... did not answer, and the other copies do not agree; retry when ...
  is reachable", though the owner had answered that it has no such bucket. It is the refusal `get` gives, with the
  "Run 'show buckets in ...'" advice, when every holder says not found; holders that disagree still get the
  comparison. A node-to-node field, `not_found`, is new and additive.

- **A batch over the store's atomic bound is now an error with its own code, `BATCH_TOO_LARGE`
  (`0x11`).** It was an ordinary `BATCH_PUT_OK` with `success: false`, no code and
  no `write_outcome`, which a driver could only recognise by the phrase `none of it was written`.
  It is now an `ERROR` frame with code `BATCH_TOO_LARGE` and, on a standalone node, `write_outcome:
  not_applied` (nothing was written), for a batch over the byte bound or the entry-count bound, on
  every bucket type that takes a batch. A cluster owner's refusal says `unknown`, because the bound
  follows each member's memtable and a member with a larger one can apply the entry the owner
  refused: a driver reads the outcome as well as the code before it resends a queue or a batch whose
  ids the server mints, and the handbook says which of those are read back first. The message also
  says how many documents and how many encoded bytes the batch was. A driver or script that read
  `success: false` on `BATCH_PUT_OK` to halve a batch must switch to the code; `hoarddb ingest`
  already has, and reads the `unknown` outcome as well. The limits page, `errors.md`, `wire.md`
  and the driver vectors say the same.

- **The driver handbook's remaining gaps are filled, each sentence read against the
  server symbol it describes.** A write concern is accepted on every data mutation, including
  `PutBlob` and the lease and dead writes (`claim`, `ack`, `release`, `extend`, `requeue`, `discard`),
  and a standalone node reads the word and refuses one it does not know, exactly as a cluster does; a
  read ignores the word but still refuses an unknown one, with no `write_outcome`. `len` is a count
  only on `FIELD_LEN`; the other field verbs that carry it send 0, and 0 there does not mean the field
  is empty (`FIELD_LIST` has no `len`). The blob value is `StoredBlob` in Go, because `Blob` is the
  store-type constant. `UNAVAILABLE` is retried with backoff only when the answer says `write_outcome:
  not_applied` or the request is a read (an `INVALID_ARGUMENT` that says `not_applied` is refused for
  good), and `PoolStats.Retries` counts those retries. A driver's own deadline or cancellation is
  `CodeInterrupted`, a driver-local code from `0x100`, and that error also matches
  `context.DeadlineExceeded` or `context.Canceled`. `retained_bytes` is absent
  when 0, so a driver reads absence as 0. A founder of a new cluster waiting on its seed list gets the
  `waiting for founders` refusal, not the `not been added` one. No server code, message or vector row
  changed; the handbook now states these rules.

- **Three refusal and reply texts changed.** A
  driver that matches these strings byte for byte must update. The ledger lockout
  refusal (`RATE_LIMITED`) now says "The server is closing this connection; wait
  out the window, then reconnect and retry with the correct password." Creating a
  bucket in a database that does not exist yet still creates both, and the reply
  now reads `created database "nodb" and bucket "b" (type: hash)`; a create in an
  existing database keeps `bucket "b" created (type: hash)`. The oversize-request
  refusal names `'hoarddb ingest'` (it read `'hoardDB ingest'`) and "the limits page
  of the user docs" instead of a docs path. The driver vectors carry the new text.

- **`pop` returns expired leases before it takes.** A pop on a queue or heap
whose front entries sit under an expired lease writes those entries back first,
then takes in pop order, in the same apply. Before, they could be stranded
behind the head until another return step.
- **A heap push that overwrites a key drops that key's return count.** Reusing a
heap element's `_id_` while it is under a lease no longer leaves a stale count
that a later return would deliver against.
- **Dumps are format 3 and carry a bucket's dead entries.** A queue's or heap's
dead entries go to `<bucket>.dead.bson`, with `dead`, `dead_bytes` and
`dead_digest` in `metadata.json`; a restore writes them back dead, not as live
documents. Format 2 dumps still restore, by the server and the CLI. A dump of a
bucket with dead entries needs a client that writes the `dead` section: an older
client refuses that bucket and says to upgrade, rather than write the dead
entries among the live documents. `DEAD_RESTORE` (`0x3B`) is restore-only and is
refused on a client frame.

- **`USER_CREATE` and `USER_ALTER` refuse a credential salt that is not standard base64, or whose decoded
  length is not `params.salt_len`.** The refusal is `INVALID_ARGUMENT` and says the salt must be
  standard base64 of `salt_len` (16 or more) bytes. Before, such a salt was stored and the account could
  never log in, with the wrong-password wording. A users file or pushed config that already holds one still
  loads; the server logs the account as unusable and names the reset.
- **A read whose forwarded answer is lost says nothing changed, and how to
  recover.** A get, exists, count, peek, scan or dump chunk forwarded to an owner
  that could not be reached used to carry a write's advice ("a write may have
  been applied there ..."). It now says the read was not answered and nothing
  changed, then `Retry in a moment; if the member is down, run 'nodes' to see
  every member and its state`. A forwarded write's message and the
  `UNAVAILABLE` code are unchanged; a restore is a write here and keeps the
  write outcome, with advice that fits a restore.
- **`describe bucket` prints no `Path:` row, and `BUCKET_STATS` answers
  `logical_bytes` instead of `bytes` and no `path`.** A bucket has had
  no directory of its own since every bucket moved into the node's shared
  store, so the path named a location that is not on disk; `show databases`
  loses its path for the same reason. A report now shows the bucket's logical
  size — the sum of its keys and values before compression and compaction, read
  from its state document — and the wire field is `logical_bytes`. **What to
  do:** a script that read `path` uses `HOARDDB_DATA_DIR`, the store's
  directory; one that read `bytes` reads `logical_bytes`, which is a logical
  figure, not the disk space allocated. An older server sends no
  `logical_bytes`, and a client against one must keep reading `bytes`.

- **A refused user-administration command carries a code that names the cause.** `create user`, `alter user`, `drop user`, `grant role` and
  `revoke role` used to answer `INTERNAL` for nearly every refusal, so a driver
  could not tell a taken name from a server fault. They now use the codes the
  database and bucket verbs use: a duplicate user is `ALREADY_EXISTS`; an
  unknown user is `NOT_FOUND`; a stale version is `CONFLICT`; an unknown or
  retired role, a rejected credential (algorithm, salt, hash, work factor), a
  reused salt, an empty or reserved username, the last-administrator guard, the
  user limit and a payload that does not decode are `INVALID_ARGUMENT`, with the
  reason in the message; "not the master" and "no quorum" are `UNAVAILABLE`.
  `INTERNAL` is left for real server faults. Every message names the user or
  role and the fix. A user-administration `UNAVAILABLE` carries no
  `write_outcome`, because nothing was applied; a driver may retry it with
  backoff and the same `request_id`, except "not the master", which wants the
  master's address, not the same node. A script that matched `INTERNAL` or
  `server error (5)` on these refusals must match the new code. No alias.
- **`USER_GET` for an unknown user is a success with `found: false`.** It was an
  `ERROR` frame, so the CLI's own "user not found" branch never ran. `alter user`
  and `user describe` now print `user "ada" not found; run 'user list' to see
  the users`. A driver that read the `INTERNAL` error for this case reads
  `found` instead.

- **A batch a replica refused as too large no longer holds its position below an
  entry a later snapshot seed has carried it past.** A replica that
  refused a batch remembered the refusal until an entry at or past it applied from
  that source, and a snapshot seed moved the node past it without clearing it, so
  the record stayed, and at the next start the node lowered its position for that
  source below an entry it already held, so the source sent it again; the record
  also stayed for a source the node would never hear from again. A
  rebase that carries the position at or past the refusal now deletes it in the
  same synced batch that writes the position, so no crash can leave a record
  below an entry the rebase passed; a rebase that clamps below the refused
  sequence keeps it, because that entry is still owed. A source that leaves the
  membership has its record deleted when the node adopts the new member list,
  however late it learns of the removal. The records are one per source, never
  per bucket, and nothing a query returns changes.

- **A node in `encryption.mode: require` refuses to start over a store that is
  not sealed.** The start-up check of the store's files used to log
  one warning and serve in both `on` and `require`. Under `require` it now stops
  the start before anything is written, naming the damaged file and the fix:
  restore the node store from a backup, or set `encryption.mode` to `on`
  (`HOARDDB_ENCRYPTION_MODE=on`) to start anyway. `on` still warns and serves.
  A `require` node that started over a damaged store before now refuses.

- **A standalone data directory started as a cluster member stops at start
  with the steps to move it.** A node first run without
  `HOARDDB_SEED_NODES` keeps its users under its own `auth.token.key`; a
  clustered node signs with a key derived from the cluster token. Started with
  seeds, such a directory used to fail with "restore the original
  auth.token.key or lose the users", although the file was present, and with the
  keystore on it started with no stored user able to log in. It now stops before
  anything is written, names the transition, and lists the steps: `user list`
  and `user describe` on the standalone node, `hoarddb dump`, a cluster member
  on an empty data directory, `hoarddb restore`, then `create user` for each
  user. Started without seeds, the directory runs as before. **What to do:**
  follow the steps in the error, also in the replication guide under "Moving a
  standalone node into a cluster".
- **Replication frames get at least 2 s whatever the write-concern timeout.** A ship, handshake or advance to a peer used half the
  write-concern timeout, so a short one (50 ms) gave a replay frame 25 ms and
  a slow replica caught up one frame per retry backoff. The timeout is now the
  larger of half the write-concern timeout and 2 s. No new setting; the default
  5 s write-concern timeout is unchanged (2.5 s). A write waiting on a silent
  peer under a write-concern timeout below 4 s can now be answered before the
  ship gives up.

- **A restarted node holds its own writes until a majority of its peers has
  answered it.** Each answer says how far that peer holds the node's
  write-ahead log, and the node moves its log past the highest before it gives
  any write a sequence. For three members both others must answer; for five,
  three of the four. Until then a write the node owns, or a `create database` or
  `drop database` sent to it, waits up to the write-concern timeout and is then
  refused `UNAVAILABLE`, naming the peers not heard from; nothing is written.
  A normal restart with its peers up costs one answer from each, which the node
  exchanges anyway. `wal/incarnation.bson` gains an optional `skips` field
  recording the sequences a log moved past; a file without it
  opens as a log with none.

- **A `unique` modifier inside a store field's braces is refused, and an inner
  scalar is noted.** `create bucket Post { Comments fifoStore { Code
  string unique } }` read as a unique index and created none, with nothing
  printed. It is now refused before anything is sent, naming the store field,
  the inner field and what to do instead. An inner scalar without `unique` joins
  the existing not-enforced note, named as `<store>.<field>`.

- **The CLI's `stored on N of M copies` note says where to look.**
  It now names `status` and the node logs, and the common causes briefly: a
  replica still catching up just after `create bucket` is normal, a node down or
  over its disk high-water mark is not. `docs/user/replication.md` lists the
  causes and the check for each (`status`, `nodes`, the log line), and
  `scripts/docker-smoke.sh` fails when a node is not healthy or a later insert is
  still confirmed by fewer copies than the factor, instead of passing over a
  one-copy cluster.

- **The binaries are now `hoarddb` and `hoarddb-server`.** The command
  line client was `hoardDB-cli` from `make build` and `hoarddb-cli` from the
  release matrix; the server was `hoardDB-server` from `make build`. Every build
  (source, release archive, image) now produces the same two lowercase names, and
  the help text, error hints and documentation use them. There is no alias: change
  any script that calls `hoardDB-cli`, `hoarddb-cli` or `hoardDB-server`. Release
  archives are named `hoarddb-<os>-<arch>` and `hoarddb-server-<os>-<arch>`.

- **A copy of a node's data directory no longer starts as that node on another
  address.** A node restarting from `membership.json` used to take the
  record listed under its node id at any address, so a cloned disk or a restored
  backup started beside the original ran as it. On a concrete `HOARDDB_LISTEN`,
  the record is now taken only when it is the same endpoint (the same port, and a
  host that is or resolves to the listen host, as after `join <id>@<host name>`);
  otherwise the node stops at start naming the id, both addresses and the fix. A
  node on a wildcard listener keeps the old match, and the cluster detects a copy
  of it instead (see **Fixed**). **What to do:** start a copy with an empty data
  directory and its own `HOARDDB_NODE_ID`, then `join` it.

- **A node is matched to its seed entry by address, and by node id only on a
  wildcard listen address with `HOARDDB_NODE_ID` set.** A node used
  to take the entry listed under its node id at any address, and the id
  defaults to the host name, so two hosts cloned from one image could both run
  as one listed node. Now a node whose address is listed takes that entry; one
  whose `HOARDDB_NODE_ID` is listed at another address, or whose host name is
  listed at another address, stops at start naming the id, both addresses and
  the fix; a seed list naming one address under two node ids stops every node
  at start; and nodes whose seed lists place one node id at different addresses
  refuse each other's member list, so the node whose list differs is never
  confirmed. A node whose member list no member has confirmed refuses user
  changes. Nothing changes on the wire: the members' addresses were already
  exchanged, now they are compared. **What to do:** give every node the same
  `HOARDDB_SEED_NODES`. A node listening on `0.0.0.0` and listed as
  `<name>@<address>` must set `HOARDDB_NODE_ID=<name>`; the host name default no
  longer stands in for it.
- **A wildcard or loopback mismatch that put a node in the ring twice is
  refused at start.** A node with `HOARDDB_LISTEN=0.0.0.0:4433` is
  advertised as `127.0.0.1:4433`, which never equals the address its seed entry
  carries. With an address-only `host:port` seed on another machine, or with no
  `HOARDDB_NODE_ID` (the host name default is never matched by id), the ring held
  the node twice: at its seed entry and on loopback, as a member no peer could
  reach, so some keys routed to nothing. A real listen address with a loopback
  seed on its own port (`127.0.0.1:4433` beside `HOARDDB_LISTEN=192.0.2.1:4433`)
  did the same. Each is now refused at start, naming the fix. Not affected: a
  wildcard node with its own `HOARDDB_NODE_ID` beside `<node-id>@<address>` seeds,
  listed or not (the add-node path, followed by `join`); a wildcard node whose
  seeds are all on this machine; and any restart whose saved `membership.json`
  lists the node once. A saved `membership.json` that already holds the extra
  entry is refused too, because a saved member set outranks the seeds and
  correcting them would not remove it. **What to do:** for a new node, set
  `HOARDDB_NODE_ID` and list every node as `<node-id>@<address>` in
  `HOARDDB_SEED_NODES`, or set `HOARDDB_LISTEN` to the address the seeds list.
  For a refused saved set: stop every node, remove `membership.json` from each
  node's data directory, set `HOARDDB_NODE_ID` on each and the same
  `<node-id>@<address>` list in `HOARDDB_SEED_NODES` on every node, then start
  them again.
- **A replaced directory's refusal now names what took its place.**
  When a store's data directory was replaced by a regular file, the refusal
  said "another directory is in its place", which sent an operator looking for
  the wrong thing. The message now says whether the path holds a regular file,
  another directory or something else. **What to do:** a script that matched the
  old sentence for the regular-file case should match the new one.
- **macOS is not a supported platform for v1.1; native macOS moves to v1.2.** v1.1 supports Linux on amd64 and arm64 (binaries and the multi-arch Docker image); on a Mac, run the Docker image. The darwin cross-build stays in CI as a compile check only.

- **A replica's write-ahead log records the owner's outcome and term of each
  replicated entry.** A replica that refused an entry its owner
  applied, and stopped before its record of that disagreement was written, came
  back counting the entry as applied. Its copy then lacked the owner's write, and
  nothing stalled the stream or re-copied the bucket. The log now carries what
  the owner's apply did, so the restart's replay finds the disagreement again,
  records it, and holds the stream there until the bucket is migrated. The log
  entry format gained a field, about two bytes on each entry a node applies as a
  replica. A log without the field is still read, and its entries
  replay as they did before. **No action is needed on upgrade.** The crash fix
  covers the entries written after the upgrade, not those already in the log.

- **The `encryption` command no longer names "changed after it opened" as the
  only cause of a plaintext store.** When the report carries a
  Problem, the explanatory sentence now names all three causes: the store
  changed after it opened, its files were altered before this start, or the
  check could not read it. The check that feeds the sentence also runs at start,
  so a store altered before start got the same Problem with a cause that was
  wrong and sent the operator looking in the wrong place. The restart and dump
  advice is unchanged. **What to do:** nothing; read the Problem line, which
  names the file or check that failed.

- **The lost-forward error for a pop now says the entry may be lost and names
  the peek that shows what is left.** A pop whose answer from the
  bucket's owner was lost was answered with the shared forward-lost sentence,
  which told the caller to "read it back before retrying a push or an $inc" —
  advice for an operation they did not run, and no way to see what the pop left
  behind. A fifo, lifo or heap pop now gets "the entry may be lost … Do not pop
  again to look for it. Peek to see what is left: `db.<bucket>.peek()` (run
  `use <database>` first)"; a store-field pop names the document's
  `db.<bucket>["<key>"].<field>.peek()`. The sentinel, the `UNAVAILABLE` code
  and `write_outcome: unknown` are unchanged, and every other write keeps the
  old sentence. **What to do:** a script that matches the pop case of the old
  sentence should match the new one.
- **A `migrate` refused because a node's disk is above
  `disk_high_water_pct` is answered `UNAVAILABLE`, not `INTERNAL`.** The message was already right (it named the node, the bucket
  and the fix), but the code said "server error", so a driver, the CLI's exit
  class or a monitor that classifies by code read a node protecting its own
  disk as faulty. The refusal now carries the code that means "the cluster
  cannot answer here, now, and nothing about the request is wrong". **What to
  do:** a caller that retries `UNAVAILABLE` keeps its deadline; a red disk
  does not clear on a retry of seconds, and the message says to free space on
  the node.
- **The ownership-moved error no longer denies replication.** A write
  answered `UNAVAILABLE` because the bucket moved said it was "stored on this
  node only and will not be replicated from here"; a ceded owner
  queues a data write for the replicas the sender's rule addresses it to, so
  that was false and made a developer treat a delivered write as a doomed single
  copy. The message now names the bucket, says the outcome is unknown, and keeps
  the instruction that was always right: read it back, then retry if it is
  missing. It claims neither delivery (a create or a drop is not delivered) nor
  loss, and the error code and `write_outcome: unknown` are unchanged. Driver
  vectors regenerated. **What to do:** a script that matches the old
  sentence should match the new one.
- **The join freeze's refusal says nothing was "created, dropped, altered or
  restored".** It said "created, dropped or restored"; the server now also
  pauses the change of a bucket's replication factor (`ALTER_BUCKET`) while a
  node joins. **What to do:** a script that matches the old sentence should match the new
  one.
- **A store field's items now carry `position` 0 and `priority` 0.** A field
  push, pop, peek or list answered the first element of a fifo or lifo field
  without `position`, and a heap element at its default priority without
  `priority`, because a zero was dropped from the answer. Those two members are
  now present for the engines they belong to, including at 0 (`position` for
  fifo and lifo, `priority` for heap) and still absent for an engine that has
  none (hash, btree, blob). **What to do:** a driver that read a missing
  `position` or `priority` as 0 should read the member as it arrives; a missing
  member now means the engine has none. Driver vectors regenerated.
- **A query whose client has gone stops, instead of running to its end.** When a
  client closes its connection (or the connection resets), every `SCAN`,
  `SEARCH`, `BTREE_RANGE`, `COUNT` and `DISTINCT` still running for it stops
  within 256 entries, and one still waiting for a query slot leaves the queue
  without running. This holds when the connection's pipelining window is full,
  and at `HOARDDB_TCP_PIPELINE_FRAMES=1`. New counter:
  `hoarddb_query_cancelled_total`. **What to expect:** a client that
  half-closes its side of the connection (`CloseWrite`, or a TLS
  `close_notify`) and then waits to read its answers now loses the answers to
  queries still running; close only once you have read what you need. Writes
  are not affected: a write the server has logged is applied and replicated
  whether or not its client is still there, and a client that leaves no longer
  makes the replica's stream stall.
- **A response the server cannot write closes the connection.** When a
  response write fails or misses `HOARDDB_TCP_FRAME_TIMEOUT` (a client that has
  stopped reading), the server now closes the connection at once and runs
  nothing more from it. Before, it logged the error and kept reading, running
  every later request in full and discarding each answer.
- **The idle timeout counts from the last answer, not from the last request.**
  `HOARDDB_TCP_IDLE_TIMEOUT` now runs only while nothing is in flight on the
  connection. Before, a request that ran longer than the idle timeout was
  answered and the connection was then closed at once; now it stays open, and
  closes after a full idle timeout with nothing sent.
- **A node whose disk is at or above `disk_high_water_pct` now refuses to take
  on buckets, and says why.** Before, it only voted against a new
  member: in a cluster of three or more a majority admitted the member anyway,
  and the full node then copied in whatever the new membership placed on it.
  Now it copies in no bucket it does not already hold, whoever asks (its own
  rebalance, another node's hand-off, or `migrate`), and the refusal names the
  node, the bucket and the fix. It still learns every membership change, serves
  the buckets it holds and gives copies away. **What to expect:** a bucket the
  change made this node the owner of refuses writes, naming the full disk, and
  one it made this node a replica of is a copy short, until you free space on
  the node and run `migrate <database> <bucket>` on it (the rebalance also
  retries), or remove the node with `remove-node <node> --token <token>` from
  another member. The node logs one WARN an hour while it refuses copies.
  `disk_high_water_pct: 0` turns this off with the vote.

- **A `BLOB_WRITE` with no data is answered `INVALID_ARGUMENT`, not `INTERNAL`.** The message was already right (it names
  `db.<bucket>.put({_id_: "key", data: "..."})`); the code said "server error"
  for an empty file the client sent, so a monitor that alerts on `INTERNAL`
  paged someone for a client mistake. No driver vector covers the case.

- **A node whose disk is at or above `disk_high_water_pct` also refuses a
  bucket that another node's replication stream would create on it, and the
  stream to it stops until it has room.** A new replica could still
  arrive with no copy: when the bucket's owner still had the bucket's creation
  in its log, its replication stream replayed it and the full node created the
  bucket and wrote its history. Now the full node refuses that create, naming
  itself, the bucket and the fix. Replication is ordered, so **replication from
  that owner to the full node stops at that point, for every bucket**: the
  copies the full node already holds of that owner's buckets fall behind,
  count toward no write concern, and the owner keeps its log for the node
  meanwhile. The owner logs one WARN when the node leaves the in-sync set,
  with the refusal as its reason. Free space on the node (or raise
  `HOARDDB_DISK_HIGH_WATER`) and replication resumes on its own and catches
  the node up; or remove the node with `remove-node <node> --token <token>`
  from another member.

- **A request whose payload is not well-formed BSON is answered
  `INVALID_ARGUMENT`, not `INTERNAL`.** The message was already
  right; the code said "server error" for a request the client got wrong, so a
  driver that retries on `INTERNAL` retried it. The driver vectors are
  regenerated (the vector "a request payload that is not BSON" now expects code
  4). A compact `GET_FAST` payload that cannot be decoded gets the same code.

- **A request nested past the bound, or whose payload is not well-formed BSON,
  is answered before the server reads anything else from it.** A
  request nested past the bound still gets the explanatory `INVALID_ARGUMENT`
  refusal; it now opens with what was sent, named from the kind of request ("The
  document", "The filter", "The update", "The bucket declaration", "The blob's
  metadata", "The node-to-node request"), and no longer names the bucket the
  request was sent to. A refused entry of a replicated push is still prefixed
  with the entry's sequence number and bucket, which come from the push that
  carries the entry, not from the refused entry itself.
- **The answer to a request whose payload is not well-formed BSON reads
  differently.** It now reads "The document is not well-formed BSON
  (...). Send it with a hoardDB driver, or encode it as one BSON document."
  instead of saying the payload could not be read to find the database it
  names; its error code is unchanged. The driver vectors are regenerated: the
  vector "a request payload that is not BSON" carries the new message, and each
  driver's copy of the vectors needs re-syncing.
- **`known_servers` is checked before it is trusted, and refused when it is
  unsafe.** The CLI's `~/.hoarddb/known_servers` and a node's
  `<data dir>/cluster/known_servers` must belong to you (or root), must not be
  writable by the group or by others, and the file must not be a symbolic link
  (its directory may be one; the directory it points to is checked). An unsafe
  one stops the CLI before it dials, and stops a node at start, with the path and
  the command that fixes it, for example
  `Fix it with: chmod 700 <your home>/.hoarddb`. Files and directories hoardDB
  creates are already owner-only, so this affects only one made or changed by
  something else: a directory created by hand under umask `0002`, a file restored
  from a backup with its modes widened, or a container volume prepared as another
  user (`chown <uid> <path>` to the user the node runs as). What to do: run the
  command the error names.
- **`known_servers` lines are split only at LF, and fields only at spaces and
  tabs.** One trailing CR is still ignored, so a file saved with
  Windows line endings reads as before. A line that used a no-break space, form
  feed, vertical tab or another Unicode space between the address and the key was
  read before; it is now skipped as malformed, and the server it named is asked
  about again on the next connection. What to do: separate the fields with a
  space.
- **Documents nested deeper than 100 levels are refused with an error that says
  how to fix it.** The document itself is level 1, and each embedded document or
  array inside it adds one, the same count the drivers already enforce. A write,
  a filter, an update whose result would nest deeper, and a restored document are
  all held to it; the refusal is `INVALID_ARGUMENT`, a write is `not_applied`, and
  the connection stays usable. Flatten the deep part, or keep it under its own
  key, in a store field or as a blob. See [Limits](docs/limits.md).

- **A verb sent to a store type that does not answer it is `INVALID_ARGUMENT`,
  not `INTERNAL`.** `range`, `first` and `last` on anything but a
  B+Tree bucket, a blob read or write on anything but a blob bucket, `update` on
  a FIFO, LIFO, heap or blob bucket, and `distinct`, a filtered `count`, a
  multi-document `update` or a `search` on a blob bucket used to come back as
  `INTERNAL`, which says the server is at fault. The message is unchanged
  (`bucket "users" is type hash; this operation requires type btree`); only the
  code is new. What to do: a script that branched on `INTERNAL` for these must
  branch on `INVALID_ARGUMENT`, and send the verb the message names.
- **`EXISTS` on a FIFO, LIFO or heap bucket is an error, not `exists: false`.** A bucket type with no keys has no yes-or-no answer, so the server
  now refuses with `INVALID_ARGUMENT` and a message naming the type. An `EXISTS`
  with an empty key is refused the same way on every type, and a storage read
  failure during `EXISTS` is an error and no longer reads as `exists: false`.
  What to do: only ask `EXISTS` of a hash, B+Tree or blob bucket; treat
  `exists: false` as "looked, and the key is not there".
- **`find` on a blob bucket answers `found: false`.** It answered
  `found: true` with no documents and the refusal in `message`, which a client
  that tests `found` read as an empty result. The message is unchanged and names
  `get` and `info`.
- **An insert-many on a FIFO or LIFO bucket reports what it pushed**.
  The documents were always pushed, but the answer said `count: 0` and no ids, so
  a client that retried on that pushed them twice. It now answers the count and
  the ids, in order. What to do: nothing; stop treating `count: 0` as "retry".
- **The verb matrix names what the server answers.** `insert` on a
  FIFO, LIFO, heap or blob bucket, `push` on a hash or B+Tree bucket, `get` and
  `exists` on a blob bucket and `search` on a bucket with no index are accepted,
  as they have been; the driver handbook and
  [docs/user/store-types.md](docs/store-types.md) now say so, and a server
  test checks the handbook's matrix against the server.

- **A report's labels line up.** `describe bucket` and `db.<bucket>.stats()` pad
  their labels to the widest of them, so the values begin in one column —
  `Type:`, `Entries:`, `Logical size:`, `Encrypted:` and `Indexes:` read as a
  block instead of five ragged lines. The same block is what the store list's
  header and a `create bucket` confirmation's `Indexes`/`Stores` rows use, so a
  confirmation that declares only stores now says `Stores: 1 store field(s)
  declared` with one space, having nothing to align to. A report of one row is
  unchanged: there is no column to line up with.

- **The `--insecure` warning is one line, printed once, and the duplicate record
  is gone.** It reads `WARNING: TLS certificate verification is disabled
  (--insecure); the connection to <address> cannot be trusted and may be
  intercepted. Use this only against a local development server.` — printed once
  in a session and in `dump`, `restore` and `ingest` (on stderr for the verbs,
  where stdout belongs to a parser). The transport layer logs the same fact again
  as a structured record — a timestamp and key=value pairs, which a user cannot
  act on — and the CLI used to keep that whole stream silent to avoid saying it
  twice, which also lost warnings that matter: a fingerprint the client could not
  record, for one. The CLI now drops **only** that one record and forwards
  everything else the client's library logs at warnings and above; `-debug` in a
  session turns the whole stream on, duplicate included. The verbs carry no
  `-debug` flag, so they print the one line and no record.

- **The first start refuses a short root password from `HOARDDB_ROOT_PASSWORD`.**
  A first start that seeds the root user from `HOARDDB_ROOT_PASSWORD` is now
  measured against `HOARDDB_PASSWORD_MIN_LENGTH` (default 12), in characters, as
  `create user` is. A shorter password stops the server before it creates any
  user, and the error names the length, the minimum and the setting. It never
  prints the password. The error names the setting that held the password,
  `HOARDDB_ROOT_PASSWORD` or `HOARDDB_ROOT_PASSWORD_FILE`. What to do: set a longer
  password in that setting, or unset it and start hoarddb-server from a terminal to
  type one. A restart over an existing user store does not seed and is unchanged.

- **An idle bucket costs less memory, and the garbage collector no longer
  walks it.** A bucket that is not being read or written is now a fixed, pointer-free record in a shared table, instead of several heap objects, so it is smaller and a forced garbage collection takes far less time with many idle buckets. A bucket that declares typed fields and nothing else costs the same as a plain one: buckets with the same declaration share it. A bucket that declares a store field or an index also costs less than before. Nothing changes on disk or on the wire.
- **`consistent get` versions are `(term, sequence)`.** The `version` in a
  `consistent get` answer is now the membership term of the write's owner in the
  high bits and the owner's log sequence in the low 44: 2^44 plus the sequence
  for a write in term 1, and above 2^53 from term 512 (a JavaScript client must
  read it as a 64-bit integer). A version written before this release is a bare
  sequence, below 2^44, and is not rewritten. Updates, update-many and pushes to
  hash and btree buckets now carry versions as inserts do.
- **`consistent get` is refused** while the bucket's owner cannot be read and
  the other copies do not all agree (the message names the owner), while a
  replica that a `remove-node` promoted takes the bucket over, while the members
  disagree on the membership for the moments after a change, and while a member
  runs another membership protocol. Each refusal is `UNAVAILABLE`; retry. It no longer
  compares a copy of another bucket of the same name, created apart.
- **`consistent get` is refused for a bucket whose holders are not all
  encrypted at rest alike** (`UNAVAILABLE`), naming the unencrypted holder and the way
  to finish encrypting it: `hoardDB dump`, a fresh data directory with
  `encryption.mode=on`, and `hoardDB restore`.
- **A replica stalled on a batch too large for its memtable stays stalled after
  a restart.** Its replay now writes the same version keys as its live apply, so
  the batch no longer fits on replay when it did not fit live. Give it the
  memtable the batch needs (`storage.memtable_mb`) and restart it.
- **Two refusals at the version limits.** A write is refused `UNAVAILABLE`,
  `not_applied`, once a node's log has reached 2^44 entries, and a bucket is not
  opened by its new owner once the cluster has passed 524,287 committed
  membership changes; both name the way out, `hoardDB dump` and a restore into a
  new cluster.
- **A delete, put or btree insert carrying `repair_bound` is refused**
  `INVALID_ARGUMENT` at the client port: the field is internal to
  `consistent get`'s repair. The put and btree insert refusals are new
; before, a client's `repair_bound` on a put was ignored.
- **A membership change is agreed before any member adopts it.** `join` and
  `remove-node` run in three rounds between the current members: a majority
  promises, a majority accepts the new member set (an acceptance is recorded and
  acts on nothing), and only then does every member adopt the new epoch. A new
  node-to-node opcode, `MEMBERSHIP_CHANGE` (0x9E), carries the three rounds.
  `CLUSTER_PROPOSE` (0x96), the old vote, is refused with
  `this member runs a newer membership protocol; upgrade every member before
  adding or removing nodes`. **Upgrade every member before adding or removing a
  node**: a change is refused while members run different builds (members log
  `member <node> runs a build without this release's membership protocol;
  upgrade every member before adding or removing nodes`). Reads, writes and
  replication are not affected. Do not downgrade a member while a change runs:
  an older build drops what the member accepted. If a member stops in the middle
  of a change, another member that accepted it finishes it, 20 to 30 seconds
  later, and backs off to every 5 minutes while it cannot.
- **New texts for a membership change that does not go through**, one per
  outcome:
  - nothing offered: `membership change not made: only 2 of 5 members of epoch
    2 promised (need 3), so nothing was offered and nothing changed. Run 'nodes'
    to see which members are down, then run the command again`;
  - offered, not confirmed: `the change (removing n5) was offered to the members
    of epoch 2, but this command stopped before a majority confirmed it. It
    usually completes within a minute, and within about five minutes while
    members are unreachable. Run 'nodes' to see whether n5 is still listed before
    you try again` (and, while finishing another change, a text naming both);
  - another change finished instead: `another membership change (removing n5)
    was in flight and is now committed at epoch 3; your change (removing n2) was
    not made. Run 'nodes', and run your command again if it is still wanted`;
  - a re-run of a change already made: success, with `this change is already
    committed: membership epoch 3 is the change (removing n5). Nothing more to
    do; run 'nodes' to see the membership`;
  - this member behind: `this node was behind: membership epoch 3 is already
    committed (removing n5). Run 'nodes', and run your command again if it is
    still wanted`;
  - another member started a change meanwhile: `membership change not made:
    another member (n2) started a membership change for epoch 3 while this one
    was being prepared, so nothing was offered and nothing changed. Run 'nodes'
    to see the result, then run your command again if it is still wanted`;
  - made, but not on the member you asked (the command exits with an error):
    `the change (removing n5) is committed at membership epoch 3, but this node
    could not adopt it: <cause>. The other members have it, and this node
    catches up by gossip once the cause is fixed. Run 'nodes' against another
    member to see the new membership`.
- The refusal a member gives while it disagrees with another about the member
  set at its epoch now says to run `nodes history` against every member and
  follow the procedure for a diverged cluster in the replication guide.

- **A mistyped `skip`, `sort` or `projection` on `find` is now refused instead of
  ignored.** `find({}, {skip: "2"})` used to start from the first document,
  `{sort: "Age"}` returned unsorted documents, and `{projection: ["Name"]}`
  returned whole documents, all with no error. Each is now a usage error that
  names the accepted form, for example `{skip: 20}`, `{sort: {Age: 1}}` and
  `{projection: {Name: 1}}`. Write the options in the forms the HQL spec shows.

- **A mistyped `limit` on `find` or a store field's `list` is now refused instead
  of answering the default page.** `list({limit: "3"})` and `find({}, {limit: "3"})`
  used to return up to 100 entries with no sign that the count was dropped; a
  fraction was truncated, and an unknown key such as `{limt: 3}` was ignored. `limit`
  must now be a whole number of at least 1, `find` accepts only `skip`, `limit`,
  `sort` and `projection`, and `list` only `limit` and `after`; anything else is a
  usage error naming the form to write, and nothing is sent. `skip` is unchanged.
  No wire change.
- **A bucket whose owner changes refuses writes until its new owner has taken it
  over.** The refusal is "unavailable, retry" with `write_outcome: not_applied`
  and says what the new owner is waiting for: the previous owner's last writes, a
  member that may hold a copy (named, with `remove-node` offered only when a
  removal can commit), or, with `auto_rebalance` off, a `migrate` to run on it.
  Other buckets keep accepting writes.
- **`show buckets` prints `leaving`** beside a bucket whose copy on that node is
  outside the bucket's replica set and is kept until the set holds what it holds
  (`rf 3, leaving`); the bucket listing's answer carries `outside: true` for it.
- **A replica that stopped replication is repaired on its own.** With
  `auto_rebalance` on, a replica that refused a write its owner applied copies
  the bucket from its owner again without a `migrate`.
- **`remove-node` no longer tells you to run `migrate` on each new owner.** The
  buckets move to their new owners on their own.
- **A blob read of a key that does not exist no longer points at `find()`.** The miss
  now reads `blob "<key>" not found in <db>.<bucket> — keys are exact; db.<bucket>.count()
  shows how many the bucket holds`, because `find()` is refused on a blob bucket and no
  verb lists a blob bucket's keys. The `blob "` ... ` not found in ` shape clients
  classify on is unchanged.
- **A create or drop of a bucket at a node that has not copied it asks the members
  first.** Outside a join, such a request waits while a member holds the bucket,
  and is refused with "unavailable, retry", naming the members that did not answer,
  when a member is unreachable and the bucket's placement changed since every member
  last settled a membership change. A drop of a bucket no member holds answers that
  it is not found and logs nothing. A drop that reached the members but was not
  applied on its owner answers `write_outcome: unknown`, not `not_applied`.
- **`migrate <database> <bucket>` repairs a replica that stopped replication.** On
  a replica that refused a write its owner applied, it replaces the replica's copy
  with the owner's, and replication from that owner resumes. On any other node that
  holds the bucket it still says nothing was pulled.
- **A replica refuses, and does not acknowledge, a write its owner applied and it
  cannot.** Replication from that owner to it stops for every bucket until
  `migrate` repairs it; `hoarddb_replica_divergences_total` counts each.
- **A drop is sent to every member** and removes only the incarnation of the bucket
  it names.
- **`nodes` reports `settled_at: 1`, not 0, on a cluster that never changed
  membership.** The founding members now record that they are settled at the
  cluster's first epoch (`schema_changes.settled_at` in the `nodes` answer; the
  driver vector "list nodes" is regenerated for it). Nothing else in the answer
  changed.

- **CLI help and statement errors.** `help` no longer lists `alter bucket` as unavailable (it adds store
  fields); `help user` teaches every user command with a worked example; there is a `help theme` topic; and
  `help btree`, `hash` and `blob` show the store-field form. A line holding a second statement after `;`
  is refused before anything runs, with `one statement per line`, instead of reading the rest of the line as
  part of the first statement. A theme file with an unknown key names the key and the valid ones instead of a
  Go type name.
- Example addresses in the docs, the compose files and the tests now use the RFC 5737 documentation
  ranges (`192.0.2.x`, `198.51.100.x`) instead of `10.x`, so no example looks like a real private network.
  The compose bridge subnet is now `198.51.100.0/24`.
- **`peek({limit: N})` reads the next N entries of a queue, a heap or a store
  field, in pop order, without removing them.** `peek()` is `limit: 1`. The
  request gains `limit` (1 to 1000; a negative number or one above 1000 is
  refused with `INVALID_ARGUMENT` and a message that names the cap) and every
  peek answers a list, `items`, one entry for a one-entry peek too. The old
  `data` member of the peek response is gone, and a store field's single-element
  peek response is gone with it: a field peek answers `items`, each with its
  position, priority and entry id. A peek is refused whole with
  `RESULT_TOO_LARGE` when its entries from the second on would pass the result
  ceiling, never answered short; its first entry is always answered, as `pop`
  would return it. `peek(N)` with a bare count is a usage error that names
  `peek({limit: N})`, and a limit that is not a whole number of 1 or more, or an
  option other than `limit`, is a usage error too. A peek of a hash, btree or
  blob bucket says why it has no pop order and which verbs read it. Drivers
  read `items`; the handbook and `docs/user/hql-spec.md` describe the new shape.
- **`pop({limit: N})` removes and returns the next N entries of a queue, a heap
  or a store field, in pop order, in one write.** `pop()` is `limit: 1`. N is 1
  to 100; above 100 the pop is refused, naming the number sent. A pop whose
  entries would exceed the result ceiling (`HOARDDB_QUERY_MAX_RESULT_BYTES`) is
  refused with `RESULT_TOO_LARGE` and removes nothing; the first entry is always
  returned, however large. A pop answers a list, `items`, one entry for a
  one-entry pop too: the `data` member of the pop response is gone, and a store
  field's pop answers `items` (each with its position, priority and entry id)
  instead of `element`. A pop's answer is the only copy of what it removed, so
  drivers never retry a pop, and an answer lost after the pop was applied loses
  up to N entries. `pop(3)` is now a usage error naming `pop({limit: 3})` (it
  used to remove one entry and ignore the 3). Pop on a hash, btree or blob
  bucket names the verbs that work.
- **`pop()` on a heap whose state and range disagree now answers an error naming
  the restore fix instead of EMPTY.** A queue's pop names it too. The error says
  to restore the bucket from a replica with `hoarddb dump` on the replica and
  `restore` here, or from a dump or backup on a standalone node.
- **Other pop wording that changed.** A pop that fails reads `popping queue
  entries (limit N): ...` or `popping heap entries (limit N): ...` where it read
  `popping a queue entry: ...`, and a plain `pop()` no longer says `(limit 1)` in
  a failure message. A pop whose deletes do not fit one batch, on a bucket or on
  a store field, says to pop fewer at a time (`pop({limit: N/2})`) and that
  nothing was removed, instead of the batch put's advice to send fewer documents.
  `RESULT_TOO_LARGE` now also answers a pop. A store-field pop does not set
  `len`: it arrives as 0, so send `len()` for the count left. `pop({bogus: true})`
  and `pop(3)` used to remove an entry and say nothing; both are now usage
  errors.
- **A replica too small for what its owner committed now pauses replication
  until it is resized.** A member whose memtable is smaller than its owner's can
  be handed a pop, a cascade delete or a TTL expiry that does not fit one batch
  there. It used to take a pop as applied while keeping the entries (a silent
  divergence) and to re-seed a cascade from its owner. It now refuses both, as it
  refuses a batch put: the owner's stream to it stalls, the owner re-sends, and
  the stall reason names the operation and the bucket and says to set
  `storage.memtable_mb` to the cluster's size on that member and restart it
  (SIGTERM, not kill -9). A restart at the same size stalls again. Copies are
  counted honestly meanwhile: the client's answer reports fewer copies than the
  replication factor.
- **The help now shows how to take N elements from a bucket.** `list({limit: N})`
  is a store-field verb, so `db.<bucket>.list({limit: N})` was refused with
  "unknown bucket method"; the help never printed the bucket's own form,
  `find({}, {limit: N})`, even though it has worked on every bucket type but
  blob. The help now prints `limit`, `skip`, both together, and — new — `sort`
  and `projection`, which `hql-spec.md` documented and the help did not. It also
  says plainly that there is no `list()` on a bucket, so the two levels are not
  confused again.
- **`help` now documents the store form.** The main help showed only
  `db.<bucket>.<verb>` for every operation, so the store on each document — the
  idea the database is built around — appeared only under `help fields`, which
  a developer has to already know about. The quick reference now carries a
  `Store Fields` section: the shape `db.<bucket>["<key>"].<field>.<verb>(...)`, a
  runnable example per verb group, the per-engine verb list, the store on a
  store's elements, and the two traps — the key is quoted, and the document must
  exist before its stores can be read. Every example in it is parsed by a test,
  so the help cannot advertise a form the parser refuses.
- **`len()`, `length()` and `count()` are now the same verb everywhere.** The help
  teaches `length()` on a bucket and `help fields` teaches `len()` on a store
  field, so following one at the other level was refused — and store fields are
  where the data lives. All three spellings now answer on a bucket and on a store
  field, on every store type, and return the same number. The refusal for a verb
  that is genuinely wrong names the aliases. Nothing is removed: `len()` on a
  store field, and `count()`/`length()` on a bucket, are unchanged.
- **The default prompt no longer paints the logo block.** It renders
  `[h]oardDB [db]>` with the `h` in the same purple as `DB` and the brackets in
  the off-white `brand`, instead of white-on-indigo `[`h`]`. Only foreground
  colours are used, so the prompt reads on a light terminal too rather than
  depending on the OSC 11 swap. A theme file that paints the block itself keeps
  working: the `tile` style is still declared and its two colours still swap
  when the terminal answers `light`. A theme file that set only `styles.tile.bg`
  now has no visible effect on the default prompt — set `styles.accent.fg` to
  recolour the mark. See [Prompt Themes](docs/configuration.md#prompt-themes).
- **A theme no longer shares the built-in theme's style map.** `lightVariant`
  rewrites the `tile` style in place, and a resolved theme that shared the
  package-level default's map repainted the built-in for every later resolution
  in the same process. The session now gets its own copy.
- **A single node's writes are now durable when it answers them.** A node with no seed nodes and a
  replication factor of 1, the zero-config default, used to answer a write before its store was synced
  to disk, so a power cut or a kernel crash could lose writes it had acknowledged (a process crash could
  not). It now syncs first; writes that arrive together share one sync, and a batch insert is one sync.
  The same holds for a node that keeps a write-ahead log but is the only member of its ring, such as
  the first seed before its peers join: its writes are not logged, and were not synced either.
  This costs write latency on a single node. Measured in process (no network), one client writing one document at a time now takes about one disk sync per write, so its writes per second fall to what the disk's sync rate allows; many clients writing at once share syncs and fall less, and the figure varies run to run with the disk; a batch of documents costs one more sync. A cluster whose nodes have peers
  is unchanged: its write-ahead log was already synced before every write. To measure everything but the
  disk in a benchmark, set
  `storage.unsafe_ack_before_sync: true` (`HOARDDB_UNSAFE_ACK_BEFORE_SYNC`); a node with it set can lose
  acknowledged writes on a power cut, and warns at every start. See
  [Storage durability](docs/configuration.md#storage-durability).

- While a node that joined the cluster is still being copied in, every schema change — create or
  drop a database or bucket, alter bucket, restore, and an ingest that creates its bucket — is
  refused with "unavailable, retry". The message names the joining node, how far the copy has got,
  what it is waiting on, and how to finish or undo the join; `nodes` shows the same, and the gauge
  `hoarddb_cluster_members_joining` counts the joins not yet settled. Reads and writes to existing
  buckets carry on. With `auto_rebalance` off the pause lasts until the `migrate` commands `nodes`
  lists have been run; making the new owner's copy can add members that took an older copy from the
  previous owner's log, so run `nodes` again after each round. A `restore --dry-run` is answered,
  and says a real restore would be refused now; a restore that stops part way names, in its
  summary, the buckets that landed.
- A node with `encryption.mode: on` now refuses to add, or vote for, a node that says its store is
  not encrypted, as `require` does: no data would ever be placed on it, so its join would never
  settle and schema changes would stay paused. The refusal names the fix. It also refuses a node it
  cannot ask — one not running yet, or too slow to answer: start it and run `join` again.
- On a new cluster's first start, a node serves database and bucket requests only once every
  founding node in its seed list has answered it and agrees; `status` shows `Membership:` on every
  cluster node (`confirmed`, `NOT ADDED YET`, `WAITING FOR FOUNDERS`, or `NOT CONFIRMED` with the
  reason).
- `/health` on the metrics listener now answers from the node's own health check: `200` with a short JSON
  body when healthy, `503` with the reasons when not (a red capacity resource, a store that should be
  sealed and is not, a member that refuses to confirm the node). `/ready` follows the same verdict. Before,
  `/health` always answered `200 ok`. A new `GET /live` answers `200` while the process is up, for a
  liveness probe. `status` prints the reasons under `Healthy:`.
- A node that is not in a cluster now refuses a write concern word it does not know (`quorum`, `2`) with
  the error a cluster gives, instead of applying the write. The accepted words are `1`, `one`, `majority`
  and `all`.
- Documents do not keep their field order; the limitations page now says so.
- A database name or a bucket name over 255 bytes is now refused when the create starts, before it is
  logged, with an error that names the limit (`bucket name "post-..." in database "app" is too long: it
  is 300 bytes and the limit is 255`). Before, a pair between 64,001 and 64,988 bytes was created, and
  a longer pair failed at the store with a raw key dump and left an empty entry behind.
- `hoardDB status` no longer prints an `Entries` row, and the health reply no longer carries
  `entry_count`. Counting every entry in every bucket made the check slower the more buckets a
  node held, so a probe could time out on a large node. `Buckets` is unchanged, except that it
  now counts the buckets the node serves (the number `hoarddb_node_buckets` reports). For one
  bucket's entry count run `describe bucket`.
- **A dotted or bracketed key is a path, everywhere.** `$set: {"Address.City": "Paris"}`
  now changes the member `City` of the sub-document `Address`; it used to add a
  top-level field literally named `Address.City` and leave `Address` unchanged, and
  filters read the literal name too. Every update operator, filter, sort,
  projection and `distinct` reads `Address.City` and `Lines[3].Qty` the same way
  ([docs/user/hql-spec.md](docs/hql-spec.md), "Paths: a value inside a
  value"). An update that writes a value and something inside it, or whose
  `$rename` pairs share a name, is refused, so every copy of a bucket holds the
  same document. A top-level field written under the old literal reading, such as
  `"Address.City"`, stays stored and is returned by `find`, but no filter, sort or
  update can name it, and a whole-document `put` that omits it removes it.
- **A top-level field name containing `.`, `[` or `]` is refused** on every write
  that stores a whole document, with the new error code `FIELD_TYPE` (`0x10`).
  `hoarddb ingest` of a file whose documents carry such names refuses those
  documents (under `--on-error=skip` they go to `--rejects`), and a dump of a
  development bucket that holds one does not restore until the name is changed.
  Names inside a sub-document are not checked.
- **An index on a path is refused** when a bucket is created; index a top-level
  copy of the value instead. One recorded before this release is kept, unusable:
  search ignores it, writes neither maintain nor enforce it, `describe bucket`
  says so and `dump` leaves it out.
- **Upgrading a cluster to this release:** stop every node once replication has
  settled, and start the new version on all of them. A path-keyed update is
  refused while any member that has answered gossip runs a version that would
  read the key literally, and so is a create that declares typed fields, which
  such a member would create undeclared.
- **A bucket's create can declare typed fields** in its options: `fields`, a list
  of `{name, type, required, members, elem}`, and `field_types` (`strict`, `warn`
  or `off`). The declaration is checked when the bucket is created (an unknown
  type word, a name with `.`, `[` or `]` or longer than 255 bytes, `_id_`, more
  than 256 fields on a level, more than 8 levels or 1,024 nodes, and `unique` on
  a type an index cannot hold are refused with a message that says how to fix
  it), recorded in the catalog, replicated, carried by migration, and shown by
  `describe bucket`. It is enforced on every insert, put, push and batch: in
  `strict`, the default, a document that does not match is refused with the
  error code `FIELD_TYPE`, naming the field, its declared type, the type
  received and the alter that would accept it (a batch is refused whole, naming
  the document); `warn` stores it and counts it in
  `hoarddb_field_type_mismatches_accepted_total`, with one log line an hour
  naming the buckets; `off` checks nothing. Refusals are counted in
  `hoarddb_field_type_refusals_total`. A bucket whose recorded declaration was
  written by a newer version, or does not pass this version's checks, serves
  reads and refuses every write with `UNSUPPORTED_VERSION`, and `describe`
  says why. Updates are checked too, in the same modes: `$inc` and `$mul` on a
  field not declared `int32`, `int`, `double`, `number` or `any` (including
  `decimal`), `$push` and `$addToSet` on a field not declared an array (or of
  an element its type does not accept), an operand of `$set`, `$min` or `$max`
  the field does not accept, the removal of a `required` field (`$unset`,
  `$rename`, `$set` to null) and a path through a field declared as something
  without members are refused with `FIELD_TYPE` before any operator runs, so a
  `$max` that would silently do nothing is refused instead; an `$inc` or `$mul`
  result the field does not accept (past `int32`'s range, past 2^53 on a
  `double`, a fraction on an integer field) and a value `$rename` moves onto a
  declared field are refused after the operators run, and a multi-update is
  refused whole. Fields an update does not write are not checked again. The same `$inc` on
  an undeclared string still answers `INVALID_ARGUMENT`. The CLI does not send a
  declaration yet. A store field can no longer be named `field_types`; a bucket that already has
  one keeps it and is served, and the server logs a warning naming it at every
  start. A store field cannot take the name of a typed field the bucket
  declares, and re-creating an existing bucket with another declaration is
  refused with the one it has.
- **The frozen URI vectors use the default user `admin`.** Three cases (the URI and its expected username) in
  `clients/testdata/uri-vectors/v1.json` said `root@`, but the default user is `admin`. Changed
  before the first release, so no version bump.
- **The server creates owner-only files.** At start-up `hoarddb-server` adds `077` to its
  umask, so the data directory is `0700` and every file in it `0600` (it was `0775` and `0664`
  under umask `0002`, including `keys/server.crt` and the store's files). A backup agent that read
  the data directory through the server's group can no longer do so; there is no setting to turn
  it off. See "File permissions and the umask" in
  [docs/user/configuration.md](docs/configuration.md).
- **The CLI stores `{Age: 36}` as an integer.** It used to send every number as a
  double, while `hoarddb ingest` kept integers as integers, so the same
  document was stored with different types depending on which one carried it. A
  statement now reads its document the way `ingest` does: `36` is an `int32`,
  `3000000000` an `int64`, and `36.0` or `1e3` a double. Data written before keeps
  its doubles; filters, sorts and indexes compare integers and doubles by value, so
  a find sees no difference between old and new numbers. What does change is the
  stored size (a small integer takes 4 bytes where a double took 8, so `describe
  bucket` and `dump` report slightly smaller totals for the same documents) and
  what a program that reads a document back with its own driver gets for a number
  the CLI wrote. An integer above `9223372036854775807` is refused; write it with a
  decimal point, or as `{"$numberDecimal": "..."}`.
- **The built-in roles are renamed.** `read` is now `reader`, `readWrite` is
  `writer`, `dbAdmin` is `db-manager`, `userAdmin` is `user-manager`,
  `clusterAdmin` is `cluster-manager`, `backup` is `backup-operator` and
  `restore` is `restore-operator`; `root` is unchanged. What each role may do is
  the same. The old names are not accepted: a grant that uses one is refused with
  the new name, and a `users.json` from a development build that still holds one
  does not load. The file is sealed and cannot be edited, so the way back is to
  move it aside and start with `HOARDDB_ROOT_PASSWORD` set, which seeds the root
  user; every other account is dropped and has to be created again with the new
  role names.
- **A malformed number or switch in an environment variable now stops the
  server** at start, with a message that names the variable, the value and the
  fix. Before, the setting silently kept its default. Switches accept `true`,
  `false`, `1`, `0`, `yes` and `no` in any case: `TRUE` and `Yes` used to mean
  false without a word, and `on`, `enabled` and `y` are now refused.
- **The CLI's exit number says why a connection failed.** The interactive
  session and `ingest` exit `6` when the server cannot be reached and `7` when
  authentication fails (the session used to exit `1` for both). `dump`,
  `restore` and `ingest` exit `1` when there are no credentials to send; for
  `ingest` this was `7`, for `dump` and `restore` it was `2`.
- **`dump`, `restore` and `ingest` no longer ask whether to trust an unknown
  server key on a terminal.** They refuse; connect once with `hoarddb` to
  record the server, or pass `-insecure` against a development server.
- **`restore` exits 3 when it left data behind.** A restore that skips blob
  bucket documents or the elements of a `blobStore` field used to exit 0. It now
  exits 3, the same code as a restore with a failed bucket, and the closing
  summary and `--json` (`documents_skipped`, `elements_skipped`) say how much was
  skipped. A script that treated 0 as "restored everything" should expect 3 for a
  database with blob buckets or blob fields. `restore --drop` also refuses to
  replace a bucket that declares a `blobStore` field. See
  [docs/user/backup-restore.md](docs/backup-restore.md#store-fields).
- **Clients and nodes talk over TCP with TLS 1.3.** The earlier transport was
  removed. A client and a server must speak the same protocol major; see
  [docs/user/compatibility.md](docs/compatibility.md).
- **Every bucket on a node now lives in one shared store.** A data directory
  with its own `catalog.json` per bucket is refused at
  start with a message that says how to move it: dump it, then
  restore into a fresh directory. It is not converted in place.
- **Environment variables start with `HOARDDB_`.** The earlier `HOARB_` prefix was
  replaced with no alias, so a script that sets the old names must be updated.
- **`majority` and `all` count the copies that are in sync, and every write says
  how many hold it.** A write at `majority` or `all` used to fail when a replica did
  not confirm in time, or at once when fewer nodes were running than the concern
  needed. It now succeeds on the copies that are reachable and caught up, and its
  response carries `copies_confirmed` beside `replication_factor`; with every
  replica out it answers `copies_confirmed: 1`. A program that relied on `all`
  failing compares the two numbers. Every error for a write carries
  `write_outcome` (`not_applied`, `applied` or `unknown`), as does a write
  answered `success: false` because the owner's store failed to apply it
  (`unknown`: its replicas hold it, so do not retry a push), and the CLI prints
  `stored on 2 of 3 copies` on stderr when a write is on fewer copies than its
  factor. See [docs/user/replication.md](docs/replication.md).
- **A `peek` on a fifo or lifo bucket or a queue field is answered by the
  bucket's owner**, whichever node the client is connected to, so it always
  shows the element the next `pop` takes. It is a read: it is no longer written
  to the log or held for the write concern.
- **A bucket is placed by its database and its name together**, not by its name
  alone. On a cluster that already holds data, most buckets move to a different
  node once, on their own, at the first start of the new build.
- **`search` on a field with no index scans the bucket** instead of refusing, and
  its response reports `none — full scan`.
- **A node chooses its memtable size once**, from the memory it is allowed: 32 MiB
  below 3.5 GiB, 64 MiB from 3.5 GiB and 128 MiB from 7 GiB.
- **`define bucket` is `create bucket`.** The old statement is refused on purpose
  and says what to use.
- **A node removed with `remove-node` stays removed.** It refuses writes after a
  restart as well, is told of its removal as the removal commits when it is
  reachable, and keeps the copies it holds, which the members can copy from it:
  empty its data directory once they hold its buckets. See
  [docs/user/replication.md](docs/replication.md).
- **The zero-configuration start** creates `./data` beside where you run it, writes
  a root credential there once, and generates a self-signed certificate.
- **Three refusals carry the right error code** instead of `INTERNAL`: a database
  that does not exist is `NOT_FOUND`, a request the user is not granted is
  `PERMISSION_DENIED`, and a read sent to a node that does not hold the bucket is
  `UNAVAILABLE` (the message still names the node that does). The messages are
  unchanged. A script or client that treated `INTERNAL` as the sign of these
  refusals should branch on the new codes.
- **`create database` on a name that exists is refused with `ALREADY_EXISTS`**,
  where it used to answer a reply that said it had failed and carried no code.
  Creating a bucket that already exists, with the same type, is still a success.
- **A request over the 32 MiB frame limit is refused with an error that says so**
  instead of resetting the connection. The Go client library refuses it before
  sending, and the server answers one that arrives anyway with
  `INVALID_ARGUMENT` and keeps the connection (see
  [docs/user/limits.md](docs/limits.md)).
- **`delete` of a key that does not exist answers `NOT FOUND`** on hash and btree
  buckets, as it did on blob buckets, where it used to print `OK  deleted`. On the
  wire the reply is `success: false`, `document not found`, `write_outcome:
  not_applied`; a delete is still safe to repeat and still reaches every copy.
- **`create bucket` on an existing name with a different store type is refused**
  with `ALREADY_EXISTS`, and the message names the type the bucket has. It used to
  answer success, with the existing type only in the message text. The same
  create with the same type is still a success, so a setup script can run twice.
- **A btree `range` over keys that look like numbers compares them as strings**,
  which is how they are stored. Before, a bound such as `2` was read as the number
  2: `range 2..5` over the keys `1 2 3 5 10 20` found nothing, and `range 2..` found
  every key including `1` and `10`. Keys now come back in byte order (`10` sorts
  before `2`); write keys with leading zeros to one width for numeric order. Keys
  that do not look like numbers, such as dates, are unaffected.
- **`migrate` on a node that already holds the bucket says nothing was pulled**,
  where it used to answer `is now served by` and change nothing. The reply names
  what to do for a replica that is behind. On a node that holds nothing, `migrate`
  still pulls the bucket and says `is now served by`.
- **The CLI reports an error when a server answers a miss with no reason.** A
  server built before the fix (`2a2c8e0f`) answers a `find`, `pop` or
  `peek` miss that way, and the CLI now exits non-zero where it used to print
  `NOT FOUND`. Upgrade the server; the error says so. No release is
  tagged yet, so the release that carries the fix will be the first one.

### Security

- **Credential files in the data directory are now sealed when encryption at rest is on; existing
  files are sealed on first start.** The cluster token (`cluster.token`), a standalone node's
  token-signing key (`auth.token.key`) and the TLS private key (`keys/server.key`) are sealed
  under the node master, so a copy of the data directory alone no longer holds them in a usable
  form. A node that finds them in plaintext seals each in place, keeping the same token, key and
  certificate fingerprint. A sealed file the node master does not open stops the start with the
  file and the fix, and nothing is generated over it; a sealed `cluster.token` cannot be copied
  to another node (give it `HOARDDB_CLUSTER_TOKEN` instead). `get-token` and `rotate-token` open
  the file with the encryption settings of the CLI's configuration, so run them with the
  server's. The `encryption` verb's "not sealed" list no longer names these files. **This is
  one-way:** do not start a sealed data directory with a build that cannot read the
  sealed TLS key and replaces the sealed `cluster.token` with a newly generated plaintext one. On
  an encrypted node a TLS key with no `keys/server.crt` beside it is now refused rather than
  replaced, and a credential file that is neither sealed nor valid plaintext stops the start.
- **A member's connection speaks for one member.** A node records the member
  that the first request on an internode connection names as its sender. It
  refuses any later request on that connection that names another member:
  gossip, replication, membership changes, the start of a bucket copy and
  versioned reads. Nothing of a refused request is applied. A node still accepts
  any process that holds the cluster token as a member, so keep the token on the
  members only.
- **A node limits how much of a bucket copy it holds in memory.** A copy from
  another node announced as larger than half of the receiving node's memory
  limit is refused before any of it is fetched, and a piece larger than the
  copy's piece size is refused when it arrives. Refused copies write nothing.
- **A copy of a bucket taken from another node is held to the 100-level nesting
  bound before any of it is written.** Every document, queue or heap entry and
  store-field element in the copy is checked as a client's write is; a copy
  carrying one nested deeper is refused whole and writes nothing; the node that
  asked for it logs the refusal and asks again later, as after any failed copy.
  A compact read
  (`GET_FAST`) answered by another node is checked the same way before it
  reaches the driver. Nothing a client can store is refused.
- **A first start shows a secret only on a terminal.** The log line about the
  cluster token names the file it is saved in, and nothing else. The one-time
  banner shows the token only when the server's stderr is a terminal; to a
  journal, a container log or a file it prints where the token is saved
  (`<data dir>/cluster.token`, mode 0600) and how to read it with the CLI's
  `get-token`. Either way it says that a joining node takes the token in
  `HOARDDB_CLUSTER_TOKEN`.
- **Changed, what to do: an unattended first start needs a root password.** A
  first start with no terminal and no `HOARDDB_ROOT_PASSWORD` (or
  `HOARDDB_ROOT_PASSWORD_FILE`) used to generate a password and print it; it
  now refuses, writes nothing, and names the fix. Set `HOARDDB_ROOT_PASSWORD`
  before the first start, or start once from a terminal and type one. A start
  from a terminal still asks, and the Docker image already required the
  setting.
- **A wrong cluster token is refused from memory.** A node reads its
  `cluster.token` file again only when the file has changed since it last read
  it, as a rotation does, rather than on every refused token. An unused
  internal join path that compared tokens separately is gone.
- **`show databases` lists only the databases you hold a role on.** A role on
  `*` lists them all, as before. A user with no role on any database gets an
  empty listing that says so, and `use` answers for a database the user holds no
  role on as for one that does not exist.
- **`nodes` needs `root` or `cluster-manager`.** The node list names every
  member's address; any other user is refused with `PERMISSION_DENIED`, naming
  the role to ask for. `status` is unchanged and open to every user. The
  session banner shows `members unknown` to a user without the role.
- **A `user-manager` works within its own roles.** A user administrator who
  does not hold `root@*` gives only roles it holds itself, on a database it
  holds them on, and changes only accounts whose roles it holds. Anything else
  is refused with `PERMISSION_DENIED`, naming the grant and saying to ask a root
  user. **What to do:** give an account administrator the data roles it hands
  out (for example `user-manager@*` plus `db-manager@app`), or make those
  changes as root.
- **Dropping, disabling or changing the password of a user ends that user's
  connections on every node.** The node that takes the change closes them at
  once, and every other node closes its own as it applies the change, normally
  within a second. A request on such a connection is answered
  `NOT_AUTHENTICATED` and the connection is closed. A role change still applies
  at the next login.

- **A fault while handling one request no longer ends the server process.** A panic in the
  code that serves a request, on the driver port or on the internode port, used to stop the
  whole node and every connection on it. Now only that request fails: the client gets an
  `INTERNAL` error that carries a fault id and says to report it, its connection is closed
  (requests it already had in flight answer first), and every other connection carries on. The
  server logs the fault at error level with the same fault id, the panic value and the stack, so
  a fault is never hidden. A fault in a goroutine a handler starts for itself, or in the code that
  reads a frame before a handler runs, is not covered.

- **The CLI shows names and messages a server sends without letting a terminal act on them.**
  A database, bucket, field, user, node or key name, or a server's message, that held a control
  character or an escape sequence used to reach your terminal as written, and the terminal acted
  on it instead of showing it. Now the CLI prints such a name
  quoted, as `"a\x1b[2Jb"`, and escapes the same characters inside a message, in `show`,
  `describe`, `status`, `nodes`, user and encryption listings, blob and document output, error
  lines, `dump`, `restore` and `ingest` reports. Newline and tab in a message are kept. A name that
  holds a control character is left out of Tab completion. A document is still printed as JSON,
  and the characters JSON leaves as they are (DEL and the C1 controls) are written as `\uXXXX`.
  Plain names print exactly as before.

- **`dump` writes files only its owner can read.** Every file a dump creates
  (`manifest.json`, each bucket's `.bson`, `.stores.bson`, `.dead.bson` and
  `.metadata.json`) is now mode 0600, and a directory the dump creates is 0700,
  whatever the umask. Before, bucket files were created 0666 and the JSON files
  0644 before the umask. A file left by an earlier dump in the same `--out`
  directory is rewritten and narrowed to 0600. A symbolic link in the place of
  a dump file is refused with a message naming it, and nothing is written
  through it. A directory that already existed keeps its mode; the files inside
  it are private either way.

- **What a login attempt writes to the server log and the audit log is bounded.** A username or client name repeated by a failed or refused login is
  cut to 64 bytes and marked with `…`; the connect line lists at most 16
  capabilities, each cut the same way, and counts the rest. A successful login
  is recorded under the account's full name, as before.
- **A failed password login takes as long whether the account exists, is disabled
  or does not exist.** The answer to the client is unchanged
  ("invalid username or password"); each such attempt now uses one derivation slot
  for as long as a real check.
- **The per-address connection cap is counted when a connection is accepted, before
  TLS.** `HOARDDB_TCP_MAX_CONNS_PER_IP` (default 64) now holds for
  connections that have not finished their handshake, so one address cannot take
  more than its share of `HOARDDB_MAX_CONNS`. At most 64 refusals are in progress
  on a port at once; past that a refused connection is closed without the
  `OVERLOADED` message. Nothing to configure.
- **Before login the server serves only AUTH and PING, on the node the client
  connected to.** Any other opcode sent before login, the reply opcodes
  `AUTH_OK`, `AUTH_FAIL` and `PONG` included, is refused with `NOT_AUTHENTICATED`.
  A login is always answered by the node it reached, whatever fields its payload
  carries, and a request whose opcode is not scoped to a database or bucket runs
  on the node that received it.
- **A connection that has not logged in may send requests of at most 64 KiB, and the
  server holds memory for a request only as its bytes arrive.** AUTH and
  PING, the only requests served before login, fit well within the limit; a larger
  request before login is answered with `INVALID_ARGUMENT`, naming the limit and
  saying to send AUTH first and wait for `AUTH_OK`, and the connection is closed.
  After login the 32 MiB frame limit is unchanged. Nothing to configure.
- **The root password is no longer kept in plain text; only its hash is
  stored.** A first start with no `HOARDDB_ROOT_PASSWORD` used to write the
  generated password to `<data dir>/root.password` (0600) and read it on every
  start. Now a first start from a terminal asks for the password twice, a first
  start with no terminal generates one and prints it once, and in both cases
  only its hash is kept, in `users.json`; `root.password` is never written. On
  upgrade, the first start converts an existing `root.password` (seeding the
  root user from it if `users.json` was never written), deletes it and logs that
  once; a failed delete is a warning with the path, and the next start retries.
  Copies and backups taken before the upgrade still hold the file. New switch
  `HOARDDB_REQUIRE_ROOT_PASSWORD` (YAML `auth.require_root_password`, default
  `false`) makes a first start without a supplied password refuse, naming
  `HOARDDB_ROOT_PASSWORD`; it is the recommended production setting, and the
  Docker image sets it. `auth_bootstrap_root: false` no longer generates and
  prints a password it never uses.
- **Changed, what to do:** the CLI no longer reads `./data/root.password` when
  it is given no credential; pass `-user admin -password` (a bare `-password`
  asks on the terminal) or set `HOARDDB_ROOT_PASSWORD`. A container must be
  given `HOARDDB_ROOT_PASSWORD` (or `HOARDDB_ROOT_PASSWORD_FILE`) for its first
  start. The compose files no longer generate a password into the `pki` volume
  (`/pki/root.password`): export `HOARDDB_ROOT_PASSWORD` before
  `docker compose up`; every node is given it. Scripts that read
  `root.password` to log in must supply the password instead.
- **A server is found in `known_servers` under any spelling of its address**.
  Addresses are recorded and looked up in one canonical form: names lowercased with one trailing dot dropped, IPv6 addresses in their short form,
  and the port always written (7433 when the address has none). So
  `DB.example:7433`, `db.example.:7433` and `db.example` are one server, and a
  different key under any of those spellings is refused as a changed key instead
  of being offered as a new server. `localhost` and `127.0.0.1` stay two
  different servers. A recorded entry written in another spelling is still found;
  nothing in the file has to change.
- **`known_servers` is no longer rewritten wholesale.** Trusting a new
  server appends one line, under a lock and after reading the file again, so two
  CLI sessions trusting two servers at once keep both, and comments, blank lines,
  notes after a key and lines hoardDB cannot read are kept as you wrote them, in
  their order. A process reading the file while another saves it may see the line
  being added cut short; such a line cannot be read as an entry, so it is skipped,
  and the reader still sees every entry saved before that save and never a wrong
  key. hoardDB keeps a `known_servers.lock` file beside it for the lock.

- **`create user` and `alter user <name> password` refuse a password shorter
  than the server's minimum** (`HOARDDB_PASSWORD_MIN_LENGTH`, 12 by default).
  They used to accept a password of any length, although the minimum was
  documented. The server reports its minimum on every login, as the new
  `AUTH_OK` field `password_min_length`, and the CLI checks the new password
  against it before hashing it; nothing is created or changed when it is too
  short. The server still never receives the password, so it cannot check it:
  a program that builds its own user-creation request is not measured. A CLI
  connected to a server that does not report a minimum refuses to set a
  password; upgrade the server. Set the same minimum on every node of a
  cluster.

- **The server listens on this machine only by default.** The client port
  (`127.0.0.1:7433`), the node-to-node port (`127.0.0.1:4434`, derived from
  `HOARDDB_LISTEN`) and the metrics port when enabled (`127.0.0.1:9090`) used to
  bind every interface. To accept clients from other machines set
  `HOARDDB_TCP_LISTEN=0.0.0.0:7433`; for a cluster across machines set
  `HOARDDB_LISTEN` to each node's own address. A node whose seeds are on another
  machine but which is still on the loopback default is refused at start, with
  the setting to change in the message. The start-up log says where the node
  listens. The Docker image sets all three to `0.0.0.0`, so `docker run -p` and
  the compose file work unchanged. See
  [docs/user/security.md](docs/security.md#default-network-exposure).
- **A replication-path issue is fixed.** Upgrade all nodes together: a node not yet upgraded
  cannot pull data from an upgraded node.
  Details will be published after the fix has shipped in a release.
- **An encrypted node no longer sends data to a node that is not encrypted**, on
  any path that moves data between nodes, and refuses to take its membership from
  one when started in `require` mode.
- **Resetting the root password removes the saved bootstrap password file**
  instead of leaving a stale secret on disk.
- **Failed logins from one address are slowed, then refused for a minute**, and a
  server that is merely busy no longer counts against the address. Only a
  successful password login clears the address's count.
- **AUTH failures are counted before the reply is sent**, so the attempt limit
  holds for back-to-back retries.
- **AUTH attempts in flight together now count against the attempt limit**, so
  logins sent in parallel from one address, or pipelined on one connection, are
  held to the same limit as logins sent one after another.
- **A node that has no auth config yet no longer counts logins against the
  address.** A clustered node still waiting for its first config, or a
  follower past the staleness bound, refuses logins and token refreshes before
  it checks any password. Those refusals used to count as failed logins, so
  retrying correct passwords against such a node could lock the address out for
  a minute once the node was ready. They are no longer counted.

### Fixed

- **The not-master refusal, `master` and the `user list` header name the address a client can connect to.** A user change sent to a node that is not the cluster master is refused with "run the same
  command against" the master's address, and that address was its cluster address, which nothing answers a
  client on. It is now the master's driver address (`tcp.listen`). Each node sends its own driver address in
  gossip, and a node whose master has not sent it yet says so, with what to do, rather than print a wrong
  address; `UserListResponse.master_addr` is empty in that case. A node on `0.0.0.0` is shown on the host of its
  `server.listen`. A master that listens for clients on loopback only (the default) while its cluster address is
  elsewhere is not offered as an address: the refusal says what to set (`tcp.listen`), and `UserListResponse` gains
  `master_addr_note` with the reason. The wire additions are optional and additive: `master_addr_note` on the user list answer, and
  a `driver` field on each node's own record in gossip and in the member lists of node listings, which an older
  node ignores and omits. Nothing is stored: no data directory changes, and a cluster with a node from before this
  change shows no master address until that node is upgraded.
- **A write-ahead log's group commit marks every entry its fsync covered as synced, and a writer no longer
retries an fsync that failed.** The leader of a group commit used to record only its own entry, so a
waiter whose entry was already on disk ran another fsync; and when the leader's fsync failed, a waiting writer
ran one of its own, which on Linux can report success for pages the kernel had dropped.
- **A failed log rotation no longer consumes a sequence number or leaves the log without an active file.**
- **A failed fsync during a log rotation poisons the log, as a failed group commit does.**
- **The log's open flushes its tail before it counts it as synced.** After a process crash the bytes
it read back can still be page cache.
- **A new log segment's name is made durable before anything is written to it.**
- **The log no longer trusts an unsynced state file for its active segment.** It derives the active
segment from the segments themselves, and `wal/state.bson` is gone.
- **Acknowledged writes no longer write hundreds of times their size to disk on newer kernels.** Each acknowledged write ends in a sync of the store's memory-mapped write-ahead log. On
  a kernel that caches a file mapping in large folios sized by the readahead window (seen on Linux 7.0 with ext4 and a large `read_ahead_kb`), every sync wrote whole folios back, up to a megabyte for a few KB changed, so seeding a dataset wrote hundreds of times its size. The store now advises the kernel that its log mappings are read at random (`MADV_RANDOM`), which keeps their folios at one page, and the same seed writes about its own size.
  On a Debian 13 test VM (Linux 6.12, already small folios) seed time and depth-1 write latency are
  unchanged. See [Configuration](docs/configuration.md#disk-write-volume-depends-on-the-kernel-and-filesystem).
- **A write to a bucket that does not exist now answers NOT_FOUND on a cluster.** A put,
  insert, push, delete, update, field-store write or blob write to a name no member holds was answered
  `UNAVAILABLE` with "this node ... has no local copy of it", the class a driver retries, so a mistyped
  name made a driver wait out its whole retry budget (about 20 s in the Go driver). The owner now asks the
  members, as an alter and a drop do, and answers `NOT_FOUND` with `write_outcome: not_applied` and the
  text a single node gives, whichever node the write was sent to. A bucket some member holds, one whose
  copy is in flight and one a member that did not answer may hold still retry. Reads of a missing bucket
  already answered `NOT_FOUND`. A write that finds the bucket held pays nothing new; a miss costs one
  question to the members.
- **An alter of a bucket that does not exist now answers NOT_FOUND.** On a cluster,
  `alter bucket` of a name no member holds was answered `UNAVAILABLE` with "this node ... has no local
  copy of it", the class a driver retries, so a mistyped name made a driver wait out its whole retry
  budget. The owner now asks the members, as a drop does, and answers `NOT_FOUND` with
  `write_outcome: not_applied` and the missing-bucket text every other verb gives, whichever node the
  alter was sent to. A bucket some member holds, and the refusals of a decrease or a placement, are
  unchanged and still retry.

- **`use <database>` works while a cluster member is down.** The CLI checked the database with the
  cluster-wide listing, which is refused while any member does not answer, so no database-scoped statement could
  run during a node loss. `use` now selects the database and prints a note naming the silent member and that the
  database could not be confirmed; a typo ends on the first statement that uses it. `show databases` and
  `show buckets` still refuse, as before.

- **A read during the removal of a running node no longer answers "not found" for a bucket that node is
  handing over.** At replication factor 1, a read at a bucket's new owner, or through another node,
  could answer `bucket "…" not found` with the note that the name was placed on the removed node, for up to about
  a second, while the removed node was still running and handing that bucket over. The new owner now waits for
  every bucket the removed node handed it: until the bucket lands, the removed node says it does not hold it, or
  the removed node stops answering for 3 seconds, a read answers the hand-off's retry naming the removed node,
  and the owner does not report the removal settled. No data was lost.

- **A read just after a remove-node no longer answers "not found" when the removed node's hand-off reaches
  the new owner before that owner has the new membership.** At replication factor 1, the removed node
  could ask its successor to take a bucket while the successor still placed the bucket on the removed node. The
  successor refused, recorded nothing, and the removed node still counted the bucket as handed over, so for about
  a second, until it asked again, reads of the bucket through any member answered `bucket "…" not found` with
  the removed-node note. The removed node now counts a bucket as handed over only when the new owner says it has
  taken it, and asks again otherwise. Until then, reads answer the hand-off's retry. No data was lost.

- **`dump` no longer warns that a cluster dump covers only the connected node.** A scopeless
  `dump` through any member lists every bucket of the cluster and reads each from its owner, but the CLI
  still printed "dump enumerates only the buckets the connected node holds locally" and the backup page
  told operators to dump each node in turn. The warning is gone and the page now says one dump holds the
  whole cluster. **What to do:** nothing; one `dump` through any node is the whole backup.

- **A filter over a limit is refused on an empty bucket, as it always was on a populated one.**
  The refusal was kept in the compiled filter and raised only when a document reached it, so
  `find({name: {$regex: "<more than 4,096 bytes>"}})` on a fresh bucket answered "no matches", and
  `count` answered `0`, while the same query was refused once the first document existed. Every
  filter-size limit in [Limits](docs/limits.md) is now answered when the filter is compiled, before
  the scan, for find, search, count, distinct and a multi-document update. The regex work on one document
  is still charged per document, which is what it measures, and a malformed clause keeps its old answer.

- **The CLI no longer prints `stored on 2 of 3 copies` on every write of a healthy cluster.** At the
  default `majority` concern the owner answers as soon as two of three copies hold the write, so the note
  appeared on nearly every insert of a healthy cluster and taught developers to ignore it. The note now appears
  only when fewer copies than a majority of the factor hold the write (1 of 3, 2 of 5). The cost: a write on 2 of 3
  copies with one member out is silent too; `status` and `nodes` show the member, and `copies_confirmed` in the
  response still carries the count.
- **Discarding a batch or a snapshot opened before the store closed no longer hangs.**
  A batch or snapshot that began before shutdown and was discarded after it posted its read timestamp
  to a channel that no longer had a reader: the first hundred such discards returned, and the next one
  waited forever, with no context able to cut it short, so a shutdown that raced one leaked a goroutine.
  A discard after close now returns at once and skips the work Badger would have done, because a closed
  store has nobody left to release the timestamp to and the transaction can never be committed. Nothing that was
  unacknowledged becomes durable, and a normal stop does not hit this.

- **A stop no longer closes the node store under a membership change that is still being written.** Shutdown waited at most 3 seconds for the membership recovery, the auth-config loop and
  the peer verifier, though all three write the node store, then closed the store beneath whichever one
  was still inside a write: the missing record was repaired at the next start, but the write was refused
  and the placement pass it began was cut short. They are now drained like every other store writer
  before the store closes, and one that is slow is logged every 3 seconds while the stop keeps waiting.
  A stop can take a little longer than before, by the write already in flight and no more; replication
  traffic and peer requests still cannot hold it up.

- **An authorized request no longer copies the session's roles, databases and grants, or decodes its
  routing fields with a decoder of its own.** Every authorized request read the session
  identity through a deep copy of all three lists, three allocations per read, and the routing scope
  check decoded the request payload with an inline copy of a decoder the rest of the server already
  shares. The identity is now published as an immutable snapshot and read without copying, and the
  scope check calls the shared decoder, which takes an authorized scoped request from 16 allocations
  to 13. No answer, no wire message and no stored byte changes; the payload is still decoded once for
  the gate and once by the handler.

- **The short root password refusal now names a remedy the server can carry out.** A first start that seeds the
  root user from `HOARDDB_ROOT_PASSWORD` below `HOARDDB_PASSWORD_MIN_LENGTH` said to unset the setting "so the
  server generates one"; no code path generates a root password, so an unattended start that followed the advice
  refused again. The refusal now says to set a longer password, or unset it and start `hoarddb-server` from a
  terminal to type one, and the same advice in the handbook and the user pages is corrected.

- **Two replicas can now pull the same bucket from its owner at the same time.** The owner kept one snapshot
  per bucket rather than one per member pulling it, so a second replica's pull replaced the first one's
  snapshot: the first replica's next chunk was refused, and whichever pull finished first dropped the other's
  snapshot, delaying replication and making an operator's `migrate` fail. Each puller now has its own snapshot,
  a pull's done clears only its own, and an abandoned pull's snapshot is freed after an hour. A refused pull
  still recovers on the pending-copy worker's next sweep, or on a second `migrate`.
- **Using the node store after it has closed now returns an error instead of hanging or crashing the
  process.** A transaction, snapshot or sync that reached the store after shutdown waited for a commit mark
  nobody would serve, or panicked on a closed channel; a test run once hung, and a late
  shutdown goroutine could take the process down. Every use after close now fails at once with "the node store
  is closed because this node is shutting down", while a commit already in flight is allowed to finish first.
  Acknowledged writes are still durable, and a normal stop does not hit this.
- **A restore refused because ownership of its bucket is moving now answers `UNAVAILABLE`, not `INTERNAL`.**
  The hand-off refusal was flattened to its message on the way out, so the retryable class was lost and a
  driver was told not to retry a restore that would succeed a moment later. The refusal now keeps its own
  class and its "Retry in a moment" sentence, and a restore start that does not fit the free disk space is
  `UNAVAILABLE` too, where it was `INTERNAL`. A restore refused for a name, index or field the caller can fix
  answers `INVALID_ARGUMENT`.
- **A dump holding a heap dead entry, or a `blobStore` field element, at the maximum nesting depth now
  restores.** The dump wraps a record in levels of its own, but restore checked every record against one flat
  limit, so a document nested the full 100 levels was refused with "nests more than 100 levels deep" about a
  document the server itself had accepted and dumped; the restore stopped there and left a partial bucket.
  Each record kind now gets a legal document plus its own wrapper, a record one level past that is still
  refused, and a dump taken by an older build restores.
- **A bucket dropped and created again under the same name in one rebalance round is no longer left out of
  that round's lists.** A round remembers each name a drop took off its lists, so it treated the new bucket as
  the dropped one, recorded no residue and marked the node settled while the new bucket's hand-off was still
  unfinished; the pause on schema changes then lifted early and nothing told the operator. The note now
  carries the dropped bucket's identity, and a create or pull of the same name takes it off, so the new bucket
  stays on the retry list and the node stays unsettled until it is handed off. No data was lost; the cost was a
  hand-off that waited for the next membership change.
- **A bucket dropped while a rebalance pass is running no longer comes back on the node's unsettled lists.**
  The pass held an older copy of the retry list, so it wrote the dropped name back after the drop had cleaned
  it; the node then retried a bucket that no longer existed, kept reporting that it had given up handing off a
  bucket, and held the join freeze open until a restart. A drop that commits during a round is now remembered
  for that round, and the round leaves the name out of both its retry list and its residue.
- **`-import-key` now refuses a node master that cannot open a sealed `users.json`.** It checked the
  imported master against the store key, the registries and the credential files, but not against
  `users.json`, so a wrong master could replace the right one and leave every user unreadable. The
  refusal names `users.json` and the existing wrapped master is left in place.
- **`-reset-password` now works on a node with encryption at rest.** It used to stop with
  "encryption.keystore is off" even when the keystore was configured, because it never opened the node
  master that seals `users.json`. It now opens the node master from the configured keystore and keeps
  the file sealed. A sealed `users.json` with no keystore configured is refused naming
  `HOARDDB_ENCRYPTION_KEYSTORE`, `HOARDDB_ENCRYPTION_KEY_FILE` and `HOARDDB_ENCRYPTION_MODE`.
- **Restoring a large LIFO bucket no longer holds the whole bucket in memory.** A LIFO restore
  kept every document of the dump in memory until the restore finished, so a bucket larger than
  the node's free memory could run the node out of memory. The documents now wait in a spill
  file in the data directory, sealed under a key that exists only in memory and removed when the
  restore ends, and memory stays at one document. The restored bucket pops in
  the same order as before. The disk headroom check of a LIFO restore now also counts the spill
  file, about the dump's size again, and a start removes a spill file left by a node that stopped
  during a restore.
- **A restore refused for something the caller can act on no longer answers `INTERNAL`.** A dump of another
  format version, a database or bucket name that is unsafe as a path, a dump whose store type differs from
  the existing bucket's, a `--drop` that would destroy blob payloads and a `restore chunk` or `restore done`
  for an unknown session now answer `INVALID_ARGUMENT`: change the request, retrying it fails the same way.
  A bucket that is being dumped or is being restored by another restore answers `UNAVAILABLE`: retry with
  backoff once the other operation ends.

- **A node started with `HOARDDB_CLUSTER_TOKEN` on a data directory that does not exist yet now
  keeps that token.** It used to fail to save it, log a warning and generate a different token,
  which no other member holds. The directory is now created first, and a token that cannot be
  saved stops the start. `rotate-token` likewise refuses, and changes nothing, when the current
  `cluster.token` cannot be read, instead of replacing it.
- **A dump holding a heap dead-letter entry at the maximum nesting depth now restores.**
  A value nested the full 100 levels, dead-lettered in a heap bucket, made the restore stop
  with "nests more than 100 levels deep" about a document the server had accepted. The same
  held for the metadata of a `blobStore` field element at that depth. Restore now allows
  each kind of dump record a legal document plus its own wrapper, and still refuses a
  record deeper than that.
- **A node no longer deletes its copy of a bucket before the copy that replaces it
  has arrived.** A replica copying its bucket afresh from the owner, and
  an owner taking a bucket over its own older copy, deleted that copy first, so a
  copy that then failed to arrive (a network error, the source refusing, or the
  copy failing this node's checks) left the node holding none until a later copy
  succeeded. The node now fetches and checks the whole new copy first, keeps
  serving reads from the old one meanwhile, and replaces it only once the new one
  has arrived and passed its checks; a refused copy leaves the old one as it was.
  Both copies are on disk for a moment, so the node also refuses, before it deletes
  anything, a copy that would take a filesystem past `disk_high_water_pct` beside
  the one it holds, and the refusal names the node, the bucket and the fix. An
  owner taking a bucket over whose chosen holder sends a copy it refuses now tries
  the other holders of an equally complete copy.

- **The cluster token file is written atomically, and a token that cannot be read is no longer replaced.**
  A node that finds a `cluster.token` it cannot read or parse now stops with the file's path and how to restore
  it (`get-token` on another member, or fix its permissions) instead of minting a new token. `--export-key`
  creates its file with mode 0600 and refuses a path that already exists or is a symbolic link.

- **A restore that meets a bucket hand-off in progress is now told to retry.** It was
  refused with code 5, INTERNAL ("report a bug"); the refusal's own class, UNAVAILABLE,
  with its "Retry in a moment" sentence, now reaches the operator and the drivers, as
  it does for a plain `create bucket`. A restore refused for a bucket or database name
  over the limit, a control character in a name, an index or a field, a reserved
  database name or an unsupported store type now answers INVALID_ARGUMENT, and one
  that does not fit the free disk space answers UNAVAILABLE; all answered INTERNAL.
  A restore chunk refused for a document write carries that write's class, and
  `declare` of store fields with a control character in a name answers
  INVALID_ARGUMENT instead of INTERNAL. The messages are unchanged.

- **An offline password reset now ends the user's open sessions on the other nodes.**
  `reset-password` already replaced the credential, but a node that received the reset
  users file kept that user's sessions open until they expired. They now end when the
  node applies the file, as they do after an online password change.

- **A cluster token generated at first start no longer expires.** It was stamped
  to expire 24 hours after the first start, and nothing renewed it, so a cluster
  built on it stopped accepting its own members a day later. A `cluster.token`
  already on disk with an `expires_at` is now honoured as never expiring, so a
  cluster running on one recovers without an operator step. `get-token` prints
  `Expires: never`.
- **`rotate-token` refuses on a cluster member, and its note no longer
  promises a grace period.** It rewrote only the local `cluster.token`, sent the
  new token to no other node, and kept no grace window, so a rotated member was
  cut off from the rest; restarted, it could not open its `users.json`, which is
  sealed with keys derived from the token. On a member it now refuses, changes
  nothing and says how to change the token by rebuilding the user store. On a
  node outside a cluster it still rotates, and says that the old token stops
  working at once. A member started on a cluster token other than the one its
  `users.json` was sealed under now says that its cluster token changed and how
  to put it back, instead of blaming `auth.token.key`.

- **An alter refused for a reason a retry clears is `UNAVAILABLE` whichever node you asked.** Lowering a
  factor before a survivor has shown it holds what the demoted copy holds, an alter while the owner's placement pass is
  unfinished, one that places a member still copying, and one while a member runs a version without `bucket_rf_alter` all
  reach a client as `UNAVAILABLE` with `write_outcome: not_applied`, as they did at the owner. Through any other node
  they arrived as `INTERNAL` with `unknown`, which a driver must not retry. The message is now the owner's sentence
  without the `owner <id> refused ALTER_BUCKET` prefix. The driver vectors gained "alter bucket: refused, retry later" and
  "claim of an entry over the hard cap".

- **`hoarddb ingest` no longer calls a resume idempotent after a lost reply on a batch of documents without an
  `_id`.** A document with no `_id` gets a new server-made key every time it is sent, so the plain
  resume that ingest told you to run stored an applied batch a second time. A lost reply on such a batch now
  stops with the batch in doubt (exit 8), says to read `db.<bucket>.count()`, and a plain resume is refused until
  you pass `--from-offset <end of batch>` to skip it or `--accept-duplicates` to send it again, as for a fifo or
  lifo bucket. A batch whose every document has an `_id` still resumes by itself. The resume file's `in_flight`
  gained `all_client_ids`; a resume file written by an earlier ingest has none and is read as "may hold documents
  without an `_id`", so its in-flight batch gets the same refusal once. The JSON report's `idempotent` is false
  for a run that stopped on such a batch.

- **`hoarddb ingest` no longer halves and re-sends a queue batch, or a batch of documents without an `_id`,
  after a cluster owner's `BATCH_TOO_LARGE`.** That refusal says `write_outcome: unknown`: a member
  with a larger `storage.memtable_mb` may have applied the batch, so the halves stored every entry a second
  time on it, and ingest reported them applied. The run now stops with the batch in doubt (exit 8) and says
  to read `db.<bucket>.count()`, then resume with `--from-offset` to skip the batch or with `--batch` (and
  `--accept-duplicates` on a queue) to send it in halves. A batch whose documents all carry an `_id`, and any
  batch refused `not_applied` (a standalone node), is still halved. A newer `hoarddb ingest` also halves again
  on an older server's in-band `success: false` refusal, which it had stopped on.

- **On a sealed cluster, re-creating an existing bucket at a larger factor no longer says it would replicate
  to a plaintext peer.** The owner judged the wider set of the declared factor before it found the
  bucket exists. It now answers as an unsealed cluster does: the create succeeds, says the bucket already
  exists and that its factor was not changed. A create of a new bucket that reaches a plaintext member is
  still refused.

- **A range walk or a count no longer copies a key or a value it never reads.** Badger hands an iterator an item's key and value in buffers it
  recycles once the iterator advances, so the storage layer copied key and value
  for every entry it visited, and `count` additionally copied both for every
  document it only counted (two allocations per document). The key is now copied
  only when a caller asks for it, into one buffer per iterator reused from key to
  key (the copy a caller keeps, as `range` returns it, is made once, as before),
  and a count reads neither key nor value. A walk that reads nothing, and a `count`, now allocate a fixed small amount instead of once per key. No answer, no
  wire message and no stored byte changes.

- **A create of a recreated name is no longer refused after 5 seconds when a hand-off or a
  migrate waits on the same name.** When the take-over of a name found no live copy
  anywhere, the first caller waiting on it took the answer, and a create waiting at the same
  time never heard it: it waited out the write-concern timeout and was refused `not_applied`
  with "ownership ... is moving to this node ... Retry in a moment". Every waiter now hears
  the answer; nothing was stored or lost before, and the retry succeeded.

- **`requeue({all: true})` reaches the dead entries behind a head of refused ones.** A request considers 100 entries, refused ones included, and
  always started at the head of the dead list, so 100 heap entries refused
  because a live element held their key starved everything behind them: the
  request answered `done 0` and the CLI stopped without printing what was left.
  `DEAD_REQUEUE` and `DEAD_DISCARD` take an optional `after` (a `dead_id`,
  exclusive, as `dead` pages), the CLI sends the last refused `dead_id` as
  `after` when a request moved nothing, and every `all` run prints
  `requeued N, left M`. A request without `after` behaves as before.
- **A lost forwarded `RESTORE_CHUNK` or `RESTORE_DONE` says its write outcome is
  `unknown` and gives restore advice.** A restore travels to the
  bucket's owner like a read so it is never replayed as one WAL entry, but the
  owner applies every document a chunk carries: a lost answer may therefore have
  changed the bucket. The error carried no `write_outcome` and told the caller to
  read it back before retrying a push or an `$inc`, which a restore cannot use.
  It now says `unknown`, and a refused restore says `not_applied`, as any other
  write does. The advice is to check the bucket before re-running with
  `db.<bucket>.count()` (after `use <database>`) or
  `dump -db <database> -bucket <bucket>` in the shell, because re-running a restore merges into
  what landed while a fifo, lifo or heap field element may be pushed twice. A
  lost read still stores nothing and says so, and a lost push keeps the
  read-back advice.

- **A session token lives at least its configured TTL.** The mint
  truncated its expiry to the whole second, so a token lived up to one second
  less than `auth.token_ttl_seconds`: with `auth_skew_seconds` at 0 and a TTL of
  1 s, a login late in a second was refused about 100 ms later. The expiry is
  now rounded up to the next whole second, so a token lives its TTL (at most one
  second more). The claim is still whole seconds, the clock-skew handling is
  unchanged, and nothing on the wire or on disk changes; with the default 60 s
  skew the defect was not observable.

- **A drop whose leftover in-memory record cannot be released no longer tells
  the user to retry it.** The refusal fires after the drop's batch has
  committed, so the catalog no longer holds the bucket; a drop sent again after
  the restart the answer used to recommend answered `bucket ... not found in
  database ...`, because there was nothing left to drop. The answer now says the
  drop is already complete, that restarting the node clears the record, that
  `show buckets in <db>` will no longer list the bucket, and that no retry is
  needed.

- **The first put to the new owner of a bucket a removed node held now gets the
  hand-off, not "never created".** After a removal that left two or
  more members, the first put sent to the bucket's new owner, before its
  take-over had started, was refused with `either the bucket was never created
  ... or rebalancing has not moved the data here yet (run 'migrate ...')`; the
  second put got `ownership of <database>.<bucket> is moving to this node ...
  Retry in a moment`. The first put now waits up to 200 ms for the take-over to
  say so and gets the hand-off (`write_outcome` `not_applied`; nothing was
  stored). A name no node holds is still answered the fence's text after the
  wait. A bucket the owner holds pays nothing.
- **A pull in flight is again answered "not yet", and a write racing a drop is
  no longer told ownership is moving.** The change above
  also answered the hand-off (`handoff_in_progress`) for any unheld bucket a
  pull held, so a joiner's pull was refused as a take-over from a removed node
  and writes racing a drop were refused with "Retry in a moment". Only a name a
  removed node owned gets the hand-off; a pull answers `awaiting_copy` and a drop
  answers success or not found, as before.
- **A node a removal left alone no longer tells a client to create a bucket that
  exists.** A put, get or other data op sent to the survivor for a
  bucket only the removed node held, before the survivor took it over, was
  answered `bucket not found ... to create it`. It now names the bucket to the
  take-over and answers `ownership of <database>.<bucket> is moving to this node
  ... Retry in a moment` (a put keeps `write_outcome` `not_applied`; nothing was
  stored). A bucket that exists nowhere is still not found, with its create
  advice, after a wait of up to 200 ms for the take-over to say so. A bucket the
  survivor holds pays no extra allocation.
- **A node a removal left alone no longer reads or writes its replica copy of
  the removed owner's bucket before it takes the bucket over.** At a
  replication factor of 2 or more the survivor holds a copy of each bucket the
  removed node owned, and until the take-over lands the removed node's last
  writes that copy can be behind. A read in that window answered from it (an
  old value, or `document not found` for a key the removed node had written), and
  a write was acknowledged and then replaced when the take-over landed the
  removed node's copy: the acknowledged write was lost. The survivor now answers
  as an owner of a larger cluster does: a write, drop or alter is refused with
  `ownership of <database>.<bucket> is moving to this node ... nothing was stored.
  Retry in a moment` and `write_outcome` `not_applied`, a create waits for the
  take-over, and a read is forwarded to the bucket's previous owner or refused
  with the same retry. Once the bucket is taken over, reads and writes are served
  as before, and a request then allocates nothing more
  than before the fix: a node that never had another member pays one atomic
  load, a node alone after a removal a few, and the membership history is read
  again only after a membership change.

- **After a removal that leaves two or more members, a create of a bucket only
  the removed node holds no longer makes an empty bucket of the name.** At a replication factor of 1 the removed node can hold a
  bucket's only copy, and the name's new owner did not know of it until the
  removed node asked it to take the bucket over; a create answered in that
  window made an empty bucket, and the removed node's acknowledged writes never
  reached the cluster. When a node removed within the membership history owned
  the name, the owner now asks for it before it creates, even with
  `auto_rebalance` off: a copy it finds is copied in and the create is answered
  `already exists`, with the writes kept; a name no node holds is created as
  before. A removed node that does not answer within 3 seconds is not waited
  for, so while a removed node is down a create of a new name waits for that
  question and is not refused for it. The same question now runs on a node a
  removal left alone, where a removed node slow to answer made the
  create fail with a retry that failed the same way each time.

- **A pull that spans a restart of its source records a position the source's
  new log really keeps.** A pull checked the source's incarnation once,
  when it asked for the bucket, and recorded the snapshot's position at the end
  without asking again. A source that restarted in between minted a new
  incarnation, its new log kept only part of the old sequence range, and the
  recorded position could name an entry the new log no longer has; the next
  handshake then rebased the copy, and the bucket was copied in a second time. A
  rebase of that source now lowers the position a pull in flight is about to
  record, so the record is the lowest of the snapshot's position and what each
  rebase's new log keeps. A position the new log keeps is still recorded, never
  dropped: dropping it would let the stream re-send entries the snapshot already holds, which a
  FIFO, LIFO or heap would apply twice. Nothing per bucket is stored; the cap
  lives only while the pull runs.

- **A bucket read at its owner reads its lineage once, not on every read.** The check that keeps a bucket's reads from being served before
  the node has taken it over asked the copy for its term, which the node keeps in
  memory, and on a miss it read the bucket's state document from the store on
  every read and never remembered the answer. A bucket that is read and not
  written — the read-mostly case — stayed in that state for as long as it was not
  written, so every owner read of it paid a store read, several times the cost of a hit and a large share of a small read's work, and a `consistent get` at the owner read the same document twice.
  The first miss now fills the memory the writers already fill, from a document
  read under the bucket's lock, so a later read answers from memory and a
  `consistent get` uses the document it already read. The read still never waits
  for the bucket's lock: when a writer holds it the read answers from its own
  document and the next read fills. A read now allocates nothing and is far cheaper; the one forced miss per bucket per start costs one store read. No answer changes.

- **A node removed from a two-node cluster forwards reads to the survivor.** Its ring held one member, so it answered gets, exists, counts
  and listings from copies that stopped changing at the removal: a key the
  survivor had deleted was still found, and a bucket the survivor had made was
  not listed. It now sends every read to the routed door, as a node removed from
  a larger cluster always did, and the read is answered by the member that owns
  the bucket. The cost is one atomic load per request and nothing per bucket.

- **A node removed from a two-node cluster refuses writes.** After
  `remove-node` on a two-node cluster, the removed node accepted puts, creates,
  alters and drops of buckets and databases, and answered them with a success:
  a put was held by no member, and a drop deleted the copy the removed node
  keeps because it may be the only complete one. It now refuses every mutation
  as a removed node of a larger cluster does: `this node was removed from the
  cluster at epoch N and no longer accepts writes; connect to <member>, a
  member, and send the write there`, with `write_outcome` `not_applied`.

- **On a node that a removal left alone, a create of a bucket the removed node
  still holds no longer makes an empty bucket of the name.** The
  create waits for the node to take the bucket over and is answered from the
  copy that lands (`already exists`), so the removed node's acknowledged writes
  are kept. A create of a name no node holds goes ahead as before. A node alone
  on its ring also now refuses database and bucket DDL while schema changes are
  paused, with the same refusal a node of a larger cluster gives.

- **A bucket created on a one-member cluster is stamped with an incarnation, a
  first term and an opened mark.** A node in cluster mode whose ring
  holds one node (a founder before any join, or the last member after
  `remove-node`) took the standalone door for a client's create, which minted no
  incarnation: the bucket kept incarnation 0, term 0 and no opened mark for its
  life. After a join, a consistent get of it could be refused as two
  incarnations, a late create of a dropped bucket of that name was not refused,
  and a drop and re-create at the lone node left two buckets of one name with the
  same number. The create now mints the incarnation, takes the membership epoch
  as its first term and is marked opened, as on any other owner. A bucket already
  created at incarnation 0 on a one-member cluster keeps it; drop and re-create
  it to give it one.

- **A push that lands on a bucket's owner keeps the sender's position, so a kill
  and a re-send no longer duplicate a queue element.** A stream push
  forwarded to the bucket's owner was logged under the owner's own origin, so
  the sender's sequence was kept only in memory until the next checkpoint. A
  kill in that window and a re-send applied the entry a second time: four
  acknowledged pushes to a FIFO or LIFO read back as eight. A push that carries
  a stream is now applied from the sender's sequence, which the log records and
  a restart reads back.

- **A failed blob put leaves no payload file and no key naming the blob.** When a blob's write failed after its payload was written, the
  rollback deleted the file but left the index entry naming it, so `info` and
  the bucket's count reported a blob that could not be read, and a later save
  could persist the entry. Its metadata and the key that maps to it are written
  in one apply batch: a refused write commits nothing, and its rollback removes
  the payload file.

- **A save of `known_servers` under contention is served in arrival order.** A save waited for the file's lock by retrying a non-blocking
  `flock` every 20 ms for 5 s, so a waiting process could lose every round and
  fail with "another process has held the lock ... for more than 5s". The wait
  is now a blocking `flock` queued in the kernel, and a process keeps at most
  one waiter per lock file however many of its saves are waiting. A pool of
  workers recording the same servers at startup no longer loses a save to a
  lock error; the 5 s bound and its message stay.

- **A client that is handed another request's answer closes the connection
  instead of using it.** The CLI's connection read the next frame on
  the wire without checking its request id, so an answer left behind by a
  timed-out call could be taken as the answer to the next request: a wrong
  result rather than an error. It now refuses the frame and closes the
  connection, saying `response carries request id X but the request sent Y: the
  connection is out of step (a stale answer to an earlier request), so it was
  closed; reconnect and retry`. A connection-level refusal, which carries an
  all-zero id, is still accepted.

- **The user reference now states the query semantics it left implicit.** `find` with no `limit` returns the server's default page of 100
  documents and prints `showing N of M` when more remain, while the shell's
  `search` sends no limit and returns every match. A `range` whose low bound is
  above its high bound is an empty range, printed `(no entries in range)`, not
  an error. A heap `push` with no `priority` is accepted at priority 0. A
  `RESULT_TOO_LARGE` answer carries `retained_bytes`, the bytes the query had
  already kept, absent when that is 0 — which is the case when the first
  document alone is over the ceiling, so no `limit` fits and the fix is to
  project fewer fields or raise `HOARDDB_QUERY_MAX_RESULT_BYTES`.

- **The user reference no longer names a `hoarddb health` command that does not
  exist.** The configuration page's Docker section listed a built-in
  `hoarddb health` command and a `HEALTHCHECK` the image never declared, and the
  internals design described both as built. Neither exists: the Docker section
  is gone with the image's move to its own repository, and the design marks the
  command as planned. For a health check, use the `/health` or `/ready` probe on
  the metrics listener (`HOARDDB_METRICS_ENABLED=true`, port 9090), or the
  interactive `status` command.

- **The driver handbook says how a user-administration refusal arrives.** A refusal is an `ERROR` frame carrying the shared `code` and
  `message`, never an in-band `success: false`, and the connection stays open;
  the success payload's `changed` list is the only proof a field was applied,
  and, as the handbook read then, `USER_GET` answered an unknown user with an
  `ERROR` and never `found: false`. The code was `INTERNAL` whatever the reason
  — a duplicate name, an unknown user, the last-administrator guard, a stale
  version — so a driver branched on the message; the three families coded
  differently (`PERMISSION_DENIED`, `NOT_AUTHENTICATED`, `UNAVAILABLE`) were
  named, with the message each situation produces. See section 6 of
  the driver handbook's authentication chapter. **Corrected here:**
  `USER_GET` of an unknown user now answers `found: false` and each refusal
  carries a code that names the cause (above).

- **A source that stops answering no longer holds a node's pending copies.** A member that an increase of a bucket's replication factor newly
  places copies the bucket in from its owner, one at a time. If the owner
  accepted the connection and then never answered (a stopped process, a stalled
  disk, a network that drops packets), that one copy held every other pending
  copy on the node for as long as the owner stayed silent, and the same held a
  take-over and an operator's `migrate`. A copy's requests now end when the
  source stays silent: two minutes for each chunk of data and for the final
  confirmation, and thirty minutes for the first request, whose answer waits for
  the source to read and hash the whole bucket. The log names the source and says
  the copy is tried again, and the bucket backs off like any failed copy while the
  next one is copied. A slow source that keeps answering is never cut off: the
  bounds are per request, and each chunk starts its own. A source that hangs
  while it prepares a bucket can still hold the node's copies for up to thirty
  minutes.
- **An alter of a bucket's factor on a one-node cluster keeps the bucket served
  and lets the next join finish.** A cluster node that was the ring's
  only member (a founder before anyone joined, or the last node left after
  `remove-node`) applied an alter as if another node had sent it: it answered
  `previous_factor: 0` and marked its only copy of the bucket as outside its
  set, so the next `join` paused schema changes on the whole cluster and never
  finished. The lone node now applies the alter as the bucket's owner, answers
  the factor the bucket had, and keeps its copy. A decrease there is answered
  with a sentence that says no copy is added or deleted.

- **Writes to a node whose store has a damaged table are refused with the
  repair instead of hanging.** When a damaged data block keeps the
  newest layer from being merged and that layer is full, every write used to wait
  forever with no message. It is now refused, within a moment of the store's next
  failed merge attempt (they repeat many times a second, and a write already
  waiting is woken by each), with a message that names the
  damaged file and says to restore the node, and the node reports itself not
  ready. A store that is only slow is never refused: there is no timeout.
  **Corrected here:** this entry first left out that the record of the damage
  is kept until the node is restarted on restored data, so while the newest
  layer is full a write is refused even when the damaged block is in an older
  layer and only the newest one is slow. The limitations page now states it.

- **A node whose store has a damaged table can be stopped.** A
  damaged data block in a table of the newest layer keeps every merge that
  includes it failing, so that layer fills and writes wait; stopping the node
  then never finished. Shutdown now completes in seconds, keeps all data, and
  reports the damaged table with the restore advice. The write stall itself is
  not removed: restore the node from a backup. It is listed in the limitations.

- **An empty-string `_id_` is refused instead of silently replaced by a
  generated key.** `db.Users.insert({_id_: "", n: 1})` — and the
  same on batch insert, push, and a store field's push or put — used to answer
  `OK` and store the document under a fresh random key, so a loader whose key
  column was sometimes empty grew a new key on every re-run. It now answers
  `INVALID_ARGUMENT`, with a message that an `_id_` must be a non-empty string,
  or be omitted for a generated key. A non-string `_id_` is still replaced, as
  documented.

- **The start refusal for a node listed at another address now leads with the
  wildcard listen.** A node refused because the member set lists its
  node id at another address was told to set `HOARDDB_LISTEN=<listed address>`,
  which cannot bind when the address changed under a moved VM or when a host
  name resolves locally to a different address (for example a `127.0.1.1`
  hosts entry). The refusal now offers `HOARDDB_LISTEN=0.0.0.0:<port>` with
  `HOARDDB_NODE_ID=<id>` first and names both cases, keeping the listed-address
  option for a node that does listen there. The user page's clearing time for
  the duplicate-identity warning now says about two minutes.

- **A start that fails to unwrap the node master is now in the audit log.** A sealed node that could not resolve its keystore or unwrap its
  node master refused before the audit log existed, so the failed attempt was
  invisible there. The refusal now appends one `key.unwrap` entry with
  `result=failed`, naming the key source and the error class and never key
  bytes. It is best-effort: if the audit path cannot be written, the node still
  exits with the same error and logs a warning.

- **A node killed with nothing unflushed no longer refuses a smaller memtable.** Badger creates a memtable write-ahead log on every open, and the
  shrink guard read any `*.mem` file as unflushed writes, so a node killed
  before it wrote anything was refused at a smaller `storage.memtable_mb` with a
  message saying writes remained, although there was nothing to replay. The
  guard now refuses only when a log holds an entry, and the refusal is unchanged
  when it does.

- **A standalone start now removes a leftover `replication_state.bson`, and
  refuses an unreadable one.** The legacy state file was imported and
  deleted only by a start that built a cluster, so a node configured
  `replication_factor: 1` with no seed nodes kept it for good — peer ids and
  `database/bucket` names in plaintext — and started over a corrupt one without
  a word. Every start now imports its positions into the node store and deletes
  the file, so a later switch back to clustered finds them where they belong;
  an unreadable file stops the start with the path and the
  `mv replication_state.bson replication_state.bson.unreadable` remedy, as a
  clustered start already did. Nothing changes for a clustered node.

- **`reset-password` now checks the cluster id of the master, not the host name.** `users.json` recorded the master by the node id from the
  configuration, which defaults to the host name, so on a cluster run on one host
  with default ids every node matched and the "not the recorded master" refusal
  protected nothing. The master now records its cluster id (the id its address is
  listed under) and `reset-password` compares with the id this node holds in the
  cluster, naming both in the refusal. A `users.json` written before this fix
  keeps the old value until the master next changes a user, so run
  `reset-password --force` on the master once if it is refused there.
- **`__node__` can no longer be used as a username.** It is the name a
  node presents to its peers, so an account with it made audit rows ambiguous.
  `create user` and the root seed now refuse it and ask for another name. A users
  file that already holds such an account still loads, and the server logs a
  warning naming it; run `drop user "__node__"` to remove it.

- **A bucket whose `large_commits` list has grown because a peer is holding the
  log back now warns.** A bulk load in large batches records each
  batch's sequence in the bucket's state document, and the list is pruned only
  below the log's first sequence. While a peer was slow, down or lagging, the
  list grew one entry per batch, every later write rewrote a longer document,
  and nothing was logged. The node now logs one WARN per start once the list
  passes 64 entries, naming the bucket, the length, and that the local log
  still starts below those commits, so a lagging peer or the log's retention is
  holding them. The WARN points at `nodes` to list the members and at the log's
  "WAL retention on this node is held" warning, which names the member holding
  them; the list shrinks as the log advances.
- **A pull from a source whose replication stream is refused for good says so at once.** When the source's stream to the pulling node could never open (its
  `wal/incarnation.bson` is lost, or the puller holds a position the source's log
  cannot place), `migrate` said the pull "is tried again" and the rebalance sweep
  waited 5 seconds on every bucket. The source now answers with the reason and the
  fix (`remove-node`, empty the node's data directory, add it again), and the pull
  returns without waiting. A sweep pass asks that source once and skips its other
  buckets, with one log line, then asks again on the next pass.

- **`show buckets` and `show databases` on a cluster list the whole cluster.** Each node answered with the buckets it held a copy of, so nodes
  listed different subsets, a node holding none of a database answered "not
  found", a database's bucket count differed per node, and a database dump
  through one node left out the buckets it did not hold. The node you ask now
  asks every member and merges the answers, the same from every node. If a
  member does not answer within 5 seconds, `LIST_BUCKETS` and `LIST_DATABASES`
  are refused with `UNAVAILABLE`, naming the member: retry, or remove the
  member. A database no member has answers "not found on any member of the
  cluster". Reply shapes are unchanged; the listing is still one unpaged reply
  (paging is planned for 1.1).

- **`majority` and `all` writes no longer wait for a replica that is still being
  sent what it missed.** A replica that came back with a backlog
  counted as in sync as soon as its stream started taking live writes, while
  thousands of entries could still be on their way to it, so `majority` and
  `all` writes waited for it and often ran out their write-concern timeout. It
  now counts again only once the owner has sent it the log up to the tail (the
  node log's "replication to a peer is back in sync"); until then writes answer
  at once with the copies that hold them, and
  `hoarddb_replication_peers_not_in_sync` counts it. Under heavy writes at `one`
  a returned replica can still stay behind while they run (see the limitations
  page).

- **A btree range, first or last lists a key exactly as it was written.** A key holding a NUL byte or another control character was listed
  as `0x...` hex, so it did not match the key `PUT` and `GET` use. The key is now
  returned byte for byte. Only a key that is not valid UTF-8, which cannot travel
  as a BSON string, is still listed as `0x` hex. The CLI shows a key with a
  control character in quotes so it cannot drive the terminal. No input is
  refused.

- **A refused create no longer leaves a bucket counted.** After a
  create failed, for example on a bad index option, `show databases` reported
  the database with one bucket while `show buckets` listed none. The failed
  create now takes back its claim on the name, so the count stays 0.

- **A document whose indexed field is too long is refused in plain words.** The refusal used to be the storage engine's own key-size error
  and a hex dump of the key. It now names the field, the index and the document,
  says nothing was written, and says what to do: index a shorter field, keep the
  long text in a field that is not indexed, or index a hash or a prefix of it.
  The limit is unchanged and is listed under
  [Limitations](docs/limitations.md).

- **`find` with a sort now orders integers above 2^53 by value.** The sort
  compared numbers as `float64`, so 64-bit integers that differ only in their low bits, such
  as snowflake ids, tied and came back in storage order. Numbers now compare by exact value,
  as filters and `distinct` already did. The order of values that were already distinct is
  unchanged.
- **A replica no longer applies, after a power cut, an entry it had passed over.** A replica skips an entry its copy already holds, or one from an
  older term, without applying it. That skip was not flushed to disk. A power
  cut could lose it while the entry stayed in the replica's log, and on restart
  the replica applied the entry. A queue push or a `$inc` was then applied
  twice, and nothing reported it. On restart the replica now checks each logged
  entry against the term recorded with it, as it did when the entry arrived,
  and skips it again. Entries logged by a build from before the term was
  recorded are replayed as before. No disk or memory cost is added per bucket,
  and nothing changes on the wire or on disk.

- **A node restored from a VM or disk snapshot, or one that lost its `wal/`
  directory, no longer reuses log sequences its replicas hold.** Its
  next write of a key carried the version of the write it had lost, or a lower
  one, so a consistent get could prefer the older value and its repair could
  write that value back over the newer one. The node now moves its log past every
  position a majority of its peers report before it takes a write, and logs one
  warning when it had to.

- **A replica seeded only by a pulled snapshot no longer drops a write after the source's log goes backwards.** When a source's log was lost or restored below a snapshot position a replica held, the
  replica judged the source's new entry at a reused sequence as already held, acknowledged it and never
  applied it. A rebase that lowers a snapshot position now stops the replica trusting its own position for
  that source, as one that lowers the stream position already did. The same holds when the source's log went
  backwards while the snapshot was being pulled.

- **A node restored from a snapshot no longer logs a database drop it learned at start at a log position its
  peers hold.** A drop the node learned while replaying its log was logged again before the node had
  heard where its peers hold its log. It is now applied during the replay and logged once a majority of peers has
  answered. If the node stops before then, that log entry is not written, and the peers learn of the drop the next
  time they send this node anything for that database.

- **A replica that lost its `wal/` directory but kept its data no longer logs
  replicated writes at positions its data already records as applied.** A bucket's applied mark is a position in the node's own log, and
  for a bucket the node only replicates no peer knows it, so the new log could
  give the next replicated write a position at or below it, and a replay would
  take that write as already applied. A node whose log holds no entry when it
  opens (its `wal/` directory or its segment files lost) now reads every
  bucket's applied mark and starts its log past the highest, and logs one
  warning when it had to. A start that finds an entry in its log costs nothing
  more. Losing only the newest segments while older ones survive is not
  covered: restore `wal/` and the data from one backup.

- **A node moved out of a bucket's set no longer keeps its copy when a retried sweep
  is cut short.** A sweep re-armed by a peer coming back at the moment a
  membership change's sweep started could supersede it, and the buckets that sweep
  was still handing off were then asked about by nothing until a restart or the next
  membership change. A sweep now claims its turn in one step, so a re-armed sweep
  waits for it to record what it left.

- **Compose now forwards `HOARDDB_DISK_HIGH_WATER` to the nodes.**
  `docker-compose.yml` and `docker-compose.hoarddb.yml` give every node the
  host's value, defaulting to the server's own `85`, so the documented fix for
  a node over its disk high-water mark works without editing the file.

- **The retention warning no longer tells an operator to restart a replica the
  owner refused.** When a sealed cluster holds its log for a member
  whose store is plaintext, the WARN now states that cause and the fix — enable
  encryption on the member, or remove it with `remove-node` — instead of telling
  the operator to restart a node that is already running. A member that is simply
  away keeps the restart advice.

- **A damaged field-store key no longer reaches a client's error text.** A
  field-store read or write whose stored key was already malformed formatted
  that key, as hex, into the failure message the server sent. The key is now
  kept for the node log in a structured error and the message carries no key.

- **A take-over that copies over a node's own copy now logs what it gave up.** When a new owner replaced its own out-of-date copy of a bucket
  with the most complete copy, the entries its copy held that the other lacked
  were given up with nothing in the log. It now writes the same `released a
  copy` WARN a release outside the replica set writes, with the count of entries
  given up, or a lower bound when the newer term was opened from an older one.

- **A `consistent get` no longer reverts a write made during the read.** A read that found a stale copy repaired it by writing the value
  it read, unconditionally, through the bucket's owner. A put of the same key
  acknowledged between the read and the repair was overwritten on every copy by
  that older value, durably, while its writer had been told it succeeded. The
  repair now carries the version it read, and each copy writes it only while its
  own copy of the key is no newer; a copy that has moved on keeps its value and
  the read still answers what it read.

- **A `consistent get` no longer brings back a key deleted during the read.** A delete removes the version the repair compares with, so a
  delete acknowledged at `all` between the read and its repair was undone on
  every copy. The repair now carries the owner's position at the read, and the
  owner, before logging it, drops it if its log after that position holds a
  write or delete of the key, or no longer reaches back that far. A dropped
  repair is reported as not repaired, and the next `consistent get` repairs the
  copy. While the owner makes that check, other writes to the same bucket wait
  for it; that happens only when copies disagree.

- **A replica refused a write for being unencrypted, by an owner whose bucket
  moved mid-write, now receives it once it is encrypted.** The
  owner's late delivery of a write it logged before the bucket moved skipped
  that replica without holding its replication back.
  It now stalls replication to the replica, as an owner's own writes do.


- **A heap read failure no longer shows an entry's storage key.** A peek or pop
  of a heap bucket whose entry could not be read, or a pop whose delete failed,
  put the entry's internal key, as hex, into the failure message. The message now
  names the bucket and the database and says to retry once and then check the
  node log, which records the key. The same hex no longer appears in
  btree, field-store, page and index read failures, and the retry advice is
  given only for a failed storage read, not for a refused search or a damaged
  blob.

- **A replica the owner refused a write for being unencrypted now receives it once it is
  encrypted.** On an encrypted cluster, an owner that learnt mid-write that a
  replica does not encrypt its store stored the write, refused it to that replica and went on
  replicating: the replica's next acknowledgement, or a retention pass, recorded it as holding
  a write it never received, and nothing sent it later. Replication to that replica now
  stalls, naming the replica and the fix, and the owner keeps its log for it until the replica
  says its store is encrypted and receives the write, or is removed with `remove-node`.

- **An out-of-range `auth.argon2_memory`, `auth.argon2_iterations` or
  `auth.argon2_parallelism` is refused at start-up, naming the key, the
  permitted range and the default.** Before, the value passed config
  validation and the server failed only when the first user record was seeded or
  saved. The ranges now live in `config` and the credential validator uses the
  same constants, so the two cannot drift. Two upper limits are new:
  `auth.argon2_memory` at most 1 GiB (1048576 KiB) and `auth.argon2_iterations`
  at most 16. A config above either one used to load and is now refused at start.

- **Two processes running as one node id are detected.** A copy of a
  node's data directory carries its id, address record, key and cluster token, so
  a copy started beside the original ran as it unnoticed: both owned its buckets
  and, for the auth master, both accepted user changes. Each process now sends a
  random instance nonce on gossip (feature `instance_nonce`). A member that sees
  two of them keep alternating under one id logs a WARN naming the id, where each
  was seen and both nonces, with the fix; `status` gains a `Duplicate:` row,
  `nodes` marks the id `DUPLICATE IDENTITY`, and
  `hoarddb_duplicate_node_id_detected_total` counts it. Both processes are told,
  show it in their own `status`, and refuse user changes until only one runs. The
  signal clears about a minute after one of them stops. A restart is not reported.
  Drivers: the `HEALTH_CHECK` answer may carry the optional field `duplicates`
  (a list of sentences) and each `LIST_NODES` row the optional field `duplicate`;
  both are absent unless a duplicate is live, and the driver vectors list them.

- **Nodes given different seed lists no longer run two nodes under one node id.** With node 1 listing `dup@A,solo@C` and node 2 listing
  `dup@B,solo@C`, each list valid on its own, both nodes ran as `dup`, owned its
  buckets and accepted user changes as the auth master. Members now compare the
  address of every node id as well as the ids: `solo` refuses node 2's list,
  naming the id and both addresses, node 2 is never confirmed and refuses
  database, bucket and user requests with the same text, and its `status` shows
  it. A node listed as `<id>@<its own address>` also no longer confirms its
  member list on its own answer at start. A node no longer takes another node's
  entry by its node id or host name, and a seed list naming one address under
  two ids is refused at start (see **Changed**).

- **A cluster whose seeds name one node id at two addresses now refuses to
  start.** `HOARDDB_SEED_NODES=dup@A,dup@B,...` (a typo) used to drop the second
  entry silently while the node at `B` still took the id, so two nodes ran as
  `dup`, both owned its buckets and both could accept user changes as the auth
  master. The node now stops at start naming the id, both addresses and the fix
  (a distinct `HOARDDB_NODE_ID` per node, each node listed once); a
  `membership.json` that names one id twice stops it the same way. The same id
  at the same address listed twice still starts. `join` under an id another
  member holds is refused before the new node is dialed, naming that member and
  its address, instead of "the node is already a cluster member".

- **User changes work on a cluster started without a node id.** A cluster whose
  seeds are listed by address and whose nodes set no `HOARDDB_NODE_ID` refused
  `create user` and every other user change on every node, the master included:
  the master check compared the ring id with the host name the node id defaults
  to. It now compares with the identity the ring gave the node, so the master
  accepts the change and the other nodes refuse it naming the master, and the
  refusal's "this node" shows the ring id.

- **An owner's failed read before an update or delete is retried at restart.** The owner logs a client update or delete and ships it to its
  replicas, then reads the document before writing. When that read failed, the
  replicas applied the write and the owner never did: nothing recorded the
  sequence, so once a later write to the bucket had applied, a restart skipped
  it for good and the owner's copy differed from its replicas' with no signal.
  The failed read is now recorded like a failed write: the owner's WAL is held
  until the restart, and the restart applies a single-document update or a
  delete on the owner. A multi-document update cannot be applied beneath a
  later write without risking an older value over a newer one, so the restart
  logs the ERROR that says the bucket differs from its replicas and to restore
  it from a replica (`hoarddb dump` there, `restore` here). A failed read
  now also logs its storage key, which none of the three handlers (update, multi-update,
  delete) logged for a failed read.

- **A document you store that contains `{"$$now": true}` is stored as you wrote it.**
  Before, hoardDB took that sub-document for the way `now()` reaches the server and
  replaced it with the current date, silently, on every insert, batch, update, push and
  store-field write. `now()` still stores the server's time of the write; it now
  reaches the server as a marker value (a BSON DBPointer with the reserved namespace
  `$$hoardDB.now`) that the CLI builds and that typed input, HQL, JSON and `hoardDB ingest`
  (which refuses that namespace, naming the field) cannot produce; only a client sending that exact
  DBPointer in raw BSON on the wire can. The `{"$$now": true}` limit is gone from
  the limitations page and from the ingest page.

- **A failed write the restart can no longer apply is settled and reported at
  the restart.** When the owner's store failed a write, the restart
  retries it. If the retry was refused before it reached the store (an `$inc`
  on a field a later write had made a string, or a value outside the bucket's
  declared field type), nothing settled it: the write
  stayed on the bucket's list of failed writes, and only later did a
  misleading "can never be replayed" error appear. The restart now settles the write and logs at once that the
  bucket may differ from its replicas and to restore it from a replica
  (`hoarddb dump` there, `restore` here), and
  `hoarddb_apply_failures_not_replayed_total` counts it.

- **A refused repair delete is no longer reported as repaired.** A
  consistent read whose owner refused a repair delete with a member-feature
  refusal rather than a Go error reported `Repaired: true` although the stray
  copy stayed. The repair-by-delete path now checks the reply frame the way the
  repair-by-put path does, through one shared helper, so a refusal is logged as
  `read repair failed` and the read reports `Repaired: false`.

- **A put into a blob bucket is held to the blob cap whichever node the client
  entered.** A node that had not yet learned a blob bucket measured
  the put against its document limit instead, refusing a put the raised blob cap
  allowed and advising the writer to use a blob bucket, or storing one above the
  cap under the defaults. Such a node now applies only the higher of its two
  limits, and the bucket's owner applies the bucket's real limit only for a put
  whose type the entering node could not resolve. The entering node's limit
  still governs a put it could type, so a lower-limit owner applies what the
  entering node accepted, and a read repair, a replica and a replay are never
  re-measured. A refused read repair is no longer reported as repaired.

- **A replica's transient store read failure is retried, not recorded as a
  divergence.** An update or delete a replica applies reads the
  document before writing it; a store fault on that read (a disk or Badger read
  error) was answered like a refusal the owner did not share. The replica then
  recorded a divergence, stopped the owner's stream for every bucket on this
  node and asked the operator for a re-seed. The read failure is now marked a
  store failure, so the replica leaves the entry unapplied and the owner's
  redelivery applies it once the store reads again; the replica's replay does
  not, because that entry was voided. A real disagreement is still recorded. An
  update or delete whose read fails on the owner now answers `write_outcome
  unknown` instead of `not_applied` — honest, because the replicas may still
  apply it — and a DELETE's `copies_confirmed` no longer counts the owner's own
  copy.
- **`migrate` copies afresh a copy marked outside its set that is back in it.** With `auto_rebalance` off, a node whose copy a membership
  change or a raised replication factor placed back in its set keeps the copy
  marked stale until an operator runs `migrate`. `migrate <database> <bucket>`
  on that node answered success and did nothing, so the copy never caught up
  and the bucket stayed a copy short. It now copies the bucket afresh from its
  owner and returns once the copy is fresh, or answers that the copy is in
  progress and to run it again in a moment.

- **A read across a hole in the log no longer reports it as retention
  outrunning the reader.** If a middle `wal/wal_NNNNNN.log` segment
  was deleted by hand or lost to the disk, the read retried and then said the
  log's segments were removed faster than they could be read. That message also
  matches the horizon refusal an operator reads as "re-seed this node from a
  snapshot", which points at the wrong cause. The read now names the hole,
  `sequences A..B are missing`, and says to restore the segment from a backup or
  re-seed this node. A segment pruned under a racing read still gets the old
  wording.

- **A node listed by id in the seed nodes is no longer logged as an implicit
  member.** With a wildcard `HOARDDB_LISTEN`, the node's advertise
  address was rewritten to `127.0.0.1`, which never equals the address in an
  `id@addr` seed entry, so the node warned that it was "not listed in the seed
  nodes" although the list named it. Membership matching now also consults the
  configured node id and adopts the entry's advertised address.

- **The `ci.sh` preflight runs both ci selftests in a quick mode.**
  The preflight and gate 7 share `hygiene_steps`, so after the race-gate
  selftests grew, every `scripts/ci.sh` run paid their full cost twice and the "fail fast" preflight took long before the first gate started. The
  preflight now sets `HOARDDB_CI_SELFTEST_QUICK=1`, under which
  `scripts/ci-selftest.sh` skips its two heavy race-gate blocks and
  `scripts/test-shards-selftest.sh` skips its slowest cases; a preflight pass is now quick. Gate 7 still runs both selftests in full, so the guarantee they
  give is unchanged.

- **The CI selftests and the race-gate wrapper run on macOS again.**
  `scripts/race-same-tree.sh` and `scripts/ci-selftest.sh` used the bash 4
  `mapfile` builtin and GNU-only forms of `sha256sum`, `stat`, `date`, `sed -i`
  and `touch -d`, so on a Mac with the stock bash 3.2 and BSD tools the wrapper
  could not compute its key and the selftest failed in cases with nothing wrong.
  The wrapper now reads the toolchain and the memo with a `while read` loop and
  hashes through `sha256sum`, else `shasum -a 256`; the selftest uses small
  helpers that try the GNU form and then the BSD one. Behaviour on Linux is
  unchanged.

- **Two CI runs that create the race-gate memo directory at the same time no
  longer lose the race.** `scripts/race-same-tree.sh` created
  `${XDG_CACHE_HOME:-~/.cache}/hoarddb/race-memo` with `mkdir -m 0700` and treated
  any failure as an unusable directory, so when two runs created it at once the
  loser's green result was not recorded and a later run repeated the 15-minute
  gate. A failed `mkdir` now re-checks the path and accepts it when it is a
  directory owned by the running user without group or other write bits, the same
  trust test an existing memo directory must pass.

- **A failed read no longer shows the client an internal storage key in hex.** A read, a value copy, or a batch add that failed reported the key
  it stopped at — a slot, bucket id, namespace and document key the client never
  sent — so the message was an encoding only the source explains. The failure now
  names the operation and the cause, and the key goes to the node's log for the
  operator.

- **A node whose copy of a bucket left the replica set no longer answers reads
  from that copy when a later membership change places it in the set again.** Such a copy stops receiving writes until it is copied again,
  but a read that reached the node, directly or as the coordinator, was still
  answered from it, so `get`, `find`, `count` and similar reads answered
  `document not found` or an old value for a write acknowledged at `majority`,
  with no time limit. The read now goes to the bucket's owner and starts the
  copy; once the copy lands, the node answers locally again, with no restart.
  `consistent get` was not affected.

- **An owner no longer logs and ships a forwarded write it cannot run.** On a cluster whose members run different builds, a write whose
  opcode the bucket's owner did not implement was appended to the owner's log
  and shipped to the replicas before the owner answered "unsupported opcode",
  so newer replicas applied a write the client was told had failed. The owner
  now refuses it before its log: an `ERROR` with `write_outcome: not_applied`
  whose message names the owner and the opcode and says to upgrade that member
  or retry once every member runs the same build.

- **Every write now declares the member feature it needs.** On a
  cluster whose members run different builds, a write whose meaning a newer
  build changed would reach an older member that runs it the old way, and the
  copies would differ with no error. Each write operation now names the feature
  it needs, or that every build has it. While a member that has reported its
  features lacks the one named, the node that would log the write first (the
  bucket's owner, or for `create database` and `drop database` the node the
  client reached) refuses it before its log:
  an `ERROR` `UNAVAILABLE` with `write_outcome: not_applied` that names the
  member and says to upgrade the cluster. Every write today is one every build
  has, except changing a bucket's replication factor, which was already refused
  this way, so nothing a current cluster does is refused that was not before.

- **WAL retention no longer removes log entries it was told to keep when a
  log segment is empty.** After a crash right after the log started
  a new segment, or a write that failed on a full disk, the empty segment made
  the segment before it look fully pruned, so retention deleted entries above
  its bound. A replica that still needed them was refused as behind the
  retained horizon, and a restart could not replay them. Retention now keeps
  every entry above its bound.

- **A failed request no longer drops the answers of the other requests on its
  connection.** When the handler of one request pipelined on a
  connection failed with an internal error, the server closed the connection at
  once, and the other requests in flight on it lost their answers. A `pop` that
  had already removed its entry lost the entry. The server now answers the
  failed request with `INTERNAL`, stops reading the connection, lets the
  requests already in flight answer, and then closes it. The wait lasts at most
  the frame timeout (`HOARDDB_TCP_FRAME_TIMEOUT`, 30 seconds); a request still
  running then is cancelled and the connection closed.
- **A write refused for a membership disagreement keeps its connection.** A cluster node whose member set disagrees with a peer's refused
  every write, but the client received a generic `INTERNAL` error and a closed
  connection. It now receives the named refusal (`CONFLICT`,
  `write_outcome: not_applied`, and the `nodes history` instruction), and the
  connection stays open.

- **A refused `join` or `remove-node` answers with its own code and keeps the
  connection.** A membership change the node refused, for
  example removing a node that is not a member, was answered `INTERNAL`, and
  the connection was closed, so the next command met `EOF`. The refusal is now
  an error answer with the code that matches its cause, and the connection
  serves the next command: a node that is not a member is `NOT_FOUND`, a node
  that is already a member is `ALREADY_EXISTS`, and a change refused because
  another one is in progress, or because the node is still marking what the last
  one moved, is `UNAVAILABLE`, which a client may retry. Other refusals keep
  their message, and those without a code of their own still answer `INTERNAL`,
  now without closing the connection. The two member refusals now say to
  run `nodes` to list the members. `CLUSTER_ADD` and `CLUSTER_REMOVE` answer a
  refusal as an `ERROR` frame instead of a response with `success: false`.

- **A put into a blob bucket is held to the blob cap, not also to the
  document limit.** With `storage.max_blob_bytes` raised above
  `storage.max_document_bytes`, a document put (`insert`, `OpPut`) into a blob
  bucket that the blob cap allowed was refused by the document limit, with
  advice to use a blob bucket. Each bucket type now has one bound: a blob
  bucket's put is held to the blob cap and the frame size, every other bucket
  to the document limit. The blob bucket is recognised whichever limit is
  lower.

- **An update that shrinks a document over a lowered document limit is
  accepted.** After `storage.max_document_bytes` was lowered below
  a stored document, every update to that document whose result was still over
  the limit was refused, even one that made it smaller, so the document could
  only be repaired in one step. An update is now refused only when its result
  is over the limit and larger than the stored document; an update that keeps
  the size or shrinks it is accepted, and growth is still refused with the same
  message.

- **A database drop no longer resets the replication factor or TTL
  registration of a bucket recreated while it ran.** When one
  incarnation of a database was dropped and a bucket of the same name was
  created in the database's other live incarnation during the drop, the drop
  forgot that new bucket's declared `replication_factor` and TTL index, so it
  was delivered at the cluster's factor until the node restarted. The drop now
  leaves the registrations of a name that is served again.

- **A pop on a cluster no longer loses its entry when its write falls short.** A fifo, lifo or heap `pop`, or a store-field `pop`, whose write
  concern was not met in time, whose bucket's owner changed while it was in
  flight, or one of whose replicas was refused for not encrypting its store,
  removed the entry on the owner and answered `TIMEOUT` or `UNAVAILABLE`
  without it, so the entry was gone and never returned. Such a pop is now answered with its entry, as a success with
  `copies_confirmed` below `replication_factor`, `write_outcome: applied`, and a
  message that says what fell short. A client that treated a pop's `TIMEOUT` as
  a failure now receives the entry instead. Every other write keeps its error.
- **A pop on a single node whose disk write fails now returns its entry.** A fifo, lifo or heap `pop`, or a store-field `pop`, on a node
  that does not route, whose removal committed but whose sync to disk then
  failed, was answered with an error and without its entry, so the entry was
  gone from the queue and never returned. It is now answered success with its
  entry, `write_outcome: applied`, and a message that names what was popped,
  says the removal is not yet on disk and to check the disk. This is a pop-only
  exception to the rule that a write whose sync fails is not acknowledged: at
  worst the entry is served again after a restart or a power loss, never lost.
  Every other write whose sync fails keeps its error.
- **A write the server refuses is no longer answered `TIMEOUT` and `applied`
  when a replica is slow.** On a cluster, a write its owner refused
  (a `$inc` of a string, a type refusal) whose write-concern wait then timed
  out, at `majority` or `all` with a lagging replica, was answered `TIMEOUT`
  with `write_outcome: applied` and "Do not retry", sent to the owner or
  through another node: the client was told a write that never happened was
  stored, and the refusal was lost. It is now answered with the refusal and
  `write_outcome: not_applied`. A write the server accepted still answers
  `TIMEOUT` with `applied`.

- **Tab completion offers `consistent-get`, not `consistent`.** The shell
  completed to a word it then refused; a test now holds every completion word
  to a command form the parser accepts.
- **The shell and the HQL reference no longer say a per-bucket replication
  factor is not honoured.** `create bucket X { replication_factor: N }`
  has been honoured since the factor-at-create change. The refusal of `create
  database ... with replication_factor N` and three places in the HQL
  reference said it was ignored. `alter bucket X { replication_factor: N }` is
  now refused with a message that names the factor, instead of one about
  indexes.

- **A replica returning under sustained writes now catches up instead of falling
  further behind.** A replica waited for one disk flush (fsync) of its
  log per replicated entry, so on a real disk one stream applied no more than
  about one entry per flush (a rate set by the disk's flush time), slower than an
  owner taking writes from many clients. It now logs the entries of each
  replication frame (up to 64), flushes once, and then applies them in order.
  Durability is unchanged: nothing is applied or acknowledged before its log
  record is flushed. When the replica cannot apply an entry of a frame (a batch
  too large for its memtable, a copy that diverged, a store failure), it records
  that the entries logged after it were never applied, so neither a restart nor
  the owner's re-send applies them out of order, twice, or not at all.
- **A replica that crashes just after refusing an entry no longer loses it and
  applies the entries after it out of order.** When a replica's store
  refused, inside its store, an entry its owner had applied (a divergence), the
  store marked the entry as dealt with,
  and the record that the frame stopped there was written just after. A crash in
  between left the entry counted as applied: on restart the replica applied the
  entries logged after it, ahead of it, and ignored the owner's re-send, so its
  copy silently missed one entry (and, for a divergence, never reported it). The
  record is now written in the same synced write as the store's, so a crash
  leaves both or neither. The same path also covers an entry of a newer term
  than the replica's copy, which no production path produces today. This costs
  one disk flush per refused entry that stops the stream, never one per entry
  applied. A divergence the handler refuses before the store is not covered yet
  (filed separately).
- **A replicated entry whose store write failed is no longer applied twice
  after a power cut.** When a replica's store failed to apply an
  entry and no other entry of the same frame was logged after it, the owner's
  re-send applied and was logged a second time, and after a power cut or crash
  the replica replayed both copies: a fifo or lifo held the element twice and
  the replica no longer matched its owner. The replica now records that the
  failed copy was never applied, as it already did for entries logged after a
  failed one, so only the re-send is ever applied.
- **A failed read of the log no longer lets a replica forget entries it never
  applied.** Recording the entries a stopped replication frame left
  unapplied first read where the replica's log begins. If that read failed (an
  I/O error, out of file descriptors) the record was never made, the owner's
  re-send was logged beside the unapplied copies, and a power cut replayed both.
  The record is now kept, and written, whether or not that read succeeds.
- **A database drop that failed on a replica is no longer reported as done
  there.** When a replica's store failed to commit a `drop database`
  pushed from another node, the replica still acknowledged it, the drop was
  never sent again, and the replica went on serving the dropped database's
  buckets until a restart replayed the drop, or for good once log retention had
  removed it. The replica now refuses the push, so the drop is sent again and
  applied once the fault clears. A drop every node refuses alike, or of a
  database the replica does not hold, is acknowledged as before.
- **A database create that failed on a replica is no longer reported as done
  there.** When a replica's store failed to commit a `create
  database` pushed from another node (its catalog entry, or the drops of
  earlier incarnations it carries), the replica still acknowledged it, the
  create was never sent again, and the replica did not hold the database until
  a restart replayed the create, or for good once log retention had removed it.
  The replica now refuses the push, so the create is sent again and applied
  once the fault clears. A create every node refuses alike (a reserved or
  invalid name) is acknowledged as before.

- **A config file value that an environment variable replaces is now named on
  stderr.** The container image sets `HOARDDB_DATA_DIR`,
  `HOARDDB_LISTEN`, `HOARDDB_TCP_LISTEN` and `HOARDDB_METRICS_LISTEN`, which beat
  a mounted file, so a file's `metrics.listen: 127.0.0.1:9090` silently became
  `0.0.0.0:9090`. When a variable and the file set the same key to different
  values, the server logs one warning per key naming the key, both values and the
  variable to unset. The variable still wins; nothing about the image changed.

- **A node whose data directory is removed or replaced while it runs now refuses
  the next write.** The store and the write-ahead log went on writing
  to their open files, which no longer had a name, so every write until the
  memtable filled (up to its size, 32 MiB to 1 GiB) was acknowledged and lost at
  the next restart. Each now notices within a tenth of a second, with or without
  writes, refuses every later write with an error that names the directory and
  says to restore it and restart the node, and reports itself unhealthy in
  `status` and on `/ready` (503). Reads are still served.

- **After a membership change, a node that has not finished taking a bucket
  over answers reads of it with "retry" instead of a stale "not found".** A bucket's new owner refused writes until it had taken the
  bucket over, but served reads from the copy it already held as a replica
  whenever that copy had reached it after the change: a create the old owner's
  stream delivered late, for example. That copy could lack a write the previous
  owner acknowledged at `majority`, so `get`, `find`, `count` and queue peeks
  answered `document not found` or an old value, through any node, until a
  write or a `migrate` started the take-over. Such a read is now refused with
  `UNAVAILABLE` ("ownership of `<database>.<bucket>` is moving to this node …
  Retry in a moment"), and the refusal starts the take-over; reads answer again
  once it lands, with no restart. `consistent get` was not affected.

- **After a crash, a replica with a smaller `storage.memtable_mb` than its
  owner no longer loses a batch it could not apply.** The replica
  logs a pushed batch before it applies it and records that it refused the
  batch after. A crash between the two left the batch in its log with no
  record, and on restart the replica counted the batch as applied: the
  owner's re-send was skipped, the replica held none of the batch, and nothing
  stalled or said so. A restarted replica whose own replay refuses a logged
  batch as too large now records that refusal and holds its position below
  the batch, so the owner re-sends it and the stream stalls with the memtable
  reason, and the re-sends are refused without being logged again. Once the
  member is resized and restarted, it holds the batch once, in every bucket
  type: a FIFO, LIFO or heap bucket holds each element of it once, as its
  owner does.

- **A node at or above `disk_high_water_pct` no longer loses a stale copy of a
  bucket when it refuses to copy a fresh one in.** A replica found
  out of date or diverged, and an owner taking a bucket over its own older copy,
  delete that copy before copying the fresh one in. On a full node the fresh copy
  was then refused, so the bucket was left a copy short. The node now checks its
  disk before it deletes anything: a refused node keeps the copy it has, and the
  refusal names the node, the bucket and the fix as before.

- **A request over the 32 MiB frame limit no longer shrinks its connection's
  pipelining window.** The refusal kept the read slot the request had taken, so
  each such request left one slot fewer, and at
  `HOARDDB_TCP_PIPELINE_FRAMES=1` the connection read nothing more after the
  first one.
- **With auto-rebalance off, a join no longer leaves schema changes paused with
  nothing to run.** A bucket created while a join moved it could end
  up on a remaining member in a state that does not follow its new owner. The
  pause waited on that copy, but the refusal listed no `migrate` command for it,
  so the operator had no way out. The refusal (and `nodes`) now lists
  `'migrate <database> <bucket>' on <member>` for such a copy, and running it on
  that member brings the copy in line with its owner and lifts the pause. To do
  that, the member may drop its copy and pull a fresh one from the owner; entries
  the owner does not have are not kept. On success `migrate` answers
  `bucket "<bucket>" in "<database>" on <member> now follows its owner's term <n>`
  where it used to say "already holds this bucket, so nothing was pulled". If the
  copy has not caught up within the write-concern timeout, `migrate` is refused
  with a retryable error: `bucket "<bucket>" in database "<database>" on <member>
  is in term <x> and its owner <owner> is in term <y>; this node is taking its
  owner's term. Run 'migrate <database> <bucket>' on <member> again in a moment`.
  `migrate` on a member whose copy already follows its owner now answers
  `bucket "<bucket>" in "<database>" on <member> already follows its owner's term
  <n>; nothing to do`, followed by the existing advice for a replica that is
  behind. On the owner itself, and on a member whose copy cannot be compared with
  its owner's (the owner has not opened its copy yet, or holds another
  incarnation of the bucket), `migrate` still answers "already holds this
  bucket, so nothing was pulled". When the owner cannot be reached, `migrate` on
  a member that holds the bucket now fails with an error that names the owner
  (`asking the owner <owner> which term its copy ... is in`); run it again once
  the owner answers.
- **A write in flight across a membership change reaches the replicas it was
  addressed to.** When a join or leave moved a bucket while a write to
  it was being answered, the write stayed on the old owner only, and the
  replicas were later corrected by copying the whole bucket. The write is now
  queued for its replicas directly, so fewer whole-bucket copies are made and
  replicas lag for less time.
- **`scripts/ci.sh` finds the test VM list again.** `ci.sh` moves
  `HOME` to a throwaway directory, and the shard runner looked for
  `~/.config/hoarddb/test-vms` there, so gates 3 and 4 ran on the local machine
  although the header promised one shard per VM. `ci.sh` now resolves the list's
  path from the real home before the move and passes it on as
  `HOARDDB_TEST_VMS_FILE`. An explicit `HOARDDB_TEST_VMS` still wins.
- **The CLI's `-address` help says which address it dials.** `-help` printed
  `(default "localhost:7433")` for the session, `dump`, `restore` and `ingest`,
  but without `-address` they dial the `tcp.listen` address of the loaded
  configuration (`HOARDDB_TCP_LISTEN` or the config file; `127.0.0.1:7433` when
  nothing is configured). The help now says so. The behaviour is unchanged.
- **`help <unknown topic>` exits non-zero.** `help nosuchtopic` listed the valid
  topics and exited 0, like a topic that was found, so a script could not tell a
  typo from a lookup. It is now a usage error (exit status 1): the same text,
  `unknown help topic "nosuchtopic". Available topics: ...`, goes to stderr
  through the normal error path. A known topic still prints and exits 0.
- **`status` prints the build's version.** It printed `Version: 0.1.0` on every
  build, from a literal. It now prints the version the server logs at start and
  the CLI banner shows (`devel` on a plain `go build`, the tag on a release
  build), so a pasted `status` says which build it came from.
- **The unsupported-opcode refusal prints the request id as hex.** A server that
  did not implement a call answered `unsupported opcode 0x7F (request id [18 4
  201 ...])`, sixteen decimal bytes. It now prints the id the way the server log
  does, 32 hex characters, so the two can be matched.
- **The changed-server-key refusal names the file hoardDB really has.** When a
  server's key differed from the remembered one, the CLI told you to remove the
  old entry from `known_hosts`, an SSH file hoardDB does not use. The refusal now
  says to delete the line for that server from the `known_servers` file and prints
  the path (for example `~/.hoarddb/known_servers`); the internode refusal prints
  the path too.
- **A `get` in the CLI no longer walks the bucket's stores.** `db.User.get("u1")`
  asked the server for the bucket's type with `BUCKET_STATS`, which counts the
  elements of every store field by walking them, so each read cost server time
  in proportion to what the bucket's stores held (a large field made each `get` slow). The CLI now asks the
  declaration call that walks nothing, once per statement: 100 `get`s are 100
  `BUCKET_INFO` and no `BUCKET_STATS`, and almost no server time. The same went for a
  `push` with a numeric `priority` and a field `put` or `push` that needs the
  field's engine. Every one of those lookups is a fresh call, never remembered, so
  a bucket another session dropped and recreated as another type is read as the
  type it is now. Against a server older than the call the CLI asks
  `BUCKET_STATS`, as before. What is unchanged: every printed line and error. One
  lookup still walks the stores: the engine of a nested store field.

- **A write refused on a single node now says nothing was written.** An error frame for a write is meant to carry `write_outcome`, so
  a driver can tell a developer whether a retry is safe, but a standalone node
  left it out of every refusal: a put to a bucket that does not exist, a
  duplicate key, a mistyped document, an update its operators refuse, a field
  verb on a missing document. So did any node, cluster member or not, for a
  write refused for permission or for the document or blob size limit. A
  driver that follows the handbook then told the developer the outcome was
  unknown and to read the document back before resending, about a write that
  never happened. Every refusal of a data write decided before anything was
  applied now carries `write_outcome: not_applied`, on a single node as on a
  cluster. Reads and DDL still carry none, and a write a single node's own
  store failed still carries none, which a driver reads as unknown. Twelve
  driver vectors that pin a refused write now carry the field, and two new
  ones pin a put and a fifo push to a bucket that does not exist.

- **A node restarted during its placement pass no longer adopts the next
  membership change before it has finished that pass.** A member
  that restarted before it had finished marking what a membership change moved
  could take the next change from a peer in the moment between opening its
  internode port and resuming the pass. The buckets the first change moved to
  it were then never marked. As their new owner it answered reads from its own
  copy before taking them over, and that copy could be missing writes the
  previous owner had acknowledged, so such a read could answer
  `document not found`. And if the resumed pass finished after the newer one,
  the node then refused every membership change ("this node is still marking
  the buckets the last membership change moved; ask again in a moment") and
  took no bucket over until it was restarted. A node now adopts no later change
  until its own pass has finished, and the epoch a pass records as finished
  never goes backwards.

- **The survivor of a removal, restarted during its placement pass, now takes
  the removed node's buckets over.** A node left as the only member
  by `remove-node` that restarted before it had finished marking what the
  removal moved never finished that pass. It took no bucket over, so each one the
  removed node held answered `bucket not found` on it and `migrate` answered
  "Retry in a moment", and it refused every membership change, so a `join` could
  not get it out either. The node now finishes an interrupted placement pass at
  start, alone or not. A node that was always alone has no pass to run.

- **A refused `migrate` answers with its reason and a retryable code, and keeps
  the connection.** A `migrate` the node refused, for example while
  ownership of the bucket was still moving to it, was answered `INTERNAL`, and
  the connection was closed. It is now answered `UNAVAILABLE`, with the same
  message, and the connection serves the next command. Other refusals keep
  their message and code, and no longer close the connection. Between nodes, a member's refusal now reaches
  the node that asked, with its reason. A member that dropped the incarnation
  of a bucket that another node still holds now tells that node, and that node
  drops its copy instead of offering it again.

- **The offline `-reset-password` counts password characters, as the CLI does.** It counted bytes, so a password of two-byte characters could pass
  the reset while the CLI refused it, and the refusal reported bytes as
  characters. Eleven `é` (22 bytes) are now refused at a minimum of 12 with
  "11 characters", and twelve `é` are accepted. ASCII passwords are unaffected.

- **The survivor of a removal, restarted alone, now takes the removed node's
  buckets over.** A node left as the only member by `remove-node`
  that restarted before it had taken over the buckets the removed node held came
  back without its take-over worker. Each such bucket answered `bucket not
  found` on it, and `migrate <database> <bucket>` answered "Retry in a moment"
  every time, until another node joined. The node now starts its take-over
  worker at start when its membership history names a removed node. A node
  that was always alone still starts none.

- **The dump and restore warning no longer says a restored blob bucket comes back
  with its keys and metadata.** restore discards those documents, so
  the bucket comes back empty; the warning now says so and names the dump's
  `.bson` and `.metadata.json` files where the keys and metadata remain.

- **Removing a running node at replication factor 1 now hands its buckets
  over.** Before, `remove-node` of a running node reported success,
  but no member ever took over a factor-1 bucket whose only copy was on the
  removed node: it answered `bucket not found` on every member until an operator
  ran `migrate`, and emptying the removed node's data directory lost it. The
  removed node now asks each bucket's new owner to take the bucket from it, at
  once and again at every start while it holds copies, keeps every copy, and logs
  `every bucket this removed node holds is served by its members` when it is safe
  to empty. It hands back no bucket dropped since, and never puts its copy over a
  bucket whose name was created again: it warns, naming that bucket, that its
  copy holds the only copy of those writes, and does not log the line. With
  `auto_rebalance` off it moves nothing and logs how many buckets
  it holds, with the `migrate` command to run. A bucket dropped on a node left
  alone by a removal is now recorded as dropped, so neither the removed node nor
  an operator's `migrate` brings it back. A node removed from a two-node
  cluster and restarted now also opens its streams to the remaining member, so
  that member can copy from it.

- **`consistent get` no longer brings back a deleted key** from a replica that
  missed the delete: when the bucket's owner does not hold the key,
  its copies the owner has passed are answered "not found" and removed, by a
  delete that spares a write made during the read.
- **`consistent get` no longer returns, and repairs onto every copy, an older
  value after an owner change**: versions are ordered by membership
  term before sequence.

- **A failed membership change no longer leaves a member on a member set that
  never took effect.** A member used to adopt a proposed set when it voted for
  it; if the change then failed, the member stayed on that set at the epoch
  number another change could take with a different set. It could take buckets
  over as a second owner, and a later change could replace the real owner's copy
  with its own, losing writes the real owner had acknowledged, at write concern
  `majority` too. A cluster that changed membership on an older build may hold
  such a member: after upgrading, check with `nodes history` and follow
  "Upgrading a cluster that changed membership on an older release" in the
  replication guide before any membership change. The check reaches back 64
  epochs; that limits what can be checked, not the danger, and an older cluster
  whose history does not reach its first change on an older build should be
  rebuilt from a backup.

- After a `join` or `remove-node`, a bucket's new owner no longer serves or
  copies whatever copy it finds first. It takes the bucket over from the most
  complete copy any node holds — the previous owner's when it answers, otherwise
  the best copy every current member and every recently removed node reports —
  before it accepts a write, so a write acknowledged before the change is not
  lost because the new owner was seeded from a replica that was behind, or from a copy frozen outside the bucket's set.
- A node that leaves a bucket's replica set no longer deletes its copy as soon as
  one node has pulled it, nor when the members merely say they serve the bucket:
  it keeps the copy until every member of the set holds everything it holds. A holder therefore never releases its copy to an owner's empty
  copy of the same name; two copies of one name created apart are
  both kept, and the name refuses writes until one goes.
- A new owner no longer re-sends, into its own term, entries it applied as a
  replica, which a replica then applied a second time.
- A write acknowledged during a membership change under load is no longer lost,
  and a queue element is no longer applied twice: the old owner finishes
  delivering what it accepted before the change, and every copy follows the new
  owner's term.
- A drop now reaches a copy left on a node outside the bucket's replica set, and a drop during a hand-off is not undone by a copy pulled back from
  a node that had not applied it yet.
- A drop at a node that owns a bucket it has not copied is no longer logged and
  answered `not_applied` while the bucket stays on its holders.
- A pulled copy is synced before the node answers that it holds it.
- A replica no longer acknowledges a write it refused, live or re-sent from the
  owner's log.
- A database drop no longer forgets the failed writes, and the log they need, of a
  bucket re-created under the same name while the drop ran.

- `peek(N)` on a store field no longer answers with a single element and no
  error. The count was accepted and discarded, so `peek(3)` returned one record
  as though three had been asked for, and repeated `peek()` returns the same
  head — so a client looping `peek(N)` would have collected N copies of one
  element. `peek(N)` is now refused by name, the way the bucket verb already
  refused it, with the message naming `list({limit: N})` as the way to read
  several. `peek()` itself is unchanged.
- The help no longer advertises `db.<bucket>.peek(N)` in three places, where the
  bucket verb has always refused it. A test now fails if the help offers a verb
  the CLI refuses.
- `search` now says which index served it. The server put `index_used` on every
  search response and the CLI dropped it, so an index hit and a full scan were
  indistinguishable — and an operator filter (`$gt`, `$regex`, `$in`) on an
  indexed field always scans, because an operator is not an equality the index
  key can hold. The header names the index (`Found 1 results (via Email_1):`)
  or says `(full scan)`; `NO RESULTS` says it too, so an empty answer is
  explainable. `find` is unchanged and still prints only results.
- `create database` now says both spellings of a store type. It showed only
  `type: hash | btree | fifo | lifo | heap | blob`, which is correct for a
  bucket's engine but led straight to `create bucket Post { Photos lifo }` — and
  a store field's engine carries a `Store` suffix (`Photos lifoStore`), so the
  declaration parser refused it. The hint now prints the field spelling
  (`hashStore`, `btreeStore`, `fifoStore`, `lifoStore`, `heapStore`, `blobStore`)
  and a worked example. Both lists come from the one store-type table, so a
  seventh type reaches the hint, the parser and Tab completion together.
- The REPL prompt is never blank because of a theme file. A file that set only some
  styles — the usual `theme.yml`, recolouring the `[h]` mark — resolved to an empty
  prompt string, so the terminal showed a bare cursor and no `hoarddb>` at all.
  Anything a theme file leaves out now comes from the built-in theme: set only
  `styles.tile.bg` and the prompt keeps its text, its other colours and both format
  strings.
- A node whose store cannot persist what it is given (its data directory removed, its disk full or failing)
  now refuses writes with an error that names the store directory and the cause, reports itself unhealthy in
  `status` (a `Store: REFUSING WRITES` row) and on `/ready` (503), and stops when asked. It used to acknowledge
  writes while retrying the failed flush every second forever, then block every write, and never stop; with
  its data directory removed, the first full memtable ended the process. Writes are accepted again once a
  flush succeeds.
- A damaged byte in the trailer of a store file's block (its entry count, its checksum) is now an error like
  any other damaged block. It could end the process.
- A read of a document in a damaged block of a store file now fails with an error that names the
  bucket, says the store file is damaged and how to restore the node from a replica or a backup,
  and a walk across the block (a search, a dump, a page) fails rather than leave the block's
  documents out. It used to answer "document not found" for an encrypted store, and a wrong value
  for a plaintext one, which verified no block checksum on read; every store now does. A compaction
  that meets such a block now fails and keeps the file instead of deleting the documents past it.
  The node counts these reads in `hoarddb_store_damaged_reads_total` and logs the first.
- A node started for joining no longer answers requests before it has been added with `join`, and
  the members no longer refuse writes while it waits. A node started for joining a
  cluster that has already changed membership no longer records itself removed before its `join`,
  which kept it refusing every write after it.
- A node whose own proposal of a membership change waited on a slow vote no longer moves back to
  that change after it has already caught up to a later one.
- A node that restarts after missing two or more membership changes now catches up on them one at a
  time even when several members gossip with it at once. It could adopt a missed change once for
  each member whose answer arrived together, and a member's gossip arriving mid catch-up could make it adopt a later
  change before it had marked the buckets the earlier one moved, so it recorded those moves under
  the wrong change.
- A `GET_FAST` whose compact payload does not decode now answers an error frame naming the decode
  failure and keeps the connection, as every other handler does. It used to answer INTERNAL and close
  the connection.
- A `create bucket`, restore or ingest sent to a node that had just joined, for a
  bucket it owned but had not copied in yet, removed that bucket's documents on
  every node. Such a create is now refused with "unavailable, retry" until the
  copy lands, and a node keeps its copy of a bucket against a create that did not
  know about it. A `create database` at a node that has not learned the database
  now answers that it already exists when another node holds it, instead of
  creating a second one. A node that has not learned a database refuses a create
  in it, with retry, while it cannot reach every member, and names them.
- The Docker Compose `pki` service no longer rewrites `/pki/root.password` when you set
  `HOARDDB_ROOT_PASSWORD` on a later run. The root user is created once, so the rewritten file
  held a password the cluster refused and the documented read-back command printed it. The password
  is now fixed at the first start (the service says so if you set one later); change it with
  `hoarddb-server -reset-password admin`. `HOARDDB_ROOT_PASSWORD_FILE` is documented in the
  configuration reference.
- A compact point read (`GET_FAST`) naming a database or bucket that does not
  exist now answers `NOT_FOUND` and keeps the connection open. It used to answer
  `INTERNAL` and close the connection, which dropped every request pipelined
  behind it.
- A compact point read (`GET_FAST`) sent to a cluster node that does not hold the
  bucket was answered from that node's own storage, so it missed a key the owner
  holds. It is now routed to the owner like `GET`.
- `now()` inside an array, such as `{Dates: [now()]}` or `{Events: [{at: now()}]}`,
  was stored as the document `{$$now: true}` instead of a date. It is now stored as
  the server's time, in an insert and in a filter's `$and`, `$or` and `$in` lists.
  The `$inc` and `$mul` refusals on a field that holds something other than a
  number now name the stored type with the same words every other type refusal
  uses: "a boolean", "a document", "a datetime" and "a 128-bit decimal" where they
  said "a bool", "a sub-document", "a date" and "a decimal128".
- `now()` in a write that names its own key (a key, or `_id_` in the document) was
  stamped by each copy's own clock, so on a replicated bucket the copies of one
  write held different dates, and a restart that replayed the write changed the
  date again. The same held for a batch, a FIFO, LIFO or heap push and a
  field-store write. The date is now fixed once, on the node the write reaches,
  before it is logged, and every copy and every restart keep it. Every `now()` in
  one request is the same instant, so a batch's documents share one date.
- `now()` in an update (`$set`, `$push`, `$addToSet`, `$min`, `$max`, and a multi
  update's filter) was stored as the document `{$$now: true}`. It is now a date,
  the same one on every copy.
- A btree insert that named no key was given a different key on each copy. Its
  key is now chosen once, before the write is logged, as a put's is.
- An update whose `$rename` pairs overlapped (`{a: "b", b: "c"}`) could leave the
  owner and its replicas holding different documents. Such an update is now
  refused before anything is written.
- A unique index declared on a dotted path such as `a.b` refused distinct values
  as duplicates of `null`. An index on a path is now refused when it is declared.
- `consistent get` printed `not found` when no holder had the key but some could
  not answer (a peer down, a refusal). It now refuses, says how many holders could
  not answer, and exits with the refused code; `not found` means every holder said
  the key is absent.
- A mistyped write concern, such as `majorty`, was acknowledged at level `one`,
  the weakest durability, with a success reply. A request now gets an
  `INVALID_ARGUMENT` error that lists `1`, `one`, `majority` and `all`, and a
  mistyped `HOARDDB_WRITE_CONCERN` stops the server at start.
- A write acknowledged as durable could be lost to a power failure that arrived
  while the store was flushing a full write buffer; the store now syncs that
  buffer too.
- A write acknowledged as durable could be lost to a power failure after one
  update wrote more than about 128 MiB of large values; the store now syncs a
  value-log file when it rotates.
- A replica could fall behind silently, or miss an acknowledged write after its
  owner restarted. Replication positions now live in the node's store and a
  replica is repaired from the log.
- A server started with no configuration on a machine with fewer than four CPUs
  failed to start.
- A batch refused for its size stays refused when the log is replayed at the next
  restart; it is not applied after all.
- Dropping a database no longer detaches a bucket of a newer database that has
  the same name.
- A piped `create user` (or `alter user ... password`) failed with `read password:
  EOF`, and `--password-stdin` could not work in a pipe. A piped session now reads
  the password from the lines after the statement (two lines, or one with
  `--password-stdin`); if the script ends first the error names `--password-file`.
  `--password-stdin` on a terminal is refused.
- A `connect <address>` that failed closed the session it was switching away
  from. The new server is now dialled first and the old session is closed only on
  success; a failed connect keeps the old session and the error says so.
- Exit codes of `dump` and `restore` now follow the exit table. `restore` of a
  missing or unreadable `--dir` exits `1` (it exited `3`, which means "partial"),
  and `dump` of a database that does not exist exits `2`, refused (it exited `1`).
- `alter user <name> roles [...]` lowercased the whole list, so a database named
  `Shop` was stored as `shop` and the grant matched nothing. Role and database
  names now keep the case you typed; only the keywords are case-insensitive.
- `$inc` with a fraction was truncated and acknowledged (`1.5` added `1`, `0.5`
  added nothing, `1e30` added an undefined amount). The CLI now refuses a delta
  that is not a whole number within 64 bits, names the field, and says to write a
  whole number or `$set` the value; the document is unchanged.
- `ISODate("...")` and `new Date("...")` could not be typed at the prompt: the
  statement parser took the last `(` on the line for the method call and refused
  the statement as an unknown bucket method. They now work in an insert and in a
  filter.
- `now()` can be typed in a document or an update at the prompt, such as
  `db.Sessions.insert({_id_: "s1", CreatedAt: now()})`. It was refused by name
  because only a scan filter resolved it. The server now reads its clock once per
  write, before the write is logged, so the prompt sends `now()` and every copy
  stores the same date; a TTL index no longer needs a hand-typed timestamp.
  `now().add(...)` is still refused, with a message that says to write `now()`
  alone or a fixed date.
- The compose file published each node's client port, and the proxy's, on every
  interface of the host, with a root password of `test123` that the file, the
  docs and this project made public. The ports are now published on `127.0.0.1`,
  and the first run generates the root password into the `pki` volume
  (`docker compose exec -T node1 cat /pki/root.password`); set
  `HOARDDB_ROOT_PASSWORD` to choose your own. To serve other machines, edit the
  port lines to drop `127.0.0.1:`.
- A `~/.hoarddb/known_servers` file that holds two different keys for one host made
  the CLI trust whichever line came last, so an appended line outranked the key you
  verified. It now refuses to start, names the file, the host and both fingerprints,
  and says to delete the line for the key you do not trust. The same key listed twice
  is still one entry.
- **A cluster grown from one node now settles its joins.** A node started
  alone never started its take-over, auth-config or WAL-retention workers, and a
  `join` did not start them either. Its first join stayed `Schema changes: PAUSED`
  ("n1 cannot finish handing off until n1 answers"), and a write at `majority` to a
  bucket the joiner took over was stored on the joiner alone. The founder also never
  pushed auth changes, never followed a new auth master, and never pruned its log.
  Restarting the founder used to be the workaround. It now starts these workers at
  the join that gives it its first member.
- **`create bucket` with a unique field no longer prints a Go map.** The
  confirmation read `Indexes: [map[fields:[Email] unique:true]]`; it now reads
  `Indexes: [Email (unique)]`, the form `describe bucket` uses. A plain index
  still prints `Indexes: [Email]`.
- **`peek` on an empty queue or heap field prints `EMPTY`.** It printed
  `NOT FOUND`, while `pop` on the same field and `peek` on a bucket print
  `EMPTY`. `NOT FOUND` stays the answer for a `get` of a missing key.
- **`dump --json` and `restore --json` print `[]` for an empty list.** A
  successful run printed `"errors":null`, and a run that dumped or restored no
  database printed `"databases":null`; a script that iterates either list failed
  on the success path. Both are now `[]`. If a script treated `null` as empty,
  it needs no change.
- **A statement that takes no arguments refuses a trailing word.** `use app
  extra` switched to `app`, `status extra` printed the status, and
  `rotate-token --dry-run` rotated the cluster token, because the parser read
  what it needed and ignored the rest. `status`, `version`, `exit`, `quit`,
  `get-token`, `rotate-token`, `encryption`, `master` and `theme` now answer
  `usage: <statement> takes no arguments (got "extra")`, `use` answers
  `usage: use <database> takes one name (got "app extra")`, and `nodes` accepts
  only `history`; none of them runs anything.
- **`dump` and `restore` take their flags after the database too.** `hoardDB dump
  app --out ./d`, and the same statement in the REPL, was refused as "too many
  positional arguments" although the usage line is `dump [database [bucket]]
  [flags]`, because parsing stopped at the first word that was not a flag. Flags
  are now read wherever they stand; `dump --out ./d app` still works.
  `rotate-token` has no dry run; type it alone.

### Known gaps

Things the project knows are not done, so that they are not discovered at 3am:

- There are no release binaries or published image yet, and the source repository
  is not public yet.
- Certificates other than the one a node generates for itself cannot be used;
  the `HOARDDB_TLS_*` settings are read and not applied.
- Per-database and per-bucket encryption keys, changing a bucket's store type in
  place, and chunked streaming of large blobs are planned and not built.
- A cluster is upgraded with every node stopped; rolling upgrades are not
  supported. See [docs/user/compatibility.md](docs/compatibility.md).
