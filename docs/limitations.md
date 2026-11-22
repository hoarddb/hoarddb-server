# Limitations

hoardDB's first release is 1.1. This page is the honest list of what it does not
do or does not do the way you might expect. Read it before you design around it. The sizes
and counts that hoardDB enforces, with their defaults and how to change them,
are on one page: [Limits](limits.md).

## Not available yet

The ring is the engine this documentation describes, and a plain `hoarddb` session
opens it ([Getting started](getting-started.md)). A bucket declares fields, and a field
can be a store of any of the six types, one per document. This section lists what
is not available yet. `help all` in a `hoarddb` session lists the statements that run, and
under "Not available yet" the ones that parse and are refused.

- **A heap store cannot be pushed to from the CLI, and a blob store cannot be put to.** You can
  declare `heapStore` and `blobStore` fields. A heap store reads with `peek`,
  `list`, `count` and `pop`; the Go client's `Bucket.PushHeap` adds to it, and the
  CLI's `push` and `take` on a heap are refused with "not available yet".
  A blob store answers `count`, and `put` is refused. A chunked blob upload, which
  sends a large value in pieces under one upload id, is not available yet. The other four
  stores (`fifoStore`, `lifoStore`, `hashStore`, `btreeStore`) run every verb in
  `help types`.
- **There is no timeline read yet.** One read that fetches the newest elements of
  many documents' stores together, such as a merged feed, is not available yet. Today it is
  one request per store. A single request can already carry up to 255 writes (see
  the next item).
- **Batching is on the wire, not in the CLI.** A batch carries up to 255 writes in
  one request, to any buckets. It is not a transaction: each write is applied or
  refused on its own, a refused write does not stop the others, and writes to one
  document apply in the order sent. After a lost answer, resend the whole request
  with the same request id and the writes that were applied answer `deduped`, for
  as long as they are still inside `replication.dedupe_window`.
  There is no `batch` statement; the Go ring client in the server
  module has `NewBatch`, and the client libraries are pre-release.
- **There are no indexes yet.** A `list` with a `filter` reads every
  document of the bucket. Indexes declared when a bucket is created are not available yet.
  [Indexes](indexes.md) shows a keyed lookup that does not scan.
- **Dropping asks for no confirmation.** `drop bucket` runs at once, with no prompt
  and no `--yes`. `drop database` is not available yet.
- **`alter database` is not available yet.** A database is created with
  `create database app` (one copy) or `create database app { replication_factor: n }`,
  and its factor cannot be changed afterwards.
- **A scalar field's declared type is not checked on write.** A field declared `int`
  holds a string if you write one. The server refuses a field name the bucket does
  not declare, and nothing else about the value ([Data types](data-types.md)).
- **A store's type never changes.** `alter bucket` adds and drops fields. To change
  a store's type, add a new field, copy, and drop the old one.
- **`pop` is at-most-once: an element whose answer does not reach you is gone.**
  A `pop` removes the elements and returns them in its answer, and the answer is
  the only copy. If the connection drops before the answer arrives, run `peek` to
  see what is left, and do not pop again to look for them, because a pop takes the
  next elements. For a `fifoStore` or `lifoStore`, use `claim` and `ack`: the
  elements stay in the store under a lease, and a lost answer becomes a redelivery
  instead of a loss.
- **A message that keeps failing is dead-lettered, and it cannot be listed.**
  A lease that ends without an `ack` returns its elements to the queue. An element
  delivered 5 times without an `ack` is dead-lettered: it leaves the queue. There
  is no statement to list, requeue or discard dead-lettered messages.
- **Encryption at rest seals what each node itself stores: its ring store, its
  log and its credential files, and nothing more.** What it does not cover:
  - **The cluster does not check that every node uses the same encryption
    setting.** A mixed-setting cluster is not checked in this release. A node with encryption off can join a cluster whose other nodes
    run with it on, and it keeps plaintext copies of the records it holds,
    including copies of records the other nodes keep sealed. Run every node
    with the same setting and check each one ([Security](security.md#check-every-node-of-a-cluster)).
  - The cluster's metadata in `ring/raft` (database names, member ids and
    addresses, settings), server logs (which may contain document ids, bucket
    names and store-field keys), dumps, memory and swap, and freed disk blocks.
    Use disk encryption for them.
  - A node's mode cannot be switched in place, and the keys of a node that holds
    data cannot be replaced: there is no re-key.
  - An encrypted ring store cut by a crash during its very first flush is
    refused at the next start, not repaired as a plaintext one is.
- **`batch` has no statement.** Batching is on the wire (see the batching item
  above). `hoarddb dump --out <file>`
  writes one file from the ring, and `hoarddb restore --verify <file>` checks that file
  without a server. `hoarddb restore <file>` puts a dump back into a ring that does not hold
  its databases yet: it refuses a database that already holds buckets when it starts on it, never merges and has
  no overwrite flag, does not restore users or roles, and restores a blob field empty
  ([Backup and restore](backup-restore.md)). A bucket that links to another bucket is
  restored (the target is created first); restore refuses before it creates anything a link
  whose target is not in the dump or on the target ring, a link to a bucket that was dropped
  before the dump, and a circle; databases that link into each other must be restored together, and any link across databases makes them a group — see [Links
  between buckets](backup-restore.md#links-between-buckets). A writer that creates
  objects in the target while a restore runs can still be adopted in the one round trip
  between the restore's check and its create, and a bucket with a different name that appears
  in an existing empty database during the restore is not refused. A session that may create
  buckets but not describe them gets no protection against adopting an equal bucket. `hoarddb ingest <file> --into <database>.<bucket>`
  loads one BSON or JSON Lines file into one bucket, creating the database and
  the bucket when they are absent ([Load a file](ingest.md)).
- **A dump is not a point-in-time copy.** Stop the writers for a copy of one
  moment; queue messages that are claimed or dead-lettered, and blob payloads,
  are not in a dump ([Backup and restore](backup-restore.md#what-a-dump-guarantees)).

### Cluster

- **`join` works on an empty cluster.** Start the new node with the first node in
  its seed list, pin its key on every member, then run `join <node>@<host:port>`
  on any member. The join gives the node a vote while the cluster has a free
  voter slot, and `promote <node>` gives one later to a node that joined without
  one ([Configuration](configuration.md#cluster-mode) has the steps). Nothing
  frees a slot yet, because `leave` and `remove-node` are not available. Cluster
  membership and database definitions are agreed through one leader (Raft), which
  the cluster elects.
- **`join` is refused once any database exists.** A joining node cannot yet be
  seeded with the data it would own. Form the cluster first, then create data.
- **Replication, seeding and databases with more than one copy are not usable end
  to end.** `create database app { replication_factor: 2 }` is refused on a
  cluster that cannot hold it, and a cluster that can is not yet a promise that
  every copy is written and readable. `alter database`, `show replication`,
  `show placement`, `leave` and `remove-node` parse and are refused.
  `replace-node` brings back a member that lost its data, at its own address
  only. `status seeding`, `seeding retry` and `seeding forget` run on a node
  that belongs to a cluster. `seeding forget` removes this node's record of
  a retired slot and leaves the slot's data on disk.
  If the node is later named a copy of that slot again, or already is one when
  you run the command, the node accepts no write for the slot and queues it
  for a seed. It deletes the kept data once another copy has accepted that
  pull, and then seeds the slot again from it; `status seeding` lists the slot
  while that runs. While no other copy can be pulled from, the data stays on
  disk, the slot stays `seeding` and takes no write on this node, and its
  status row says so; it recovers by itself when a copy appears. If the node
  leaves the slot's copy set again before any copy accepted, it deletes the
  kept data then, as it does for any slot it was still seeding. `seeding
  forget` therefore refuses a retired slot
  that no other member holds a copy of, because this node may hold the only
  copy of its data; `seeding forget <db> <slot> anyway` goes ahead.

## User management and authorization

- **In a cluster (`cluster.enabled` on), users are the cluster's, with these
  limits in this build.** A user change needs a majority of the voters; a login
  is checked against the member's own copy, so a member cut off from the others
  keeps accepting an account dropped, disabled or re-passworded on the other side
  until it rejoins. There is no
  offline password reset in a cluster: `-reset-password` refuses on a cluster
  member, so keep two administrators. User names, roles (with the database names in them) and password hashes
  are kept in the cluster's metadata (`ring/raft`), which is not encrypted, at
  every `encryption.mode`; the hashes cannot be checked without the cluster's
  password key (`auth.cluster.pepper`), but the names and roles are readable.
- **Role changes apply at the next AUTH.** Authorization is decided per
  connection from the identity established at AUTH. A `grant`, `revoke`, or
  `alter user ... roles` does not change what an already authenticated
  connection may do and does not terminate it; the change takes effect when that
  client authenticates again. If you need the change to bite immediately, change
  the password or disable the user (below).
- **Disable, drop and a new password close that user's sessions on every
  member.** These are the deliberate exception: a disable, a drop and a password
  change close every session that user has open on a member when that member
  applies the change, normally a moment after it is answered. `grant` and
  `revoke` do not close sessions.
- **A member cut off from the cluster keeps serving logins from the users it
  last applied, with no time bound.** It cannot learn of a drop, a disable or a
  new password made on the other side until it rejoins: until then it accepts
  that account's password and its unexpired tokens, and its open sessions of that account
  go on. It changes no user while cut off (a change needs the cluster). This is
  the deliberate trade-off: an administrator can always log in to a member of a
  broken cluster to look at it. `auth_config_max_staleness_seconds` has no effect
  on a cluster member.
- **Revocation is per user, not per token.** Changing a password, disabling a
  user, or a role change bumps that user's `epoch` and refuses all their
  outstanding tokens at their next refresh. There is no way to revoke one
  individual token without affecting the rest.
- **Every node that holds users holds every password hash.** A node without a
  cluster keeps them in `users.json`. A cluster member has no `users.json`: every
  member, witnesses included, keeps the cluster's users in the cluster's metadata
  (`ring/raft`). Decommissioning a node means deleting that file or directory;
  a node taken out of service keeps its copy until you delete it.
- **On a node without a cluster, the user store is sealed under a key your
  backups must keep.** `users.json` is encrypted under the node master key when
  encryption at rest is on, and under the node's auth signing key when it is off.
  A node that starts without that key refuses to start, and `-reset-password`
  cannot recover it: it reads the same file. Losing the key loses every user and
  role in the file, not just the ability to check a password.
- **In a cluster, passwords are checked with the cluster's password key**
  (`auth.cluster.pepper`, in each data member's data directory). A data member
  that does not hold it refuses every password login and says why, and asks the
  other members for it. If no member holds it any more, no password can be
  checked: restore the file from a backup of a member's own data directory.

## Expiry

- **Expiry takes a `datetime` field only.** `expire at <Field>` names a declared `datetime` field, not `any`. There is no duration form: you write the expiry instant.
- **Only `fifoStore` and `lifoStore` elements expire.** `expire at "<path>"` is refused on a hash, btree, heap or blob store.
- **Removal is background work, reads are not.** An expired document or element is invisible to every read at once, but the node removes it on its retention tick, within the reclaim budget; a backlog larger than the budget is removed over several ticks. One pop or claim removes at most 64 expired elements and may deliver fewer than asked when more are waiting.
- **`set expire at` is refused on a bucket that already holds documents,** and the check and the schema bump are two steps: a document written between them, or between the alter and its publication, is not covered by the expiry. Declare the expiry when you create the bucket.
- **A clock that steps back brings an expired document back to view until the clock catches up;** the removal waits for the clock too.

## Source and distribution

- **You build it from source.** hoardDB is source-available: the server under
  Business Source License 1.1 at `github.com/hoarddb/hoarddb-server`, the client
  libraries under Apache-2.0 in their own repositories. No release binary or
  image is published yet, and the client libraries are pre-release.
