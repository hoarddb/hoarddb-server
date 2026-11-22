# Configuration

All configuration is done via environment variables or a YAML config file named with `--config` (see [YAML Config File](#yaml-config-file)). Environment variables take precedence over the file, and the file over the built-in defaults.

When a variable replaces a value the file sets, and the two differ, the server logs one warning on stderr at start for each such key. It names the key, both values and the variable to unset, for example `metrics.listen is set in the config file to 127.0.0.1:9090, but HOARDDB_METRICS_LISTEN=0.0.0.0:9090 overrides it`. The variable still wins. The official Docker image sets `HOARDDB_DATA_DIR`, `HOARDDB_LISTEN`, `HOARDDB_TCP_LISTEN` and `HOARDDB_METRICS_LISTEN` itself, so a mounted file that sets those keys needs `-e HOARDDB_METRICS_LISTEN=` (an empty variable is ignored) to take effect.

[Default settings](defaults.md) lists every setting with the value it has when you set nothing.

A variable that holds a number or a switch must parse. A number is a plain integer with no unit, space or fraction (`HOARDDB_MEMTABLE_MB=64`, not `64MB` or `64.5`); a switch is `true`, `false`, `1`, `0`, `yes` or `no`. The switches are `HOARDDB_METRICS_ENABLED`, `HOARDDB_AUDIT_ENABLED`, `HOARDDB_PPROF_ENABLED`, `HOARDDB_AUTO_REBALANCE`, `HOARDDB_AUTH_BOOTSTRAP_ROOT`, `HOARDDB_REQUIRE_ROOT_PASSWORD` and `HOARDDB_UNSAFE_ACK_BEFORE_SYNC`. Any other value stops the server at start with a message that names the variable, the value and the fix, and lists every bad variable at once. It is never replaced by the default.

## Environment Variables

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_TCP_LISTEN` | `127.0.0.1:7433` | TCP+TLS driver listener address. The default is this machine only; set `0.0.0.0:7433` (or one interface's address) to accept clients from other machines. |
| `HOARDDB_LISTEN` | `127.0.0.1:4433` | This node's address in the ring: the address its peers are told to reach it on. The internode port is this port plus one (`4434`). Set it to the node's own address, such as `192.0.2.1:4433`, on every node of a cluster that spans machines. A node whose seeds are on another machine but which is still on loopback is refused at start. See [Security](security.md#opening-a-node-to-other-machines). |
| `HOARDDB_TCP_ADVERTISE` | empty | The `host:port` a member of the entry-ring cluster tells drivers to connect to (yaml `tcp.advertise`); it takes effect once the node is in a ring cluster (`cluster.enabled`), and a node that is not logs a warning and ignores it. Set it when `HOARDDB_TCP_LISTEN` is a wildcard or loopback address. It must be on the same host as the node's cluster address (`cluster.advertise`, else `cluster.listen`), and a different port is fine: a node refuses to start with any other host. Empty: the node publishes `HOARDDB_TCP_LISTEN` when another machine could dial it, and nothing when it is a wildcard or loopback address; a member that publishes nothing has no driver address, and a driver that routes by the cluster's topology skips it. |
| `HOARDDB_TCP_INTERNODE_LISTEN` | derived from `HOARDDB_LISTEN` | The internode listener address, when it must differ from `HOARDDB_LISTEN`'s port plus one. |
| `HOARDDB_DATA_DIR` | `./data` | Data directory, relative to the working directory. |
| `HOARDDB_BLOB_DIR` | `<HOARDDB_DATA_DIR>/blobs` | Where blob buckets keep their payload files. Set it to put large payloads on a volume of their own, for example `HOARDDB_BLOB_DIR=/mnt/blobs`. It belongs to one node: the server refuses to start on a directory another running node holds, and refuses one inside the data directory's `store`, `keys`, `wal` or `cluster`. A database cannot be named after the directory (`blobs` by default). When the directory is on a different filesystem from the data directory, the disk ratio and the join guard cover both (a `blob_disk` resource beside `disk`). |
| `HOARDDB_MAX_BLOB_BYTES` | `16777216` (16 MiB) | The largest blob, in bytes, this node accepts from a client. Raise it to store bigger blobs, up to `33488896` (31.9 MiB), the largest blob that fits one 32 MiB frame with its envelope and its copy for a replica. A value below 1 or above that stops the server from starting, with an error naming the key and the range. A larger blob is refused with the limit in the message; keep larger files on your own storage instead, see [Large file stores](large-files.md). A write that reaches a replica is not checked against the replica's own value, so keep the setting the same across a cluster. YAML key: `storage.max_blob_bytes`. |
| `HOARDDB_MAX_DOCUMENT_BYTES` | `25165824` (24 MiB) | The largest single document, in encoded bytes, this node accepts from a client. It counts one document's own bytes, not its bucket and not the elements of its store fields, which are stored separately. Lower it to as little as `4096`, or raise it to at most `31457280` (30 MiB), the largest document that fits a write frame, its copy for a replica and a dump. A value outside that range stops the server from starting, with an error naming the key and the range. A larger document is refused with the limit in the message; a list that grows belongs in a nested store field, and a large value in a blob bucket or behind a pointer document, see [Large file stores](large-files.md). A write that reaches a replica is not checked against the replica's own value, so keep the setting the same across a cluster. YAML key: `storage.max_document_bytes`. |
| `HOARDDB_NODE_ID` | `hostname` | Unique node identifier. The host name default is never matched to a `HOARDDB_SEED_NODES` entry by id, because cloned hosts share one: a node listening on `0.0.0.0` and listed as `<node-id>@<address>` must set it. A node whose id is listed at another node's address stops at start. |
| `HOARDDB_SEED_NODES` | (none) | Comma-separated `host:port` list for cluster formation |

### TLS

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_TLS_MODE` | `auto` | `auto` is the only mode in this version: every node serves the certificate it generated itself. Any other value, including `custom`, stops the server at start-up with an error (see [Custom certificates are not available yet](#custom-certificates-are-not-available-yet)) |
| `HOARDDB_TLS_CERT` | (none) | Not available in this version; setting it stops the server at start-up with an error |
| `HOARDDB_TLS_KEY` | (none) | Not available in this version; setting it stops the server at start-up with an error |
| `HOARDDB_TLS_CA` | (none) | Not available in this version; setting it stops the server at start-up with an error |

### Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_ROOT_USER` | `admin` | Root username for first start. |
| `HOARDDB_ROOT_PASSWORD` | asked on a terminal on first start | Root password, used only to create the root user on the first start. If unset, a first start from a terminal asks for one (twice), and a first start with no terminal refuses, writes nothing and says to set it. Only its hash is kept, in `<HOARDDB_DATA_DIR>/users.json` (in a cluster, in the cluster's state: the first data node started with it creates the cluster's first administrator, and a node of a cluster that has users, a joiner or a witness ignores it with a warning); the password itself is never written to disk. The environment value is wiped after use. Later changes go through `hoarddb-server -reset-password`. A container always needs it (the image sets `HOARDDB_REQUIRE_ROOT_PASSWORD=true`). |
| `HOARDDB_ROOT_PASSWORD_FILE` | (none) | Path to a file holding the root password, read instead of putting it in the environment (an environment variable is visible in `docker inspect` and `/proc`; a file is readable only by whoever can read the file). Keep it at 0600; a broader mode is warned about, an empty or unreadable file stops the server at start-up. If both this and `HOARDDB_ROOT_PASSWORD` are set the file wins, and the server logs a warning when they differ. Like `HOARDDB_ROOT_PASSWORD`, it only seeds the root user on the first start. |
| `HOARDDB_REQUIRE_ROOT_PASSWORD` | `false` | With `true`, a first start that is not given `HOARDDB_ROOT_PASSWORD` or `HOARDDB_ROOT_PASSWORD_FILE` refuses to start, naming the variable, instead of asking for one on a terminal. The recommended production setting, so an unattended node never invents its own root password. Restarts are unaffected: once the root user exists, nothing is asked for. YAML: `auth.require_root_password`. |

### Cluster mode

With `HOARDDB_CLUSTER_ENABLED=true` the node starts as an entry-ring cluster member: it binds one extra listener, the peer listener, and keeps its cluster state in `<data dir>/ring`. A node with none of these keys set runs as it always has and binds no peer listener.

The peer listener binds `HOARDDB_CLUSTER_LISTEN`, which defaults to `HOARDDB_LISTEN` (port 4433). It may not share a port with the driver listener (`HOARDDB_TCP_LISTEN`) or the node-to-node listener of the hash ring, which stays on `HOARDDB_LISTEN` plus one (4434): the start is refused, naming `cluster.listen`. Every peer connection is mutual TLS and is accepted only from a key you pinned: there is no trust on first use, and a connection to a node is accepted only if that node presents the key pinned under its id. A node never dials an address that no member and no configured server names: `join` registers the address first.

An address with an IPv6 zone, such as `[fe80::1%eth0]:4433`, only means something on this host, so a node in cluster mode refuses to start with one as the address peers would be told to dial: `HOARDDB_CLUSTER_LISTEN` (`cluster.listen`) or `HOARDDB_CLUSTER_ADVERTISE` (`cluster.advertise`), or, when `cluster.listen` and `cluster.advertise` are both unset, `HOARDDB_LISTEN` (`server.listen`). The message names the key you set. Use the node's routable address instead, for example `192.0.2.1:4433`.

`HOARDDB_SEED_NODES` drives the entry-ring cluster only, and only with `HOARDDB_CLUSTER_ENABLED=true`: a server given a replication factor above 1, or a seed list, without cluster mode refuses to start and names the fix. The ring does not read `HOARDDB_REPLICATION_FACTOR`; set a database's copies with `create database <name> { replication_factor: N }`. `HOARDDB_NODE_ID` may not contain a colon.

**Which node starts the cluster.** One rule: a node starts a new cluster when `HOARDDB_SEED_NODES` is empty or names only itself, and joins when it names any other node. So the first node lists nobody (or only itself), and every later node lists the nodes that already exist, not itself. Do not give every node the same list: all of them would wait to be joined and none would start the cluster. A node that waits to be joined logs a warning, and `entry-ring node open` shows `join` instead of `bootstrap`.

**Pinning.** Give each node, in `HOARDDB_SEED_FINGERPRINTS`, one `<node-id>=SHA256:<64 hex>` entry per node it lists (each node prints its own fingerprint on its start-up line); the node adds its own key itself. List the nodes in `HOARDDB_SEED_NODES` as `<node-id>@<address>`. The address form `<host:port>=SHA256:<64 hex>` also works for a seed listed at that address. A seed with no pinned fingerprint stops the start and names the entry to add on both nodes; one key under two node ids, and an entry with an empty id, are refused. A seed that names this node's own id at an address other than its advertised one is refused, naming both. The start-up line `entry-ring node open` shows the node id, its advertised address, `bootstrap` or `join`, and its fingerprint.

**Adding a node: pin, restart, join.** The full procedure, with the refusals you can meet and their fixes, is [Add a node to a cluster](cluster-add-a-node.md). A node joins in three steps, in this order, because every node trusts only the keys it was told about and reads that list when it starts.

1. Start the new node with **every** existing node in its seed list and every existing node's key pinned. The second node (`node-b`) lists `node-a` alone: `HOARDDB_SEED_NODES=node-a@192.0.2.1:4433` and `HOARDDB_SEED_FINGERPRINTS=node-a=SHA256:<64 hex>`. The third (`node-c`) lists both: `HOARDDB_SEED_NODES=node-a@192.0.2.1:4433,node-b@192.0.2.2:4433` and `HOARDDB_SEED_FINGERPRINTS=node-a=SHA256:<64 hex>,node-b=SHA256:<64 hex>`. Pinning only the first node is not enough from the third node on: the Raft leader dials the new node during the join and can be any member, and a node refuses a key it did not pin. It waits to be joined and prints its own fingerprint on its start-up line.
2. Pin the new node's key on every node that is already in the cluster, `node-c=SHA256:<64 hex>` in `HOARDDB_SEED_FINGERPRINTS`, and restart each of them, one at a time (a node reads the list at start; a reload without a restart does not exist yet).
3. Run `join` on any member. The statement is forwarded to the cluster's leader, which asks the new node who it is over its pinned key, adds it, and activates it:

```
join node-b@192.0.2.2:4433
join node-w@192.0.2.3:4433
join node-c@192.0.2.4:4433
```

```
Added node-b@192.0.2.2:4433 as a member: joining, then active when it holds the slots it will own (epoch 2)
Added node-w@192.0.2.3:4433 as a witness: it votes and holds no documents (epoch 2)
Added node-c@192.0.2.4:4433 as a member: joining (not a voter: the cluster already has its 3 voters) (epoch 3)
```

A node that votes is promoted by the join while the cluster has a free voter slot (`cluster.voters`, 3 by default, witnesses included); a node that finds none joins without a vote, and `promote <node>` gives it one after a slot is freed. A witness is a node started with `HOARDDB_CLUSTER_WITNESS=true`; the join takes that from the node itself. The address you type must be the one the node advertises (`HOARDDB_CLUSTER_ADVERTISE`, else `HOARDDB_CLUSTER_LISTEN`); if it is not, the refusal prints the one to use. Running the same `join` again answers the same line.

**Form the cluster before you create data.** In this release `join` is refused while any database exists, because a node that joins a cluster with data cannot yet be filled with the slots it would own. Start the members on fresh data directories, join them, then `create database`. `replace-node` brings back a member that lost its data, under its own id and at its own address ([If a member lost its data](cluster-add-a-node.md#if-a-member-lost-its-data)); `leave` and `remove-node` are not available. A joined node publishes the client address it is configured with (`HOARDDB_TCP_ADVERTISE`, else a listen address another machine can dial); a node with none publishes no driver address, and a driver that routes by the cluster's topology skips it. Set `HOARDDB_TCP_ADVERTISE` on a node before you join it if drivers should reach it directly.

`HOARDDB_CLUSTER_ALLOW_MEMBERSHIP` is the lock on these statements: `false` refuses every membership statement on this node, and on the leader on behalf of the members that forward to it.

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_CLUSTER_ENABLED` | `false` | Starts the node as an entry-ring cluster member. It needs `server.node_id` set to a name no other node uses; a start without it is refused, naming the key to set. YAML: `cluster.enabled`. |
| `HOARDDB_CLUSTER_LISTEN` | (`HOARDDB_LISTEN`) | The address the peer listener binds, as `host:port`. Empty uses `HOARDDB_LISTEN`. YAML: `cluster.listen`. |
| `HOARDDB_CLUSTER_ADVERTISE` | (`HOARDDB_CLUSTER_LISTEN`) | The address peers are told to dial, as `host:port`. Required with cluster mode when the listen address is a wildcard (`0.0.0.0` or `::`), because no other machine can dial a wildcard; set it to this node's own address, such as `192.0.2.1:4443`. YAML: `cluster.advertise`. |
| `HOARDDB_CLUSTER_WITNESS` | `false` | Joins the node as a witness: it votes in elections and holds no data. YAML: `cluster.witness`. |
| `HOARDDB_CLUSTER_ALLOW_MEMBERSHIP` | `true` in cluster mode | `false` refuses `join`, `promote`, `leave`, `remove-node` and `replace-node` on this node and, when it leads, for the members that forward to it; a change window is the use. `true` with `HOARDDB_CLUSTER_ENABLED` false is refused at start. YAML: `cluster.allow_membership`. |

### Replication

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_REPLICATION_FACTOR` | `1` | Above `1` without `HOARDDB_CLUSTER_ENABLED=true`, the server refuses to start: this setting used to select the old replicated mode, which is gone (see [Replication Factor](#replication-factor)). Above `1` is refused too, because the ring would ignore it and keep one copy; a database sets its own copies with `create database <name> { replication_factor: N }`. |
| `HOARDDB_SEED_NODES` | (none) | Comma-separated seed addresses (`host:port`, or `node-id@host:port`). With `HOARDDB_CLUSTER_ENABLED=true` it lists the nodes of the entry-ring cluster, as `node-id@host:port`, and each other node needs a pinned fingerprint (see [Cluster mode](#cluster-mode)). Without `HOARDDB_CLUSTER_ENABLED=true` a seed list is refused at start (it used to select the old replicated mode, which is gone). See [Cluster mode](#cluster-mode) (the first node lists nobody else). A list that names one node id at two addresses, or one address under two node ids, stops the server at start, naming both; a node whose list places a node id at another address than a member's list is never confirmed by that member and serves nothing, naming the id and both addresses. |
| `HOARDDB_SEED_FINGERPRINTS` | (none) | **Required whenever `HOARDDB_SEED_NODES` is set.** Comma-separated trust anchors: `SHA256:<64 hex>` (trusted for any seed) or `host:port=SHA256:<64 hex>` (trusted only for that address). With `HOARDDB_CLUSTER_ENABLED=true`, write `<node-id>=SHA256:<64 hex>` for every node you list (see [Cluster mode](#cluster-mode)). Without it the server refuses to start. |
| `HOARDDB_WRITE_CONCERN` | per RF | `1` (or `one`), `majority` or `all`; default is `majority` for RF>1, `one` otherwise. Any other value stops the server at start and names these |
| `HOARDDB_TTL_SWEEP_SECONDS` | `60` | Seconds between TTL sweeps (YAML `server.ttl_sweep_seconds`). A document in a bucket with a TTL index can be read for up to one interval after it expires, plus the time a backlog takes to drain. `0` disables the sweeper: nothing expires. See [indexes.md](indexes.md). |
| `HOARDDB_TTL_SWEEP_BUDGET_SECONDS` | `5` | Longest one TTL sweep runs (YAML `server.ttl_sweep_budget_seconds`). A sweep that ends on its budget with expired documents left starts the next one at once. `0` or less uses the default. |
| `HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS` | `60` | Seconds between the entry-ring node's retention checks (YAML `replication.retention_check_seconds`, 1 to 3600). Each tick also sweeps the dedupe entries that have left `replication.dedupe_window`, on the `storage.reclaim_keys_per_second` budget every background sweep shares. A value outside the range stops the server at start, naming the key. |
| `HOARDDB_WAL_RETENTION_INTERVAL` | `60` | Seconds between WAL retention passes: each prunes segments every other member has confirmed and every local store has been synced past, so a lagging or stopped member always stays repairable from the log. `0` disables the loop (log grows for the life of the node). |
| `HOARDDB_DISK_HIGH_WATER` | `85` | Used-disk percent (0-100) at or above which this node copies in no bucket it does not already hold and votes against a new cluster member: an mmap-backed write into a full disk crashes the node (SIGBUS). A bucket it refuses stays on its other copies, and is unwritable if this node is its owner, until space is freed. `0` disables the guard. |
| `HOARDDB_AUTO_REBALANCE` | `true` | No effect on a node this build starts: it belonged to a replicated mode without cluster mode, which is refused at start. YAML: `server.auto_rebalance`. |

Write concern and retention apply to replicated (multi-node) deployments; a single node
serves with no WAL and no replication. See [replication.md](replication.md).

A cluster with exactly two members can permanently lose the ability to change
membership at all if either node dies (a majority of 2 is 2) — this is logged
loudly at startup, but there is no automatic recovery from it. Use an odd
member count of three or more in production.

### Seed pinning (required for every join)

hoardDB uses self-signed certificates, so a node cannot learn a seed's identity
from a CA. Every join must be told what it is connecting to: `seed_fingerprints`
(`HOARDDB_SEED_FINGERPRINTS`) lists the expected certificates. It is required
whenever seed nodes are configured — a server with seeds and no fingerprints
refuses to start, with an error naming the variable. Without an anchor, any host
could impersonate a member of the cluster.

Two forms are accepted in the comma-separated list:

- `SHA256:<64 hex>` — trusted for any seed address.
- `host:port=SHA256:<64 hex>` — trusted only for that address. The mapped form
  is recommended because a bare fingerprint would also be accepted if the same
  key ever appeared at another seed's address.

Every node prints its fingerprint at startup; the log line carries a
`fingerprint` field:

    grep fingerprint <seed startup log>

On one host, leave ring ports at least two apart (for example 4433, 4435, 4437). A node dials the
ring port plus one, so a mapped entry for `host:4433` also covers `host:4434`; if another entry is
for `host:4434`, it silently replaces the first, and the dial to the node on 4433 fails with a
fingerprint mismatch. Nodes on different hosts do not collide.

A peer verified once is remembered in `{data_dir}/cluster/known_servers` (keyed
by its internode address), so later dials do not need the configured list. The
node creates `{data_dir}/cluster` owner-only and refuses to start when that
directory or file is writable by another user or owned by one (the error names
the `chmod` or `chown` to run), which matters for a volume prepared by another
tool or user. A
certificate that changes for an already-known peer is a hard failure, never a
silent re-trust. Legitimate rotation is explicit:

1. Update `seed_fingerprints` with the new `host:port=SHA256:<hex>` value (or
   remove that peer's line from `{data_dir}/cluster/known_servers` on the
   dialing node), then restart the node.
2. The mismatch error names both fingerprints and both possible causes, so a
   real rotation is never "fixed" by turning the check off.

A new node added after the cluster is running must also be pinned by the nodes
that dial it; because its key is generated on first start, add its
`host:port=SHA256:<hex>` entry to the other nodes' `seed_fingerprints` (or trust
its entry in `known_servers`) before adding it. A single node with no seed nodes
never dials a peer and is unaffected.

### Storage (BadgerDB tuning)

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_BADGER_BLOCK_CACHE_MB` | `128` | BadgerDB block cache, in MiB. A node keeps one BadgerDB instance that every bucket shares, so this is a node-wide cost and does not multiply by bucket count. Raise it when the working set outgrows it and reads start to touch the disk. |
| `HOARDDB_BADGER_INDEX_CACHE_MB` | `64` | BadgerDB index cache, in MiB. Node-wide, like the block cache. |
| `HOARDDB_BADGER_NUM_COMPACTORS` | `4` | BadgerDB background compaction goroutines for the node. Node-wide; lower it to bound background CPU. |
| `HOARDDB_BADGER_TABLE_SYNC` | `msync` | How the store makes each new table file durable: `msync` or `stepwise`. `stepwise` can lower write tail latency, but has been tested only on ext4 with a journal (XFS is expected to be safe but is not yet tested); see "New table files and your filesystem" below. Any other value stops the server from starting, with an error naming the key and both values. YAML key: `storage.badger_table_sync`. |

### Storage memtable

| Variable | YAML key | Default | Range |
|----------|----------|---------|-------|
| `HOARDDB_MEMTABLE_MB` | `storage.memtable_mb` | chosen at first start from memory (`32`, `64` or `128`) | `32` to `1024` (MiB) |

The memtable is the write buffer in front of the node's one store; every bucket
shares it, so this is a per-node setting and does not multiply by bucket count.
A value outside the range, or one that is not a whole number, stops the server at start with a message that names the
value, the bound and the fix; it is not silently adjusted.

#### How big is my memtable, and why

- **A new node chooses its memtable once**, when it creates its store, from the
  memory it is allowed (its cgroup limit, or the machine's RAM): 32 MiB below
  3.5 GiB, 64 MiB from 3.5 GiB, 128 MiB from 7 GiB. It keeps that size from then on, even if the
  machine is later resized, so a node that crashed is never asked to replay its
  log into a smaller memtable. The choice is saved in
  `<data dir>/store/memtable-choice.json`; copy it with the store. A store
  that holds data and no such file keeps the size it last ran at.
- **`hoardDB status` shows the size and why**, and the server logs it at every
  start, with the memory it was chosen from and what write bursts can peak at:

  ```text
  memtable: 64 MiB, chosen now for this new store from this node's memory limit of 3.8 GiB (the host's MemTotal; no cgroup memory limit is set). Write bursts can peak near 0.72 GB of memory at this size; the largest atomic batch is 10 MB. The choice is saved in /var/lib/hoarddb/store/memtable-choice.json and kept on every restart, even if this host's memory changes. To use another size, set storage.memtable_mb (32 to 1024) in the config file.
  ```

  A size that is large for the node's memory (write bursts predicted over a
  third of it) is logged as a warning that says how much memory the node needs.
- **Set `storage.memtable_mb` to choose it yourself.** 256 MiB and above are only
  ever set this way. Prefer the config file to `HOARDDB_MEMTABLE_MB`: a variable
  lost on a restart takes the node back to its chosen size. Removing the setting
  goes back to the chosen size too.
- **A node that joins a cluster must run the cluster's size.** `join` refuses a
  node at another size and says what to set; if its machine is too small for
  the cluster's size, give it more memory before it joins. Members already
  running at different sizes log an error naming each other, and `status` shows
  `DIFFERS`. The difference is not cosmetic: a member at a smaller size cannot
  apply the larger members' biggest batches. Its replication stops at the
  first such batch, and the owner's log names the member, the batch and the
  size it needs, until you set that member to the cluster's size and restart
  it. Nothing is lost while it waits; the owner keeps the batch and re-sends it.
  A smaller member killed (`kill -9`, power loss) while it refuses such a batch
  still stalls on it after its restart, and once resized and restarted it holds
  the batch once, in a FIFO, LIFO or heap bucket as in any other.
  **Give every node the same `storage.memtable_mb`**, set explicitly in each node's config file.
- **If the choice file is missing** from a store that already exists, the node
  keeps the size the store last ran at, so that it never shrinks, and records
  that size as the store's choice, even if a setting asked for it at the last
  run. The start-up line says so.

**What it trades.**

- **Write stalls.** A bigger memtable flushes less often, so a burst of writes
  stalls less. A small one flushes more often and stalls longer under a heavy
  write load. hoardDB publishes no figure for either until it has been certified.
- **The largest atomic batch.** One batch (an insert-many, or one batch of
  `hoarddb ingest`) is applied all-or-nothing, and the store commits at most 15%
  of the memtable at once: 5 MB at the 32 MiB floor, 10 MB at 64 MiB, about 20 MB
  at 128 MiB (what a host with 7 GiB or more chooses). Because the size depends on the host, only batches up to 5 MB are
  atomic on every host. The floor is where that stays at the 5 MB `hoarddb ingest`
  sizes its batches for. A batch over the limit is refused whole, with the limit
  in the message.
- **Memory.** The node holds up to three memtables. An idle node uses the same
  resident memory at any size (133 MB on the measuring host), and a memtable
  takes memory only as it fills: at 128 MiB a node killed with its memtable 90%
  full held about 1 GB, against 0.4 GB at 32 MiB, and under the sustained write
  load above peaked near 6.8 GB, against 3.7 GB. Plan for several gigabytes on a
  node that takes heavy writes, and keep an eye on the `memory` ratio.

**Lowering it after a crash: "Memtable size mismatch".** The size is fixed when the
server starts. If the node was killed (`kill -9`, power loss, the out-of-memory
killer) while running at a larger size, the write-ahead log it left cannot be
replayed into a smaller memtable. The server refuses to start in that case. This
can also happen when you remove an explicit setting after a crash: the node goes
back to its chosen size. It then says:

```text
Memtable size mismatch: this node now runs at its chosen 64 MiB because
storage.memtable_mb is not set and HOARDDB_MEMTABLE_MB is not set, but its node
store directory /var/lib/hoarddb/store was last running with 256 MiB and was not
stopped cleanly: 1 write-ahead log file(s) of unflushed writes remain (000001.mem),
and they do not fit in a 64 MiB memtable. Nothing is lost. To fix it, set
HOARDDB_MEMTABLE_MB=256 (or storage.memtable_mb: 256 in the config file), start the
node once and wait until it is serving, then stop it cleanly (SIGTERM or Ctrl-C,
not kill -9) and remove the setting: the node goes back to its chosen 64 MiB.
```

When a setting asks for the smaller size, the message names it:

```text
Memtable size mismatch: this node is set to 128 MiB by HOARDDB_MEMTABLE_MB, but its
node store directory /var/lib/hoarddb/store was last running with 256 MiB and was
not stopped cleanly: 1 write-ahead log file(s) of unflushed writes remain
(000001.mem), and they do not fit in a 128 MiB memtable. Nothing is lost. To fix
it, set storage.memtable_mb (or HOARDDB_MEMTABLE_MB) to 256, start the node once
and wait until it is serving, then stop it cleanly (SIGTERM or Ctrl-C, not kill
-9). After that you can lower it to 128.
```

Without this check the store would exit with `Arena too small`, which names
neither the setting nor the way out; if you meet that message, this is the cause,
and the fix is the same. A node that stopped cleanly can change to any size in the
range, and raising the size is always allowed.

```yaml
storage:
  memtable_mb: 256
```

### Storage durability

| Variable | YAML key | Default |
|----------|----------|---------|
| `HOARDDB_UNSAFE_ACK_BEFORE_SYNC` | `storage.unsafe_ack_before_sync` | `false` |

A node answers a write only once it is on disk, so a write it acknowledged
survives a crash of the process, a kernel crash and a power cut. A node in a
cluster with other members writes each entry to its write-ahead log and syncs
the log before it applies the entry. A write the node does not log is synced in
the store before the node answers it: every write of a single node with no seed
nodes and a replication factor of 1, the zero-config default, which keeps no
log, and every write of a node that keeps a log but is still the only member of
its ring, such as the first seed before its peers join. Writes that arrive
together share one sync, so many clients writing at once cost far fewer syncs
than writes; a client writing one document at a time
waits for one disk flush per write. A batch insert is one sync for the whole
batch, so loading data in batches is the fast way to write on a single node.

**`unsafe_ack_before_sync: true` gives that up.** The node then answers before
the sync, and **a power cut or a kernel crash can lose writes it has already
acknowledged**; a crash of the server process alone does not lose them. It
exists for benchmarks that measure everything but the disk. Never set it on a
node whose data you need. The server logs a warning at every start while it is
set. It changes nothing for a write the node logs, because the write-ahead log
is synced before that write is applied either way; on a node with a log it
applies only while the node is the only member of its ring.

#### Write-ahead log segments

| Variable | YAML key | Default |
|----------|----------|---------|
| `HOARDDB_WAL_PREALLOCATE` | `storage.wal_preallocate` | `auto` |

Two logs read this one setting: the entry-ring node's log, which every server
opens, and the node-to-node ring's log, which exists only with a replication factor
above 1 or seed nodes. A node with neither has no such log, and if
you set the key the server says it has no effect there (the
entry-ring node logs its own line).

- `auto` (the default) turns `on` for ext2, ext3 and ext4 only and leaves it off
  on every other filesystem, XFS included, until it has been measured there.
  To turn it off, set `HOARDDB_WAL_PREALLOCATE=off`.
- `off` starts each 64 MiB log segment empty and lets it grow.
- `on` writes zeros through each segment before the log uses it and keeps the
  next segment ready, so a flush of the log writes data only. It can lower the
  latency of replicated writes. It holds 128 MiB of disk ahead of the log (the
  active segment and the spare) and writes about as many bytes of zeros as of
  log entries.

Any other value (`true`, `ON`, or `wal_preallocate: ""` in the YAML file) stops the
server from starting, with an error that names the key, the variable and the three values. (An empty
`HOARDDB_WAL_PREALLOCATE=` is ignored and gives the default.) A change
needs a restart. In a cluster, change it one node at a time: change it on one
node and restart it, wait until `hoarddb_replication_peers_not_in_sync` is 0 and
`status` shows the expected `WAL` row, then do the next. The setting does not
change the log's format: every node with a log writes segment format 2.

**What the 128 MiB moves.** With preallocation in effect (the default on ext2/3/4, or `on`), an idle node already holds about 128 MiB
of log, so `hoarddb_node_data_bytes{part="wal"}` reads that much at rest, and
the disk high-water mark (`server.disk_high_water_pct`) is reached 128 MiB sooner.
A node that cannot make its next segment because the disk has no room refuses
writes, and `status`, `HEALTH_CHECK` and `/ready` say so with the fix: free
space, or turn preallocation off. Each start begins a new log segment, which
retention removes later.

**What each start logs** (INFO, with the fields `mode`, `configured`,
`filesystem` and `reason`):

    write-ahead log segments are preallocated (storage.wal_preallocate=on)
    write-ahead log segments are preallocated (storage.wal_preallocate=auto: ext2/3/4)
    write-ahead log segments are not preallocated (storage.wal_preallocate=off)
    write-ahead log segments are not preallocated (storage.wal_preallocate=auto: XFS is not measured yet)
    storage.wal_preallocate=on has no effect here: this node keeps no node-to-node write-ahead log (replication_factor 1, no seed nodes); the entry-ring node logs its own mode
    entry-ring: write-ahead log segments are preallocated (storage.wal_preallocate=auto: ext2/3/4)

`status` shows the same on its `WAL:` row, for example `WAL:          preallocated
(auto: ext2/3/4), spare ready`, and `HEALTH_CHECK` carries it as the `wal` field.
Ten node-wide series, `hoarddb_wal_preallocated`, `hoarddb_wal_flush_seconds`,
`hoarddb_wal_rotations_total`, `hoarddb_wal_rotation_waits_total`,
`hoarddb_wal_rotation_wait_seconds_total`, `hoarddb_wal_zero_fill_bytes_total`,
`hoarddb_wal_zero_fill_seconds_total`, `hoarddb_wal_spare_fill_failures_total`,
`hoarddb_wal_spare_ready` and `hoarddb_wal_bytes_appended_total`, are exported when
metrics are on, and only by a node that keeps a log. None carries a bucket or
database label. `hoarddb_wal_bytes_appended_total` is the framed bytes of every
entry the log wrote since the node started and starts again at a restart, so read
its rate over a run rather than a total.

**Where to put the log.** A separate device can be mounted at
`<data_dir>/wal`; hoardDB has no setting to move the log elsewhere. A mount there
is outside the disk ratio that the capacity probe and the `status` disk row
measure. A container whose data directory is in the image's writable layer is on
overlayfs, so `auto` resolves to `off`; mount a volume to get the gain.

**A rotation can wait.** When the log fills a segment faster than a fill of the
next one, the rotation waits for the fill and every write and every
replication read waits with it, for the rest of the fill. That is counted in
`hoarddb_wal_rotation_waits_total` and `hoarddb_wal_rotation_wait_seconds_total`.

#### New table files and your filesystem

As writes arrive, the store writes them out to new table files in the
background, and merges older files into new ones. Each new file must be on
disk before the store's file list (its manifest) names it, or a power cut can
leave the store unable to open.

By default (`storage.badger_table_sync: msync`) the server syncs each new file
on its own before the manifest names it. That is durable on any filesystem. On
ext4 each of those syncs waits for a filesystem journal commit, and writes
queue behind it, so the slowest writes are slower than they need to be while
a file is being synced.

`stepwise` is an opt-in alternative that can lower write tail latency on
ext4. The server writes each new file to disk in 4 MiB pieces, waits until all
of it is there, and leaves the file's size and location on disk to the one
sync that records it in the manifest. That is safe only where the filesystem
commits those changes in the order they were made:

- **Tested and safe:** ext4 with its journal (how `mkfs.ext4` makes it).
- **Expected to be safe, not yet tested:** XFS.
- **Not safe:** btrfs, ZFS, and ext4 without a journal. A power cut can leave
  the manifest naming a file whose contents never reached the disk, and the
  store then refuses to open. Nothing warns you before the power cut.
- **Unknown:** ext4 with the `fast_commit` feature. Use `msync` there.

The server cannot tell these apart, which is why `stepwise` is never the
default. To see your filesystem and its options, run
`findmnt -no FSTYPE,OPTIONS -T <data dir>`, and `tune2fs -l <device>` for an
ext4 device's features (look for `has_journal` and `fast_commit`).

To turn it on, set it in the configuration file:

```yaml
storage:
  badger_table_sync: stepwise
```

or in the environment, which wins over the file:

```bash
HOARDDB_BADGER_TABLE_SYNC=stepwise hoarddb-server
```

The setting is read at start, so restart the server for a change to apply.
It affects only table files written after the restart; files already on disk
are not rewritten. Switching back to `msync` is the same: change the setting
and restart.

Where the kernel or a container's security profile refuses the call
`stepwise` uses (`sync_file_range`), the server syncs each new file the
`msync` way on its own, with nothing to set. If a table write fails with an
error that names `storage.badger_table_sync`, set it to `msync` (or remove it)
and restart.

#### Disk write volume depends on the kernel and filesystem

The store's write-ahead log is a memory-mapped file, and the sync behind each
acknowledged write asks the kernel to write the log's changed pages to disk.
How much the kernel writes for one sync depends on how it caches the file:
newer kernels keep a file's pages in larger units (folios) sized by the
device's readahead window, and a sync writes a whole unit back even when only
a few KB of it changed. It was measured on ext4 with Linux 7.0 and a
readahead window of 8 MB (`read_ahead_kb` 8192), and is expected on other
kernels that use large folios for file mappings; ext4 with Linux 6.12 and a
128 KB readahead window was measured unaffected.

hoardDB tells the kernel its log files are not read in sequence, which keeps
those units at one page, so a sync writes about what the write changed: on
the host this was measured on, seeding a 230 MB dataset wrote under 1 GB to
disk with this in place and 67 to 80 GB without it. Nothing needs to be configured.
To see what a node writes, compare `write_bytes` in `/proc/<server pid>/io`
with the data you sent; if it is hundreds of times the data, report it with
the output of `uname -r` and `cat /sys/block/<device>/queue/read_ahead_kb`.

The hint is lifted while the server reads the log through at start (after a
crash, the log holds writes not yet in the store's tables), so that replay
keeps the kernel's readahead.

### Go memory limit

With no `GOMEMLIMIT` in its environment, the server gives the Go runtime a soft
memory limit of 75% of the node's memory limit (the same limit the memtable is
chosen from) and says so in its first log line, `hoardDB server starting`, as
`go_memory_limit`. Under a write burst the garbage collector then works harder
instead of letting the heap grow to several times what it holds. Set
`GOMEMLIMIT` (for example `GOMEMLIMIT=3GiB`) to choose another value; yours is
always used.

Secondary indexes (declared on a bucket) open their own BadgerDB instance per
indexed field and always use the defaults above, regardless of these
settings — an index is normally much smaller than its bucket's primary data.

### Networking

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_MAX_CONNS` | `1000` | Max concurrent client connections |

### Queries

| Variable | Default | Range | What it bounds |
|----------|---------|-------|----------------|
| `HOARDDB_QUERY_CONCURRENCY` (`query.concurrency`) | twice the CPU count | 4 to 256 | How many `SCAN`, `SEARCH`, `BTREE_RANGE`, `COUNT` and `DISTINCT` run at once on the node. A query beyond it waits for a slot; it is never refused for one. Point reads and writes take no slot |
| `HOARDDB_QUERY_MAX_RESULT_BYTES` (`query.max_result_bytes`) | `16777216` (16 MiB) | 1 MiB to 24 MiB; a smaller value is raised to 1 MiB, with a warning | What one query may hold in memory while it builds its answer. A query over it is answered `RESULT_TOO_LARGE`, and the connection stays up |

Together they bound the memory queries hold on a node: at most the
concurrency times the ceiling. A query whose client disconnects stops, and
gives its slot back.

### Backup and restore

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_DUMP_PIN_CEILING_SECONDS` | `300` | Maximum time a `dump` of a FIFO/LIFO bucket may hold that bucket's compaction pinned before the dump fails with a named error instead of pinning indefinitely. This setting applies to the legacy engine's dump; `hoarddb dump` on the ring takes no pin. See [Backup and restore](backup-restore.md). |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `HOARDDB_LOG_FORMAT` | `json` | `json` or `text` |
| `HOARDDB_LOG_OUTPUT` | `stdout` | `stdout`, `stderr`, or file path |

### Metrics

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_METRICS_ENABLED` | `false` | Enable Prometheus `/metrics` endpoint |
| `HOARDDB_METRICS_LISTEN` | `127.0.0.1:9090` | Metrics HTTP listen address. Loopback by default |

With metrics off, the node's capacity warnings still go to its log and to
`hoardDB status`.

### Audit

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_AUDIT_ENABLED` | `false` | Enable audit logging |
| `HOARDDB_AUDIT_OUTPUT` | `/var/log/hoarddb/audit.jsonl` | Audit log file path |

### Encryption variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_ENCRYPTION_KEYSTORE` | `off` | Where the key-encryption key comes from: `off`, `keyfile`, `command` or `systemd-creds`. See [Encryption at rest](#encryption-at-rest) |
| `HOARDDB_ENCRYPTION_KEY_FILE` | (none) | Path to the key file, `keystore=keyfile` only. Must be `0600` and outside the data directory |
| `HOARDDB_ENCRYPTION_MODE` | `off` | `off`, `on` or `require`. `on` and `require` need a keystore and seal the node's ring store and log; `require` also checks the structure of the node's own store before it opens. Every node of a cluster must use the same setting: the cluster does not check it. See [Encryption at rest](#encryption-at-rest) |

`key_command` (the `command` keystore's argument list) is set in the [YAML file](#yaml-config-file) only
(`--config`); it has no environment variable in this version.

### Debug

| Variable | Default | Description |
|----------|---------|-------------|
| `HOARDDB_PPROF_ENABLED` | `false` | Enable pprof endpoint on `:6060` |
| `HOARDDB_PPROF_BLOCK_RATE_NS` | `1000000` | Only with pprof enabled: sample one blocking event per this many nanoseconds spent blocked, so `/debug/pprof/block` has data. `0` turns it off. A whole number; any other value stops the server at start. YAML key: `debug.pprof_block_rate_ns` |
| `HOARDDB_PPROF_MUTEX_FRACTION` | `100` | Only with pprof enabled: report 1 in this many contended mutex events, so `/debug/pprof/mutex` has data. `0` turns it off. A whole number; any other value stops the server at start. YAML key: `debug.pprof_mutex_fraction` |

## YAML Config File

For non-containerized deployments, name a YAML file with `--config`:

```bash
hoarddb-server --config /etc/hoarddb/config.yaml
```

`HOARDDB_CONFIG=/etc/hoarddb/config.yaml` does the same; when both are given, the flag wins. The server reads **only** the file you name: there is no search path, and no file at all is the normal zero-config start. The server logs the file it read.

Settings are layered, lowest to highest:

1. the built-in defaults;
2. the YAML file;
3. `HOARDDB_*` environment variables, which override the file one key at a time.

So a file can hold the settings a host shares, and one environment variable can still change a single value for a single start.

The server refuses to start, with a message that names the problem, when:

- the file named by `--config` or `HOARDDB_CONFIG` does not exist or cannot be read;
- the file is not valid YAML;
- the file holds a key the server does not define. The message names the key, its line and the section, and suggests the nearest valid key when there is one. A key that is ignored without a word is how a typo turns into a setting you believe is applied; here `data_dr:` stops the start instead of leaving the data in `./data`.

`key_command` (the `command` keystore's argument list, see [Encryption at rest](#encryption-at-rest)) can only be set in this file.

A complete example. Every key below is a key the server defines, and a test parses this block with unknown keys refused, so it cannot drift from the server:

```yaml
# /etc/hoarddb/config.yaml

server:
  listen: "node1:4433"         # this node's address in the ring, written exactly as the seeds below list it (each node sets its own); node-to-node traffic uses the next port (4434). HOARDDB_LISTEN defaults to loopback, this machine only. 0.0.0.0 here needs node_id and id@address seeds, as below
  node_id: "node1"
  max_conns: 1000

tcp:
  listen: "0.0.0.0:7433"       # driver and CLI address (TCP+TLS); the default is 127.0.0.1:7433, this machine only
  # advertise: "node1:7433"    # what drivers are told to dial in a cluster; same host as server.listen, any port

storage:
  data_dir: "/var/lib/hoarddb"
  # blob_dir: "/mnt/blobs"           # blob payloads; default <data_dir>/blobs
  # max_blob_bytes: 16777216         # largest blob accepted; default 16 MiB, at most 33488896
  # max_document_bytes: 25165824     # largest document accepted; default 24 MiB, 4096 to 31457280
  # memtable_mb: 256                 # see "How big is my memtable, and why"
  # unsafe_ack_before_sync: true     # benchmarks only: can lose acknowledged writes, see "Storage durability"
  # badger_table_sync: stepwise      # opt-in, tested on ext4 with a journal only (XFS not yet tested); see "New table files and your filesystem"
  # wal_preallocate: auto            # auto (default; on for ext2/3/4 only), on, or off; see "Write-ahead log segments"

hash_ring:
  # As written, every node lists all three. With cluster.enabled the
  # first node (node1) lists only itself or nobody, and node2 and node3 list the nodes that
  # already exist; see "Cluster mode".
  seed_nodes:
    - "node1@node1:4433"
    - "node2@node2:4433"
    - "node3@node3:4433"
  # Required when seed_nodes is set. Mapped form is recommended.
  seed_fingerprints:
    - "node1:4433=SHA256:<64 hex>"
    - "node2:4433=SHA256:<64 hex>"
    - "node3:4433=SHA256:<64 hex>"

tls:
  mode: auto                   # the only mode in this version; any other value stops the server

auth:
  token_ttl_seconds: 900
  token_max_lifetime_hours: 12
  password_min_length: 12
  require_root_password: true  # a first start needs HOARDDB_ROOT_PASSWORD; recommended in production
  argon2_memory: 65536         # KiB, so 64 MiB
  argon2_iterations: 3
  argon2_parallelism: 4
  argon2_salt_length: 16       # bytes
  argon2_hash_length: 32       # bytes

logging:
  level: info                  # debug, info, warn, error
  format: json                 # json or text
  output: stdout               # stdout, stderr, or a file path

metrics:
  enabled: true
  listen: "127.0.0.1:9090"     # the default; use a monitoring-network address to scrape from another host

audit:
  enabled: true
  output: "/var/log/hoarddb/audit.jsonl"

encryption:
  keystore: keyfile            # off, keyfile, command or systemd-creds
  key_file: /etc/hoarddb/kek   # keystore=keyfile only; 0600, outside data_dir
  mode: require                # off, on or require
  # key_command: ["/usr/local/bin/print-kek"]   # keystore=command only; YAML only
```

## TLS Modes

### What a node serves in this version

Every node generates an Ed25519 keypair and a self-signed certificate on first start and serves it:

```
{data_dir}/keys/server.key
{data_dir}/keys/server.crt
```

Clients use trust-on-first-use (TOFU) — fingerprint is pinned on first connect. Identical to SSH.

### Custom certificates are not available yet

There is no CA verification and no way to substitute a certificate in this version. So that nobody
believes otherwise, the server refuses to start when `HOARDDB_TLS_MODE` is anything but `auto`, or when
`HOARDDB_TLS_CERT`, `HOARDDB_TLS_KEY` or `HOARDDB_TLS_CA` is set. The same applies to the `tls:` keys of
the YAML file. The error names the setting:

```
tls.mode (HOARDDB_TLS_MODE) is "custom", but custom certificates are not available in this version: ...
       remove the setting (or set HOARDDB_TLS_MODE=auto) to start
```

Remove the setting to start. If you need to know what a node presents, read its fingerprint from the
start-up log and pin it with `HOARDDB_SEED_FINGERPRINTS`, as described under "Seed pinning" above.

### Transport Details

- **Protocol:** TCP+TLS 1.3 (mandatory, no downgrade)
- **Cipher:** ChaCha20-Poly1305 (preferred) or AES-256-GCM
- **Key Exchange:** X25519
- **Keys:** Ed25519 (faster than RSA, smaller keys)

## Replication Factor

The old replicated mode is gone. A replication factor above one in
`HOARDDB_REPLICATION_FACTOR` (`server.replication_factor`), or a seed list in
`HOARDDB_SEED_NODES` (`hash_ring.seed_nodes`), without `HOARDDB_CLUSTER_ENABLED=true`
used to start a hash ring with a write-ahead log. The server now refuses to start
with either, before it opens a port or touches the data directory, and the error
names the setting and the fix:

```
HOARDDB_REPLICATION_FACTOR=3 (server.replication_factor) selects the old replicated mode, which is no longer supported. To run a cluster, start the ring instead: set HOARDDB_CLUSTER_ENABLED=true; the first node lists nobody (or only itself) in HOARDDB_SEED_NODES, and each later node lists the nodes already in the cluster as <node-id>@<host:port>; HOARDDB_SEED_FINGERPRINTS holds one <node-id>=SHA256:<64 hex> entry per node listed ... (see Cluster mode). In the ring a database gets its copies from 'create database <name> { replication_factor: N }' ... The ring does not read the data the old mode wrote: it opens <data dir>/ring and leaves the old files alone. No command of this build moves that data into the ring: keep the old data directory untouched and start the ring in a fresh one. To run one node on fresh data, remove HOARDDB_REPLICATION_FACTOR and HOARDDB_SEED_NODES.
```

To run one node, remove both settings. To run a cluster, use [Cluster mode](#cluster-mode): the first node lists no seeds, later nodes list the nodes already in the cluster, and each pin is `<node-id>=SHA256:<64 hex>`.

**Old data.** The ring opens `<data dir>/ring` and does not read what the old mode wrote there or beside it; those files are left alone, and no command of this build moves that data into the ring: keep the old data directory untouched and start the ring in a fresh one. A plain `hoarddb` session, `hoarddb dump`, `hoarddb restore` and `hoarddb ingest` all talk to the ring.

In the ring, copies are a property of the database, not of the node. Say how many
when you create it:

```
create database app { replication_factor: 3 }
```

A database that names none has one copy. The create is refused, with what to do
instead, when the cluster cannot hold the factor (see [Limitations](limitations.md#cluster)).
`HOARDDB_REPLICATION_FACTOR` above 1 is refused at start too, with the statement above in the message; unset it. The per-bucket
`replication_factor`, the `alter bucket` forms and the write-concern text
are not usable; [Replication](replication.md) says what is.

## Logging

### Levels

| Level | When | Example |
|-------|------|---------|
| `debug` | Development only | "Parsed command: insert into User" |
| `info` | Normal operations | "Node node1 joined hash ring" |
| `warn` | Degraded but functioning | "Replication lag > 5s on node2" |
| `error` | Requires attention | "Failed to write segment: disk full" |

### Format

- `json` — Machine-readable, structured fields (production)
- `text` — Human-readable terminal output (development)

### Correlation IDs

Every request gets a UUID v7 correlation ID, propagated through all internal calls and included in every log line. Enables tracing a single request across nodes:

```
Client → node1 (correlation_id: 0192a3...) → node2 (replication)
```

## Metrics

Prometheus endpoint at `http://<listen>:9090/metrics` (when enabled).

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `hoarddb_requests_total` | counter | Total requests by method/status |
| `hoarddb_request_duration_seconds` | histogram | Request latency by method |
| `hoarddb_bucket_operations_total` | counter | Total operations by type |
| `hoarddb_query_in_flight` | gauge | Queries holding an execution slot (`HOARDDB_QUERY_CONCURRENCY`) |
| `hoarddb_query_admission_wait_seconds` | histogram | How long queries waited for a slot, by `pool` |
| `hoarddb_query_result_too_large_total` | counter | Queries refused with `RESULT_TOO_LARGE`, by `method` |
| `hoarddb_query_cancelled_total` | counter | Queries stopped because their client disconnected |
| `hoarddb_request_faults_recovered_total` | counter | Request handler faults the server recovered from, by `port` (`driver` or `internode`): the request failed and its connection closed, and the server kept serving. Any increase is a server bug; the error-level log line with the same fault id has the detail to report |
| `hoarddb_node_capacity_ratio` | gauge | Each resource's reading over its live limit, by `resource` (`fds`, `mappings`, `memory`, `disk`, and `blob_disk` when the blob directory is on its own filesystem); absent while a resource cannot be measured |
| `hoarddb_node_capacity_threshold_ratio` | gauge | Where each resource turns red, by `resource` |
| `hoarddb_node_capacity_sample_errors_total` | counter | Samples in which a resource could not be read, by `resource` |
| `hoarddb_node_capacity_last_sample_timestamp_seconds` | gauge | When the last capacity sample completed |
| `hoarddb_node_buckets` | gauge | Buckets this node serves |
| `hoarddb_node_memtable_bytes` | gauge | The node store's memtable size for this run; one series per node |
| `hoarddb_node_encryption_sealed` | gauge | 1 when the node's store was found sealed at rest when it started, 0 otherwise; one series per node |
| `hoarddb_node_encryption_info` | gauge | Always 1; the labels are the configured `mode` and `keystore`; one series per node |
| `hoarddb_node_memory_mappings`, `hoarddb_node_memory_mappings_limit` | gauge | Memory mappings, and `vm.max_map_count` |
| `hoarddb_node_memory_anon_bytes`, `hoarddb_node_memory_limit_bytes` | gauge | Anonymous memory (resident plus swapped), and the memory limit it is measured against |
| `hoarddb_node_disk_used_bytes`, `hoarddb_node_disk_size_bytes` | gauge | The data directory's filesystem, used and total |
| `hoarddb_node_blob_disk_used_bytes`, `hoarddb_node_blob_disk_size_bytes` | gauge | The blob directory's filesystem, used and total; present only when it is not the data directory's |
| `hoarddb_node_data_bytes` | gauge | Bytes hoardDB itself holds, by `part` (`store_lsm`, `store_vlog`, `wal`) |
| `hoarddb_replication_peers_not_in_sync` | gauge | Members whose replication stream from this node is stalled, or is still being sent what it missed; 0 is healthy |
| `hoarddb_replication_lag_entries` | gauge | Entries of this node's log the furthest-behind member has not confirmed; a member sent nothing counts 0 |
| `hoarddb_replication_lag_seconds` | gauge | How long the member that is owed entries has gone without confirming any of them |
| `hoarddb_cluster_peers_unreachable` | gauge | Members whose membership check from this node fails |
| `hoarddb_replication_entries_stranded_total` | counter | Writes this node did not send before it stopped owning their bucket; they are not sent late |
| `hoarddb_membership_changes_total` | counter | Membership changes this node proposed, by `outcome` (`committed`, `finished_another`, `already_committed`, `refused`, `no_majority`, `indeterminate`); one series per outcome per node |
| `hoarddb_handoffs_in_progress` | gauge | Buckets on this node with a hand-off record; after a `join` or `remove-node` it reads 0 on every node once every moved bucket is taken over. Read every node: one with no copy yet, and every node before its placement pass, reads 0 |
| `hoarddb_handoff_copies_total` | counter | Buckets this node copied whole to take them over or to follow their new owner |
| `hoarddb_handoffs_without_previous_owner_total` | counter | Buckets this node took over without their previous owner, after it was removed while down |
| `hoarddb_handoff_entries_reconciled_total` | counter | Writes this node applied from a node that no longer owned their bucket, while the bucket was changing owner |
| `hoarddb_node_slots_ready` | gauge | Slots this node holds ready, each a copy it can serve; one series per node, no label |
| `hoarddb_node_slots_seeding` | gauge | Slots this node is copying from another node, queued or pulling; one series per node, no label |
| `hoarddb_node_slots_retired` | gauge | Slots this node gave up, retired or retiring, whose range is kept until the deletion gate passes; one series per node, no label |
| `hoarddb_node_seed_bytes_total` | counter | Bytes of seed chunks this node applied as a receiver since the process started; one series per node, no label |
| `hoarddb_node_seed_retries_total` | counter | Seed pulls this node moved to another source or restarted on a fresh cut since the process started; one series per node, no label |

Which resource is red decides what to do, and for disk, adding a node is not the
fix. None of the
replication series is labelled: one series per node, nothing per bucket. The
membership change counter's one label is its outcome.

## Audit Logging

Append-only log of security-relevant operations. Written to a dedicated file, not stdout.

### What Gets Audited

- Authentication: login success/failure
- User management: create, alter, drop user; grant/revoke roles
- Schema changes: create/drop database, define/drop bucket
- Encryption at rest: on an encrypted node, one `key.unwrap` entry per start, naming the node
  master's key id. A start that fails to unwrap the key is refused, and the failure is still
  recorded in the audit log as a failed `key.unwrap` entry; the server's log has the error too.

### Audit Log Format

```json
{
  "time": "2026-09-11T12:00:00Z",
  "event": "auth.login",
  "user": "alice",
  "source_ip": "192.0.2.100",
  "result": "success",
  "correlation_id": "0192a3b4-c5d6-7890-abcd-ef1234567890"
}
```

Audit writes are async and non-blocking. If the queue fills (disk full), entries are dropped with a warning.

## Encryption at rest

Encryption at rest is **per node** and **off by default**, so a node with no configuration starts
exactly as before. Turn it on for a **new** node by choosing a keystore and a mode before its first
start:

```yaml
encryption:
  keystore: keyfile
  key_file: /etc/hoarddb/kek
  mode: on
```

A node with `on` or `require` seals its ring store (every database's documents, store fields and
declarations, in `<data_dir>/ring/badger`) and its ring log (`<data_dir>/ring/wal`) under keys of
its own, kept in `<data_dir>/ring/keys/` and wrapped by the node master, which the key-encryption
key from the keystore wraps in turn. The ring log is also *authenticated*, so a changed entry is
detected; the store is encrypted for confidentiality only (its storage engine encrypts without a
MAC), so a changed byte there is not. The step-by-step setup, the backups, the cluster rule and how
to check every node are on the [Security](security.md#encryption-at-rest) page.

### Encryption belongs to the node

- **Everything the node stores in its ring is sealed.** There is no per-database or per-bucket key
  or setting.
- **A node's encryption is fixed when its ring is created.** Starting a sealed ring with
  `mode: off`, or a plaintext one with `mode: on` or `require`, is refused, naming what the start
  found and the setting to use; nothing in `ring/` changes. Switching means moving the ring aside
  and starting the node again empty (a cluster member comes back with `replace-node`); see
  [Security](security.md#a-nodes-mode-is-fixed-when-its-ring-is-created).
- **`on` and `require` both seal.** `require` also checks the structure of the node's own ring store
  before the store opens (every table carries a key id, the key registry refuses an empty key) and
  refuses a store that fails it, naming the finding. `require` does not look at other nodes.
- **Every node of a cluster must use the same setting, and the cluster does not check it.** A node
  with encryption off keeps plaintext copies of the records it holds, including copies of records
  the other nodes seal. Each node makes its own keys; a node that joins seals what it copies if its
  own mode is on.
- **Creating the ring keys needs hard links.** A start on a filesystem that refuses them (some FUSE,
  SMB/CIFS and network mounts) is refused and says to put `data_dir` on a local filesystem that
  supports them (ext4, xfs, btrfs, zfs). There is no fallback in this release.

### The node's credentials

An encrypted node seals its credential files under the node master, in the same authenticated
envelope as `users.json`: a standalone node's signing key
(`auth.token.key`), the TLS private key (`keys/server.key`) and, on a data node of a cluster, the
cluster's password key (`auth.cluster.pepper`). The certificate, `keys/server.crt`, stays
plaintext because it is public. With encryption off, `auth.cluster.pepper` is a plaintext file
readable by the owner only (`0600`).

- **A first start writes them sealed.** They are never written in the clear.
- **An existing node seals them on its next start.** A node that ran with encryption on before
  these files were sealed finds them in plaintext and replaces each with its sealed form under the
  same name, keeping the same signing key and certificate. A start stopped part-way leaves
  each file either plaintext or sealed, never missing, and the next start finishes the job.
- **A file the node master does not open stops the start.** The error names the file and how to
  restore it; the node never generates a new key over a sealed file. A sealed file
  cannot be copied to another node.
- **A TLS key with no certificate beside it is refused**, not replaced: a new key would change the
  fingerprint every peer and client pinned. Restore `keys/server.crt`, or move `keys/server.key`
  aside to generate a new identity. `keys/server.crt` is written owner-only (`0600`), like every
  file the server creates.
- **A credential file that is neither sealed nor a valid plaintext file stops the start**, naming
  the file; it is not sealed again as it is.
- **`auth.cluster.pepper` is the one credential file a node can get back from the cluster.** Moved
  aside or missing on a data node, it is asked for again from another member once the node is back
  in the cluster. Lost on every data node at once, it cannot be recreated, and the cluster's
  passwords can no longer be checked; restore it from a backup of the same node's data directory
  (a sealed copy opens only on the node that sealed it). See "The cluster's password key" in the
  page on adding a node for the shapes of cluster where that can happen and the backups that
  prevent it. A wrong key kept as `auth.cluster.pepper.wrong-<kid>` is not sealed in place when
  encryption is turned on; the start logs a warning naming each such plaintext copy.
- **Sealing is one-way.** A credential file sealed in place stays sealed.

### Encryption in a cluster

Each node seals only what it stores, under its own key, and only when it runs with encryption on.
Records travel between nodes decoded, over the nodes' pinned TLS connections; there is no cluster
key and no key on the wire. **The cluster does not check that its nodes use the same setting**: a
plaintext node joins a sealed cluster without a refusal, and stores its copies in plaintext. Set the
same `encryption.mode` on every node before its first start, give each node its own KEK, and check
every node after each join ([Security](security.md#check-every-node-of-a-cluster)).

What reaches every node's metadata (`ring/raft`) whatever the mode, and is not sealed there: the
names of databases, member ids and addresses, settings, and the cluster's users, roles and password
hashes.

### The key-encryption key and the keystore

The node generates its own **node master key** on its first encrypted start and keeps it in the
data directory only in wrapped form (`keys/node-master.key.wrapped`), encrypted under a 32-byte
**key-encryption key (KEK)** that you provide and that is never in the data directory. The keystore
is where that KEK comes from:

- **`keyfile`** — a file holding exactly 32 raw bytes or 64 hex characters (one trailing newline
  is allowed). The server refuses a key file that is group- or world-readable, or that sits inside
  the data directory:

      (umask 077 && head -c 32 /dev/urandom > /etc/hoarddb/kek)

- **`command`** — a program whose standard output is the key, in the same two forms. It is run
  without a shell, with an empty environment and a 10-second timeout, and its output is never
  logged. Set it as a YAML list; anyone on the host can read a running program's arguments, so
  never put a secret in them:

      encryption:
        keystore: command
        key_command: ["/usr/local/bin/hoarddb-kek"]
        mode: require

- **`systemd-creds`** — the key is read from `$CREDENTIALS_DIRECTORY/hoarddb-dek`, which systemd
  provides to a unit that loads the credential. Encrypt it with the TPM; `--with-key=auto` falls
  back to a host key on a machine without one, which is weak against a stolen disk:

      head -c 32 /dev/urandom | systemd-creds encrypt --name=hoarddb-dek --with-key=tpm2 - /etc/credstore.encrypted/hoarddb-dek

  and in the unit:

      [Service]
      LoadCredential=hoarddb-dek:/etc/credstore.encrypted/hoarddb-dek
      LimitCORE=0

  Commands run with the server stopped (for example `hoarddb-server -reset-password`) do not see
  the unit's credentials. Run them through `systemd-run` with the same credential:

      systemd-run --wait --pipe -p LoadCredential=hoarddb-dek:/etc/credstore.encrypted/hoarddb-dek hoarddb-server -reset-password admin

Every start logs the encryption mode, and an encrypted node also logs its keystore and key ids.
The first encrypted start prints them once, in full, with the paths of the two wrapped key files.

### Checking a running node

Ask the node instead of reading its configuration. On `/metrics` (with `HOARDDB_METRICS_ENABLED=true`),
`hoarddb_node_encryption_info` carries the configured `mode` and `keystore` as labels, and
`hoarddb_node_encryption_sealed` is `1` when the node's store files show it sealed. The start log
says `ring store and log encrypted at rest (node master kid <kid>)` or `ring store and log are not
encrypted (encryption.mode off)`. The CLI has no statement that prints the encryption state: not
available yet.

"Sealed" is not read from `encryption.mode`. The node inspects its store's files once, right after
the store opens (every table carries a key id, the key registry refuses an empty key), so a store
that is not actually sealed is never reported as sealed. Key ids are fingerprints and are safe to
share; none of these views carries a key.

### Losing the key

**Losing the keystore is unrecoverable data loss.** Without the KEK, no bucket, user or log entry
on the node can be read again, and turning encryption off does not bring the data back as
plaintext. Back up the KEK separately from the data directory: a backup that holds both is a
backup anyone can read.

The node master can be exported and imported, with the server stopped, so that a copy of it
exists that does not depend on the KEK:

    hoarddb-server -export-key /safe/place/node-master.key
    hoarddb-server -import-key /safe/place/node-master.key

Both read the same configuration as the server (the keystore must be set up so the node can be
opened). The sealed credential files are sealed under the node master, so an imported node master
opens them as it opens the rest. `-export-key` writes the **raw 32-byte node master, unencrypted**, to a file that must be
outside the data directory, with mode `0600`; it never creates a master, and it fails if the data
directory has none. Treat that file as you treat the KEK, and encrypt it (with `gpg` or `age`)
before it leaves the host. `-import-key` wraps the file under the keystore you have configured
*now*, so it also moves a node onto a different KEK. It refuses a file that does not open the
data directory's existing sealed keys, and refuses to replace a node master that is already there
unless you add `-force`.

Without an export, keep a copy of `keys/node-master.key.wrapped` with your data directory backups
and the KEK in a separate place. That restores the node only while that KEK exists.

**Back up `ring/keys/` with the data directory.** `ring/keys/store.wrapped` and
`ring/keys/wal.wrapped` hold the keys of the node's ring store and log, wrapped under the node
master. They exist nowhere else, and the exported node master does not cover them: a sealed ring
whose `ring/keys/` is lost cannot be read, and the start says which files are missing.

**There is no key rotation command yet.** Nothing in this version re-encrypts a node under a new
node master, and the node's ring keys (the store key, the log's keys) are not rotated; a node that
holds data cannot be given new keys. Replacing the KEK by exporting and importing, as above, is the only change of key you can
make.

### When the wrapped node master is missing

If `keys/node-master.key.wrapped` is gone from a data directory that still holds sealed data, the
server **refuses to start**. It does not create a new node master, because a new one could not open
what is already there, and it writes nothing. The error names the missing file and the sealed
files it found. There are two ways out:

1. Restore `keys/node-master.key.wrapped` from a backup of this data directory (it opens with the
   KEK you have configured).
2. Import the exported node master while the server is stopped:

       hoarddb-server -import-key /safe/place/node-master.key

   Then start again.

If you meant to start a new, empty node, point `data_dir` at an empty directory instead. With no
sealed data in the directory, a missing node master is a first start and one is generated.

### What encryption at rest does not cover

- **Other nodes of the cluster**, and a node that runs with encryption off (above).
- **The cluster's metadata in `ring/raft`**: database names, member ids and addresses, settings and
  the cluster's users.
- **Server logs**, which may contain document ids, bucket names and store-field keys.
- **A `dump`**, which is plaintext whatever the mode, and the file `-export-key` writes.
- **`drop bucket` is not a cryptographic erase.** Every bucket on a node shares one key, and a
  dropped bucket's data stays in the encrypted store until it is compacted away.
- **Someone who can run code as the server's user** can ask the same keystore for the key. This
  protects a copied disk or data directory, not a running, compromised host.
- **It does not replace full-disk encryption**, which also covers swap, journals and freed blocks.
  Run with encrypted swap or none, and with core dumps off (`LimitCORE=0`): the server's memory
  holds the keys.

## File permissions and the umask

`hoarddb-server` makes everything it creates owner-only. At start-up it adds `077` to the
process's umask, so a file is `0600` and a directory `0700` whatever the umask the shell or the
service manager gave it. Before this, under the common umask `0002`, the data directory was `0775`
and the store's own files (`*.vlog`, `*.mem`, `DISCARD`, `LOCK`) were `0664`, readable by anyone in
the server user's group. The server logs one line when it changes the umask, with the old and new
values.

- **It only tightens.** A process already running under `0277` keeps `0277`.
- **It does not change what exists.** Files and directories from a previous start keep their modes
  until you run `chmod -R go-rwx` on the data directory; Badger creates new table files under the
  new umask as it compacts.
- **It is process-wide.** The umask belongs to the whole process, so a program the server starts
  (the `command` keystore's program) runs under it too. There is no setting to turn it off.
- **What it breaks:** anything that read the data directory through group membership, such as a
  backup agent running as another user in the server's group, can no longer read it. Back up with
  `hoarddb dump`, or give the backup agent the server's user.

## Prompt Themes

The CLI prompt can be themed from a file. The default is the logo palette;
a theme in `~/.hoarddb/theme.yml` overrides it.

```yaml
version: 1
name: my-theme
styles:
  brand:    {fg: "#f1efe8", bg: null}      # "hoard" and the [ ] around the mark
  accent:   {fg: "#a99cf5", bg: null}      # the mark's h and "DB"
  tile:     {fg: "#f1efe8", bg: "#3c3489"} # the logo block, for a format that uses it
  database: {fg: "#6f63d6", bg: null}      # [dbname]
  error:    {fg: "#f07178", bg: null}      # the `CLI error:` prefix
prompt_format:   "{brand:[}{accent:h}{brand:]}{brand:oard}{accent:DB}{database: [%database]}> "
prompt_format_nodb: "{brand:[}{accent:h}{brand:]}{brand:oard}{accent:DB}> "
```

The default prompt is one line of foreground colour: the `[ ]` and `hoard` in
the off-white `brand`, the `h` and `DB` in the lightened indigo `accent`, so the
mark reads as part of the wordmark. No span paints a background, which keeps it
legible whatever the terminal background is — the logo indigo `#3c3489` is
1.78:1 as text on black and would be unusable, so no text span uses it. The
`tile` style still carries that logo block for a `prompt_format` that wants it,
written `{tile:[}{tile:h}{tile:]}`.

The directory is 0700, the file 0644, created only if the directory does not
exist. Precedence, lowest to highest: the built-in default, the file, the
`HOARDDB_THEME` environment variable (a path), and `--theme`.

**A partial file is a recolouring, not a replacement.** Everything it does not
name comes from the built-in theme: set only `styles.accent.fg` to change the
mark's colour and keep the rest of the prompt, styles and both format strings as
they are. This is the usual file — most people want one colour changed. Run
`theme` to see what resolved, including which file it came from.

`--theme plain` (or `HOARDDB_THEME=plain`) disables colour entirely; docs and
screenshots pin it so output does not depend on whose home directory ran it.

The format grammar: `%database` is the current database (empty when none is
selected), `{style:text}` spans colour their text, `%%`, `{{` and `}}` are the
escaped literals, and a span whose expansion is empty is omitted — which keeps
the `[ ]` around an absent database from appearing.

**Colour is on only when all of these hold**: stdout is a terminal, `NO_COLOR`
is unset, and `TERM` is set and not `dumb`. A piped session therefore carries no escape
bytes. A broken theme file — invalid YAML, a typo in a key, a colour that is
not `#rrggbb`, an unknown style name, an unclosed brace — prints one warning
naming the file and falls back to the default theme; it never stops the REPL.

The terminal's default background is queried once per session (OSC 11), and a
`tile` style swaps its two colours when the terminal answers `light` — so a
theme file that paints the logo block stays legible either way. The default
prompt paints no block, so the answer changes nothing for it.
`HOARDDB_BACKGROUND=dark|light` pins the answer for terminals that never reply.
Run `theme` in the REPL to see the resolved name, file, colour state and
background.

**The default palette is built for a dark terminal.** `brand` is 18.25:1 on
black but 1.15:1 on white, so on a light terminal the default prompt is hard to
read. That is a property of the palette, not of this release: only `tile` was
ever adapted. If your terminal has a light background, either use `--theme
plain`, or write a `theme.yml` with dark foregrounds — `brand: "#1c1b18"` and
`accent: "#4a3fa8"` are 17.22:1 and 8.18:1 on white, both AAA. That pair is for
a light terminal only: no single foreground is legible on black *and* white,
which is why the terminal's background is queried rather than guessed.
