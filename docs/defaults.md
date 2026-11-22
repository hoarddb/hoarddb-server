# Default settings

Every setting hoardDB reads, with the value it has when you set nothing. The
server needs none of them to start: `hoarddb-server` with no arguments and no
configuration file runs on these values.

A setting is changed by an environment variable, or by a key in the YAML
configuration file, and the environment wins. [Configuration](configuration.md) says
how, what each setting does and what happens when its value is wrong. Where a
default is not a fixed number (it depends on the machine, or on another setting)
the cell says what it depends on.

## Listening and connections

Where the server listens, and how many connections it accepts. The server listens on this machine only until you change it.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_TCP_LISTEN` | `127.0.0.1:7433` | The address clients connect to (TCP with TLS). Set `0.0.0.0:7433` to accept clients from other machines. (`tcp.listen`) |
| `HOARDDB_TCP_ADVERTISE` | (empty): the listen address | The `host:port` a cluster member tells clients to connect to, when that is not the listen address. (`tcp.advertise`) |
| `HOARDDB_LISTEN` | `127.0.0.1:4433` | The node's own address in a cluster, which other nodes are told to dial. Node-to-node traffic uses the next port up. (`server.listen`) |
| `HOARDDB_TCP_INTERNODE_LISTEN` | (empty): `HOARDDB_LISTEN` with the port plus one | Overrides the node-to-node listen address. (`tcp.internode_listen`) |
| `HOARDDB_MAX_CONNS` | `1000` | Client connections the server accepts at once. (`server.max_conns`) |
| `HOARDDB_MAX_PEER_CONNS` | `64` | Connections kept for other nodes, separate from the client budget so clients cannot starve them. (`server.max_peer_conns`) |
| `HOARDDB_TCP_MAX_CONNS_PER_IP` | `64` | Connections from one source address. `0` turns the limit off. (`tcp.max_conns_per_ip`) |
| `HOARDDB_TCP_AUTH_MAX_FAILS` | `20` | Failed logins in a row before an address is slowed down and then refused. `0` turns it off. (`tcp.auth_max_fails`) |
| `HOARDDB_TCP_IDLE_TIMEOUT` | `300` | Seconds of silence before a connection is closed. (`tcp.idle_timeout_seconds`) |
| `HOARDDB_TCP_FRAME_TIMEOUT` | `30` | Seconds allowed to read one request once it has started. (`tcp.frame_timeout_seconds`) |
| `HOARDDB_TCP_PIPELINE_FRAMES` | `4` | Requests read ahead on one connection. `1` is strict one-at-a-time. (`tcp.pipeline_frames`) |
| `HOARDDB_TCP_PIPELINE_BYTES` | `1048576` | Bytes of requests read ahead on one connection (1 MiB). (`tcp.pipeline_bytes`) |

## Node and root user

Who the node is, and the user it creates on its first start.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_NODE_ID` | (empty): the host name | The node's name, unique in a cluster. (`server.node_id`) |
| `HOARDDB_ROOT_USER` | `admin` | The name of the root user created on the first start. |
| `HOARDDB_ROOT_PASSWORD` | (none) | The root password for the first start. Without it a terminal asks for one, and a start with no terminal refuses. Only its hash is kept. |
| `HOARDDB_ROOT_PASSWORD_FILE` | (none) | A file holding the root password, read instead of the environment variable. |
| `HOARDDB_REQUIRE_ROOT_PASSWORD` | `false` | When `true`, a first start with no password supplied refuses instead of asking. (`auth.require_root_password`) |
| `HOARDDB_AUTH_BOOTSTRAP_ROOT` | `true` | Whether the root user variables may create the root user in an empty user store. (`auth.auth_bootstrap_root`) |

## TLS

The server creates a self-signed certificate on its first start.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_TLS_MODE` | `auto` | `auto` is the only mode in this version: the node serves a certificate it generated itself. Any other value stops the server at start-up. (`tls.mode`) |
| `HOARDDB_TLS_CERT` | (none) | Not available in this version: setting it stops the server at start-up. (`tls.cert`) |
| `HOARDDB_TLS_KEY` | (none) | Not available in this version: setting it stops the server at start-up. (`tls.key`) |
| `HOARDDB_TLS_CA` | (none) | Not available in this version: setting it stops the server at start-up. (`tls.ca`) |

## Authentication

Password and token settings.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_PASSWORD_MIN_LENGTH` | `12` | The shortest password accepted, in characters. (`auth.password_min_length`) |
| `HOARDDB_TOKEN_TTL` | `900` | Seconds a login token lasts; using it refreshes it. (`auth.token_ttl_seconds`) |
| `HOARDDB_TOKEN_MAX_LIFETIME` | `12` | Hours after which a token stops refreshing and the user must log in again. (`auth.token_max_lifetime_hours`) |
| `HOARDDB_AUTH_SKEW` | `60` | Seconds of clock difference tolerated when checking a token. (`auth.auth_skew_seconds`) |
| `HOARDDB_AUTH_MAX_DERIVATIONS` | (empty): the number of CPUs | Password hashes computed at once. (`auth.auth_max_derivations`) |
| `HOARDDB_AUTH_KEYRING_WINDOW` | `1800` | Seconds a previous signing key still validates after a rotation. When unset it is twice the token lifetime, and at least 15 minutes. (`auth.auth_keyring_window_seconds`) |
| `HOARDDB_AUTH_DEPLOYMENT_ID` | (empty) | A suffix that separates this deployment's token keys from another's. (`auth.auth_deployment_id`) |
| `HOARDDB_AUTH_CONFIG_MAX_STALENESS` | `300` | No effect on a node this build starts: a cluster member serves logins from the users it last applied, with no time bound. (`auth.auth_config_max_staleness_seconds`) |
| `HOARDDB_AUTH_MASTER_RETRY` | `5` | Seconds between retries when copying users to a node that is behind. (`auth.auth_master_retry_seconds`) |

## Storage

Where data lives, and the limits on what one write may hold.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_DATA_DIR` | `./data` | The directory for the node's data, keys and users. (`storage.data_dir`) |
| `HOARDDB_BLOB_DIR` | (empty): `<data dir>/blobs` | Where large blob payloads are kept. (`storage.blob_dir`) |
| `HOARDDB_MAX_BLOB_BYTES` | `16777216` (16 MiB) | The largest blob accepted. (`storage.max_blob_bytes`) |
| `HOARDDB_MAX_DOCUMENT_BYTES` | `25165824` (24 MiB) | The largest document accepted. (`storage.max_document_bytes`) |
| `HOARDDB_MEMTABLE_MB` | (empty): chosen at the first start from the node's memory | The store's write buffer, in MiB. The choice is saved and kept on every restart. (`storage.memtable_mb`) |
| `HOARDDB_BADGER_BLOCK_CACHE_MB` | `128` | Block cache of the store, in MiB (the default applies when the setting is `0`). (`storage.badger_block_cache_mb`) |
| `HOARDDB_BADGER_INDEX_CACHE_MB` | `64` | Index cache of the store, in MiB (the default applies when the setting is `0`). (`storage.badger_index_cache_mb`) |
| `HOARDDB_BADGER_NUM_COMPACTORS` | `4` | Background compaction workers of the store (the default applies when the setting is `0`). (`storage.badger_num_compactors`) |
| `HOARDDB_BADGER_TABLE_SYNC` | `msync` | How the store makes new table files durable: `msync` or `stepwise`. (`storage.badger_table_sync`) |
| `HOARDDB_WAL_PREALLOCATE` | `auto` | Whether write-ahead log segments are filled with zeros before use: `auto` (on for ext2, ext3 and ext4), `on` or `off`. (`storage.wal_preallocate`) |
| `HOARDDB_UNSAFE_ACK_BEFORE_SYNC` | `false` | When `true`, a write is answered before it is synced to disk. For benchmarks only: it can lose a write the server has acknowledged. (`storage.unsafe_ack_before_sync`) |
| `HOARDDB_RECLAIM_KEYS_PER_SECOND` | (empty): `10000` | How fast the background cleaner deletes the data of dropped buckets, in keys per second for the whole node. (`storage.reclaim_keys_per_second`) |
| `HOARDDB_DISK_HIGH_WATER` | `85` | Percent of disk used at which the node stops taking in new data it does not already hold. `0` turns it off. (`server.disk_high_water_pct`) |
| `HOARDDB_DUMP_PIN_CEILING_SECONDS` | `300` | Seconds a dump may hold a queue before it gives up. (`server.dump_pin_ceiling_seconds`) |
| `HOARDDB_RESTORE_DISK_HEADROOM_MULTIPLIER` | `2` | Free disk a restore needs, as a multiple of the dump's size. (`server.restore_disk_headroom_multiplier`) |
| `HOARDDB_TTL_SWEEP_SECONDS` | `60` | Seconds between passes that delete expired documents. `0` turns it off. (`server.ttl_sweep_seconds`) |
| `HOARDDB_TTL_SWEEP_BUDGET_SECONDS` | `5` | Seconds one expiry pass may run. (`server.ttl_sweep_budget_seconds`) |

## Queries

Limits on one query.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_QUERY_MAX_RESULT_BYTES` | `16777216` (16 MiB) | The most data one query may hold before it is refused. Allowed from 1 MiB to 24 MiB. (`query.max_result_bytes`) |
| `HOARDDB_QUERY_CONCURRENCY` | (empty): twice the number of CPUs, at least 4 and at most 256 | Queries running at once. More wait their turn and are not refused. (`query.concurrency`) |
| `HOARDDB_QUERY_PAGE_LIMIT_DEFAULT` | `100` | Documents in a page when a request names no size. (`query.page_limit_default`) |
| `HOARDDB_QUERY_PAGE_LIMIT_MAX` | `10000` | The largest page a request may ask for. (`query.page_limit_max`) |
| `HOARDDB_QUERY_NEAREST_MAX_ELEMENTS` | `100000` | The most elements one nearest-neighbour call scans in a single store field. A field holding more is refused before anything is read. Allowed from 1 to 10000000. (`query.nearest_max_elements`) |

## Logging, metrics and audit

The log goes to standard output as JSON. Metrics, the audit log and profiling are off.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. (`logging.level`) |
| `HOARDDB_LOG_FORMAT` | `json` | `json` or `text`. (`logging.format`) |
| `HOARDDB_LOG_OUTPUT` | `stdout` | Where the log is written. (`logging.output`) |
| `HOARDDB_METRICS_ENABLED` | `false` | Serve Prometheus metrics. (`metrics.enabled`) |
| `HOARDDB_METRICS_LISTEN` | `127.0.0.1:9090` | The address of the metrics endpoint, when enabled. (`metrics.listen`) |
| `HOARDDB_AUDIT_ENABLED` | `false` | Write the audit log. (`audit.enabled`) |
| `HOARDDB_AUDIT_OUTPUT` | `/var/log/hoarddb/audit.jsonl` | The audit log file, when enabled. (`audit.output`) |
| `HOARDDB_PPROF_ENABLED` | `false` | Serve the Go profiling endpoint on port 6060. (`debug.pprof_enabled`) |
| `HOARDDB_PPROF_BLOCK_RATE_NS` | `1000000` | With pprof enabled, sample one blocking event per this many nanoseconds spent blocked, so the `block` profile has data. `0` turns it off. (`debug.pprof_block_rate_ns`) |
| `HOARDDB_PPROF_MUTEX_FRACTION` | `100` | With pprof enabled, report 1 in this many contended mutex events, so the `mutex` profile has data. `0` turns it off. (`debug.pprof_mutex_fraction`) |

## Encryption at rest

Off.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_ENCRYPTION_KEYSTORE` | `off` | Where the key comes from: `off`, `keyfile`, `command` or `systemd-creds`. (`encryption.keystore`) |
| `HOARDDB_ENCRYPTION_KEY_FILE` | (none) | The key file, for the `keyfile` keystore. (`encryption.key_file`) |
| `HOARDDB_ENCRYPTION_MODE` | `off` | `off`, `on` or `require`. (`encryption.mode`) |

## Cluster

A node is a one-node cluster until you set `HOARDDB_CLUSTER_ENABLED`. [Configuration](configuration.md#cluster-mode) has the steps to add a node.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_CLUSTER_ENABLED` | `false` | Start the node as a member of a ring cluster, with a peer listener. (`cluster.enabled`) |
| `HOARDDB_CLUSTER_LISTEN` | (empty): `HOARDDB_LISTEN` | The peer listener address. (`cluster.listen`) |
| `HOARDDB_CLUSTER_ADVERTISE` | (empty): the peer listener address | The address other nodes are told to dial. A wildcard listen address needs it set. (`cluster.advertise`) |
| `HOARDDB_CLUSTER_WITNESS` | `false` | Join as a witness: the node votes and holds no data. (`cluster.witness`) |
| `HOARDDB_CLUSTER_ALLOW_MEMBERSHIP` | (empty): `true` in cluster mode | Lock for `join`, `promote`, `leave`, `remove-node` and `replace-node`. (`cluster.allow_membership`) |
| `HOARDDB_CLUSTER_VOTERS` | `3` | Voting members, witnesses included: `3` or `5`. (`cluster.voters`) |
| `HOARDDB_SEED_NODES` | (none) | The nodes this node knows about, as `node-id@host:port`. A node with none starts a new cluster. (`hash_ring.seed_nodes`) |
| `HOARDDB_SEED_FINGERPRINTS` | (none) | The keys to trust for those nodes, as `node-id=SHA256:<hex>`. Required whenever there are seed nodes. (`hash_ring.seed_fingerprints`) |

## Cluster timing and limits

Tuning. The Raft and connection limits below can be set. The lease and failure-detection timings cannot: in this release every cluster runs on fixed values, and a configuration file or environment variable that sets `cluster.heartbeat_ms`, `cluster.step_down_ms`, `cluster.failover_ms`, `cluster.dead_ms`, `cluster.probe_interval_ms`, `cluster.probe_timeout_ms`, `cluster.probe_failures` or `cluster.stale_reads` is refused at start. Remove the key. The fixed values are: a lease grant every 100 ms, a data node serving for up to 2000 ms after its last grant, a second report of a failure within 5000 ms, a member declared dead after 600000 ms down, a probe every 1000 ms that fails after 500 ms, 3 failed probes in a row before a node is suspected, and reads that never answer from a copy unless the request asks for it.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_CLUSTER_RAFT_HEARTBEAT_MS` | `1000` | Milliseconds between the leader's heartbeats. (`cluster.raft_heartbeat_ms`) |
| `HOARDDB_CLUSTER_RAFT_ELECTION_MS` | `1000` | Milliseconds without a heartbeat before an election starts. (`cluster.raft_election_ms`) |
| `HOARDDB_CLUSTER_RAFT_LEADER_LEASE_MS` | `500` | Milliseconds a leader holds its lease without contact from a majority. (`cluster.raft_leader_lease_ms`) |
| `HOARDDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD` | `128` | Log entries between snapshots of the cluster state. (`cluster.raft_snapshot_threshold`) |
| `HOARDDB_CLUSTER_RAFT_TRAILING_LOGS` | `64` | Log entries kept after a snapshot. (`cluster.raft_trailing_logs`) |
| `HOARDDB_CLUSTER_RAFT_MAX_CONNS` | `64` | Inbound connections from other voters. (`cluster.raft_max_conns`) |
| `HOARDDB_CLUSTER_RAFT_IDLE_SECONDS` | `120` | Seconds of silence before such a connection is closed. (`cluster.raft_idle_seconds`) |
| `HOARDDB_CLUSTER_DECL_CACHE_ENTRIES` | `4096` | Bucket declarations kept in the cache. (`cluster.decl_cache_entries`) |
| `HOARDDB_CLUSTER_DECL_CACHE_BYTES` | `33554432` (32 MiB) | The size limit of that cache. It takes precedence over the entry limit. (`cluster.decl_cache_bytes`) |
| `HOARDDB_CLUSTER_PEER_MAX_PENDING` | `128` | Node-to-node connections held before they are routed. (`cluster.peer_max_pending`) |
| `HOARDDB_CLUSTER_PEER_MAX_CONNS` | `256` | Connections held on each node-to-node channel. (`cluster.peer_max_conns`) |
| `HOARDDB_CLUSTER_PEER_MAX_IN_FLIGHT` | `256` | Requests handled at once on each node-to-node channel. (`cluster.peer_max_in_flight`) |
| `HOARDDB_CLUSTER_PEER_IDLE_SECONDS` | `120` | Seconds of silence before a node-to-node connection is closed. (`cluster.peer_idle_seconds`) |
| `HOARDDB_CLUSTER_PEER_FRAME_TIMEOUT_MS` | `10000` | Milliseconds allowed to finish reading one node-to-node frame. (`cluster.peer_frame_timeout_ms`) |

## Replication, retention and seeding

Copying data between nodes is not usable end to end yet ([Limitations](limitations.md#cluster)). These are its settings, and their defaults.

| Environment variable | Default | What it sets |
|----------------------|---------|--------------|
| `HOARDDB_REPLICATION_FACTOR` | `1` | Refused above `1` at start, in every mode: without `HOARDDB_CLUSTER_ENABLED=true` it selected the old replicated mode, which is gone, and the ring would ignore it. A database gets its copies from `create database <name> { replication_factor: N }` and has one copy when it names none. (`server.replication_factor`) |
| `HOARDDB_WRITE_CONCERN` | (empty): `majority` when the factor is above 1, otherwise `one` | How many copies a write waits for: `one`, `majority` or `all`. (`server.write_concern`) |
| `HOARDDB_AUTO_REBALANCE` | `true` | Start moving data as soon as the membership changes. (`server.auto_rebalance`) |
| `HOARDDB_WAL_RETENTION_INTERVAL` | `60` | Seconds between passes that prune the write-ahead log. `0` turns it off. (`server.wal_retention_seconds`) |
| `HOARDDB_REPLICATION_RETAINED_LOG_SECONDS` | `1800` | Seconds of write-ahead log kept for a node that falls behind. (`replication.retained_log_seconds`) |
| `HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS` | `60` | Seconds between retention checks, from 1 to 3600. (`replication.retention_check_seconds`) |
| `HOARDDB_REPLICATION_LOG_MAX_BYTES` | `4294967296` (4 GiB) | The most write-ahead log kept. (`replication.log_max_bytes`) |
| `HOARDDB_REPLICATION_REPL_WINDOW` | `64` | Records in flight to a copy at once, from 1 to 4096. (`replication.repl_window`) |
| `HOARDDB_REPLICATION_DEDUPE_WINDOW` | `8192` | Applied records remembered, so a repeated request is not applied twice. (`replication.dedupe_window`) |
| `HOARDDB_REPLICATION_DEDUPE_BYTES` | `536870912` (512 MiB) | The size limit of those remembered records. (`replication.dedupe_bytes`) |
| `HOARDDB_REPLICATION_HANDOFF_DRAIN_MS` | `2000` | Milliseconds allowed for handing off writes in flight when ownership moves. (`replication.handoff_drain_ms`) |
| `HOARDDB_REPLICATION_HANDOFF_RETRY_MS` | `200` | Milliseconds between retries of a hand-off request. (`replication.handoff_retry_ms`) |
| `HOARDDB_REPLICATION_WC_TIMEOUT_MS` | `5000` | Milliseconds a write waits for its copies. (`replication.wc_timeout_ms`) |
| `HOARDDB_REPLICATION_SEED_SLOTS_IN_FLIGHT` | `2` | Slots copied to a new node at once. (`replication.seed_slots_in_flight`) |
| `HOARDDB_REPLICATION_SEED_BYTES_PER_SECOND` | `67108864` (64 MiB) | The most bytes per second a node takes in while it is seeded, shared by the pulls running at once; `0` is unlimited. A share too small to pay for one chunk within half of `seed_idle_seconds` (below about 17 KiB/s per pull at the defaults) is not held. (`replication.seed_bytes_per_second`) |
| `HOARDDB_REPLICATION_SEED_WINDOW_BYTES` | `8388608` (8 MiB) | Seeding data in flight before an acknowledgement. (`replication.seed_window_bytes`) |
| `HOARDDB_REPLICATION_SEED_CHUNK_BYTES` | `1048576` (1 MiB) | Bytes in one seeding chunk. (`replication.seed_chunk_bytes`) |
| `HOARDDB_REPLICATION_SEED_CHUNK_ENTRIES` | `1024` | Entries in one seeding chunk. (`replication.seed_chunk_entries`) |
| `HOARDDB_REPLICATION_SEED_BUFFER_BYTES` | `268435456` (256 MiB) | Memory a node may use to buffer seeding data. (`replication.seed_buffer_bytes`) |
| `HOARDDB_REPLICATION_SEED_SYNC_BYTES` | `67108864` (64 MiB) | Seeding bytes written between syncs to disk. (`replication.seed_sync_bytes`) |
| `HOARDDB_REPLICATION_SEED_RESUME_HOLD_SECONDS` | `300` | Seconds an interrupted seed is kept so it can resume. (`replication.seed_resume_hold_seconds`) |
| `HOARDDB_REPLICATION_SEED_IDLE_SECONDS` | `120` | Seconds of silence before a seeding connection is closed, and the time without an applied chunk after which `status seeding` shows the pull as stalled and it retries on the next legal source. (`replication.seed_idle_seconds`) |
| `HOARDDB_REPLICATION_SEED_DISK_HIGH_WATER_PCT` | `85` | Percent of disk used at which a node refuses to be seeded; a receiver at or above it also pauses its own pulls and keeps the data it has. (`replication.seed_disk_high_water_pct`) |
| `HOARDDB_REPLICATION_INCOMING_SEEDS_MAX` | `4` | Seeds a node accepts at once. (`replication.incoming_seeds_max`) |

## Settings with no environment variable

These are set only in the YAML file.

| YAML key | Default | What it sets |
|----------|---------|--------------|
| `auth.argon2_memory` | `65536` (KiB, so 64 MiB) | Memory the password hash uses. |
| `auth.argon2_iterations` | `3` | Passes of the password hash. |
| `auth.argon2_parallelism` | `4` | Threads of the password hash. |
| `auth.argon2_salt_length` | `16` (bytes) | Length of the random salt. |
| `auth.argon2_hash_length` | `32` (bytes) | Length of the stored hash. |
| `auth.token_expiry_hours` | `0` (not set) | Deprecated: use `HOARDDB_TOKEN_MAX_LIFETIME`. |
| `encryption.key_command` | (none) | The command that prints the key, for the `command` keystore. A list of arguments, not a shell string. |
