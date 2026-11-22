# Security

What hoardDB protects out of the box, what you have to turn on, what it does not
protect, and how to tell us about a vulnerability. Every feature on this page is
free and unlocked in every build; there is no enterprise tier for TLS, users or
encryption.

## What a fresh install does

With no configuration, `hoarddb-server`:

- serves every connection over **TLS 1.3**, with no way to downgrade;
- **requires a login** on every connection before it will answer anything;
- creates the root user the first time it starts: it asks for the password on a
  terminal, and with no terminal it refuses until you set `HOARDDB_ROOT_USER` and
  `HOARDDB_ROOT_PASSWORD`. It never makes a password up or prints one.
  Only the password's hash is kept, in `<data dir>/users.json`; the password is
  never written to disk, so a copy or backup of the data directory does not
  carry it;
- creates its own private keys with mode 0600, in the data directory;
- stores passwords only as hashes, and leaves the audit log, the metrics
  endpoint and the profiling endpoint **off**.

It does **not** encrypt the data on disk until you ask it to, and it listens on
loopback only until you change `HOARDDB_TCP_LISTEN`. Both are covered below.

## TLS and self-signed certificates

A node generates an Ed25519 key and a self-signed certificate on its first start
and serves them from `<data dir>/keys/server.key` and `server.crt`. No
certificate authority is involved, so there is nothing to renew and nothing for
a client to download — and nothing that proves to a client that it reached the
right machine, other than the certificate's **fingerprint**.

The server prints its fingerprint every time it starts, in the line that says it
is listening:

```text
{"level":"INFO","msg":"TCP driver listener listening","address":"[::]:7433","fingerprint":"SHA256:d8aa96c0…","alpn":"hoarddb/1"}
```

### Trusting a server from the CLI

The CLI trusts no server by default. It connects only when you give it the
fingerprint the server printed, or when you say `-insecure`:

```bash
hoarddb -address db.example:7433 -fingerprint 'SHA256:d8aa96c0…'
```

Without either, it refuses and names both:

```text
The CLI trusts no server by default. Pass -fingerprint SHA256:<hex> (the server prints it when it starts), or -insecure for a node you started yourself on this machine
```

The connection then succeeds only if the server's certificate has that
fingerprint. A different certificate at the same address fails the connection:
if you did not replace the server, treat that as an attack until you know
otherwise. The CLI pins one server, so give `-fingerprint` once, for the node at
`-address`. Compare the fingerprint with the one in the server's start-up log, and
copy it over a channel you trust. For a script, keep the fingerprint with the
rest of the script's settings.

`-insecure` skips the check altogether. It prints a warning that says exactly
what it gives up, and it is for a server you started yourself on the same
machine. It is a setting of the client and never makes a server less strict.
Do not put it in a script that talks to a server across a network.

### Nodes trusting each other

Nodes use the same certificates between themselves. A node that is told to join
a cluster must be given the fingerprints it should expect
(`HOARDDB_SEED_FINGERPRINTS`) and refuses to start without them; a certificate
that changes for a node it already knows is a hard failure, not a quiet
re-trust. [Configuration](configuration.md#seed-pinning-required-for-every-join)
has the details.

Both sides prove who they are with those keys. A node that opens a connection to
another member checks that member's certificate against the key pinned under its
node id. A node that accepts a connection asks for the caller's certificate and
refuses, at the TLS handshake, a key that is not pinned. This release has no
cluster token: members trust each other by pinned keys. Whoever holds a
member's TLS private key (`<data dir>/keys/server.key`) can act as that member,
so keep that file on its node only, and keep the node-to-node ports on a private
network.

When a node copies a bucket in from another node, it sets its own limit on how
much it holds. It refuses a copy announced as larger than half of its memory
limit before it fetches any of it, and refuses any piece larger than the copy's
piece size when it arrives.

### Your own certificate

You cannot substitute your own certificate in this version. A node started with
`HOARDDB_TLS_MODE=custom`, or with `HOARDDB_TLS_CERT`, `HOARDDB_TLS_KEY` or
`HOARDDB_TLS_CA` set, refuses to start: it exits with an error that names the
setting, rather than quietly serving the certificate it generated. Unset the
setting and use the generated certificate, which clients trust by its fingerprint. Operator-supplied
certificates come after the first release. There is no certificate authority check to
configure; the fingerprint is the identity.

## Users and authentication

Every connection logs in once, and what the connection may do is decided by the
roles the user held at that moment.

- **Passwords are hashed with Argon2id**, with a server-side secret added, and
  the plain text is never stored.
- **A new password must be at least 12 characters** (`HOARDDB_PASSWORD_MIN_LENGTH`,
  1 to 1024). `create user` and `alter user <name> password` refuse a shorter one
  and change nothing:

      the password for user "bob" is 8 characters; this server requires at least 12.
      Choose a longer password, or ask an administrator to change HOARDDB_PASSWORD_MIN_LENGTH; nothing was changed

  The CLI does the measuring, against the number the server it is connected to
  reports when it logs in. It has to: the CLI hashes the new password itself and
  sends the server only the hash, so the server never sees the password and
  cannot count its characters. Two things follow. A program that builds its own
  user-creation request (a hand-written client, or a driver that does not apply
  the check) is not measured by the server, so only give the user-administration
  role to people and tools you trust to choose long passwords. And set the same
  value on every node of a cluster, because the CLI uses the value of the node it
  is connected to. `hoarddb-server -reset-password` applies the same minimum
  itself, since it runs with the password in hand. The root password you give in
  `HOARDDB_ROOT_PASSWORD` for the first start is measured too: a first start that
  seeds the root user refuses a password shorter than `HOARDDB_PASSWORD_MIN_LENGTH`
  and names the setting that held it, `HOARDDB_ROOT_PASSWORD` or
  `HOARDDB_ROOT_PASSWORD_FILE`. A restart over an existing user store does not seed from
  it, so a stale value there is only warned about, as before.
- **Give the CLI a password without putting it on the command line.** Set
  `HOARDDB_ROOT_PASSWORD` (it stays out of `ps`), or give `-password` with no
  value and the CLI asks for it on a terminal. `-password=VALUE`
  puts the password on the command line, where `ps` and shell history can show
  it; do not use it on a shared host. `-password-file` and `-password-stdin` supply
  the new password for `create user`, not the login.
- **Repeated failures slow an address down.** After five failed logins in a
  row from one address, each further attempt is delayed, doubling from one second up to
  thirty. At `HOARDDB_TCP_AUTH_MAX_FAILS` failures in a row (default 20) the
  address is refused outright for sixty seconds, before any password is checked.
  A successful password login clears the count, and so does fifteen minutes
  without a failure. A login whose password is still being checked counts toward
  the limit until it settles, so many guesses sent at once cannot all be checked
  before the first ones fail: at most `HOARDDB_TCP_AUTH_MAX_FAILS` logins from one
  address are checked at once, and the rest wait up to five seconds for a place
  (up to four times that many may wait). A login refused because the wait ran out
  is answered `RATE_LIMITED` with "too many logins from this address are being
  checked at once"; it is not a failed login and opens no sixty-second window.
  With the audit log on, it is recorded as an `auth.failure` entry whose result
  reads "login refused: N being checked, M waiting; no password was checked".
- **Roles are per database**, from `reader` (also accepted as `read`) up to `root`. A user with no grant on a
  database is refused there, and the refusal names the roles the user has and
  the command that would fix it.
- **Disabling or dropping a user, or changing their password, ends that user's
  open connections on every node**: at once on the node that takes the change,
  and on each other node as soon as the change reaches it, normally within a
  second. Granting or revoking a role takes effect at the user's next login; an
  open connection keeps the roles it logged in with. If you need a role change to
  bite immediately, change the password or disable the user.

Creating users, roles and the full list of what each can do:
[User management](user-management.md). The honest list of what this model does
not do: [Limitations](limitations.md#user-management-and-authorization).

### Requiring a root password in production

A first start with no root password set asks for one on a terminal, and refuses
when there is none. To refuse even at a terminal, so that every node's root
password comes from your secret store, turn the prompt off:

```bash
HOARDDB_REQUIRE_ROOT_PASSWORD=true HOARDDB_ROOT_PASSWORD='<choose one>' hoarddb-server
```

With `HOARDDB_REQUIRE_ROOT_PASSWORD=true` (YAML: `auth.require_root_password: true`),
a first start that is not given `HOARDDB_ROOT_PASSWORD` or
`HOARDDB_ROOT_PASSWORD_FILE` refuses to start and names the variable, rather than
asking or inventing a password nobody chose. The Docker image sets it, so a
container must always be given `HOARDDB_ROOT_PASSWORD`
(`docker run -e HOARDDB_ROOT_PASSWORD=... hoarddb`). Restarts are unaffected:
the root user already exists, and nothing is asked for.

### Rotating the root password

Stop the server and run:

```bash
hoarddb-server -reset-password admin
```

It asks for the new password twice, without echo, and writes only its hash. The
new password has to meet the minimum length above.

On a node with encryption at rest, run it with the same encryption settings as
the server (the same configuration file or `HOARDDB_ENCRYPTION_*` variables): it
opens the sealed `users.json` and the credential files under the node key, never
creates one, and writes `users.json` back sealed. Without them it cannot open the
sealed files and refuses, telling you to run it with the encryption settings the
data directory was created with. Under `systemd-creds` the key is
handed to a unit, so run it through `systemd-run`
([Configuration](configuration.md#encryption-at-rest)).

On a node with `cluster.enabled` it refuses before it asks for anything and
writes nothing: a cluster's users live in the cluster's replicated state, which a
stopped node cannot change. Log in on any node as another administrator and run
`alter user <name> password`; keep two administrators (see "Keep two
administrators" in [User management](user-management.md)). A node without a
cluster whose `users.json` names another node as its
master is refused once: run `hoarddb-server -reset-password admin -force` on it.
The next user change records the node itself and the refusal does not come back.

### A node's own keys

Each node signs the login tokens it issues with its own key
(`<data dir>/auth.token.key`), so a login token is good on the node that issued
it and on no other; whoever holds that file can forge a login on that node. When
you decommission a node, delete its data directory: it holds the signing key,
the TLS key and the node's copy of the users (`users.json` on a node without a
cluster, the `ring` directory on a cluster member).

## What a query can cost

A filter runs once for every document a query examines, so a hostile or careless
filter could otherwise tie up a node. A query is limited in three ways: the shape
of its filter (the size of a `$regex` pattern, the values in an `$in`, how deep
`$and` and `$or` nest, the clauses in all), the regex work done on any one
document, and the bytes a result may hold. A query over a limit is refused with
`INVALID_ARGUMENT` or `RESULT_TOO_LARGE`, naming the limit and what to change,
and nothing is partly done. The numbers are on [Limits](limits.md). The filter
and regex limits are fixed; the result limit can be set from 1 MiB to 24 MiB with
`HOARDDB_QUERY_MAX_RESULT_BYTES`.

Driver connection strings carry a password, so a driver that refuses one
(an unencoded `/`, `?`, `#` or extra `@` in the password, a bad percent-escape)
names the problem without repeating any part of the user info. Percent-encode
those characters in the password and the string is accepted.

## Encryption at rest

Encryption at rest is off by default. Turned on, it seals what a node stores on its
own disk: its ring store (every database's documents, every store field, the
declarations) and its ring log, under keys of that node's own. Someone who copies
the node's disk or data directory without its key reads neither. It is a setting of
each node, made before the node's first start, and every node of a cluster must use
the same one.

### Turn it on for a new node

1. Make a key-encryption key (KEK) **outside** the data directory, readable by the
   server's user only:

   ```bash
   sudo install -d -m 700 -o hoarddb /etc/hoarddb
   sudo -u hoarddb sh -c 'umask 077 && head -c 32 /dev/urandom > /etc/hoarddb/kek'
   ```

   The KEK can also come from a program or from systemd credentials
   ([Configuration](configuration.md#the-key-encryption-key-and-the-keystore)).

2. Set the mode and the keystore before the node's **first** start:

   ```yaml
   encryption:
     keystore: keyfile
     key_file: /etc/hoarddb/kek
     mode: on
   ```

   or `HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile
   HOARDDB_ENCRYPTION_KEY_FILE=/etc/hoarddb/kek`.

3. Start it. The log says so on every start:

   ```
   level=INFO msg="ring store and log encrypted at rest (node master kid a26783e7)"
   ```

   A node with encryption off says `ring store and log are not encrypted
   (encryption.mode off)` instead.

`require` seals exactly as `on` does, and also checks the structure of the node's own
store before it opens it (every table carries a key id, the key registry refuses an
empty key); a store that fails the check is refused. It does not look at other nodes.

### The keys, and what to back up

Each node has its own chain of keys, and none of them ever leaves the node:

- the **KEK**, from the keystore you configure, never in the data directory;
- the **node master**, `keys/node-master.key.wrapped`, wrapped under the KEK;
- the **ring keys**, `ring/keys/store.wrapped` (the store's key) and
  `ring/keys/wal.wrapped` (the log's keys), wrapped under the node master and made
  on the node's first start with encryption on.

Back up:

- **the KEK, apart from the data directory.** A backup that holds both is a backup
  anyone can read. **Losing the KEK loses the node's data**: turning encryption off
  does not bring it back.
- **the data directory, `ring/keys/` included.** The ring keys exist nowhere else.
  `hoarddb-server -export-key` exports the node master
  ([Configuration](configuration.md#losing-the-key)), which does **not** cover
  `ring/keys/`: without those two files the node's ring cannot be read.

### What a sealed node needs to start again

The same keystore and KEK, `keys/node-master.key.wrapped` and `ring/keys/`. A start
that is missing one of them, or that does not match the data directory, is refused and
changes nothing in `ring/`; the message names the file or the setting and the fix:

| The start finds | It answers |
|---|---|
| another KEK | the key ids of both, and to point the keystore at the key the node was first started with |
| no key file at `key_file` | the path, and to restore the original key file |
| a sealed ring with `mode: off` | the evidence it found, and to start with `mode: on` and the keystore that sealed it |
| a plaintext ring with `mode: on` or `require` | the evidence, and to start with `mode: off` |
| sealed data with `ring/keys/` files missing | every missing file, and to restore `ring/keys/` from a backup of this data directory |
| a ring key file that does not unwrap | the file, and to restore `ring/keys/` from a backup |
| a ring key registry it cannot read | the file and the error (file ownership, a disk error), and what to fix |

### Every node of a cluster: the same setting

**Set encryption the same way on every node of a cluster. The cluster does not check
it.** A mixed-setting cluster is not checked in this release. Each node seals what it stores under its own key. Records travel between nodes
decoded, over the nodes' pinned TLS connections, and each receiving node seals what it
stores if, and only if, it runs with encryption on. So a node started with encryption
off, or a node whose `ring/` was moved aside and that came back with encryption off,
joins and serves without a refusal, and keeps **plaintext copies** of the records it
holds, including copies of records the other nodes keep sealed.

A node that joins a cluster makes its own keys on its first start, with its own
keystore and KEK; nothing key-related is sent. With encryption on, it seals every
record it copies from the others. Give each node its own KEK, so that one stolen disk
and one key open only that node's copy: its own records and the replicas it holds
(with three copies on three nodes, that is every record).

#### Check every node of a cluster

Ask each node, after every join and after any node's `ring/` was moved aside. With
metrics on (`HOARDDB_METRICS_ENABLED=true`; the listener is on loopback by default),
`/metrics` carries the node's configured mode and whether its store's files are
sealed:

```bash
for node in node-a node-b node-c; do
  printf '%s: ' "$node"
  ssh "$node" curl -s 127.0.0.1:9090/metrics | grep -e '^hoarddb_node_encryption_info' -e '^hoarddb_node_encryption_sealed' | tr '\n' ' '
  echo
done
```

```
node-a: hoarddb_node_encryption_info{keystore="keyfile",mode="on"} 1 hoarddb_node_encryption_sealed 1
node-b: hoarddb_node_encryption_info{keystore="keyfile",mode="on"} 1 hoarddb_node_encryption_sealed 1
node-c: hoarddb_node_encryption_info{keystore="keyfile",mode="on"} 1 hoarddb_node_encryption_sealed 1
```

Every line must show the same `mode`, and `sealed 1` wherever the mode is `on` or
`require`. "Sealed" is read from the store's files, not from the configuration.

### A node's mode is fixed when its ring is created

A node does not switch between sealed and plaintext in place, and a node that holds
data cannot be given new keys (there is no re-key). To change a node's mode:

- **A single node:** stop it, move `<data_dir>/ring` aside (keep it: it is the only
  copy of the node's data) and also the node store this build keeps beside it,
  `<data_dir>/store` and `<data_dir>/keys/store.wrapped`. Keep
  `encryption.keystore` set, so the node's credential files still open, and start it
  in the other mode. It starts empty; load the data into it again.
- **A cluster member:** do not move `ring/` aside while the cluster runs: that can
  lose committed cluster state. Stop the node, keep its `ring/` (move it aside), start
  it again in the other mode on an empty ring directory with the same node id, keys
  (`keys/`) and address, and run `replace-node <node> --with <node>@<addr>` on any
  member ([Add a node to a cluster](cluster-add-a-node.md#if-a-member-lost-its-data)).
  It pulls its copies from the other members and seals them if its mode is on. The
  refusal a member gets for a mismatched mode says the same.

### A crash during the very first flush

A plaintext store cut by a crash during its very first flush (the first time it writes
a table) is repaired at the next start. An encrypted one cannot be, and the start is
refused instead; nothing is changed. The node then holds only its very first writes. A
single node is told to move `ring/badger` aside and start again, so that it rebuilds
its store from its own log; a cluster member is told to come back with `replace-node`,
as above. If the refusal's way
does not work, keep `ring/` and report it.

### The node's credentials

With encryption on, the node also seals the credential files it keeps in its data
directory under the node master: `users.json`, a
standalone node's signing key (`auth.token.key`), the TLS private key
(`keys/server.key`) and, on a data node of a cluster, the cluster's password key
(`auth.cluster.pepper`). A copy of the data directory alone cannot sign a login, join
the cluster or impersonate the node. A node that finds these files in plaintext seals
them in place on its next start, keeping the same keys and certificate
fingerprint. A wrong password key the node kept aside as
`auth.cluster.pepper.wrong-<kid>` stays as it was when it was moved: sealed if
encryption was on then, plaintext if not. A start with encryption on does not seal those
plaintext copies; it logs a warning naming each one, so you can delete them. A sealed
file belongs to its node: a copy of it does not open on another node. Going
from `on` to `off`, keep `encryption.keystore` set: these files stay sealed.

### What it does not cover

- **Other nodes.** Each node seals only what it stores, and only when it runs with
  encryption on (above).
- **The cluster's metadata in `ring/raft`**: database names, member ids and addresses,
  the cluster id, settings, user names, roles and password hashes. No document value,
  store-field key or bucket name.
- **Server logs** (stderr, journald). **They may contain document ids, bucket names
  and store-field keys**: some error messages quote them. In an entity-centric store an
  id or a key is user data, so treat the logs as data.
- **Dumps**, which are plaintext whatever the mode, and the file `-export-key` writes.
- **Memory and swap**, including decrypted table indexes the store caches in memory,
  and core dumps.
- **Freed disk blocks** of files the store rewrote or removed.
- **Structure**: file sizes and names, entry lengths and expiry times in the store's
  files, the log's segment headers, `ring/FORMAT`, `ring/incarnation` and the store's
  sync marker (the file `VOUCHED` beside the store, two counters). No names, keys or values.
- **Files this build writes beside the ring:**
  `membership.json`, `wal/incarnation.bson` and its blob payload file names.
- **Tampering.** The ring log is authenticated, so a changed entry is detected; the
  store's encryption is not authenticated, so a changed byte there is not.
- **A running node.** Anyone who can run code as the server's user, or as root, can ask
  the same keystore for the key.

Put the data directory, the swap and the log destination on an encrypted block device
as well. For example, with LUKS on a dedicated disk or partition (it erases it):

```bash
sudo cryptsetup luksFormat /dev/sdb
sudo cryptsetup open /dev/sdb hoarddb-data
sudo mkfs.ext4 /dev/mapper/hoarddb-data
sudo mount /dev/mapper/hoarddb-data /var/lib/hoarddb
```

How the keystore works, the export and import of the node master, and what the node
does when its wrapped node master is missing:
[Configuration](configuration.md#encryption-at-rest).

## Default network exposure

A node you start with no arguments listens on **this machine only**. Nothing on
the network can reach it until you say so.

| What | Default (binary) | Setting |
| --- | --- | --- |
| Client port (TCP+TLS) | `127.0.0.1:7433` | `HOARDDB_TCP_LISTEN` |
| Node-to-node port | `127.0.0.1:4434` | `HOARDDB_LISTEN` (the node address; the port is its port plus one), or `HOARDDB_TCP_INTERNODE_LISTEN` to set it directly |
| Metrics | **off**; when on, `127.0.0.1:9090` | `HOARDDB_METRICS_ENABLED`, `HOARDDB_METRICS_LISTEN` |
| Profiling | **off**; when on, `127.0.0.1:6060` | `HOARDDB_PPROF_ENABLED` |
| Audit log | **off** | `HOARDDB_AUDIT_ENABLED`, `HOARDDB_AUDIT_OUTPUT` |

The start-up log says where the node listens, and when it is on loopback it
names the settings that open it up.

The start-up log shows the node's address in the ring, which is `HOARDDB_LISTEN`.
Its default, `127.0.0.1:4433` for `HOARDDB_LISTEN`, is the name other nodes use
for it, not a port the node opens.

### Opening a node to other machines

- **Applications on other machines.** Set the client address. `0.0.0.0` means
  every interface; use one interface's address to be narrower:

  ```bash
  HOARDDB_TCP_LISTEN=0.0.0.0:7433 hoarddb-server
  ```

- **A cluster across machines.** Start every node with `HOARDDB_CLUSTER_ENABLED=true` and
  set `HOARDDB_LISTEN` to the address the other nodes reach it on. The first node lists no
  seeds; each later node lists the nodes already in the cluster as `<node-id>@<host:port>` and
  pins each one's key. The second node:

  ```bash
  HOARDDB_CLUSTER_ENABLED=true HOARDDB_NODE_ID=node-b HOARDDB_LISTEN=192.0.2.2:4433 \
    HOARDDB_SEED_NODES=node-a@192.0.2.1:4433 HOARDDB_SEED_FINGERPRINTS=node-a=SHA256:<64 hex> ...
  ```

  A node whose seeds are on another machine but which is still on the loopback
  default is **refused at start**, with the setting to change in the message: it
  would tell its peers to reach it on `127.0.0.1`, which only it can do. A cluster
  whose nodes all run on one machine (every seed on `127.0.0.1`) needs no change.

Two things follow:

- On a host that is reachable from outside your network, bind to the interface
  your applications use rather than `0.0.0.0`, or put a firewall in front of the
  client port and the node-to-node port. A member accepts a peer only on a pinned
  key, but the port accepts a connection from anywhere it is reachable, so keep it
  to the nodes of your cluster.
- The client port is protected by login and TLS, not by obscurity. Anyone who can
  reach it can try passwords, which the failure delay slows down but does not
  stop. Choose long root and user passwords.

### Metrics exposure

When you enable metrics, `/metrics` is served over plain HTTP with **no
authentication**. It reports counts, durations and capacity readings for the
node and carries no document content, and it keeps no series per bucket. It is
still information about your system. It listens on the loopback interface by
default, so a scraper on the same machine needs nothing more; to let a
monitoring host reach it, bind to the address of your monitoring network (not
`0.0.0.0` on a host that faces the internet):

```bash
HOARDDB_METRICS_ENABLED=true HOARDDB_METRICS_LISTEN=192.0.2.5:9090 hoarddb-server
```

With metrics off, the node's capacity warnings still go to its log and to
`hoardDB status`. The profiling endpoint
is also unauthenticated and plain; leave it off on a server.

## What is not protected

A short list, so that you do not assume otherwise:

- **A connection's roles are set when it logs in.** A role change reaches an open
  connection only at its next login; a drop, a disable or a new password ends it.
- **A cluster member cut off from the others** cannot learn of a drop, a disable
  or a new password made elsewhere. It keeps accepting that user's logins and
  tokens from the users it last applied, with no time bound (it ignores
  `auth_config_max_staleness_seconds`), and a connection already open on it lasts
  until it rejoins and applies the change.
- **There is no way to revoke one login token** without affecting that user's
  others. Revocation is per user.
- **A cluster has no offline password reset.** `hoarddb-server -reset-password`
  refuses on a node with `cluster.enabled`, so keep at least two administrators.
  If no administrator can log in, the cluster's users cannot be recovered: the
  cluster is rebuilt on new, empty data directories and the data is loaded
  again from a dump taken earlier
  ([Keep two administrators](user-management.md#keep-two-administrators)).
- **A cluster's password key cannot be made again.** Passwords are checked with
  `auth.cluster.pepper`, which each data node keeps in its data directory. When
  every copy is gone, no password can be checked and there is no way around it:
  back up each data node's data directory
  ([The cluster's password key](cluster-add-a-node.md#the-clusters-password-key)).
- **A witness serves no logins.** It holds no password key and refuses every
  login and statement; connect to a data node.
- **Anyone who holds a node's key files** can act as it: the TLS private key
  (`keys/server.key`) lets its holder connect as that member, and the signing key
  (`auth.token.key`) lets its holder forge a login on that node.
- **Anyone who can read the data directory** can read a plaintext node's data
  and its credentials: the signing key and the TLS private
  key are plaintext files there. The only file sealed on a node without
  encryption at rest is `users.json`, and its key sits in the same directory.
  If a copy of such a directory is lost, rotate the root password and give the
  node a new TLS identity.
- **Plaintext credentials sealed on an upgrade leave their old disk blocks
  behind.** The node replaces each file with its sealed form; the freed blocks
  are not overwritten. Only disk encryption covers freed blocks.
- **A dump file, and the key file you export for escrow, are not encrypted.**
- **The CLI's `-insecure` flag** removes the server-identity check for that
  session.

## Reporting a vulnerability

Please report a security problem **privately**, not in a public issue or a
discussion.

Send your report by email to admin@hoarddb.com. Include:

- what you found and which version or commit it affects (`hoarddb-server -v`);
- how to reproduce it, as short as you can make it;
- what an attacker gains, if you know.

We acknowledge a report, agree a disclosure date with you, and fix the problem
before it is described in public. A security fix is announced in the changelog
after it ships, and the people who reported it are credited if they want to be.
Please do not test against systems you do not own.
