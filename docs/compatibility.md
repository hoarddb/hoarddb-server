# Compatibility and versions

What you can rely on when you upgrade hoardDB: what talks to what, what is read
back from disk, what happens when you run two builds at once, which platforms
are built, and how a feature is retired.

hoardDB's first release is 1.1 and none has been tagged yet. Where this page says
"today", it describes the build in the repository. Where it states a rule, the
rule is stated for the releases that follow 1.1.

## What carries a version

Three things are versioned, separately from the release number, because they
have different consequences when they change.

| What | Where you see it | Today |
| --- | --- | --- |
| **Wire protocol** | `hoarddb-server -v` and `hoarddb -v` print `protocol=N`; the TLS handshake negotiates `hoarddb/N` | major `1` |
| **Data directory** | Checked by the server at start | storage layout version `2` (the shared store); ring directory format `9` |
| **Dump format** | `format_version` in a dump's `manifest.json` and each bucket's `metadata.json` | `3` (format `2` still restores) |
| **Cluster version** | `status` prints `Cluster version`; `hoarddb-server -v` prints the build's `state_level` and `state_range` | `1` |

The release (or `devel` for an untagged build) is printed by `-v` too:

```bash
hoarddb-server -v
```

```text
hoarddb-server version=devel commit= build_date= protocol=1
```

## The wire protocol

A client and a server speak the same protocol **major** or they do not talk.

- **The major is negotiated before anything is sent.** Both sides offer
  `hoarddb/1` in the TLS handshake (the ALPN identifier), and every frame also
  carries the version in its header, so a connection can never mix versions.
- **A mismatch is named, not left to a decode failure later.** A client that
  offers no major the server serves is refused in the handshake, and one that
  states a different major at login gets `server speaks protocol 1, client sent
  2`.
- **Inside a major, change is additive only.** New operations, new optional
  fields and new capability names can appear. A client ignores fields it does
  not know, and a server ignores capability names it does not know. A server
  that is asked for an operation it does not have answers `unsupported opcode`
  and the opcode's number, never silence.
- **A breaking change gets a new major.** It is never a silent reinterpretation
  of `hoarddb/1`. The server keeps serving the previous major for at least one
  release after a new one ships, and warns on every connection that uses the
  old one, with the date it will stop.

Today only major `1` exists, so no client has had to move yet. The one thing to
check when you mix versions is the line above: `hoarddb -v` and
`hoarddb-server -v` should print the same `protocol=`.

## The data directory

The server reads what is on disk at start and refuses what it does not
understand, instead of guessing:

- **The storage layout.** This build keeps every bucket in one shared store per
  node. A data directory in the per-bucket layout (it has a `catalog.json` of
  its own) is refused at start with a message that says so, and nothing in it
  is changed. Start the server with an empty data directory.
- **`users.json`.** It carries its own `format_version` (today `1`), and a file
  with another version is refused. It is sealed under a key your backups must
  keep; see [User management](user-management.md).
- **Encryption.** A node's encryption mode is fixed when its data directory is
  first started. A data directory started with encryption on is refused by a
  server configured without it, and the reverse; see
  [Configuration](configuration.md#encryption-at-rest).
- **Sealed credential files.** On a node with encryption at rest, the cluster
  token, the signing key and the TLS private key are sealed under the node key.
  A node that finds them in plaintext seals them in place at its next start, and
  that is one-way. A sealed file belongs to its node;
  do not copy the file to a new node. See
  [Security](security.md#encryption-at-rest).
- **The memtable size** a store chose at its first start is saved with the store
  and kept on every restart (`store/memtable-choice.json`).
- **Dumps.** `restore` accepts the dump format it writes (`format_version` 3)
  and format `2`, and refuses
  any other with the version on both sides in the message. There is no format 1
  to restore. A format 3 dump may carry a `<bucket>.dead.bson` file for a
  queue's or heap's dead entries; a restore keeps them dead.

A member that disagrees with the cluster about its state does not stay up: it exits with status 70 and a named line that says what to do ([A node that stops itself](cluster-add-a-node.md#a-node-that-stops-itself-exit-code-70)). Start a member that stops this way on the build the proposing member runs and keep its data directory. If it still stops, replace it: move its `ring` directory aside and run `replace-node` ([If a member lost its data](cluster-add-a-node.md#if-a-member-lost-its-data)); its slots are pulled from the other copies. There is no per-node rollback: the way back from an upgrade is the copy of the data directory taken before it, restored on every node.

### Going back is not supported

A node that has written to its data directory on a newer build cannot be started
on an older one.

So the way back is **a backup, not the old binary**: copy the whole data
directory before you upgrade, and go back by restoring that copy on every
node. [Limitations](limitations.md#cluster) and [Replication](replication.md)
have the details.

## Upgrading

No tagged release exists yet, so there is no earlier release to upgrade from.
This is the procedure the current builds require, and it is the one to follow
for every upgrade that touches a data directory.

1. **Stop writes and wait one minute.** The wait is the margin for the log to
   catch up.
2. **Stop every node.**
3. **Copy each node's whole data directory.** `dump` is not a substitute: it
   leaves out blob payloads and user accounts. A copy of the directory keeps
   both.
4. **Install the new build on every node.**
5. **Start every node.** Check `hoarddb-server -v` on each, then `status` and
   `nodes` from the CLI.

### Rolling upgrades

**A cluster is not upgraded one node at a time.** Do not run two builds side by
side in one cluster. Builds can disagree about where a bucket lives, and
that disagreement is not detected. Most other mismatches are: a node whose state
version is not the cluster's is refused when it joins and when it connects (below),
and a node on this build that disagrees about the cluster's state stops with exit
status 70 and a named line ([A node that stops itself](cluster-add-a-node.md#a-node-that-stops-itself-exit-code-70)).
A member copies data from the
others when it is brought back with `replace-node`, and a node on another build
is refused there too.

The rule that follows: **upgrade every node to the same build before you add or
replace a node**, and if you find a cluster of mixed builds, finish the upgrade
first. Upgrading a cluster one node at a time, with no outage,
is not available. The stop-all rule above is the only supported procedure.

**The cluster carries a committed cluster version.** Each build declares the
cluster versions it can run: `hoarddb-server -v` prints `state_level` (the
newest) and `state_range` (from the oldest to the newest). In this release both
are `1`. The cluster records the version it runs, set when it is bootstrapped,
and every member keeps it with the cluster state; `status` prints it as
`Cluster version: 1 (1.1)`. Every node checks it:

- **At start, and whenever it meets the version.** A node whose build cannot run
  the cluster's version refuses to start, or stops when it meets that version in
  a snapshot or a log entry. It exits with status 70 and a line that names the
  build to start, and changes nothing in its data directory: keep the directory
  and start the node on a build whose `state_range` includes the version
  ([A node that stops itself](cluster-add-a-node.md#a-node-that-stops-itself-exit-code-70)).
- **When a node joins.** `join` refuses a node whose build cannot act at the
  cluster's version, before anything is registered, with a line that names the
  version.
- **When a member connects.** Every member refuses the connections of a node
  whose build range does not overlap its own, logs the refusal once a minute
  and lists the node under `Refused peers` in `status`
  ([A node on another build is refused](cluster-add-a-node.md#a-node-on-another-build-is-refused)).

**Rolling upgrade.** This release has no earlier release to roll from, so every
node in a cluster runs the same build. It carries the version gate that a later
release needs to roll: whether a cluster on this release can be upgraded to a
later one node at a time is for that later release's notes to say. Moving a
cluster to a newer cluster version, and turning a feature on only once every
member supports it, are not available yet. **There is no per-node rollback:** the
way back is the copy of the data directory taken before the upgrade, restored on
every node.

## Supported platforms

The server and the CLI are released for two platforms:

| Operating system | Architecture |
| --- | --- |
| Linux | amd64, arm64 |

Windows is supported through WSL2, using the Linux binaries. The server and the
CLI do not compile for native Windows, so there is no `.exe` to build. macOS is
not supported natively in this release; on a Mac, run the Docker image. Native
macOS is not available.

FreeBSD 15.x on amd64 builds from source and runs: the port's builder built it and
ran the getting-started path on FreeBSD 15.x on 2026-11-18. We do not run FreeBSD
in CI. It is outside the released table, so no binary ships for it, and the
only release check it gets is the compile gate `scripts/build-matrix.sh` runs for
`freebsd/amd64`. Four things differ there. The interactive session has no line
editing and no pager, a password typed at a prompt is echoed, and `-password`
with no value is refused (set `HOARDDB_ROOT_PASSWORD` or pass
`-password=VALUE`). `status` reports the
descriptor, mapping and memory ceilings as unavailable, and the memtable takes
its 128 MiB default because no memory limit can be read. A store killed during
its first flush is not repaired automatically: the server refuses to start, names
the leftover table and says to move it out by hand.

What "supported" means, precisely: the platform build produces each file and
checks that it is the architecture its name says (see [Installation](installation.md)), and the captured session in
[Getting started](getting-started.md) was run end to end on `linux/amd64`.
A platform outside that table may compile, but only linux/amd64 and linux/arm64
are released; FreeBSD builds, runs and is compile-gated; macOS is untested; and
neither FreeBSD nor macOS is supported.

Building from source needs the Go version named in `go.mod` (Go 1.26.9
at the time of writing).

## How a feature is retired

The rule (and the reason for it): hoardDB never breaks your data or your
programs silently. Trust is the whole product.

- **Deprecate loudly.** A feature that is going away — a setting, a CLI verb or
  flag, an HQL statement, a configuration key — is deprecated in a minor
  release and keeps working until the next minor release *and* at least 90
  days after the deprecation shipped, whichever is later. Each time it is
  used it prints a warning on standard error that names what to use instead
  and the date the old way stops. Removing a deprecated feature does not need
  a major release; the removal is listed under **Removed** in the release
  notes.
- **Version explicitly.** A change a program could notice — a wire change, a
  changed meaning of a setting, a new on-disk layout — is named in the
  release notes under what changed and
  what to do about it,
  and a wire change raises the protocol major as described above.
- **Refuse, do not guess.** When a form is not accepted, it fails with a
  message that says what to run instead. For example, a statement the CLI does not
  have is refused with `"find" is not a statement. Run 'help' to list them, for
  example 'show databases'`, so an old script stops at once instead of doing
  something different.
- **Never lose data on the way.** An upgrade that cannot read your data stops
  and says so. It does not discard what it does not understand.

Before 1.1 this was not always possible. Changes made before a release —
for instance, the environment variable prefix that became `HOARDDB_` — were
made without a deprecation period, and the release notes record them. From 1.1 the
rules above apply.

