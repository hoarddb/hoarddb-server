# Installation

Build hoardDB from source, and you get a server answering on port `7433` and a CLI
that can log in to it, and [Getting started](getting-started.md) takes it from there.

hoardDB is two programs:

| Program | What it is |
| --- | --- |
| `hoarddb-server` | The database. With no arguments it starts a single node and needs no configuration file. |
| `hoarddb` | The client: an interactive session, a script on standard input, and the `dump`, `restore` and `ingest` commands. |


## Supported platforms

| Operating system | Architecture | Server | CLI |
| --- | --- | --- | --- |
| Linux | amd64 (x86-64) | yes | yes |
| Linux | arm64 | yes | yes |

Windows is supported through WSL2, using the Linux binaries. The server and the
CLI do not build for native Windows: `GOOS=windows go build ./cmd/server` stops
with `undefined: syscall.O_NOFOLLOW`, because both open key and dump files with
a flag Windows does not have, so there is no `.exe`. macOS is not
supported natively in this release: on a Mac, run the Docker image (the
`linux/arm64` image runs on Apple silicon and the `linux/amd64` image on Intel).
Native macOS binaries are not available. The binaries
are built with cgo off, so they
have no C library dependency and a Linux binary runs on any distribution of the
matching architecture.

FreeBSD 15.x on amd64 also builds from source. `GOOS=freebsd go build ./cmd/server`
and `GOOS=freebsd go build ./cmd/cli` build there, and the port's builder ran the session in
[Getting started](getting-started.md) on FreeBSD 15.x on 2026-11-18. We do not
run FreeBSD in CI. It is not a
released platform, so there is nothing to download, and its interactive session
is plainer: no line editing, no pager, a password typed at a prompt is echoed,
and `-password` with no value is refused, so set `HOARDDB_ROOT_PASSWORD` or pass
`-password=VALUE` instead. `status` reports its file-descriptor, mapping and
memory ceilings as unavailable there, and the memtable takes its 128 MiB default
because no memory limit can be read.

## Release binaries

Release downloads are not available yet. Build the binaries
yourself; the next section shows how.

<!-- TODO(owner): replace this section with the release URL and a download
     command once the first tagged release is cut. Keep the artifact names and
     the checksum command below: they are what the build already produces. -->

A release carries one server and one CLI per platform, named
`hoarddb-server-<os>-<arch>` and `hoarddb-<os>-<arch>`, plus a
`checksums.txt` of their SHA-256 sums. After downloading, check them before you
run anything:

```bash
sha256sum -c checksums.txt
```

Each line should end in `OK`. The checksum file lists every platform's files,
so `sha256sum` reports the ones you did not download as missing; add
`--ignore-missing` to skip them.

## Build from source

You need Go 1.26.9 or newer (the `go` line in `go.mod`). If your Go is
older but recent enough to manage toolchains (Go 1.21 or later), the `go`
command downloads the right one the first time you build; it needs network
access once to do so.

<!-- TODO(owner): the source repository is not public yet. Replace the
     directory step below with the real `git clone` URL when it is. -->

In the directory that holds `go.mod`:

```bash
make build
```

That writes `hoarddb-server` and `hoarddb` into that directory,
with the version and commit stamped in. Without `make`:

```bash
go build -o hoarddb-server ./cmd/server
go build -o hoarddb ./cmd/cli
```

Check what you built:

```bash
./hoarddb-server -v
./hoarddb -v
```

```text
hoarddb-server version=8f479df commit=8f479df build_date=2026-10-03T11:47:07Z protocol=1
hoardDB version=8f479df commit=8f479df build_date=2026-10-03T11:47:07Z protocol=1
```

A plain `go build` reports `version=devel`. That is honest, not an error: an
untagged build is not a release.

Put them on your `PATH` if you like:

```bash
sudo mv hoarddb-server hoarddb /usr/local/bin/
```

### Build for another platform

Set `GOOS` and `GOARCH` to the target and build both programs from the root of
the checkout, for example:

```bash
GOOS=linux GOARCH=arm64 go build -o hoarddb-server ./cmd/server
GOOS=linux GOARCH=arm64 go build -o hoarddb ./cmd/cli
```

The two release targets are `linux/amd64` and `linux/arm64`. The source also
cross-compiles for macOS (`GOOS=darwin`), but those builds are not released or
supported. It does not compile for native Windows (see above): use
WSL2.

## Start it

```bash
./hoarddb-server
```

With no configuration it creates `./data` in the working directory, generates a
self-signed certificate, creates the root user (asking for its password twice on a
terminal; only the hash is stored), and
listens on `127.0.0.1:7433` for
clients and `127.0.0.1:4434` for other nodes. That is this machine only: nothing
on the network can reach it. To accept clients from other machines set
`HOARDDB_TCP_LISTEN=0.0.0.0:7433`; to run a cluster across machines set
`HOARDDB_LISTEN` to each node's own address ([Security](security.md#opening-a-node-to-other-machines)).
Started without a terminal (a service manager, a script, a container), it does
not make a password up: it refuses to start, writes nothing to `./data`, and tells
you to set one. Give it the password for that first start only:

```bash
HOARDDB_ROOT_PASSWORD='<choose one>' ./hoarddb-server
```

The password must be at least 12 characters (`HOARDDB_PASSWORD_MIN_LENGTH`).
In another terminal:

```bash
./hoarddb -insecure -user admin -password
```

A bare `-password` asks for the root password on the terminal. `-insecure` skips certificate verification and is for a server you started
yourself on the same machine. [Security](security.md) shows the other way.

## Run it in Docker

The image runs as an unprivileged user (uid 1000) on a supported, minimal base
and refuses its first start without a root password. No release image is
published yet and the repository does not carry the Dockerfile, so there is no
`hoarddb` image to pull; use [Build from source](#build-from-source) until one is
published. Once you have an image tagged `hoarddb`, this is how it runs:

```bash
docker run -e HOARDDB_ROOT_PASSWORD='<choose one>' -p 127.0.0.1:7433:7433 hoarddb
```

Only `7433`, the client port, needs publishing. The ports other nodes use (`4433`
for the cluster listener, `4434` for the node-to-node port) stay unpublished unless a node outside your Docker network must reach this
one; then publish the one you use on an address you choose.

The example three-node `docker-compose.yml` starts a ring: `node1` starts the
cluster, `node2` and `node3` wait to be joined, and every key is already pinned.
Run `join node2@node2:4433` and `join node3@node3:4433` on `node1` to form it
([Configuration](configuration.md#cluster-mode)). This file has not been run under Docker; report what you see. It sets copies on the database, not with `HOARDDB_REPLICATION_FACTOR`, and follows the same rules. It publishes
the client ports on `127.0.0.1` only (`7433`, `7434`, `7435`) and keeps the
internode port on the compose network. It hands the root password to the nodes as
a file (`HOARDDB_ROOT_PASSWORD_FILE`), not as an environment variable, so it does
not show in `docker inspect`, and each node gets only its own private key. Set
`HOARDDB_ROOT_PASSWORD` on the host before `docker compose up`. The compose
file's CLI example uses `--insecure` because the nodes' certificates are
self-signed; against your own nodes, pin one with `-fingerprint`
(see [Security](security.md#trusting-a-server-from-the-cli)).

The optional `proxy` profile puts every client behind one address, so the
per-address limits (the failed-login lockout and the 64-connection cap) are
shared by everyone using it. Connect to the node ports directly when more than a
few clients share the cluster.

## Upgrading

Read [Compatibility and versions](compatibility.md) before you upgrade a
node that holds data you care about. In short: back up the whole data
directory first, stop and upgrade every node of a cluster together (upgrading one
node at a time is not available), and do not expect to roll back
by starting the old build on the new data.

## If it does not start

- **`bind: address already in use`** — something else holds `7433` (or `4434`).
  Stop it, or set `HOARDDB_TCP_LISTEN` to another address.
- **A client on another machine gets "connection refused"** — the server
  listens on `127.0.0.1` unless told otherwise. Set
  `HOARDDB_TCP_LISTEN=0.0.0.0:7433` (or one interface's address) and restart.
  The start-up log says which addresses the node is listening on.
- **The server refuses to start and names `HOARDDB_LISTEN`** — it has seeds on
  other machines but is still listening on loopback, so its peers could not reach
  it. Set `HOARDDB_LISTEN` to this node's own address, for example
  `192.0.2.1:4433`.
- **The CLI says the server is not trusted** — it has never seen this server's
  certificate and refuses to trust it without asking. Pass `-insecure` for a
  local server, or see [Security](security.md).
- **The CLI says no credentials** — it was given no user and password. Pass
  `-user admin -password` (a bare `-password` asks for it), or set
  `HOARDDB_ROOT_USER` and `HOARDDB_ROOT_PASSWORD`. The server does not keep the
  root password anywhere the CLI could read it: only its hash is stored.
- **A container or a node with `HOARDDB_REQUIRE_ROOT_PASSWORD=true` will not
  start, naming `HOARDDB_ROOT_PASSWORD`** — set it (or
  `HOARDDB_ROOT_PASSWORD_FILE`) for the first start:
  `docker run -e HOARDDB_ROOT_PASSWORD='<choose one>' ...`.
