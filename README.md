# hoardDB

**Storage shaped by how you read.** You pick the store that matches how you read,
and every user, order or device keeps its own queues, maps and files.

hoardDB has six stores: FIFO, LIFO, hash, btree, heap and blob. A bucket declares
its fields, a field can be a store of any of the six types, and each document gets
its own. `User.Purchases` is that user's own queue. You always choose the
structure; hoardDB never picks one for you.

hoardDB's first release is 1.1. [Limitations](docs/limitations.md)
is the honest list of what it does not do yet.

## Features

- **Six stores per entity**: FIFO, LIFO, hash, btree, heap and blob, declared as fields of a bucket and read back in their own order
- **HQL**: JSON-shaped statements with store-native verbs (`push`, `pop`, `peek`, `claim`, `ack`, `put`, `get`)
- **An interactive CLI**: Tab completion, a startup summary of the node you reached, and `help` generated from the same grammar the parser reads
- **Batching**: up to 255 writes in one request (on the wire; it is not a transaction)
- **TLS**: a self-signed certificate is generated on first start, and clients pin its fingerprint
- **Authentication**: always required, Argon2id password hashes, roles and per-database grants
- **Cluster**: a second node joins a cluster that holds no data with `join`. Replication across nodes is not usable end to end yet
- **BSON wire format**: a wire-format choice, not a compatibility claim
- **Prometheus metrics and audit logging**: both off by default

## Quick start

Build from source, in the directory that holds `go.mod` (there are no release
binaries yet):

```bash
go build -o hoarddb-server ./cmd/server
go build -o hoarddb ./cmd/cli
```

Start the server. It needs no configuration, and asks you to choose a root password:

```bash
./hoarddb-server
```

In a second terminal, connect and try it:

```text
$ ./hoarddb -insecure -user admin -password
[h]oardDB> create database app
[h]oardDB> use app
[h]oardDB [app]> create bucket User { Name string, Photos lifoStore }
[h]oardDB [app]> db.User.insert({_id_: "ada", Name: "Ada"})
[h]oardDB [app]> db.User["ada"].Photos.push({File: "beach.jpg"})
[h]oardDB [app]> db.User["ada"].Photos.peek()
{"File": "beach.jpg"}
```

[Getting started](docs/getting-started.md) has the same walk with every line
of output, and what to do when a command is refused.

## Build and test

Requires Go 1.26.9 or newer (the `go` line in `go.mod`). A Go that manages
toolchains (1.21 or later) downloads the right one on the first build.

```bash
make build            # hoarddb-server and hoarddb, with the version stamped in
make test             # the test suite
make lint             # gofmt, go vet, the tests, and golangci-lint (required before a push)
make docs-links       # every relative link in docs/ resolves
```

`make lint` fetches a fixed version of `golangci-lint` into `.ci-tools/` on first use, so
it needs network access once. [CONTRIBUTING.md](CONTRIBUTING.md) has the rest.

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Build, first run, connect, declare a bucket |
| [Store Types](docs/store-types.md) | The six stores and when to use each |
| [Configuration](docs/configuration.md) | Environment variables, YAML config, TLS, logging, cluster mode |
| [Limitations](docs/limitations.md) | What is not available yet |
| [Replication](docs/replication.md) | What the ring does across nodes, and what it does not do yet |
| [HQL Spec](docs/hql-spec.md) | The statement families; `help all` lists every statement |

## License

The hoardDB **server** is licensed under the Business Source License 1.1 (BSL 1.1).
See [LICENSE](LICENSE) for the full text and parameters.

The client libraries are Apache-2.0 and live in their own repositories under `github.com/hoarddb`: `hoarddb-go`, `hoarddb-node` and `hoarddb-python`, with the wire codec in `hoarddb-wire`. They are pre-release: their APIs can still change. There is no client-library code in this repository.
