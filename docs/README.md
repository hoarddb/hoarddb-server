# hoardDB documentation

hoardDB is storage shaped by how you read. You pick the store that matches how
you read, and every user, order or device keeps its own queues, maps and files.
There are six stores (FIFO, LIFO, hash, btree, heap and blob). A bucket declares
its fields, a field can be a store of any of the six types, and each document
gets its own. The wire protocol is versioned binary over TCP+TLS.

You always choose the structure: `user.Purchases` is that user's own queue, and
hoardDB never picks one for you. A social network holds a person's feed, friends
and photos this way, and so can an order (its lines and events), a device (its
readings) or a session (its history).

The ring is the engine these pages describe, and a plain `hoarddb` session opens
it. A few pages were written before it and say at the top that they have not been
rewritten yet.

Start with [Installation](installation.md), then [Getting started](getting-started.md). The
sidebar lists every page.

## Start here

| Page | Read it when |
| --- | --- |
| [Installation](installation.md) | You want the server and CLI on a machine: build from source |
| [Quickstart](quickstart.md) | You want the shortest path from a build to a stored document, with every command to run |
| [Getting started](getting-started.md) | You want a running server and a working bucket, with real transcripts |
| [Data types](data-types.md) | You are declaring a field and want to know the type words, how to write a date, a decimal or a uuid, and what is checked |
| [Store types](store-types.md) | You need to choose an engine for an access pattern |
| [Example: large file stores](large-files.md) | Your files live on shared storage and you want hoardDB to track where they are, not hold the bytes |
| [How-to: from your schema to hoardDB](howto.md) | You have an application on a relational, document or in-memory layout and want to see how its reads become buckets and stores |
| [How-to: agent memory](howto-agent-memory.md) | Your application keeps sessions, scratch state, task queues and per-user memories for an agent |
| [How-to: chat rooms](howto-chat.md) | Your application has rooms, members on both sides and a newest-first message list per room |
| [How-to: a ledger](howto-ledger.md) | Your application keeps accounts with expenses and invoices under them and reads sums |
| [How-to: a social network](howto-social.md) | Your application has users with feeds, followers on both sides, posts with comments and likes, and fan-out |
| [How-to: a video library](howto-video.md) | Your application keeps the files on storage you already run and the owner, path, counters, comments and viewer progress in the database |
| [HQL reference](hql-spec.md) | You are writing queries; the statement families, and `help all` lists every statement |
| [Indexes](indexes.md) | You want to know why the ring has no indexes, what to do instead, and how documents expire |
| [Default settings](defaults.md) | You want to know what every setting is when you set nothing |
| [Configuration](configuration.md) | You are setting a key and want its default and its failure mode |
| [Add a node to a cluster](cluster-add-a-node.md) | You have a running pinned cluster and want one more node in it, or a join was refused |
| [User management](user-management.md) | You are creating, granting to, or disabling a user |
| [Limitations](limitations.md) | Before you design around hoardDB — read this one first |
| [Limits](limits.md) | You want a size or a count: the largest document, blob or frame, the memtable, connections, and what bounds the number of buckets |
| [Security](security.md) | You are deciding what to expose, how clients trust a server, whether to encrypt the node, or how to report a vulnerability |
| [Compatibility and versions](compatibility.md) | You are upgrading, mixing builds, or want to know what stays stable and how a feature is retired |
| [Replication](replication.md) | You want to know what the ring does across nodes, and what it does not do yet |
| [Backup and restore](backup-restore.md) | You want a `dump`/`restore` drill, what it guarantees, and what it does not carry |
| [Load a file](ingest.md) | You have a BSON file, a JSON Lines file or a directory of .bson files and want it in a bucket, with progress, a summary and a resume if it is interrupted |
| [Let hoardDB suggest a design](plan.md) | You have a dump of your data, or a description of what your application stores and reads, and want a design to start from, with a scorecard of how well the application fits |

## Five minutes, verified

Start a server. With no configuration it creates `./data` in the working
directory and bootstraps a root credential:

```
hoarddb-server
```

On a first start from a terminal it asks you to choose the root password (twice).
Started with no terminal (from a script or a service manager), it needs the
password set instead, and refuses without it: set `HOARDDB_ROOT_USER` and
`HOARDDB_ROOT_PASSWORD` before the first start. Either way only the password's
hash is kept, in `./data/users.json`, and the password is never printed.

`reset-password` changes that credential later (`hoarddb-server -reset-password admin`,
with the server stopped).

The server generates a self-signed certificate and listens on
`:7433` for drivers and the CLI. Node-to-node traffic uses a separate port and is
irrelevant until you run a second node ([configuration.md](configuration.md#cluster-mode)).

Connect and use it. The CLI is a REPL: it opens a session,
authenticates, and takes JavaScript-like statements. A bare `-password` asks for
the password on the terminal, so it never shows in `ps`:

```
hoarddb -insecure -user admin -password
```

```
[h]oardDB> create database app
OK  created database app (replication factor 1, this node's default)
[h]oardDB> use app
Switched to database 'app'
[h]oardDB [app]> create bucket User { Name string, Photos lifoStore }
OK  created bucket app.User (2 fields)
Fields: Name string, Photos lifoStore
[h]oardDB [app]> db.User.insert({_id_: "ada", Name: "Ada"})
OK  id=ada
[h]oardDB [app]> db.User["ada"].Photos.push({File: "beach.jpg"})
OK  pushed: Photos now holds 1 element
[h]oardDB [app]> db.User["ada"].Photos.peek()
{"File": "beach.jpg"}
```

`help` in the REPL is a quick start, and `help all` lists every statement.
Statements end with an optional semicolon. There are no comments in the language.

`-insecure` skips certificate verification and is for a local server you started
yourself. Against any other host, pass `-fingerprint SHA256:<hex>` with the
fingerprint the server printed instead.

## Where the rest of the documentation is

- **Writing a client** for a language we do not ship one for? The wire protocol
  is specified. The Go, Node.js and Python client libraries are pre-release, each
  in its own repository under `github.com/hoarddb` (`hoarddb-go`, `hoarddb-node`,
  `hoarddb-python`); their APIs can still change.

Every command on this page was run against a built server and CLI before it was
written down. If you find a page whose example does not work, that is a defect in
the documentation, not a mistake on your side — and worth reporting as one.
