# Getting Started

Five minutes from install to a working database: start the server, connect the
CLI, declare a bucket whose documents keep their own stacks and queues, write,
read.

The prompts and output on this page are copied from real sessions against a
build of the repository. Where a value differs on your machine (a generated
identifier, a password, a fingerprint), the page says so.

## Prerequisites

- **Linux** on amd64 or arm64 (Windows through WSL2; on macOS, run the Docker image, as native macOS is not available)
- **Go 1.26.9 or newer** if you build from source; [Installation](installation.md)
  covers that
- No configuration file: the server runs with [sensible defaults](defaults.md)

## Build it

In the directory that holds `go.mod`:

```bash
go build -o hoarddb-server ./cmd/server
go build -o hoarddb ./cmd/cli
```

Two programs: `hoarddb-server` runs the database, `hoarddb` is the client
(an interactive session, or a script on standard input). `make build` does the
same and stamps the version in.

## Start the server

```bash
./hoarddb-server
```

That is the whole first run; there is no separate `init` step. On first start the
server creates `./data`, generates a self-signed certificate, and creates the root
user. Started from a terminal, it asks you to choose the root password, twice:

```text
No root password is set. Choose one for the root user "admin":
Repeat it:
```

Started with no terminal (from a script, a service manager or a container without
`-t`), there is nobody to ask, so give it the password instead:

```bash
HOARDDB_ROOT_PASSWORD='<choose one>' ./hoarddb-server
```

Without it, an unattended first start refuses, writes nothing and names these two
ways forward. It never makes a password up: one it chose would have to be printed
somewhere a log keeps it.

The root password is never written to disk: the server keeps only its hash, in
`data/users.json`. A copy or backup of the data directory therefore does not
carry it.

The server's log has the line that says it is ready:

```text
{"level":"INFO","msg":"TCP driver listener listening","address":"127.0.0.1:7433","fingerprint":"SHA256:d8aa96c0…","alpn":"hoarddb/1"}
{"level":"INFO","msg":"internode TCP listener listening","address":"127.0.0.1:4434","alpn":"hoarddb/1"}
{"level":"INFO","msg":"metrics server disabled"}
```

Clients connect to port `7433`. The server listens on `127.0.0.1`, so only this
machine can reach it; the start-up log says so and names the setting that opens
it up (`HOARDDB_TCP_LISTEN=0.0.0.0:7433`, see [Security](security.md#default-network-exposure)).
Ports `4433` (the cluster listener, bound only when you set
`HOARDDB_CLUSTER_ENABLED=true`) and `4434` (node to node) are irrelevant until you
run a second node
([Configuration](configuration.md#cluster-mode)). To choose the root credential without the
prompt, set `HOARDDB_ROOT_USER` and `HOARDDB_ROOT_PASSWORD` before the first
start.

Stop the server with Ctrl-C; it logs `server stopped` once it has shut down
cleanly.

Useful environment variables (all optional; the full list is in
[Configuration](configuration.md)):

| Variable | Default | Meaning |
|----------|---------|---------|
| `HOARDDB_TCP_LISTEN` | `127.0.0.1:7433` | Client listen address. Loopback means this machine only; set `0.0.0.0:7433` to accept clients from other machines |
| `HOARDDB_DATA_DIR` | `./data` | Where store files, keys and users live |
| `HOARDDB_ROOT_USER` | `admin` | Root user the server creates on first start |
| `HOARDDB_ROOT_PASSWORD` | asked on a terminal | Root password for the first start; if unset, asked for on a terminal, and a first start with no terminal refuses. Only its hash is stored |
| `HOARDDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Connect the CLI

In a second terminal, in the same directory, connect:

```bash
./hoarddb -insecure -user admin -password
```

```text
Password:
WARNING: TLS certificate verification is disabled (--insecure); the connection to 127.0.0.1:7433 cannot be trusted and may be intercepted. Use this only against a local development server.
Authenticated as admin
Connected to 127.0.0.1:7433
┌─ hoardDB ────────────────────────────────────────────────────────────────┐
│ node        my-laptop                     version devel                  │
│ address     127.0.0.1:7433                uptime 5s                      │
│ cluster     single node                   RF default 1                   │
│ process mem 5.4 MiB of 28.7 GiB (0.0%)                                   │
│ cpus        16                            load 0.31 0.25 0.20            │
│ server ram  9.2 GiB of 28.7 GiB                                          │
│ session     admin — no database selected  2026-11-03 10:01:03            │
└──────────────────────────────────────────────────────────────────────────┘
No databases yet. Start with: create database app
Type 'help' to get started, 'exit' to quit.

[h]oardDB>
```

The box is the node you reached: its name, address and uptime, and in a cluster
its state, how many members there are and which one leads (a single node says
`single node`). It is drawn only on a terminal. A piped run prints two plain lines
instead, and `show banner` draws the box again.

With no `-address` the CLI dials the client listen address from the same
configuration the server uses (`HOARDDB_TCP_LISTEN`, which defaults to
`127.0.0.1:7433`). A bare `-password` asks for the password on the terminal, so it
appears neither in `ps` nor in your shell history. Instead of the flags you can
set the environment, as you did for an unattended first start. The environment
keeps the password out of `ps`, but an inline assignment like the one below is
still recorded in your shell history.

```bash
HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD=… ./hoarddb -insecure
```

`-insecure` skips the check that you reached the server you meant to, and is for
a server you started yourself on the same machine. Against any other host, leave
it out and pass `-fingerprint SHA256:<hex>` to pin the server; without either,
the session trusts no server. [Security](security.md) has the details.

`help` is a one-screen quick start made of statements that run as printed.
`help <word>` shows every form of one statement (`help create`, `help push`),
`help types` lists the six stores and the verbs each runs, and `help all` lists
every statement. Tab completes statements, database names, bucket names and
field names. Help and completion are generated from the grammar the parser
reads, so neither lists a statement that does not run.

You can also pipe statements in, which is how the examples below were captured.
The CLI prints no prompt in that case:

```bash
./hoarddb -insecure <<'EOF'
show databases
exit
EOF
```

## Declare a bucket, write, read

A bucket declares **fields**. A field is a scalar (`string`, `int`, `double`,
`bool`, `datetime` and the rest of [Data types](data-types.md)) or a **store**, and a store
belongs to each document: every `User` has a `Photos` stack of its own. You
choose the structure, here and for every field; hoardDB never picks one for you.

```text
[h]oardDB> create database app
OK  created database app (replication factor 1, this node's default)
Next: use app
Then: create bucket <name> { Name string, Purchases fifoStore }   (declare every field)
[h]oardDB> use app
Switched to database 'app'
[h]oardDB [app]> create bucket User { Name string, Age int, Photos lifoStore, Friends hashStore, Recipes btreeStore }
OK  created bucket app.User (5 fields)
Fields: Name string, Age int, Photos lifoStore, Friends hashStore, Recipes btreeStore
[h]oardDB [app]> db.User.insert({_id_: "ada", Name: "Ada", Age: 36})
OK  id=ada
[h]oardDB [app]> db.User.insert({_id_: "bob", Name: "Bob", Age: 41})
OK  id=bob
[h]oardDB [app]> db.User["ada"].get()
{"Age": 36, "Name": "Ada", "_id_": "ada"}
[h]oardDB [app]> db.User.list({filter: {Age: 41}})
{"Age": 41, "Name": "Bob", "_id_": "bob"}
[h]oardDB [app]> db.User.count()
2
```

`insert` returns the document's id. A document is read by id with
`db.User["ada"].get()`, and `list` pages through the bucket (`list({filter: …,
limit: 10})`; a page that is not the last prints a `more:` line to continue
from). `upsert` adds a document or replaces the one with the same `_id_`, and
`delete` removes a document and its stores.

`update` changes some of a document's fields, in one write, with the operators
`$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$push`, `$addToSet` and
`$rename`:

```text
[h]oardDB [app]> db.User.update({_id_: "ada", $set: {Age: 37}})
OK  updated id=ada
[h]oardDB [app]> db.User["ada"].get({fields: ["Name", "Age"]})
{"Age": 37, "Name": "Ada", "_id_": "ada"}
```

Naming `fields` returns only those fields. A field can be added to a running
bucket with `alter bucket User { add Email string }`; no document is rewritten,
and existing documents read the new field as unset.

## A store on each document

Ada's `Photos` is her own stack. Bob's is his:

```text
[h]oardDB [app]> db.User["ada"].Photos.push({File: "beach.jpg"})
OK  pushed: Photos now holds 1 element
[h]oardDB [app]> db.User["ada"].Photos.push({File: "sunset.jpg"})
OK  pushed: Photos now holds 2 elements
[h]oardDB [app]> db.User["ada"].Photos.peek()
{"File": "sunset.jpg"}
[h]oardDB [app]> db.User["ada"].Photos.pop()
{"File": "sunset.jpg"}
[h]oardDB [app]> db.User["bob"].Photos.count()
0
```

A `lifoStore` is a stack and hands back the newest element first. A `fifoStore` is
a queue and hands back the oldest. `peek` looks without removing, `pop` removes
and returns, and `pop(3)` removes up to three in one write.

A `hashStore` holds elements by key, and a `btreeStore` holds them by key in key
order:

```text
[h]oardDB [app]> db.User["ada"].Friends.put("grace", {Name: "Grace"})
OK  put "grace": Friends now holds 1 element
[h]oardDB [app]> db.User["ada"].Friends.get("grace")
{"Name": "Grace"}
[h]oardDB [app]> db.User["ada"].Recipes.put("2026-11-01", {Dish: "soup"})
OK  put "2026-11-01": Recipes now holds 1 element
[h]oardDB [app]> db.User["ada"].Recipes.put("2026-10-01", {Dish: "bread"})
OK  put "2026-10-01": Recipes now holds 2 elements
[h]oardDB [app]> db.User["ada"].Recipes.list()
2026-10-01  {"Dish": "bread"}
2026-11-01  {"Dish": "soup"}
```

To consume a queue without losing work, `claim` leases elements to a worker and
`ack` removes them once the work is done; an element whose lease runs out comes
back. `help claim` and `help ack` have the forms.

```text
[h]oardDB [app]> create bucket Job { Title string, Queue fifoStore }
OK  created bucket app.Job (2 fields)
Fields: Title string, Queue fifoStore
[h]oardDB [app]> db.Job.insert({_id_: "j1", Title: "nightly"})
OK  id=j1
[h]oardDB [app]> db.Job["j1"].Queue.push({Task: "resize"})
OK  pushed: Queue now holds 1 element
[h]oardDB [app]> db.Job["j1"].Queue.push({Task: "email"})
OK  pushed: Queue now holds 2 elements
[h]oardDB [app]> db.Job["j1"].Queue.claim(1)
lease 000001a1a0dc4ed5d69d8946625f3294
{"Task": "resize"}
[h]oardDB [app]> db.Job["j1"].Queue.ack("000001a1a0dc4ed5d69d8946625f3294")
OK  acked lease 000001a1a0dc4ed5d69d8946625f3294
```

A `heapStore` and a `blobStore` can be declared, but not every verb runs yet: a
heap store reads with `peek`, `list`, `count` and `pop` and has no `push` from the
CLI (the Go client can push), and a blob store has `count` and no `put`.
[Limitations](limitations.md#not-available-yet) lists what is not
available yet, and `help types` always says what runs on the build you have.

## Change a document only if it still says what you expect

An update normally applies whatever the document holds. When the next state
depends on the current one, add `if` and the change applies only while the named
fields still hold those values, and `return: "after"` gives you the document back
in the same call:

```text
[h]oardDB [app]> create bucket Shipment { Status string, Version int, Owner string }
OK  created bucket app.Shipment (3 fields)
[h]oardDB [app]> db.Shipment.insert({_id_: "s1", Status: "processing", Version: 7})
OK  id=s1
[h]oardDB [app]> db.Shipment.update({_id_: "s1", if: {Status: "processing"}, $set: {Status: "shipped"}, return: "after"})
OK  updated id=s1
{"Status": "shipped", "Version": 7, "_id_": "s1"}
```

(The CLI prints a document's fields in sorted order, so `_id_` comes last.)

If another writer got there first, nothing changes, no position is taken, and
the answer shows what the document holds now. The refusal is an error line, so
in a piped script it ends the script with exit status 2:

```text
[h]oardDB [app]> db.Shipment.update({_id_: "s1", if: {Status: "processing"}, $set: {Status: "cancelled"}})
CLI error: NOT APPLIED  Shipment "s1": Status is "shipped", not "processing". Nothing was changed.
{"Status": "shipped", "Version": 7, "_id_": "s1"}
Re-read with db.Shipment["s1"].get(), then retry with the value you see.
```

Two writers can never both win: the document's owner checks the condition and
applies the change as one step. `null` matches a field that was never set, which
makes a lease: the first worker to ask takes it, the next is told who holds it:

```text
[h]oardDB [app]> db.Shipment.update({_id_: "s1", if: {Owner: null}, $set: {Owner: "worker-1"}, return: "after"})
OK  updated id=s1
{"Owner": "worker-1", "Status": "shipped", "Version": 7, "_id_": "s1"}
```

A version column is the same pattern in one line, and an `$inc` with
`return: "before"` hands out the next number of a sequence: the number you
get is the value the counter held before your increment, and no other caller
can get the same one:

```text
[h]oardDB [app]> db.Shipment.update({_id_: "s1", if: {Version: 7}, $inc: {Version: 1}, return: "after"})
OK  updated id=s1
{"Owner": "worker-1", "Status": "shipped", "Version": 8, "_id_": "s1"}
[h]oardDB [app]> db.Shipment.update({_id_: "s1", $inc: {Version: 1}, return: "before"})
OK  updated id=s1
{"Owner": "worker-1", "Status": "shipped", "Version": 8, "_id_": "s1"}
```

Here the second answer is the document as it was before that increment, so
`Version` is the 8 this caller was handed and the counter now holds 9.

`delete` takes `if` and `return: "before"` too. `if` compares declared scalar
fields for equality; a store is changed with its own verbs. `insert` and `upsert`
take neither `if` nor `return` (a field that is itself named `if` or `return` is
written with update's `$set`). `help update` has the forms.

## Documents that expire

A session or a token should leave when it runs out. Declare the field that holds the expiry instant and name it:

```
create bucket Session { UserId objectid, ExpiresAt datetime, expire at ExpiresAt }
db.Session.insert({_id_: "s1", UserId: ObjectId(), ExpiresAt: <now plus 30 minutes>})
```

Once `ExpiresAt` passes, `db.Session["s1"].get()` answers `NOT_FOUND` at once and the node removes the document soon after. The field holds the expiry instant, not the creation time; see [Documents and queue elements that expire](store-types.md#documents-and-queue-elements-that-expire).

## Inspect what you made

```text
[h]oardDB [app]> show buckets
BUCKETS in app
---
NAME  ID                STATE
Job   310241c42cf2eade  active
User  6374c74b680168bc  active
[h]oardDB [app]> describe bucket User
BUCKET app.User
---
Id:                  6374c74b680168bc
State:               active
Declaration version: 1
Fields:              (in declaration order)
  Name         string
  Age          int
  Photos       lifoStore
  Friends      hashStore
  Recipes      btreeStore
```

`show databases`, `describe database app`, `status` and `nodes` cover the rest.
`drop bucket User` deletes the bucket and every document and store in it. It runs
at once, with no confirmation: the bucket stops answering the moment the drop
starts, and its documents are deleted in the background.

## Data survives a restart

Databases, buckets and every document are on disk under the data directory. Stop
the server with Ctrl-C and start it again from the same directory:

```bash
./hoarddb-server
```

```text
[h]oardDB [app]> db.User["ada"].Friends.get("grace")
{"Name": "Grace"}
```

A restart keeps the root credential too: the server's users are stored, as
password hashes, in `users.json` beside the data. A restart never asks for the
root password again.

## A second node

Start a node with `HOARDDB_CLUSTER_ENABLED=true` and a second node can join it.
[Configuration](configuration.md#cluster-mode) has the steps: pin each node's
key, then run `join`. A node joins an empty cluster, so form the cluster first and
create data after. Copying data between nodes is not usable
end to end yet.

## Where to go next

- [Limitations](limitations.md) and [Limits](limits.md): read these before you design around hoardDB
- [Store types](store-types.md): which store for which access pattern
- [HQL reference](hql-spec.md): the statement families; `help all` lists every statement
- [User management](user-management.md): more users than `admin`
- [Security](security.md): TLS, users and encryption
