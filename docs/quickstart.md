# Quickstart

From a source checkout to a stored document in about five minutes. The commands
run as printed; [Installation](installation.md) covers the other platforms and Docker.

## 1. Build

Run this in the directory that holds `go.mod`. You need Go 1.26.9 or newer.

```bash
go build -o hoarddb-server ./cmd/server
go build -o hoarddb ./cmd/cli
```

You get two programs: `hoarddb-server` is the database and `hoarddb` is the client.

## 2. Start the server

```bash
./hoarddb-server
```

No configuration file is needed. On the first start from a terminal the server
creates `./data`, generates a self-signed certificate and asks you to choose the
root password, twice. It listens on `127.0.0.1:7433`, so only this machine can
connect. Leave it running.

Started with no terminal (a script or a container), it refuses to start without
a password. Set one for the first start instead:

```bash
HOARDDB_ROOT_PASSWORD='<choose one>' ./hoarddb-server
```

The password must be at least 12 characters.

## 3. Connect

In a second terminal, in the same directory:

```bash
./hoarddb -insecure -user admin -password
```

Type the root password at the `Password:` prompt. `-insecure` skips certificate
verification, which is right for a server you started yourself on this machine
and wrong for any other. The client warns that verification is off, prints
`Authenticated as admin` and gives you the `[h]oardDB>` prompt.

## 4. Create a database, a bucket and a document

Type these at the prompt, one statement per line:

```text
create database app
use app
create bucket User { Name string, Purchases fifoStore }
db.User.insert({_id_: "ada", Name: "Ada"})
db.User["ada"].get()
db.User.list()
db.User["ada"].Purchases.push({Item: "coffee"})
db.User["ada"].Purchases.peek()
```

`create database app` makes the database and `use app` selects it: the prompt
becomes `[h]oardDB [app]>`. The bucket declares its fields: `Purchases` is a
store, so each user document gets a queue of its own. `insert` answers
`OK  id=ada`, `get` prints `{"Name": "Ada", "_id_": "ada"}`, and `list` prints
every document in the bucket. `push` adds an item to Ada's queue and answers
`OK  pushed: Purchases now holds 1 element`; `peek` reads the next one back
without removing it: `{"Item": "coffee"}`.

## 5. Stop

Type `exit` at the prompt. In the server's terminal, press Ctrl-C; the last
line the server logs is `server stopped`, once it has shut down cleanly.

Databases, buckets and documents survive a restart. Start `./hoarddb-server`
again from the same directory (it does not ask for a password this time),
connect as in step 3, type `use app` and then `db.User["ada"].get()`. Ada is
still there, now shown with her queue:
`{"Name": "Ada", "Purchases": "fifoStore (1 element)", "_id_": "ada"}`.

## Where next

- [Getting started](getting-started.md): updates, conditional writes, every store type
- [Load a file](ingest.md): load a BSON or JSON Lines file into a bucket
- [Configuration](configuration.md): every setting and its default
