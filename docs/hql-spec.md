# hoardDB Query Language (HQL)

HQL is what you type at the `hoarddb` prompt, or pipe into it. The statement list
is generated from the grammar the CLI parses, so it is always the list that runs on
the build you have. In a session:

```text
[h]oardDB> help all
```

`help all` lists every statement, `help <word>` shows every form of one (`help create`,
`help push`), and `help types` lists the six stores and the verbs each runs. The full
statement list is in `help all`. [Getting started](getting-started.md) walks through
the statements below with their output.

## The statement families

Each family has one example. All of them run as printed against a fresh server.

**Databases**

```text
[h]oardDB> create database app
```

**Buckets.** A bucket declares fields; a field is a scalar or a store, and a store
belongs to each document.

```text
[h]oardDB> use app
[h]oardDB [app]> create bucket User { Name string, Age int, Photos lifoStore, Friends hashStore }
[h]oardDB [app]> alter bucket User { add Email string }
```

**Documents**

```text
[h]oardDB [app]> db.User.insert({_id_: "ada", Name: "Ada", Age: 36})
[h]oardDB [app]> db.User["ada"].get()
[h]oardDB [app]> db.User.list({filter: {Age: 36}, limit: 10})
[h]oardDB [app]> db.User.update({_id_: "ada", $set: {Age: 37}})
```

`upsert`, `delete` and `count` follow the same shape.

**Store fields.** A document's own stack, queue, map, heap or blob is written and
read through the field.

```text
[h]oardDB [app]> db.User["ada"].Photos.push({File: "beach.jpg"})
[h]oardDB [app]> db.User["ada"].Photos.peek()
[h]oardDB [app]> db.User["ada"].Friends.put("grace", {Name: "Grace"})
```

`help types` says which verb runs on which store.

**Users**

```text
[h]oardDB> create user ada roles [reader@app]
```

**Looking around**

```text
[h]oardDB> show databases
[h]oardDB> show buckets in app
[h]oardDB> describe bucket User in app
```

**The cluster**

```text
[h]oardDB> nodes
[h]oardDB> status
```

`join node-d@<address>` adds a node; [Add a node](cluster-add-a-node.md) has the steps.

**Loading and copying data.** `ingest`, `dump` and `restore` are commands of the
`hoarddb` program, not statements typed at the prompt:

```bash
hoarddb ingest users.bson --into app.users --insecure
hoarddb dump --out app.hoarddump
```

See [Ingest](ingest.md) and [Backup and restore](backup-restore.md).

## Not available yet

- Indexes: an `index` clause in `create bucket` or `alter bucket` is refused. See [Indexes](indexes.md) for a lookup that does not scan.
- Search and range reads.
- `put` on a blob store, and `push` on a heap store from the CLI (the Go client can push to a heap).
- Dropping a database and changing a database's replication factor.

`help <word>` says whether a statement runs on your build.

## Legal notes

hoardDB is not wire-compatible with any other database and does not aim to be. HQL is
inspired by JSON syntax; it is a different language from every other query language.
No other database's tools connect to hoardDB.
